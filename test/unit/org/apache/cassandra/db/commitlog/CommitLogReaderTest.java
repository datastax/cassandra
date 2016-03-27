/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.db.commitlog;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.commitlog.AbstractCommitLogSegmentManager.SegmentManagerType;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.io.util.RandomAccessReader;

import static org.junit.Assert.assertEquals;

public class CommitLogReaderTest extends CQLTester
{
    private static boolean _initialized = false;
    private static String KEYSPACE = "clr_test";
    private static String TABLE = "clr_test_table";

    @After
    public void after() throws Throwable
    {
        execute(String.format("TRUNCATE TABLE %s;", TABLE));
    }

    /**
     * Intentionally not using static @BeforeClass since we need access to CQLTester internal methods here and we want
     * the convenience of access to its guts for the following tests.
     */
    @Before
    public void before() throws Throwable
    {
        if (!_initialized)
        {
            execute(String.format("DROP KEYSPACE IF EXISTS %s;", KEYSPACE));
            execute(String.format("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};", KEYSPACE));
            execute(String.format("USE %s;", KEYSPACE));
            execute(String.format("CREATE TABLE %s (idx INT, data TEXT, PRIMARY KEY(idx));", TABLE));
            _initialized = true;
        }
    }

    /**
     * Returns offset of active written data at halfway point of data
     */
    private CommitLogSegmentPosition populateData(int entryCount) throws Throwable
    {
        assert entryCount % 2 == 0 : "entryCount must be an even number.";
        int midpoint = entryCount / 2;

        for (int i = 0; i < midpoint; i++)
            execute(String.format("INSERT INTO %s (idx, data) VALUES (?, ?)", TABLE), i, Integer.toString(i));

        CommitLogSegmentPosition result = CommitLog.instance.getCurrentSegmentPosition(SegmentManagerType.STANDARD);

        for (int i = midpoint; i < entryCount; i++)
            execute(String.format("INSERT INTO %s (idx, data) VALUES (?, ?)", TABLE), i, Integer.toString(i));

        Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE).forceBlockingFlush();
        return result;
    }

    private File getFirstCommitLogFile()
    {
        File dir = new File(CommitLog.instance.getSegmentManager(SegmentManagerType.STANDARD).storageDirectory);
        File[] files = dir.listFiles();
        return files[0];
    }

    private CFMetaData testCFM()
    {
        return Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE).metadata;
    }

    private class TestCLRHandler implements ICommitLogReadHandler
    {
        public List<Mutation> seenMutations = new ArrayList<Mutation>();
        public boolean sawStopOnErrorCheck = false;

        private final CFMetaData cfm;

        // Accept all
        public TestCLRHandler()
        {
            this.cfm = null;
        }

        public TestCLRHandler(CFMetaData cfm)
        {
            this.cfm = cfm;
        }

        public void prepReader(CommitLogDescriptor desc, RandomAccessReader reader)
        { }

        public boolean logAndCheckIfShouldSkip(File file, CommitLogDescriptor desc)
        {
            return false;
        }

        public boolean shouldSkipSegment(long id, int position)
        {
            return false;
        }

        public boolean shouldStopOnError(CommitLogReadException exception) throws IOException
        {
            sawStopOnErrorCheck = true;
            return false;
        }

        public void handleMutation(Mutation m, int size, long entryLocation, CommitLogDescriptor desc)
        {
            if ((cfm == null) || (cfm != null && m.get(cfm) != null))
                seenMutations.add(m);
        }
    }

    @Test
    public void testReadAll() throws Throwable
    {
        int samples = 1000;
        populateData(samples);
        File toCheck = getFirstCommitLogFile();

        /*
        CommitLogReader reader = new CommitLogReader();

        CFMetaData cfm = testCFM();
        TestCLRHandler testHandler = new TestCLRHandler(cfm);
        reader.readCommitLogSegment(testHandler, toCheck, false);

        ColumnDefinition cd = cfm.getColumnDefinition(new ColumnIdentifier("data", false));

        assert testHandler.seenMutations.size() == 1000 : "Expected 1000 seen mutations, got: " + testHandler.seenMutations.size();

        // Confirm that we got back in expected order
        for (int i = 0; i < samples; i++)
        {
            PartitionUpdate pu = testHandler.seenMutations.get(i).get(cfm);
            for (Row r : pu)
                assertEquals(ByteBuffer.wrap(Integer.toString(i).getBytes()), r.getCell(cd).value());
        }
        */
    }

    /*
    @Test
    public void testReadCount() throws Throwable
    {
        int samples = 50;
        int readCount = 10;
        populateData(samples);
        File toCheck = getFirstCommitLogFile();

        CommitLogReader reader = new CommitLogReader();
        CFMetaData cfm = testCFM();
        TestCLRHandler testHandler = new TestCLRHandler();

        reader.readCommitLogSegment(testHandler, toCheck, readCount, false);
        assert testHandler.seenMutations.size() == readCount : "Expected " + readCount + " seen mutations, got: " + testHandler.seenMutations.size();
    }

    @Test
    public void testReadFromMidpoint() throws Throwable
    {
        int samples = 1000;
        int readCount = 500;
        CommitLogSegmentPosition midpoint = populateData(samples);
        File toCheck = getFirstCommitLogFile();

        CommitLogReader reader = new CommitLogReader();
        CFMetaData cfm = testCFM();
        TestCLRHandler testHandler = new TestCLRHandler();

        reader.readCommitLogSegment(testHandler, toCheck, midpoint, readCount, false);

        // Confirm correct count on replay
        assert testHandler.seenMutations.size() == readCount : "Expected " + readCount + " seen mutations, got: " + testHandler.seenMutations.size();

        ColumnDefinition cd = cfm.getColumnDefinition(new ColumnIdentifier("data", false));
        // Confirm correct ordering on replay, should be 500-1000 on data
        for (int i = 0; i < readCount; i++)
        {
            PartitionUpdate pu = testHandler.seenMutations.get(i).get(cfm);
            for (Row r : pu)
                assertEquals(ByteBuffer.wrap(Integer.toString(i + samples / 2).getBytes()), r.getCell(cd).value());
        }
    }

    @Test
    public void testReadFromMidpointTooMany() throws Throwable
    {
        int samples = 1000;
        int readCount = 5000;
        CommitLogSegmentPosition midpoint = populateData(samples);
        File toCheck = getFirstCommitLogFile();

        CommitLogReader reader = new CommitLogReader();
        CFMetaData cfm = testCFM();
        TestCLRHandler testHandler = new TestCLRHandler(cfm);

        // Reading from mid to overflow by 4.5k
        reader.readCommitLogSegment(testHandler, toCheck, midpoint, readCount, false);

        assert testHandler.seenMutations.size() == samples / 2 : "Expected " + samples / 2 + " seen mutations, got: " + testHandler.seenMutations.size();

        ColumnDefinition cd = cfm.getColumnDefinition(new ColumnIdentifier("data", false));
        // Confirm correct ordering on replay, should be 500-1000 on data
        for (int i = 0; i < samples / 2; i++)
        {
            PartitionUpdate pu = testHandler.seenMutations.get(i).get(cfm);
            for (Row r : pu)
                assertEquals(ByteBuffer.wrap(Integer.toString(i + samples / 2).getBytes()), r.getCell(cd).value());
        }
    }

    @Test
    public void testReadCountFromMidpoint() throws Throwable
    {
        int samples = 1000;
        int readCount = 10;
        CommitLogSegmentPosition midpoint = populateData(samples);
        File toCheck = getFirstCommitLogFile();

        CommitLogReader reader = new CommitLogReader();
        CFMetaData cfm = testCFM();
        TestCLRHandler testHandler = new TestCLRHandler();

        reader.readCommitLogSegment(testHandler, toCheck, midpoint, readCount, false);

        assert testHandler.seenMutations.size() == readCount;

        ColumnDefinition cd = cfm.getColumnDefinition(new ColumnIdentifier("data", false));

        // Confirm correct count on replay
        assert testHandler.seenMutations.size() == readCount : "Expected " + readCount + " seen mutations, got: " + testHandler.seenMutations.size();

        // Confirm correct ordering on replay, should be 500-510 on data
        for (int i = 0; i < readCount; i++)
        {
            PartitionUpdate pu = testHandler.seenMutations.get(i).get(cfm);
            for (Row r : pu)
                assertEquals(ByteBuffer.wrap(Integer.toString(i + samples / 2).getBytes()), r.getCell(cd).value());
        }
    }
    */
}
