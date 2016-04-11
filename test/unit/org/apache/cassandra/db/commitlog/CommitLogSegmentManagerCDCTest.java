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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Random;

import org.junit.*;

import org.apache.cassandra.config.*;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.commitlog.AbstractCommitLogSegmentManager.SegmentManagerType;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.io.util.FileUtils;

import static org.junit.Assert.assertEquals;

public class CommitLogSegmentManagerCDCTest extends CQLTester
{
    private static boolean _initialized = false;
    private static String KEYSPACE = "clr_test";
    private static String TABLE = "clr_test_table";
    private static Random random = new Random();

    /**
     * Intentionally not using static @BeforeClass since we need access to CQLTester internal methods here and we want
     * the convenience of access to its guts for the following tests.
     */
    @Before
    public void before() throws Throwable
    {
        // Swallow since we don't care about failure to drop CDCLOG on test prep
        try { execute(String.format("ALTER KEYSPACE %s DROP CDCLOG;", KEYSPACE)); }
        catch (Exception e) {}

        execute(String.format("DROP KEYSPACE IF EXISTS %s;", KEYSPACE));
        execute(String.format("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};", KEYSPACE));
        execute(String.format("ALTER KEYSPACE %s WITH cdc_datacenters = {'test1'};", KEYSPACE));
        execute(String.format("USE %s;", KEYSPACE));
        execute(String.format("CREATE TABLE %s (idx INT, data TEXT, PRIMARY KEY(idx));", TABLE));
    }

    /**
     * Returns offset of active written data at halfway point of data
     */
    private CommitLogSegmentPosition populateData(int entryCount) throws Throwable
    {
        assert entryCount % 2 == 0 : "entryCount must be an even number.";
        int midpoint = entryCount / 2;

        for (int i = 0; i < midpoint; i++) {
            execute(String.format("INSERT INTO %s (idx, data) VALUES (?, ?)", TABLE), i, Integer.toString(i));
        }

        CommitLogSegmentPosition result = CommitLog.instance.getCurrentSegmentPosition(SegmentManagerType.STANDARD);

        for (int i = midpoint; i < entryCount; i++)
            execute(String.format("INSERT INTO %s (idx, data) VALUES (?, ?)", TABLE), i, Integer.toString(i));

        Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE).forceBlockingFlush();
        return result;
    }

    private CFMetaData testCFM()
    {
        return Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE).metadata;
    }

    @Test
    public void testCDCFunctionality() throws Throwable
    {
        CommitLog.instance.resetUnsafe(true);

        CommitLogSegmentManagerCDC cdcMgr = (CommitLogSegmentManagerCDC)CommitLog.instance.getSegmentManager(SegmentManagerType.CDC);
        long startSize = cdcMgr.updateCDCOverflowSize();

        int samples = 1000;

        // Confirm CommitLogSegmentPosition increments on mutation application
        CommitLogSegmentPosition start = cdcMgr.getCurrentSegmentPosition();
        populateData(samples);
        CommitLogSegmentPosition end = cdcMgr.getCurrentSegmentPosition();
        assert start.position < end.position : "CommitLogSegmentPosition for CDC Segment Manager did not increment. start: " + start + " end: " + end;

        // Confirm directory structure appropriately placing segments in a folder w/cdc in the name.
        // Note: this could break if people change this in the test .yaml. So don't.
        ArrayList<File> toCheck = CommitLogReaderTest.getCommitLogs(SegmentManagerType.CDC);
        for (File f : toCheck)
            if (!f.toString().contains("cdc"))
                throw new AssertionError("Expected name of allocated CommitLogSegment to contain cdc.");

        CFMetaData cfm = testCFM();
        CommitLogReaderTest.TestCLRHandler testHandler = new CommitLogReaderTest.TestCLRHandler(cfm);

        CommitLogReader reader = new CommitLogReader();
        for (File f : toCheck)
            reader.readCommitLogSegment(testHandler, f, false);

        ColumnDefinition cd = cfm.getColumnDefinition(new ColumnIdentifier("data", false));

        // Confirm the correct # of mutations were written to and read from the CDC log.
        assert testHandler.seenMutations.size() == 1000 : "Expected 1000 seen mutations, got: " + testHandler.seenMutations.size();

        // Confirm that we got back in expected order
        for (int i = 0; i < samples; i++)
        {
            PartitionUpdate pu = testHandler.seenMutations.get(i).get(cfm);
            for (Row r : pu)
                assertEquals(ByteBuffer.wrap(Integer.toString(i).getBytes()), r.getCell(cd).value());
        }

        assert cdcMgr.updateCDCOverflowSize() == startSize : "Expected no change in overflow folder size.";

        // Recycle, confirm files are moved to cdc overflow.
        int fileCount = new File(DatabaseDescriptor.getCDCOverflowLocation()).listFiles().length;
        assert fileCount == 0 : "Expected there to be no files in cdc_overflow but found: " + fileCount;
        CommitLog.instance.forceRecycleAllSegments();
        assert cdcMgr.updateCDCOverflowSize() != startSize : "Expected change in overflow size on forced segment recycle";
        fileCount = new File(DatabaseDescriptor.getCDCOverflowLocation()).listFiles().length;
        assert fileCount == 1 : "Expected to have 1 segment in cdc_overflow. Got " + fileCount;
    }

    @Test
    public void testCDCWriteTimeout() throws Throwable
    {
        CommitLog.instance.resetUnsafe(true);
        CommitLogSegmentManagerCDC cdcMgr = (CommitLogSegmentManagerCDC)CommitLog.instance.getSegmentManager(SegmentManagerType.CDC);
        CFMetaData cfm = testCFM();

        // Confirm that logic to check for whether or not we can allocate new CDC segments works
        Integer originalCDCSize = DatabaseDescriptor.getCommitLogSpaceInMBCDC();
        Integer originalCheckInterval = DatabaseDescriptor.getCDCDiskCheckInterval();
        try
        {
            DatabaseDescriptor.setCDCDiskCheckInterval(0);

            // Populate some initial data for atCapacity confirmation
            for (int i = 0; i < 100; i++)
            {
                new RowUpdateBuilder(cfm, 0, i)
                    .add("data", randomizeBuffer(DatabaseDescriptor.getCommitLogSegmentSize() / 3))
                    .build().apply();
            }

            // Confirm atCapacity is performing as expected
            assert !cdcMgr.atCapacity() : "Expected to be able to allocate new CDC segments but apparently can't.";
            DatabaseDescriptor.setCommitLogSpaceInMBCDC(0);
            assert cdcMgr.atCapacity() : "Expected inability to allocate new CLSegments within CDC after changing apace max value to 1";

            DatabaseDescriptor.setCommitLogSpaceInMBCDC(16);
            // Spin until we hit CDC capacity and make sure we get a WriteTimeout
            boolean pass = false;
            try
            {
                // Should trigger on anything < 20:1 compression ratio during compressed test
                for (int i = 0; i < 1000; i++)
                {
                    new RowUpdateBuilder(cfm, 0, i)
                        .add("data", randomizeBuffer(DatabaseDescriptor.getCommitLogSegmentSize() / 3))
                        .build().apply();
                }
            }
            catch (WriteTimeoutException e)
            {
                // expected, do nothing
                pass = true;
            }
            assert pass : "Expected WriteTimeoutException from full CDC but did not receive it.";

            Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE).forceBlockingFlush();
            CommitLog.instance.forceRecycleAllSegments();
            assert getCDCOverflowCount() > 0 : "Expected files to be moved to overflow.";

            // Force an update - this would normally be handled by failed allocate() calls within CommitLogSegmentManagerCDC,
            // but I'm using a test hook here to isolate it to a sequential operation.
            long newSize;
            while ((newSize = cdcMgr.updateCDCOverflowSize()) > 0)
            {
                // Simulate a CDC consumer reading files then deleting them
                for (File f : new File(DatabaseDescriptor.getCDCOverflowLocation()).listFiles())
                    FileUtils.deleteWithConfirm(f);
            }
            assert newSize == 0 : "Expected empty overflow, instead found: " + newSize + " bytes on disk.";

            cdcMgr.atCapacity();
            // After updating the size, we should now again be able to allocate mutations within CDC
            new RowUpdateBuilder(cfm, 0, 1024)
                .add("data", randomizeBuffer(DatabaseDescriptor.getCommitLogSegmentSize() / 3))
                .build().apply();
        }
        finally
        {
            DatabaseDescriptor.setCommitLogSpaceInMBCDC(originalCDCSize);
            DatabaseDescriptor.setCDCDiskCheckInterval(originalCheckInterval);
        }
    }

    private ByteBuffer randomizeBuffer(int size)
    {
        byte[] toWrap = new byte[size];
        random.nextBytes(toWrap);
        return ByteBuffer.wrap(toWrap);
    }

    @Test
    public void testDCRestriction() throws Throwable
    {
        execute(String.format("ALTER KEYSPACE %s WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 1, 'dc2': 1}", KEYSPACE));
        execute(String.format("ALTER KEYSPACE %s WITH cdc_datacenters = {'dc1'};", KEYSPACE));
        execute(String.format("USE %s;", KEYSPACE));

        CommitLog.instance.resetUnsafe(true);

        CommitLogSegmentManagerCDC cdcMgr = (CommitLogSegmentManagerCDC)CommitLog.instance.getSegmentManager(SegmentManagerType.CDC);

        int samples = 50;

        // Confirm CommitLogSegmentPosition increments on mutation application
        CommitLogSegmentPosition cdcStart = cdcMgr.getCurrentSegmentPosition();

        String originalDC = DatabaseDescriptor.getLocalDataCenter();
        try
        {
            // First, set to dc1 and make sure CDC picks it up. Have to drop and recreated cdc log, since the has local check
            // occurs at time of keyspace alter and we need our DC to match at that time. Only relevant for tests as we won't
            // be changing DC names on the fly in production.
            execute(String.format("ALTER KEYSPACE %s DROP CDCLOG;", KEYSPACE));
            DatabaseDescriptor.setLocalDataCenter("dc1");
            execute(String.format("ALTER KEYSPACE %s WITH cdc_datacenters = {'dc1'};", KEYSPACE));
            populateData(samples);
            CommitLogSegmentPosition cdcEnd = cdcMgr.getCurrentSegmentPosition();

            // Can't really check standard mgr for position change since other things (system tables) can be written to it during test
            // We can reliably check the CDC segment position, however, since no system tables have CDC enabled.
            Assert.assertTrue(cdcEnd.position > cdcStart.position);

            // Change and confirm future mutations don't go CDC
            execute(String.format("ALTER KEYSPACE %s DROP CDCLOG;", KEYSPACE));
            DatabaseDescriptor.setLocalDataCenter("dc2");
            execute(String.format("ALTER KEYSPACE %s WITH cdc_datacenters = {'dc1'};", KEYSPACE));
            cdcStart = cdcMgr.getCurrentSegmentPosition();
            populateData(samples);
            cdcEnd = cdcMgr.getCurrentSegmentPosition();
            Assert.assertTrue(cdcEnd.position == cdcStart.position);
        }
        finally
        {
            DatabaseDescriptor.setLocalDataCenter(originalDC);
        }
    }

    private int getCDCOverflowCount()
    {
        return new File(DatabaseDescriptor.getCDCOverflowLocation()).listFiles().length;
    }
}
