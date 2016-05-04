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
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.io.util.FileUtils;

import static org.junit.Assert.assertEquals;

public class CommitLogSegmentManagerCDCTest extends CQLTester
{
    private static Random random = new Random();

    @BeforeClass
    public static void beforeClass()
    {
        CommitLog.instance.switchToCDCSegmentManager();
    }

    /**
     * Returns offset of active written data at halfway point of data
     */
    private CommitLogSegmentPosition populateData(int entryCount) throws Throwable
    {
        Assert.assertEquals("entryCount must be an even number.", 0, entryCount % 2);
        int midpoint = entryCount / 2;

        for (int i = 0; i < midpoint; i++) {
            execute("INSERT INTO %s (idx, data) VALUES (?, ?)", i, Integer.toString(i));
        }

        CommitLogSegmentPosition result = CommitLog.instance.getCurrentSegmentPosition();

        for (int i = midpoint; i < entryCount; i++)
            execute("INSERT INTO %s (idx, data) VALUES (?, ?)", i, Integer.toString(i));

        Keyspace.open(keyspace()).getColumnFamilyStore(currentTable()).forceBlockingFlush();
        return result;
    }

    @Test
    public void testCDCFunctionality() throws Throwable
    {
        CommitLog.instance.resetUnsafe(true);

        createTable("CREATE TABLE %s (idx int, data text, primary key(idx)) WITH cdc=true;");
        CommitLogSegmentManagerCDC cdcMgr = (CommitLogSegmentManagerCDC)CommitLog.instance.segmentManager;
        long startSize = cdcMgr.updateCDCOverflowSize();

        int samples = 1000;

        // Confirm CommitLogSegmentPosition increments on mutation application
        CommitLogSegmentPosition start = cdcMgr.getCurrentSegmentPosition();
        populateData(samples);
        CommitLogSegmentPosition end = cdcMgr.getCurrentSegmentPosition();
        Assert.assertTrue("CommitLogSegmentPosition for CDC Segment Manager did not increment. start: " + start + " end: " + end,
                          start.position < end.position);

        CFMetaData cfm = currentTableMetadata();
        CommitLogReaderTest.TestCLRHandler testHandler = new CommitLogReaderTest.TestCLRHandler(cfm);

        CommitLogReader reader = new CommitLogReader();
        ArrayList<File> toCheck = CommitLogReaderTest.getCommitLogs();
        for (File f : toCheck)
            reader.readCommitLogSegment(testHandler, f, CommitLogReader.ALL_MUTATIONS, false);

        ColumnDefinition cd = cfm.getColumnDefinition(new ColumnIdentifier("data", false));

        // Confirm the correct # of mutations were written to and read from the CDC log.
        Assert.assertEquals("Expected 1000 seen mutations, got: " + testHandler.seenMutations.size(),
            1000, testHandler.seenMutationCount());

        // Confirm that we got back in expected order
        for (int i = 0; i < samples; i++)
        {
            PartitionUpdate pu = testHandler.seenMutations.get(i).get(cfm);
            for (Row r : pu)
                assertEquals(ByteBuffer.wrap(Integer.toString(i).getBytes()), r.getCell(cd).value());
        }

        Assert.assertEquals("Expected no change in overflow folder size.", startSize, cdcMgr.updateCDCOverflowSize());

        // Recycle, confirm files are moved to cdc overflow.
        int fileCount = new File(DatabaseDescriptor.getCDCLogLocation()).listFiles().length;
        Assert.assertEquals("Expected there to be no files in cdc_raw but found: " + fileCount, 0, fileCount);
        CommitLog.instance.forceRecycleAllSegments();
        Assert.assertTrue("Expected change in overflow size on forced segment recycle", cdcMgr.updateCDCOverflowSize() != startSize);
        fileCount = new File(DatabaseDescriptor.getCDCLogLocation()).listFiles().length;
        Assert.assertEquals("Expected to have 1 segment in cdc_raw. Got " + fileCount, 1, fileCount);
    }

    @Test
    public void testCDCWriteTimeout() throws Throwable
    {
        CommitLog.instance.resetUnsafe(true);

        createTable("CREATE TABLE %s (idx int, data text, primary key(idx)) WITH cdc=true;");
        CommitLogSegmentManagerCDC cdcMgr = (CommitLogSegmentManagerCDC)CommitLog.instance.segmentManager;
        CFMetaData cfm = currentTableMetadata();

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
            Assert.assertTrue("Expected to be able to allocate new CDC segments but apparently can't.", !cdcMgr.atCapacity());
            DatabaseDescriptor.setCommitLogSpaceInMBCDC(0);
            Assert.assertTrue("Expected inability to allocate new CLSegments within CDC after changing apace max value to 1",
                              cdcMgr.atCapacity());

            DatabaseDescriptor.setCommitLogSpaceInMBCDC(16);
            // Spin until we hit CDC capacity and make sure we get a WriteTimeout
            try
            {
                // Should trigger on anything < 20:1 compression ratio during compressed test
                for (int i = 0; i < 1000; i++)
                {
                    new RowUpdateBuilder(cfm, 0, i)
                        .add("data", randomizeBuffer(DatabaseDescriptor.getCommitLogSegmentSize() / 3))
                        .build().apply();
                }
                Assert.fail("Expected WriteTimeoutException from full CDC but did not receive it.");
            }
            catch (WriteTimeoutException e)
            {
                // expected, do nothing
            }

            // Confirm we can create a non-cdc table and write to it even while at cdc capacity
            createTable("CREATE TABLE %s (idx int, data text, primary key(idx)) WITH cdc=false;");
            execute("INSERT INTO %s (idx, data) VALUES (1, '1');");

            Keyspace.open(keyspace()).getColumnFamilyStore(currentTable()).forceBlockingFlush();
            CommitLog.instance.forceRecycleAllSegments();
            Assert.assertTrue("Expected files to be moved to overflow.", getCDCOverflowCount() > 0);

            // Force an update - this would normally be handled by failed allocate() calls within CommitLogSegmentManagerCDC,
            // but I'm using a test hook here to isolate it to a sequential operation.
            long newSize;
            while ((newSize = cdcMgr.updateCDCOverflowSize()) > 0)
            {
                // Simulate a CDC consumer reading files then deleting them
                for (File f : new File(DatabaseDescriptor.getCDCLogLocation()).listFiles())
                {
                    FileUtils.deleteWithConfirm(f);
                }
            }
            Assert.assertEquals("Expected empty overflow, instead found: " + newSize + " bytes on disk.", 0, newSize);

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

    @Test
    public void testCLSMCDCDiscardLogic() throws Throwable
    {
        CommitLog.instance.resetUnsafe(true);

        createTable("CREATE TABLE %s (idx int, data text, primary key(idx)) WITH cdc=false;");
        for (int i = 0; i < 8; i++)
        {
            new RowUpdateBuilder(currentTableMetadata(), 0, i)
                .add("data", randomizeBuffer(DatabaseDescriptor.getCommitLogSegmentSize() / 3))
                .build().apply();
        }
        CommitLog.instance.forceRecycleAllSegments();
        Assert.assertEquals(0, new File(DatabaseDescriptor.getCDCLogLocation()).listFiles().length);

        createTable("CREATE TABLE %s (idx int, data text, primary key(idx)) WITH cdc=true;");
        for (int i = 0; i < 8; i++)
        {
            new RowUpdateBuilder(currentTableMetadata(), 0, i)
                .add("data", randomizeBuffer(DatabaseDescriptor.getCommitLogSegmentSize() / 3))
                .build().apply();
        }
        CommitLog.instance.forceRecycleAllSegments();
        Assert.assertTrue(new File(DatabaseDescriptor.getCDCLogLocation()).listFiles().length > 0);
    }

    private ByteBuffer randomizeBuffer(int size)
    {
        byte[] toWrap = new byte[size];
        random.nextBytes(toWrap);
        return ByteBuffer.wrap(toWrap);
    }

    private int getCDCOverflowCount()
    {
        return new File(DatabaseDescriptor.getCDCLogLocation()).listFiles().length;
    }
}
