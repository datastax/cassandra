/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.db;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.SSTableRewriter;
import org.apache.cassandra.schema.KeyspaceParams;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;

import static org.apache.cassandra.SchemaLoader.counterCFMD;
import static org.apache.cassandra.SchemaLoader.createKeyspace;
import static org.apache.cassandra.SchemaLoader.getCompressionParameters;
import static org.apache.cassandra.SchemaLoader.loadSchema;
import static org.apache.cassandra.SchemaLoader.standardCFMD;
import static org.apache.cassandra.db.ScrubTest.CF;
import static org.apache.cassandra.db.ScrubTest.CF_INDEX1;
import static org.apache.cassandra.db.ScrubTest.CF_INDEX1_BYTEORDERED;
import static org.apache.cassandra.db.ScrubTest.CF_INDEX2;
import static org.apache.cassandra.db.ScrubTest.CF_INDEX2_BYTEORDERED;
import static org.apache.cassandra.db.ScrubTest.CF_UUID;
import static org.apache.cassandra.db.ScrubTest.COMPRESSION_CHUNK_LENGTH;
import static org.apache.cassandra.db.ScrubTest.COUNTER_CF;
import static org.junit.Assert.assertTrue;

@RunWith(BMUnitRunner.class)
public class EarlyOpenWithBytemanTest
{
    public static AtomicInteger moveStartsCounter = new AtomicInteger(0);
    public static AtomicInteger failedEarlyOpenCounter = new AtomicInteger(0);
    public static AtomicBoolean canTriggerException = new AtomicBoolean(false);

    static String KEYSPACE = "early_open_byteman_test";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        loadSchema();
        createKeyspace(KEYSPACE,
                KeyspaceParams.simple(1),
                standardCFMD(KEYSPACE, CF),
                counterCFMD(KEYSPACE, COUNTER_CF).compression(getCompressionParameters(COMPRESSION_CHUNK_LENGTH)),
                standardCFMD(KEYSPACE, CF_UUID, 0, UUIDType.instance),
                SchemaLoader.keysIndexCFMD(KEYSPACE, CF_INDEX1, true),
                SchemaLoader.compositeIndexCFMD(KEYSPACE, CF_INDEX2, true),
                SchemaLoader.keysIndexCFMD(KEYSPACE, CF_INDEX1_BYTEORDERED, true).partitioner(ByteOrderedPartitioner.instance),
                SchemaLoader.compositeIndexCFMD(KEYSPACE, CF_INDEX2_BYTEORDERED, true).partitioner(ByteOrderedPartitioner.instance));
    }


    // DSP-25176
    // Abort early-open attempts by blocking every second attempt to move an sstable's start (making sure some can
    // succeed to test if created sstable references leak).
    // Please check the output for "LEAK DETECTED" lines.
    @Test
    @BMRule(name="simulate_corruption",
            targetClass = "org.apache.cassandra.io.sstable.SSTableRewriter",
            targetMethod = "moveStarts",
            targetLocation = "AT INVOKE org.apache.cassandra.io.sstable.format.SSTableReader.firstKeyBeyond",
            condition = "org.apache.cassandra.db.EarlyOpenWithBytemanTest.canTriggerException.get() && " +
                        "(org.apache.cassandra.db.EarlyOpenWithBytemanTest.moveStartsCounter.incrementAndGet() % 2 == 0)",
            action = "org.apache.cassandra.db.EarlyOpenWithBytemanTest.failedEarlyOpenCounter.incrementAndGet();" +
                     "throw new java.lang.RuntimeException(\"!!!Test simulated corruption!!!\");")
    public void testExceptionsOnLifecycleBoundaries() throws Exception
    {
        boolean oldDisabledVal = SSTableRewriter.disableEarlyOpeningForTests;
        int oldInterval = DatabaseDescriptor.getSSTablePreemptiveOpenIntervalInMB();
        int earlyOpenThresholdMB = 1;
        int cfsRowsNum = 100_000;
        try
        {
            // Ensure early opening is enabled
            SSTableRewriter.disableEarlyOpeningForTests = false;
            // Set a very low interval (e.g., 1MB) to trigger early opening quickly
            DatabaseDescriptor.setSSTablePreemptiveOpenIntervalInMB(earlyOpenThresholdMB);

            CompactionManager.instance.disableAutoCompaction();
            Keyspace keyspace = Keyspace.open(KEYSPACE);
            ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);
            cfs.clearUnsafe();

            // Insert enough data to trigger early opening
            ScrubTest.fillCF(cfs, cfsRowsNum);
            // Do it again to have multiple source sstables
            ScrubTest.fillCF(cfs, cfsRowsNum);
            ScrubTest.assertOrderedAll(cfs, cfsRowsNum);


            // This should trigger:
            // 1. maybeReopenEarly() during normal writing
            // 2. Corruption simulation
            // 3. switchWriter() or other operation will try to update the transaction
            failedEarlyOpenCounter.set(0);
            canTriggerException.set(true);
            cfs.forceMajorCompaction();
            canTriggerException.set(false);
            assertTrue("Corruption was not triggered " + failedEarlyOpenCounter.get(), failedEarlyOpenCounter.get() > 0);

            // check data is still there
            ScrubTest.assertOrderedAll(cfs, cfsRowsNum);
        }
        finally
        {
            SSTableRewriter.disableEarlyOpeningForTests = oldDisabledVal;
            DatabaseDescriptor.setSSTablePreemptiveOpenIntervalInMB(oldInterval);
        }
    }
}
