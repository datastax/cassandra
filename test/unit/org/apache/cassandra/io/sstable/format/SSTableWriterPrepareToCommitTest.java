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

package org.apache.cassandra.io.sstable.format;

import java.nio.ByteBuffer;

import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.UpdateBuilder;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.SSTableWriterTestBase;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.util.File;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

@RunWith(BMUnitRunner.class)
public class SSTableWriterPrepareToCommitTest extends SSTableWriterTestBase
{
    public static volatile boolean shouldFail;

    @Test
    @BMRule(name = "Fail SSTableWriter prepare",
            targetClass = "org.apache.cassandra.io.sstable.format.SSTableWriter$TransactionalProxy",
            targetMethod = "doPrepare",
            targetLocation = "ENTRY",
            condition = "org.apache.cassandra.io.sstable.format.SSTableWriterPrepareToCommitTest.shouldFail",
            action = "throw new RuntimeException(\"prepare failed\")")
    public void testPrepareToCommitTracksNewWrittenWhenTxnPrepareThrows()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);
        truncate(cfs);

        File dir = cfs.getDirectories().getDirectoryForNewSSTables();
        TestLifecycleNewTracker tracker = new TestLifecycleNewTracker();
        RuntimeException failure;

        try (SSTableWriter writer = newWriter(cfs, dir, tracker))
        {
            UpdateBuilder row = UpdateBuilder.create(cfs.metadata(), "k").withTimestamp(1);
            row.newRow("c").add("val", ByteBuffer.wrap(new byte[] { 1 }));
            writer.append(row.build().unfilteredIterator());

            shouldFail = true;
            try
            {
                failure = assertThrows(RuntimeException.class, writer::prepareToCommit);
            }
            finally
            {
                shouldFail = false;
            }
            assertTrue(tracker.trackNewCalled);
            assertFalse(tracker.trackNewWrittenCalled);
            assertEquals(null, tracker.trackedWritten);
        }
        assertNotEquals(null, failure);
        assertEquals("prepare failed", failure.getMessage());
    }

    private static SSTableWriter newWriter(ColumnFamilyStore cfs, File directory, LifecycleNewTracker tracker)
    {
        Descriptor desc = cfs.newSSTableDescriptor(directory);
        return desc.getFormat().getWriterFactory().builder(desc)
                   .setTableMetadataRef(cfs.metadata)
                   .setKeyCount(0)
                   .setRepairedAt(0)
                   .setPendingRepair(null)
                   .setTransientSSTable(false)
                   .setSerializationHeader(new SerializationHeader(true,
                                                                   cfs.metadata(),
                                                                   cfs.metadata().regularAndStaticColumns(),
                                                                   EncodingStats.NO_STATS))
                   .setSecondaryIndexGroups(cfs.indexManager.listIndexGroups())
                   .setMetadataCollector(new MetadataCollector(cfs.metadata().comparator))
                   .addDefaultComponents(cfs.indexManager.listIndexGroups())
                   .build(tracker, cfs);
    }

    private static final class TestLifecycleNewTracker implements LifecycleNewTracker
    {
        private boolean trackNewCalled;
        private boolean trackNewWrittenCalled;
        private SSTable trackedWritten;

        @Override
        public void trackNew(SSTable table)
        {
            trackNewCalled = true;
        }

        @Override
        public void trackNewWritten(SSTable table)
        {
            trackNewWrittenCalled = true;
            trackedWritten = table;
        }

        @Override
        public void untrackNew(SSTable table)
        {
        }

        @Override
        public OperationType opType()
        {
            return OperationType.WRITE;
        }
    }
}
