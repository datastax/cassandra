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
package org.apache.cassandra.db.partition;

import com.google.common.collect.ImmutableMap;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.UpdateBuilder;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.partitions.BTreePartitionUpdate;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.TriePartitionUpdate;
import org.apache.cassandra.db.partitions.TriePartitionUpdateStage2;
import org.apache.cassandra.db.partitions.TriePartitionUpdateStage3;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.DeserializationHelper;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIteratorSerializer;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

public class PartitionUpdateTest extends CQLTester
{
    @Test
    public void testOperationCount()
    {
        createTable("CREATE TABLE %s (key text, clustering int, a int, s int static, PRIMARY KEY(key, clustering))");
        TableMetadata cfm = currentTableMetadata();

        UpdateBuilder builder = UpdateBuilder.create(cfm, "key0");
        Assert.assertEquals(0, builder.build().operationCount());
        Assert.assertEquals(1, builder.newRow(1).add("a", 1).build().operationCount());

        builder = UpdateBuilder.create(cfm, "key0");
        Assert.assertEquals(1, builder.newRow().add("s", 1).build().operationCount());

        builder = UpdateBuilder.create(cfm, "key0");
        builder.newRow().add("s", 1);
        builder.newRow(1).add("a", 1);
        Assert.assertEquals(2, builder.build().operationCount());
    }

    @Test
    public void testMutationSize()
    {
        createTable("CREATE TABLE %s (key text, clustering int, a int, s int static, PRIMARY KEY(key, clustering))");
        TableMetadata cfm = currentTableMetadata();

        UpdateBuilder builder = UpdateBuilder.create(cfm, "key0");
        builder.newRow().add("s", 1);
        builder.newRow(1).add("a", 2);
        PartitionUpdate update1 = builder.build();

        int size1 = update1.dataSize();
        Assert.assertEquals(rowDataSizeSum(update1), size1);
        // The table's memtable factory builds trie updates when the storage version has the trie encoding, and
        // trie-backed rows do not count the clustering, liveness and deletion sizes (see TrieBackedRow.dataSize).
        Assert.assertEquals(update1 instanceof TriePartitionUpdate ? 64 : 94, size1);

        // BTree rows count the key, liveness and deletion too.
        PartitionUpdate btreeUpdate1 = BTreePartitionUpdate.asBTreeUpdate(update1);
        Assert.assertEquals(rowDataSizeSum(btreeUpdate1), btreeUpdate1.dataSize());
        Assert.assertEquals(94, btreeUpdate1.dataSize());
        Assert.assertEquals(btreeUpdate1.dataSize(), TriePartitionUpdate.asTrieUpdate(btreeUpdate1).dataSize());

        builder = UpdateBuilder.create(cfm, "key0");
        builder.newRow(1).add("a", 2);
        int size2 = builder.build().dataSize();
        Assert.assertTrue(size1 != size2);

        builder = UpdateBuilder.create(cfm, "key0");
        int size3 = builder.build().dataSize();
        Assert.assertTrue(size2 != size3);
    }

    private static int rowDataSizeSum(PartitionUpdate update)
    {
        int sum = update.staticRow().dataSize();
        for (Row row : update.rows())
            sum += row.dataSize();
        return sum;
    }

    @Test
    public void testUpdateAllTimestamp()
    {
        createTable("CREATE TABLE %s (key text, clustering int, a int, b int, c int, s int static, PRIMARY KEY(key, clustering))");
        TableMetadata cfm = currentTableMetadata();

        long timestamp = FBUtilities.timestampMicros();
        RowUpdateBuilder rub = new RowUpdateBuilder(cfm, timestamp, "key0").clustering(1).add("a", 1);
        PartitionUpdate pu = rub.buildUpdate();
        PartitionUpdate pu2 = pu.withUpdatedTimestamps(0);

        Assert.assertTrue(pu.maxTimestamp() > 0);
        Assert.assertTrue(pu2.maxTimestamp() == 0);
    }

    /**
     * Paxos rewrites an update's timestamps to the ballot time, which can be below the ones it was written with. The
     * memtable takes its minimum timestamp from stats(), so the rewritten update's stats must still bound its content.
     */
    @Test
    public void testUpdatedTimestampsStatsBoundTheContent()
    {
        createTable("CREATE TABLE %s (key text, clustering int, a int, b int, m map<int, int>, s int static, PRIMARY KEY(key, clustering))");
        TableMetadata cfm = currentTableMetadata();

        long timestamp = FBUtilities.timestampMicros();
        PartitionUpdate.SimpleBuilder builder = PartitionUpdate.simpleBuilder(cfm, "key0");
        // Below the rest, so that it does not shadow them.
        builder.timestamp(timestamp - 10).delete();
        builder.timestamp(timestamp);
        builder.row().add("s", 1);
        builder.row(1).add("a", 1).delete("b").add("m", ImmutableMap.of(1, 1));
        builder.row(2).delete();
        builder.addRangeTombstone().start(3).end(5);
        PartitionUpdate update = builder.build();

        long newTimestamp = timestamp - 1000;
        for (PartitionUpdate pu : new PartitionUpdate[]{ BTreePartitionUpdate.asBTreeUpdate(update),
                                                         TriePartitionUpdate.asTrieUpdate(update),
                                                         TriePartitionUpdateStage2.asTrieUpdate(update),
                                                         TriePartitionUpdateStage3.asTrieUpdate(update) })
        {
            PartitionUpdate updated = pu.withUpdatedTimestamps(newTimestamp);
            String name = pu.getClass().getSimpleName();
            // The deletions are rewritten to newTimestamp - 1, below everything the update held before.
            Assert.assertEquals(name, newTimestamp - 1, minTimestamp(updated));
            Assert.assertTrue(name, updated.stats().minTimestamp <= minTimestamp(updated));
        }
    }

    /**
     * The smallest timestamp anywhere in the update: liveness, cells, complex, row, range and partition deletions.
     */
    private static long minTimestamp(PartitionUpdate update)
    {
        try (UnfilteredRowIterator iter = update.unfilteredIterator())
        {
            long min = minTimestamp(Long.MAX_VALUE, iter.partitionLevelDeletion());
            min = minTimestamp(min, iter.staticRow());
            while (iter.hasNext())
            {
                Unfiltered unfiltered = iter.next();
                if (unfiltered.isRow())
                {
                    min = minTimestamp(min, (Row) unfiltered);
                    continue;
                }
                RangeTombstoneMarker marker = (RangeTombstoneMarker) unfiltered;
                if (marker.isOpen(false))
                    min = minTimestamp(min, marker.openDeletionTime(false));
                if (marker.isClose(false))
                    min = minTimestamp(min, marker.closeDeletionTime(false));
            }
            return min;
        }
    }

    private static long minTimestamp(long min, Row row)
    {
        if (!row.primaryKeyLivenessInfo().isEmpty())
            min = Math.min(min, row.primaryKeyLivenessInfo().timestamp());
        min = minTimestamp(min, row.deletion().time());
        for (ColumnData cd : row)
        {
            if (cd.column().isSimple())
            {
                min = Math.min(min, ((Cell<?>) cd).timestamp());
                continue;
            }
            ComplexColumnData complex = (ComplexColumnData) cd;
            min = minTimestamp(min, complex.complexDeletion());
            for (Cell<?> cell : complex)
                min = Math.min(min, cell.timestamp());
        }
        return min;
    }

    private static long minTimestamp(long min, DeletionTime deletion)
    {
        return deletion.isLive() ? min : Math.min(min, deletion.markedForDeleteAt());
    }

    @Test
    public void testTriePartitionUpdateSerialization() throws Exception
    {
        createTable("CREATE TABLE %s (key text, clustering int, a int, s int static, PRIMARY KEY(key, clustering))");
        TableMetadata cfm = currentTableMetadata();

        UpdateBuilder builder = UpdateBuilder.create(cfm, "key0");
        builder.newRow().add("s", 1);
        builder.newRow(1).add("a", 2);
        PartitionUpdate originalUpdate = builder.build();
        PartitionUpdate trieUpdate = TriePartitionUpdate.asTrieUpdate(originalUpdate);

        for (PartitionUpdate update : new PartitionUpdate[]{ originalUpdate, trieUpdate })
        {
            int version = MessagingService.current_version;

            long serializedSize = PartitionUpdate.serializer.serializedSize(update, version);
            DataOutputBuffer out = new DataOutputBuffer();
            PartitionUpdate.serializer.serialize(update, out, version);

            Assert.assertEquals(serializedSize, out.position());

            DataInputBuffer in = new DataInputBuffer(out.buffer(), false);
            PartitionUpdate deserializedUpdate = PartitionUpdate.serializer.deserialize(in, version, DeserializationHelper.Flag.LOCAL);

            Assert.assertEquals(update.partitionKey(), deserializedUpdate.partitionKey());
            Assert.assertEquals(update.rowCount(), deserializedUpdate.rowCount());
            // dataSize is the accounting an update makes of the rows it was handed, and rows that have been through
            // the BTree encoding account for themselves differently from the ones a trie builder produced, at
            // VERSION_DS_20 as much as here. What a round trip owes the caller is the content, so assert that.
            Assert.assertEquals(TriePartitionUpdate.asTrieUpdate(update), TriePartitionUpdate.asTrieUpdate(deserializedUpdate));
            Assert.assertEquals(update.operationCount(), deserializedUpdate.operationCount());
        }
    }

    /**
     * VERSION_DSE_68 is numerically above VERSION_DS_21, but its partition updates carry no format byte, whichever
     * class the update is.
     */
    @Test
    public void testSerializedSizeAtDse68HasNoFormatByte()
    {
        createTable("CREATE TABLE %s (key text, clustering int, a int, s int static, PRIMARY KEY(key, clustering))");
        TableMetadata cfm = currentTableMetadata();

        UpdateBuilder builder = UpdateBuilder.create(cfm, "key0");
        builder.newRow().add("s", 1);
        builder.newRow(1).add("a", 2);
        PartitionUpdate originalUpdate = builder.build();

        int version = MessagingService.VERSION_DSE_68;
        for (PartitionUpdate update : new PartitionUpdate[]{ originalUpdate, TriePartitionUpdate.asTrieUpdate(originalUpdate) })
        {
            long expected;
            try (UnfilteredRowIterator iter = update.unfilteredIterator())
            {
                expected = cfm.id.serializedSize()
                           + TypeSizes.LONG_SIZE
                           + UnfilteredRowIteratorSerializer.serializer.serializedSize(iter, null, version, update.rowCount());
            }
            Assert.assertEquals(update.getClass().getSimpleName(), expected, PartitionUpdate.serializer.serializedSize(update, version));
        }
    }

    @Test
    public void testComprehensiveHeterogeneousTrieSerializationRoundTrip() throws Exception
    {
        createTable("CREATE TABLE %s (key text, clustering int, a int, s int static, PRIMARY KEY(key, clustering))");
        TableMetadata cfm = currentTableMetadata();

        long timestamp = FBUtilities.timestampMicros();

        // 1. Build update with static row and regular rows
        UpdateBuilder builder = UpdateBuilder.create(cfm, "key0");
        builder.newRow().add("s", 10);
        builder.newRow(1).add("a", 100);
        builder.newRow(2).add("a", 200);
        PartitionUpdate originalUpdate = builder.build();
        PartitionUpdate trieUpdate = TriePartitionUpdate.asTrieUpdate(originalUpdate);

        int version = MessagingService.current_version;

        // Verify size calculation and round-trip serialization for TriePartitionUpdate
        long expectedSize = PartitionUpdate.serializer.serializedSize(trieUpdate, version);
        DataOutputBuffer out = new DataOutputBuffer();
        PartitionUpdate.serializer.serialize(trieUpdate, out, version);
        Assert.assertEquals(expectedSize, out.position());

        DataInputBuffer in = new DataInputBuffer(out.buffer(), false);
        PartitionUpdate deserialized = PartitionUpdate.serializer.deserialize(in, version, DeserializationHelper.Flag.LOCAL);

        Assert.assertEquals(trieUpdate.partitionKey(), deserialized.partitionKey());
        Assert.assertEquals(trieUpdate.rowCount(), deserialized.rowCount());
        // As above: dataSize is accounting that a round trip does not carry, so compare the content instead.
        Assert.assertEquals(trieUpdate, TriePartitionUpdate.asTrieUpdate(deserialized));
        Assert.assertEquals(trieUpdate.stats().minTimestamp, deserialized.stats().minTimestamp);

        // 2. Build update with expiring cells (TTL)
        RowUpdateBuilder expiringBuilder = new RowUpdateBuilder(cfm, timestamp, 3600, "key1");
        expiringBuilder.clustering(1).add("a", 500);
        PartitionUpdate expiringTrie = TriePartitionUpdate.asTrieUpdate(expiringBuilder.buildUpdate());

        out = new DataOutputBuffer();
        PartitionUpdate.serializer.serialize(expiringTrie, out, version);
        in = new DataInputBuffer(out.buffer(), false);
        PartitionUpdate deserializedExpiring = PartitionUpdate.serializer.deserialize(in, version, DeserializationHelper.Flag.LOCAL);

        Assert.assertEquals(expiringTrie.partitionKey(), deserializedExpiring.partitionKey());
        Assert.assertEquals(expiringTrie.rowCount(), deserializedExpiring.rowCount());

        // 3. Verify empty partition serialization round-trip
        PartitionUpdate emptyTrie = TriePartitionUpdate.asTrieUpdate(PartitionUpdate.emptyUpdate(cfm, cfm.partitioner.decorateKey(ByteBufferUtil.bytes("emptyKey"))));
        out = new DataOutputBuffer();
        PartitionUpdate.serializer.serialize(emptyTrie, out, version);
        in = new DataInputBuffer(out.buffer(), false);
        PartitionUpdate deserializedEmpty = PartitionUpdate.serializer.deserialize(in, version, DeserializationHelper.Flag.LOCAL);
        Assert.assertEquals(emptyTrie.partitionKey(), deserializedEmpty.partitionKey());
        Assert.assertEquals(0, deserializedEmpty.rowCount());
    }
}
