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

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.UpdateBuilder;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.TriePartitionUpdate;
import org.apache.cassandra.db.rows.DeserializationHelper;
import org.apache.cassandra.db.rows.Row;
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
        PartitionUpdate trieUpdate1 = TriePartitionUpdate.asTrieUpdate(update1);

        int size1 = update1.dataSize();
        int rowSum = update1.staticRow().dataSize();
        for (Row row : update1.rows())
            rowSum += row.dataSize();

        Assert.assertEquals(rowSum, size1);
        // TriePartitionUpdate provides a compact 64-byte trie node dataSize footprint
        Assert.assertEquals(64, trieUpdate1.dataSize());

        builder = UpdateBuilder.create(cfm, "key0");
        builder.newRow(1).add("a", 2);
        int size2 = builder.build().dataSize();
        Assert.assertTrue(size1 != size2);

        builder = UpdateBuilder.create(cfm, "key0");
        int size3 = builder.build().dataSize();
        Assert.assertTrue(size2 != size3);
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
            Assert.assertEquals(update.dataSize(), deserializedUpdate.dataSize());
            Assert.assertEquals(update.operationCount(), deserializedUpdate.operationCount());
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
        Assert.assertEquals(trieUpdate.dataSize(), deserialized.dataSize());
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
