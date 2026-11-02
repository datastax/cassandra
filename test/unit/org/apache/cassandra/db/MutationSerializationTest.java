/*
 * Copyright IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.db;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntFunction;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.partitions.BTreePartitionUpdate;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.TriePartitionUpdate;
import org.apache.cassandra.io.util.CountingDataOutputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * How {@link Mutation.MutationSerializer} serializes a mutation for the first time at a version: one smaller than
 * the cacheable size limit is serialized once, without being sized first, and its bytes are kept; writing one that
 * reaches the limit stops at the limit, and only its size is kept.
 */
@RunWith(BMUnitRunner.class)
@BMRule(name = "Count the payloads a trie partition update lays out to keep",
        targetClass = "org.apache.cassandra.db.partitions.TriePartitionUpdateSerializer$ContentSerDe",
        targetMethod = "serialize(org.apache.cassandra.io.util.DataOutputPlus, java.lang.Object)",
        action = "org.apache.cassandra.db.MutationSerializationTest.layingOut($1)")
public class MutationSerializationTest extends CQLTester
{
    private static final long LIMIT = CassandraRelevantProperties.CACHEABLE_MUTATION_SIZE_LIMIT.getLong();
    private static final int MAX_RECYCLE_BYTES = CassandraRelevantProperties.DOB_MAX_RECYCLE_BYTES.getInt();
    private static final List<Integer> VERSIONS = Arrays.asList(MessagingService.VERSION_40, MessagingService.VERSION_DS_21);

    private static final AtomicInteger payloadsLaidOut = new AtomicInteger();

    /**
     * Called for every payload of a trie's live branch that a trie partition update lays out. Counts those laid out
     * to keep, and not only to be counted, as sizing an update does.
     */
    public static void layingOut(DataOutputPlus out)
    {
        if (!(out instanceof CountingDataOutputPlus))
            payloadsLaidOut.incrementAndGet();
    }

    /** Counts how often the partition updates of a mutation are serialized and sized. */
    private static class CountingSerializer extends PartitionUpdate.PartitionUpdateSerializer
    {
        int serializations;
        int sizings;

        CountingSerializer()
        {
            super(tableId -> Schema.instance.getExistingTableMetadata(tableId));
        }

        @Override
        public void serialize(PartitionUpdate update, DataOutputPlus out, int version) throws IOException
        {
            ++serializations;
            super.serialize(update, out, version);
        }

        @Override
        public long serializedSize(PartitionUpdate update, int version)
        {
            ++sizings;
            return super.serializedSize(update, version);
        }
    }

    @Test
    public void testMutationBelowLimitIsSerializedOnce() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v blob)");
        for (IntFunction<PartitionUpdate> update : updates())
        {
            for (int version : VERSIONS)
            {
                Mutation mutation = new Mutation(update.apply(100));
                CountingSerializer counting = new CountingSerializer();
                Mutation.MutationSerializer serializer = new Mutation.MutationSerializer(counting);

                long size = serializer.serializedSize(mutation, version);
                byte[] written = serialize(serializer, mutation, version);
                assertArrayEquals(written, serialize(serializer, mutation, version));

                String shape = mutation + " at " + version;
                assertEquals(shape, 1, counting.serializations);
                assertEquals(shape, 0, counting.sizings);
                assertEquals(shape, size, written.length);
                assertArrayEquals(shape, expectedBytes(mutation, version), written);
            }
        }
    }

    /**
     * The limit is exclusive: a mutation of one byte less is kept, and one of the limit's size is written again for
     * every serialization, and is not built in memory past the limit to find out that it is that large.
     */
    @Test
    public void testMutationReachingLimitIsSizedAndNotKept() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v blob)");
        for (IntFunction<PartitionUpdate> update : updates())
        {
            for (int version : VERSIONS)
                assertKeptOnlyBelowLimit(length -> new Mutation(update.apply(length)), version);
        }
    }

    /**
     * The limit is that of the whole mutation, so a mutation of updates of several tables can reach it in an update
     * other than its first, which has the room the updates before it left.
     */
    @Test
    public void testMutationReachingLimitInItsSecondUpdateIsSizedAndNotKept() throws Throwable
    {
        TableMetadata first = Schema.instance.getTableMetadata(KEYSPACE, createTable("CREATE TABLE %s (k int PRIMARY KEY, v blob)"));
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v blob)");
        for (IntFunction<PartitionUpdate> firstUpdate : updates(first))
        {
            PartitionUpdate head = firstUpdate.apply(100);
            for (IntFunction<PartitionUpdate> secondUpdate : updates())
            {
                for (int version : VERSIONS)
                {
                    assertKeptOnlyBelowLimit(length -> new Mutation(KEYSPACE,
                                                                    head.partitionKey(),
                                                                    ImmutableMap.of(head.metadata().id, head, currentTableMetadata().id, secondUpdate.apply(length)),
                                                                    0),
                                             version);
                }
            }
        }
    }

    /**
     * A trie partition update lays its trie out before it writes it, and that of a mutation too large to keep is laid
     * out no further than the limit has room for. The update holds a value repeated in many rows, as a statement that
     * binds one value to many clustering keys makes, which is small in memory and ten times the limit serialized.
     */
    @Test
    public void testTrieOfMutationAboveLimitIsLaidOutOnlyUpToTheLimit() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v blob, PRIMARY KEY (k, c))");
        int valueLength = 10_000;
        ByteBuffer value = ByteBuffer.allocate(valueLength);
        PartitionUpdate.SimpleBuilder builder = PartitionUpdate.simpleBuilder(currentTableMetadata(), 1).timestamp(1000);
        for (int clustering = 0; clustering < 10 * LIMIT / valueLength; ++clustering)
            builder.row(clustering).add("v", value);
        Mutation mutation = new Mutation(TriePartitionUpdate.asTrieUpdate(builder.build()));
        int version = MessagingService.VERSION_DS_21;
        long expectedSize = expectedSize(mutation, version);
        assertTrue(expectedSize > 10 * LIMIT);

        payloadsLaidOut.set(0);
        assertEquals(expectedSize, new Mutation.MutationSerializer(PartitionUpdate.serializer).serializedSize(mutation, version));
        // A row lays out its liveness and its cell, and no more cells are laid out than fit in the limit.
        int laidOut = payloadsLaidOut.get();
        assertTrue(String.valueOf(laidOut), laidOut > 0 && laidOut <= 2 * (LIMIT / valueLength + 1));
        assertScratchBufferIsReusable();
    }

    /**
     * A mutation one byte smaller than the limit is serialized once and kept; one of the limit's size is serialized
     * until it reaches the limit, then sized, and only its size is kept.
     */
    private static void assertKeptOnlyBelowLimit(IntFunction<Mutation> mutation, int version) throws IOException
    {
        Mutation belowLimit = mutation.apply(valueLength(mutation, version, LIMIT - 1));
        int updates = belowLimit.getPartitionUpdates().size();
        String shape = belowLimit + " at " + version;
        CountingSerializer counting = new CountingSerializer();
        Mutation.MutationSerializer serializer = new Mutation.MutationSerializer(counting);
        assertEquals(shape, LIMIT - 1, serializer.serializedSize(belowLimit, version));
        assertArrayEquals(shape, expectedBytes(belowLimit, version), serialize(serializer, belowLimit, version));
        assertEquals(shape, updates, counting.serializations);
        assertEquals(shape, 0, counting.sizings);

        Mutation atLimit = mutation.apply(valueLength(mutation, version, LIMIT));
        counting = new CountingSerializer();
        serializer = new Mutation.MutationSerializer(counting);
        assertEquals(shape, LIMIT, serializer.serializedSize(atLimit, version));
        // The serialization that stopped at the limit in the last update, and the sizing that followed it.
        assertEquals(shape, updates, counting.serializations);
        assertEquals(shape, updates, counting.sizings);
        assertScratchBufferIsReusable();

        byte[] expected = expectedBytes(atLimit, version);
        assertArrayEquals(shape, expected, serialize(serializer, atLimit, version));
        assertArrayEquals(shape, expected, serialize(serializer, atLimit, version));
        assertEquals(shape, 3 * updates, counting.serializations);
        assertEquals(shape, updates, counting.sizings);
        assertEquals(shape, LIMIT, serializer.serializedSize(atLimit, version));
        assertEquals(shape, updates, counting.sizings);
    }

    /** The scratch buffer the mutation was written to up to the limit holds nothing, and is not kept oversized. */
    private static void assertScratchBufferIsReusable()
    {
        DataOutputBuffer scratch = DataOutputBuffer.scratchBuffer.get();
        assertEquals(0, scratch.getLength());
        assertTrue(scratch.buffer().capacity() <= MAX_RECYCLE_BYTES);
    }

    /** A single-row update of the current table with a blob of the given length, as a trie and as a BTree update. */
    private List<IntFunction<PartitionUpdate>> updates()
    {
        return updates(currentTableMetadata());
    }

    /** As above, of the given table. */
    private static List<IntFunction<PartitionUpdate>> updates(TableMetadata metadata)
    {
        IntFunction<PartitionUpdate> update = length -> {
            PartitionUpdate.SimpleBuilder builder = PartitionUpdate.simpleBuilder(metadata, 1).timestamp(1000);
            builder.row().add("v", ByteBuffer.allocate(length));
            return builder.build();
        };
        return Arrays.asList(length -> TriePartitionUpdate.asTrieUpdate(update.apply(length)),
                             length -> BTreePartitionUpdate.asBTreeUpdate(update.apply(length)));
    }

    /** The length of the blob that makes the serialization of the mutation the given size. */
    private static int valueLength(IntFunction<Mutation> mutation, int version, long size)
    {
        int length = (int) size;
        for (int i = 0; i < 10; ++i)
        {
            long serializedSize = expectedSize(mutation.apply(length), version);
            if (serializedSize == size)
                return length;
            length += (int) (size - serializedSize);
        }
        throw new AssertionError("No blob length gives a mutation of " + size + " bytes");
    }

    private static long expectedSize(Mutation mutation, int version)
    {
        long size = TypeSizes.sizeofUnsignedVInt(mutation.getPartitionUpdates().size());
        for (PartitionUpdate update : mutation.getPartitionUpdates())
            size += PartitionUpdate.serializer.serializedSize(update, version);
        return size;
    }

    /** What a mutation is written as: the number of its updates, then the updates. */
    private static byte[] expectedBytes(Mutation mutation, int version) throws IOException
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            out.writeUnsignedVInt32(mutation.getPartitionUpdates().size());
            for (PartitionUpdate update : mutation.getPartitionUpdates())
                PartitionUpdate.serializer.serialize(update, out, version);
            return out.toByteArray();
        }
    }

    private static byte[] serialize(Mutation.MutationSerializer serializer, Mutation mutation, int version) throws IOException
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            serializer.serialize(mutation, out, version);
            return out.toByteArray();
        }
    }
}
