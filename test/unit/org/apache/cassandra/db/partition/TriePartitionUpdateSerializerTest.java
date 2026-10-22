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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Supplier;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.CounterMutation;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.partitions.BTreePartitionUpdate;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.PartitionUpdateSizeCache;
import org.apache.cassandra.db.partitions.TriePartitionUpdate;
import org.apache.cassandra.db.partitions.TriePartitionUpdateSerializer;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.DeserializationHelper;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.paxos.Ballot;
import org.apache.cassandra.service.paxos.Commit;
import org.apache.cassandra.service.paxos.PaxosState;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CounterId;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.ObjectSizes;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Round-trip tests for {@link TriePartitionUpdateSerializer}, the encoding used for partition updates
 * in the commit log and in internode messages from {@link MessagingService#VERSION_DS_21} on.
 */
public class TriePartitionUpdateSerializerTest extends CQLTester
{
    private static final int VERSION = MessagingService.VERSION_DS_21;

    /**
     * A partition carrying one of everything the encoding has to deal with: a partition-level
     * deletion, a static row with a complex column, regular rows with a complex column, and a
     * range tombstone.
     */
    private TriePartitionUpdate richUpdate()
    {
        TableMetadata metadata = currentTableMetadata();
        PartitionUpdate.SimpleBuilder builder = PartitionUpdate.simpleBuilder(metadata, "key0");

        // Older than everything below, so nothing it covers is shadowed away.
        builder.timestamp(1000).nowInSec(1500).delete();

        builder.timestamp(2000).nowInSec(1500);
        builder.row().add("s", 7).add("ss", ImmutableSet.of("x", "y"));
        builder.row(1).add("v", 11).add("m", ImmutableMap.of("a", "1", "b", "2"));
        builder.row(3).add("v", 33);
        builder.addRangeTombstone().start(5).end(9).inclStart().exclEnd();
        builder.addRangeTombstone().start(20).end(30).exclStart().inclEnd();

        return TriePartitionUpdate.asTrieUpdate(builder.build());
    }

    private void createRichTable()
    {
        createTable("CREATE TABLE %s (k text, c int, v int, m map<text, text>, " +
                    "s int static, ss set<text> static, PRIMARY KEY(k, c))");
    }

    private static byte[] serialize(PartitionUpdate update) throws IOException
    {
        return serialize(update, VERSION);
    }

    private static byte[] serialize(PartitionUpdate update, int version) throws IOException
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            TriePartitionUpdateSerializer.serialize(update, out, version);
            return out.toByteArray();
        }
    }

    private TriePartitionUpdate deserialize(byte[] bytes) throws IOException
    {
        return deserialize(bytes, VERSION);
    }

    private TriePartitionUpdate deserialize(byte[] bytes, int version) throws IOException
    {
        try (DataInputBuffer in = new DataInputBuffer(bytes))
        {
            return TriePartitionUpdateSerializer.deserialize(in, version, DeserializationHelper.Flag.LOCAL, currentTableMetadata());
        }
    }

    /** Consume an update the way a replay or a memtable apply would. */
    private static void walk(TriePartitionUpdate update)
    {
        update.partitionLevelDeletion();
        update.deletionInfo();
        update.staticRow();
        update.rowCount();
        try (UnfilteredRowIterator iterator = update.unfilteredIterator())
        {
            iterator.forEachRemaining(u -> {});
        }
    }

    @Test
    public void testRoundTrip() throws Throwable
    {
        createRichTable();
        TriePartitionUpdate update = richUpdate();
        assertFalse("the fixture must carry a partition-level deletion",
                    update.deletionInfo().getPartitionDeletion().isLive());
        assertTrue("the fixture must carry range tombstones", update.deletionInfo().hasRanges());
        assertFalse("the fixture must carry a static row", update.staticRow().isEmpty());

        TriePartitionUpdate read = deserialize(serialize(update));

        assertEquals(update.partitionKey(), read.partitionKey());
        assertEquals(update.deletionInfo(), read.deletionInfo());
        assertEquals(update.staticRow(), read.staticRow());
        assertEquals(update.rowCount(), read.rowCount());
        assertEquals(update.dataSize(), read.dataSize());
        assertEquals(update.operationCount(), read.operationCount());
        assertEquals(update.columns(), read.columns());
        assertEquals(update.stats(), read.stats());
        assertEquals(update, read);
    }

    @Test
    public void testRoundTripOfEmptyUpdate() throws Throwable
    {
        createRichTable();
        TriePartitionUpdate update =
            TriePartitionUpdate.asTrieUpdate(PartitionUpdate.emptyUpdate(currentTableMetadata(),
                                                                         currentTableMetadata().partitioner.decorateKey(ByteBufferUtil.bytes("key0"))));

        assertEquals(update, deserialize(serialize(update)));
    }

    /**
     * The sizing path writes the trie a second time to measure it ({@code serializedTrieSize}).
     * If the two runs ever disagreed, the commit log would get a wrong length.
     */
    @Test
    public void testSerializedSizeMatchesBytesWritten() throws Throwable
    {
        createRichTable();
        TriePartitionUpdate update = richUpdate();

        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            long size = TriePartitionUpdateSerializer.serializedSize(update, VERSION);
            TriePartitionUpdateSerializer.serialize(update, out, VERSION);
            assertEquals(size, out.getLength());
        }

        // And through the entry point the mutation path actually uses, which adds the table id and
        // the format byte and caches the size on the update.
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            long size = PartitionUpdate.serializer.serializedSize(update, VERSION);
            PartitionUpdate.serializer.serialize(update, out, VERSION);
            assertEquals(size, out.getLength());
            // Second call is served from the cache; it must still describe the same bytes.
            assertEquals(size, PartitionUpdate.serializer.serializedSize(update, VERSION));
        }
    }

    /**
     * Sizing an update lays its trie out and the write that follows reuses that layout instead of building it a
     * second time. What is written must be exactly what the writer would have produced, so sizing first cannot
     * change the bytes, and the layout must be handed over only once.
     */
    @Test
    public void testSizingFirstDoesNotChangeTheBytesWritten() throws Throwable
    {
        createRichTable();

        byte[] withoutSizing = serialize(richUpdate());

        TriePartitionUpdate sizedFirst = richUpdate();
        TriePartitionUpdateSerializer.serializedSize(sizedFirst, VERSION);
        assertArrayEquals(withoutSizing, serialize(sizedFirst));

        // The retained layout is consumed by that write; a second one has to build the trie again and must still
        // produce the same bytes.
        assertArrayEquals(withoutSizing, serialize(sizedFirst));
    }

    /**
     * The layout sizing keeps is for {@link MessagingService#VERSION_DS_21}, like the size it keeps, and a write at
     * another version must not take it. That is the only version the trie encoding is written at today, so this writes
     * at a later, hypothetical, one; sizing at that version keeps nothing.
     */
    @Test
    public void testRetainedLayoutNotTakenAtAnotherVersion() throws Throwable
    {
        createRichTable();
        int otherVersion = MessagingService.VERSION_DS_21 + 1;

        TriePartitionUpdate update = richUpdate();
        TriePartitionUpdateSerializer.serializedSize(update, MessagingService.VERSION_DS_21);
        assertTrue(PartitionUpdateSizeCache.hasRetainedTrie(update));
        assertArrayEquals(serialize(richUpdate(), otherVersion), serialize(update, otherVersion));
        assertTrue(PartitionUpdateSizeCache.hasRetainedTrie(update));

        TriePartitionUpdate sizedAtOther = richUpdate();
        TriePartitionUpdateSerializer.serializedSize(sizedAtOther, otherVersion);
        assertFalse(PartitionUpdateSizeCache.hasRetainedTrie(sizedAtOther));
    }

    /**
     * The layout sizing keeps on the update holds the trie's bytes and no more, rather than the buffer they were written
     * to, which grows by doubling.
     */
    @Test
    public void testRetainedLayoutIsTrimmed() throws Throwable
    {
        createRichTable();
        TriePartitionUpdate update = richUpdate();
        TriePartitionUpdateSerializer.serializedSize(update, VERSION);
        ByteBuffer retained = PartitionUpdateSizeCache.retainedTrie(update);
        assertEquals(retained.remaining(), retained.capacity());
    }

    /**
     * {@link PartitionUpdateSizeCache#invalidate} goes through the update's own invalidation, the one a counter leader
     * rewriting a counter cell uses, which must clear the cached serialized sizes and any retained trie layout. The
     * update's heap size counts the retained layout for as long as the update holds it.
     */
    @Test
    public void testPartitionUpdateSizeCacheInvalidate() throws Throwable
    {
        createRichTable();
        TriePartitionUpdate update = richUpdate();
        long heapSize = update.unsharedHeapSize();
        TriePartitionUpdateSerializer.serializedSize(update, MessagingService.VERSION_DS_21);
        TriePartitionUpdateSerializer.serializedSize(update, MessagingService.VERSION_DS_20);

        assertTrue(PartitionUpdateSizeCache.isSized(update, MessagingService.VERSION_DS_21));
        assertTrue(PartitionUpdateSizeCache.isSized(update, MessagingService.VERSION_DS_20));
        assertTrue(PartitionUpdateSizeCache.hasRetainedTrie(update));
        assertEquals(heapSize + ObjectSizes.sizeOnHeapOf(PartitionUpdateSizeCache.retainedTrie(update)), update.unsharedHeapSize());

        PartitionUpdateSizeCache.invalidate(update);

        assertFalse(PartitionUpdateSizeCache.isSized(update, MessagingService.VERSION_DS_21));
        assertFalse(PartitionUpdateSizeCache.isSized(update, MessagingService.VERSION_DS_20));
        assertFalse(PartitionUpdateSizeCache.hasRetainedTrie(update));
        assertEquals(heapSize, update.unsharedHeapSize());

        // The write that takes the layout off the update stops it being counted as well.
        TriePartitionUpdateSerializer.serializedSize(update, MessagingService.VERSION_DS_21);
        serialize(update);
        assertFalse(PartitionUpdateSizeCache.hasRetainedTrie(update));
        assertEquals(heapSize, update.unsharedHeapSize());
    }

    /**
     * {@link org.apache.cassandra.db.Mutation#serializedSize} sizes a mutation and the result is used to reserve the
     * commit log region that {@code serialize} then writes into, so the two must agree to the byte for every shape an
     * update can take. Counts, liveness and deletion times are all encoded as deltas or vints, whose width depends on
     * the values, which is exactly where the two can drift apart.
     */
    @Test
    public void testSerializedSizeMatchesBytesWrittenForEveryShape() throws Throwable
    {
        createRichTable();
        TableMetadata metadata = currentTableMetadata();

        assertSizeAndRoundTrip("empty update",
                               TriePartitionUpdate.asTrieUpdate(PartitionUpdate.emptyUpdate(metadata, metadata.partitioner.decorateKey(ByteBufferUtil.bytes("key0")))));

        assertSizeAndRoundTrip("one row", build(builder -> {
            builder.timestamp(2000).nowInSec(1500);
            builder.row(1).add("v", 11);
        }));

        assertSizeAndRoundTrip("ten rows", build(builder -> {
            builder.timestamp(2000).nowInSec(1500);
            for (int i = 0; i < 10; ++i)
                builder.row(i).add("v", i);
        }));

        assertSizeAndRoundTrip("row with a ttl", build(builder -> {
            builder.timestamp(2000).nowInSec(1500).ttl(60);
            builder.row(1).add("v", 11);
        }));

        assertSizeAndRoundTrip("range tombstone", build(builder -> {
            builder.timestamp(2000).nowInSec(1500);
            builder.row(1).add("v", 11);
            builder.addRangeTombstone().start(5).end(9).inclStart().exclEnd();
        }));

        assertSizeAndRoundTrip("complex columns", build(builder -> {
            builder.timestamp(2000).nowInSec(1500);
            builder.row().add("s", 7).add("ss", ImmutableSet.of("x", "y"));
            builder.row(1).add("m", ImmutableMap.of("a", "1", "b", "2"));
        }));

        assertSizeAndRoundTrip("range tombstone without rows", build(builder -> {
            builder.timestamp(2000).nowInSec(1500);
            builder.addRangeTombstone().start(5).end(9).inclStart().exclEnd();
        }));

        assertSizeAndRoundTrip("row deletion without rows", build(builder -> builder.timestamp(2000).nowInSec(1500).row(6).delete()));

        assertSizeAndRoundTrip("partition deletion", build(builder -> builder.timestamp(1000).nowInSec(1500).delete()));
    }

    /**
     * A received or replayed update is consumed in several ways besides being merged into a memtable: re-serialized
     * at the storage version for the commit log (VERSION_40 in the HCD_1 storage mode, whose size check every
     * received mutation goes through), forwarded, hinted or batched at the peer's version, converted to a BTree
     * update, and walked by indexes and views. Each has to work on every shape, and above all on the ones with
     * deletions and no live rows, which schema changes and deletes produce, and agree with the update it was read
     * from. Each step runs under a time limit, as some of these walks have failed by never returning.
     */
    @Test
    public void testDeserializedUpdateConsumers() throws Throwable
    {
        createRichTable();
        assertConsumersOfDeserialized("plain row", builder -> builder.timestamp(2000).nowInSec(1500).row(1).add("v", 11));
        assertConsumersOfDeserialized("row deletion and insert", builder -> {
            builder.timestamp(1999).nowInSec(1500).row(1).delete();
            builder.timestamp(2000).nowInSec(1500).row(1).add("v", 11);
        });
        assertConsumersOfDeserialized("row deletion only", builder -> builder.timestamp(1999).nowInSec(1500).row(1).delete());
        assertConsumersOfDeserialized("partition deletion only", builder -> builder.timestamp(1999).nowInSec(1500).delete());
        assertConsumersOfDeserialized("range tombstone only", builder -> {
            builder.timestamp(1999).nowInSec(1500);
            builder.addRangeTombstone().start(5).end(9).inclStart().exclEnd();
        });
        assertConsumersOfDeserialized("map overwrite", builder -> builder.timestamp(2000).nowInSec(1500).row(1).add("m", ImmutableMap.of("a", "1")));
        assertConsumersOfDeserialized("everything", builder -> {
            builder.timestamp(1000).nowInSec(1500).delete();
            builder.timestamp(2000).nowInSec(1500);
            builder.row().add("s", 7).add("ss", ImmutableSet.of("x", "y"));
            builder.row(1).add("v", 11).add("m", ImmutableMap.of("a", "1", "b", "2"));
            builder.row(2).delete();
            builder.addRangeTombstone().start(5).end(9).inclStart().exclEnd();
        });
    }

    private void assertConsumersOfDeserialized(String shape, Consumer<PartitionUpdate.SimpleBuilder> content) throws Throwable
    {
        TriePartitionUpdate original = build(content);
        byte[] bytes = serialize(original);
        Supplier<TriePartitionUpdate> received = () -> {
            try
            {
                return deserialize(bytes);
            }
            catch (IOException e)
            {
                throw new UncheckedIOException(e);
            }
        };

        withTimeLimit(shape + ": unfilteredIterator", () -> {
            try (UnfilteredRowIterator expected = original.unfilteredIterator();
                 UnfilteredRowIterator actual = received.get().unfilteredIterator())
            {
                assertEquals(expected.partitionLevelDeletion(), actual.partitionLevelDeletion());
                assertEquals(expected.staticRow(), actual.staticRow());
                assertTrue(Iterators.elementsEqual(expected, actual));
            }
        });
        withTimeLimit(shape + ": rowsIncludingStatic", () -> assertTrue(Iterators.elementsEqual(original.rowsIncludingStatic(), received.get().rowsIncludingStatic())));
        withTimeLimit(shape + ": maxTimestamp", () -> assertEquals(original.maxTimestamp(), received.get().maxTimestamp()));
        withTimeLimit(shape + ": asBTreeUpdate", () -> assertEquals(BTreePartitionUpdate.asBTreeUpdate(original), BTreePartitionUpdate.asBTreeUpdate(received.get())));
        for (int version : Arrays.asList(MessagingService.VERSION_40, MessagingService.VERSION_DS_20, VERSION))
        {
            withTimeLimit(shape + ": re-serialized at " + version, () -> {
                TriePartitionUpdate update = received.get();
                try (DataOutputBuffer expected = new DataOutputBuffer(); DataOutputBuffer actual = new DataOutputBuffer())
                {
                    PartitionUpdate.serializer.serialize(original, expected, version);
                    long size = PartitionUpdate.serializer.serializedSize(update, version);
                    PartitionUpdate.serializer.serialize(update, actual, version);
                    assertEquals(size, actual.getLength());
                    assertArrayEquals(expected.toByteArray(), actual.toByteArray());
                }
            });
        }
    }

    private interface Step
    {
        void run() throws Exception;
    }

    private static void withTimeLimit(String step, Step body) throws Throwable
    {
        FutureTask<Void> task = new FutureTask<>(() -> {
            body.run();
            return null;
        });
        Thread thread = new Thread(task, "TriePartitionUpdateSerializerTest " + step);
        thread.setDaemon(true);
        thread.start();
        try
        {
            task.get(30, TimeUnit.SECONDS);
        }
        catch (TimeoutException e)
        {
            thread.interrupt();
            fail(step + " did not return");
        }
        catch (ExecutionException e)
        {
            throw new AssertionError(step, e.getCause());
        }
    }

    private TriePartitionUpdate build(Consumer<PartitionUpdate.SimpleBuilder> content)
    {
        PartitionUpdate.SimpleBuilder builder = PartitionUpdate.simpleBuilder(currentTableMetadata(), "key0");
        content.accept(builder);
        return TriePartitionUpdate.asTrieUpdate(builder.build());
    }

    /** Size the update the way the mutation path does, then write it, and check the two agree and that it reads back. */
    private void assertSizeAndRoundTrip(String shape, TriePartitionUpdate update) throws IOException
    {
        byte[] bytes = assertSizeMatchesBytesWritten(shape, update);
        try (DataInputBuffer in = new DataInputBuffer(bytes))
        {
            assertEquals(shape, update, PartitionUpdate.serializer.deserialize(in, VERSION, DeserializationHelper.Flag.LOCAL));
        }
    }

    /** Size the update the way the mutation path does, then write it, and check the two agree. */
    private byte[] assertSizeMatchesBytesWritten(String shape, TriePartitionUpdate update) throws IOException
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            long size = PartitionUpdate.serializer.serializedSize(update, VERSION);
            PartitionUpdate.serializer.serialize(update, out, VERSION);
            assertEquals(shape, size, out.getLength());
            return out.toByteArray();
        }
    }

    /**
     * A row written without primary key liveness (an UPDATE) carries {@link LivenessInfo#EMPTY}, which the trie row
     * and partition code recognise by identity. It must read back as that instance, or an UPDATE turns into an
     * INSERT when it is re-proposed or re-timestamped; and it must not cost more than a row with real liveness.
     */
    @Test
    public void testEmptyLivenessRoundTrip() throws Throwable
    {
        createRichTable();
        TriePartitionUpdate withoutLiveness = build(builder -> builder.timestamp(2000).nowInSec(1500).row(1).noPrimaryKeyLivenessInfo().add("v", 11));
        TriePartitionUpdate withLiveness = build(builder -> builder.timestamp(2000).nowInSec(1500).row(1).add("v", 11));
        assertSame(LivenessInfo.EMPTY, withoutLiveness.rowIterator().next().primaryKeyLivenessInfo());

        TriePartitionUpdate read = deserialize(serialize(withoutLiveness));
        assertSame(LivenessInfo.EMPTY, read.rowIterator().next().primaryKeyLivenessInfo());
        assertSame(LivenessInfo.EMPTY, read.withUpdatedTimestamps(3000).rowIterator().next().primaryKeyLivenessInfo());
        assertEquals(withoutLiveness, read);

        assertTrue(serialize(withoutLiveness).length < serialize(withLiveness).length);
    }

    /**
     * The counter leader rewrites each counter cell in place through {@link PartitionUpdate.CounterMark#setValue}
     * after the update may already have been sized, which lays its trie out and keeps the layout for the write that
     * follows. That write must carry the new counter context, not the one the layout was taken from.
     */
    @Test
    public void testCounterValueChangeAfterSizingIsWritten() throws Throwable
    {
        createTable("CREATE TABLE %s (k text, c int, cnt counter, PRIMARY KEY(k, c))");
        TriePartitionUpdate update = counterUpdate();
        PartitionUpdate.serializer.serializedSize(update, VERSION);

        setCounterValues(update);

        byte[] written;
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            long size = PartitionUpdate.serializer.serializedSize(update, VERSION);
            PartitionUpdate.serializer.serialize(update, out, VERSION);
            written = out.toByteArray();
            assertEquals(size, written.length);
        }

        // What a freshly built update with the same values lays out.
        TriePartitionUpdate fresh = counterUpdate();
        setCounterValues(fresh);
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            PartitionUpdate.serializer.serialize(fresh, out, VERSION);
            assertArrayEquals(out.toByteArray(), written);
        }
    }

    /**
     * A counter write whose leader is not the coordinator reaches the leader as a deserialized update, whose trie is
     * read in place from the message. The leader then rewrites the update's counter cells in place.
     */
    @Test
    public void testCounterUpdateReceivedByLeader() throws Throwable
    {
        createTable("CREATE TABLE %s (k text, c int, cnt counter, PRIMARY KEY(k, c))");

        for (int i = 0; i < 2; ++i)
        {
            Mutation received = roundTrip(new Mutation(counterUpdate()));
            new CounterMutation(received, ConsistencyLevel.ONE).applyCounterMutation();
        }

        assertRows(execute("SELECT cnt FROM %s WHERE k = 'key0' AND c = 1"), row(10L));
    }

    /**
     * A Paxos replica accepts a proposal it received from the coordinator and keeps it in its in-memory state, whose
     * cache weighs it by its heap size, and in system.paxos.
     */
    @Test
    public void testPaxosProposalReceivedByReplica() throws Throwable
    {
        createRichTable();
        TriePartitionUpdate update = build(builder -> builder.row(1).add("v", 11));
        Ballot ballot = Ballot.atUnixMicrosWithLsb(FBUtilities.timestampMicros(), 0, Ballot.Flag.GLOBAL);
        Commit proposal = Commit.newProposal(ballot, update);

        Commit received;
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            Commit.serializer.serialize(proposal, out, VERSION);
            try (DataInputBuffer in = new DataInputBuffer(out.toByteArray()))
            {
                received = Commit.serializer.deserialize(in, VERSION);
            }
        }
        assertTrue(received.update.unsharedHeapSize() > 0);

        assertTrue(PaxosState.legacyPrepare(received).promised);
        assertTrue(PaxosState.legacyPropose(received));
        PaxosState.Snapshot state = PaxosState.unsafeGetIfPresent(update.partitionKey(), update.metadata());
        assertEquals(proposal.update, TriePartitionUpdate.asTrieUpdate(state.accepted.update));

        PaxosState.unsafeReset();
        state = SystemKeyspace.loadPaxosState(update.partitionKey(), update.metadata(), 0);
        assertEquals(proposal.update, TriePartitionUpdate.asTrieUpdate(state.accepted.update));
    }

    @Test
    public void testWithOnlyPresentColumnsOfDeserializedUpdate() throws Throwable
    {
        createRichTable();
        TriePartitionUpdate update = richUpdate();
        assertEquals(update.withOnlyPresentColumns(), deserialize(serialize(update)).withOnlyPresentColumns());
    }

    /**
     * The leader finds each counter cell it rewrites by its column's index among the table's columns, which is how
     * the trie keys it, and not among the columns the update happens to touch.
     */
    @Test
    public void testCounterUpdateOfOneOfSeveralCounters() throws Throwable
    {
        createTable("CREATE TABLE %s (k text, c int, a counter, b counter, PRIMARY KEY(k, c))");

        for (int i = 0; i < 2; ++i)
        {
            PartitionUpdate.SimpleBuilder builder = PartitionUpdate.simpleBuilder(currentTableMetadata(), "key0");
            builder.timestamp(2000).row(1).add("b", CounterContext.instance().createUpdate(5L));
            new CounterMutation(new Mutation(TriePartitionUpdate.asTrieUpdate(builder.build())), ConsistencyLevel.ONE).applyCounterMutation();
        }

        assertRows(execute("SELECT a, b FROM %s WHERE k = 'key0' AND c = 1"), row(null, 10L));
    }

    /**
     * The trie keys each column by its position among the table's columns on the node that built it. A node that has
     * added a column the sender has not yet seen numbers the columns differently, and has to read the update with its
     * own numbering: here the added columns sort first, so every existing column moves.
     */
    @Test
    public void testReceiverHasAddedColumns() throws Throwable
    {
        createSchemaChangeTable();
        byte[] bytes = serialize(build(this::schemaChangeContent));

        alterTable("ALTER TABLE %s ADD a int");
        alterTable("ALTER TABLE %s ADD a_s int static");

        assertReadAsLocalUpdate(bytes, build(this::schemaChangeContent));
        assertRows(execute("SELECT c, a, a_s, b, d, m, s FROM %s WHERE k = 'key0'"),
                   row(1, null, null, 1, 42, map("x", "1"), 7),
                   row(2, null, null, 2, null, null, 7));
    }

    /** As {@link #testReceiverHasAddedColumns}, with a column the receiver has dropped and the sender has not. */
    @Test
    public void testReceiverHasDroppedColumn() throws Throwable
    {
        createSchemaChangeTable();
        byte[] bytes = serialize(build(this::schemaChangeContent));

        alterTable("ALTER TABLE %s DROP b");

        TriePartitionUpdate expected = build(builder -> {
            builder.timestamp(1000).nowInSec(1500).delete();
            builder.timestamp(2000).nowInSec(1500);
            builder.row().add("s", 7);
            builder.row(1).add("d", 42).add("m", ImmutableMap.of("x", "1"));
            builder.row(2).delete("m");
            builder.row(3).delete();
            builder.addRangeTombstone().start(5).end(9).inclStart().exclEnd();
        });
        assertReadAsLocalUpdate(bytes, expected);
        assertRows(execute("SELECT c, d, m, s FROM %s WHERE k = 'key0'"),
                   row(1, 42, map("x", "1"), 7),
                   row(2, null, null, 7));
    }

    /** A sender that has added columns the receiver has not seen yet, which the update does not use. */
    @Test
    public void testSenderHasAddedColumns() throws Throwable
    {
        createSchemaChangeTable();
        TableMetadata senderMetadata = currentTableMetadata().unbuild()
                                                             .addRegularColumn("a", Int32Type.instance)
                                                             .addStaticColumn("a_s", Int32Type.instance)
                                                             .build();

        assertReadAsLocalUpdate(serialize(build(senderMetadata, this::schemaChangeContent)), build(this::schemaChangeContent));
    }

    /** A sender that has dropped a column the receiver still has. */
    @Test
    public void testSenderHasDroppedColumn() throws Throwable
    {
        createSchemaChangeTable();
        TableMetadata senderMetadata = currentTableMetadata().unbuild()
                                                             .removeRegularOrStaticColumn(ColumnIdentifier.getInterned("b", true))
                                                             .build();
        Consumer<PartitionUpdate.SimpleBuilder> content = builder -> {
            builder.timestamp(2000).nowInSec(1500);
            builder.row().add("s", 7);
            builder.row(1).add("d", 42).add("m", ImmutableMap.of("x", "1"));
            builder.row(2).delete("m");
        };

        assertReadAsLocalUpdate(serialize(build(senderMetadata, content)), build(content));
    }

    private void createSchemaChangeTable()
    {
        createTable("CREATE TABLE %s (k text, c int, b int, d int, m map<text, text>, s int static, PRIMARY KEY(k, c))");
    }

    /** Every kind of content whose trie key carries a column: simple and complex cells, static and regular, and a
     * complex-column deletion, next to the row, range and partition deletions that carry none. */
    private void schemaChangeContent(PartitionUpdate.SimpleBuilder builder)
    {
        builder.timestamp(1000).nowInSec(1500).delete();
        builder.timestamp(2000).nowInSec(1500);
        builder.row().add("s", 7);
        builder.row(1).add("b", 1).add("d", 42).add("m", ImmutableMap.of("x", "1"));
        builder.row(2).add("b", 2).delete("m");
        builder.row(3).delete();
        builder.addRangeTombstone().start(5).end(9).inclStart().exclEnd();
    }

    /** Read the bytes as a node with the current schema would, check the result, and apply it. */
    private void assertReadAsLocalUpdate(byte[] bytes, TriePartitionUpdate expected) throws IOException
    {
        TriePartitionUpdate read = deserialize(bytes);
        assertEquals(expected, read);
        assertEquals(expected.staticRow(), read.staticRow());
        assertEquals(expected.deletionInfo(), read.deletionInfo());
        // A cell carries its column, so comparing rows does not show that the trie keys it by this node's numbering;
        // looking each column up by its key does.
        for (Iterator<Row> rows = expected.rowsIncludingStatic(); rows.hasNext();)
        {
            Row row = rows.next();
            Row readRow = row.isStatic() ? read.staticRow() : read.getRow(row.clustering());
            for (ColumnData data : row)
                assertEquals(data, readRow.getColumnData(data.column()));
        }
        new Mutation(read).apply();
    }

    private TriePartitionUpdate build(TableMetadata metadata, Consumer<PartitionUpdate.SimpleBuilder> content)
    {
        PartitionUpdate.SimpleBuilder builder = PartitionUpdate.simpleBuilder(metadata, "key0");
        content.accept(builder);
        return TriePartitionUpdate.asTrieUpdate(builder.build());
    }

    private Mutation roundTrip(Mutation mutation) throws IOException
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            Mutation.serializer.serialize(mutation, out, VERSION);
            try (DataInputBuffer in = new DataInputBuffer(out.toByteArray()))
            {
                return Mutation.serializer.deserialize(in, VERSION, DeserializationHelper.Flag.FROM_REMOTE);
            }
        }
    }

    private TriePartitionUpdate counterUpdate()
    {
        PartitionUpdate.SimpleBuilder builder = PartitionUpdate.simpleBuilder(currentTableMetadata(), "key0");
        builder.timestamp(2000).row(1).add("cnt", CounterContext.instance().createUpdate(5L));
        return TriePartitionUpdate.asTrieUpdate(builder.build());
    }

    private static void setCounterValues(PartitionUpdate update)
    {
        for (PartitionUpdate.CounterMark mark : update.collectCounterMarks())
            mark.setValue(CounterContext.instance().createGlobal(CounterId.fromInt(1), 1234567L, 5L));
    }

    /**
     * Merging a row's liveness into an existing one adjusted the update's data size by assignment instead of by
     * increment, which drove it negative whenever the merged liveness was smaller than the one it replaced (an
     * expiring liveness overwritten by a newer non-expiring one), and the reader then rejected the update as corrupt.
     */
    @Test
    public void testLivenessMergeKeepsDataSize() throws Throwable
    {
        createRichTable();
        TableMetadata metadata = currentTableMetadata();
        DecoratedKey key = metadata.partitioner.decorateKey(ByteBufferUtil.bytes("key0"));
        ColumnMetadata v = metadata.getColumn(ByteBufferUtil.bytes("v"));
        Clustering<?> clustering = Clustering.make(ByteBufferUtil.bytes(1));

        PartitionUpdate.Builder merging = TriePartitionUpdate.FACTORY.builder(metadata, key, metadata.regularAndStaticColumns(), 2);
        Row.Builder first = BTreeRow.unsortedBuilder();
        first.newRow(clustering);
        first.addPrimaryKeyLivenessInfo(LivenessInfo.expiring(1000, 100, 1500));
        first.addCell(BufferCell.live(v, 1000, ByteBufferUtil.bytes(5)));
        merging.add(first.build());
        Row.Builder second = BTreeRow.unsortedBuilder();
        second.newRow(clustering);
        second.addPrimaryKeyLivenessInfo(LivenessInfo.create(2000, 1500));
        merging.add(second.build());
        TriePartitionUpdate merged = (TriePartitionUpdate) merging.build();

        // The same content added as a single row, so no merge happens.
        PartitionUpdate.Builder single = TriePartitionUpdate.FACTORY.builder(metadata, key, metadata.regularAndStaticColumns(), 1);
        Row.Builder row = BTreeRow.unsortedBuilder();
        row.newRow(clustering);
        row.addPrimaryKeyLivenessInfo(LivenessInfo.create(2000, 1500));
        row.addCell(BufferCell.live(v, 1000, ByteBufferUtil.bytes(5)));
        single.add(row.build());

        assertEquals(single.build().dataSize(), merged.dataSize());
        assertEquals(merged, deserialize(serialize(merged)));
    }

    /**
     * The reader walks the serialized trie in place, following pointers that run backwards from the
     * root. A payload that has lost bytes must be rejected rather than followed off the end.
     */
    @Test
    public void testTruncatedPayloadIsRejected() throws Throwable
    {
        createRichTable();
        byte[] bytes = serialize(richUpdate());

        for (int length = 0; length < bytes.length; ++length)
        {
            byte[] truncated = Arrays.copyOf(bytes, length);
            try
            {
                // Opening the trie does not touch it, so the update has to be consumed too.
                walk(deserialize(truncated));
                fail("Truncating to " + length + " of " + bytes.length + " bytes was accepted");
            }
            catch (IOException | RuntimeException expected)
            {
                // Any rejection will do; what must not happen is a silent accept, a hang or an
                // allocation sized from a corrupt length.
            }
        }
    }

    /**
     * Content lengths inside the trie are stored as vints read backwards from the node that carries
     * them, and are offsets back from that node. {@link org.apache.cassandra.db.tries.OnDiskCursor}
     * rejects one that reaches before the start of the payload; check that the rejection reaches a
     * caller of the serializer rather than being followed into a wild read or allocation.
     */
    @Test
    public void testCorruptTrieContentLengthIsRejected() throws Throwable
    {
        createRichTable();
        byte[] bytes = serialize(richUpdate());

        // The writer emits the root last, so the last byte of the trie block is the root's node
        // code and the one before it is the leading byte of its content-length vint. 0xFF makes
        // that a nine-byte vint whose value reaches far before the start of the payload.
        bytes[bytes.length - 2] = (byte) 0xFF;

        try
        {
            walk(deserialize(bytes));
            fail("A content length reaching before the start of the payload was accepted");
        }
        catch (UncheckedIOException e)
        {
            assertTrue(e.getMessage(), e.getMessage().contains("Corrupt serialized trie"));
        }
    }
}
