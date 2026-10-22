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
import java.util.Collections;
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
import com.google.common.primitives.Bytes;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.CounterMutation;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.partitions.BTreePartitionUpdate;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.PartitionUpdateSizeCache;
import org.apache.cassandra.db.partitions.TrieBackedPartition;
import org.apache.cassandra.db.partitions.TriePartitionUpdate;
import org.apache.cassandra.db.partitions.TriePartitionUpdateSerializer;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.DeserializationHelper;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.TrieBackedRow;
import org.apache.cassandra.db.rows.TrieTombstoneMarker;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.tries.DeletionAwareTrie;
import org.apache.cassandra.db.tries.InMemoryDeletionAwareTrie;
import org.apache.cassandra.db.tries.OnDiskDeletionAwareTrie;
import org.apache.cassandra.db.tries.RangeTrie;
import org.apache.cassandra.db.tries.TrieSpaceExhaustedException;
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
import org.apache.cassandra.utils.Hex;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
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
     * Every other test here compares the encoding with itself: sizes with bytes written, and what is read with what
     * was written. A change that alters the writer and the reader alike passes all of them, and still leaves commit
     * logs already written unreadable. These are the bytes {@link MessagingService#VERSION_DS_21} writes for each shape
     * an update can take, and they must not change. A counter is pinned as the update a client write makes, as the
     * context its leader replaces that with carries the leader's host id.
     */
    @Test
    public void testWireBytesArePinned() throws Throwable
    {
        createRichTable();

        assertWireBytes("rich update", viaBTree(richUpdate()),
                        "046b657930030380fdfffffadfb5522583e8ffffffffffa9ff6bdc00020173027373020176016d000100018160070183"
                        + "e800010100000000000c604007010000000183e8000100000c6040070183e800010100000000000c2040070100000001"
                        + "83e8000100000c2040070183e700030100000000000c07010000000183e7000300000cf6014007010000000100000000"
                        + "010b07010000000100000000010bf73840655647381e140905018c00008042070183e700030100000000000c07010000"
                        + "000183e7000300000cf6014007010000000100000000010b07010000000100000000010bf73640188007010000000000"
                        + "0008070001000000000008f703000083e800000021090040040083e804f5384003020083e80162013209380041030200"
                        + "83e801610131093800410d62618040400501f503000083e80000000b090a010080040083e804f5384039030180000080"
                        + "4203030483e801790738004103030483e80178073800410b79788040400501f503010083e800000007090a0100800400"
                        + "83e804f5334018808c010601f7");
        assertWireBytes("row deletion only", viaBTree(build(builder -> builder.timestamp(1999).nowInSec(1500).row(1).delete())),
                        "046b657930010119fffffadfb5522587cfffffffffffa9ff6bdc0000001f070100000200000108070001000002000108"
                        + "f63801000080404500010601f6");
        assertWireBytes("expiring row", viaBTree(build(builder -> builder.timestamp(2000).nowInSec(1500).ttl(60).row(1).add("v", 11))),
                        "046b65793001003dfffffadfb5522587d0ffffffffffa9ff6c183c00010176001f0300020000000000000b0a00400401"
                        + "00000005f538010000804440400601f5");
        assertWireBytes("row without primary key liveness",
                        viaBTree(build(builder -> builder.timestamp(2000).nowInSec(1500).row(1).noPrimaryKeyLivenessInfo().add("v", 11))),
                        "046b657930010031fffffadfb5522587d00000000101760019030000000000000b0800400801f5380100008044404006"
                        + "01f5");
        assertWireBytes("deletions only", viaBTree(build(builder -> {
                            builder.timestamp(1000).nowInSec(1500).delete();
                            builder.timestamp(2000).nowInSec(1500);
                            builder.addRangeTombstone().start(5).end(9).inclStart().exclEnd();
                        })),
                        "046b657930000200fffffadfb5522583e8ffffffffffa9ff6bdc00000040070183e800010100000000000c2040070100"
                        + "00000183e8000100000c20400f090580000080424040070100000000000008070001000000000008f700010601f6");
        assertWireBytes("point deletion inside a range tombstone", pointDeletionInsideRange(),
                        "046b657930000200fffffadfb5522587cfffffffffffa9ff6bdc00020173027373020176016d00010001370701000001"
                        + "00000008604007010000010100000101010002000e384007000100000100000820401c0b090605840000804043000106"
                        + "01f6");
        // The trie keys a column by its index among the table's columns, and a cell names its column by its index
        // among the header's: 1 and 0 here, for the static column and the regular one alike.
        assertWireBytes("second static and second regular column only",
                        viaBTree(build(builder -> {
                            builder.timestamp(2000).nowInSec(1500);
                            builder.row().add("ss", ImmutableSet.of("x"));
                            builder.row(1).add("m", ImmutableMap.of("a", "1"));
                        })),
                        "046b657930020071fffffadfb5522587cfffffffffffa9ff6bdc000102737301016d0101808d07010000030000000807"
                        + "0001000003000008f60140070000000105070000000105f7380100008044070100000300000008070001000003000008"
                        + "f60140070000000105070000000105f7224018800300000101610131083800614240400501f5014004000103f5380100"
                        + "008044030104010178063800784240400501f5014004000103f5174018803a010601f7");
        assertWireBytes("cell tombstone", viaBTree(build(builder -> builder.timestamp(2000).nowInSec(1500).row(1).delete("v"))),
                        "046b65793001002dfffffadfb5522587d0ffffffffffa9ff6bdc00000101760018030005000005004004000003f53801"
                        + "0000804440400601f5");
        assertWireBytes("map element deletion",
                        viaBTree(build(builder -> builder.timestamp(2000).nowInSec(1500).row(1).appendAll("m", Collections.singletonMap("a", null)))),
                        "046b65793001003afffffadfb5522587d0ffffffffffa9ff6bdc000001016d0123030005000001610738006142404005"
                        + "01f5014004000003f538010000804440400601f5");

        // Through the entry point the mutation path uses: the table id, then the format byte of the trie encoding.
        TriePartitionUpdate update = richUpdate();
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            PartitionUpdate.serializer.serialize(update, out, VERSION);
            byte[] written = out.toByteArray();
            int idLength = update.metadata().id.serializedSize();
            try (DataOutputBuffer id = new DataOutputBuffer())
            {
                update.metadata().id.serialize(id);
                assertArrayEquals(id.toByteArray(), Arrays.copyOf(written, idLength));
            }
            assertEquals(1, written[idLength]);
            assertArrayEquals(serialize(update), Arrays.copyOfRange(written, idLength + 1, written.length));
        }

        createTable("CREATE TABLE %s (k text, c int, cnt counter, PRIMARY KEY(k, c))");
        assertWireBytes("counter update",
                        viaBTree(build(builder -> builder.timestamp(2000).nowInSec(1500).row(1).add("cnt", CounterContext.instance().createUpdate(5L)))),
                        "046b657930010051fffffadfb5522587d00000000103636e74003c0300000024000100000000000000000000c0000000"
                        + "000000000000000000000001000000000000000529004004000003f538010000804440400601f5");
    }

    /**
     * The update as converted from a BTree update. The row and tombstone counts and the data size an update carries
     * depend on how it was built, and a simple builder builds with the table's partition update factory, which the
     * storage compatibility mode chooses. Converting from the same BTree update gives the same update in every mode.
     */
    private static TriePartitionUpdate viaBTree(TriePartitionUpdate update)
    {
        return TriePartitionUpdate.asTrieUpdate(BTreePartitionUpdate.asBTreeUpdate(update));
    }

    /**
     * A row deleted inside a range tombstone, the row deletion being a point deletion (the form the stage 3 trie
     * partitions give one), so that the marker carries the range's deletion on both of its sides.
     */
    private TriePartitionUpdate pointDeletionInsideRange() throws TrieSpaceExhaustedException
    {
        TableMetadata metadata = currentTableMetadata();
        InMemoryDeletionAwareTrie<Object, TrieTombstoneMarker> rowDeletion = TrieBackedRow.newTrie();
        rowDeletion.mutator((Object existing, Object update) -> update,
                            (TrieTombstoneMarker existing, TrieTombstoneMarker update) -> update,
                            (Object existing, TrieTombstoneMarker update) -> existing,
                            (TrieTombstoneMarker deletion, Object update) -> update,
                            true,
                            features -> false)
                   .apply(DeletionAwareTrie.deletionBranch(ByteComparable.EMPTY,
                                                           TrieBackedPartition.BYTE_COMPARABLE_VERSION,
                                                           RangeTrie.point(ByteComparable.EMPTY,
                                                                           TrieBackedPartition.BYTE_COMPARABLE_VERSION,
                                                                           true,
                                                                           TrieTombstoneMarker.point(DeletionTime.build(2000, 1500),
                                                                                                     TrieTombstoneMarker.Kind.ROW))));

        PartitionUpdate.Builder builder = TriePartitionUpdate.FACTORY.builder(metadata,
                                                                              metadata.partitioner.decorateKey(ByteBufferUtil.bytes("key0")),
                                                                              metadata.regularAndStaticColumns(),
                                                                              1);
        builder.add(new RangeTombstone(Slice.make(Clustering.make(ByteBufferUtil.bytes(5)), Clustering.make(ByteBufferUtil.bytes(9))),
                                       DeletionTime.build(1999, 1500)));
        builder.add(TrieBackedRow.create(metadata, Clustering.make(ByteBufferUtil.bytes(6)), rowDeletion));
        return (TriePartitionUpdate) builder.build();
    }

    private void assertWireBytes(String shape, TriePartitionUpdate update, String expectedHex) throws IOException
    {
        byte[] bytes = serialize(update);
        assertEquals(shape, expectedHex, Hex.bytesToHex(bytes));
        assertEquals(shape, update, deserialize(bytes));
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

        assertTrue(PartitionUpdateSizeCache.isSized(update, MessagingService.VERSION_DS_21));
        assertTrue(PartitionUpdateSizeCache.hasRetainedTrie(update));
        assertEquals(heapSize + ObjectSizes.sizeOnHeapOf(PartitionUpdateSizeCache.retainedTrie(update)), update.unsharedHeapSize());

        PartitionUpdateSizeCache.invalidate(update);

        assertFalse(PartitionUpdateSizeCache.isSized(update, MessagingService.VERSION_DS_21));
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
     * Data written before a column was dropped must not come back if the column is added again: the reader drops
     * the cells of a dropped column that are not newer than the drop, as the BTree encoding's reader does.
     */
    @Test
    public void testCellsOfDroppedAndReAddedColumnAreDropped() throws Throwable
    {
        createTable("CREATE TABLE %s (k text, c int, v int, w int, PRIMARY KEY(k, c))");
        TriePartitionUpdate update = build(builder -> {
            builder.timestamp(10);
            builder.row(1).add("v", 7).add("w", 8);
            builder.row(2).add("v", 9);
            builder.row(3).noPrimaryKeyLivenessInfo().add("v", 10);
        });
        byte[] trieBytes = serialize(update);
        byte[] btreeBytes;
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            PartitionUpdate.serializer.serialize(BTreePartitionUpdate.asBTreeUpdate(update), out, MessagingService.VERSION_DS_20);
            btreeBytes = out.toByteArray();
        }

        alterTable("ALTER TABLE %s DROP v");
        alterTable("ALTER TABLE %s ADD v int");

        PartitionUpdate fromBTree;
        try (DataInputBuffer in = new DataInputBuffer(btreeBytes))
        {
            fromBTree = PartitionUpdate.serializer.deserialize(in, MessagingService.VERSION_DS_20, DeserializationHelper.Flag.FROM_REMOTE);
        }
        TriePartitionUpdate fromTrie = deserialize(trieBytes);

        assertEquals(TriePartitionUpdate.asTrieUpdate(fromBTree), fromTrie);
        assertEquals(2, Iterators.size(fromTrie.rowIterator()));
        for (Iterator<Row> rows = fromTrie.rowIterator(); rows.hasNext();)
            assertNull(rows.next().getCell(currentTableMetadata().getColumn(ByteBufferUtil.bytes("v"))));
    }

    /**
     * As {@link #testCellsOfDroppedAndReAddedColumnAreDropped}, for a complex column, whose complex deletions not newer
     * than the drop must not come back either. The rows left with nothing are dropped, and the counts the update
     * carries must not include them.
     */
    @Test
    public void testComplexDeletionsOfDroppedAndReAddedColumnAreDropped() throws Throwable
    {
        createTable("CREATE TABLE %s (k text, c int, v int, m map<text, text>, PRIMARY KEY(k, c))");
        TriePartitionUpdate update = build(builder -> {
            builder.timestamp(10).nowInSec(1500);
            builder.row(1).add("v", 7).add("m", ImmutableMap.of("a", "1"));
            builder.row(2).add("v", 9).delete("m");
            builder.row(3).noPrimaryKeyLivenessInfo().add("m", ImmutableMap.of("b", "2"));
            builder.row(4).noPrimaryKeyLivenessInfo().delete("m");
        });
        byte[] trieBytes = serialize(update);
        byte[] btreeBytes;
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            PartitionUpdate.serializer.serialize(BTreePartitionUpdate.asBTreeUpdate(update), out, MessagingService.VERSION_DS_20);
            btreeBytes = out.toByteArray();
        }

        alterTable("ALTER TABLE %s DROP m");
        alterTable("ALTER TABLE %s ADD m map<text, text>");

        PartitionUpdate fromBTree;
        try (DataInputBuffer in = new DataInputBuffer(btreeBytes))
        {
            fromBTree = PartitionUpdate.serializer.deserialize(in, MessagingService.VERSION_DS_20, DeserializationHelper.Flag.FROM_REMOTE);
        }
        TriePartitionUpdate fromTrie = deserialize(trieBytes);

        assertEquals(TriePartitionUpdate.asTrieUpdate(fromBTree), fromTrie);
        assertEquals(2, Iterators.size(fromTrie.rowIterator()));
        ColumnMetadata m = currentTableMetadata().getColumn(ByteBufferUtil.bytes("m"));
        for (Iterator<Row> rows = fromTrie.rowIterator(); rows.hasNext();)
            assertNull(rows.next().getComplexColumnData(m));

        // What a sender that never had the dropped data would have sent.
        TriePartitionUpdate expected = build(builder -> {
            builder.timestamp(10).nowInSec(1500);
            builder.row(1).add("v", 7);
            builder.row(2).add("v", 9);
        });
        assertEquals(expected, fromTrie);
        assertEquals(expected.rowCount(), fromTrie.rowCount());
        assertEquals(expected.affectedRowCount(), fromTrie.affectedRowCount());
        // The data size is an estimate, and how much it counts for the same content depends on how the update was
        // built, so only check that the dropped data is not counted.
        assertTrue(fromTrie.dataSize() < update.dataSize());
    }

    /**
     * A column dropped and added again keeps its drop on record, but an update all of whose timestamps are newer than
     * the drop has nothing of it to leave out, and is read in place rather than rebuilt.
     */
    @Test
    public void testUpdateNewerThanDropOfReAddedColumnIsReadInPlace() throws Throwable
    {
        long dropTime = FBUtilities.timestampMicros();
        createDroppedAndReAddedTable(dropTime);
        TriePartitionUpdate update = build(builder -> {
            builder.timestamp(dropTime + 1).nowInSec(1500);
            builder.row().add("s", 7);
            builder.row(1).add("v", 11);
            builder.row(2).noPrimaryKeyLivenessInfo().add("v", 22);
            builder.row(3).delete();
        });

        TriePartitionUpdate read = deserialize(serialize(update));

        assertTrue(isReadInPlace(read));
        assertEquals(update, read);
        assertEquals(update.staticRow(), read.staticRow());
        assertEquals(update.rowCount(), read.rowCount());
        assertEquals(update.operationCount(), read.operationCount());
    }

    /**
     * An update with data both at the drop of a column dropped and added again and after it is rebuilt, leaving out
     * the data at the drop, as the BTree encoding's reader does.
     */
    @Test
    public void testUpdateAtDropOfReAddedColumnIsRebuilt() throws Throwable
    {
        long dropTime = FBUtilities.timestampMicros();
        createDroppedAndReAddedTable(dropTime);
        TriePartitionUpdate update = build(builder -> {
            builder.timestamp(dropTime).nowInSec(1500);
            builder.row().add("s", 7);
            builder.row(1).add("v", 11);
            builder.timestamp(dropTime + 1);
            builder.row(2).add("v", 22);
        });

        TriePartitionUpdate fromBTree = readViaBTreeEncoding(update);
        TriePartitionUpdate read = deserialize(serialize(update));

        assertFalse(isReadInPlace(read));
        assertEquals(fromBTree, read);
        assertEquals(fromBTree.staticRow(), read.staticRow());
        assertNull(read.staticRow().getCell(currentTableMetadata().getColumn(ByteBufferUtil.bytes("s"))));
        ColumnMetadata v = currentTableMetadata().getColumn(ByteBufferUtil.bytes("v"));
        assertNull(read.getRow(Clustering.make(ByteBufferUtil.bytes(1))).getCell(v));
        assertEquals(ByteBufferUtil.bytes(22), read.getRow(Clustering.make(ByteBufferUtil.bytes(2))).getCell(v).buffer());
        assertEquals(fromBTree.rowCount(), read.rowCount());
        assertEquals(fromBTree.operationCount(), read.operationCount());
    }

    /**
     * Writing a whole collection deletes what was there one microsecond before the write, so an update newer than the
     * drop of a collection column added again can still hold a complex deletion at the drop. Its stats' minimum
     * timestamp is that of the deletion, so it is rebuilt, and the deletion is left out as the BTree encoding's reader
     * leaves it out.
     */
    @Test
    public void testComplexDeletionAtDropOfReAddedColumnIsLeftOut() throws Throwable
    {
        long dropTime = FBUtilities.timestampMicros();
        createTable("CREATE TABLE %s (k text, c int, m map<text, text>, PRIMARY KEY(k, c))");
        alterTable("ALTER TABLE %s DROP m USING TIMESTAMP " + dropTime);
        alterTable("ALTER TABLE %s ADD m map<text, text>");
        TriePartitionUpdate update = build(builder -> builder.timestamp(dropTime + 1).nowInSec(1500)
                                                             .row(1).add("m", ImmutableMap.of("a", "1")));
        assertEquals(dropTime, update.stats().minTimestamp);

        TriePartitionUpdate fromBTree = readViaBTreeEncoding(update);
        TriePartitionUpdate read = deserialize(serialize(update));

        assertFalse(isReadInPlace(read));
        assertEquals(fromBTree, read);
        ColumnMetadata m = currentTableMetadata().getColumn(ByteBufferUtil.bytes("m"));
        ComplexColumnData data = read.getRow(Clustering.make(ByteBufferUtil.bytes(1))).getComplexColumnData(m);
        assertTrue(data.complexDeletion().isLive());
        assertEquals(1, data.cellsCount());
    }

    /**
     * Stats that saw no timestamp carry TIMESTAMP_EPOCH as their minimum timestamp, so that value bounds nothing, and
     * an update whose stats carry it is rebuilt even when the drop is older still.
     */
    @Test
    public void testUpdateWithEpochMinTimestampIsRebuilt() throws Throwable
    {
        createDroppedAndReAddedTable(EncodingStats.TIMESTAMP_EPOCH - 1);
        TriePartitionUpdate update = build(builder -> builder.timestamp(EncodingStats.TIMESTAMP_EPOCH).row(1).add("v", 11));
        assertEquals(EncodingStats.TIMESTAMP_EPOCH, update.stats().minTimestamp);

        TriePartitionUpdate read = deserialize(serialize(update));

        assertFalse(isReadInPlace(read));
        assertEquals(readViaBTreeEncoding(update), read);
        assertEquals(update, read);
    }

    /**
     * The update is checked against the latest drop of its columns: with timestamps newer than the drop of one column
     * but not of another dropped later, it is rebuilt, and the cells of the column dropped later are left out. The
     * column iterated first, v, is the one dropped later, so taking the last drop seen rather than the latest fails.
     */
    @Test
    public void testUpdateBetweenDropsOfReAddedColumnsIsRebuilt() throws Throwable
    {
        long firstDropTime = FBUtilities.timestampMicros();
        long secondDropTime = firstDropTime + 10;
        createTable("CREATE TABLE %s (k text, c int, v int, w int, PRIMARY KEY(k, c))");
        alterTable("ALTER TABLE %s DROP w USING TIMESTAMP " + firstDropTime);
        alterTable("ALTER TABLE %s ADD w int");
        alterTable("ALTER TABLE %s DROP v USING TIMESTAMP " + secondDropTime);
        alterTable("ALTER TABLE %s ADD v int");
        TriePartitionUpdate update = build(builder -> builder.timestamp(firstDropTime + 1).nowInSec(1500)
                                                             .row(1).add("v", 11).add("w", 12));

        TriePartitionUpdate fromBTree = readViaBTreeEncoding(update);
        TriePartitionUpdate read = deserialize(serialize(update));

        assertFalse(isReadInPlace(read));
        assertEquals(fromBTree, read);
        Row row = read.getRow(Clustering.make(ByteBufferUtil.bytes(1)));
        assertNull(row.getCell(currentTableMetadata().getColumn(ByteBufferUtil.bytes("v"))));
        assertEquals(ByteBufferUtil.bytes(12), row.getCell(currentTableMetadata().getColumn(ByteBufferUtil.bytes("w"))).buffer());
    }

    /**
     * An update made from a BTree update built without stats carries NO_STATS whatever its content, so its minimum
     * timestamp bounds nothing even when the drop is below it, and the update is rebuilt to leave out the cell not
     * newer than the drop.
     */
    @Test
    public void testUpdateWithNoStatsIsRebuilt() throws Throwable
    {
        long dropTime = EncodingStats.TIMESTAMP_EPOCH - 10;
        createDroppedAndReAddedTable(dropTime);
        TableMetadata metadata = currentTableMetadata();
        Row.Builder row = BTreeRow.unsortedBuilder();
        row.newRow(Clustering.make(ByteBufferUtil.bytes(1)));
        row.addPrimaryKeyLivenessInfo(LivenessInfo.create(dropTime, 1500));
        row.addCell(BufferCell.live(metadata.getColumn(ByteBufferUtil.bytes("v")), dropTime, ByteBufferUtil.bytes(11)));
        TriePartitionUpdate update = TriePartitionUpdate.asTrieUpdate(BTreePartitionUpdate.singleRowUpdate(metadata, ByteBufferUtil.bytes("key0"), row.build()));
        assertEquals(EncodingStats.NO_STATS, update.stats());

        TriePartitionUpdate fromBTree = readViaBTreeEncoding(update);
        TriePartitionUpdate read = deserialize(serialize(update));

        assertFalse(isReadInPlace(read));
        assertEquals(fromBTree, read);
        assertNull(read.getRow(Clustering.make(ByteBufferUtil.bytes(1))).getCell(metadata.getColumn(ByteBufferUtil.bytes("v"))));
    }

    /** A table whose regular column v and static column s were both dropped at the given time and added again. */
    private void createDroppedAndReAddedTable(long dropTime)
    {
        createTable("CREATE TABLE %s (k text, c int, v int, s int static, PRIMARY KEY(k, c))");
        alterTable("ALTER TABLE %s DROP (v, s) USING TIMESTAMP " + dropTime);
        alterTable("ALTER TABLE %s ADD (v int, s int static)");
    }

    /** Whether the update reads the bytes it was deserialized from in place, rather than having been rebuilt in memory. */
    private static boolean isReadInPlace(TriePartitionUpdate update)
    {
        return TriePartitionUpdate.asMergableTrieWithoutPartitionPrefix(update) instanceof OnDiskDeletionAwareTrie;
    }

    /** Write the update in the BTree encoding and read it back, as a node on the version before the trie encoding would. */
    private static TriePartitionUpdate readViaBTreeEncoding(TriePartitionUpdate update) throws IOException
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            PartitionUpdate.serializer.serialize(BTreePartitionUpdate.asBTreeUpdate(update), out, MessagingService.VERSION_DS_20);
            try (DataInputBuffer in = new DataInputBuffer(out.toByteArray()))
            {
                return TriePartitionUpdate.asTrieUpdate(PartitionUpdate.serializer.deserialize(in, MessagingService.VERSION_DS_20, DeserializationHelper.Flag.FROM_REMOTE));
            }
        }
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

    /**
     * A deletion marker never holds a live deletion, which is what a deletion time whose timestamp is
     * {@link Long#MIN_VALUE} reads back as.
     */
    @Test
    public void testLiveDeletionTimeInMarkerIsRejected() throws Throwable
    {
        createRichTable();
        // Timestamps are written as deltas from the update's smallest one, which is the deletion's own here.
        TriePartitionUpdate update = viaBTree(build(builder -> builder.timestamp(Long.MAX_VALUE).nowInSec(1500).row(1).delete()));
        assertEquals(Long.MAX_VALUE, update.stats().minTimestamp);
        byte[] bytes = serialize(update);

        // The first marker: its tag, the presence of its left side, and the delta of that side's timestamp. Raising
        // the delta by one wraps the timestamp round to Long.MIN_VALUE.
        int marker = trieStart(update, bytes);
        assertArrayEquals(new byte[]{ 7, 1, 0 }, Arrays.copyOfRange(bytes, marker, marker + 3));
        bytes[marker + 2] = 1;

        assertCorruptionRejected(bytes, "Live deletion time");
    }

    /**
     * A deletion marker with the same deletion on both sides and neither a point deletion nor a level marker changes
     * nothing, and the writer never writes one.
     */
    @Test
    public void testMarkerChangingNothingIsRejected() throws Throwable
    {
        createRichTable();
        byte[] bytes = serialize(pointDeletionInsideRange());

        // The row deletion's marker: the range's deletion on its left and right, the row's as its point deletion, and
        // no level marker. Dropping the point deletion leaves the bytes of its deletion time unread.
        byte[] pointMarker = { 7, 1, 0, 0, 1, 1, 0, 0, 1, 1, 1, 0, 2, 0 };
        int marker = Bytes.indexOf(bytes, pointMarker);
        assertTrue(marker > 0);
        bytes[marker + 9] = 0;
        bytes[marker + 10] = 0;

        assertCorruptionRejected(bytes, "nothing else");
    }

    /**
     * Every payload is read to the end of the slot the trie gives it and no further; one that is not has bytes that
     * are not what the writer wrote.
     */
    @Test
    public void testPayloadNotReadToItsEndIsRejected() throws Throwable
    {
        createRichTable();
        TriePartitionUpdate update = viaBTree(build(builder -> builder.timestamp(1999).nowInSec(1500).row(1).delete()));
        byte[] bytes = serialize(update);

        // The row deletion's first marker: present on its left with a deletion time and kind, absent on its right,
        // no point deletion, and a row level marker. Marking the left side absent has the rest of it read as the right
        // side, the point deletion and the level marker, three bytes short of its end.
        int marker = trieStart(update, bytes);
        assertArrayEquals(new byte[]{ 7, 1, 0, 0, 2, 0, 0, 1 }, Arrays.copyOfRange(bytes, marker, marker + 8));
        bytes[marker + 1] = 0;

        assertCorruptionRejected(bytes, "Read 5 bytes of a payload of 8");
    }

    /** Where the trie starts in the bytes written for the update, the trie being written last. */
    private static int trieStart(TriePartitionUpdate update, byte[] bytes)
    {
        TriePartitionUpdateSerializer.serializedSize(update, VERSION);
        return bytes.length - PartitionUpdateSizeCache.retainedTrie(update).remaining();
    }

    private void assertCorruptionRejected(byte[] bytes, String expectedMessage) throws IOException
    {
        try
        {
            walk(deserialize(bytes));
            fail("Corrupt bytes were accepted");
        }
        catch (UncheckedIOException e)
        {
            assertTrue(e.getMessage(), e.getMessage().contains(expectedMessage));
        }
    }
}
