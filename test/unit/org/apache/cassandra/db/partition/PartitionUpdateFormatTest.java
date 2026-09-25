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
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.function.Consumer;
import java.util.function.Supplier;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.partitions.BTreePartitionUpdate;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.TriePartitionUpdate;
import org.apache.cassandra.db.partitions.TriePartitionUpdateSerializer;
import org.apache.cassandra.db.rows.DeserializationHelper;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIteratorSerializer;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * From {@link MessagingService#VERSION_DS_21} on {@link PartitionUpdate.PartitionUpdateSerializer} writes a
 * {@link TriePartitionUpdate} in the trie encoding and every other update in the BTree encoding, the format byte saying
 * which. These tests pin that rule down and, above all, that {@code serializedSize} describes the bytes
 * {@code serialize} then writes: the commit log reserves the region the first reports and the second fills it.
 */
public class PartitionUpdateFormatTest extends CQLTester
{
    private static final int VERSION = MessagingService.VERSION_DS_21;

    private static final byte BTREE_FORMAT = 0;
    private static final byte TRIE_FORMAT = 1;

    private void createRichTable()
    {
        createTable("CREATE TABLE %s (k text, c int, v int, m map<text, text>, " +
                    "s int static, ss set<text> static, PRIMARY KEY(k, c))");
    }

    /** A table of the shape a write workload of single-row inserts uses: one row per partition, one blob column. */
    private void createBlobTable()
    {
        createTable("CREATE TABLE %s (k text, c int, v blob, PRIMARY KEY(k, c))");
    }

    private TriePartitionUpdate singleRowBlobUpdate()
    {
        return build(builder -> {
            builder.timestamp(2000).nowInSec(1500);
            builder.row(1).add("v", ByteBuffer.allocate(100));
        });
    }

    /** A partition whose clustering keys share long prefixes, which the trie stores once and BTree repeats per row. */
    private void createSharedPrefixTable()
    {
        createTable("CREATE TABLE %s (k text, c1 text, c2 text, v int, PRIMARY KEY(k, c1, c2))");
    }

    private TriePartitionUpdate sharedPrefixUpdate()
    {
        return build(builder -> {
            builder.timestamp(2000).nowInSec(1500);
            for (int i = 0; i < 10; ++i)
                builder.row("a-common-long-clustering-prefix-" + (i / 5), "suffix-" + i).add("v", i);
        });
    }

    private TriePartitionUpdate richUpdate()
    {
        return build(builder -> {
            // Older than everything below, so nothing it covers is shadowed away.
            builder.timestamp(1000).nowInSec(1500).delete();

            builder.timestamp(2000).nowInSec(1500);
            builder.row().add("s", 7).add("ss", ImmutableSet.of("x", "y"));
            builder.row(1).add("v", 11).add("m", ImmutableMap.of("a", "1", "b", "2"));
            builder.row(3).add("v", 33);
            builder.addRangeTombstone().start(5).end(9).inclStart().exclEnd();
            builder.addRangeTombstone().start(20).end(30).exclStart().inclEnd();
        });
    }

    private TriePartitionUpdate build(Consumer<PartitionUpdate.SimpleBuilder> content)
    {
        PartitionUpdate.SimpleBuilder builder = PartitionUpdate.simpleBuilder(currentTableMetadata(), "key0");
        content.accept(builder);
        return TriePartitionUpdate.asTrieUpdate(builder.build());
    }

    /**
     * Every shape of trie-backed update has to be written in the trie encoding, and the size reported for it has to be
     * the number of bytes that then get written.
     */
    @Test
    public void testTrieEncodingIsWrittenForEveryShape() throws Throwable
    {
        createRichTable();
        TableMetadata metadata = currentTableMetadata();

        assertTrieEncodingIsWritten("empty update",
                                    () -> TriePartitionUpdate.asTrieUpdate(PartitionUpdate.emptyUpdate(metadata, metadata.partitioner.decorateKey(ByteBufferUtil.bytes("key0")))));

        assertTrieEncodingIsWritten("one row", () -> build(builder -> {
            builder.timestamp(2000).nowInSec(1500);
            builder.row(1).add("v", 11);
        }));

        assertTrieEncodingIsWritten("ten rows", () -> build(builder -> {
            builder.timestamp(2000).nowInSec(1500);
            for (int i = 0; i < 10; ++i)
                builder.row(i).add("v", i);
        }));

        assertTrieEncodingIsWritten("row with a ttl", () -> build(builder -> {
            builder.timestamp(2000).nowInSec(1500).ttl(60);
            builder.row(1).add("v", 11);
        }));

        assertTrieEncodingIsWritten("range tombstone", () -> build(builder -> {
            builder.timestamp(2000).nowInSec(1500);
            builder.row(1).add("v", 11);
            builder.addRangeTombstone().start(5).end(9).inclStart().exclEnd();
        }));

        assertTrieEncodingIsWritten("complex columns", () -> build(builder -> {
            builder.timestamp(2000).nowInSec(1500);
            builder.row().add("s", 7).add("ss", ImmutableSet.of("x", "y"));
            builder.row(1).add("m", ImmutableMap.of("a", "1", "b", "2"));
        }));

        assertTrieEncodingIsWritten("everything at once", this::richUpdate);

        createSharedPrefixTable();
        assertTrieEncodingIsWritten("shared clustering prefixes", this::sharedPrefixUpdate);

        createBlobTable();
        assertTrieEncodingIsWritten("single row with a 100 byte blob", this::singleRowBlobUpdate);
    }

    /**
     * The shape a write workload of single-row inserts produces, which is most of what a commit log holds. The trie's
     * nodes and pointers are overhead a partition of one row has nothing to amortise them over, so the BTree encoding
     * is the smaller one here, and a trie-backed update must still go out in the trie encoding: the encoding follows
     * the update's class, not its size.
     */
    @Test
    public void testSingleRowUpdateIsWrittenInTheTrieFormat() throws Throwable
    {
        createBlobTable();

        assertTrue("the fixture must be one the BTree encoding is smaller for, or it pins nothing",
                   btreeEncoded(singleRowBlobUpdate()).length < trieEncoded(singleRowBlobUpdate()).length);
        assertEquals(TRIE_FORMAT, assertTrieEncodingIsWritten("single row with a 100 byte blob", this::singleRowBlobUpdate));
    }

    /**
     * An update that is not trie-backed goes out in the BTree encoding, as it did before the trie encoding existed.
     */
    @Test
    public void testBTreeBackedUpdateIsWrittenInTheBTreeFormat() throws Throwable
    {
        createSharedPrefixTable();

        BTreePartitionUpdate update = BTreePartitionUpdate.asBTreeUpdate(sharedPrefixUpdate());
        long size = PartitionUpdate.serializer.serializedSize(update, VERSION);
        byte[] written = serialize(update);

        assertEquals("serializedSize must describe the bytes written", size, written.length);
        assertEquals(BTREE_FORMAT, formatOf(update, written));
        assertArrayEquals("body must be the BTree encoding",
                          btreeEncoded(update),
                          Arrays.copyOfRange(written, bodyOffset(update), written.length));
    }

    /**
     * Sizing an update memoizes its size on it and lays its trie out, so the state a write starts from depends on
     * whether a sizing ran first. The bytes must not: a write that runs on its own has to write the same format and
     * the same bytes as one that follows a sizing.
     */
    @Test
    public void testSerializingWithoutSizingFirstWritesTheSameBytes() throws Throwable
    {
        createRichTable();
        assertSameBytesWithAndWithoutSizingFirst("everything at once", this::richUpdate);
        assertSameBytesWithAndWithoutSizingFirst("one row", () -> build(builder -> {
            builder.timestamp(2000).nowInSec(1500);
            builder.row(1).add("v", 11);
        }));

        createSharedPrefixTable();
        assertSameBytesWithAndWithoutSizingFirst("shared clustering prefixes", this::sharedPrefixUpdate);

        createBlobTable();
        assertSameBytesWithAndWithoutSizingFirst("single row with a 100 byte blob", this::singleRowBlobUpdate);
    }

    /**
     * The BTree format is read back through the table's own partition update factory, so on a table that asks for a
     * trie memtable an update written in either format has to come back trie-backed. Were that not so, an update a
     * BTree-backed sender wrote would quietly hand the memtable a different kind of update.
     */
    @Test
    public void testTrieBackedTableReadsBothFormatsBackAsTrieUpdates() throws Throwable
    {
        createTable("CREATE TABLE %s (k text, c1 text, c2 text, v int, PRIMARY KEY(k, c1, c2)) WITH memtable = 'trie'");

        BTreePartitionUpdate singleRow = BTreePartitionUpdate.asBTreeUpdate(build(builder -> {
            builder.timestamp(2000).nowInSec(1500);
            builder.row("c", "r").add("v", 1);
        }));
        assertEquals(BTREE_FORMAT, formatOf(singleRow, serialize(singleRow)));
        assertTrue("an update written in the BTree format must read back trie-backed on a trie-backed table",
                   deserialize(serialize(singleRow)) instanceof TriePartitionUpdate);

        TriePartitionUpdate sharedPrefixes = sharedPrefixUpdate();
        assertEquals(TRIE_FORMAT, formatOf(sharedPrefixes, serialize(sharedPrefixes)));
        assertTrue(deserialize(serialize(sharedPrefixes)) instanceof TriePartitionUpdate);
    }

    /**
     * Size the update the way the mutation path does, write it, and check that the size describes the bytes written,
     * that the format byte is the trie encoding's, that the body is that encoding's bytes, and that it reads back.
     *
     * @return the format byte that was written
     */
    private byte assertTrieEncodingIsWritten(String shape, Supplier<TriePartitionUpdate> updates) throws IOException
    {
        // Measured on an update of its own: sizing one lays its trie out and memoizes, and the update that is
        // written below must be in the state a freshly built one is in.
        byte[] expectedBody = trieEncoded(updates.get());

        TriePartitionUpdate update = updates.get();
        long size;
        byte[] written;
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            size = PartitionUpdate.serializer.serializedSize(update, VERSION);
            PartitionUpdate.serializer.serialize(update, out, VERSION);
            written = out.toByteArray();
        }

        assertEquals(shape + ": serializedSize must describe the bytes written", size, written.length);
        assertEquals(shape + ": format byte", TRIE_FORMAT, formatOf(update, written));
        assertArrayEquals(shape + ": body must be the trie encoding",
                          expectedBody,
                          Arrays.copyOfRange(written, bodyOffset(update), written.length));

        assertEquals(shape + ": round trip", update, TriePartitionUpdate.asTrieUpdate(deserialize(written)));
        return formatOf(update, written);
    }

    private void assertSameBytesWithAndWithoutSizingFirst(String shape, Supplier<TriePartitionUpdate> updates) throws IOException
    {
        byte[] withoutSizing = serialize(updates.get());

        TriePartitionUpdate sizedFirst = updates.get();
        PartitionUpdate.serializer.serializedSize(sizedFirst, VERSION);
        assertArrayEquals(shape, withoutSizing, serialize(sizedFirst));

        // And a second write of the same update, which no longer has the layout the sizing retained.
        assertArrayEquals(shape, withoutSizing, serialize(sizedFirst));
    }

    /** The format byte, which the writer puts right after the table id. */
    private static byte formatOf(PartitionUpdate update, byte[] written)
    {
        return written[(int) update.metadata().id.serializedSize()];
    }

    /** The offset of the encoded update itself, past the table id and the format byte. */
    private static int bodyOffset(PartitionUpdate update)
    {
        return (int) update.metadata().id.serializedSize() + 1;
    }

    private static byte[] serialize(PartitionUpdate update) throws IOException
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            PartitionUpdate.serializer.serialize(update, out, VERSION);
            return out.toByteArray();
        }
    }

    private static PartitionUpdate deserialize(byte[] written) throws IOException
    {
        try (DataInputBuffer in = new DataInputBuffer(written))
        {
            return PartitionUpdate.serializer.deserialize(in, VERSION, DeserializationHelper.Flag.LOCAL);
        }
    }

    /** The update's body in the trie encoding, written out rather than sized, so that the sizes are not their own check. */
    private static byte[] trieEncoded(TriePartitionUpdate update) throws IOException
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            TriePartitionUpdateSerializer.serialize(update, out, VERSION);
            return out.toByteArray();
        }
    }

    /** The update's body in the BTree encoding, likewise. */
    private static byte[] btreeEncoded(PartitionUpdate update) throws IOException
    {
        try (DataOutputBuffer out = new DataOutputBuffer();
             UnfilteredRowIterator iter = update.unfilteredIterator())
        {
            UnfilteredRowIteratorSerializer.serializer.serialize(iter, null, out, VERSION, update.rowCount());
            return out.toByteArray();
        }
    }
}
