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
import java.util.Arrays;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.TriePartitionUpdate;
import org.apache.cassandra.db.partitions.TriePartitionUpdateSerializer;
import org.apache.cassandra.db.rows.DeserializationHelper;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            TriePartitionUpdateSerializer.serialize(update, out, VERSION);
            return out.toByteArray();
        }
    }

    private TriePartitionUpdate deserialize(byte[] bytes) throws IOException
    {
        try (DataInputBuffer in = new DataInputBuffer(bytes))
        {
            return TriePartitionUpdateSerializer.deserialize(in, VERSION, DeserializationHelper.Flag.LOCAL, currentTableMetadata());
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
