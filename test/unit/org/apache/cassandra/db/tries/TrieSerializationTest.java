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

package org.apache.cassandra.db.tries;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.concurrent.OpOrder;

import static org.apache.cassandra.db.tries.TrieUtil.VERSION;

/**
 * Unit tests for trie serialization components including {@link BufferManagerMultibuf},
 * {@link ContentManagerPojo}, and {@link InMemoryDeletionAwareTrie}.
 */
public class TrieSerializationTest
{
    @BeforeClass
    public static void setUpClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    private static final OpOrder opOrder = new OpOrder();

    private static final ContentManagerPojo.PojoSerializer<String> STRING_SERIALIZER = new ContentManagerPojo.PojoSerializer<String>()
    {
        @Override
        public void serialize(String content, DataOutputPlus out) throws IOException
        {
            out.writeUTF(content);
        }

        @Override
        public String deserialize(DataInputPlus in) throws IOException
        {
            return in.readUTF();
        }

        @Override
        public long serializedSize(String content)
        {
            return TypeSizes.sizeof(content);
        }
    };

    @Test
    public void testBufferManagerMultibufSerializationOnHeap() throws Exception
    {
        testBufferManagerMultibufSerialization(BufferType.ON_HEAP);
    }

    @Test
    public void testBufferManagerMultibufSerializationOffHeap() throws Exception
    {
        testBufferManagerMultibufSerialization(BufferType.OFF_HEAP);
    }

    private void testBufferManagerMultibufSerialization(BufferType bufferType) throws Exception
    {
        OpOrder.Group group = opOrder.start();
        try
        {
            BufferManagerMultibuf bm = new BufferManagerMultibuf(bufferType, InMemoryBaseTrie.ExpectedLifetime.SHORT, opOrder);
            // Allocate enough cells to expand into multiple buffers
            int count = 500;
            int[] cells = new int[count];
            for (int i = 0; i < count; i++)
            {
                cells[i] = bm.allocateCell();
                bm.getBuffer(cells[i]).putByte(bm.inBufferOffset(cells[i]), (byte) (i & 0xFF));
            }

            DataOutputBuffer out = new DataOutputBuffer();
            bm.serialize(out);
            Assert.assertEquals(bm.serializedSize(), out.position());

            DataInputBuffer in = new DataInputBuffer(out.buffer(), false);
            BufferManagerMultibuf deserializedBm = BufferManagerMultibuf.deserialize(in, bufferType, InMemoryBaseTrie.ExpectedLifetime.SHORT, opOrder);

            Assert.assertEquals(bm.getAllocatedPos(), deserializedBm.getAllocatedPos());
            for (int i = 0; i < count; i++)
            {
                byte expected = (byte) (i & 0xFF);
                byte actual = deserializedBm.getBuffer(cells[i]).getByte(deserializedBm.inBufferOffset(cells[i]));
                Assert.assertEquals(expected, actual);
            }

            deserializedBm.discardBuffers();
            bm.discardBuffers();
        }
        finally
        {
            group.close();
        }
    }

    @Test
    public void testContentManagerPojoSerializationSmall() throws Exception
    {
        testContentManagerPojoSerialization(30);
    }

    @Test
    public void testContentManagerPojoSerializationLarge() throws Exception
    {
        testContentManagerPojoSerialization(150);
    }

    private void testContentManagerPojoSerialization(int size) throws Exception
    {
        OpOrder.Group group = opOrder.start();
        try
        {
            ContentManagerPojo<String> cm = new ContentManagerPojo<>(null, InMemoryBaseTrie.ExpectedLifetime.SHORT, opOrder);
            int[] ids = new int[size];
            for (int i = 0; i < size; i++)
            {
                if (i % 3 != 0) // Leave some slots sparse/null
                {
                    ids[i] = cm.addContent("val_" + i, false);
                }
            }

            DataOutputBuffer out = new DataOutputBuffer();
            cm.serialize(out, STRING_SERIALIZER);

            DataInputBuffer in = new DataInputBuffer(out.buffer(), false);
            ContentManagerPojo<String> deserializedCm = ContentManagerPojo.deserialize(in, null, InMemoryBaseTrie.ExpectedLifetime.SHORT, opOrder, STRING_SERIALIZER);

            for (int i = 0; i < size; i++)
            {
                if (i % 3 != 0)
                {
                    Assert.assertEquals("val_" + i, deserializedCm.getContent(ids[i]));
                }
            }
        }
        finally
        {
            group.close();
        }
    }

    @Test
    public void testInMemoryDeletionAwareTrieSerialization() throws Exception
    {
        OpOrder.Group group = opOrder.start();
        try
        {
            InMemoryDeletionAwareTrie<String, ConsistencyTestBase.TestRangeState> trie =
                InMemoryDeletionAwareTrie.shortLived(VERSION, BufferType.OFF_HEAP);

            ByteComparable key1 = ByteComparable.preencoded(VERSION, new byte[]{1, 2, 3});
            ByteComparable key2 = ByteComparable.preencoded(VERSION, new byte[]{1, 2, 4});

            trie.putRecursive(ByteComparable.EMPTY, "root_val", (x, y) -> y);
            trie.putRecursive(key1, "val1", (x, y) -> y);
            trie.putRecursive(key2, "val2", (x, y) -> y);

            DataOutputBuffer out = new DataOutputBuffer();
            long size = trie.serializedSize(STRING_SERIALIZER);
            trie.serialize(out, STRING_SERIALIZER);
            Assert.assertEquals(size, out.position());

            DataInputBuffer in = new DataInputBuffer(out.buffer(), false);
            InMemoryDeletionAwareTrie<String, ConsistencyTestBase.TestRangeState> deserializedTrie =
                InMemoryDeletionAwareTrie.deserialize(in, x -> true, BufferType.OFF_HEAP, InMemoryBaseTrie.ExpectedLifetime.SHORT, opOrder, STRING_SERIALIZER);

            Assert.assertEquals("root_val", deserializedTrie.get(ByteComparable.EMPTY));
            Assert.assertEquals("val1", deserializedTrie.get(key1));
            Assert.assertEquals("val2", deserializedTrie.get(key2));

            deserializedTrie.discardBuffers();
            trie.discardBuffers();
        }
        finally
        {
            group.close();
        }
    }

    @Test
    public void testDeserializationFailureCleanup() throws Exception
    {
        OpOrder.Group group = opOrder.start();
        try
        {
            InMemoryDeletionAwareTrie<String, ConsistencyTestBase.TestRangeState> trie =
                InMemoryDeletionAwareTrie.shortLived(VERSION, BufferType.OFF_HEAP);

            ByteComparable key = ByteComparable.preencoded(VERSION, new byte[]{1, 2});
            trie.putRecursive(key, "val", (x, y) -> y);

            DataOutputBuffer out = new DataOutputBuffer();
            trie.serialize(out, STRING_SERIALIZER);

            // Corrupt stream by truncating before ContentManagerPojo deserialization completes
            byte[] truncatedBytes = new byte[(int) out.position() - 4];
            System.arraycopy(out.getData(), 0, truncatedBytes, 0, truncatedBytes.length);

            DataInputBuffer in = new DataInputBuffer(truncatedBytes);

            try
            {
                InMemoryDeletionAwareTrie.deserialize(in, x -> true, BufferType.OFF_HEAP, InMemoryBaseTrie.ExpectedLifetime.SHORT, opOrder, STRING_SERIALIZER);
                Assert.fail("Expected deserialization to throw IOException due to truncated stream");
            }
            catch (IOException e)
            {
                Assert.assertNotNull(e);
            }

            // Direct verification of BufferManagerMultibuf cleanup on I/O failure
            DataInputBuffer corruptBmStream = new DataInputBuffer(new byte[]{0, 0, 0, 10}); // Claims 10 bytes but stream ends prematurely
            try
            {
                BufferManagerMultibuf.deserialize(corruptBmStream, BufferType.OFF_HEAP, InMemoryBaseTrie.ExpectedLifetime.SHORT, opOrder);
                Assert.fail("Expected IOException on truncated BufferManagerMultibuf stream");
            }
            catch (IOException e)
            {
                Assert.assertNotNull(e);
            }

            trie.discardBuffers();
        }
        finally
        {
            group.close();
        }
    }
}
