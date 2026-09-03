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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;

import org.junit.After;
import org.junit.BeforeClass;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.ByteBufferUtil;

/// Runs [IntersectedTailsTest] with the trie the range operations are applied to read back off disk.
///
/// The inherited cases are the ones this branch's on-disk reader is least covered for: the read-back trie becomes an
/// operand of an intersection, of a range-apply cursor and of both forms of deletion-aware merge, and then ten
/// thousand tails are taken of each result at every prefix length of a key. All four ask the source for the range
/// state active at a position rather than only walking it, which is the pair of calls the round-trip comparisons
/// never make, and the tails are rooted wherever the sampled prefix lands -- including inside a chain node.
public class OnDiskIntersectedTailsTest extends IntersectedTailsTest
{
    @BeforeClass
    public static void setUpOnDisk()
    {
        // The on-disk readers pull in BufferPools, whose static initializer needs configuration.
        DatabaseDescriptor.toolInitialization();
    }

    private final List<OnDiskTrie<ByteBuffer>> opened = new ArrayList<>();

    @After
    public void closeOpened()
    {
        opened.forEach(OnDiskTrie::close);
        opened.clear();
    }

    @Override
    public void testIntersectedTails(BiFunction<Trie<ByteBuffer>, TrieSet, Trie<ByteBuffer>> deleter)
    {
        super.testIntersectedTails((trie, set) -> deleter.apply(readBack(trie), set));
    }

    private Trie<ByteBuffer> readBack(Trie<ByteBuffer> trie)
    {
        // The base class builds its trie with the ordered strategy, so it has to be written and read as ordered too.
        OnDiskTrie<ByteBuffer> read = TrieUtil.onDiskRoundtrip(trie, true, BUFFER_SERDE, BUFFER_SERDE);
        opened.add(read);
        return read;
    }

    /// The base class's content is the four bytes of a key's hash, so the value is written as it stands.
    static class BufferSerDe implements FileWriter.DataSerializer<ByteBuffer>, OnDiskCursor.DataDeserializer<ByteBuffer>
    {
        @Override
        public int serializedSize(ByteBuffer value)
        {
            return value.remaining();
        }

        @Override
        public int serialize(DataOutputPlus out, ByteBuffer value) throws IOException
        {
            out.write(value.duplicate());
            return value.remaining();
        }

        @Override
        public ByteBuffer deserialize(DataInputPlus rdr, int length) throws IOException
        {
            byte[] bytes = new byte[length];
            rdr.readFully(bytes);
            return ByteBuffer.wrap(bytes);
        }
    }

    static final BufferSerDe BUFFER_SERDE = new BufferSerDe();

    static
    {
        // Guard against the base class changing to a content type this serializer silently truncates.
        assert ByteBufferUtil.bytes(1).remaining() == 4;
    }
}
