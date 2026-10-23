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

/// Runs [IntersectedTailsTest] with the trie the range operations are applied to read back off disk.
///
/// The inherited cases are the ones the on-disk reader is least covered for: the read-back trie becomes an
/// operand of an intersection, of a range-apply cursor and of both forms of deletion-aware merge, and then ten
/// thousand tails are taken of each result at sampled prefixes of its keys. The read-back trie is a plain trie --
/// the merges see it through [DeletionAwareTrie#wrap], with no deletion branch -- so it reports no range state; the
/// range state those operations read comes from the in-memory range set. What this exercises is the plain reader
/// driven by those operations rather than by a walk of its own -- the intersection moves it past each range with
/// `skipTo` -- and its `tailCursor`, as the tails are taken rooted wherever the sampled prefix lands, including
/// inside a chain node.
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
    static class BufferSerDe implements OnDiskTrieWriter.DataSerializer<ByteBuffer>, OnDiskCursor.DataDeserializer<ByteBuffer>
    {
        @Override
        public void serialize(DataOutputPlus out, ByteBuffer value) throws IOException
        {
            out.write(value.duplicate());
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
}
