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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static org.apache.cassandra.db.tries.TrieUtil.VERSION;
import static org.junit.Assert.assertEquals;

/// Walked in reverse, an ordered trie presents its root's content on the root's return path, at depth 0. A walk that
/// tracks the path must bring it back to the root's there, which has no transition to add: the path is empty.
public class RootReturnPathWalkTest
{
    private static InMemoryTrie<String> orderedTrie(String... keys) throws TrieSpaceExhaustedException
    {
        InMemoryTrie<String> trie = InMemoryTrie.shortLivedOrdered(VERSION);
        for (String key : keys)
            trie.putRecursive(ByteComparable.preencoded(VERSION, key.getBytes(StandardCharsets.UTF_8)), key, (x, y) -> y);
        return trie;
    }

    /// The skip past the branch of "a" lands on the root's return path.
    @Test
    public void testProcessSkippingBranches() throws TrieSpaceExhaustedException
    {
        List<String> entries = new ArrayList<>();
        orderedTrie("", "a").forEachEntrySkippingBranches(Direction.REVERSE, (key, value) ->
            entries.add(new String(key.asByteComparableArray(VERSION), StandardCharsets.UTF_8) + "=" + value));
        assertEquals(Arrays.asList("a=a", "="), entries);
    }

    /// The same for the walk of a deletion-aware trie, which takes the path bytes of its data trie itself.
    @Test
    public void testDeletionAwareProcess() throws TrieSpaceExhaustedException
    {
        DeletionAwareTrie<String, TestRangeState> trie = DeletionAwareTrie.wrap(orderedTrie("", "a"));
        List<String> entries = new ArrayList<>();
        trie.process(Direction.REVERSE, new TrieEntriesWalker.DeletionAware<String, TestRangeState, Void>()
        {
            @Override
            protected void content(String content, byte[] bytes, int byteLength)
            {
                entries.add(new String(bytes, 0, byteLength, StandardCharsets.UTF_8) + "=" + content);
            }

            @Override
            protected void deletionMarker(TestRangeState marker, byte[] bytes, int byteLength)
            {
                throw new AssertionError("no deletion branches in " + marker);
            }

            @Override
            public Void complete()
            {
                return null;
            }
        });
        assertEquals(Arrays.asList("a=a", "="), entries);
    }
}
