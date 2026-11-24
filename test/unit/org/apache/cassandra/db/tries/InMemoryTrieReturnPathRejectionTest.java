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
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static org.apache.cassandra.db.tries.TrieUtil.VERSION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/// A plain trie, ordered or not, has at most one content per node, which a forward walk presents before the node's
/// branch; only range tries can have content after a branch (see "Stopping on the ascent path" in `Trie.md`). The
/// in-memory tries that store plain or deletion-aware data must refuse anything else, whether or not assertions are
/// enabled.
public class InMemoryTrieReturnPathRejectionTest
{
    static ByteComparable key(String key)
    {
        return ByteComparable.preencoded(VERSION, key.getBytes(StandardCharsets.UTF_8));
    }

    static List<String> entries(BaseTrie<?, ?, ?> trie)
    {
        List<String> entries = new ArrayList<>();
        for (Direction direction : Direction.values())
            trie.forEachEntry(direction, (key, content) -> entries.add(direction + " " + new String(key.asByteComparableArray(VERSION), StandardCharsets.UTF_8) + " " + content));
        return entries;
    }

    static InMemoryDeletionAwareTrie<String, TestRangeState>.Mutator<String, TestRangeState> mutator(InMemoryDeletionAwareTrie<String, TestRangeState> trie)
    {
        return trie.mutator((existing, update) -> update,
                            (existing, update) -> update,
                            (existing, deletion) -> existing,
                            (deletion, inserted) -> inserted,
                            true,
                            features -> false,
                            features -> false);
    }

    /// The merged view of a deletion-aware trie that has live content at "m" and "mx", and deletes the branch of "m":
    /// it presents the end of the deletion after the branch, on the return path.
    static Trie<String> mergedViewWithDeletionAfterBranch() throws TrieSpaceExhaustedException
    {
        InMemoryTrie<String> live = InMemoryTrie.shortLived(VERSION);
        live.putRecursive(key("m"), "m", (existing, update) -> update);
        live.putRecursive(key("mx"), "mx", (existing, update) -> update);

        InMemoryRangeTrie<TestRangeState> deletion = InMemoryRangeTrie.shortLived(VERSION);
        deletion.putRecursive(ByteComparable.EMPTY, new TestRangeState(ByteComparable.EMPTY, false, -1, 1), new TestRangeState(ByteComparable.EMPTY, true, 1, -1), (existing, update) -> update);

        InMemoryDeletionAwareTrie<String, TestRangeState> trie = InMemoryDeletionAwareTrie.shortLived(VERSION);
        InMemoryDeletionAwareTrie<String, TestRangeState>.Mutator<String, TestRangeState> mutator = mutator(trie);
        mutator.apply(DeletionAwareTrie.wrap(live));
        mutator.apply(DeletionAwareTrie.deletionBranch(key("m"), VERSION, deletion));
        return trie.mergedTrie((content, deletionState) -> content != null ? content : "deletion " + deletionState);
    }

    /// The source of the tests below does present content on the return path of a forward walk.
    @Test
    public void testMergedViewPresentsContentAfterBranch() throws TrieSpaceExhaustedException
    {
        List<String> afterBranch = new ArrayList<>();
        Cursor<String> cursor = mergedViewWithDeletionAfterBranch().cursor(Direction.FORWARD);
        for (long position = cursor.encodedPosition(); !Cursor.isExhausted(position); position = cursor.advance())
            if (Cursor.isOnReturnPath(position) && cursor.content() != null)
                afterBranch.add(Cursor.toString(position).trim() + " " + cursor.content());
        assertEquals(afterBranch.toString(), 1, afterBranch.size());
    }

    @Test
    public void testApplyRejectsContentAfterBranch() throws TrieSpaceExhaustedException
    {
        Trie<String> merged = mergedViewWithDeletionAfterBranch();
        for (boolean ordered : new boolean[]{ false, true })
        {
            InMemoryTrie<String> trie = ordered ? InMemoryTrie.shortLivedOrdered(VERSION) : InMemoryTrie.shortLived(VERSION);
            trie.putRecursive(key("a"), "a", (existing, update) -> update);

            IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                                                      () -> trie.apply(merged, (existing, update) -> update, features -> false));
            assertTrue(e.getMessage(), e.getMessage().toLowerCase().contains("return path"));
            // what was there before is untouched
            assertEquals("a", trie.get(key("a")));
        }
    }

    @Test
    public void testDeletionAwareApplyRejectsLiveContentAfterBranch() throws TrieSpaceExhaustedException
    {
        DeletionAwareTrie<String, TestRangeState> mutation = DeletionAwareTrie.wrap(mergedViewWithDeletionAfterBranch());
        InMemoryDeletionAwareTrie<String, TestRangeState> trie = InMemoryDeletionAwareTrie.shortLived(VERSION);
        mutator(trie).apply(DeletionAwareTrie.wrap(TrieUtil.directTrie("a")));

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> mutator(trie).apply(mutation));
        assertTrue(e.getMessage(), e.getMessage().toLowerCase().contains("return path"));
        assertEquals("a", trie.get(key("a")));
    }
}
