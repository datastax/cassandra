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
package org.apache.cassandra.index.sai.utils;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.junit.Test;

import org.apache.cassandra.cql3.LikePatterns;
import org.apache.lucene.util.automaton.Automata;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * The SAI-side helpers of the automaton intersections; the pattern engine itself is tested by
 * {@code org.apache.cassandra.cql3.LikePatternsTest}.
 */
public class AutomatonQueriesTest
{
    @Test
    public void commonPrefixBytes()
    {
        assertArrayEquals(utf8("abc"), AutomatonQueries.commonPrefixBytes(LikePatterns.fromLikePattern("abc%")));
        assertArrayEquals(utf8("abc"), AutomatonQueries.commonPrefixBytes(LikePatterns.fromLikePattern("abc%def")));
        assertArrayEquals(utf8("abc"), AutomatonQueries.commonPrefixBytes(LikePatterns.fromLikePattern("abc"))); // SINGLE
        assertArrayEquals(new byte[0], AutomatonQueries.commonPrefixBytes(LikePatterns.fromLikePattern("%abc")));
        assertArrayEquals(new byte[0], AutomatonQueries.commonPrefixBytes(LikePatterns.fromLikePattern("%"))); // ALL
        assertArrayEquals(new byte[0], AutomatonQueries.commonPrefixBytes(LikePatterns.compile(Automata.makeEmpty()))); // NONE
        // multi-byte characters contribute their full UTF-8 encoding to the prefix
        assertArrayEquals(utf8("café"), AutomatonQueries.commonPrefixBytes(LikePatterns.fromLikePattern("café%")));
    }

    @Test
    public void prefixUpperBound()
    {
        assertArrayEquals(new byte[]{ 0x62 }, AutomatonQueries.prefixUpperBound(new byte[]{ 0x61 }));
        assertArrayEquals(new byte[]{ 0x61, 0x63 }, AutomatonQueries.prefixUpperBound(new byte[]{ 0x61, 0x62 }));
        // trailing 0xFF bytes cannot be incremented and are dropped
        assertArrayEquals(new byte[]{ 0x62 }, AutomatonQueries.prefixUpperBound(new byte[]{ 0x61, (byte) 0xFF }));
        assertArrayEquals(new byte[]{ 0x62 }, AutomatonQueries.prefixUpperBound(new byte[]{ 0x61, (byte) 0xFF, (byte) 0xFF }));
        // no upper bound exists for an empty or all-0xFF prefix
        assertNull(AutomatonQueries.prefixUpperBound(new byte[0]));
        assertNull(AutomatonQueries.prefixUpperBound(new byte[]{ (byte) 0xFF, (byte) 0xFF }));

        // sanity: the bound is exclusive and sorts after every extension of the prefix
        byte[] prefix = utf8("ab");
        byte[] upper = AutomatonQueries.prefixUpperBound(prefix);
        assertTrue(Arrays.compareUnsigned(prefix, upper) < 0);
        assertTrue(Arrays.compareUnsigned(utf8("ab￿￿"), upper) < 0);
        assertTrue(Arrays.compareUnsigned(utf8("ac"), upper) >= 0);
    }

    @Test
    public void expansionsBudgetIsASharedCountdown()
    {
        AutomatonQueries.ExpansionsBudget budget = new AutomatonQueries.ExpansionsBudget(3);
        assertEquals(3, budget.limit());
        // consumable from multiple scans (as the per-query budget is, across segments and memtable shards)
        budget.consumeVisitedTerm();
        budget.consumeVisitedTerm();
        budget.consumeVisitedTerm();
        AutomatonTermsExceededException e = assertThrows(AutomatonTermsExceededException.class, budget::consumeVisitedTerm);
        // the exception carries the budget's total limit, not the count of any single scan
        assertEquals(3, e.maxVisitedTerms());
    }

    @Test
    public void maxAutomatonExpansionsValidation()
    {
        assertEquals(1, AutomatonQueries.parseMaxAutomatonExpansions("1"));
        assertEquals(100, AutomatonQueries.parseMaxAutomatonExpansions(" 100 "));
        // invalid values fall back to the default
        for (String invalid : new String[]{ "0", "-1", "abc", "", "1.5", "99999999999", null })
            assertEquals(invalid, AutomatonQueries.DEFAULT_MAX_VISITED_TERMS, AutomatonQueries.parseMaxAutomatonExpansions(invalid));
    }

    @Test
    public void expansionsBudgetCheckpoints()
    {
        int[] checkpoints = { 0 };
        AutomatonQueries.ExpansionsBudget budget = new AutomatonQueries.ExpansionsBudget(200, () -> checkpoints[0]++);
        for (int i = 0; i < 200; i++)
            budget.consumeVisitedTerm();
        assertEquals(200, budget.visitedTerms());
        // every 64 consumed terms
        assertEquals(3, checkpoints[0]);
        assertThrows(AutomatonTermsExceededException.class, budget::consumeVisitedTerm);

        // a checkpoint failure (e.g. a query timeout) aborts the consuming scan
        AutomatonQueries.ExpansionsBudget aborting = new AutomatonQueries.ExpansionsBudget(200, () -> { throw new AbortedOperationException(); });
        for (int i = 0; i < 63; i++)
            aborting.consumeVisitedTerm();
        assertThrows(AbortedOperationException.class, aborting::consumeVisitedTerm);
    }

    private static byte[] utf8(String s)
    {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
