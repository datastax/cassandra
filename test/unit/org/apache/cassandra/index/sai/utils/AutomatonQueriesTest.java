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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.junit.Test;

import org.apache.cassandra.cql3.Operator;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.CompiledAutomaton;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class AutomatonQueriesTest
{
    // ---------------------------------------------------------------------------------------------------------
    // CQL LIKE pattern translation. CQL LIKE semantics (SingleColumnRestriction.LikeRestriction): '%' is the only
    // wildcard, there is no escape mechanism, and '_' is a literal character.
    // ---------------------------------------------------------------------------------------------------------

    @Test
    public void likePrefixPattern()
    {
        CompiledAutomaton a = AutomatonQueries.fromLikePattern("abc%");
        assertAccepts(a, "abc", "abcd", "abc%", "abc def");
        assertRejects(a, "ab", "xabc", "ABC", "");
    }

    @Test
    public void likeSuffixPattern()
    {
        CompiledAutomaton a = AutomatonQueries.fromLikePattern("%abc");
        assertAccepts(a, "abc", "xabc", "xxabc");
        assertRejects(a, "abcx", "ab", "");
    }

    @Test
    public void likeContainsPattern()
    {
        CompiledAutomaton a = AutomatonQueries.fromLikePattern("%abc%");
        assertAccepts(a, "abc", "xabc", "abcx", "xabcx", "ababc");
        assertRejects(a, "ab", "axbxc", "");
    }

    @Test
    public void likeGenericPattern()
    {
        CompiledAutomaton a = AutomatonQueries.fromLikePattern("a%b");
        assertAccepts(a, "ab", "axb", "axxxb", "abb", "aab");
        assertRejects(a, "ba", "a", "b", "axbx", "xab", "");
    }

    @Test
    public void likeConsecutiveWildcardsCollapse()
    {
        CompiledAutomaton single = AutomatonQueries.fromLikePattern("a%b");
        CompiledAutomaton doubled = AutomatonQueries.fromLikePattern("a%%b");
        for (String candidate : new String[]{ "ab", "axb", "a", "b", "axxb", "" })
            assertEquals(candidate,
                         AutomatonQueries.accepts(single, utf8(candidate)),
                         AutomatonQueries.accepts(doubled, utf8(candidate)));
    }

    @Test
    public void likeMatchAllAndEmptyPatterns()
    {
        CompiledAutomaton all = AutomatonQueries.fromLikePattern("%");
        assertEquals(CompiledAutomaton.AUTOMATON_TYPE.ALL, all.type);
        assertAccepts(all, "", "a", "anything at all", "日本語");

        // an empty pattern accepts only the empty term
        CompiledAutomaton empty = AutomatonQueries.fromLikePattern("");
        assertAccepts(empty, "");
        assertRejects(empty, "a", " ");
    }

    @Test
    public void likeWithoutWildcardIsExactMatch()
    {
        CompiledAutomaton a = AutomatonQueries.fromLikePattern("abc");
        assertEquals(CompiledAutomaton.AUTOMATON_TYPE.SINGLE, a.type);
        assertAccepts(a, "abc");
        assertRejects(a, "ab", "abcd", "xabc");
    }

    @Test
    public void likeUnderscoreAndRegexpMetacharsAreLiterals()
    {
        // '_' is not a wildcard in CQL LIKE
        CompiledAutomaton underscore = AutomatonQueries.fromLikePattern("a_c");
        assertAccepts(underscore, "a_c");
        assertRejects(underscore, "abc", "axc");

        // regexp/wildcard metacharacters in the pattern are literal characters
        CompiledAutomaton dot = AutomatonQueries.fromLikePattern("a.c%");
        assertAccepts(dot, "a.c", "a.cd");
        assertRejects(dot, "abc", "abcd");

        CompiledAutomaton star = AutomatonQueries.fromLikePattern("a*c");
        assertAccepts(star, "a*c");
        assertRejects(star, "ac", "axxc");
    }

    @Test
    public void likeUnicodePattern()
    {
        CompiledAutomaton a = AutomatonQueries.fromLikePattern("caf%é");
        assertAccepts(a, "café", "cafffé", "cafés café é");
        assertRejects(a, "cafe", "café.");

        CompiledAutomaton emoji = AutomatonQueries.fromLikePattern("😀%");
        assertAccepts(emoji, "😀", "😀abc", "😀😁");
        assertRejects(emoji, "😁", "a😀");
    }

    // ---------------------------------------------------------------------------------------------------------
    // Scan-bound helpers.
    // ---------------------------------------------------------------------------------------------------------

    @Test
    public void commonPrefixBytes()
    {
        assertArrayEquals(utf8("abc"), AutomatonQueries.commonPrefixBytes(AutomatonQueries.fromLikePattern("abc%")));
        assertArrayEquals(utf8("abc"), AutomatonQueries.commonPrefixBytes(AutomatonQueries.fromLikePattern("abc%def")));
        assertArrayEquals(utf8("abc"), AutomatonQueries.commonPrefixBytes(AutomatonQueries.fromLikePattern("abc"))); // SINGLE
        assertArrayEquals(new byte[0], AutomatonQueries.commonPrefixBytes(AutomatonQueries.fromLikePattern("%abc")));
        assertArrayEquals(new byte[0], AutomatonQueries.commonPrefixBytes(AutomatonQueries.fromLikePattern("%"))); // ALL
        assertArrayEquals(new byte[0], AutomatonQueries.commonPrefixBytes(AutomatonQueries.compile(Automata.makeEmpty()))); // NONE
        // multi-byte characters contribute their full UTF-8 encoding to the prefix
        assertArrayEquals(utf8("café"), AutomatonQueries.commonPrefixBytes(AutomatonQueries.fromLikePattern("café%")));
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
    public void forPatternOperatorCachesCompiledAutomata()
    {
        // Post-filtering calls forPatternOperator once per candidate row: equal (operator, pattern bytes) keys
        // must hit the shared compile cache and return the same instance, also for a different buffer holding
        // the same content (the probe is content-keyed and copies nothing).
        ByteBuffer pattern = ByteBuffer.wrap(utf8("cache%test"));
        CompiledAutomaton first = AutomatonQueries.forPatternOperator(Operator.LIKE_CONTAINS, pattern, "c");
        assertSame(first, AutomatonQueries.forPatternOperator(Operator.LIKE_CONTAINS, ByteBuffer.wrap(utf8("cache%test")), "c"));
        // the operator is part of the key: same bytes under another operator compile independently
        assertNotSame(first, AutomatonQueries.forPatternOperator(Operator.LIKE_MATCHES, ByteBuffer.wrap(utf8("cache%test")), "c"));
    }

    private static void assertAccepts(CompiledAutomaton automaton, String... candidates)
    {
        for (String candidate : candidates)
            assertTrue("expected automaton to accept: " + candidate, AutomatonQueries.accepts(automaton, utf8(candidate)));
    }

    private static void assertRejects(CompiledAutomaton automaton, String... candidates)
    {
        for (String candidate : candidates)
            assertFalse("expected automaton to reject: " + candidate, AutomatonQueries.accepts(automaton, utf8(candidate)));
    }

    private static byte[] utf8(String s)
    {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
