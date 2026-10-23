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
package org.apache.cassandra.cql3;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.junit.Test;

import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.InvalidPatternException;
import org.apache.lucene.util.automaton.CompiledAutomaton;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * CQL LIKE pattern translation. CQL LIKE semantics (SingleColumnRestriction.LikeRestriction): '%' is the only
 * wildcard, there is no escape mechanism, and '_' is a literal character.
 */
public class LikePatternsTest
{
    @Test
    public void likePrefixPattern()
    {
        CompiledAutomaton a = LikePatterns.fromLikePattern("abc%");
        assertAccepts(a, "abc", "abcd", "abc%", "abc def");
        assertRejects(a, "ab", "xabc", "ABC", "");
    }

    @Test
    public void likeSuffixPattern()
    {
        CompiledAutomaton a = LikePatterns.fromLikePattern("%abc");
        assertAccepts(a, "abc", "xabc", "xxabc");
        assertRejects(a, "abcx", "ab", "");
    }

    @Test
    public void likeContainsPattern()
    {
        CompiledAutomaton a = LikePatterns.fromLikePattern("%abc%");
        assertAccepts(a, "abc", "xabc", "abcx", "xabcx", "ababc");
        assertRejects(a, "ab", "axbxc", "");
    }

    @Test
    public void likeGenericPattern()
    {
        CompiledAutomaton a = LikePatterns.fromLikePattern("a%b");
        assertAccepts(a, "ab", "axb", "axxxb", "abb", "aab");
        assertRejects(a, "ba", "a", "b", "axbx", "xab", "");
    }

    @Test
    public void likeConsecutiveWildcardsCollapse()
    {
        CompiledAutomaton single = LikePatterns.fromLikePattern("a%b");
        CompiledAutomaton doubled = LikePatterns.fromLikePattern("a%%b");
        for (String candidate : new String[]{ "ab", "axb", "a", "b", "axxb", "" })
            assertEquals(candidate,
                         LikePatterns.accepts(single, utf8(candidate)),
                         LikePatterns.accepts(doubled, utf8(candidate)));
    }

    @Test
    public void likeMatchAllAndEmptyPatterns()
    {
        CompiledAutomaton all = LikePatterns.fromLikePattern("%");
        assertEquals(CompiledAutomaton.AUTOMATON_TYPE.ALL, all.type);
        assertAccepts(all, "", "a", "anything at all", "日本語");

        // an empty pattern accepts only the empty term
        CompiledAutomaton empty = LikePatterns.fromLikePattern("");
        assertAccepts(empty, "");
        assertRejects(empty, "a", " ");
    }

    @Test
    public void likeWithoutWildcardIsExactMatch()
    {
        CompiledAutomaton a = LikePatterns.fromLikePattern("abc");
        assertEquals(CompiledAutomaton.AUTOMATON_TYPE.SINGLE, a.type);
        assertAccepts(a, "abc");
        assertRejects(a, "ab", "abcd", "xabc");
    }

    @Test
    public void likeUnderscoreAndRegexpMetacharsAreLiterals()
    {
        // '_' is not a wildcard in CQL LIKE
        CompiledAutomaton underscore = LikePatterns.fromLikePattern("a_c");
        assertAccepts(underscore, "a_c");
        assertRejects(underscore, "abc", "axc");

        // regexp/wildcard metacharacters in the pattern are literal characters
        CompiledAutomaton dot = LikePatterns.fromLikePattern("a.c%");
        assertAccepts(dot, "a.c", "a.cd");
        assertRejects(dot, "abc", "abcd");

        CompiledAutomaton star = LikePatterns.fromLikePattern("a*c");
        assertAccepts(star, "a*c");
        assertRejects(star, "ac", "axxc");
    }

    @Test
    public void likeUnicodePattern()
    {
        CompiledAutomaton a = LikePatterns.fromLikePattern("caf%é");
        assertAccepts(a, "café", "cafffé", "cafés café é");
        assertRejects(a, "cafe", "café.");

        CompiledAutomaton emoji = LikePatterns.fromLikePattern("😀%");
        assertAccepts(emoji, "😀", "😀abc", "😀😁");
        assertRejects(emoji, "😁", "a😀");
    }

    @Test
    public void suffixAndContainsAutomataMatchTheirLiteralsLiterally()
    {
        // the value of LIKE_SUFFIX/LIKE_CONTAINS is a literal: a '%' in it is not a wildcard
        CompiledAutomaton suffix = LikePatterns.suffixAutomaton("a%c");
        assertAccepts(suffix, "a%c", "xa%c");
        assertRejects(suffix, "abc", "a%cx");

        CompiledAutomaton contains = LikePatterns.containsAutomaton("a%c");
        assertAccepts(contains, "a%c", "xa%cx");
        assertRejects(contains, "abc", "ac");
    }

    @Test
    public void forPatternOperatorCachesCompiledAutomata()
    {
        // Post-filtering calls forPatternOperator once per candidate row: equal (operator, pattern bytes) keys
        // must hit the shared compile cache and return the same instance, also for a different buffer holding
        // the same content (the probe is content-keyed and copies nothing).
        ByteBuffer pattern = ByteBuffer.wrap(utf8("cache%test"));
        CompiledAutomaton first = LikePatterns.forPatternOperator(Operator.LIKE_CONTAINS, pattern, "c");
        assertSame(first, LikePatterns.forPatternOperator(Operator.LIKE_CONTAINS, ByteBuffer.wrap(utf8("cache%test")), "c"));
        // the operator is part of the key: same bytes under another operator compile independently
        assertNotSame(first, LikePatterns.forPatternOperator(Operator.LIKE_MATCHES, ByteBuffer.wrap(utf8("cache%test")), "c"));
    }

    @Test
    public void forPatternOperatorRejectsPatternsTooComplexToDeterminize()
    {
        // enough alternating wildcards and literals to exceed Lucene's determinization work limit; the failure
        // is cached too, so the second call fails the same way without re-running the determinization
        ByteBuffer pattern = ByteBuffer.wrap(utf8("a%".repeat(1000) + 'a'));
        for (int i = 0; i < 2; i++)
        {
            assertThatThrownBy(() -> LikePatterns.forPatternOperator(Operator.LIKE_MATCHES, pattern, "v"))
                .isInstanceOf(InvalidPatternException.class)
                .hasMessageStartingWith("Invalid pattern for LIKE '<term>' restriction on column v: ");
        }
        // the column is optional (the Operator post-filter doesn't know it)
        assertThatThrownBy(() -> LikePatterns.forPatternOperator(Operator.LIKE_MATCHES, pattern, null))
            .isInstanceOf(InvalidPatternException.class)
            .hasMessageStartingWith("Invalid pattern for LIKE '<term>' restriction: ");
    }

    @Test
    public void compilesPatternsWithLongLeadingLiterals()
    {
        // Lucene's finiteness check recurses over the automaton and gives up past 1000 states; a LIKE pattern's
        // finiteness is known without it (wildcards: infinite language, none: a single string), so a pattern
        // whose first literal is longer than that compiles
        String literal = "x".repeat(1500);
        CompiledAutomaton pattern = LikePatterns.fromLikePattern(literal + "%y");
        assertTrue(LikePatterns.accepts(pattern, utf8(literal + "abcy")));
        assertFalse(LikePatterns.accepts(pattern, utf8(literal + "abc")));
        assertEquals(CompiledAutomaton.AUTOMATON_TYPE.SINGLE, LikePatterns.fromLikePattern(literal).type);
        assertTrue(LikePatterns.accepts(LikePatterns.forPatternOperator(Operator.LIKE_MATCHES, ByteBuffer.wrap(utf8(literal + "%y")), "v"),
                                        utf8(literal + "y")));
    }

    @Test
    public void likeMatchesOperatorPostFilterSemantics()
    {
        // The LIKE_MATCHES post-filter (used by filtering and replica filtering protection) evaluates the same
        // automaton as the index side: every '%' is an any-string wildcard. It used to be a plain 'contains'
        // check treating '%' as a literal character, so 'a%b' patterns could never match.
        ByteBuffer pattern = ByteBuffer.wrap(utf8("qu%ck"));
        assertTrue(Operator.LIKE_MATCHES.isSatisfiedBy(UTF8Type.instance, ByteBuffer.wrap(utf8("quick")), pattern));
        assertTrue(Operator.LIKE_MATCHES.isSatisfiedBy(UTF8Type.instance, ByteBuffer.wrap(utf8("quck")), pattern));
        assertFalse(Operator.LIKE_MATCHES.isSatisfiedBy(UTF8Type.instance, ByteBuffer.wrap(utf8("qu%ck!")), pattern));
        // a pattern without wildcards is an exact match, not a substring match
        ByteBuffer exact = ByteBuffer.wrap(utf8("quick"));
        assertTrue(Operator.LIKE_MATCHES.isSatisfiedBy(UTF8Type.instance, ByteBuffer.wrap(utf8("quick")), exact));
        assertFalse(Operator.LIKE_MATCHES.isSatisfiedBy(UTF8Type.instance, ByteBuffer.wrap(utf8("quicker")), exact));

        // NOT_LIKE_MATCHES (not reachable from CQL) is defined as the negation and flips accordingly
        assertFalse(Operator.NOT_LIKE_MATCHES.isSatisfiedBy(UTF8Type.instance, ByteBuffer.wrap(utf8("quick")), pattern));
        assertTrue(Operator.NOT_LIKE_MATCHES.isSatisfiedBy(UTF8Type.instance, ByteBuffer.wrap(utf8("qu%ck!")), pattern));
        assertTrue(Operator.NOT_LIKE_MATCHES.isSatisfiedBy(UTF8Type.instance, ByteBuffer.wrap(utf8("quicker")), exact));
    }

    @Test
    public void likeMatchesWithLeadingAndTrailingWildcards()
    {
        // generic patterns reach LIKE_MATCHES with their leading/trailing wildcards too (LikeRestriction#makeSpecific)
        ByteBuffer pattern = ByteBuffer.wrap(utf8("%a%b%"));
        assertTrue(Operator.LIKE_MATCHES.isSatisfiedBy(UTF8Type.instance, ByteBuffer.wrap(utf8("xaxbx")), pattern));
        assertTrue(Operator.LIKE_MATCHES.isSatisfiedBy(UTF8Type.instance, ByteBuffer.wrap(utf8("ab")), pattern));
        assertFalse(Operator.LIKE_MATCHES.isSatisfiedBy(UTF8Type.instance, ByteBuffer.wrap(utf8("ba")), pattern));
        pattern = ByteBuffer.wrap(utf8("%%a%%b"));
        assertTrue(Operator.LIKE_MATCHES.isSatisfiedBy(UTF8Type.instance, ByteBuffer.wrap(utf8("xab")), pattern));
        assertFalse(Operator.LIKE_MATCHES.isSatisfiedBy(UTF8Type.instance, ByteBuffer.wrap(utf8("xabx")), pattern));
    }

    private static void assertAccepts(CompiledAutomaton automaton, String... candidates)
    {
        for (String candidate : candidates)
            assertTrue("expected automaton to accept: " + candidate, LikePatterns.accepts(automaton, utf8(candidate)));
    }

    private static void assertRejects(CompiledAutomaton automaton, String... candidates)
    {
        for (String candidate : candidates)
            assertFalse("expected automaton to reject: " + candidate, LikePatterns.accepts(automaton, utf8(candidate)));
    }

    private static byte[] utf8(String s)
    {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
