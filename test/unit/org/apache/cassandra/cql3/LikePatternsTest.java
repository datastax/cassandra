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
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.UnaryOperator;

import org.junit.Test;

import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.tries.TriePattern;
import org.apache.cassandra.exceptions.InvalidPatternException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * CQL LIKE pattern translation. CQL LIKE semantics (SingleColumnRestriction.LikeRestriction): '%' matches any
 * string and '_' any single character; the patterns of LIKE_MATCHES escape their literal wildcards with '\'.
 * The results are compared with {@link LikeReference}, an independent implementation of the semantics.
 */
public class LikePatternsTest
{
    @Test
    public void likePrefixPattern()
    {
        TriePattern a = pattern("abc%");
        assertAccepts(a, "abc", "abcd", "abc%", "abc def");
        assertRejects(a, "ab", "xabc", "ABC", "");
    }

    @Test
    public void likeSuffixPattern()
    {
        TriePattern a = pattern("%abc");
        assertAccepts(a, "abc", "xabc", "xxabc");
        assertRejects(a, "abcx", "ab", "");
    }

    @Test
    public void likeContainsPattern()
    {
        TriePattern a = pattern("%abc%");
        assertAccepts(a, "abc", "xabc", "abcx", "xabcx", "ababc");
        assertRejects(a, "ab", "axbxc", "");
    }

    @Test
    public void likeGenericPattern()
    {
        TriePattern a = pattern("a%b");
        assertAccepts(a, "ab", "axb", "axxxb", "abb", "aab");
        assertRejects(a, "ba", "a", "b", "axbx", "xab", "");
    }

    @Test
    public void likeConsecutiveWildcardsCollapse()
    {
        TriePattern single = pattern("a%b");
        TriePattern doubled = pattern("a%%b");
        assertEquals(single, doubled);
        for (String candidate : new String[]{ "ab", "axb", "a", "b", "axxb", "" })
            assertEquals(candidate, single.matches(utf8Buffer(candidate)), doubled.matches(utf8Buffer(candidate)));
    }

    @Test
    public void likeMatchAllAndEmptyPatterns()
    {
        TriePattern all = pattern("%");
        assertAccepts(all, "", "a", "anything at all", "日本語");

        // an empty pattern accepts only the empty term
        TriePattern empty = pattern("");
        assertAccepts(empty, "");
        assertRejects(empty, "a", " ");
    }

    @Test
    public void likeWithoutWildcardIsExactMatch()
    {
        TriePattern a = pattern("abc");
        assertAccepts(a, "abc");
        assertRejects(a, "ab", "abcd", "xabc");
    }

    @Test
    public void likeUnderscoreMatchesOneCharacter()
    {
        TriePattern underscore = pattern("a_c");
        assertAccepts(underscore, "a_c", "abc", "aéc", "a😀c", "a%c");
        assertRejects(underscore, "ac", "abbc", "aééc", "abcd");

        TriePattern mixed = pattern("_a%b__");
        assertAccepts(mixed, "xab12", "😀aéb😀😀", "xaxxxb12");
        assertRejects(mixed, "ab12", "xab1", "xab123");
    }

    @Test
    public void likeEscapesAreLiterals()
    {
        // the internal form escapes the literal wildcards and the escape itself with a backslash
        TriePattern escaped = pattern("a\\_\\%\\\\%");
        assertAccepts(escaped, "a_%\\", "a_%\\xyz");
        assertRejects(escaped, "ab%\\", "a_x\\", "a_%x");

        TriePattern exact = pattern("a\\%");
        assertAccepts(exact, "a%");
        assertRejects(exact, "a", "ab");

        assertThatThrownBy(() -> pattern("a\\")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> pattern("a\\b")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void hasWildcardIgnoresEscapedWildcards()
    {
        for (String pattern : new String[]{ "a%", "_", "%a", "a_b", "\\\\%", "\\\\_", "é_", "\\%%" })
            assertTrue(pattern, LikePatterns.hasWildcard(utf8Buffer(pattern)));
        for (String pattern : new String[]{ "", "abc", "\\%", "\\_", "\\\\", "a\\_\\%\\\\b", "é日😀" })
            assertFalse(pattern, LikePatterns.hasWildcard(utf8Buffer(pattern)));

        // only the remaining bytes of the buffer count
        ByteBuffer slice = utf8Buffer("%_abc%");
        slice.position(2).limit(5);
        assertFalse(LikePatterns.hasWildcard(slice));
        assertEquals(2, slice.position());
    }

    @Test
    public void unescapeRemovesEscapes()
    {
        assertEquals("", unescape(""));
        assertEquals("abc", unescape("abc"));
        assertEquals("a_%\\", unescape("a\\_\\%\\\\"));
        assertEquals("é_日%😀", unescape("é\\_日\\%😀"));
        // a trailing lone escape (never produced by the coordinator) is kept
        assertEquals("a\\", unescape("a\\"));

        ByteBuffer slice = utf8Buffer("xx\\_ayy");
        slice.position(2).limit(5);
        assertEquals("_a", UTF8Type.instance.compose(LikePatterns.unescape(slice)));
        assertEquals(2, slice.position());
    }

    @Test
    public void escapeEscapesWildcardsAndEscapes()
    {
        assertEquals("", LikePatterns.escape(""));
        assertEquals("abc", LikePatterns.escape("abc"));
        assertEquals("a\\_\\%\\\\é", LikePatterns.escape("a_%\\é"));
        // escape and unescape are inverse, and an escaped literal has no wildcard
        for (String literal : new String[]{ "", "abc", "%", "_", "\\", "\\%", "a%b_c\\d", "é_😀%日\\" })
        {
            ByteBuffer escaped = utf8Buffer(LikePatterns.escape(literal));
            assertFalse(literal, LikePatterns.hasWildcard(escaped));
            assertEquals(literal, UTF8Type.instance.compose(LikePatterns.unescape(escaped)));
            assertAccepts(pattern(LikePatterns.escape(literal)), literal);
        }
    }

    @Test
    public void normalizeLiteralsKeepsWildcardsAndEscapes()
    {
        // a normalization turning literals into wildcards or escapes (like ASCII folding of full-width characters)
        // must not change the structure of the pattern
        UnaryOperator<ByteBuffer> fold = literal -> UTF8Type.instance.decompose(UTF8Type.instance.compose(literal)
                                                                                            .replace('＿', '_')
                                                                                            .replace('％', '%')
                                                                                            .replace('＼', '\\')
                                                                                            .toLowerCase());
        assertEquals("a\\_\\%\\\\_b%\\_", normalize("A＿％＼_B%\\_", fold));
        assertEquals("abc", normalize("ABC", fold));
        assertEquals("%_", normalize("%_", fold));
        // a literal normalized to nothing
        assertEquals("%a", normalize("x%a", literal -> UTF8Type.instance.compose(literal).equals("x") ? null : literal));
        assertEquals("%a", normalize("x%a", literal -> UTF8Type.instance.compose(literal).equals("x") ? ByteBuffer.allocate(0) : literal));
    }

    @Test
    public void normalizeLiteralsNormalizesWholeUnescapedRuns()
    {
        // the normalization sees each run of literals between wildcards once, unescaped
        List<String> runs = new ArrayList<>();
        UnaryOperator<ByteBuffer> record = literal -> {
            runs.add(UTF8Type.instance.compose(literal));
            return literal;
        };
        assertEquals("ab%c_\\%d\\\\é", normalize("ab%c_\\%d\\\\é", record));
        assertEquals(List.of("ab", "c", "%d\\é"), runs);

        runs.clear();
        assertEquals("%%_%", normalize("%%_%", record));
        assertEquals(List.of(), runs);
        assertEquals("", normalize("", record));
        assertEquals(List.of(), runs);
    }

    @Test
    public void normalizeLiteralsEscapesNormalizedWildcards()
    {
        // a normalization producing wildcards or escapes in a literal: they are escaped
        UnaryOperator<ByteBuffer> toWildcards = literal -> utf8Buffer(UTF8Type.instance.compose(literal).replace('x', '_')
                                                                                                   .replace('y', '%')
                                                                                                   .replace('z', '\\'));
        String normalized = normalize("axyz%x", toWildcards);
        assertEquals("a\\_\\%\\\\%\\_", normalized);
        assertAccepts(pattern(normalized), "a_%\\b_", "a_%\\_");
        assertRejects(pattern(normalized), "abcd_", "a_%\\b");
    }

    @Test
    public void normalizeLiteralsReadsBufferSlices()
    {
        ByteBuffer slice = utf8Buffer("%%AB%C");
        slice.position(2);
        ByteBuffer normalized = LikePatterns.normalizeLiterals(slice, literal -> utf8Buffer(UTF8Type.instance.compose(literal).toLowerCase()));
        assertEquals("ab%c", UTF8Type.instance.compose(normalized));
        assertEquals(2, slice.position());
    }

    private static String normalize(String pattern, UnaryOperator<ByteBuffer> normalize)
    {
        return UTF8Type.instance.compose(LikePatterns.normalizeLiterals(UTF8Type.instance.decompose(pattern), normalize));
    }

    @Test
    public void likeRegexpMetacharsAreLiterals()
    {
        // regexp/wildcard metacharacters in the pattern are literal characters
        TriePattern dot = pattern("a.c%");
        assertAccepts(dot, "a.c", "a.cd");
        assertRejects(dot, "abc", "abcd");

        TriePattern star = pattern("a*c");
        assertAccepts(star, "a*c");
        assertRejects(star, "ac", "axxc");

        TriePattern question = pattern("a?c");
        assertAccepts(question, "a?c");
        assertRejects(question, "abc");
    }

    @Test
    public void likeUnicodePattern()
    {
        TriePattern a = pattern("caf%é");
        assertAccepts(a, "café", "cafffé", "cafés café é");
        assertRejects(a, "cafe", "café.");

        TriePattern emoji = pattern("😀%");
        assertAccepts(emoji, "😀", "😀abc", "😀😁");
        assertRejects(emoji, "😁", "a😀");
    }

    @Test
    public void forPatternOperatorCachesPatterns()
    {
        // Post-filtering calls forPatternOperator once per candidate row: equal (operator, pattern bytes) keys
        // must hit the shared cache and return the same instance, also for a different buffer holding the same
        // content (the probe is content-keyed and copies nothing).
        ByteBuffer pattern = ByteBuffer.wrap(utf8("cache%test"));
        TriePattern first = LikePatterns.forPatternOperator(Operator.LIKE_CONTAINS, pattern, "c");
        assertSame(first, LikePatterns.forPatternOperator(Operator.LIKE_CONTAINS, ByteBuffer.wrap(utf8("cache%test")), "c"));
        // the operator is part of the key: same bytes under another operator build another pattern
        assertNotSame(first, LikePatterns.forPatternOperator(Operator.LIKE_MATCHES, ByteBuffer.wrap(utf8("cache%test")), "c"));
    }

    /**
     * Patterns alternating literals and wildcards used to be rejected, as their automata were too complex to
     * determinize. Matching doesn't need that, and they are served.
     */
    @Test
    public void servesPatternsWithManyWildcards()
    {
        String patternString = "a%".repeat(1000) + 'a';
        TriePattern pattern = LikePatterns.forPatternOperator(Operator.LIKE_MATCHES, utf8Buffer(patternString), "v");
        String matching = "ab".repeat(1000) + 'a';
        assertTrue(pattern.matches(utf8Buffer(matching)));
        assertTrue(pattern.matches(utf8Buffer("a".repeat(1001))));
        assertFalse(pattern.matches(utf8Buffer("a".repeat(1000))));
        assertFalse(pattern.matches(utf8Buffer(matching + 'b')));
    }

    /**
     * The automaton of {@code '%a<n x _>b%'} has about 2^n states, and was rejected from n = 12. These patterns are
     * served, and match what they should.
     */
    @Test
    public void servesPatternsThatExplodeAsAutomata()
    {
        Random random = new Random(1);
        String[] alphabet = { "a", "b", "c", "é", "日" };
        for (int underscores : new int[]{ 10, 12, 50, 1000 })
        {
            String patternString = "%a" + "_".repeat(underscores) + "b%";
            TriePattern pattern = LikePatterns.forPatternOperator(Operator.LIKE_MATCHES, utf8Buffer(patternString), "v");
            assertTrue(pattern.matches(utf8Buffer("x" + "a".repeat(underscores + 1) + "bx")));
            assertTrue(pattern.matches(utf8Buffer("a" + "é".repeat(underscores) + "b")));
            assertFalse(pattern.matches(utf8Buffer("a" + "a".repeat(underscores - 1) + "b")));
            assertFalse(pattern.matches(utf8Buffer("a" + "a".repeat(underscores + 1) + "c")));
            for (int i = 0; i < 200; i++)
            {
                StringBuilder value = new StringBuilder();
                for (int length = underscores + random.nextInt(20); length > 0; length--)
                    value.append(alphabet[random.nextInt(alphabet.length)]);
                assertEquals(patternString + " on " + value,
                             LikeReference.matchesPattern(patternString, value.toString()),
                             pattern.matches(utf8Buffer(value.toString())));
            }
        }
    }

    @Test
    public void servesPatternsWithLongLiterals()
    {
        String literal = "x".repeat(1500);
        TriePattern pattern = pattern(literal + "%y");
        assertTrue(pattern.matches(utf8Buffer(literal + "abcy")));
        assertFalse(pattern.matches(utf8Buffer(literal + "abc")));
        assertTrue(pattern(literal).matches(utf8Buffer(literal)));
        assertTrue(LikePatterns.forPatternOperator(Operator.LIKE_MATCHES, utf8Buffer(literal + "%y"), "v")
                               .matches(utf8Buffer(literal + "y")));
    }

    @Test
    public void likeMatchesOperatorPostFilterSemantics()
    {
        // The LIKE_MATCHES post-filter (used by filtering and replica filtering protection) evaluates the same
        // pattern as the index side: every '%' is an any-string wildcard. It used to be a plain 'contains'
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

    @Test
    public void forPatternOperatorReadsPrefixSuffixAndContainsValuesAsLiterals()
    {
        // the values of LIKE_PREFIX, LIKE_SUFFIX and LIKE_CONTAINS are unescaped literals: '_', '%' and '\\' are no
        // wildcards
        TriePattern prefix = LikePatterns.forPatternOperator(Operator.LIKE_PREFIX, utf8Buffer("a_\\"), "v");
        assertAccepts(prefix, "a_\\x", "a_\\");
        assertRejects(prefix, "ab\\x", "xa_\\");
        TriePattern suffix = LikePatterns.forPatternOperator(Operator.LIKE_SUFFIX, utf8Buffer("a_\\"), "v");
        assertAccepts(suffix, "xa_\\", "a_\\");
        assertRejects(suffix, "xab\\", "a_");
        TriePattern contains = LikePatterns.forPatternOperator(Operator.LIKE_CONTAINS, utf8Buffer("%_"), "v");
        assertAccepts(contains, "x%_y", "%_");
        assertRejects(contains, "x%ay", "x_%y");
        // while the same bytes are a pattern for LIKE_MATCHES
        assertAccepts(LikePatterns.forPatternOperator(Operator.LIKE_MATCHES, utf8Buffer("%_"), "v"), "x", "xyz");
    }

    @Test
    public void forPatternOperatorRejectsInvalidEscapes()
    {
        for (String pattern : new String[]{ "a\\", "a\\b%" })
            assertThatThrownBy(() -> LikePatterns.forPatternOperator(Operator.LIKE_MATCHES, utf8Buffer(pattern), "v"))
            .isInstanceOf(InvalidPatternException.class)
            .hasMessageStartingWith("Invalid pattern for LIKE '<term>' restriction on column v");
        // the column is optional (the Operator post-filter doesn't know it)
        assertThatThrownBy(() -> LikePatterns.forPatternOperator(Operator.LIKE_MATCHES, utf8Buffer("a\\"), null))
            .isInstanceOf(InvalidPatternException.class)
            .hasMessageStartingWith("Invalid pattern for LIKE '<term>' restriction: ");
    }

    @Test
    public void matchesBufferSlicesAndDirectBuffers()
    {
        for (TriePattern pattern : new TriePattern[]{ pattern("a_c"), pattern("abc") })
        {
            ByteBuffer slice = utf8Buffer("xxabcyy");
            slice.position(2).limit(5);
            assertTrue(pattern.matches(slice));
            assertEquals(2, slice.position());

            ByteBuffer direct = ByteBuffer.allocateDirect(3);
            direct.put(utf8("abc")).flip();
            assertTrue(pattern.matches(direct));
            assertEquals(0, direct.position());

            assertFalse(pattern.matches(utf8Buffer("abcd")));
        }
    }

    /** Random patterns and values over a small alphabet, compared with the reference implementation. */
    @Test
    public void matchesLikeReference()
    {
        Random random = new Random(42);
        String[] alphabet = { "a", "b", "%", "_", "\\", "é", "日", "😀" };
        String[] valueAlphabet = { "a", "b", "%", "_", "\\", "é", "日", "😀" };
        for (int i = 0; i < 2000; i++)
        {
            StringBuilder pattern = new StringBuilder();
            for (int length = random.nextInt(8); length > 0; length--)
            {
                String c = alphabet[random.nextInt(alphabet.length)];
                if (c.equals("\\"))
                    pattern.append('\\').append("%_\\".charAt(random.nextInt(3))); // a valid escape
                else
                    pattern.append(c);
            }
            Operator operator = new Operator[]{ Operator.LIKE_MATCHES, Operator.LIKE_PREFIX, Operator.LIKE_SUFFIX, Operator.LIKE_CONTAINS }[random.nextInt(4)];
            TriePattern triePattern = LikePatterns.forPatternOperator(operator, utf8Buffer(pattern.toString()), "v");
            for (int j = 0; j < 20; j++)
            {
                StringBuilder value = new StringBuilder();
                for (int length = random.nextInt(8); length > 0; length--)
                    value.append(valueAlphabet[random.nextInt(valueAlphabet.length)]);
                assertEquals(operator + " '" + pattern + "' on '" + value + "'",
                             LikeReference.matches(operator, pattern.toString(), value.toString()),
                             triePattern.matches(utf8Buffer(value.toString())));
            }
        }
    }

    private static TriePattern pattern(String internalForm)
    {
        return LikePatterns.triePattern(utf8Buffer(internalForm));
    }

    private static void assertAccepts(TriePattern pattern, String... candidates)
    {
        for (String candidate : candidates)
            assertTrue("expected pattern " + pattern + " to accept: " + candidate, pattern.matches(utf8Buffer(candidate)));
    }

    private static void assertRejects(TriePattern pattern, String... candidates)
    {
        for (String candidate : candidates)
            assertFalse("expected pattern " + pattern + " to reject: " + candidate, pattern.matches(utf8Buffer(candidate)));
    }

    private static String unescape(String pattern)
    {
        return UTF8Type.instance.compose(LikePatterns.unescape(utf8Buffer(pattern)));
    }

    private static ByteBuffer utf8Buffer(String s)
    {
        return ByteBuffer.wrap(utf8(s));
    }

    private static byte[] utf8(String s)
    {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
