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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.BeforeClass;
import org.junit.Test;

import com.googlecode.concurrenttrees.common.Iterables;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static org.apache.cassandra.db.tries.TrieUtil.VERSION;
import static org.apache.cassandra.db.tries.TrieUtil.directComparable;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.SourceDSL.*;

public class PatternCursorTest
{
    @BeforeClass
    public static void enableVerification()
    {
        CassandraRelevantProperties.TRIE_DEBUG.setBoolean(true);
    }

    /**
     * Extract the values of the provided trie into a list.
     */
    private static <T> List<T> toList(Trie<T> trie, Direction direction)
    {
        return Iterables.toList(trie.values(direction));
    }

    private static Trie<String> createTestTrie(String... values) throws TrieSpaceExhaustedException
    {
        return TrieUtil.directOrderedTrie(values);
    }

    private void checkPatternMatch(String message, List<String> expected, Trie<String> trie, String pattern)
    {
        // Test all matcher choices
        for (int algoChoice : ALGO_CHOICES)
        {
            String algoMessage = message + " [algo=" + algoChoice + "]";
            Trie<String> matched = patternMatched(trie, pattern, algoChoice);
            List<String> forwardResult = toList(matched, Direction.FORWARD);
            List<String> reverseResult = toList(matched, Direction.REVERSE);
            List<String> expectedForward = expected.stream()
                                                   .sorted()
                                                   .collect(Collectors.toList());
            List<String> expectedReverse = expected.stream()
                                                   .sorted((a, b) -> b.compareTo(a))
                                                   .collect(Collectors.toList());

            assertEquals(algoMessage + " forward", expectedForward, forwardResult);
            assertEquals(algoMessage + " reverse", expectedReverse, reverseResult);
        }
    }

    @Test
    public void testExactMatch() throws TrieSpaceExhaustedException
    {
        Trie<String> trie = createTestTrie("apple", "banana", "cherry", "date");

        // Exact match should return only that entry
        checkPatternMatch("exact apple", ImmutableList.of("apple"), trie, "apple");
        checkPatternMatch("exact banana", ImmutableList.of("banana"), trie, "banana");
        checkPatternMatch("exact cherry", ImmutableList.of("cherry"), trie, "cherry");
        checkPatternMatch("exact date", ImmutableList.of("date"), trie, "date");
    }

    @Test
    public void testNoMatch() throws TrieSpaceExhaustedException
    {
        Trie<String> trie = createTestTrie("apple", "banana", "cherry");

        // Pattern that doesn't match anything
        checkPatternMatch("no match", ImmutableList.of(), trie, "orange");
        checkPatternMatch("no match prefix", ImmutableList.of(), trie, "app");
        checkPatternMatch("no match suffix", ImmutableList.of(), trie, "pple");
    }

    @Test
    public void testSingleCharacterWildcard() throws TrieSpaceExhaustedException
    {
        Trie<String> trie = createTestTrie("cat", "cot", "cut", "bat", "hat");

        // ? matches any single character
        checkPatternMatch("c?t", ImmutableList.of("cat", "cot", "cut"), trie, "c?t");
        checkPatternMatch("?at", ImmutableList.of("bat", "cat", "hat"), trie, "?at");
        checkPatternMatch("ca?", ImmutableList.of("cat"), trie, "ca?");
    }

    @Test
    public void testMultipleQuestionMarks() throws TrieSpaceExhaustedException
    {
        Trie<String> trie = createTestTrie("abc", "adc", "aec", "axc", "abcd");

        checkPatternMatch("a?c", ImmutableList.of("abc", "adc", "aec", "axc"), trie, "a?c");
        checkPatternMatch("??c", ImmutableList.of("abc", "adc", "aec", "axc"), trie, "??c");
        checkPatternMatch("a??", ImmutableList.of("abc", "adc", "aec", "axc"), trie, "a??");
        checkPatternMatch("????", ImmutableList.of("abcd"), trie, "????");
    }

    @Test
    public void testStarWildcard() throws TrieSpaceExhaustedException
    {
        Trie<String> trie = createTestTrie("apple", "application", "apply", "banana", "band");

        // * matches any sequence of characters (including empty)
        checkPatternMatch("app*", ImmutableList.of("apple", "application", "apply"), trie, "app*");
        checkPatternMatch("*ana", ImmutableList.of("banana"), trie, "*ana");
        checkPatternMatch("ban*", ImmutableList.of("banana", "band"), trie, "ban*");
    }

    @Test
    public void testStarAtStart() throws TrieSpaceExhaustedException
    {
        Trie<String> trie = createTestTrie("apple", "pineapple", "grapple", "banana");

        checkPatternMatch("*apple", ImmutableList.of("apple", "grapple", "pineapple"), trie, "*apple");
        checkPatternMatch("*ana", ImmutableList.of("banana"), trie, "*ana");
    }

    @Test
    public void testStarAtEnd() throws TrieSpaceExhaustedException
    {
        Trie<String> trie = createTestTrie("test", "testing", "tester", "tested", "best");

        checkPatternMatch("test*", ImmutableList.of("test", "tested", "tester", "testing"), trie, "test*");
        checkPatternMatch("tes*", ImmutableList.of("test", "tested", "tester", "testing"), trie, "tes*");
    }

    @Test
    public void testStarInMiddle() throws TrieSpaceExhaustedException
    {
        Trie<String> trie = createTestTrie("abc", "adc", "abbc", "abbbc", "ac", "axyzc");

        checkPatternMatch("a*c", ImmutableList.of("abbbc", "abbc", "abc", "ac", "adc", "axyzc"), trie, "a*c");
        checkPatternMatch("ab*c", ImmutableList.of("abbbc", "abbc", "abc"), trie, "ab*c");
    }

    @Test
    public void testMultipleStars() throws TrieSpaceExhaustedException
    {
        Trie<String> trie = createTestTrie("abc", "adc", "abxc", "abyc", "abxyc");

        // Multiple stars should be collapsed to one
        checkPatternMatch("a*c", ImmutableList.of("abc", "abxc", "abxyc", "abyc", "adc"), trie, "a**c");
        checkPatternMatch("a*c", ImmutableList.of("abc", "abxc", "abxyc", "abyc", "adc"), trie, "a***c");
    }

    @Test
    public void testCombinedWildcards() throws TrieSpaceExhaustedException
    {
        Trie<String> trie = createTestTrie("abc", "axc", "axyc", "abcd", "axyzc");

        checkPatternMatch("a?c", ImmutableList.of("abc", "axc"), trie, "a?c");
        checkPatternMatch("a?*c", ImmutableList.of("abc", "axc", "axyc", "axyzc"), trie, "a?*c");
        checkPatternMatch("a*?c", ImmutableList.of("abc", "axc", "axyc", "axyzc"), trie, "a*?c");
    }

    @Test
    public void testQuestionBeforeStar() throws TrieSpaceExhaustedException
    {
        Trie<String> trie = createTestTrie("abc", "axc", "axyc", "abcd");

        // Pattern simplification should move ? before *
        checkPatternMatch("a?*", ImmutableList.of("abc", "abcd", "axc", "axyc"), trie, "a?*");
        checkPatternMatch("a*?", ImmutableList.of("abc", "abcd", "axc", "axyc"), trie, "a*?");
    }

    @Test
    public void testMatchAll() throws TrieSpaceExhaustedException
    {
        Trie<String> trie = createTestTrie("a", "ab", "abc", "xyz");

        // * alone should match everything
        checkPatternMatch("*", ImmutableList.of("a", "ab", "abc", "xyz"), trie, "*");
    }

    @Test
    public void testEmptyPattern() throws TrieSpaceExhaustedException
    {
        Trie<String> trie = createTestTrie("", "a", "ab");

        // Empty pattern should match only empty string
        checkPatternMatch("empty", ImmutableList.of(""), trie, "");
    }

    @Test
    public void testEmptyTrie() throws TrieSpaceExhaustedException
    {
        Trie<String> trie = createTestTrie();

        checkPatternMatch("empty trie", ImmutableList.of(), trie, "abc");
        checkPatternMatch("empty trie wildcard", ImmutableList.of(), trie, "a*");
        checkPatternMatch("empty trie star", ImmutableList.of(), trie, "*");
    }

    @Test
    public void testPrefixMatch() throws TrieSpaceExhaustedException
    {
        Trie<String> trie = createTestTrie("prefix", "prefixA", "prefixAB", "prefixABC", "other");

        checkPatternMatch("prefix*", ImmutableList.of("prefix", "prefixA", "prefixAB", "prefixABC"), trie, "prefix*");
    }

    @Test
    public void testSuffixMatch() throws TrieSpaceExhaustedException
    {
        Trie<String> trie = createTestTrie("suffix", "Asuffix", "ABsuffix", "ABCsuffix", "other");

        checkPatternMatch("*suffix", ImmutableList.of("ABCsuffix", "ABsuffix", "Asuffix", "suffix"), trie, "*suffix");
    }

    @Test
    public void testComplexPattern() throws TrieSpaceExhaustedException
    {
        Trie<String> trie = createTestTrie(
        "test", "testing", "tester", "tested",
        "best", "besting", "resting",
        "contest", "attest"
        );

        checkPatternMatch("?est*",
                          ImmutableList.of("best", "besting", "resting", "test", "tested", "tester", "testing"),
                          trie, "?est*");

        checkPatternMatch("*test*",
                          ImmutableList.of("attest", "contest", "test", "tested", "tester", "testing"),
                          trie, "*test*");

        checkPatternMatch("*est",
                          ImmutableList.of("attest", "best", "contest", "test"),
                          trie, "*est");
    }

    @Test
    public void testSingleCharacterEntries() throws TrieSpaceExhaustedException
    {
        Trie<String> trie = createTestTrie("a", "b", "c", "d");

        checkPatternMatch("?", ImmutableList.of("a", "b", "c", "d"), trie, "?");
        checkPatternMatch("a", ImmutableList.of("a"), trie, "a");
        checkPatternMatch("*", ImmutableList.of("a", "b", "c", "d"), trie, "*");
    }

    @Test
    public void testLongPatterns() throws TrieSpaceExhaustedException
    {
        Trie<String> trie = createTestTrie(
        "verylongstring",
        "verylongstringwithmore",
        "verylongstringwithevenmorecharacters"
        );

        checkPatternMatch("verylong*",
                          ImmutableList.of("verylongstring", "verylongstringwithevenmorecharacters", "verylongstringwithmore"),
                          trie, "verylong*");

        checkPatternMatch("verylongstring*",
                          ImmutableList.of("verylongstring", "verylongstringwithevenmorecharacters", "verylongstringwithmore"),
                          trie, "verylongstring*");
    }

    @Test
    public void testSpecialCharacters() throws TrieSpaceExhaustedException
    {
        Trie<String> trie = createTestTrie("a-b", "a_b", "a.b", "a b");

        checkPatternMatch("a?b", ImmutableList.of("a b", "a-b", "a.b", "a_b"), trie, "a?b");
        checkPatternMatch("a*b", ImmutableList.of("a b", "a-b", "a.b", "a_b"), trie, "a*b");
    }

    @Test
    public void testNumericStrings() throws TrieSpaceExhaustedException
    {
        Trie<String> trie = createTestTrie("100", "101", "102", "200", "201");

        checkPatternMatch("10?", ImmutableList.of("100", "101", "102"), trie, "10?");
        checkPatternMatch("?0?", ImmutableList.of("100", "101", "102", "200", "201"), trie, "?0?");
        checkPatternMatch("1*", ImmutableList.of("100", "101", "102"), trie, "1*");
    }

    @Test
    public void testMixedLengthStrings() throws TrieSpaceExhaustedException
    {
        Trie<String> trie = createTestTrie("a", "aa", "aaa", "aaaa", "b", "bb");

        checkPatternMatch("a*", ImmutableList.of("a", "aa", "aaa", "aaaa"), trie, "a*");
        checkPatternMatch("a?", ImmutableList.of("aa"), trie, "a?");
        checkPatternMatch("a??", ImmutableList.of("aaa"), trie, "a??");
        checkPatternMatch("?", ImmutableList.of("a", "b"), trie, "?");
        checkPatternMatch("??", ImmutableList.of("aa", "bb"), trie, "??");
    }

    @Test
    public void testTrailingStarBehavior() throws TrieSpaceExhaustedException
    {
        Trie<String> trie = createTestTrie("test", "test/a", "test/a/b", "test/a/b/c", "other");

        // Trailing star should match the prefix and all descendants
        checkPatternMatch("test*",
                          ImmutableList.of("test", "test/a", "test/a/b", "test/a/b/c"),
                          trie, "test*");

        checkPatternMatch("test/*",
                          ImmutableList.of("test/a", "test/a/b", "test/a/b/c"),
                          trie, "test/*");
    }

    @Test
    public void testPatternLongerThanEntries() throws TrieSpaceExhaustedException
    {
        Trie<String> trie = createTestTrie("a", "ab", "abc");

        // Pattern longer than any entry should match nothing
        checkPatternMatch("abcdef", ImmutableList.of(), trie, "abcdef");
        checkPatternMatch("abc?", ImmutableList.of(), trie, "abc?");
    }

    @Test
    public void testConsecutiveWildcards() throws TrieSpaceExhaustedException
    {
        Trie<String> trie = createTestTrie("abc", "axc", "axyc", "axyzc");

        checkPatternMatch("a??c", ImmutableList.of("axyc"), trie, "a??c");
        checkPatternMatch("a???c", ImmutableList.of("axyzc"), trie, "a???c");
        checkPatternMatch("a?*c", ImmutableList.of("abc", "axc", "axyc", "axyzc"), trie, "a?*c");
    }

    @Test
    public void testSkipToWithExactMatch() throws TrieSpaceExhaustedException
    {
        Trie<String> trie = createTestTrie("apple", "application", "apply", "banana", "band", "cherry");

        // Test subtrie operations on pattern-matched trie with trailing star
        // Note: Patterns with trailing * currently don't properly restrict subtrie bounds
        // This test documents the current behavior
        checkPatternMatch("subtrie app* [application:cherry)",
                          ImmutableList.of("application", "apply"),
                          trie, "app*",
                          TrieUtil.directComparable("application"),
                          TrieUtil.directComparable("cherry"));
    }

    @Test
    public void testSkipToWithWildcards() throws TrieSpaceExhaustedException
    {
        Trie<String> trie = createTestTrie("a", "aa", "aaa", "aaaa", "ab", "abc", "abcd", "b", "ba", "baa");

        // Pattern match with trailing star then slice
        // Note: Trailing star patterns currently match beyond subtrie bounds
        checkPatternMatch("a* [aa:ab)",
                          ImmutableList.of("aa", "aaa", "aaaa"),
                          trie, "a*",
                          TrieUtil.directComparable("aa"),
                          TrieUtil.directComparable("ab"));

        checkPatternMatch("a?* [aa:ab)",
                          ImmutableList.of("aa", "aaa", "aaaa"),
                          trie, "a?*",
                          TrieUtil.directComparable("aa"),
                          TrieUtil.directComparable("ab"));
    }

    @Test
    public void testSkipToWithQuestionMark() throws TrieSpaceExhaustedException
    {
        Trie<String> trie = createTestTrie("cat", "bat", "hat", "mat", "rat");

        // Pattern ?at then slice from 'c' to 'd' - should only include cat
        checkPatternMatch("?at [c:d)",
                          ImmutableList.of("cat"),
                          trie, "?at",
                          TrieUtil.directComparable("c"),
                          TrieUtil.directComparable("d"));

        // Pattern ?at then slice from 'h' onwards
        checkPatternMatch("?at [h:null)",
                          ImmutableList.of("hat", "mat", "rat"),
                          trie, "?at",
                          TrieUtil.directComparable("h"),
                          null);
    }

    @Test
    public void testSkipToWithComplexPattern() throws TrieSpaceExhaustedException
    {
        Trie<String> trie = createTestTrie(
        "test", "testing", "tester", "tested",
        "best", "besting", "resting",
        "contest", "attest", "fastest"
        );

        // Pattern *test* then slice [contest:fastest) - specific range
        checkPatternMatch("*test* [contest:fastest)",
                          ImmutableList.of("contest"),
                          trie, "*test*",
                          TrieUtil.directComparable("contest"),
                          TrieUtil.directComparable("fastest"));

        checkPatternMatch("?est* [best:fastest)",
                          ImmutableList.of("best", "besting"),
                          trie, "?est*",
                          TrieUtil.directComparable("best"),
                          TrieUtil.directComparable("fastest"));
    }

    @Test
    public void testSkipToEmptyRange() throws TrieSpaceExhaustedException
    {
        Trie<String> trie = createTestTrie("apple", "application", "apply", "banana");

        // Pattern match then slice to empty range
        checkPatternMatch("app* [z:zz)",
                          ImmutableList.of(),
                          trie, "app*",
                          TrieUtil.directComparable("z"),
                          TrieUtil.directComparable("zz"));
    }

    @Test
    public void testSkipToWithNullBounds() throws TrieSpaceExhaustedException
    {
        Trie<String> trie = createTestTrie("apple", "application", "apply", "banana", "band");

        // Pattern match then slice with null bounds (should return all matches)
        checkPatternMatch("app* [null:null)",
                          ImmutableList.of("apple", "application", "apply"),
                          trie, "app*",
                          null,
                          null);

        // Pattern match then slice with null start - [null:b) means < "b", but "banana" and "band" start with "b"
        checkPatternMatch("*an* [null:ban)",
                          ImmutableList.of(),
                          trie, "*an*",
                          null,
                          TrieUtil.directComparable("ban"));

        // Pattern match then slice with null end
        checkPatternMatch("*an* [ban:null)",
                          ImmutableList.of("banana", "band"),
                          trie, "*an*",
                          TrieUtil.directComparable("ban"),
                          null);
    }

    @Test
    public void testSkipToMultipleRanges() throws TrieSpaceExhaustedException
    {
        Trie<String> trie = createTestTrie(
        "a1", "a2", "a3", "b1", "b2", "b3", "c1", "c2", "c3"
        );

        // Pattern a* then slice [a1:a3) - includes a1, a2 but excludes a3
        checkPatternMatch("a* [a1:a3)",
                          ImmutableList.of("a1", "a2"),
                          trie, "a*",
                          TrieUtil.directComparable("a1"),
                          TrieUtil.directComparable("a3"));

        // Pattern ?1 then slice [a:c) - includes a1, b1 but excludes c1
        checkPatternMatch("?1 [a:c)",
                          ImmutableList.of("a1", "b1"),
                          trie, "?1",
                          TrieUtil.directComparable("a"),
                          TrieUtil.directComparable("c"));
    }

    @Test
    public void testSkipToWithTrailingStar() throws TrieSpaceExhaustedException
    {
        Trie<String> trie = createTestTrie("test", "test/a", "test/a/b", "test/a/b/c", "other");

        // Trailing star with slice - test specific range within pattern matches
        checkPatternMatch("test* [test/a:test/a/b)",
                          ImmutableList.of("test/a"),
                          trie, "test*",
                          TrieUtil.directComparable("test/a"),
                          TrieUtil.directComparable("test/a/b"));
    }

    private void checkPatternMatch(String message, List<String> expected, Trie<String> trie, String pattern,
                                   org.apache.cassandra.utils.bytecomparable.ByteComparable left,
                                   org.apache.cassandra.utils.bytecomparable.ByteComparable right)
    {
        // Test all matcher choices
        for (int algoChoice : ALGO_CHOICES)
        {
            String algoMessage = message + " [algo=" + algoChoice + "]";
            Trie<String> matched = patternMatched(trie, pattern, algoChoice);
            // Use slice with inclusive left, exclusive right to match subtrie semantics
            Trie<String> sliced = matched.slice(left, true, right, false);

            List<String> forwardResult = toList(sliced, Direction.FORWARD);
            List<String> reverseResult = toList(sliced, Direction.REVERSE);
            List<String> expectedReverse = expected.stream()
                                                   .sorted((a, b) -> b.compareTo(a))
                                                   .collect(Collectors.toList());

            assertEquals(algoMessage + " forward", expected, forwardResult);
            assertEquals(algoMessage + " reverse", expectedReverse, reverseResult);
        }
    }

    @Test
    public void testPattern64BytesExact() throws TrieSpaceExhaustedException
    {
        // Create strings that are exactly 64 bytes long
        String base64 = "a".repeat(64);
        String match64_1 = "a".repeat(63) + "b";
        String match64_2 = "a".repeat(63) + "c";
        String noMatch64 = "b".repeat(64);

        Trie<String> trie = createTestTrie(base64, match64_1, match64_2, noMatch64);

        // Pattern of 64 characters; the single-character wildcard takes positions 63 and 64
        String pattern64 = "a".repeat(63) + "?";
        checkPatternMatch("64 bytes exact",
                          ImmutableList.of(base64, match64_1, match64_2),
                          trie, pattern64);

        // Pattern 64 bytes with trailing star
        String pattern64Star = "a".repeat(63) + "*";
        checkPatternMatch("64 bytes with trailing star",
                          ImmutableList.of(base64, match64_1, match64_2),
                          trie, pattern64Star);
    }

    @Test
    public void testPattern65BytesAndOver() throws TrieSpaceExhaustedException
    {
        // Create strings over 64 bytes - forces use of PatternMatcherLong
        String base64 = "a".repeat(64);
        String base65 = "a".repeat(65);
        String match65_1 = "a".repeat(64) + "b";
        String match65_2 = "a".repeat(64) + "c";
        String match70 = "a".repeat(70);
        String noMatch = "b".repeat(65);

        Trie<String> trie = createTestTrie(base64, base65, match65_1, match65_2, match70, noMatch);

        // Pattern 65 bytes - first byte in second long (only PatternMatcherLong supports this)
        String pattern65 = "a".repeat(64) + "?";
        checkPatternMatch("65 bytes pattern", ImmutableList.of(base65, match65_1, match65_2), trie, pattern65);

        // Pattern with wildcard at 64th position (boundary)
        String patternBoundary = "a".repeat(63) + "?" + "a";
        checkPatternMatch("wildcard at 64th byte", ImmutableList.of(base65), trie, patternBoundary);

        // Pattern 70 bytes
        String pattern70 = "a".repeat(70);
        checkPatternMatch("70 bytes exact", ImmutableList.of(match70), trie, pattern70);

        // Pattern 65 bytes with trailing star
        String pattern65Star = "a".repeat(65) + "*";
        checkPatternMatch("65 bytes with star", ImmutableList.of(base65, match70), trie, pattern65Star);

        // Pattern 64 bytes with trailing star
        String pattern64Star = "a".repeat(64) + "*";
        checkPatternMatch("64 bytes with star", ImmutableList.of(base64, base65, match65_1, match65_2, match70), trie, pattern64Star);

        // Pattern with wildcard at 64th position (boundary)
        String patternBoundaryStar = "a".repeat(63) + "?" + "*";
        checkPatternMatch("wildcard at 64th byte plus star", ImmutableList.of(base64, base65, match65_1, match65_2, match70), trie, patternBoundaryStar);

        // Pattern with star at 65th position + char
        String patternBoundaryStarA = "a".repeat(63) + "?" + "*" + "a";
        checkPatternMatch("wildcard at 64th byte plus star then A", ImmutableList.of(base65, match70), trie, patternBoundaryStarA);
    }

    @Test
    public void testPatternCarryLogic() throws TrieSpaceExhaustedException
    {
        // Test the carry logic in PatternMatcherLong.descend
        // Carry happens when a match at position 63 needs to set bit 64 (in next long)

        String match63 = "a".repeat(63);
        String match64 = "a".repeat(64);
        String match65 = "a".repeat(65);
        String match128 = "a".repeat(128);
        String noMatch = "b".repeat(65);

        Trie<String> trie = createTestTrie(match63, match64, match65, match128, noMatch);

        // Pattern that causes carry from first long to second long
        String patternCarry64 = "a".repeat(64);
        checkPatternMatch("carry at 64 bytes",
                          ImmutableList.of(match64),
                          trie, patternCarry64);

        // Pattern with trailing star to match 64+ bytes
        String patternCarry64Star = "a".repeat(64) + "*";
        checkPatternMatch("carry at 64 bytes with star", ImmutableList.of(match64, match65, match128), trie, patternCarry64Star);

        // Pattern with star at position 63 - tests carry with star
        String patternStarCarry = "a".repeat(63) + "*";
        checkPatternMatch("star at 63rd byte with carry",
                          ImmutableList.of(match63, match64, match65, match128),
                          trie, patternStarCarry);

        // Pattern with ? at position 63 followed by more chars
        // This tests carry logic: pattern is 65 bytes, should match only 65-byte strings
        String patternQuestionCarry = "a".repeat(63) + "?" + "a";
        checkPatternMatch("? at 63rd byte with carry", ImmutableList.of(match65), trie, patternQuestionCarry);

        // Pattern with ? at position 64 (tests carry into second long)
        String patternQuestion64 = "a".repeat(64) + "?";
        checkPatternMatch("? at 64th byte", ImmutableList.of(match65), trie, patternQuestion64);
    }

    @Test
    public void testPattern128BytesBoundary() throws TrieSpaceExhaustedException
    {
        // Test patterns at 128 bytes boundary (second long boundary)
        String match127 = "a".repeat(127);
        String match128 = "a".repeat(128);
        String match129 = "a".repeat(129);
        String match192 = "a".repeat(192);

        Trie<String> trie = createTestTrie(match127, match128, match129, match192);

        // Pattern exactly 128 bytes
        String pattern128 = "a".repeat(128);
        checkPatternMatch("128 bytes exact", ImmutableList.of(match128), trie, pattern128);

        // Pattern 128 bytes with trailing star
        String pattern128Star = "a".repeat(128) + "*";
        checkPatternMatch("128 bytes with star", ImmutableList.of(match128, match129, match192), trie, pattern128Star);

        // Pattern with wildcard at 127th position
        String patternBoundary127 = "a".repeat(127) + "?";
        checkPatternMatch("wildcard at 127th byte", ImmutableList.of(match128), trie, patternBoundary127);

        // Pattern with star at 127th position
        String patternStar127 = "a".repeat(127) + "*";
        checkPatternMatch("star at 127th byte", ImmutableList.of(match127, match128, match129, match192), trie, patternStar127);
    }

    @Test
    public void testLongPatternWithWildcardsAtBoundaries() throws TrieSpaceExhaustedException
    {
        // Test wildcards specifically at 64-byte boundaries
        String base = "a".repeat(62) + "xy" + "b".repeat(62) + "zw" + "c".repeat(60);
        String match1 = "a".repeat(62) + "ab" + "b".repeat(62) + "cd" + "c".repeat(60);
        String match2 = "a".repeat(62) + "xy" + "b".repeat(62) + "ab" + "c".repeat(60);

        Trie<String> trie = createTestTrie(base, match1, match2);

        // Wildcard at position 63 (last of first long)
        String pattern63 = "a".repeat(62) + "?" + "?" + "b".repeat(62) + "??" + "c".repeat(60);
        checkPatternMatch("wildcards at boundaries 63-64, 127-128",
                          ImmutableList.of(base, match1, match2),
                          trie, pattern63);

        // Star spanning boundary
        String patternStarBoundary = "a".repeat(62) + "*" + "c".repeat(60);
        checkPatternMatch("star spanning 64-byte boundary",
                          ImmutableList.of(base, match1, match2),
                          trie, patternStarBoundary);
    }

    @Test
    public void testVeryLongPatterns() throws TrieSpaceExhaustedException
    {
        // Test patterns over 192 bytes (3 longs)
        String match200 = "a".repeat(200);
        String match250 = "a".repeat(250);
        String match255 = "a".repeat(254) + "b";
        String noMatch = "b".repeat(200);

        Trie<String> trie = createTestTrie(match200, match250, match255, noMatch);

        // Pattern 200 bytes
        String pattern200 = "a".repeat(200);
        checkPatternMatch("200 bytes exact",
                          ImmutableList.of(match200),
                          trie, pattern200);

        // Pattern 200 bytes with trailing star
        String pattern200Star = "a".repeat(200) + "*";
        checkPatternMatch("200 bytes with star",
                          ImmutableList.of(match200, match250, match255),
                          trie, pattern200Star);

        // Pattern with wildcards in third long
        String pattern195 = "a".repeat(190) + "?????";
        checkPatternMatch("wildcards in third long",
                          ImmutableList.of(),
                          trie, pattern195);

        // Pattern with star at 192nd byte (third long boundary)
        String patternStar192 = "a".repeat(192) + "*";
        checkPatternMatch("star at 192nd byte",
                          ImmutableList.of(match200, match250, match255),
                          trie, patternStar192);

        // Pattern with star at 192nd byte (third long boundary)
        String patternStar192a = "a".repeat(192) + "*a";
        checkPatternMatch("star at 192nd byte plus a",
                          ImmutableList.of(match200, match250),
                          trie, patternStar192a);
    }

    @Test
    public void testCarryThenStar() throws TrieSpaceExhaustedException
    {
        // Test carry logic when star is at boundary and next position is also star
        String match64 = "a".repeat(64);
        String match65 = "a".repeat(65);
        String match100 = "a".repeat(100);
        String matchab64 = "a".repeat(63) + "b".repeat(63) + "a";
        String matchab65 = "a".repeat(64) + "b".repeat(62) + "a";
        String nomatch63 = "a".repeat(63) + "b".repeat(64);
        String nomatch64 = "a".repeat(64) + "b".repeat(63);
        String nomatch65 = "a".repeat(65) + "b".repeat(63);

        Trie<String> trie = createTestTrie(match64, match65, match100, matchab65, matchab64, nomatch64, nomatch63, nomatch65);

        // Multiple stars around boundary
        String patternMultiStar = "a".repeat(62) + "*a*a";
        checkPatternMatch("multiple stars at boundary",
                          ImmutableList.of(match64, matchab65, match65, matchab64, match100),
                          trie, patternMultiStar);

        // Multiple stars around boundary
        String patternMultiStar2 = "a".repeat(63) + "*a*a";
        checkPatternMatch("multiple stars at boundary",
                          ImmutableList.of(matchab65, match65, match100),
                          trie, patternMultiStar2);
    }

    @Test
    public void testMixedWildcardsAcrossBoundaries() throws TrieSpaceExhaustedException
    {
        // Test complex patterns with mixed wildcards across boundaries
        String match = "a".repeat(62) + "bc" + "d".repeat(62) + "ef" + "g".repeat(60);
        String match2 = "a".repeat(62) + "xy" + "d".repeat(62) + "zw" + "g".repeat(60);

        Trie<String> trie = createTestTrie(match, match2);

        // Pattern: 62 a's, then ??, then 62 d's, then ??, then 60 g's
        String pattern = "a".repeat(62) + "??" + "d".repeat(62) + "??" + "g".repeat(60);
        checkPatternMatch("mixed wildcards across boundaries",
            ImmutableList.of(match, match2),
            trie, pattern);

        // Pattern with star before boundary and ? after
        String patternStarQuestion = "a".repeat(62) + "*" + "d".repeat(62) + "?" + "?" + "g".repeat(60);
        checkPatternMatch("star before boundary, ? after",
            ImmutableList.of(match, match2),
            trie, patternStarQuestion);
    }

    @Test
    public void testRandomPatternsWithQuickTheories()
    {
        qt()
            .forAll(
                strings().allPossible().ofLengthBetween(1, 10),
                lists().of(strings().allPossible().ofLengthBetween(0, 100)).ofSizeBetween(5, 20)
            )
            .checkAssert((pattern, testStrings) -> {
                try {
                    // Create trie with test strings
                    Trie<String> trie = createTestTrie(testStrings.toArray(new String[0]));

                    // Get expected matches by checking every key of the trie (in its order, without duplicates) with
                    // the reference glob matcher. (A regular expression does not work here: the random patterns
                    // contain regex syntax.)
                    List<String> expected = matching(trie.values(Direction.FORWARD), pattern.getBytes(StandardCharsets.UTF_8));

                    // Test all matcher choices
                    for (int algoChoice : ALGO_CHOICES)
                    {
                        Trie<String> matched = patternMatched(trie, pattern, algoChoice);
                        List<String> actual = toList(matched, Direction.FORWARD);

                        assertEquals("Pattern: " + pattern + " [algo=" + algoChoice + "]",
                                   expected, actual);
                    }
                } catch (TrieSpaceExhaustedException e) {
                    throw new RuntimeException(e);
                }
            });
    }

    @Test
    public void testRandomLongPatternsWithQuickTheories()
    {
        qt()
            .forAll(
                integers().between(50, 150),
                lists().of(integers().between(50, 150)).ofSizeBetween(5, 15)
            )
            .checkAssert((patternLength, stringLengths) -> {
                try {
                    // Create pattern with 'a's and wildcards at boundaries
                    StringBuilder patternBuilder = new StringBuilder();
                    for (int i = 0; i < patternLength; i++) {
                        if (i == 63 || i == 64 || i == 127 || i == 128) {
                            patternBuilder.append('?');
                        } else {
                            patternBuilder.append('a');
                        }
                    }
                    String pattern = patternBuilder.toString();

                    // Create test strings of various lengths
                    List<String> testStrings = new java.util.ArrayList<>();
                    for (int length : stringLengths) {
                        testStrings.add("a".repeat(length));
                    }

                    // Convert pattern to regex
                    String regex = pattern.replace("?", ".").replace("*", ".*");
                    java.util.regex.Pattern javaPattern = java.util.regex.Pattern.compile("^" + regex + "$");

                    // Create trie
                    Trie<String> trie = createTestTrie(testStrings.toArray(new String[0]));

                    // Get expected matches
                    List<String> expected = testStrings.stream()
                                                      .filter(s -> javaPattern.matcher(s).matches())
                                                      .sorted()
                                                      .distinct()
                                                      .collect(Collectors.toList());

                    // Test the long[] matcher for long patterns
                    Trie<String> matched = patternMatched(trie, pattern, 1);
                    List<String> actual = toList(matched, Direction.FORWARD);

                    assertEquals("Pattern length: " + patternLength + " [algo=1]",
                               expected, actual);
                } catch (TrieSpaceExhaustedException e) {
                    throw new RuntimeException(e);
                }
            });
    }

    @Test
    public void testRandomBoundaryPatternsWithQuickTheories()
    {
        qt()
            .forAll(
                integers().between(60, 68),
                booleans().all(),
                booleans().all()
            )
            .checkAssert((baseLength, addWildcard, addStar) -> {
                try {
                    // Create pattern around 64-byte boundary
                    StringBuilder patternBuilder = new StringBuilder("a".repeat(baseLength));
                    if (addWildcard) {
                        patternBuilder.append('?');
                    }
                    if (addStar) {
                        patternBuilder.append('*');
                    }
                    String pattern = patternBuilder.toString();

                    // Create test strings
                    List<String> testStrings = Arrays.asList(
                        "a".repeat(baseLength),
                        "a".repeat(baseLength + 1),
                        "a".repeat(baseLength + 2),
                        "a".repeat(baseLength + 10),
                        "a".repeat(baseLength - 1),
                        "a".repeat(64),
                        "a".repeat(65),
                        "a".repeat(128)
                    );

                    // Convert pattern to regex
                    String regex = pattern.replace("?", ".").replace("*", ".*");
                    java.util.regex.Pattern javaPattern = java.util.regex.Pattern.compile("^" + regex + "$");

                    // Create trie
                    Trie<String> trie = createTestTrie(testStrings.toArray(new String[0]));

                    // Get expected matches
                    List<String> expected = testStrings.stream()
                                                      .filter(s -> javaPattern.matcher(s).matches())
                                                      .sorted()
                                                      .distinct()
                                                      .collect(Collectors.toList());

                    // Test appropriate algorithm based on pattern length
                    for (int algoChoice : ALGO_CHOICES)
                    {
                        Trie<String> matched = patternMatched(trie, pattern, algoChoice);
                        List<String> actual = toList(matched, Direction.FORWARD);
                        assertEquals("Pattern: " + pattern + " [algo=" + algoChoice + "]",
                                     expected, actual);
                    }
                } catch (TrieSpaceExhaustedException e) {
                    throw new RuntimeException(e);
                }
            });
    }

    /// Matcher choices for [#patternMatched(Trie, TriePattern, int)]: 0 uses [Trie#patternMatched], which picks
    /// [PatternMatcher64] for patterns of up to 63 positions and [PatternMatcherLong] otherwise, and 1 forces
    /// [PatternMatcherLong], so that it is also tested with short patterns.
    private static final int[] ALGO_CHOICES = { 0, 1 };

    static <T> Trie<T> patternMatched(Trie<T> trie, String pattern, int algoChoice)
    {
        return patternMatched(trie, TriePattern.glob(pattern), algoChoice);
    }

    static <T> Trie<T> patternMatched(Trie<T> trie, TriePattern pattern, int algoChoice)
    {
        switch (algoChoice)
        {
            case 0:
                return trie.patternMatched(pattern);
            case 1:
                return dir -> new PatternCursor<>(trie.cursor(dir), new PatternMatcherLong(pattern));
            default:
                throw new AssertionError();
        }
    }

    private static final long SEED = CassandraRelevantProperties.TEST_RANDOM_SEED.getLong(System.nanoTime());

    @Test
    public void testImplementationSelection()
    {
        Trie<String> trie = Trie.empty(VERSION);
        String p63 = "a".repeat(63);
        String p64 = "a".repeat(64);
        String p64Star = "a".repeat(63) + "*";

        assertImplementation(PatternMatcher64.class, trie, "abc", 0);
        assertImplementation(PatternMatcherLong.class, trie, "abc", 1);

        assertImplementation(PatternMatcher64.class, trie, p63, 0);
        assertImplementation(PatternMatcherLong.class, trie, p64, 0);
        assertImplementation(PatternMatcherLong.class, trie, p64Star, 0);

        // Selection is done on the normalized pattern.
        assertImplementation(PatternMatcher64.class, trie, "a".repeat(62) + "***", 0);
        // A single-character wildcard takes two positions.
        assertImplementation(PatternMatcherLong.class, trie, TriePattern.builder().literal("a".repeat(62)).anyChar().build(), 0);
    }

    private static void assertImplementation(Class<?> expected, Trie<String> trie, String pattern, int algoChoice)
    {
        assertImplementation(expected, trie, TriePattern.glob(pattern), algoChoice);
    }

    private static void assertImplementation(Class<?> expected, Trie<String> trie, TriePattern pattern, int algoChoice)
    {
        assertEquals(pattern + " [algo=" + algoChoice + "]",
                     expected,
                     ((PatternCursor<?>) patternMatched(trie, pattern, algoChoice).makeCursor(Direction.FORWARD)).matcher.getClass());
    }

    /// Patterns of 33 to 63 bytes with a star exactly 32 positions before a literal last byte. The trailing star check
    /// must look at the last position of the pattern and not at one that aliases it in 32-bit arithmetic.
    @Test
    public void testStarAliasingLastPosition() throws TrieSpaceExhaustedException
    {
        for (int length = 33; length < 64; ++length)
        {
            int starPosition = length - 33;
            String head = "x".repeat(starPosition);
            String middle = "y".repeat(length - starPosition - 2);
            String pattern = head + "*" + middle + "z";
            assertEquals(length, pattern.length());

            String exact = head + middle + "z";
            String withGap = head + "w" + middle + "z";
            String missingLast = head + middle;
            String belowMissingLast = missingLast + "q";
            Trie<String> trie = createTestTrie(exact, withGap, missingLast, belowMissingLast);
            checkPatternMatch(pattern, ImmutableList.of(exact, withGap), trie, pattern);
        }
    }

    /// Multi-byte UTF-8 characters put bytes >= 0x80 on the trie transitions and in the pattern.
    @Test
    public void testNonAsciiLiterals() throws TrieSpaceExhaustedException
    {
        String cafeAcute = "café";
        String cafeEuro = "caf€";
        String naive = "naïve";
        String uber = "über";
        String euro = "€uro";
        Trie<String> trie = createTestTrie(cafeAcute, "cafe", cafeEuro, naive, uber, "uber", euro);

        checkPatternMatch("exact", ImmutableList.of(cafeAcute), trie, cafeAcute);
        checkPatternMatch("suffix", ImmutableList.of(cafeAcute), trie, "*é");
        checkPatternMatch("prefix", ImmutableList.of("cafe", cafeAcute, cafeEuro), trie, "caf*");
        checkPatternMatch("infix", ImmutableList.of(naive), trie, "*ï*");
        checkPatternMatch("leading", ImmutableList.of(euro), trie, "€*");
        checkPatternMatch("anywhere", ImmutableList.of(cafeEuro, euro), trie, "*€*");
        checkPatternMatch("non-ascii prefix", ImmutableList.of(uber), trie, "ü*r");
    }

    @Test
    public void testTailTrie() throws TrieSpaceExhaustedException
    {
        Trie<String> trie = createTestTrie("ab", "abxb", "abxc", "ac", "acb", "b");
        for (int algo : ALGO_CHOICES)
        {
            String msg = "a*b [algo=" + algo + "] tail ";
            Trie<String> matched = patternMatched(trie, "a*b", algo);
            checkValues(msg + "a", ImmutableList.of("ab", "abxb", "acb"), matched.tailTrie(directComparable("a")));
            checkValues(msg + "ab", ImmutableList.of("ab", "abxb"), matched.tailTrie(directComparable("ab")));
            checkValues(msg + "abx", ImmutableList.of("abxb"), matched.tailTrie(directComparable("abx")));
            checkValues(msg + "acb", ImmutableList.of("acb"), matched.tailTrie(directComparable("acb")));

            msg = "ab* [algo=" + algo + "] tail ";
            matched = patternMatched(trie, "ab*", algo);
            checkValues(msg + "a", ImmutableList.of("ab", "abxb", "abxc"), matched.tailTrie(directComparable("a")));
            checkValues(msg + "ab", ImmutableList.of("ab", "abxb", "abxc"), matched.tailTrie(directComparable("ab")));
            checkValues(msg + "abx", ImmutableList.of("abxb", "abxc"), matched.tailTrie(directComparable("abx")));
        }
    }

    /// Tail tries are collected during an iteration and only examined after the iteration has moved on. They must not
    /// share mutable state with the cursor that produced them.
    @Test
    public void testTailTriesAreIndependentOfSource() throws TrieSpaceExhaustedException
    {
        Trie<String> trie = createTestTrie("ab", "abxb", "abxc", "ac", "acb", "b");
        for (int algo : ALGO_CHOICES)
        {
            String msg = "a*b [algo=" + algo + "]";
            List<Map.Entry<ByteComparable.Preencoded, Trie<String>>> tails =
                Iterables.toList(patternMatched(trie, "a*b", algo).tailTries(Direction.FORWARD, x -> true));
            assertEquals(msg, ImmutableList.of("ab", "acb"), tails.stream()
                                                                .map(e -> decode(e.getKey()))
                                                                .collect(Collectors.toList()));
            checkValues(msg + " tail ab", ImmutableList.of("ab", "abxb"), tails.get(0).getValue());
            checkValues(msg + " tail acb", ImmutableList.of("acb"), tails.get(1).getValue());
        }
    }

    /// In reverse direction, ordered tries present the content of prefixes on the return path, after the node's
    /// children. A cursor that skips to the next pattern byte must not try to descend from such a position.
    @Test
    public void testNoDescentFromReturnPath() throws TrieSpaceExhaustedException
    {
        List<String> keys = ImmutableList.of("", "a", "ab", "abc", "abcd", "abd", "b", "bc");
        Trie<String> source = createTestTrie(keys.toArray(new String[0]));
        Trie<String> strict = dir -> new NoDescentFromReturnPathCursor<>(source.cursor(dir));
        for (String pattern : new String[] { "", "a", "ab", "abc", "abcd", "a?c", "b", "bc", "ab*" })
            for (int algo : ALGO_CHOICES)
                checkValues(pattern + " [algo=" + algo + "]",
                            matching(keys, pattern.getBytes(StandardCharsets.UTF_8)),
                            patternMatched(strict, pattern, algo));
    }

    /// Delegating cursor that fails if it is asked to descend from a position on the return path.
    private static class NoDescentFromReturnPathCursor<T> implements Cursor<T>
    {
        final Cursor<T> source;

        NoDescentFromReturnPathCursor(Cursor<T> source)
        {
            this.source = source;
        }

        @Override
        public long encodedPosition()
        {
            return source.encodedPosition();
        }

        @Override
        public T content()
        {
            return source.content();
        }

        @Override
        public ByteComparable.Version byteComparableVersion()
        {
            return source.byteComparableVersion();
        }

        @Override
        public long advance()
        {
            return source.advance();
        }

        @Override
        public long advanceMultiple(TransitionsReceiver receiver)
        {
            return source.advanceMultiple(receiver);
        }

        @Override
        public long skipTo(long skipPosition)
        {
            long position = source.encodedPosition();
            assertFalse("Descent to " + Cursor.toString(skipPosition) + " requested from return path position " + Cursor.toString(position),
                        Cursor.isOnReturnPath(position) && Cursor.depth(skipPosition) > Cursor.depth(position));
            return source.skipTo(skipPosition);
        }

        @Override
        public Cursor<T> tailCursor(Direction direction)
        {
            return new NoDescentFromReturnPathCursor<>(source.tailCursor(direction));
        }
    }

    /// Checks that the trie lists the given values in forward direction and the reverse of the list in reverse.
    /// Only valid for ordered tries.
    private static void checkValues(String msg, List<String> expectedForward, Trie<String> trie)
    {
        List<String> expectedReverse = Lists.reverse(expectedForward);
        assertEquals(msg + " forward", expectedForward, trie == null ? ImmutableList.of() : toList(trie, Direction.FORWARD));
        assertEquals(msg + " reverse", expectedReverse, trie == null ? ImmutableList.of() : toList(trie, Direction.REVERSE));
    }

    // Randomized tests comparing all implementations against a simple glob matcher, over different kinds of source
    // tries and using all the ways a pattern-matched trie can be walked (values, entries, advance/advanceMultiple,
    // slices, tailTrie, tailTries).

    @Test
    public void testRandomShortPatterns() throws TrieSpaceExhaustedException
    {
        Random rand = new Random(SEED);
        for (int i = 0; i < 300; ++i)
            checkRandomCase(rand, "seed " + SEED + " short case " + i, "abc", 0, 10);
    }

    @Test
    public void testRandomLongPatterns() throws TrieSpaceExhaustedException
    {
        Random rand = new Random(SEED + 1);
        for (int i = 0; i < 150; ++i)
            checkRandomCase(rand, "seed " + SEED + " long case " + i, "ab", 25, 140);
    }

    @Test
    public void testTriePatternNormalization()
    {
        // Runs of stars collapse, and single-character wildcards move before them.
        assertEquals(TriePattern.glob("?*"), TriePattern.glob("*?"));
        assertEquals(TriePattern.glob("a*"), TriePattern.glob("a**"));
        assertEquals(TriePattern.glob("a??*b"), TriePattern.glob("a*?*?b"));

        // A character is a start byte and a loop of continuation bytes, which a following star makes unnecessary.
        assertArrayEquals(new int[]{ 'a', TriePattern.CHAR_START, TriePattern.CHAR_CONTINUATION, 'b' },
                          TriePattern.glob("a?b").elements);
        assertArrayEquals(new int[]{ 'a', TriePattern.CHAR_START, TriePattern.CHAR_CONTINUATION },
                          TriePattern.glob("a?").elements);
        assertArrayEquals(new int[]{ TriePattern.CHAR_START, TriePattern.CHAR_CONTINUATION, TriePattern.CHAR_START,
                                     TriePattern.ANY_STRING, 'a' },
                          TriePattern.glob("*?**?a").elements);

        // Literals are never wildcards.
        assertArrayEquals(new int[]{ '*', '?', '_', '%' }, TriePattern.builder().literal("*?_%").build().elements);
        assertEquals("a?\\x3f*\\x2aé", TriePattern.builder().literal("a").anyChar().literal("?").anyString().literal("*é").build().toString());

        // The literal prefix ends with the first wildcard.
        assertArrayEquals("ab".getBytes(StandardCharsets.UTF_8), TriePattern.glob("ab?c*").literalPrefix());
        assertArrayEquals("abc".getBytes(StandardCharsets.UTF_8), TriePattern.glob("abc").literalPrefix());
        assertArrayEquals(new byte[0], TriePattern.glob("*abc").literalPrefix());
        assertArrayEquals("é".getBytes(StandardCharsets.UTF_8), TriePattern.glob("é?").literalPrefix());
        assertArrayEquals(new int[]{ 0xC3, 0xA9 }, TriePattern.builder().literal("é").build().elements);
    }

    /// The literals of a pattern must be valid UTF-8, which puts the wildcards on character boundaries.
    @Test
    public void testInvalidUtf8Rejected()
    {
        // complete characters, also when added byte by byte
        TriePattern.builder().literal((byte) 0xC3).literal((byte) 0xA9).anyChar().build();
        TriePattern.builder().anyString().literal(new byte[]{ (byte) 0xF0, (byte) 0x9F, (byte) 0x98, (byte) 0x80 }).build();

        assertInvalidUtf8(TriePattern.builder().literal((byte) 0xC3)); // incomplete character
        assertInvalidUtf8(TriePattern.builder().literal((byte) 0xC3).anyString()); // incomplete before a wildcard
        assertInvalidUtf8(TriePattern.builder().literal((byte) 0xC3).anyChar().literal((byte) 0xA9)); // split by a wildcard
        assertInvalidUtf8(TriePattern.builder().anyChar().literal((byte) 0xA9)); // continuation byte after a wildcard
        assertInvalidUtf8(TriePattern.builder().literal((byte) 0xFF)); // never valid
        assertInvalidUtf8(TriePattern.builder().literal(new byte[]{ (byte) 0xC0, (byte) 0x80 })); // overlong encoding
        assertInvalidUtf8(TriePattern.builder().literal(new byte[]{ (byte) 0xED, (byte) 0xA0, (byte) 0x80 })); // surrogate
    }

    private static void assertInvalidUtf8(TriePattern.Builder builder)
    {
        assertThatThrownBy(builder::build).isInstanceOf(IllegalArgumentException.class)
                                          .hasMessageContaining("not valid UTF-8");
    }

    /// Single-character wildcards take two positions, a start byte and a loop of continuation bytes (one when followed
    /// by a star). Puts them at all offsets around the word boundaries of the matchers' states, over keys whose
    /// characters have all encoding lengths, so that the loops are entered and left across the boundaries.
    @Test
    public void testCharacterWildcardsAtWordBoundaries() throws TrieSpaceExhaustedException
    {
        String[] chars = { "a", "é", "€", "😀" }; // 1, 2, 3 and 4 bytes
        for (int offset : IntStream.concat(IntStream.range(58, 70), IntStream.range(122, 134)).toArray())
        {
            String head = "a".repeat(offset);
            List<String> keys = new ArrayList<>();
            keys.add(head);
            keys.add(head + "b");
            for (String c : chars)
            {
                keys.add(head + c);
                keys.add(head + c + "b");
                for (String d : chars)
                {
                    keys.add(head + c + d);
                    keys.add(head + c + d + "b");
                }
            }
            Trie<String> trie = createTestTrie(keys.toArray(new String[0]));
            for (String tail : new String[]{ "?", "??", "?b", "??b", "?*", "?*b", "*?b" })
            {
                String pattern = head + tail;
                checkPatternMatch(offset + " " + tail, matching(keys, pattern.getBytes(StandardCharsets.UTF_8)), trie, pattern);
            }
        }
    }

    /// Patterns with any-character wildcards over UTF-8 keys whose characters have all encoding lengths: the wildcard
    /// must match a whole character, and literal `*` and `?` must only match themselves. The reference matcher takes
    /// the length of each character from the UTF-8 encoding of its first byte.
    @Test
    public void testRandomUtf8Patterns() throws TrieSpaceExhaustedException
    {
        String[] alphabet = { "a", "b", "*", "?", "é", "€", "😀" }; // 1, 1, 1, 1, 2, 3 and 4 bytes
        Random rand = new Random(SEED + 2);
        for (int i = 0; i < 200; ++i)
        {
            // Long patterns of multi-byte characters need more than 64 positions and are served by the long cursors.
            int length = rand.nextInt(4) == 0 ? 20 + rand.nextInt(20) : rand.nextInt(8);
            TriePattern.Builder builder = TriePattern.builder();
            List<Integer> tokens = new ArrayList<>(); // the pattern for the reference matcher
            List<List<String>> keyParts = new ArrayList<>(); // choices of key text for each pattern element
            for (int j = 0; j < length; ++j)
            {
                int r = rand.nextInt(20);
                if (r < 3)
                {
                    builder.anyString();
                    tokens.add(REF_ANY_STRING);
                    keyParts.add(null);
                }
                else if (r < 7)
                {
                    builder.anyChar();
                    tokens.add(REF_ANY_CHAR);
                    keyParts.add(Arrays.asList(alphabet));
                }
                else
                {
                    String literal = alphabet[rand.nextInt(alphabet.length)];
                    builder.literal(literal);
                    for (byte b : literal.getBytes(StandardCharsets.UTF_8))
                        tokens.add(b & 0xFF);
                    keyParts.add(ImmutableList.of(literal));
                }
            }
            TriePattern pattern = builder.build();
            List<String> keys = randomUtf8Keys(rand, keyParts, alphabet);

            String context = "seed " + SEED + " UTF-8 case " + i + " pattern " + pattern;
            for (Map.Entry<String, Trie<String>> source : randomSources(rand, keys).entrySet())
                for (int algo : ALGO_CHOICES)
                    verifyAgainstReference(context + " " + source.getKey() + " [algo=" + algo + "]",
                                           source.getValue(), pattern, key -> referenceMatches(tokens, key),
                                           algo, keys, rand);
        }
    }

    // Tokens of the reference matcher; values 0-255 are literal bytes.
    private static final int REF_ANY_STRING = -1;
    private static final int REF_ANY_CHAR = -2;

    /// Reference matcher of the tokens against the whole key, by dynamic programming over (token, key position).
    private static boolean referenceMatches(List<Integer> tokens, byte[] key)
    {
        int t = tokens.size();
        int k = key.length;
        // matches[i][j]: whether tokens from i match the key bytes from j
        boolean[][] matches = new boolean[t + 1][k + 1];
        matches[t][k] = true;
        for (int i = t - 1; i >= 0; --i)
        {
            int token = tokens.get(i);
            for (int j = k; j >= 0; --j)
            {
                switch (token)
                {
                    case REF_ANY_STRING:
                        matches[i][j] = matches[i + 1][j] || j < k && matches[i][j + 1];
                        break;
                    case REF_ANY_CHAR:
                    {
                        int length = j < k ? utf8Length(key, j) : -1;
                        matches[i][j] = length > 0 && matches[i + 1][j + length];
                        break;
                    }
                    default:
                        matches[i][j] = j < k && (key[j] & 0xFF) == token && matches[i + 1][j + 1];
                }
            }
        }
        return matches[0][0];
    }

    /// The length of the UTF-8 character starting at the given position, or -1 if there isn't a valid one.
    private static int utf8Length(byte[] key, int pos)
    {
        int lead = key[pos] & 0xFF;
        int length = lead < 0x80 ? 1 : lead < 0xC0 ? -1 : lead < 0xE0 ? 2 : lead < 0xF0 ? 3 : lead < 0xF8 ? 4 : -1;
        if (length < 0 || pos + length > key.length)
            return -1;
        for (int i = 1; i < length; ++i)
            if ((key[pos + i] & 0xC0) != 0x80)
                return -1;
        return length;
    }

    /// Keys made of the characters of the given alphabet: mostly instances of the pattern (`null` parts are filled by
    /// random strings) with a random edit, and some random strings.
    private static List<String> randomUtf8Keys(Random rand, List<List<String>> parts, String[] alphabet)
    {
        List<String> keys = new ArrayList<>();
        int count = 10 + rand.nextInt(20);
        for (int i = 0; i < count; ++i)
        {
            List<String> chars = new ArrayList<>();
            for (List<String> part : parts)
            {
                if (part == null)
                    for (int j = rand.nextInt(4); j > 0; --j)
                        chars.add(alphabet[rand.nextInt(alphabet.length)]);
                else
                    chars.add(part.get(rand.nextInt(part.size())));
            }

            int pos = rand.nextInt(chars.size() + 1);
            String c = alphabet[rand.nextInt(alphabet.length)];
            switch (rand.nextInt(5))
            {
                case 0:
                    if (pos < chars.size())
                        chars.set(pos, c);
                    break;
                case 1:
                    chars.add(pos, c);
                    break;
                case 2:
                    if (pos < chars.size())
                        chars.remove(pos);
                    break;
                case 3:
                    chars.subList(pos, chars.size()).clear();
                    break;
                default:
                    break;
            }
            keys.add(String.join("", chars));
        }
        for (int i = 0; i < 5; ++i)
        {
            StringBuilder key = new StringBuilder();
            for (int j = rand.nextInt(parts.size() + 3); j > 0; --j)
                key.append(alphabet[rand.nextInt(alphabet.length)]);
            keys.add(key.toString());
        }
        return keys;
    }

    private void checkRandomCase(Random rand, String context, String alphabet, int minPatternLength, int maxPatternLength)
    throws TrieSpaceExhaustedException
    {
        String pattern = randomPattern(rand, alphabet, minPatternLength, maxPatternLength);
        byte[] patternBytes = pattern.getBytes(StandardCharsets.UTF_8);
        List<String> keys = randomKeys(rand, pattern, alphabet);
        for (Map.Entry<String, Trie<String>> source : randomSources(rand, keys).entrySet())
            for (int algo : ALGO_CHOICES)
                verifyAgainstReference(context + " " + source.getKey() + " pattern " + pattern + " [algo=" + algo + "]",
                                       source.getValue(), TriePattern.glob(pattern), key -> globMatches(patternBytes, key),
                                       algo, keys, rand);
    }

    /// Checks all the ways of walking the pattern-matched trie against the given reference matcher of key bytes.
    private static void verifyAgainstReference(String msg, Trie<String> source, TriePattern pattern, Predicate<byte[]> reference,
                                               int algo, List<String> keys, Random rand)
    {
        Trie<String> matched = patternMatched(source, pattern, algo);

        // matching single values, as post-filtering does
        PatternMatcher matcher = algo == 0 ? PatternMatcher.create(pattern) : new PatternMatcherLong(pattern);
        for (String key : keys)
        {
            byte[] bytes = key.getBytes(StandardCharsets.UTF_8);
            assertEquals(msg + " matches " + key, reference.test(bytes), matcher.matches(ByteBuffer.wrap(bytes)));
            if (algo == 0)
                assertEquals(msg + " pattern matches " + key, reference.test(bytes), pattern.matches(ByteBuffer.wrap(bytes)));
        }
        byte[] literalPrefix = pattern.literalPrefix();
        for (String key : keys)
        {
            byte[] bytes = key.getBytes(StandardCharsets.UTF_8);
            if (reference.test(bytes))
                assertEquals(msg + " literal prefix of " + key, 0, Arrays.compare(bytes, 0, Math.min(bytes.length, literalPrefix.length), literalPrefix, 0, literalPrefix.length));
        }

        for (Direction dir : Direction.values())
        {
            List<String> expected = matching(source.values(dir), reference);
            assertEquals(msg + " values " + dir, expected, toList(matched, dir));
            assertEquals(msg + " entries " + dir, expected, entryKeys(matched, dir));
            assertEquals(msg + " walk " + dir, expected, walk(matched.cursor(dir), rand));
        }

        for (int i = 0; i < 3; ++i)
        {
            String a = randomProbe(rand, keys);
            String b = randomProbe(rand, keys);
            if (Arrays.compareUnsigned(a.getBytes(StandardCharsets.UTF_8), b.getBytes(StandardCharsets.UTF_8)) > 0)
            {
                String t = a;
                a = b;
                b = t;
            }
            ByteComparable left = rand.nextInt(6) == 0 ? null : directComparable(a);
            ByteComparable right = rand.nextInt(6) == 0 ? null : directComparable(b);
            boolean includeLeft = rand.nextBoolean() || a.equals(b);
            boolean includeRight = rand.nextBoolean() || a.equals(b);
            String sliceMsg = msg + " slice " + (includeLeft ? '[' : '(') + (left == null ? "null" : a) + ", "
                              + (right == null ? "null" : b) + (includeRight ? ']' : ')');
            Trie<String> sourceSlice = source.slice(left, includeLeft, right, includeRight);
            for (Direction dir : Direction.values())
            {
                List<String> expected = matching(sourceSlice.values(dir), reference);
                assertEquals(sliceMsg + " matched then sliced " + dir,
                             expected, toList(matched.slice(left, includeLeft, right, includeRight), dir));
                assertEquals(sliceMsg + " sliced then matched " + dir,
                             expected, toList(patternMatched(sourceSlice, pattern, algo), dir));
            }
        }

        for (int i = 0; i < 3; ++i)
        {
            String prefix = randomProbe(rand, keys);
            Trie<String> sourceTail = source.tailTrie(directComparable(prefix));
            Trie<String> matchedTail = matched.tailTrie(directComparable(prefix));
            for (Direction dir : Direction.values())
            {
                List<String> expected = sourceTail == null ? ImmutableList.of() : matching(sourceTail.values(dir), reference);
                List<String> actual = matchedTail == null ? ImmutableList.of() : toList(matchedTail, dir);
                assertEquals(msg + " tailTrie " + prefix + " " + dir, expected, actual);
            }
        }

        // Collect all tails before examining any, so that the iteration has moved on when they are walked.
        List<Map.Entry<ByteComparable.Preencoded, Trie<String>>> tails =
            Iterables.toList(matched.tailTries(Direction.FORWARD, x -> true));
        // tailTries skips the branch of each returned tail, so only matches without a matching prefix are listed.
        List<String> expectedTailKeys = new ArrayList<>();
        for (String s : matching(source.values(Direction.FORWARD), reference))
            if (expectedTailKeys.isEmpty() || !s.startsWith(expectedTailKeys.get(expectedTailKeys.size() - 1)))
                expectedTailKeys.add(s);
        assertEquals(msg + " tailTries keys",
                     expectedTailKeys,
                     tails.stream().map(e -> decode(e.getKey())).collect(Collectors.toList()));
        for (Map.Entry<ByteComparable.Preencoded, Trie<String>> tail : tails)
        {
            Trie<String> sourceTail = source.tailTrie(tail.getKey());
            for (Direction dir : Direction.values())
                assertEquals(msg + " tailTries " + decode(tail.getKey()) + " " + dir,
                             matching(sourceTail.values(dir), reference),
                             toList(tail.getValue(), dir));
        }
    }

    /// Reference glob match of UTF-8 encoded patterns and keys, on their characters: '?' matches any one character,
    /// '*' any sequence of characters.
    static boolean globMatches(byte[] patternBytes, byte[] keyBytes)
    {
        int[] pattern = new String(patternBytes, StandardCharsets.UTF_8).codePoints().toArray();
        int[] key = new String(keyBytes, StandardCharsets.UTF_8).codePoints().toArray();
        int p = 0;
        int k = 0;
        int starP = -1;
        int starK = -1;
        while (k < key.length)
        {
            if (p < pattern.length && pattern[p] == '*')
            {
                starP = p++;
                starK = k;
            }
            else if (p < pattern.length && (pattern[p] == '?' || pattern[p] == key[k]))
            {
                ++p;
                ++k;
            }
            else if (starP >= 0)
            {
                // Let the last star absorb one more character and retry.
                p = starP + 1;
                k = ++starK;
            }
            else
                return false;
        }
        while (p < pattern.length && pattern[p] == '*')
            ++p;
        return p == pattern.length;
    }

    private static List<String> matching(Iterable<String> values, byte[] pattern)
    {
        return matching(values, key -> globMatches(pattern, key));
    }

    private static List<String> matching(Iterable<String> values, Predicate<byte[]> matcher)
    {
        List<String> result = new ArrayList<>();
        for (String v : values)
            if (matcher.test(v.getBytes(StandardCharsets.UTF_8)))
                result.add(v);
        return result;
    }

    private static String decode(ByteComparable.Preencoded key)
    {
        return new String(key.asByteComparableArray(VERSION), StandardCharsets.UTF_8);
    }

    /// Lists the keys of the trie's entries, checking that each value is its own key.
    private static List<String> entryKeys(Trie<String> trie, Direction dir)
    {
        List<String> result = new ArrayList<>();
        for (Map.Entry<ByteComparable.Preencoded, String> e : trie.entrySet(dir))
        {
            String key = decode(e.getKey());
            assertEquals("Entry value does not match its key", key, e.getValue());
            result.add(key);
        }
        return result;
    }

    /// Lists the content of the cursor, walking with a random mix of `advance` and `advanceMultiple`.
    private static List<String> walk(Cursor<String> cursor, Random rand)
    {
        List<String> result = new ArrayList<>();
        for (long pos = cursor.encodedPosition(); !Cursor.isExhausted(pos); pos = rand.nextBoolean() ? cursor.advance()
                                                                                                    : cursor.advanceMultiple(null))
        {
            String content = Cursor.content(cursor, pos);
            if (content != null)
                result.add(content);
        }
        return result;
    }

    private static String randomString(Random rand, String alphabet, int minLength, int maxLength)
    {
        int length = minLength + rand.nextInt(maxLength - minLength + 1);
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; ++i)
            sb.append(alphabet.charAt(rand.nextInt(alphabet.length())));
        return sb.toString();
    }

    private static String randomPattern(Random rand, String alphabet, int minLength, int maxLength)
    {
        int length = minLength + rand.nextInt(maxLength - minLength + 1);
        // Vary the wildcard density between patterns.
        double starChance = rand.nextInt(3) * 0.1;
        double questionChance = rand.nextInt(3) * 0.1;
        StringBuilder sb = new StringBuilder(length + 2);
        if (rand.nextInt(4) == 0)
            sb.append('*');
        for (int i = 0; i < length; ++i)
        {
            double r = rand.nextDouble();
            if (r < starChance)
                sb.append('*');
            else if (r < starChance + questionChance)
                sb.append('?');
            else
                sb.append(alphabet.charAt(rand.nextInt(alphabet.length())));
        }
        if (rand.nextInt(4) == 0)
            sb.append('*');
        return sb.toString();
    }

    /// Generates keys that are a mix of matches of the pattern, near misses (one edit away, prefixes, extensions) and
    /// random strings.
    private static List<String> randomKeys(Random rand, String pattern, String alphabet)
    {
        List<String> keys = new ArrayList<>();
        int count = 10 + rand.nextInt(20);
        for (int i = 0; i < count; ++i)
        {
            StringBuilder key = new StringBuilder();
            for (char c : pattern.toCharArray())
            {
                if (c == '*')
                    key.append(randomString(rand, alphabet, 0, 3));
                else if (c == '?')
                    key.append(alphabet.charAt(rand.nextInt(alphabet.length())));
                else
                    key.append(c);
            }

            int pos = rand.nextInt(key.length() + 1);
            char c = alphabet.charAt(rand.nextInt(alphabet.length()));
            switch (rand.nextInt(6))
            {
                case 0:
                    if (pos < key.length())
                        key.setCharAt(pos, c);
                    break;
                case 1:
                    key.insert(pos, c);
                    break;
                case 2:
                    if (pos < key.length())
                        key.deleteCharAt(pos);
                    break;
                case 3:
                    key.setLength(pos);
                    break;
                case 4:
                    key.append(randomString(rand, alphabet, 1, 3));
                    break;
                default:
                    break;
            }
            keys.add(key.toString());
        }
        for (int i = 0; i < 5; ++i)
            keys.add(randomString(rand, alphabet, 0, pattern.length() + 3));
        if (rand.nextInt(4) == 0)
            keys.add("");
        return keys;
    }

    /// A random prefix of one of the keys (whole characters only), possibly extended by one character.
    private static String randomProbe(Random rand, List<String> keys)
    {
        String key = keys.get(rand.nextInt(keys.size()));
        String probe = key.substring(0, key.offsetByCodePoints(0, rand.nextInt(key.codePointCount(0, key.length()) + 1)));
        return rand.nextInt(4) == 0 ? probe + "ab~".charAt(rand.nextInt(3)) : probe;
    }

    private static Map<String, Trie<String>> randomSources(Random rand, List<String> keys) throws TrieSpaceExhaustedException
    {
        Map<String, Trie<String>> sources = new LinkedHashMap<>();
        String[] array = keys.toArray(new String[0]);
        sources.put("ordered", TrieUtil.directOrderedTrie(array));
        sources.put("unordered", TrieUtil.directTrie(array));

        InMemoryTrie<String> left = InMemoryTrie.shortLivedOrdered(VERSION);
        InMemoryTrie<String> right = InMemoryTrie.shortLivedOrdered(VERSION);
        for (String key : keys)
        {
            int where = rand.nextInt(3); // left, right or both
            if (where != 1)
                left.putRecursive(directComparable(key), key, (ex, n) -> n);
            if (where != 0)
                right.putRecursive(directComparable(key), key, (ex, n) -> n);
        }
        sources.put("merged", left.mergeWith(right, (a, b) -> a));
        return sources;
    }
}