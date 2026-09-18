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

package org.apache.cassandra.index.sai.cql;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeSet;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.utils.AutomatonQueries;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

/**
 * Tests the automaton-served SAI text query surface: the non-prefix {@code LIKE} variants
 * ({@code LIKE '%x'}, {@code LIKE '%x%'}, {@code LIKE 'a%b'}) on non-analyzed literal SAI indexes, across the
 * memtable-only / flushed / multi-sstable / compacted matrix, against client-side brute force, and pins the
 * exact agreement between the index path and the {@link Operator} post-filters used by filtering and replica
 * filtering protection.
 */
public class AutomatonQueryTest extends SAITester
{
    private static final long SEED = System.nanoTime();

    /** Alphabet for randomized corpora: ascii, whitespace, accents, CJK, an astral-plane emoji and '%'. */
    private static final String[] VALUE_ALPHABET = { "a", "b", "c", "ab", "abc", "z", "0", "1", " ", "é", "ß", "日", "本", "😀", "%" };

    /** Alphabet for random pattern segments: no LIKE metacharacters. */
    private static final String[] SEGMENT_ALPHABET = { "a", "b", "c", "ab", "z", "0", "é", "日", "😀" };

    private final Random random = new Random(SEED);

    // ---------------------------------------------------------------------------------------------------------
    // Client-side ground truth, computed independently of both the index and the Operator post-filters.
    // ---------------------------------------------------------------------------------------------------------

    /** Reference implementation of full CQL LIKE semantics: '%' = any string, no escapes, '_' literal. */
    private static boolean likeMatches(String value, String pattern)
    {
        String[] segments = pattern.split("%", -1);
        StringBuilder regex = new StringBuilder();
        for (int i = 0; i < segments.length; i++)
        {
            if (i > 0)
                regex.append(".*");
            regex.append(Pattern.quote(segments[i]));
        }
        return Pattern.compile(regex.toString(), Pattern.DOTALL).matcher(value).matches();
    }

    private Set<Integer> keysOf(UntypedResultSet result)
    {
        Set<Integer> keys = new TreeSet<>();
        for (UntypedResultSet.Row row : result)
            keys.add(row.getInt("pk"));
        return keys;
    }

    private static Set<Integer> expected(Map<Integer, String> rows, Predicate<String> predicate)
    {
        Set<Integer> keys = new TreeSet<>();
        for (Map.Entry<Integer, String> entry : rows.entrySet())
            if (predicate.test(entry.getValue()))
                keys.add(entry.getKey());
        return keys;
    }

    /** Evaluates an {@link Operator} post-filter exactly as filtering/replica-filtering-protection would. */
    private static Predicate<String> postFilter(Operator operator, String queriedValue)
    {
        ByteBuffer queried = UTF8Type.instance.decompose(queriedValue);
        return value -> operator.isSatisfiedBy(UTF8Type.instance, UTF8Type.instance.decompose(value), queried);
    }

    // ---------------------------------------------------------------------------------------------------------
    // Fixed corpora over the memtable/flushed/compacted matrix.
    // ---------------------------------------------------------------------------------------------------------

    private void createTableWithIndex()
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text)");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
    }

    @Test
    public void testLikeVariantsFixed()
    {
        createTableWithIndex();

        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 1, "quick brown fox");
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 2, "quick red fox");
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 3, "lazy dog");
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 4, "fox");
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 5, "FOX");
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 6, "boxfox");
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 7, "日本語のfox");

        for (int i = 0; i < 3; i++)
        {
            // suffix: LIKE '%<term>'
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE v LIKE '%%fox'"), row(1), row(2), row(4), row(6), row(7));
            // contains: LIKE '%<term>%'
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE v LIKE '%%fox%%'"), row(1), row(2), row(4), row(6), row(7));
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE v LIKE '%%quick%%'"), row(1), row(2));
            // generic pattern: LIKE '<a>%<b>' — the term equal to the concatenated segments matches too
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE v LIKE 'quick%%fox'"), row(1), row(2));
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE v LIKE 'q%%k%%x'"), row(1), row(2));
            // LIKE with no wildcard at all is an exact match
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE v LIKE 'fox'"), row(4));
            // case-sensitive by default
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE v LIKE '%%FOX'"), row(5));
            // unicode
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE v LIKE '日本%%fox'"), row(7));
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE v LIKE '%%語のfox'"), row(7));
            // no matches
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE v LIKE '%%missing%%'"));

            if (i == 0)
                flush();
            else if (i == 1)
                compact();
        }
    }

    @Test
    public void testLikeComplexityGuard()
    {
        createTableWithIndex();
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 1, "abc");

        // A generic LIKE pattern ('a%a%a%...a', LIKE_MATCHES) whose NFA-to-DFA determinization exceeds the
        // Lucene work limit must fail at the coordinator with a clear invalid-request "Invalid pattern" error
        // from the eager compile in LikeRestriction#addToRowFilter. Without that eager compile the first
        // compile happens on the replicas, mid-read, and surfaces as a generic read failure with an UNKNOWN
        // failure reason.
        StringBuilder pattern = new StringBuilder();
        for (int i = 0; i < 1000; i++)
            pattern.append("a%");
        pattern.append('a');
        assertThatThrownBy(() -> execute("SELECT pk FROM %s WHERE v LIKE ?", pattern.toString()))
            .hasMessageContaining("Invalid pattern");

        // ... while reasonable generic patterns keep working
        assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE v LIKE 'a%%c'"), row(1));
    }

    @Test
    public void testSingleTermPatternEstimates() throws Throwable
    {
        createTableWithIndex();
        for (int pk = 0; pk < 100; pk++)
            execute("INSERT INTO %s (pk, v) VALUES (?, ?)", pk, "term" + pk);

        beforeAndAfterFlush(() -> {
            // A single-term automaton (LIKE with no wildcard at all is an exact match served by LIKE_MATCHES)
            // estimates like an exact match of that term, mirroring the search path's exact lookup.
            assertThatPlanFor("SELECT pk FROM %s WHERE v LIKE 'term11'", 1)
                .hasEstimatedRowsCountBetween(0.0, 10.0);
        });
    }

    @Test
    public void testExpansionCapError()
    {
        createTableWithIndex();

        for (int pk = 0; pk < 32; pk++)
            execute("INSERT INTO %s (pk, v) VALUES (?, ?)", pk, "term" + pk + "a");

        String previous = CassandraRelevantProperties.SAI_MAX_AUTOMATON_EXPANSIONS.setString("4");
        try
        {
            for (int i = 0; i < 2; i++)
            {
                // '%a' has no common prefix, so the scan visits every term and trips the cap; the failure is
                // reported to the client with the SAI_AUTOMATON_EXPANSIONS_EXCEEDED failure reason (the detailed
                // message, suggesting a more selective pattern, goes to the replica logs)
                assertThatThrownBy(() -> execute("SELECT pk FROM %s WHERE v LIKE '%%a'"))
                    .hasMessageContaining(RequestFailureReason.SAI_AUTOMATON_EXPANSIONS_EXCEEDED.name());
                // a selective pattern with a common prefix stays under the cap
                assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE v LIKE 'term31%%a'"), row(31));

                if (i == 0)
                    flush();
            }
        }
        finally
        {
            if (previous == null)
                System.clearProperty(CassandraRelevantProperties.SAI_MAX_AUTOMATON_EXPANSIONS.getKey());
            else
                CassandraRelevantProperties.SAI_MAX_AUTOMATON_EXPANSIONS.setString(previous);
        }
    }

    /**
     * The memtable automaton walk bounds (and seek-skips) its scan exactly like the sstable one, so a
     * common-prefix-anchored pattern over a term count far above the budget gives the SAME guardrail outcome
     * before and after flush: were the memtable scan to visit every term of the index sequentially, the
     * identical query would fail on unflushed data and succeed after a flush.
     */
    @Test
    public void testSkippablePatternBudgetIsFlushIndependent()
    {
        createTableWithIndex();
        // 64 terms sharing no prefix with the pattern's target ('zz...'): a prefix-bounded walk jumps
        // straight past them, an unbounded sequential scan visits them all
        for (int pk = 0; pk < 64; pk++)
            execute("INSERT INTO %s (pk, v) VALUES (?, ?)", pk, String.format("term%03da", pk));
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 100, "zzz");

        String previous = CassandraRelevantProperties.SAI_MAX_AUTOMATON_EXPANSIONS.setString("8");
        try
        {
            // memtable-only, then flushed: same outcome under the tiny budget
            for (int i = 0; i < 2; i++)
            {
                assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE v LIKE 'z%%zz'"), row(100));
                assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE v LIKE 'zz%%z'"), row(100));
                if (i == 0)
                    flush();
            }
        }
        finally
        {
            if (previous == null)
                System.clearProperty(CassandraRelevantProperties.SAI_MAX_AUTOMATON_EXPANSIONS.getKey());
            else
                CassandraRelevantProperties.SAI_MAX_AUTOMATON_EXPANSIONS.setString(previous);
        }
    }

    /**
     * The expansions cap is a PER-QUERY budget shared across all sstable segments and memtable shards
     * (docs/sai-parity/text-queries.md Area 3 guardrails: "cap automaton-visited terms per query"), not a
     * per-segment allowance: a query whose every individual segment scan is under the cap must still fail when
     * its aggregate visited-terms count exceeds it. Otherwise the worst case scales with the segment count and
     * the same query on the same data succeeds before compaction and fails after it (or vice versa).
     */
    @Test
    public void testExpansionCapIsPerQueryAcrossSegments()
    {
        createTableWithIndex();

        // 3 flushed sstables with 4 terms each, plus 4 memtable-only terms; '*a' has no common prefix, so every
        // scan visits every term of its segment/shard: 4 per segment, 16 for the whole query.
        int pk = 0;
        for (int sstable = 0; sstable < 3; sstable++)
        {
            for (int i = 0; i < 4; i++, pk++)
                execute("INSERT INTO %s (pk, v) VALUES (?, ?)", pk, String.format("term%02da", pk));
            flush();
        }
        for (int i = 0; i < 4; i++, pk++)
            execute("INSERT INTO %s (pk, v) VALUES (?, ?)", pk, String.format("term%02da", pk));
        int totalRows = pk;

        String previous = CassandraRelevantProperties.SAI_MAX_AUTOMATON_EXPANSIONS.setString("8");
        try
        {
            // Every single segment visits only 4 terms - under the cap - but the aggregate exceeds it: with a
            // per-segment interpretation of the cap this query would (wrongly) succeed.
            assertThatThrownBy(() -> execute("SELECT pk FROM %s WHERE v LIKE '%%a'"))
                .hasMessageContaining(RequestFailureReason.SAI_AUTOMATON_EXPANSIONS_EXCEEDED.name());

            // The mirror case: over the very same many-segment layout, an aggregate within the budget succeeds.
            CassandraRelevantProperties.SAI_MAX_AUTOMATON_EXPANSIONS.setString("16");
            Object[][] allRows = new Object[totalRows][];
            for (int k = 0; k < totalRows; k++)
                allRows[k] = row(k);
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE v LIKE '%%a'"), allRows);

            // and a selective, prefix-bounded pattern stays well under the budget
            CassandraRelevantProperties.SAI_MAX_AUTOMATON_EXPANSIONS.setString("8");
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE v LIKE 'term01%%a'"), row(1));
        }
        finally
        {
            if (previous == null)
                System.clearProperty(CassandraRelevantProperties.SAI_MAX_AUTOMATON_EXPANSIONS.getKey());
            else
                CassandraRelevantProperties.SAI_MAX_AUTOMATON_EXPANSIONS.setString(previous);
        }
    }

    // ---------------------------------------------------------------------------------------------------------
    // Applicability matrix and rejections.
    // ---------------------------------------------------------------------------------------------------------

    @Test
    public void testAnalyzedIndexRejections()
    {
        // tokenized (Lucene-analyzed) index: LIKE is rejected
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text)");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = " +
                    "{ 'index_analyzer' : 'standard' }");
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 1, "quick brown");

        assertInvalidMessage(String.format(StatementRestrictions.INDEX_DOES_NOT_SUPPORT_LIKE_MESSAGE, 'v'),
                             "SELECT pk FROM %s WHERE v LIKE '%%quick%%'");
        // ... while the analyzer match operator works
        assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE v : 'quick'"), row(1));
    }

    @Test
    public void testNormalizedIndexServesLikeOnNormalizedTerms()
    {
        // a non-tokenizing normalizer (case_sensitive: false) still is an analyzer: the stored terms differ
        // from the raw values, but LIKE (whose patterns survive the normalization) is served with both sides
        // normalized
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text)");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = { 'case_sensitive' : 'false' }");

        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 1, "Apple");
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 2, "apricot");
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 3, "APPLE PIE");
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 4, "banana");

        for (int i = 0; i < 2; i++)
        {
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE v LIKE '%%PPLE'"), row(1));
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE v LIKE '%%pple%%'"), row(1), row(3));
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE v LIKE 'A%%E'"), row(1), row(3));

            if (i == 0)
                flush();
        }
    }

    @Test
    public void testCollectionRejections()
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, l list<text>)");
        createIndex("CREATE CUSTOM INDEX ON %s(l) USING 'StorageAttachedIndex'");
        execute("INSERT INTO %s (pk, l) VALUES (?, ?)", 1, list("abc"));

        // LIKE on collections is rejected by the CQL layer before even considering the index
        assertInvalidMessage("cannot be restricted by a 'LIKE' relation", "SELECT pk FROM %s WHERE l LIKE '%%a'");
        // collections keep working with their supported operators
        assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE l CONTAINS 'abc'"), row(1));
    }

    @Test
    public void testNoIndexRejection()
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text)");
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 1, "abc");

        assertInvalidMessage("is only supported on properly indexed columns",
                             "SELECT pk FROM %s WHERE v LIKE '%%a'");
        assertInvalidMessage("is only supported on properly indexed columns",
                             "SELECT pk FROM %s WHERE v LIKE '%%a' ALLOW FILTERING");
    }

    // ---------------------------------------------------------------------------------------------------------
    // Randomized comparison against client-side brute force AND the Operator post-filters, over the
    // memtable-only / flushed+memtable / two-sstable / compacted matrix. The post-filter comparison is the
    // "automaton == post-filter" property test: the rows returned through the index must be exactly the rows the
    // RowFilter/RFP post-filter would keep.
    // ---------------------------------------------------------------------------------------------------------

    @Test
    public void testRandomizedAgainstBruteForceAndPostFilter()
    {
        createTableWithIndex();

        Map<Integer, String> rows = new HashMap<>();

        insertRandomRows(rows, 0, 100);
        verifyRandomizedPatterns(rows);

        flush();
        insertRandomRows(rows, 100, 200);
        verifyRandomizedPatterns(rows);

        flush();
        verifyRandomizedPatterns(rows);

        compact();
        verifyRandomizedPatterns(rows);

        // Overwrites and deletes: memtable entries shadowing flushed values (the index still lists the old terms
        // in the sstable segments, so the automaton engine must agree with post-filtering under shadowed
        // postings), verified again after flushing the shadows and after compacting them away.
        overwriteAndDeleteRandomRows(rows);
        verifyRandomizedPatterns(rows);

        flush();
        overwriteAndDeleteRandomRows(rows);
        verifyRandomizedPatterns(rows);

        flush();
        verifyRandomizedPatterns(rows);

        compact();
        verifyRandomizedPatterns(rows);
    }

    private void insertRandomRows(Map<Integer, String> rows, int from, int to)
    {
        for (int pk = from; pk < to; pk++)
        {
            String value = randomString(VALUE_ALPHABET, 1, 6);
            rows.put(pk, value);
            execute("INSERT INTO %s (pk, v) VALUES (?, ?)", pk, value);
        }
    }

    /** Overwrites about a third of the surviving rows with fresh values and deletes about a tenth. */
    private void overwriteAndDeleteRandomRows(Map<Integer, String> rows)
    {
        for (Integer pk : new ArrayList<>(rows.keySet()))
        {
            int action = random.nextInt(10);
            if (action < 3)
            {
                String value = randomString(VALUE_ALPHABET, 1, 6);
                rows.put(pk, value);
                execute("INSERT INTO %s (pk, v) VALUES (?, ?)", pk, value);
            }
            else if (action == 9)
            {
                rows.remove(pk);
                execute("DELETE FROM %s WHERE pk = ?", pk);
            }
        }
    }

    private void verifyRandomizedPatterns(Map<Integer, String> rows)
    {
        List<String> values = new ArrayList<>(rows.values());
        for (int i = 0; i < 20; i++)
        {
            String segment = randomSegment(values);

            // LIKE '%x' (suffix; trimmed value is literal, embedded '%' included). A trailing '%' in the
            // segment would change the pattern classification (LikeRestriction#makeSpecific), so it is stripped.
            String suffixSegment = stripTrailingPercent(segment);
            checkQuery(rows, "SELECT pk FROM %s WHERE v LIKE ?", "%" + suffixSegment,
                       value -> value.endsWith(suffixSegment),
                       postFilter(Operator.LIKE_SUFFIX, suffixSegment),
                       "LIKE '%" + suffixSegment + '\'');

            // LIKE '%x%' (contains; trimmed value is literal)
            checkQuery(rows, "SELECT pk FROM %s WHERE v LIKE ?", "%" + segment + "%",
                       value -> value.contains(segment),
                       postFilter(Operator.LIKE_CONTAINS, segment),
                       "LIKE '%" + segment + "%'");

            // LIKE 'a%b' (generic pattern; every '%' is a wildcard)
            String safeA = randomString(SEGMENT_ALPHABET, 1, 3);
            String safeB = randomString(SEGMENT_ALPHABET, 1, 3);
            String pattern = random.nextBoolean() ? safeA + '%' + safeB : safeA + '%' + safeB + '%' + randomString(SEGMENT_ALPHABET, 1, 2);
            checkQuery(rows, "SELECT pk FROM %s WHERE v LIKE ?", pattern,
                       value -> likeMatches(value, pattern),
                       postFilter(Operator.LIKE_MATCHES, pattern),
                       "LIKE '" + pattern + '\'');

        }
    }

    private void checkQuery(Map<Integer, String> rows,
                            String query,
                            String bindValue,
                            Predicate<String> bruteForce,
                            Predicate<String> postFilter,
                            String description)
    {
        Set<Integer> actual = keysOf(execute(query, bindValue));
        Set<Integer> expectedBruteForce = expected(rows, bruteForce);
        Set<Integer> expectedPostFilter = expected(rows, postFilter);

        assertEquals("Seed: " + SEED + ", index path vs brute force for " + description, expectedBruteForce, actual);
        assertEquals("Seed: " + SEED + ", post-filter vs brute force for " + description, expectedBruteForce, expectedPostFilter);
    }

    private static String stripTrailingPercent(String segment)
    {
        while (segment.endsWith("%"))
            segment = segment.substring(0, segment.length() - 1);
        return segment.isEmpty() ? "a" : segment;
    }

    private String randomSegment(List<String> values)
    {
        if (random.nextBoolean())
        {
            // a random substring (by code points) of an indexed value, so that matches actually happen
            String source = values.get(random.nextInt(values.size()));
            int codePoints = source.codePointCount(0, source.length());
            int from = random.nextInt(codePoints);
            int to = from + 1 + random.nextInt(codePoints - from);
            return source.substring(source.offsetByCodePoints(0, from), source.offsetByCodePoints(0, to));
        }
        return randomString(VALUE_ALPHABET, 1, 3);
    }

    private String randomString(String[] alphabet, int minParts, int maxParts)
    {
        int length = minParts + random.nextInt(maxParts - minParts + 1);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++)
            sb.append(alphabet[random.nextInt(alphabet.length)]);
        return sb.toString();
    }
}
