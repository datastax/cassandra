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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runners.model.Statement;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.restrictions.SingleColumnRestriction;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.utils.Pair;

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
    /** The seed of the randomized tests, which can be fixed with the {@link CassandraRelevantProperties#TEST_RANDOM_SEED} property. */
    private static final long SEED = CassandraRelevantProperties.TEST_RANDOM_SEED.getLong(System.nanoTime());

    /** Reports the seed with any failure of a test, including unexpected exceptions, not only assertion messages. */
    @Rule
    public final TestRule reportSeed = (base, description) -> new Statement()
    {
        @Override
        public void evaluate() throws Throwable
        {
            try
            {
                base.evaluate();
            }
            catch (Throwable t)
            {
                t.addSuppressed(new RuntimeException("Random seed: -D" + CassandraRelevantProperties.TEST_RANDOM_SEED.getKey() + '=' + SEED));
                throw t;
            }
        }
    };

    /** Alphabet for randomized corpora: ascii, whitespace, accents, CJK, an astral-plane emoji and '%'. */
    private static final String[] VALUE_ALPHABET = { "a", "b", "c", "ab", "abc", "z", "0", "1", " ", "é", "ß", "日", "本", "😀", "%" };

    /** Alphabet for random pattern segments: no LIKE metacharacters. */
    private static final String[] SEGMENT_ALPHABET = { "a", "b", "c", "ab", "z", "0", "é", "日", "😀" };

    private final Random random = new Random(SEED);

    // ---------------------------------------------------------------------------------------------------------
    // Client-side ground truth, computed independently of both the index and the Operator post-filters.
    // ---------------------------------------------------------------------------------------------------------

    /** Reference implementation of CQL LIKE semantics for patterns without '_' or escapes: '%' = any string. */
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
        // from the eager compile in StorageAttachedIndex#validate(ReadCommand). Without that eager compile the
        // first compile happens on the replicas, mid-read, and surfaces as a generic read failure with an UNKNOWN
        // failure reason.
        StringBuilder pattern = new StringBuilder();
        for (int i = 0; i < 1000; i++)
            pattern.append("a%");
        pattern.append('a');
        assertThatThrownBy(() -> execute("SELECT pk FROM %s WHERE v LIKE ?", pattern.toString()))
            .isInstanceOf(InvalidRequestException.class)
            .hasMessageStartingWith("Invalid pattern for LIKE '<term>' restriction on column v: ");

        // ... while reasonable generic patterns keep working
        assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE v LIKE 'a%%c'"), row(1));
    }

    /**
     * The selected index validates the pattern when the restriction is bound, even when the query ends up with no
     * index query plan: a disjunction with an unindexed sibling can't be served by the index, so the LIKE is
     * post-filtered on the replicas, where an invalid pattern would otherwise surface as a read failure.
     */
    @Test
    public void testLikeValidationWithoutQueryPlan()
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text, w int)");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        execute("INSERT INTO %s (pk, v, w) VALUES (?, ?, ?)", 1, "abc", 7);

        String pattern = "a%".repeat(1000) + 'a';
        assertThatThrownBy(() -> execute("SELECT pk FROM %s WHERE v LIKE ? OR w = 7 ALLOW FILTERING", pattern))
            .isInstanceOf(InvalidRequestException.class)
            .hasMessageStartingWith("Invalid pattern for LIKE '<term>' restriction on column v: ");
        assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE v LIKE 'a%%c' OR w = 8 ALLOW FILTERING"), row(1));
    }

    /**
     * LIKE values larger than the maximum size of the indexed terms are not rejected: the maximum is a system
     * property that may have been larger when some of the terms were indexed, so such a value may still match
     * indexed terms. Here it doesn't, and the query simply returns no row. (Long literals behind a wildcard are a
     * different matter: whether their automaton can be determinized within Lucene's work limit depends on the
     * literal, not on the maximum term size, so this doesn't test them.)
     */
    @Test
    public void testLikeValuesLargerThanMaxTermSize()
    {
        createTableWithIndex();
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 1, "abc");

        String oversized = "a".repeat(IndexContext.MAX_STRING_TERM_SIZE + 1);
        for (String pattern : new String[]{ oversized + '%', oversized, oversized + "%%%%", oversized + "%a" })
            assertRows(execute("SELECT pk FROM %s WHERE v LIKE ?", pattern));
        // wildcards don't count towards any size
        assertRows(execute("SELECT pk FROM %s WHERE v LIKE ?", 'a' + "%".repeat(2 * IndexContext.MAX_STRING_TERM_SIZE) + 'c'), row(1));
    }

    @Test
    public void testSingleTermPatternEstimates() throws Throwable
    {
        createTableWithIndex();
        for (int pk = 0; pk < 100; pk++)
            execute("INSERT INTO %s (pk, v) VALUES (?, ?)", pk, "term" + pk);

        beforeAndAfterFlush(() -> {
            // LIKE with no wildcard at all is an exact match (LIKE_MATCHES), served and estimated by the index
            // exactly like an equality.
            assertThatPlanFor("SELECT pk FROM %s WHERE v LIKE 'term11'", 1)
                .hasEstimatedRowsCountBetween(1.0, 2.0);
        });
    }

    /**
     * Every '%' of a LIKE pattern is a wildcard, wherever it appears: patterns with inner wildcards are generic
     * patterns even when they also start or end with '%', both through the index and through post-filtering.
     */
    @Test
    public void testInnerWildcards()
    {
        createTableWithIndex();
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 1, "aXbY");
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 2, "a%b%");
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 3, "XaYb");
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 4, "ab");
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 5, "ba");
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 6, "%a");

        for (int i = 0; i < 3; i++)
        {
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE v LIKE 'a%%b%%'"), row(1), row(2), row(4));
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE v LIKE '%%a%%b'"), row(3), row(4));
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE v LIKE '%%a%%b%%'"), row(1), row(2), row(3), row(4));
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE v LIKE 'a%%%%b'"), row(4));
            // runs of leading/trailing wildcards
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE v LIKE '%%%%a'"), row(5), row(6));
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE v LIKE 'b%%%%'"), row(5));
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE v LIKE '%%%%Y%%%%'"), row(1), row(3));
            // patterns made only of wildcards are empty
            for (String pattern : new String[]{ "%", "%%", "%%%" })
                assertInvalidMessage("LIKE value can't be empty", "SELECT pk FROM %s WHERE v LIKE ?", pattern);

            if (i == 0)
                flush();
            else if (i == 1)
                compact();
        }
    }

    /**
     * The same patterns on an index with a non-tokenizing analyzer, which post-filters LIKE per analyzed token
     * (RowFilter), and with an additional restriction that makes the query post-filter the LIKE results.
     */
    @Test
    public void testInnerWildcardsWithAnalyzerAndPostFiltering()
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text, n int)");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = { 'case_sensitive' : 'false' }");
        createIndex("CREATE CUSTOM INDEX ON %s(n) USING 'StorageAttachedIndex'");
        execute("INSERT INTO %s (pk, v, n) VALUES (?, ?, ?)", 1, "AxBy", 1);
        execute("INSERT INTO %s (pk, v, n) VALUES (?, ?, ?)", 2, "a%B%", 1);
        execute("INSERT INTO %s (pk, v, n) VALUES (?, ?, ?)", 3, "xAyB", 1);
        execute("INSERT INTO %s (pk, v, n) VALUES (?, ?, ?)", 4, "ab", 2);

        for (int i = 0; i < 2; i++)
        {
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE v LIKE 'A%%b%%'"), row(1), row(2), row(4));
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE v LIKE '%%a%%B'"), row(3), row(4));
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE v LIKE '%%a%%b%%'"), row(1), row(2), row(3), row(4));
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE v LIKE '%%a%%b%%' AND n = 1"), row(1), row(2), row(3));
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE v LIKE 'a%%b%%' AND n = 2"), row(4));

            if (i == 0)
                flush();
        }
    }

    /**
     * LIKE combined with ORDER BY on the same column: the ordering scans of the memtable index must return every
     * term the pattern can match, not just the term equal to the pattern.
     */
    @Test
    public void testLikeWithOrderBy()
    {
        createTableWithIndex();
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 1, "abd");
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 2, "ab");
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 3, "xyz");
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 4, "abc");
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 5, "zab");
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 6, "ac");

        // memtable only, sstable only, and sstable + memtable
        for (int i = 0; i < 3; i++)
        {
            assertRows(execute("SELECT pk FROM %s WHERE v LIKE 'ab%%' ORDER BY v LIMIT 10"), row(2), row(4), row(1));
            assertRows(execute("SELECT pk FROM %s WHERE v LIKE 'ab%%' ORDER BY v DESC LIMIT 10"), row(1), row(4), row(2));
            assertRows(execute("SELECT pk FROM %s WHERE v LIKE 'ab%%' ORDER BY v LIMIT 2"), row(2), row(4));
            assertRows(execute("SELECT pk FROM %s WHERE v LIKE 'a%%d' ORDER BY v LIMIT 10"), row(1));
            assertRows(execute("SELECT pk FROM %s WHERE v LIKE 'a%%' ORDER BY v DESC LIMIT 10"), row(6), row(1), row(4), row(2));
            assertRows(execute("SELECT pk FROM %s WHERE v LIKE '%%b%%' ORDER BY v LIMIT 10"), row(2), row(4), row(1), row(5));
            assertRows(execute("SELECT pk FROM %s WHERE v LIKE '%%c' ORDER BY v DESC LIMIT 10"), row(6), row(4));
            assertRows(execute("SELECT pk FROM %s WHERE v LIKE 'ab' ORDER BY v LIMIT 10"), row(2));

            if (i == 0)
            {
                flush();
            }
            else if (i == 1)
            {
                execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 1, "abd");
                execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 6, "ac");
            }
        }
    }

    /**
     * LIKE combined with ORDER BY on the same column: the ordering scans only visit the terms the pattern can match,
     * on memtables and sstables alike. The query optimizer is disabled so that the plan keeps the LIKE restriction
     * on the ordering index scan: for an unselective pattern it would otherwise order the whole index and
     * post-filter the rows, which doesn't match the pattern against the index.
     */
    @Test
    public void testLikeWithOrderByOnUnselectivePatterns()
    {
        disableQueryOptimization();
        createTableWithIndex();
        // 32 terms not matching '%a' sorting before the only one matching it, all starting with 'aaa'
        for (int pk = 0; pk < 32; pk++)
            execute("INSERT INTO %s (pk, v) VALUES (?, ?)", pk, String.format("aaa%02db", pk));
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 100, "zzza");
        // a prefix of 10 of the terms above
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 200, "aaa0");

        // memtable only, then sstable only
        for (int i = 0; i < 2; i++)
        {
            // an ordering scan over '%a' (no common prefix) visits the 32 non-matching terms first, ascending
            assertRows(execute("SELECT pk FROM %s WHERE v LIKE '%%a' ORDER BY v LIMIT 1"), row(100));
            assertRows(execute("SELECT pk FROM %s WHERE v LIKE 'aaa%%a' ORDER BY v DESC LIMIT 1"));
            // a pattern whose common prefix bounds the scan to few terms
            assertRows(execute("SELECT pk FROM %s WHERE v LIKE 'zz%%a' ORDER BY v LIMIT 10"), row(100));
            assertRows(execute("SELECT pk FROM %s WHERE v LIKE 'zz%%a' ORDER BY v DESC LIMIT 10"), row(100));
            // prefix patterns and exact matches
            assertRows(execute("SELECT pk FROM %s WHERE v LIKE 'aaa%%' ORDER BY v LIMIT 2"), row(200), row(0));
            assertRows(execute("SELECT pk FROM %s WHERE v LIKE 'aaa%%' ORDER BY v DESC LIMIT 2"), row(31), row(30));
            assertEquals(33, execute("SELECT pk FROM %s WHERE v LIKE 'aaa%%' ORDER BY v LIMIT 100").size());
            assertRows(execute("SELECT pk FROM %s WHERE v LIKE 'aaa0' ORDER BY v LIMIT 10"), row(200));
            assertRows(execute("SELECT pk FROM %s WHERE v LIKE 'aaa0' ORDER BY v DESC LIMIT 10"), row(200));

            if (i == 0)
                flush();
        }
    }

    /**
     * Prefix and pattern predicates are estimated over the range of terms they can match.
     */
    @Test
    public void testPrefixAndPatternEstimates() throws Throwable
    {
        createTableWithIndex();
        for (int pk = 0; pk < 100; pk++)
            execute("INSERT INTO %s (pk, v) VALUES (?, ?)", pk, String.format("a%03d", pk));
        for (int pk = 100; pk < 200; pk++)
            execute("INSERT INTO %s (pk, v) VALUES (?, ?)", pk, String.format("m%03d", pk));
        for (int pk = 200; pk < 300; pk++)
            execute("INSERT INTO %s (pk, v) VALUES (?, ?)", pk, String.format("z%03d", pk));

        beforeAndAfterFlush(() -> {
            // prefix: interpolated over the term range [prefix, nextOf(prefix)) (linearly in the byte space for
            // memtables, with the terms histogram for sstables), so neither nothing nor everything
            assertThatPlanFor("SELECT pk FROM %s WHERE v LIKE 'm%%'", 100)
                .hasEstimatedRowsCountBetween(50.0, 150.0);
            // pattern with a common prefix: estimated like that prefix, an upper bound of its matches
            assertThatPlanFor("SELECT pk FROM %s WHERE v LIKE 'm%%5'", 10)
                .hasEstimatedRowsCountBetween(10.0, 150.0);
            // pattern without a common prefix: may match anywhere, estimated as all the rows
            assertThatPlanFor("SELECT pk FROM %s WHERE v LIKE '%%5'", 30)
                .hasEstimatedRowsCountBetween(200.0, 300.0);
            // pattern matching nothing (a prefix past every term)
            assertThatPlanFor("SELECT pk FROM %s WHERE v LIKE 'q%%'", 0)
                .hasEstimatedRowsCountBetween(0.0, 10.0);
        });
    }

    /**
     * The plan caps its expected rows at the memtable's total row count estimate, which is the memtable's live data
     * size divided by the size of a sample of its rows. The two are not measured the same way (e.g. the trie memtable
     * doesn't account the row deletion and the clustering, the sampled rows do), so for tiny rows the total is
     * underestimated and the cap would hide the index estimates these tests assert on. Padding the rows keeps the
     * discrepancy a small fraction of the row size.
     */
    private static final String ROW_PADDING = "p".repeat(200);

    /**
     * Prefix and pattern estimates don't collapse to zero when the prefix is longer than the bytes the term
     * interpolation resolves, nor for reversed (DESC clustering) columns, whose memtable min/max terms are tracked
     * in the reversed order.
     */
    @Test
    public void testPrefixAndPatternEstimatesForLongPrefixesAndReversedColumns() throws Throwable
    {
        String common = "a-common-prefix-longer-than-20-bytes-";
        createTableWithIndex();
        for (int pk = 0; pk < 100; pk++)
            execute("INSERT INTO %s (pk, v) VALUES (?, ?)", pk, String.format("%sx%03d", common, pk));
        for (int pk = 100; pk < 200; pk++)
            execute("INSERT INTO %s (pk, v) VALUES (?, ?)", pk, String.format("%sy%03d", common, pk));
        for (int pk = 200; pk < 300; pk++)
            execute("INSERT INTO %s (pk, v) VALUES (?, ?)", pk, String.format("b%03d", pk));

        beforeAndAfterFlush(() -> {
            assertThatPlanFor("SELECT pk FROM %s WHERE v LIKE '" + common + "x%%'", 100)
                .hasEstimatedRowsCountBetween(50.0, 150.0);
            assertThatPlanFor("SELECT pk FROM %s WHERE v LIKE '" + common + "x%%5'", 10)
                .hasEstimatedRowsCountBetween(10.0, 150.0);
        });

        createTable("CREATE TABLE %s (pk int, c text, pad text, PRIMARY KEY (pk, c)) WITH CLUSTERING ORDER BY (c DESC)");
        createIndex("CREATE CUSTOM INDEX ON %s(c) USING 'StorageAttachedIndex'");
        // more terms per prefix than the memtable estimate counts, so that it interpolates, over a range of terms
        // mostly covered by the prefix
        for (int i = 0; i < 600; i++)
            execute("INSERT INTO %s (pk, c, pad) VALUES (?, ?, ?)", i, String.format("m%03d", i), ROW_PADDING);
        for (int i = 600; i < 610; i++)
            execute("INSERT INTO %s (pk, c, pad) VALUES (?, ?, ?)", i, String.format("n%03d", i), ROW_PADDING);

        beforeAndAfterFlush(() -> {
            assertThatPlanFor("SELECT pk FROM %s WHERE c LIKE 'm%%'", 600)
                .hasEstimatedRowsCountBetween(450.0, 610.0);
            assertThatPlanFor("SELECT pk FROM %s WHERE c LIKE 'm%%5'", 60)
                .hasEstimatedRowsCountBetween(60.0, 610.0);
        });
    }

    /**
     * The memtable prefix estimate stops counting the rows of the terms of the prefix after a fixed budget of rows
     * (counting the keys of a term is linear), and then still doesn't estimate fewer rows than it counted.
     */
    @Test
    public void testPrefixEstimateRowCountBudget()
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text, pad text)");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        // 5000 rows over only 3 terms starting with 'k', far fewer than those matching the prefix in the other terms
        for (int pk = 0; pk < 5000; pk++)
            execute("INSERT INTO %s (pk, v, pad) VALUES (?, ?, ?)", pk, "k" + (pk % 3), ROW_PADDING);
        for (int pk = 5000; pk < 5010; pk++)
            execute("INSERT INTO %s (pk, v, pad) VALUES (?, ?, ?)", pk, String.format("a%03d", pk), ROW_PADDING);
        for (int pk = 5010; pk < 5020; pk++)
            execute("INSERT INTO %s (pk, v, pad) VALUES (?, ?, ?)", pk, String.format("z%03d", pk), ROW_PADDING);

        assertThatPlanFor("SELECT pk FROM %s WHERE v LIKE 'k%%'", 5000)
            .hasEstimatedRowsCountBetween(4096.0, 5020.0);
        assertThatPlanFor("SELECT pk FROM %s WHERE v LIKE 'k%%2'", 1666)
            .hasEstimatedRowsCountBetween(1666.0, 5020.0);
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
            // segments may contain '%' (VALUE_ALPHABET does), which is a wildcard wherever it appears
            String segment = randomSegment(values);

            // LIKE '%x'
            checkPattern(rows, "%" + segment);
            // LIKE '%x%'
            checkPattern(rows, "%" + segment + "%");
            // LIKE 'x%'
            checkPattern(rows, segment + "%");

            // generic patterns: random segments separated by wildcards, with or without leading/trailing ones
            StringBuilder pattern = new StringBuilder();
            if (random.nextBoolean())
                pattern.append('%');
            pattern.append(randomString(SEGMENT_ALPHABET, 1, 3));
            int inner = 1 + random.nextInt(2);
            for (int k = 0; k < inner; k++)
                pattern.append('%').append(random.nextBoolean() ? randomString(SEGMENT_ALPHABET, 1, 2) : randomSegment(values));
            if (random.nextBoolean())
                pattern.append('%');
            checkPattern(rows, pattern.toString());
        }
    }

    /**
     * Checks a LIKE pattern through the index against client-side brute force with full LIKE semantics, and the
     * post-filter of the operator the CQL layer classifies the pattern as against brute force as well.
     */
    private void checkPattern(Map<Integer, String> rows, String pattern)
    {
        if (pattern.chars().allMatch(c -> c == '%'))
            return; // rejected as empty, see testInnerWildcards

        Pair<Operator, ByteBuffer> specific = SingleColumnRestriction.LikeRestriction.makeSpecific(UTF8Type.instance.decompose(pattern));
        String operatorValue = UTF8Type.instance.compose(specific.right);
        checkQuery(rows, "SELECT pk FROM %s WHERE v LIKE ?", pattern,
                   value -> likeMatches(value, pattern),
                   postFilter(specific.left, operatorValue),
                   "LIKE '" + pattern + "' (" + specific.left + " '" + operatorValue + "')");
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
