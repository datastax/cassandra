/*
 * Copyright DataStax, Inc.
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.cassandra.index.sai.SSTableIndex;
import org.apache.cassandra.index.sai.memory.MemtableIndex;
import org.apache.cassandra.index.sai.memory.TrieMemtableIndex;
import org.assertj.core.api.Assertions;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.SAIUtil;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.disk.v1.SegmentBuilder;
import org.apache.cassandra.index.sai.plan.QueryController;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.cassandra.index.sai.analyzer.AnalyzerEqOperatorSupport.EQ_AMBIGUOUS_ERROR;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class BM25Test extends SAITester
{
    @Parameterized.Parameter
    public Version testVersion;

    @Parameterized.Parameters(name = "version={0}")
    public static List<Object> data()
    {
        return Arrays.asList(new Object[]{ Version.BM25_EARLIEST, Version.ED });
    }

    // Pattern that treats apostrophes within words as part of the word
    private static final Pattern PATTERN = Pattern.compile("[^\\w']+|'(?=\\s)|(?<=\\s)'");
    public static final int DATASET_BODY_COLUMN = 3;

    @Before
    public void setup() throws Throwable
    {
        SAIUtil.setCurrentVersion(testVersion);
    }

    @Test
    public void testTwoIndexes()
    {
        // create un-analyzed index
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text)");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'org.apache.cassandra.index.sai.StorageAttachedIndex'");
        execute("INSERT INTO %s (k, v) VALUES (1, 'apple')");

        // BM25 should fail with only an equality index
        assertInvalidMessage("BM25 ordering on column v requires an analyzed index",
                             "SELECT k FROM %s WHERE v : 'apple' ORDER BY v BM25 OF 'apple' LIMIT 3");

        createAnalyzedIndex();
        // BM25 query should work now
        var result = execute("SELECT k FROM %s WHERE v : 'apple' ORDER BY v BM25 OF 'apple' LIMIT 3");
        assertRows(result, row(1));
    }

    @Test
    public void testDeletedRow() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text)");
        createAnalyzedIndex();
        execute("INSERT INTO %s (k, v) VALUES (1, 'apple')");
        execute("INSERT INTO %s (k, v) VALUES (2, 'apple juice')");
        var result = execute("SELECT k FROM %s ORDER BY v BM25 OF 'apple' LIMIT 3");
        assertThat(result).hasSize(2);
        execute("DELETE FROM %s WHERE k=2");
        String select = "SELECT k FROM %s ORDER BY v BM25 OF 'apple' LIMIT 3";
        beforeAndAfterFlush(() -> assertRows(execute(select), row(1)));
    }

    @Test
    public void testDeletedColumn() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text)");
        createAnalyzedIndex();
        execute("INSERT INTO %s (k, v) VALUES (1, 'apple')");
        execute("INSERT INTO %s (k, v) VALUES (2, 'apple juice')");
        String select = "SELECT k FROM %s ORDER BY v BM25 OF 'apple' LIMIT 3";
        assertRows(execute(select), row(1), row(2));
        execute("DELETE v FROM %s WHERE k = 2");
        beforeAndAfterFlush(() -> assertRows(execute(select), row(1)));
    }

    @Test
    public void testDeletedRowWithPredicate() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text, n int)");
        createIndex("CREATE CUSTOM INDEX ON %s(n) USING 'org.apache.cassandra.index.sai.StorageAttachedIndex'");
        createAnalyzedIndex();
        execute("INSERT INTO %s (k, v, n) VALUES (1, 'apple', 0)");
        execute("INSERT INTO %s (k, v, n) VALUES (2, 'apple juice', 0)");
        String select = "SELECT k FROM %s WHERE n = 0 ORDER BY v BM25 OF 'apple' LIMIT 3";
        assertRows(execute(select), row(1), row(2));
        execute("DELETE FROM %s WHERE k=2");
        beforeAndAfterFlush(() -> assertRows(execute(select), row(1)));
    }

    @Test
    public void testTwoIndexesAmbiguousPredicate() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text)");

        createAnalyzedIndex();
        // Create  un-analyzed indexes
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'org.apache.cassandra.index.sai.StorageAttachedIndex'");

        execute("INSERT INTO %s (k, v) VALUES (1, 'apple')");
        execute("INSERT INTO %s (k, v) VALUES (2, 'apple juice')");
        execute("INSERT INTO %s (k, v) VALUES (3, 'orange juice')");

        // equality predicate is ambiguous (both analyzed and un-analyzed indexes could support it) so it should
        // be rejected
        beforeAndAfterFlush(() -> {
            // Single predicate
            assertInvalidMessage(String.format(EQ_AMBIGUOUS_ERROR, 'v', getIndex(0), getIndex(1)),
                                 "SELECT k FROM %s WHERE v = 'apple'");

            // AND
            assertInvalidMessage(String.format(EQ_AMBIGUOUS_ERROR, 'v', getIndex(0), getIndex(1)),
                                 "SELECT k FROM %s WHERE v = 'apple' AND v : 'juice'");

            // OR
            assertInvalidMessage(String.format(EQ_AMBIGUOUS_ERROR, 'v', getIndex(0), getIndex(1)),
                                 "SELECT k FROM %s WHERE v = 'apple' OR v : 'juice'");
        });
    }

    @Test
    public void testTwoIndexesWithEqualsUnsupported() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text)");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'org.apache.cassandra.index.sai.StorageAttachedIndex'");
        // analyzed index with equals_behavior:unsupported option
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'org.apache.cassandra.index.sai.StorageAttachedIndex' " +
                    "WITH OPTIONS = { 'equals_behaviour_when_analyzed': 'unsupported', " +
                    "'index_analyzer':'{\"tokenizer\":{\"name\":\"standard\"},\"filters\":[{\"name\":\"porterstem\"}]}' }");

        execute("INSERT INTO %s (k, v) VALUES (1, 'apple')");
        execute("INSERT INTO %s (k, v) VALUES (2, 'apple juice')");

        beforeAndAfterFlush(() -> {
            // combining two EQ predicates is not allowed
            assertInvalid("SELECT k FROM %s WHERE v = 'apple' AND v = 'juice'");

            // combining EQ and MATCH predicates is also not allowed (when we're not converting EQ to MATCH)
            assertInvalid("SELECT k FROM %s WHERE v = 'apple' AND v : 'apple'");

            // combining two MATCH predicates is fine
            assertRows(execute("SELECT k FROM %s WHERE v : 'apple' AND v : 'juice'"),
                       row(2));

            // = operator should use un-analyzed index since equals is unsupported in analyzed index
            assertRows(execute("SELECT k FROM %s WHERE v = 'apple'"),
                       row(1));

            // : operator should use analyzed index
            assertRows(execute("SELECT k FROM %s WHERE v : 'apple'"),
                       row(1), row(2));
        });
    }

    @Test
    public void testComplexQueriesWithMultipleIndexes() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v1 text, v2 text, v3 int)");

        // Create mix of analyzed, unanalyzed, and non-text indexes
        createIndex("CREATE CUSTOM INDEX ON %s(v1) USING 'org.apache.cassandra.index.sai.StorageAttachedIndex'");
        createAnalyzedIndex("v2");
        createIndex("CREATE CUSTOM INDEX ON %s(v3) USING 'org.apache.cassandra.index.sai.StorageAttachedIndex'");

        execute("INSERT INTO %s (k, v1, v2, v3) VALUES (1, 'apple', 'orange juice', 5)");
        execute("INSERT INTO %s (k, v1, v2, v3) VALUES (2, 'apple juice', 'apple', 10)");
        execute("INSERT INTO %s (k, v1, v2, v3) VALUES (3, 'banana', 'grape juice', 5)");

        beforeAndAfterFlush(() -> {
            // Complex query mixing different types of indexes and operators
            assertRows(execute("SELECT k FROM %s WHERE v1 = 'apple' AND v2 : 'juice' AND v3 = 5"),
                       row(1));

            // Mix of AND and OR conditions across different index types
            assertRows(execute("SELECT k FROM %s WHERE v3 = 5 AND (v1 = 'apple' OR v2 : 'apple')"),
                       row(1));

            // Multi-term analyzed query
            assertRows(execute("SELECT k FROM %s WHERE v2 : 'orange juice'"),
                       row(1));

            // Range query with text match
            assertRows(execute("SELECT k FROM %s WHERE v3 >= 5 AND v2 : 'juice'"),
                       row(1), row(3));
        });
    }

    @Test
    public void testMatchingAllowed() throws Throwable
    {
        // match operator should be allowed with BM25 on the same column
        // (seems obvious but exercises a corner case in the internal RestrictionSet processing)
        createSimpleTable();

        execute("INSERT INTO %s (k, v) VALUES (1, 'apple')");

        beforeAndAfterFlush(() ->
        {
            var result = execute("SELECT k FROM %s WHERE v : 'apple' ORDER BY v BM25 OF 'apple' LIMIT 3");
            assertRows(result, row(1));
        });
    }

    @Test
    public void testUnknownQueryTerm() throws Throwable
    {
        createSimpleTable();

        execute("INSERT INTO %s (k, v) VALUES (1, 'apple')");

        beforeAndAfterFlush(() ->
                            {
                                var result = execute("SELECT k FROM %s ORDER BY v BM25 OF 'orange' LIMIT 1");
                                assertEmpty(result);
                            });
    }

    @Test
    public void testDuplicateQueryTerm() throws Throwable
    {
        createSimpleTable();

        execute("INSERT INTO %s (k, v) VALUES (1, 'apple')");

        beforeAndAfterFlush(() ->
                            {
                                var result = execute("SELECT k FROM %s ORDER BY v BM25 OF 'apple apple' LIMIT 1");
                                assertRows(result, row(1));
                            });
    }

    @Test
    public void testEmptyQuery() throws Throwable
    {
        createSimpleTable();

        execute("INSERT INTO %s (k, v) VALUES (1, 'apple')");

        beforeAndAfterFlush(() ->
                            assertInvalidMessage("BM25 query must contain at least one term (perhaps your analyzer is discarding tokens you didn't expect)",
                                                 "SELECT k FROM %s ORDER BY v BM25 OF '+' LIMIT 1"));
    }

    @Test
    public void testTermFrequencyOrdering() throws Throwable
    {
        createSimpleTable();

        // Insert documents with varying frequencies of the term "apple"
        execute("INSERT INTO %s (k, v) VALUES (1, 'apple')");
        execute("INSERT INTO %s (k, v) VALUES (2, 'apple apple')");
        execute("INSERT INTO %s (k, v) VALUES (3, 'apple apple apple')");

        beforeAndAfterFlush(() ->
        {
            // Results should be ordered by term frequency (highest to lowest)
            var result = execute("SELECT k FROM %s ORDER BY v BM25 OF 'apple' LIMIT 3");
            assertRows(result,
                       row(3),  // 3 occurrences
                       row(2),  // 2 occurrences
                       row(1)); // 1 occurrence
        });
    }

    @Test
    public void testTermFrequenciesWithOverwrites() throws Throwable
    {
        createSimpleTable();

        // Insert documents with varying frequencies of the term "apple", but overwrite the first term
        // This exercises the code that is supposed to reset frequency counts for overwrites
        execute("INSERT INTO %s (k, v) VALUES (1, 'apple')");
        execute("INSERT INTO %s (k, v) VALUES (1, 'apple')");
        execute("INSERT INTO %s (k, v) VALUES (1, 'apple')");
        execute("INSERT INTO %s (k, v) VALUES (1, 'apple')");
        execute("INSERT INTO %s (k, v) VALUES (2, 'apple apple')");
        execute("INSERT INTO %s (k, v) VALUES (3, 'apple apple apple')");

        beforeAndAfterFlush(() ->
                            {
                                // Results should be ordered by term frequency (highest to lowest)
                                var result = execute("SELECT k FROM %s ORDER BY v BM25 OF 'apple' LIMIT 3");
                                assertRows(result,
                                           row(3),  // 3 occurrences
                                           row(2),  // 2 occurrences
                                           row(1)); // 1 occurrence
                            });
    }

    @Test
    public void testDocumentLength() throws Throwable
    {
        createSimpleTable();
        // Create documents with same term frequency but different lengths
        execute("INSERT INTO %s (k, v) VALUES (1, 'test test')");
        execute("INSERT INTO %s (k, v) VALUES (2, 'test test other words here to make it longer')");
        execute("INSERT INTO %s (k, v) VALUES (3, 'test test extremely long document with many additional words to significantly increase the document length while maintaining the same term frequency for our target term')");

        beforeAndAfterFlush(() ->
        {
            // Documents with same term frequency should be ordered by length (shorter first)
            var result = execute("SELECT k FROM %s ORDER BY v BM25 OF 'test' LIMIT 3");
            assertRows(result,
                       row(1),
                       row(2),
                       row(3));
        });
    }

    @Test
    public void testMultiTermQueryScoring() throws Throwable
    {
        createSimpleTable();
        // Two terms, but "apple" appears in fewer documents
        execute("INSERT INTO %s (k, v) VALUES (1, 'apple banana')");
        execute("INSERT INTO %s (k, v) VALUES (2, 'apple apple banana')");
        execute("INSERT INTO %s (k, v) VALUES (3, 'apple banana banana')");
        execute("INSERT INTO %s (k, v) VALUES (4, 'apple apple banana banana')");
        execute("INSERT INTO %s (k, v) VALUES (5, 'banana banana')");

        beforeAndAfterFlush(() ->
        {
            var result = execute("SELECT k FROM %s ORDER BY v BM25 OF 'apple banana' LIMIT 4");
            assertRows(result,
                       row(2),  // Highest frequency of most important term
                       row(4),  // More mentions of both terms
                       row(1),  // One of each term
                       row(3)); // Low frequency of most important term
        });
    }

    @Test
    public void testIrrelevantRowsScoring() throws Throwable
    {
        createSimpleTable();
        // Insert pizza reviews with varying relevance to "crispy crust"
        execute("INSERT INTO %s (k, v) VALUES (1, 'The pizza had a crispy crust and was delicious')"); // Basic mention
        execute("INSERT INTO %s (k, v) VALUES (2, 'Very crispy crispy crust, perfectly cooked')"); // Emphasized crispy
        execute("INSERT INTO %s (k, v) VALUES (3, 'The crust crust crust was okay, nothing special')"); // Only crust mentions
        execute("INSERT INTO %s (k, v) VALUES (4, 'Super crispy crispy crust crust, best pizza ever!')");  // Most mentions of both
        execute("INSERT INTO %s (k, v) VALUES (5, 'The toppings were good but the pizza was soggy')"); // Irrelevant review

        beforeAndAfterFlush(this::assertIrrelevantRowsCorrect);
    }

    private void assertIrrelevantRowsCorrect()
    {
        var result = execute("SELECT k FROM %s ORDER BY v BM25 OF 'crispy crust' LIMIT 5");
        assertRows(result,
                   row(4), // Highest frequency of both terms
                   row(2), // High frequency of 'crispy', one 'crust'
                   row(1)); // One mention of each term
        // Rows 3 and 5 do not contain all terms
    }

    @Test
    public void testIrrelevantRowsWithCompaction()
    {
        // same dataset as testIrrelevantRowsScoring, but split across two sstables
        createSimpleTable();
        disableCompaction();

        execute("INSERT INTO %s (k, v) VALUES (1, 'The pizza had a crispy crust and was delicious')"); // Basic mention
        execute("INSERT INTO %s (k, v) VALUES (2, 'Very crispy crispy crust, perfectly cooked')"); // Emphasized crispy
        flush();

        execute("INSERT INTO %s (k, v) VALUES (3, 'The crust crust crust was okay, nothing special')"); // Only crust mentions
        execute("INSERT INTO %s (k, v) VALUES (4, 'Super crispy crispy crust crust, best pizza ever!')");  // Most mentions of both
        execute("INSERT INTO %s (k, v) VALUES (5, 'The toppings were good but the pizza was soggy')"); // Irrelevant review
        flush();

        assertIrrelevantRowsCorrect();

        compact();
        assertIrrelevantRowsCorrect();

        // Force segmentation and requery
        long original = SegmentBuilder.updateLastValidSegmentRowId(2);
        compact();
        assertIrrelevantRowsCorrect();

        SegmentBuilder.updateLastValidSegmentRowId(original);
    }

    private void createSimpleTable()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text)");
        createAnalyzedIndex();
    }

    private String createAnalyzedIndex()
    {
        return createAnalyzedIndex("v");
    }

    private String createAnalyzedIndex(String column)
    {
        return createAnalyzedIndex(column, false);
    }

    private String createAnalyzedIndex(String column, boolean lowercase)
    {
        return createIndex("CREATE CUSTOM INDEX ON %s(" + column + ") " +
                           "USING 'org.apache.cassandra.index.sai.StorageAttachedIndex' " +
                           "WITH OPTIONS = {" +
                           "'index_analyzer': '{" +
                           "\"tokenizer\" : {\"name\" : \"standard\"}, " +
                           "\"filters\" : [{\"name\" : \"porterstem\"}" +
                           (lowercase ? ", {\"name\" : \"lowercase\"}]" : "]")
                           + "}'}"
        );
    }

    @Test
    public void testWithPredicate() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, p int, v text)");
        createAnalyzedIndex();
        createIndex("CREATE CUSTOM INDEX ON %s(p) USING 'StorageAttachedIndex'");

        // Insert documents with varying frequencies of the term "apple"
        execute("INSERT INTO %s (k, p, v) VALUES (1, 5, 'apple')");
        execute("INSERT INTO %s (k, p, v) VALUES (2, 5, 'apple apple')");
        execute("INSERT INTO %s (k, p, v) VALUES (3, 5, 'apple apple apple')");
        execute("INSERT INTO %s (k, p, v) VALUES (4, 6, 'apple apple apple')");
        execute("INSERT INTO %s (k, p, v) VALUES (5, 7, 'apple apple apple')");

        beforeAndAfterFlush(() ->
        {
            // Results should be ordered by term frequency (highest to lowest)
            var result = execute("SELECT k FROM %s WHERE p = 5 ORDER BY v BM25 OF 'apple' LIMIT 3");
            assertRows(result,
                       row(3),  // 3 occurrences
                       row(2),  // 2 occurrences
                       row(1)); // 1 occurrence
        });
    }

    @Test
    public void testWidePartition() throws Throwable
    {
        createTable("CREATE TABLE %s (k1 int, k2 int, v text, PRIMARY KEY (k1, k2))");
        createAnalyzedIndex();

        // Insert documents with varying frequencies of the term "apple"
        execute("INSERT INTO %s (k1, k2, v) VALUES (0, 1, 'apple')");
        execute("INSERT INTO %s (k1, k2, v) VALUES (0, 2, 'apple apple')");
        execute("INSERT INTO %s (k1, k2, v) VALUES (0, 3, 'apple apple apple')");

        beforeAndAfterFlush(() ->
        {
            // Results should be ordered by term frequency (highest to lowest)
            var result = execute("SELECT k2 FROM %s ORDER BY v BM25 OF 'apple' LIMIT 3");
            assertRows(result,
                       row(3),  // 3 occurrences
                       row(2),  // 2 occurrences
                       row(1)); // 1 occurrence
        });
    }

    @Test
    public void testWidePartitionWithPkPredicate() throws Throwable
    {
        createTable("CREATE TABLE %s (k1 int, k2 int, v text, PRIMARY KEY (k1, k2))");
        createAnalyzedIndex();

        // Insert documents with varying frequencies of the term "apple"
        execute("INSERT INTO %s (k1, k2, v) VALUES (0, 1, 'apple')");
        execute("INSERT INTO %s (k1, k2, v) VALUES (0, 2, 'apple apple')");
        execute("INSERT INTO %s (k1, k2, v) VALUES (0, 3, 'apple apple apple')");
        execute("INSERT INTO %s (k1, k2, v) VALUES (1, 3, 'apple apple apple')");
        execute("INSERT INTO %s (k1, k2, v) VALUES (2, 3, 'apple apple apple')");

        beforeAndAfterFlush(() ->
        {
            // Results should be ordered by term frequency (highest to lowest)
            var result = execute("SELECT k2 FROM %s WHERE k1 = 0 ORDER BY v BM25 OF 'apple' LIMIT 3");
            assertRows(result,
                       row(3),  // 3 occurrences
                       row(2),  // 2 occurrences
                       row(1)); // 1 occurrence
        });
    }

    @Test
    public void testWidePartitionWithPredicate() throws Throwable
    {
        createTable("CREATE TABLE %s (k1 int, k2 int, p int, v text, PRIMARY KEY (k1, k2))");
        createAnalyzedIndex();
        createIndex("CREATE CUSTOM INDEX ON %s(p) USING 'StorageAttachedIndex'");

        // Insert documents with varying frequencies of the term "apple"
        execute("INSERT INTO %s (k1, k2, p, v) VALUES (0, 1, 5, 'apple')");
        execute("INSERT INTO %s (k1, k2, p, v) VALUES (0, 2, 5, 'apple apple')");
        execute("INSERT INTO %s (k1, k2, p, v) VALUES (0, 3, 5, 'apple apple apple')");
        execute("INSERT INTO %s (k1, k2, p, v) VALUES (0, 4, 6, 'apple apple apple')");
        execute("INSERT INTO %s (k1, k2, p, v) VALUES (0, 5, 7, 'apple apple apple')");

        beforeAndAfterFlush(() ->
        {
            // Results should be ordered by term frequency (highest to lowest)
            var result = execute("SELECT k2 FROM %s WHERE p = 5 ORDER BY v BM25 OF 'apple' LIMIT 3");
            assertRows(result,
                       row(3),  // 3 occurrences
                       row(2),  // 2 occurrences
                       row(1)); // 1 occurrence
        });
    }

    @Test
    public void testWithPredicateSearchThenOrder() throws Throwable
    {
        QueryController.QUERY_OPT_LEVEL = 0;
        testWithPredicate();
    }

    @Test
    public void testWidePartitionWithPredicateOrderThenSearch() throws Throwable
    {
        QueryController.QUERY_OPT_LEVEL = 1;
        testWidePartitionWithPredicate();
    }

    @Test
    public void testQueryWithNulls() throws Throwable
    {
        createSimpleTable();

        execute("INSERT INTO %s (k, v) VALUES (0, null)");
        execute("INSERT INTO %s (k, v) VALUES (1, 'test document')");
        beforeAndAfterFlush(() ->
        {
            var result = execute("SELECT k FROM %s ORDER BY v BM25 OF 'test' LIMIT 1");
            assertRows(result, row(1));
        });
    }

    @Test
    public void testQueryEmptyTable()
    {
        createSimpleTable();
        var result = execute("SELECT k FROM %s ORDER BY v BM25 OF 'test' LIMIT 1");
        assertThat(result).hasSize(0);
    }

    @Test
    public void testBM25RaceConditionConcurrentQueriesInInvertedIndexSearcher() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, v text, PRIMARY KEY (pk))");
        createAnalyzedIndex();

        // Create 3 docs that have the same BM25 score and will be our top docs
        execute("INSERT INTO %s (pk, v) VALUES (1, 'apple apple apple')");
        execute("INSERT INTO %s (pk, v) VALUES (2, 'apple apple apple')");
        execute("INSERT INTO %s (pk, v) VALUES (3, 'apple apple apple')");

        // Now insert a lot of docs that will hit the query, but will be lower in frequency and therefore in score
        for (int i = 4; i < 10000; i++)
            execute("INSERT INTO %s (pk, v) VALUES (?, 'apple apple')", i);

        // Bug only present in sstable
        flush();

        // Trigger many concurrent queries
        final ExecutorService executor = Executors.newFixedThreadPool(10);
        String select = "SELECT pk FROM %s ORDER BY v BM25 OF 'apple' LIMIT 3";
        var futures = new ArrayList<Future<UntypedResultSet>>();
        for (int i = 0; i < 1000; i++)
            futures.add(executor.submit(() -> execute(select)));

        // The top results are always the same rows
        for (Future<UntypedResultSet> future : futures)
            assertRowsIgnoringOrder(future.get(), row(1), row(2), row(3));

        // Shutdown executor
        assertEquals(0, executor.shutdownNow().size());
    }

    @Test
    public void testWildcardSelection()
    {
        createTable("CREATE TABLE %s (k int, c int, v text, PRIMARY KEY (k, c))");
        createAnalyzedIndex();
        execute("INSERT INTO %s (k, c, v) VALUES (1, 1, 'apple')");

        var result = execute("SELECT * FROM %s ORDER BY v BM25 OF 'apple' LIMIT 3");
        assertThat(result).hasSize(1);
    }

    @Test
    public void cannotHaveAggregationOnBM25Query()
    {
        createSimpleTable();

        execute("INSERT INTO %s (k, v) VALUES (1, '4')");
        execute("INSERT INTO %s (k, v) VALUES (2, '3')");
        execute("INSERT INTO %s (k, v) VALUES (3, '2')");
        execute("INSERT INTO %s (k, v) VALUES (4, '1')");

        assertThatThrownBy(() -> execute("SELECT max(v) FROM %s ORDER BY v BM25 OF 'apple' LIMIT 4"))
                .isInstanceOf(InvalidRequestException.class)
                .hasMessage(SelectStatement.TOPK_AGGREGATION_ERROR);

        assertThatThrownBy(() -> execute("SELECT max(v) FROM %s WHERE k = 1 ORDER BY v BM25 OF 'apple' LIMIT 4"))
                .isInstanceOf(InvalidRequestException.class)
                .hasMessage(SelectStatement.TOPK_AGGREGATION_ERROR);

        assertThatThrownBy(() -> execute("SELECT * FROM %s GROUP BY k ORDER BY v BM25 OF 'apple' LIMIT 4"))
                .isInstanceOf(InvalidRequestException.class)
                .hasMessage(SelectStatement.TOPK_AGGREGATION_ERROR);

        assertThatThrownBy(() -> execute("SELECT count(*) FROM %s ORDER BY v BM25 OF 'apple' LIMIT 4"))
                .isInstanceOf(InvalidRequestException.class)
                .hasMessage(SelectStatement.TOPK_AGGREGATION_ERROR);
    }

    @Test
    public void testBM25andFilterz() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, category text, score int, title text, body text)");
        createAnalyzedIndex("body");
        createIndex("CREATE CUSTOM INDEX ON %s (score) USING 'StorageAttachedIndex'");
        insertPrimitiveData();
        beforeAndAfterFlush(
                () -> {
                    // 10 docs have score 3 and 3 of those have "health"
                    var result = execute("SELECT * FROM %s WHERE score = 3 ORDER BY body BM25 OF ? LIMIT 10",
                                         "health");
                    assertThat(result).hasSize(3);

                    // 4 docs have score 2 and one of those has "discussed"
                    result = execute("SELECT * FROM %s WHERE score = 2 ORDER BY body BM25 OF ? LIMIT 10",
                                         "discussed");
                    assertThat(result).hasSize(1);
                });
    }

    @Test
    public void testOrderingSeveralSSTablesWithMapPredicate() throws Throwable
    {
        // Force search-then-sort
        QueryController.QUERY_OPT_LEVEL = 0;
        createTable("CREATE TABLE %s (id int PRIMARY KEY, category text, map_category map<int, int>)");
        createAnalyzedIndex("category", true);
        createIndex("CREATE CUSTOM INDEX ON %s (entries(map_category)) USING 'StorageAttachedIndex'");
        // We don't want compaction to merge the two sstables since they are key to testing this code path.
        disableCompaction();

        // Insert documents so that they all have the same bm25 score and are easy to query across sstables
        for (int i = 0; i < 10; i++)
        {
            execute("INSERT INTO %s (id, category, map_category) VALUES (?, ?, ?)",
                    i, "Health", map(0, i));
            if (i == 4)
                flush();
        }

        // Confirm that the memtable/sstable and sstable/sstable pairings work as expected.
        beforeAndAfterFlush(() -> {
            // Submit a query that will fetch keys from 2 overlapping sstables. The key is that they are overlapping
            // because we have optimizations that will skip keys that are out of the sstable's range. In this case,
            // the actual bm25 data doesn't matter because we are covering the edge case of mapping PrK back to
            // its value here.
            assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE map_category[0] >= 4 AND map_category[0] <= 6 ORDER BY category BM25 OF 'health' LIMIT 10"),
                                    row(4), row(5), row(6));
        });
    }

    @Test
    public void testErrorMessages()
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, category text, score int, " +
                    "title text, body text, bodyset set<text>, " +
                    "map_category map<int, text>, map_body map<text, text>)");
        createAnalyzedIndex("body", true);
        createAnalyzedIndex("bodyset", true);
        createAnalyzedIndex("map_body", true);

        // Improve message issue CNDB-13514
        assertInvalidMessage("BM25 ordering on column bodyset requires an analyzed index",
                             "SELECT * FROM %s ORDER BY bodyset BM25 OF ? LIMIT 10");

        // Discussion of message incosistency CNDB-13526
        assertInvalidMessage("Ordering on non-clustering column requires each restricted column to be indexed except for fully-specified partition keys",
                             "SELECT * FROM %s WHERE map_body CONTAINS KEY 'Climate' ORDER BY body BM25 OF ? LIMIT 10");
    }

    @Test
    public void testWithLowercase() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, body text)");
        createAnalyzedIndex("body", true);
        execute("INSERT INTO %s (id, body) VALUES (?, ?)", 1, "Hi hi");
        execute("INSERT INTO %s (id, body) VALUES (?, ?)", 2, "hi hi longer");
        executeQuery(Arrays.asList(1, 2), "SELECT * FROM %s ORDER BY body BM25 OF 'hi' LIMIT 4");
    }

    @Test
    public void testCollections() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, category text, score int, " +
                    "title text, body text, bodyset set<text>, " +
                    "map_category map<int, text>, map_body map<text, text>)");
        createAnalyzedIndex("body", true);
        createAnalyzedIndex("bodyset", true);
        createAnalyzedIndex("map_body", true);
        createIndex("CREATE CUSTOM INDEX ON %s (score) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s (category) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s (map_category) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s (KEYS(map_body)) USING 'StorageAttachedIndex'");
        insertCollectionData();
        analyzeDataset("climate");
        analyzeDataset("health");

        beforeAndAfterFlush(
        () -> {
            // ID 10: total words = 12, climate occurrences = 4
            // ID 18: total words = 13, climate occurrences = 4
            // ID 0: total words = 16, climate occurrences = 3
            // ID 15: total words = 11, climate occurrences = 2
            // ID 5: total words = 13, climate occurrences = 2
            // ID 11: total words = 12, climate occurrences = 1
            // ID 17: total words = 14, climate occurrences = 1
            executeQuery(Arrays.asList(10, 18, 0, 15, 5, 11, 17), "SELECT * FROM %s  ORDER BY body BM25 OF ? LIMIT 10",
                         "climate");
            executeQuery(Arrays.asList(10, 18, 0), "SELECT * FROM %s WHERE score = 5 ORDER BY body BM25 OF ? LIMIT 10",
                         "climate");
            executeQuery(Arrays.asList(10, 18, 0, 15, 5, 11, 17), "SELECT * FROM %s WHERE bodyset CONTAINS 'climate' ORDER BY body BM25 OF ? LIMIT 10",
                         "climate");
            executeQuery(Arrays.asList(15, 5, 11, 17), "SELECT * FROM %s WHERE bodyset CONTAINS 'health' ORDER BY body BM25 OF ? LIMIT 10",
                         "climate");
            executeQuery(Arrays.asList(10, 18, 0, 15, 5, 11, 17), "SELECT * FROM %s WHERE map_category CONTAINS 'Climate' ORDER BY body BM25 OF ? LIMIT 10",
                         "climate");
            executeQuery(Arrays.asList(18, 15, 5, 11, 17), "SELECT * FROM %s WHERE map_category CONTAINS 'Health' ORDER BY body BM25 OF ? LIMIT 10",
                         "climate");
            executeQuery(Arrays.asList(10, 18, 0, 15, 5, 11, 17), "SELECT * FROM %s WHERE map_body CONTAINS 'Climate' ORDER BY body BM25 OF ? LIMIT 10",
                         "climate");
            executeQuery(Arrays.asList(10, 18, 15, 5, 11, 17), "SELECT * FROM %s WHERE map_body CONTAINS 'health' ORDER BY body BM25 OF ? LIMIT 10",
                         "climate");
            executeQuery(Arrays.asList(10, 18, 15, 5, 11, 17), "SELECT * FROM %s WHERE map_body CONTAINS KEY 'Health' ORDER BY body BM25 OF ? LIMIT 10",
                         "climate");

            // ID 3: total words = 15, health occurrences = 3
            // ID 11: total words = 12, health occurrences = 2
            // ID 5: total words = 13, health occurrences = 2
            // ID 8: total words = 13, health occurrences = 2
            // ID 17: total words = 14, health occurrences = 2
            // ID 13: total words = 11, health occurrences = 1
            // ID 15: total words = 11, health occurrences = 1
            executeQuery(Arrays.asList(5, 15), "SELECT * FROM %s WHERE score > 3 ORDER BY body BM25 OF ? LIMIT 10",
                         "health");
            executeQuery(Arrays.asList(3, 11, 8, 17, 13), "SELECT * FROM %s WHERE category = 'Health' " +
                                                          "ORDER BY body BM25 OF ? LIMIT 10",
                         "Health");
            executeQuery(Arrays.asList(3, 11, 8, 17, 13), "SELECT * FROM %s WHERE score <= 3 AND category = 'Health' " +
                                                          "ORDER BY body BM25 OF ? LIMIT 10",
                         "health");
        });
    }

    @Test
    public void testOrderingSeveralSegments() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, category text, score int," +
                    "title text, body text)");
        createAnalyzedIndex("body", true);
        createIndex("CREATE CUSTOM INDEX ON %s (score) USING 'StorageAttachedIndex'");
        insertPrimitiveData(0, 10);
        flush();
        insertPrimitiveData(10, 20);

        // One memtable, one sstable - different result from the reference in testCollections
        // ID 0 and 5 contain 3 and 2 climate occurrences correspondingly,
        // while ID 10 and 18 - 4 climate occurrences. However,
        // since the segment with 0-9 IDs have only 2 rows with climate and 10-19 - 5,
        // 0 and 5 win over 10 and 18.
        executeQuery(Arrays.asList(0, 5, 10, 18, 15, 11, 17), "SELECT * FROM %s  ORDER BY body BM25 OF ? LIMIT 10",
                "climate");
        executeQuery(Arrays.asList(0, 10, 18), "SELECT * FROM %s WHERE score = 5 ORDER BY body BM25 OF ? LIMIT 10",
                "climate");

        // Flush into Two sstables - same result as the different above
        flush();
        executeQuery(Arrays.asList(0, 5, 10, 18, 15, 11, 17), "SELECT * FROM %s  ORDER BY body BM25 OF ? LIMIT 10",
                "climate");
        executeQuery(Arrays.asList(0, 10, 18), "SELECT * FROM %s WHERE score = 5 ORDER BY body BM25 OF ? LIMIT 10",
                "climate");

        // Compact into one sstable - same as reference from testCollections
        compact();
        executeQuery(Arrays.asList(10, 18, 0, 15, 5, 11, 17), "SELECT * FROM %s  ORDER BY body BM25 OF ? LIMIT 10",
                "climate");
        executeQuery(Arrays.asList(10, 18, 0), "SELECT * FROM %s WHERE score = 5 ORDER BY body BM25 OF ? LIMIT 10",
                "climate");
    }

    /**
     * Asserts that memtable SAI index maintains expected row count, which is, then,
     * used to store row count in SSTable SAI index and its segments. This is also
     * asserted.
     */
    @Test
    public void testIndexMetaForNumRows()
    {
        SAIUtil.setCurrentVersion(Version.ED);

        createTable("CREATE TABLE %s (id int PRIMARY KEY, category text, score int, " +
                    "title text, body text, bodyset set<text>, " +
                    "map_category map<int, text>, map_body map<text, text>)");
        String bodyIndexName = createAnalyzedIndex("body", true);
        String scoreIndexName = createIndex("CREATE CUSTOM INDEX ON %s (score) USING 'StorageAttachedIndex'");
        String mapIndexName = createIndex("CREATE CUSTOM INDEX ON %s (map_category) USING 'StorageAttachedIndex'");
        insertCollectionData();
        int totalTermsCount = IntStream.range(0, DATASET.length)
                                       .map(this::calculateTotalTermsForRow)
                                       .sum();

        assertNumRowsMemtable(scoreIndexName, DATASET.length, DATASET.length);
        assertNumRowsMemtable(bodyIndexName, DATASET.length, totalTermsCount);
        assertNumRowsMemtable(mapIndexName, DATASET.length);
        execute("DELETE FROM %s WHERE id = ?", 4);
        // Deletion is not tracked by Memindex
        assertNumRowsMemtable(bodyIndexName, DATASET.length, totalTermsCount);
        // Test an update to different value for analyzed index
        execute("UPDATE %s SET body = ? WHERE id = ?", DATASET[10][DATASET_BODY_COLUMN], 6);
        totalTermsCount += calculateTotalTermsForRow(10) - calculateTotalTermsForRow(6);
        assertNumRowsMemtable(bodyIndexName, DATASET.length, totalTermsCount);
        // Update back to the original value
        execute("UPDATE %s SET body = ? WHERE id = ?", DATASET[6][DATASET_BODY_COLUMN], 10);
        totalTermsCount += calculateTotalTermsForRow(6) - calculateTotalTermsForRow(10);
        assertNumRowsMemtable(bodyIndexName, DATASET.length, totalTermsCount);
        // Flush will account for the deleted row
        totalTermsCount -= calculateTotalTermsForRow(4);
        flush();
        assertNumRowsAndTotalTermsSSTable(scoreIndexName, DATASET.length - 1, DATASET.length - 1);
        assertNumRowsAndTotalTermsSSTable(bodyIndexName, DATASET.length - 1, totalTermsCount);
        assertNumRowsSSTable(mapIndexName, DATASET.length - 1);
        execute("DELETE FROM %s WHERE id = ?", 9);
        flush();
        assertNumRowsAndTotalTermsSSTable(scoreIndexName, DATASET.length - 1, DATASET.length - 1);
        assertNumRowsAndTotalTermsSSTable(bodyIndexName, DATASET.length - 1, totalTermsCount);
        assertNumRowsSSTable(mapIndexName, DATASET.length - 1);
        compact();
        totalTermsCount -= calculateTotalTermsForRow(9);
        assertNumRowsAndTotalTermsSSTable(scoreIndexName, DATASET.length - 2, DATASET.length - 2);
        assertNumRowsAndTotalTermsSSTable(bodyIndexName, DATASET.length - 2, totalTermsCount);
        assertNumRowsSSTable(mapIndexName, DATASET.length - 2);
    }

    private void assertNumRowsMemtable(String indexName, int expectedNumRows)
    {
        assertNumRowsMemtable(indexName, expectedNumRows, -1);
    }

    private void assertNumRowsMemtable(String indexName, int expectedNumRows, int expectedTotalTermsCount)
    {
        int rowCount = 0;
        long termCount = 0;

        for (var memtable : getCurrentColumnFamilyStore().getAllMemtables())
        {
            MemtableIndex memIndex = getIndexContext(indexName).getLiveMemtables().get(memtable);
            assert memIndex instanceof TrieMemtableIndex;
            rowCount += ((TrieMemtableIndex) memIndex).indexedRows();
            termCount += ((TrieMemtableIndex) memIndex).approximateTotalTermCount();
        }
        assertEquals(expectedNumRows, rowCount);
        if (expectedTotalTermsCount >= 0)
            assertEquals(expectedTotalTermsCount, termCount);
    }

    private void assertNumRowsSSTable(String indexName, int expectedNumRows)
    {
        assertNumRowsAndTotalTermsSSTable(indexName, expectedNumRows, -1);
    }

    private void assertNumRowsAndTotalTermsSSTable(String indexName, int expectedNumRows, int expectedTotalTermsCount
    )
    {
        long indexRowCount = 0;
        long segmentRowCount = 0;
        long totalTermCount = 0;
        for (SSTableIndex sstableIndex : getIndexContext(indexName).getView())
        {
            indexRowCount += sstableIndex.getRowCount();
            for (var segment : sstableIndex.getSegments())
            {
                var metadata = segment.metadata;
                Assert.assertNotNull(metadata);
                segmentRowCount += metadata.numRows;
                totalTermCount += metadata.totalTermCount;
            }
        }
        assertEquals(indexRowCount, segmentRowCount);
        assertEquals(expectedNumRows, indexRowCount);
        if (expectedTotalTermsCount >= 0)
            assertEquals(expectedTotalTermsCount, totalTermCount);
    }

    private final static Object[][] DATASET =
    {
    { 0, "Climate", 5, "Climate change is a pressing issue. Climate patterns are shifting globally. Scientists study climate data daily.", 1 },
    { 1, "Technology", 3, "Technology is advancing. New technology in AI and robotics is groundbreaking.", 1 },
    { 2, "Economy", 4, "The economy is recovering. Economy experts are optimistic. However, the global economy still faces risks.", 1 },
    { 3, "Health", 3, "Health is wealth. Health policies need to be improved to ensure better public health outcomes.", 1 },
    { 4, "Education", 2, "Education is the foundation of success. Online education is booming.", 4 },
    { 5, "Climate", 4, "Climate and health are closely linked. Climate affects air quality and health outcomes.", 2 },
    { 6, "Education", 3, "Technology and education go hand in hand. EdTech is revolutionizing education through technology.", 3 },
    { 7, "Economy", 3, "The global economy is influenced by technology. Fintech is a key part of the economy today.", 2 },
    { 8, "Health", 3, "Education and health programs must be prioritized. Health education is vital in schools.", 2 },
    { 9, "Mixed", 3, "Technology, economy, and education are pillars of development.", 2 },
    { 10, "Climate", 5, "Climate climate climate. It's everywhere. Climate drives political and economic decisions.", 1 },
    { 11, "Health", 2, "Health concerns rise with climate issues. Health organizations are sounding the alarm.", 2 },
    { 12, "Economy", 3, "The economy is fluctuating. Uncertainty looms over the economy.", 1 },
    { 13, "Health", 3, "Cutting-edge technology is transforming healthcare. Healthtech merges health and technology.", 1 },
    { 14, "Education", 2, "Education reforms are underway. Education experts suggest holistic changes.", 1 },
    { 15, "Climate", 4, "Climate affects the economy and health. Climate events cost billions annually.", 1 },
    { 16, "Technology", 3, "Technology is the backbone of the modern economy. Without technology, economic growth stagnates.", 2 },
    { 17, "Health", 2, "Health is discussed less than economy or climate or technology, but health matters deeply.", 1 },
    { 18, "Climate", 5, "Climate change, climate policies, climate research—climate is the buzzword of our time.", 2 },
    { 19, "Mixed", 3, "Investments in education and technology will shape the future of the global economy.", 1 }
    };

    private void analyzeDataset(String term)
    {
        for (Object[] row : DATASET)
        {
            String body = (String) row[DATASET_BODY_COLUMN];
            String[] words = PATTERN.split(body.toLowerCase());

            long totalWords = words.length;
            long termCount = Arrays.stream(words)
                                   .filter(word -> word.equals(term))
                                   .count();

            if (termCount > 0)
                System.out.printf("            // ID %d: total words = %d, %s occurrences = %d%n",
                                  (Integer) row[0], totalWords, term, termCount);
        }
    }

    private int calculateTotalTermsForRow(int row)
    {
        String body = (String) DATASET[row][DATASET_BODY_COLUMN];
        return PATTERN.split(body.toLowerCase()).length;
    }

    private void insertPrimitiveData()
    {
        insertPrimitiveData(0, DATASET.length);
    }

    private void insertPrimitiveData(int start, int end)
    {
        for (int i = start; i < end; i++)
        {
            Object[] row = DATASET[i];
            execute(
            "INSERT INTO %s (id, category, score, body) VALUES (?, ?, ?, ?)",
            row[0],
            row[1],
            row[2],
            row[3]
            );
        }
    }

    private void insertCollectionData()
    {
        int setsize = 1;
        for (int row = 0; row < DATASET.length; row++)
        {
            var set = new HashSet<String>();
            for (int j = 0; j < setsize; j++)
                set.add((String) DATASET[row - j][3]);
            if (setsize >= 3)
                setsize -= 2;
            else
                setsize++;
            var map = new HashMap<Integer, String>();
            var map_text = new HashMap<String, String>();
            for (int j = 0; j <= row && j < 3; j++)
            {
                map.putIfAbsent((Integer) DATASET[row - j][2], (String) DATASET[row - j][1]);
                map_text.putIfAbsent((String) DATASET[row - j][1], (String) DATASET[row - j][3]);
            }

            execute(
            "INSERT INTO %s (id, category, score, body, bodyset, map_category, map_body) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            DATASET[row][0],
            DATASET[row][1],
            DATASET[row][2],
            DATASET[row][3],
            set,
            map,
            map_text
            );
        }
    }

    private void executeQuery(List<Integer> expected, String query, Object... values) throws Throwable
    {
        assertResult(execute(query, values), expected);
        prepare(query);
        assertResult(execute(query, values), expected);
    }

    private void assertResult(UntypedResultSet result, List<Integer> expected)
    {
        Assertions.assertThat(result).hasSize(expected.size());
        var ids = result.stream()
                        .map(row -> row.getInt("id"))
                        .collect(Collectors.toList());
        Assertions.assertThat(ids).isEqualTo(expected);
    }
}
