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

package org.apache.cassandra.index.sai.cql;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.SAIUtil;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.disk.v1.SegmentBuilder;
import org.apache.cassandra.index.sai.plan.QueryController;

import static org.apache.cassandra.index.sai.analyzer.AnalyzerEqOperatorSupport.EQ_AMBIGUOUS_ERROR;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

public class BM25Test extends SAITester
{
    @Before
    public void setup() throws Throwable
    {
        SAIUtil.setLatestVersion(Version.EC);
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

        // create analyzed index
        analyzeIndex();
        // BM25 query should work now
        var result = execute("SELECT k FROM %s WHERE v : 'apple' ORDER BY v BM25 OF 'apple' LIMIT 3");
        assertRows(result, row(1));
    }

    @Test
    public void testTwoIndexesAmbiguousPredicate() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text)");

        // Create analyzed and un-analyzed indexes
        analyzeIndex();
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'org.apache.cassandra.index.sai.StorageAttachedIndex'");

        execute("INSERT INTO %s (k, v) VALUES (1, 'apple')");
        execute("INSERT INTO %s (k, v) VALUES (2, 'apple juice')");
        execute("INSERT INTO %s (k, v) VALUES (3, 'orange juice')");

        // equality predicate is ambiguous (both analyzed and un-analyzed indexes could support it) so it should
        // be rejected
        beforeAndAfterFlush(() -> {
            // Single predicate
            assertInvalidMessage(String.format(EQ_AMBIGUOUS_ERROR, "v", getIndex(0), getIndex(1)),
                                 "SELECT k FROM %s WHERE v = 'apple'");

            // AND
            assertInvalidMessage(String.format(EQ_AMBIGUOUS_ERROR, "v", getIndex(0), getIndex(1)),
                                 "SELECT k FROM %s WHERE v = 'apple' AND v : 'juice'");

            // OR
            assertInvalidMessage(String.format(EQ_AMBIGUOUS_ERROR, "v", getIndex(0), getIndex(1)),
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
        createIndex("CREATE CUSTOM INDEX ON %s(v2) " +
                    "USING 'org.apache.cassandra.index.sai.StorageAttachedIndex' " +
                    "WITH OPTIONS = {" +
                    "'index_analyzer': '{" +
                    "\"tokenizer\" : {\"name\" : \"standard\"}, " +
                    "\"filters\" : [{\"name\" : \"porterstem\"}]" +
                    "}'" +
                    "}");
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
                            {
                                assertInvalidMessage("BM25 query must contain at least one term (perhaps your analyzer is discarding tokens you didn't expect)",
                                                     "SELECT k FROM %s ORDER BY v BM25 OF '+' LIMIT 1");
                            });
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
        // Rows 4 and 5 do not contain all terms
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
        SegmentBuilder.updateLastValidSegmentRowId(2);
        compact();
        assertIrrelevantRowsCorrect();
    }

    private void createSimpleTable()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text)");
        analyzeIndex();
    }

    private String analyzeIndex()
    {
        return createIndex("CREATE CUSTOM INDEX ON %s(v) " +
                           "USING 'org.apache.cassandra.index.sai.StorageAttachedIndex' " +
                           "WITH OPTIONS = {" +
                           "'index_analyzer': '{" +
                           "\"tokenizer\" : {\"name\" : \"standard\"}, " +
                           "\"filters\" : [{\"name\" : \"porterstem\"}]" +
                           "}'}"
        );
    }

    @Test
    public void testWithPredicate() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, p int, v text)");
        analyzeIndex();
        execute("CREATE CUSTOM INDEX ON %s(p) USING 'StorageAttachedIndex'");

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
        analyzeIndex();

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
        analyzeIndex();

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
        analyzeIndex();
        execute("CREATE CUSTOM INDEX ON %s(p) USING 'StorageAttachedIndex'");

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
}
