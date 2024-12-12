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

import org.junit.Test;

import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.plan.QueryController;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

public class BM25Test extends SAITester
{
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
    public void testTwoIndexes() throws Throwable
    {
        // create un-analyzed index
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text)");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'org.apache.cassandra.index.sai.StorageAttachedIndex'");
        execute("INSERT INTO %s (k, v) VALUES (1, 'apple')");

        // BM25 should fail with only an equality index
        assertThatThrownBy(() -> execute("SELECT k FROM %s WHERE v : 'apple' ORDER BY v BM25 OF 'apple' LIMIT 3"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessage("BM25 ordering on column v requires an analyzed index");

        // create analyzed index
        analyzeIndex();
        // BM25 query should work now
        var result = execute("SELECT k FROM %s WHERE v : 'apple' ORDER BY v BM25 OF 'apple' LIMIT 3");
        assertRows(result, row(1));
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

        beforeAndAfterFlush(() ->
        {
            var result = execute("SELECT k FROM %s ORDER BY v BM25 OF 'crispy crust' LIMIT 5");
            assertRows(result,
                       row(4), // Highest frequency of both terms
                       row(2), // High frequency of 'crispy', one 'crust'
                       row(1)); // One mention of each term
                                // Rows 4 and 5 do not contain all terms
        });
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
