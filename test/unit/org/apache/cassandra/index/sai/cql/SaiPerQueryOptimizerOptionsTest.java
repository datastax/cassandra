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

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.index.sai.plan.Plan;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end integration tests for per-query SAI optimizer options ({@code WITH query_options = {...}}).
 *
 * <p>Covers all four option keys across real query execution:
 * <ul>
 *   <li>{@code sai_query_optimization_level} — disables/enables the optimizer for a single query.</li>
 *   <li>{@code sai_intersection_clause_limit} — limits the number of intersected index clauses.</li>
 *   <li>{@code sai_use_term_statistics} — controls which selectivity estimator is used.</li>
 *   <li>{@code sai_hybrid_sort_order} — overrides the optimizer's filter-then-sort / sort-then-filter
 *       plan decision for hybrid ANN queries.</li>
 * </ul>
 *
 * <p>The hybrid sort order tests use {@link Plan.NumericIndexScan} as a proxy for filter-then-sort
 * (keys are materialised from the WHERE-clause index before scoring) and {@link Plan.AnnIndexScan}
 * as a proxy for sort-then-filter (candidates stream from the ANN index, post-filtered by WHERE).
 */
public class SaiPerQueryOptimizerOptionsTest extends VectorTester
{
    // -----------------------------------------------------------------------
    // Table shared across hybrid sort-order tests:
    //   - n=0  → 2 rows  (selective WHERE)
    //   - n>=0 → 20 rows (non-selective WHERE)
    // -----------------------------------------------------------------------

    @Before
    public void createHybridTable()
    {
        createTable("CREATE TABLE %s (k int, c int, v vector<float, 2>, n int, PRIMARY KEY(k, c))");
        createIndex("CREATE CUSTOM INDEX vec_idx ON %s(v) USING 'StorageAttachedIndex' " +
                    "WITH OPTIONS = {'similarity_function': 'euclidean'}");
        createIndex("CREATE CUSTOM INDEX num_idx ON %s(n) USING 'StorageAttachedIndex'");

        for (int i = 0; i < 20; i++)
            execute("INSERT INTO %s (k, c, v, n) VALUES (0, ?, ?, ?)",
                    i,
                    vector(0f, (float) i),
                    i < 2 ? 0 : 1); // n=0 for rows 0,1 only
    }

    // -----------------------------------------------------------------------
    // sai_hybrid_sort_order = sort_then_filter
    // -----------------------------------------------------------------------

    /**
     * When the WHERE predicate is very selective (n=0 matches 2/20 rows), the optimizer normally
     * chooses filter-then-sort: it materialises keys from the numeric index, then scores them with ANN.
     * {@code sort_then_filter} must override this and produce an {@link Plan.AnnIndexScan} plan instead,
     * while still returning the correct rows.
     */
    @Test
    public void testSortThenFilterOverridesSelectiveWhereClause()
    {
        // Sanity: without override, the optimizer picks filter-then-sort for n=0 (selective).
        assertQueryHasSubplan("SELECT c FROM %s WHERE n = 0 ORDER BY v ANN OF [0, 0] LIMIT 5",
                              Plan.NumericIndexScan.class,
                              row(0), row(1));

        // With sort_then_filter override, the plan must use AnnIndexScan (sort-then-filter),
        // and the query must still return the correct rows.
        disablePreparedReuseForTest();
        assertQueryHasSubplan(
                "SELECT c FROM %s WHERE n = 0 ORDER BY v ANN OF [0, 0] LIMIT 5 " +
                "WITH query_options = {'sai_hybrid_sort_order': 'sort_then_filter'}",
                Plan.AnnIndexScan.class,
                row(0), row(1));
    }

    /**
     * When the plan is already sort-then-filter (non-selective WHERE), the {@code sort_then_filter}
     * override is a no-op: the plan must remain an {@link Plan.AnnIndexScan}.
     */
    @Test
    public void testSortThenFilterIsNoOpWhenAlreadySortThenFilter()
    {
        // n>=0 is non-selective: optimizer chooses sort-then-filter by default.
        assertQueryHasSubplan("SELECT c FROM %s WHERE n >= 0 ORDER BY v ANN OF [0, 0] LIMIT 5",
                              Plan.AnnIndexScan.class,
                              row(0), row(1), row(2), row(3), row(4));

        // Override should be a no-op.
        disablePreparedReuseForTest();
        assertQueryHasSubplan(
                "SELECT c FROM %s WHERE n >= 0 ORDER BY v ANN OF [0, 0] LIMIT 5 " +
                "WITH query_options = {'sai_hybrid_sort_order': 'sort_then_filter'}",
                Plan.AnnIndexScan.class,
                row(0), row(1), row(2), row(3), row(4));
    }

    // -----------------------------------------------------------------------
    // sai_hybrid_sort_order = filter_then_sort
    // -----------------------------------------------------------------------

    /**
     * When the WHERE predicate is non-selective (n>=0 matches all 20 rows), the optimizer normally
     * chooses sort-then-filter: it streams from the ANN index.
     * {@code filter_then_sort} must override this and produce a {@link Plan.NumericIndexScan} plan
     * (filter-then-sort), while still returning correct results.
     */
    @Test
    public void testFilterThenSortOverridesNonSelectiveWhereClause()
    {
        // Sanity: without override, the optimizer picks sort-then-filter for n>=0 (non-selective).
        assertQueryHasSubplan("SELECT c FROM %s WHERE n >= 0 ORDER BY v ANN OF [0, 0] LIMIT 5",
                              Plan.AnnIndexScan.class,
                              row(0), row(1), row(2), row(3), row(4));

        // With filter_then_sort override, the plan must use NumericIndexScan (filter-then-sort),
        // and the query must still return the correct rows.
        disablePreparedReuseForTest();
        assertQueryHasSubplan(
                "SELECT c FROM %s WHERE n >= 0 ORDER BY v ANN OF [0, 0] LIMIT 5 " +
                "WITH query_options = {'sai_hybrid_sort_order': 'filter_then_sort'}",
                Plan.NumericIndexScan.class,
                row(0), row(1), row(2), row(3), row(4));
    }

    /**
     * When the plan is already filter-then-sort (selective WHERE), the {@code filter_then_sort}
     * override is a no-op: the plan must remain a {@link Plan.NumericIndexScan}.
     */
    @Test
    public void testFilterThenSortIsNoOpWhenAlreadyFilterThenSort()
    {
        // n=0 is selective: optimizer already chooses filter-then-sort.
        assertQueryHasSubplan("SELECT c FROM %s WHERE n = 0 ORDER BY v ANN OF [0, 0] LIMIT 5",
                              Plan.NumericIndexScan.class,
                              row(0), row(1));

        // Override should be a no-op.
        disablePreparedReuseForTest();
        assertQueryHasSubplan(
                "SELECT c FROM %s WHERE n = 0 ORDER BY v ANN OF [0, 0] LIMIT 5 " +
                "WITH query_options = {'sai_hybrid_sort_order': 'filter_then_sort'}",
                Plan.NumericIndexScan.class,
                row(0), row(1));
    }

    // -----------------------------------------------------------------------
    // sai_hybrid_sort_order = auto
    // -----------------------------------------------------------------------

    @Test
    public void testAutoPreservesOptimizerDecision()
    {
        // auto must not change anything — same plans as without the option.
        disablePreparedReuseForTest();
        assertQueryHasSubplan(
                "SELECT c FROM %s WHERE n = 0 ORDER BY v ANN OF [0, 0] LIMIT 5 " +
                "WITH query_options = {'sai_hybrid_sort_order': 'auto'}",
                Plan.NumericIndexScan.class,
                row(0), row(1));

        disablePreparedReuseForTest();
        assertQueryHasSubplan(
                "SELECT c FROM %s WHERE n >= 0 ORDER BY v ANN OF [0, 0] LIMIT 5 " +
                "WITH query_options = {'sai_hybrid_sort_order': 'auto'}",
                Plan.AnnIndexScan.class,
                row(0), row(1), row(2), row(3), row(4));
    }

    // -----------------------------------------------------------------------
    // sai_query_optimization_level — end-to-end result correctness
    // -----------------------------------------------------------------------

    @Test
    public void testOptLevelZeroQueryReturnsCorrectResults()
    {
        // opt_level=0 must disable the optimizer but still return correct results.
        disablePreparedReuseForTest();
        var results = execute("SELECT c FROM %s WHERE n = 0 ORDER BY v ANN OF [0, 0] LIMIT 5 " +
                              "WITH query_options = {'sai_query_optimization_level': '0'}");
        assertThat(results.size()).isEqualTo(2);
    }

    // -----------------------------------------------------------------------
    // sai_intersection_clause_limit — end-to-end result correctness
    // -----------------------------------------------------------------------

    @Test
    public void testIntersectionClauseLimitQueryReturnsCorrectResults()
    {
        disablePreparedReuseForTest();
        // A limit of 1 restricts to a single indexed clause; the query must still return results.
        var results = execute("SELECT c FROM %s WHERE n = 0 ORDER BY v ANN OF [0, 0] LIMIT 5 " +
                              "WITH query_options = {'sai_intersection_clause_limit': '1'}");
        assertThat(results.size()).isEqualTo(2);
    }

    // -----------------------------------------------------------------------
    // sai_use_term_statistics — end-to-end result correctness
    // -----------------------------------------------------------------------

    @Test
    public void testUseTermStatisticsFalseQueryReturnsCorrectResults()
    {
        disablePreparedReuseForTest();
        var results = execute("SELECT c FROM %s WHERE n = 0 ORDER BY v ANN OF [0, 0] LIMIT 5 " +
                              "WITH query_options = {'sai_use_term_statistics': 'false'}");
        assertThat(results.size()).isEqualTo(2);
    }

    // -----------------------------------------------------------------------
    // Combined options — all four at once
    // -----------------------------------------------------------------------

    @Test
    public void testAllOptionsTogetherReturnCorrectResults()
    {
        disablePreparedReuseForTest();
        var results = execute("SELECT c FROM %s WHERE n = 0 ORDER BY v ANN OF [0, 0] LIMIT 5 " +
                              "WITH query_options = {" +
                              "'sai_query_optimization_level': '1', " +
                              "'sai_intersection_clause_limit': '3', " +
                              "'sai_use_term_statistics': 'true', " +
                              "'sai_hybrid_sort_order': 'sort_then_filter'" +
                              "}");
        assertThat(results.size()).isEqualTo(2);
    }
}
