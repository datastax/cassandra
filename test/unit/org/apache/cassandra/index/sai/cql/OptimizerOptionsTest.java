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

import org.junit.Assume;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.cql3.statements.SelectOptions;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.filter.OptimizerOptions;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.plan.QueryController;
import org.apache.cassandra.index.sai.plan.StorageAttachedIndexQueryPlan;
import org.apache.cassandra.index.sai.plan.StorageAttachedIndexSearcher;
import org.apache.cassandra.inject.Injections;

import static org.apache.cassandra.inject.InvokePointBuilder.newInvokePoint;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for per-query SAI optimizer options via {@code WITH optimizer_options = {...}}.
 *
 * <p>This class covers parsing, validation, the interaction with index hints and the effect of
 * {@code query_optimization_level}, {@code intersection_clause_limit} and {@code use_term_statistics}.
 * Tests that check how {@code hybrid_sort_order} changes the plan of hybrid queries live in
 * {@link PerQueryHybridVectorOptimizerOptionsTest}.
 *
 * <p>Verifies:
 * <ul>
 *   <li>Valid options parse and are stored in the {@link OptimizerOptions} on the {@code RowFilter}.</li>
 *   <li>Invalid keys and out-of-range values are rejected at prepare time.</li>
 *   <li>{@code query_optimization_level=0} disables the optimizer for that query only,
 *       without mutating the global {@link CassandraRelevantProperties#SAI_QUERY_OPT_LEVEL}.</li>
 *   <li>{@code intersection_clause_limit} caps the number of indexes in the plan.</li>
 *   <li>{@code use_term_statistics} selects between histogram-based and search-based selectivity estimation.</li>
 *   <li>The global properties are never mutated by per-query options.</li>
 * </ul>
 */
public class OptimizerOptionsTest extends SAITester
{
    /**
     * Counts how many times the selectivity of a predicate has been estimated by running the index search,
     * which is what happens when term statistics are not used.
     */
    private static final Injections.Counter SEARCH_BASED_ESTIMATE_COUNTER =
            Injections.newCounter("SearchBasedEstimateCounter")
                      .add(newInvokePoint().onClass(QueryController.class)
                                           .onMethod("estimateMatchingRowCountUsingIndex"))
                      .build();

    // -------------------------------------------------------------------------
    // Parsing and validation
    // -------------------------------------------------------------------------

    @Test
    public void testValidOptimizerOptionsAreParsedAndStored()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");

        String query = formatQuery("SELECT * FROM %s WHERE v = 1 " +
                                   "WITH optimizer_options = {" +
                                   "'query_optimization_level': '0', " +
                                   "'intersection_clause_limit': '5', " +
                                   "'use_term_statistics': 'false', " +
                                   "'hybrid_sort_order': 'sort_then_filter'" +
                                   "}");

        ReadCommand command = parseReadCommand(query);

        OptimizerOptions opts = command.rowFilter().optimizerOptions;
        assertThat(opts).isNotSameAs(OptimizerOptions.NONE);
        assertThat(opts.queryOptimizationLevel()).isEqualTo(0);
        assertThat(opts.intersectionClauseLimit()).isEqualTo(5);
        assertThat(opts.useTermStatistics()).isFalse();
        assertThat(opts.hybridSortOrder()).isEqualTo(OptimizerOptions.HybridSortOrder.SORT_THEN_FILTER);
    }

    @Test
    public void testAbsentOptimizerOptionsResultsInNone()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");

        ReadCommand command = parseReadCommand(formatQuery("SELECT * FROM %s WHERE v = 1"));
        assertThat(command.rowFilter().optimizerOptions).isSameAs(OptimizerOptions.NONE);
    }

    @Test
    public void testUnknownOptimizerOptionKeyIsRejected()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        assertInvalidThrowMessage("Unknown SAI optimizer option: bad_key",
                                  InvalidRequestException.class,
                                  "SELECT * FROM %s WHERE v = 1 ALLOW FILTERING WITH optimizer_options = {'bad_key': 'x'}");
    }

    @Test
    public void testOutOfRangeOptimizationLevelIsRejected()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        assertInvalidThrowMessage("query_optimization_level",
                                  InvalidRequestException.class,
                                  "SELECT * FROM %s WHERE v = 1 ALLOW FILTERING WITH optimizer_options = {'query_optimization_level': '99'}");
    }

    @Test
    public void testZeroIntersectionClauseLimitIsRejected()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        assertInvalidThrowMessage("intersection_clause_limit",
                                  InvalidRequestException.class,
                                  "SELECT * FROM %s WHERE v = 1 ALLOW FILTERING WITH optimizer_options = {'intersection_clause_limit': '0'}");
    }

    @Test
    public void testInvalidHybridSortOrderIsRejected()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        assertInvalidThrowMessage("hybrid_sort_order",
                                  InvalidRequestException.class,
                                  "SELECT * FROM %s WHERE v = 1 ALLOW FILTERING WITH optimizer_options = {'hybrid_sort_order': 'chaos'}");
    }

    // -------------------------------------------------------------------------
    // query_optimization_level — optimizer disabled per-query
    // -------------------------------------------------------------------------

    /**
     * Verify that {@code query_optimization_level=0} really disables the optimizer for that query only, and that an
     * explicit {@code query_optimization_level=1} behaves like the default.
     *
     * <p>{@code v2 = 'common'} matches almost every row, so with the optimizer enabled intersecting it with the very
     * selective {@code v1 = 'rare'} is not worth its cost, and the optimizer drops it from the plan. With the optimizer
     * disabled there is no cost-based pruning, so both clauses are kept, as they fit in the intersection clause limit.
     */
    @Test
    public void testOptLevelZeroDisablesOptimizerForThatQueryOnly() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v1 text, v2 text)");
        String idx1 = createIndex("CREATE CUSTOM INDEX idx1 ON %s(v1) USING 'StorageAttachedIndex'");
        String idx2 = createIndex("CREATE CUSTOM INDEX idx2 ON %s(v2) USING 'StorageAttachedIndex'");

        for (int i = 0; i < 1000; i++)
        {
            execute("INSERT INTO %s (k, v1, v2) VALUES (?, ?, ?)",
                    i,
                    i == 0 || i == 1 ? "rare" : "common",
                    i == 0 || i == 2 ? "rare" : "common");
        }

        String query = "SELECT * FROM %s WHERE v1 = 'rare' AND v2 = 'common'";
        String optimizerOff = " WITH optimizer_options = {'query_optimization_level': '0', 'intersection_clause_limit': '2'}";

        assertThat(CassandraRelevantProperties.SAI_QUERY_OPT_LEVEL.getInt()).isEqualTo(1);

        beforeAndAfterFlush(() -> {
            // Optimizer on: the non-selective clause is dropped.
            assertThatPlanFor(query, 1).uses(idx1);

            // Explicit level 1 behaves like the default.
            assertThatPlanFor(query + " WITH optimizer_options = {'query_optimization_level': '1', " +
                              "'intersection_clause_limit': '2'}", 1).uses(idx1);

            // Optimizer off for this query only: both clauses are kept.
            assertThatPlanFor(query + optimizerOff, 1).uses(idx1, idx2);

            // The next query without options is optimized again, and the global is untouched.
            assertThatPlanFor(query, 1).uses(idx1);
            assertThat(CassandraRelevantProperties.SAI_QUERY_OPT_LEVEL.getInt()).isEqualTo(1);
        });
    }

    // -------------------------------------------------------------------------
    // intersection_clause_limit — limits the number of intersected clauses
    // -------------------------------------------------------------------------

    /**
     * Verify that {@code intersection_clause_limit} caps the number of indexes used by the plan.
     *
     * <p>The optimizer is disabled so that the number of indexes in the plan is determined by the limit alone.
     */
    @Test
    public void testIntersectionClauseLimitCapsIndexesInPlan() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v1 int, v2 int, v3 int)");
        createIndex("CREATE CUSTOM INDEX idx1 ON %s(v1) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX idx2 ON %s(v2) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX idx3 ON %s(v3) USING 'StorageAttachedIndex'");

        for (int i = 0; i < 10; i++)
            execute("INSERT INTO %s (k, v1, v2, v3) VALUES (?, 0, 0, 0)", i);

        String query = "SELECT * FROM %s WHERE v1 = 0 AND v2 = 0 AND v3 = 0";
        int globalLimit = CassandraRelevantProperties.SAI_INTERSECTION_CLAUSE_LIMIT.getInt();

        beforeAndAfterFlush(() -> {
            for (int limit = 1; limit <= 3; limit++)
            {
                assertThatPlanFor(query + " WITH optimizer_options = {'query_optimization_level': '0', " +
                                  "'intersection_clause_limit': '" + limit + "'}", 10).uses(limit);
            }

            assertThat(CassandraRelevantProperties.SAI_INTERSECTION_CLAUSE_LIMIT.getInt()).isEqualTo(globalLimit);
        });
    }

    // -------------------------------------------------------------------------
    // use_term_statistics — selects the selectivity estimator
    // -------------------------------------------------------------------------

    /**
     * Verify that {@code use_term_statistics} selects the estimator used while building the plan.
     *
     * <p>With term statistics the row count is estimated from in-memory histograms, so no index search is done.
     * Without term statistics the optimizer runs the index search to count the matching rows.
     */
    @Test
    public void testUseTermStatisticsControlsHowSelectivityIsEstimated() throws Throwable
    {
        Assume.assumeTrue("Term histograms require index format EB or later",
                          Version.current(KEYSPACE).onOrAfter(Version.EB));

        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");

        for (int i = 0; i < 100; i++)
            execute("INSERT INTO %s (k, v) VALUES (?, ?)", i, i % 10);

        Injections.inject(SEARCH_BASED_ESTIMATE_COUNTER);
        boolean globalUseTermStats = CassandraRelevantProperties.SAI_QUERY_OPTIMIZATION_USE_TERM_STATISTICS.getBoolean();
        assertThat(globalUseTermStats).isTrue();

        beforeAndAfterFlush(() -> {
            assertThat(searchBasedEstimates("SELECT * FROM %s WHERE v = 1"))
                    .as("default (term statistics) must not run an index search to estimate")
                    .isZero();

            assertThat(searchBasedEstimates("SELECT * FROM %s WHERE v = 1 " +
                                            "WITH optimizer_options = {'use_term_statistics': 'true'}"))
                    .as("explicit use_term_statistics=true must not run an index search to estimate")
                    .isZero();

            assertThat(searchBasedEstimates("SELECT * FROM %s WHERE v = 1 " +
                                            "WITH optimizer_options = {'use_term_statistics': 'false'}"))
                    .as("use_term_statistics=false must run an index search to estimate")
                    .isPositive();

            assertThat(CassandraRelevantProperties.SAI_QUERY_OPTIMIZATION_USE_TERM_STATISTICS.getBoolean())
                    .isEqualTo(globalUseTermStats);
        });
    }

    /**
     * Builds the query plan for the specified query, without executing it, and returns how many times the
     * selectivity of a predicate was estimated by running the index search.
     */
    private long searchBasedEstimates(String query)
    {
        SEARCH_BASED_ESTIMATE_COUNTER.reset();

        ReadCommand command = parseReadCommand(formatQuery(query));
        StorageAttachedIndexQueryPlan queryPlan = (StorageAttachedIndexQueryPlan) command.indexQueryPlan();
        assertThat(queryPlan).isNotNull();

        StorageAttachedIndexSearcher searcher = queryPlan.searcherFor(command);
        try
        {
            assertThat(searcher.buildPlan()).isNotNull();
        }
        finally
        {
            searcher.abort();
        }
        return SEARCH_BASED_ESTIMATE_COUNTER.get();
    }

    // -------------------------------------------------------------------------
    // Global isolation — per-query options must never mutate global statics
    // -------------------------------------------------------------------------

    /**
     * Verify that per-query optimizer options do not mutate global system properties.
     */
    @Test
    public void testPerQueryOptimizerOptionsDoNotMutateGlobals()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        execute("INSERT INTO %s (k, v) VALUES (1, 42)");

        int globalOptLevel = CassandraRelevantProperties.SAI_QUERY_OPT_LEVEL.getInt();
        boolean globalUseTermStats = CassandraRelevantProperties.SAI_QUERY_OPTIMIZATION_USE_TERM_STATISTICS.getBoolean();
        int globalIntersectionLimit = CassandraRelevantProperties.SAI_INTERSECTION_CLAUSE_LIMIT.getInt();

        // Execute a query with all overrides set to non-default values.
        execute("SELECT * FROM %s WHERE v = 42 " +
                "WITH optimizer_options = {" +
                "'query_optimization_level': '0', " +
                "'intersection_clause_limit': '99', " +
                "'use_term_statistics': 'false'" +
                "}");

        // Global properties must be unchanged after query execution.
        assertThat(CassandraRelevantProperties.SAI_QUERY_OPT_LEVEL.getInt()).isEqualTo(globalOptLevel);
        assertThat(CassandraRelevantProperties.SAI_QUERY_OPTIMIZATION_USE_TERM_STATISTICS.getBoolean()).isEqualTo(globalUseTermStats);
        assertThat(CassandraRelevantProperties.SAI_INTERSECTION_CLAUSE_LIMIT.getInt()).isEqualTo(globalIntersectionLimit);
    }

    // -------------------------------------------------------------------------
    // included_indexes + hybrid_sort_order contradiction
    // -------------------------------------------------------------------------

    /**
     * Verify that combining {@code included_indexes} with a non-{@code auto} {@code hybrid_sort_order}
     * is rejected at prepare time, because the hint forces a specific sort index while the option
     * overrides the sort order.
     */
    @Test
    public void testIncludedIndexesContradictsHybridSortOrderIsRejected()
    {
        createTable("CREATE TABLE %s (k int, c int, s text, n int, PRIMARY KEY(k, c))");
        String idxS = createIndex("CREATE CUSTOM INDEX ON %s(s) USING 'StorageAttachedIndex'");
        String idxN = createIndex("CREATE CUSTOM INDEX ON %s(n) USING 'StorageAttachedIndex'");

        for (int i = 0; i < 10; i++)
            execute("INSERT INTO %s (k, c, s, n) VALUES (0, ?, ?, ?)",
                    i, String.valueOf((char) ('a' + i)), i < 2 ? 0 : 1);

        // filter_then_sort + included_indexes is contradictory: rejected.
        assertInvalidThrowMessage(SelectOptions.CONFLICTING_OPTIMIZER_OPTIONS_AND_INDEX_HINTS_ERROR,
                                  InvalidRequestException.class,
                                  "SELECT c FROM %s WHERE n >= 0 ORDER BY s ASC LIMIT 5 " +
                                  "WITH optimizer_options = {'hybrid_sort_order': 'filter_then_sort'} " +
                                  "AND included_indexes = { " + idxS + " }");

        // sort_then_filter + included_indexes is also contradictory: rejected.
        assertInvalidThrowMessage(SelectOptions.CONFLICTING_OPTIMIZER_OPTIONS_AND_INDEX_HINTS_ERROR,
                                  InvalidRequestException.class,
                                  "SELECT c FROM %s WHERE n = 0 ORDER BY s ASC LIMIT 5 " +
                                  "WITH optimizer_options = {'hybrid_sort_order': 'sort_then_filter'} " +
                                  "AND included_indexes = { " + idxS + " }");

        // auto + included_indexes is fine: no contradiction.
        execute("SELECT c FROM %s WHERE n >= 0 ORDER BY s ASC LIMIT 5 " +
                "WITH optimizer_options = {'hybrid_sort_order': 'auto'} " +
                "AND included_indexes = { " + idxS + " }");

        // Without ORDER BY the query is not hybrid, so the hint does not contradict the sort order.
        execute("SELECT c FROM %s WHERE n >= 0 " +
                "WITH optimizer_options = {'hybrid_sort_order': 'filter_then_sort'} " +
                "AND included_indexes = { " + idxN + " }");
        execute("SELECT c FROM %s WHERE n >= 0 " +
                "WITH optimizer_options = {'hybrid_sort_order': 'sort_then_filter'} " +
                "AND included_indexes = { " + idxN + " }");

        // Without WHERE the query is not hybrid, so the hint does not contradict the sort order.
        execute("SELECT c FROM %s ORDER BY s ASC LIMIT 5 " +
                "WITH optimizer_options = {'hybrid_sort_order': 'filter_then_sort'} " +
                "AND included_indexes = { " + idxS + " }");
        execute("SELECT c FROM %s ORDER BY s ASC LIMIT 5 " +
                "WITH optimizer_options = {'hybrid_sort_order': 'sort_then_filter'} " +
                "AND included_indexes = { " + idxS + " }");
    }

    /**
     * Verify that {@code hybrid_sort_order} can be combined with {@code excluded_indexes}.
     */
    @Test
    public void testExcludedIndexesWithHybridSortOrder()
    {
        createTable("CREATE TABLE %s (k int, c int, s text, n int, PRIMARY KEY(k, c))");
        String idxS = createIndex("CREATE CUSTOM INDEX ON %s(s) USING 'StorageAttachedIndex'");
        String idxN = createIndex("CREATE CUSTOM INDEX ON %s(n) USING 'StorageAttachedIndex'");

        for (int i = 0; i < 10; i++)
        {
            execute("INSERT INTO %s (k, c, s, n) VALUES (0, ?, ?, ?)",
                    i, String.valueOf((char) ('a' + i)), i < 2 ? 0 : 1);
        }

        // Filter only: the excluded index forces filtering, and the option is a no-op.
        for (OptimizerOptions.HybridSortOrder order : OptimizerOptions.HybridSortOrder.values())
        {
            execute("SELECT c FROM %s WHERE n >= 0 ALLOW FILTERING " +
                    "WITH optimizer_options = {'hybrid_sort_order': '" + order + "'} " +
                    "AND excluded_indexes = { " + idxN + " }");
        }

        // Order only: excluding the only usable index makes the query invalid regardless of the option.
        for (OptimizerOptions.HybridSortOrder order : OptimizerOptions.HybridSortOrder.values())
        {
            assertInvalidThrowMessage("requires the column to be indexed with a non-analyzed index",
                                      InvalidRequestException.class,
                                      "SELECT c FROM %s ORDER BY s ASC LIMIT 5 " +
                                      "WITH optimizer_options = {'hybrid_sort_order': '" + order + "'} " +
                                      "AND excluded_indexes = { " + idxS + " }");
        }
    }
}
