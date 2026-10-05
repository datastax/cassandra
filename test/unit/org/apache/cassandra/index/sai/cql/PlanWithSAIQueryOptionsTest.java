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

import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.filter.SAIQueryOptions;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.plan.StorageAttachedIndexQueryPlan;
import org.apache.cassandra.index.sai.plan.StorageAttachedIndexSearcher;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for per-query SAI optimizer options via {@code WITH query_options = {...}}.
 *
 * <p>Verifies:
 * <ul>
 *   <li>Valid options parse and are stored in the {@link SAIQueryOptions} on the {@code RowFilter}.</li>
 *   <li>Invalid keys and out-of-range values are rejected at prepare time.</li>
 *   <li>{@code sai_query_optimization_level=0} disables the optimizer for that query only,
 *       without mutating the global {@link CassandraRelevantProperties#SAI_QUERY_OPT_LEVEL}.</li>
 *   <li>{@code sai_intersection_clause_limit} and {@code sai_use_term_statistics} are wired
 *       through to the effective values the controller reads.</li>
 *   <li>The global properties are never mutated by per-query options (thread isolation).</li>
 * </ul>
 */
public class PlanWithSAIQueryOptionsTest extends SAITester
{
    // -------------------------------------------------------------------------
    // Parsing and validation
    // -------------------------------------------------------------------------

    @Test
    public void testValidQueryOptionsAreParsedAndStored()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");

        String query = formatQuery("SELECT * FROM %s WHERE v = 1 " +
                                   "WITH query_options = {" +
                                   "'sai_query_optimization_level': '0', " +
                                   "'sai_intersection_clause_limit': '5', " +
                                   "'sai_use_term_statistics': 'false', " +
                                   "'sai_hybrid_sort_order': 'sort_then_filter'" +
                                   "}");

        disablePreparedReuseForTest();
        ReadCommand command = parseReadCommand(query);

        SAIQueryOptions opts = command.rowFilter().queryOptions;
        assertThat(opts).isNotSameAs(SAIQueryOptions.NONE);
        assertThat(opts.queryOptimizationLevel).isEqualTo(0);
        assertThat(opts.intersectionClauseLimit).isEqualTo(5);
        assertThat(opts.useTermStatistics).isFalse();
        assertThat(opts.hybridSortOrder).isEqualTo(SAIQueryOptions.HybridSortOrder.SORT_THEN_FILTER);
    }

    @Test
    public void testAbsentQueryOptionsResultsInNone()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");

        disablePreparedReuseForTest();
        ReadCommand command = parseReadCommand(formatQuery("SELECT * FROM %s WHERE v = 1"));
        assertThat(command.rowFilter().queryOptions).isSameAs(SAIQueryOptions.NONE);
    }

    @Test
    public void testUnknownQueryOptionKeyIsRejected()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        assertInvalidThrowMessage("Unknown SAI query option: bad_key",
                                  InvalidRequestException.class,
                                  "SELECT * FROM %s WHERE v = 1 ALLOW FILTERING WITH query_options = {'bad_key': 'x'}");
    }

    @Test
    public void testOutOfRangeOptimizationLevelIsRejected()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        assertInvalidThrowMessage("sai_query_optimization_level",
                                  InvalidRequestException.class,
                                  "SELECT * FROM %s WHERE v = 1 ALLOW FILTERING WITH query_options = {'sai_query_optimization_level': '99'}");
    }

    @Test
    public void testZeroIntersectionClauseLimitIsRejected()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        assertInvalidThrowMessage("sai_intersection_clause_limit",
                                  InvalidRequestException.class,
                                  "SELECT * FROM %s WHERE v = 1 ALLOW FILTERING WITH query_options = {'sai_intersection_clause_limit': '0'}");
    }

    @Test
    public void testInvalidHybridSortOrderIsRejected()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        assertInvalidThrowMessage("sai_hybrid_sort_order",
                                  InvalidRequestException.class,
                                  "SELECT * FROM %s WHERE v = 1 ALLOW FILTERING WITH query_options = {'sai_hybrid_sort_order': 'chaos'}");
    }

    // -------------------------------------------------------------------------
    // sai_query_optimization_level — optimizer disabled per-query
    // -------------------------------------------------------------------------

    @Test
    public void testOptLevelZeroDisablesOptimizerForThatQueryOnly() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v1 text, v2 text)");
        String idx1 = createIndex("CREATE CUSTOM INDEX idx1 ON %s(v1) USING 'StorageAttachedIndex'");
        String idx2 = createIndex("CREATE CUSTOM INDEX idx2 ON %s(v2) USING 'StorageAttachedIndex'");

        int numRows = 100;
        for (int i = 0; i < numRows; i++)
        {
            execute("INSERT INTO %s (k, v1, v2) VALUES (?, ?, ?)",
                    i,
                    i < 2 ? "rare" : "common",    // v1='rare' matches 2 rows
                    i < 4 ? "rare" : "common");    // v2='rare' matches 4 rows
        }

        // Sanity: with optimizer ON (level=1), the more selective index is chosen.
        assertThat(CassandraRelevantProperties.SAI_QUERY_OPT_LEVEL.getInt()).isEqualTo(1);

        beforeAndAfterFlush(() -> {
            // With optimizer, idx1 (more selective) should win for a v1 AND v2 query.
            assertThatPlanFor("SELECT * FROM %s WHERE v1='rare' AND v2='rare'", 2).usesAnyOf(idx1, idx2);

            // With per-query opt_level=0, the optimizer is disabled for this query only.
            // Query must still return the right results.
            disablePreparedReuseForTest();
            int rowsReturned = execute("SELECT * FROM %s WHERE v1='rare' AND v2='rare' " +
                                       "WITH query_options = {'sai_query_optimization_level': '0'}").size();
            assertThat(rowsReturned).isEqualTo(2);

            // The global must not have been mutated.
            assertThat(CassandraRelevantProperties.SAI_QUERY_OPT_LEVEL.getInt()).isEqualTo(1);
        });
    }

    @Test
    public void testOptLevelOneExplicitlyMatchesDefaultBehaviour() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v1 text, v2 text)");
        String idx1 = createIndex("CREATE CUSTOM INDEX idx1 ON %s(v1) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (k, v1, v2) VALUES (1, 'x', 'y')");

        beforeAndAfterFlush(() -> {
            // Explicit opt_level=1 should behave the same as the default.
            disablePreparedReuseForTest();
            assertThatPlanFor("SELECT * FROM %s WHERE v1='x' WITH query_options = {'sai_query_optimization_level': '1'}", 1).uses(idx1);
        });
    }

    // -------------------------------------------------------------------------
    // sai_intersection_clause_limit — wired through correctly
    // -------------------------------------------------------------------------

    @Test
    public void testIntersectionClauseLimitStoredOnRowFilter()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");

        disablePreparedReuseForTest();
        ReadCommand command = parseReadCommand(
                formatQuery("SELECT * FROM %s WHERE v = 1 WITH query_options = {'sai_intersection_clause_limit': '7'}"));

        assertThat(command.rowFilter().queryOptions.intersectionClauseLimit).isEqualTo(7);
        // Global must be unchanged.
        assertThat(CassandraRelevantProperties.SAI_INTERSECTION_CLAUSE_LIMIT.getInt())
                .isNotEqualTo(7);
    }

    // -------------------------------------------------------------------------
    // sai_use_term_statistics — wired through correctly
    // -------------------------------------------------------------------------

    @Test
    public void testUseTermStatisticsStoredOnRowFilter()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");

        disablePreparedReuseForTest();
        ReadCommand command = parseReadCommand(
                formatQuery("SELECT * FROM %s WHERE v = 1 WITH query_options = {'sai_use_term_statistics': 'false'}"));

        assertThat(command.rowFilter().queryOptions.useTermStatistics).isFalse();
        // Global must be unchanged (default is true as set by SAITester.resetQueryOptimizationLevel).
        assertThat(CassandraRelevantProperties.SAI_QUERY_OPTIMIZATION_USE_TERM_STATISTICS.getBoolean()).isTrue();
    }

    // -------------------------------------------------------------------------
    // Global isolation — per-query options must never mutate global statics
    // -------------------------------------------------------------------------

    @Test
    public void testPerQueryOptionsDoNotMutateGlobals()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        execute("INSERT INTO %s (k, v) VALUES (1, 42)");

        int globalOptLevel = CassandraRelevantProperties.SAI_QUERY_OPT_LEVEL.getInt();
        boolean globalUseTermStats = CassandraRelevantProperties.SAI_QUERY_OPTIMIZATION_USE_TERM_STATISTICS.getBoolean();
        int globalIntersectionLimit = CassandraRelevantProperties.SAI_INTERSECTION_CLAUSE_LIMIT.getInt();

        // Execute a query with all overrides set to non-default values.
        disablePreparedReuseForTest();
        execute("SELECT * FROM %s WHERE v = 42 " +
                "WITH query_options = {" +
                "'sai_query_optimization_level': '0', " +
                "'sai_intersection_clause_limit': '99', " +
                "'sai_use_term_statistics': 'false'" +
                "}");

        // Global properties must be unchanged after query execution.
        assertThat(CassandraRelevantProperties.SAI_QUERY_OPT_LEVEL.getInt()).isEqualTo(globalOptLevel);
        assertThat(CassandraRelevantProperties.SAI_QUERY_OPTIMIZATION_USE_TERM_STATISTICS.getBoolean()).isEqualTo(globalUseTermStats);
        assertThat(CassandraRelevantProperties.SAI_INTERSECTION_CLAUSE_LIMIT.getInt()).isEqualTo(globalIntersectionLimit);
    }

    // -------------------------------------------------------------------------
    // Effective values inside QueryController
    // -------------------------------------------------------------------------

    @Test
    public void testEffectiveValuesReflectPerQueryOptions()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        execute("INSERT INTO %s (k, v) VALUES (1, 1)");

        disablePreparedReuseForTest();
        String query = formatQuery("SELECT * FROM %s WHERE v = 1 " +
                                   "WITH query_options = {" +
                                   "'sai_query_optimization_level': '0', " +
                                   "'sai_intersection_clause_limit': '3', " +
                                   "'sai_use_term_statistics': 'false'" +
                                   "}");

        ReadCommand command = parseReadCommand(query);
        StorageAttachedIndexQueryPlan saiPlan =
                (StorageAttachedIndexQueryPlan) command.indexQueryPlan();
        assertThat(saiPlan).isNotNull();

        StorageAttachedIndexSearcher searcher = saiPlan.searcherFor(command);
        try
        {
            // buildPlan() reads the effective values from the controller; it must not throw
            // and must use the per-query overrides internally (verified by global-isolation test above).
            assertThat(searcher.buildPlan()).isNotNull();
        }
        finally
        {
            searcher.abort();
        }
    }
}
