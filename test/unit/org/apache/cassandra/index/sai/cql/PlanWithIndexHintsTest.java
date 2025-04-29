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
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.plan.Plan;
import org.apache.cassandra.index.sai.plan.StorageAttachedIndexQueryPlan;
import org.apache.cassandra.index.sai.plan.StorageAttachedIndexSearcher;
import org.assertj.core.api.Assertions;

/**
 * Tests the effects of {@link org.apache.cassandra.db.filter.IndexHints} in SAI's internal query planning:
 * <ul>
 *    <li>Preferred indexes shouldn't be pruned in the optimized the query {@link Plan}.</li>
 *    <li>Excluded indexes shouldn't be included in the query {@link Plan}.</li>
 * </ul>
 */
@RunWith(Parameterized.class)
public class PlanWithIndexHintsTest extends SAITester
{
    private static final int NUM_ROWS = 1000;

    @Parameterized.Parameter
    public boolean flush;

    @Parameterized.Parameters(name = "{index}: flush={0}")
    public static Collection<Object[]> parameters()
    {
        List<Object[]> result = new ArrayList<>();
        for (boolean flush : new boolean[]{ false, true })
            result.add(new Object[]{ flush });
        return result;
    }

    @Test
    public void testQueryPlanning()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v1 text, v2 text)");
        createIndex("CREATE CUSTOM INDEX idx1 ON %s(v1) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX idx2 ON %s(v2) USING 'StorageAttachedIndex'");

        // Insert some rows, with some rare values and some common values, so the query planner will tend to prefer
        // the index with the most selective predicate.
        for (int i = 0; i < NUM_ROWS; i++)
        {
            execute("INSERT INTO %s (k, v1, v2) VALUES (?, ?, ?)",
                    i,
                    i == 0 || i == 1 ? "rare" : "common",
                    i == 0 || i == 2 ? "rare" : "common");
        }

        if (flush)
            flush();

        // test some queries without any hints, so selection is based on selectivity only
        assertThatPlan("SELECT * FROM %s WHERE v1='rare' AND v2='rare'", 1).selectsAnyOf("idx1", "idx2");
        assertThatPlan("SELECT * FROM %s WHERE v1='rare' AND v2='common'", 1).selects("idx1");
        assertThatPlan("SELECT * FROM %s WHERE v1='common' AND v2='rare'", 1).selects("idx2");
        assertThatPlan("SELECT * FROM %s WHERE v1='common' AND v2='common'", NUM_ROWS - 3).selectsAnyOf("idx1", "idx2");
        assertThatPlan("SELECT * FROM %s WHERE v1='rare' OR v2='rare'", 3).selects("idx1", "idx2");
        assertThatPlan("SELECT * FROM %s WHERE v1='rare' OR v2='common'", NUM_ROWS - 1).selects("idx1", "idx2");
        assertThatPlan("SELECT * FROM %s WHERE v1='common' OR v2='rare'", NUM_ROWS - 1).selects("idx1", "idx2");
        assertThatPlan("SELECT * FROM %s WHERE v1='common' OR v2='common'", NUM_ROWS - 1).selects("idx1", "idx2");
        assertThatPlan("SELECT * FROM %s ORDER BY v1 LIMIT 10", 10).selects("idx1");
        assertThatPlan("SELECT * FROM %s ORDER BY v2 LIMIT 10", 10).selects("idx2");

        // run the same queries as before, but with hints preferring idx1
        assertThatPlan("SELECT * FROM %s WHERE v1='rare' AND v2='rare' WITH preferred_indexes = {idx1}", 1).selectsAtLeast("idx1");
        assertThatPlan("SELECT * FROM %s WHERE v1='rare' AND v2='common' WITH preferred_indexes = {idx1}", 1).selectsAtLeast("idx1");
        assertThatPlan("SELECT * FROM %s WHERE v1='common' AND v2='rare' WITH preferred_indexes = {idx1}", 1).selectsAtLeast("idx1");
        assertThatPlan("SELECT * FROM %s WHERE v1='common' AND v2='common' WITH preferred_indexes = {idx1}", NUM_ROWS - 3).selectsAtLeast("idx1");
        assertThatPlan("SELECT * FROM %s WHERE v1='rare' OR v2='rare' WITH preferred_indexes = {idx1}", 3).selects("idx1", "idx2");
        assertThatPlan("SELECT * FROM %s WHERE v1='rare' OR v2='common' WITH preferred_indexes = {idx1}", NUM_ROWS - 1).selects("idx1", "idx2");
        assertThatPlan("SELECT * FROM %s WHERE v1='common' OR v2='rare' WITH preferred_indexes = {idx1}", NUM_ROWS - 1).selects("idx1", "idx2");
        assertThatPlan("SELECT * FROM %s WHERE v1='common' OR v2='common' WITH preferred_indexes = {idx1}", NUM_ROWS - 1).selects("idx1", "idx2");
        assertThatPlan("SELECT * FROM %s ORDER BY v1 LIMIT 10 WITH preferred_indexes = {idx1}", 10).selects("idx1");
        assertThatPlan("SELECT * FROM %s ORDER BY v2 LIMIT 10 WITH preferred_indexes = {idx1}", 10).selects("idx2");

        // preferring idx2
        assertThatPlan("SELECT * FROM %s WHERE v1='rare' AND v2='rare' WITH preferred_indexes = {idx2}", 1).selectsAtLeast("idx2");
        assertThatPlan("SELECT * FROM %s WHERE v1='rare' AND v2='common' WITH preferred_indexes = {idx2}", 1).selectsAtLeast("idx2");
        assertThatPlan("SELECT * FROM %s WHERE v1='common' AND v2='rare' WITH preferred_indexes = {idx2}", 1).selectsAtLeast("idx2");
        assertThatPlan("SELECT * FROM %s WHERE v1='common' AND v2='common' WITH preferred_indexes = {idx2}", NUM_ROWS - 3).selectsAtLeast("idx2");
        assertThatPlan("SELECT * FROM %s WHERE v1='rare' OR v2='rare' WITH preferred_indexes = {idx2}", 3).selects("idx1", "idx2");
        assertThatPlan("SELECT * FROM %s WHERE v1='rare' OR v2='common' WITH preferred_indexes = {idx2}", NUM_ROWS - 1).selects("idx1", "idx2");
        assertThatPlan("SELECT * FROM %s WHERE v1='common' OR v2='rare' WITH preferred_indexes = {idx2}", NUM_ROWS - 1).selects("idx1", "idx2");
        assertThatPlan("SELECT * FROM %s WHERE v1='common' OR v2='common' WITH preferred_indexes = {idx2}", NUM_ROWS - 1).selects("idx1", "idx2");
        assertThatPlan("SELECT * FROM %s ORDER BY v1 LIMIT 10 WITH preferred_indexes = {idx2}", 10).selects("idx1");
        assertThatPlan("SELECT * FROM %s ORDER BY v2 LIMIT 10 WITH preferred_indexes = {idx2}", 10).selects("idx2");

        // preferring both idx1 and idx2
        assertThatPlan("SELECT * FROM %s WHERE v1='rare' AND v2='rare' WITH preferred_indexes = {idx1, idx2}", 1).selects("idx1", "idx2");
        assertThatPlan("SELECT * FROM %s WHERE v1='rare' AND v2='common' WITH preferred_indexes = {idx1, idx2}", 1).selects("idx1", "idx2");
        assertThatPlan("SELECT * FROM %s WHERE v1='common' AND v2='rare' WITH preferred_indexes = {idx1, idx2}", 1).selects("idx1", "idx2");
        assertThatPlan("SELECT * FROM %s WHERE v1='common' AND v2='common'  WITH preferred_indexes = {idx1, idx2}", NUM_ROWS - 3).selects("idx1", "idx2");
        assertThatPlan("SELECT * FROM %s WHERE v1='rare' OR v2='rare' WITH preferred_indexes = {idx1, idx2}", 3).selects("idx1", "idx2");
        assertThatPlan("SELECT * FROM %s WHERE v1='rare' OR v2='common' WITH preferred_indexes = {idx1, idx2}", NUM_ROWS - 1).selects("idx1", "idx2");
        assertThatPlan("SELECT * FROM %s WHERE v1='common' OR v2='rare' WITH preferred_indexes = {idx1, idx2}", NUM_ROWS - 1).selects("idx1", "idx2");
        assertThatPlan("SELECT * FROM %s WHERE v1='common' OR v2='common' WITH preferred_indexes = {idx1, idx2}", NUM_ROWS - 1).selects("idx1", "idx2");
        assertThatPlan("SELECT * FROM %s ORDER BY v1 LIMIT 10 WITH preferred_indexes = {idx1, idx2}", 10).selects("idx1");
        assertThatPlan("SELECT * FROM %s ORDER BY v2 LIMIT 10 WITH preferred_indexes = {idx1, idx2}", 10).selects("idx2");

        // excluding idx1
        assertThatPlan("SELECT * FROM %s WHERE v1='rare' AND v2='rare' ALLOW FILTERING WITH excluded_indexes = {idx1}", 1).selects("idx2");
        assertThatPlan("SELECT * FROM %s WHERE v1='rare' AND v2='common' ALLOW FILTERING WITH excluded_indexes = {idx1}", 1).selects("idx2");
        assertThatPlan("SELECT * FROM %s WHERE v1='common' AND v2='rare' ALLOW FILTERING WITH excluded_indexes = {idx1}", 1).selects("idx2");
        assertThatPlan("SELECT * FROM %s WHERE v1='common' AND v2='common' ALLOW FILTERING WITH excluded_indexes = {idx1}", NUM_ROWS - 3).selects("idx2");
        assertThatPlan("SELECT * FROM %s WHERE v1='rare' OR v2='rare' ALLOW FILTERING WITH excluded_indexes = {idx1}", 3).selectsNone();
        assertThatPlan("SELECT * FROM %s WHERE v1='rare' OR v2='common'ALLOW FILTERING WITH excluded_indexes = {idx1}", NUM_ROWS - 1).selectsNone();
        assertThatPlan("SELECT * FROM %s WHERE v1='common' OR v2='rare' ALLOW FILTERING WITH excluded_indexes = {idx1}", NUM_ROWS - 1).selectsNone();
        assertThatPlan("SELECT * FROM %s WHERE v1='common' OR v2='common' ALLOW FILTERING WITH excluded_indexes = {idx1}", NUM_ROWS - 1).selectsNone();
        assertOrderingNeedsIndex("SELECT * FROM %s ORDER BY v1 LIMIT 10 WITH excluded_indexes = {idx1}", "v1");
        assertThatPlan("SELECT * FROM %s ORDER BY v2 LIMIT 10 WITH excluded_indexes = {idx1}", 10).selects("idx2");

        // excluding idx2
        assertThatPlan("SELECT * FROM %s WHERE v1='rare' AND v2='rare' ALLOW FILTERING WITH excluded_indexes = {idx2}", 1).selects("idx1");
        assertThatPlan("SELECT * FROM %s WHERE v1='rare' AND v2='common' ALLOW FILTERING WITH excluded_indexes = {idx2}", 1).selects("idx1");
        assertThatPlan("SELECT * FROM %s WHERE v1='common' AND v2='rare' ALLOW FILTERING WITH excluded_indexes = {idx2}", 1).selects("idx1");
        assertThatPlan("SELECT * FROM %s WHERE v1='common' AND v2='common' ALLOW FILTERING WITH excluded_indexes = {idx2}", NUM_ROWS - 3).selects("idx1");
        assertThatPlan("SELECT * FROM %s WHERE v1='rare' OR v2='rare' ALLOW FILTERING WITH excluded_indexes = {idx2}", 3).selectsNone();
        assertThatPlan("SELECT * FROM %s WHERE v1='rare' OR v2='common'ALLOW FILTERING WITH excluded_indexes = {idx2}", NUM_ROWS - 1).selectsNone();
        assertThatPlan("SELECT * FROM %s WHERE v1='common' OR v2='rare' ALLOW FILTERING WITH excluded_indexes = {idx2}", NUM_ROWS - 1).selectsNone();
        assertThatPlan("SELECT * FROM %s WHERE v1='common' OR v2='common' ALLOW FILTERING WITH excluded_indexes = {idx2}", NUM_ROWS - 1).selectsNone();
        assertThatPlan("SELECT * FROM %s ORDER BY v1 LIMIT 10 WITH excluded_indexes = {idx2}", 10).selects("idx1");
        assertOrderingNeedsIndex("SELECT * FROM %s ORDER BY v2 LIMIT 10 WITH excluded_indexes = {idx2}", "v2");

        // excluding both idx1 and idx2
        assertThatPlan("SELECT * FROM %s WHERE v1='rare' AND v2='rare' ALLOW FILTERING WITH excluded_indexes = {idx1, idx2}", 1).selectsNone();
        assertThatPlan("SELECT * FROM %s WHERE v1='rare' AND v2='common' ALLOW FILTERING WITH excluded_indexes = {idx1, idx2}", 1).selectsNone();
        assertThatPlan("SELECT * FROM %s WHERE v1='common' AND v2='rare' ALLOW FILTERING WITH excluded_indexes = {idx1, idx2}", 1).selectsNone();
        assertThatPlan("SELECT * FROM %s WHERE v1='common' AND v2='common' ALLOW FILTERING WITH excluded_indexes = {idx1, idx2}", NUM_ROWS - 3).selectsNone();
        assertThatPlan("SELECT * FROM %s WHERE v1='rare' OR v2='rare' ALLOW FILTERING WITH excluded_indexes = {idx1, idx2}", 3).selectsNone();
        assertThatPlan("SELECT * FROM %s WHERE v1='rare' OR v2='common'ALLOW FILTERING WITH excluded_indexes = {idx1, idx2}", NUM_ROWS - 1).selectsNone();
        assertThatPlan("SELECT * FROM %s WHERE v1='common' OR v2='rare' ALLOW FILTERING WITH excluded_indexes = {idx1, idx2}", NUM_ROWS - 1).selectsNone();
        assertThatPlan("SELECT * FROM %s WHERE v1='common' OR v2='common' ALLOW FILTERING WITH excluded_indexes = {idx1, idx2}", NUM_ROWS - 1).selectsNone();
        assertOrderingNeedsIndex("SELECT * FROM %s ORDER BY v1 LIMIT 10 WITH excluded_indexes = {idx1, idx2}", "v1");
        assertOrderingNeedsIndex("SELECT * FROM %s ORDER BY v2 LIMIT 10 WITH excluded_indexes = {idx1, idx2}", "v2");
    }

    @Test
    public void testQueryPlanningWithAnalyzer()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text, n int)");
        createIndex("CREATE CUSTOM INDEX idx ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = { 'index_analyzer': 'standard' }");
        execute("INSERT INTO %s (k, v) VALUES (1, 'Johann Strauss')");
        execute("INSERT INTO %s (k, v) VALUES (2, 'Richard Strauss')");
        execute("INSERT INTO %s (k, v) VALUES (3, 'Levi Strauss')");
        execute("INSERT INTO %s (k, v) VALUES (4, 'Lévi-Strauss')");

        // eq without any hints
        assertThatPlan("SELECT * FROM %s WHERE v='Strauss'", 4).selects("idx");
        assertThatPlan("SELECT * FROM %s WHERE v='Levi'", 1).selects("idx");
        assertThatPlan("SELECT * FROM %s WHERE v='Lévi-Strauss'", 1).selects("idx");

        // match without any hints
        assertThatPlan("SELECT * FROM %s WHERE v:'Strauss'", 4).selects("idx");
        assertThatPlan("SELECT * FROM %s WHERE v:'Levi'", 1).selects("idx");
        assertThatPlan("SELECT * FROM %s WHERE v:'Lévi-Strauss'", 1).selects("idx");

        // eq preferring the index
        assertThatPlan("SELECT * FROM %s WHERE v='Strauss' WITH preferred_indexes = {idx}", 4).selects("idx");
        assertThatPlan("SELECT * FROM %s WHERE v='Levi' WITH preferred_indexes = {idx}", 1).selects("idx");
        assertThatPlan("SELECT * FROM %s WHERE v='Lévi-Strauss' WITH preferred_indexes = {idx}", 1).selects("idx");

        // match preferring the index
        assertThatPlan("SELECT * FROM %s WHERE v:'Strauss' WITH preferred_indexes = {idx}", 4).selects("idx");
        assertThatPlan("SELECT * FROM %s WHERE v:'Levi' WITH preferred_indexes = {idx}", 1).selects("idx");
        assertThatPlan("SELECT * FROM %s WHERE v:'Lévi-Strauss' WITH preferred_indexes = {idx}", 1).selects("idx");

        // eq excluding the index
        assertThatPlan("SELECT * FROM %s WHERE v='Strauss' ALLOW FILTERING WITH excluded_indexes = {idx}", 0).selectsNone();
        assertThatPlan("SELECT * FROM %s WHERE v='Levi' ALLOW FILTERING WITH excluded_indexes = {idx}", 0).selectsNone();
        assertThatPlan("SELECT * FROM %s WHERE v='Lévi-Strauss' ALLOW FILTERING WITH excluded_indexes = {idx}", 1).selectsNone();

        // match excluding the index
        assertMatchNeedsIndex("SELECT * FROM %s WHERE v:'Strauss' ALLOW FILTERING WITH excluded_indexes = {idx}", "v", "Strauss");
        assertMatchNeedsIndex("SELECT * FROM %s WHERE v:'Levi' ALLOW FILTERING WITH excluded_indexes = {idx}", "v", "Levi");
        assertMatchNeedsIndex("SELECT * FROM %s WHERE v:'Lévi-Strauss' ALLOW FILTERING WITH excluded_indexes = {idx}", "v", "Lévi-Strauss");
    }

    private void assertOrderingNeedsIndex(String query, String column)
    {
        Assertions.assertThatThrownBy(() -> execute(query))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessageContaining(String.format(StatementRestrictions.NON_CLUSTER_ORDERING_REQUIRES_INDEX_MESSAGE, column));
    }

    private void assertMatchNeedsIndex(String query, String column, String value)
    {
        Assertions.assertThatThrownBy(() -> execute(query))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessageContaining(String.format(StatementRestrictions.RESTRICTION_REQUIRES_INDEX_MESSAGE,
                                                      ':',
                                                      String.format("%s : '%s'", column, value)));
    }

    private PlanSelectionAssertion assertThatPlan(String query, int numExpectedRows)
    {
        // First execute the query and check the number of rows returned
        Assertions.assertThat(execute(query).size()).isEqualTo(numExpectedRows);

        ReadCommand command = parseReadCommand(query);
        Index.QueryPlan queryPlan = command.indexQueryPlan();
        if (queryPlan == null)
            return new PlanSelectionAssertion(null);

        StorageAttachedIndexQueryPlan saiQueryPlan = (StorageAttachedIndexQueryPlan) queryPlan;
        Assertions.assertThat(saiQueryPlan).isNotNull();
        StorageAttachedIndexSearcher searcher = saiQueryPlan.searcherFor(command);
        Set<String> selectedIndexes = searcher.plannedIndexes();
        return new PlanSelectionAssertion(selectedIndexes);
    }

    private static class PlanSelectionAssertion
    {
        private final Set<String> selectedIndexes;

        public PlanSelectionAssertion(@Nullable Set<String> selectedIndexes)
        {
            this.selectedIndexes = selectedIndexes;
        }

        public void selectsNone()
        {
            Assertions.assertThat(selectedIndexes).isNull();
        }

        public void selects(String... indexes)
        {
            Assertions.assertThat(selectedIndexes)
                      .isNotNull()
                      .as("Expected to select only %s, but got: %s", indexes, selectedIndexes)
                      .isEqualTo(Set.of(indexes));
        }

        public void selectsAnyOf(String index1, String index2, String... otherIndexes)
        {
            Set<String> expectedIndexes = new HashSet<>(otherIndexes.length + 1);
            expectedIndexes.add(index1);
            expectedIndexes.add(index2);
            expectedIndexes.addAll(Arrays.asList(otherIndexes));

            Assertions.assertThat(selectedIndexes)
                      .isNotNull()
                      .as("Expected to select any of %s, but got: %s", expectedIndexes, selectedIndexes)
                      .containsAnyElementsOf(expectedIndexes);
        }

        public void selectsAtLeast(String... indexes)
        {
            Assertions.assertThat(selectedIndexes)
                      .isNotNull()
                      .as("Expected to select at least %s, but got: %s", indexes, selectedIndexes)
                      .containsAll(Set.of(indexes));
        }
    }
}
