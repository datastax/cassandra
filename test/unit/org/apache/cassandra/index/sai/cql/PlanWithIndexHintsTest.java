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
import java.util.Collection;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.plan.Plan;
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
        String idx1 = createIndex("CREATE CUSTOM INDEX idx1 ON %s(v1) USING 'StorageAttachedIndex'");
        String idx2 = createIndex("CREATE CUSTOM INDEX idx2 ON %s(v2) USING 'StorageAttachedIndex'");

        // Insert some rows, with some rare values and some common values, so the query planner will tend to prefer
        // the index with the most selective predicate.
        int numRows = 1000;
        for (int i = 0; i < numRows; i++)
        {
            execute("INSERT INTO %s (k, v1, v2) VALUES (?, ?, ?)",
                    i,
                    i == 0 || i == 1 ? "rare" : "common",
                    i == 0 || i == 2 ? "rare" : "common");
        }

        if (flush)
            flush();

        // test some queries without any hints, so selection is based on selectivity only
        assertThatPlanFor("SELECT * FROM %s WHERE v1='rare' AND v2='rare'", 1).usesAnyOf(idx1, idx2);
        assertThatPlanFor("SELECT * FROM %s WHERE v1='rare' AND v2='common'", 1).uses(idx1);
        assertThatPlanFor("SELECT * FROM %s WHERE v1='common' AND v2='rare'", 1).uses(idx2);
        assertThatPlanFor("SELECT * FROM %s WHERE v1='common' AND v2='common'", numRows - 3).usesAnyOf(idx1, idx2);
        assertThatPlanFor("SELECT * FROM %s WHERE v1='rare' OR v2='rare'", 3).uses(idx1, idx2);
        assertThatPlanFor("SELECT * FROM %s WHERE v1='rare' OR v2='common'", numRows - 1).uses(idx1, idx2);
        assertThatPlanFor("SELECT * FROM %s WHERE v1='common' OR v2='rare'", numRows - 1).uses(idx1, idx2);
        assertThatPlanFor("SELECT * FROM %s WHERE v1='common' OR v2='common'", numRows - 1).uses(idx1, idx2);
        assertThatPlanFor("SELECT * FROM %s ORDER BY v1 LIMIT 10", 10).uses(idx1);
        assertThatPlanFor("SELECT * FROM %s ORDER BY v2 LIMIT 10", 10).uses(idx2);

        // run the same queries as before, but with hints preferring idx1
        assertThatPlanFor("SELECT * FROM %s WHERE v1='rare' AND v2='rare' WITH preferred_indexes = {idx1}", 1).usesAtLeast(idx1);
        assertThatPlanFor("SELECT * FROM %s WHERE v1='rare' AND v2='common' WITH preferred_indexes = {idx1}", 1).usesAtLeast(idx1);
        assertThatPlanFor("SELECT * FROM %s WHERE v1='common' AND v2='rare' WITH preferred_indexes = {idx1}", 1).usesAtLeast(idx1);
        assertThatPlanFor("SELECT * FROM %s WHERE v1='common' AND v2='common' WITH preferred_indexes = {idx1}", numRows - 3).usesAtLeast(idx1);
        assertThatPlanFor("SELECT * FROM %s WHERE v1='rare' OR v2='rare' WITH preferred_indexes = {idx1}", 3).uses(idx1, idx2);
        assertThatPlanFor("SELECT * FROM %s WHERE v1='rare' OR v2='common' WITH preferred_indexes = {idx1}", numRows - 1).uses(idx1, idx2);
        assertThatPlanFor("SELECT * FROM %s WHERE v1='common' OR v2='rare' WITH preferred_indexes = {idx1}", numRows - 1).uses(idx1, idx2);
        assertThatPlanFor("SELECT * FROM %s WHERE v1='common' OR v2='common' WITH preferred_indexes = {idx1}", numRows - 1).uses(idx1, idx2);
        assertThatPlanFor("SELECT * FROM %s ORDER BY v1 LIMIT 10 WITH preferred_indexes = {idx1}", 10).uses(idx1);
        assertThatPlanFor("SELECT * FROM %s ORDER BY v2 LIMIT 10 WITH preferred_indexes = {idx1}", 10).uses(idx2);

        // preferring idx2
        assertThatPlanFor("SELECT * FROM %s WHERE v1='rare' AND v2='rare' WITH preferred_indexes = {idx2}", 1).usesAtLeast(idx2);
        assertThatPlanFor("SELECT * FROM %s WHERE v1='rare' AND v2='common' WITH preferred_indexes = {idx2}", 1).usesAtLeast(idx2);
        assertThatPlanFor("SELECT * FROM %s WHERE v1='common' AND v2='rare' WITH preferred_indexes = {idx2}", 1).usesAtLeast(idx2);
        assertThatPlanFor("SELECT * FROM %s WHERE v1='common' AND v2='common' WITH preferred_indexes = {idx2}", numRows - 3).usesAtLeast(idx2);
        assertThatPlanFor("SELECT * FROM %s WHERE v1='rare' OR v2='rare' WITH preferred_indexes = {idx2}", 3).uses(idx1, idx2);
        assertThatPlanFor("SELECT * FROM %s WHERE v1='rare' OR v2='common' WITH preferred_indexes = {idx2}", numRows - 1).uses(idx1, idx2);
        assertThatPlanFor("SELECT * FROM %s WHERE v1='common' OR v2='rare' WITH preferred_indexes = {idx2}", numRows - 1).uses(idx1, idx2);
        assertThatPlanFor("SELECT * FROM %s WHERE v1='common' OR v2='common' WITH preferred_indexes = {idx2}", numRows - 1).uses(idx1, idx2);
        assertThatPlanFor("SELECT * FROM %s ORDER BY v1 LIMIT 10 WITH preferred_indexes = {idx2}", 10).uses(idx1);
        assertThatPlanFor("SELECT * FROM %s ORDER BY v2 LIMIT 10 WITH preferred_indexes = {idx2}", 10).uses(idx2);

        // preferring both idx1 and idx2
        assertThatPlanFor("SELECT * FROM %s WHERE v1='rare' AND v2='rare' WITH preferred_indexes = {idx1, idx2}", 1).uses(idx1, idx2);
        assertThatPlanFor("SELECT * FROM %s WHERE v1='rare' AND v2='common' WITH preferred_indexes = {idx1, idx2}", 1).uses(idx1, idx2);
        assertThatPlanFor("SELECT * FROM %s WHERE v1='common' AND v2='rare' WITH preferred_indexes = {idx1, idx2}", 1).uses(idx1, idx2);
        assertThatPlanFor("SELECT * FROM %s WHERE v1='common' AND v2='common'  WITH preferred_indexes = {idx1, idx2}", numRows - 3).uses(idx1, idx2);
        assertThatPlanFor("SELECT * FROM %s WHERE v1='rare' OR v2='rare' WITH preferred_indexes = {idx1, idx2}", 3).uses(idx1, idx2);
        assertThatPlanFor("SELECT * FROM %s WHERE v1='rare' OR v2='common' WITH preferred_indexes = {idx1, idx2}", numRows - 1).uses(idx1, idx2);
        assertThatPlanFor("SELECT * FROM %s WHERE v1='common' OR v2='rare' WITH preferred_indexes = {idx1, idx2}", numRows - 1).uses(idx1, idx2);
        assertThatPlanFor("SELECT * FROM %s WHERE v1='common' OR v2='common' WITH preferred_indexes = {idx1, idx2}", numRows - 1).uses(idx1, idx2);
        assertThatPlanFor("SELECT * FROM %s ORDER BY v1 LIMIT 10 WITH preferred_indexes = {idx1, idx2}", 10).uses(idx1);
        assertThatPlanFor("SELECT * FROM %s ORDER BY v2 LIMIT 10 WITH preferred_indexes = {idx1, idx2}", 10).uses(idx2);

        // excluding idx1
        assertThatPlanFor("SELECT * FROM %s WHERE v1='rare' AND v2='rare' ALLOW FILTERING WITH excluded_indexes = {idx1}", 1).uses(idx2);
        assertThatPlanFor("SELECT * FROM %s WHERE v1='rare' AND v2='common' ALLOW FILTERING WITH excluded_indexes = {idx1}", 1).uses(idx2);
        assertThatPlanFor("SELECT * FROM %s WHERE v1='common' AND v2='rare' ALLOW FILTERING WITH excluded_indexes = {idx1}", 1).uses(idx2);
        assertThatPlanFor("SELECT * FROM %s WHERE v1='common' AND v2='common' ALLOW FILTERING WITH excluded_indexes = {idx1}", numRows - 3).uses(idx2);
        assertThatPlanFor("SELECT * FROM %s WHERE v1='rare' OR v2='rare' ALLOW FILTERING WITH excluded_indexes = {idx1}", 3).usesNone();
        assertThatPlanFor("SELECT * FROM %s WHERE v1='rare' OR v2='common'ALLOW FILTERING WITH excluded_indexes = {idx1}", numRows - 1).usesNone();
        assertThatPlanFor("SELECT * FROM %s WHERE v1='common' OR v2='rare' ALLOW FILTERING WITH excluded_indexes = {idx1}", numRows - 1).usesNone();
        assertThatPlanFor("SELECT * FROM %s WHERE v1='common' OR v2='common' ALLOW FILTERING WITH excluded_indexes = {idx1}", numRows - 1).usesNone();
        assertOrderingNeedsIndex("SELECT * FROM %s ORDER BY v1 LIMIT 10 WITH excluded_indexes = {idx1}", "v1");
        assertThatPlanFor("SELECT * FROM %s ORDER BY v2 LIMIT 10 WITH excluded_indexes = {idx1}", 10).uses(idx2);

        // excluding idx2
        assertThatPlanFor("SELECT * FROM %s WHERE v1='rare' AND v2='rare' ALLOW FILTERING WITH excluded_indexes = {idx2}", 1).uses(idx1);
        assertThatPlanFor("SELECT * FROM %s WHERE v1='rare' AND v2='common' ALLOW FILTERING WITH excluded_indexes = {idx2}", 1).uses(idx1);
        assertThatPlanFor("SELECT * FROM %s WHERE v1='common' AND v2='rare' ALLOW FILTERING WITH excluded_indexes = {idx2}", 1).uses(idx1);
        assertThatPlanFor("SELECT * FROM %s WHERE v1='common' AND v2='common' ALLOW FILTERING WITH excluded_indexes = {idx2}", numRows - 3).uses(idx1);
        assertThatPlanFor("SELECT * FROM %s WHERE v1='rare' OR v2='rare' ALLOW FILTERING WITH excluded_indexes = {idx2}", 3).usesNone();
        assertThatPlanFor("SELECT * FROM %s WHERE v1='rare' OR v2='common'ALLOW FILTERING WITH excluded_indexes = {idx2}", numRows - 1).usesNone();
        assertThatPlanFor("SELECT * FROM %s WHERE v1='common' OR v2='rare' ALLOW FILTERING WITH excluded_indexes = {idx2}", numRows - 1).usesNone();
        assertThatPlanFor("SELECT * FROM %s WHERE v1='common' OR v2='common' ALLOW FILTERING WITH excluded_indexes = {idx2}", numRows - 1).usesNone();
        assertThatPlanFor("SELECT * FROM %s ORDER BY v1 LIMIT 10 WITH excluded_indexes = {idx2}", 10).uses(idx1);
        assertOrderingNeedsIndex("SELECT * FROM %s ORDER BY v2 LIMIT 10 WITH excluded_indexes = {idx2}", "v2");

        // excluding both idx1 and idx2
        assertThatPlanFor("SELECT * FROM %s WHERE v1='rare' AND v2='rare' ALLOW FILTERING WITH excluded_indexes = {idx1, idx2}", 1).usesNone();
        assertThatPlanFor("SELECT * FROM %s WHERE v1='rare' AND v2='common' ALLOW FILTERING WITH excluded_indexes = {idx1, idx2}", 1).usesNone();
        assertThatPlanFor("SELECT * FROM %s WHERE v1='common' AND v2='rare' ALLOW FILTERING WITH excluded_indexes = {idx1, idx2}", 1).usesNone();
        assertThatPlanFor("SELECT * FROM %s WHERE v1='common' AND v2='common' ALLOW FILTERING WITH excluded_indexes = {idx1, idx2}", numRows - 3).usesNone();
        assertThatPlanFor("SELECT * FROM %s WHERE v1='rare' OR v2='rare' ALLOW FILTERING WITH excluded_indexes = {idx1, idx2}", 3).usesNone();
        assertThatPlanFor("SELECT * FROM %s WHERE v1='rare' OR v2='common'ALLOW FILTERING WITH excluded_indexes = {idx1, idx2}", numRows - 1).usesNone();
        assertThatPlanFor("SELECT * FROM %s WHERE v1='common' OR v2='rare' ALLOW FILTERING WITH excluded_indexes = {idx1, idx2}", numRows - 1).usesNone();
        assertThatPlanFor("SELECT * FROM %s WHERE v1='common' OR v2='common' ALLOW FILTERING WITH excluded_indexes = {idx1, idx2}", numRows - 1).usesNone();
        assertOrderingNeedsIndex("SELECT * FROM %s ORDER BY v1 LIMIT 10 WITH excluded_indexes = {idx1, idx2}", "v1");
        assertOrderingNeedsIndex("SELECT * FROM %s ORDER BY v2 LIMIT 10 WITH excluded_indexes = {idx1, idx2}", "v2");
    }

    @Test
    public void testQueryPlanningWithAnalyzer()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text)");
        String idx = createIndex("CREATE CUSTOM INDEX idx ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = { 'index_analyzer': 'standard' }");

        String insert = "INSERT INTO %s (k, v) VALUES (?, ?)";
        Object[] row1 = row(1, "Johann Strauss");
        Object[] row2 = row(2, "Richard Strauss");
        Object[] row3 = row(3, "Levi Strauss");
        Object[] row4 = row(4, "Lévi-Strauss");
        execute(insert, row1);
        execute(insert, row2);
        execute(insert, row3);
        execute(insert, row4);

        if (flush)
            flush();

        // eq without any hints
        assertThatPlanFor("SELECT * FROM %s WHERE v='Strauss'", row1, row2, row3, row4).uses(idx);
        assertThatPlanFor("SELECT * FROM %s WHERE v='Levi'", row3).uses(idx);
        assertThatPlanFor("SELECT * FROM %s WHERE v='Lévi-Strauss'", row4).uses(idx);

        // match without any hints
        assertThatPlanFor("SELECT * FROM %s WHERE v:'Strauss'", row1, row2, row3, row4).uses(idx);
        assertThatPlanFor("SELECT * FROM %s WHERE v:'Levi'", row3).uses(idx);
        assertThatPlanFor("SELECT * FROM %s WHERE v:'Lévi-Strauss'", row4).uses(idx);

        // eq preferring the index
        assertThatPlanFor("SELECT * FROM %s WHERE v='Strauss' WITH preferred_indexes = {idx}", row1, row2, row3, row4).uses(idx);
        assertThatPlanFor("SELECT * FROM %s WHERE v='Levi' WITH preferred_indexes = {idx}", row3).uses(idx);
        assertThatPlanFor("SELECT * FROM %s WHERE v='Lévi-Strauss' WITH preferred_indexes = {idx}", row4).uses(idx);

        // match preferring the index
        assertThatPlanFor("SELECT * FROM %s WHERE v:'Strauss' WITH preferred_indexes = {idx}", row1, row2, row3, row4).uses(idx);
        assertThatPlanFor("SELECT * FROM %s WHERE v:'Levi' WITH preferred_indexes = {idx}", row3).uses(idx);
        assertThatPlanFor("SELECT * FROM %s WHERE v:'Lévi-Strauss' WITH preferred_indexes = {idx}", row4).uses(idx);

        // eq excluding the index
        assertThatPlanFor("SELECT * FROM %s WHERE v='Strauss' ALLOW FILTERING WITH excluded_indexes = {idx}", 0).usesNone();
        assertThatPlanFor("SELECT * FROM %s WHERE v='Levi' ALLOW FILTERING WITH excluded_indexes = {idx}", 0).usesNone();
        assertThatPlanFor("SELECT * FROM %s WHERE v='Lévi-Strauss' ALLOW FILTERING WITH excluded_indexes = {idx}", row4).usesNone();

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
}
