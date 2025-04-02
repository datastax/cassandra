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

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.cassandra.index.sai.SAIUtil;
import org.apache.cassandra.index.sai.analyzer.AnalyzerEqOperatorSupport;

import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.db.filter.IndexHints;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.plan.Plan;
import org.assertj.core.api.Assertions;

import static java.lang.String.format;

/**
 * Tests the effects of {@link org.apache.cassandra.db.filter.IndexHints} in SAI's internal query planning:
 * <ul>
 *    <li>Included indexes shouldn't be pruned in the optimized the query {@link Plan}.</li>
 *    <li>Excluded indexes shouldn't be included in the query {@link Plan}.</li>
 * </ul>
 */
@RunWith(Parameterized.class)
public class PlanWithIndexHintsTest extends SAITester
{
    @Parameterized.Parameter
    public Version version;

    @Parameterized.Parameters(name = "version={0}")
    public static List<Object> data()
    {
        return Version.ALL.stream().map(v -> new Object[]{v}).collect(Collectors.toList());
    }

    @Before
    public void setup() throws Throwable
    {
        SAIUtil.setCurrentVersion(version);
    }

    @Test
    public void testQueryPlanning() throws Throwable
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

        beforeAndAfterFlush(() -> {

            // test some queries without any hints, so selection is based on selectivity only
            assertThatPlanFor("SELECT * FROM %s WHERE v1='rare'", 2).uses(idx1);
            assertThatPlanFor("SELECT * FROM %s WHERE v2='rare'", 2).uses(idx2);
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

            // run the same queries as before, but with hints including either idx1 or idx2
            assertThatPlanFor("SELECT * FROM %s WHERE v1='rare' WITH included_indexes = {idx1}", 2).uses(idx1);
            assertNonIncludableIndexError("SELECT * FROM %s WHERE v1='rare' WITH included_indexes = {idx2}", idx2);
            assertNonIncludableIndexError("SELECT * FROM %s WHERE v2='rare' WITH included_indexes = {idx1}", idx1);
            assertThatPlanFor("SELECT * FROM %s WHERE v2='rare' WITH included_indexes = {idx2}", 2).uses(idx2);
            for (String idx : Arrays.asList(idx1, idx2))
            {
                String otherIdx = idx.equals(idx1) ? idx2 : idx1;
                assertThatPlanFor(format("SELECT * FROM %%s WHERE v1='rare' AND v2='rare' WITH included_indexes = {%s}", idx), 1).usesAtLeast(idx);
                assertThatPlanFor(format("SELECT * FROM %%s WHERE v1='rare' AND v2='common' WITH included_indexes = {%s}", idx), 1).usesAtLeast(idx);
                assertThatPlanFor(format("SELECT * FROM %%s WHERE v1='common' AND v2='rare' WITH included_indexes = {%s}", idx), 1).usesAtLeast(idx);
                assertThatPlanFor(format("SELECT * FROM %%s WHERE v1='common' AND v2='common' WITH included_indexes = {%s}", idx), numRows - 3).usesAtLeast(idx);
                assertThatPlanFor(format("SELECT * FROM %%s WHERE v1='rare' OR v2='rare' WITH included_indexes = {%s}", idx), 3).uses(idx, otherIdx);
                assertThatPlanFor(format("SELECT * FROM %%s WHERE v1='rare' OR v2='common' WITH included_indexes = {%s}", idx), numRows - 1).uses(idx, otherIdx);
                assertThatPlanFor(format("SELECT * FROM %%s WHERE v1='common' OR v2='rare' WITH included_indexes = {%s}", idx), numRows - 1).uses(idx, otherIdx);
                assertThatPlanFor(format("SELECT * FROM %%s WHERE v1='common' OR v2='common' WITH included_indexes = {%s}", idx), numRows - 1).uses(idx, otherIdx);
            }
            assertThatPlanFor("SELECT * FROM %s ORDER BY v1 LIMIT 10 WITH included_indexes = {idx1}", 10).uses(idx1);
            assertNonIncludableIndexError("SELECT * FROM %s ORDER BY v2 LIMIT 10 WITH included_indexes = {idx1}", idx1);
            assertNonIncludableIndexError("SELECT * FROM %s ORDER BY v1 LIMIT 10 WITH included_indexes = {idx2}", idx2);
            assertThatPlanFor("SELECT * FROM %s ORDER BY v2 LIMIT 10 WITH included_indexes = {idx2}", 10).uses(idx2);

            // including both idx1 and idx2
            assertNonIncludableIndexError("SELECT * FROM %s WHERE v1='rare' WITH included_indexes = {idx1,idx2}", idx2);
            assertNonIncludableIndexError("SELECT * FROM %s WHERE v2='rare' WITH included_indexes = {idx1,idx2}", idx1);
            assertThatPlanFor("SELECT * FROM %s WHERE v1='rare' AND v2='rare' WITH included_indexes = {idx1,idx2}", 1).uses(idx1, idx2);
            assertThatPlanFor("SELECT * FROM %s WHERE v1='rare' AND v2='common' WITH included_indexes = {idx1,idx2}", 1).uses(idx1, idx2);
            assertThatPlanFor("SELECT * FROM %s WHERE v1='common' AND v2='rare' WITH included_indexes = {idx1,idx2}", 1).uses(idx1, idx2);
            assertThatPlanFor("SELECT * FROM %s WHERE v1='common' AND v2='common'  WITH included_indexes = {idx1,idx2}", numRows - 3).uses(idx1, idx2);
            assertThatPlanFor("SELECT * FROM %s WHERE v1='rare' OR v2='rare' WITH included_indexes = {idx1,idx2}", 3).uses(idx1, idx2);
            assertThatPlanFor("SELECT * FROM %s WHERE v1='rare' OR v2='common' WITH included_indexes = {idx1,idx2}", numRows - 1).uses(idx1, idx2);
            assertThatPlanFor("SELECT * FROM %s WHERE v1='common' OR v2='rare' WITH included_indexes = {idx1,idx2}", numRows - 1).uses(idx1, idx2);
            assertThatPlanFor("SELECT * FROM %s WHERE v1='common' OR v2='common' WITH included_indexes = {idx1,idx2}", numRows - 1).uses(idx1, idx2);
            assertNonIncludableIndexError("SELECT * FROM %s ORDER BY v1 LIMIT 10 WITH included_indexes = {idx1,idx2}", idx2);
            assertNonIncludableIndexError("SELECT * FROM %s ORDER BY v2 LIMIT 10 WITH included_indexes = {idx1,idx2}", idx1);

            // excluding either idx1 or idx2
            assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1='rare' WITH excluded_indexes = {idx1}");
            assertThatPlanFor("SELECT * FROM %s WHERE v1='rare' WITH excluded_indexes = {idx2}", 2).uses(idx1);
            assertThatPlanFor("SELECT * FROM %s WHERE v2='rare' WITH excluded_indexes = {idx1}", 2).uses(idx2);
            assertNeedsAllowFiltering("SELECT * FROM %s WHERE v2='rare' WITH excluded_indexes = {idx2}");
            for (String idx : Arrays.asList(idx1, idx2))
            {
                String otherIdx = idx.equals(idx1) ? idx2 : idx1;
                assertThatPlanFor(format("SELECT * FROM %%s WHERE v1='rare' AND v2='rare' ALLOW FILTERING WITH excluded_indexes={%s}", idx), 1).uses(otherIdx);
                assertThatPlanFor(format("SELECT * FROM %%s WHERE v1='rare' AND v2='common' ALLOW FILTERING WITH excluded_indexes={%s}", idx), 1).uses(otherIdx);
                assertThatPlanFor(format("SELECT * FROM %%s WHERE v1='common' AND v2='rare' ALLOW FILTERING WITH excluded_indexes={%s}", idx), 1).uses(otherIdx);
                assertThatPlanFor(format("SELECT * FROM %%s WHERE v1='common' AND v2='common' ALLOW FILTERING WITH excluded_indexes={%s}", idx), numRows - 3).uses(otherIdx);
                assertThatPlanFor(format("SELECT * FROM %%s WHERE v1='rare' OR v2='rare' ALLOW FILTERING WITH excluded_indexes={%s}", idx), 3).usesNone();
                assertThatPlanFor(format("SELECT * FROM %%s WHERE v1='rare' OR v2='common'ALLOW FILTERING WITH excluded_indexes={%s}", idx), numRows - 1).usesNone();
                assertThatPlanFor(format("SELECT * FROM %%s WHERE v1='common' OR v2='rare' ALLOW FILTERING WITH excluded_indexes={%s}", idx), numRows - 1).usesNone();
                assertThatPlanFor(format("SELECT * FROM %%s WHERE v1='common' OR v2='common' ALLOW FILTERING WITH excluded_indexes={%s}", idx), numRows - 1).usesNone();
            }
            assertOrderingNeedsIndex("SELECT * FROM %s ORDER BY v1 LIMIT 10 WITH excluded_indexes={idx1}", "v1");
            assertThatPlanFor("SELECT * FROM %s ORDER BY v2 LIMIT 10 WITH excluded_indexes={idx1}", 10).uses(idx2);
            assertThatPlanFor("SELECT * FROM %s ORDER BY v1 LIMIT 10 WITH excluded_indexes={idx2}", 10).uses(idx1);
            assertOrderingNeedsIndex("SELECT * FROM %s ORDER BY v2 LIMIT 10 WITH excluded_indexes={idx2}", "v2");

            // excluding both idx1 and idx2
            assertThatPlanFor("SELECT * FROM %s WHERE v1='rare' ALLOW FILTERING WITH excluded_indexes = {idx1,idx2}", 2).usesNone();
            assertThatPlanFor("SELECT * FROM %s WHERE v2='rare' ALLOW FILTERING WITH excluded_indexes = {idx1,idx2}", 2).usesNone();
            assertThatPlanFor("SELECT * FROM %s WHERE v1='rare' AND v2='rare' ALLOW FILTERING WITH excluded_indexes={idx1,idx2}", 1).usesNone();
            assertThatPlanFor("SELECT * FROM %s WHERE v1='rare' AND v2='common' ALLOW FILTERING WITH excluded_indexes={idx1,idx2}", 1).usesNone();
            assertThatPlanFor("SELECT * FROM %s WHERE v1='common' AND v2='rare' ALLOW FILTERING WITH excluded_indexes={idx1,idx2}", 1).usesNone();
            assertThatPlanFor("SELECT * FROM %s WHERE v1='common' AND v2='common' ALLOW FILTERING WITH excluded_indexes={idx1,idx2}", numRows - 3).usesNone();
            assertThatPlanFor("SELECT * FROM %s WHERE v1='rare' OR v2='rare' ALLOW FILTERING WITH excluded_indexes={idx1,idx2}", 3).usesNone();
            assertThatPlanFor("SELECT * FROM %s WHERE v1='rare' OR v2='common'ALLOW FILTERING WITH excluded_indexes={idx1,idx2}", numRows - 1).usesNone();
            assertThatPlanFor("SELECT * FROM %s WHERE v1='common' OR v2='rare' ALLOW FILTERING WITH excluded_indexes={idx1,idx2}", numRows - 1).usesNone();
            assertThatPlanFor("SELECT * FROM %s WHERE v1='common' OR v2='common' ALLOW FILTERING WITH excluded_indexes={idx1,idx2}", numRows - 1).usesNone();
            assertOrderingNeedsIndex("SELECT * FROM %s ORDER BY v1 LIMIT 10 WITH excluded_indexes={idx1,idx2}", "v1");
            assertOrderingNeedsIndex("SELECT * FROM %s ORDER BY v2 LIMIT 10 WITH excluded_indexes={idx1,idx2}", "v2");
        });
    }

    @Test
    public void testQueryPlanningWithAnalyzer() throws Throwable
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

        // equality queries using the analyzed index should emit a warning
        String warning = format(AnalyzerEqOperatorSupport.EQ_RESTRICTION_ON_ANALYZED_WARNING, 'v', idx);

        beforeAndAfterFlush(() -> {

            // eq without any hints
            assertThatPlanFor("SELECT * FROM %s WHERE v='Strauss'", row1, row2, row3, row4).uses(idx).warns(warning);
            assertThatPlanFor("SELECT * FROM %s WHERE v='Levi'", row3).uses(idx).warns(warning);
            assertThatPlanFor("SELECT * FROM %s WHERE v='Lévi-Strauss'", row4).uses(idx).warns(warning);

            // match without any hints
            assertThatPlanFor("SELECT * FROM %s WHERE v:'Strauss'", row1, row2, row3, row4).uses(idx).doesntWarn();
            assertThatPlanFor("SELECT * FROM %s WHERE v:'Levi'", row3).uses(idx).doesntWarn();
            assertThatPlanFor("SELECT * FROM %s WHERE v:'Lévi-Strauss'", row4).uses(idx).doesntWarn();

            // eq including the index
            assertThatPlanFor("SELECT * FROM %s WHERE v='Strauss' WITH included_indexes = {idx}", row1, row2, row3, row4).uses(idx).warns(warning);
            assertThatPlanFor("SELECT * FROM %s WHERE v='Levi' WITH included_indexes = {idx}", row3).uses(idx).warns(warning);
            assertThatPlanFor("SELECT * FROM %s WHERE v='Lévi-Strauss' WITH included_indexes = {idx}", row4).uses(idx).warns(warning);

            // match including the index
            assertThatPlanFor("SELECT * FROM %s WHERE v:'Strauss' WITH included_indexes = {idx}", row1, row2, row3, row4).uses(idx).doesntWarn();
            assertThatPlanFor("SELECT * FROM %s WHERE v:'Levi' WITH included_indexes = {idx}", row3).uses(idx).doesntWarn();
            assertThatPlanFor("SELECT * FROM %s WHERE v:'Lévi-Strauss' WITH included_indexes = {idx}", row4).uses(idx).doesntWarn();

            // eq excluding the index
            assertThatPlanFor("SELECT * FROM %s WHERE v='Strauss' ALLOW FILTERING WITH excluded_indexes={idx}", 0).usesNone().doesntWarn();
            assertThatPlanFor("SELECT * FROM %s WHERE v='Levi' ALLOW FILTERING WITH excluded_indexes={idx}", 0).usesNone().doesntWarn();
            assertThatPlanFor("SELECT * FROM %s WHERE v='Lévi-Strauss' ALLOW FILTERING WITH excluded_indexes={idx}", row4).usesNone().doesntWarn();

            // match excluding the index
            assertMatchNeedsIndex("SELECT * FROM %s WHERE v:'Strauss' ALLOW FILTERING WITH excluded_indexes={idx}", "v", "Strauss");
            assertMatchNeedsIndex("SELECT * FROM %s WHERE v:'Levi' ALLOW FILTERING WITH excluded_indexes={idx}", "v", "Levi");
            assertMatchNeedsIndex("SELECT * FROM %s WHERE v:'Lévi-Strauss' ALLOW FILTERING WITH excluded_indexes={idx}", "v", "Lévi-Strauss");

            // if the tested version supports BM25...
            if (version.onOrAfter(Version.BM25_EARLIEST))
            {
                // BM25 without any hints
                assertThatPlanFor("SELECT * FROM %s ORDER BY v BM25 OF 'Strauss' LIMIT 10", row1, row2, row3, row4).uses(idx).doesntWarn();
                assertThatPlanFor("SELECT * FROM %s ORDER BY v BM25 OF 'Strauss' LIMIT 2", row1, row2).uses(idx).doesntWarn();

                // BM25 including the index
                assertThatPlanFor("SELECT * FROM %s ORDER BY v BM25 OF 'Strauss' LIMIT 10 WITH included_indexes = {idx}", row1, row2, row3, row4).uses(idx).doesntWarn();
                assertThatPlanFor("SELECT * FROM %s ORDER BY v BM25 OF 'Strauss' LIMIT 2 WITH included_indexes = {idx}", row1, row2).uses(idx).doesntWarn();

                // BM25 excluding the index
                assertBM25RequiresAnAnalyzedIndex("SELECT * FROM %s ORDER BY v BM25 OF 'Strauss' LIMIT 10 WITH excluded_indexes={idx}", "v");
                assertBM25RequiresAnAnalyzedIndex("SELECT * FROM %s ORDER BY v BM25 OF 'Strauss' LIMIT 2 WITH excluded_indexes={idx}", "v");
            }
        });
    }

    @Test
    public void testQueryPlanningWithNumericQueries() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v1 int, v2 int)");
        String idx1 = createIndex("CREATE CUSTOM INDEX idx1 ON %s(v1) USING 'StorageAttachedIndex'");
        String idx2 = createIndex("CREATE CUSTOM INDEX idx2 ON %s(v2) USING 'StorageAttachedIndex'");

        String insert = "INSERT INTO %s (k, v1, v2) VALUES (?, ?, ?)";
        Object[] row1 = row(1, 0, 0);
        Object[] row2 = row(2, 0, 1);
        Object[] row3 = row(3, 1, 0);
        Object[] row4 = row(4, 1, 1);
        Object[] row5 = row(5, 2, 0);
        Object[] row6 = row(6, 2, 1);
        execute(insert, row1);
        execute(insert, row2);
        execute(insert, row3);
        execute(insert, row4);
        execute(insert, row5);
        execute(insert, row6);

        beforeAndAfterFlush(() -> {

            // without any hints
            assertThatPlanFor("SELECT * FROM %s WHERE v1=0", row1, row2).uses(idx1);
            assertThatPlanFor("SELECT * FROM %s WHERE v1!=0", row3, row4, row5, row6).uses(idx1);
            assertThatPlanFor("SELECT * FROM %s WHERE v1>0", row3, row4, row5, row6).uses(idx1);
            assertThatPlanFor("SELECT * FROM %s WHERE v1<2", row1, row2, row3, row4).uses(idx1);
            assertThatPlanFor("SELECT * FROM %s WHERE v1>0 AND v1<=2", row3, row4, row5, row6).uses(idx1);
            assertThatPlanFor("SELECT * FROM %s WHERE v1>=0 AND v1<2", row1, row2, row3, row4).uses(idx1);
            assertThatPlanFor("SELECT * FROM %s WHERE v1>=0", row1, row2, row3, row4, row5, row6).uses(idx1);
            assertThatPlanFor("SELECT * FROM %s WHERE v1<=2", row1, row2, row3, row4, row5, row6).uses(idx1);
            assertThatPlanFor("SELECT * FROM %s WHERE v2=0", row1, row3, row5).uses(idx2);
            assertThatPlanFor("SELECT * FROM %s WHERE v2>0", row2, row4, row6).uses(idx2);
            assertThatPlanFor("SELECT * FROM %s WHERE v2<2", row1, row2, row3, row4, row5, row6).uses(idx2);
            assertThatPlanFor("SELECT * FROM %s WHERE v2>=0", row1, row2, row3, row4, row5, row6).uses(idx2);
            assertThatPlanFor("SELECT * FROM %s WHERE v2<=2", row1, row2, row3, row4, row5, row6).uses(idx2);
            assertThatPlanFor("SELECT * FROM %s WHERE v2>0 AND v2<=1", row2, row4, row6).uses(idx2);
            assertThatPlanFor("SELECT * FROM %s WHERE v2>=0 AND v2<1", row1, row3, row5).uses(idx2);
            assertThatPlanFor("SELECT * FROM %s WHERE v1>=0 AND v2>0", row2, row4, row6).usesAnyOf(idx1, idx2);
            assertThatPlanFor("SELECT * FROM %s WHERE v1>=0 AND v2>=0", row1, row2, row3, row4, row5, row6).usesAnyOf(idx1, idx2);
            assertThatPlanFor("SELECT * FROM %s WHERE v1=0 AND v2=0", row1).usesAnyOf(idx1, idx2);
            assertThatPlanFor("SELECT * FROM %s WHERE v1=0 AND v2>0", row2).usesAnyOf(idx1, idx2);
            assertThatPlanFor("SELECT * FROM %s WHERE v1=0 AND v2>=0", row1, row2).usesAnyOf(idx1, idx2);
            assertThatPlanFor("SELECT * FROM %s WHERE v1>0 AND v2=0", row3, row5).usesAnyOf(idx1, idx2);
            assertThatPlanFor("SELECT * FROM %s WHERE v1>0 AND v2>0", row4, row6).usesAnyOf(idx1, idx2);
            assertThatPlanFor("SELECT * FROM %s WHERE v1>0 AND v2>=0", row3, row4, row5, row6).usesAnyOf(idx1, idx2);
            assertThatPlanFor("SELECT * FROM %s WHERE v1>=0 AND v2=0", row1, row3, row5).usesAnyOf(idx1, idx2);

            // with restriction in one column only and hints including idx1
            assertThatPlanFor("SELECT * FROM %s WHERE v1=0 WITH included_indexes = {idx1}", row1, row2).uses(idx1);
            assertThatPlanFor("SELECT * FROM %s WHERE v1>0 WITH included_indexes = {idx1}", row3, row4, row5, row6).uses(idx1);
            assertThatPlanFor("SELECT * FROM %s WHERE v1<2 WITH included_indexes = {idx1}", row1, row2, row3, row4).uses(idx1);
            assertThatPlanFor("SELECT * FROM %s WHERE v1>=0 WITH included_indexes = {idx1}", row1, row2, row3, row4, row5, row6).uses(idx1);
            assertThatPlanFor("SELECT * FROM %s WHERE v1<=2 WITH included_indexes = {idx1}", row1, row2, row3, row4, row5, row6).uses(idx1);
            assertThatPlanFor("SELECT * FROM %s WHERE v1>0 AND v1<=2 WITH included_indexes = {idx1}", row3, row4, row5, row6).uses(idx1);
            assertThatPlanFor("SELECT * FROM %s WHERE v1>=0 AND v1<2 WITH included_indexes = {idx1}", row1, row2, row3, row4).uses(idx1);
            assertNonIncludableIndexError("SELECT * FROM %s WHERE v2=0 WITH included_indexes = {idx1}", idx1);
            assertNonIncludableIndexError("SELECT * FROM %s WHERE v2>0 WITH included_indexes = {idx1}", idx1);
            assertNonIncludableIndexError("SELECT * FROM %s WHERE v2<2 WITH included_indexes = {idx1}", idx1);
            assertNonIncludableIndexError("SELECT * FROM %s WHERE v2>=0 WITH included_indexes = {idx1}", idx1);
            assertNonIncludableIndexError("SELECT * FROM %s WHERE v2<=2 WITH included_indexes = {idx1}", idx1);
            assertNonIncludableIndexError("SELECT * FROM %s WHERE v2>0 AND v2<=1 WITH included_indexes = {idx1}", idx1);
            assertNonIncludableIndexError("SELECT * FROM %s WHERE v2>=0 AND v2<1 WITH included_indexes = {idx1}", idx1);

            // with restriction in one column only and hints including idx2
            assertNonIncludableIndexError("SELECT * FROM %s WHERE v1=0 WITH included_indexes = {idx2}", idx2);
            assertNonIncludableIndexError("SELECT * FROM %s WHERE v1!=0 WITH included_indexes = {idx2}", idx2);
            assertNonIncludableIndexError("SELECT * FROM %s WHERE v1>0 WITH included_indexes = {idx2}", idx2);
            assertNonIncludableIndexError("SELECT * FROM %s WHERE v1<2 WITH included_indexes = {idx2}", idx2);
            assertNonIncludableIndexError("SELECT * FROM %s WHERE v1>=0 WITH included_indexes = {idx2}", idx2);
            assertNonIncludableIndexError("SELECT * FROM %s WHERE v1<=2 WITH included_indexes = {idx2}", idx2);
            assertNonIncludableIndexError("SELECT * FROM %s WHERE v1>0 AND v1<=2 WITH included_indexes = {idx2}", idx2);
            assertNonIncludableIndexError("SELECT * FROM %s WHERE v1>=0 AND v1<2 WITH included_indexes = {idx2}", idx2);
            assertThatPlanFor("SELECT * FROM %s WHERE v2=0 WITH included_indexes = {idx2}", row1, row3, row5).uses(idx2);
            assertThatPlanFor("SELECT * FROM %s WHERE v2>0 WITH included_indexes = {idx2}", row2, row4, row6).uses(idx2);
            assertThatPlanFor("SELECT * FROM %s WHERE v2<2 WITH included_indexes = {idx2}", row1, row2, row3, row4, row5, row6).uses(idx2);
            assertThatPlanFor("SELECT * FROM %s WHERE v2>=0 WITH included_indexes = {idx2}", row1, row2, row3, row4, row5, row6).uses(idx2);
            assertThatPlanFor("SELECT * FROM %s WHERE v2<=2 WITH included_indexes = {idx2}", row1, row2, row3, row4, row5, row6).uses(idx2);
            assertThatPlanFor("SELECT * FROM %s WHERE v2>0 AND v2<=1 WITH included_indexes = {idx2}", row2, row4, row6).uses(idx2);
            assertThatPlanFor("SELECT * FROM %s WHERE v2>=0 AND v2<1 WITH included_indexes = {idx2}", row1, row3, row5).uses(idx2);

            // with restrictions in both columns and hints including either idx1 or idx2
            for (String idx : Arrays.asList(idx1, idx2))
            {
                assertThatPlanFor(format("SELECT * FROM %%s WHERE v1=0 AND v2=0 WITH included_indexes = {%s}", idx), row1).usesAtLeast(idx);
                assertThatPlanFor(format("SELECT * FROM %%s WHERE v1!=0 AND v2!=0 WITH included_indexes = {%s}", idx), row4, row6).usesAtLeast(idx);
                assertThatPlanFor(format("SELECT * FROM %%s WHERE v1=0 AND v2>0 WITH included_indexes = {%s}", idx), row2).usesAtLeast(idx);
                assertThatPlanFor(format("SELECT * FROM %%s WHERE v1=0 AND v2>=0 WITH included_indexes = {%s}", idx), row1, row2).usesAtLeast(idx);
                assertThatPlanFor(format("SELECT * FROM %%s WHERE v1>0 AND v2=0 WITH included_indexes = {%s}", idx), row3, row5).usesAtLeast(idx);
                assertThatPlanFor(format("SELECT * FROM %%s WHERE v1>0 AND v2>0 WITH included_indexes = {%s}", idx), row4, row6).usesAtLeast(idx);
                assertThatPlanFor(format("SELECT * FROM %%s WHERE v1>0 AND v2>=0 WITH included_indexes = {%s}", idx), row3, row4, row5, row6).usesAtLeast(idx);
                assertThatPlanFor(format("SELECT * FROM %%s WHERE v1>=0 AND v2=0 WITH included_indexes = {%s}", idx), row1, row3, row5).usesAtLeast(idx);
                assertThatPlanFor(format("SELECT * FROM %%s WHERE v1>=0 AND v2>0 WITH included_indexes = {%s}", idx), row2, row4, row6).usesAtLeast(idx);
                assertThatPlanFor(format("SELECT * FROM %%s WHERE v1>=0 AND v2>=0 WITH included_indexes = {%s}", idx), row1, row2, row3, row4, row5, row6).usesAtLeast(idx);
            }

            // with restriction in one column only and hints excluding idx1
            assertThatPlanFor("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH excluded_indexes = {idx1}", row1, row2).usesNone();
            assertThatPlanFor("SELECT * FROM %s WHERE v1!=0 ALLOW FILTERING WITH excluded_indexes = {idx1}", row3, row4, row5, row6).usesNone();
            assertThatPlanFor("SELECT * FROM %s WHERE v1>0 ALLOW FILTERING WITH excluded_indexes = {idx1}", row3, row4, row5, row6).usesNone();
            assertThatPlanFor("SELECT * FROM %s WHERE v1<2 ALLOW FILTERING WITH excluded_indexes = {idx1}", row1, row2, row3, row4).usesNone();
            assertThatPlanFor("SELECT * FROM %s WHERE v1>=0 ALLOW FILTERING WITH excluded_indexes = {idx1}", row1, row2, row3, row4, row5, row6).usesNone();
            assertThatPlanFor("SELECT * FROM %s WHERE v1<=2 ALLOW FILTERING WITH excluded_indexes = {idx1}", row1, row2, row3, row4, row5, row6).usesNone();
            assertThatPlanFor("SELECT * FROM %s WHERE v1>0 AND v1<=2 ALLOW FILTERING WITH excluded_indexes = {idx1}", row3, row4, row5, row6).usesNone();
            assertThatPlanFor("SELECT * FROM %s WHERE v1>=0 AND v1<2 ALLOW FILTERING WITH excluded_indexes = {idx1}", row1, row2, row3, row4).usesNone();
            assertThatPlanFor("SELECT * FROM %s WHERE v2=0 WITH excluded_indexes = {idx1}", row1, row3, row5).uses(idx2);
            assertThatPlanFor("SELECT * FROM %s WHERE v2>0 WITH excluded_indexes = {idx1}", row2, row4, row6).uses(idx2);
            assertThatPlanFor("SELECT * FROM %s WHERE v2<2 WITH excluded_indexes = {idx1}", row1, row2, row3, row4, row5, row6).uses(idx2);
            assertThatPlanFor("SELECT * FROM %s WHERE v2>=0 WITH excluded_indexes = {idx1}", row1, row2, row3, row4, row5, row6).uses(idx2);
            assertThatPlanFor("SELECT * FROM %s WHERE v2<=2 WITH excluded_indexes = {idx1}", row1, row2, row3, row4, row5, row6).uses(idx2);
            assertThatPlanFor("SELECT * FROM %s WHERE v2>0 AND v2<=1", row2, row4, row6).uses(idx2);
            assertThatPlanFor("SELECT * FROM %s WHERE v2>=0 AND v2<1", row1, row3, row5).uses(idx2);

            // with restriction in one column only and hints excluding idx2
            assertThatPlanFor("SELECT * FROM %s WHERE v1=0 WITH excluded_indexes = {idx2}", row1, row2).uses(idx1);
            assertThatPlanFor("SELECT * FROM %s WHERE v1!=0 WITH excluded_indexes = {idx2}", row3, row4, row5, row6).uses(idx1);
            assertThatPlanFor("SELECT * FROM %s WHERE v1>0 WITH excluded_indexes = {idx2}", row3, row4, row5, row6).uses(idx1);
            assertThatPlanFor("SELECT * FROM %s WHERE v1<2 WITH excluded_indexes = {idx2}", row1, row2, row3, row4).uses(idx1);
            assertThatPlanFor("SELECT * FROM %s WHERE v1>=0 WITH excluded_indexes = {idx2}", row1, row2, row3, row4, row5, row6).uses(idx1);
            assertThatPlanFor("SELECT * FROM %s WHERE v1<=2 WITH excluded_indexes = {idx2}", row1, row2, row3, row4, row5, row6).uses(idx1);
            assertThatPlanFor("SELECT * FROM %s WHERE v1>0 AND v1<=2 WITH excluded_indexes = {idx2}", row3, row4, row5, row6).uses(idx1);
            assertThatPlanFor("SELECT * FROM %s WHERE v1>=0 AND v1<2 WITH excluded_indexes = {idx2}", row1, row2, row3, row4).uses(idx1);
            assertThatPlanFor("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING WITH excluded_indexes = {idx2}", row1, row3, row5).usesNone();
            assertThatPlanFor("SELECT * FROM %s WHERE v2>0 ALLOW FILTERING WITH excluded_indexes = {idx2}", row2, row4, row6).usesNone();
            assertThatPlanFor("SELECT * FROM %s WHERE v2<2 ALLOW FILTERING WITH excluded_indexes = {idx2}", row1, row2, row3, row4, row5, row6).usesNone();
            assertThatPlanFor("SELECT * FROM %s WHERE v2>=0 ALLOW FILTERING WITH excluded_indexes = {idx2}", row1, row2, row3, row4, row5, row6).usesNone();
            assertThatPlanFor("SELECT * FROM %s WHERE v2<=2 ALLOW FILTERING WITH excluded_indexes = {idx2}", row1, row2, row3, row4, row5, row6).usesNone();
            assertThatPlanFor("SELECT * FROM %s WHERE v2>0 AND v2<=1 ALLOW FILTERING WITH excluded_indexes = {idx2}", row2, row4, row6).usesNone();
            assertThatPlanFor("SELECT * FROM %s WHERE v2>=0 AND v2<1 ALLOW FILTERING WITH excluded_indexes = {idx2}", row1, row3, row5).usesNone();

            // with restrictions in both columns and hints excluding either idx1 or idx2
            for (String one : Arrays.asList(idx1, idx2))
            {
                String other = one.equals(idx1) ? idx2 : idx1;
                assertThatPlanFor(format("SELECT * FROM %%s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH excluded_indexes = {%s}", one), row1).uses(other);
                assertThatPlanFor(format("SELECT * FROM %%s WHERE v1!=0 AND v2!=0 ALLOW FILTERING WITH excluded_indexes = {%s}", one), row4, row6).uses(other);
                assertThatPlanFor(format("SELECT * FROM %%s WHERE v1=0 AND v2>0 ALLOW FILTERING WITH excluded_indexes = {%s}", one), row2).uses(other);
                assertThatPlanFor(format("SELECT * FROM %%s WHERE v1=0 AND v2>=0 ALLOW FILTERING WITH excluded_indexes = {%s}", one), row1, row2).uses(other);
                assertThatPlanFor(format("SELECT * FROM %%s WHERE v1>0 AND v2=0 ALLOW FILTERING WITH excluded_indexes = {%s}", one), row3, row5).uses(other);
                assertThatPlanFor(format("SELECT * FROM %%s WHERE v1>0 AND v2>0 ALLOW FILTERING WITH excluded_indexes = {%s}", one), row4, row6).uses(other);
                assertThatPlanFor(format("SELECT * FROM %%s WHERE v1>0 AND v2>=0 ALLOW FILTERING WITH excluded_indexes = {%s}", one), row3, row4, row5, row6).uses(other);
                assertThatPlanFor(format("SELECT * FROM %%s WHERE v1>=0 AND v2=0 ALLOW FILTERING WITH excluded_indexes = {%s}", one), row1, row3, row5).uses(other);
                assertThatPlanFor(format("SELECT * FROM %%s WHERE v1>=0 AND v2>0 ALLOW FILTERING WITH excluded_indexes = {%s}", one), row2, row4, row6).uses(other);
                assertThatPlanFor(format("SELECT * FROM %%s WHERE v1>=0 AND v2>=0 ALLOW FILTERING WITH excluded_indexes = {%s}", one), row1, row2, row3, row4, row5, row6).uses(other);
            }
        });
    }

    @Test
    public void testQueryPlanningWithRestrictedButUnusableIndex() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v1 int, v2 int, v3 int)");
        createIndex("CREATE CUSTOM INDEX idx1 ON %s(v1) USING 'StorageAttachedIndex'");
        String idx3 = createIndex("CREATE CUSTOM INDEX idx3 ON %s(v3) USING 'StorageAttachedIndex'");

        String insert = "INSERT INTO %s (k, v1, v2, v3) VALUES (?, ?, ?, ?)";
        Object[] row1 = row(1, 0, 0, 9);
        Object[] row2 = row(2, 0, 1, 9);
        Object[] row3 = row(3, 1, 0, 9);
        Object[] row4 = row(4, 1, 1, 9);
        execute(insert, row1);
        execute(insert, row2);
        execute(insert, row3);
        execute(insert, row4);

        beforeAndAfterFlush(() -> {

            String query = "SELECT * FROM %s WHERE v1=0 OR v2=0 ALLOW FILTERING";
            assertThatPlanFor(query, row1, row2, row3).usesNone();
            assertNonIncludableIndexError(query + " WITH included_indexes={idx1}");
            assertThatPlanFor(query + " WITH excluded_indexes={idx1}", row1, row2, row3).usesNone();

            query = "SELECT * FROM %s WHERE (v1=0 OR v2=0) AND v3=9 ALLOW FILTERING";
            assertThatPlanFor(query, row1, row2, row3).uses(idx3);
            assertNonIncludableIndexError(query + " WITH included_indexes={idx1}");
            assertThatPlanFor(query + " WITH excluded_indexes={idx1}", row1, row2, row3).uses(idx3);
        });
    }

    /**
     * Tests that there will be an error when the included indexes exceed the intersection clause limit.
     */
    @Test
    public void testIntersectionClauseLimit() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v1 int, v2 int, v3 int, v4 int)");
        String idx1 = createIndex("CREATE CUSTOM INDEX idx1 ON %s(v1) USING 'StorageAttachedIndex'");
        String idx2 = createIndex("CREATE CUSTOM INDEX idx2 ON %s(v2) USING 'StorageAttachedIndex'");
        String idx3 = createIndex("CREATE CUSTOM INDEX idx3 ON %s(v3) USING 'StorageAttachedIndex'");
        String idx4 = createIndex("CREATE CUSTOM INDEX idx4 ON %s(v4) USING 'StorageAttachedIndex'");

        String insert = "INSERT INTO %s (k, v1, v2, v3, v4) VALUES (?, ?, ?, ?, ?)";
        Object[] row1 = row(1, 0, 0, 0, 1);
        Object[] row2 = row(2, 0, 1, 0, 2);
        Object[] row3 = row(3, 1, 0, 0, 3);
        Object[] row4 = row(4, 1, 1, 0, 4);
        execute(insert, row1);
        execute(insert, row2);
        execute(insert, row3);
        execute(insert, row4);

        int defaultIntersectionClauseLimit = CassandraRelevantProperties.SAI_INTERSECTION_CLAUSE_LIMIT.getInt();
        try
        {
            beforeAndAfterFlush(() -> {
                String query = "SELECT * FROM %s WHERE v1=0 AND v2=0 AND v3 = 0";

                CassandraRelevantProperties.SAI_INTERSECTION_CLAUSE_LIMIT.setInt(3);
                assertThatPlanFor(query, row1).usesAnyOf(idx1, idx2, idx3);
                assertThatPlanFor(query + " WITH included_indexes={idx1}", row1).uses(idx1);
                assertThatPlanFor(query + " WITH included_indexes={idx1, idx2}", row1).uses(idx1, idx2);
                assertThatPlanFor(query + " WITH included_indexes={idx1, idx2, idx3}", row1).uses(idx1, idx2, idx3);

                CassandraRelevantProperties.SAI_INTERSECTION_CLAUSE_LIMIT.setInt(2);
                assertThatPlanFor(query, row1).usesAnyOf(idx1, idx2, idx3);
                assertThatPlanFor(query + " WITH included_indexes={idx1}", row1).uses(idx1);
                assertThatPlanFor(query + " WITH included_indexes={idx1, idx2}", row1).uses(idx1, idx2);
                assertHintsExceedIntersectionClauseLimit(query + " WITH included_indexes={idx1, idx2, idx3}");

                CassandraRelevantProperties.SAI_INTERSECTION_CLAUSE_LIMIT.setInt(1);
                assertThatPlanFor(query, row1).usesAnyOf(idx1, idx2, idx3);
                assertThatPlanFor(query + " WITH included_indexes={idx1}", row1).uses(idx1);
                assertHintsExceedIntersectionClauseLimit(query + " WITH included_indexes={idx1, idx2}");
                assertHintsExceedIntersectionClauseLimit(query + " WITH included_indexes={idx1, idx2, idx3}");

                // test with an OR clause, so the intersection is nested
                query = "SELECT * FROM %s WHERE (v1=0 AND v2=0 AND v3 = 0) OR v4 = 0";

                CassandraRelevantProperties.SAI_INTERSECTION_CLAUSE_LIMIT.setInt(3);
                assertThatPlanFor(query, row1).usesAnyOf(idx1, idx2, idx3, idx4).usesAtLeast(idx4);
                assertThatPlanFor(query + " WITH included_indexes={idx1}", row1).uses(idx1, idx4);
                assertThatPlanFor(query + " WITH included_indexes={idx1, idx2}", row1).uses(idx1, idx2, idx4);
                assertThatPlanFor(query + " WITH included_indexes={idx1, idx2, idx3}", row1).uses(idx1, idx2, idx3, idx4);

                CassandraRelevantProperties.SAI_INTERSECTION_CLAUSE_LIMIT.setInt(2);
                assertThatPlanFor(query, row1).usesAnyOf(idx1, idx2, idx3, idx4).usesAtLeast(idx4);
                assertThatPlanFor(query + " WITH included_indexes={idx1}", row1).uses(idx1, idx4);
                assertThatPlanFor(query + " WITH included_indexes={idx1, idx2}", row1).uses(idx1, idx2, idx4);
                assertHintsExceedIntersectionClauseLimit(query + " WITH included_indexes={idx1, idx2, idx3}");

                CassandraRelevantProperties.SAI_INTERSECTION_CLAUSE_LIMIT.setInt(1);
                assertThatPlanFor(query, row1).usesAnyOf(idx1, idx2, idx3, idx4).usesAtLeast(idx4);
                assertThatPlanFor(query + " WITH included_indexes={idx1}", row1).uses(idx1, idx4);
                assertHintsExceedIntersectionClauseLimit(query + " WITH included_indexes={idx1, idx2}");
                assertHintsExceedIntersectionClauseLimit(query + " WITH included_indexes={idx1, idx2, idx3}");
            });
        }
        finally
        {
            CassandraRelevantProperties.SAI_INTERSECTION_CLAUSE_LIMIT.setInt(defaultIntersectionClauseLimit);
        }
    }

    @Test
    public void testVector() throws Throwable
    {
        Assume.assumeTrue(version.onOrAfter(Version.JVECTOR_EARLIEST));

        createTable("CREATE TABLE %s (k int PRIMARY KEY, v vector<float, 2>)");
        String idx = createIndex("CREATE CUSTOM INDEX idx ON %s(v) USING 'StorageAttachedIndex'");

        String insert = "INSERT INTO %s (k, v) VALUES (?, ?)";
        execute(insert, row(1, vector(0.1f, 0.2f)));

        beforeAndAfterFlush(() -> {

            // with ANN queries
            String query = "SELECT k FROM %s ORDER BY v ANN OF [0.1, 0.2] LIMIT 10";
            assertThatPlanFor(query, row(1)).uses(idx);
            assertThatPlanFor(query + " WITH included_indexes={idx}", row(1)).uses(idx);
            assertOrderingNeedsIndex(query + " WITH excluded_indexes={idx}", "v");

            // with an unsupported eq query, that can be unshaded with excluded_indexes and ALLOW FILTERING
            query = "SELECT k FROM %s WHERE v = [0.1, 0.2] LIMIT 10";
            assertUnsupportVectorOperator(query);
            assertUnsupportVectorOperator(query + " WITH included_indexes={idx}");
            assertNeedsAllowFiltering(query + " WITH excluded_indexes={idx}");
            assertThatPlanFor(query + " ALLOW FILTERING WITH excluded_indexes={idx}", row(1)).usesNone();
        });
    }

    private void assertNeedsAllowFiltering(String query)
    {
        Assertions.assertThatThrownBy(() -> execute(query))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessageContaining(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE);
    }

    private void assertHintsExceedIntersectionClauseLimit(String query)
    {
        Assertions.assertThatThrownBy(() -> execute(query))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessageContaining(String.format(StorageAttachedIndex.HINTS_EXCEED_INTERSECTION_CLAUSE_LIMIT_ERROR,
                                                      CassandraRelevantProperties.SAI_INTERSECTION_CLAUSE_LIMIT.getInt(),
                                                      CassandraRelevantProperties.SAI_INTERSECTION_CLAUSE_LIMIT.name()));
    }

    public void assertNonIncludableIndexError(String query, String... indexes)
    {
        Assertions.assertThatThrownBy(() -> execute(query))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessageContaining(String.format(IndexHints.NON_INCLUDABLE_INDEX_ERROR, String.join(",", indexes)));
    }

    private void assertOrderingNeedsIndex(String query, String column)
    {
        Assertions.assertThatThrownBy(() -> execute(query))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessageContaining(String.format(StatementRestrictions.NON_CLUSTER_ORDERING_REQUIRES_INDEX_MESSAGE, column));
    }

    private void assertUnsupportVectorOperator(String query)
    {
        Assertions.assertThatThrownBy(() -> execute(query))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessageContaining(StatementRestrictions.VECTOR_INDEXES_UNSUPPORTED_OP_MESSAGE);
    }

    private void assertMatchNeedsIndex(String query, String column, String value)
    {
        Assertions.assertThatThrownBy(() -> execute(query))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessageContaining(String.format(StatementRestrictions.RESTRICTION_REQUIRES_INDEX_MESSAGE,
                                                      ':',
                                                      String.format("%s : '%s'", column, value)));
    }

    private void assertBM25RequiresAnAnalyzedIndex(String query, String column)
    {
        Assertions.assertThatThrownBy(() -> execute(query))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessageContaining(format(StatementRestrictions.BM25_ORDERING_REQUIRES_ANALYZED_INDEX_MESSAGE, column));
    }
}
