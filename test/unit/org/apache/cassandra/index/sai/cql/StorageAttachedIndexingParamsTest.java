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

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.plan.QueryController;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.net.MessagingService;

import static org.apache.cassandra.inject.InvokePointBuilder.newInvokePoint;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class StorageAttachedIndexingParamsTest extends SAITester.Versioned
{
    @BeforeClass
    public static void setUpClass()
    {
        CassandraRelevantProperties.DS_CURRENT_MESSAGING_VERSION.setInt(MessagingService.VERSION_DS_12);
        SAITester.setUpClass();
    }

    /**
     * Tests that query optimization can be enabled/disabled via table storage_attached_indexing.
     * When optimization is enabled, indexes with poor selectivity should not be used.
     * When optimization is disabled, all applicable indexes should be used.
     */
    @Test
    public void testQueryOptimization() throws Throwable
    {
        // Create table with query optimization enabled (level 1)
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v1 int, v2 int) " +
                    "WITH storage_attached_indexing = {'query_optimization_level': 1}");
        String idx1 = createIndex("CREATE CUSTOM INDEX idx1 ON %s(v1) USING 'StorageAttachedIndex'");
        String idx2 = createIndex("CREATE CUSTOM INDEX idx2 ON %s(v2) USING 'StorageAttachedIndex'");

        // Insert data where v1 has good selectivity (rare value) and v2 has poor selectivity (common value)
        int numRows = 1000;
        for (int i = 0; i < numRows; i++)
        {
            execute("INSERT INTO %s (k, v1, v2) VALUES (?, ?, ?)", i, i, 0);
        }

        // Flush so we get more accurate estimates and the clause on v2=0 gets properly estimated to match 100% rows
        // so idx2 would be dropped by the optimizer as not useful.
        flush();

        String query = "SELECT * FROM %s WHERE v1=0 AND v2=0";

        // With optimization enabled (level 1), only the selective index (idx1) should be used
        // The optimizer should skip idx2 because v2=0 matches all rows (poor selectivity)
        assertThatPlanFor(query, row(0, 0, 0)).uses(idx1);

        // Index hints should still allow forcing both indexes
        assertThatPlanFor(query + " WITH included_indexes={idx1, idx2}", row(0, 0, 0)).uses(idx1, idx2);

        // Alter table to disable query optimization (level 0)
        alterTable("ALTER TABLE %s WITH storage_attached_indexing = {'query_optimization_level': 0}");

        // With optimization disabled (level 0), both indexes should be used
        // even though idx2 has poor selectivity
        assertThatPlanFor(query, row(0, 0, 0)).uses(idx1, idx2);

        // Index hints should still work to exclude indexes even when optimizer is turned off
        assertThatPlanFor(query + " ALLOW FILTERING WITH excluded_indexes={idx2}", row(0, 0, 0)).uses(idx1);
    }

    /**
     * Tests that intersection clause limit can be controlled through table level options
     */
    @Test
    public void testIntersectionClauseLimit() throws Throwable
    {
        // Test with intersection clause limit set to 3 via table storage_attached_indexing
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v1 int, v2 int, v3 int, v4 int) " +
                    "WITH storage_attached_indexing = {'intersection_clause_limit': 3}");
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

        // disable query optimization so we can test the intersection clause limit in a more predictable way
        disableQueryOptimization();

        beforeAndAfterFlush(() -> {
            String query = "SELECT * FROM %s WHERE v1=0 AND v2=0 AND v3 = 0";

            // With limit 3, all three indexes should be used
            assertThatPlanFor(query, row1).usesAnyOf(idx1, idx2, idx3);
            assertThatPlanFor(query + " WITH included_indexes={idx1}", row1).usesAtLeast(idx1).uses(3);
            assertThatPlanFor(query + " WITH included_indexes={idx1, idx2}", row1).usesAtLeast(idx1, idx2).uses(3);
            assertThatPlanFor(query + " WITH included_indexes={idx1, idx2, idx3}", row1).uses(idx1, idx2, idx3);

            // test with an OR clause, so the intersection is nested
            query = "SELECT * FROM %s WHERE (v1=0 AND v2=0 AND v3 = 0) OR v4 = 0";
            assertThatPlanFor(query, row1).uses(idx1, idx2, idx3, idx4);
            assertThatPlanFor(query + " WITH included_indexes={idx1}", row1).uses(idx1, idx2, idx3, idx4);
            assertThatPlanFor(query + " WITH included_indexes={idx1, idx2}", row1).uses(idx1, idx2, idx3, idx4);
            assertThatPlanFor(query + " WITH included_indexes={idx1, idx2, idx3}", row1).uses(idx1, idx2, idx3, idx4);
        });

        // Alter the table to change the intersection clause limit to 2
        alterTable("ALTER TABLE %s WITH storage_attached_indexing = {'intersection_clause_limit': 2}");

        beforeAndAfterFlush(() -> {
            String query = "SELECT * FROM %s WHERE v1=0 AND v2=0 AND v3 = 0";

            // With limit 2, only two indexes should be used unless hints override
            assertThatPlanFor(query, row1).usesAnyOf(idx1, idx2, idx3);
            assertThatPlanFor(query + " WITH included_indexes={idx1}", row1).usesAtLeast(idx1).uses(2);
            assertThatPlanFor(query + " WITH included_indexes={idx1, idx2}", row1).uses(idx1, idx2);
            assertThatPlanFor(query + " WITH included_indexes={idx1, idx2, idx3}", row1).uses(idx1, idx2, idx3);

            // test with an OR clause, so the intersection is nested
            query = "SELECT * FROM %s WHERE (v1=0 AND v2=0 AND v3 = 0) OR v4 = 0";
            assertThatPlanFor(query, row1).usesAtLeast(idx4).uses(3);
            assertThatPlanFor(query + " WITH included_indexes={idx1}", row1).usesAtLeast(idx1, idx4).uses(3);
            assertThatPlanFor(query + " WITH included_indexes={idx1, idx2}", row1).uses(idx1, idx2, idx4);
            assertThatPlanFor(query + " WITH included_indexes={idx1, idx2, idx3}", row1).uses(idx1, idx2, idx3, idx4);
        });

        // Alter the table to change the intersection clause limit to 1
        alterTable("ALTER TABLE %s WITH storage_attached_indexing = {'intersection_clause_limit': 1}");

        beforeAndAfterFlush(() -> {
            String query = "SELECT * FROM %s WHERE v1=0 AND v2=0 AND v3 = 0";

            // With limit 1, only one index should be used unless hints override
            assertThatPlanFor(query, row1).usesAnyOf(idx1, idx2, idx3);
            assertThatPlanFor(query + " WITH included_indexes={idx1}", row1).uses(idx1);
            assertThatPlanFor(query + " WITH included_indexes={idx1, idx2}", row1).uses(idx1, idx2);
            assertThatPlanFor(query + " WITH included_indexes={idx1, idx2, idx3}", row1).uses(idx1, idx2, idx3);

            // test with an OR clause, so the intersection is nested
            query = "SELECT * FROM %s WHERE (v1=0 AND v2=0 AND v3 = 0) OR v4 = 0";
            assertThatPlanFor(query, row1).usesAtLeast(idx4).uses(2);
            assertThatPlanFor(query + " WITH included_indexes={idx1}", row1).uses(idx1, idx4);
            assertThatPlanFor(query + " WITH included_indexes={idx1, idx2}", row1).uses(idx1, idx2, idx4);
            assertThatPlanFor(query + " WITH included_indexes={idx1, idx2, idx3}", row1).uses(idx1, idx2, idx3, idx4);
        });
    }

    /**
     * Tests that use_term_statistics can be enabled/disabled via table storage_attached_indexing.
     * When enabled, the query planner should use histogram-based estimation (for indexes >= EB version).
     * When disabled, it should use index-based estimation.
     */
    @Test
    public void testUseTermStatistics() throws Throwable
    {
        // Create counters to track which estimation method is called
        Injections.Counter histogramCounter = Injections.newCounter("HistogramEstimationCounter")
                                                        .add(newInvokePoint().onClass(QueryController.class)
                                                                             .onMethod("estimateMatchingRowCountUsingHistograms"))
                                                        .build();

        Injections.Counter indexCounter = Injections.newCounter("IndexEstimationCounter")
                                                    .add(newInvokePoint().onClass(QueryController.class)
                                                                         .onMethod("estimateMatchingRowCountUsingIndex"))
                                                    .build();
        try
        {
            Injections.inject(histogramCounter, indexCounter);

            // Create table with term statistics enabled (default)
            createTable("CREATE TABLE %s (k int PRIMARY KEY, v int) " +
                        "WITH storage_attached_indexing = {'use_term_statistics': 'true'}");
            createIndex("CREATE CUSTOM INDEX idx ON %s(v) USING 'StorageAttachedIndex'");

            // Insert some data
            for (int i = 0; i < 100; i++)
            {
                execute("INSERT INTO %s (k, v) VALUES (?, ?)", i, i % 10);
            }

            flush();

            // With term statistics enabled, histogram estimation should be used
            assertEstimationMethod(histogramCounter, indexCounter, version.onDiskFormat().indexFeatureSet().hasTermsHistogram());

            // Alter table to disable term statistics
            alterTable("ALTER TABLE %s WITH storage_attached_indexing = {'use_term_statistics': 'false'}");

            // With term statistics disabled, index estimation should be used
            assertEstimationMethod(histogramCounter, indexCounter, false);
        }
        finally
        {
            histogramCounter.disable();
            indexCounter.disable();
        }
    }

    /**
     * Helper method to verify which estimation method is called based on the useHistogram flag.
     *
     * @param histogramCounter counter for histogram-based estimation calls
     * @param indexCounter counter for index-based estimation calls
     * @param useHistogram true if histogram estimation should be used, false for index estimation
     */
    private void assertEstimationMethod(Injections.Counter histogramCounter,
                                        Injections.Counter indexCounter,
                                        boolean useHistogram) throws Throwable
    {
        // Reset counters before testing
        histogramCounter.reset();
        indexCounter.reset();

        // Execute a range query that will trigger estimation
        execute("SELECT * FROM %s WHERE v > 5");

        if (useHistogram)
        {
            assertTrue("estimateMatchingRowCountUsingHistograms should be called when use_term_statistics is true",
                       histogramCounter.get() > 0);
            assertEquals("estimateMatchingRowCountUsingIndex should not be called when use_term_statistics is true",
                         0, indexCounter.get());
        }
        else
        {
            assertEquals("estimateMatchingRowCountUsingHistograms should not be called when use_term_statistics is false",
                         0, histogramCounter.get());
            assertTrue("estimateMatchingRowCountUsingIndex should be called when use_term_statistics is false",
                       indexCounter.get() > 0);
        }
    }
}