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
package org.apache.cassandra.index.sai.metrics;

import java.util.concurrent.ThreadLocalRandom;
import javax.management.InstanceNotFoundException;
import javax.management.JMX;
import javax.management.ObjectName;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.datastax.driver.core.ResultSet;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.index.sai.metrics.TableQueryMetrics.QueryKind;
import org.apache.cassandra.index.sai.plan.QueryController;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;

import static org.apache.cassandra.index.sai.metrics.TableQueryMetrics.AbstractQueryMetrics.makeName;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class QueryMetricsTest extends AbstractMetricsTest
{
    private static final String CREATE_TABLE_TEMPLATE = "CREATE TABLE %s.%s (id1 TEXT PRIMARY KEY, v1 INT, v2 TEXT) WITH compaction = " +
                                                        "{'class' : 'SizeTieredCompactionStrategy', 'enabled' : false }";
    private static final String CREATE_INDEX_TEMPLATE = "CREATE CUSTOM INDEX IF NOT EXISTS %s ON %s.%s(%s) USING 'StorageAttachedIndex'";

    private static final String TABLE_QUERY_METRIC_TYPE = TableQueryMetrics.PerTable.METRIC_TYPE;
    private static final String TABLE_SP_FILTER_QUERY_METRIC_TYPE = makeName(TABLE_QUERY_METRIC_TYPE, QueryKind.SP_FILTER_ONLY);
    private static final String TABLE_MP_FILTER_QUERY_METRIC_TYPE = makeName(TABLE_QUERY_METRIC_TYPE, QueryKind.MP_FILTER_ONLY);
    private static final String TABLE_SP_TOPK_QUERY_METRIC_TYPE = makeName(TABLE_QUERY_METRIC_TYPE, QueryKind.SP_TOPK_ONLY);
    private static final String TABLE_MP_TOPK_QUERY_METRIC_TYPE = makeName(TABLE_QUERY_METRIC_TYPE, QueryKind.MP_TOPK_ONLY);
    private static final String TABLE_SP_HYBRID_QUERY_METRIC_TYPE = makeName(TABLE_QUERY_METRIC_TYPE, QueryKind.SP_HYBRID);
    private static final String TABLE_MP_HYBRID_QUERY_METRIC_TYPE = makeName(TABLE_QUERY_METRIC_TYPE, QueryKind.MP_HYBRID);

    private static final String PER_QUERY_METRIC_TYPE = TableQueryMetrics.PerQuery.METRIC_TYPE;
    private static final String PER_SP_FILTER_QUERY_METRIC_TYPE = makeName(PER_QUERY_METRIC_TYPE, QueryKind.SP_FILTER_ONLY);
    private static final String PER_MP_FILTER_QUERY_METRIC_TYPE = makeName(PER_QUERY_METRIC_TYPE, QueryKind.MP_FILTER_ONLY);
    private static final String PER_SP_TOPK_QUERY_METRIC_TYPE = makeName(PER_QUERY_METRIC_TYPE, QueryKind.SP_TOPK_ONLY);
    private static final String PER_MP_TOPK_QUERY_METRIC_TYPE = makeName(PER_QUERY_METRIC_TYPE, QueryKind.MP_TOPK_ONLY);
    private static final String PER_SP_HYBRID_QUERY_METRIC_TYPE = makeName(PER_QUERY_METRIC_TYPE, QueryKind.SP_HYBRID);
    private static final String PER_MP_HYBRID_QUERY_METRIC_TYPE = makeName(PER_QUERY_METRIC_TYPE, QueryKind.MP_HYBRID);

    private static final String GLOBAL_METRIC_TYPE = "ColumnQueryMetrics";

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void testSameIndexNameAcrossKeyspaces()
    {
        String table = "test_same_index_name_across_keyspaces";
        String index = "test_same_index_name_across_keyspaces_index";

        String keyspace1 = createKeyspace(CREATE_KEYSPACE_TEMPLATE);
        String keyspace2 = createKeyspace(CREATE_KEYSPACE_TEMPLATE);

        createTable(String.format(CREATE_TABLE_TEMPLATE, keyspace1, table));
        createIndex(String.format(CREATE_INDEX_TEMPLATE, index, keyspace1, table, "v1"));

        createTable(String.format(CREATE_TABLE_TEMPLATE, keyspace2, table));
        createIndex(String.format(CREATE_INDEX_TEMPLATE, index, keyspace2, table, "v1"));

        execute("INSERT INTO " + keyspace1 + "." + table + " (id1, v1, v2) VALUES ('0', 0, '0')");

        ResultSet rows = executeNet("SELECT id1 FROM " + keyspace1 + "." + table + " WHERE v1 = 0");
        assertEquals(1, rows.all().size());

        assertEquals(1L, getTableQueryMetrics(keyspace1, table, "TotalQueriesCompleted"));
        assertEquals(1L, getPerQueryMetrics(keyspace1, table, "PostFilteringReadLatency"));
        assertEquals(0L, getTableQueryMetrics(keyspace2, table, "TotalQueriesCompleted"));
        assertEquals(0L, getPerQueryMetrics(keyspace2, table, "PostFilteringReadLatency"));

        execute("INSERT INTO " + keyspace2 + "." + table + " (id1, v1, v2) VALUES ('0', 0, '0')");
        execute("INSERT INTO " + keyspace2 + "." + table + " (id1, v1, v2) VALUES ('1', 1, '1')");

        rows = executeNet("SELECT id1 FROM " + keyspace1 + "." + table + " WHERE v1 = 0");
        assertEquals(1, rows.all().size());

        rows = executeNet("SELECT id1 FROM " + keyspace2 + "." + table + " WHERE v1 = 1");
        assertEquals(1, rows.all().size());

        assertEquals(2L, getTableQueryMetrics(keyspace1, table, "TotalQueriesCompleted"));
        assertEquals(1L, getTableQueryMetrics(keyspace2, table, "TotalQueriesCompleted"));
        assertEquals(2L, getPerQueryMetrics(keyspace1, table, "PostFilteringReadLatency"));
        assertEquals(1L, getPerQueryMetrics(keyspace2, table, "PostFilteringReadLatency"));
    }

    @Test
    public void testMetricRelease()
    {
        String table = "test_metric_release";
        String index = "test_metric_release_index";

        String keyspace = createKeyspace(CREATE_KEYSPACE_TEMPLATE);

        createTable(String.format(CREATE_TABLE_TEMPLATE, keyspace, table));
        createIndex(String.format(CREATE_INDEX_TEMPLATE, index, keyspace, table, "v1"));

        execute("INSERT INTO " + keyspace + "." + table + " (id1, v1, v2) VALUES ('0', 0, '0')");

        ResultSet rows = executeNet("SELECT id1 FROM " + keyspace + "." + table + " WHERE v1 = 0");
        assertEquals(1, rows.all().size());

        assertEquals(1L, getTableQueryMetrics(keyspace, table, "TotalQueriesCompleted"));

        // Even if we drop the last index on the table, we should finally fail to find table-level metrics:
        dropIndex(String.format("DROP INDEX %s." + index, keyspace));
        assertThatThrownBy(() -> getTableQueryMetrics(keyspace, table, "TotalIndexCount")).hasRootCauseInstanceOf(InstanceNotFoundException.class);

        // When the whole table is dropped, we should finally fail to find table-level metrics:
        dropTable(String.format("DROP TABLE %s." + table, keyspace));
        assertThatThrownBy(() -> getTableQueryMetrics(keyspace, table, "TotalQueriesCompleted")).hasRootCauseInstanceOf(InstanceNotFoundException.class);
    }

    @Test
    public void testIndexQueryWithPartitionKey()
    {
        String table = "test_range_key_type_with_index";
        String index = "test_range_key_type_with_index_index";

        String keyspace = createKeyspace(CREATE_KEYSPACE_TEMPLATE);

        createTable(String.format(CREATE_TABLE_TEMPLATE, keyspace, table));
        createIndex(String.format(CREATE_INDEX_TEMPLATE, index, keyspace, table, "v1"));

        int rowsWrittenPerSSTable = 10;
        int numberOfSSTable = 5;
        int rowsWritten = 0;
        int i = 0;
        for (int j = 0; j < numberOfSSTable; j++)
        {
            rowsWritten += rowsWrittenPerSSTable;
            for (; i < rowsWritten; i++)
            {
                execute("INSERT INTO " + keyspace + "." + table + " (id1, v1, v2) VALUES (?, ?, '0')", Integer.toString(i), i);
            }
            flush(keyspace, table);
        }

        ResultSet rows2 = executeNet("SELECT id1 FROM " + keyspace + "." + table + " WHERE id1 = '36' and v1 < 51");
        assertEquals(1, rows2.all().size());

        ResultSet rows3 = executeNet("SELECT id1 FROM " + keyspace + "." + table + " WHERE id1 = '49' and v1 < 51 ALLOW FILTERING");
        assertEquals(1, rows3.all().size());

        ResultSet rows4 = executeNet("SELECT id1 FROM " + keyspace + "." + table + " WHERE id1 = '21' and v1 >= 0 and v1 < 51 ALLOW FILTERING");
        assertEquals(1, rows4.all().size());

        ResultSet rows5 = executeNet("SELECT id1 FROM " + keyspace + "." + table + " WHERE id1 = '35' and v1 > 0");
        assertEquals(1, rows5.all().size());

        ResultSet rows6 = executeNet("SELECT id1 FROM " + keyspace + "." + table + " WHERE v1 > 0 and id1 = '20'");
        assertEquals(1, rows6.all().size());

        ObjectName oName = objectNameNoIndex("SSTableIndexesHit", keyspace, table, PER_QUERY_METRIC_TYPE);
        CassandraMetricsRegistry.JmxHistogramMBean o = JMX.newMBeanProxy(jmxConnection, oName, CassandraMetricsRegistry.JmxHistogramMBean.class);

        assertTrue(o.getMean() < 2);
    }

    @Test
    public void testKDTreeQueryMetricsWithSingleIndex()
    {
        String table = "test_metrics_through_write_lifecycle";
        String index = "test_metrics_through_write_lifecycle_index";

        String keyspace = createKeyspace(CREATE_KEYSPACE_TEMPLATE);

        createTable(String.format(CREATE_TABLE_TEMPLATE, keyspace, table));
        createIndex(String.format(CREATE_INDEX_TEMPLATE, index, keyspace, table, "v1"));

        int resultCounter = 0;
        int queryCounter = 0;

        int rowsWritten = 10;

        for (int i = 0; i < rowsWritten; i++)
        {
            execute("INSERT INTO " + keyspace + "." + table + " (id1, v1, v2) VALUES (?, ?, '0')", Integer.toString(i), i);
        }

        flush(keyspace, table);
        compact(keyspace, table);
        waitForIndexCompaction(keyspace, table, index);

        waitForTableIndexesQueryable(keyspace, table);

        ResultSet rows = executeNet("SELECT id1 FROM " + keyspace + "." + table + " WHERE v1 >= 0");

        int actualRows = rows.all().size();
        assertEquals(rowsWritten, actualRows);
        resultCounter += actualRows;
        queryCounter++;

        rows = executeNet("SELECT id1 FROM " + keyspace + "." + table + " WHERE v1 >= 5");

        actualRows = rows.all().size();
        assertEquals(5, actualRows);
        resultCounter += actualRows;
        queryCounter++;

        assertEquals(2L, getPerQueryMetrics(keyspace, table, "SSTableIndexesHit"));
        assertEquals(2L, getPerQueryMetrics(keyspace, table, "IndexSegmentsHit"));
        assertEquals(2L, getTableQueryMetrics(keyspace, table, "TotalQueriesCompleted"));

        // run several times to get buffer faults across the metrics
        for (int x = 0; x < 20; x++)
        {
            rows = executeNet("SELECT id1 FROM " + keyspace + "." + table + " WHERE v1 >= 5");

            actualRows = rows.all().size();
            assertEquals(5, actualRows);
            resultCounter += actualRows;
            queryCounter++;
        }

        // column metrics

        waitForGreaterThanZero(objectNameNoIndex("QueryLatency", keyspace, table, PER_QUERY_METRIC_TYPE));

        waitForEquals(objectNameNoIndex("TotalPartitionsFetched", keyspace, table, TABLE_QUERY_METRIC_TYPE), resultCounter);
        waitForEquals(objectName("KDTreeIntersectionLatency", keyspace, table, index, GLOBAL_METRIC_TYPE), queryCounter);
    }

    @Test
    public void testKDTreePostingsQueryMetricsWithSingleIndex()
    {
        // Turn off the query optimizer.
        // We need to do this in order to remove unpredictability of query plans, so that we get consistent metrics.
        // We don't want the query optimizer to eliminate the use of indexes.
        QueryController.QUERY_OPT_LEVEL = 0;

        String table = "test_kdtree_postings_metrics_through_write_lifecycle";
        String v1Index = "test_kdtree_postings_metrics_through_write_lifecycle_v1_index";
        String v2Index = "test_kdtree_postings_metrics_through_write_lifecycle_v2_index";

        String keyspace = createKeyspace(CREATE_KEYSPACE_TEMPLATE);

        createTable(String.format(CREATE_TABLE_TEMPLATE, keyspace, table));

        createIndex(String.format(CREATE_INDEX_TEMPLATE + " WITH OPTIONS = {'bkd_postings_min_leaves' : 1}", v1Index, keyspace, table, "v1"));
        createIndex(String.format(CREATE_INDEX_TEMPLATE, v2Index, keyspace, table, "v2"));

        int rowsWritten = 50;

        for (int i = 0; i < rowsWritten; i++)
        {
            execute("INSERT INTO " + keyspace + "." + table + " (id1, v1, v2) VALUES (?, ?, ?)", Integer.toString(i), i, Integer.toString(i));
        }

        flush(keyspace, table);
        compact(keyspace, table);
        waitForIndexCompaction(keyspace, table, v1Index);

        waitForTableIndexesQueryable(keyspace, table);

        ResultSet rows = executeNet("SELECT id1 FROM " + keyspace + "." + table + " WHERE v1 >= 0");

        int actualRows = rows.all().size();
        assertEquals(rowsWritten, actualRows);

        assertTrue(((Number) getMetricValue(objectName("NumPostings", keyspace, table, v1Index, "KDTreePostings"))).longValue() > 0);
        waitForHistogramCountEquals(objectNameNoIndex("KDTreePostingsNumPostings", keyspace, table, PER_QUERY_METRIC_TYPE), 1);
        waitForHistogramMeanBetween(objectNameNoIndex("KDTreePostingsNumPostings", keyspace, table, PER_QUERY_METRIC_TYPE), 1.0, 1.0);

        // the query performed no skips, but the metric should be updated because the index was used, so we should get
        // a single entry in the histogram with 0 skips
        waitForHistogramCountEquals(objectNameNoIndex("KDTreePostingsSkips", keyspace, table, PER_QUERY_METRIC_TYPE), 1);
        waitForHistogramMeanBetween(objectNameNoIndex("KDTreePostingsSkips", keyspace, table, PER_QUERY_METRIC_TYPE), 0.0, 0.0);

        // V2 index is very selective, so it should lead the union merge process, causing V1 index to skip/advance
        execute("SELECT id1 FROM " + keyspace + "." + table + " WHERE v1 >= 0 AND v1 <= 1000 AND v2 IN ('5', '10', '20', '22') ALLOW FILTERING");

        // we expect exactly 4 skips from this query, but the mean will be 2.0 because of the previous query which had 0 skips
        waitForHistogramCountEquals(objectNameNoIndex("KDTreePostingsSkips", keyspace, table, PER_QUERY_METRIC_TYPE), 2);
        waitForHistogramMeanBetween(objectNameNoIndex("KDTreePostingsSkips", keyspace, table, PER_QUERY_METRIC_TYPE), 1.99, 2.01);
    }

    @Test
    public void testInvertedIndexQueryMetricsWithSingleIndex()
    {
        String table = "test_invertedindex_metrics_through_write_lifecycle";
        String index = "test_invertedindex_metrics_through_write_lifecycle_index";

        String keyspace = createKeyspace(CREATE_KEYSPACE_TEMPLATE);
        createTable(String.format(CREATE_TABLE_TEMPLATE, keyspace, table));
        createIndex(String.format(CREATE_INDEX_TEMPLATE, index, keyspace, table, "v2"));

        int resultCounter = 0;
        int queryCounter = 0;

        int rowsWritten = 10;

        for (int i = 0; i < rowsWritten; i++)
        {
            execute("INSERT INTO " + keyspace + "." + table + " (id1, v1, v2) VALUES (?, ?, ?)", Integer.toString(i), i, Integer.toString(i));
        }

        flush(keyspace, table);
        compact(keyspace, table);
        waitForIndexCompaction(keyspace, table, index);

        waitForTableIndexesQueryable(keyspace, table);

        ResultSet rows = executeNet("SELECT id1 FROM " + keyspace + "." + table + " WHERE v2 = '0'");

        int actualRows = rows.all().size();
        assertEquals(1, actualRows);
        resultCounter += actualRows;
        queryCounter++;

        rows = executeNet("SELECT id1 FROM " + keyspace + "." + table + " WHERE v2 = '5'");

        actualRows = rows.all().size();
        assertEquals(1, actualRows);
        resultCounter += actualRows;
        queryCounter++;

        assertEquals(2L, getPerQueryMetrics(keyspace, table, "SSTableIndexesHit"));
        assertEquals(2L, getPerQueryMetrics(keyspace, table, "IndexSegmentsHit"));
        assertEquals(2L, getTableQueryMetrics(keyspace, table, "TotalQueriesCompleted"));

        // run several times to get buffer faults across the metrics
        for (int x = 0; x < 20; x++)
        {
            rows = executeNet("SELECT id1 FROM " + keyspace + "." + table + " WHERE v2 = '" + ThreadLocalRandom.current().nextInt(0, 9) + "'");

            actualRows = rows.all().size();
            assertEquals(1, actualRows);
            resultCounter += actualRows;
            queryCounter++;
        }

        waitForGreaterThanZero(objectName("TermsLookupLatency", keyspace, table, index, GLOBAL_METRIC_TYPE));

        waitForGreaterThanZero(objectNameNoIndex("QueryLatency", keyspace, table, PER_QUERY_METRIC_TYPE));

        waitForEquals(objectNameNoIndex("TotalPartitionsFetched", keyspace, table, TABLE_QUERY_METRIC_TYPE), resultCounter);
    }

    @Test
    public void testKDTreePartitionsReadAndRowsFiltered()
    {
        String table = "test_rows_filtered_large_partition";
        String index = "test_rows_filtered_large_partition_index";

        String keyspace = createKeyspace(CREATE_KEYSPACE_TEMPLATE);

        createTable(String.format("CREATE TABLE %s.%s (pk int, ck int, v1 int, PRIMARY KEY (pk, ck)) " +
                                  "WITH compaction = {'class' : 'SizeTieredCompactionStrategy', 'enabled' : false }", keyspace,  table));

        createIndex(String.format(CREATE_INDEX_TEMPLATE, index, keyspace, table, "v1"));

        execute("INSERT INTO " + keyspace + "." + table + "(pk, ck, v1) VALUES (0, 0, 0)");
        execute("INSERT INTO " + keyspace + "." + table + "(pk, ck, v1) VALUES (1, 1, 1)");
        execute("INSERT INTO " + keyspace + "." + table + "(pk, ck, v1) VALUES (1, 2, 2)");
        execute("INSERT INTO " + keyspace + "." + table + "(pk, ck, v1) VALUES (2, 1, 3)");

        flush(keyspace, table);

        ResultSet rows = executeNet("SELECT pk, ck FROM " + keyspace + "." + table + " WHERE v1 > 0");

        int actualRows = rows.all().size();
        assertEquals(3, actualRows);

        // This is 2 due to partition read batching.
        waitForEquals(objectNameNoIndex("TotalPartitionsFetched", keyspace, table, TABLE_QUERY_METRIC_TYPE), 2);
        waitForHistogramCountEquals(objectNameNoIndex("RowsFetched", keyspace, table, PER_QUERY_METRIC_TYPE), 1);
        waitForEquals(objectNameNoIndex("TotalRowsFetched", keyspace, table, TABLE_QUERY_METRIC_TYPE), 3);
    }

    @Test
    public void testKDTreeQueryEarlyExit()
    {
        String table = "test_queries_exited_early";
        String index = "test_queries_exited_early_index";

        String keyspace = createKeyspace(CREATE_KEYSPACE_TEMPLATE);

        createTable(String.format("CREATE TABLE %s.%s (pk int, ck int, v1 int, PRIMARY KEY (pk, ck)) " +
                                  "WITH compaction = {'class' : 'SizeTieredCompactionStrategy', 'enabled' : false }", keyspace, table));

        createIndex(String.format(CREATE_INDEX_TEMPLATE, index, keyspace, table, "v1"));

        execute("INSERT INTO " + keyspace + "." + table + "(pk, ck, v1) VALUES (0, 0, 0)");
        execute("INSERT INTO " + keyspace + "." + table + "(pk, ck, v1) VALUES (1, 1, 1)");
        execute("INSERT INTO " + keyspace + "." + table + "(pk, ck, v1) VALUES (1, 2, 2)");

        flush(keyspace, table);

        ResultSet rows = executeNet("SELECT pk, ck FROM " + keyspace + "." + table + " WHERE v1 > 2");

        assertEquals(0, rows.all().size());

        rows = executeNet("SELECT pk, ck FROM " + keyspace + "." + table + " WHERE v1 < 0");
        assertEquals(0, rows.all().size());

        waitForEquals(objectName("KDTreeIntersectionLatency", keyspace, table, index, GLOBAL_METRIC_TYPE), 0L);
        waitForEquals(objectName("KDTreeIntersectionEarlyExits", keyspace, table, index, GLOBAL_METRIC_TYPE), 2L);

        rows = executeNet("SELECT pk, ck FROM " + keyspace + "." + table + " WHERE v1 > 0");
        assertEquals(2, rows.all().size());

        waitForEquals(objectName("KDTreeIntersectionLatency", keyspace, table, index, GLOBAL_METRIC_TYPE), 1L);
        waitForEquals(objectName("KDTreeIntersectionEarlyExits", keyspace, table, index, GLOBAL_METRIC_TYPE), 2L);
    }

    /**
     * Test the {@link ReadCommand} flags that are used to determine the kind of query (top-k only, filtering only,
     * hybrid, single partition and multipartition) in metrics.
     */
    @Test
    public void testQueryKindFlags()
    {
        createTable("CREATE TABLE %s (k int, c int, n int, s text, v vector<float, 2>, PRIMARY KEY(k, c))");

        // test without indexes
        assertQueryTypeFlags("SELECT * FROM %s", false, false);
        assertQueryTypeFlags("SELECT * FROM %s WHERE n = 1 ALLOW FILTERING", false, false);

        // test with legacy indexes
        String idx = createIndex("CREATE INDEX ON %s(n)");
        assertQueryTypeFlags("SELECT * FROM %s", false, false);
        assertQueryTypeFlags("SELECT * FROM %s WHERE n = 1", false, true);
        assertQueryTypeFlags("SELECT * FROM %s WHERE n = 1 AND s = 'a' ALLOW FILTERING", false, true);
        assertQueryTypeFlags("SELECT * FROM %s WHERE n = 1 OR s = 'a' ALLOW FILTERING", false, false);

        // test with SAI indexes
        dropIndex("DROP INDEX %s." + idx);
        createIndex("CREATE CUSTOM INDEX ON %s(n) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        assertQueryTypeFlags("SELECT * FROM %s", false, false);
        assertQueryTypeFlags("SELECT * FROM %s WHERE n = 1", false, true);
        assertQueryTypeFlags("SELECT * FROM %s WHERE n = 1 AND s = 'a' ALLOW FILTERING", false, true);
        assertQueryTypeFlags("SELECT * FROM %s WHERE n = 1 OR s = 'a' ALLOW FILTERING", false, false);
        assertQueryTypeFlags("SELECT * FROM %s ORDER BY v ANN OF [1, 1] LIMIT 10", true, false);
        assertQueryTypeFlags("SELECT * FROM %s WHERE n=1 ORDER BY v ANN OF [1, 1] LIMIT 10", true, true);
    }

    private void assertQueryTypeFlags(String query, boolean expectedIsTopK, boolean expectedUsesIndexFiltering)
    {
        ReadCommand command = parseReadCommand(query);
        Assert.assertEquals(expectedIsTopK, command.isTopK());
        Assert.assertEquals(expectedUsesIndexFiltering, command.usesIndexFiltering());
    }

    /**
     * Test that metrics are correctly separated for different types of queries.
     */
    @Test
    public void testQueryKindMetrics()
    {
        testQueryKindMetrics(false, false);
        testQueryKindMetrics(false, true);
        testQueryKindMetrics(true, false);
        testQueryKindMetrics(true, true);
    }

    private void testQueryKindMetrics(boolean perTable, boolean perQuery)
    {
        CassandraRelevantProperties.SAI_QUERY_KIND_PER_TABLE_METRICS_ENABLED.setBoolean(perTable);
        CassandraRelevantProperties.SAI_QUERY_KIND_PER_QUERY_METRICS_ENABLED.setBoolean(perQuery);

        // create table and indexes for vector and numeric
        createTable("CREATE TABLE %s (k int, c int, n int, v vector<float, 2>, PRIMARY KEY(k, c))");
        createIndex("CREATE CUSTOM INDEX ON %s(n) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");

        // insert some data
        int numPartitions = 11;
        int numRowsPerPartition = 13;
        int numRows = numPartitions * numRowsPerPartition;
        for (int k = 0; k < numPartitions; k++)
            for (int c = 0; c < numRowsPerPartition; c++)
                execute("INSERT INTO %s (k, c, n, v) VALUES (?, ?, 1, [1, 1])", k, c);

        // add a partition tombstone
        execute("INSERT INTO %s (k, c, n, v) VALUES (?, ?, 1, [1, 1])", numPartitions, numRowsPerPartition);
        execute("DELETE FROM %s WHERE k = ?", numPartitions);

        // add a row range tombstone
        execute("INSERT INTO %s (k, c, n, v) VALUES (?, ?, 1, [1, 1])", numPartitions + 1, numRowsPerPartition);
        execute("DELETE FROM %s WHERE k = ? AND c > 0", numPartitions + 1);

        // filter query (goes to the general, filter and range query metrics)
        UntypedResultSet rows = execute("SELECT k, c FROM %s WHERE n = 1");
        assertEquals(numRows, rows.size());

        // top-k query (goes to the general, top-k and range query metrics)
        rows = execute("SELECT k, c FROM %s ORDER BY v ANN OF [1, 1] LIMIT 1000");
        assertEquals(numRows, rows.size());

        // single partition top-k query (goes to the general, top-k and range query metrics)
        rows = execute("SELECT k, c FROM %s WHERE k = 0 ORDER BY v ANN OF [1, 1] LIMIT 1000");
        assertEquals(numRowsPerPartition, rows.size());

        // partition query (goes to the general, filter single-partition query metrics)
        rows = execute("SELECT k, c FROM %s WHERE k = 0 AND n = 1");
        assertEquals(numRowsPerPartition, rows.size());

        // hybrid query, postfiltering (goes to the general, hybrid and range query metrics)
        rows = execute("SELECT k, c FROM %s WHERE n = 1 ORDER BY v ANN OF [1, 1] LIMIT 1000");
        assertEquals(numRows, rows.size());

        // hybrid query, single partition (goes to the general, hybrid and range query metrics)
        rows = execute("SELECT k, c FROM %s WHERE k = 0 AND n = 1 ORDER BY v ANN OF [1, 1] LIMIT 1000");
        assertEquals(numRowsPerPartition, rows.size());

        // Verify metrics for total queries completed.
        String name = "TotalQueriesCompleted";
        waitForEquals(objectName(name, TABLE_QUERY_METRIC_TYPE), 6);
        waitForEqualsIfExists(perTable, objectName(name, TABLE_SP_FILTER_QUERY_METRIC_TYPE), 1);
        waitForEqualsIfExists(perTable, objectName(name, TABLE_MP_FILTER_QUERY_METRIC_TYPE), 1);
        waitForEqualsIfExists(perTable, objectName(name, TABLE_SP_TOPK_QUERY_METRIC_TYPE), 1);
        waitForEqualsIfExists(perTable, objectName(name, TABLE_MP_TOPK_QUERY_METRIC_TYPE), 1);
        waitForEqualsIfExists(perTable, objectName(name, TABLE_SP_HYBRID_QUERY_METRIC_TYPE), 1);
        waitForEqualsIfExists(perTable, objectName(name, TABLE_MP_HYBRID_QUERY_METRIC_TYPE), 1);

        // Verify counters for total keys fetched.
        name = "TotalKeysFetched";
        waitForEquals(objectName(name, TABLE_QUERY_METRIC_TYPE), 3L * (numRowsPerPartition + numRows + 2));
        waitForEqualsIfExists(perTable, objectName(name, TABLE_SP_FILTER_QUERY_METRIC_TYPE), numRowsPerPartition);
        waitForEqualsIfExists(perTable, objectName(name, TABLE_MP_FILTER_QUERY_METRIC_TYPE), numRows + 2);
        waitForEqualsIfExists(perTable, objectName(name, TABLE_SP_TOPK_QUERY_METRIC_TYPE), numRowsPerPartition);
        waitForEqualsIfExists(perTable, objectName(name, TABLE_MP_TOPK_QUERY_METRIC_TYPE), numRows + 2);
        waitForEqualsIfExists(perTable, objectName(name, TABLE_SP_HYBRID_QUERY_METRIC_TYPE), numRowsPerPartition);
        waitForEqualsIfExists(perTable, objectName(name, TABLE_MP_HYBRID_QUERY_METRIC_TYPE), numRows + 2);

        // Verify counters for total partitions fetched.
        name = "TotalPartitionsFetched";
        waitForEquals(objectName(name, TABLE_QUERY_METRIC_TYPE), 1 + numPartitions + 1 + numRowsPerPartition + numRows + 1 + numRowsPerPartition + numRows + 1);
        waitForEqualsIfExists(perTable, objectName(name, TABLE_SP_FILTER_QUERY_METRIC_TYPE), 1);
        waitForEqualsIfExists(perTable, objectName(name, TABLE_MP_FILTER_QUERY_METRIC_TYPE), numPartitions + 1);
        waitForEqualsIfExists(perTable, objectName(name, TABLE_SP_TOPK_QUERY_METRIC_TYPE), numRowsPerPartition); // single-partition top-k issues a partition access per each row
        waitForEqualsIfExists(perTable, objectName(name, TABLE_MP_TOPK_QUERY_METRIC_TYPE), numRows + 1);
        waitForEqualsIfExists(perTable, objectName(name, TABLE_SP_HYBRID_QUERY_METRIC_TYPE), numRowsPerPartition); // single-partition top-k issues a partition access per each row
        waitForEqualsIfExists(perTable, objectName(name, TABLE_MP_HYBRID_QUERY_METRIC_TYPE), numRows + 1);

        // Verify counters for total partitions returned.
        name = "TotalPartitionsReturned";
        waitForEquals(objectName(name, TABLE_QUERY_METRIC_TYPE), 1 + numPartitions + 1 + numPartitions + 1 + numPartitions);
        waitForEqualsIfExists(perTable, objectName(name, TABLE_SP_FILTER_QUERY_METRIC_TYPE), 1);
        waitForEqualsIfExists(perTable, objectName(name, TABLE_MP_FILTER_QUERY_METRIC_TYPE), numPartitions);
        waitForEqualsIfExists(perTable, objectName(name, TABLE_SP_TOPK_QUERY_METRIC_TYPE), 1);
        waitForEqualsIfExists(perTable, objectName(name, TABLE_MP_TOPK_QUERY_METRIC_TYPE), numPartitions);
        waitForEqualsIfExists(perTable, objectName(name, TABLE_SP_HYBRID_QUERY_METRIC_TYPE), 1);
        waitForEqualsIfExists(perTable, objectName(name, TABLE_MP_HYBRID_QUERY_METRIC_TYPE), numPartitions);

        // Verify counters for total partition tombstones fetched.
        name = "TotalPartitionTombstonesFetched";
        waitForEquals(objectName(name, TABLE_QUERY_METRIC_TYPE), 3);
        waitForEqualsIfExists(perTable, objectName(name, TABLE_SP_FILTER_QUERY_METRIC_TYPE), 0);
        waitForEqualsIfExists(perTable, objectName(name, TABLE_MP_FILTER_QUERY_METRIC_TYPE), 1);
        waitForEqualsIfExists(perTable, objectName(name, TABLE_SP_TOPK_QUERY_METRIC_TYPE), 0);
        waitForEqualsIfExists(perTable, objectName(name, TABLE_MP_TOPK_QUERY_METRIC_TYPE), 1);
        waitForEqualsIfExists(perTable, objectName(name, TABLE_SP_HYBRID_QUERY_METRIC_TYPE), 0);
        waitForEqualsIfExists(perTable, objectName(name, TABLE_MP_HYBRID_QUERY_METRIC_TYPE), 1);

        // Verify counters for total rows fetched.
        name = "TotalRowsFetched";
        waitForEquals(objectName(name, TABLE_QUERY_METRIC_TYPE), numRowsPerPartition + numRows + numRowsPerPartition + numRows + numRowsPerPartition + numRows);
        waitForEqualsIfExists(perTable, objectName(name, TABLE_SP_FILTER_QUERY_METRIC_TYPE), numRowsPerPartition);
        waitForEqualsIfExists(perTable, objectName(name, TABLE_MP_FILTER_QUERY_METRIC_TYPE), numRows);
        waitForEqualsIfExists(perTable, objectName(name, TABLE_SP_TOPK_QUERY_METRIC_TYPE), numRowsPerPartition);
        waitForEqualsIfExists(perTable, objectName(name, TABLE_MP_TOPK_QUERY_METRIC_TYPE), numRows);
        waitForEqualsIfExists(perTable, objectName(name, TABLE_SP_HYBRID_QUERY_METRIC_TYPE), numRowsPerPartition);
        waitForEqualsIfExists(perTable, objectName(name, TABLE_MP_HYBRID_QUERY_METRIC_TYPE), numRows);

        // Verify counters for total rows returned.
        name = "TotalRowsReturned";
        waitForEquals(objectName(name, TABLE_QUERY_METRIC_TYPE), numRowsPerPartition + numRows + numRowsPerPartition + numRows + numRowsPerPartition + numRows);
        waitForEqualsIfExists(perTable, objectName(name, TABLE_SP_FILTER_QUERY_METRIC_TYPE), numRowsPerPartition);
        waitForEqualsIfExists(perTable, objectName(name, TABLE_MP_FILTER_QUERY_METRIC_TYPE), numRows);
        waitForEqualsIfExists(perTable, objectName(name, TABLE_SP_TOPK_QUERY_METRIC_TYPE), numRowsPerPartition);
        waitForEqualsIfExists(perTable, objectName(name, TABLE_MP_TOPK_QUERY_METRIC_TYPE), numRows);
        waitForEqualsIfExists(perTable, objectName(name, TABLE_SP_HYBRID_QUERY_METRIC_TYPE), numRowsPerPartition);
        waitForEqualsIfExists(perTable, objectName(name, TABLE_MP_HYBRID_QUERY_METRIC_TYPE), numRows);

        // Verify counters for total row tombstones fetched.
        name = "TotalRowTombstonesFetched";
        waitForEquals(objectName(name, TABLE_QUERY_METRIC_TYPE), 6);
        waitForEqualsIfExists(perTable, objectName(name, TABLE_SP_FILTER_QUERY_METRIC_TYPE), 0);
        waitForEqualsIfExists(perTable, objectName(name, TABLE_MP_FILTER_QUERY_METRIC_TYPE), 2);
        waitForEqualsIfExists(perTable, objectName(name, TABLE_SP_TOPK_QUERY_METRIC_TYPE), 0);
        waitForEqualsIfExists(perTable, objectName(name, TABLE_MP_TOPK_QUERY_METRIC_TYPE), 2);
        waitForEqualsIfExists(perTable, objectName(name, TABLE_SP_HYBRID_QUERY_METRIC_TYPE), 0);
        waitForEqualsIfExists(perTable, objectName(name, TABLE_MP_HYBRID_QUERY_METRIC_TYPE), 2);

        // Verify counters for timeouts.
        name = "TotalQueryTimeouts";
        waitForEquals(objectName(name, TABLE_QUERY_METRIC_TYPE), 0);
        waitForEqualsIfExists(perTable, objectName(name, TABLE_SP_FILTER_QUERY_METRIC_TYPE), 0);
        waitForEqualsIfExists(perTable, objectName(name, TABLE_MP_FILTER_QUERY_METRIC_TYPE), 0);
        waitForEqualsIfExists(perTable, objectName(name, TABLE_SP_TOPK_QUERY_METRIC_TYPE), 0);
        waitForEqualsIfExists(perTable, objectName(name, TABLE_MP_TOPK_QUERY_METRIC_TYPE), 0);
        waitForEqualsIfExists(perTable, objectName(name, TABLE_SP_HYBRID_QUERY_METRIC_TYPE), 0);
        waitForEqualsIfExists(perTable, objectName(name, TABLE_MP_HYBRID_QUERY_METRIC_TYPE), 0);

        // Verify counters for sort-then-filter hybrid queries.
        // Topk-only queries do not count because they don't do filtering.
        // We have no support for forcing the different order of operations in the query engine, so we cannot
        // directly test sort-then-filter queries.
        name = "SortThenFilterQueriesCompleted";
        waitForEquals(objectName(name, TABLE_QUERY_METRIC_TYPE), 0);
        waitForEqualsIfExists(perTable, objectName(name, TABLE_SP_HYBRID_QUERY_METRIC_TYPE), 0);
        waitForEqualsIfExists(perTable, objectName(name, TABLE_MP_HYBRID_QUERY_METRIC_TYPE), 0);

        // Verify counters for filter-then-sort hybrid queries.
        name = "FilterThenSortQueriesCompleted";
        waitForEquals(objectName(name, TABLE_QUERY_METRIC_TYPE), 2);
        waitForEqualsIfExists(perTable, objectName(name, TABLE_SP_HYBRID_QUERY_METRIC_TYPE), 1);
        waitForEqualsIfExists(perTable, objectName(name, TABLE_MP_HYBRID_QUERY_METRIC_TYPE), 1);

        // Verify histograms
        verifyHistogramCount("KeysFetched", perQuery);
        verifyHistogramCount("PartitionsFetched", perQuery);
        verifyHistogramCount("PartitionsReturned", perQuery);
        verifyHistogramCount("PartitionTombstonesFetched", perQuery);
        verifyHistogramCount("RowsFetched", perQuery);
        verifyHistogramCount("RowsReturned", perQuery);
        verifyHistogramCount("RowTombstonesFetched", perQuery);
    }

    private void verifyHistogramCount(String name, boolean hasPerQueryKindMetrics)
    {
        waitForHistogramCountEquals(objectName(name, PER_QUERY_METRIC_TYPE), 6);
        waitForHistogramCountEqualsIfExists(hasPerQueryKindMetrics, objectName(name, PER_SP_FILTER_QUERY_METRIC_TYPE), 1);
        waitForHistogramCountEqualsIfExists(hasPerQueryKindMetrics, objectName(name, PER_MP_FILTER_QUERY_METRIC_TYPE), 1);
        waitForHistogramCountEqualsIfExists(hasPerQueryKindMetrics, objectName(name, PER_SP_TOPK_QUERY_METRIC_TYPE), 1);
        waitForHistogramCountEqualsIfExists(hasPerQueryKindMetrics, objectName(name, PER_MP_TOPK_QUERY_METRIC_TYPE), 1);
        waitForHistogramCountEqualsIfExists(hasPerQueryKindMetrics, objectName(name, PER_SP_HYBRID_QUERY_METRIC_TYPE), 1);
        waitForHistogramCountEqualsIfExists(hasPerQueryKindMetrics, objectName(name, PER_MP_HYBRID_QUERY_METRIC_TYPE), 1);
    }

    private ObjectName objectName(String name, String type)
    {
        return objectNameNoIndex(name, KEYSPACE, currentTable(), type);
    }

    protected void waitForEqualsIfExists(boolean shouldExist, ObjectName name, long value)
    {
        if (shouldExist)
            waitForEquals(name, value);
        else
            assertMetricDoesNotExist(name);
    }

    protected void waitForHistogramCountEqualsIfExists(boolean shouldExist, ObjectName name, long count)
    {
        if (shouldExist)
            waitForHistogramCountEquals(name, count);
        else
            assertMetricDoesNotExist(name);
    }

    private long getPerQueryMetrics(String keyspace, String table, String metricsName)
    {
        return (long) getMetricValue(objectNameNoIndex(metricsName, keyspace, table, PER_QUERY_METRIC_TYPE));
    }
}
