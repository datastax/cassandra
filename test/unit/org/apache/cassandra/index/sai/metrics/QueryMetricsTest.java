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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.datastax.driver.core.ResultSet;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.plan.QueryController;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;

import static org.apache.cassandra.index.sai.metrics.TableQueryMetrics.TABLE_QUERY_METRIC_TYPE;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class QueryMetricsTest extends AbstractMetricsTest
{
    private static final String CREATE_TABLE_TEMPLATE = "CREATE TABLE %s.%s (id1 TEXT PRIMARY KEY, v1 INT, v2 TEXT) WITH compaction = " +
                                                        "{'class' : 'SizeTieredCompactionStrategy', 'enabled' : false }";
    private static final String CREATE_INDEX_TEMPLATE = "CREATE CUSTOM INDEX IF NOT EXISTS %s ON %s.%s(%s) USING 'StorageAttachedIndex'";

    private static final String PER_QUERY_METRIC_TYPE = "PerQuery";
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
        assertEquals(0L, getTableQueryMetrics(keyspace2, table, "TotalQueriesCompleted"));

        execute("INSERT INTO " + keyspace2 + "." + table + " (id1, v1, v2) VALUES ('0', 0, '0')");
        execute("INSERT INTO " + keyspace2 + "." + table + " (id1, v1, v2) VALUES ('1', 1, '1')");

        rows = executeNet("SELECT id1 FROM " + keyspace1 + "." + table + " WHERE v1 = 0");
        assertEquals(1, rows.all().size());

        rows = executeNet("SELECT id1 FROM " + keyspace2 + "." + table + " WHERE v1 = 1");
        assertEquals(1, rows.all().size());

        assertEquals(2L, getTableQueryMetrics(keyspace1, table, "TotalQueriesCompleted"));
        assertEquals(1L, getTableQueryMetrics(keyspace2, table, "TotalQueriesCompleted"));
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

        waitForTableIndexesQueryable(keyspace, table);

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

        waitForEquals(objectNameNoIndex("TotalPartitionReads", keyspace, table, TableQueryMetrics.TABLE_QUERY_METRIC_TYPE), resultCounter);
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

        waitForEquals(objectNameNoIndex("TotalPartitionReads", keyspace, table, TableQueryMetrics.TABLE_QUERY_METRIC_TYPE), resultCounter);
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
        waitForEquals(objectNameNoIndex("TotalPartitionReads", keyspace, table, TABLE_QUERY_METRIC_TYPE), 2);
        waitForHistogramCountEquals(objectNameNoIndex("RowsFiltered", keyspace, table, PER_QUERY_METRIC_TYPE), 1);
        waitForEquals(objectNameNoIndex("TotalRowsFiltered", keyspace, table, TABLE_QUERY_METRIC_TYPE), 3);
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

    private long getPerQueryMetrics(String keyspace, String table, String metricsName)
    {
        return (long) getMetricValue(objectNameNoIndex(metricsName, keyspace, table, PER_QUERY_METRIC_TYPE));
    }

    private long getTableQueryMetrics(String keyspace, String table, String metricsName)
    {
        return (long) getMetricValue(objectNameNoIndex(metricsName, keyspace, table, TableQueryMetrics.TABLE_QUERY_METRIC_TYPE));
    }
}
