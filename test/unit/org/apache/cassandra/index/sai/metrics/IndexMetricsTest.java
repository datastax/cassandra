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

import java.util.concurrent.TimeUnit;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.junit.Test;

import com.datastax.driver.core.ResultSet;
import org.apache.cassandra.utils.Throwables;

import javax.management.InstanceNotFoundException;
import javax.management.ObjectName;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.*;

public class IndexMetricsTest extends AbstractMetricsTest
{

    private static final String TABLE = "table_name";
    private static final String INDEX = "table_name_index";

    private static final String CREATE_TABLE_TEMPLATE = "CREATE TABLE %s." + TABLE + " (ID1 TEXT PRIMARY KEY, v1 INT, v2 TEXT) WITH compaction = " +
                                                        "{'class' : 'SizeTieredCompactionStrategy', 'enabled' : false }";
    private static final String CREATE_INDEX_TEMPLATE = "CREATE CUSTOM INDEX IF NOT EXISTS " + INDEX + " ON %s." + TABLE + "(%s) USING 'StorageAttachedIndex'";

    @Test
    public void testSameIndexNameAcrossKeyspaces()
    {
        String keyspace1 = createKeyspace(CREATE_KEYSPACE_TEMPLATE);
        String keyspace2 = createKeyspace(CREATE_KEYSPACE_TEMPLATE);

        createTable(String.format(CREATE_TABLE_TEMPLATE, keyspace1));
        createIndex(String.format(CREATE_INDEX_TEMPLATE, keyspace1, "v1"));

        createTable(String.format(CREATE_TABLE_TEMPLATE, keyspace2));
        createIndex(String.format(CREATE_INDEX_TEMPLATE, keyspace2, "v1"));

        execute("INSERT INTO " + keyspace1 + '.' + TABLE + " (id1, v1, v2) VALUES ('0', 0, '0')");

        assertEquals(1L, getMetricValue(objectName("LiveMemtableIndexWriteCount", keyspace1, TABLE, INDEX, "IndexMetrics")));
        assertEquals(0L, getMetricValue(objectName("LiveMemtableIndexWriteCount", keyspace2, TABLE, INDEX, "IndexMetrics")));

        execute("INSERT INTO " + keyspace2 + '.' + TABLE + " (id1, v1, v2) VALUES ('0', 0, '0')");
        execute("INSERT INTO " + keyspace2 + '.' + TABLE + " (id1, v1, v2) VALUES ('1', 1, '1')");

        assertEquals(1L, getMetricValue(objectName("LiveMemtableIndexWriteCount", keyspace1, TABLE, INDEX, "IndexMetrics")));
        assertEquals(2L, getMetricValue(objectName("LiveMemtableIndexWriteCount", keyspace2, TABLE, INDEX, "IndexMetrics")));
    }

    @Test
    public void testMetricRelease()
    {
        String table = createTable("CREATE TABLE %s (ID1 TEXT PRIMARY KEY, v1 INT, v2 TEXT) WITH compaction = " +
                                   "{'class' : 'SizeTieredCompactionStrategy', 'enabled' : false }");
        String index = createIndex("CREATE CUSTOM INDEX IF NOT EXISTS ON %s (v1) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (id1, v1, v2) VALUES ('0', 0, '0')");
        assertEquals(1L, getMetricValue(objectName("LiveMemtableIndexWriteCount", KEYSPACE, table, index, "IndexMetrics")));

        dropIndex("DROP INDEX %s." + index);

        // once the index is dropped, make sure MBeans are no longer accessible
        assertThatThrownBy(() -> getMetricValue(objectName("LiveMemtableIndexWriteCount", KEYSPACE, table, index, "IndexMetrics")))
                .hasCauseInstanceOf(javax.management.InstanceNotFoundException.class);
    }

    @Test
    public void testMetricsThroughWriteLifecycle()
    {
        String table = createTable("CREATE TABLE %s (ID1 TEXT PRIMARY KEY, v1 INT, v2 TEXT) WITH compaction = " +
                                   "{'class' : 'SizeTieredCompactionStrategy', 'enabled' : false }");
        String index = createIndex("CREATE CUSTOM INDEX IF NOT EXISTS ON %s (v1) USING 'StorageAttachedIndex'");

        int rowCount = 10;
        for (int i = 0; i < rowCount; i++)
            execute("INSERT INTO %s (id1, v1, v2) VALUES (?, ?, '0')", Integer.toString(i), i);

        assertEquals(10L, getMetricValue(objectName("LiveMemtableIndexWriteCount", KEYSPACE, table, index, "IndexMetrics")));
        assertTrue((Long)getMetricValue(objectName("MemtableOnHeapIndexBytes", KEYSPACE, table, index, "IndexMetrics")) > 0);
        assertTrue((Long)getMetricValue(objectName("MemtableOffHeapIndexBytes", KEYSPACE, table, index, "IndexMetrics")) > 0);
        assertEquals(0L, getMetricValue(objectName("MemtableIndexFlushCount", KEYSPACE, table, index, "IndexMetrics")));

        waitForAssert(() -> {
            try
            {
                assertEquals(10L, getMBeanAttribute(objectName("MemtableIndexWriteLatency", KEYSPACE, table, index, "IndexMetrics"), "Count"));
            }
            catch (Throwable ex)
            {
                throw Throwables.unchecked(ex);
            }
        }, 60, TimeUnit.SECONDS);

        assertEquals(0L, getMetricValue(objectName("SSTableCellCount", KEYSPACE, table, index, "IndexMetrics")));
        assertEquals(0L, getMetricValue(objectName("DiskUsedBytes", KEYSPACE, table, index, "IndexMetrics")));
        assertEquals(0L, getMetricValue(objectName("CompactionCount", KEYSPACE, table, index, "IndexMetrics")));

        waitForHistogramCountEquals(objectName("MemtableIndexFlushCellsPerSecond", KEYSPACE, table, index, "IndexMetrics"), 0);

        flush(KEYSPACE, table);

        assertEquals(0L, getMetricValue(objectName("LiveMemtableIndexWriteCount", KEYSPACE, table, index, "IndexMetrics")));
        assertEquals(0L, getMetricValue(objectName("MemtableOnHeapIndexBytes", KEYSPACE, table, index, "IndexMetrics")));
        assertEquals(0L, getMetricValue(objectName("MemtableOffHeapIndexBytes", KEYSPACE, table, index, "IndexMetrics")));
        assertEquals(1L, getMetricValue(objectName("MemtableIndexFlushCount", KEYSPACE, table, index, "IndexMetrics")));
        assertEquals(10L, getMetricValue(objectName("SSTableCellCount", KEYSPACE, table, index, "IndexMetrics")));
        assertTrue((Long)getMetricValue(objectName("DiskUsedBytes", KEYSPACE, table, index, "IndexMetrics")) > 0);
        assertEquals(0L, getMetricValue(objectName("CompactionCount", KEYSPACE, table, index, "IndexMetrics")));

        waitForHistogramCountEquals(objectName("MemtableIndexFlushCellsPerSecond", KEYSPACE, table, index, "IndexMetrics"), 1);
        waitForHistogramMeanBetween(objectName("MemtableIndexFlushCellsPerSecond", KEYSPACE, table, index, "IndexMetrics"), 1.0, 1000000.0);

        compact(KEYSPACE, table);

        waitForIndexCompaction(KEYSPACE, table, index);

        waitForTableIndexesQueryable(KEYSPACE, table);

        ResultSet rows = executeNet("SELECT id1 FROM %s WHERE v1 >= 0");
        assertEquals(rowCount, rows.all().size());

        assertEquals(0L, getMetricValue(objectName("LiveMemtableIndexWriteCount", KEYSPACE, table, index, "IndexMetrics")));
        assertEquals(1L, getMetricValue(objectName("MemtableIndexFlushCount", KEYSPACE, table, index, "IndexMetrics")));
        assertEquals(10L, getMetricValue(objectName("SSTableCellCount", KEYSPACE, table, index, "IndexMetrics")));
        assertTrue((Long)getMetricValue(objectName("DiskUsedBytes", KEYSPACE, table, index, "IndexMetrics")) > 0);
        assertEquals(1L, getMetricValue(objectName("CompactionCount", KEYSPACE, table, index, "IndexMetrics")));

        waitForHistogramCountEquals(objectName("CompactionSegmentCellsPerSecond", KEYSPACE, table, index, "IndexMetrics"), 1);
        waitForHistogramMeanBetween(objectName("CompactionSegmentCellsPerSecond", KEYSPACE, table, index, "IndexMetrics"), 1.0, 1000000.0);
    }

    @Test
    public void testIndexMetricsEnabledAndDisabled()
    {
        testIndexMetrics(true);
        testIndexMetrics(false);
    }

    private void testIndexMetrics(boolean metricsEnabled)
    {
        // Set the property before creating any indexes
        CassandraRelevantProperties.SAI_INDEX_METRICS_ENABLED.setBoolean(metricsEnabled);
        
        try
        {
            String table = createTable("CREATE TABLE %s (ID1 TEXT PRIMARY KEY, v1 INT, v2 TEXT) WITH compaction = " +
                                       "{'class' : 'SizeTieredCompactionStrategy', 'enabled' : false }");
            String index = createIndex("CREATE CUSTOM INDEX IF NOT EXISTS ON %s (v1) USING 'StorageAttachedIndex'");
            
            // Test all Gauge metrics
            assertMetricExistsIfEnabled(metricsEnabled, "SSTableCellCount", table, index);
            assertMetricExistsIfEnabled(metricsEnabled, "LiveMemtableIndexWriteCount", table, index);
            assertMetricExistsIfEnabled(metricsEnabled, "DiskUsedBytes", table, index);
            assertMetricExistsIfEnabled(metricsEnabled, "MemtableOnHeapIndexBytes", table, index);
            assertMetricExistsIfEnabled(metricsEnabled, "MemtableOffHeapIndexBytes", table, index);
            assertMetricExistsIfEnabled(metricsEnabled, "IndexFileCacheBytes", table, index);
            
            // Test all Counter metrics
            assertMetricExistsIfEnabled(metricsEnabled, "MemtableIndexFlushCount", table, index);
            assertMetricExistsIfEnabled(metricsEnabled, "CompactionCount", table, index);
            assertMetricExistsIfEnabled(metricsEnabled, "MemtableIndexFlushErrors", table, index);
            assertMetricExistsIfEnabled(metricsEnabled, "CompactionSegmentFlushErrors", table, index);
            assertMetricExistsIfEnabled(metricsEnabled, "QueriesCount", table, index);
            
            // Test all Histogram metrics
            assertMetricExistsIfEnabled(metricsEnabled, "MemtableIndexFlushCellsPerSecond", table, index);
            assertMetricExistsIfEnabled(metricsEnabled, "SegmentsPerCompaction", table, index);
            assertMetricExistsIfEnabled(metricsEnabled, "CompactionSegmentCellsPerSecond", table, index);
            assertMetricExistsIfEnabled(metricsEnabled, "CompactionSegmentBytesPerSecond", table, index);
            
            // Test Timer metrics
            assertMetricExistsIfEnabled(metricsEnabled, "MemtableIndexWriteLatency", table, index);
        }
        finally
        {
            // Reset property to default
            CassandraRelevantProperties.SAI_INDEX_METRICS_ENABLED.setBoolean(true);
        }
    }

    private void assertMetricExistsIfEnabled(boolean shouldExist, String metricName, String table, String index)
    {
        ObjectName name = objectName(metricName, KEYSPACE, table, index, "IndexMetrics");
        
        if (shouldExist)
            assertMetricExists(name, metricName);
        else
            assertMetricDoesNotExist(name, metricName);
    }

    private void assertMetricExists(ObjectName name, String metricName)
    {
        try
        {
            getMetricValue(name);
            // If we get here without exception, the metric exists - that's what we want
        }
        catch (Exception e)
        {
            fail("Expected metric " + metricName + " to be registered when metrics are enabled, but got: " + e.getMessage());
        }
    }

    private void assertMetricDoesNotExist(ObjectName name, String metricName)
    {
        try
        {
            getMetricValue(name);
            fail("Expected metric " + metricName + " to not be registered when metrics are disabled");
        }
        catch (Exception e)
        {
            // Expected - metrics should not be accessible when disabled
            assertTrue("Expected InstanceNotFoundException for " + metricName + " but got: " + e.getClass().getSimpleName(),
                      e.getCause() instanceof InstanceNotFoundException ||
                      e instanceof InstanceNotFoundException);
        }
    }

    private void assertIndexQueryCount(String index, long expectedCount)
    {
        assertEquals(expectedCount,
                     getMetricValue(objectName("QueriesCount", KEYSPACE, currentTable(), index, "IndexMetrics")));
    }

    @Test
    public void testQueriesCount()
    {
        createTable("CREATE TABLE %s (id1 TEXT PRIMARY KEY, v1 INT, v2 TEXT, v3 VECTOR<FLOAT, 2>)");
        String indexV1 = createIndex("CREATE CUSTOM INDEX ON %s (v1) USING 'StorageAttachedIndex'");

        int rowCount = 10;
        for (int i = 0; i < rowCount; i++)
            execute("INSERT INTO %s (id1, v1, v2, v3) VALUES (?, ?, '0', ?)", Integer.toString(i), i, vector(i, i));

        assertIndexQueryCount(indexV1, 0L);

        ResultSet rows = executeNet("SELECT id1 FROM %s WHERE v1 >= 0");
        assertEquals(rowCount, rows.all().size());
        assertIndexQueryCount(indexV1, 1L);

        executeNet("SELECT id1 FROM %s WHERE (v1 >= 0 OR v1 = 4) AND v2 = '2' ALLOW FILTERING");
        assertIndexQueryCount(indexV1, 2L);

        String indexV2 = createIndex("CREATE CUSTOM INDEX ON %s (v2) USING 'StorageAttachedIndex'");
        executeNet("SELECT id1 FROM %s WHERE (v1 >= 0 OR v1 = 4)");
        assertIndexQueryCount(indexV1, 3L);
        assertIndexQueryCount(indexV2, 0L);

        executeNet("SELECT id1 FROM %s WHERE v2 = '2'");
        assertIndexQueryCount(indexV2, 1L);
        executeNet("SELECT id1 FROM %s WHERE (v1 >= 0 OR v1 = 4) AND v2 = '2'");
        assertIndexQueryCount(indexV1, 4L);
        assertIndexQueryCount(indexV2, 1L);
        executeNet("SELECT id1 FROM %s WHERE (v1 >= 0 OR v1 = 4) ORDER BY v2 LIMIT 10");
        assertIndexQueryCount(indexV1, 4L);
        assertIndexQueryCount(indexV2, 2L);

        String indexV3 = createIndex("CREATE CUSTOM INDEX ON %s (v3) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function': 'euclidean'}");
        assertIndexQueryCount(indexV3, 0L);
        executeNet("SELECT id1 FROM %s WHERE v2 = '2' ORDER BY v3 ANN OF [5,0] LIMIT 10");
        assertIndexQueryCount(indexV1, 4L);
        assertIndexQueryCount(indexV2, 2L);
        assertIndexQueryCount(indexV3, 1L);
    }
}
