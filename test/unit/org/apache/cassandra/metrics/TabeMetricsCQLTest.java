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
package org.apache.cassandra.metrics;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.net.MessagingService;
import org.assertj.core.api.Assertions;

public class TabeMetricsCQLTest extends CQLTester
{
    @BeforeClass
    @SuppressWarnings("deprecation")
    public static void setUpClass()
    {
        // Set the messaging version that adds support for index hints before starting the server
        CassandraRelevantProperties.DS_CURRENT_MESSAGING_VERSION.setInt(MessagingService.VERSION_DS_12);
        CQLTester.setUpClass();
        CQLTester.enableCoordinatorExecution();
    }

    @Test
    public void testQueryLatency()
    {
        testQueryLatency(true);
        testQueryLatency(false);
    }

    private void testQueryLatency(boolean separateIndexLatency)
    {
        CassandraRelevantProperties.SEPARATE_INDEX_LATENCY_HISTOGRAM_ENABLED.setBoolean(separateIndexLatency);

        createTable("CREATE TABLE %s (k int, c int, v1 int, v2 int, PRIMARY KEY(k, c))");
        String idx1 = createIndex("CREATE CUSTOM INDEX ON %s(v1) USING 'StorageAttachedIndex'");
        String idx2 = createIndex("CREATE INDEX ON %s(v2)");
        execute("INSERT INTO %s (k, c, v1, v2) VALUES (0, 0, 0, 0)");

        TableMetrics metrics = getCurrentColumnFamilyStore().metric;

        // range query
        execute("SELECT * FROM %s");
        assertLatency(metrics.readLatency, 0);
        assertLatency(metrics.rangeLatency, 1);
        assertLatency(metrics.indexLatency, 0);

        // partition query
        execute("SELECT * FROM %s WHERE k = 0");
        execute("SELECT * FROM %s WHERE k = 1");
        assertLatency(metrics.readLatency, 2);
        assertLatency(metrics.rangeLatency, 1);
        assertLatency(metrics.indexLatency, 0);

        // index range query (SAI)
        execute("SELECT * FROM %s WHERE v1 = 0");
        execute("SELECT * FROM %s WHERE v1 = 1");
        execute("SELECT * FROM %s WHERE v1 = 2");
        assertLatency(metrics.readLatency, 2);
        assertLatency(metrics.rangeLatency, separateIndexLatency ? 1 : 4);
        assertLatency(metrics.indexLatency, separateIndexLatency ? 3 : 0);

        // index range query (legacy)
        execute("SELECT * FROM %s WHERE v2 = 0");
        execute("SELECT * FROM %s WHERE v2 = 1");
        execute("SELECT * FROM %s WHERE v2 = 2");
        execute("SELECT * FROM %s WHERE v2 = 3");
        assertLatency(metrics.readLatency, 2);
        assertLatency(metrics.rangeLatency, separateIndexLatency ? 1 : 8);
        assertLatency(metrics.indexLatency, separateIndexLatency ? 7 : 0);

        // index partition queries
        execute("SELECT * FROM %s WHERE k = 0 AND v1 = 0");
        execute("SELECT * FROM %s WHERE k = 0 AND v2 = 0");
        assertLatency(metrics.readLatency, separateIndexLatency ? 2 : 4);
        assertLatency(metrics.rangeLatency, separateIndexLatency ? 1 : 8);
        assertLatency(metrics.indexLatency, separateIndexLatency ? 9 : 0);

        // queries with index hints
        execute("SELECT * FROM %s WHERE v1 = 0 ALLOW FILTERING WITH excluded_indexes={" + idx1 + '}');
        execute("SELECT * FROM %s WHERE v2 = 0 ALLOW FILTERING WITH excluded_indexes={" + idx2 + '}');
        assertLatency(metrics.readLatency, separateIndexLatency ? 2 : 4);
        assertLatency(metrics.rangeLatency, separateIndexLatency ? 3 : 10);
        assertLatency(metrics.indexLatency, separateIndexLatency ? 9 : 0);
    }

    private void assertLatency(TableMetrics.TableLatencyMetrics metrics, int expectedCount)
    {
        Assertions.assertThat(metrics.tableOrKeyspaceMetric().latency.getCount()).isEqualTo(expectedCount);
    }
}
