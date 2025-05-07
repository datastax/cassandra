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

package org.apache.cassandra.db.memtable;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.awaitility.Awaitility;

import static org.assertj.core.api.Assertions.assertThat;

public class FlushConfigTest extends CQLTester
{
    @Before
    public void setup() throws Throwable
    {
        CassandraRelevantProperties.FLUSH_PERIOD_IN_MILLIS.reset();

        CassandraRelevantProperties.SAI_VECTOR_FLUSH_PERIOD_IN_MILLIS.reset();
        CassandraRelevantProperties.SAI_VECTOR_FLUSH_THRESHOLD_MAX_ROWS.reset();

        CassandraRelevantProperties.SAI_NON_VECTOR_FLUSH_PERIOD_IN_MILLIS.reset();
        CassandraRelevantProperties.SAI_NON_VECTOR_FLUSH_THRESHOLD_MAX_ROWS.reset();
    }

    @Test
    public void testFlushPeriod()
    {
        CassandraRelevantProperties.FLUSH_PERIOD_IN_MILLIS.setInt(5000);
        createTable("CREATE TABLE %s (pk int, v1 int, v2 text, PRIMARY KEY(pk))");

        int rowCount = 15;
        for (int i = 1; i <= rowCount; i++)
            execute("INSERT INTO %s (pk, v1, v2) VALUES (?, ?, ?)", i, i, String.valueOf(i));

        Awaitility.await("Memtable flushed")
                  .atMost(30, TimeUnit.SECONDS)
                  .untilAsserted(() -> assertThat(getCurrentColumnFamilyStore().getLiveSSTables()).hasSize(1));
    }

    @Test
    public void testTrieIndexFlushThreshold()
    {
        int maxRows = 10;
        CassandraRelevantProperties.SAI_NON_VECTOR_FLUSH_THRESHOLD_MAX_ROWS.setInt(maxRows);

        createTable("CREATE TABLE %s (pk int, v1 int, v2 text, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(v1) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(v2) USING 'StorageAttachedIndex'");

        int rowCount = 25;
        for (int i = 1; i <= rowCount; i++)
        {
            execute("INSERT INTO %s (pk, v1, v2) VALUES (?, ?, ?)", i, i, String.valueOf(i));

            int sstables = (i / maxRows);
            if (i % maxRows == 0)
            {
                Awaitility.await("Memtable flushed")
                          .atMost(30, TimeUnit.SECONDS)
                          .untilAsserted(() -> assertThat(getCurrentColumnFamilyStore().getLiveSSTables()).hasSize(sstables));
            }
            else
            {
                assertThat(getCurrentColumnFamilyStore().getLiveSSTables()).hasSize(sstables);
            }
        }

        // flush remaining memtable
        getCurrentColumnFamilyStore().forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);
        assertThat(getCurrentColumnFamilyStore().getLiveSSTables()).hasSize(3);
    }

    @Test
    public void testTrieIndexFlushPeriod()
    {
        CassandraRelevantProperties.SAI_NON_VECTOR_FLUSH_PERIOD_IN_MILLIS.setInt(5000);
        createTable("CREATE TABLE %s (pk int, v1 int, v2 text, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(v1) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(v2) USING 'StorageAttachedIndex'");

        int rowCount = 15;
        for (int i = 1; i <= rowCount; i++)
            execute("INSERT INTO %s (pk, v1, v2) VALUES (?, ?, ?)", i, i, String.valueOf(i));

        Awaitility.await("Memtable flushed")
                  .atMost(30, TimeUnit.SECONDS)
                  .untilAsserted(() -> assertThat(getCurrentColumnFamilyStore().getLiveSSTables()).hasSize(1));
    }


    @Test
    public void testVectorFlushThreshold()
    {
        int maxRows = 10;
        CassandraRelevantProperties.SAI_VECTOR_FLUSH_THRESHOLD_MAX_ROWS.setInt(maxRows);

        int dimension = 2048;
        createTable(String.format("CREATE TABLE %%s (pk int, str_val text, val vector<float, %d>, PRIMARY KEY(pk))", dimension));
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");

        int vectorCount = 25;
        List<Vector<Float>> vectors = IntStream.range(0, vectorCount).mapToObj(s -> randomVectorBoxed(dimension)).collect(Collectors.toList());

        int pk = 0;
        for (int i = 0; i < vectorCount; i++)
        {
            execute("INSERT INTO %s (pk, str_val, val) VALUES (?, 'A', ?)", pk++, vectors.get(i));

            int sstables = pk / maxRows;
            if (pk % maxRows == 0)
            {
                Awaitility.await("Memtable flushed")
                          .atMost(30, TimeUnit.SECONDS)
                          .untilAsserted(() -> assertThat(getCurrentColumnFamilyStore().getLiveSSTables()).hasSize(sstables));
            }
            else
            {
                assertThat(getCurrentColumnFamilyStore().getLiveSSTables()).hasSize(sstables);
            }
        }
        // flush remaining memtable
        getCurrentColumnFamilyStore().forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);
        assertThat(getCurrentColumnFamilyStore().getLiveSSTables()).hasSize(3);
    }

    @Test
    public void testVectorFlushPeriod()
    {
        CassandraRelevantProperties.SAI_VECTOR_FLUSH_PERIOD_IN_MILLIS.setInt(5000);
        int dimension = 2048;
        createTable(String.format("CREATE TABLE %%s (pk int, str_val text, val vector<float, %d>, PRIMARY KEY(pk))", dimension));
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");

        int vectorCount = 15;
        List<Vector<Float>> vectors = IntStream.range(0, vectorCount).mapToObj(s -> randomVectorBoxed(dimension)).collect(Collectors.toList());

        int pk = 0;
        for (Vector<Float> vector : vectors)
            execute("INSERT INTO %s (pk, str_val, val) VALUES (?, 'A', ?)", pk++, vector);

        Awaitility.await("Memtable flushed")
                  .atMost(30, TimeUnit.SECONDS)
                  .untilAsserted(() -> assertThat(getCurrentColumnFamilyStore().getLiveSSTables()).hasSize(1));
    }

    @Test
    public void testMultipleFlushPeriodConfigs()
    {
        testMultipleFlushPeriodConfigs(5000, 6000, 7000, 8000,
                                       5000,
                                       5000,
                                       5000);
        testMultipleFlushPeriodConfigs(8000, 7000, 6000, 5000,
                                       7000,
                                       6000,
                                       5000);
        testMultipleFlushPeriodConfigs(-1, 7000, -1, -1,
                                       7000,
                                       7000,
                                       7000);
        testMultipleFlushPeriodConfigs(8000, -1, 7000, -1,
                                       8000,
                                       7000,
                                       7000);
        testMultipleFlushPeriodConfigs(-1, -1, -1, 5000,
                                       -1,
                                       -1,
                                       5000);
        testMultipleFlushPeriodConfigs(-1, -1, -1, -1,
                                       -1,
                                       -1,
                                       -1);
    }

    private void testMultipleFlushPeriodConfigs(int schemaFlushPeriodInMs, int systemFlushPeriodInMs, int vectorFlushPeriodInMiss, int nonVectorFlushPeriodInMiss,
                                                int withoutIndex, int withVector, int withAllIndexes)
    {
        CassandraRelevantProperties.FLUSH_PERIOD_IN_MILLIS.setInt(systemFlushPeriodInMs);
        CassandraRelevantProperties.SAI_VECTOR_FLUSH_PERIOD_IN_MILLIS.setInt(vectorFlushPeriodInMiss);
        CassandraRelevantProperties.SAI_NON_VECTOR_FLUSH_PERIOD_IN_MILLIS.setInt(nonVectorFlushPeriodInMiss);

        createTable("CREATE TABLE %s (pk int, str_val text, val vector<float, 1024>, PRIMARY KEY(pk)) with MEMTABLE_FLUSH_PERIOD_IN_MS=" + Math.max(0, schemaFlushPeriodInMs));
        int config = Math.min(schemaFlushPeriodInMs, systemFlushPeriodInMs);
        assertThat(getCurrentColumnFamilyStore().getMemtableFlushPeriodInMs()).isEqualTo(withoutIndex);

        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
        config = Math.min(config, vectorFlushPeriodInMiss);
        assertThat(getCurrentColumnFamilyStore().getMemtableFlushPeriodInMs()).isEqualTo(withVector);

        createIndex("CREATE CUSTOM INDEX ON %s(str_val) USING 'StorageAttachedIndex'");
        config = Math.min(config, nonVectorFlushPeriodInMiss);
        assertThat(getCurrentColumnFamilyStore().getMemtableFlushPeriodInMs()).isEqualTo(withAllIndexes);
    }
}
