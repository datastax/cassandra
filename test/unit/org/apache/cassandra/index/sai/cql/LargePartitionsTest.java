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

import java.util.Collection;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.index.sai.SAIUtil;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.assertj.core.api.Assertions;

@RunWith(Parameterized.class)
public class LargePartitionsTest extends VectorTester
{
    public static final int NUM_PARTITIONS = 10;
    public static final int LARGE_PARTITION_SIZE = CassandraRelevantProperties.SAI_PARTITION_ROW_BATCH_SIZE.getInt() * 4;

    @Parameterized.Parameter
    public Version version;

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data()
    {
        return Version.ALL.stream().map(v -> new Object[]{ v }).collect(Collectors.toList());
    }

    @Before
    public void setup() throws Throwable
    {
        super.setup();
        SAIUtil.setCurrentVersion(version);
    }

    @Test
    public void testLargePartition() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, n int, t text, a text, v vector<float, 2>, PRIMARY KEY (k, c))");
        createIndex("CREATE CUSTOM INDEX ON %s(n) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(t) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(a) USING 'StorageAttachedIndex' WITH OPTIONS = {'index_analyzer': 'standard'}");
        if (version.onOrAfter(Version.JVECTOR_EARLIEST))
            createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");

        for (int k = 0; k < NUM_PARTITIONS; k++)
        {
            for (int c = 0; c < LARGE_PARTITION_SIZE; c++)
            {
                int n = c % 2;
                String t = "t_" + n;
                String a = "other a_" + n;
                Vector<Float> v = vector(1.0f, n);
                execute("INSERT INTO %s (k, c, n, t, a, v) VALUES (?, ?, ?, ?, ?, ?)", k, c, n, t, a, v);
            }
        }

        beforeAndAfterFlush( () -> {

            // test filtering with single partition queries
            int numExpectedRows = LARGE_PARTITION_SIZE / 2;
            Assertions.assertThat(execute("SELECT * FROM %s WHERE k = 2 AND n = 1").size()).isEqualTo(numExpectedRows);
            Assertions.assertThat(execute("SELECT * FROM %s WHERE k = 2 AND t = 't_1'").size()).isEqualTo(numExpectedRows);
            Assertions.assertThat(execute("SELECT * FROM %s WHERE k = 2 AND a = 'a_1'").size()).isEqualTo(numExpectedRows);

            // test filtering with range queries
            numExpectedRows = NUM_PARTITIONS * LARGE_PARTITION_SIZE / 2;
            Assertions.assertThat(execute("SELECT * FROM %s WHERE n = 1").size()).isEqualTo(numExpectedRows);
            Assertions.assertThat(execute("SELECT * FROM %s WHERE t = 't_1'").size()).isEqualTo(numExpectedRows);
            Assertions.assertThat(execute("SELECT * FROM %s WHERE a = 'a_1'").size()).isEqualTo(numExpectedRows);

            // the generic ORDER BY
            if (version.after(Version.AA))
            {
                Assertions.assertThat(execute("SELECT * FROM %s WHERE k = 2 ORDER BY n LIMIT " + LARGE_PARTITION_SIZE).size()).isEqualTo(LARGE_PARTITION_SIZE);
                Assertions.assertThat(execute("SELECT * FROM %s WHERE k = 2 ORDER BY t LIMIT " + LARGE_PARTITION_SIZE).size()).isEqualTo(LARGE_PARTITION_SIZE);

                Assertions.assertThat(execute("SELECT * FROM %s ORDER BY n LIMIT " + LARGE_PARTITION_SIZE).size()).isEqualTo(LARGE_PARTITION_SIZE);
                Assertions.assertThat(execute("SELECT * FROM %s ORDER BY t LIMIT " + LARGE_PARTITION_SIZE).size()).isEqualTo(LARGE_PARTITION_SIZE);
            }

            // test ANN
            if (version.onOrAfter(Version.JVECTOR_EARLIEST))
            {
                int limit = CassandraRelevantProperties.SAI_PARTITION_ROW_BATCH_SIZE.getInt() + 1;
                Assertions.assertThat(execute("SELECT * FROM %s WHERE k = 2 ORDER BY v ANN OF [1, 1] LIMIT " + limit).size()).isEqualTo(limit);
                Assertions.assertThat(execute("SELECT * FROM %s ORDER BY v ANN OF [1, 1] LIMIT " + limit).size()).isEqualTo(limit);
            }
        });
    }
}
