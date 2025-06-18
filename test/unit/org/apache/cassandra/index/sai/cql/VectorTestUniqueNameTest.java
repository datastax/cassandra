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

package org.apache.cassandra.index.sai.cql;

import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.index.sai.SAIUtil;
import org.apache.cassandra.index.sai.disk.format.Version;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(Parameterized.class)
public class VectorTestUniqueNameTest extends VectorTester
{
    @Parameterized.Parameter
    public Version version;

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data()
    {
        return Stream.of(Version.CA, Version.DC).map(v -> new Object[]{ v}).collect(Collectors.toList());
    }

    @BeforeClass
    public static void setUpClass()
    {
        System.setProperty("cassandra.custom_tracing_class", "org.apache.cassandra.tracing.TracingTestImpl");
        VectorTester.setUpClass();
    }

    @Before
    public void before() throws Throwable
    {
        SAIUtil.setCurrentVersion(version);
    }

    @Test
    public void repeatingTestForInsertingVectors() throws Throwable
    {
        for (int i = 0; i < 100; i++)
        {
            createTable("CREATE TABLE %s (id text PRIMARY KEY, embedding vector<float, 5>)");
            createIndex("CREATE CUSTOM INDEX ON %s(embedding) USING 'StorageAttachedIndex' " +
                        "WITH OPTIONS = {'similarity_function': 'dot_product', 'source_model': 'OTHER'}");

            // Insert initial data
            execute("INSERT INTO %s (id, embedding) VALUES ('row1', [0.1, 0.1, 0.1, 0.1, 0.1])");
            execute("INSERT INTO %s (id, embedding) VALUES ('row2', [0.9, 0.9, 0.9, 0.9, 0.9])");

            // Initial vector search
            UntypedResultSet initialSearch = execute("SELECT * FROM %s ORDER BY embedding ANN OF [0.8, 0.8, 0.8, 0.8, 0.8] LIMIT 1");
            assertThat(initialSearch).hasSize(1);

            // Update one of the rows
            execute("UPDATE %s SET embedding = [0.7, 0.7, 0.7, 0.7, 0.7] WHERE id = 'row1'");

            for (int j = 0; j < 10; j++)
            {
                // Get all data to verify we have 2 rows
                UntypedResultSet allData = execute("SELECT * FROM %s ORDER BY embedding ANN OF [0.8, 0.8, 0.8, 0.8, 0.8] LIMIT 1000");
                assertThat(allData).hasSize(2);
            }
        }
    }
}
