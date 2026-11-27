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

package org.apache.cassandra.fuzz.harry.integration;

import org.junit.Test;

import com.datastax.driver.core.Session;
import org.apache.cassandra.harry.SchemaSpec;
import org.apache.cassandra.harry.dsl.HistoryBuilder;
import org.apache.cassandra.harry.dsl.ReplayingHistoryBuilder;
import org.apache.cassandra.harry.execution.DriverVisitExecutor;
import org.apache.cassandra.harry.gen.SchemaGenerators;
import org.apache.cassandra.harry.gen.rng.JdkRandomEntropySource;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class DriverSoakRunnerTest extends DriverSoakRunnerTestBase
{
    @Test
    public void testSoak()
    {
        // Rotates twice, so that it also validates and drops two whole tables
        assertPasses("--seed", "1", "--max-visits", "6000", "--rotate-every", "2500", "--sai", "off");
    }

    @Test
    public void testMismatchIsReported()
    {
        SchemaSpec schema = SchemaGenerators.trivialSchema(KEYSPACE, "mismatch", 100).generate(new JdkRandomEntropySource(1));
        cluster.schemaChange("CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
        cluster.schemaChange(schema.compile());

        try (com.datastax.driver.core.Cluster driver = com.datastax.driver.core.Cluster.builder()
                                                                                      .addContactPoint(cluster.get(1).broadcastAddress().getHostString())
                                                                                      .withoutJMXReporting()
                                                                                      .build();
             Session session = driver.connect())
        {
            HistoryBuilder history = new ReplayingHistoryBuilder(schema.valueGenerators,
                                                                 hb -> DriverVisitExecutor.builder().build(schema, hb, session));
            history.insert(0, 0);
            history.insert(0, 1);
            history.selectPartition(0);

            // Data the model does not know is gone
            cluster.schemaChange("TRUNCATE " + schema.keyspace + '.' + schema.table);
            assertThatThrownBy(() -> history.selectPartition(0))
            .isInstanceOf(DriverVisitExecutor.ValidationMismatchException.class)
            .hasMessageContaining("at visit 3")
            .hasMessageContaining(schema.table);
        }
    }
}
