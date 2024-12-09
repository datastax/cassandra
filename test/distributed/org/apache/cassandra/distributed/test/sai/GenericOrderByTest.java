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

package org.apache.cassandra.distributed.test.sai;

import java.util.Arrays;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.test.TestBaseImpl;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;

/**
 * Test for generic ORDER BY queries with SAI.
 */
public class GenericOrderByTest extends TestBaseImpl
{
    private static final int NUM_REPLICAS = 3;
    private static final int RF = 2;

    @Test
    public void testOrderBy() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(NUM_REPLICAS)
                                           .withConfig(config -> config.with(GOSSIP).with(NETWORK))
                                           .start(), RF))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.t(k int, c int, v int, PRIMARY KEY(k, c))"));
            cluster.schemaChange(withKeyspace("CREATE CUSTOM INDEX ON %s.t(v) USING 'StorageAttachedIndex'"));
            SAIUtil.waitForIndexQueryable(cluster, KEYSPACE);

            ICoordinator coordinator = cluster.coordinator(1);

            String insertQuery = withKeyspace("INSERT INTO %s.t(k, c, v) VALUES (?, ?, ?)");
            coordinator.execute(insertQuery, ConsistencyLevel.ALL, 1, 1, 1);
            coordinator.execute(insertQuery, ConsistencyLevel.ALL, 1, 2, 8);
            coordinator.execute(insertQuery, ConsistencyLevel.ALL, 1, 3, 3);
            coordinator.execute(insertQuery, ConsistencyLevel.ALL, 2, 1, 6);
            coordinator.execute(insertQuery, ConsistencyLevel.ALL, 2, 2, 5);
            coordinator.execute(insertQuery, ConsistencyLevel.ALL, 2, 3, 4);
            coordinator.execute(insertQuery, ConsistencyLevel.ALL, 3, 1, 7);
            coordinator.execute(insertQuery, ConsistencyLevel.ALL, 3, 2, 2);
            coordinator.execute(insertQuery, ConsistencyLevel.ALL, 3, 3, 9);

            assertRowsWithLimit(cluster, "SELECT * FROM %s.t ORDER BY v ASC",
                                row(1, 1, 1),
                                row(3, 2, 2),
                                row(1, 3, 3),
                                row(2, 3, 4),
                                row(2, 2, 5),
                                row(2, 1, 6),
                                row(3, 1, 7),
                                row(1, 2, 8),
                                row(3, 3, 9));
            assertRowsWithLimit(cluster, "SELECT * FROM %s.t ORDER BY v DESC",
                                row(3, 3, 9),
                                row(1, 2, 8),
                                row(3, 1, 7),
                                row(2, 1, 6),
                                row(2, 2, 5),
                                row(2, 3, 4),
                                row(1, 3, 3),
                                row(3, 2, 2),
                                row(1, 1, 1));

            // with partition key restriction
            assertRowsWithLimit(cluster, "SELECT * FROM %s.t WHERE k=1 ORDER BY v ASC",
                                row(1, 1, 1),
                                row(1, 3, 3),
                                row(1, 2, 8));
            assertRowsWithLimit(cluster, "SELECT * FROM %s.t WHERE k=1 ORDER BY v DESC",
                                row(1, 2, 8),
                                row(1, 3, 3),
                                row(1, 1, 1));
            assertRowsWithLimit(cluster, "SELECT * FROM %s.t WHERE k=2 ORDER BY v ASC",
                                row(2, 3, 4),
                                row(2, 2, 5),
                                row(2, 1, 6));
            assertRowsWithLimit(cluster, "SELECT * FROM %s.t WHERE k=2 ORDER BY v DESC",
                                row(2, 1, 6),
                                row(2, 2, 5),
                                row(2, 3, 4));
            assertRowsWithLimit(cluster, "SELECT * FROM %s.t WHERE k=3 ORDER BY v ASC",
                                row(3, 2, 2),
                                row(3, 1, 7),
                                row(3, 3, 9));
            assertRowsWithLimit(cluster, "SELECT * FROM %s.t WHERE k=3 ORDER BY v DESC",
                                row(3, 3, 9),
                                row(3, 1, 7),
                                row(3, 2, 2));

            // with indexed column filter
            cluster.schemaChange(withKeyspace("CREATE CUSTOM INDEX ON %s.t(c) USING 'StorageAttachedIndex'"));
            SAIUtil.waitForIndexQueryable(cluster, KEYSPACE);
            assertRowsWithLimit(cluster, "SELECT * FROM %s.t WHERE c=1 ORDER BY v ASC",
                                row(1, 1, 1),
                                row(2, 1, 6),
                                row(3, 1, 7));
            assertRowsWithLimit(cluster, "SELECT * FROM %s.t WHERE c=1 ORDER BY v DESC",
                                row(3, 1, 7),
                                row(2, 1, 6),
                                row(1, 1, 1));
            assertRowsWithLimit(cluster, "SELECT * FROM %s.t WHERE c=2 ORDER BY v ASC",
                                row(3, 2, 2),
                                row(2, 2, 5),
                                row(1, 2, 8));
            assertRowsWithLimit(cluster, "SELECT * FROM %s.t WHERE c=2 ORDER BY v DESC",
                                row(1, 2, 8),
                                row(2, 2, 5),
                                row(3, 2, 2));
            assertRowsWithLimit(cluster, "SELECT * FROM %s.t WHERE c=3 ORDER BY v ASC",
                                row(1, 3, 3),
                                row(2, 3, 4),
                                row(3, 3, 9));
            assertRowsWithLimit(cluster, "SELECT * FROM %s.t WHERE c=3 ORDER BY v DESC",
                                row(3, 3, 9),
                                row(2, 3, 4),
                                row(1, 3, 3));
        }
    }

    private void assertRowsWithLimit(Cluster cluster, String query, Object[]... expected)
    {
        for (int node = 1; node <= cluster.size(); node++)
        {
            assertRowsWithLimit(cluster.coordinator(node), query, expected);
        }
    }

    private void assertRowsWithLimit(ICoordinator coordinator, String query, Object[]... expected)
    {
        for (int limit = 1; limit <= expected.length; limit++)
        {
            String queryWithLimit = withKeyspace(query) + " LIMIT " + limit;
            Object[][] expectedWithLimit = Arrays.copyOfRange(expected, 0, limit);
            assertRows(coordinator.execute(queryWithLimit, ConsistencyLevel.ONE), expectedWithLimit);
        }
    }
}
