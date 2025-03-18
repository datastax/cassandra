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

package org.apache.cassandra.distributed.test.sai.allowfiltering;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.AfterClass;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.test.index.IndexTestBase;
import org.apache.cassandra.index.Index;
import org.assertj.core.api.Assertions;

import static java.lang.String.format;

/**
 * Distributed tests for ALLOW FILTERING during index build.
 * </p>
 * Implementors should provide the {@link #cluster} setup.
 */
public abstract class AllowFilteringDuringIndexBuildTester extends IndexTestBase
{
    protected static final String INDEX_NOT_AVAILABLE_MESSAGE = "^Operation failed - received 0 responses" +
                                                                " and 2 failures: INDEX_NOT_AVAILABLE from .+" +
                                                                " INDEX_NOT_AVAILABLE from .+$";
    protected static final int NUM_REPLICAS = 2;
    protected static final int RF = 2;

    private static final AtomicInteger SEQ = new AtomicInteger();
    protected static Cluster cluster;

    @AfterClass
    public static void closeCluster()
    {
        if (cluster != null)
            cluster.close();
    }

    protected void testSelectWithAllowFilteringDuringIndexBuilding(String expectedErrorMessage,
                                                                   boolean isInitialBuild,
                                                                   boolean isNewTable)
    {
        if (isInitialBuild && isNewTable)
            throw new IllegalArgumentException("Initial build cannot happen with a new table");

        String table = "table_" + SEQ.getAndIncrement();
        String index = table + "_idx";

        cluster.schemaChange(format("CREATE TABLE %s.%s (k int PRIMARY KEY, n int, v vector<float, 2>)", KEYSPACE, table));
        cluster.schemaChange(format("CREATE CUSTOM INDEX %s ON %s.%s(n) USING 'StorageAttachedIndex'", index, KEYSPACE, table));

        Index.Status expectedStatus = isInitialBuild ? Index.Status.INITIAL_BUILD_STARTED : Index.Status.FULL_REBUILD_STARTED;

        for (int i = 1; i <= cluster.size(); i++)
            markIndexBuilding(cluster.get(i), KEYSPACE, table, index, isInitialBuild, isNewTable);

        for (int i = 1; i <= cluster.size(); i++)
            for (int j = 1; j <= cluster.size(); j++)
                waitForIndexingStatus(cluster.get(i), KEYSPACE, index, cluster.get(j), expectedStatus);

        String select = format("SELECT * FROM %s.%s WHERE n = 1 ALLOW FILTERING", KEYSPACE, table);

        for (int i = 1; i <= cluster.size(); i++)
        {
            ICoordinator coordinator = cluster.coordinator(i);
            if (expectedErrorMessage == null)
            {
                coordinator.execute(select, ConsistencyLevel.ONE);
            }
            else
            {
                Assertions.assertThatThrownBy(() -> coordinator.execute(select, ConsistencyLevel.ONE))
                          .hasMessageMatching(expectedErrorMessage);
            }
        }

        cluster.schemaChange(format("DROP TABLE IF EXISTS %s.%s", KEYSPACE, table));
    }
}
