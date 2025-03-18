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

package org.apache.cassandra.distributed.test;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.distributed.test.sai.SAIUtil;
import org.apache.cassandra.service.QueryInfoTrackerTest.TestQueryInfoTracker;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.reads.repair.ReadRepairStrategy;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;

public class QueryInfoTrackerDistributedTest extends TestBaseImpl
{
    private static Cluster cluster;
    private final static String rfOneKs = "rfoneks";

    @BeforeClass
    public static void setupCluster() throws Throwable
    {
        cluster = init(Cluster.build().withNodes(3).withConfig(config -> config.with(NETWORK, GOSSIP)).start());
        cluster.schemaChange("CREATE KEYSPACE " + rfOneKs +
                             " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};");
    }

    @AfterClass
    public static void close() throws Exception
    {
        cluster.close();
        cluster = null;
    }

    @Test
    @SuppressWarnings("rawtypes")
    public void testTrackingInDataResolverResolve()
    {
        ReadRepairTester tester = new ReadRepairTester(cluster, ReadRepairStrategy.BLOCKING, 1, false, false, false)
        {
            @Override
            ReadRepairTester self()
            {
                return this;
            }
        };

        String keyspace = tester.qualifiedTableName.split("\\.")[0];

        tester.createTable("CREATE TABLE %s (pk int, ck int, v int, PRIMARY KEY (pk, ck))");
        cluster.coordinator(1).execute("INSERT INTO " + tester.qualifiedTableName + " (pk, ck, v) VALUES (1, 1, 1)",
                                       ConsistencyLevel.QUORUM);

        tester.mutate(2, "INSERT INTO %s (pk, ck, v) VALUES (1, 1, 2)");

        setQueryTracker(tester.coordinator, keyspace);

        tester.assertRowsDistributed("SELECT * FROM %s WHERE pk=1 AND ck=1",
                                     2,
                                     row(1, 1, 2));

        assertQueryTracker(tester.coordinator, tracker -> {
            Assert.assertEquals(1, tracker.reads.get());
            Assert.assertEquals(1, tracker.readPartitions.get());
            Assert.assertEquals(1, tracker.readRows.get());
            Assert.assertEquals(1, tracker.replicaPlans.get());
        });
    }

    @Test
    public void testTrackingInDigestResolverGetData()
    {
        cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");
        cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1)",
                                       ConsistencyLevel.QUORUM);

        setQueryTracker(1, KEYSPACE);

        assertRows(cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1",
                                                  ConsistencyLevel.QUORUM),
                   row(1, 1, 1));

        assertQueryTracker(1, tracker -> {
            Assert.assertEquals(1, tracker.reads.get());
            Assert.assertEquals(1, tracker.readPartitions.get());
            Assert.assertEquals(1, tracker.readRows.get());
            Assert.assertEquals(1, tracker.replicaPlans.get());
        });
    }

    @Test
    public void testTrackingReadsWithEndpointGrouping()
    {
        String table = rfOneKs + ".saiTbl";
        cluster.schemaChange("CREATE TABLE " + table + " (id1 TEXT PRIMARY KEY, v1 INT, v2 TEXT)");
        cluster.schemaChange("CREATE CUSTOM INDEX IF NOT EXISTS test_idx ON " + table + " (v1) USING 'StorageAttachedIndex'");
        SAIUtil.waitForIndexQueryable(cluster, KEYSPACE);

        int rowsCount = 1000;

        for (int i = 0; i < rowsCount; ++i)
        {
            cluster.coordinator(1).execute("INSERT INTO " + table + " (id1, v1, v2) VALUES (?, ?, ?);",
                                           ConsistencyLevel.QUORUM,
                                           String.valueOf(i),
                                           i,
                                           String.valueOf(i));
        }

        setQueryTracker(1, rfOneKs);

        cluster.coordinator(1).execute(String.format("SELECT id1 FROM %s WHERE v1>=0", table),
                                       ConsistencyLevel.QUORUM);

        assertQueryTracker(1, tracker -> {
            Assert.assertEquals(1, tracker.rangeReads.get());
            Assert.assertEquals(rowsCount, tracker.readPartitions.get());
            Assert.assertEquals(rowsCount, tracker.readRows.get());
            Assert.assertEquals(4, tracker.replicaPlans.get());
        });
    }

    @Test
    public void testANNQueryWithIndexRestrictionAndLIMIT()
    {
        String table = rfOneKs + ".ann_table";
        cluster.schemaChange("CREATE TABLE " + table + " (p int PRIMARY KEY, v int, ni int, vec VECTOR<FLOAT, 2>)");
        cluster.schemaChange("CREATE CUSTOM INDEX ON " + table + "(vec) USING 'StorageAttachedIndex'");
        cluster.schemaChange("CREATE CUSTOM INDEX ON " + table + "(v) USING 'StorageAttachedIndex'");
        SAIUtil.waitForIndexQueryable(cluster, rfOneKs);

        for (int rowIdx = 0; rowIdx < 100; rowIdx++)
        {
            cluster.coordinator(1).execute("INSERT INTO " + table + "(p, v, ni, vec) VALUES (?, ?, ?, [0.5, 0.3])",
                                           ConsistencyLevel.ALL, rowIdx, rowIdx, rowIdx);
        }

        setQueryTracker(1, rfOneKs);

        cluster.coordinator(1).execute("SELECT * FROM " + table + " WHERE v > 50 ORDER BY vec ANN OF [0.1, 0.9] LIMIT 3",
                                       ConsistencyLevel.ONE);

        assertQueryTracker(1, tracker -> {
            Assert.assertEquals(1, tracker.rangeReads.get());
            Assert.assertEquals(3, tracker.readFilteredRows.get());
        });
    }

    private void setQueryTracker(int node, String keyspace)
    {
        cluster.get(node).runOnInstance(() -> StorageProxy.instance.registerQueryTracker(new TestQueryInfoTracker(keyspace)));
    }

    private void assertQueryTracker(int node, IIsolatedExecutor.SerializableConsumer<TestQueryInfoTracker> tester)
    {
        cluster.get(node).runOnInstance(() -> {
            TestQueryInfoTracker tracker = (TestQueryInfoTracker) StorageProxy.queryTracker();
            tester.accept(tracker);
        });
    }
}
