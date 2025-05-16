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

import java.net.InetSocketAddress;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.locator.DynamicEndpointSnitch;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertEquals;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertTrue;

public class CounterLeaderDynamicSnitchTest extends TestBaseImpl
{
    @BeforeClass
    public static void init()
    {
        CassandraRelevantProperties.USE_DYNAMIC_SNITCH_FOR_COUNTER_LEADER.setBoolean(true);
        // test latency could be lower than 1ms. Disable it for before accuracy
        CassandraRelevantProperties.DYNAMIC_ENDPOINT_SNITCH_QUANTIZE_TO_MILLIS.setBoolean(false);
    }

    @AfterClass
    public static void cleanup()
    {
        CassandraRelevantProperties.USE_DYNAMIC_SNITCH_FOR_COUNTER_LEADER.reset();
        CassandraRelevantProperties.DYNAMIC_ENDPOINT_SNITCH_QUANTIZE_TO_MILLIS.reset();
    }

    @Test
    public void testDynamicSnitchScore() throws Throwable
    {
        testDynamicSnitchScore(false);
    }

    @Test
    public void testDynamicSnitchScoreWithTimeout() throws Throwable
    {
        testDynamicSnitchScore(true);
    }

    private void testDynamicSnitchScore(boolean remoteReplicaTimeout) throws Throwable
    {
        try (Cluster cluster = Cluster.build(2).withConfig(c -> c.with(GOSSIP, NATIVE_PROTOCOL)
                                                                 // effectively disable auto-update
                                                                 .set("dynamic_snitch_update_interval_in_ms", "3600000")
                                                                 .set("dynamic_snitch", "true")).start())
        {
            cluster.schemaChange("CREATE KEYSPACE k WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");

            String createTable = "CREATE TABLE k.t (k int, c int, total counter, PRIMARY KEY (k, c))";
            cluster.schemaChange(createTable);

            ConsistencyLevel cl = ConsistencyLevel.ONE;

            int coordinator = 1;
            // before executing counter requests: no score on the coordinator
            cluster.get(coordinator).runOnInstance(() -> {
                DynamicEndpointSnitch snitch = (DynamicEndpointSnitch) DatabaseDescriptor.getEndpointSnitch();
                snitch.updateScores();
                assertTrue("Expect 0 scores, but got " + snitch.getScores(), snitch.getScores().isEmpty());
            });

            if (remoteReplicaTimeout)
            {
                // simulate timeout on remote replica
                cluster.filters().verbs(Verb.COUNTER_MUTATION_REQ.id).from(coordinator).to(2).drop();
            }

            int failures = 0;
            int requests = 10;
            for (int key = 0; key < requests; key++)
            {
                try
                {
                    cluster.coordinator(coordinator).execute("UPDATE k.t SET total = total + 1 WHERE k = ? AND c = 1", cl, key);
                }
                catch (Throwable t)
                {
                    failures++;
                }
            }

            if (remoteReplicaTimeout)
                assertTrue("Expected remote counter leader failure " + failures, failures > 0 && failures < requests);
            else
                assertTrue("Expected no remote counter leader failure " + failures, failures == 0);

            // after executing counter requests: 1 score for remote replica on the coordinator
            InetSocketAddress remoteReplica = cluster.get(2).broadcastAddress();
            cluster.get(coordinator).runOnInstance(() -> {
                DynamicEndpointSnitch snitch = (DynamicEndpointSnitch) DatabaseDescriptor.getEndpointSnitch();
                snitch.updateScores();
                if (remoteReplicaTimeout)
                {
                    assertEquals("Expect 1 score for remote replica, but got " + snitch.getScores(), snitch.getScores().size(), 1);
                    if (!snitch.getScores().get(remoteReplica.getAddress()).equals(1.0))
                        throw new RuntimeException("Expect 1.0 max score for remote replica, but got " + snitch.getScores().get(remoteReplica.getAddress()));
                }
                else
                {
                    assertEquals("Expect no score for remote replica, but got " + snitch.getScores(), snitch.getScores().size(), 0);
                }
            });
        }
    }

    @Test
    public void testApplyDynamicSnitch() throws Throwable
    {
        try (Cluster cluster = Cluster.build(3).withConfig(c -> c.with(GOSSIP, NATIVE_PROTOCOL)
                                                                 // effectively disable auto-update
                                                                 .set("dynamic_snitch_update_interval_in_ms", "3600000")
                                                                 .set("dynamic_snitch", "true")).start())
        {
            cluster.schemaChange("CREATE KEYSPACE k WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}");

            String createTable = "CREATE TABLE k.t (k int, c int, total counter, PRIMARY KEY (k, c))";
            cluster.schemaChange(createTable);

            ConsistencyLevel cl = ConsistencyLevel.ALL;

            int coordinator = 1;
            // before executing counter requests: no score on the coordinator
            cluster.get(coordinator).runOnInstance(() -> {
                DynamicEndpointSnitch snitch = (DynamicEndpointSnitch) DatabaseDescriptor.getEndpointSnitch();
                snitch.updateScores();
                assertTrue("Expect 0 scores, but got " + snitch.getScores(), snitch.getScores().isEmpty());
            });

            // fail if node2 is selected as counter leader
            cluster.filters().verbs(Verb.COUNTER_MUTATION_REQ.id).from(coordinator).to(2).drop();
            int failures = 0;
            int requests = 100;
            int idx = 0;
            while (failures == 0 && idx++ < requests)
            {
                try
                {
                    cluster.coordinator(coordinator).execute("UPDATE k.t SET total = total + 1 WHERE k = ? AND c = 1", cl, idx);
                }
                catch (Throwable t)
                {
                    failures++;
                }
            }
            assertTrue("Expected node2 failure " + failures, failures > 0);

            // wait for callback expired
            FBUtilities.sleepQuietly(2000);

            // update dynamic snitch score: subsequent request should avoid coordinator as counter leader
            InetSocketAddress remoteReplica2 = cluster.get(2).broadcastAddress();
            cluster.get(coordinator).runOnInstance(() -> {
                DynamicEndpointSnitch snitch = (DynamicEndpointSnitch) DatabaseDescriptor.getEndpointSnitch();
                snitch.updateScores();
                assertEquals("Expect 1 score from expired request, but got " + snitch.getScores(), snitch.getScores().size(), 1);
                if (!snitch.getScores().get(remoteReplica2.getAddress()).equals(1.0))
                    throw new RuntimeException("Expect 1.0 max score for node2, but got " + snitch.getScores().get(remoteReplica2.getAddress()));
            });

            // subsequent requests should not select node2 as leader
            for (int key = 0; key < requests; key++)
                cluster.coordinator(coordinator).execute("UPDATE k.t SET total = total + 1 WHERE k = ? AND c = 1", cl, key);
        }
    }
}
