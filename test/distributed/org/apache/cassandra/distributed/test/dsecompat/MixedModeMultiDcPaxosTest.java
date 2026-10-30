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

package org.apache.cassandra.distributed.test.dsecompat;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Test;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.IMessageFilters;

import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.apache.cassandra.distributed.test.dsecompat.TwoDataCenters.hostId;
import static org.apache.cassandra.distributed.test.dsecompat.TwoDataCenters.localPeers;
import static org.apache.cassandra.distributed.test.dsecompat.TwoDataCenters.networkTopology;
import static org.apache.cassandra.distributed.test.dsecompat.TwoDataCenters.recordRoutes;
import static org.apache.cassandra.distributed.test.dsecompat.TwoDataCenters.remoteNodes;
import static org.apache.cassandra.distributed.test.dsecompat.TwoDataCenters.route;
import static org.apache.cassandra.distributed.test.dsecompat.TwoDataCenters.target;
import static org.apache.cassandra.distributed.test.dsecompat.TwoDataCenters.withSession;
import static org.apache.cassandra.net.Verb.PAXOS_COMMIT_REQ;
import static org.apache.cassandra.net.Verb.PAXOS_PREPARE_REQ;
import static org.apache.cassandra.net.Verb.PAXOS_PROPOSE_REQ;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Lightweight transactions at {@code LOCAL_SERIAL} and {@code SERIAL} in two data centers, between HCD 1.2 nodes at
 * messaging version 11 (standing in for DSE 6) and nodes of this software in {@code DSE_6} mode (Paxos v1 on both), in
 * the layout of {@link TwoDataCenters}, three replicas in each data center. At {@code LOCAL_SERIAL} the prepare and
 * propose rounds stay in the coordinator's data center, but the commit goes to every replica of every data center (as
 * in DSE 6), so an LWT coordinated by DSE reaches the upgraded replicas of the other data center, and the reverse. A
 * replica that is down when the commit is sent gets a hint from the coordinator: this software delivers it to a DSE 6
 * node at version 11. DSE 6 keeps the hints it writes for an upgraded node (it sends hints only to nodes of its schema
 * version), so read repair or the repair after the flag drop brings the write there: the test checks that the HCD 1.2
 * coordinator writes the hint and that a read repairs the row, not what HCD 1.2's dispatch does with the hint, which is
 * not DSE's code.
 * <p>
 * The statements go through the native protocol (the in-JVM coordinator of HCD 1.2 cannot set a serial consistency).
 */
public class MixedModeMultiDcPaxosTest extends DseDialectTestBase
{
    private static final String KS = "ks_lwt";
    private static final String TABLE = KS + ".t";

    @Test
    public void lightweightTransactionsAcrossDataCenters() throws Throwable
    {
        TwoDataCenters.testCase(config -> config.with(NATIVE_PROTOCOL)
                                                .set("hinted_handoff_enabled", true)
                                                .set("hints_flush_period_in_ms", 1000))
                      .setup(cluster -> {
                          cluster.schemaChange("CREATE KEYSPACE " + KS + " WITH replication = " + networkTopology(3, 3));
                          cluster.schemaChange("CREATE TABLE " + TABLE + " (k int PRIMARY KEY, v int)");
                      })
                      .runAfterClusterUpgrade(cluster -> {
                          TwoDataCenters.assertMixed(cluster);
                          // a coordinator of each version in each data center: 2 and 6 run HCD 1.2, 1 and 4 this software
                          int[] coordinators = { 2, 1, 6, 4 };
                          for (int coordinator : coordinators)
                              localSerialThenSerial(cluster, coordinator, 100 * coordinator);

                          commitHintedByUpgradedCoordinator(cluster, 1_000);
                          commitHintedByHcdCoordinator(cluster, 2_000);
                      })
                      .run();
    }

    /**
     * An insert at {@code LOCAL_SERIAL} committed at {@code LOCAL_QUORUM}, then an update at {@code SERIAL} committed at
     * {@code QUORUM}, both through one coordinator (one client session): the prepare and propose messages go to its own
     * data center at {@code LOCAL_SERIAL}, also to the other at {@code SERIAL}, and the commit always reaches the replicas
     * of the other data center, of both versions.
     */
    private static void localSerialThenSerial(UpgradeableCluster cluster, int coordinator, int k)
    {
        withSession(cluster, coordinator, session -> {
            Rounds local = new Rounds(cluster);
            try
            {
                assertThat(lwt(session, "INSERT INTO " + TABLE + " (k, v) VALUES (" + k + ", 1) IF NOT EXISTS",
                               ConsistencyLevel.LOCAL_SERIAL, ConsistencyLevel.LOCAL_QUORUM))
                .as("LOCAL_SERIAL insert through node %s", coordinator).isTrue();
            }
            finally
            {
                local.off();
            }
            for (Set<String> routes : List.of(local.prepares, local.proposes))
                assertThat(routes).as("LOCAL_SERIAL prepares and proposes of node %s stay in its data center", coordinator)
                                  .isNotEmpty()
                                  .allMatch(r -> r.startsWith(coordinator + "->") && localPeers(coordinator).contains(target(r)));
            for (int remote : remoteNodes(coordinator))
                assertThat(local.commits).as("LOCAL_SERIAL commit of node %s", coordinator).contains(route(coordinator, remote));
            assertOnEveryNode(cluster, k, 1);

            Rounds serial = new Rounds(cluster);
            try
            {
                assertThat(lwt(session, "UPDATE " + TABLE + " SET v = 2 WHERE k = " + k + " IF v = 1",
                               ConsistencyLevel.SERIAL, ConsistencyLevel.QUORUM))
                .as("SERIAL update through node %s", coordinator).isTrue();
            }
            finally
            {
                serial.off();
            }
            for (Set<String> routes : List.of(serial.prepares, serial.proposes))
                assertThat(routes).as("SERIAL prepares and proposes of node %s reach the other data center", coordinator)
                                  .anyMatch(r -> r.startsWith(coordinator + "->") && remoteNodes(coordinator).contains(target(r)));
            for (int remote : remoteNodes(coordinator))
                assertThat(serial.commits).as("SERIAL commit of node %s", coordinator).contains(route(coordinator, remote));
            assertOnEveryNode(cluster, k, 2);
            return null;
        });
    }

    /** The routes of the Paxos messages sent while the recorders are on. */
    private static final class Rounds
    {
        final Set<String> prepares = ConcurrentHashMap.newKeySet();
        final Set<String> proposes = ConcurrentHashMap.newKeySet();
        final Set<String> commits = ConcurrentHashMap.newKeySet();
        private final List<IMessageFilters.Filter> recorders;

        Rounds(UpgradeableCluster cluster)
        {
            recorders = List.of(recordRoutes(cluster, PAXOS_PREPARE_REQ, prepares),
                                recordRoutes(cluster, PAXOS_PROPOSE_REQ, proposes),
                                recordRoutes(cluster, PAXOS_COMMIT_REQ, commits));
        }

        void off()
        {
            recorders.forEach(IMessageFilters.Filter::off);
        }
    }

    /**
     * Node 1 (this software) coordinates a {@code LOCAL_SERIAL} insert while node 6 (HCD 1.2, the other data center) is
     * down: the commit to node 6 times out and node 1 writes a hint, which it delivers once node 6 is back (node 1 speaks
     * version 11 with every HCD 1.2 node: {@code TwoDataCenters.assertMixed}).
     */
    private static void commitHintedByUpgradedCoordinator(UpgradeableCluster cluster, int k) throws Exception
    {
        int coordinator = 1;
        int down = 6;
        UUID downHostId = hostId(cluster, down);
        cluster.get(down).shutdown().get();
        assertThat(lwt(cluster, coordinator, "INSERT INTO " + TABLE + " (k, v) VALUES (" + k + ", 1) IF NOT EXISTS",
                       ConsistencyLevel.LOCAL_SERIAL, ConsistencyLevel.LOCAL_QUORUM)).isTrue();
        awaitAsserted(60, () -> assertThat(hintsFiles(cluster, coordinator, downHostId)).as("hints of node 1 for node 6").isNotEmpty());

        cluster.get(down).startup();
        assertRows(cluster.get(down).executeInternal("SELECT v FROM " + TABLE + " WHERE k = ?", k));
        // the in-JVM nodes run without gossip, so the dispatch of hints is started by hand (DseDialectTestBase)
        startHintsDispatch(coordinator);
        awaitAsserted(120, () -> assertRows(cluster.get(down).executeInternal("SELECT v FROM " + TABLE + " WHERE k = ?", k), row(1)));
        awaitAsserted(60, () -> assertThat(hintsFiles(cluster, coordinator, downHostId)).as("hints of node 1 for node 6, delivered").isEmpty());
    }

    /**
     * Node 2 (HCD 1.2, for DSE 6) coordinates a {@code LOCAL_SERIAL} insert while node 5 (this software, the other data
     * center) is down: the commit to node 5 times out and node 2 writes a hint for it. A read at ALL through node 5, once it
     * is back, repairs the row there.
     */
    private static void commitHintedByHcdCoordinator(UpgradeableCluster cluster, int k) throws Exception
    {
        int coordinator = 2;
        int down = 5;
        UUID downHostId = hostId(cluster, down);
        cluster.get(down).shutdown().get();
        assertThat(lwt(cluster, coordinator, "INSERT INTO " + TABLE + " (k, v) VALUES (" + k + ", 1) IF NOT EXISTS",
                       ConsistencyLevel.LOCAL_SERIAL, ConsistencyLevel.LOCAL_QUORUM)).isTrue();
        awaitAsserted(60, () -> assertThat(hintsFiles(cluster, coordinator, downHostId)).as("hints of node 2 for node 5").isNotEmpty());

        cluster.get(down).startup();
        // node 2 does not dispatch its hint: the in-JVM nodes run without gossip, which would start the dispatch,
        // and the test does not start it by hand, so only the read at ALL below can bring the row
        assertRows(cluster.get(down).executeInternal("SELECT v FROM " + TABLE + " WHERE k = ?", k));
        assertRows(cluster.coordinator(down).execute("SELECT v FROM " + TABLE + " WHERE k = ?", ALL, k), row(1));
        assertRows(cluster.get(down).executeInternal("SELECT v FROM " + TABLE + " WHERE k = ?", k), row(1));
    }

    /** Whether a conditional statement was applied, run through the native protocol on one node. */
    private static boolean lwt(UpgradeableCluster cluster, int coordinator, String statement, ConsistencyLevel serial, ConsistencyLevel commit)
    {
        return withSession(cluster, coordinator, session -> lwt(session, statement, serial, commit));
    }

    private static boolean lwt(Session session, String statement, ConsistencyLevel serial, ConsistencyLevel commit)
    {
        ResultSet result = session.execute(new SimpleStatement(statement).setConsistencyLevel(commit).setSerialConsistencyLevel(serial));
        return result.wasApplied();
    }

    private static void assertOnEveryNode(UpgradeableCluster cluster, int k, int v)
    {
        for (int node = 1; node <= cluster.size(); node++)
        {
            int n = node;
            awaitAsserted(() -> assertRows(cluster.get(n).executeInternal("SELECT v FROM " + TABLE + " WHERE k = ?", k), row(v)));
        }
    }

    /** The hints files a node holds for a host, named {@code <host id>-<timestamp>-<version>.hints} by both versions. */
    private static List<String> hintsFiles(UpgradeableCluster cluster, int node, UUID hostId)
    {
        Path directory = Paths.get(cluster.get(node).config().getString("hints_directory"));
        if (!Files.isDirectory(directory))
            return Collections.emptyList();
        try (Stream<Path> files = Files.list(directory))
        {
            return files.map(file -> file.getFileName().toString())
                        .filter(name -> name.startsWith(hostId.toString()) && name.endsWith(".hints"))
                        .collect(Collectors.toList());
        }
        catch (IOException e)
        {
            throw new UncheckedIOException(e);
        }
    }
}
