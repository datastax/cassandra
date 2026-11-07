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

import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.db.Dse51SystemTables;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.dsecompat.Dse51Fixtures;
import org.apache.cassandra.dsecompat.DseFixtures;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.service.DseUpgradeMarker;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.config.CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE_ALLOW_DDL;
import static org.apache.cassandra.config.CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE_DSE_RESTRICTIONS_LOCAL_CHECK_ONLY;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A cluster of nodes of this software in {@code internode_compatibility_mode: DSE_5_1}, on the storage settings the
 * mode requires, started without the mode first and restarted in it, as upgraded DSE 5.1 nodes, writes, flushes and
 * restarts: each node writes big {@code nb} sstables with numeric generations, writes the DSE 5.1 upgrade marker once,
 * at its first start in the mode, and is not refused by it when it starts again in the mode, reads and records the
 * values of DSE 5.1's columns of {@code system.local} and {@code system.peers} at its first start in the mode and loads
 * them from that record at the restart, and replays its own commit log segments at the restarts without moving them
 * aside. Its saved definitions of {@code system.local} and
 * {@code system.peers} list the columns DSE 5.1 adds as dropped columns. The nodes talk to each other as nodes of
 * this software, at their own messaging version: GROUP BY queries with paging, logged batches and lightweight
 * transactions run between them, while the schema is frozen as in {@code DSE_6} and the property lifts the freeze. A
 * node in the mode also starts on the {@code system.local} and {@code system.peers} sstables a DSE 5.1.51 node wrote,
 * and reads and records their values. Each node publishes in gossip what DSE 5.1 nodes read of it: the states drivers
 * learn a node by, at DSE 5.1's ordinals, and its DSE state in DSE 5.1's form.
 */
public class Dse51ModeSmokeTest extends TestBaseImpl
{
    private static final int NODES = 2;
    private static final int ROWS = 10;
    /** Added to the generations of the sstables DSE 5.1.51 wrote, so that they are above those of the node. */
    private static final int CAPTURED_GENERATION_OFFSET = 1000;

    @Test
    public void aClusterInTheModeStartsWritesFlushesAndRestarts() throws Throwable
    {
        try (Cluster cluster = upgradedNodes(NODES, "CREATE TABLE %s.t (id int PRIMARY KEY, v text)"))
        {
            for (int i = 0; i < ROWS; i++)
                cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.t (id, v) VALUES (?, ?)"),
                                               ConsistencyLevel.ALL, i, "v" + i);
            for (int node = 1; node <= NODES; node++)
            {
                IInvokableInstance instance = cluster.get(node);
                instance.flush(KEYSPACE);
                assertThat(instance.callOnInstance(DatabaseDescriptor::isDse51CompatibilityMode)).isTrue();
                List<String> dataFiles = dataFiles(instance);
                assertThat(dataFiles).isNotEmpty()
                                     .allMatch(f -> f.matches("nb-[0-9]+-big-Data\\.db"),
                                               "big nb, numeric generation: " + dataFiles);
                assertMarkers(instance);
                // the definitions of the local system keyspaces saved at the start list the columns DSE 5.1 adds to
                // system.local and system.peers as dropped columns, as in every mode
                for (String table : List.of("local", "peers"))
                {
                    Object[][] rows = instance.executeInternal("SELECT column_name FROM " +
                                                               "system_schema.dropped_columns WHERE " +
                                                               "keyspace_name = 'system' AND table_name = ?", table);
                    assertThat(Arrays.stream(rows).map(row -> (String) row[0]))
                    .as("dropped columns of system.%s", table)
                    .contains("workload", "workloads", "graph", "dse_version", "server_id");
                }
            }

            for (int node = 1; node <= NODES; node++)
            {
                IInvokableInstance instance = cluster.get(node);
                instance.shutdown().get();
                instance.startup();
                assertMarkers(instance);
                // written at the first start only
                assertThat(instance.logs().grep("Wrote the DSE 5.1 upgrade marker ").getResult()).hasSize(1);
                // the values of DSE 5.1's columns of system.local and system.peers read and recorded at the first
                // start only (these nodes never ran DSE 5.1, so none is found), loaded from the record at the restart
                assertThat(instance.logs().grep("DSE 5.1 compatibility mode: recorded the values of DSE 5.1's " +
                                                "columns of system.local and system.peers in ").getResult())
                .hasSize(1);
                String summary = "DSE 5.1 compatibility mode: values of DSE 5.1's columns " +
                                 "\\[workload, workloads, graph, dse_version\\] of system.local and " +
                                 "system.peers ";
                assertThat(instance.logs().grep(summary + "read from their sstables; rows holding a value: 0")
                                   .getResult())
                .hasSize(1);
                assertThat(instance.logs().grep(summary + "loaded from .*; rows holding a value: 0").getResult())
                .hasSize(1);
                // the segments the node wrote in the mode (version 7) are replayed as its own, not moved aside
                assertThat(instance.logs().grep("was written by DataStax Enterprise").getResult()).isEmpty();
                assertThat(instance.callOnInstance(() -> {
                    String[] names = DatabaseDescriptor.getCommitLogLocation().tryListNames();
                    return names == null ? List.<String>of() : List.of(names);
                })).isNotEmpty().noneMatch(name -> name.endsWith(CommitLog.REPLAYED_DSE_SEGMENT_SUFFIX));
            }
            // the nodes see each other up before the read at ALL
            ClusterUtils.awaitRingStatus(cluster.get(2), cluster.get(1), "Up");
            ClusterUtils.awaitRingStatus(cluster.get(1), cluster.get(2), "Up");
            assertThat(cluster.coordinator(2).execute(withKeyspace("SELECT count(*) FROM %s.t"), ConsistencyLevel.ALL))
            .isDeepEqualTo(new Object[][]{ { (long) ROWS } });

            // what node 1 publishes in gossip (its own state; the nodes of this software drop the legacy states of each
            // other's while no node of major version 3 is in gossip): the states DSE 5.1 nodes announce a node to
            // drivers with, STATUS normal, RPC_ADDRESS (the broadcast native address, not the any-address) and
            // RPC_READY true, at DSE 5.1's ordinals 0, 8 and 14; its DSE state in DSE 5.1's form at 9; no ordinal above
            // DSE 5.1's last, 24
            String rpcAddress = "127.0.0.101"; // the broadcast_rpc_address of node 1 (upgradedNodes)
            String dseState = cluster.get(1).callOnInstance(() -> new VersionedValue.VersionedValueFactory(DatabaseDescriptor.getPartitioner())
                                                                  .dse51State(Dse51Fixtures.DSE_VERSION).value);
            Map<Integer, String> gossiped = Map.of();
            for (int attempt = 0; attempt < 60 && !"true".equals(gossiped.get(ApplicationState.RPC_READY.ordinal())); attempt++)
            {
                if (attempt > 0)
                    Thread.sleep(500);
                gossiped = cluster.get(1).callOnInstance(() -> {
                    EndpointState state = Gossiper.instance.getEndpointStateForEndpoint(FBUtilities.getBroadcastAddressAndPort());
                    Map<Integer, String> byOrdinal = new HashMap<>();
                    for (Map.Entry<ApplicationState, VersionedValue> e : state.states())
                        byOrdinal.put(e.getKey().ordinal(), e.getValue().value);
                    return byOrdinal;
                });
            }
            assertThat(ApplicationState.STATUS.ordinal()).isZero();
            assertThat(ApplicationState.RPC_ADDRESS.ordinal()).isEqualTo(8);
            assertThat(ApplicationState.X_11_PADDING.ordinal()).isEqualTo(9);
            assertThat(ApplicationState.RPC_READY.ordinal()).isEqualTo(14);
            assertThat(gossiped.get(0)).startsWith("NORMAL,");
            assertThat(gossiped).containsEntry(8, rpcAddress)
                                .containsEntry(9, dseState)
                                .containsEntry(14, "true");
            assertThat(cluster.get(1).config().getString("rpc_address")).isEqualTo("0.0.0.0");
            assertThat(gossiped.keySet()).allMatch(ordinal -> ordinal <= ApplicationState.INDEX_STATUS.ordinal());
        }
    }

    /**
     * The messaging between nodes of this software in the mode, which opens the pre-4.0 code paths only below 4.0:
     * GROUP BY queries, paged, logged batches and lightweight transactions (Paxos v1, the variant of the mode) run at
     * ALL and SERIAL; a schema change is refused, with the text of the mode, and the property lifts the freeze, but
     * for an SAI index, which is refused even then.
     */
    @Test
    public void theNodesInTheModeServeGroupByBatchesAndLightweightTransactions() throws Throwable
    {
        try (Cluster cluster = upgradedNodes(NODES, "CREATE TABLE %s.g (pk int, ck int, v int, PRIMARY KEY (pk, ck))",
                                             "CREATE TABLE %s.lwt (id int PRIMARY KEY, v int)"))
        {
            for (int pk = 0; pk < 4; pk++)
                for (int ck = 0; ck < 5; ck++)
                    cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.g (pk, ck, v) VALUES (?, ?, ?)"),
                                                   ConsistencyLevel.ALL, pk, ck, pk * 10 + ck);
            // GROUP BY, at ALL across both nodes, paged two rows at a time
            String groupBy = withKeyspace("SELECT pk, count(*) FROM %s.g GROUP BY pk");
            Object[][] groups = toArray(cluster.coordinator(2).executeWithPaging(groupBy, ConsistencyLevel.ALL, 2));
            assertThat(groups).hasNumberOfRows(4);
            for (Object[] group : groups)
                assertThat(group[1]).isEqualTo(5L);
            // a logged batch
            cluster.coordinator(1).execute("BEGIN BATCH " +
                                           withKeyspace("INSERT INTO %s.g (pk, ck, v) VALUES (9, 0, 90); ") +
                                           withKeyspace("INSERT INTO %s.lwt (id, v) VALUES (9, 9); ") +
                                           "APPLY BATCH", ConsistencyLevel.ALL);
            assertThat(cluster.coordinator(2).execute(withKeyspace("SELECT v FROM %s.lwt WHERE id = 9"),
                                                      ConsistencyLevel.ALL))
            .isDeepEqualTo(new Object[][]{ { 9 } });
            // lightweight transactions
            String insert = withKeyspace("INSERT INTO %s.lwt (id, v) VALUES (1, 1) IF NOT EXISTS");
            Object[][] applied = cluster.coordinator(1).execute(insert, ConsistencyLevel.QUORUM);
            assertThat(applied[0][0]).isEqualTo(true);
            applied = cluster.coordinator(2).execute(withKeyspace("UPDATE %s.lwt SET v = 2 WHERE id = 1 IF v = 1"),
                                                     ConsistencyLevel.QUORUM);
            assertThat(applied[0][0]).isEqualTo(true);
            assertThat(cluster.coordinator(1).execute(withKeyspace("SELECT v FROM %s.lwt WHERE id = 1"),
                                                      ConsistencyLevel.SERIAL))
            .isDeepEqualTo(new Object[][]{ { 2 } });

            // the schema is frozen
            String alter = withKeyspace("ALTER TABLE %s.g ADD w int");
            assertThatThrownBy(() -> cluster.coordinator(1).execute(alter, ConsistencyLevel.ALL))
            .hasMessageContaining("schema changes are frozen during the DSE 5.1 upgrade; run them after the flag is " +
                                  "dropped");
            try (WithProperties properties = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE_ALLOW_DDL, true))
            {
                cluster.coordinator(1).execute(withKeyspace("ALTER TABLE %s.g ADD w int"), ConsistencyLevel.ALL);
                // but no SAI index is created, the freeze lifted or not
                String sai = withKeyspace("CREATE CUSTOM INDEX g_v_sai ON %s.g (v) USING 'sai'");
                assertThatThrownBy(() -> cluster.coordinator(1).execute(sai, ConsistencyLevel.ALL))
                .hasMessageContaining("Index g_v_sai was not created: with internode_compatibility_mode DSE_5_1 no " +
                                      "Storage Attached Index (SAI) is created, even with " +
                                      "-Dcassandra.internode_compatibility_mode.allow_ddl=true");
            }
            assertThat(cluster.get(1).executeInternal("SELECT index_name FROM system_schema.indexes WHERE " +
                                                      "keyspace_name = ?",
                                                      KEYSPACE)).isEmpty();
        }
    }

    /**
     * Nodes of this software, started without the mode on the storage settings it requires, with the test keyspace and
     * the tables of {@code schema} created then, and restarted in {@code DSE_5_1}: the restart of an upgraded DSE 5.1
     * node, which has joined its ring (a node in the mode does not bootstrap, the guards refuse it) and whose schema is
     * frozen in the mode.
     */
    private static Cluster upgradedNodes(int nodes, String... schema) throws Throwable
    {
        Cluster cluster = Cluster.build(nodes)
                                 .withDynamicPortAllocation(true)
                                 .withConfig(c -> c.with(NETWORK, GOSSIP, NATIVE_PROTOCOL)
                                                   // the any-address, so that the broadcast one is what gossip carries
                                                   .set("rpc_address", "0.0.0.0")
                                                   // advertised only, distinct from the internode address
                                                   .set("broadcast_rpc_address", "127.0.0." + (100 + c.num()))
                                                   .set("storage_compatibility_mode", "HCD_1")
                                                   .set("uuid_sstable_identifiers_enabled", false)
                                                   .set("sstable", Map.of("selected_format", "big")))
                                 .start();
        try
        {
            init(cluster, nodes);
            for (String statement : schema)
                cluster.schemaChange(withKeyspace(statement));
            for (int node = 1; node <= nodes; node++)
            {
                IInvokableInstance instance = cluster.get(node);
                instance.shutdown().get();
                instance.config().set("internode_compatibility_mode", "DSE_5_1");
                instance.startup();
            }
            return cluster;
        }
        catch (Throwable t)
        {
            cluster.close();
            throw t;
        }
    }

    private static Object[][] toArray(Iterator<Object[]> rows)
    {
        List<Object[]> list = new ArrayList<>();
        rows.forEachRemaining(list::add);
        return list.toArray(new Object[0][]);
    }

    /**
     * A node in the mode started on the {@code system.local} and {@code system.peers} sstables a DSE 5.1.51 node wrote
     * (the compacted ones of node 1 of the three-node cluster of the DSE 5.1.51 fixtures): copied into its data
     * directory while it is down, its DSE 5.1 upgrade marker and the record of the values deleted so that it starts as
     * at a first start in the mode, the start reads them with the five columns skipped, logs the values of DSE 5.1's
     * columns of its own row and of both peers' rows, and records them. As at the first start of node 1 of a real
     * upgrade, the start also migrates both peers to {@code peers_v2}, with their tokens.
     */
    @Test
    public void aNodeInTheModeStartsOnTheSystemSSTablesDse51Wrote() throws Throwable
    {
        try (Cluster cluster = upgradedNodes(1))
        {
            IInvokableInstance instance = cluster.get(1);
            String dataDirectory = instance.callOnInstance(() -> DatabaseDescriptor.getAllDataFileLocations()[0]
                                                                                   .absolutePath());
            String marker = instance.callOnInstance(() -> DseUpgradeMarker.path(InternodeCompatibilityMode.DSE_5_1)
                                                                          .toString());
            String record = instance.callOnInstance(() -> Dse51SystemTables.recordPath().toString());
            instance.shutdown().get();

            Path captured = DseFixtures.dse51().systemSSTables(true);
            for (String table : List.of("local", "peers"))
            {
                Path from = DseFixtures.dse51().tableDirectory(captured, table);
                // the table ids of system.local and system.peers are the same in DSE 5.1 and in this software
                Path to = Path.of(dataDirectory, "system", from.getFileName().toString());
                assertThat(to).isDirectory();
                for (Path file : DseFixtures.dse51().sstableComponents(from))
                {
                    // a generation above those of the node: me-<generation>-big-<component>
                    String[] parts = file.getFileName().toString().split("-", 4);
                    String name = parts[0] + '-' + (CAPTURED_GENERATION_OFFSET + Integer.parseInt(parts[1])) + '-' +
                                  parts[2] + '-' + parts[3];
                    Files.copy(file, to.resolve(name));
                }
            }
            Files.delete(Path.of(marker));
            Files.delete(Path.of(record));

            // the node now has two DSE 5.1 peers on record, which never come up, so the cluster-wide read of the check
            // of the DSE access controls cannot be made: only this node's replica is checked
            try (WithProperties localCheck = new WithProperties()
                                             .set(INTERNODE_COMPATIBILITY_MODE_DSE_RESTRICTIONS_LOCAL_CHECK_ONLY, true))
            {
                instance.startup();
            }
            assertMarkers(instance);
            String row = "DSE 5.1 compatibility mode: DSE 5.1's columns of system.local and system.peers for %s " +
                         "\\(workloads \\[Cassandra\\], workload Cassandra, graph false, dse_version 5.1.51\\)";
            for (String node : List.of("local", "127.0.51.12", "127.0.51.13"))
                assertThat(instance.logs().grep(String.format(row, Pattern.quote(node))).getResult()).as(node)
                                                                                               .hasSize(1);
            assertThat(instance.logs().grep("DSE 5.1 compatibility mode: values of DSE 5.1's columns " +
                                            "\\[workload, workloads, graph, dse_version\\] of system.local and " +
                                            "system.peers read from their sstables; rows holding a value: 3")
                               .getResult())
            .hasSize(1);
            assertThat(Files.readAllLines(Path.of(record)))
            .containsExactly("# values of DSE 5.1's columns of system.local and system.peers, read at the first " +
                             "start in DSE_5_1 mode; one row per line: node, dse_version, workload, graph, " +
                             "workloads; - when absent",
                             "local\t5.1.51\tCassandra\tfalse\tCassandra",
                             "127.0.51.12\t5.1.51\tCassandra\tfalse\tCassandra",
                             "127.0.51.13\t5.1.51\tCassandra\tfalse\tCassandra");
            // the rows of the peers are read with the five columns skipped
            Map<String, Set<String>> tokens = Map.of("127.0.51.12", Set.of("-1824726619835567993"),
                                                     "127.0.51.13", Set.of("-2751153962405162262"));
            for (String table : List.of("system.peers", "system.peers_v2"))
            {
                Map<String, Set<String>> found = new HashMap<>();
                for (Object[] peer : instance.executeInternal("SELECT peer, release_version, tokens FROM " + table))
                {
                    assertThat(peer[1]).as("release of %s in %s", peer[0], table)
                                       .isEqualTo(Dse51Fixtures.RELEASE_VERSION);
                    Set<String> peerTokens = ((Set<?>) peer[2]).stream().map(Object::toString)
                                                               .collect(Collectors.toSet());
                    found.put(((InetAddress) peer[0]).getHostAddress(), peerTokens);
                }
                // in peers_v2, migrated at the start, with their tokens
                assertThat(found).as(table).isEqualTo(tokens);
            }
        }
    }

    /** The node has the marker of the upgrade from DSE 5.1, and not the one of the upgrade from DSE 6. */
    private static void assertMarkers(IInvokableInstance instance)
    {
        assertThat(instance.callOnInstance(() -> DseUpgradeMarker.exists(InternodeCompatibilityMode.DSE_5_1))).isTrue();
        assertThat(instance.callOnInstance(() -> DseUpgradeMarker.exists())).isFalse();
    }

    /** The names of the data files of the test keyspace on the instance. */
    private static List<String> dataFiles(IInvokableInstance instance)
    {
        return instance.callOnInstance(() -> {
            List<String> files = new ArrayList<>();
            for (File dir : DatabaseDescriptor.getAllDataFileLocations())
            {
                File[] tables = dir.resolve(TestBaseImpl.KEYSPACE).tryList();
                if (tables == null)
                    continue;
                for (File table : tables)
                {
                    String[] names = table.tryListNames();
                    if (names == null)
                        continue;
                    for (String f : names)
                        if (f.endsWith("-Data.db"))
                            files.add(f);
                }
            }
            return files;
        });
    }
}
