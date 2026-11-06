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
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.dsecompat.Dse51Fixtures;
import org.apache.cassandra.dsecompat.DseFixtures;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.service.DseUpgradeMarker;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * A cluster of nodes of this software in {@code internode_compatibility_mode: DSE_5_1}, on the storage settings the
 * mode requires, starts, writes, flushes and restarts: each node writes big {@code nb} sstables with numeric
 * generations, writes the DSE 5.1 upgrade marker once, at its first start, and is not refused by it when it starts
 * again in the mode, reads and records the values of DSE 5.1's columns of {@code system.local} and
 * {@code system.peers} at its first start and loads them from that record at the restart, and replays its own commit
 * log segments at the restart without moving them aside. Its saved definitions of {@code system.local} and
 * {@code system.peers} list the columns DSE 5.1 adds as dropped columns. The nodes talk to each other as nodes of
 * this software: the mode speaks no pre-4.0 dialect yet. A node in the mode also starts on the {@code system.local}
 * and {@code system.peers} sstables a DSE 5.1.51 node wrote, and reads and records their values.
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
        try (Cluster cluster = Cluster.build(NODES)
                                      .withDynamicPortAllocation(true)
                                      .withConfig(c -> c.with(NETWORK, GOSSIP)
                                                        .set("storage_compatibility_mode", "HCD_1")
                                                        .set("sstable", Map.of("selected_format", "big"))
                                                        .set("internode_compatibility_mode", "DSE_5_1"))
                                      .start())
        {
            init(cluster, NODES);
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.t (id int PRIMARY KEY, v text)"));
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
        }
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
        try (Cluster cluster = Cluster.build(1)
                                      .withDynamicPortAllocation(true)
                                      .withConfig(c -> c.with(NETWORK, GOSSIP)
                                                        .set("storage_compatibility_mode", "HCD_1")
                                                        .set("sstable", Map.of("selected_format", "big"))
                                                        .set("internode_compatibility_mode", "DSE_5_1"))
                                      .start())
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

            instance.startup();
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
