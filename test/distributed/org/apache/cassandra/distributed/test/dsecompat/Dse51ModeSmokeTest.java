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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.service.DseUpgradeMarker;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * A cluster of nodes of this software in {@code internode_compatibility_mode: DSE_5_1}, on the storage settings the
 * mode requires, starts, writes, flushes and restarts: each node writes big {@code nb} sstables with numeric
 * generations, writes the DSE 5.1 upgrade marker once, at its first start, and is not refused by it when it starts
 * again in the mode, and replays its own commit log segments at the restart without moving them aside. The nodes talk
 * to each other as nodes of this software: the mode speaks no pre-4.0 dialect yet.
 */
public class Dse51ModeSmokeTest extends TestBaseImpl
{
    private static final int NODES = 2;
    private static final int ROWS = 10;

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
            }

            for (int node = 1; node <= NODES; node++)
            {
                IInvokableInstance instance = cluster.get(node);
                instance.shutdown().get();
                instance.startup();
                assertMarkers(instance);
                // written at the first start only
                assertThat(instance.logs().grep("Wrote the DSE 5.1 upgrade marker ").getResult()).hasSize(1);
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
