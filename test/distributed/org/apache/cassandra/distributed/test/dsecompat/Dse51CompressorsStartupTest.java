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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.service.DseUpgradeMarker;

import static org.apache.cassandra.config.CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE_IGNORE_DSE_PEERS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A node in {@code internode_compatibility_mode: DSE_5_1} refuses to start, once its schema is loaded, while a table is
 * compressed with a compressor DSE 5.1 does not read, and starts while every table uses one DSE 5.1 reads. The node
 * first runs without the mode, on the storage settings the mode requires, then is restarted in it. The advice of the
 * refusal, followed, leads back to a start in the mode.
 */
public class Dse51CompressorsStartupTest extends TestBaseImpl
{
    @Test
    public void aTableCompressedWithZstdRefusesTheStartInTheMode() throws Throwable
    {
        try (Cluster cluster = dse51ReadyCluster())
        {
            init(cluster);
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.zstd (k int PRIMARY KEY, v int) " +
                                              "WITH compression = {'class': 'ZstdCompressor'}"));
            IInvokableInstance node = restartedInTheMode(cluster, false);
            assertThatThrownBy(node::startup)
            .hasStackTraceContaining("internode_compatibility_mode DSE_5_1 refused: " + KEYSPACE + ".zstd " +
                                     "(ZstdCompressor) is compressed with a compressor DSE 5.1 does not read");
        }
    }

    /**
     * The advice of the refusal leads back to the mode. Its starting point is reproduced without a refused start, which
     * an in-JVM instance cannot follow with another start: the node has run in the mode, so it holds the DSE 5.1
     * upgrade marker, and a table is compressed with a compressor DSE 5.1 does not read. The start without the mode,
     * with the override of the marker and the storage settings of the mode, is accepted; there the compression is
     * changed and the sstables rewritten, and the next start in the mode, without the override, is accepted. The
     * refusal of a start without the mode and without the override is in {@code Dse51StartupChecksTest}.
     */
    @Test
    public void theAdviceOfTheRefusalLeadsBackToTheMode() throws Throwable
    {
        try (Cluster cluster = dse51ReadyCluster())
        {
            init(cluster);
            IInvokableInstance node = restartedInTheMode(cluster, true);
            assertThat(node.callOnInstance(() -> DseUpgradeMarker.exists(InternodeCompatibilityMode.DSE_5_1))).isTrue();
            node.shutdown().get();

            node.config().set("internode_compatibility_mode", "NONE");
            try (WithProperties ignore = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE_IGNORE_DSE_PEERS, true))
            {
                node.startup();
            }
            assertThat(node.logs().grep("The DSE 5.1 upgrade marker .* is present but ignored").getResult()).hasSize(1);
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.zstd (k int PRIMARY KEY, v int) " +
                                              "WITH compression = {'class': 'ZstdCompressor'}"));
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.zstd (k, v) VALUES (1, 1)"),
                                           ConsistencyLevel.ONE);
            node.flush(KEYSPACE);
            cluster.schemaChange(withKeyspace("ALTER TABLE %s.zstd WITH compression = {'class': 'LZ4Compressor'}"));
            node.nodetoolResult("upgradesstables", "-a", KEYSPACE, "zstd").asserts().success();
            // every sstable of the table is now compressed with LZ4, which DSE 5.1 reads
            assertThat(node.callOnInstance(() -> compressorsOfTheSSTables(KEYSPACE, "zstd")))
            .containsExactly("LZ4Compressor");
            node.shutdown().get();

            node.config().set("internode_compatibility_mode", "DSE_5_1");
            node.startup();
            assertThat(node.callOnInstance(DatabaseDescriptor::isDse51CompatibilityMode)).isTrue();
            assertThat(cluster.coordinator(1).execute(withKeyspace("SELECT v FROM %s.zstd WHERE k = 1"),
                                                      ConsistencyLevel.ONE))
            .isDeepEqualTo(new Object[][]{ { 1 } });
        }
    }

    @Test
    public void tablesCompressedAsDse51ReadsStartInTheMode() throws Throwable
    {
        try (Cluster cluster = dse51ReadyCluster())
        {
            init(cluster);
            for (String compressor : new String[]{ "LZ4Compressor", "SnappyCompressor", "DeflateCompressor" })
                cluster.schemaChange(withKeyspace("CREATE TABLE %s." + compressor.toLowerCase() +
                                                  " (k int PRIMARY KEY, v int) WITH compression = {'class': '" +
                                                  compressor + "'}"));
            IInvokableInstance node = restartedInTheMode(cluster, true);
            assertThat(node.callOnInstance(DatabaseDescriptor::isDse51CompatibilityMode)).isTrue();
            // announced once, at the start in the mode, with the version the node writes
            assertThat(node.logs().grep("Running in internode_compatibility_mode DSE_5_1: the node writes big nb " +
                                        "sstables, which DSE 5.1 does not read, with numeric generations").getResult())
            .hasSize(1);
        }
    }

    /** The simple names of the compressors of the live sstables of the table, run inside an instance. */
    private static Set<String> compressorsOfTheSSTables(String keyspace, String table)
    {
        Set<String> compressors = new HashSet<>();
        for (SSTableReader sstable : Keyspace.open(keyspace).getColumnFamilyStore(table).getLiveSSTables())
            compressors.add(sstable.getCompressionMetadata().compressor().getClass().getSimpleName());
        return compressors;
    }

    /** One node of this software, started without the mode on the storage settings DSE_5_1 requires. */
    private static Cluster dse51ReadyCluster() throws IOException
    {
        return Cluster.build(1)
                      .withDynamicPortAllocation(true)
                      .withConfig(c -> c.set("storage_compatibility_mode", "HCD_1")
                                        .set("uuid_sstable_identifiers_enabled", false)
                                        .set("sstable", Map.of("selected_format", "big")))
                      .start();
    }

    /** Stops the node, sets DSE_5_1 in its configuration and, when {@code start}, starts it again. */
    private static IInvokableInstance restartedInTheMode(Cluster cluster, boolean start) throws Exception
    {
        IInvokableInstance node = cluster.get(1);
        node.shutdown().get();
        node.config().set("internode_compatibility_mode", "DSE_5_1");
        if (start)
            node.startup();
        return node;
    }
}
