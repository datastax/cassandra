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
import java.util.Map;

import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.test.TestBaseImpl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A node in {@code internode_compatibility_mode: DSE_5_1} refuses to start, once its schema is loaded, while a table is
 * compressed with a compressor DSE 5.1 does not read, and starts while every table uses one DSE 5.1 reads. The node
 * first runs without the mode, on the storage settings the mode requires, then is restarted in it.
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
