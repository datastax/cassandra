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

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.distributed.test.TestBaseImpl;

import static org.apache.cassandra.config.CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE_ALLOW_DDL;
import static org.apache.cassandra.config.CassandraRelevantProperties.TRIE_INDEX_FORMAT_VERSION;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * A node running with {@code internode_compatibility_mode: DSE_6} writes what a DSE 6 node can read: trie
 * sstables of version {@code bb} with numeric generations. A node without the flag keeps writing the current
 * version with uuid identifiers.
 * <p>
 * As on a real upgrade, the node enters the mode on a restart, on data it wrote before (first started without the
 * mode on DSE-readable storage settings).
 * <p>
 * The mode sets {@code cassandra.trie_index_format_version} as a system property, which the in-JVM
 * instances share: every test restores it, so that the order of the tests does not matter. The tests run
 * in name order, the DSE_6 one first, so that a leak of the property into the NONE one would fail it.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class Dse6ModeSmokeTest extends TestBaseImpl
{
    @After
    public void clearTrieVersion()
    {
        // the WithProperties of each test restores the property already; this also covers a test that forgets it,
        // since an instance in DSE_6 sets it in the JVM it shares with the other instances
        TRIE_INDEX_FORMAT_VERSION.clearValue(); // checkstyle: suppress nearby 'clearValueSystemPropertyUsage'
    }

    @Test
    public void formerDseNodeWritesBbSstablesWithNumericGenerations() throws Throwable
    {
        try (WithProperties properties = new WithProperties().clear(TRIE_INDEX_FORMAT_VERSION);
             Cluster cluster = Cluster.build(1)
                                      .withDynamicPortAllocation(true)
                                      .withConfig(c -> c.set("storage_compatibility_mode", "HCD_1")
                                                        .set("uuid_sstable_identifiers_enabled", false))
                                      .createWithoutStarting())
        {
            try (WithProperties bb = new WithProperties().set(TRIE_INDEX_FORMAT_VERSION, InternodeCompatibilityMode.DSE_6_TRIE_INDEX_FORMAT_VERSION))
            {
                cluster.startup();
                init(cluster);
            }
            cluster.get(1).shutdown().get();
            cluster.get(1).config().set("internode_compatibility_mode", "DSE_6")
                                   .set("uuid_sstable_identifiers_enabled", true); // forced off by the mode
            cluster.get(1).startup();

            List<String> dataFiles;
            try (WithProperties ddl = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE_ALLOW_DDL, true))
            {
                dataFiles = writeAndFlush(cluster);
            }
            // one sstable per data directory of the instance: trie version bb, numeric generations
            assertThat(dataFiles).allMatch(f -> f.matches("bb-[0-9]+-bti-Data\\.db"), "trie version bb, numeric generation: " + dataFiles);

            NodeToolResult status = cluster.get(1).nodetoolResult("compatstatus");
            status.asserts().success();
            String out = status.getStdout();
            assertThat(out).contains("Internode compatibility mode : DSE_6");
            assertThat(out).contains("Storage compatibility mode   : HCD_1");
            assertThat(out).contains("SSTable format written       : bti-bb");

            // announced once the written version is verified, with that version: once, at the start in the mode
            assertThat(cluster.get(1).logs().grep("Running in internode_compatibility_mode DSE_6: the node writes trie bb sstables " +
                                                  "with numeric generations and requires paxos_variant v1, until the flag is " +
                                                  "dropped").getResult()).hasSize(1);
        }
    }

    @Test
    public void noneModeIsUnchanged() throws Throwable
    {
        try (WithProperties properties = new WithProperties().clear(TRIE_INDEX_FORMAT_VERSION);
             Cluster cluster = init(Cluster.build(1).withDynamicPortAllocation(true).start()))
        {
            NodeToolResult status = cluster.get(1).nodetoolResult("compatstatus");
            status.asserts().success();
            assertThat(status.getStdout()).contains("Internode compatibility mode : NONE");
            assertThat(status.getStdout()).contains("SSTable format written       : bti-cc");
            assertThat(cluster.get(1).logs().grep("Running in internode_compatibility_mode").getResult()).isEmpty();
            List<String> lines = Arrays.stream(status.getStdout().split("\n")).map(String::trim).collect(Collectors.toList());
            assertThat(lines).anyMatch(l -> l.startsWith("Peers with a known version"));

            List<String> dataFiles = writeAndFlush(cluster);
            // the default trie version, cc, and uuid sstable identifiers (enabled by the in-JVM default configuration)
            assertThat(dataFiles).allMatch(f -> f.matches("cc-[0-9a-z]{4}_[0-9a-z]{4}_[0-9a-z]{18}-bti-Data\\.db"),
                                           "trie version cc, uuid identifier: " + dataFiles);
        }
    }

    /** Writes a few rows to a new table, flushes it and returns the names of the data files of the keyspace. */
    private static List<String> writeAndFlush(Cluster cluster)
    {
        cluster.schemaChange(withKeyspace("CREATE TABLE %s.t (id int PRIMARY KEY, v text)"));
        for (int i = 0; i < 10; i++)
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.t (id, v) VALUES (?, ?)"), ConsistencyLevel.ONE, i, "v" + i);
        cluster.get(1).flush(KEYSPACE);

        List<String> dataFiles = cluster.get(1).callOnInstance(() -> {
            List<String> files = new ArrayList<>();
            for (Object dir : DatabaseDescriptor.getAllDataFileLocations())
            {
                File ks = new File(dir.toString(), TestBaseImpl.KEYSPACE);
                File[] tables = ks.listFiles();
                if (tables == null)
                    continue;
                for (File table : tables)
                    for (String f : table.list())
                        if (f.endsWith("-Data.db"))
                            files.add(f);
            }
            return files;
        });
        assertThat(dataFiles).isNotEmpty();
        return dataFiles;
    }
}
