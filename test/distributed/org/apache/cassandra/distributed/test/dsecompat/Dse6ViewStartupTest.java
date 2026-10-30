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

import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;

import static org.apache.cassandra.config.CassandraRelevantProperties.SAI_CURRENT_VERSION;
import static org.apache.cassandra.config.CassandraRelevantProperties.TRIE_INDEX_FORMAT_VERSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * The startup check of the materialized views of a node in {@code internode_compatibility_mode: DSE_6}, through the
 * start of an instance ({@code DseViewHiddenColumnCatalog.checkViewsAtStartup}): a node whose data directories hold
 * DSE's {@code system_schema.hidden_columns} starts with its views, one whose data directories do not hold it does not.
 */
public class Dse6ViewStartupTest extends TestBaseImpl
{
    /** The directory of DSE's table, under its table id. */
    private static final String HIDDEN_COLUMNS_DIRECTORY = "hidden_columns-2461e8b28f6a31a1ac7230b9cd9c6ca5";

    @Test
    public void aNodeWithViewsStartsInDse6ModeOnlyWithDsesTableOfHiddenColumns() throws Throwable
    {
        // the SAI on-disk version is pinned to aa, the one DSE 6.8 writes, as on a node upgraded from DSE 6.8
        try (WithProperties properties = new WithProperties().clear(TRIE_INDEX_FORMAT_VERSION).set(SAI_CURRENT_VERSION, "aa");
             Cluster cluster = Cluster.build(1)
                                      .withDynamicPortAllocation(true)
                                      .withConfig(c -> c.set("storage_compatibility_mode", "HCD_1")
                                                        .set("uuid_sstable_identifiers_enabled", false)
                                                        .set("materialized_views_enabled", true)
                                                        // the build of a view waits for the schema to agree, through gossip
                                                        .with(Feature.NETWORK, Feature.GOSSIP))
                                      .createWithoutStarting())
        {
            IInvokableInstance node = cluster.get(1);
            // as on a real upgrade, the node first runs without the mode on the storage settings the mode requires (trie
            // sstables of version bb, numeric generations), as Dse6ModeSmokeTest does
            try (WithProperties bb = new WithProperties().set(TRIE_INDEX_FORMAT_VERSION, InternodeCompatibilityMode.DSE_6_TRIE_INDEX_FORMAT_VERSION))
            {
                cluster.startup();
                init(cluster);
                cluster.schemaChange(withKeyspace("CREATE TABLE %s.t (pk int, ck int, v text, PRIMARY KEY (pk, ck))"));
                cluster.schemaChange("CREATE MATERIALIZED VIEW " + KEYSPACE + ".t_by_v AS SELECT pk, ck, v FROM " + KEYSPACE + ".t " +
                                     "WHERE v IS NOT NULL AND pk IS NOT NULL AND ck IS NOT NULL PRIMARY KEY (v, pk, ck)");
                cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.t (pk, ck, v) VALUES (1, 1, 'a')"), ConsistencyLevel.ONE);
                // a table with the columns of DSE's system_schema.hidden_columns, listing the hidden column of the view as
                // DSE does: its sstables stand for DSE's
                cluster.schemaChange(withKeyspace("CREATE TABLE %s.hidden_columns (keyspace_name text, table_name text, column_name text, " +
                                                  "clustering_order text, column_name_bytes blob, kind text, position int, " +
                                                  "required_for_liveness boolean, type text, PRIMARY KEY ((keyspace_name), table_name, column_name))"));
                cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.hidden_columns (keyspace_name, table_name, column_name, clustering_order, " +
                                                            "column_name_bytes, kind, position, required_for_liveness, type) " +
                                                            "VALUES (?, 't_by_v', '.v', 'none', 0x2e76, 'regular', -1, true, 'text')"),
                                               ConsistencyLevel.ONE, KEYSPACE);
                // the build of the view is done before the restarts, which would otherwise build it again
                await().atMost(1, TimeUnit.MINUTES)
                       .until(() -> node.executeInternal("SELECT view_name FROM system.built_views WHERE keyspace_name = ?", KEYSPACE).length == 1);
                node.flush(KEYSPACE);

                // without the mode, a node with views starts without DSE's table, which it does not read
                node.shutdown().get();
                node.startup();
                assertThat(node.logs().grep("system_schema.hidden_columns").getResult()).isEmpty();
                assertThat(cluster.coordinator(1).execute(withKeyspace("SELECT pk, ck FROM %s.t_by_v WHERE v = 'a'"), ConsistencyLevel.ONE).length).isEqualTo(1);
            }
            node.shutdown().get();
            node.config().set("internode_compatibility_mode", "DSE_6")
                         // the in-JVM default, which the mode turns off (Dse6ModeSmokeTest shows the numeric generations it writes)
                         .set("uuid_sstable_identifiers_enabled", true);

            // DSE's table, listing the hidden column of the view: the node starts, and reproduces the column
            String[] dataDirectories = (String[]) node.config().get("data_file_directories");
            Path hiddenColumns = Files.createDirectories(Path.of(dataDirectories[0], "system_schema", HIDDEN_COLUMNS_DIRECTORY));
            for (String dataDirectory : dataDirectories)
            {
                try (DirectoryStream<Path> tables = Files.newDirectoryStream(Path.of(dataDirectory, KEYSPACE), "hidden_columns-*"))
                {
                    for (Path table : tables)
                    {
                        try (DirectoryStream<Path> files = Files.newDirectoryStream(table, path -> Files.isRegularFile(path)))
                        {
                            for (Path file : files)
                                Files.copy(file, hiddenColumns.resolve(file.getFileName()));
                        }
                    }
                }
            }
            node.startup();
            assertThat(node.logs().grep("DSE 6 compatibility mode: the data of materialized view " + KEYSPACE + ".t_by_v is exchanged with DSE 6 " +
                                        "nodes with DSE's hidden column .v").getResult()).hasSize(1);
            assertThat(cluster.coordinator(1).execute(withKeyspace("SELECT pk, ck FROM %s.t_by_v WHERE v = 'a'"), ConsistencyLevel.ONE).length).isEqualTo(1);
            node.shutdown().get();

            // DSE's table, without sstables (the table of a DSE node with no view of DSE's new format): the node starts,
            // and warns that the table lists no hidden column for the view
            FileUtils.deleteRecursive(new File(hiddenColumns));
            Files.createDirectories(hiddenColumns);
            node.startup();
            assertThat(node.logs().grep("DSE 6 compatibility mode: DSE's system_schema.hidden_columns lists hidden columns for 0 materialized views").getResult()).hasSize(1);
            assertThat(node.logs().grep("DSE 6 compatibility mode: DSE's system_schema.hidden_columns on this node lists no hidden column for " +
                                        "materialized view " + KEYSPACE + ".t_by_v, whose data is exchanged with DSE 6 nodes as it is, as for a " +
                                        "view of DSE's old format or one created by this software").getResult()).hasSize(1);
            assertThat(cluster.coordinator(1).execute(withKeyspace("SELECT pk, ck FROM %s.t_by_v WHERE v = 'a'"), ConsistencyLevel.ONE).length).isEqualTo(1);
            node.shutdown().get();

            // without DSE's table, the node in the mode does not start with its view
            FileUtils.deleteRecursive(new File(hiddenColumns));
            assertThatThrownBy(node::startup)
            .hasStackTraceContaining("DSE 6 compatibility mode: cannot read DSE's system_schema.hidden_columns from the data directories of this node " +
                                     "(no data directory holds system_schema/" + HIDDEN_COLUMNS_DIRECTORY + ": ")
            .hasStackTraceContaining(" are not the data directories of a node that ran DSE 6); the data of materialized views is not exchanged " +
                                     "with DSE 6 nodes");
        }
    }
}
