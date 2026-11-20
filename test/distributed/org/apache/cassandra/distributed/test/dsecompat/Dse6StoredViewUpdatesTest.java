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
import java.nio.ByteBuffer;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.dsecompat.DseFixtures;
import org.apache.cassandra.net.DseCompat;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.config.CassandraRelevantProperties.SAI_CURRENT_VERSION;
import static org.apache.cassandra.config.CassandraRelevantProperties.TRIE_INDEX_FORMAT_VERSION;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.apache.cassandra.utils.TimeUUID.Generator.atUnixMillis;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * The view updates DSE 6 stored on the disk of a node before its upgrade, at the first start of the node in
 * {@code internode_compatibility_mode: DSE_6}: a batch of its batchlog and a hint of its hints files, each holding the
 * update of a materialized view that names DSE's hidden column {@code .v}, are translated with DSE's
 * {@code system_schema.hidden_columns}, which the start reads before it converts the hints files and replays the
 * batchlog. The batch is replayed to both view replicas and the hint delivered to its host. Without the mode, the batch
 * is kept in the batchlog, as one that cannot be decoded.
 * <p>
 * Not a mixed-version test: the HCD 1.2 stand-in of the mixed tests has no hidden view columns, so a catalog listing the
 * hidden column of a view would make the upgraded node send it cells that node does not know. Both nodes run this
 * software; node 2 stays without the mode.
 */
public class Dse6StoredViewUpdatesTest extends TestBaseImpl
{
    /** The directory of DSE's table, under its table id. */
    private static final String HIDDEN_COLUMNS_DIRECTORY = "hidden_columns-2461e8b28f6a31a1ac7230b9cd9c6ca5";

    @Test
    public void viewUpdatesDseStoredAreTranslatedAtTheFirstStartInDse6Mode() throws Throwable
    {
        // the SAI on-disk version is pinned to aa, the one DSE 6.8 writes, as on a node upgraded from DSE 6.8
        try (WithProperties properties = new WithProperties().clear(TRIE_INDEX_FORMAT_VERSION).set(SAI_CURRENT_VERSION, "aa");
             Cluster cluster = Cluster.build(2)
                                      .withDynamicPortAllocation(true)
                                      .withConfig(c -> c.set("storage_compatibility_mode", "HCD_1")
                                                        .set("uuid_sstable_identifiers_enabled", false)
                                                        .set("materialized_views_enabled", true)
                                                        .with(Feature.NETWORK, Feature.GOSSIP))
                                      .createWithoutStarting())
        {
            IInvokableInstance node = cluster.get(1);
            IInvokableInstance peer = cluster.get(2);
            UUID batchId = atUnixMillis(FBUtilities.now().toEpochMilli() - TimeUnit.HOURS.toMillis(1), 0).asUUID();
            // as on a real upgrade, the node first runs without the mode on the storage settings the mode requires (trie
            // sstables of version bb, numeric generations), as Dse6ViewStartupTest does
            try (WithProperties bb = new WithProperties().set(TRIE_INDEX_FORMAT_VERSION, InternodeCompatibilityMode.DSE_6_TRIE_INDEX_FORMAT_VERSION))
            {
                cluster.startup();
                // RF 2: both nodes are replicas of every view partition
                init(cluster);
                cluster.schemaChange(withKeyspace("CREATE TABLE %s.t (id int, ck text, v text, PRIMARY KEY (id, ck))"));
                cluster.schemaChange("CREATE MATERIALIZED VIEW " + KEYSPACE + ".t_by_v AS SELECT id, ck, v FROM " + KEYSPACE + ".t " +
                                     "WHERE v IS NOT NULL AND id IS NOT NULL AND ck IS NOT NULL PRIMARY KEY (v, id, ck)");
                // a table with the columns of DSE's system_schema.hidden_columns, listing the hidden column of the view as
                // DSE does: its sstables stand for DSE's
                cluster.schemaChange(withKeyspace("CREATE TABLE %s.hidden_columns (keyspace_name text, table_name text, column_name text, " +
                                                  "clustering_order text, column_name_bytes blob, kind text, position int, " +
                                                  "required_for_liveness boolean, type text, PRIMARY KEY ((keyspace_name), table_name, column_name))"));
                cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.hidden_columns (keyspace_name, table_name, column_name, clustering_order, " +
                                                            "column_name_bytes, kind, position, required_for_liveness, type) " +
                                                            "VALUES (?, 't_by_v', '.v', 'none', 0x2e76, 'regular', -1, true, 'text')"),
                                               ConsistencyLevel.ALL, KEYSPACE);
                // the builds of the view are done before the restart, which would otherwise build it again
                for (IInvokableInstance instance : new IInvokableInstance[]{ node, peer })
                    await().atMost(1, TimeUnit.MINUTES)
                           .until(() -> instance.executeInternal("SELECT view_name FROM system.built_views WHERE keyspace_name = ?", KEYSPACE).length == 1);
                node.flush(KEYSPACE);

                // the batch DSE left in the batchlog of the node: kept without the mode, as one that cannot be decoded
                ByteBuffer batched = ByteBuffer.wrap(node.callOnInstance(() -> storedViewUpdate("x")));
                node.executeInternal("INSERT INTO system.batches (id, mutations, version) VALUES (?, ?, ?)",
                                     batchId, Collections.singletonList(batched), DseCompat.DSE_681_VERSION);
                await().atMost(2, TimeUnit.MINUTES)
                       .until(() -> !node.logs().grep("Batch " + batchId + " of system.batches, stored with messaging version 1024, cannot be decoded: " +
                                                      "java.lang.RuntimeException: Unknown column .v during deserialization of " + KEYSPACE + ".t_by_v").getResult().isEmpty());
                assertRows(node.executeInternal("SELECT count(*) FROM system.batches"), row(1L));
            }
            byte[] hinted = node.callOnInstance(() -> storedViewUpdate("y"));
            int gcgs = node.callOnInstance(() -> Schema.instance.getTableMetadata(KEYSPACE, "t_by_v").params.gcGraceSeconds);
            UUID peerHostId = peer.callOnInstance(SystemKeyspace::getLocalHostId);
            // the log of the node from its restart on
            long mark = node.logs().mark();
            node.shutdown().get();

            // the hints file DSE left for the peer
            long now = FBUtilities.now().toEpochMilli();
            DseFixtures.writeDseHintsFile(Path.of((String) node.config().get("hints_directory")), peerHostId, now,
                                          Collections.singletonList(DseFixtures.dseHint(now, gcgs, ByteBuffer.wrap(hinted))));
            // DSE's table, listing the hidden column of the view
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
            node.config().set("internode_compatibility_mode", "DSE_6")
                         // the in-JVM default, which the mode turns off (Dse6ModeSmokeTest shows the numeric generations it writes)
                         .set("uuid_sstable_identifiers_enabled", true);
            node.startup();

            assertThat(node.logs().grep(mark, "DSE 6 compatibility mode: the data of materialized view " + KEYSPACE + ".t_by_v is exchanged with " +
                                              "DSE 6 nodes with DSE's hidden column .v").getResult()).hasSize(1);
            assertThat(node.logs().grep(mark, "DSE 6 compatibility mode: converted the hints file .* 1 hint\\(s\\) written to .*, 0 dropped " +
                                              "\\(0 for a host not in the ring, 0 for an unknown table, 0 for an unknown column, 0 expired\\)")
                           .getResult()).hasSize(1);
            // the batch, replayed once the ring delay has passed, to both view replicas
            await().atMost(2, TimeUnit.MINUTES)
                   .until(() -> node.executeInternal("SELECT count(*) FROM system.batches")[0][0].equals(0L));
            // the hint, delivered to the peer
            await().atMost(2, TimeUnit.MINUTES)
                   .until(() -> peer.executeInternal("SELECT id, ck FROM " + KEYSPACE + ".t_by_v WHERE v = 'y'").length == 1);
            assertThat(node.logs().grep(mark, "cannot be decoded").getResult()).isEmpty();
            assertThat(node.logs().grep(mark, "which is not translated").getResult()).isEmpty();
            // the live entry of each update, not the one DSE holds dead (a hidden tombstone)
            for (IInvokableInstance instance : new IInvokableInstance[]{ node, peer })
                assertRows(instance.executeInternal("SELECT v, id, ck FROM " + KEYSPACE + ".t_by_v WHERE v = 'x'"), row("x", 1, "a"));
            assertRows(peer.executeInternal("SELECT v, id, ck FROM " + KEYSPACE + ".t_by_v WHERE v = 'y'"), row("y", 1, "a"));
            assertRows(node.executeInternal("SELECT v, id, ck FROM " + KEYSPACE + ".t_by_v WHERE v = 'y'"));
        }
    }

    /**
     * The update DSE stores for the view {@code t_by_v} in its partition {@code v} ({@link DseFixtures#dseViewUpdate}), in
     * DSE's layout; run on an instance, which holds the schema.
     */
    private static byte[] storedViewUpdate(String v)
    {
        ByteBuffer bytes;
        try
        {
            bytes = DseFixtures.dse68Mutation(DseFixtures.dseViewUpdate(Schema.instance.getTableMetadata(KEYSPACE, "t_by_v"), v));
        }
        catch (IOException e)
        {
            throw new UncheckedIOException(e);
        }
        byte[] array = new byte[bytes.remaining()];
        bytes.get(array);
        return array;
    }
}
