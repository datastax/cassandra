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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.junit.After;
import org.junit.Test;

import org.apache.cassandra.batchlog.BatchlogManager;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.dsecompat.DseFixtures;
import org.apache.cassandra.dsecompat.DseFixtures.DseVersion;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.config.CassandraRelevantProperties.SAI_CURRENT_VERSION;
import static org.apache.cassandra.config.CassandraRelevantProperties.TRIE_INDEX_FORMAT_VERSION;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.apache.cassandra.utils.TimeUUID.Generator.atUnixMillis;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Two nodes of this software, the first holding in {@code system.batches} the batches DSE 6 left there before the upgrade
 * (the mutations of the DSE 6.8.65 and 6.9.26 hints fixtures, tagged with DSE's messaging versions 1024 and 1536) behind
 * a batch that cannot be decoded: the batchlog replay of the first node applies DSE's batches on both replicas, removes
 * them, and keeps the undecodable one with its ERROR, in {@code DSE_6} and in {@code NONE}. In {@code DSE_6} the nodes enter
 * the mode on a restart, as on an upgrade, after the schema is created.
 */
public class DseBatchlogStateTest extends TestBaseImpl
{
    private static final String KS = DseFixtures.FIXTURE_KEYSPACE;

    @After
    public void clearTrieVersion()
    {
        // an instance in DSE_6 sets the property in the JVM it shares with the other instances
        TRIE_INDEX_FORMAT_VERSION.clearValue(); // checkstyle: suppress nearby 'clearValueSystemPropertyUsage'
    }

    @Test
    public void replayInDse6() throws Throwable
    {
        // the nodes enter the mode on a restart, as on an upgrade
        replayDseBatches(true, c -> c.set("storage_compatibility_mode", "HCD_1").set("uuid_sstable_identifiers_enabled", false));
    }

    @Test
    public void replayInNone() throws Throwable
    {
        replayDseBatches(false, c -> {});
    }

    private static void replayDseBatches(boolean dse6, Consumer<IInstanceConfig> config) throws Throwable
    {
        List<ByteBuffer> dse68 = DseFixtures.fixtureHintMutations(DseVersion.DSE_6_8_65);
        List<ByteBuffer> dse69 = DseFixtures.fixtureHintMutations(DseVersion.DSE_6_9_26);
        long hourAgo = FBUtilities.now().toEpochMilli() - TimeUnit.HOURS.toMillis(1);
        UUID undecodable = atUnixMillis(hourAgo, 0).asUUID();
        UUID batch68 = atUnixMillis(hourAgo + 1, 0).asUUID();
        UUID batch69 = atUnixMillis(hourAgo + 2, 0).asUUID();

        WithProperties properties = new WithProperties().clear(TRIE_INDEX_FORMAT_VERSION);
        if (dse6)
            properties.set(SAI_CURRENT_VERSION, "aa"); // DSE_6 needs the SAI on-disk version pinned; these nodes store no DSE release
        try (properties;
             Cluster cluster = Cluster.build(2).withConfig(c -> config.accept(c.with(NETWORK, GOSSIP))).createWithoutStarting())
        {
            if (dse6)
            {
                // first started on DSE-readable storage settings, as the DSE nodes the cluster comes from
                try (WithProperties bb = new WithProperties().set(TRIE_INDEX_FORMAT_VERSION, InternodeCompatibilityMode.DSE_6_TRIE_INDEX_FORMAT_VERSION))
                {
                    cluster.startup();
                }
            }
            else
            {
                cluster.startup();
            }
            for (String statement : DseDialectTestBase.fixtureSchema(DseVersion.DSE_6_8_65, 2))
                cluster.schemaChange(statement);
            cluster.schemaChange("CREATE TABLE " + KS + ".t69 (id int, ck text, fu frozen<addr>, l list<int>, m map<text, int>, " +
                                 "n bigint, s int static, tp frozen<tuple<int, text>>, u addr, v text, PRIMARY KEY (id, ck)) WITH ID = " +
                                 DseFixtures.fixtureTableId(DseVersion.DSE_6_9_26));
            if (dse6)
            {
                // the schema is in place: every node enters the mode on a restart
                for (IInvokableInstance node : cluster)
                {
                    node.shutdown().get();
                    node.config().set("internode_compatibility_mode", "DSE_6");
                    node.startup();
                    assertThat(node.callOnInstance(() -> DatabaseDescriptor.isDse6CompatibilityMode())).describedAs("node " + node.config().num()).isTrue();
                }
                // a restart only waits for its own node's view of gossip to settle: every node must see the other up again, or
                // the replay below would hint the mutations instead of delivering them to the second replica
                for (IInvokableInstance node : cluster)
                    ClusterUtils.awaitRingHealthy(node);
            }

            IInvokableInstance first = cluster.get(1);
            stopBatchlogReplay(first);
            long hintsBefore = first.callOnInstance(() -> StorageMetrics.totalHints.getCount());
            int ownVersion = first.callOnInstance(() -> MessagingService.current_version);
            String insert = "INSERT INTO system.batches (id, mutations, version) VALUES (?, ?, ?)";
            // DSE's bytes tagged with this software's version: decoded with this software's own layout, as the replay decoded
            // DSE's batches before this change
            first.executeInternal(insert, undecodable, dse68, ownVersion);
            first.executeInternal(insert, batch68, dse68, DseVersion.DSE_6_8_65.relativeMessagingVersion << 8);
            first.executeInternal(insert, batch69, dse69, DseVersion.DSE_6_9_26.relativeMessagingVersion << 8);

            replayFromTheFirstBatch(first);
            // the second replica acknowledged the replayed mutations: none was hinted
            assertThat(first.callOnInstance(() -> StorageMetrics.totalHints.getCount())).isEqualTo(hintsBefore);

            // only the undecodable batch is left (looked up by its id: the id column read back does not compare equal to a UUID)
            assertRows(first.executeInternal("SELECT version FROM system.batches"), row(ownVersion));
            assertRows(first.executeInternal("SELECT version FROM system.batches WHERE id = ?", undecodable), row(ownVersion));
            for (String table : new String[]{ "t", "t69" })
            {
                // what the fixture data set wrote (test/data/dse-compat/README.md), on the first replica and on the second, which
                // received the mutations from the replay at this software's version
                for (IInvokableInstance node : cluster)
                {
                    assertRows(node.executeInternal("SELECT count(*) FROM " + KS + '.' + table), row((long) DseFixtures.COLLECTION_TABLE_COUNT_STAR));
                    assertRows(node.executeInternal("SELECT n, s, v FROM " + KS + '.' + table + " WHERE id = 25 AND ck = 'ck2'"), row(5000L, 250, "val50"));
                }
                Object[][] applied = first.executeInternal("SELECT * FROM " + KS + '.' + table);
                assertRows(cluster.get(2).executeInternal("SELECT * FROM " + KS + '.' + table), applied);
            }
            assertThat(first.logs().grep("Batch " + undecodable + " of system.batches, stored with messaging version " + ownVersion +
                                         ", cannot be decoded: ").getResult()).hasSize(1);
            for (IInvokableInstance node : cluster)
                assertThat(node.logs().grep("Exception in thread Thread\\[node\\d+_BatchlogTasks").getResult()).describedAs("node " + node.config().num()).isEmpty();
        }
    }

    /**
     * Stops the batchlog replay of {@code node}, which runs every 10 seconds and moves the start of its scan past the batches
     * a test writes an hour in the past, so that {@link #replayFromTheFirstBatch} alone replays them.
     */
    static void stopBatchlogReplay(IInvokableInstance node)
    {
        node.runOnInstance(() -> {
            try
            {
                BatchlogManager.instance.shutdownAndWait(1, TimeUnit.MINUTES);
            }
            catch (Exception e)
            {
                throw new AssertionError(e);
            }
        });
    }

    /** Replays the batchlog of {@code node} from its first batch, as its first replay after a start does. */
    static void replayFromTheFirstBatch(IInvokableInstance node)
    {
        node.runOnInstance(() -> {
            BatchlogManager restarted = new BatchlogManager();
            try
            {
                restarted.forceBatchlogReplay();
                restarted.shutdownAndWait(1, TimeUnit.MINUTES);
            }
            catch (Exception e)
            {
                throw new AssertionError(e);
            }
        });
    }
}
