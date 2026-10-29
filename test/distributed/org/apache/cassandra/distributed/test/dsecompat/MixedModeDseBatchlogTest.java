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

import org.junit.Test;

import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.dsecompat.DseFixtures;
import org.apache.cassandra.dsecompat.DseFixtures.DseVersion;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.apache.cassandra.utils.TimeUUID.Generator.atUnixMillis;
import static org.junit.Assert.assertEquals;

/**
 * An upgraded node in {@code DSE_6} mode replays the batches DSE 6 left in its {@code system.batches} (the mutations of the
 * DSE 6.8.65 and 6.9.26 hints fixtures, tagged with DSE's messaging versions 1024 and 1536) to a replica that is still an
 * HCD 1.2 node at messaging version 11: the mutations decoded from DSE's layout reach the old replica in the pre-4.0
 * dialect.
 */
public class MixedModeDseBatchlogTest extends DseDialectTestBase
{
    private static final String KS = DseFixtures.FIXTURE_KEYSPACE;

    @Test
    public void dseBatchesReplayedToAnOldReplica() throws Throwable
    {
        List<ByteBuffer> dse68 = DseFixtures.fixtureHintMutations(DseVersion.DSE_6_8_65);
        List<ByteBuffer> dse69 = DseFixtures.fixtureHintMutations(DseVersion.DSE_6_9_26);
        dse6TestCase(2)
        .nodesToUpgrade(1)
        .setup(cluster -> {
            for (String statement : fixtureSchema(DseVersion.DSE_6_8_65, 2))
                cluster.schemaChange(statement);
            cluster.schemaChange("CREATE TABLE " + KS + ".t69 (id int, ck text, fu frozen<addr>, l list<int>, m map<text, int>, " +
                                 "n bigint, s int static, tp frozen<tuple<int, text>>, u addr, v text, PRIMARY KEY (id, ck)) WITH ID = " +
                                 DseFixtures.fixtureTableId(DseVersion.DSE_6_9_26));
        })
        .runAfterNodeUpgrade((cluster, upgraded) -> {
            assertPeersAtDialect(cluster, 1, 1);
            long hourAgo = FBUtilities.now().toEpochMilli() - TimeUnit.HOURS.toMillis(1);
            UUID batch68 = atUnixMillis(hourAgo, 0).asUUID();
            UUID batch69 = atUnixMillis(hourAgo + 1, 0).asUUID();
            IInvokableInstance first = upgraded(cluster);
            DseBatchlogStateTest.stopBatchlogReplay(first);
            long hintsBefore = first.callOnInstance(() -> StorageMetrics.totalHints.getCount());
            String insert = "INSERT INTO system.batches (id, mutations, version) VALUES (?, ?, ?)";
            first.executeInternal(insert, batch68, dse68, DseVersion.DSE_6_8_65.relativeMessagingVersion << 8);
            first.executeInternal(insert, batch69, dse69, DseVersion.DSE_6_9_26.relativeMessagingVersion << 8);

            DseBatchlogStateTest.replayFromTheFirstBatch(first);

            assertRows(first.executeInternal("SELECT count(*) FROM system.batches"), row(0L));
            // the old replica acknowledged the replayed mutations: none was hinted
            assertEquals(hintsBefore, (long) first.callOnInstance(() -> StorageMetrics.totalHints.getCount()));
            // what the fixture data set wrote (test/data/dse-compat/README.md), on the upgraded node and on the HCD 1.2 one
            for (String table : new String[]{ "t", "t69" })
            {
                for (int node = 1; node <= cluster.size(); node++)
                {
                    assertRows(cluster.get(node).executeInternal("SELECT count(*) FROM " + KS + '.' + table),
                               row((long) DseFixtures.COLLECTION_TABLE_COUNT_STAR));
                    assertRows(cluster.get(node).executeInternal("SELECT n, s, v FROM " + KS + '.' + table + " WHERE id = 25 AND ck = 'ck2'"),
                               row(5000L, 250, "val50"));
                }
                assertRows(cluster.get(2).executeInternal("SELECT id, ck, l, m, n, s, tp, v FROM " + KS + '.' + table),
                           first.executeInternal("SELECT id, ck, l, m, n, s, tp, v FROM " + KS + '.' + table));
            }
        })
        .run();
    }

    /** The upgraded node, which runs this software: code can run inside it. */
    private static IInvokableInstance upgraded(UpgradeableCluster cluster)
    {
        return (IInvokableInstance) cluster.get(1);
    }
}
