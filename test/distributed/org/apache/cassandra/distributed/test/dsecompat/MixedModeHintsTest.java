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

import org.junit.Ignore;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.metrics.HintsServiceMetrics;

import static org.apache.cassandra.distributed.api.ConsistencyLevel.ONE;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.apache.cassandra.net.Verb.HINT_RSP;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Hinted handoff between HCD 1.2 nodes at messaging version 11 and a node of this software in {@code DSE_6} mode.
 * Three nodes, RF 3, node 1 upgraded. A replica is stopped, rows are written at ONE by a coordinator of the other
 * version, which stores hints for the stopped replica when the mutations time out, and the replica is restarted:
 * the hints must then reach it across the version boundary.
 */
public class MixedModeHintsTest extends DseDialectTestBase
{
    private static final int UPGRADED = 1;
    private static final int HCD = 2;
    private static final int KEYS = 20;

    /**
     * This software coordinates while the HCD 1.2 node is down. Its hints files are written at its own messaging
     * version, so every hint is decoded and sent again as a HINT_REQ at 11, and counts as delivered only once the peer
     * acknowledges it, as DSE 6.9 does for a hint it applies: HCD 1.2 answers a hint with an empty response at 11, verb
     * REQUEST_RSP, matched to the hint's callback by its message id. The file is then removed. HCD 1.2 also
     * acknowledges a hint it drops, which DSE does not: only DseHintsAcknowledgementTest covers a hint left without an
     * answer by the peer itself.
     */
    @Test
    public void hintsFromUpgradedNodeToHcd() throws Throwable
    {
        testCase()
        .runAfterClusterUpgrade(cluster -> {
            assertPeersAtDialect(cluster, UPGRADED, 2);
            long acknowledged = hintsSucceeded(cluster);
            hintsFor(cluster, HCD, UPGRADED);
            awaitAsserted(60, () -> assertEquals("no hints file left", 0, pendingHintsFiles(cluster)));
            // at least: a page that timed out on a slow machine is sent, and acknowledged, again
            assertTrue("every hint was acknowledged", hintsSucceeded(cluster) - acknowledged >= KEYS);
            assertPeersAtDialect(cluster, UPGRADED, 2);
        })
        .run();
    }

    /**
     * The acknowledgements of the HCD 1.2 node are dropped: the hints reach it, but this software counts them as not
     * delivered, keeps the hints file and sends the hints again, until the acknowledgements get through again.
     */
    @Test
    public void hintsToHcdWaitForTheAcknowledgement() throws Throwable
    {
        testCase()
        .runAfterClusterUpgrade(cluster -> {
            assertPeersAtDialect(cluster, UPGRADED, 2);
            long notDelivered = hintsNotDelivered(cluster);
            long acknowledged = hintsSucceeded(cluster);
            cluster.filters().outbound().verbs(HINT_RSP.id).from(HCD).to(UPGRADED).drop();
            try
            {
                hintsFor(cluster, HCD, UPGRADED);
                // the hints were applied, not acknowledged: they are not counted as delivered and the file stays
                // (at least KEYS: the page is sent again every ten seconds)
                awaitAsserted(60, () -> assertTrue("unacknowledged hints are not delivered",
                                                   hintsNotDelivered(cluster) - notDelivered >= KEYS));
                assertEquals("no hint counts as delivered without its acknowledgement", acknowledged, hintsSucceeded(cluster));
                assertTrue("the hints file stays", pendingHintsFiles(cluster) > 0);
                // HCD 1.2 is at version 11: the upgraded node takes it for a DSE 6 node and warns
                awaitAsserted(() -> assertFalse("the upgraded node logs the DSE 6 WARN",
                                                upgraded(cluster).logs()
                                                                 .grep("DSE 6 compatibility mode: hints not acknowledged by DSE 6 " +
                                                                       "node .*: \\d+ in file .*\\.hints; the file stays")
                                                                 .getResult()
                                                                 .isEmpty()));
            }
            finally
            {
                cluster.filters().reset();
            }
            // the next dispatch, at most ten seconds later, sends them again and is acknowledged (at least KEYS: a
            // dispatch in flight when the filter was reset may be acknowledged in part, and its page sent again)
            awaitAsserted(60, () -> assertEquals("no hints file left", 0, pendingHintsFiles(cluster)));
            assertTrue("every hint was acknowledged", hintsSucceeded(cluster) - acknowledged >= KEYS);
            assertPeersAtDialect(cluster, UPGRADED, 2);
        })
        .run();
    }

    /**
     * An HCD 1.2 node coordinates while the upgraded node is down; the upgraded node then restarts in {@code DSE_6}
     * mode among HCD 1.2 peers.
     */
    @Ignore("HCD 1.2, like Cassandra 3.0 to 4.0 and DSE 6, dispatches hints only to a peer that gossips its own " +
            "schema version (HintsDispatchTrigger, HintsEndpointProvider.isSameSchemaVersion), and a node of this " +
            "software in DSE_6 mode computes a different schema version for the same schema (checked with nodetool " +
            "describecluster), so these hints stay on the HCD 1.2 node until the versions agree. This software has no " +
            "such check, hence the other direction works.")
    @Test
    public void hintsFromHcdToUpgradedNode() throws Throwable
    {
        testCase()
        .runAfterClusterUpgrade(cluster -> {
            assertPeersAtDialect(cluster, UPGRADED, 2);
            hintsFor(cluster, UPGRADED, HCD);
            assertPeersAtDialect(cluster, UPGRADED, 2);
        })
        .run();
    }

    private static TestCase testCase()
    {
        return dse6TestCase(3, config -> config.set("hinted_handoff_enabled", true)
                                               .set("hints_flush_period_in_ms", 1000))
               .nodesToUpgrade(UPGRADED)
               .setup(cluster -> cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".t (k int PRIMARY KEY, v text)"));
    }

    /** The upgraded node, which runs this software: code can run inside it. */
    private static IInvokableInstance upgraded(UpgradeableCluster cluster)
    {
        return (IInvokableInstance) cluster.get(UPGRADED);
    }

    private static long hintsSucceeded(UpgradeableCluster cluster)
    {
        return upgraded(cluster).callOnInstance(() -> HintsServiceMetrics.hintsSucceeded.getCount());
    }

    /** Hints that timed out, or failed when the expiry of their callback came first (it reports a timeout failure). */
    private static long hintsNotDelivered(UpgradeableCluster cluster)
    {
        return upgraded(cluster).callOnInstance(() -> HintsServiceMetrics.hintsTimedOut.getCount() + HintsServiceMetrics.hintsFailed.getCount());
    }

    /**
     * The hints files in the hints directory of the upgraded node, all for the HCD 1.2 node in these tests. Counted on
     * disk: a file leaves the dispatch queue while it is being dispatched, but is deleted only once delivered.
     */
    private static int pendingHintsFiles(UpgradeableCluster cluster)
    {
        return upgraded(cluster).callOnInstance(() -> DatabaseDescriptor.getHintsDirectory()
                                                                            .tryList(f -> f.name().matches("^[0-9a-fA-F-]{36}-\\d+-\\d+\\.hints$"))
                                                                            .length);
    }

    private static void hintsFor(UpgradeableCluster cluster, int down, int coordinator) throws Exception
    {
        String table = KEYSPACE + ".t";
        cluster.get(down).shutdown().get();
        // the nodes run without gossip, so the coordinator keeps sending to the stopped replica; each mutation it
        // sends there times out and is hinted
        for (int k = 0; k < KEYS; k++)
            cluster.coordinator(coordinator).execute("INSERT INTO " + table + " (k, v) VALUES (?, ?)", ONE, k, "v" + k);
        // the third replica got the writes directly
        int other = 6 - down - coordinator;
        awaitAsserted(() -> assertRows(cluster.get(other).executeInternal("SELECT count(*) FROM " + table), row((long) KEYS)));

        cluster.get(down).startup();
        assertRows(cluster.get(down).executeInternal("SELECT count(*) FROM " + table), row(0L));
        // the dispatch of the in-JVM nodes is off without gossip: start it on the coordinator, once
        startHintsDispatch(coordinator);
        // the coordinator's hints reach the restarted replica; nothing else sends it these rows (the dispatcher looks
        // for hints every ten seconds)
        awaitAsserted(120, () -> assertRows(cluster.get(down).executeInternal("SELECT count(*) FROM " + table), row((long) KEYS)));
        for (int k = 0; k < KEYS; k++)
            assertRows(cluster.get(down).executeInternal("SELECT v FROM " + table + " WHERE k = ?", k), row("v" + k));
    }
}
