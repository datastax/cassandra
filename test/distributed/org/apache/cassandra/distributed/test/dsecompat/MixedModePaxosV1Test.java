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

import org.junit.Test;

import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;

import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ONE;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.QUORUM;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.SERIAL;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.apache.cassandra.net.Verb.PAXOS_COMMIT_REQ;

/**
 * Lightweight transactions (Paxos v1, the only variant HCD 1.2 and DSE 6 have; the upgraded nodes run with
 * {@code paxos_variant: v1}) between HCD 1.2 nodes at messaging version 11 and nodes of this software in
 * {@code DSE_6} mode. Three nodes, RF 3, node 1 upgraded first, then node 2: after each upgrade conditional
 * inserts, updates and deletes are coordinated by both versions on the same partitions, their outcome is checked
 * from the other version with a SERIAL read, and a round whose commit never reached a quorum is completed by a
 * coordinator of the other version.
 */
public class MixedModePaxosV1Test extends DseDialectTestBase
{
    private static final int HCD = 3;

    @Test
    public void lightweightTransactionsAcrossVersions() throws Throwable
    {
        dse6TestCase(3)
        .nodesToUpgrade(1, 2)
        .setup(cluster -> cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".lwt (k int, c int, v int, PRIMARY KEY (k, c))"))
        .runAfterNodeUpgrade((cluster, upgraded) -> {
            for (int node = 1; node <= upgraded; node++)
                assertPeersAtDialect(cluster, node, cluster.size() - upgraded);
            int[][] directions = { { upgraded, HCD }, { HCD, upgraded } };
            for (int d = 0; d < directions.length; d++)
            {
                int base = upgraded * 1_000 + d * 100;
                ConsistencyLevel[] commitLevels = { ONE, QUORUM, ALL };
                for (int i = 0; i < commitLevels.length; i++)
                    conditionalUpdates(cluster, directions[d][0], directions[d][1], base + i, commitLevels[i]);
                unfinishedRoundCompletedByTheOtherVersion(cluster, directions[d][0], directions[d][1], base + 10);
            }
        })
        .run();
    }

    private static void conditionalUpdates(UpgradeableCluster cluster, int first, int second, int k, ConsistencyLevel commit)
    {
        String table = KEYSPACE + ".lwt";
        String serialRead = "SELECT c, v FROM " + table + " WHERE k = ?";

        assertRows(cluster.coordinator(first).execute("INSERT INTO " + table + " (k, c, v) VALUES (?, 1, 1) IF NOT EXISTS", commit, k), row(true));
        // the other version sees the row in the Paxos state and refuses a second insert, returning the current row
        assertRows(cluster.coordinator(second).execute("INSERT INTO " + table + " (k, c, v) VALUES (?, 1, 2) IF NOT EXISTS", commit, k),
                   row(false, k, 1, 1));
        assertRows(cluster.coordinator(second).execute(serialRead, SERIAL, k), row(1, 1));

        assertRows(cluster.coordinator(second).execute("UPDATE " + table + " SET v = 3 WHERE k = ? AND c = 1 IF v = 1", commit, k), row(true));
        assertRows(cluster.coordinator(first).execute("UPDATE " + table + " SET v = 4 WHERE k = ? AND c = 1 IF v = 1", commit, k), row(false, 3));
        assertRows(cluster.coordinator(first).execute("UPDATE " + table + " SET v = 5 WHERE k = ? AND c = 1 IF v = 3", commit, k), row(true));
        assertRows(cluster.coordinator(second).execute(serialRead, SERIAL, k), row(1, 5));
        assertRows(cluster.coordinator(first).execute(serialRead, SERIAL, k), row(1, 5));

        // IF EXISTS on a missing row, then on the row; a conditional batch on the partition
        assertRows(cluster.coordinator(second).execute("UPDATE " + table + " SET v = 6 WHERE k = ? AND c = 2 IF EXISTS", commit, k), row(false));
        assertRows(cluster.coordinator(second).execute("BEGIN BATCH " +
                                                       "INSERT INTO " + table + " (k, c, v) VALUES (?, 2, 20) " +
                                                       "UPDATE " + table + " SET v = 6 WHERE k = ? AND c = 1 IF v = 5 " +
                                                       "APPLY BATCH", commit, k, k),
                   row(true));
        assertRows(cluster.coordinator(first).execute(serialRead, SERIAL, k), row(1, 6), row(2, 20));
        assertRows(cluster.coordinator(first).execute("DELETE FROM " + table + " WHERE k = ? AND c = 2 IF EXISTS", commit, k), row(true));
        assertRows(cluster.coordinator(second).execute(serialRead, SERIAL, k), row(1, 6));

        // the commits reached every replica, whichever version coordinated them
        assertOnEveryReplica(cluster, "SELECT c, v FROM " + table + " WHERE k = " + k, row(1, 6));
    }

    /**
     * The commit of an accepted proposal is lost on the way to the other replicas: only the coordinator applied it.
     * A SERIAL read coordinated by the other version finds the accepted proposal (or the commit) in the prepare
     * responses, completes the round and returns the value; the replicas then all hold it.
     */
    private static void unfinishedRoundCompletedByTheOtherVersion(UpgradeableCluster cluster, int first, int second, int k)
    {
        String table = KEYSPACE + ".lwt";
        cluster.filters().outbound().verbs(PAXOS_COMMIT_REQ.id).from(first).drop();
        try
        {
            // commit at ONE: the coordinator's own replica acknowledges it, the others never receive it
            assertRows(cluster.coordinator(first).execute("INSERT INTO " + table + " (k, c, v) VALUES (?, 1, 7) IF NOT EXISTS", ONE, k), row(true));
        }
        finally
        {
            cluster.filters().reset();
        }
        for (int node = 1; node <= cluster.size(); node++)
            if (node != first)
                assertRows(cluster.get(node).executeInternal("SELECT v FROM " + table + " WHERE k = ?", k));

        assertRows(cluster.coordinator(second).execute("SELECT c, v FROM " + table + " WHERE k = ?", SERIAL, k), row(1, 7));
        // the round only commits to the replicas of the prepare quorum; a read at ALL repairs the last one if needed
        assertRows(cluster.coordinator(second).execute("SELECT c, v FROM " + table + " WHERE k = ?", ALL, k), row(1, 7));
        assertOnEveryReplica(cluster, "SELECT c, v FROM " + table + " WHERE k = " + k, row(1, 7));
    }
}
