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

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Test;

import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.dsecompat.DseFixtures;
import org.apache.cassandra.dsecompat.DseFixtures.PaxosRow;
import org.apache.cassandra.dsecompat.DseFixtures.Statement;
import org.apache.cassandra.service.paxos.Ballot;
import org.apache.cassandra.utils.StorageCompatibilityMode;

import static org.apache.cassandra.config.CassandraRelevantProperties.SAI_CURRENT_VERSION;
import static org.apache.cassandra.config.CassandraRelevantProperties.TRIE_INDEX_FORMAT_VERSION;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.QUORUM;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.SERIAL;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * A cluster of this software holding, on every node, the Paxos state DSE 6.8.65 left in {@code system.paxos}
 * ({@link DseFixtures#paxosRows()}, the replicas of a DSE 6.8.65 test cluster being upgraded in place, with their ballots
 * moved to an hour ago). In {@code NONE}, Paxos repair completes without errors, commits an accepted proposal of an
 * upgraded coordinator and leaves those DSE proposed to the next lightweight transaction (Paxos v1) or loses them (Paxos
 * v2). In {@code DSE_6}, which the nodes enter on a restart as on an upgrade, the test runs no repair: serial reads
 * complete the rounds instead, and only Paxos v1 runs, the only variant DSE 6 peers have. In both modes a row of a DSE
 * version this software does not know, deleted on every replica as its error says and flushed, leaves the lightweight
 * transactions of its partition (and in {@code NONE} the repairs) working, lightweight transactions on those partitions
 * work through every coordinator, and no node logs a decoding error.
 */
public class DsePaxosStateTest extends TestBaseImpl
{
    /** What a node logged on every LWT or Paxos repair of such a partition before the fix, as observed with DSE 6.8.65. */
    private static final String DECODING_ERRORS = "AssertionError|VIntOutOfRangeException|Failed paxos prepare locally|" +
                                                  "Failing paxos cleanup|Cannot decode system.paxos";

    @After
    public void clearTrieVersion()
    {
        // an instance in DSE_6 sets the property in the JVM it shares with the other instances
        TRIE_INDEX_FORMAT_VERSION.clearValue(); // checkstyle: suppress nearby 'clearValueSystemPropertyUsage'
    }

    @Test
    public void paxosV1InDse6() throws Throwable
    {
        // the nodes enter the mode on a restart, as on an upgrade
        repairAndLwtOnDsePaxosState(false, true, c -> c.set("storage_compatibility_mode", "HCD_1")
                                                       .set("uuid_sstable_identifiers_enabled", false)
                                                       .set("paxos_variant", "v1"));
    }

    @Test
    public void paxosV1InNone() throws Throwable
    {
        repairAndLwtOnDsePaxosState(false, false, c -> c.set("paxos_variant", "v1"));
    }

    @Test
    public void paxosV2InNone() throws Throwable
    {
        repairAndLwtOnDsePaxosState(true, false, c -> c.set("paxos_variant", "v2"));
    }

    private static void repairAndLwtOnDsePaxosState(boolean paxosV2, boolean dse6, Consumer<IInstanceConfig> config) throws Throwable
    {
        List<PaxosRow> rows = DseFixtures.paxosRowsOfAnHourAgo();
        List<PaxosRow> lwtRows = rows.stream().filter(r -> r.cfId.equals(DseFixtures.PAXOS_LWT_TABLE_ID)).collect(Collectors.toList());
        List<PaxosRow> emptyRounds = rows.stream().filter(r -> r.proposalBallot != null).collect(Collectors.toList());
        // two committed conditional batches of bbtest.t, left out of the state and saved on a quorum of the replicas (nodes 1
        // and 2) as accepted proposals, as when the commit got lost. The last digit of a ballot this software generates
        // carries the serial consistency of the round, GLOBAL for SERIAL; the ballots DSE generates end in 0, NONE. Paxos
        // repair completes the first, a round the third node of the test cluster coordinated once it ran this software and
        // DSE replicas stored with DSE's version; it skips the second, a round of a DSE coordinator.
        List<PaxosRow> batches = rows.stream()
                                     .filter(r -> r.cfId.equals(DseFixtures.PAXOS_COLLECTION_TABLE_ID) && r.proposalBallot == null)
                                     .collect(Collectors.toList());
        // a row stored with a DSE version this software does not know, not one of the rows the coordinators use below
        assertThat(emptyRounds.size()).isGreaterThan(4);
        PaxosRow unknown = emptyRounds.get(emptyRounds.size() - 1);
        PaxosRow fromUpgraded = batches.stream().filter(r -> r.casBatchValue().equals("cas-dse-node-3.lab")).findFirst().orElseThrow(AssertionError::new);
        PaxosRow fromDse = batches.stream().filter(r -> r.casBatchValue().equals("cas-dse-node-1.lab")).findFirst().orElseThrow(AssertionError::new);
        assertThat(Ballot.fromUuid(fromUpgraded.mostRecentCommitAt).flag()).isEqualTo(Ballot.Flag.GLOBAL);
        assertThat(Ballot.fromUuid(fromDse.mostRecentCommitAt).flag()).isEqualTo(Ballot.Flag.NONE);
        WithProperties properties = new WithProperties().clear(TRIE_INDEX_FORMAT_VERSION);
        if (dse6)
            properties.set(SAI_CURRENT_VERSION, "aa"); // DSE_6 needs the SAI on-disk version pinned; these nodes store no DSE release
        try (properties;
             Cluster cluster = Cluster.build(3).withConfig(c -> config.accept(c.with(NETWORK, GOSSIP))).createWithoutStarting())
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
            for (String statement : DseFixtures.paxosSchema(3))
                cluster.schemaChange(statement);
            for (PaxosRow row : lwtRows)
                cluster.coordinator(1).execute("INSERT INTO lab.lwt (id, v) VALUES (?, 2)", ALL, row.key());
            for (IInvokableInstance node : cluster)
                for (PaxosRow row : rows)
                    if (row != fromUpgraded && row != fromDse)
                        for (Statement statement : row.saveStatements(row == unknown ? DseFixtures.PAXOS_UNKNOWN_DSE_VERSION : row.mostRecentCommitVersion))
                            node.executeInternal(statement.cql, statement.values);
            for (PaxosRow accepted : new PaxosRow[]{ fromUpgraded, fromDse })
            {
                Statement proposal = accepted.saveCommitAsAcceptedProposal(accepted.mostRecentCommitVersion);
                for (int node = 1; node <= 2; node++)
                    cluster.get(node).executeInternal(proposal.cql, proposal.values);
            }
            // in sstables, as DSE left it
            for (IInvokableInstance node : cluster)
                node.nodetoolResult("flush", "system", "paxos").asserts().success();

            // the remedy its error names for the row of a DSE version this software does not know, on every replica: delete it
            // above the timestamps of its cells, then flush (the flush failed on such a deleted row before the fix)
            for (IInvokableInstance node : cluster)
            {
                Object[][] writetimes = node.executeInternal("SELECT writetime(in_progress_ballot), writetime(in_progress_read_ballot), " +
                                                             "writetime(proposal_ballot), writetime(proposal_version), " +
                                                             "writetime(most_recent_commit_at) " +
                                                             "FROM system.paxos WHERE row_key = ? AND cf_id = ?", unknown.rowKey, unknown.cfId);
                assertThat(writetimes.length).isEqualTo(1);
                long[] cells = Arrays.stream(writetimes[0]).filter(Objects::nonNull).mapToLong(w -> (Long) w).toArray();
                Statement deletion = DseFixtures.paxosRowDeletion(unknown, cells);
                node.executeInternal(deletion.cql, deletion.values);
                node.nodetoolResult("flush", "system", "paxos").asserts().success();
            }

            if (dse6)
            {
                // the schema is in place: every node enters the mode on a restart, on the state above
                // (each start waits for gossip to settle, so every node sees the others up before the reads below)
                for (IInvokableInstance node : cluster)
                {
                    node.shutdown().get();
                    node.config().set("internode_compatibility_mode", "DSE_6");
                    node.startup();
                }
            }

            // Paxos repair of every key, then the other repairs with their Paxos step, which flooded the logs with
            // AssertionErrors before the fix (in DSE_6 the test runs no repair, and the rounds are completed by serial
            // reads below)
            repairAndCheckLogs(cluster, dse6);
            Object[][] dseBatch = { row("u1", fromDse.casBatchValue()), row("u3", fromDse.casBatchValue()) };
            Object[][] upgradedBatch = { row("u1", fromUpgraded.casBatchValue()), row("u3", fromUpgraded.casBatchValue()) };
            if (dse6)
            {
                // a serial read completes the first proposal, a read at ALL brings it to every replica
                assertRows(cluster.coordinator(3).execute("SELECT ck, v FROM bbtest.t WHERE id = ?", SERIAL, fromUpgraded.key()), upgradedBatch);
                assertRows(cluster.coordinator(3).execute("SELECT ck, v FROM bbtest.t WHERE id = ?", ALL, fromUpgraded.key()), upgradedBatch);
            }
            for (IInvokableInstance node : cluster)
            {
                // the first proposal reached every replica: by the commit of the Paxos repair, or in DSE_6 by the commit of the
                // serial read or the read repair of the read at ALL
                assertRows(node.executeInternal("SELECT ck, v FROM bbtest.t WHERE id = ?", fromUpgraded.key()), upgradedBatch);
                // committed by this software, at its version; the Paxos repair waits for every replica, the commit of a serial
                // read only for a quorum, the last replica gets it shortly after
                int ownVersion = node.callOnInstance(() -> StorageCompatibilityMode.current().storageMessagingVersion());
                Awaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> assertRows(
                    node.executeInternal("SELECT most_recent_commit_version FROM system.paxos WHERE row_key = ? AND cf_id = ?",
                                         fromUpgraded.rowKey, fromUpgraded.cfId),
                    row(ownVersion)));
                // the second was left alone by the Paxos repair (in DSE_6 nothing has touched it yet)
                assertRows(node.executeInternal("SELECT ck, v FROM bbtest.t WHERE id = ?", fromDse.key()));
            }
            if (paxosV2)
            {
                // Paxos v2 ignores a proposal older than the Paxos repair of its range: DSE's accepted round is lost, which
                // is why paxos_variant stays v1 until DSE's Paxos rows have expired
                assertRows(cluster.coordinator(3).execute("SELECT ck, v FROM bbtest.t WHERE id = ?", SERIAL, fromDse.key()));
            }
            else
            {
                // a serial read through the replica that did not accept it completes it, a read at ALL finds it everywhere
                assertRows(cluster.coordinator(3).execute("SELECT ck, v FROM bbtest.t WHERE id = ?", SERIAL, fromDse.key()), dseBatch);
                assertRows(cluster.coordinator(3).execute("SELECT ck, v FROM bbtest.t WHERE id = ?", ALL, fromDse.key()), dseBatch);
            }

            // serial reads and conditional updates through every coordinator
            for (int node = 1; node <= cluster.size(); node++)
            {
                int lwt = lwtRows.get(node).key();
                assertRows(cluster.coordinator(node).execute("SELECT v FROM lab.lwt WHERE id = ?", SERIAL, lwt), row(2));
                assertRows(cluster.coordinator(node).execute("UPDATE lab.lwt SET v = 3 WHERE id = ? IF v = 2", QUORUM, lwt), row(true));
                assertRows(cluster.coordinator(node).execute("SELECT v FROM lab.lwt WHERE id = ?", SERIAL, lwt), row(3));

                int collection = emptyRounds.get(node).key();
                assertRows(cluster.coordinator(node).execute("SELECT v FROM bbtest.t WHERE id = ?", SERIAL, collection));
                assertRows(cluster.coordinator(node).execute("INSERT INTO bbtest.t (id, ck, v) VALUES (?, 'u1', 'x') IF NOT EXISTS",
                                                             QUORUM, collection),
                           row(true));
            }

            // the partition of the deleted row takes lightweight transactions again
            assertRows(cluster.coordinator(1).execute("INSERT INTO bbtest.t (id, ck, v) VALUES (?, 'u1', 'x') IF NOT EXISTS", QUORUM, unknown.key()),
                       row(true));

            // and again over the state this software wrote back next to DSE's
            repairAndCheckLogs(cluster, dse6);
        }
    }

    private static void repairAndCheckLogs(Cluster cluster, boolean dse6)
    {
        for (String keyspace : new String[]{ "lab", "bbtest" })
        {
            if (dse6)
                continue; // no repair: serial reads complete the rounds, only the logs are checked
            cluster.get(1).nodetoolResult("repair", "--paxos-only", keyspace).asserts().success();
            cluster.get(2).nodetoolResult("repair", "-full", keyspace).asserts().success();
            cluster.get(3).nodetoolResult("repair", keyspace).asserts().success(); // incremental
        }
        for (IInvokableInstance node : cluster)
            assertThat(node.logs().grep(DECODING_ERRORS).getResult()).describedAs("node " + node.config().num()).isEmpty();
    }
}
