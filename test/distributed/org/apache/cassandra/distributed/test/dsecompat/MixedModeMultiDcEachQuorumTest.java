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

import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.junit.Test;

import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.IMessageFilters;

import static org.apache.cassandra.distributed.api.ConsistencyLevel.EACH_QUORUM;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.QUORUM;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.apache.cassandra.distributed.test.dsecompat.TwoDataCenters.DC1;
import static org.apache.cassandra.distributed.test.dsecompat.TwoDataCenters.DC2;
import static org.apache.cassandra.distributed.test.dsecompat.TwoDataCenters.dataCenter;
import static org.apache.cassandra.distributed.test.dsecompat.TwoDataCenters.isUpgraded;
import static org.apache.cassandra.distributed.test.dsecompat.TwoDataCenters.networkTopology;
import static org.apache.cassandra.distributed.test.dsecompat.TwoDataCenters.otherVersionInOtherDataCenter;
import static org.apache.cassandra.distributed.test.dsecompat.TwoDataCenters.recordRoutes;
import static org.apache.cassandra.distributed.test.dsecompat.TwoDataCenters.target;
import static org.apache.cassandra.net.Verb.MUTATION_RSP;
import static org.apache.cassandra.net.Verb.READ_REQ;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@code EACH_QUORUM} writes and reads across two data centers, between HCD 1.2 nodes at messaging version 11 (standing in
 * for DSE 6) and nodes of this software in {@code DSE_6} mode, in the layout of {@link TwoDataCenters}: three replicas in
 * each data center, {@code datacenter1} partly upgraded (node 1 of 1 to 3), {@code datacenter2} mostly (nodes 4 and 5 of 4
 * to 6). The coordinator counts the acknowledgements per data center, whichever version sends them and whichever forwarded
 * the write into the other data center; a read at {@code EACH_QUORUM} repairs the replicas it contacts in each data
 * center.
 */
public class MixedModeMultiDcEachQuorumTest extends DseDialectTestBase
{
    private static final String KS = "ks_eq";
    private static final String TABLE = KS + ".t";
    private static final int KEYS = 10;

    @Test
    public void eachQuorumAcrossVersions() throws Throwable
    {
        TwoDataCenters.testCase(config -> config.set("dynamic_snitch", false))
                      .setup(cluster -> {
                          cluster.schemaChange("CREATE KEYSPACE " + KS + " WITH replication = " + networkTopology(3, 3));
                          cluster.schemaChange("CREATE TABLE " + TABLE + " (k int PRIMARY KEY, v int)");
                      })
                      .runAfterClusterUpgrade(cluster -> {
                          TwoDataCenters.assertMixed(cluster);
                          for (int coordinator = 1; coordinator <= cluster.size(); coordinator++)
                              writeAndReadAtEachQuorum(cluster, coordinator, 1_000 * coordinator);
                          oneAcknowledgementLostInDatacenter2(cluster, 10_000);
                          twoAcknowledgementsLostInDatacenter2(cluster, 20_000);
                          eachQuorumReadRepairsBothDataCenters(cluster, 30_000);
                      })
                      .run();
    }

    /**
     * Writes at {@code EACH_QUORUM} through one node: every replica of both data centers gets the rows, read back at
     * {@code EACH_QUORUM} through a node of the other version in the other data center.
     */
    private static void writeAndReadAtEachQuorum(UpgradeableCluster cluster, int coordinator, int base)
    {
        int reader = otherVersionInOtherDataCenter(coordinator);
        for (int k = base; k < base + KEYS; k++)
            cluster.coordinator(coordinator).execute("INSERT INTO " + TABLE + " (k, v) VALUES (?, ?)", EACH_QUORUM, k, coordinator);
        // every replica first, so that the read repair of the read cannot make up for a forwarded write that never arrived
        for (int k = base; k < base + KEYS; k++)
            assertOnEveryReplica(cluster, "SELECT v FROM " + TABLE + " WHERE k = " + k, row(coordinator));
        for (int k = base; k < base + KEYS; k++)
            assertRows(cluster.coordinator(reader).execute("SELECT v FROM " + TABLE + " WHERE k = ?", EACH_QUORUM, k), row(coordinator));
    }

    /**
     * Node 6 (HCD 1.2) applies the writes but its acknowledgements are lost: {@code datacenter2} still has a quorum, from
     * the two upgraded nodes, for coordinators of both versions in {@code datacenter1}.
     */
    private static void oneAcknowledgementLostInDatacenter2(UpgradeableCluster cluster, int base)
    {
        IMessageFilters.Filter lostAcknowledgements = cluster.filters().outbound().verbs(MUTATION_RSP.id).from(6).drop();
        try
        {
            for (int coordinator : new int[]{ 1, 2 })
                for (int k = base + coordinator * 100; k < base + coordinator * 100 + KEYS; k++)
                    cluster.coordinator(coordinator).execute("INSERT INTO " + TABLE + " (k, v) VALUES (?, ?)", EACH_QUORUM, k, coordinator);
        }
        finally
        {
            lostAcknowledgements.off();
        }
        for (int coordinator : new int[]{ 1, 2 })
            for (int k = base + coordinator * 100; k < base + coordinator * 100 + KEYS; k++)
                assertOnEveryReplica(cluster, "SELECT v FROM " + TABLE + " WHERE k = " + k, row(coordinator));
    }

    /**
     * The acknowledgements of nodes 5 (this software) and 6 (HCD 1.2) are lost: {@code datacenter2} has one acknowledgement
     * of three, so an {@code EACH_QUORUM} write times out through a coordinator of either version, although four of the six
     * replicas acknowledge it, which is enough for {@code QUORUM}.
     */
    private static void twoAcknowledgementsLostInDatacenter2(UpgradeableCluster cluster, int base)
    {
        IMessageFilters.Filter lostAcknowledgements = cluster.filters().outbound().verbs(MUTATION_RSP.id).from(5, 6).drop();
        try
        {
            for (int coordinator : new int[]{ 1, 2 })
            {
                int k = base + coordinator;
                assertThatThrownBy(() -> cluster.coordinator(coordinator).execute("INSERT INTO " + TABLE + " (k, v) VALUES (?, ?)", EACH_QUORUM, k, 1))
                .as("EACH_QUORUM write through node %s with one acknowledgement in datacenter2", coordinator)
                .satisfies(t -> assertThat(t.getClass().getSimpleName()).isEqualTo("WriteTimeoutException"));
                cluster.coordinator(coordinator).execute("INSERT INTO " + TABLE + " (k, v) VALUES (?, ?)", QUORUM, k + 100, 2);
            }
        }
        finally
        {
            lostAcknowledgements.off();
        }
        // only the acknowledgements were lost: every replica applied the QUORUM writes
        for (int coordinator : new int[]{ 1, 2 })
            assertOnEveryReplica(cluster, "SELECT v FROM " + TABLE + " WHERE k = " + (base + coordinator + 100), row(2));
    }

    /**
     * A row written on one of the replicas an {@code EACH_QUORUM} read contacts in each data center (a node of the other
     * version than the reader where there is one), read at {@code EACH_QUORUM} through nodes of both versions: the read
     * returns it and, before returning, repairs it on the other replica it contacted in each data center. Without the
     * dynamic snitch the replicas contacted for a key do not change, which a first read of the key records.
     */
    private static void eachQuorumReadRepairsBothDataCenters(UpgradeableCluster cluster, int base)
    {
        for (int reader : new int[]{ 2, 4 })
        {
            int k = base + reader;
            String select = "SELECT v FROM " + TABLE + " WHERE k = ?";
            Set<Integer> contacted = contactedByEachQuorumRead(cluster, reader, k, select);
            for (String dataCenter : new String[]{ DC1, DC2 })
            {
                List<Integer> local = contacted.stream().filter(n -> dataCenter(n).equals(dataCenter)).collect(Collectors.toList());
                assertThat(local).as("replicas of %s an EACH_QUORUM read through node %s contacts", dataCenter, reader).hasSize(2);
                int holder = local.stream().filter(n -> isUpgraded(n) != isUpgraded(reader)).findFirst().orElse(local.get(0));
                cluster.get(holder).executeInternal("INSERT INTO " + TABLE + " (k, v) VALUES (?, ?) USING TIMESTAMP 1", k, reader);
            }
            assertThat(contactedByEachQuorumRead(cluster, reader, k, select, row(reader))).as("replicas contacted again").isEqualTo(contacted);
            for (int node : contacted)
                assertRows(cluster.get(node).executeInternal(select, k), row(reader));
        }
    }

    /**
     * The replicas an {@code EACH_QUORUM} read of a key through a node contacts (the node itself and the targets of its
     * read requests), the read returning the expected rows.
     */
    private static Set<Integer> contactedByEachQuorumRead(UpgradeableCluster cluster, int reader, int k, String select, Object[]... expected)
    {
        Set<String> reads = ConcurrentHashMap.newKeySet();
        IMessageFilters.Filter recorder = recordRoutes(cluster, READ_REQ, reads);
        try
        {
            assertRows(cluster.coordinator(reader).execute(select, EACH_QUORUM, k), expected);
        }
        finally
        {
            recorder.off();
        }
        Set<Integer> contacted = new TreeSet<>();
        contacted.add(reader);
        for (String route : reads)
            if (route.startsWith(reader + "->"))
                contacted.add(target(route));
        return contacted;
    }
}
