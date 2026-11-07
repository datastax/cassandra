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

package org.apache.cassandra.service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.dsecompat.DseFixtures;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.HeartBeatState;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.DseCompatRefusals;
import org.apache.cassandra.net.DsePeers;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.repair.messages.PrepareMessage;

import static org.apache.cassandra.config.CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE_LOST_NODE_SILENCE_SECONDS;
import static org.apache.cassandra.config.CassandraRelevantProperties.RING_DELAY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The operational guards of the upgrade in the DSE 5.1 compatibility mode: in progress from the start of the mode, a
 * single-node cluster included; the DSE 5.1 peers (at 10 or 11, or gossiping such a {@code NET_VERSION}) are the ones
 * they name and count, a lost DSE 5.1 node can be removed as a lost DSE 6 node can in {@code DSE_6}, and every text
 * names DSE 5.1 and the mode. {@code Dse6UpgradeGuardsTest} keeps the DSE 6 texts.
 */
public class Dse51UpgradeGuardsTest
{
    private static InetAddressAndPort dse51Peer;
    private InternodeCompatibilityMode modeBefore;
    private File metadataBefore;
    private Path metadata;
    private WithProperties lostNodeSilence;

    @BeforeClass
    public static void init() throws Exception
    {
        DatabaseDescriptor.daemonInitialization();
        dse51Peer = InetAddressAndPort.getByName("127.0.0.71:7000");
    }

    @Before
    public void setUp() throws IOException
    {
        modeBefore = DatabaseDescriptor.getInternodeCompatibilityMode();
        metadataBefore = DatabaseDescriptor.getMetadataDirectory();
        metadata = Files.createTempDirectory("dse51-upgrade-guards");
        DatabaseDescriptor.setMetadataDirectory(new File(metadata));
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_5_1);
        DseCompatRefusals.reset();
        lostNodeSilence = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE_LOST_NODE_SILENCE_SECONDS, 0);
    }

    @After
    public void tearDown() throws IOException
    {
        lostNodeSilence.close();
        try
        {
            Gossiper.runInGossipStageBlocking(() -> Gossiper.instance.unsafeAnnulEndpoint(dse51Peer));
            MessagingService.instance().versions.reset(dse51Peer);
            DsePeers.reset();
            DsePeersRecord.clear();
        }
        finally
        {
            DatabaseDescriptor.setInternodeCompatibilityMode(modeBefore);
            DseCompatRefusals.reset();
            DatabaseDescriptor.setMetadataDirectory(metadataBefore);
            new File(metadata).deleteRecursive();
        }
    }

    /** The upgrade is in progress in the mode, with no peer yet (a single-node cluster), and the refusal names it. */
    @Test
    public void operationsAreRefusedInDse51Mode()
    {
        assertThat(DseUpgradeGuards.upgradeInProgress()).isTrue();
        assertThatThrownBy(() -> DseUpgradeGuards.refuseWhileUpgrading("decommission"))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("decommission is refused while the DSE 5.1 upgrade is in progress " +
                    "(internode_compatibility_mode: DSE_5_1); finish the upgrade of every node and drop the mode " +
                    "first");

        MessagingService.instance().versions.set(dse51Peer, MessagingService.VERSION_3014);
        assertThat(DseUpgradeGuards.knownDsePeers()).containsExactly(dse51Peer);
        assertThatThrownBy(() -> DseUpgradeGuards.refuseWhileUpgrading("repair"))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("repair is refused while the DSE 5.1 upgrade is in progress (internode_compatibility_mode: " +
                    "DSE_5_1, DSE 5.1 peers: [" + dse51Peer + "]); finish the upgrade of every node and drop the " +
                    "mode first");
        assertThat(DseCompatRefusals.count("decommission")).isEqualTo(1);
        assertThat(DseCompatRefusals.count("repair")).isEqualTo(1);
    }

    @Test
    public void aReplacementIsRefusedNamingTheRemovalInDse51Mode()
    {
        assertThatThrownBy(DseUpgradeGuards::refuseReplacementWhileUpgrading)
        .isInstanceOf(ConfigurationException.class)
        .hasMessage("replacing a node is refused while the DSE 5.1 upgrade is in progress " +
                    "(internode_compatibility_mode: DSE_5_1); a replacement cannot start in " +
                    "internode_compatibility_mode DSE_5_1; remove the node lost for good instead, a DSE 5.1 node or " +
                    "a node of this software (nodetool removenode once every DSE 5.1 node still in the cluster is " +
                    "lost, nodetool assassinate otherwise), and add a new node once the upgrade is finished");
    }

    /** A DSE 5.1 peer seen in gossip only (its NET_VERSION 11) counts too; one seen at 4.0 or later does not. */
    @Test
    public void aDse51PeerInGossipIsKnown()
    {
        EndpointState state = stateWithNetVersion(MessagingService.VERSION_3014);
        Gossiper.instance.endpointStateMap.put(dse51Peer, state);
        assertThat(DseUpgradeGuards.knownDsePeers()).containsExactly(dse51Peer);
        state.addApplicationState(ApplicationState.NET_VERSION, VersionedValue.unsafeMakeVersionedValue("12", 2));
        assertThat(DseUpgradeGuards.knownDsePeers()).isEmpty();
    }

    /**
     * A DSE 5.1 node lost for good (down, as its gossip state says) can be removed: the three removals are allowed, not
     * counted, each with its WARN naming DSE 5.1; with another DSE 5.1 node alive, removenode, which streams, is
     * refused, naming it, and assassinate is still allowed.
     */
    @Test
    public void aLostDse51NodeCanBeRemoved() throws Throwable
    {
        EndpointState state = stateWithNetVersion(MessagingService.VERSION_3014);
        Gossiper.instance.endpointStateMap.put(dse51Peer, state);
        Gossiper.runInGossipStageBlocking(() -> Gossiper.instance.markDead(dse51Peer, state));
        MessagingService.instance().versions.set(dse51Peer, MessagingService.VERSION_3014);
        assertThat(DseUpgradeGuards.isLostDsePeer(dse51Peer)).isTrue();
        assertThat(DseUpgradeGuards.liveDsePeers()).isEmpty();
        List<String> warnings = warnings(() -> {
            DseUpgradeGuards.refuseRemoveNodeWhileUpgrading(dse51Peer);
            DseUpgradeGuards.refuseAssassinateWhileUpgrading(dse51Peer);
            DseUpgradeGuards.refuseForcedRemovalWhileUpgrading(Collections.singleton(dse51Peer));
        });
        assertThat(DseCompatRefusals.total()).isZero();
        assertThat(warnings).hasSize(3);
        assertThat(warnings.get(0)).startsWith("Allowing the removal of the DSE 5.1 node " + dse51Peer + ", silent ")
                                   .contains("during the DSE 5.1 upgrade: its data is streamed from its other " +
                                             "replicas, all of this software (a DSE 5.1 node that comes back");
        assertThat(warnings.get(1)).startsWith("Allowing the assassination of the DSE 5.1 node " + dse51Peer)
                                   .contains("during the DSE 5.1 upgrade: nothing is streamed");
        assertThat(warnings.get(2)).startsWith("Allowing the forced removal of the lost nodes [" + dse51Peer + "]")
                                   .contains("during the DSE 5.1 upgrade: the data not yet streamed");

        // another DSE 5.1 node alive: removenode is refused, assassinate allowed
        InetAddressAndPort live = InetAddressAndPort.getByName("127.0.0.72:7000");
        try
        {
            Gossiper.instance.endpointStateMap.put(live, stateWithNetVersion(MessagingService.VERSION_3014));
            MessagingService.instance().versions.set(live, MessagingService.VERSION_3014);
            assertThat(DseUpgradeGuards.liveDsePeers()).containsExactly(live);
            assertThatThrownBy(() -> DseUpgradeGuards.refuseRemoveNodeWhileUpgrading(dse51Peer))
            .isInstanceOf(UnsupportedOperationException.class)
            .hasMessage("removenode of the DSE 5.1 node " + dse51Peer + " lost for good is refused while DSE 5.1 " +
                        "nodes not known to be lost are in the cluster ([" + live + "]): it streams data with the " +
                        "other replicas, which DSE 5.1 nodes cannot do with this software; upgrade those DSE 5.1 " +
                        "nodes first, or remove the lost node with nodetool assassinate 127.0.0.71:7000 (no " +
                        "streaming: the data of its ranges is then held by fewer replicas until a full repair, which " +
                        "runs once the upgrade is finished)");
            assertThat(DseCompatRefusals.count("removenode")).isEqualTo(1);
            DseUpgradeGuards.refuseAssassinateWhileUpgrading(dse51Peer);
        }
        finally
        {
            Gossiper.runInGossipStageBlocking(() -> Gossiper.instance.unsafeAnnulEndpoint(live));
            MessagingService.instance().versions.reset(live);
        }
    }

    /** The refusal of a silence below the floor names the DSE 5.1 upgrade. */
    @Test
    public void aLostNodeSilenceBelowTheFloorIsRefusedNamingDse51()
    {
        try (WithProperties properties = new WithProperties()
                                         .set(INTERNODE_COMPATIBILITY_MODE_LOST_NODE_SILENCE_SECONDS, 59)
                                         .set(RING_DELAY, 30000))
        {
            assertThatThrownBy(DseUpgradeGuards::checkLostNodeSilence)
            .isInstanceOf(ConfigurationException.class)
            .hasMessage("-Dcassandra.internode_compatibility_mode.lost_node_silence_seconds=59 is refused: the " +
                        "silence after which a node that is down counts as lost during the DSE 5.1 upgrade must be " +
                        "at least 60 seconds, the larger of one minute and twice the ring delay; unset it for the " +
                        "default (the largest of 30 minutes, max_hint_window and twice the ring delay)");
        }
    }

    /** A new node whose shadow round shows a DSE 5.1 node is refused, the remedy naming DSE 5.1. */
    @Test
    public void aJoinWithDse51NodesInGossipIsRefused()
    {
        assertThatThrownBy(() -> DseUpgradeGuards.refuseJoinWithDsePeersInGossip(
            Collections.singletonMap(dse51Peer, stateWithNetVersion(MessagingService.VERSION_3014)), null, "bootstrap"))
        .isInstanceOf(ConfigurationException.class)
        .hasMessage("bootstrap is refused while the DSE 5.1 upgrade is in progress (DSE 5.1 nodes in gossip: [" +
                    dse51Peer + "]); finish upgrading (or remove) the DSE 5.1 nodes first");
    }

    /** The messages {@link DseUpgradeGuards} logged at WARN while {@code action} ran. */
    private static List<String> warnings(ThrowingRunnable action) throws Throwable
    {
        Logger logger = (Logger) LoggerFactory.getLogger(DseUpgradeGuards.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        try
        {
            action.run();
        }
        finally
        {
            logger.detachAppender(appender);
        }
        return appender.list.stream()
                            .filter(e -> e.getLevel() == Level.WARN)
                            .map(ILoggingEvent::getFormattedMessage)
                            .collect(Collectors.toList());
    }

    private interface ThrowingRunnable
    {
        void run() throws Throwable;
    }

    @Test
    public void theTextsNameDse51()
    {
        assertThat(DseUpgradeGuards.repairWithDsePeer())
        .isEqualTo("repair is refused during the DSE 5.1 upgrade: repair messages cannot be exchanged with a peer " +
                   "speaking the pre-4.0 dialect (DSE 5.1); run repair after the flag is dropped");
        assertThat(DseUpgradeGuards.statusLine())
        .isEqualTo("DSE 5.1 upgrade in progress (internode_compatibility_mode: DSE_5_1): no peer speaks the pre-4.0 " +
                   "dialect any more and none is on record; once every node runs this software, drop the mode and " +
                   "delete the upgrade marker; requests refused so far: 0");
        // the last line of the repair refusal, for a repair message that reaches the serializer, counted
        assertThatThrownBy(() -> PrepareMessage.serializer.serializedSize(null, MessagingService.VERSION_3014))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage(DseUpgradeGuards.repairWithDsePeer());
        MessagingService.instance().versions.set(dse51Peer, MessagingService.VERSION_3014);
        assertThat(DseUpgradeGuards.statusLine())
        .isEqualTo("DSE 5.1 upgrade in progress (internode_compatibility_mode: DSE_5_1): 1 peer(s) speaking the " +
                   "pre-4.0 dialect or on record as DSE 5.1 peers: [" + dse51Peer + "]; requests refused so far: 1 " +
                   "{repair=1}");
        // the texts of the other modes keep naming DSE 6
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        assertThat(DseUpgradeGuards.repairWithDsePeer()).isEqualTo(DseUpgradeGuards.REPAIR_WITH_DSE_PEER);
        assertThat(DseUpgradeGuards.schemaFrozen()).isEqualTo(DseUpgradeGuards.SCHEMA_FROZEN);
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
        assertThat(DseUpgradeGuards.repairWithDsePeer()).isEqualTo(DseUpgradeGuards.REPAIR_WITH_DSE_PEER);
    }

    /**
     * In DSE 5.1 compatibility mode the periodic tasks are the status line and the new check of the DSE access
     * controls, each on its executor, as in {@code DSE_6}.
     */
    @Test
    public void theAccessControlsAreCheckedAgainInDse51Mode()
    {
        ScheduledExecutorService status = Mockito.mock(ScheduledExecutorService.class);
        ScheduledExecutorService recheck = Mockito.mock(ScheduledExecutorService.class);
        DseUpgradeGuards.schedulePeriodicTasks(status, recheck);
        long interval = DseUpgradeGuards.STATUS_LOG_INTERVAL_MINUTES;
        Mockito.verify(status).scheduleWithFixedDelay(Mockito.any(Runnable.class), Mockito.eq(interval),
                                                      Mockito.eq(interval), Mockito.eq(TimeUnit.MINUTES));
        Mockito.verify(recheck).scheduleWithFixedDelay(Mockito.any(Runnable.class), Mockito.eq(interval),
                                                       Mockito.eq(interval), Mockito.eq(TimeUnit.MINUTES));
        Mockito.verifyNoMoreInteractions(status, recheck);
    }

    /**
     * The status task checks again the workloads the DSE 5.1 peers gossip: a peer gossiping the DSE state of the
     * Search and Graph node of the DSE 5.1.51 fixtures is reported in the status of the check, then forgotten once it
     * gossips the Cassandra workload alone.
     */
    @Test
    public void theStatusTaskChecksTheGossipedWorkloadsAgain() throws Exception
    {
        Dse6NonCoreTraces.resetStatus();
        try
        {
            EndpointState state = stateWithNetVersion(MessagingService.VERSION_3014);
            state.addApplicationState(ApplicationState.X_11_PADDING,
                                      VersionedValue.unsafeMakeVersionedValue(searchGraphDseState(), 1));
            Gossiper.instance.endpointStateMap.put(dse51Peer, state);
            DseUpgradeGuards.logStatus();
            assertThat(Dse6NonCoreTraces.status())
            .isEqualTo("found by the periodic check in gossip: peer 127.0.0.71 (gossip): " +
                       "workloads [Cassandra, Graph, Search], graph true, workload Search");

            state.addApplicationState(ApplicationState.X_11_PADDING,
                                      VersionedValue.unsafeMakeVersionedValue("{\"workloads\":\"Cassandra\"," +
                                                                              "\"workload\":\"Cassandra\"," +
                                                                              "\"graph\":false}", 2));
            DseUpgradeGuards.logStatus();
            assertThat(Dse6NonCoreTraces.status()).isNull();
        }
        finally
        {
            Dse6NonCoreTraces.resetStatus();
        }
    }

    /** The DSE state the Search and Graph node of the DSE 5.1.51 fixtures gossips, at ordinal 9. */
    private static String searchGraphDseState() throws IOException
    {
        for (DseFixtures.GossipState s : DseFixtures.dse51().gossipStates(DseFixtures.dse51().searchGraphGossipInfo(),
                                                                          "/127.0.51.21"))
        {
            if (s.name.equals(ApplicationState.X_11_PADDING.name()))
                return s.value;
        }
        throw new AssertionError("no DSE state in the gossip of the Search and Graph node");
    }

    private static EndpointState stateWithNetVersion(int version)
    {
        EndpointState state = new EndpointState(new HeartBeatState(1, 1));
        state.addApplicationState(ApplicationState.NET_VERSION,
                                  new VersionedValue.VersionedValueFactory(null).networkVersion(version));
        return state;
    }
}
