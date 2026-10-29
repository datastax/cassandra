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
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Murmur3Partitioner.LongToken;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.HeartBeatState;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.DseCompatRefusals;
import org.apache.cassandra.net.DsePeers;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.repair.messages.PrepareMessage;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.paxos.Paxos;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.config.CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE_ALLOW_DDL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * The operational guards of the DSE 6 upgrade: every guarded operation is refused with its text and counted while
 * the mode is on, and nothing changes with {@code NONE}.
 */
public class Dse6UpgradeGuardsTest
{
    private static final String REFUSED = "is refused while the DSE 6 upgrade is in progress";
    private static final String DROP_MODE_FIRST = "finish the upgrade of every node and drop the mode first";
    private static final String NONE_REMEDY = "finish upgrading (or remove) the DSE 6 nodes first";
    private static InetAddressAndPort dsePeer;
    private InternodeCompatibilityMode modeBefore;
    private Config.PaxosVariant paxosVariantBefore;
    private File metadataBefore;
    private Path metadata;

    @BeforeClass
    public static void init() throws Exception
    {
        DatabaseDescriptor.daemonInitialization();
        dsePeer = InetAddressAndPort.getByName("127.0.0.47:7000");
    }

    @Before
    public void setUp() throws IOException
    {
        modeBefore = DatabaseDescriptor.getInternodeCompatibilityMode();
        paxosVariantBefore = Paxos.getPaxosVariant();
        // a metadata directory of each test's own, so that no record of DSE 6 peers is left where other tests start a node
        metadataBefore = DatabaseDescriptor.getMetadataDirectory();
        metadata = Files.createTempDirectory("dse-upgrade-guards");
        DatabaseDescriptor.setMetadataDirectory(new File(metadata));
        DseCompatRefusals.reset();
        // a DSE 6 node marked down is lost at once, unless a test says otherwise
        DseUpgradeGuards.setLostPeerSilenceMillis(0);
    }

    @After
    public void tearDown() throws IOException
    {
        dse6();
        MessagingService.instance().versions.reset(dsePeer);
        DatabaseDescriptor.setInternodeCompatibilityMode(modeBefore);
        Paxos.setPaxosVariant(paxosVariantBefore);
        DseCompatRefusals.reset();
        DsePeersRecord.clear();
        DatabaseDescriptor.setMetadataDirectory(metadataBefore);
        new File(metadata).deleteRecursive();
        DseUpgradeGuards.setLostPeerSilenceMillis(-1);
    }

    private static void dse6()
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
    }

    private static void none()
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
    }

    @Test
    public void upgradeIsInProgressWithTheModeOrADsePeer()
    {
        none();
        assertThat(DseUpgradeGuards.upgradeInProgress()).isFalse();
        assertThat(DseUpgradeGuards.pre40Peers()).isEmpty();
        // versions of this software do not count
        MessagingService.instance().versions.set(dsePeer, MessagingService.VERSION_DS_20);
        assertThat(DseUpgradeGuards.upgradeInProgress()).isFalse();
        // without the mode, a DSE node gossiped by an upgraded peer (DSE's own NET_VERSION): the upgrade is in progress
        MessagingService.instance().versions.set(dsePeer, 1536);
        assertThat(DseUpgradeGuards.pre40Peers()).containsExactly(dsePeer);
        assertThat(DseUpgradeGuards.upgradeInProgress()).isTrue();
        assertThatThrownBy(() -> DseUpgradeGuards.refuseWhileUpgrading("decommission"))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("decommission is refused while the DSE 6 upgrade is in progress (internode_compatibility_mode: NONE, but DSE 6 peers " +
                    "are in the cluster: [" + dsePeer + "]); " + NONE_REMEDY);
        assertThatThrownBy(() -> DseUpgradeGuards.refuseAtStartupWhileUpgrading("bootstrapping a new node"))
        .isInstanceOf(ConfigurationException.class);
        // the schema freeze is a property of the mode only
        assertThat(DseUpgradeGuards.schemaChangesFrozen()).isFalse();
        MessagingService.instance().versions.set(dsePeer, MessagingService.VERSION_DS_20);
        assertThat(DseUpgradeGuards.upgradeInProgress()).isFalse();
        MessagingService.instance().versions.set(dsePeer, MessagingService.VERSION_3014);

        dse6();
        assertThat(DseUpgradeGuards.upgradeInProgress()).isTrue();
        assertThat(DseUpgradeGuards.pre40Peers()).containsExactly(dsePeer);
        MessagingService.instance().versions.set(dsePeer, MessagingService.VERSION_40);
        assertThat(DseUpgradeGuards.pre40Peers()).isEmpty();
        assertThat(DseUpgradeGuards.upgradeInProgress()).isTrue();
    }

    @Test
    public void refusalTextsAndCounters()
    {
        dse6();
        assertThatThrownBy(() -> DseUpgradeGuards.refuseWhileUpgrading("decommission"))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("decommission is refused while the DSE 6 upgrade is in progress (internode_compatibility_mode: DSE_6); " + DROP_MODE_FIRST);
        assertThatThrownBy(() -> DseUpgradeGuards.refuseAtStartupWhileUpgrading("bootstrapping a new node"))
        .isInstanceOf(ConfigurationException.class)
        .hasMessage("bootstrapping a new node is refused while the DSE 6 upgrade is in progress (internode_compatibility_mode: DSE_6); " + DROP_MODE_FIRST);
        assertThat(DseCompatRefusals.count("decommission")).isEqualTo(1);
        // a refusal at start is not counted: the node exits
        assertThat(DseCompatRefusals.count("bootstrapping a new node")).isZero();

        // the DSE peers known are named
        MessagingService.instance().versions.set(dsePeer, MessagingService.VERSION_3014);
        assertThatThrownBy(() -> DseUpgradeGuards.refuseWhileUpgrading("repair"))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("repair is refused while the DSE 6 upgrade is in progress (internode_compatibility_mode: DSE_6, DSE 6 peers: [" +
                    dsePeer + "]); " + DROP_MODE_FIRST);
        assertThat(DseCompatRefusals.count("repair")).isEqualTo(1);
        assertThat(DseCompatRefusals.total()).isEqualTo(2);
    }

    @Test
    public void guardsAreInertInNoneMode()
    {
        none();
        DseUpgradeGuards.refuseWhileUpgrading("decommission");
        DseUpgradeGuards.refuseAtStartupWhileUpgrading("bootstrapping a new node");
        assertThat(DseUpgradeGuards.schemaChangesFrozen()).isFalse();
        assertThat(DseCompatRefusals.total()).isZero();
        assertThat(DseUpgradeGuards.statusLogStarted()).isFalse();
        DseUpgradeGuards.startStatusLog();
        assertThat(DseUpgradeGuards.statusLogStarted()).isFalse();
    }

    @Test
    public void storageServiceOperationsAreRefusedInDse6Mode()
    {
        dse6();
        StorageService ss = StorageService.instance;
        assertRefused("decommission", () -> ss.decommission(false));
        assertRefused("move", () -> ss.move("0"));
        assertRefused("removenode", () -> ss.removeNode(UUID.randomUUID().toString()));
        assertRefused("removenode force", ss::forceRemoveCompletion);
        assertRefused("assassinate", () -> Gossiper.instance.assassinateEndpoint("127.0.0.47:7000"));
        assertRefused("rebuild", () -> ss.rebuild(null));
        assertRefused("rebuild", () -> ss.rebuild(null, null, null, null));
        assertRefused("rebuild", () -> ss.rebuild(null, null, null, null, false));
        assertRefused("bootstrap resume", ss::resumeBootstrap);
        // a node that has not completed a bootstrap (started without gossip, for example)
        assertRefused("join", ss::joinRing);
        // a checked exception for the sstable rewrites: nodetool upgradesstables swallows a RuntimeException (CASSANDRA-18635)
        for (Operation rewrite : new Operation[]{ () -> ss.upgradeSSTables("system", false, "local"),
                                                  () -> ss.upgradeSSTables("system", false, 0, 0, "local") })
        {
            assertThatThrownBy(rewrite::run)
            .isInstanceOf(IOException.class)
            .hasMessageStartingWith("upgradesstables " + REFUSED)
            .hasCauseInstanceOf(UnsupportedOperationException.class);
        }
        assertThatThrownBy(() -> ss.recompressSSTables("system", 0, "local"))
        .isInstanceOf(IOException.class)
        .hasMessageStartingWith("recompress_sstables " + REFUSED);
        assertRefused("refresh", () -> ss.loadNewSSTables("system", "local"));
        assertRefused("repair", () -> ss.repairAsync("system", Collections.emptyMap()));
        assertRefused("import", () -> Keyspace.open("system").getColumnFamilyStore("local")
                                              .importNewSSTables(Collections.singleton("/nonexistent/dse6"), false, false, false, false, false, false));
        assertThat(DseCompatRefusals.count("rebuild")).isEqualTo(3);
        assertThat(DseCompatRefusals.count("upgradesstables")).isEqualTo(2);
        assertThat(DseCompatRefusals.count("recompress_sstables")).isEqualTo(1);
        assertThat(DseCompatRefusals.count("join")).isEqualTo(1);
        assertThat(DseCompatRefusals.total()).isEqualTo(16);
    }

    @Test
    public void storageServiceOperationsAreNotGuardedInNoneMode()
    {
        none();
        StorageService ss = StorageService.instance;
        // these fail for their usual reasons on a node that never joined a ring, or do nothing; never with the guard's text
        assertNotGuarded(() -> ss.decommission(false));
        assertNotGuarded(() -> ss.move("0"));
        assertNotGuarded(() -> ss.removeNode(UUID.randomUUID().toString()));
        assertNotGuarded(() -> ss.rebuild(null, null, "1", null));
        assertNotGuarded(ss::resumeBootstrap);
        assertNotGuarded(ss::forceRemoveCompletion);
        assertNotGuarded(() -> ss.loadNewSSTables("system", "local"));
        assertNotGuarded(() -> ss.upgradeSSTables("system", false, "local"));
        assertNotGuarded(() -> ss.recompressSSTables("system", 0, "local"));
        assertNotGuarded(() -> ss.repairAsync("system", Collections.emptyMap()));
        assertNotGuarded(() -> Keyspace.open("system").getColumnFamilyStore("local")
                                       .importNewSSTables(Collections.singleton("/nonexistent/dse6"), false, false, false, false, false, false));
        assertThat(DseCompatRefusals.total()).isZero();
    }

    @Test
    public void paxosVariantCannotChangeDuringTheUpgrade()
    {
        dse6();
        Paxos.setPaxosVariant(Config.PaxosVariant.v1);
        // DSE_6 accepts exactly v1 at startup, and so at run time: setting it again is not a change of the variant
        StorageProxy.instance.setPaxosVariant("v1");
        assertThat(Paxos.getPaxosVariant()).isEqualTo(Config.PaxosVariant.v1);
        int refused = 0;
        for (Config.PaxosVariant variant : Config.PaxosVariant.values())
        {
            if (variant == Config.PaxosVariant.v1)
                continue;
            assertRefused("changing paxos_variant to " + variant, () -> StorageProxy.instance.setPaxosVariant(variant.name()));
            assertThat(Paxos.getPaxosVariant()).isEqualTo(Config.PaxosVariant.v1);
            assertThat(DseCompatRefusals.count("changing paxos_variant to " + variant)).isEqualTo(1);
            refused++;
        }
        // v1_without_linearizable_reads_or_rejected_writes and the three v2 variants
        assertThat(DseCompatRefusals.total()).isEqualTo(refused).isEqualTo(4);

        // without the mode and without DSE peers, every variant is accepted
        none();
        for (Config.PaxosVariant variant : Config.PaxosVariant.values())
        {
            StorageProxy.instance.setPaxosVariant(variant.name());
            assertThat(Paxos.getPaxosVariant()).isEqualTo(variant);
        }
        assertThat(DseCompatRefusals.total()).isEqualTo(4);
    }

    @Test
    public void paxosVariantChangeWithoutTheModeButWithADsePeerRefusesOnlyV2()
    {
        none();
        Paxos.setPaxosVariant(Config.PaxosVariant.v1);
        MessagingService.instance().versions.set(dsePeer, 1536);
        assertThat(DseUpgradeGuards.upgradeInProgress()).isTrue();
        // as before the DSE_6 mode required exactly v1: both v1 variants are accepted
        StorageProxy.instance.setPaxosVariant("v1_without_linearizable_reads_or_rejected_writes");
        assertThat(Paxos.getPaxosVariant()).isEqualTo(Config.PaxosVariant.v1_without_linearizable_reads_or_rejected_writes);
        StorageProxy.instance.setPaxosVariant("v1");
        assertThat(Paxos.getPaxosVariant()).isEqualTo(Config.PaxosVariant.v1);
        for (Config.PaxosVariant variant : new Config.PaxosVariant[]{ Config.PaxosVariant.v2,
                                                                      Config.PaxosVariant.v2_without_linearizable_reads,
                                                                      Config.PaxosVariant.v2_without_linearizable_reads_or_rejected_writes })
        {
            assertThatThrownBy(() -> StorageProxy.instance.setPaxosVariant(variant.name()))
            .isInstanceOf(UnsupportedOperationException.class)
            .hasMessageStartingWith("changing paxos_variant to " + variant + " " + REFUSED)
            .hasMessageEndingWith(NONE_REMEDY);
            assertThat(Paxos.getPaxosVariant()).isEqualTo(Config.PaxosVariant.v1);
            assertThat(DseCompatRefusals.count("changing paxos_variant to " + variant)).isEqualTo(1);
        }
        assertThat(DseCompatRefusals.total()).isEqualTo(3);
    }

    @Test
    public void schemaChangesAreFrozenInDse6ModeUnlessAllowed()
    {
        dse6();
        assertThat(DseUpgradeGuards.schemaChangesFrozen()).isTrue();
        assertThatThrownBy(DseUpgradeGuards::refuseSchemaChange)
        .isInstanceOf(InvalidRequestException.class)
        .hasMessage(DseUpgradeGuards.SCHEMA_FROZEN + " (or start the node with -Dcassandra.internode_compatibility_mode.allow_ddl=true)");
        assertThat(DseCompatRefusals.count(DseUpgradeGuards.SCHEMA_CHANGE)).isEqualTo(1);

        try (WithProperties properties = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE_ALLOW_DDL, true))
        {
            assertThat(DseUpgradeGuards.schemaChangesFrozen()).isFalse();
        }

        none();
        assertThat(DseUpgradeGuards.schemaChangesFrozen()).isFalse();
        assertThat(DseCompatRefusals.count(DseUpgradeGuards.SCHEMA_CHANGE)).isEqualTo(1);
    }

    @Test
    public void repairMessagesRefuseThePre40Dialect() throws IOException
    {
        dse6();
        List<Range<Token>> ranges = new ArrayList<>();
        ranges.add(new Range<>(new LongToken(1), new LongToken(2)));
        PrepareMessage message = new PrepareMessage(TimeUUID.Generator.nextTimeUUID(), Collections.singletonList(TableId.generate()),
                                                    ranges, false, 0, false, PreviewKind.NONE);
        for (int version : new int[]{ MessagingService.VERSION_30, MessagingService.VERSION_3014 })
        {
            assertThatThrownBy(() -> PrepareMessage.serializer.serialize(message, new DataOutputBuffer(), version))
            .isInstanceOf(UnsupportedOperationException.class)
            .hasMessage(DseUpgradeGuards.REPAIR_WITH_DSE_PEER);
            assertThatThrownBy(() -> PrepareMessage.serializer.deserialize(new DataInputBuffer(ByteBuffer.allocate(64), false), version))
            .isInstanceOf(UnsupportedOperationException.class)
            .hasMessage(DseUpgradeGuards.REPAIR_WITH_DSE_PEER);
            assertThatThrownBy(() -> PrepareMessage.serializer.serializedSize(message, version))
            .isInstanceOf(UnsupportedOperationException.class)
            .hasMessage(DseUpgradeGuards.REPAIR_WITH_DSE_PEER);
        }
        // a message fails at the first method it reaches (the size of an outgoing message, before it is written), counted
        // there: each call here stands for one message
        assertThat(DseCompatRefusals.count("repair")).isEqualTo(6);

        // without the mode the refusal is not this one (a pre-4.0 version is never negotiated there)
        none();
        assertThatThrownBy(() -> PrepareMessage.serializer.serialize(message, new DataOutputBuffer(), MessagingService.VERSION_3014))
        .hasMessageNotContaining(DseUpgradeGuards.REPAIR_WITH_DSE_PEER);
        assertThat(DseCompatRefusals.count("repair")).isEqualTo(6);
    }

    @Test
    public void statusLineNamesThePeersAndTheRefusals()
    {
        dse6();
        assertThat(DseUpgradeGuards.statusLine())
        .isEqualTo("DSE 6 upgrade in progress (internode_compatibility_mode: DSE_6): no peer speaks the pre-4.0 dialect any more and " +
                   "none is on record; once every node runs this software, drop the mode and delete the upgrade marker; requests refused so far: 0");

        MessagingService.instance().versions.set(dsePeer, MessagingService.VERSION_3014);
        DseCompatRefusals.refused("repair");
        DseCompatRefusals.refused("repair");
        assertThat(DseUpgradeGuards.statusLine())
        .isEqualTo("DSE 6 upgrade in progress (internode_compatibility_mode: DSE_6): 1 peer(s) speaking the pre-4.0 dialect or on record " +
                   "as DSE 6 peers: [" + dsePeer + "]; requests refused so far: 2 {repair=2}");
    }

    @Test
    public void aNewNodeIsRefusedWhenItsShadowRoundShowsDseNodes() throws Exception
    {
        none();
        InetAddressAndPort upgraded = InetAddressAndPort.getByName("127.0.0.49:7000");
        Map<InetAddressAndPort, EndpointState> states = new HashMap<>();
        states.put(upgraded, stateWithNetVersion(MessagingService.VERSION_DS_20));
        // a cluster of this software: nothing to refuse
        DseUpgradeGuards.refuseJoinWithDsePeersInGossip(states, null, "bootstrapping a new node");
        // a DSE node that left the ring (decommissioned or removed) stays in gossip for days: it does not count
        EndpointState left = stateWithNetVersion(1536);
        left.addApplicationState(ApplicationState.STATUS_WITH_PORT,
                                 new VersionedValue.VersionedValueFactory(DatabaseDescriptor.getPartitioner())
                                 .left(Collections.singleton(DatabaseDescriptor.getPartitioner().getMinimumToken()), 0L));
        states.put(InetAddressAndPort.getByName("127.0.0.52:7000"), left);
        DseUpgradeGuards.refuseJoinWithDsePeersInGossip(states, null, "bootstrapping a new node");
        // a NET_VERSION that is not a number is not interpreted
        Map<InetAddressAndPort, EndpointState> garbled = new HashMap<>();
        EndpointState notANumber = new EndpointState(new HeartBeatState(1, 1));
        notANumber.addApplicationState(ApplicationState.NET_VERSION, VersionedValue.unsafeMakeVersionedValue("not-a-version", 1));
        garbled.put(InetAddressAndPort.getByName("127.0.0.60:7000"), notANumber);
        DseUpgradeGuards.refuseJoinWithDsePeersInGossip(garbled, null, "bootstrapping a new node");
        // a DSE node as the upgraded peer gossips it, a 4.0 node, and one without NET_VERSION
        states.put(dsePeer, stateWithNetVersion(1024));
        states.put(InetAddressAndPort.getByName("127.0.0.50:7000"), new EndpointState(new HeartBeatState(1, 1)));
        states.put(InetAddressAndPort.getByName("127.0.0.51:7000"), stateWithNetVersion(MessagingService.VERSION_40));
        assertThatThrownBy(() -> DseUpgradeGuards.refuseJoinWithDsePeersInGossip(states, null, "replacing a node"))
        .isInstanceOf(ConfigurationException.class)
        .hasMessage("replacing a node is refused while the DSE 6 upgrade is in progress (DSE 6 nodes in gossip: [" + dsePeer + "]); " +
                    NONE_REMEDY);
        // the text does not depend on the mode (a node in the mode is refused before its shadow round)
        dse6();
        assertThatThrownBy(() -> DseUpgradeGuards.refuseJoinWithDsePeersInGossip(states, null, "replacing a node")).hasMessageEndingWith(NONE_REMEDY);
        // a node gossiping the dialect itself (HCD 1.2 at 11, the stand-in of the test harness) counts too: a new node
        // cannot speak it either, and unlike the listing of DSE peers the refusal does not tell it from a DSE node
        none();
        InetAddressAndPort atTheDialect = InetAddressAndPort.getByName("127.0.0.58:7000");
        Map<InetAddressAndPort, EndpointState> dialectOnly = new HashMap<>();
        dialectOnly.put(atTheDialect, stateWithNetVersion(MessagingService.VERSION_3014));
        assertThatThrownBy(() -> DseUpgradeGuards.refuseJoinWithDsePeersInGossip(dialectOnly, null, "bootstrapping a new node"))
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining("(DSE 6 nodes in gossip: [" + atTheDialect + "])");
    }

    @Test
    public void onlyADseNodeThatLeftTheRingIsNoLongerAPeer() throws Exception
    {
        VersionedValue.VersionedValueFactory factory = new VersionedValue.VersionedValueFactory(DatabaseDescriptor.getPartitioner());
        InetAddressAndPort removed = InetAddressAndPort.getByName("127.0.0.53:7000");
        InetAddressAndPort removing = InetAddressAndPort.getByName("127.0.0.54:7000");
        InetAddressAndPort hibernating = InetAddressAndPort.getByName("127.0.0.55:7000");
        EndpointState removedState = stateWithNetVersion(1536);
        removedState.addApplicationState(ApplicationState.STATUS_WITH_PORT, factory.removedNonlocal(UUID.randomUUID(), 0L));
        EndpointState removingState = stateWithNetVersion(1536);
        removingState.addApplicationState(ApplicationState.STATUS_WITH_PORT, factory.removingNonlocal(UUID.randomUUID()));
        EndpointState hibernatingState = stateWithNetVersion(1536);
        hibernatingState.addApplicationState(ApplicationState.STATUS_WITH_PORT, factory.hibernate(true));
        try
        {
            // a new node: a removed DSE node does not hold it back, one whose removal is streaming or that is being
            // replaced (hibernating) does
            none();
            Map<InetAddressAndPort, EndpointState> states = new HashMap<>();
            states.put(removed, removedState);
            DseUpgradeGuards.refuseJoinWithDsePeersInGossip(states, null, "bootstrapping a new node");
            states.put(removing, removingState);
            states.put(hibernating, hibernatingState);
            assertThatThrownBy(() -> DseUpgradeGuards.refuseJoinWithDsePeersInGossip(states, null, "bootstrapping a new node"))
            .isInstanceOf(ConfigurationException.class)
            .hasMessageContaining("(DSE 6 nodes in gossip: [" + removing + ", " + hibernating + "])");

            // the DSE peers named in DSE_6 mode: an identified DSE node that was removed, still identified before the
            // gossiper excises it (Gossiper.removeEndpoint forgets it), is not named; the same rule as nodetool compatstatus
            dse6();
            Gossiper.instance.endpointStateMap.put(removed, removedState);
            Gossiper.instance.endpointStateMap.put(hibernating, hibernatingState);
            DsePeers.identify(removed, "gossiped NET_VERSION 1536");
            DsePeers.identify(hibernating, "gossiped NET_VERSION 1536");
            assertThat(DseUpgradeGuards.knownDsePeers()).containsExactly(hibernating);
            assertThat(MessagingService.instance().getDsePeers()).containsOnlyKeys(hibernating.getHostAddressAndPort());
        }
        finally
        {
            Gossiper.instance.endpointStateMap.remove(removed);
            Gossiper.instance.endpointStateMap.remove(hibernating);
            DsePeers.reset();
        }
    }

    @Test
    public void aDseNodeKnownOnlyFromItsGossipStateIsNamed() throws Exception
    {
        // a DSE node whose version the storage service does not record (not a ring member yet, or hibernating), not
        // identified and not on record: named in DSE_6 mode, as nodetool compatstatus lists it
        InetAddressAndPort inGossip = InetAddressAndPort.getByName("127.0.0.56:7000");
        InetAddressAndPort hcd = InetAddressAndPort.getByName("127.0.0.57:7000");
        InetAddressAndPort self = FBUtilities.getBroadcastAddressAndPort();
        EndpointState selfBefore = null;
        try
        {
            Gossiper.instance.endpointStateMap.put(inGossip, stateWithNetVersion(1536));
            // HCD 1.2 at the dialect gossips 11: not a DSE version
            Gossiper.instance.endpointStateMap.put(hcd, stateWithNetVersion(MessagingService.VERSION_3014));
            none();
            assertThat(DseUpgradeGuards.knownDsePeers()).isEmpty();
            // this node's own state is never a peer, whatever it carries
            selfBefore = Gossiper.instance.endpointStateMap.put(self, stateWithNetVersion(1536));
            dse6();
            assertThat(DseUpgradeGuards.pre40Peers()).isEmpty();
            assertThat(DseUpgradeGuards.knownDsePeers()).containsExactly(inGossip);
            assertThat(MessagingService.instance().getDsePeers()).containsOnlyKeys(inGossip.getHostAddressAndPort());
        }
        finally
        {
            Gossiper.instance.endpointStateMap.remove(inGossip);
            Gossiper.instance.endpointStateMap.remove(hcd);
            if (selfBefore == null)
                Gossiper.instance.endpointStateMap.remove(self);
            else
                Gossiper.instance.endpointStateMap.put(self, selfBefore);
        }
    }

    /**
     * A DSE 6 node lost for good (down, as its gossip state says) can be removed in both modes: assassinate and a
     * forced removal of lost nodes only, which stream nothing, whatever DSE 6 nodes are alive; removenode, which
     * streams, once none is alive.
     */
    @Test
    public void aDseNodeLostForGoodCanBeRemovedInBothModes() throws Exception
    {
        for (boolean dse6Mode : new boolean[]{ false, true })
        {
            InetAddressAndPort lost = InetAddressAndPort.getByName("127.0.0.61:7000");
            InetAddressAndPort live = InetAddressAndPort.getByName("127.0.0.62:7000");
            InetAddressAndPort hcd = InetAddressAndPort.getByName("127.0.0.63:7000");
            String mode = dse6Mode ? "DSE_6" : "NONE";
            try
            {
                if (dse6Mode)
                    dse6();
                else
                    none();
                DseCompatRefusals.reset();
                EndpointState lostState = stateWithNetVersion(1536);
                Gossiper.instance.endpointStateMap.put(lost, lostState);
                markDown(lost);
                MessagingService.instance().versions.set(lost, 1536);
                // a node of this software that is down is not a lost DSE 6 node
                Gossiper.instance.endpointStateMap.put(hcd, stateWithNetVersion(MessagingService.VERSION_DS_20));
                markDown(hcd);

                assertThat(DseUpgradeGuards.isLostDsePeer(lost)).as(mode).isTrue();
                assertThat(DseUpgradeGuards.isLostDsePeer(hcd)).as(mode).isFalse();
                assertThat(DseUpgradeGuards.liveDsePeers()).as(mode).isEmpty();
                // no DSE 6 node alive: all three are allowed, and not counted
                DseUpgradeGuards.refuseRemoveNodeWhileUpgrading(lost);
                DseUpgradeGuards.refuseAssassinateWhileUpgrading(lost);
                DseUpgradeGuards.refuseForcedRemovalWhileUpgrading(Collections.singleton(lost));
                assertThat(DseCompatRefusals.total()).as(mode).isZero();
                // any other node is refused as before; so is a forced removal with nothing being removed
                String remedy = dse6Mode ? DROP_MODE_FIRST : NONE_REMEDY;
                assertThatThrownBy(() -> DseUpgradeGuards.refuseRemoveNodeWhileUpgrading(hcd)).as(mode)
                .hasMessageStartingWith("removenode " + REFUSED).hasMessageEndingWith(remedy);
                assertThatThrownBy(() -> DseUpgradeGuards.refuseRemoveNodeWhileUpgrading(null)).as(mode)
                .hasMessageStartingWith("removenode " + REFUSED);
                assertThatThrownBy(() -> DseUpgradeGuards.refuseAssassinateWhileUpgrading(hcd)).as(mode)
                .hasMessageStartingWith("assassinate " + REFUSED);
                assertThatThrownBy(() -> DseUpgradeGuards.refuseForcedRemovalWhileUpgrading(Collections.emptySet())).as(mode)
                .hasMessageStartingWith("removenode force " + REFUSED);
                assertThatThrownBy(() -> DseUpgradeGuards.refuseForcedRemovalWhileUpgrading(Arrays.asList(lost, hcd))).as(mode)
                .hasMessageStartingWith("removenode force " + REFUSED);

                // another DSE 6 node is alive: removenode streams with it, refused; assassinate and the forced removal are not
                Gossiper.instance.endpointStateMap.put(live, stateWithNetVersion(1536));
                MessagingService.instance().versions.set(live, 1536);
                assertThat(DseUpgradeGuards.isLostDsePeer(live)).as(mode).isFalse();
                assertThat(DseUpgradeGuards.liveDsePeers()).as(mode).containsExactly(live);
                assertThatThrownBy(() -> DseUpgradeGuards.refuseRemoveNodeWhileUpgrading(lost)).as(mode)
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("removenode of the DSE 6 node " + lost + " lost for good is refused while DSE 6 nodes not known to be lost are in " +
                            "the cluster ([" + live + "]): it streams data with the other replicas, which DSE 6 nodes cannot do with this " +
                            "software; upgrade those DSE 6 nodes first, or remove the lost node with nodetool assassinate 127.0.0.61:7000 (no " +
                            "streaming: the data of its ranges is then held by fewer replicas until a full repair, which runs once the upgrade " +
                            "is finished)");
                DseUpgradeGuards.refuseAssassinateWhileUpgrading(lost);
                DseUpgradeGuards.refuseForcedRemovalWhileUpgrading(Collections.singleton(lost));
                // the live DSE 6 node itself is not lost
                assertThatThrownBy(() -> DseUpgradeGuards.refuseAssassinateWhileUpgrading(live)).as(mode)
                .hasMessageStartingWith("assassinate " + REFUSED);
                assertThatThrownBy(() -> DseUpgradeGuards.refuseForcedRemovalWhileUpgrading(Arrays.asList(lost, live))).as(mode)
                .hasMessageStartingWith("removenode force " + REFUSED);
                assertThat(DseCompatRefusals.count("removenode")).as(mode).isEqualTo(3);
                assertThat(DseCompatRefusals.count("assassinate")).as(mode).isEqualTo(2);
                assertThat(DseCompatRefusals.count("removenode force")).as(mode).isEqualTo(3);

                // a DSE 6 node that came back is no longer lost
                lostState.markAlive();
                assertThat(DseUpgradeGuards.isLostDsePeer(lost)).as(mode).isFalse();
                assertThatThrownBy(() -> DseUpgradeGuards.refuseAssassinateWhileUpgrading(lost)).as(mode)
                .hasMessageStartingWith("assassinate " + REFUSED);
            }
            finally
            {
                for (InetAddressAndPort peer : new InetAddressAndPort[]{ lost, live, hcd })
                {
                    Gossiper.runInGossipStageBlocking(() -> Gossiper.instance.unsafeAnnulEndpoint(peer));
                    dse6();
                    MessagingService.instance().versions.reset(peer);
                }
                DsePeersRecord.clear();
                DsePeers.reset();
            }
        }
    }

    /** A DSE 6 peer of which this node has no gossip state (only on record) is not taken as lost, and counts as alive. */
    @Test
    public void aDsePeerWithoutAGossipStateIsNotLost() throws Exception
    {
        dse6();
        InetAddressAndPort onRecord = InetAddressAndPort.getByName("127.0.0.64:7000");
        InetAddressAndPort lost = InetAddressAndPort.getByName("127.0.0.65:7000");
        Files.createDirectories(DsePeersRecord.path().getParent());
        Files.write(DsePeersRecord.path(), (onRecord.getHostAddressAndPort() + "\n").getBytes(StandardCharsets.UTF_8));
        DsePeersRecord.loadAtStartup();
        try
        {
            Gossiper.instance.endpointStateMap.put(lost, stateWithNetVersion(1536));
            markDown(lost);
            assertThat(DseUpgradeGuards.isLostDsePeer(onRecord)).isFalse();
            assertThat(DseUpgradeGuards.isLostDsePeer(lost)).isTrue();
            assertThat(DseUpgradeGuards.liveDsePeers()).containsExactly(onRecord);
            assertThatThrownBy(() -> DseUpgradeGuards.refuseRemoveNodeWhileUpgrading(lost))
            .hasMessageStartingWith("removenode of the DSE 6 node " + lost + " lost for good is refused while DSE 6 nodes not known to be lost are " +
                                    "in the cluster ([" + onRecord + "]; known without a gossip state (on record or presumed): [" + onRecord + "])");
            assertThatThrownBy(() -> DseUpgradeGuards.refuseAssassinateWhileUpgrading(onRecord)).hasMessageStartingWith("assassinate " + REFUSED);
        }
        finally
        {
            Gossiper.runInGossipStageBlocking(() -> Gossiper.instance.unsafeAnnulEndpoint(lost));
        }
    }

    /**
     * A DSE 6 node that is down is lost only once nothing has been heard of it for the silence of the guard: a node
     * that restarts (a new generation, its echo pending) or whose heartbeat still advances, relayed by another node (as
     * a node without the mode sees a live DSE 6 node it never completes an echo with), is not lost. A peer that is
     * only on the record, or presumed, is not lost either, even when it is down: nothing says it runs DSE.
     */
    @Test
    public void aDseNodeHeardOfRecentlyIsNotLost() throws Exception
    {
        // long enough for the gossip handling of a state to complete well within it
        long silenceMillis = 3000;
        for (boolean dse6Mode : new boolean[]{ false, true })
        {
            String mode = dse6Mode ? "DSE_6" : "NONE";
            InetAddressAndPort lost = InetAddressAndPort.getByName("127.0.0.69:7000");
            try
            {
                if (dse6Mode)
                    dse6();
                else
                    none();
                DseUpgradeGuards.setLostPeerSilenceMillis(silenceMillis);
                Gossiper.instance.endpointStateMap.put(lost, stateWithNetVersion(1536, 1, 1));
                markDown(lost);
                MessagingService.instance().versions.set(lost, 1536);
                assertThat(DseUpgradeGuards.isLostDsePeer(lost)).as(mode + ": just marked down").isFalse();
                Thread.sleep(silenceMillis + 500);
                assertThat(DseUpgradeGuards.isLostDsePeer(lost)).as(mode + ": silent").isTrue();

                // a newer heartbeat, as another node relays it
                apply(lost, stateWithNetVersion(1536, 1, 1_000_000_000));
                assertThat(DseUpgradeGuards.isLostDsePeer(lost)).as(mode + ": heartbeat relayed").isFalse();
                assertThatThrownBy(() -> DseUpgradeGuards.refuseAssassinateWhileUpgrading(lost)).as(mode).hasMessageStartingWith("assassinate " + REFUSED);
                Thread.sleep(silenceMillis + 500);
                assertThat(DseUpgradeGuards.isLostDsePeer(lost)).as(mode + ": silent again").isTrue();

                // a restart: a new generation, whose echo is not answered yet
                apply(lost, stateWithNetVersion(1536, 2, 1));
                assertThat(Gossiper.instance.isAlive(lost)).as(mode).isFalse();
                assertThat(DseUpgradeGuards.isLostDsePeer(lost)).as(mode + ": restarting").isFalse();
                assertThatThrownBy(() -> DseUpgradeGuards.refuseRemoveNodeWhileUpgrading(lost)).as(mode).hasMessageStartingWith("removenode " + REFUSED);
            }
            finally
            {
                Gossiper.runInGossipStageBlocking(() -> Gossiper.instance.unsafeAnnulEndpoint(lost));
                dse6();
                MessagingService.instance().versions.reset(lost);
                DsePeers.reset();
            }
        }

        // a peer of this software on record and down is not a lost DSE 6 node
        dse6();
        InetAddressAndPort onRecord = InetAddressAndPort.getByName("127.0.0.70:7000");
        Files.createDirectories(DsePeersRecord.path().getParent());
        Files.write(DsePeersRecord.path(), (onRecord.getHostAddressAndPort() + "\n").getBytes(StandardCharsets.UTF_8));
        DsePeersRecord.loadAtStartup();
        try
        {
            Gossiper.instance.endpointStateMap.put(onRecord, stateWithNetVersion(MessagingService.VERSION_DS_20));
            markDown(onRecord);
            assertThat(DseUpgradeGuards.knownDsePeers()).contains(onRecord);
            assertThat(DseUpgradeGuards.isLostDsePeer(onRecord)).isFalse();
        }
        finally
        {
            Gossiper.runInGossipStageBlocking(() -> Gossiper.instance.unsafeAnnulEndpoint(onRecord));
        }
    }

    /** Receives {@code state} of {@code peer} as the gossip verb handlers do: reported to the failure detector, then applied. */
    private static void apply(InetAddressAndPort peer, EndpointState state)
    {
        Map<InetAddressAndPort, EndpointState> states = Collections.singletonMap(peer, state);
        Gossiper.runInGossipStageBlocking(() -> {
            Gossiper.instance.notifyFailureDetector(states);
            Gossiper.instance.applyStateLocally(states);
        });
    }

    /** Marks {@code peer} down as the failure detector does when it convicts it: unreachable. */
    private static void markDown(InetAddressAndPort peer)
    {
        Gossiper.runInGossipStageBlocking(() -> Gossiper.instance.markDead(peer, Gossiper.instance.getEndpointStateForEndpoint(peer)));
    }

    /**
     * A replacement: in DSE_6 mode it is refused with the ways to replace or remove a DSE 6 node lost for good; without
     * the mode, its shadow round does not count the node it replaces, only the other DSE 6 nodes.
     */
    @Test
    public void aDseNodeLostForGoodCanBeReplacedByANodeWithoutTheMode() throws Exception
    {
        dse6();
        assertThatThrownBy(DseUpgradeGuards::refuseReplacementWhileUpgrading)
        .isInstanceOf(ConfigurationException.class)
        .hasMessage("replacing a node is refused while the DSE 6 upgrade is in progress (internode_compatibility_mode: DSE_6); a replacement " +
                    "cannot start in internode_compatibility_mode DSE_6; remove a DSE 6 node lost for good instead (nodetool removenode once " +
                    "every other DSE 6 node is lost too, nodetool assassinate otherwise) and add a node once the upgrade is finished; it can be replaced " +
                    "only once every node runs internode_compatibility_mode NONE and no other DSE 6 node is in gossip, as can a node of this " +
                    "software lost during the upgrade");
        // not counted: the node exits
        assertThat(DseCompatRefusals.total()).isZero();

        none();
        DseUpgradeGuards.refuseReplacementWhileUpgrading();
        InetAddressAndPort replaced = InetAddressAndPort.getByName("127.0.0.66:7000");
        InetAddressAndPort other = InetAddressAndPort.getByName("127.0.0.67:7000");
        Map<InetAddressAndPort, EndpointState> states = new HashMap<>();
        states.put(replaced, stateWithNetVersion(1536));
        states.put(InetAddressAndPort.getByName("127.0.0.68:7000"), stateWithNetVersion(MessagingService.VERSION_DS_20));
        DseUpgradeGuards.refuseJoinWithDsePeersInGossip(states, replaced, "replacing a node");
        // the same states refuse a bootstrap
        assertThatThrownBy(() -> DseUpgradeGuards.refuseJoinWithDsePeersInGossip(states, null, "bootstrapping a new node"))
        .hasMessageContaining("(DSE 6 nodes in gossip: [" + replaced + "])");
        // and another DSE 6 node in gossip refuses the replacement
        states.put(other, stateWithNetVersion(1536));
        assertThatThrownBy(() -> DseUpgradeGuards.refuseJoinWithDsePeersInGossip(states, replaced, "replacing a node"))
        .isInstanceOf(ConfigurationException.class)
        .hasMessage("replacing a node is refused while the DSE 6 upgrade is in progress (DSE 6 nodes in gossip: [" + other + "]); " + NONE_REMEDY);
    }

    private static EndpointState stateWithNetVersion(int version)
    {
        return stateWithNetVersion(version, 1, 1);
    }

    private static EndpointState stateWithNetVersion(int version, int generation, int heartbeat)
    {
        EndpointState state = new EndpointState(new HeartBeatState(generation, heartbeat));
        state.addApplicationState(ApplicationState.NET_VERSION, new VersionedValue.VersionedValueFactory(null).networkVersion(version));
        return state;
    }

    @Test
    public void resetLocalSchemaIsRefusedInDse6Mode()
    {
        dse6();
        assertRefused("resetlocalschema", StorageService.instance::resetLocalSchema);
        assertThat(DseCompatRefusals.count("resetlocalschema")).isEqualTo(1);
    }

    @Test
    public void thePeriodicStatusLineIsLoggedAtWarn()
    {
        dse6();
        String expected = DseUpgradeGuards.statusLine();
        assertThat(logged(DseUpgradeGuards::logStatus, Level.WARN)).containsExactly(expected);
        assertThat(logged(DseUpgradeGuards::logStatus, Level.ERROR)).isEmpty();
    }

    /** Runs {@code action} and returns the messages {@link DseUpgradeGuards} logged at {@code level} meanwhile. */
    private static List<String> logged(Runnable action, Level level)
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
                            .filter(e -> e.getLevel() == level)
                            .map(ILoggingEvent::getFormattedMessage)
                            .collect(Collectors.toList());
    }

    @Test
    public void aDsePeerThatIsDownButOnRecordIsStillNamed() throws Exception
    {
        dse6();
        Files.createDirectories(DsePeersRecord.path().getParent());
        Files.write(DsePeersRecord.path(), "127.0.0.48:7000\n".getBytes(StandardCharsets.UTF_8));
        DsePeersRecord.loadAtStartup();
        InetAddressAndPort down = InetAddressAndPort.getByName("127.0.0.48:7000");
        // not in the messaging version map: the peer never connected during this run
        assertThat(DseUpgradeGuards.pre40Peers()).isEmpty();
        assertThat(DseUpgradeGuards.knownDsePeers()).containsExactly(down);
        assertThat(DseUpgradeGuards.statusLine()).contains("1 peer(s) speaking the pre-4.0 dialect or on record as DSE 6 peers: [" + down + "]");
        assertThatThrownBy(() -> DseUpgradeGuards.refuseWhileUpgrading("move")).hasMessageContaining("DSE 6 peers: [" + down + "]");
    }

    @Test
    public void anEntryOfTheRecordThatIsNotAnAddressIsInTheStatusLine() throws Exception
    {
        dse6();
        Files.createDirectories(DsePeersRecord.path().getParent());
        Files.write(DsePeersRecord.path(), "127.0.0.99:not-a-port\n".getBytes(StandardCharsets.UTF_8));
        DsePeersRecord.loadAtStartup();
        assertThat(DseUpgradeGuards.knownDsePeers()).isEmpty();
        // it keeps a start with NONE refused: the status line does not say that the mode can be dropped
        assertThat(DseUpgradeGuards.statusLine())
        .isEqualTo("DSE 6 upgrade in progress (internode_compatibility_mode: DSE_6): no peer speaks the pre-4.0 dialect any more; the " +
                   "record of DSE 6 peers " + DsePeersRecord.path() + " has entries that are not addresses, which keep a start with " +
                   "internode_compatibility_mode NONE refused until they are removed by hand: [127.0.0.99:not-a-port]; " +
                   "requests refused so far: 0");
        // named next to the DSE peers too
        MessagingService.instance().versions.set(dsePeer, MessagingService.VERSION_3014);
        assertThat(DseUpgradeGuards.statusLine())
        .contains("1 peer(s) speaking the pre-4.0 dialect or on record as DSE 6 peers: [" + dsePeer + "]; the record of DSE 6 peers ")
        .contains("removed by hand: [127.0.0.99:not-a-port]; requests refused so far: 0");
    }

    @Test
    public void aPeerPresumedToBeADseNodeIsNamed() throws Exception
    {
        InetAddressAndPort presumed = InetAddressAndPort.getByName("127.0.0.59:7000");
        try
        {
            dse6();
            DsePeers.presume(presumed, "stored DSE 6 release 4.0.0.6865");
            assertThat(DseUpgradeGuards.knownDsePeers()).contains(presumed);
            assertThatThrownBy(() -> DseUpgradeGuards.refuseWhileUpgrading("move")).hasMessageContaining(presumed.toString());
            // without the mode a presumption is not a known DSE peer
            none();
            assertThat(DseUpgradeGuards.knownDsePeers()).doesNotContain(presumed);
        }
        finally
        {
            DsePeers.reset();
            MessagingService.instance().versions.reset(presumed);
        }
    }

    @Test
    public void aPeerIdentifiedAsDseBeforeItsVersionIsRecordedIsNamed() throws Exception
    {
        InetAddressAndPort identified = InetAddressAndPort.getByName("127.0.0.49:7000");
        try
        {
            none();
            // with NONE nothing is identified and nothing is named
            DsePeers.fromGossipedNetVersion(identified, 1536);
            assertThat(DseUpgradeGuards.knownDsePeers()).isEmpty();

            dse6();
            // a DSE NET_VERSION relayed by another node identifies the peer before any version is recorded for it
            DsePeers.fromGossipedNetVersion(identified, 1536);
            assertThat(DseUpgradeGuards.pre40Peers()).isEmpty();
            assertThat(DsePeersRecord.peers()).isEmpty();
            assertThat(DseUpgradeGuards.knownDsePeers()).containsExactly(identified);
            assertThat(DseUpgradeGuards.statusLine()).contains("1 peer(s) speaking the pre-4.0 dialect or on record as DSE 6 peers: [" + identified + "]");
            assertThatThrownBy(() -> DseUpgradeGuards.refuseWhileUpgrading("move")).hasMessageContaining("DSE 6 peers: [" + identified + "]");

            // gossip that says it is not a DSE node (an upgraded node gossips 12) clears the identification
            DsePeers.fromGossipedNetVersion(identified, 12);
            assertThat(DseUpgradeGuards.knownDsePeers()).isEmpty();
        }
        finally
        {
            DsePeers.reset();
        }
    }

    private interface Operation
    {
        void run() throws Exception;
    }

    private static void assertRefused(String operation, Operation op)
    {
        assertThatThrownBy(op::run)
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageStartingWith(operation + " " + REFUSED)
        .hasMessageEndingWith(DROP_MODE_FIRST);
    }

    private static void assertNotGuarded(Operation op)
    {
        Throwable t = catchThrowable(op::run);
        if (t != null)
            assertThat(t).hasMessageNotContaining(REFUSED);
    }
}
