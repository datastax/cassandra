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

package org.apache.cassandra.net;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.dsecompat.DseFixtures.DseVersion;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.HeartBeatState;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.nodes.IPeerInfo;
import org.apache.cassandra.nodes.Nodes;
import org.apache.cassandra.nodes.PeerInfo;
import org.apache.cassandra.service.DsePeersRecord;
import org.apache.cassandra.service.DseUpgradeMarker;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.CassandraVersion;

import static org.apache.cassandra.config.CassandraRelevantProperties.GOSSIP_DISABLE_THREAD_VALIDATION;
import static org.apache.cassandra.net.MessagingService.VERSION_40;
import static org.apache.cassandra.net.MessagingService.current_version;
import static org.apache.cassandra.net.NoPayload.noPayload;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * DSE 6 compatibility mode: the saved peers stored with a DSE 6 release are recorded at the pre-4.0 dialect, and
 * presumed to be DSE nodes, before the first connection, so that they get the pre-4.0 first handshake message and never
 * the 4.0-style probe; a peer taken for a DSE node by mistake (a stale stored release) goes back to the 4.0-style
 * handshake as soon as its version of 4.0 or later is known, whichever way; a pre-4.0 handshake from a peer gossiping 4.0
 * or later does not lower its version, unless its first message is one only DSE sends. With the mode off nothing is
 * seeded and no connection corrects its version. The bytes of the first messages, and the retries, are asserted over
 * sockets in {@link DseHandshakeTest}.
 */
public class DseDialectVersionsTest
{
    private static final int DSE_DIALECT = DseCompat.DSE_DIALECT;
    private static final String DSE_RELEASE = DseVersion.DSE_6_9_26.releaseVersion;

    private InternodeCompatibilityMode modeBefore;
    private int minClusterVersionBefore;

    @BeforeClass
    public static void startup()
    {
        // the gossiper checks that state changes run on the gossip stage; the property is read when the class initialises
        try (WithProperties properties = new WithProperties().set(GOSSIP_DISABLE_THREAD_VALIDATION, true))
        {
            SchemaLoader.prepareServer();
            Gossiper.instance.getClass();
        }
    }

    @Before
    public void saveState()
    {
        modeBefore = DatabaseDescriptor.getInternodeCompatibilityMode();
        // resetting a version does not recompute the minimum when the map becomes empty: restored after each test
        minClusterVersionBefore = MessagingService.instance().versions.minClusterVersion;
    }

    @After
    public void restoreState()
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(modeBefore);
        DsePeers.reset();
        MessagingService.instance().versions.minClusterVersion = minClusterVersionBefore;
    }

    private static void dse6()
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
    }

    private static void none()
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
    }

    static IPeerInfo peer(InetAddressAndPort address, String releaseVersion)
    {
        PeerInfo peer = new PeerInfo().setPeerAddressAndPort(address);
        return releaseVersion == null ? peer : peer.setReleaseVersion(new CassandraVersion(releaseVersion));
    }

    private static EndpointState stateWithNetVersion(String netVersion)
    {
        Map<ApplicationState, VersionedValue> states = new EnumMap<>(ApplicationState.class);
        states.put(ApplicationState.NET_VERSION, VersionedValue.unsafeMakeVersionedValue(netVersion, 1));
        EndpointState state = new EndpointState(new HeartBeatState(1, 1), states);
        state.markDead(); // onRestart would otherwise first treat the peer as one that went down
        return state;
    }

    @Test
    public void onlyPeersStoredWithADseReleaseAndNotKnownYetAreSeeded() throws Exception
    {
        dse6();
        InetAddressAndPort dse = InetAddressAndPort.getByName("127.0.0.211:7000");
        InetAddressAndPort dseKnown = InetAddressAndPort.getByName("127.0.0.212:7000");
        InetAddressAndPort upgraded = InetAddressAndPort.getByName("127.0.0.213:7000");
        InetAddressAndPort hcd = InetAddressAndPort.getByName("127.0.0.214:7000");
        InetAddressAndPort noRelease = InetAddressAndPort.getByName("127.0.0.215:7000");
        EndpointMessagingVersions versions = new EndpointMessagingVersions();
        // a handshake or gossip already spoke for this one
        versions.set(dseKnown, current_version);

        assertThat(DseDialectVersions.seed(Stream.of(peer(dse, DseVersion.DSE_6_8_65.releaseVersion), peer(dseKnown, DSE_RELEASE), peer(upgraded, "5.0.7.0"),
                                                     peer(hcd, "4.0.11.0"), peer(noRelease, null)), versions))
        .containsExactly(dse);
        assertThat(versions.getRaw(dse)).isEqualTo(DSE_DIALECT);
        assertThat(versions.getRaw(dseKnown)).isEqualTo(current_version);
        assertThat(versions.knows(upgraded)).isFalse();
        assertThat(versions.knows(hcd)).isFalse();
        assertThat(versions.knows(noRelease)).isFalse();
        // presumed, not identified: the gossip acks sent to it carry no endpoint state until its gossip settles it
        assertThat(DsePeers.isPresumed(dse)).isTrue();
        assertThat(DsePeers.isDse(dse)).isFalse();
        assertThat(DsePeers.isPresumed(dseKnown)).isFalse();
    }

    /** The messaging service seeds the saved peers (system.peers) when it starts listening, in DSE_6 mode only. */
    @Test
    public void theSavedDsePeersAreSeededOnlyInDse6Mode() throws Exception
    {
        InetAddressAndPort dse = InetAddressAndPort.getByName("127.0.0.216:7000");
        InetAddressAndPort upgraded = InetAddressAndPort.getByName("127.0.0.217:7000");
        Nodes.peers().update(dse, p -> p.setReleaseVersion(new CassandraVersion(DSE_RELEASE)), true);
        Nodes.peers().update(upgraded, p -> p.setReleaseVersion(new CassandraVersion("5.0.7.0")), true);
        try
        {
            none();
            MessagingService withModeOff = new MessagingService(true);
            withModeOff.seedDseDialectPeers();
            assertThat(withModeOff.versions.knows(dse)).isFalse();
            assertThat(withModeOff.versions.knows(upgraded)).isFalse();
            assertThat(DsePeers.isPresumed(dse)).isFalse();

            dse6();
            MessagingService withDse6 = new MessagingService(true);
            withDse6.seedDseDialectPeers();
            assertThat(withDse6.versions.getRaw(dse)).isEqualTo(DSE_DIALECT);
            assertThat(withDse6.versions.knows(upgraded)).isFalse();
        }
        finally
        {
            Nodes.peers().remove(dse, true, true);
            Nodes.peers().remove(upgraded, true, true);
        }
    }

    /** The seeding runs when the messaging service starts listening, before its inbound sockets listen. */
    @Test
    public void listeningSeedsFirst() throws Exception
    {
        List<Boolean> listeningWhenSeeded = new ArrayList<>();
        MessagingService messaging = new MessagingService(true)
        {
            @Override
            void seedDseDialectPeers()
            {
                listeningWhenSeeded.add(inboundSockets().isListening());
            }
        };
        try
        {
            messaging.listen();
            assertThat(listeningWhenSeeded).containsExactly(false);
        }
        finally
        {
            messaging.inboundSockets().close().get(30, TimeUnit.SECONDS);
        }
    }

    /** Saved peers that cannot be read leave every peer unknown, with a warning: each then gets the 4.0-style probe. */
    @Test
    public void unreadableSavedPeersSeedNothing()
    {
        dse6();
        Logger dialectLogger = (Logger) LoggerFactory.getLogger(DseDialectVersions.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        dialectLogger.addAppender(appender);
        try
        {
            EndpointMessagingVersions versions = new EndpointMessagingVersions();
            DseDialectVersions.seedFromSavedPeers(() -> { throw new IllegalStateException("unreadable"); }, versions);
            // nothing assumed: the minimum is still the current version
            assertThat(versions.minClusterVersion).isEqualTo(current_version);
            assertThat(appender.list).singleElement()
                                     .satisfies(e -> {
                                         assertThat(e.getLevel()).isEqualTo(Level.WARN);
                                         assertThat(e.getFormattedMessage()).startsWith("DSE 6 compatibility mode: could not read the saved peers");
                                         assertThat(e.getThrowableProxy().getMessage()).isEqualTo("unreadable");
                                     });
        }
        finally
        {
            dialectLogger.detachAppender(appender);
        }
    }

    /**
     * At startup, after the wait for gossip to settle, a node in DSE_6 mode that knows no DSE 6 peer is told that the
     * mode can be dropped. Each source of a DSE 6 peer keeps it quiet: a peer at the dialect, a presumed or identified
     * DSE node, a gossiped DSE NET_VERSION, an entry of the record (an address or not); a node of this software and a
     * DSE node that left the ring do not. Nothing is logged without the mode.
     */
    @Test
    public void aNodeThatKnowsNoDsePeerAtStartupIsToldTheModeCanBeDropped() throws Exception
    {
        File metadataBefore = DatabaseDescriptor.getMetadataDirectory();
        Path metadata = Files.createTempDirectory("dse-peers-known");
        DatabaseDescriptor.setMetadataDirectory(new File(metadata));
        Logger peersLogger = (Logger) LoggerFactory.getLogger(DsePeers.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        peersLogger.addAppender(appender);
        EndpointMessagingVersions versions = MessagingService.instance().versions;
        InetAddressAndPort dsePeer = InetAddressAndPort.getByName("127.0.0.230:7000");
        InetAddressAndPort gossipingDse = InetAddressAndPort.getByName("127.0.0.231:7000");
        InetAddressAndPort upgraded = InetAddressAndPort.getByName("127.0.0.232:7000");
        InetAddressAndPort left = InetAddressAndPort.getByName("127.0.0.233:7000");
        try
        {
            DsePeersRecord.clear();
            none();
            DsePeers.warnIfNoneKnownAtStartup();
            assertThat(DsePeers.knownAtStartup()).isEmpty();
            assertThat(appender.list).isEmpty();

            dse6();
            DsePeers.warnIfNoneKnownAtStartup();
            assertThat(appender.list).singleElement().satisfies(e -> {
                assertThat(e.getLevel()).isEqualTo(Level.WARN);
                assertThat(e.getFormattedMessage()).isEqualTo("No DSE peers found, the mode can be dropped (internode_compatibility_mode DSE_6: " +
                                                              "no peer speaks the pre-4.0 dialect or gossips a DSE messaging version, and none is " +
                                                              "known or on record as a DSE 6 node): once every node runs this software, delete " +
                                                              "the DSE 6 upgrade marker " + DseUpgradeMarker.path() +
                                                              " and restart the node with internode_compatibility_mode NONE");
            });

            // a node of this software, and a DSE node identified before it left the ring, are not DSE peers
            Gossiper.instance.initializeNodeUnsafe(upgraded, UUID.randomUUID(), VERSION_40, 1);
            versions.set(upgraded, current_version);
            Gossiper.instance.initializeNodeUnsafe(left, UUID.randomUUID(), 1536, 1);
            Gossiper.instance.endpointStateMap.get(left)
                                              .addApplicationState(ApplicationState.STATUS_WITH_PORT,
                                                                   StorageService.instance.valueFactory.left(Collections.singleton(DatabaseDescriptor.getPartitioner().getRandomToken()),
                                                                                                             Long.MAX_VALUE));
            DsePeers.identify(left, "DSE-flagged first handshake message");
            assertThat(DsePeers.knownAtStartup()).isEmpty();

            // a peer at the dialect, which is also on record
            versions.set(dsePeer, DSE_DIALECT);
            assertThat(DsePeers.knownAtStartup()).containsExactly("127.0.0.230:7000");
            versions.reset(dsePeer);
            DsePeersRecord.awaitPersisted();
            assertThat(DsePeers.knownAtStartup()).isEmpty();

            // presumed, then identified
            DsePeers.presume(dsePeer, "stored release " + DSE_RELEASE);
            assertThat(DsePeers.knownAtStartup()).containsExactly("127.0.0.230:7000");
            DsePeers.identify(dsePeer, "DSE-flagged first handshake message");
            assertThat(DsePeers.knownAtStartup()).containsExactly("127.0.0.230:7000");
            DsePeers.reset();

            // a gossiped DSE NET_VERSION
            Gossiper.instance.initializeNodeUnsafe(gossipingDse, UUID.randomUUID(), 1536, 1);
            assertThat(DsePeers.knownAtStartup()).containsExactly("127.0.0.231:7000");
            Gossiper.instance.unsafeAnnulEndpoint(gossipingDse);

            // the record of an earlier run, whose entries that are not addresses count too
            Files.write(DsePeersRecord.path(), List.of("127.0.0.234:7000", "not-an-address"));
            DsePeersRecord.loadAtStartup();
            assertThat(DsePeers.knownAtStartup()).containsExactly("127.0.0.234:7000", "not-an-address");

            appender.list.clear();
            DsePeers.warnIfNoneKnownAtStartup();
            assertThat(appender.list).noneMatch(e -> e.getLevel() == Level.WARN);
        }
        finally
        {
            peersLogger.detachAppender(appender);
            Gossiper.instance.unsafeAnnulEndpoint(upgraded);
            Gossiper.instance.unsafeAnnulEndpoint(left);
            Gossiper.instance.unsafeAnnulEndpoint(gossipingDse);
            versions.reset(upgraded);
            versions.reset(dsePeer);
            DsePeersRecord.clear();
            DatabaseDescriptor.setMetadataDirectory(metadataBefore);
            new File(metadata).deleteRecursive();
        }
    }

    @Test
    public void nodesOfThisSoftwareAreTheOnesGossipingVersion40OrLaterButNoDseVersion()
    {
        assertThat(DseDialectVersions.gossipedVersion40OrLater(stateWithNetVersion("12"))).isEqualTo(12);
        assertThat(DseDialectVersions.gossipedVersion40OrLater(stateWithNetVersion(String.valueOf(current_version)))).isEqualTo(current_version);
        assertThat(DseDialectVersions.gossipedVersion40OrLater(stateWithNetVersion("11"))).as("HCD 1.2 at the dialect").isEqualTo(-1);
        assertThat(DseDialectVersions.gossipedVersion40OrLater(stateWithNetVersion("1536"))).as("DSE 6.9.7+").isEqualTo(-1);
        assertThat(DseDialectVersions.gossipedVersion40OrLater(stateWithNetVersion("6.9.26"))).isEqualTo(-1);
        assertThat(DseDialectVersions.gossipedVersion40OrLater(new EndpointState(new HeartBeatState(1, 1)))).isEqualTo(-1);
        assertThat(DseDialectVersions.gossipedVersion40OrLater(null)).isEqualTo(-1);
    }

    @Test
    public void aConnectionWithAStaleDialectTakesTheNodesVersion() throws Exception
    {
        InetAddressAndPort peer = InetAddressAndPort.getByName("127.0.0.218:7000");
        EndpointMessagingVersions node = new EndpointMessagingVersions();
        EndpointMessagingVersions connection = new EndpointMessagingVersions();
        connection.set(peer, DSE_DIALECT);

        // the node does not know the peer, or knows it at the dialect: the connection keeps the dialect
        assertThat(DseDialectVersions.connectionVersion(peer, DSE_DIALECT, connection, node)).isEqualTo(DSE_DIALECT);
        node.set(peer, DSE_DIALECT);
        assertThat(DseDialectVersions.connectionVersion(peer, DSE_DIALECT, connection, node)).isEqualTo(DSE_DIALECT);
        node.set(peer, 1536);
        assertThat(DseDialectVersions.connectionVersion(peer, DSE_DIALECT, connection, node)).as("a raw DSE version").isEqualTo(DSE_DIALECT);
        assertThat(connection.getRaw(peer)).isEqualTo(DSE_DIALECT);

        // the node knows it at 4.0 or later now: the connection's copy is corrected
        node.set(peer, VERSION_40);
        assertThat(DseDialectVersions.connectionVersion(peer, DSE_DIALECT, connection, node)).isEqualTo(VERSION_40);
        assertThat(connection.getRaw(peer)).isEqualTo(VERSION_40);

        // a connection at 4.0 or later is left alone while the peer is not identified as a DSE node
        node.set(peer, DSE_DIALECT);
        assertThat(DseDialectVersions.connectionVersion(peer, current_version, connection, node)).isEqualTo(current_version);
        assertThat(connection.getRaw(peer)).isEqualTo(VERSION_40);
    }

    /**
     * Another connection type of a DSE node answered the probe in the dialect: a connection whose copy has 4.0 or later,
     * or nothing, goes straight to the dialect, without probing DSE again; only for a peer identified as a DSE node.
     */
    @Test
    public void theOtherConnectionTypesOfADseNodeAnsweringInTheDialectDoNotProbeIt() throws Exception
    {
        dse6();
        InetAddressAndPort peer = InetAddressAndPort.getByName("127.0.0.225:7000");
        EndpointMessagingVersions node = new EndpointMessagingVersions();
        EndpointMessagingVersions connection = new EndpointMessagingVersions();
        node.set(peer, DSE_DIALECT);
        // HCD 1.2 at the dialect is not identified: each connection type makes its own 4.0-style handshake, as before
        assertThat(DseDialectVersions.connectionVersion(peer, current_version, connection, node)).isEqualTo(current_version);
        assertThat(connection.knows(peer)).isFalse();

        DsePeers.identify(peer, "handshake reply with the DSE messaging version 1536");
        assertThat(DseDialectVersions.connectionVersion(peer, current_version, connection, node)).isEqualTo(DSE_DIALECT);
        assertThat(connection.getRaw(peer)).isEqualTo(DSE_DIALECT);
        // with the mode off, nothing changes
        EndpointMessagingVersions another = new EndpointMessagingVersions();
        none();
        assertThat(DsePeers.isDse(peer)).isFalse();
        assertThat(DseDialectVersions.connectionVersion(peer, current_version, another, node)).isEqualTo(current_version);
    }

    /**
     * The gossiper calls onRestart with the state from before the restart, after storing the new one: in DSE_6 mode the
     * NET_VERSION recorded is the new one. A DSE node upgraded to this software keeps the version its new handshake
     * negotiated (the old 1536 must not lower it, nor the raise that would follow interrupt its connections); a node
     * rolled back to DSE stays at the dialect and identified (the old 12 must neither raise it nor clear it).
     */
    @Test
    public void aRestartRecordsTheNewNetVersionNotTheOldOne() throws Exception
    {
        dse6();
        InetAddressAndPort upgraded = InetAddressAndPort.getByName("127.0.0.226:7000");
        InetAddressAndPort rolledBack = InetAddressAndPort.getByName("127.0.0.227:7000");
        InetAddressAndPort upgradedEarly = InetAddressAndPort.getByName("127.0.0.228:7000");
        EndpointMessagingVersions versions = MessagingService.instance().versions;
        try
        {
            versions.set(upgraded, current_version);
            Gossiper.instance.initializeNodeUnsafe(upgraded, UUID.randomUUID(), VERSION_40, 2);
            StorageService.instance.onRestart(upgraded, stateWithNetVersion("1536"));
            assertThat(versions.getRaw(upgraded)).isEqualTo(current_version);
            assertThat(DsePeers.isDse(upgraded)).isFalse();

            versions.set(rolledBack, DSE_DIALECT);
            DsePeers.identify(rolledBack, "DSE-flagged first handshake message");
            Gossiper.instance.initializeNodeUnsafe(rolledBack, UUID.randomUUID(), 1536, 2);
            StorageService.instance.onRestart(rolledBack, stateWithNetVersion(String.valueOf(VERSION_40)));
            assertThat(versions.getRaw(rolledBack)).isEqualTo(DSE_DIALECT);
            assertThat(DsePeers.isDse(rolledBack)).isTrue();

            // a DSE node upgraded before any new handshake with it: its new 12 raises it out of the dialect and clears its
            // identification (the old 1536 would have left both)
            versions.set(upgradedEarly, DSE_DIALECT);
            DsePeers.identify(upgradedEarly, "handshake reply with the DSE messaging version 1536");
            Gossiper.instance.initializeNodeUnsafe(upgradedEarly, UUID.randomUUID(), VERSION_40, 2);
            StorageService.instance.onRestart(upgradedEarly, stateWithNetVersion("1536"));
            assertThat(versions.getRaw(upgradedEarly)).isEqualTo(VERSION_40);
            assertThat(DsePeers.isDse(upgradedEarly)).isFalse();
        }
        finally
        {
            Gossiper.instance.unsafeAnnulEndpoint(upgraded);
            Gossiper.instance.unsafeAnnulEndpoint(rolledBack);
            Gossiper.instance.unsafeAnnulEndpoint(upgradedEarly);
            versions.reset(upgraded);
            versions.reset(rolledBack);
            versions.reset(upgradedEarly);
        }
    }

    @Test
    public void aConnectionMadeInTheDialectIsSupersededOnlyByANodeVersionOf40OrLater() throws Exception
    {
        InetAddressAndPort peer = InetAddressAndPort.getByName("127.0.0.223:7000");
        EndpointMessagingVersions node = new EndpointMessagingVersions();
        assertThat(DseDialectVersions.supersededDialect(peer, DSE_DIALECT, node)).as("unknown").isFalse();
        node.set(peer, DSE_DIALECT);
        assertThat(DseDialectVersions.supersededDialect(peer, DSE_DIALECT, node)).isFalse();
        node.set(peer, 1536);
        assertThat(DseDialectVersions.supersededDialect(peer, DSE_DIALECT, node)).as("a raw DSE version").isFalse();
        node.set(peer, VERSION_40);
        assertThat(DseDialectVersions.supersededDialect(peer, DSE_DIALECT, node)).isTrue();
        assertThat(DseDialectVersions.supersededDialect(peer, current_version, node)).as("a connection at 4.0 or later").isFalse();
    }

    @Test
    public void aPeerAnsweringTheProbeInTheDialectIsRecordedAtIt() throws Exception
    {
        dse6();
        InetAddressAndPort rolledBack = InetAddressAndPort.getByName("127.0.0.224:7000");
        EndpointMessagingVersions versions = MessagingService.instance().versions;
        try
        {
            versions.set(rolledBack, current_version);
            DseDialectVersions.answeredInTheDialect(rolledBack, DSE_DIALECT);
            assertThat(versions.getRaw(rolledBack)).isEqualTo(DSE_DIALECT);
        }
        finally
        {
            versions.reset(rolledBack);
        }
    }

    @Test
    public void pre40HandshakeDoesNotLowerThePeerGossipingVersion40OrLaterUnlessOnlyDseSendsIt() throws Exception
    {
        dse6();
        InetAddressAndPort upgraded = InetAddressAndPort.getByName("127.0.0.219:7000");
        InetAddressAndPort dse = InetAddressAndPort.getByName("127.0.0.220:7000");
        InetAddressAndPort unknown = InetAddressAndPort.getByName("127.0.0.221:7000");
        EndpointMessagingVersions versions = MessagingService.instance().versions;
        Gossiper.instance.initializeNodeUnsafe(upgraded, UUID.randomUUID(), VERSION_40, 1);
        Gossiper.instance.initializeNodeUnsafe(dse, UUID.randomUUID(), 1536, 1);
        try
        {
            versions.set(upgraded, current_version);
            DseDialectVersions.recordHandshakeVersion(upgraded, DSE_DIALECT, DSE_DIALECT);
            assertThat(versions.getRaw(upgraded)).isEqualTo(current_version);
            versions.reset(upgraded);
            DseDialectVersions.recordHandshakeVersion(upgraded, DSE_DIALECT, DSE_DIALECT);
            assertThat(versions.knows(upgraded)).isFalse();

            // a first message only DSE sends means the node was rolled back to DSE, whatever its stale gossip state says
            for (int onlyDse : new int[]{ DseCompat.DSE_VERSION_BYTE, DseCompat.NET_VERSION_PUBLISHED })
            {
                versions.set(upgraded, current_version);
                DseDialectVersions.recordHandshakeVersion(upgraded, DSE_DIALECT, onlyDse);
                assertThat(versions.getRaw(upgraded)).as("requested " + onlyDse).isEqualTo(DSE_DIALECT);
            }

            // a DSE node, and a node whose gossip state is not known yet, are recorded at the dialect as always
            DseDialectVersions.recordHandshakeVersion(dse, DSE_DIALECT, DSE_DIALECT);
            assertThat(versions.getRaw(dse)).isEqualTo(DSE_DIALECT);
            DseDialectVersions.recordHandshakeVersion(unknown, DSE_DIALECT, DSE_DIALECT);
            assertThat(versions.getRaw(unknown)).isEqualTo(DSE_DIALECT);

            // a 4.0-style handshake is recorded as always, and raises a peer out of the dialect
            DseDialectVersions.recordHandshakeVersion(unknown, current_version, current_version);
            assertThat(versions.getRaw(unknown)).isEqualTo(current_version);
        }
        finally
        {
            Gossiper.instance.unsafeAnnulEndpoint(upgraded);
            Gossiper.instance.unsafeAnnulEndpoint(dse);
            versions.reset(upgraded);
            versions.reset(dse);
            versions.reset(unknown);
        }
    }

    /**
     * A node of this software taken for a DSE node (a stale stored release) is connected to at the dialect, which this
     * software accepts; its version of 4.0 or later, learnt from its gossiped NET_VERSION 12 or from a 4.0-style
     * handshake it makes, closes that connection, and the next one is made with the 4.0-style handshake, at the current
     * version.
     */
    @Test
    public void upgradedPeerTakenForADseNodeIsReconnectedAtTheCurrentVersion() throws Throwable
    {
        dse6();
        // its gossiped NET_VERSION
        reconnectedAtTheCurrentVersion(endpoint -> StorageService.instance.onRestart(endpoint, stateWithNetVersion(String.valueOf(VERSION_40))));
        // a 4.0-style handshake it makes with this node
        reconnectedAtTheCurrentVersion(endpoint -> DseDialectVersions.recordHandshakeVersion(endpoint, current_version, current_version));
    }

    private interface VersionRaise
    {
        void raise(InetAddressAndPort endpoint) throws Exception;
    }

    private static void reconnectedAtTheCurrentVersion(VersionRaise raise) throws Throwable
    {
        InboundSockets inbound = new InboundSockets(new InboundConnectionSettings().withAcceptMessaging(null));
        inbound.open().sync();
        InetAddressAndPort endpoint = inbound.sockets().stream().map(s -> s.settings.bindAddress).findFirst().get();
        MessagingService messaging = MessagingService.instance();
        Semaphore delivered = new Semaphore(0);
        Supplier<? extends IVerbHandler<?>> echoHandler = Verb.ECHO_REQ.unsafeSetHandler(() -> message -> delivered.release());
        try
        {
            // the inbound handshakes of other tests record this node's own address, which the inbound sockets bind to
            messaging.versions.reset(endpoint);
            assertThat(DseDialectVersions.seed(Stream.of(peer(endpoint, DSE_RELEASE)), messaging.versions)).containsExactly(endpoint);
            messaging.send(Message.out(Verb.ECHO_REQ, noPayload), endpoint);
            assertThat(delivered.tryAcquire(30, TimeUnit.SECONDS)).isTrue();
            OutboundConnection connection = messaging.channelManagers.get(endpoint).connectionFor(Message.out(Verb.ECHO_REQ, noPayload));
            assertThat(connection.messagingVersion()).as("this software answers a pre-4.0 first message with the dialect").isEqualTo(DSE_DIALECT);
            assertThat(connection.successfulConnections()).isEqualTo(1);

            raise.raise(endpoint);
            assertThat(DseDialectVersions.isPre40(messaging.versions, endpoint)).isFalse();
            Util.spinAssertEquals(false, connection::isConnected, 10);

            messaging.send(Message.out(Verb.ECHO_REQ, noPayload), endpoint);
            assertThat(delivered.tryAcquire(30, TimeUnit.SECONDS)).isTrue();
            assertThat(connection.messagingVersion()).isEqualTo(current_version);
            assertThat(connection.successfulConnections()).isEqualTo(2);
        }
        finally
        {
            messaging.closeOutboundNow(endpoint);
            inbound.close().get(30, TimeUnit.SECONDS);
            Verb.ECHO_REQ.unsafeSetHandler(echoHandler);
            messaging.versions.reset(endpoint);
            DsePeers.reset();
        }
    }

    /**
     * A connection whose own copy of the versions has the dialect for a peer the node knows at 4.0 or later corrects its
     * copy before connecting in DSE_6 mode, and only then: with the mode off it keeps the version it had. The peer does
     * not listen, so the copy is observed without a handshake overwriting it.
     */
    @Test
    public void aStaleDialectIsCorrectedBeforeConnectingOnlyInDse6Mode() throws Throwable
    {
        for (boolean dse6 : new boolean[]{ true, false })
        {
            // bound but not listening for the whole test: connections to it are refused, and no other process takes the port
            try (Socket notListening = new Socket())
            {
                notListening.bind(new InetSocketAddress(InetAddressAndPort.getByName("127.0.0.1").getAddress(), 0));
                InetAddressAndPort endpoint = InetAddressAndPort.getByAddressOverrideDefaults(notListening.getLocalAddress(), notListening.getLocalPort());
                staleDialectBeforeConnecting(endpoint, dse6);
            }
        }
    }

    private static void staleDialectBeforeConnecting(InetAddressAndPort endpoint, boolean dse6) throws Throwable
    {
        EndpointMessagingVersions versions = MessagingService.instance().versions;
        versions.set(endpoint, DSE_DIALECT);
        OutboundConnectionSettings template = new OutboundConnectionSettings(endpoint).withDefaultReserveLimits()
                                                                                      .withDefaults(ConnectionCategory.MESSAGING);
        ResourceLimits.EndpointAndGlobal reserve = new ResourceLimits.EndpointAndGlobal(new ResourceLimits.Concurrent(template.applicationSendQueueReserveEndpointCapacityInBytes),
                                                                                         template.applicationSendQueueReserveGlobalCapacityInBytes);
        OutboundConnection outbound = new OutboundConnection(ConnectionType.SMALL_MESSAGES, template, reserve);
        try
        {
            if (dse6)
                dse6();
            else
                none();
            versions.set(endpoint, current_version);
            assertThat(outbound.messagingVersion()).as("the connection's copy").isEqualTo(DSE_DIALECT);
            outbound.enqueue(Message.out(Verb.ECHO_REQ, noPayload));
            if (dse6)
            {
                Util.spinAssertEquals(current_version, outbound::messagingVersion, 10);
            }
            else
            {
                // two attempts: the first one has certainly decided its version by the time the second one starts
                Util.spinAssertEquals(true, () -> outbound.connectionAttempts() >= 2, 30);
                assertThat(outbound.messagingVersion()).as("NONE").isEqualTo(DSE_DIALECT);
            }
        }
        finally
        {
            outbound.close(false).get(30, TimeUnit.SECONDS);
            versions.reset(endpoint);
        }
    }

    /**
     * A peer seeded at the dialect is assumed, not seen, at it: it does not enter the record of DSE peers the startup
     * checks read (in memory nor in the persisted file), which a handshake, its gossip or its answer in the dialect to the
     * probe fill as before; a peer already on record stays there.
     */
    @Test
    public void seededPeersStayOutOfTheRecordOfDsePeers() throws Exception
    {
        dse6();
        InetAddressAndPort dse = InetAddressAndPort.getByName("127.0.0.222:7000");
        InetAddressAndPort onRecord = InetAddressAndPort.getByName("127.0.0.229:7000");
        EndpointMessagingVersions versions = MessagingService.instance().versions;
        DsePeersRecord.clear();
        try
        {
            // a peer on the record loaded at startup, whose version this node does not know yet
            DsePeersRecord.versionRecorded(onRecord, DSE_DIALECT);
            assertThat(DseDialectVersions.seed(Stream.of(peer(dse, DSE_RELEASE), peer(onRecord, DSE_RELEASE)), versions)).containsExactly(dse, onRecord);
            assertThat(versions.getRaw(dse)).isEqualTo(DSE_DIALECT);
            DsePeersRecord.awaitPersisted();
            assertThat(DsePeersRecord.peers()).doesNotContain(dse).contains(onRecord);
            assertThat(DsePeersRecord.persistedPeers()).doesNotContain(dse.getHostAddressAndPort()).contains(onRecord.getHostAddressAndPort());

            // what an inbound handshake or gossip does (EndpointMessagingVersions.set) records it
            versions.set(dse, DSE_DIALECT);
            assertThat(DsePeersRecord.peers()).contains(dse);
            versions.reset(dse);
            assertThat(DsePeersRecord.peers()).doesNotContain(dse);
            // and so does its answer in the dialect to the 4.0-style probe
            DseDialectVersions.answeredInTheDialect(dse, DSE_DIALECT);
            assertThat(DsePeersRecord.peers()).contains(dse);
        }
        finally
        {
            versions.reset(dse);
            versions.reset(onRecord);
            DsePeersRecord.clear();
        }
    }

    /** Assuming a version never replaces a known one, lowers the minimum like any version, and records nothing. */
    @Test
    public void assumingAVersion() throws Exception
    {
        dse6();
        InetAddressAndPort known = InetAddressAndPort.getByName("127.0.0.230:7000");
        InetAddressAndPort unknown = InetAddressAndPort.getByName("127.0.0.231:7000");
        EndpointMessagingVersions versions = MessagingService.instance().versions;
        DsePeersRecord.clear();
        try
        {
            versions.set(known, VERSION_40);
            assertThat(versions.assume(known, DSE_DIALECT)).isFalse();
            assertThat(versions.getRaw(known)).isEqualTo(VERSION_40);

            assertThat(versions.assume(unknown, DSE_DIALECT)).isTrue();
            assertThat(versions.getRaw(unknown)).isEqualTo(DSE_DIALECT);
            assertThat(versions.minClusterVersion).isEqualTo(DSE_DIALECT);
            DsePeersRecord.awaitPersisted();
            assertThat(DsePeersRecord.peers()).doesNotContain(unknown);
            assertThat(DsePeersRecord.persistedPeers()).doesNotContain(unknown.getHostAddressAndPort());
            // the record follows this map: setting the same version is a sighting, recorded and persisted
            versions.set(unknown, DSE_DIALECT);
            DsePeersRecord.awaitPersisted();
            assertThat(DsePeersRecord.peers()).contains(unknown);
            assertThat(DsePeersRecord.persistedPeers()).contains(unknown.getHostAddressAndPort());
        }
        finally
        {
            versions.reset(known);
            versions.reset(unknown);
            DsePeersRecord.clear();
        }
    }
}
