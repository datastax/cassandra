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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.dsecompat.Dse51Fixtures;
import org.apache.cassandra.dsecompat.DseFixtures;
import org.apache.cassandra.dsecompat.DseFixtures.GossipState;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.HeartBeatState;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.DsePeersRecord;
import org.apache.cassandra.service.DseUpgradeMarker;
import org.apache.cassandra.service.StorageService;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The DSE 5.1 peers {@link DsePeers} identifies in the DSE 5.1 compatibility mode, by the rule of that mode: a gossiped
 * {@code NET_VERSION} of 10 or 11, a handshake requesting or answering 10 or 11; the DSE 6 rule is not used then, nor
 * the DSE 5.1 one in {@code DSE_6}. A DSE 5.1 peer of another release than DSE 5.1.51 is logged at WARN once per peer.
 * The states are those DSE 5.1.51 nodes gossiped ({@link Dse51Fixtures#gossipInfo}), changed where a test needs
 * another release.
 */
public class Dse51PeersTest
{
    private static final Dse51Fixtures DSE51 = DseFixtures.dse51();
    private static InetAddressAndPort node1;
    private static InetAddressAndPort node2;

    private InternodeCompatibilityMode modeBefore;
    private File metadataBefore;
    private Path metadata;
    private final Logger logger = (Logger) LoggerFactory.getLogger(DsePeers.class);
    private final ListAppender<ILoggingEvent> appender = new ListAppender<>();

    @BeforeClass
    public static void init() throws Exception
    {
        DatabaseDescriptor.daemonInitialization();
        node1 = InetAddressAndPort.getByName("127.0.51.11");
        node2 = InetAddressAndPort.getByName("127.0.51.12");
    }

    @Before
    public void dse51Mode() throws IOException
    {
        modeBefore = DatabaseDescriptor.getInternodeCompatibilityMode();
        // a metadata directory of each test's own, where the record of the DSE 5.1 peers it sees is written
        metadataBefore = DatabaseDescriptor.getMetadataDirectory();
        metadata = Files.createTempDirectory("dse51-peers");
        DatabaseDescriptor.setMetadataDirectory(new File(metadata));
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_5_1);
        DsePeers.reset();
        appender.start();
        logger.addAppender(appender);
    }

    @After
    public void restore() throws Exception
    {
        logger.detachAppender(appender);
        for (InetAddressAndPort node : List.of(node1, node2))
        {
            Gossiper.instance.endpointStateMap.remove(node);
            MessagingService.instance().versions.reset(node);
        }
        DsePeers.reset();
        DsePeersRecord.clear();
        DatabaseDescriptor.setInternodeCompatibilityMode(modeBefore);
        DatabaseDescriptor.setMetadataDirectory(metadataBefore);
        new File(metadata).deleteRecursive();
    }

    @Test
    public void aDse51NodeIsIdentifiedByItsGossipedNetVersion() throws Exception
    {
        EndpointState state = capturedState(node1);
        assertThat(state.getApplicationState(ApplicationState.NET_VERSION).value).isEqualTo("11");
        assertThat(DsePeers.gossipsDseVersion(state)).isTrue();
        DsePeers.fromGossipedNetVersion(node1, state.getApplicationState(ApplicationState.NET_VERSION).value);
        assertThat(DsePeers.isDse(node1)).isTrue();
        assertThat(DsePeers.snapshot()).containsEntry(node1, "gossiped NET_VERSION 11");
        DsePeers.fromGossipedNetVersion(node1, "10");
        assertThat(DsePeers.snapshot()).containsEntry(node1, "gossiped NET_VERSION 11");

        // a NET_VERSION of 4.0 or later clears the record: the node was upgraded
        DsePeers.fromGossipedNetVersion(node1, "12");
        assertThat(DsePeers.isDse(node1)).isFalse();
        assertThat(messages(Level.INFO))
        .containsExactly("Identified " + node1 + " as a DSE 5.1 node (gossiped NET_VERSION 11)",
                         node1 + " is not a DSE 5.1 node (gossiped NET_VERSION 12; it was identified by gossiped " +
                         "NET_VERSION 11)");
        // DSE 6's own versions identify no DSE 5.1 node
        DsePeers.fromGossipedNetVersion(node2, 1024);
        assertThat(DsePeers.isDse(node2)).isFalse();
    }

    @Test
    public void aDse51NodeIsIdentifiedByItsHandshake()
    {
        DsePeers.fromPre40Initiate(node1, MessagingService.VERSION_3014);
        assertThat(DsePeers.snapshot()).containsEntry(node1, "pre-4.0 first handshake message requesting version 11");
        DsePeers.fromPre40Reply(node2, MessagingService.VERSION_3014, true);
        assertThat(DsePeers.snapshot()).containsEntry(node2, "pre-4.0 handshake reply with the messaging version 11");
        DsePeers.reset();
        DsePeers.fromPre40Reply(node2, MessagingService.VERSION_30, false);
        assertThat(DsePeers.snapshot()).containsEntry(node2, "handshake reply with the messaging version 10");

        // what only DSE 6 sends identifies nothing in this mode
        DsePeers.reset();
        DsePeers.fromPre40Initiate(node1, DseCompat.DSE_VERSION_BYTE);
        DsePeers.fromPre40Initiate(node1, MessagingService.VERSION_40);
        DsePeers.fromPre40Reply(node2, DseCompat.DSE_681_VERSION, false);
        assertThat(DsePeers.snapshot()).isEmpty();
    }

    /** The DSE 6 rule is not used in DSE_5_1 (no presumption either), nor the DSE 5.1 one in DSE_6, nor any in NONE. */
    @Test
    public void eachModeHasItsOwnRule() throws Exception
    {
        DsePeers.presume(node1, "stored release 4.0.0.6926");
        assertThat(DsePeers.presumedSnapshot()).isEmpty();
        assertThat(DsePeers.isPresumed(node1)).isFalse();

        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        DsePeers.fromGossipedNetVersion(node1, 11);
        DsePeers.fromPre40Initiate(node1, MessagingService.VERSION_3014);
        DsePeers.fromPre40Reply(node1, MessagingService.VERSION_3014, true);
        assertThat(DsePeers.snapshot()).isEmpty();
        DsePeers.fromGossipedNetVersion(node2, DseCompat.DSE_681_VERSION);
        assertThat(DsePeers.snapshot()).containsOnlyKeys(node2);
        assertThat(messages(Level.INFO))
        .containsExactly("Identified " + node2 + " as a DSE 6 node (gossiped NET_VERSION 1024): gossip with it uses " +
                         "DSE's application-state ordinals");

        // and with NONE nobody is identified, and nothing is logged
        DsePeers.reset();
        appender.list.clear();
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
        DsePeers.fromGossipedNetVersion(node1, 11);
        DsePeers.fromPre40Initiate(node1, MessagingService.VERSION_3014);
        DsePeers.fromPre40Reply(node1, MessagingService.VERSION_3014, true);
        DsePeers.fromGossipedNetVersion(node2, DseCompat.DSE_681_VERSION);
        EndpointState dse5118 = capturedState(node1);
        dse5118.addApplicationState(ApplicationState.RELEASE_VERSION, value("3.11.3.5118", 100));
        DsePeers.warnIfNotDse5151(node1, dse5118);
        assertThat(DsePeers.snapshot()).isEmpty();
        assertThat(appender.list).isEmpty();
    }

    /**
     * The captured gossip of a DSE 5.1.51 node, {@code 3.11.3.5151} and {@code dse_version} 5.1.51, logs nothing;
     * another release or {@code dse_version} logs a WARN, once per peer, and is not refused: the peer is identified.
     */
    @Test
    public void aDse51PeerOfAnotherReleaseIsLoggedOncePerPeer() throws Exception
    {
        EndpointState dse5151 = capturedState(node1);
        assertThat(DsePeers.gossipedDseRelease(dse5151)).isEqualTo(Dse51Fixtures.DSE_VERSION);
        Gossiper.instance.endpointStateMap.put(node1, dse5151);
        DsePeers.fromGossipedNetVersion(node1, 11);
        assertThat(messages(Level.WARN)).isEmpty();

        EndpointState dse5118 = capturedState(node2);
        dse5118.addApplicationState(ApplicationState.RELEASE_VERSION, value("3.11.3.5118", 100));
        Gossiper.instance.endpointStateMap.put(node2, dse5118);
        DsePeers.fromGossipedNetVersion(node2, 11);
        DsePeers.fromGossipedNetVersion(node2, 11);
        DsePeers.fromPre40Initiate(node2, MessagingService.VERSION_3014);
        assertThat(messages(Level.WARN)).containsExactly(warning(node2, "3.11.3.5118", "5.1.51"));
        assertThat(DsePeers.isDse(node2)).isTrue();

        // a peer forgotten (it left the ring) and seen again is warned of again
        DsePeers.forget(node2);
        assertThat(DsePeers.isDse(node2)).isFalse();
        DsePeers.fromGossipedNetVersion(node2, 11);
        assertThat(messages(Level.WARN)).containsExactly(warning(node2, "3.11.3.5118", "5.1.51"),
                                                         warning(node2, "3.11.3.5118", "5.1.51"));

        // the dse_version alone, or a build number that names no release, tells another release too
        DsePeers.reset();
        appender.list.clear();
        EndpointState otherDseVersion = capturedState(node1);
        String dseState = dse5151.getApplicationState(ApplicationState.X_11_PADDING).value;
        otherDseVersion.addApplicationState(ApplicationState.X_11_PADDING,
                                            value(dseState.replace("5.1.51", "5.1.18"), 300));
        DsePeers.warnIfNotDse5151(node1, otherDseVersion);
        EndpointState noRelease = capturedState(node2);
        noRelease.addApplicationState(ApplicationState.RELEASE_VERSION, value("3.11.3.5105", 100));
        DsePeers.warnIfNotDse5151(node2, noRelease);
        assertThat(messages(Level.WARN)).containsExactly(warning(node1, "3.11.3.5151", "5.1.18"),
                                                         warning(node2, "3.11.3.5105", "5.1.51"));

        // a state without either says nothing yet, and nothing is checked outside the mode
        appender.list.clear();
        DsePeers.reset();
        DsePeers.warnIfNotDse5151(node1, new EndpointState(new HeartBeatState(1, 1)));
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        DsePeers.warnIfNotDse5151(node2, noRelease);
        assertThat(messages(Level.WARN)).isEmpty();
    }

    /**
     * A DSE 5.1 peer identified by its handshake before its gossip tells its release: the {@code RELEASE_VERSION} of
     * another release, as the gossip code hands it to the storage service, logs the WARN then, once; a peer not
     * identified as DSE 5.1, or one in {@code DSE_6}, is not checked.
     */
    @Test
    public void aReleaseGossipedAfterTheIdentificationIsChecked() throws Exception
    {
        DsePeers.fromPre40Initiate(node1, MessagingService.VERSION_3014);
        assertThat(messages(Level.WARN)).isEmpty();
        EndpointState state = capturedState(node1);
        VersionedValue dse5118 = value("3.11.3.5118", 100);
        state.addApplicationState(ApplicationState.RELEASE_VERSION, dse5118);
        Gossiper.instance.endpointStateMap.put(node1, state);
        StorageService.instance.onChange(node1, ApplicationState.RELEASE_VERSION, dse5118);
        StorageService.instance.onChange(node1, ApplicationState.RELEASE_VERSION, dse5118);
        assertThat(messages(Level.WARN)).containsExactly(warning(node1, "3.11.3.5118", "5.1.51"));

        appender.list.clear();
        EndpointState other = capturedState(node2);
        other.addApplicationState(ApplicationState.RELEASE_VERSION, dse5118);
        Gossiper.instance.endpointStateMap.put(node2, other);
        StorageService.instance.onChange(node2, ApplicationState.RELEASE_VERSION, dse5118);
        assertThat(DsePeers.isDse(node2)).isFalse();
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        DsePeers.fromGossipedNetVersion(node2, DseCompat.DSE_681_VERSION);
        assertThat(DsePeers.isDse(node2)).isTrue();
        StorageService.instance.onChange(node2, ApplicationState.RELEASE_VERSION, dse5118);
        assertThat(messages(Level.WARN)).isEmpty();
    }

    /**
     * A {@code NET_VERSION} that is not a number, as the gossip code hands it to the storage service, is ignored with a
     * WARN in the mode: the gossip stage goes on, the version stays unknown and nobody is identified.
     */
    @Test
    public void anUnparsableNetVersionIsIgnored() throws Exception
    {
        Map<ApplicationState, VersionedValue> states = new EnumMap<>(ApplicationState.class);
        states.put(ApplicationState.NET_VERSION, value("eleven", 1));
        EndpointState state = new EndpointState(new HeartBeatState(1, 1), states);
        state.markDead(); // onRestart would otherwise first treat the peer as one that went down
        Logger storageLogger = (Logger) LoggerFactory.getLogger(StorageService.class);
        ListAppender<ILoggingEvent> storageAppender = new ListAppender<>();
        storageAppender.start();
        storageLogger.addAppender(storageAppender);
        try
        {
            StorageService.instance.onRestart(node1, state);
        }
        finally
        {
            storageLogger.detachAppender(storageAppender);
        }
        assertThat(storageAppender.list.stream()
                                       .filter(e -> e.getLevel() == Level.WARN)
                                       .map(ILoggingEvent::getFormattedMessage))
        .containsExactly("Ignoring the NET_VERSION eleven gossiped by " + node1 + ": not a number; the messaging " +
                         "version of the peer stays unknown");
        assertThat(MessagingService.instance().versions.knows(node1)).isFalse();
        assertThat(DsePeers.isDse(node1)).isFalse();
        assertThat(DsePeers.snapshot()).isEmpty();
    }

    /**
     * As {@link #aReleaseGossipedAfterTheIdentificationIsChecked}, with DSE's state ({@code X_11_PADDING}) of another
     * {@code dse_version} arriving later in gossip.
     */
    @Test
    public void aDseVersionGossipedAfterTheIdentificationIsChecked() throws Exception
    {
        DsePeers.fromPre40Initiate(node1, MessagingService.VERSION_3014);
        EndpointState state = capturedState(node1);
        String dseState = state.getApplicationState(ApplicationState.X_11_PADDING).value;
        VersionedValue dse5118 = value(dseState.replace("5.1.51", "5.1.18"), 300);
        state.addApplicationState(ApplicationState.X_11_PADDING, dse5118);
        Gossiper.instance.endpointStateMap.put(node1, state);
        StorageService.instance.onChange(node1, ApplicationState.X_11_PADDING, dse5118);
        StorageService.instance.onChange(node1, ApplicationState.X_11_PADDING, dse5118);
        assertThat(messages(Level.WARN)).containsExactly(warning(node1, Dse51Fixtures.RELEASE_VERSION, "5.1.18"));
    }

    /**
     * {@code nodetool compatstatus} lists the DSE 5.1 peers seen in gossip, with their release, {@code NET_VERSION} and
     * DSE version, and those identified by a handshake; their gossip uses this software's ordinals.
     */
    @Test
    public void theDse51PeersAreListed() throws Exception
    {
        Gossiper.instance.endpointStateMap.put(node1, capturedState(node1));
        String gossiped = "release 3.11.3.5151, NET_VERSION 11, DSE 5.1.51, gossip ordinals: this software's ";
        assertThat(MessagingService.instance().getDsePeers())
        .containsEntry(node1.getHostAddressAndPort(), gossiped + "(not identified as DSE 5.1 yet)");
        DsePeers.fromGossipedNetVersion(node1, 11);
        DsePeers.fromPre40Initiate(node2, MessagingService.VERSION_3014);
        assertThat(MessagingService.instance().getDsePeers())
        .containsEntry(node1.getHostAddressAndPort(), gossiped + "(identified as DSE 5.1 by gossiped NET_VERSION 11)")
        .containsEntry(node2.getHostAddressAndPort(), "NET_VERSION unknown, gossip ordinals: this software's " +
                                                      "(identified as DSE 5.1 by pre-4.0 first handshake message " +
                                                      "requesting version 11)");
        assertThat(DsePeers.knownAtStartup()).contains(node1.getHostAddressAndPort(), node2.getHostAddressAndPort());
    }

    /** With no DSE 5.1 peer known at startup, the WARN that the mode can be dropped names DSE 5.1 and its marker. */
    @Test
    public void noDse51PeerKnownAtStartupIsWarned()
    {
        assertThat(DsePeers.knownAtStartup()).isEmpty();
        DsePeers.warnIfNoneKnownAtStartup();
        assertThat(messages(Level.WARN))
        .containsExactly("No DSE peers found, the mode can be dropped (internode_compatibility_mode DSE_5_1: no peer " +
                         "speaks the pre-4.0 dialect, and none is known or on record as a DSE 5.1 node): once every " +
                         "node runs this software, delete the DSE 5.1 upgrade marker " +
                         DseUpgradeMarker.path(InternodeCompatibilityMode.DSE_5_1) + " and restart the node with " +
                         "internode_compatibility_mode NONE");
        appender.list.clear();
        MessagingService.instance().versions.set(node1, MessagingService.VERSION_3014);
        DsePeers.warnIfNoneKnownAtStartup();
        assertThat(messages(Level.WARN)).isEmpty();
    }

    /** The WARN of a DSE 5.1 peer of another release than DSE 5.1.51. */
    private static String warning(InetAddressAndPort peer, String release, String dseVersion)
    {
        return peer + " is a DSE 5.1 node of release " + release + " (dse_version " + dseVersion + "), not of DSE " +
               "5.1.51, the one DSE 5.1 release internode_compatibility_mode DSE_5_1 supports; it is not refused: " +
               "upgrade it to DSE 5.1.51 before it is upgraded to this software";
    }

    private static VersionedValue value(String value, int version)
    {
        return VersionedValue.unsafeMakeVersionedValue(value, version);
    }

    /** The endpoint state a DSE 5.1.51 node gossiped, as node 1 printed it, but its tokens, which it hides. */
    private static EndpointState capturedState(InetAddressAndPort node) throws Exception
    {
        Map<ApplicationState, VersionedValue> states = new EnumMap<>(ApplicationState.class);
        for (GossipState state : DSE51.gossipStates(DSE51.gossipInfo(1), '/' + node.getHostAddress(false)))
            if (!state.name.equals("TOKENS"))
                states.put(ApplicationState.valueOf(state.name), value(state.value, state.version));
        return new EndpointState(new HeartBeatState(1, 1), states);
    }

    private List<String> messages(Level level)
    {
        return appender.list.stream()
                            .filter(e -> e.getLevel() == level)
                            .map(ILoggingEvent::getFormattedMessage)
                            .collect(Collectors.toList());
    }
}
