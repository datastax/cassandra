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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.HeartBeatState;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.DseCompat;
import org.apache.cassandra.net.DsePeers;
import org.apache.cassandra.net.EndpointMessagingVersions;
import org.apache.cassandra.net.MessagingService;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The record of DSE 6 peers: maintained from the messaging versions the node records (the dialect from a
 * handshake, DSE's own gossip values), loaded from the previous run at startup, persisted for the startup checks
 * (once more at a drain), and untouched with {@code internode_compatibility_mode: NONE}.
 */
public class DsePeersRecordTest
{
    private static InetAddressAndPort dsePeer;
    private static InetAddressAndPort otherDsePeer;
    private static InetAddressAndPort upgradedPeer;
    private InternodeCompatibilityMode modeBefore;
    private File metadataBefore;
    private Path metadata;

    @BeforeClass
    public static void init() throws Exception
    {
        DatabaseDescriptor.daemonInitialization();
        dsePeer = InetAddressAndPort.getByName("127.0.0.44:7000");
        otherDsePeer = InetAddressAndPort.getByName("127.0.0.45:7000");
        upgradedPeer = InetAddressAndPort.getByName("127.0.0.46:7000");
    }

    @Before
    public void setUp() throws IOException
    {
        modeBefore = DatabaseDescriptor.getInternodeCompatibilityMode();
        // a metadata directory of each test's own, so that no record is left where other tests start a node
        metadataBefore = DatabaseDescriptor.getMetadataDirectory();
        metadata = Files.createTempDirectory("dse-peers-record");
        DatabaseDescriptor.setMetadataDirectory(new File(metadata));
        DsePeersRecord.clear();
    }

    @After
    public void tearDown() throws IOException
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        for (InetAddressAndPort peer : new InetAddressAndPort[]{ dsePeer, otherDsePeer, upgradedPeer })
            MessagingService.instance().versions.reset(peer);
        DatabaseDescriptor.setInternodeCompatibilityMode(modeBefore);
        DsePeersRecord.clear();
        DatabaseDescriptor.setMetadataDirectory(metadataBefore);
        new File(metadata).deleteRecursive();
    }

    private static void set(InetAddressAndPort peer, int version) throws Exception
    {
        MessagingService.instance().versions.set(peer, version);
        DsePeersRecord.awaitPersisted();
    }

    private static void reset(InetAddressAndPort peer) throws Exception
    {
        MessagingService.instance().versions.reset(peer);
        DsePeersRecord.awaitPersisted();
    }

    @Test
    public void pre40VersionsAreRecordedAndPersistedInDse6Mode() throws Exception
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        set(dsePeer, MessagingService.VERSION_3014);
        set(upgradedPeer, MessagingService.current_version);

        assertThat(DsePeersRecord.peers()).containsExactly(dsePeer);
        assertThat(DsePeersRecord.path()).exists();
        assertThat(DsePeersRecord.persistedPeers()).containsExactly("127.0.0.44:7000");
        String content = Files.readString(DsePeersRecord.path(), StandardCharsets.UTF_8);
        assertThat(content).startsWith("# peers known to speak the pre-4.0 dialect").endsWith("127.0.0.44:7000\n");

        set(otherDsePeer, MessagingService.VERSION_30);
        assertThat(DsePeersRecord.persistedPeers()).containsExactly("127.0.0.44:7000", "127.0.0.45:7000");
    }

    @Test
    public void gossipedDseVersionsReachTheRecordAtTheDialect() throws Exception
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        try
        {
            // DSE's own NET_VERSION (6.8: 1024): the gossip code stores the dialect, which puts the peer on record
            gossipNetVersion(dsePeer, "1024");
            assertThat(MessagingService.instance().versions.getRaw(dsePeer)).isEqualTo(MessagingService.VERSION_3014);
            assertThat(DsePeers.isDse(dsePeer)).isTrue();
            assertThat(DsePeersRecord.peers()).containsExactly(dsePeer);
            assertThat(DsePeersRecord.persistedPeers()).containsExactly("127.0.0.44:7000");

            // a node of this software in DSE_6 mode gossips 12 for DSE's sake: the version negotiated with it stays, and it
            // does not enter the record
            set(upgradedPeer, MessagingService.current_version);
            gossipNetVersion(upgradedPeer, String.valueOf(DseCompat.NET_VERSION_PUBLISHED));
            assertThat(MessagingService.instance().versions.getRaw(upgradedPeer)).isEqualTo(MessagingService.current_version);
            assertThat(DsePeersRecord.peers()).containsExactly(dsePeer);

            // the DSE node is upgraded and gossips 12: it is no longer a DSE node and leaves the record
            gossipNetVersion(dsePeer, String.valueOf(DseCompat.NET_VERSION_PUBLISHED));
            assertThat(MessagingService.instance().versions.getRaw(dsePeer)).isEqualTo(DseCompat.NET_VERSION_PUBLISHED);
            assertThat(DsePeers.isDse(dsePeer)).isFalse();
            assertThat(DsePeersRecord.peers()).isEmpty();
            assertThat(DsePeersRecord.persistedPeers()).isEmpty();
        }
        finally
        {
            DsePeers.reset();
        }
    }

    /** A {@code NET_VERSION} as the gossip code applies it to the storage service (a node restarted, or a state change). */
    private static void gossipNetVersion(InetAddressAndPort peer, String netVersion) throws Exception
    {
        Map<ApplicationState, VersionedValue> states = new EnumMap<>(ApplicationState.class);
        states.put(ApplicationState.NET_VERSION, VersionedValue.unsafeMakeVersionedValue(netVersion, 1));
        EndpointState state = new EndpointState(new HeartBeatState(1, 1), states);
        state.markDead(); // onRestart would otherwise first treat the peer as one that went down
        StorageService.instance.onRestart(peer, state);
        DsePeersRecord.awaitPersisted();
    }

    @Test
    public void rawDseGossipValuesAreRecordedAsDsePeers() throws Exception
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        // NET_VERSION as DSE 6.8 and 6.9.7+ gossip it (relative version << 8): the gossip code stores the dialect in
        // DSE_6 mode (see above), the record still reads a raw value as the dialect
        set(dsePeer, 1024);
        set(otherDsePeer, 1536);
        assertThat(DsePeersRecord.peers()).containsExactly(dsePeer, otherDsePeer);
        assertThat(DsePeersRecord.persistedPeers()).containsExactly("127.0.0.44:7000", "127.0.0.45:7000");
        // the handshake then records the dialect: still the same peer, nothing changes
        set(dsePeer, MessagingService.VERSION_3014);
        assertThat(DsePeersRecord.peers()).containsExactly(dsePeer, otherDsePeer);
    }

    @Test
    public void copiesOfTheVersionMapDoNotTouchTheRecord() throws Exception
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        set(dsePeer, MessagingService.VERSION_3014);
        set(otherDsePeer, MessagingService.VERSION_3014);
        // an outbound connection works from a copy taken while the peers were still at the dialect
        EndpointMessagingVersions copy = MessagingService.instance().versions.copy();
        // the peer is upgraded: the node's own map sees it at 4.0 or later and it leaves the record
        set(dsePeer, MessagingService.current_version);
        assertThat(DsePeersRecord.peers()).containsExactly(otherDsePeer);
        // the stale copy records the dialect again (a reconnect from the old value) and forgets the other peer: the
        // record is not affected
        copy.set(dsePeer, MessagingService.VERSION_3014);
        copy.reset(otherDsePeer);
        DsePeersRecord.awaitPersisted();
        assertThat(DsePeersRecord.peers()).containsExactly(otherDsePeer);
        assertThat(DsePeersRecord.persistedPeers()).containsExactly("127.0.0.45:7000");
        // nor does a map that is not the node's own
        EndpointMessagingVersions other = new EndpointMessagingVersions();
        other.set(dsePeer, MessagingService.VERSION_3014);
        other.reset(otherDsePeer);
        DsePeersRecord.awaitPersisted();
        assertThat(DsePeersRecord.peers()).containsExactly(otherDsePeer);
        assertThat(DsePeersRecord.persistedPeers()).containsExactly("127.0.0.45:7000");
    }

    @Test
    public void aPeerLeavesTheRecordWhenSeenAt40OrForgotten() throws Exception
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        set(dsePeer, MessagingService.VERSION_3014);
        set(otherDsePeer, MessagingService.VERSION_3014);
        assertThat(DsePeersRecord.persistedPeers()).hasSize(2);

        // the peer was upgraded: it now handshakes at 4.0 or later
        set(dsePeer, MessagingService.VERSION_40);
        assertThat(DsePeersRecord.peers()).containsExactly(otherDsePeer);
        assertThat(DsePeersRecord.persistedPeers()).containsExactly("127.0.0.45:7000");

        // the peer left the ring: its version is forgotten, and the empty record is deleted
        reset(otherDsePeer);
        assertThat(DsePeersRecord.peers()).isEmpty();
        assertThat(DsePeersRecord.path()).doesNotExist();
        assertThat(DsePeersRecord.persistedPeers()).isEmpty();
    }

    @Test
    public void thePersistedRecordIsLoadedAtStartupAndOnlyLeftWhenSeenAgain() throws Exception
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        Files.createDirectories(DsePeersRecord.path().getParent());
        Files.write(DsePeersRecord.path(), "# from the previous run\n127.0.0.44:7000\n127.0.0.45:7000\n127.0.0.99:not-a-port\n".getBytes(StandardCharsets.UTF_8));
        DsePeersRecord.loadAtStartup();
        assertThat(DsePeersRecord.peers()).containsExactly(dsePeer, otherDsePeer);
        // nodetool compatstatus lists the line that is not an address after the peers
        assertThat(DsePeersRecord.unparsedEntries()).containsExactly("127.0.0.99:not-a-port");
        assertThat(MessagingService.instance().getDsePeersOnRecord()).containsExactly("127.0.0.44:7000", "127.0.0.45:7000", "127.0.0.99:not-a-port");

        // a DSE peer that stays down during this run (127.0.0.45) is not forgotten when another peer is recorded,
        // and a line that does not parse is kept in the file
        set(upgradedPeer, MessagingService.current_version);
        set(dsePeer, MessagingService.VERSION_3014);
        assertThat(DsePeersRecord.peers()).containsExactly(dsePeer, otherDsePeer);
        assertThat(DsePeersRecord.persistedPeers()).containsExactly("127.0.0.44:7000", "127.0.0.45:7000", "127.0.0.99:not-a-port");

        // a peer upgraded while this node was down leaves the record when it is seen at 4.0 or later
        set(dsePeer, MessagingService.VERSION_40);
        assertThat(DsePeersRecord.peers()).containsExactly(otherDsePeer);
        assertThat(DsePeersRecord.persistedPeers()).containsExactly("127.0.0.45:7000", "127.0.0.99:not-a-port");

        // loaded once per process
        Files.write(DsePeersRecord.path(), "127.0.0.46:7000\n".getBytes(StandardCharsets.UTF_8));
        DsePeersRecord.loadAtStartup();
        assertThat(DsePeersRecord.peers()).containsExactly(otherDsePeer);
    }

    @Test
    public void persistedFileIgnoresCommentsAndBlankLines() throws IOException
    {
        Files.createDirectories(DsePeersRecord.path().getParent());
        Files.write(DsePeersRecord.path(), "# a comment\n\n 127.0.0.45:7000 \n127.0.0.44:7000\n".getBytes(StandardCharsets.UTF_8));
        assertThat(DsePeersRecord.persistedPeers()).containsExactly("127.0.0.44:7000", "127.0.0.45:7000");
    }

    @Test
    public void nothingIsRecordedOrLoadedInNoneMode() throws Exception
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
        Files.createDirectories(DsePeersRecord.path().getParent());
        Files.write(DsePeersRecord.path(), "127.0.0.45:7000\n".getBytes(StandardCharsets.UTF_8));
        DsePeersRecord.loadAtStartup();
        assertThat(DsePeersRecord.peers()).isEmpty();
        Files.delete(DsePeersRecord.path());

        set(dsePeer, MessagingService.VERSION_3014);
        set(otherDsePeer, 1536);
        assertThat(DsePeersRecord.peers()).isEmpty();
        assertThat(DsePeersRecord.path()).doesNotExist();
        assertThat(DsePeersRecord.persistedPeers()).isEmpty();
        reset(dsePeer);
        assertThat(DsePeersRecord.path()).doesNotExist();
    }

    @Test
    public void aFailedWriteIsLoggedAndKeepsThePeerInMemory() throws Exception
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        Path notADirectory = Files.write(metadata.resolve("metadata-file"), new byte[0]);
        Logger logger = (Logger) LoggerFactory.getLogger(DsePeersRecord.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        try
        {
            // the metadata "directory" is a regular file: the record cannot be written under it
            DatabaseDescriptor.setMetadataDirectory(new File(notADirectory));
            set(dsePeer, MessagingService.VERSION_3014);
            assertThat(DsePeersRecord.peers()).containsExactly(dsePeer);
            List<String> errors = appender.list.stream()
                                               .filter(e -> e.getLevel() == Level.ERROR)
                                               .map(ILoggingEvent::getFormattedMessage)
                                               .collect(Collectors.toList());
            assertThat(errors).containsExactly("Unable to persist the record of DSE 6 peers to " + notADirectory.resolve(DsePeersRecord.FILE_NAME) +
                                               "; a later start with internode_compatibility_mode: NONE will not know about them");
        }
        finally
        {
            logger.detachAppender(appender);
            DatabaseDescriptor.setMetadataDirectory(new File(metadata));
        }
    }

    @Test
    public void aLeftoverTemporaryFileIsReplaced() throws Exception
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        Path temporary = DsePeersRecord.path().resolveSibling(DsePeersRecord.FILE_NAME + ".tmp");
        Files.createDirectories(temporary.getParent());
        Files.write(temporary, "127.0.0.99:7000\n".getBytes(StandardCharsets.UTF_8));
        // the temporary file of a write interrupted by a crash is never read
        assertThat(DsePeersRecord.persistedPeers()).isEmpty();
        set(dsePeer, MessagingService.VERSION_3014);
        assertThat(DsePeersRecord.persistedPeers()).containsExactly("127.0.0.44:7000");
        assertThat(temporary).doesNotExist();

        // nor is one left when the record empties
        Files.write(temporary, "127.0.0.99:7000\n".getBytes(StandardCharsets.UTF_8));
        reset(dsePeer);
        assertThat(DsePeersRecord.path()).doesNotExist();
        assertThat(temporary).doesNotExist();
    }

    @Test
    public void theRecordIsWrittenInPlaceWhenTheExecutorIsShutDownOrRejectsIt() throws Exception
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        set(dsePeer, MessagingService.VERSION_3014);

        // shut down: written on the calling thread
        Files.delete(DsePeersRecord.path());
        ExecutorService shutDown = Executors.newSingleThreadExecutor();
        shutDown.shutdown();
        assertThat(DsePeersRecord.persistOn(shutDown)).isNull();
        assertThat(DsePeersRecord.persistedPeers()).containsExactly("127.0.0.44:7000");

        // shut down between the check and the submission: the rejection is answered the same way
        Files.delete(DsePeersRecord.path());
        ExecutorService rejecting = Mockito.mock(ExecutorService.class);
        Mockito.when(rejecting.isShutdown()).thenReturn(false);
        Mockito.when(rejecting.submit(Mockito.any(Runnable.class))).thenThrow(new RejectedExecutionException("shut down"));
        assertThat(DsePeersRecord.persistOn(rejecting)).isNull();
        assertThat(DsePeersRecord.persistedPeers()).containsExactly("127.0.0.44:7000");
    }

    @Test
    public void theDrainWritesTheRecordOnceMoreWhenItChanged() throws Exception
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        // a record loaded from the previous run and not changed since: the file already holds it and is left as it is
        Files.createDirectories(DsePeersRecord.path().getParent());
        Files.write(DsePeersRecord.path(), "127.0.0.45:7000\n".getBytes(StandardCharsets.UTF_8));
        DsePeersRecord.loadAtStartup();
        Files.write(DsePeersRecord.path(), "# edited by hand\n127.0.0.45:7000\n".getBytes(StandardCharsets.UTF_8));
        DsePeersRecord.persistAtDrain();
        assertThat(Files.readString(DsePeersRecord.path(), StandardCharsets.UTF_8)).isEqualTo("# edited by hand\n127.0.0.45:7000\n");

        // a change whose write did not reach the file (dropped by the drain): the final write, on the calling thread,
        // puts the last state there
        set(dsePeer, MessagingService.VERSION_3014);
        Files.delete(DsePeersRecord.path());
        DsePeersRecord.persistAtDrain();
        assertThat(DsePeersRecord.persistedPeers()).containsExactly("127.0.0.44:7000", "127.0.0.45:7000");
    }

    @Test
    public void theDrainWritesNothingInNoneMode() throws Exception
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
        Files.createDirectories(DsePeersRecord.path().getParent());
        Files.write(DsePeersRecord.path(), "127.0.0.45:7000\n".getBytes(StandardCharsets.UTF_8));
        set(dsePeer, MessagingService.VERSION_3014);
        DsePeersRecord.persistAtDrain();
        assertThat(Files.readString(DsePeersRecord.path(), StandardCharsets.UTF_8)).isEqualTo("127.0.0.45:7000\n");

        // nor for a record changed in DSE_6 mode once the mode is off
        Files.delete(DsePeersRecord.path());
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        set(dsePeer, MessagingService.VERSION_3014);
        Files.delete(DsePeersRecord.path());
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
        DsePeersRecord.persistAtDrain();
        assertThat(DsePeersRecord.path()).doesNotExist();
    }

    @Test
    public void anIpv6PeerIsPersistedAndReadBack() throws Exception
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        InetAddressAndPort ipv6Peer = InetAddressAndPort.getByName("[::1]:7000");
        try
        {
            set(ipv6Peer, MessagingService.VERSION_3014);
            assertThat(DsePeersRecord.persistedPeers()).containsExactly("[0:0:0:0:0:0:0:1]:7000");
            assertThat(DsePeersRecord.parseEntry("[0:0:0:0:0:0:0:1]:7000")).isEqualTo(ipv6Peer);
        }
        finally
        {
            reset(ipv6Peer);
        }
        assertThat(DsePeersRecord.path()).doesNotExist();
    }

    @Test
    public void onlyIpAddressesWithPortAreEntries() throws Exception
    {
        assertThat(DsePeersRecord.parseEntry("127.0.0.44:7000")).isEqualTo(dsePeer);
        assertThat(DsePeersRecord.parseEntry("[::1]:7000")).isEqualTo(InetAddressAndPort.getByName("[::1]:7000"));
        // the port is part of an entry
        assertThat(DsePeersRecord.parseEntry("127.0.0.44")).isNull();
        // a host name is not resolved: reading the record never waits on DNS
        assertThat(DsePeersRecord.parseEntry("localhost:7000")).isNull();
        assertThat(DsePeersRecord.parseEntry("127.0.0.99:not-a-port")).isNull();
        assertThat(DsePeersRecord.parseEntry("")).isNull();
    }
}
