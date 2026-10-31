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

package org.apache.cassandra.tools.nodetool;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.UndeclaredThrowableException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import javax.management.AttributeNotFoundException;

import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.config.StartupChecksOptions;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.HeartBeatState;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.DseCompatRefusals;
import org.apache.cassandra.net.DsePeers;
import org.apache.cassandra.net.EndpointMessagingVersions;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.Dse6SaiVersionPin;
import org.apache.cassandra.service.DsePeersRecord;
import org.apache.cassandra.service.StartupChecks;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.Output;
import org.apache.cassandra.tools.ToolRunner;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.config.CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE_IGNORE_SAI_VERSION_PIN;
import static org.apache.cassandra.config.CassandraRelevantProperties.SAI_CURRENT_VERSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class CompatStatusTest extends CQLTester
{
    @BeforeClass
    public static void setup() throws Exception
    {
        requireNetwork();
        startJMXServer();
    }

    @Test
    public void helpIsListed()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("help", "compatstatus");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).contains("compatstatus").contains("compatibility");
    }

    @Test
    public void printsModesAndPeerVersions() throws Exception
    {
        EndpointMessagingVersions versions = MessagingService.instance().versions;
        InetAddressAndPort peer = InetAddressAndPort.getByName("127.0.0.42:7000");
        InetAddressAndPort dsePeer = InetAddressAndPort.getByName("127.0.0.43:7000");
        boolean localKnown = knowLocalEndpoint();
        versions.set(peer, MessagingService.current_version);
        versions.set(dsePeer, MessagingService.VERSION_3014);
        try
        {
            ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("compatstatus");
            tool.assertOnCleanExit();
            String out = tool.getStdout();
            assertThat(out).contains("Internode compatibility mode : " + DatabaseDescriptor.getInternodeCompatibilityMode().name());
            assertThat(out).contains("Storage compatibility mode   : " + DatabaseDescriptor.getStorageCompatibilityMode().name());
            SSTableFormat<?, ?> format = DatabaseDescriptor.getSelectedSSTableFormat();
            assertThat(out).contains("SSTable format written       : " + format.name() + '-' + format.getLatestVersion().version);
            List<String> lines = Arrays.asList(out.split("\\R"));
            assertThat(lines).contains(String.format("  %-45s %d", "127.0.0.42:7000", MessagingService.current_version));
            assertThat(lines).contains(String.format("  %-45s %d  (pre-4.0 dialect)", "127.0.0.43:7000", MessagingService.VERSION_3014));
            assertThat(out).contains("(1 speaking the pre-4.0 dialect)");
            // NONE: the DSE peers and their record are not kept, and nothing was refused
            assertThat(out).contains("DSE peers                    : not tracked (internode_compatibility_mode NONE)\n");
            assertThat(out).contains("DSE 6 peers on record        : not maintained (internode_compatibility_mode NONE)\n");
            assertThat(out).contains("SAI version pinned           : not checked (internode_compatibility_mode NONE)\n");
            assertThat(out).contains("Requests refused (upgrade)   : 0\n");
        }
        finally
        {
            versions.reset(dsePeer);
            versions.reset(peer);
            forgetLocalEndpoint(localKnown);
        }
        assertThat(versions.minClusterVersion).isEqualTo(MessagingService.current_version);
    }

    @Test
    public void printsTheRefusalsByReason()
    {
        DseCompatRefusals.reset();
        try
        {
            ToolRunner.ToolResult none = ToolRunner.invokeNodetool("compatstatus");
            none.assertOnCleanExit();
            assertThat(none.getStdout()).contains("Requests refused (upgrade)   : 0");

            DseCompatRefusals.refused("verb PING_REQ to a DSE 6 peer");
            DseCompatRefusals.refused("verb PING_REQ to a DSE 6 peer");
            DseCompatRefusals.refused("read with an index hint");
            ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("compatstatus");
            tool.assertOnCleanExit();
            List<String> lines = Arrays.asList(tool.getStdout().split("\\R"));
            assertThat(lines).contains("Requests refused (upgrade)   : 3");
            // the peer versions, the DSE peers, the record of DSE peers, the SAI pin, then the refusals
            String out = tool.getStdout();
            assertThat(out.indexOf("Peers with a known version")).isNotNegative()
                                                                  .isLessThan(out.indexOf("DSE peers                    :"));
            assertThat(out.indexOf("DSE peers                    :")).isLessThan(out.indexOf("DSE 6 peers on record        :"));
            assertThat(out.indexOf("DSE 6 peers on record        :")).isLessThan(out.indexOf("SAI version pinned           :"));
            assertThat(out.indexOf("SAI version pinned           :")).isLessThan(out.indexOf("Requests refused (upgrade)"));
            assertThat(lines).contains(String.format("  %-45s %d", "read with an index hint", 1));
            assertThat(lines).contains(String.format("  %-45s %d", "verb PING_REQ to a DSE 6 peer", 2));
        }
        finally
        {
            DseCompatRefusals.reset();
        }
    }

    @Test
    public void printsTheDialectPeersAndTheRefusalsInDse6Mode() throws Exception
    {
        InetAddressAndPort dsePeer = InetAddressAndPort.getByName("127.0.0.44:7000");
        boolean localKnown = knowLocalEndpoint();
        InternodeCompatibilityMode modeBefore = DatabaseDescriptor.getInternodeCompatibilityMode();
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        DseCompatRefusals.reset();
        try
        {
            // a DSE peer recorded at the dialect
            MessagingService.instance().versions.set(dsePeer, MessagingService.VERSION_3014);
            DseCompatRefusals.refused("repair");
            DseCompatRefusals.refused("repair");
            DseCompatRefusals.refused("schema change");
            ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("compatstatus");
            tool.assertOnCleanExit();
            String out = tool.getStdout();
            assertThat(out).contains("Internode compatibility mode : DSE_6");
            assertThat(Arrays.asList(out.split("\\R"))).contains(String.format("  %-45s %d  (pre-4.0 dialect)", "127.0.0.44:7000", MessagingService.VERSION_3014));
            assertThat(out).contains("(1 speaking the pre-4.0 dialect)");
            assertThat(out).contains("Requests refused (upgrade)   : 3");
            assertThat(out).containsPattern("repair +2");
            assertThat(out).containsPattern("schema change +1");
        }
        finally
        {
            MessagingService.instance().versions.reset(dsePeer);
            forgetLocalEndpoint(localKnown);
            DseCompatRefusals.reset();
            DatabaseDescriptor.setInternodeCompatibilityMode(modeBefore);
        }
        assertThat(MessagingService.instance().versions.minClusterVersion).isEqualTo(MessagingService.current_version);
    }

    @Test
    public void printsTheRecordInDse6Mode() throws Exception
    {
        InetAddressAndPort dsePeer = InetAddressAndPort.getByName("127.0.0.44:7000");
        boolean localKnown = knowLocalEndpoint();
        InternodeCompatibilityMode modeBefore = DatabaseDescriptor.getInternodeCompatibilityMode();
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        DsePeersRecord.clear();
        try
        {
            // in DSE_6 mode a peer seen at the dialect enters the record of DSE 6 peers
            MessagingService.instance().versions.set(dsePeer, MessagingService.VERSION_3014);
            DsePeersRecord.awaitPersisted();
            ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("compatstatus");
            tool.assertOnCleanExit();
            assertThat(tool.getStdout()).contains("DSE 6 peers on record        : 1 [127.0.0.44:7000]\n");
        }
        finally
        {
            MessagingService.instance().versions.reset(dsePeer);
            forgetLocalEndpoint(localKnown);
            DatabaseDescriptor.setInternodeCompatibilityMode(modeBefore);
            DsePeersRecord.clear();
        }
        assertThat(MessagingService.instance().versions.minClusterVersion).isEqualTo(MessagingService.current_version);
    }

    @Test
    public void printsTheSaiVersionPinInDse6Mode() throws Exception
    {
        InternodeCompatibilityMode modeBefore = DatabaseDescriptor.getInternodeCompatibilityMode();
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        try
        {
            // before the startup check: unknown
            Dse6SaiVersionPin.resetStatus();
            assertThat(ToolRunner.invokeNodetool("compatstatus").getStdout()).contains("SAI version pinned           : unknown (not checked yet)\n");

            // the startup check of the SAI pin: no DSE release is stored in this JVM, the pin is taken as set
            try (WithProperties pin = new WithProperties().set(SAI_CURRENT_VERSION, "aa").clear(INTERNODE_COMPATIBILITY_MODE_IGNORE_SAI_VERSION_PIN))
            {
                StartupChecks.checkDse6SaiVersionPin.execute(new StartupChecksOptions());
            }
            assertThat(ToolRunner.invokeNodetool("compatstatus").getStdout())
            .contains("SAI version pinned           : aa, set explicitly; the DSE release the cluster ran cannot be determined: " +
                      "no DSE 6 release (4.0.0.<build>) is stored for this node or its saved peers\n");
        }
        finally
        {
            DatabaseDescriptor.setInternodeCompatibilityMode(modeBefore);
            Dse6SaiVersionPin.resetStatus();
        }
    }

    @Test
    public void aDseNodeKnownFromGossipOnlyWithoutTheMode() throws Exception
    {
        InetAddressAndPort dseNode = InetAddressAndPort.getByName("127.0.0.46:7000");
        boolean localKnown = knowLocalEndpoint();
        InternodeCompatibilityMode modeBefore = DatabaseDescriptor.getInternodeCompatibilityMode();
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
        try
        {
            // without the mode, DSE's own gossip value of a DSE node gossiped by an upgraded peer is stored as it is
            MessagingService.instance().versions.set(dseNode, 1536);
            ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("compatstatus");
            tool.assertOnCleanExit();
            List<String> lines = Arrays.asList(tool.getStdout().split("\\R"));
            assertThat(lines).anyMatch(line -> line.matches("Peers with a known version   : \\d+ \\(0 speaking the pre-4.0 dialect, 1 DSE 6 node\\(s\\) known from gossip only\\)"))
                             .contains(String.format("  %-45s %d  (DSE 6 node known from gossip only; reachable in DSE_6 mode only)", "127.0.0.46:7000", 1536))
                             .contains("DSE peers                    : not tracked (internode_compatibility_mode NONE)")
                             .contains("DSE 6 peers on record        : not maintained (internode_compatibility_mode NONE)");
            assertThat(DsePeersRecord.peers()).isEmpty();
        }
        finally
        {
            MessagingService.instance().versions.reset(dseNode);
            forgetLocalEndpoint(localKnown);
            DatabaseDescriptor.setInternodeCompatibilityMode(modeBefore);
        }
        assertThat(MessagingService.instance().versions.minClusterVersion).isEqualTo(MessagingService.current_version);
    }

    @Test
    public void dsePeersOfAServerWithoutTheAttributeAreUnknown()
    {
        NodeProbe probe = Mockito.mock(NodeProbe.class);
        Mockito.when(probe.getDsePeers()).thenThrow(new UndeclaredThrowableException(new AttributeNotFoundException("DsePeers")));
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (PrintStream out = new PrintStream(bytes, true, StandardCharsets.UTF_8))
        {
            CompatStatus.printDsePeers(probe, out);
        }
        assertThat(bytes.toString(StandardCharsets.UTF_8)).isEqualTo(String.format("DSE peers                    : unknown (this node does not report them)%n"));

        // any other failure is not hidden
        UndeclaredThrowableException other = new UndeclaredThrowableException(new IOException("connection lost"));
        Mockito.doThrow(other).when(probe).getDsePeers();
        assertThatThrownBy(() -> CompatStatus.printDsePeers(probe, new PrintStream(new ByteArrayOutputStream(), true, StandardCharsets.UTF_8)))
        .isSameAs(other);
    }

    @Test
    public void attributesOfAnEarlierServerBuildAreUnknown()
    {
        NodeProbe probe = Mockito.mock(NodeProbe.class);
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(bytes, true, StandardCharsets.UTF_8);
        Mockito.when(probe.output()).thenReturn(new Output(out, out));
        Mockito.when(probe.getInternodeCompatibilityMode()).thenReturn("DSE_6");
        Mockito.when(probe.getStorageCompatibilityMode()).thenReturn("NONE");
        Mockito.when(probe.getSSTableFormatVersion()).thenReturn("bti-da");
        Mockito.when(probe.getEndpointMessagingVersions()).thenReturn(Map.of("127.0.0.45:7000", MessagingService.VERSION_3014));
        Mockito.when(probe.getDsePeers()).thenReturn(Map.of("127.0.0.45:7000", "release 4.0.0.6865, NET_VERSION 1024"));
        Mockito.when(probe.getDsePeersOnRecord()).thenThrow(new UndeclaredThrowableException(new AttributeNotFoundException("DsePeersOnRecord")));
        Mockito.when(probe.getDseSaiVersionPin()).thenThrow(new UndeclaredThrowableException(new AttributeNotFoundException("DseSaiVersionPin")));
        Mockito.when(probe.getDseCompatRefusals()).thenThrow(new UndeclaredThrowableException(new AttributeNotFoundException("DseCompatRefusals")));

        new CompatStatus().execute(probe);

        assertThat(Arrays.asList(bytes.toString(StandardCharsets.UTF_8).split("\\R")))
        .containsExactly("Internode compatibility mode : DSE_6",
                         "Storage compatibility mode   : NONE",
                         "SSTable format written       : bti-da",
                         "Peers with a known version   : 1 (1 speaking the pre-4.0 dialect)",
                         String.format("  %-45s %d  (pre-4.0 dialect)", "127.0.0.45:7000", MessagingService.VERSION_3014),
                         "DSE peers                    : 1",
                         String.format("  %-45s %s", "127.0.0.45:7000", "release 4.0.0.6865, NET_VERSION 1024"),
                         "DSE 6 peers on record        : unknown (this node does not report them)",
                         "SAI version pinned           : unknown (this node does not report it)",
                         "Requests refused (upgrade)   : unknown (this node does not report them)");

        // any other failure is not hidden
        UndeclaredThrowableException other = new UndeclaredThrowableException(new IOException("connection lost"));
        Mockito.doThrow(other).when(probe).getDsePeersOnRecord();
        assertThatThrownBy(() -> new CompatStatus().execute(probe)).isSameAs(other);
    }

    @Test
    public void aServerWithoutTheAttributesHasNoCompatibilityStatus()
    {
        NodeProbe probe = Mockito.mock(NodeProbe.class);
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(bytes, true, StandardCharsets.UTF_8);
        Mockito.when(probe.output()).thenReturn(new Output(out, out));
        Mockito.when(probe.getInternodeCompatibilityMode()).thenThrow(new UndeclaredThrowableException(new AttributeNotFoundException("InternodeCompatibilityMode")));

        new CompatStatus().execute(probe);

        assertThat(bytes.toString(StandardCharsets.UTF_8))
        .isEqualTo(String.format("Internode compatibility mode : unknown (this node does not report its compatibility status)%n"));
        Mockito.verify(probe).output();
        Mockito.verify(probe).getInternodeCompatibilityMode();
        Mockito.verifyNoMoreInteractions(probe);

        // any other failure is not hidden
        UndeclaredThrowableException other = new UndeclaredThrowableException(new IOException("connection lost"));
        Mockito.doThrow(other).when(probe).getInternodeCompatibilityMode();
        assertThatThrownBy(() -> new CompatStatus().execute(probe)).isSameAs(other);
    }

    @Test
    public void listsTheDsePeersSeenInGossip() throws Exception
    {
        InetAddressAndPort dsePeer = InetAddressAndPort.getByName("127.0.0.44:7000");
        Map<ApplicationState, VersionedValue> states = new EnumMap<>(ApplicationState.class);
        states.put(ApplicationState.NET_VERSION, VersionedValue.unsafeMakeVersionedValue("1024", 1));
        states.put(ApplicationState.RELEASE_VERSION, VersionedValue.unsafeMakeVersionedValue("4.0.0.6865", 4));
        states.put(ApplicationState.X_11_PADDING, VersionedValue.unsafeMakeVersionedValue("{\"dse_version\":\"6.8.65\",\"workload\":\"Cassandra\"}", 808));
        // a ring member: gossip evicts an endpoint without tokens ("fat client") after a second of silence
        StorageService.instance.getTokenMetadata().updateNormalToken(DatabaseDescriptor.getPartitioner().getRandomToken(), dsePeer);
        Gossiper.instance.endpointStateMap.put(dsePeer, new EndpointState(new HeartBeatState(1, 1), states));
        InternodeCompatibilityMode modeBefore = DatabaseDescriptor.getInternodeCompatibilityMode();
        // the DSE peers are listed in DSE 6 compatibility mode only
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        try
        {
            ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("compatstatus");
            tool.assertOnCleanExit();
            List<String> lines = Arrays.asList(tool.getStdout().split("\\R"));
            assertThat(lines).contains("DSE peers                    : 1")
                             .contains(String.format("  %-45s %s", "127.0.0.44:7000",
                                                     "release 4.0.0.6865, NET_VERSION 1024, DSE 6.8.65, gossip ordinals: this software's (not identified as DSE yet)"));

            // identified by a handshake: the evidence is shown, also for a peer without a gossip state
            InetAddressAndPort handshakeOnly = InetAddressAndPort.getByName("127.0.0.47:7000");
            DsePeers.identify(dsePeer, "DSE-flagged first handshake message");
            DsePeers.identify(handshakeOnly, "handshake reply with the DSE messaging version 1536");
            tool = ToolRunner.invokeNodetool("compatstatus");
            tool.assertOnCleanExit();
            assertThat(tool.getStdout().split("\\R"))
            .containsSequence("DSE peers                    : 2",
                              String.format("  %-45s %s", "127.0.0.44:7000",
                                            "release 4.0.0.6865, NET_VERSION 1024, DSE 6.8.65, gossip ordinals: DSE's (DSE-flagged first handshake message)"),
                              String.format("  %-45s %s", "127.0.0.47:7000",
                                            "NET_VERSION unknown, gossip ordinals: DSE's (handshake reply with the DSE messaging version 1536)"));
        }
        finally
        {
            DsePeers.reset();
            DatabaseDescriptor.setInternodeCompatibilityMode(modeBefore);
            Gossiper.instance.endpointStateMap.remove(dsePeer);
            StorageService.instance.getTokenMetadata().removeEndpoint(dsePeer);
        }
    }

    /**
     * The attribute itself: empty without the mode; in DSE_6 mode a DSE node gossiping its DSE version is listed even
     * without a release or a DSE version in its {@code DSE_GOSSIP_STATE}, while the DSE nodes that left the ring
     * (decommissioned or removed) and the nodes gossiping a version below 256 are not, identified or not.
     */
    @Test
    public void dsePeersAttributeLeavesOutNodesThatLeftTheRing() throws Exception
    {
        InetAddressAndPort bare = InetAddressAndPort.getByName("127.0.0.48:7000");
        InetAddressAndPort left = InetAddressAndPort.getByName("127.0.0.49:7000");
        InetAddressAndPort removed = InetAddressAndPort.getByName("127.0.0.50:7000");
        InetAddressAndPort hcd = InetAddressAndPort.getByName("127.0.0.51:7000");
        gossip(bare, Map.of(ApplicationState.NET_VERSION, "1536", ApplicationState.X_11_PADDING, "{\"workload\":\"Cassandra\"}"));
        gossip(left, Map.of(ApplicationState.NET_VERSION, "1536", ApplicationState.STATUS_WITH_PORT, VersionedValue.STATUS_LEFT + ",0,0"));
        gossip(removed, Map.of(ApplicationState.NET_VERSION, "1024", ApplicationState.STATUS_WITH_PORT, VersionedValue.REMOVED_TOKEN + ",0,0"));
        gossip(hcd, Map.of(ApplicationState.NET_VERSION, String.valueOf(MessagingService.VERSION_3014)));
        InternodeCompatibilityMode modeBefore = DatabaseDescriptor.getInternodeCompatibilityMode();
        try
        {
            DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
            DsePeers.identify(left, "DSE-flagged first handshake message");
            DsePeers.identify(removed, "DSE-flagged first handshake message");
            assertThat(MessagingService.instance().getDsePeers())
            .containsExactlyInAnyOrderEntriesOf(Map.of("127.0.0.48:7000", "release unknown, NET_VERSION 1536, gossip ordinals: this software's (not identified as DSE yet)"));

            DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
            assertThat(MessagingService.instance().getDsePeers()).isEmpty();
        }
        finally
        {
            DsePeers.reset();
            DatabaseDescriptor.setInternodeCompatibilityMode(modeBefore);
            for (InetAddressAndPort peer : new InetAddressAndPort[]{ bare, left, removed, hcd })
            {
                Gossiper.instance.endpointStateMap.remove(peer);
                StorageService.instance.getTokenMetadata().removeEndpoint(peer);
            }
        }
    }

    /** A gossip state of {@code peer} with the given values, for a ring member (gossip evicts an endpoint without tokens). */
    private static void gossip(InetAddressAndPort peer, Map<ApplicationState, String> values)
    {
        Map<ApplicationState, VersionedValue> states = new EnumMap<>(ApplicationState.class);
        int version = 1;
        for (Map.Entry<ApplicationState, String> e : values.entrySet())
            states.put(e.getKey(), VersionedValue.unsafeMakeVersionedValue(e.getValue(), version++));
        StorageService.instance.getTokenMetadata().updateNormalToken(DatabaseDescriptor.getPartitioner().getRandomToken(), peer);
        Gossiper.instance.endpointStateMap.put(peer, new EndpointState(new HeartBeatState(1, 1), states));
    }

    /** Registers the local endpoint unless it is known, so that removing the peers afterwards recomputes minClusterVersion from it. */
    private static boolean knowLocalEndpoint()
    {
        EndpointMessagingVersions versions = MessagingService.instance().versions;
        InetAddressAndPort local = FBUtilities.getBroadcastAddressAndPort();
        boolean localKnown = versions.knows(local);
        if (!localKnown)
            versions.set(local, MessagingService.current_version);
        return localKnown;
    }

    private static void forgetLocalEndpoint(boolean localKnown)
    {
        if (!localKnown)
            MessagingService.instance().versions.reset(FBUtilities.getBroadcastAddressAndPort());
    }
}
