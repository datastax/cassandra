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
import java.util.List;
import java.util.Map;
import javax.management.AttributeNotFoundException;

import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.DseCompatRefusals;
import org.apache.cassandra.net.EndpointMessagingVersions;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.Output;
import org.apache.cassandra.tools.ToolRunner;
import org.apache.cassandra.utils.FBUtilities;

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
            // NONE: nothing was refused
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
            // the peer versions, then the refusals
            String out = tool.getStdout();
            assertThat(out.indexOf("Peers with a known version")).isNotNegative()
                                                                  .isLessThan(out.indexOf("Requests refused (upgrade)"));
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
    public void aDseNodeKnownFromGossipOnlyWithoutTheMode() throws Exception
    {
        InetAddressAndPort dseNode = InetAddressAndPort.getByName("127.0.0.46:7000");
        boolean localKnown = knowLocalEndpoint();
        InternodeCompatibilityMode modeBefore = DatabaseDescriptor.getInternodeCompatibilityMode();
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
        try
        {
            // DSE's own gossip value of a DSE node gossiped by an upgraded peer is stored as it is
            MessagingService.instance().versions.set(dseNode, 1536);
            ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("compatstatus");
            tool.assertOnCleanExit();
            List<String> lines = Arrays.asList(tool.getStdout().split("\\R"));
            assertThat(lines).anyMatch(line -> line.matches("Peers with a known version   : \\d+ \\(0 speaking the pre-4.0 dialect, 1 DSE 6 node\\(s\\) known from gossip only\\)"))
                             .contains(String.format("  %-45s %d  (DSE 6 node known from gossip only)", "127.0.0.46:7000", 1536));
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
        Mockito.when(probe.getDseCompatRefusals()).thenThrow(new UndeclaredThrowableException(new AttributeNotFoundException("DseCompatRefusals")));

        new CompatStatus().execute(probe);

        assertThat(Arrays.asList(bytes.toString(StandardCharsets.UTF_8).split("\\R")))
        .containsExactly("Internode compatibility mode : DSE_6",
                         "Storage compatibility mode   : NONE",
                         "SSTable format written       : bti-da",
                         "Peers with a known version   : 1 (1 speaking the pre-4.0 dialect)",
                         String.format("  %-45s %d  (pre-4.0 dialect)", "127.0.0.45:7000", MessagingService.VERSION_3014),
                         "Requests refused (upgrade)   : unknown (this node does not report them)");

        // any other failure is not hidden
        UndeclaredThrowableException other = new UndeclaredThrowableException(new IOException("connection lost"));
        Mockito.doThrow(other).when(probe).getDseCompatRefusals();
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
