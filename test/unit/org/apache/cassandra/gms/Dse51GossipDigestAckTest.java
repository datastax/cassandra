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

package org.apache.cassandra.gms;

import java.util.Map;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.dsecompat.Dse51Fixtures;
import org.apache.cassandra.dsecompat.DseFixtures;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.DsePeers;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.nodes.DseNodeWorkloads;
import org.apache.cassandra.utils.JsonUtils;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The {@code GOSSIP_DIGEST_ACK} a DSE 5.1.51 node sent ({@link Dse51Fixtures#messages}), decoded at version 11 in the
 * DSE 5.1 compatibility mode: the states of the three nodes come out with this software's ordinals, as DSE 5.1 has
 * Apache Cassandra 3.11's, each with the release {@code 3.11.3.5151}, the {@code NET_VERSION} 11 and DSE's state, at
 * the ordinal of {@code X_11_PADDING}, naming {@code dse_version} 5.1.51.
 */
public class Dse51GossipDigestAckTest
{
    private static final Dse51Fixtures DSE51 = DseFixtures.dse51();

    private InternodeCompatibilityMode modeBefore;

    @BeforeClass
    public static void init()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void dse51Mode()
    {
        modeBefore = DatabaseDescriptor.getInternodeCompatibilityMode();
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_5_1);
    }

    @After
    public void restore()
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(modeBefore);
    }

    @Test
    public void theCapturedAckIsDecodedWithThisSoftwaresOrdinals() throws Exception
    {
        Dse51Fixtures.Message captured = DSE51.messages("gossip-digest-ack").get(0);
        InetAddressAndPort sender = InetAddressAndPort.getByName(captured.from);
        Message<GossipDigestAck> message;
        try (DataInputBuffer in = new DataInputBuffer(captured.bytes))
        {
            message = Message.serializer.deserialize(in, sender, MessagingService.VERSION_3014);
            assertThat(in.available()).isZero();
        }
        assertThat(message.verb()).isEqualTo(Verb.GOSSIP_DIGEST_ACK);

        Map<InetAddressAndPort, EndpointState> states = message.payload.getEndpointStateMap(sender);
        assertThat(states.keySet().stream().map(a -> a.getHostAddress(false)).collect(Collectors.toSet()))
        .containsExactlyInAnyOrder("127.0.51.11", "127.0.51.12", "127.0.51.13");
        for (Map.Entry<InetAddressAndPort, EndpointState> e : states.entrySet())
        {
            EndpointState state = e.getValue();
            String node = e.getKey().toString();
            assertThat(value(state, ApplicationState.RELEASE_VERSION)).as(node)
                                                                      .isEqualTo(Dse51Fixtures.RELEASE_VERSION);
            assertThat(value(state, ApplicationState.NET_VERSION)).as(node).isEqualTo("11");
            assertThat(DsePeers.gossipedDseRelease(state)).as(node).isEqualTo(Dse51Fixtures.DSE_VERSION);
            assertThat(DsePeers.gossipsDseVersion(state)).as(node).isTrue();
        }
    }

    /**
     * The DSE state each of the three nodes gossips in the captured ack, at DSE 5.1's ordinal 9: the JSON with
     * {@code active} a string ("true", or "false" for the node that had just restarted) and the {@code Cassandra}
     * workload, read by {@link Dse51GossipStates}; the ack carries no state at an ordinal above DSE 5.1's last, 24.
     */
    @Test
    public void theCapturedAckCarriesTheDseStateOfEachNodeAtOrdinal9() throws Exception
    {
        Dse51Fixtures.Message captured = DSE51.messages("gossip-digest-ack").get(0);
        InetAddressAndPort sender = InetAddressAndPort.getByName(captured.from);
        Message<GossipDigestAck> message;
        try (DataInputBuffer in = new DataInputBuffer(captured.bytes))
        {
            message = Message.serializer.deserialize(in, sender, MessagingService.VERSION_3014);
        }
        Map<InetAddressAndPort, EndpointState> states = message.payload.getEndpointStateMap(sender);
        assertThat(states).hasSize(3);
        // DSE 5.1 keeps its state at ordinal 9, this software's X_11_PADDING
        assertThat(ApplicationState.X_11_PADDING.ordinal()).isEqualTo(9);
        for (Map.Entry<InetAddressAndPort, EndpointState> e : states.entrySet())
        {
            String node = e.getKey().toString();
            EndpointState state = e.getValue();
            assertThat(state.states()).as(node).allMatch(s -> s.getKey().ordinal() <= Dse51GossipStates.DSE51_MAX_ORDINAL);
            Map<String, Object> dseState = JsonUtils.fromJsonMap(value(state, ApplicationState.X_11_PADDING));
            // active is a JSON string, "false" for node 3, which had just restarted when node 1 sent the ack
            assertThat(dseState.get("active")).as(node).isEqualTo(e.getKey().getHostAddress(false).equals("127.0.51.13") ? "false" : "true");
            assertThat(dseState).as(node).containsEntry("dse_version", Dse51Fixtures.DSE_VERSION)
                                         .containsEntry("graph", false)
                                         .containsEntry("workloads", "Cassandra");
            DseNodeWorkloads workloads = Dse51GossipStates.gossipedWorkloads(e.getKey(), state);
            assertThat(workloads.workloads).as(node).containsExactly("Cassandra");
            assertThat(workloads.workload).as(node).isEqualTo("Cassandra");
            assertThat(workloads.graph).as(node).isFalse();
        }
    }

    private static String value(EndpointState state, ApplicationState name)
    {
        VersionedValue value = state.getApplicationState(name);
        assertThat(value).as(name.name()).isNotNull();
        return value.value;
    }
}
