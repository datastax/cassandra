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

    private static String value(EndpointState state, ApplicationState name)
    {
        VersionedValue value = state.getApplicationState(name);
        assertThat(value).as(name.name()).isNotNull();
        return value.value;
    }
}
