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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.DseCompat;
import org.apache.cassandra.net.DsePeers;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JsonUtils;

import static org.apache.cassandra.config.CassandraRelevantProperties.GOSSIP_DISABLE_THREAD_VALIDATION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

/**
 * The gossip states exchanged at the pre-4.0 dialect use DSE's application-state ordinals only with the peers identified
 * as DSE 6 nodes ({@link DsePeers}): HCD 1.2 at version 11 exchanges the 4.0 line's ordinals, and so does this node with
 * it. The identification by handshake is covered by {@code DseHandshakeTest}.
 */
public class DsePeerGossipTest
{
    private static final int DIALECT = DseCompat.DSE_DIALECT;
    private static final List<Token> TOKENS = Collections.singletonList(Murmur3Partitioner.instance.getToken(ByteBufferUtil.bytes("k")));

    // DSE's ordinals that differ from this software's
    private static final int DSE_NATIVE_TRANSPORT_PORT = 15;
    private static final int DSE_STORAGE_PORT = 17;
    // DSE's ordinal that is the same here (this software's X_11_PADDING)
    private static final int DSE_GOSSIP_STATE = 9;

    // the pre-4.0 layout carries no port: endpoints are read back with the default storage port, known once the
    // configuration is loaded
    private static InetAddressAndPort dsePeer;
    private static InetAddressAndPort hcdPeer;
    private static InetAddressAndPort relayed;
    private static InetAddressAndPort subject;

    private InternodeCompatibilityMode modeBefore;

    private static InetAddressAndPort address(String address)
    {
        try
        {
            return InetAddressAndPort.getByAddress(InetAddressAndPort.getByName(address).getAddress());
        }
        catch (Exception e)
        {
            throw new AssertionError(e);
        }
    }

    @BeforeClass
    public static void startup()
    {
        DatabaseDescriptor.daemonInitialization();
        dsePeer = address("127.0.0.211");
        hcdPeer = address("127.0.0.212");
        relayed = address("127.0.0.213");
        subject = address("127.0.0.214");
        // the gossiper checks that state changes run on the gossip stage; the property is read when the class initialises
        try (WithProperties properties = new WithProperties().set(GOSSIP_DISABLE_THREAD_VALIDATION, true))
        {
            Gossiper.instance.getClass();
        }
    }

    @Before
    public void dse6()
    {
        modeBefore = DatabaseDescriptor.getInternodeCompatibilityMode();
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        DsePeers.reset();
    }

    @After
    public void restore()
    {
        DsePeers.reset();
        DatabaseDescriptor.setInternodeCompatibilityMode(modeBefore);
    }

    /** The state this node gossips about an endpoint of this software. */
    private static EndpointState ourState() throws Exception
    {
        VersionedValue.VersionedValueFactory factory = new VersionedValue.VersionedValueFactory(Murmur3Partitioner.instance);
        Map<ApplicationState, VersionedValue> states = new EnumMap<>(ApplicationState.class);
        states.put(ApplicationState.STATUS_WITH_PORT, factory.normal(TOKENS));
        states.put(ApplicationState.NATIVE_ADDRESS_AND_PORT, factory.nativeaddressAndPort(InetAddressAndPort.getByName("10.0.0.5:9042")));
        states.put(ApplicationState.INTERNAL_ADDRESS_AND_PORT, factory.internalAddressAndPort(InetAddressAndPort.getByName("10.0.0.5:7000")));
        states.put(ApplicationState.SSTABLE_VERSIONS, VersionedValue.unsafeMakeVersionedValue("bti-da", 4));
        states.put(ApplicationState.NET_VERSION, factory.networkVersion());
        states.put(ApplicationState.DC, factory.datacenter("dc1"));
        states.put(ApplicationState.RELEASE_VERSION, factory.releaseVersion("5.0.7.0-abc1234def"));
        return new EndpointState(new HeartBeatState(7, 8), states);
    }

    /**
     * {@link #ourState()} by ordinal as a peer at the dialect that is not a DSE node reads it: this software's ordinals,
     * only the release version without the suffix a 3.x parser rejects, as before.
     */
    private static Map<Integer, String> oursAtTheDialect() throws Exception
    {
        Map<Integer, String> values = byOrdinal(ourState());
        values.put(ApplicationState.RELEASE_VERSION.ordinal(), "5.0.7.0");
        return values;
    }

    /** A state as its sender wrote it: {@code ordinal, value} pairs keyed by the constants of this software with those ordinals. */
    private static EndpointState raw(Object... ordinalsAndValues)
    {
        Map<ApplicationState, VersionedValue> states = new EnumMap<>(ApplicationState.class);
        for (int i = 0; i < ordinalsAndValues.length; i += 2)
            states.put(ApplicationState.values()[(Integer) ordinalsAndValues[i]], VersionedValue.unsafeMakeVersionedValue((String) ordinalsAndValues[i + 1], 1));
        return new EndpointState(new HeartBeatState(5, 6), states);
    }

    private static Map<Integer, String> byOrdinal(EndpointState state)
    {
        Map<Integer, String> values = new HashMap<>();
        for (Map.Entry<ApplicationState, VersionedValue> e : state.states())
            values.put(e.getKey().ordinal(), e.getValue().value);
        return values;
    }

    private static <T> byte[] write(IVersionedSerializer<T> serializer, T message, int version) throws IOException
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            serializer.serialize(message, out, version);
            assertThat((long) out.getLength()).as("serialized size").isEqualTo(serializer.serializedSize(message, version));
            return out.toByteArray();
        }
    }

    private static <T> T read(IVersionedSerializer<T> serializer, byte[] bytes, int version) throws IOException
    {
        try (DataInputBuffer in = new DataInputBuffer(bytes))
        {
            return serializer.deserialize(in, version);
        }
    }

    /** The state of {@link #subject} as the recipient of an ack and of an ack2 reads it, by the ordinals on the wire. */
    private static List<Map<Integer, String>> sentTo(InetAddressAndPort recipient, int version) throws Exception
    {
        return sentTo(recipient, version, () -> {});
    }

    /** As {@link #sentTo(InetAddressAndPort, int)}; {@code before} runs before each of the two messages is written. */
    private static List<Map<Integer, String>> sentTo(InetAddressAndPort recipient, int version, Runnable before) throws Exception
    {
        Map<InetAddressAndPort, EndpointState> states = Collections.singletonMap(subject, ourState());
        before.run();
        GossipDigestAck ack = new GossipDigestAck(Collections.emptyList(), states, recipient);
        GossipDigestAck ackRead = read(GossipDigestAck.serializer, write(GossipDigestAck.serializer, ack, version), version);
        before.run();
        GossipDigestAck2 ack2 = new GossipDigestAck2(states, recipient);
        GossipDigestAck2 ack2Read = read(GossipDigestAck2.serializer, write(GossipDigestAck2.serializer, ack2, version), version);
        return List.of(byOrdinal(ackRead.epStateMap.get(subject)), byOrdinal(ack2Read.epStateMap.get(subject)));
    }

    @Test
    public void statesSentToADsePeerHaveDseOrdinalsAndToAnotherPeerAtTheDialectOurs() throws Exception
    {
        DsePeers.identify(dsePeer, "test");
        Map<Integer, String> ours = byOrdinal(ourState());

        for (Map<Integer, String> sent : sentTo(dsePeer, DIALECT))
        {
            assertThat(sent).containsEntry(ApplicationState.STATUS.ordinal(), "NORMAL," + TOKENS.get(0))
                            .containsEntry(ApplicationState.RPC_ADDRESS.ordinal(), "10.0.0.5")
                            .containsEntry(DSE_NATIVE_TRANSPORT_PORT, "9042")
                            .containsEntry(ApplicationState.INTERNAL_IP.ordinal(), "10.0.0.5")
                            .containsEntry(DSE_STORAGE_PORT, "7000")
                            .containsEntry(ApplicationState.NET_VERSION.ordinal(), "12")
                            .containsEntry(ApplicationState.DC.ordinal(), "dc1")
                            .containsEntry(ApplicationState.RELEASE_VERSION.ordinal(), "5.0.7.0")
                            .hasSize(8);
        }
        // HCD 1.2 at the dialect, never identified: the states with this software's ordinals
        for (Map<Integer, String> sent : sentTo(hcdPeer, DIALECT))
            assertThat(sent).isEqualTo(oursAtTheDialect());
        // at 4.0 and later the states go as they are, even to an identified DSE peer
        for (Map<Integer, String> sent : sentTo(dsePeer, MessagingService.current_version))
            assertThat(sent).isEqualTo(ours);
    }

    @Test
    public void theOrdinalsOfAMessageAreDecidedOnceSoItsSizeAndBytesAgree() throws Exception
    {
        Map<InetAddressAndPort, EndpointState> states = Collections.singletonMap(subject, ourState());

        // sized for an identified DSE peer, then written after the record is gone: DSE's ordinals, the size matches
        DsePeers.identify(dsePeer, "test");
        GossipDigestAck2 toDse = new GossipDigestAck2(states, dsePeer);
        long size = GossipDigestAck2.serializer.serializedSize(toDse, DIALECT);
        DsePeers.reset();
        byte[] bytes;
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            GossipDigestAck2.serializer.serialize(toDse, out, DIALECT);
            bytes = out.toByteArray();
        }
        assertThat((long) bytes.length).isEqualTo(size);
        assertThat(byOrdinal(read(GossipDigestAck2.serializer, bytes, DIALECT).epStateMap.get(subject))).containsEntry(DSE_STORAGE_PORT, "7000");

        // sized before the peer is identified: this software's ordinals, also once it is
        GossipDigestAck2 beforeIdentification = new GossipDigestAck2(states, dsePeer);
        size = GossipDigestAck2.serializer.serializedSize(beforeIdentification, DIALECT);
        DsePeers.identify(dsePeer, "test");
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            GossipDigestAck2.serializer.serialize(beforeIdentification, out, DIALECT);
            bytes = out.toByteArray();
        }
        assertThat((long) bytes.length).isEqualTo(size);
        assertThat(byOrdinal(read(GossipDigestAck2.serializer, bytes, DIALECT).epStateMap.get(subject))).isEqualTo(oursAtTheDialect());
    }

    /** An ack carrying {@code states}, as its sender writes it at the dialect, read back: not translated yet. */
    private static GossipDigestAck receivedAck(Map<InetAddressAndPort, EndpointState> states) throws IOException
    {
        // written without a recipient: the states go on the wire with the ordinals they are keyed by, as the sender wrote them
        GossipDigestAck ack = read(GossipDigestAck.serializer, write(GossipDigestAck.serializer, new GossipDigestAck(Collections.emptyList(), states), DIALECT), DIALECT);
        assertThat(ack.readInDialect).isTrue();
        return ack;
    }

    /** As {@link #receivedAck}, for an ack2. */
    private static GossipDigestAck2 receivedAck2(Map<InetAddressAndPort, EndpointState> states) throws IOException
    {
        GossipDigestAck2 ack2 = read(GossipDigestAck2.serializer, write(GossipDigestAck2.serializer, new GossipDigestAck2(states), DIALECT), DIALECT);
        assertThat(ack2.readInDialect).isTrue();
        return ack2;
    }

    /**
     * The states of an ack and of an ack2 carrying {@code states} from {@code sender}, translated as their verb handlers
     * do; {@code before} runs before each of the two translations (to reset what the other one identified).
     */
    private static List<Map<InetAddressAndPort, EndpointState>> receivedFrom(InetAddressAndPort sender, Map<InetAddressAndPort, EndpointState> states,
                                                                             Runnable before) throws IOException
    {
        GossipDigestAck ack = receivedAck(states);
        GossipDigestAck2 ack2 = receivedAck2(states);
        before.run();
        Map<InetAddressAndPort, EndpointState> fromAck = ack.getEndpointStateMap(sender);
        before.run();
        Map<InetAddressAndPort, EndpointState> fromAck2 = ack2.getEndpointStateMap(sender);
        return List.of(fromAck, fromAck2);
    }

    private static List<Map<InetAddressAndPort, EndpointState>> receivedFrom(InetAddressAndPort sender, Map<InetAddressAndPort, EndpointState> states) throws IOException
    {
        return receivedFrom(sender, states, () -> {});
    }

    private static void assertTranslatedFromDse(Map<InetAddressAndPort, EndpointState> received)
    {
        assertThat(received.get(relayed).getApplicationState(DseGossipStates.STORAGE_PORT_SLOT).value).isEqualTo("7000");
        assertThat(received.get(relayed).getApplicationState(ApplicationState.STATUS_WITH_PORT)).isNull();
    }

    @Test
    public void statesReceivedFromADsePeerAreTranslatedAndFromAnotherPeerAtTheDialectNot() throws Exception
    {
        // a DSE node sends its own state, with its NET_VERSION, and relays another one: identified by each message itself
        Map<InetAddressAndPort, EndpointState> fromDse = new HashMap<>();
        fromDse.put(dsePeer, raw(ApplicationState.NET_VERSION.ordinal(), "1536",
                                 ApplicationState.RPC_ADDRESS.ordinal(), "10.0.0.8",
                                 DSE_NATIVE_TRANSPORT_PORT, "9042"));
        fromDse.put(relayed, raw(DSE_STORAGE_PORT, "7000"));
        for (Map<InetAddressAndPort, EndpointState> received : receivedFrom(dsePeer, fromDse, DsePeers::reset))
        {
            assertThat(DsePeers.isDse(dsePeer)).isTrue();
            assertThat(received.get(dsePeer).getApplicationState(DseGossipStates.NATIVE_TRANSPORT_PORT_SLOT).value).isEqualTo("9042");
            assertThat(received.get(dsePeer).getApplicationState(ApplicationState.INTERNAL_ADDRESS_AND_PORT)).isNull();
            assertThat(received.get(dsePeer).getApplicationState(ApplicationState.RPC_ADDRESS).value).isEqualTo("10.0.0.8");
            assertTranslatedFromDse(received);
        }

        // HCD 1.2 at the dialect sends its 4.0 ordinals: kept as they are, and it is not identified
        EndpointState hcd = raw(ApplicationState.NET_VERSION.ordinal(), "11",
                                ApplicationState.STATUS_WITH_PORT.ordinal(), "NORMAL," + TOKENS.get(0),
                                ApplicationState.INTERNAL_ADDRESS_AND_PORT.ordinal(), "10.0.0.9:7000");
        for (Map<InetAddressAndPort, EndpointState> received : receivedFrom(hcdPeer, Collections.singletonMap(hcdPeer, hcd)))
        {
            assertThat(DsePeers.isDse(hcdPeer)).isFalse();
            assertThat(byOrdinal(received.get(hcdPeer))).isEqualTo(byOrdinal(hcd));
        }

        // at 4.0 and later nothing is translated, whoever the sender is
        GossipDigestAck2 at40 = read(GossipDigestAck2.serializer, write(GossipDigestAck2.serializer, new GossipDigestAck2(fromDse), MessagingService.current_version),
                                     MessagingService.current_version);
        assertThat(at40.readInDialect).isFalse();
        assertThat(byOrdinal(at40.getEndpointStateMap(dsePeer).get(relayed))).containsEntry(DSE_STORAGE_PORT, "7000");
    }

    @Test
    public void identificationSurvivesMessagesWithoutTheSendersStateUntilGossipSaysOtherwise() throws Exception
    {
        // identified by a handshake: a later message relaying another endpoint only is still translated
        DsePeers.identify(dsePeer, "test");
        for (Map<InetAddressAndPort, EndpointState> received : receivedFrom(dsePeer, Collections.singletonMap(relayed, raw(DSE_STORAGE_PORT, "7000"))))
            assertTranslatedFromDse(received);

        // the node was upgraded to this software and gossips 12: not a DSE node any more, its message is not translated
        Map<InetAddressAndPort, EndpointState> upgraded = new HashMap<>();
        upgraded.put(dsePeer, raw(ApplicationState.NET_VERSION.ordinal(), "12"));
        upgraded.put(relayed, raw(ApplicationState.STATUS_WITH_PORT.ordinal(), "NORMAL," + TOKENS.get(0)));
        for (Map<InetAddressAndPort, EndpointState> received : receivedFrom(dsePeer, upgraded))
        {
            assertThat(DsePeers.isDse(dsePeer)).isFalse();
            assertThat(received.get(relayed).getApplicationState(ApplicationState.STATUS_WITH_PORT).value).isEqualTo("NORMAL," + TOKENS.get(0));
        }
    }

    /** Stores {@code state} for {@code endpoint} as the gossiper keeps a state relayed by another node, without its side effects. */
    private static void storedInGossip(InetAddressAndPort endpoint, EndpointState state)
    {
        Gossiper.instance.endpointStateMap.put(endpoint, state);
    }

    /** The stored state of a DSE node that is not a ring member: its NET_VERSION only. */
    private static EndpointState dseNodeState(String netVersion)
    {
        Map<ApplicationState, VersionedValue> states = new EnumMap<>(ApplicationState.class);
        states.put(ApplicationState.NET_VERSION, VersionedValue.unsafeMakeVersionedValue(netVersion, 2));
        return new EndpointState(new HeartBeatState((int) FBUtilities.now().getEpochSecond(), 3), states);
    }

    /**
     * A DSE node first heard of through another node, whose state this node stores (and whose NET_VERSION the storage
     * service did not look at: it is not subscribed to gossip here, as for a node that is not a ring member), is
     * identified by that stored NET_VERSION the first time gossip is exchanged with it: a message it sends without its
     * own state is translated, and a message sent to it has DSE's ordinals.
     */
    @Test
    public void anEndpointWhoseStoredStateHasADseNetVersionIsIdentifiedWhenGossipIsExchanged() throws Exception
    {
        try
        {
            storedInGossip(dsePeer, dseNodeState("1536"));
            assertThat(DsePeers.isDse(dsePeer)).isFalse();
            for (Map<InetAddressAndPort, EndpointState> received : receivedFrom(dsePeer, Collections.singletonMap(relayed, raw(DSE_STORAGE_PORT, "7000")), DsePeers::reset))
            {
                assertThat(DsePeers.isDse(dsePeer)).isTrue();
                assertTranslatedFromDse(received);
            }

            for (Map<Integer, String> sent : sentTo(dsePeer, DIALECT, DsePeers::reset))
                assertThat(sent).containsEntry(DSE_STORAGE_PORT, "7000").containsEntry(ApplicationState.STATUS.ordinal(), "NORMAL," + TOKENS.get(0));
            assertThat(DsePeers.isDse(dsePeer)).isTrue();
        }
        finally
        {
            Gossiper.instance.endpointStateMap.remove(dsePeer);
        }
    }

    /**
     * A stored NET_VERSION below 256 does not clear an identification by handshake: it may be the one of the node's
     * previous life (a node of this software rolled back to DSE on the same address). Only a NET_VERSION the node
     * gossips again clears it.
     */
    @Test
    public void aStoredNetVersionNeverClearsAnIdentification() throws Exception
    {
        try
        {
            storedInGossip(dsePeer, dseNodeState("12"));
            DsePeers.identify(dsePeer, "DSE-flagged first handshake message");
            for (Map<InetAddressAndPort, EndpointState> received : receivedFrom(dsePeer, Collections.singletonMap(relayed, raw(DSE_STORAGE_PORT, "7000"))))
                assertTranslatedFromDse(received);
            for (Map<Integer, String> sent : sentTo(dsePeer, DIALECT))
                assertThat(sent).containsEntry(DSE_STORAGE_PORT, "7000");
            assertThat(DsePeers.isDse(dsePeer)).isTrue();
        }
        finally
        {
            Gossiper.instance.endpointStateMap.remove(dsePeer);
        }
    }

    /**
     * Without the mode, the acks bound to a recipient, the answer to a shadow round included, are written exactly as the
     * acks of upstream (no recipient), byte for byte and with the same size, at every version of the 4.0 line and later,
     * whatever was recorded about the recipient while the mode was on; a received ack gives the states it read, as they
     * are.
     */
    @Test
    public void withoutTheModeTheAcksAreWrittenAndReadAsBefore() throws Exception
    {
        DsePeers.identify(dsePeer, "DSE-flagged first handshake message");
        DsePeers.presume(hcdPeer, "stored release 4.0.0.6926");
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
        Map<InetAddressAndPort, EndpointState> states = new HashMap<>();
        states.put(subject, ourState());
        states.put(relayed, raw(ApplicationState.DC.ordinal(), "dc2", DSE_STORAGE_PORT, "7000"));
        List<GossipDigest> digests = Collections.singletonList(new GossipDigest(subject, 7, 8));
        for (int version : new int[]{ MessagingService.VERSION_40, MessagingService.VERSION_50, MessagingService.VERSION_DS_10, MessagingService.VERSION_DS_11,
                                      MessagingService.VERSION_DS_12, MessagingService.VERSION_DS_20, MessagingService.current_version })
        {
            for (InetAddressAndPort to : new InetAddressAndPort[]{ dsePeer, hcdPeer, subject })
            {
                byte[] upstream = write(GossipDigestAck.serializer, new GossipDigestAck(digests, states), version);
                assertThat(write(GossipDigestAck.serializer, new GossipDigestAck(digests, states, to), version)).as("ack at %d", version)
                                                                                                                 .isEqualTo(upstream);
                byte[] upstreamShadow = write(GossipDigestAck.serializer, new GossipDigestAck(Collections.emptyList(), states), version);
                assertThat(write(GossipDigestAck.serializer, GossipDigestAck.shadowRoundAnswer(states, to), version)).as("shadow round answer at %d", version)
                                                                                                                       .isEqualTo(upstreamShadow);
                byte[] upstreamAck2 = write(GossipDigestAck2.serializer, new GossipDigestAck2(states), version);
                assertThat(write(GossipDigestAck2.serializer, new GossipDigestAck2(states, to), version)).as("ack2 at %d", version)
                                                                                                        .isEqualTo(upstreamAck2);

                GossipDigestAck ack = read(GossipDigestAck.serializer, upstream, version);
                assertThat(ack.getEndpointStateMap(to)).isSameAs(ack.epStateMap);
                GossipDigestAck2 ack2 = read(GossipDigestAck2.serializer, upstreamAck2, version);
                assertThat(ack2.getEndpointStateMap(to)).isSameAs(ack2.epStateMap);
                assertThat(byOrdinal(ack2.epStateMap.get(relayed))).isEqualTo(byOrdinal(states.get(relayed)));
            }
        }
    }

    /**
     * From a sender not identified as a DSE node, the state of an endpoint with a bare number at any of the ordinals 15
     * to 20, or with any value at 21, is a DSE node's (ports, JMX port, schema compatibility version, disk usage): it is
     * left out whole, never stored in this software's address, status, sstable versions, disk usage, index status or
     * padding.
     */
    @Test
    public void dseStatesFromAnUnidentifiedSenderAreLeftOut() throws Exception
    {
        Object[][] dseValues = { { DSE_NATIVE_TRANSPORT_PORT, "9042" }, { 16, "9142" }, { DSE_STORAGE_PORT, "7000" },
                                 { 18, "7001" }, { 19, "7199" }, { 20, "2" }, { 21, "SPACIOUS" } };
        for (Object[] dseValue : dseValues)
        {
            Map<InetAddressAndPort, EndpointState> states = new HashMap<>();
            states.put(relayed, raw(dseValue[0], dseValue[1], ApplicationState.DC.ordinal(), "dc1"));
            states.put(subject, raw(ApplicationState.DC.ordinal(), "dc2"));
            for (Map<InetAddressAndPort, EndpointState> received : receivedFrom(hcdPeer, states))
            {
                assertThat(DsePeers.isDse(hcdPeer)).isFalse();
                assertThat(received).as("ordinal " + dseValue[0]).containsOnlyKeys(subject);
            }
            // the sender's own state, without its NET_VERSION: a DSE node not identified yet, left out as well
            Map<InetAddressAndPort, EndpointState> own = new HashMap<>();
            own.put(dsePeer, raw(dseValue[0], dseValue[1], ApplicationState.DC.ordinal(), "dc1"));
            own.put(subject, raw(ApplicationState.DC.ordinal(), "dc2"));
            for (Map<InetAddressAndPort, EndpointState> received : receivedFrom(dsePeer, own))
            {
                assertThat(DsePeers.isDse(dsePeer)).isFalse();
                assertThat(received).as("own state, ordinal " + dseValue[0]).containsOnlyKeys(subject);
            }
        }
        // real addresses, statuses, sstable versions, disk usage and index status are kept
        EndpointState hcd = raw(ApplicationState.INTERNAL_ADDRESS_AND_PORT.ordinal(), "10.0.0.9:7000",
                                ApplicationState.NATIVE_ADDRESS_AND_PORT.ordinal(), "10.0.0.9:9042",
                                ApplicationState.STATUS_WITH_PORT.ordinal(), "NORMAL," + TOKENS.get(0),
                                ApplicationState.SSTABLE_VERSIONS.ordinal(), "big-nb",
                                ApplicationState.DISK_USAGE.ordinal(), "SPACIOUS",
                                ApplicationState.INDEX_STATUS.ordinal(), "{}");
        for (Map<InetAddressAndPort, EndpointState> received : receivedFrom(hcdPeer, Collections.singletonMap(relayed, hcd)))
            assertThat(byOrdinal(received.get(relayed))).isEqualTo(byOrdinal(hcd));
    }

    /**
     * A peer presumed to be a DSE node (stored with a DSE 6 release, before any evidence) is sent acks without any
     * endpoint state, only digests, at the dialect; what it sends is read as from a peer not identified. Its own state
     * gossiping 12 clears the presumption; DSE evidence turns it into a record.
     */
    @Test
    public void aPresumedDsePeerIsSentNoEndpointStateUntilItsGossipSettlesIt() throws Exception
    {
        DsePeers.presume(dsePeer, "stored release 4.0.0.6926");
        assertThat(DsePeers.isDse(dsePeer)).isFalse();
        assertThat(DsePeers.isPresumed(dsePeer)).isTrue();
        Map<InetAddressAndPort, EndpointState> states = Collections.singletonMap(subject, ourState());
        List<GossipDigest> digests = Collections.singletonList(new GossipDigest(subject, 7, 8));
        GossipDigestAck ack = read(GossipDigestAck.serializer, write(GossipDigestAck.serializer, new GossipDigestAck(digests, states, dsePeer), DIALECT), DIALECT);
        assertThat(ack.epStateMap).isEmpty();
        assertThat(ack.gDigestList).hasSize(1);
        assertThat(ack.gDigestList.get(0).endpoint).isEqualTo(subject);
        GossipDigestAck2 ack2 = read(GossipDigestAck2.serializer, write(GossipDigestAck2.serializer, new GossipDigestAck2(states, dsePeer), DIALECT), DIALECT);
        assertThat(ack2.epStateMap).isEmpty();
        // at 4.0 and later the states go as they are
        for (Map<Integer, String> sent : sentTo(dsePeer, MessagingService.current_version))
            assertThat(sent).isEqualTo(byOrdinal(ourState()));

        // a relayed DSE state is left out, not translated
        for (Map<InetAddressAndPort, EndpointState> received : receivedFrom(dsePeer, Collections.singletonMap(relayed, raw(DSE_STORAGE_PORT, "7000"))))
            assertThat(received).doesNotContainKey(relayed);

        // a node of this software after all: its own state gossiping 12 clears the presumption, the states go with our ordinals
        for (Map<InetAddressAndPort, EndpointState> received : receivedFrom(dsePeer, Collections.singletonMap(dsePeer, raw(ApplicationState.NET_VERSION.ordinal(), "12"))))
            assertThat(received).containsOnlyKeys(dsePeer);
        assertThat(DsePeers.isPresumed(dsePeer)).isFalse();
        for (Map<Integer, String> sent : sentTo(dsePeer, DIALECT))
            assertThat(sent).isEqualTo(oursAtTheDialect());

        // DSE evidence turns a presumption into a record, which reads and writes DSE's ordinals
        DsePeers.presume(dsePeer, "stored release 4.0.0.6926");
        DsePeers.identify(dsePeer, "handshake reply with the DSE messaging version 1536");
        assertThat(DsePeers.isPresumed(dsePeer)).isFalse();
        assertThat(DsePeers.isDse(dsePeer)).isTrue();
        for (Map<Integer, String> sent : sentTo(dsePeer, DIALECT))
            assertThat(sent).containsEntry(DSE_STORAGE_PORT, "7000");

        // forgotten with the endpoint, and nothing is presumed with the mode off
        DsePeers.presume(hcdPeer, "stored release 4.0.0.6926");
        DsePeers.forget(hcdPeer);
        assertThat(DsePeers.isPresumed(hcdPeer)).isFalse();
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
        DsePeers.presume(hcdPeer, "stored release 4.0.0.6926");
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        assertThat(DsePeers.isPresumed(hcdPeer)).isFalse();
    }

    /** The states of an ack from {@code sender} asking for {@code digests} and carrying {@code states}, as its handler reads them. */
    private static Map<InetAddressAndPort, EndpointState> ackFrom(InetAddressAndPort sender, List<GossipDigest> digests,
                                                                  Map<InetAddressAndPort, EndpointState> states) throws IOException
    {
        GossipDigestAck ack = read(GossipDigestAck.serializer, write(GossipDigestAck.serializer, new GossipDigestAck(digests, states), DIALECT), DIALECT);
        return ack.getEndpointStateMap(sender);
    }

    /** As {@link #ackFrom}, for an ack2. */
    private static Map<InetAddressAndPort, EndpointState> ack2From(InetAddressAndPort sender, Map<InetAddressAndPort, EndpointState> states) throws IOException
    {
        return receivedAck2(states).getEndpointStateMap(sender);
    }

    /**
     * The ways a presumption ends besides the peer's own state. A message without any state answering a request (an
     * ack2, or an ack asking for states itself) while this node holds an entry of the peer without its NET_VERSION: only
     * a node of this software presuming this node withholds its state (two such nodes would otherwise never exchange
     * one). Not an ack with states, not the minimal ack of a node in its shadow round (it asks for nothing), not when this
     * node holds no entry of the peer (its digests did not ask for the peer's state), nor one with a NET_VERSION (it may
     * simply be up to date). And a stored NET_VERSION of 12 or more.
     */
    @Test
    public void aPresumptionEndsWithAMessageWithoutStatesAnsweringARequestOrAStoredVersion40OrLater() throws Exception
    {
        String presumption = "stored release 4.0.0.6926";
        List<GossipDigest> request = Collections.singletonList(new GossipDigest(subject, 7, 8));
        Gossiper.instance.addSavedEndpoint(dsePeer);
        try
        {
            DsePeers.presume(dsePeer, presumption);
            assertThat(ackFrom(dsePeer, request, Collections.singletonMap(subject, raw(ApplicationState.DC.ordinal(), "dc2")))).containsOnlyKeys(subject);
            assertThat(DsePeers.isPresumed(dsePeer)).as("an ack with states").isTrue();
            assertThat(ackFrom(dsePeer, Collections.emptyList(), Collections.emptyMap())).isEmpty();
            assertThat(DsePeers.isPresumed(dsePeer)).as("a minimal ack").isTrue();

            assertThat(ackFrom(dsePeer, request, Collections.emptyMap())).isEmpty();
            assertThat(DsePeers.isPresumed(dsePeer)).as("an ack asking for states, without any").isFalse();
            DsePeers.presume(dsePeer, presumption);
            assertThat(ack2From(dsePeer, Collections.emptyMap())).isEmpty();
            assertThat(DsePeers.isPresumed(dsePeer)).as("an ack2 without states").isFalse();
        }
        finally
        {
            Gossiper.instance.unsafeAnnulEndpoint(dsePeer);
        }

        // no entry of the peer: this node's digests did not ask for its state
        DsePeers.presume(dsePeer, presumption);
        assertThat(ackFrom(dsePeer, request, Collections.emptyMap())).isEmpty();
        assertThat(ack2From(dsePeer, Collections.emptyMap())).isEmpty();
        assertThat(DsePeers.isPresumed(dsePeer)).as("no entry").isTrue();

        // an entry with a NET_VERSION that neither identifies nor refutes (11, HCD 1.2 at the dialect): this node may be up to date
        Gossiper.instance.initializeNodeUnsafe(dsePeer, UUID.randomUUID(), DIALECT, 1);
        try
        {
            assertThat(ackFrom(dsePeer, request, Collections.emptyMap())).isEmpty();
            assertThat(ack2From(dsePeer, Collections.emptyMap())).isEmpty();
            assertThat(DsePeers.isPresumed(dsePeer)).as("a stored NET_VERSION 11").isTrue();
        }
        finally
        {
            Gossiper.instance.unsafeAnnulEndpoint(dsePeer);
        }

        // a stored NET_VERSION: 12 clears the presumption before a message is written, 1536 turns it into a record
        Gossiper.instance.initializeNodeUnsafe(dsePeer, UUID.randomUUID(), MessagingService.VERSION_40, 1);
        try
        {
            // the endpoint removed above took its presumption with it
            DsePeers.presume(dsePeer, presumption);
            assertThat(DsePeers.isPresumed(dsePeer)).isTrue();
            for (Map<Integer, String> sent : sentTo(dsePeer, DIALECT))
                assertThat(sent).isEqualTo(oursAtTheDialect());
            assertThat(DsePeers.isPresumed(dsePeer)).isFalse();
            assertThat(DsePeers.isDse(dsePeer)).isFalse();
        }
        finally
        {
            Gossiper.instance.unsafeAnnulEndpoint(dsePeer);
        }
        Gossiper.instance.initializeNodeUnsafe(dsePeer, UUID.randomUUID(), 1536, 1);
        try
        {
            DsePeers.presume(dsePeer, presumption);
            for (Map<Integer, String> sent : sentTo(dsePeer, DIALECT))
                assertThat(sent).containsEntry(DSE_STORAGE_PORT, "7000");
            assertThat(DsePeers.isDse(dsePeer)).isTrue();
        }
        finally
        {
            Gossiper.instance.unsafeAnnulEndpoint(dsePeer);
        }
    }

    /**
     * The answer to a shadow round is never left out: to a peer presumed to be a DSE node it goes with DSE's ordinals, so
     * that it cannot be taken for the minimal ack of a seed in its own shadow round.
     */
    @Test
    public void theAnswerToAShadowRoundOfAPresumedPeerIsNotWithheld() throws Exception
    {
        DsePeers.presume(dsePeer, "stored release 4.0.0.6926");
        GossipDigestAck answer = GossipDigestAck.shadowRoundAnswer(Collections.singletonMap(subject, ourState()), dsePeer);
        GossipDigestAck read = read(GossipDigestAck.serializer, write(GossipDigestAck.serializer, answer, DIALECT), DIALECT);
        assertThat(read.gDigestList).isEmpty();
        assertThat(byOrdinal(read.epStateMap.get(subject))).containsEntry(DSE_STORAGE_PORT, "7000").containsEntry(DSE_NATIVE_TRANSPORT_PORT, "9042");
        // to a peer that is neither identified nor presumed, this software's ordinals, as before
        GossipDigestAck toHcd = read(GossipDigestAck.serializer, write(GossipDigestAck.serializer, GossipDigestAck.shadowRoundAnswer(Collections.singletonMap(subject, ourState()), hcdPeer), DIALECT), DIALECT);
        assertThat(byOrdinal(toHcd.epStateMap.get(subject))).isEqualTo(oursAtTheDialect());
    }

    private static void gossipedNetVersion(InetAddressAndPort endpoint, String netVersion)
    {
        Map<ApplicationState, VersionedValue> states = new EnumMap<>(ApplicationState.class);
        states.put(ApplicationState.NET_VERSION, VersionedValue.unsafeMakeVersionedValue(netVersion, 1));
        EndpointState state = new EndpointState(new HeartBeatState(5, 6), states);
        state.markDead(); // onRestart would otherwise first treat the peer as one that went down
        StorageService.instance.onRestart(endpoint, state);
    }

    @Test
    public void aNetVersionAppliedByTheStorageServiceIdentifiesTheEndpoint()
    {
        try
        {
            // the state of a DSE node relayed by any peer, before any handshake with it
            gossipedNetVersion(relayed, "1536");
            assertThat(DsePeers.isDse(relayed)).isTrue();
            assertThat(MessagingService.instance().versions.getRaw(relayed)).isEqualTo(DIALECT);
            // the same node upgraded in place gossips 12, and HCD 1.2 at the dialect 11: neither is a DSE node
            gossipedNetVersion(relayed, "12");
            assertThat(DsePeers.isDse(relayed)).isFalse();
            gossipedNetVersion(relayed, "1280");
            assertThat(DsePeers.isDse(relayed)).isTrue();
            gossipedNetVersion(relayed, "11");
            assertThat(DsePeers.isDse(relayed)).isFalse();
            // a value that is not a number says nothing
            gossipedNetVersion(relayed, "1536");
            gossipedNetVersion(relayed, "6.9.26");
            assertThat(DsePeers.isDse(relayed)).isTrue();
        }
        finally
        {
            MessagingService.instance().versions.reset(relayed);
        }
    }

    @Test
    public void identificationIsForgottenWhenTheEndpointIsRemovedOrEvicted()
    {
        DsePeers.identify(dsePeer, "test");
        DsePeers.identify(hcdPeer, "test");
        try
        {
            Gossiper.instance.removeEndpoint(dsePeer);
            assertThat(DsePeers.isDse(dsePeer)).isFalse();
            assertThat(DsePeers.isDse(hcdPeer)).isTrue();

            Gossiper.instance.evictFromMembership(hcdPeer);
            assertThat(DsePeers.isDse(hcdPeer)).isFalse();
        }
        finally
        {
            // out of quarantine, for the other tests of the JVM
            Gossiper.instance.unsafeAnnulEndpoint(dsePeer);
            Gossiper.instance.unsafeAnnulEndpoint(hcdPeer);
        }
    }

    /**
     * The states of this node sent in answer to a digest of {@code from} asking for those above {@code askedAbove}: by the
     * answer to a syn ({@code Gossiper.examineGossiper}) and by the ack2 answering an ack ({@code requestedStates}), which
     * must agree; null when neither sends any.
     */
    private static EndpointState ownStatesSentTo(InetAddressAndPort from, int generation, int askedAbove)
    {
        InetAddressAndPort self = FBUtilities.getBroadcastAddressAndPort();
        List<GossipDigest> digests = Collections.singletonList(new GossipDigest(self, generation, askedAbove));
        Map<InetAddressAndPort, EndpointState> answeringSyn = new HashMap<>();
        Gossiper.instance.examineGossiper(digests, new ArrayList<>(), answeringSyn, from);
        Map<InetAddressAndPort, EndpointState> answeringAck = GossipDigestAckVerbHandler.requestedStates(digests, from);
        EndpointState sent = answeringAck.get(self);
        if (sent == null)
        {
            assertThat(answeringSyn).as("the answer to a syn sends nothing either").doesNotContainKey(self);
            return null;
        }
        assertThat(byOrdinal(answeringSyn.get(self))).as("the answer to a syn and the ack2 send the same states").isEqualTo(byOrdinal(sent));
        assertThat(answeringSyn.get(self).getHeartBeatState().getHeartBeatVersion()).isEqualTo(sent.getHeartBeatState().getHeartBeatVersion());
        return sent;
    }

    /**
     * Since CASSANDRA-19983 a node answers a digest about itself with all of its states, whatever version the digest asks
     * for. DSE fires its listeners for every state a message carries, so a DSE node would handle this node's STATUS as new,
     * a "state jump to NORMAL" and a topology update, every gossip round: a DSE node is sent the states it asks for only.
     */
    @Test
    public void aDsePeerIsSentOnlyTheStatesOfThisNodeItAsksFor() throws Exception
    {
        InetAddressAndPort self = FBUtilities.getBroadcastAddressAndPort();
        Map<ApplicationState, VersionedValue> states = new EnumMap<>(ApplicationState.class);
        states.put(ApplicationState.STATUS_WITH_PORT, VersionedValue.unsafeMakeVersionedValue("NORMAL,1", 3));
        states.put(ApplicationState.STATUS, VersionedValue.unsafeMakeVersionedValue("NORMAL,1", 4));
        states.put(ApplicationState.DC, VersionedValue.unsafeMakeVersionedValue("dc1", 5));
        states.put(ApplicationState.LOAD, VersionedValue.unsafeMakeVersionedValue("1.0", 9));
        int generation = 7;
        Map<Integer, String> all = byOrdinal(new EndpointState(new HeartBeatState(generation, 10), states));
        EndpointState before = Gossiper.instance.endpointStateMap.put(self, new EndpointState(new HeartBeatState(generation, 10), states));
        DsePeers.identify(dsePeer, "test");
        try
        {
            // the heartbeat moved since the peer's digest (8): a DSE node gets it and LOAD, the only state above 8
            EndpointState toDse = ownStatesSentTo(dsePeer, generation, 8);
            assertThat(toDse.getHeartBeatState().getHeartBeatVersion()).isEqualTo(10);
            assertThat(byOrdinal(toDse)).containsOnlyKeys(ApplicationState.LOAD.ordinal());

            // and so a DSE node reads the ack: no STATUS, DC or other state it already has
            GossipDigestAck ack = new GossipDigestAck(Collections.emptyList(), Collections.singletonMap(self, toDse), dsePeer);
            GossipDigestAck read = read(GossipDigestAck.serializer, write(GossipDigestAck.serializer, ack, DIALECT), DIALECT);
            assertThat(byOrdinal(read.epStateMap.get(self))).containsOnlyKeys(ApplicationState.LOAD.ordinal());

            // a peer not identified as a DSE node (this software, HCD 1.2) still gets all of them (CASSANDRA-19983), and so
            // does a peer only presumed to be one (the acks sent to it carry no state at all on the wire)
            assertThat(byOrdinal(ownStatesSentTo(hcdPeer, generation, 8))).isEqualTo(all);
            DsePeers.presume(relayed, "test");
            assertThat(byOrdinal(ownStatesSentTo(relayed, generation, 8))).isEqualTo(all);

            // a DSE node whose digest is up to date is sent nothing, as any peer
            assertThat(ownStatesSentTo(dsePeer, generation, 10)).isNull();

            // a DSE node that has none of this node's states asks for all of them, and gets them on both paths
            assertThat(byOrdinal(ownStatesSentTo(dsePeer, generation, HeartBeatState.EMPTY_VERSION))).isEqualTo(all);

            // a DSE node that holds an older generation of this node gets all the states in the answer to its syn
            Map<InetAddressAndPort, EndpointState> answeringSyn = new HashMap<>();
            Gossiper.instance.examineGossiper(Collections.singletonList(new GossipDigest(self, generation - 1, 10)), new ArrayList<>(), answeringSyn, dsePeer);
            assertThat(byOrdinal(answeringSyn.get(self))).isEqualTo(all);

            // a DSE node known so far only by the NET_VERSION this node stores for it is identified first, as when a
            // message to it is written
            Map<ApplicationState, VersionedValue> subjectStates = new EnumMap<>(ApplicationState.class);
            subjectStates.put(ApplicationState.NET_VERSION, VersionedValue.unsafeMakeVersionedValue("1024", 1));
            Gossiper.instance.endpointStateMap.put(subject, new EndpointState(new HeartBeatState(3, 4), subjectStates));
            assertThat(DsePeers.isDse(subject)).isFalse();
            assertThat(byOrdinal(ownStatesSentTo(subject, generation, 8))).containsOnlyKeys(ApplicationState.LOAD.ordinal());
            assertThat(DsePeers.isDse(subject)).isTrue();

            // with the mode off a recorded DSE peer gets all the states, as before
            DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
            assertThat(byOrdinal(ownStatesSentTo(dsePeer, generation, 8))).isEqualTo(all);
        }
        finally
        {
            Gossiper.instance.endpointStateMap.remove(subject);
            if (before == null)
                Gossiper.instance.endpointStateMap.remove(self);
            else
                Gossiper.instance.endpointStateMap.put(self, before);
        }
    }

    /**
     * The DSE_GOSSIP_STATE this node gossips, which tells DSE nodes the DSE version that keeps them from sending this
     * node the range reads of an index query with READS.MULTI_RANGE_READ ({@code DseCompat.gossipedDseVersion}, covered
     * by {@code Dse6StartupChecksTest}), and what it runs.
     */
    @Test
    public void thisNodeGossipsADseStateThatDseReadsAtItsOwnOrdinal() throws Exception
    {
        String json = new VersionedValue.VersionedValueFactory(Murmur3Partitioner.instance).dseState("6.8.1").value;
        Map<String, Object> state = JsonUtils.fromJsonMap(json);
        // the keys and types DSE gossips (test/data/dse-compat/gossip/dse-6.9.26/gossipinfo.txt): graph a boolean, active a string
        assertThat(state).containsExactly(entry("cluster_name", DatabaseDescriptor.getClusterName()),
                                          entry("dse_version", "6.8.1"),
                                          entry("graph", false),
                                          entry("workload", "Cassandra"),
                                          entry("workloads", "Cassandra"),
                                          entry("active", "true"),
                                          entry("partitioner_name", DatabaseDescriptor.getPartitionerName()));

        // written to a DSE node at its own ordinal, that of DSE's DSE_GOSSIP_STATE, with its version
        DsePeers.identify(dsePeer, "test");
        EndpointState withDseState = ourState();
        withDseState.addApplicationState(ApplicationState.X_11_PADDING, VersionedValue.unsafeMakeVersionedValue(json, 42));
        GossipDigestAck2 ack2 = new GossipDigestAck2(Collections.singletonMap(subject, withDseState), dsePeer);
        EndpointState read = read(GossipDigestAck2.serializer, write(GossipDigestAck2.serializer, ack2, DIALECT), DIALECT).epStateMap.get(subject);
        assertThat(byOrdinal(read)).containsEntry(DSE_GOSSIP_STATE, json);
        assertThat(read.getApplicationState(ApplicationState.values()[DSE_GOSSIP_STATE]).version).isEqualTo(42);
    }

    @Test
    public void nothingIsIdentifiedOrTranslatedWithTheModeOff() throws Exception
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
        DsePeers.identify(dsePeer, "test");
        DsePeers.fromGossipedNetVersion(relayed, "1536");
        // the recipient of a message is not even recorded, and nothing read at a pre-4.0 version is marked for translation
        assertThat(new GossipDigestAck2(Collections.emptyMap(), dsePeer).recipient).isNull();
        assertThat(new GossipDigestAck(Collections.emptyList(), Collections.emptyMap(), dsePeer).recipient).isNull();
        assertThat(GossipDigestAck.shadowRoundAnswer(Collections.emptyMap(), dsePeer).recipient).isNull();
        assertThat(GossipDigestAck2.received(Collections.emptyMap(), DIALECT).readInDialect).isFalse();
        assertThat(GossipDigestAck.received(Collections.emptyList(), Collections.emptyMap(), DIALECT).readInDialect).isFalse();

        // nothing was recorded either: back in DSE_6 mode, neither peer is identified
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        assertThat(DsePeers.isDse(dsePeer)).isFalse();
        assertThat(DsePeers.isDse(relayed)).isFalse();
    }
}
