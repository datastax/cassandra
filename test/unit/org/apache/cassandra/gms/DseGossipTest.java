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

import com.google.common.net.InetAddresses;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.dsecompat.DseFixtures;
import org.apache.cassandra.dsecompat.DseFixtures.DseVersion;
import org.apache.cassandra.dsecompat.DseFixtures.GossipState;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.DseCompat;
import org.apache.cassandra.net.DsePeers;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.nodes.PeerInfo;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CassandraVersion;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.config.CassandraRelevantProperties.GOSSIP_DISABLE_THREAD_VALIDATION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Gossip with DSE 6 peers: what this node publishes as {@code NET_VERSION} in {@code DSE_6} mode, how a DSE
 * {@code NET_VERSION} is recorded, and how an endpoint state serialized with DSE's {@code ApplicationState}
 * ordinals is read at messaging version 11 and written for a DSE peer, with the
 * values of the captured {@code nodetool gossipinfo} of a 6.9.26 node as the expected data. With the mode off,
 * everything behaves as before.
 */
public class DseGossipTest
{
    private static final int DSE_DIALECT = DseCompat.DSE_DIALECT;
    /** {@code ApplicationState} ordinals of DSE 6.8 and 6.9, by the names {@code nodetool gossipinfo} prints. */
    private static final Map<String, Integer> DSE_ORDINALS = new HashMap<>();
    static
    {
        String[] names = { "STATUS", "LOAD", "SCHEMA", "DC", "RACK", "RELEASE_VERSION", "REMOVAL_COORDINATOR", "INTERNAL_IP",
                           "NATIVE_TRANSPORT_ADDRESS", "DSE_GOSSIP_STATE", "SEVERITY", "NET_VERSION", "HOST_ID", "TOKENS",
                           "NATIVE_TRANSPORT_READY", "NATIVE_TRANSPORT_PORT", "NATIVE_TRANSPORT_PORT_SSL", "STORAGE_PORT",
                           "STORAGE_PORT_SSL", "JMX_PORT", "SCHEMA_COMPATIBILITY_VERSION", "DISK_USAGE" };
        for (int i = 0; i < names.length; i++)
            DSE_ORDINALS.put(names[i], i);
        for (int i = 1; i <= 10; i++)
            DSE_ORDINALS.put("X" + i, 21 + i);
    }
    private static final int DSE_GENERATION = 1790256234;
    private static final int DSE_HEARTBEAT = 819;
    /** The captured gossipinfo hides the tokens: a real one in their place. */
    private static final List<Token> TOKENS = Collections.singletonList(Murmur3Partitioner.instance.getToken(ByteBufferUtil.bytes("k")));
    private static final InetAddressAndPort DSE_ENDPOINT = InetAddressAndPort.getByAddressOverrideDefaults(InetAddresses.forString("127.0.0.1"), 7000);

    private InternodeCompatibilityMode modeBefore;

    @BeforeClass
    public static void startup()
    {
        DatabaseDescriptor.daemonInitialization();
        // the gossiper checks that state changes run on the gossip stage; the property is read when the class initialises
        try (WithProperties properties = new WithProperties().set(GOSSIP_DISABLE_THREAD_VALIDATION, true))
        {
            Gossiper.instance.getClass();
        }
    }

    @Before
    public void saveMode()
    {
        modeBefore = DatabaseDescriptor.getInternodeCompatibilityMode();
    }

    @After
    public void restoreMode()
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(modeBefore);
        // updateNetVersion records the DSE peers it sees
        DsePeers.reset();
    }

    private static void dse6()
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
    }

    private static void none()
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
    }

    private static VersionedValue.VersionedValueFactory factory()
    {
        return new VersionedValue.VersionedValueFactory(Murmur3Partitioner.instance);
    }

    /*
     * NET_VERSION published
     */

    @Test
    public void publishesTheVersionDseCanParseInDse6Mode()
    {
        dse6();
        assertThat(factory().networkVersion().value).isEqualTo(String.valueOf(DseCompat.NET_VERSION_PUBLISHED)).isEqualTo("12");
        none();
        assertThat(factory().networkVersion().value).isEqualTo(String.valueOf(MessagingService.current_version));
        // the explicit-version variant used by tests is not affected by the mode
        dse6();
        assertThat(factory().networkVersion(1536).value).isEqualTo("1536");
    }

    /*
     * NET_VERSION received
     */

    private static EndpointState stateWithNetVersion(String netVersion)
    {
        Map<ApplicationState, VersionedValue> states = new EnumMap<>(ApplicationState.class);
        states.put(ApplicationState.NET_VERSION, VersionedValue.unsafeMakeVersionedValue(netVersion, 1));
        EndpointState state = new EndpointState(new HeartBeatState(DSE_GENERATION, DSE_HEARTBEAT), states);
        state.markDead(); // onRestart would otherwise first treat the peer as one that went down
        return state;
    }

    private static int recordedVersion(InetAddressAndPort peer, String netVersion)
    {
        StorageService.instance.onRestart(peer, stateWithNetVersion(netVersion));
        return MessagingService.instance().versions.getRaw(peer);
    }

    @Test
    public void dseNetVersionIsRecordedAsTheDialectInDse6Mode() throws Exception
    {
        InetAddressAndPort peer = InetAddressAndPort.getByName("127.0.0.201:7000");
        try
        {
            dse6();
            for (DseVersion v : DseVersion.values())
                assertThat(recordedVersion(peer, String.valueOf(v.relativeMessagingVersion << 8))).as(v.name()).isEqualTo(DSE_DIALECT);
            assertThat(recordedVersion(peer, "1280")).as("6.9.0-6.9.6").isEqualTo(DSE_DIALECT);
            // what HCD 1.2 at the dialect, a 4.0 peer and a peer of this software gossip is stored as it is
            assertThat(recordedVersion(peer, "11")).isEqualTo(11);
            assertThat(recordedVersion(peer, "12")).isEqualTo(12);
            assertThat(recordedVersion(peer, String.valueOf(MessagingService.current_version))).isEqualTo(MessagingService.current_version);
            // a value this software cannot parse is ignored with a warning, the recorded version stays
            Logger storageLogger = (Logger) LoggerFactory.getLogger(StorageService.class);
            ListAppender<ILoggingEvent> appender = new ListAppender<>();
            appender.start();
            storageLogger.addAppender(appender);
            try
            {
                assertThat(recordedVersion(peer, "6.9.26")).isEqualTo(MessagingService.current_version);
            }
            finally
            {
                storageLogger.detachAppender(appender);
            }
            assertThat(appender.list).filteredOn(e -> e.getLevel() == Level.WARN)
                                     .extracting(ILoggingEvent::getFormattedMessage)
                                     .containsExactly("Ignoring the NET_VERSION 6.9.26 gossiped by " + peer + ": not a number; the messaging version " +
                                                      "of the peer stays " + MessagingService.current_version);
        }
        finally
        {
            MessagingService.instance().versions.reset(peer);
        }
    }

    @Test
    public void gossipedPublishedVersionDoesNotLowerANegotiatedOne() throws Exception
    {
        InetAddressAndPort peer = InetAddressAndPort.getByName("127.0.0.206:7000");
        try
        {
            dse6();
            // the handshake with a peer of this software came first, its gossip second
            MessagingService.instance().versions.set(peer, MessagingService.current_version);
            assertThat(recordedVersion(peer, String.valueOf(DseCompat.NET_VERSION_PUBLISHED))).isEqualTo(MessagingService.current_version);
            // gossip first: 12 is recorded until a handshake says more
            MessagingService.instance().versions.reset(peer);
            assertThat(recordedVersion(peer, String.valueOf(DseCompat.NET_VERSION_PUBLISHED))).isEqualTo(DseCompat.NET_VERSION_PUBLISHED);
            // a peer rolled back to DSE is recorded at the dialect whatever was negotiated before
            MessagingService.instance().versions.set(peer, MessagingService.current_version);
            assertThat(recordedVersion(peer, "1536")).isEqualTo(DSE_DIALECT);
            // with the mode off, the gossiped value is stored as it is
            none();
            MessagingService.instance().versions.set(peer, MessagingService.current_version);
            assertThat(recordedVersion(peer, "12")).isEqualTo(12);
        }
        finally
        {
            MessagingService.instance().versions.reset(peer);
        }
    }

    @Test
    public void netVersionHandlingIsUnchangedWithTheModeOff() throws Exception
    {
        InetAddressAndPort peer = InetAddressAndPort.getByName("127.0.0.202:7000");
        try
        {
            none();
            assertThat(recordedVersion(peer, "1536")).isEqualTo(1536);
            assertThat(recordedVersion(peer, "12")).isEqualTo(12);
            assertThatThrownBy(() -> recordedVersion(peer, "6.9.26"))
            .isInstanceOf(AssertionError.class)
            .hasMessageContaining("Got invalid value for NET_VERSION application state: 6.9.26");
            assertThat(MessagingService.instance().versions.getRaw(peer)).isEqualTo(12);
        }
        finally
        {
            MessagingService.instance().versions.reset(peer);
        }
    }

    /*
     * Endpoint state received from a DSE node
     */

    /** Serializes an endpoint state the way a DSE node does: heartbeat, count, then (DSE ordinal, value) pairs. */
    private static byte[] dseEndpointState(List<Object[]> states) throws IOException
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            out.writeInt(DSE_GENERATION);
            out.writeInt(DSE_HEARTBEAT);
            out.writeInt(states.size());
            for (Object[] state : states)
            {
                out.writeInt((Integer) state[0]);
                VersionedValue.serializer.serialize((VersionedValue) state[1], out, DSE_DIALECT);
            }
            return out.toByteArray();
        }
    }

    private static Object[] dseState(String name, int version, String value)
    {
        Integer ordinal = DSE_ORDINALS.get(name);
        assertThat(ordinal).as("DSE ordinal of " + name).isNotNull();
        return new Object[]{ ordinal, VersionedValue.unsafeMakeVersionedValue(value, version) };
    }

    /** The captured states of the 6.9.26 node, with real tokens in place of the hidden ones. */
    private static List<Object[]> capturedStates(VersionedValue tokens) throws IOException
    {
        List<Object[]> states = new ArrayList<>();
        for (GossipState s : DseFixtures.gossipStates(DseVersion.DSE_6_9_26))
            states.add(s.name.equals("TOKENS") ? new Object[]{ DSE_ORDINALS.get("TOKENS"), tokens } : dseState(s.name, s.version, s.value));
        return states;
    }

    /** Reads a state received at the dialect from a peer identified as a DSE node, as the gossip messages translate it. */
    private static EndpointState read(byte[] bytes) throws IOException
    {
        try (DataInputBuffer in = new DataInputBuffer(bytes))
        {
            return DseGossipStates.fromDse(EndpointState.serializer.deserialize(in, DSE_DIALECT));
        }
    }

    private static Map<ApplicationState, String> values(EndpointState state)
    {
        Map<ApplicationState, String> values = new EnumMap<>(ApplicationState.class);
        for (Map.Entry<ApplicationState, VersionedValue> e : state.states())
            values.put(e.getKey(), e.getValue().value);
        return values;
    }

    /** What the gossiper does with a state received from a DSE node before applying it (DSE_6 mode only). */
    private static EndpointState received(byte[] bytes, EndpointState stored) throws IOException
    {
        EndpointState state = read(bytes);
        DseGossipStates.deriveAddressesAndPorts(DSE_ENDPOINT, state, stored);
        return state;
    }

    private static List<Object[]> capturedStates() throws IOException
    {
        return capturedStates(factory().tokens(TOKENS));
    }

    @Test
    public void capturedDseStateIsTranslatedAtTheDialect() throws IOException
    {
        dse6();
        VersionedValue tokensValue = factory().tokens(TOKENS);
        EndpointState state = read(dseEndpointState(capturedStates(tokensValue)));

        assertThat(state.getHeartBeatState().getGeneration()).isEqualTo(DSE_GENERATION);
        assertThat(state.getHeartBeatState().getHeartBeatVersion()).isEqualTo(DSE_HEARTBEAT);
        Map<ApplicationState, String> values = values(state);
        // the states whose ordinal is the same on both sides, as captured
        assertThat(values).containsEntry(ApplicationState.STATUS, "NORMAL,-660323125693613959")
                          .containsEntry(ApplicationState.LOAD, "272483.0")
                          .containsEntry(ApplicationState.SCHEMA, "1a50fe72-f814-3069-b1f3-46399971119e")
                          .containsEntry(ApplicationState.DC, "Cassandra")
                          .containsEntry(ApplicationState.RACK, "rack1")
                          .containsEntry(ApplicationState.RELEASE_VERSION, DseVersion.DSE_6_9_26.releaseVersion)
                          .containsEntry(ApplicationState.RPC_ADDRESS, "127.0.0.1")
                          .containsEntry(ApplicationState.NET_VERSION, "1536")
                          .containsEntry(ApplicationState.HOST_ID, "55771b40-b5c1-4123-94e5-8af8ce644811")
                          .containsEntry(ApplicationState.RPC_READY, "true")
                          .containsEntry(ApplicationState.TOKENS, tokensValue.value);
        assertThat(state.getTokens(Murmur3Partitioner.instance)).containsExactlyElementsOf(TOKENS);
        assertThat(state.getStatus()).isEqualTo("NORMAL");
        // DSE_GOSSIP_STATE lands in the padding slot with the same ordinal; only compatstatus reads it
        assertThat(values.get(ApplicationState.X_11_PADDING)).contains("\"dse_version\":\"6.9.26\"").contains("\"cluster_name\":\"Test Cluster\"");
        // the DSE states from ordinal 15 on are kept in unused padding slots, never in the states of this software
        // that share their ordinal (INTERNAL_ADDRESS_AND_PORT, NATIVE_ADDRESS_AND_PORT, STATUS_WITH_PORT, ...)
        assertThat(values).containsEntry(DseGossipStates.NATIVE_TRANSPORT_PORT_SLOT, "9042")
                          .containsEntry(DseGossipStates.DSE_ONLY_SLOTS.get(16), "9042")
                          .containsEntry(DseGossipStates.STORAGE_PORT_SLOT, "7000")
                          .containsEntry(DseGossipStates.DSE_ONLY_SLOTS.get(18), "7001")
                          .containsEntry(DseGossipStates.DSE_ONLY_SLOTS.get(19), "7199")
                          .containsEntry(DseGossipStates.DSE_ONLY_SLOTS.get(20), "2")
                          .doesNotContainKeys(ApplicationState.INTERNAL_ADDRESS_AND_PORT, ApplicationState.NATIVE_ADDRESS_AND_PORT,
                                              ApplicationState.STATUS_WITH_PORT, ApplicationState.SSTABLE_VERSIONS,
                                              ApplicationState.DISK_USAGE, ApplicationState.INDEX_STATUS, ApplicationState.INTERNAL_IP)
                          .hasSize(18);

        // the gossiper then combines the native transport address and port, versioned by the newer of the two; the
        // captured node gossips no INTERNAL_IP, so there is no listen address to combine with its storage port
        DseGossipStates.deriveAddressesAndPorts(DSE_ENDPOINT, state, null);
        assertThat(state.getApplicationState(ApplicationState.NATIVE_ADDRESS_AND_PORT).value).isEqualTo("127.0.0.1:9042");
        assertThat(state.getApplicationState(ApplicationState.NATIVE_ADDRESS_AND_PORT).version).isEqualTo(7);
        assertThat(state.getApplicationState(ApplicationState.INTERNAL_ADDRESS_AND_PORT)).isNull();
    }

    @Test
    public void capturedDseStateIsWrittenBackUnchanged() throws IOException
    {
        dse6();
        List<Object[]> captured = capturedStates();
        Map<Integer, VersionedValue> expected = new HashMap<>();
        for (Object[] state : captured)
            expected.put((Integer) state[0], (VersionedValue) state[1]);

        for (boolean legacyStatesDropped : new boolean[]{ false, true })
        {
            EndpointState state = received(dseEndpointState(captured), null);
            if (legacyStatesDropped) // what the gossiper does once no node of major version 3 is left
                state.removeMajorVersion3LegacyApplicationStates();
            Map<Integer, VersionedValue> sent = asDseReadsIt(state);
            assertThat(sent.keySet()).as("legacy states dropped: " + legacyStatesDropped).isEqualTo(expected.keySet());
            for (Map.Entry<Integer, VersionedValue> e : expected.entrySet())
            {
                assertThat(sent.get(e.getKey()).value).as("value at DSE ordinal " + e.getKey()).isEqualTo(e.getValue().value);
                // the native transport address is written from the combined state, with the version of the newer piece
                int version = e.getKey() == DSE_ORDINALS.get("NATIVE_TRANSPORT_ADDRESS") && legacyStatesDropped ? 7 : e.getValue().version;
                assertThat(sent.get(e.getKey()).version).as("version at DSE ordinal " + e.getKey()).isEqualTo(version);
            }
        }
    }

    @Test
    public void peersTableOfADsePeer() throws IOException
    {
        dse6();
        // a native port other than the one of this JVM's configuration, which RPC_ADDRESS alone would give
        List<Object[]> states = new ArrayList<>();
        for (Object[] state : capturedStates())
            states.add(state[0].equals(DSE_ORDINALS.get("NATIVE_TRANSPORT_PORT")) ? dseState("NATIVE_TRANSPORT_PORT", 7, "9043") : state);
        EndpointState state = received(dseEndpointState(states), null);
        PeerInfo peer = new PeerInfo();
        state.maybeSetUpdater(update -> update.apply(peer));
        state.maybeUpdate();
        assertThat(peer.getReleaseVersion()).isEqualTo(new CassandraVersion(DseVersion.DSE_6_9_26.releaseVersion));
        assertThat(DatabaseDescriptor.getNativeTransportPort()).isNotEqualTo(9043);
        assertThat(peer.getNativeTransportAddressAndPort()).isEqualTo(InetAddressAndPort.getByNameOverrideDefaults("127.0.0.1", 9043));
        assertThat(peer.getHostId()).isEqualTo(UUID.fromString("55771b40-b5c1-4123-94e5-8af8ce644811"));
        assertThat(peer.getDataCenter()).isEqualTo("Cassandra");
    }

    @Test
    public void internalIpAndStoragePortBecomeOneAddress() throws IOException
    {
        dse6();
        List<Object[]> states = new ArrayList<>();
        states.add(dseState("INTERNAL_IP", 3, "10.0.0.7"));
        states.add(dseState("STORAGE_PORT", 9, "7010"));
        states.add(dseState("NATIVE_TRANSPORT_ADDRESS", 4, "10.0.0.8"));
        states.add(dseState("NATIVE_TRANSPORT_PORT", 2, "9043"));
        states.add(dseState("DISK_USAGE", 5, "spacious"));
        EndpointState state = received(dseEndpointState(states), null);
        Map<ApplicationState, String> values = values(state);
        assertThat(values).containsEntry(ApplicationState.INTERNAL_ADDRESS_AND_PORT, "10.0.0.7:7010")
                          .containsEntry(ApplicationState.NATIVE_ADDRESS_AND_PORT, "10.0.0.8:9043")
                          .containsEntry(ApplicationState.DISK_USAGE, "spacious");
        assertThat(state.getApplicationState(ApplicationState.INTERNAL_ADDRESS_AND_PORT).version).isEqualTo(9);
        assertThat(state.getApplicationState(ApplicationState.NATIVE_ADDRESS_AND_PORT).version).isEqualTo(4);
    }

    @Test
    public void addressAndPortAreCombinedAcrossGossipDeltas() throws IOException
    {
        dse6();
        List<Object[]> states = new ArrayList<>();
        states.add(dseState("NATIVE_TRANSPORT_ADDRESS", 3, "10.0.0.8"));
        states.add(dseState("NATIVE_TRANSPORT_PORT", 7, "9042"));
        EndpointState stored = received(dseEndpointState(states), null);
        assertThat(stored.getApplicationState(ApplicationState.NATIVE_ADDRESS_AND_PORT).value).isEqualTo("10.0.0.8:9042");

        // a delta with the address only takes the port from the stored state
        states.clear();
        states.add(dseState("NATIVE_TRANSPORT_ADDRESS", 20, "10.0.0.9"));
        EndpointState delta = received(dseEndpointState(states), stored);
        assertThat(delta.getApplicationState(ApplicationState.NATIVE_ADDRESS_AND_PORT).value).isEqualTo("10.0.0.9:9042");
        assertThat(delta.getApplicationState(ApplicationState.NATIVE_ADDRESS_AND_PORT).version).isEqualTo(20);

        // a delta with the port only takes the address from the stored state, also once the legacy address state is gone
        stored.addApplicationStates(delta.states());
        stored.removeMajorVersion3LegacyApplicationStates();
        assertThat(stored.getApplicationState(ApplicationState.RPC_ADDRESS)).isNull();
        states.clear();
        states.add(dseState("NATIVE_TRANSPORT_PORT", 21, "9142"));
        delta = received(dseEndpointState(states), stored);
        assertThat(delta.getApplicationState(ApplicationState.NATIVE_ADDRESS_AND_PORT).value).isEqualTo("10.0.0.9:9142");
        assertThat(delta.getApplicationState(ApplicationState.NATIVE_ADDRESS_AND_PORT).version).isEqualTo(21);

        // a delta with neither piece adds nothing
        states.clear();
        states.add(dseState("LOAD", 22, "1.0"));
        assertThat(values(received(dseEndpointState(states), stored))).containsOnlyKeys(ApplicationState.LOAD);
    }

    @Test
    public void ipv6AddressAndPortAreCombined() throws IOException
    {
        dse6();
        List<Object[]> states = new ArrayList<>();
        states.add(dseState("NATIVE_TRANSPORT_ADDRESS", 4, "2001:db8::8"));
        states.add(dseState("NATIVE_TRANSPORT_PORT", 2, "9042"));
        EndpointState state = received(dseEndpointState(states), null);
        assertThat(state.getApplicationState(ApplicationState.NATIVE_ADDRESS_AND_PORT).value).isEqualTo("[2001:db8:0:0:0:0:0:8]:9042");
        // and written back to DSE as the address and the port
        Map<Integer, VersionedValue> sent = asDseReadsIt(state);
        assertThat(sent.get(DSE_ORDINALS.get("NATIVE_TRANSPORT_ADDRESS")).value).isEqualTo("2001:db8::8");
        assertThat(sent.get(DSE_ORDINALS.get("NATIVE_TRANSPORT_PORT")).value).isEqualTo("9042");
    }

    /** A received DSE-shaped state: the native address and the port slot, as the serializer leaves them. */
    private static EndpointState dseShaped(int generation, int version, String address, String port)
    {
        Map<ApplicationState, VersionedValue> states = new EnumMap<>(ApplicationState.class);
        if (address != null)
            states.put(ApplicationState.RPC_ADDRESS, VersionedValue.unsafeMakeVersionedValue(address, version));
        if (port != null)
            states.put(DseGossipStates.NATIVE_TRANSPORT_PORT_SLOT, VersionedValue.unsafeMakeVersionedValue(port, version));
        return new EndpointState(new HeartBeatState(generation, version), states);
    }

    @Test
    public void gossiperCombinesAddressAndPortInDse6ModeOnly() throws Exception
    {
        InetAddressAndPort peer = InetAddressAndPort.getByName("127.0.0.210:7000");
        int generation = (int) (FBUtilities.now().getEpochSecond());
        try
        {
            dse6();
            Gossiper.instance.applyStateLocally(Collections.singletonMap(peer, dseShaped(generation, 3, "10.0.0.8", "9043")));
            assertThat(Gossiper.instance.getEndpointStateForEndpoint(peer).getApplicationState(ApplicationState.NATIVE_ADDRESS_AND_PORT).value)
            .isEqualTo("10.0.0.8:9043");
            // a delta with the address only, combined with the stored port
            Gossiper.instance.applyStateLocally(Collections.singletonMap(peer, dseShaped(generation, 20, "10.0.0.9", null)));
            assertThat(Gossiper.instance.getEndpointStateForEndpoint(peer).getApplicationState(ApplicationState.NATIVE_ADDRESS_AND_PORT).value)
            .isEqualTo("10.0.0.9:9043");

            Gossiper.instance.endpointStateMap.remove(peer);
            none();
            Gossiper.instance.applyStateLocally(Collections.singletonMap(peer, dseShaped(generation, 3, "10.0.0.8", "9043")));
            assertThat(Gossiper.instance.getEndpointStateForEndpoint(peer).getApplicationState(ApplicationState.NATIVE_ADDRESS_AND_PORT)).isNull();
        }
        finally
        {
            Gossiper.instance.endpointStateMap.remove(peer);
            Gossiper.instance.liveEndpoints.remove(peer);
        }
    }

    @Test
    public void addressWithoutOrWithUnusablePortStaysAnAddress() throws IOException
    {
        dse6();
        for (String port : new String[]{ "not-a-port", "70000", "-1" })
        {
            List<Object[]> states = new ArrayList<>();
            states.add(dseState("INTERNAL_IP", 3, "10.0.0.7"));
            states.add(dseState("NATIVE_TRANSPORT_ADDRESS", 4, "10.0.0.8"));
            states.add(dseState("NATIVE_TRANSPORT_PORT", 2, port));
            Map<ApplicationState, String> values = values(received(dseEndpointState(states), null));
            assertThat(values).as(port).containsEntry(ApplicationState.INTERNAL_IP, "10.0.0.7")
                              .containsEntry(ApplicationState.RPC_ADDRESS, "10.0.0.8")
                              .containsEntry(DseGossipStates.NATIVE_TRANSPORT_PORT_SLOT, port)
                              .doesNotContainKeys(ApplicationState.INTERNAL_ADDRESS_AND_PORT, ApplicationState.NATIVE_ADDRESS_AND_PORT);
        }
        // a host name is not resolved on the gossip stage and not combined
        List<Object[]> states = new ArrayList<>();
        states.add(dseState("NATIVE_TRANSPORT_ADDRESS", 4, "localhost"));
        states.add(dseState("NATIVE_TRANSPORT_PORT", 2, "9042"));
        assertThat(values(received(dseEndpointState(states), null))).doesNotContainKey(ApplicationState.NATIVE_ADDRESS_AND_PORT);
        // a port alone never becomes an address (InetAddress would read "9042" as 0.0.35.82)
        states.clear();
        states.add(dseState("STORAGE_PORT", 9, "7000"));
        states.add(dseState("NATIVE_TRANSPORT_PORT", 2, "9042"));
        assertThat(values(received(dseEndpointState(states), null))).containsOnlyKeys(DseGossipStates.STORAGE_PORT_SLOT,
                                                                                      DseGossipStates.NATIVE_TRANSPORT_PORT_SLOT);
    }

    @Test
    public void unknownOrdinalIsDroppedInDse6ModeAndFailsOtherwise() throws IOException
    {
        List<Object[]> states = new ArrayList<>();
        states.add(dseState("DC", 14, "Cassandra"));
        states.add(dseState("X4", 1, "dse-padding")); // DSE padding: known to this software, dropped as DSE's padding
        states.add(dseState("X10", 1, "dse-padding")); // DSE ordinal 31, one past this software's last state
        states.add(new Object[]{ 40, VersionedValue.unsafeMakeVersionedValue("future", 1) });
        states.add(new Object[]{ -1, VersionedValue.unsafeMakeVersionedValue("negative", 1) });
        states.add(dseState("RACK", 17, "rack1"));
        byte[] bytes = dseEndpointState(states);

        dse6();
        Map<ApplicationState, String> values = values(read(bytes));
        assertThat(values).containsEntry(ApplicationState.DC, "Cassandra").containsEntry(ApplicationState.RACK, "rack1").hasSize(2);

        none();
        assertThatThrownBy(() -> read(bytes)).isInstanceOf(ArrayIndexOutOfBoundsException.class);
    }

    /*
     * Endpoint state sent to a DSE node
     */

    /** Reads what a DSE node receives from this node, which has identified it: the raw (DSE ordinal, value) pairs. */
    private static Map<Integer, VersionedValue> asDseReadsIt(EndpointState state) throws IOException
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            EndpointStateSerializer.instance.serialize(state, out, DSE_DIALECT, true);
            assertThat(out.getLength()).isEqualTo(EndpointStateSerializer.instance.serializedSize(state, DSE_DIALECT, true));
            try (DataInputBuffer in = new DataInputBuffer(out.toByteArray()))
            {
                assertThat(in.readInt()).isEqualTo(state.getHeartBeatState().getGeneration());
                assertThat(in.readInt()).isEqualTo(state.getHeartBeatState().getHeartBeatVersion());
                int count = in.readInt();
                Map<Integer, VersionedValue> states = new HashMap<>();
                for (int i = 0; i < count; i++)
                {
                    int ordinal = in.readInt();
                    VersionedValue value = VersionedValue.serializer.deserialize(in, DSE_DIALECT);
                    assertThat(states.put(ordinal, value)).as("ordinal " + ordinal + " sent twice").isNull();
                }
                return states;
            }
        }
    }

    @Test
    public void ownStateIsWrittenWithDseOrdinalsAtTheDialect() throws Exception
    {
        dse6();
        VersionedValue.VersionedValueFactory factory = factory();
        List<Token> tokens = TOKENS;
        UUID hostId = UUID.randomUUID();
        Map<ApplicationState, VersionedValue> own = new EnumMap<>(ApplicationState.class);
        own.put(ApplicationState.STATUS_WITH_PORT, factory.normal(tokens));
        own.put(ApplicationState.STATUS, factory.normal(tokens));
        own.put(ApplicationState.NET_VERSION, factory.networkVersion());
        own.put(ApplicationState.HOST_ID, factory.hostId(hostId));
        own.put(ApplicationState.NATIVE_ADDRESS_AND_PORT, factory.nativeaddressAndPort(InetAddressAndPort.getByName("10.0.0.5:9042")));
        own.put(ApplicationState.RPC_ADDRESS, factory.rpcaddress(InetAddressAndPort.getByName("10.0.0.5").getAddress()));
        own.put(ApplicationState.INTERNAL_ADDRESS_AND_PORT, factory.internalAddressAndPort(InetAddressAndPort.getByName("10.0.0.5:7000")));
        // published next to INTERNAL_ADDRESS_AND_PORT by GossipingPropertyFileSnitch and Ec2MultiRegionSnitch
        own.put(ApplicationState.INTERNAL_IP, factory.internalIP(InetAddressAndPort.getByName("10.0.0.6").getAddress()));
        own.put(ApplicationState.RELEASE_VERSION, factory.releaseVersion("5.0.7.0-abc1234def"));
        own.put(ApplicationState.SSTABLE_VERSIONS, VersionedValue.unsafeMakeVersionedValue("bti-bb", 1));
        own.put(ApplicationState.INDEX_STATUS, factory.indexStatus("{}"));
        own.put(ApplicationState.DISK_USAGE, factory.diskUsage("spacious"));
        own.put(ApplicationState.DC, factory.datacenter("dc1"));
        own.put(ApplicationState.TOKENS, factory.tokens(tokens));
        EndpointState state = new EndpointState(new HeartBeatState(7, 8), own);

        Map<Integer, VersionedValue> sent = asDseReadsIt(state);
        Map<Integer, String> values = new HashMap<>();
        sent.forEach((ordinal, value) -> values.put(ordinal, value.value));
        // STATUS as DSE itself gossips it, with the token; STATUS_WITH_PORT is not sent next to it
        assertThat(values).containsEntry(DSE_ORDINALS.get("STATUS"), "NORMAL," + tokens.get(0))
                          .containsEntry(DSE_ORDINALS.get("NET_VERSION"), "12")
                          .containsEntry(DSE_ORDINALS.get("HOST_ID"), hostId.toString())
                          .containsEntry(DSE_ORDINALS.get("NATIVE_TRANSPORT_ADDRESS"), "10.0.0.5")
                          .containsEntry(DSE_ORDINALS.get("NATIVE_TRANSPORT_PORT"), "9042")
                          // INTERNAL_IP is written as it is when the endpoint has it, not the address of INTERNAL_ADDRESS_AND_PORT
                          .containsEntry(DSE_ORDINALS.get("INTERNAL_IP"), "10.0.0.6")
                          .containsEntry(DSE_ORDINALS.get("STORAGE_PORT"), "7000")
                          .containsEntry(DSE_ORDINALS.get("RELEASE_VERSION"), "5.0.7.0")
                          .containsEntry(DSE_ORDINALS.get("DISK_USAGE"), "spacious")
                          .containsEntry(DSE_ORDINALS.get("DC"), "dc1")
                          .containsEntry(DSE_ORDINALS.get("TOKENS"), own.get(ApplicationState.TOKENS).value);
        // nothing DSE cannot read: no sstable versions, no index status, no ordinal DSE reserves for SSL ports, JMX or padding
        assertThat(values.keySet()).allMatch(ordinal -> ordinal <= DSE_ORDINALS.get("DISK_USAGE"))
                                   .doesNotContain(DSE_ORDINALS.get("NATIVE_TRANSPORT_PORT_SSL"), DSE_ORDINALS.get("STORAGE_PORT_SSL"),
                                                   DSE_ORDINALS.get("JMX_PORT"), DSE_ORDINALS.get("SCHEMA_COMPATIBILITY_VERSION"));
        assertThat(values).hasSize(11);
    }

    @Test
    public void statusWithPortIsWrittenAsStatusWhenStatusIsGone() throws Exception
    {
        dse6();
        VersionedValue.VersionedValueFactory factory = factory();
        Map<ApplicationState, VersionedValue> own = new EnumMap<>(ApplicationState.class);
        own.put(ApplicationState.STATUS_WITH_PORT, factory.left(TOKENS, 1790256234000L));
        own.put(ApplicationState.NATIVE_ADDRESS_AND_PORT, factory.nativeaddressAndPort(InetAddressAndPort.getByName("10.0.0.5:9042")));
        Map<Integer, VersionedValue> sent = asDseReadsIt(new EndpointState(new HeartBeatState(7, 8), own));
        // LEFT keeps its token and expiry time, as DSE's handleStateLeft reads them
        assertThat(sent.get(DSE_ORDINALS.get("STATUS")).value).isEqualTo("LEFT," + TOKENS.get(0) + ",1790256234000");
        assertThat(sent.get(DSE_ORDINALS.get("NATIVE_TRANSPORT_ADDRESS")).value).isEqualTo("10.0.0.5");
        assertThat(sent.get(DSE_ORDINALS.get("NATIVE_TRANSPORT_PORT")).value).isEqualTo("9042");
        assertThat(sent).hasSize(3);
    }

    /*
     * The DSE peers of the cluster, as compatstatus lists them
     */

    @Test
    public void dsePeersAreTheOnesGossipingADseNetVersion() throws Exception
    {
        InetAddressAndPort dsePeer = InetAddressAndPort.getByName("127.0.0.203:7000");
        InetAddressAndPort hcdPeer = InetAddressAndPort.getByName("127.0.0.204:7000");
        InetAddressAndPort peer = InetAddressAndPort.getByName("127.0.0.205:7000");
        InetAddressAndPort dse6Peer = InetAddressAndPort.getByName("127.0.0.206:7000");
        InetAddressAndPort byHandshake = InetAddressAndPort.getByName("127.0.0.207:7000");
        InetAddressAndPort left = InetAddressAndPort.getByName("127.0.0.208:7000");
        InetAddressAndPort saved = InetAddressAndPort.getByName("127.0.0.209:7000");
        InetAddressAndPort rolledBack = InetAddressAndPort.getByName("127.0.0.200:7000");
        try
        {
            dse6();
            Gossiper.instance.endpointStateMap.put(dsePeer, read(dseEndpointState(capturedStates())));
            Gossiper.instance.endpointStateMap.put(hcdPeer, stateWithNetVersion("11"));
            Gossiper.instance.endpointStateMap.put(peer, stateWithNetVersion(String.valueOf(MessagingService.current_version)));
            // a node of this software in DSE_6 mode gossips 12 and a DSE_GOSSIP_STATE with a DSE version: not a DSE node
            EndpointState dse6State = stateWithNetVersion("12");
            dse6State.addApplicationState(ApplicationState.X_11_PADDING, factory().dseState("6.8.1"));
            Gossiper.instance.endpointStateMap.put(dse6Peer, dse6State);
            // seen in gossip but not identified yet: listed, with the ordinals gossip with it still uses
            assertThat(MessagingService.instance().getDsePeers())
            .containsEntry("127.0.0.203:7000", "release 4.0.0.6926, NET_VERSION 1536, DSE 6.9.26, gossip ordinals: this software's (not identified as DSE yet)")
            .hasSize(1);

            DsePeers.identify(dsePeer, "gossiped NET_VERSION 1536");
            DsePeers.identify(byHandshake, "DSE-flagged first handshake message");
            // a DSE node that left stays in gossip, and identified, until it is evicted: it is not a peer any more
            Map<ApplicationState, VersionedValue> leftStates = new EnumMap<>(ApplicationState.class);
            leftStates.put(ApplicationState.STATUS_WITH_PORT, factory().left(TOKENS, 1790256234000L));
            leftStates.put(ApplicationState.NET_VERSION, VersionedValue.unsafeMakeVersionedValue("1536", 2));
            Gossiper.instance.endpointStateMap.put(left, new EndpointState(new HeartBeatState(DSE_GENERATION, DSE_HEARTBEAT), leftStates));
            DsePeers.identify(left, "gossiped NET_VERSION 1536");
            // after a restart a saved peer has a state without NET_VERSION; a node rolled back to DSE may still have 12
            Gossiper.instance.endpointStateMap.put(saved, new EndpointState(new HeartBeatState(DSE_GENERATION, DSE_HEARTBEAT)));
            DsePeers.identify(saved, "handshake reply with the DSE messaging version 1536");
            Gossiper.instance.endpointStateMap.put(rolledBack, stateWithNetVersion("12"));
            DsePeers.identify(rolledBack, "DSE-flagged first handshake message");
            assertThat(MessagingService.instance().getDsePeers())
            .containsEntry("127.0.0.203:7000", "release 4.0.0.6926, NET_VERSION 1536, DSE 6.9.26, gossip ordinals: DSE's (gossiped NET_VERSION 1536)")
            .containsEntry("127.0.0.207:7000", "NET_VERSION unknown, gossip ordinals: DSE's (DSE-flagged first handshake message)")
            .containsEntry("127.0.0.209:7000", "NET_VERSION unknown, gossip ordinals: DSE's (handshake reply with the DSE messaging version 1536)")
            .containsEntry("127.0.0.200:7000", "NET_VERSION 12, gossip ordinals: DSE's (DSE-flagged first handshake message)")
            .hasSize(4);

            // with the mode off no peer is a DSE node this node talks to
            none();
            assertThat(MessagingService.instance().getDsePeers()).isEmpty();
        }
        finally
        {
            Gossiper.instance.endpointStateMap.remove(dsePeer);
            Gossiper.instance.endpointStateMap.remove(hcdPeer);
            Gossiper.instance.endpointStateMap.remove(peer);
            Gossiper.instance.endpointStateMap.remove(dse6Peer);
            Gossiper.instance.endpointStateMap.remove(left);
            Gossiper.instance.endpointStateMap.remove(saved);
            Gossiper.instance.endpointStateMap.remove(rolledBack);
        }
    }
}
