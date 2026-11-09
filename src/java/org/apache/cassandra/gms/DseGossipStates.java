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

import java.net.InetAddress;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import com.google.common.net.InetAddresses;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.DseCompat;
import org.apache.cassandra.net.DsePeers;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.JsonUtils;
import org.apache.cassandra.utils.NoSpamLogger;

/**
 * The application states exchanged with DataStax Enterprise 6 nodes, which gossip at messaging version 11 with their
 * own {@code ApplicationState} ordinals (DSE 6.8 and 6.9 alike):
 * <pre>
 *  0 STATUS                7 INTERNAL_IP                 14 NATIVE_TRANSPORT_READY      21 DISK_USAGE
 *  1 LOAD                  8 NATIVE_TRANSPORT_ADDRESS    15 NATIVE_TRANSPORT_PORT       22-31 X1-X10 (padding)
 *  2 SCHEMA                9 DSE_GOSSIP_STATE            16 NATIVE_TRANSPORT_PORT_SSL
 *  3 DC                   10 SEVERITY                    17 STORAGE_PORT
 *  4 RACK                 11 NET_VERSION                 18 STORAGE_PORT_SSL
 *  5 RELEASE_VERSION      12 HOST_ID                     19 JMX_PORT
 *  6 REMOVAL_COORDINATOR  13 TOKENS                      20 SCHEMA_COMPATIBILITY_VERSION
 * </pre>
 * Ordinals 0 to 14 mean the same on both sides (DSE's {@code NATIVE_TRANSPORT_ADDRESS} is this software's
 * {@code RPC_ADDRESS}, {@code DSE_GOSSIP_STATE} lands in {@code X_11_PADDING}). From 15 on they differ, so:
 * <ul>
 *   <li>received DSE states 15 to 20, which have no counterpart here, are kept in padding slots this software never
 *       uses ({@link #DSE_ONLY_SLOTS}), so that this node relays them to other nodes unchanged and writes them back
 *       at their DSE ordinal to DSE nodes; DSE's 21 is {@code DISK_USAGE}; DSE's padding (22 to 31) is dropped;</li>
 *   <li>the gossiper combines a DSE address with its port into this software's {@code NATIVE_ADDRESS_AND_PORT} and
 *       {@code INTERNAL_ADDRESS_AND_PORT} ({@link #deriveAddressesAndPorts}), also when a gossip delta carries only
 *       one of the two;</li>
 *   <li>states written to a DSE node are cut to DSE's ordinals: an address and a port come from the legacy address
 *       state ({@code RPC_ADDRESS}, {@code INTERNAL_IP}) and the DSE port slot when the endpoint has them, from the
 *       address-and-port state otherwise; {@code STATUS_WITH_PORT} becomes {@code STATUS} when the endpoint has no
 *       {@code STATUS}; states DSE does not know are not sent; no ordinal is written twice.</li>
 * </ul>
 * The two translations apply only to the gossip messages ({@code GossipDigestAck}, {@code GossipDigestAck2}) exchanged
 * at a pre-4.0 messaging version, which this software speaks only in DSE 6 compatibility mode, with a peer identified as
 * a DSE node ({@link DsePeers}, which documents how a peer is identified and what happens before it is); the acks sent to
 * a peer only presumed to be one carry no endpoint state ({@link DsePeers#isPresumed}). HCD 1.2 started
 * at version 11 is not such a peer: it exchanges the 4.0 line's ordinals at version 11, as at 4.0 and later, and so does
 * this node with it (as each side numbers them: HCD 1.2 has DISK_USAGE at 18 and SSTABLE_VERSIONS at 19, this software
 * the other way round, at every version; a difference older than the DSE 6 compatibility mode). The
 * serializer cannot tell the two apart, since it does not know the peer: a message to send is bound to its recipient
 * ({@link Recipient}), and a received one is translated by its verb handler, which knows the sender
 * ({@link #receivedFrom}).
 * <p>
 * Besides the translations, a peer identified as a DSE node is not sent all the states of this node when it asks for
 * some of them, unlike any other peer ({@link #sendsAllOwnStates}), and this node gossips a {@code DSE_GOSSIP_STATE}
 * of its own ({@link #publishedDseState}).
 */
final class DseGossipStates
{
    private static final Logger logger = LoggerFactory.getLogger(DseGossipStates.class);
    /** One budget per message format: a second endpoint with the same problem within the minute is not logged. */
    private static final NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(logger, 1, TimeUnit.MINUTES);

    /** DSE's ordinals of the states this translation needs by name. */
    static final int DSE_STATUS = 0;
    static final int DSE_INTERNAL_IP = 7;
    static final int DSE_NATIVE_TRANSPORT_ADDRESS = 8;
    static final int DSE_NATIVE_TRANSPORT_PORT = 15;
    static final int DSE_NATIVE_TRANSPORT_PORT_SSL = 16;
    static final int DSE_STORAGE_PORT = 17;
    static final int DSE_STORAGE_PORT_SSL = 18;
    static final int DSE_JMX_PORT = 19;
    static final int DSE_SCHEMA_COMPATIBILITY_VERSION = 20;
    static final int DSE_DISK_USAGE = 21;
    /** The first of DSE's padding ordinals; DSE never gossips a state at or above it. */
    static final int DSE_FIRST_PADDING = 22;

    /** The workload of a DSE node that runs neither search, analytics nor graph, as DSE writes it in its gossip state. */
    static final String DSE_WORKLOAD = "Cassandra";

    /**
     * Where a received DSE state without a counterpart in this software is kept, by DSE ordinal: DSE's 15 to 20 in
     * this software's padding states X2 to X7, in order (X1 is at ordinal 21, where DSE has DISK_USAGE, which is kept as
     * this software's DISK_USAGE).
     * {@code ApplicationState} marks these padding states as reserved while this mode exists.
     */
    static final Map<Integer, ApplicationState> DSE_ONLY_SLOTS = ImmutableMap.<Integer, ApplicationState>builder()
                                                                             .put(DSE_NATIVE_TRANSPORT_PORT, ApplicationState.X2)
                                                                             .put(DSE_NATIVE_TRANSPORT_PORT_SSL, ApplicationState.X3)
                                                                             .put(DSE_STORAGE_PORT, ApplicationState.X4)
                                                                             .put(DSE_STORAGE_PORT_SSL, ApplicationState.X5)
                                                                             .put(DSE_JMX_PORT, ApplicationState.X6)
                                                                             .put(DSE_SCHEMA_COMPATIBILITY_VERSION, ApplicationState.X7)
                                                                             .build();
    /** The reverse of {@link #DSE_ONLY_SLOTS}: the DSE ordinal a padding slot is written back at. */
    private static final Map<ApplicationState, Integer> SLOT_TO_DSE_ORDINAL = invert(DSE_ONLY_SLOTS);

    private static Map<ApplicationState, Integer> invert(Map<Integer, ApplicationState> slots)
    {
        Map<ApplicationState, Integer> inverse = new EnumMap<>(ApplicationState.class);
        slots.forEach((dseOrdinal, slot) -> inverse.put(slot, dseOrdinal));
        return Collections.unmodifiableMap(inverse);
    }

    /** The slot of the DSE native transport port, combined with {@code RPC_ADDRESS} into {@code NATIVE_ADDRESS_AND_PORT}. */
    static final ApplicationState NATIVE_TRANSPORT_PORT_SLOT = DSE_ONLY_SLOTS.get(DSE_NATIVE_TRANSPORT_PORT);
    /** The slot of the DSE storage port, combined with {@code INTERNAL_IP} into {@code INTERNAL_ADDRESS_AND_PORT}. */
    static final ApplicationState STORAGE_PORT_SLOT = DSE_ONLY_SLOTS.get(DSE_STORAGE_PORT);

    private DseGossipStates()
    {
    }

    /** The {@code ApplicationState} whose ordinal is the given DSE ordinal, for writing a state at that ordinal. */
    private static ApplicationState atDseOrdinal(int dseOrdinal)
    {
        return ApplicationState.values()[dseOrdinal];
    }

    /**
     * The states received from a DSE node, keyed by the constants of this software with the same ordinal, translated to
     * what they mean here (see the class comment).
     */
    static Map<ApplicationState, VersionedValue> fromDse(Map<ApplicationState, VersionedValue> received)
    {
        Map<ApplicationState, VersionedValue> states = new EnumMap<>(ApplicationState.class);
        for (Map.Entry<ApplicationState, VersionedValue> state : received.entrySet())
        {
            int dseOrdinal = state.getKey().ordinal();
            VersionedValue value = state.getValue();
            if (logger.isTraceEnabled())
                logger.trace("Storing the DSE state with ordinal {} and value {}", dseOrdinal, value);
            ApplicationState slot = DSE_ONLY_SLOTS.get(dseOrdinal);
            if (slot != null)
                states.put(slot, value);
            else if (dseOrdinal == DSE_DISK_USAGE)
                states.put(ApplicationState.DISK_USAGE, value);
            else if (dseOrdinal >= DSE_FIRST_PADDING)
                noSpamLogger.warn("Dropping the gossip state with DSE's padding ordinal {} (value '{}')", dseOrdinal, value.value);
            else
                states.put(state.getKey(), value);
        }
        return states;
    }

    /**
     * The endpoint states of a gossip message read at a pre-4.0 messaging version from {@code sender}, translated from
     * DSE's ordinals when the sender is a DSE node, as they are otherwise. Before that the sender's {@code NET_VERSION},
     * which has the same ordinal on both sides, is looked at ({@link #identify}): in its own state when the message
     * carries it, so that the first message a DSE node sends with its full state is already translated, or else in the
     * state this node stores for it.
     * <p>
     * From a sender that is not identified as a DSE node, the state of an endpoint with a bare number at any of the
     * ordinals 15 to 20, or with any value at ordinal 21, is left out: this software never has a bare number at 15 to 20
     * ({@code INTERNAL_ADDRESS_AND_PORT}, {@code NATIVE_ADDRESS_AND_PORT}, {@code STATUS_WITH_PORT},
     * {@code SSTABLE_VERSIONS}, {@code DISK_USAGE}, {@code INDEX_STATUS}) nor any value at 21 (the padding state
     * {@code X1}), and neither has HCD 1.2, so it is the state of a DSE node (DSE's ports, JMX port, schema compatibility
     * version and disk usage) sent by a DSE node not identified yet, or relayed by a node that stored DSE's values at
     * those ordinals (HCD 1.2 forced to version 11, which does not translate). Applied, those values would sit in this
     * software's states until the DSE node restarts, since a value is not sent again once this node's digest covers it;
     * left out, the endpoint's version does not move here, and the state is asked for again later.
     * <p>
     * A message without any state from a peer presumed to be a DSE node can end the presumption (see {@link DsePeers}):
     * when it answers a request (an ack2, or an ack asking for states itself, unlike the minimal ack of a node in its
     * shadow round), outside this node's shadow round, and this node holds an entry of the peer without its
     * {@code NET_VERSION}: its digest then asked for the peer's state, which a DSE node sends, while a node of this
     * software presuming this node to be a DSE node withholds it.
     *
     * @param answersRequest false for an ack that asks for no state (the only kind a node in its shadow round sends)
     */
    static Map<InetAddressAndPort, EndpointState> receivedFrom(InetAddressAndPort sender, Map<InetAddressAndPort, EndpointState> received, boolean answersRequest)
    {
        if (received.isEmpty() && answersRequest && DsePeers.isPresumed(sender) && !Gossiper.instance.isInShadowRound() && heldWithoutNetVersion(sender))
            DsePeers.refutePresumption(sender, "a gossip message without any endpoint state, which a DSE node does not send when asked for its own");
        identify(sender, received.get(sender));
        if (DsePeers.isDse(sender))
        {
            Map<InetAddressAndPort, EndpointState> translated = new HashMap<>(received.size());
            for (Map.Entry<InetAddressAndPort, EndpointState> e : received.entrySet())
                translated.put(e.getKey(), fromDse(e.getValue()));
            return translated;
        }
        Map<InetAddressAndPort, EndpointState> kept = null;
        for (Map.Entry<InetAddressAndPort, EndpointState> e : received.entrySet())
        {
            int dseOrdinal = dseOnlyValueOrdinal(e.getValue());
            if (dseOrdinal < 0)
                continue;
            if (kept == null)
                kept = new HashMap<>(received);
            kept.remove(e.getKey());
            noSpamLogger.info("Leaving out the gossip state of {} received from {}, a peer not identified as a DSE node: it has '{}' at ordinal {}, where DSE " +
                              "has a port, its schema compatibility version or its disk usage and this software never such a value; the state is asked for " +
                              "again later",
                              e.getKey(), sender, e.getValue().getApplicationState(atDseOrdinal(dseOrdinal)).value, dseOrdinal);
        }
        return kept == null ? received : kept;
    }

    /**
     * The first ordinal at which {@code state} has a value only DSE gossips there: a bare number at the ordinals 15 to 20,
     * where DSE has its ports and its schema compatibility version and this software never has a number, or any value at
     * 21, where DSE has its disk usage and this software the unused padding state {@code X1}; -1 when there is none.
     */
    private static int dseOnlyValueOrdinal(EndpointState state)
    {
        for (int dseOrdinal : DSE_ONLY_SLOTS.keySet())
        {
            VersionedValue value = state.getApplicationState(atDseOrdinal(dseOrdinal));
            if (value != null && !value.value.isEmpty() && value.value.chars().allMatch(c -> c >= '0' && c <= '9'))
                return dseOrdinal;
        }
        return state.getApplicationState(atDseOrdinal(DSE_DISK_USAGE)) != null ? DSE_DISK_USAGE : -1;
    }

    /** True when this node holds an entry of {@code peer} (its digests cover it) without a {@code NET_VERSION}. */
    private static boolean heldWithoutNetVersion(InetAddressAndPort peer)
    {
        EndpointState stored = Gossiper.instance.getEndpointStateForEndpoint(peer);
        return stored != null && stored.getApplicationState(ApplicationState.NET_VERSION) == null;
    }

    /** The {@code NET_VERSION} this node stores for {@code peer}, or null. */
    @Nullable
    private static VersionedValue storedNetVersion(InetAddressAndPort peer)
    {
        EndpointState stored = Gossiper.instance.getEndpointStateForEndpoint(peer);
        return stored == null ? null : stored.getApplicationState(ApplicationState.NET_VERSION);
    }

    /**
     * Identifies {@code peer} by its {@code NET_VERSION}. The one of {@code state}, when it has one, is authoritative
     * both ways ({@link DsePeers#fromGossipedNetVersion}). Otherwise the one this node stores for the peer can only
     * identify it as a DSE node: it may be older than a handshake that identified the peer (a node of this software
     * rolled back to DSE on the same address), so it never clears a record. It does clear a mere presumption (a stored
     * DSE 6 release, older still) when it is 12 or more.
     */
    static void identify(InetAddressAndPort peer, @Nullable EndpointState state)
    {
        VersionedValue netVersion = state == null ? null : state.getApplicationState(ApplicationState.NET_VERSION);
        if (netVersion != null)
        {
            DsePeers.fromGossipedNetVersion(peer, netVersion.value);
            return;
        }
        VersionedValue storedNetVersion = storedNetVersion(peer);
        if (storedNetVersion == null)
            return;
        try
        {
            int version = Integer.parseInt(storedNetVersion.value);
            if (version >= DseCompat.DSE_HANDSHAKE_MIN)
                DsePeers.identify(peer, "gossiped NET_VERSION " + version);
            else if (version >= MessagingService.VERSION_40)
                DsePeers.refutePresumption(peer, "stored NET_VERSION " + version);
        }
        catch (NumberFormatException ignored)
        {
            // a value that is not a number says nothing
        }
    }

    /**
     * Whether this node answers a gossip digest of {@code peer} about this node with all of its own states, whatever
     * version the digest asks for, as this software does since CASSANDRA-19983: always, except in DSE 6 compatibility
     * mode to a peer identified as a DSE node, which is sent the states above the version its digest asks for, as before
     * CASSANDRA-19983 (DSE 6.8.65 was observed answering the digests of the other DSE nodes that way).
     * <p>
     * DSE fires its state listeners for every state a gossip message carries, whether or not it already has it at that
     * version, where this software skips the states it has ({@code Gossiper.applyNewStates}). As observed, every gossip
     * round between a DSE 6.8.65 node and a node of this software carried all the states of the latter, so
     * each DSE node handled its {@code STATUS} as a new one ("state jump to NORMAL"), updated its topology for its
     * {@code DC} and {@code RACK}, asked for a schema pull and rewrote its peer row, about every 1.3 seconds. A DSE node
     * does not need the whole state: its digest asks for all of it when it has none of it or an older generation, which
     * is answered in full ({@code Gossiper.examineGossiper}). What it loses is the repair CASSANDRA-19983 brought, of a
     * peer that holds this node's state at a version as high as this node's with states missing: DSE nodes do not have
     * it with each other either.
     * <p>
     * The gossiped {@code NET_VERSION} this node stores for the peer identifies it first, as when a message to it is
     * written ({@link Recipient}), so that the states sent to a DSE node and their ordinals are decided alike.
     */
    static boolean sendsAllOwnStates(InetAddressAndPort peer)
    {
        if (!DseCompat.enabled())
            return true;
        identify(peer, null);
        return !DsePeers.isDse(peer);
    }

    /**
     * The JSON this node gossips in DSE 6 compatibility mode as DSE's {@code DSE_GOSSIP_STATE} (ordinal 9, this software's
     * {@code X_11_PADDING}), with {@code dseVersion} ({@link DseCompat#gossipedDseVersion}), so that DSE nodes do not send
     * it a read only DSE nodes understand.
     * <p>
     * DSE sends the range reads of a storage-attached index query as {@code READS.MULTI_RANGE_READ}, which it cannot
     * write to a node speaking the pre-4.0 dialect ("Cannot write message READS.MULTI_RANGE_READ ... to legacy node",
     * after which the connection is closed and the read times out), when every live node gossips a {@code dse_version}
     * of 6.8.2 or later. DSE's code suggests that a node without one makes DSE fall back to {@code READS.RANGE_READ};
     * for a node that ran DSE before, it does not. As observed with DSE 6.8.65, the DSE nodes kept the endpoint grouping on
     * for an upgraded node that gossiped no {@code DSE_GOSSIP_STATE}, also after they restarted, and for one whose state
     * had no {@code dse_version} or a null one, while their {@code system.peers} kept the {@code dse_version} of the
     * node's DSE days: DSE apparently falls back to that value, which nothing clears (observed, not confirmed in DSE's
     * code). With a {@code dse_version} below 6.8.2 DSE disables the endpoint grouping ("One or more nodes have not
     * reached DSE >= 6.8.2. Endpoint grouping range read will be disabled."), stores that version in its
     * {@code system.peers} and reads with {@code READS.RANGE_READ}. DSE fails on a version it cannot parse ("Invalid
     * version value"), every second.
     * <p>
     * DSE then counts this node as a DSE node at that version, as it did with the stale value: the DSE features gated at
     * that version or below stay on, among them NodeSync, whose {@code READS.NODESYNC} DSE cannot write to this node
     * either, so NodeSync must stay disabled during the upgrade; the features DSE reserves to clusters at a later
     * version are off, as with a DSE node at that version in the cluster. On a DSE 6.9 cluster DSE logs "Detected DSE
     * version downgrade of a node" and, as its feature gates read,
     * deactivates the features above that version it had active; not observed with DSE 6.9. The other entries say what
     * this node is, as DSE writes them (with the types of the DSE 6.9.26 capture): the cluster name and partitioner,
     * the {@code Cassandra} workload, no graph, and {@code active} as a DSE node that runs. DSE's server id and node
     * health are left out: DSE keeps the server id of its {@code system.peers} (observed), and reads a missing health
     * as 0.0, the lowest, as before this state existed, and such a node stays eligible for requests.
     */
    static String publishedDseState(String dseVersion)
    {
        Map<String, Object> state = new LinkedHashMap<>();
        state.put("cluster_name", DatabaseDescriptor.getClusterName());
        state.put("dse_version", dseVersion);
        state.put("graph", false);
        state.put("workload", DSE_WORKLOAD);
        state.put("workloads", DSE_WORKLOAD);
        state.put("active", "true");
        state.put("partitioner_name", DatabaseDescriptor.getPartitionerName());
        return JsonUtils.writeAsJsonString(state);
    }

    /** An endpoint state received from a DSE node, with its states translated by {@link #fromDse(Map)}. */
    static EndpointState fromDse(EndpointState received)
    {
        Map<ApplicationState, VersionedValue> states = new EnumMap<>(ApplicationState.class);
        for (Map.Entry<ApplicationState, VersionedValue> state : received.states())
            states.put(state.getKey(), state.getValue());
        return new EndpointState(received.getHeartBeatState(), fromDse(states));
    }

    /**
     * The recipient of a gossip message to send, in DSE 6 compatibility mode, or null outside it: the message's endpoint
     * states are written with DSE's ordinals when it is sent at a pre-4.0 version to a peer identified as a DSE node, and
     * left out when it is sent at such a version to a peer only presumed to be one ({@link DsePeers#isPresumed}).
     */
    @Nullable
    static Recipient recipient(InetAddressAndPort peer)
    {
        return DseCompat.enabled() ? new Recipient(peer, false) : null;
    }

    /**
     * As {@link #recipient}, for the answer to a shadow round, whose states are never left out: they go with DSE's ordinals
     * to a peer presumed to be a DSE node as well (see {@link DsePeers}).
     */
    @Nullable
    static Recipient shadowRoundRecipient(InetAddressAndPort peer)
    {
        return DseCompat.enabled() ? new Recipient(peer, true) : null;
    }

    /**
     * The recipient of one gossip message. Whether the message is written with DSE's ordinals, and whether its states are
     * left out, is decided the first time it is sized or written at a pre-4.0 version, that is on the connection to the
     * peer, once the handshake that may have identified the peer is over, and it does not change afterwards: the size and
     * the bytes of a message must agree.
     */
    static final class Recipient
    {
        private final InetAddressAndPort peer;
        private final boolean shadowRoundAnswer;
        private Boolean dseOrdinals;
        private boolean withheld;

        private Recipient(InetAddressAndPort peer, boolean shadowRoundAnswer)
        {
            this.peer = peer;
            this.shadowRoundAnswer = shadowRoundAnswer;
        }

        /** Decides, once, whether the states go with DSE's ordinals or are left out; at a pre-4.0 version only. */
        private void decide()
        {
            if (dseOrdinals != null)
                return;
            // the gossiped NET_VERSION this node stores for the peer identifies it too
            identify(peer, null);
            // the presumption first: identifying the peer records it before it ends the presumption, so a peer identified
            // between the two reads is still written to with DSE's ordinals
            boolean presumed = DsePeers.isPresumed(peer);
            boolean dse = DsePeers.isDse(peer);
            if (shadowRoundAnswer)
            {
                dseOrdinals = dse || presumed;
                withheld = false;
            }
            else
            {
                dseOrdinals = dse;
                withheld = presumed && !dse;
            }
        }

        /** Synchronized: the first two calls must not decide differently, whichever threads make them. */
        synchronized boolean dseOrdinals(int version)
        {
            if (version >= MessagingService.VERSION_40)
                return false;
            decide();
            return dseOrdinals;
        }

        /**
         * True when the message goes at a pre-4.0 version to a peer only presumed to be a DSE node: it carries no
         * endpoint state. Decided with {@link #dseOrdinals}, once.
         */
        synchronized boolean withholdsStates(int version)
        {
            if (version >= MessagingService.VERSION_40)
                return false;
            decide();
            return withheld;
        }
    }

    /** The states of an endpoint as a DSE node reads them: at DSE's ordinals, each at most once (see the class comment). */
    static Map<ApplicationState, VersionedValue> toDse(Set<Map.Entry<ApplicationState, VersionedValue>> states)
    {
        Map<ApplicationState, VersionedValue> byState = new EnumMap<>(ApplicationState.class);
        for (Map.Entry<ApplicationState, VersionedValue> state : states)
            byState.put(state.getKey(), state.getValue());

        Map<ApplicationState, VersionedValue> out = new EnumMap<>(ApplicationState.class);
        for (Map.Entry<ApplicationState, VersionedValue> state : byState.entrySet())
        {
            VersionedValue value = state.getValue();
            Integer dseOrdinal = SLOT_TO_DSE_ORDINAL.get(state.getKey());
            if (dseOrdinal != null)
            {
                out.put(atDseOrdinal(dseOrdinal), value);
                continue;
            }
            switch (state.getKey())
            {
                case STATUS_WITH_PORT:
                    // STATUS carries the value DSE expects at ordinal 0; only a node without it is described by this one
                    if (!byState.containsKey(ApplicationState.STATUS))
                        out.put(atDseOrdinal(DSE_STATUS), VersionedValue.unsafeMakeVersionedValue(withoutPorts(value.value), value.version));
                    break;
                case INTERNAL_IP:
                    out.put(atDseOrdinal(DSE_INTERNAL_IP), value);
                    break;
                case RPC_ADDRESS:
                    out.put(atDseOrdinal(DSE_NATIVE_TRANSPORT_ADDRESS), value);
                    break;
                case INTERNAL_ADDRESS_AND_PORT:
                    putAddressAndPort(value,
                                      byState.containsKey(ApplicationState.INTERNAL_IP) ? -1 : DSE_INTERNAL_IP,
                                      byState.containsKey(STORAGE_PORT_SLOT) ? -1 : DSE_STORAGE_PORT, out);
                    break;
                case NATIVE_ADDRESS_AND_PORT:
                    putAddressAndPort(value,
                                      byState.containsKey(ApplicationState.RPC_ADDRESS) ? -1 : DSE_NATIVE_TRANSPORT_ADDRESS,
                                      byState.containsKey(NATIVE_TRANSPORT_PORT_SLOT) ? -1 : DSE_NATIVE_TRANSPORT_PORT, out);
                    break;
                case DISK_USAGE:
                    out.put(atDseOrdinal(DSE_DISK_USAGE), value);
                    break;
                case RELEASE_VERSION:
                    out.put(ApplicationState.RELEASE_VERSION, EndpointStateSerializer.filterValue(ApplicationState.RELEASE_VERSION, value, DseCompat.DSE_DIALECT));
                    break;
                default:
                    // SSTABLE_VERSIONS, INDEX_STATUS and unused padding have no DSE counterpart; everything below 15 is shared
                    if (state.getKey().ordinal() < DSE_NATIVE_TRANSPORT_PORT)
                        out.put(state.getKey(), value);
            }
        }
        return out;
    }

    /**
     * Writes an {@code address:port} value as DSE's address state when {@code dseAddressOrdinal >= 0} and as its port
     * state when {@code dsePortOrdinal >= 0}: a piece the endpoint also has in a legacy state or a DSE port slot is
     * written from there, with its own version.
     */
    private static void putAddressAndPort(VersionedValue value, int dseAddressOrdinal, int dsePortOrdinal, Map<ApplicationState, VersionedValue> out)
    {
        if (dseAddressOrdinal < 0 && dsePortOrdinal < 0)
            return;
        InetAddressAndPort parsed = parseLiteral(value.value, null);
        if (parsed == null)
        {
            noSpamLogger.warn("Not sending the address '{}' to DSE nodes: not an IP address with an optional port", value.value);
            return;
        }
        if (dseAddressOrdinal >= 0)
            out.put(atDseOrdinal(dseAddressOrdinal), VersionedValue.unsafeMakeVersionedValue(parsed.getAddress().getHostAddress(), value.version));
        if (dsePortOrdinal >= 0 && hasPort(value.value))
            out.put(atDseOrdinal(dsePortOrdinal), VersionedValue.unsafeMakeVersionedValue(String.valueOf(parsed.getPort()), value.version));
    }

    /** A status value with every {@code address:port} piece cut to the address, as a pre-4.0 {@code STATUS} carries it. */
    static String withoutPorts(String status)
    {
        String[] pieces = status.split(VersionedValue.DELIMITER_STR, -1);
        for (int i = 1; i < pieces.length; i++)
        {
            // only an IP address with a port is cut; a token, a host id or a bare address is left as it is
            InetAddressAndPort address = hasPort(pieces[i]) ? parseLiteral(pieces[i], null) : null;
            if (address != null)
                pieces[i] = address.getAddress().getHostAddress();
        }
        return String.join(VersionedValue.DELIMITER_STR, pieces);
    }

    private static boolean hasPort(String value)
    {
        try
        {
            return HostAndPort.fromString(value).hasPort();
        }
        catch (IllegalArgumentException e)
        {
            return false;
        }
    }

    /**
     * An IP address literal with an optional port ({@code 10.0.0.1}, {@code 10.0.0.1:7000}, {@code [2001:db8::7]:7000}),
     * without any name resolution; {@code defaultPort} when the value has no port; null when the value is anything else.
     */
    @Nullable
    static InetAddressAndPort parseLiteral(String value, @Nullable Integer defaultPort)
    {
        try
        {
            HostAndPort hap = HostAndPort.fromString(value);
            if (!InetAddresses.isInetAddress(hap.getHost()))
                return null;
            InetAddress address = InetAddresses.forString(hap.getHost());
            return InetAddressAndPort.getByAddressOverrideDefaults(address, hap.hasPort() ? Integer.valueOf(hap.getPort()) : defaultPort);
        }
        catch (IllegalArgumentException e)
        {
            return null;
        }
    }

    /**
     * Adds to a state received for {@code endpoint} the address-and-port states of this software that its DSE address
     * and port states describe, taking the piece a gossip delta does not carry from what this node already stores
     * ({@code stored}, null for an endpoint seen for the first time). Called by the gossiper for every received
     * endpoint state in DSE 6 compatibility mode; a state without DSE port slots is left alone.
     */
    static void deriveAddressesAndPorts(InetAddressAndPort endpoint, EndpointState received, @Nullable EndpointState stored)
    {
        derive(endpoint, received, stored, ApplicationState.RPC_ADDRESS, NATIVE_TRANSPORT_PORT_SLOT, ApplicationState.NATIVE_ADDRESS_AND_PORT);
        derive(endpoint, received, stored, ApplicationState.INTERNAL_IP, STORAGE_PORT_SLOT, ApplicationState.INTERNAL_ADDRESS_AND_PORT);
    }

    private static void derive(InetAddressAndPort endpoint, EndpointState received, @Nullable EndpointState stored,
                               ApplicationState addressState, ApplicationState portSlot, ApplicationState addressAndPortState)
    {
        VersionedValue receivedAddress = received.getApplicationState(addressState);
        VersionedValue receivedPort = received.getApplicationState(portSlot);
        if (receivedAddress == null && receivedPort == null)
            return;

        VersionedValue address = receivedAddress;
        String addressValue = address == null ? null : address.value;
        if (address == null && stored != null)
        {
            address = stored.getApplicationState(addressState);
            if (address != null)
            {
                addressValue = address.value;
            }
            else
            {
                // the legacy address state is dropped once the combined one exists: take the address from that
                address = stored.getApplicationState(addressAndPortState);
                InetAddressAndPort previous = address == null ? null : parseLiteral(address.value, null);
                addressValue = previous == null ? null : previous.getAddress().getHostAddress();
            }
        }
        VersionedValue port = receivedPort != null ? receivedPort : stored == null ? null : stored.getApplicationState(portSlot);
        if (addressValue == null || port == null)
            return; // the other piece has not arrived yet: the address alone stays in its legacy state

        InetAddressAndPort combined = parseLiteral(addressValue, null);
        int portNumber;
        try
        {
            portNumber = Integer.parseInt(port.value);
        }
        catch (NumberFormatException e)
        {
            portNumber = -1;
        }
        if (combined == null || portNumber < 0 || portNumber > 65535)
        {
            noSpamLogger.warn("Ignoring the {} '{}' and port '{}' gossiped for {}: not an IP address and a port",
                              addressState, addressValue, port.value, endpoint);
            return;
        }
        combined = InetAddressAndPort.getByAddressOverrideDefaults(combined.getAddress(), portNumber);
        int version = Math.max(address.version, port.version);
        VersionedValue already = received.getApplicationState(addressAndPortState);
        if (already != null && already.version >= version)
            return;
        received.addApplicationState(addressAndPortState, VersionedValue.unsafeMakeVersionedValue(combined.getHostAddressAndPort(), version));
    }
}
