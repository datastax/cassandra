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

import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.DsePeersRecord;
import org.apache.cassandra.service.DseUpgradeMarker;
import org.apache.cassandra.utils.FBUtilities;

/**
 * The peers this node has identified as DataStax Enterprise 6 nodes, in DSE 6 compatibility mode. At messaging version
 * 11 a DSE node and an HCD 1.2 node forced to that version use the same message layouts, but not the same gossip:
 * DSE numbers its application states differently from ordinal 15 on, while the 4.0
 * line exchanges its own ordinals, as at 4.0 and later. Gossip states are translated to and from DSE's ordinals only
 * for the peers recorded here; any other peer at a pre-4.0 version keeps this software's ordinals.
 * <p>
 * A peer is recorded as a DSE node on any of this evidence, which no other kind of peer produces:
 * <ol>
 *   <li>its first handshake message is one only DSE sends, followed by a third message on the same socket: DSE-flagged
 *       ({@code 0xFF} and its relative version, {@link DseCompat#isDseFlaggedInitiate}), or pre-4.0-shaped and
 *       requesting version 12, which DSE sends once it has read this node's gossiped {@code NET_VERSION}, while a 3.x
 *       or HCD 1.2 node never requests more than 11 in that shape. DSE 6.9.26 sends the third message on the same
 *       socket after either first message, answered with 11, as DSE's code has it, so a DSE 6.9.26 node is identified
 *       here by a connection it opens with one of them (every DSE 6.9.26 connection to an upgraded node was seen to
 *       open with the flagged one). DSE 6.8.65 does not: it closes the socket without a third message and
 *       reconnects at 11 on a new one, with a plain first message that identifies nothing, and is identified by the
 *       evidence below;</li>
 *   <li>it answers our 4.0-style first handshake message with a DSE messaging version
 *       ({@link DseCompat#isDseHandshakeVersion});</li>
 *   <li>it answers our pre-4.0 first handshake message at 11 with a DSE messaging version instead of echoing 11, as
 *       observed with DSE 6.8.65 (DSE 6.9.26 echoes 11, which identifies nothing);</li>
 *   <li>gossip carries a DSE {@code NET_VERSION} (256 or more) for it, whichever node relayed the state:
 *       {@code NET_VERSION} has ordinal 11 in DSE and here, so it is read correctly before the relaying peer's
 *       dialect is known. This is how a DSE node first heard of through another node is identified before any
 *       direct handshake with it. The value is looked at when the storage service applies it (for a member of the
 *       ring whose state is not dead, and when the node restarts) and, for any endpoint this node stores a state
 *       of, whenever a gossip message is exchanged with it at a pre-4.0 version;</li>
 *   <li>a gossip message it sends at a pre-4.0 version carries its own state with a DSE {@code NET_VERSION}: that
 *       state is read before the rest of the message is translated.</li>
 * </ol>
 * A gossiped {@code NET_VERSION} is authoritative both ways: one below 256 (11 from HCD 1.2 at the dialect, 12 or
 * more from this software and the 4.0 line) clears the record. A handshake cannot clear it: a DSE node that stored 11
 * for this node reconnects with a plain pre-4.0 first message at 11, which looks exactly like HCD 1.2's, so the record
 * survives reconnects. It is forgotten when the gossiper removes or evicts the endpoint, and it is not persisted: after
 * a restart a DSE peer is identified again by the first gossip message carrying its state, or by the handshake when this
 * node does not know its version and sends the 4.0-style first message, which DSE answers with its own version, or
 * when this node sends a DSE 6.8.65 peer the pre-4.0 first message, which it answers with its own version (see the
 * third item), or by an inbound first message only DSE sends when a third message follows, as DSE 6.9.26 sends it and
 * DSE 6.8.65 does not (see the first item). The record of peers at a pre-4.0 version that {@code DsePeersRecord}
 * persists for the startup checks does not tell a DSE node from HCD 1.2 at the dialect, so it neither identifies nor
 * presumes a peer.
 * <p>
 * A peer stored with a DSE 6 release ({@link DseDialectVersions}) is first connected to at the dialect, which
 * DSE 6.9.26 answers with 11, like HCD 1.2: that handshake identifies nothing, while the 4.0-style probe used to
 * identify a DSE peer before any gossip message could be sent to it (DSE 6.8.65 answers with its own version, which
 * identifies it: the third item). Such a peer is <em>presumed</em> to be a DSE node instead ({@link #presume}): the
 * gossip acks this node sends it carry no endpoint state at all, only digests, since this node's ordinals would make
 * DSE fail on its port states and DSE's would be misread, some states dropped, by a node of this software the stale
 * release took for DSE. The states it sends are read as from a peer not identified. The presumption ends with the first
 * of:
 * <ul>
 *   <li>any evidence above, which turns it into a record: a DSE node sends its own state in answer to this node's gossip
 *       (after a restart this node has none of it, and asks for it);</li>
 *   <li>a {@code NET_VERSION} of 12 or more, gossiped or stored for the peer, which clears it ({@link #refutePresumption});</li>
 *   <li>a gossip ack or ack2 from the peer carrying no endpoint state at all in answer to a request, outside a shadow
 *       round, while this node holds an entry of the peer without its {@code NET_VERSION}: this node's digest asked for
 *       the peer's state, which DSE sends, while a node of this software that presumes this node to be a DSE node
 *       withholds it ({@code DseGossipStates.receivedFrom}). This ends the case of two nodes of this software each
 *       presuming the other (both restarted with stale releases of each other, as in an upgrade of the whole cluster at
 *       once), where neither would otherwise send its state.</li>
 * </ul>
 * In practice that costs one gossip round after this node starts, for a DSE 6.9.26 peer not identified by its own first
 * connection (none for DSE 6.8.65, which the answer to the first connection to it identifies). No ordinary sequence
 * makes a DSE node send such a message (a node in its shadow round asks for nothing, and a DSE node sends its own state
 * when this node's digest asks for it); only a race could, such as the entry of the peer being created here between
 * this node's message and the answer. The peer is then treated as not identified yet, as without the presumption, until
 * its own state identifies it (the failure mode below).
 * <p>
 * The answer to a shadow round is never withheld: it goes with DSE's ordinals to a presumed peer, so that it cannot be
 * taken for the minimal ack of a seed in its own shadow round. A node of this software reads it as a regular ack from a
 * DSE node not identified: every endpoint state with a bare number at the ordinals 15 to 20, or any value at 21, is left
 * out ({@code DseGossipStates.receivedFrom}), which is every live endpoint including this node, and only saved entries
 * (host id, tokens) remain. Its shadow round ends with those incomplete states; they are only used for its startup
 * checks (the endpoint collision check, which passes without a state of the node itself, and the snitch check, which
 * reads the saved information of fewer endpoints), and by a replacement, which is not part of the upgrade.
 * <p>
 * Failure mode: a DSE peer not identified yet is treated like HCD 1.2, with this software's ordinals. That takes a DSE
 * node that reconnected with a plain first message at 11 or less, of which this node stores no {@code NET_VERSION},
 * whose answers to this node's first messages carried no DSE messaging version (DSE 6.9.26 echoes the pre-4.0 one), and
 * a gossip message that does not carry the node's own state: in practice the first exchange after this node restarted
 * (it then stores an empty state for every saved peer), when the DSE node still has 11 stored for it. Then, unless the
 * node is presumed (stored with a DSE 6 release, whose acks carry no endpoint state), the states this node sends it at
 * ordinals 15 and above are misread, and DSE drops the message when its port states are not numbers; of the states it
 * receives, presumed or not, those of DSE nodes are left out whole, recognised by the values DSE has where this
 * software never has them (see {@code DseGossipStates.receivedFrom}), and asked for again later. It lasts until the
 * first gossip exchange that carries the node's own state, which identifies it: at the latest the answer to this node's
 * request for the state of a node whose state it does not have.
 */
public final class DsePeers
{
    private static final Logger logger = LoggerFactory.getLogger(DsePeers.class);

    /** The identified DSE peers, with the evidence that identified them first. */
    private static final ConcurrentMap<InetAddressAndPort, String> IDENTIFIED = new ConcurrentHashMap<>();
    /** The peers presumed to be DSE nodes (see the class comment), with the reason. */
    private static final ConcurrentMap<InetAddressAndPort, String> PRESUMED = new ConcurrentHashMap<>();

    private DsePeers()
    {
    }

    /**
     * Records {@code peer} as a DSE 6 node, on the evidence {@code evidence} describes; nothing happens outside DSE 6
     * compatibility mode or when the peer is identified already.
     */
    public static void identify(InetAddressAndPort peer, String evidence)
    {
        if (!DseCompat.enabled())
            return;
        if (IDENTIFIED.putIfAbsent(peer, evidence) == null)
            logger.info("Identified {} as a DSE 6 node ({}): gossip with it uses DSE's application-state ordinals", peer, evidence);
        PRESUMED.remove(peer);
    }

    /**
     * Presumes {@code peer} to be a DSE 6 node, for the reason {@code reason} gives: the gossip acks sent to it carry no
     * endpoint state until it is identified or cleared (see the class comment). Nothing happens outside DSE 6
     * compatibility mode or when the peer is identified already.
     */
    public static void presume(InetAddressAndPort peer, String reason)
    {
        if (!DseCompat.enabled() || IDENTIFIED.containsKey(peer))
            return;
        if (PRESUMED.putIfAbsent(peer, reason) == null)
            logger.debug("Presuming {} to be a DSE 6 node ({}) until its gossip state says: the gossip acks sent to it carry no endpoint state",
                         peer, reason);
    }

    /**
     * Clears the presumption that {@code peer} is a DSE 6 node, for the reason {@code evidence} gives (see the class
     * comment); a record is left alone, and nothing happens outside DSE 6 compatibility mode.
     */
    public static void refutePresumption(InetAddressAndPort peer, String evidence)
    {
        if (!DseCompat.enabled())
            return;
        String presumed = PRESUMED.remove(peer);
        if (presumed != null)
            logger.info("{} is not a DSE 6 node ({}; it was presumed one: {}): gossip with it uses this software's application-state ordinals",
                        peer, evidence, presumed);
    }

    /** Clears the record or the presumption of {@code peer}: gossip says it is not a DSE node. Nothing happens otherwise. */
    private static void notDse(InetAddressAndPort peer, String evidence)
    {
        String previous = IDENTIFIED.remove(peer);
        if (previous != null)
            logger.info("{} is not a DSE 6 node ({}; it was identified by {}): gossip with it uses this software's application-state ordinals",
                        peer, evidence, previous);
        String presumed = PRESUMED.remove(peer);
        if (presumed != null)
            logger.info("{} is not a DSE 6 node ({}; it was presumed one: {}): gossip with it uses this software's application-state ordinals",
                        peer, evidence, presumed);
    }

    /** Forgets {@code peer}: the gossiper removed or evicted it. */
    public static void forget(InetAddressAndPort peer)
    {
        if (!DseCompat.enabled())
            return;
        if (IDENTIFIED.remove(peer) != null)
            logger.debug("Forgot the DSE 6 identification of {}", peer);
        PRESUMED.remove(peer);
    }

    /** True when {@code peer} was identified as a DSE 6 node, in DSE 6 compatibility mode. */
    public static boolean isDse(InetAddressAndPort peer)
    {
        return DseCompat.enabled() && IDENTIFIED.containsKey(peer);
    }

    /**
     * True when {@code peer} is presumed to be a DSE 6 node and not identified as one, in DSE 6 compatibility mode: the
     * gossip acks sent to it carry no endpoint state.
     */
    public static boolean isPresumed(InetAddressAndPort peer)
    {
        return DseCompat.enabled() && PRESUMED.containsKey(peer) && !IDENTIFIED.containsKey(peer);
    }

    /**
     * Records what a gossiped {@code NET_VERSION} of {@code endpoint} says about it (see the class comment): a DSE node
     * for a value of 256 or more, not one for a smaller value; a value that is not a number says nothing. Nothing
     * happens outside DSE 6 compatibility mode.
     */
    public static void fromGossipedNetVersion(InetAddressAndPort endpoint, String netVersion)
    {
        if (!DseCompat.enabled())
            return;
        int version;
        try
        {
            version = Integer.parseInt(netVersion);
        }
        catch (NumberFormatException e)
        {
            return;
        }
        fromGossipedNetVersion(endpoint, version);
    }

    /** As {@link #fromGossipedNetVersion(InetAddressAndPort, String)}, for a value already parsed. */
    public static void fromGossipedNetVersion(InetAddressAndPort endpoint, int version)
    {
        if (!DseCompat.enabled())
            return;
        if (version >= DseCompat.DSE_HANDSHAKE_MIN)
            identify(endpoint, "gossiped NET_VERSION " + version);
        else
            notDse(endpoint, "gossiped NET_VERSION " + version);
    }

    /**
     * Records the sender of a pre-4.0 first handshake message, named {@code peer} by the third message, as a DSE node
     * when only DSE sends such a first message: DSE-flagged, or requesting the version this node gossips as its
     * {@code NET_VERSION} in DSE 6 compatibility mode, 12 (see the class comment). Reached with DSE 6.9.26, which sends
     * the third message on the same socket after either of those first messages, as its logs of the identifications
     * made here show; not with DSE 6.8.65, which sends no third message after them but closes the socket and reconnects
     * at 11.
     */
    static void fromPre40Initiate(InetAddressAndPort peer, int requestedVersion)
    {
        if (requestedVersion == DseCompat.DSE_VERSION_BYTE)
            identify(peer, "DSE-flagged first handshake message");
        else if (requestedVersion == DseCompat.NET_VERSION_PUBLISHED)
            identify(peer, "pre-4.0 first handshake message requesting version " + requestedVersion);
    }

    /**
     * Records {@code peer} as a DSE node when the 4-byte reply to our first handshake message carries a DSE messaging
     * version ({@link DseCompat#isDseHandshakeVersion}): a 3.x or HCD 1.2 node answers a version below 256, never
     * {@code relative << 8}.
     *
     * @param toPre40FirstMessage whether our first message was the pre-4.0 one rather than the 4.0-style one, which the
     *                            evidence tells apart
     */
    static void fromPre40Reply(InetAddressAndPort peer, int peerVersion, boolean toPre40FirstMessage)
    {
        if (DseCompat.isDseHandshakeVersion(peerVersion))
            identify(peer, (toPre40FirstMessage ? "pre-4.0 handshake reply" : "handshake reply") + " with the DSE messaging version " + peerVersion);
    }

    /**
     * True when the gossip state of a node says it left the ring: decommissioned ({@code LEFT}) or removed
     * ({@code REMOVED_TOKEN}). Such a node stays in gossip for days, and a DSE node gone that way is no longer a DSE
     * peer. A node that is only down, hibernating (being replaced) or whose removal is still streaming
     * ({@code REMOVING_TOKEN}) still is one. Used by {@code nodetool compatstatus} and {@link #knownAtStartup};
     * a null state has not left.
     */
    public static boolean hasLeftTheRing(EndpointState state)
    {
        String status = Gossiper.getGossipStatus(state);
        return VersionedValue.STATUS_LEFT.equals(status) || VersionedValue.REMOVED_TOKEN.equals(status);
    }

    /**
     * DSE's own messaging version (256 or more) that the gossip state of a node carries as its {@code NET_VERSION}, or -1
     * when the state is null or carries none, a smaller value or one that is not a number. The test of "gossips a DSE
     * version" used by {@code nodetool compatstatus} and {@link #knownAtStartup}.
     */
    public static int gossipedDseVersion(EndpointState state)
    {
        VersionedValue netVersion = state == null ? null : state.getApplicationState(ApplicationState.NET_VERSION);
        if (netVersion == null)
            return -1;
        try
        {
            int version = Integer.parseInt(netVersion.value);
            return version >= DseCompat.DSE_HANDSHAKE_MIN ? version : -1;
        }
        catch (NumberFormatException e)
        {
            return -1;
        }
    }

    /**
     * In DSE 6 compatibility mode, called once at startup, after the wait for gossip to settle: warns when this node
     * knows no DSE 6 peer ({@link #knownAtStartup}), the cue that the mode is no longer needed. A node that skips that
     * wait (its broadcast address is the loopback address, or {@code cassandra.skip_wait_for_gossip_to_settle} is 0)
     * warns on what it knows then: the record and the peers assumed at the dialect, which need no gossip, still keep it
     * quiet. Nothing happens outside the mode or when a DSE 6 peer is known.
     */
    public static void warnIfNoneKnownAtStartup()
    {
        if (!DseCompat.enabled() || !knownAtStartup().isEmpty())
            return;
        logger.warn("No DSE peers found, the mode can be dropped (internode_compatibility_mode DSE_6: no peer speaks the pre-4.0 " +
                    "dialect or gossips a DSE messaging version, and none is known or on record as a DSE 6 node): once every node " +
                    "runs this software, delete the DSE 6 upgrade marker {} and restart the node with internode_compatibility_mode NONE",
                    DseUpgradeMarker.path());
    }

    /**
     * The DSE 6 peers this node knows of, in DSE 6 compatibility mode, sorted (empty outside it): the peers recorded at a
     * pre-4.0 messaging version now (DSE's own gossip values, 256 or more, read as the dialect), which includes the peers
     * stored with a DSE 6 release that {@link DseDialectVersions#seed} assumed at the dialect until gossip says otherwise;
     * the peers on record from earlier runs ({@link DsePeersRecord}, where a DSE peer that stays down is only found),
     * with its entries that are not addresses as a peer each; the peers presumed or identified as DSE nodes, and the
     * endpoints whose gossip state carries a DSE {@code NET_VERSION}, unless they left the ring ({@link #hasLeftTheRing}).
     * Each source errs on the side of a DSE peer: a peer is left out only once gossip has given it a {@code NET_VERSION}
     * of 4.0 or later and it is seen at that version.
     */
    @VisibleForTesting
    static Set<String> knownAtStartup()
    {
        Set<String> known = new TreeSet<>();
        if (!DseCompat.enabled())
            return known;
        InetAddressAndPort self = FBUtilities.getBroadcastAddressAndPort();
        for (Map.Entry<InetAddressAndPort, Integer> e : MessagingService.instance().versions.snapshot().entrySet())
            if (!e.getKey().equals(self) && DseCompat.messagingVersionFromGossip(e.getValue()) < MessagingService.VERSION_40)
                known.add(e.getKey().getHostAddressAndPort());
        for (InetAddressAndPort peer : DsePeersRecord.peers())
            known.add(peer.getHostAddressAndPort());
        known.addAll(DsePeersRecord.unparsedEntries());
        for (InetAddressAndPort peer : PRESUMED.keySet())
            if (!hasLeftTheRing(Gossiper.instance.getEndpointStateForEndpoint(peer)))
                known.add(peer.getHostAddressAndPort());
        for (InetAddressAndPort peer : IDENTIFIED.keySet())
            if (!hasLeftTheRing(Gossiper.instance.getEndpointStateForEndpoint(peer)))
                known.add(peer.getHostAddressAndPort());
        for (Map.Entry<InetAddressAndPort, EndpointState> e : Gossiper.instance.endpointStateMap.entrySet())
            if (!e.getKey().equals(self) && !hasLeftTheRing(e.getValue()) && gossipedDseVersion(e.getValue()) >= 0)
                known.add(e.getKey().getHostAddressAndPort());
        return known;
    }

    /** The identified peers with the evidence that identified each, in DSE 6 compatibility mode; empty otherwise. */
    public static Map<InetAddressAndPort, String> snapshot()
    {
        return DseCompat.enabled() ? Map.copyOf(IDENTIFIED) : Map.of();
    }

    @VisibleForTesting
    public static void reset()
    {
        IDENTIFIED.clear();
        PRESUMED.clear();
    }
}
