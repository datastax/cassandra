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

/**
 * The peers this node has identified as DataStax Enterprise 6 nodes, in DSE 6 compatibility mode. At messaging version
 * 11 a DSE node and an HCD 1.2 node forced to that version use the same handshake and message layouts, so a pre-4.0
 * peer is not a DSE node by its version alone. The record is read by {@code nodetool compatstatus} and by the version
 * an outbound connection requests ({@link DseDialectVersions#connectionVersion}).
 * <p>
 * A peer is recorded as a DSE node on any of this evidence, which no other kind of peer produces:
 * <ol>
 *   <li>its first handshake message is one only DSE sends, followed by a third message on the same socket: DSE-flagged
 *       ({@code 0xFF} and its relative version, {@link DseCompat#isDseFlaggedInitiate}), or pre-4.0-shaped and
 *       requesting version 12, which DSE sends once it has stored version 12 (Cassandra 4.0) for this node, while a
 *       3.x or HCD 1.2 node never requests more than 11 in that shape. DSE 6.9.26 sends the third message on the same
 *       socket after either first message, answered with 11, so a DSE 6.9.26 node is identified here by a connection
 *       it opens with one of them.
 *       DSE 6.8.65 does not: it closes the socket without a third message and reconnects at 11 on a new one, with a
 *       plain first message that identifies nothing; it is identified by the second or third item when this node
 *       connects to it;</li>
 *   <li>it answers our 4.0-style first handshake message with a DSE messaging version
 *       ({@link DseCompat#isDseHandshakeVersion});</li>
 *   <li>it answers our pre-4.0 first handshake message at 11 with a DSE messaging version instead of echoing 11, as
 *       observed with DSE 6.8.65 (DSE 6.9.26 echoes 11, which identifies nothing).</li>
 * </ol>
 * A handshake cannot clear the record: a DSE node that stored 11 for this node reconnects with a plain pre-4.0 first
 * message at 11, which looks exactly like HCD 1.2's, so the record survives reconnects. Nothing else clears it while the
 * node runs, and it is not persisted: after a restart a DSE peer is identified again by the handshake when this node
 * does not know its version and sends the 4.0-style first message, which DSE answers with its own version, or when
 * this node sends a DSE 6.8.65 peer the pre-4.0 first message, which it answers with its own version (see the third
 * item), or by an inbound first message only DSE sends when a third message follows, as DSE 6.9.26 sends it and DSE
 * 6.8.65 does not (see the first item). The record of peers at a pre-4.0 version that {@code DsePeersRecord} persists
 * for the startup checks does not tell a DSE node from HCD 1.2 at the dialect, so it does not identify a peer.
 * <p>
 * Failure mode: a DSE peer not identified yet is treated like HCD 1.2 at the dialect. That takes a DSE node that
 * reconnected with a plain first message at 11 or less and whose answers to this node's first messages carried no DSE
 * messaging version (DSE 6.9.26 echoes the pre-4.0 one): {@code nodetool compatstatus} lists it only when it gossips
 * its DSE messaging version, and a connection to it whose copy of the versions predates its handshake probes it once
 * with the 4.0-style first message, whose answer identifies it.
 */
public final class DsePeers
{
    private static final Logger logger = LoggerFactory.getLogger(DsePeers.class);

    /** The identified DSE peers, with the evidence that identified them first. */
    private static final ConcurrentMap<InetAddressAndPort, String> IDENTIFIED = new ConcurrentHashMap<>();

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
            logger.info("Identified {} as a DSE 6 node ({})", peer, evidence);
    }

    /** True when {@code peer} was identified as a DSE 6 node, in DSE 6 compatibility mode. */
    public static boolean isDse(InetAddressAndPort peer)
    {
        return DseCompat.enabled() && IDENTIFIED.containsKey(peer);
    }

    /**
     * Records the sender of a pre-4.0 first handshake message, named {@code peer} by the third message, as a DSE node
     * when only DSE sends such a first message: DSE-flagged, or requesting version 12
     * ({@link DseCompat#NET_VERSION_PUBLISHED}; see the class comment). Reached with DSE 6.9.26, which sends the third
     * message on the same socket after either of those first messages, as its logs of the identifications made here
     * show; not with DSE 6.8.65, which sends no third message after them but closes the socket and reconnects at 11.
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
     * ({@code REMOVING_TOKEN}) still is one. Used by {@code nodetool compatstatus}; a null state has not left.
     */
    public static boolean hasLeftTheRing(EndpointState state)
    {
        String status = Gossiper.getGossipStatus(state);
        return VersionedValue.STATUS_LEFT.equals(status) || VersionedValue.REMOVED_TOKEN.equals(status);
    }

    /**
     * DSE's own messaging version (256 or more) that the gossip state of a node carries as its {@code NET_VERSION}, or -1
     * when the state is null or carries none, a smaller value or one that is not a number. The test of "gossips a DSE
     * version" used by {@code nodetool compatstatus}.
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

    /** The identified peers with the evidence that identified each, in DSE 6 compatibility mode; empty otherwise. */
    public static Map<InetAddressAndPort, String> snapshot()
    {
        return DseCompat.enabled() ? Map.copyOf(IDENTIFIED) : Map.of();
    }

    @VisibleForTesting
    public static void reset()
    {
        IDENTIFIED.clear();
    }
}
