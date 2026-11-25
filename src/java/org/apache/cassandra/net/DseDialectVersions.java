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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.nodes.IPeerInfo;
import org.apache.cassandra.nodes.Nodes;
import org.apache.cassandra.utils.NoSpamLogger;

import static org.apache.cassandra.net.MessagingService.VERSION_40;

/**
 * The messaging version this node opens a connection to a peer with, in DSE 6 compatibility mode, where some peers speak
 * only the pre-4.0 dialect (messaging version 11). Every method is only called in that mode.
 * <p>
 * A peer whose version this node does not know gets the 4.0-style first handshake message. DSE answers it with its own
 * version and then reads the rest of that 19-byte message as a third handshake message, which fails with an error and a
 * stack trace in DSE's log before this node reconnects at 11 (observed with DSE 6.8.65). So the peers this node can
 * tell are DSE 6 nodes before any connection are assumed at 11 when the messaging service starts listening, and the
 * first message they get is the 8-byte pre-4.0 one at 11, which DSE 6.9.26 echoes and DSE 6.8.65 answers with its own
 * version (1024), as observed with both.
 * <p>
 * Rule: the {@code release_version} this node stores for the peer ({@code system.peers}, from gossip or imported from
 * DSE's {@code nodes/peers}) is a DSE 6 one, {@code 4.0.0.<build>} ({@link DseCompat#isDse6ReleaseVersion}).
 * Nothing else is used: at that time gossip has no state for any peer, and the record of pre-4.0 peers persisted for the
 * startup checks ({@code DsePeersRecord}) does not tell a DSE node from HCD 1.2 at the dialect. A peer this node knows a
 * version of already is left alone. The dialect is assumed, not seen ({@link EndpointMessagingVersions#assume}): it does
 * not enter that record, so that a stale stored release cannot make a later start with {@code NONE} refuse a peer
 * upgraded meanwhile.
 * <p>
 * Failure mode: the stored release is stale, the peer was upgraded to this software while this node was down. It is then
 * connected to at 11 and, since this software answers any pre-4.0 first message with 11, the two nodes may speak the
 * dialect for a moment. That never lasts: the upgraded peer gossips its own {@code NET_VERSION}, which replaces the 11
 * ({@code StorageService.updateNetVersion}), and connects to this node with the 4.0-style handshake; when such a
 * handshake raises this node's version of the peer from the dialect to 4.0 or later, its connections to the peer are
 * closed ({@link #versionRaised}); a connection that still has the dialect for a peer the node knows at 4.0 or later
 * makes the 4.0-style handshake ({@link #connectionVersion}), and one that completes at the dialect for such a peer is
 * dropped and made again ({@link #supersededDialect}). A pre-4.0 handshake from a peer whose gossip state shows it is a
 * node of this software does not lower the version this node has for it ({@link #recordHandshakeVersion}). Conversely,
 * a peer that answers the 4.0-style first message in the dialect (a node rolled back to DSE, whose stale gossip state
 * still shows 12 or more) is recorded at the dialect ({@link #answeredInTheDialect}), so that this node does not probe
 * it again.
 */
public final class DseDialectVersions
{
    private static final Logger logger = LoggerFactory.getLogger(DseDialectVersions.class);
    private static final NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(logger, 1, TimeUnit.MINUTES);

    private DseDialectVersions()
    {
    }

    /**
     * Assumes the dialect for every saved peer whose stored release is a DSE 6 one, unless {@code versions} knows it
     * already; called when the node is about to listen, before any connection to a
     * peer. The saved peers are read first: a failure to read them is logged and every peer is left unknown (each then
     * gets the 4.0-style probe, as without this seeding).
     */
    public static void seedFromSavedPeers(EndpointMessagingVersions versions)
    {
        seedFromSavedPeers(() -> Nodes.peers().get(), versions);
    }

    /** As {@link #seedFromSavedPeers(EndpointMessagingVersions)}, with the saved peers read from {@code savedPeers}. */
    @VisibleForTesting
    static void seedFromSavedPeers(Supplier<Stream<? extends IPeerInfo>> savedPeers, EndpointMessagingVersions versions)
    {
        try
        {
            List<IPeerInfo> saved = savedPeers.get().collect(Collectors.toList());
            seed(saved.stream(), versions);
        }
        catch (RuntimeException e)
        {
            logger.warn("DSE 6 compatibility mode: could not read the saved peers to find the DSE 6 nodes; " +
                        "each peer is first connected to with the 4.0-style handshake", e);
        }
    }

    /** As {@link #seedFromSavedPeers}, for the given peers; returns the peers assumed at the dialect. */
    @VisibleForTesting
    public static List<InetAddressAndPort> seed(Stream<? extends IPeerInfo> peers, EndpointMessagingVersions versions)
    {
        List<InetAddressAndPort> seeded = new ArrayList<>();
        peers.forEach(peer -> {
            InetAddressAndPort address = peer.getPeerAddressAndPort();
            if (address == null || !DseCompat.isDse6ReleaseVersion(peer.getReleaseVersion()) || versions.knows(address))
                return;
            // assumed, not seen: the record of DSE peers the startup checks read is left alone
            if (versions.assume(address, DseCompat.DSE_DIALECT))
                seeded.add(address);
        });
        if (!seeded.isEmpty())
            logger.info("DSE 6 compatibility mode: {} stored with a DSE 6 release (4.0.0.<build>); connecting to them with the pre-4.0 handshake at version {}",
                        seeded, DseCompat.DSE_DIALECT);
        return seeded;
    }

    /**
     * The {@code NET_VERSION} of a gossip state showing a node of this software (or of the 4.0 line outside the dialect):
     * 12 or more but not a DSE version (256 or more); -1 for any other state, value or none. HCD 1.2 at the dialect
     * gossips 11.
     */
    @VisibleForTesting
    static int gossipedVersion40OrLater(EndpointState state)
    {
        VersionedValue netVersion = state == null ? null : state.getApplicationState(ApplicationState.NET_VERSION);
        if (netVersion == null)
            return -1;
        try
        {
            int version = Integer.parseInt(netVersion.value);
            return version >= VERSION_40 && version < DseCompat.DSE_HANDSHAKE_MIN ? version : -1;
        }
        catch (NumberFormatException e)
        {
            return -1;
        }
    }

    /**
     * The raw version {@code versions} has for {@code peer}, or null when it has none, including when the version is
     * forgotten concurrently (the handshakes, gossip and the removal of a node update the node's versions on different
     * threads).
     */
    private static Integer rawVersion(EndpointMessagingVersions versions, InetAddressAndPort peer)
    {
        try
        {
            return versions.knows(peer) ? versions.getRaw(peer) : null;
        }
        catch (IllegalStateException e)
        {
            return null;
        }
    }

    /** True when {@code versions} knows {@code peer} at the pre-4.0 dialect (or at a DSE version, which maps to it). */
    static boolean isPre40(EndpointMessagingVersions versions, InetAddressAndPort peer)
    {
        Integer raw = rawVersion(versions, peer);
        return raw != null && DseCompat.messagingVersionFromGossip(raw) < VERSION_40;
    }

    /**
     * The version, capped at the current one, at which {@code versions} knows {@code peer} when it is 4.0 or later; -1
     * when it does not know the peer, or knows it at the dialect or at a DSE version.
     */
    private static int version40OrLater(EndpointMessagingVersions versions, InetAddressAndPort peer)
    {
        Integer raw = rawVersion(versions, peer);
        // a raw DSE version (256 or more) is the dialect, not a version above the current one
        if (raw == null || DseCompat.messagingVersionFromGossip(raw) < VERSION_40)
            return -1;
        return Math.min(raw, MessagingService.current_version);
    }

    /**
     * Records the version an inbound handshake with {@code peer} settled on, in the node's messaging versions, as the
     * handshake always has, except for a pre-4.0 handshake from a peer whose gossip state shows it is a node of this
     * software: it only connected at the dialect because it had a stale version of this node, and recording the dialect
     * here would make this node's own connections to it fall back too. A first message only DSE sends (DSE-flagged, or
     * pre-4.0-shaped requesting 12) is recorded at the dialect whatever gossip says: the peer was rolled back to DSE.
     * DSE 6.9.26 sends the third message on the same socket after such a first message, so a node rolled back to it
     * can take this path (not checked in a rollback); DSE 6.8.65 sends none, it closes the socket and reconnects with a
     * plain first message at 11. For a node rolled back to DSE 6.8.65 whose stale gossip state still shows 12 or more,
     * the plain reconnect at 11 leaves this node's version of it unchanged, until the node's new gossip state, with its
     * DSE version, replaces it ({@code StorageService.updateNetVersion}).
     * Then calls {@link #versionRaised} as needed. The check of the gossip state and the update are made under the lock
     * of the node's versions.
     *
     * @param requestedVersion the version the peer's first message requested
     */
    static void recordHandshakeVersion(InetAddressAndPort peer, int version, int requestedVersion)
    {
        EndpointMessagingVersions versions = MessagingService.instance().versions;
        boolean keep;
        boolean wasPre40 = false;
        Integer kept = null;
        synchronized (versions)
        {
            // a 4.0-style first message also carries 12 in its legacy byte (acceptVersions.min of a node without the
            // mode): only a pre-4.0 handshake, at the dialect, can be one only DSE sends
            boolean onlyDseSendsIt = version < VERSION_40
                                     && (requestedVersion == DseCompat.DSE_VERSION_BYTE || requestedVersion == DseCompat.NET_VERSION_PUBLISHED);
            keep = version < VERSION_40 && !onlyDseSendsIt && gossipedVersion40OrLater(Gossiper.instance.getEndpointStateForEndpoint(peer)) > 0;
            if (keep)
            {
                kept = rawVersion(versions, peer);
            }
            else
            {
                wasPre40 = isPre40(versions, peer);
                versions.set(peer, version);
            }
        }
        if (keep)
        {
            // logged outside the lock, at most one such line a minute whatever the peer: every connection type of such a
            // peer reconnects this way
            noSpamLogger.info("DSE 6 compatibility mode: {} connected with the pre-4.0 handshake, but gossips a messaging version of 4.0 or later; " +
                              "keeping {} for it", peer, kept == null ? "no version" : "version " + kept);
            return;
        }
        versionRaised(peer, wasPre40, version);
    }

    /**
     * Called after the node's messaging version of {@code peer} was recorded as {@code version}: when it was at the dialect
     * before and is at 4.0 or later now, the peer was taken for a pre-4.0 node (a stale stored release, a stale version of
     * a peer upgraded meanwhile) and its established connections are closed, so that they reconnect with the 4.0-style
     * handshake ({@link #connectionVersion}); messages queued on them are not lost. Connections already at 4.0 or later
     * reconnect too, at the same version: interrupting all of them is simpler than telling them apart, and happens once
     * per wrong identification. A connection being made at that time is handled when it completes
     * ({@link #supersededDialect}).
     */
    static void versionRaised(InetAddressAndPort peer, boolean wasPre40, int version)
    {
        if (!wasPre40 || version < VERSION_40)
            return;
        logger.info("DSE 6 compatibility mode: {} is at messaging version {}, not at the pre-4.0 dialect; reconnecting to it with the 4.0-style handshake",
                    peer, version);
        MessagingService.instance().interruptOutbound(peer);
    }

    /**
     * The version an outbound connection to {@code peer} requests: the one in the connection's own copy of the versions
     * ({@code connectionVersions}, taken when the connection was created and updated by its handshakes), except in two
     * cases, where the copy is corrected and the node's version returned:
     * <ul>
     *   <li>the copy has the dialect while the node's versions ({@code nodeVersions}) know the peer at 4.0 or later: the
     *       peer was upgraded, or taken for a DSE node by mistake. A DSE peer is at the dialect in the node's versions once
     *       it answered in it ({@link #answeredInTheDialect}) or gossiped its version (or completed a handshake after a
     *       first message only DSE sends, as DSE 6.9.26 does and DSE 6.8.65 does not), so that this correction cannot
     *       repeat for it;</li>
     *   <li>the copy does not have the dialect (it has 4.0 or later, or knows nothing, which reads as the current
     *       version) while the node's versions have the dialect for a peer identified as a DSE node ({@link DsePeers}):
     *       another connection type of the same peer made the 4.0-style handshake and DSE answered it; the other
     *       connection types, whose copies were taken before, go straight to the pre-4.0 handshake instead of probing
     *       DSE again.</li>
     * </ul>
     */
    static int connectionVersion(InetAddressAndPort peer, int connectionVersion, EndpointMessagingVersions connectionVersions, EndpointMessagingVersions nodeVersions)
    {
        if (connectionVersions == nodeVersions)
            return connectionVersion;
        if (connectionVersion >= VERSION_40)
        {
            if (!isPre40(nodeVersions, peer) || !DsePeers.isDse(peer))
                return connectionVersion;
            logger.debug("DSE 6 compatibility mode: {} is a DSE node at the pre-4.0 dialect; connecting to it with the pre-4.0 handshake", peer);
            connectionVersions.set(peer, DseCompat.DSE_DIALECT);
            return DseCompat.DSE_DIALECT;
        }
        int nodeVersion = version40OrLater(nodeVersions, peer);
        if (nodeVersion < 0)
            return connectionVersion;
        logger.debug("DSE 6 compatibility mode: the connection to {} had the pre-4.0 dialect, but the peer is at messaging version {}; connecting with the 4.0-style handshake",
                     peer, nodeVersion);
        connectionVersions.set(peer, nodeVersion);
        return nodeVersion;
    }

    /**
     * True when an outbound handshake with {@code peer} just completed at {@code negotiatedVersion}, the dialect, while the
     * node's versions know the peer at 4.0 or later: the version rose while the connection was being made (the interrupt
     * of {@link #versionRaised} reaches only established connections), and the connection must be made again, with the
     * 4.0-style handshake.
     */
    static boolean supersededDialect(InetAddressAndPort peer, int negotiatedVersion, EndpointMessagingVersions nodeVersions)
    {
        if (negotiatedVersion >= VERSION_40)
            return false;
        int nodeVersion = version40OrLater(nodeVersions, peer);
        if (nodeVersion < 0)
            return false;
        logger.info("DSE 6 compatibility mode: the connection to {} was made in the pre-4.0 dialect, but the peer is at messaging version {} now; " +
                    "making it again with the 4.0-style handshake", peer, nodeVersion);
        return true;
    }

    /**
     * Called when {@code peer} answered this node's 4.0-style first message in the dialect (a DSE version, or 11), which
     * makes the connection retry at {@code version}: only a pre-4.0 node does that, this software answers with its own
     * handshake. The node's versions record that version, so that no connection to the peer corrects itself back to the
     * 4.0-style handshake ({@link #connectionVersion}) while a stale gossip state of a node rolled back to DSE still shows
     * 12 or more, which would make DSE fail on the probe again at every attempt.
     */
    static void answeredInTheDialect(InetAddressAndPort peer, int version)
    {
        // the node's own map records the version and the record of DSE peers under its lock (EndpointMessagingVersions.set)
        MessagingService.instance().versions.set(peer, version);
    }
}
