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

package org.apache.cassandra.service;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.DseCompat;
import org.apache.cassandra.net.DseCompatRefusals;
import org.apache.cassandra.net.DseDialectVersions;
import org.apache.cassandra.net.DsePeers;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.utils.Clock.Global.nanoTime;

/**
 * The operational guards of the in-place upgrade from DataStax Enterprise 6.
 * <p>
 * While the upgrade is in progress, the cluster topology and the on-disk formats must not change: bootstrap (and
 * bootstrap resume), replace, a join of the ring by a node that has not completed a bootstrap, decommission,
 * removenode (also forced), assassinate, move, rebuild, repair, upgradesstables, recompress_sstables, refresh, sstable
 * import and resetlocalschema are refused, and so is a change of {@code paxos_variant} to anything but {@code v1} in
 * {@code DSE_6} mode (to a v2 variant in {@code NONE} mode). The schema is frozen too: DDL is refused in {@code DSE_6}
 * mode unless {@code -Dcassandra.internode_compatibility_mode.allow_ddl=true} (a schema change made on a DSE node never
 * reaches this software, and one made here would not be understood by DSE); a statement that would change nothing
 * runs. The refusals of operations requested through JMX or nodetool, of schema changes and of repair messages (under
 * {@code repair}) are counted in {@link DseCompatRefusals} and shown by {@code nodetool compatstatus}; those of the
 * node's own start are not (the node exits). In {@code DSE_6} mode a status line with the DSE peers, the entries of
 * the record that are not addresses and the counters is logged every {@link #STATUS_LOG_INTERVAL_MINUTES} minutes.
 * <p>
 * The upgrade is in progress in {@code DSE_6} mode, or while any peer is recorded at a pre-4.0 messaging version: with
 * {@code NONE}, a node that gossips with upgraded nodes learns DSE's own {@code NET_VERSION} (256 or more) of the DSE
 * nodes still in the cluster, which counts the same way. A cluster that never ran DSE never holds such a
 * value, so there every guard is inert. A new node is also checked against the gossip states of its shadow round
 * ({@link #refuseJoinWithDsePeersInGossip}), before any version is recorded.
 * <p>
 * A node lost for good during the upgrade, a DSE 6 node or a node of this software (down, and silent for
 * {@link #lostNodeSilenceNanos()}: see {@link #isLostNode}), can be removed without finishing the upgrade first, in both
 * modes: {@code assassinate} of it, and {@code removenode force} of a removal of lost nodes only, are allowed, since
 * they stream nothing; {@code removenode} of it is allowed once every DSE 6 node still in the cluster is lost too,
 * since it streams data between the other replicas and the nodes that take over its ranges, which DSE 6 nodes cannot
 * do with this software (nodes of this software stream with each other within either mode). Once removed, the node has
 * left the ring, and the record of DSE 6 peers of every node that sees its removal, so it no longer keeps the upgrade
 * from finishing. A replacement in {@code DSE_6} mode stays refused, its message naming the removal, and a node without
 * the mode cannot join nodes in the mode anyway (their schema is of another major version to it). The code lets a node
 * without the mode replace a lost node once every node runs without the mode and no other DSE 6 node is in gossip
 * ({@link #refuseJoinWithDsePeersInGossip} does not count the replaced node), but a lost DSE 6 node stays on the record
 * of DSE 6 peers of the nodes that saw it until it is removed, and their start without the mode is refused until then
 * (unless {@code -Dcassandra.internode_compatibility_mode.ignore_dse_peers=true}): the removal is the way out.
 */
public final class DseUpgradeGuards
{
    private static final Logger logger = LoggerFactory.getLogger(DseUpgradeGuards.class);

    public static final long STATUS_LOG_INTERVAL_MINUTES = 10;

    /** The reason under which refused schema changes are counted. */
    public static final String SCHEMA_CHANGE = "schema change";

    /** The error text of a refused schema change: refused, not only warned about. */
    public static final String SCHEMA_FROZEN = "schema changes are frozen during the DSE 6 upgrade; run them after the flag is dropped";

    /** The error text of a repair message to or from a peer speaking the pre-4.0 dialect. */
    public static final String REPAIR_WITH_DSE_PEER = "repair is refused during the DSE 6 upgrade: repair messages cannot be exchanged " +
                                                      "with a peer speaking the pre-4.0 dialect (DSE 6); run repair after the flag is dropped";

    private static final String DSE6_REMEDY = "finish the upgrade of every node and drop the mode first";
    private static final String NONE_REMEDY = "finish upgrading (or remove) the DSE 6 nodes first";
    /** The remedy of a refused replacement in {@code DSE_6} mode: the removal, which needs no node to change its mode first. */
    private static final String REPLACE_REMEDY = "a replacement cannot start in internode_compatibility_mode DSE_6; remove the node lost for " +
                                                 "good instead, a DSE 6 node or a node of this software (nodetool removenode once every DSE 6 " +
                                                 "node still in the cluster is lost, nodetool assassinate otherwise), and add a new node once " +
                                                 "the upgrade is finished";

    /** What the removal of a lost node leaves to the operator, logged when it is allowed. */
    private static final String LOST_NODE_CAVEAT = "the guard cannot tell a node lost for good from one silent for longer (restarting slowly, " +
                                                   "being upgraded or rolled back): make sure first that it will not come back; after a rollback " +
                                                   "to DSE the node is back in DSE's ring, down (DSE keeps its own record of its peers): remove " +
                                                   "it again there with DSE's nodetool removenode";

    /** The kinds of lost node ({@link #lostNodeKind}). */
    private static final String LOST_DSE_NODE = "DSE 6 node";
    private static final String LOST_NODE_OF_THIS_SOFTWARE = "node of this software";

    private static final AtomicBoolean STATUS_LOG_STARTED = new AtomicBoolean();

    private DseUpgradeGuards()
    {
    }

    /**
     * True while the upgrade is in progress from this node's point of view: the DSE 6 compatibility mode is on, or a
     * peer is recorded at the pre-4.0 dialect (with {@code NONE}: a DSE node gossiped by an upgraded peer).
     */
    public static boolean upgradeInProgress()
    {
        return DseCompat.enabled() || !pre40Peers().isEmpty();
    }

    /**
     * The peers recorded at a pre-4.0 messaging version, sorted: the raw values of the messaging version map, with
     * DSE's own gossip values (256 or more) mapped to the dialect.
     */
    public static Set<InetAddressAndPort> pre40Peers()
    {
        Set<InetAddressAndPort> peers = new TreeSet<>();
        // with NONE in a process whose messaging never started (a tool, an embedded use) there is nothing to read, and
        // reading would create the messaging service
        if (!DseCompat.enabled() && !MessagingService.isInstantiated())
            return peers;
        for (Map.Entry<InetAddressAndPort, Integer> e : MessagingService.instance().versions.snapshot().entrySet())
            if (DseCompat.messagingVersionFromGossip(e.getValue()) < MessagingService.VERSION_40)
                peers.add(e.getKey());
        return peers;
    }

    /**
     * The DSE 6 peers this node knows of: those recorded at a pre-4.0 messaging version now, and in {@code DSE_6} mode
     * those on record from earlier (a DSE peer that is down is only on record), and, unless they left the ring
     * ({@link DsePeers#hasLeftTheRing}), those {@link DsePeers} identified or presumed as DSE nodes (by a handshake, a
     * gossiped DSE {@code NET_VERSION} or a stored DSE 6 release, possibly before their version is recorded) and those
     * whose gossip state carries a DSE {@code NET_VERSION} ({@link DsePeers#gossipedDseVersion}; the storage service does
     * not record the version of a node that is not a ring member or is hibernating, being replaced). The entries of the
     * record that are not addresses are not peers here ({@link DsePeersRecord#unparsedEntries}). Sorted.
     */
    public static Set<InetAddressAndPort> knownDsePeers()
    {
        Set<InetAddressAndPort> peers = pre40Peers();
        if (DseCompat.enabled())
        {
            peers.addAll(DsePeersRecord.peers());
            // defensive: the gossiper forgets the identification when it excises a node that left (Gossiper.removeEndpoint)
            for (InetAddressAndPort peer : DsePeers.snapshot().keySet())
                if (!DsePeers.hasLeftTheRing(Gossiper.instance.getEndpointStateForEndpoint(peer)))
                    peers.add(peer);
            for (InetAddressAndPort peer : DsePeers.presumedSnapshot().keySet())
                if (!DsePeers.hasLeftTheRing(Gossiper.instance.getEndpointStateForEndpoint(peer)))
                    peers.add(peer);
            InetAddressAndPort self = FBUtilities.getBroadcastAddressAndPort();
            for (Map.Entry<InetAddressAndPort, EndpointState> e : Gossiper.instance.endpointStateMap.entrySet())
                if (!e.getKey().equals(self) && !DsePeers.hasLeftTheRing(e.getValue()) && DsePeers.gossipedDseVersion(e.getValue()) >= 0)
                    peers.add(e.getKey());
        }
        return peers;
    }

    /**
     * Refuses an operation requested through JMX or nodetool while the upgrade is in progress.
     *
     * @param operation the operation as the operator names it ({@code decommission}, {@code repair}, ...); also the
     *                  reason the refusal is counted under
     * @throws UnsupportedOperationException while the upgrade is in progress
     */
    public static void refuseWhileUpgrading(String operation)
    {
        if (!upgradeInProgress())
            return;
        DseCompatRefusals.refused(operation);
        throw new UnsupportedOperationException(refusalMessage(operation));
    }

    /**
     * Refuses an operation of the node's own start (bootstrap, replace) while the upgrade is in progress. Not
     * counted: the node exits.
     *
     * @throws ConfigurationException while the upgrade is in progress; the daemon reports it and exits
     */
    public static void refuseAtStartupWhileUpgrading(String operation) throws ConfigurationException
    {
        if (!upgradeInProgress())
            return;
        throw new ConfigurationException(refusalMessage(operation), false);
    }

    /**
     * Refuses the start of a replacement node while the upgrade is in progress (in {@code DSE_6} mode, in practice: a
     * node without the mode is checked by its shadow round), naming the removal of a node lost for good, which the
     * operator can run without finishing the upgrade first. Not counted: the node exits.
     *
     * @throws ConfigurationException while the upgrade is in progress; the daemon reports it and exits
     */
    public static void refuseReplacementWhileUpgrading() throws ConfigurationException
    {
        if (!upgradeInProgress())
            return;
        throw new ConfigurationException(refusalMessage("replacing a node", DseCompat.enabled() ? REPLACE_REMEDY : NONE_REMEDY), false);
    }

    /**
     * True when {@code endpoint} is a node lost to this node during the upgrade: a peer whose gossip state this node
     * holds, that is not alive to this node, and of which nothing new has been heard for {@link #lostNodeSilenceNanos()}
     * (no newer heartbeat, directly or relayed by another node, and no new generation), and that is either a DSE 6 node
     * (it gossips a pre-4.0 {@code NET_VERSION} or is recorded at a pre-4.0 messaging version; a peer that is only on
     * the record of DSE 6 peers or presumed is not enough) or a node of this software (it gossips a {@code NET_VERSION}
     * of 4.0 or later: a node in {@code DSE_6} mode gossips 4.0, one without the mode its own version; in a cluster
     * upgraded from DSE 6 no other node gossips such a value). A node that restarts, or that this node cannot talk to
     * but others can (a DSE 6 node seen by a node without the mode, which never completes an echo with it), keeps its
     * gossip state fresh and is not lost; neither is this node itself, a peer without a gossip state, nor a peer whose
     * gossip state carries no {@code NET_VERSION} this check can read and that is not recorded at a pre-4.0 version,
     * since nothing says it is down or what it runs.
     */
    public static boolean isLostNode(InetAddressAndPort endpoint)
    {
        return lostNodeKind(endpoint) != null;
    }

    /** True when {@code endpoint} is a DSE 6 node lost to this node ({@link #isLostNode}). */
    @VisibleForTesting
    static boolean isLostDsePeer(InetAddressAndPort endpoint)
    {
        return LOST_DSE_NODE.equals(lostNodeKind(endpoint));
    }

    /**
     * The kind of lost node {@code endpoint} is ({@link #isLostNode}), {@link #LOST_DSE_NODE} or
     * {@link #LOST_NODE_OF_THIS_SOFTWARE}; null when it is not lost.
     */
    private static String lostNodeKind(InetAddressAndPort endpoint)
    {
        if (endpoint.equals(FBUtilities.getBroadcastAddressAndPort()))
            return null;
        EndpointState state = Gossiper.instance.getEndpointStateForEndpoint(endpoint);
        if (state == null || isAlive(endpoint) || nanoTime() - state.getUpdateTimestamp() < lostNodeSilenceNanos())
            return null;
        if (gossipsPre40Version(state) || pre40Peers().contains(endpoint))
            return LOST_DSE_NODE;
        return DseDialectVersions.gossipedVersion40OrLater(state) > 0 ? LOST_NODE_OF_THIS_SOFTWARE : null;
    }

    /** How a lost node of {@code kind} ({@link #lostNodeKind}) is named in the log lines and the refusals. */
    private static String describe(String kind, InetAddressAndPort endpoint)
    {
        return LOST_DSE_NODE.equals(kind) ? "the DSE 6 node " + endpoint : "the node " + endpoint + " of this software";
    }

    /**
     * How long nothing must have been heard of a peer that is down for it to be lost. By default the largest of 30
     * minutes, the hint window ({@code max_hint_window}) and twice the ring delay, so that a node that restarts, even
     * slowly (a long commit log replay), is being upgraded or rolled back in place, or is briefly cut off is not taken as
     * lost; {@code -Dcassandra.internode_compatibility_mode.lost_node_silence_seconds} replaces it. The property is read
     * at each use and checked only at startup ({@link #checkLostNodeSilence}); a negative value read later counts as
     * zero, and one that is not a number makes {@code removenode}, {@code assassinate} and {@code removenode force}
     * fail with a {@link ConfigurationException}, not counted among the refusals. The guard still cannot tell a node
     * lost for good from one that stays silent for longer: the operator confirms it is gone.
     */
    static long lostNodeSilenceNanos()
    {
        String seconds = CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE_LOST_NODE_SILENCE_SECONDS.getString();
        if (seconds != null)
            return TimeUnit.SECONDS.toNanos(Math.max(0, parseLostNodeSilenceSeconds(seconds)));
        long millis = Math.max(TimeUnit.MINUTES.toMillis(30), Math.max(DatabaseDescriptor.getMaxHintWindow(), 2L * StorageService.RING_DELAY_MILLIS));
        return TimeUnit.MILLISECONDS.toNanos(millis);
    }

    /**
     * The smallest silence {@code cassandra.internode_compatibility_mode.lost_node_silence_seconds} may set: the larger
     * of one minute and twice the ring delay, the time gossip takes to settle, so that the heartbeats of a node that only
     * this node cannot reach, relayed by the others, have arrived before it can count as lost (the minute holds when the
     * ring delay is set very low). The ring delay is read from {@code cassandra.ring_delay_ms} as {@code StorageService}
     * reads it, without initialising {@code StorageService}, which the startup checks run before.
     */
    static long lostNodeSilenceFloorMillis()
    {
        return Math.max(TimeUnit.MINUTES.toMillis(1), 2L * CassandraRelevantProperties.RING_DELAY.getInt(StorageService.DEFAULT_RING_DELAY_MILLIS));
    }

    /**
     * Checks {@code cassandra.internode_compatibility_mode.lost_node_silence_seconds} at startup, in every mode (the
     * removal of a lost node is allowed in both): a value that is not a whole number of seconds, or that is below
     * {@link #lostNodeSilenceFloorMillis()}, refuses the start rather than being corrected, so that the node never uses a
     * silence the operator did not set.
     *
     * @return the silence set, in seconds, or -1 when the property is not set (the default applies)
     * @throws ConfigurationException when the value is refused
     */
    public static long checkLostNodeSilence() throws ConfigurationException
    {
        String value = CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE_LOST_NODE_SILENCE_SECONDS.getString();
        if (value == null)
            return -1;
        long seconds = parseLostNodeSilenceSeconds(value);
        long floorMillis = lostNodeSilenceFloorMillis();
        if (seconds < TimeUnit.MILLISECONDS.toSeconds(floorMillis + 999))
            throw new ConfigurationException(String.format("-D%s=%s is refused: the silence after which a node that is down counts as lost during " +
                                                           "the DSE 6 upgrade must be at least %d seconds, the larger of one minute and twice the " +
                                                           "ring delay; unset it for the default (the largest of 30 minutes, max_hint_window and " +
                                                           "twice the ring delay)",
                                                           CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE_LOST_NODE_SILENCE_SECONDS.getKey(),
                                                           value, TimeUnit.MILLISECONDS.toSeconds(floorMillis + 999)),
                                             false);
        return seconds;
    }

    private static long parseLostNodeSilenceSeconds(String value) throws ConfigurationException
    {
        try
        {
            return Long.parseLong(value.trim());
        }
        catch (NumberFormatException e)
        {
            throw new ConfigurationException(String.format("-D%s=%s is refused: not a whole number of seconds",
                                                           CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE_LOST_NODE_SILENCE_SECONDS.getKey(), value),
                                             false);
        }
    }

    /**
     * The DSE 6 nodes not known to be lost: the known DSE 6 peers and the endpoints whose gossip state carries a
     * pre-4.0 {@code NET_VERSION} (unless they left the ring), alive, recently heard of, or of no known state (on the
     * record of DSE 6 peers only, or presumed), less the lost nodes ({@link #isLostNode}; a peer on the record that
     * gossips 4.0 or later was upgraded meanwhile). Those a stream could involve. Sorted.
     */
    public static Set<InetAddressAndPort> liveDsePeers()
    {
        Set<InetAddressAndPort> peers = knownDsePeers();
        InetAddressAndPort self = FBUtilities.getBroadcastAddressAndPort();
        for (Map.Entry<InetAddressAndPort, EndpointState> e : Gossiper.instance.endpointStateMap.entrySet())
            if (!e.getKey().equals(self) && !DsePeers.hasLeftTheRing(e.getValue()) && gossipsPre40Version(e.getValue()))
                peers.add(e.getKey());
        peers.removeIf(DseUpgradeGuards::isLostNode);
        return peers;
    }

    /** How long nothing has been heard of {@code endpoint}, in whole seconds, for the log lines. */
    private static long silenceSeconds(InetAddressAndPort endpoint)
    {
        EndpointState state = Gossiper.instance.getEndpointStateForEndpoint(endpoint);
        return state == null ? 0 : TimeUnit.NANOSECONDS.toSeconds(nanoTime() - state.getUpdateTimestamp());
    }

    private static boolean isAlive(InetAddressAndPort endpoint)
    {
        return FailureDetector.instance.isAlive(endpoint) || Gossiper.instance.getLiveMembers().contains(endpoint);
    }

    /**
     * The guard of {@code nodetool removenode}: refuses it while the upgrade is in progress, unless {@code endpoint} is a
     * node lost for good ({@link #isLostNode}), a DSE 6 node or a node of this software, and every DSE 6 node still in
     * the cluster is lost too ({@link #liveDsePeers}): the removal streams the data of the lost node from its other
     * replicas to the nodes that take over its ranges, which a DSE 6 node cannot do with this software, so it then
     * involves nodes of this software only. A DSE 6 node that comes back during the removal takes over its ranges
     * without their data, which a full repair restores once the upgrade is finished: the WARN says so.
     *
     * @param endpoint the node to remove, or null when its host id is unknown
     * @throws UnsupportedOperationException when refused
     */
    public static void refuseRemoveNodeWhileUpgrading(InetAddressAndPort endpoint)
    {
        if (!upgradeInProgress())
            return;
        String kind = endpoint == null ? null : lostNodeKind(endpoint);
        if (kind == null)
        {
            refuseWhileUpgrading("removenode");
            return;
        }
        Set<InetAddressAndPort> live = liveDsePeers();
        if (live.isEmpty())
        {
            logger.warn("Allowing the removal of {}, silent for {} seconds, during the DSE 6 upgrade: its data is streamed from its other " +
                        "replicas, all of this software (a DSE 6 node that comes back meanwhile takes over ranges without their data until " +
                        "a full repair, which runs once the upgrade is finished); " + LOST_NODE_CAVEAT, describe(kind, endpoint), silenceSeconds(endpoint));
            return;
        }
        Set<InetAddressAndPort> withoutState = new TreeSet<>();
        for (InetAddressAndPort peer : live)
            if (Gossiper.instance.getEndpointStateForEndpoint(peer) == null)
                withoutState.add(peer);
        DseCompatRefusals.refused("removenode");
        throw new UnsupportedOperationException("removenode of " + describe(kind, endpoint) + " lost for good is refused while DSE 6 nodes not " +
                                                "known to be lost are in the cluster (" + live +
                                                (withoutState.isEmpty() ? "" : "; known without a gossip state (on record or presumed): " + withoutState) +
                                                "): it streams data with the other replicas, which DSE 6 nodes cannot do with this software; " +
                                                "upgrade those DSE 6 nodes first, or remove the lost node with nodetool assassinate " +
                                                endpoint.getHostAddressAndPort() + " (no streaming: the data of its ranges is then held by fewer " +
                                                "replicas until a full repair, which runs once the upgrade is finished)");
    }

    /**
     * The guard of {@code nodetool assassinate}: refuses it while the upgrade is in progress, unless {@code endpoint} is
     * a node lost for good ({@link #isLostNode}), a DSE 6 node or a node of this software, which it then removes
     * without streaming, whatever DSE 6 nodes are alive.
     *
     * @throws UnsupportedOperationException when refused
     */
    public static void refuseAssassinateWhileUpgrading(InetAddressAndPort endpoint)
    {
        if (!upgradeInProgress())
            return;
        String kind = lostNodeKind(endpoint);
        if (kind == null)
        {
            refuseWhileUpgrading("assassinate");
            return;
        }
        logger.warn("Allowing the assassination of {}, silent for {} seconds, during the DSE 6 upgrade: nothing is streamed, so the data " +
                    "of its ranges is held by fewer replicas until a full repair, which runs once the upgrade is finished; " +
                    LOST_NODE_CAVEAT, describe(kind, endpoint), silenceSeconds(endpoint));
    }

    /**
     * The guard of {@code nodetool removenode force}: refuses it while the upgrade is in progress, unless every node
     * being removed ({@code leaving}) is a node lost for good ({@link #isLostNode}), a DSE 6 node or a node of this
     * software; forcing the removal streams nothing.
     *
     * @throws UnsupportedOperationException when refused
     */
    public static void refuseForcedRemovalWhileUpgrading(Collection<InetAddressAndPort> leaving)
    {
        if (!upgradeInProgress())
            return;
        if (leaving.isEmpty() || !leaving.stream().allMatch(DseUpgradeGuards::isLostNode))
        {
            refuseWhileUpgrading("removenode force");
            return;
        }
        logger.warn("Allowing the forced removal of the lost nodes {}, each silent for at least {} seconds, during the DSE 6 upgrade: the " +
                    "data not yet streamed is held by fewer replicas until a full repair, which runs once the upgrade is finished; " +
                    LOST_NODE_CAVEAT, leaving, TimeUnit.NANOSECONDS.toSeconds(lostNodeSilenceNanos()));
    }

    /**
     * Refuses a new or replacement node whose shadow round of gossip shows DSE nodes, whatever this node's mode: it
     * cannot join a cluster that is being upgraded from DSE 6. A no-op for a cluster that never ran DSE. A DSE node
     * that left the ring does not count ({@link DsePeers#hasLeftTheRing}); one whose removal is still streaming does.
     * <p>
     * The rule is the one of {@link #upgradeInProgress}, on the gossiped {@code NET_VERSION}: any value that maps
     * to a pre-4.0 messaging version counts ({@link DseCompat#messagingVersionFromGossip}), DSE's own (256 or more) and
     * the dialect itself (11 or less), which a new node cannot speak either. It is deliberately wider than
     * {@link DsePeers#gossipedDseVersion}, which tells a DSE node from HCD 1.2 at the dialect for the gossip ordinals and
     * the listing: in a cluster upgraded from DSE only DSE nodes gossip such values, and the HCD 1.2 stand-in of the
     * test harness, which gossips 11, is refused like one.
     * <p>
     * In practice this refuses a node without the mode: a new or replacing node in {@code DSE_6} mode is refused
     * earlier, before its shadow round ({@link #refuseAtStartupWhileUpgrading}). The remedy of the message is the one
     * for a node without the mode.
     * <p>
     * The node a replacement replaces is not counted ({@code replaced}): this check lets a DSE 6 node lost for good be
     * replaced once no other DSE 6 node is in gossip, since the replacement streams its data from the other replicas,
     * which are then all of this software (and without the mode, as a node without the mode joins no node in the mode);
     * the procedure removes the node instead (see the class javadoc). The replacement keeps its own check that the
     * replaced node is not alive.
     *
     * @param states   the endpoint states of the shadow round
     * @param replaced the node being replaced, or null for a bootstrap
     * @throws ConfigurationException when a DSE node other than {@code replaced} is in {@code states}
     */
    public static void refuseJoinWithDsePeersInGossip(Map<InetAddressAndPort, EndpointState> states, InetAddressAndPort replaced, String operation) throws ConfigurationException
    {
        Set<InetAddressAndPort> dsePeers = new TreeSet<>();
        for (Map.Entry<InetAddressAndPort, EndpointState> e : states.entrySet())
            if (!e.getKey().equals(replaced) && !DsePeers.hasLeftTheRing(e.getValue()) && gossipsPre40Version(e.getValue()))
                dsePeers.add(e.getKey());
        if (dsePeers.isEmpty())
            return;
        throw new ConfigurationException(operation + " is refused while the DSE 6 upgrade is in progress (DSE 6 nodes in gossip: " + dsePeers +
                                         "); " + NONE_REMEDY, false);
    }

    /** True when the gossip state carries a {@code NET_VERSION} that maps to a pre-4.0 messaging version (DSE's own, or the dialect). */
    private static boolean gossipsPre40Version(EndpointState state)
    {
        VersionedValue netVersion = state.getApplicationState(ApplicationState.NET_VERSION);
        if (netVersion == null)
            return false;
        try
        {
            return DseCompat.messagingVersionFromGossip(Integer.parseInt(netVersion.value)) < MessagingService.VERSION_40;
        }
        catch (NumberFormatException ignored)
        {
            // not a version this check can interpret; the gossip code reports it
            return false;
        }
    }

    /** The error text of a refused operation. */
    public static String refusalMessage(String operation)
    {
        return refusalMessage(operation, DseCompat.enabled() ? DSE6_REMEDY : NONE_REMEDY);
    }

    private static String refusalMessage(String operation, String remedy)
    {
        Set<InetAddressAndPort> peers = knownDsePeers();
        StringBuilder message = new StringBuilder(operation).append(" is refused while the DSE 6 upgrade is in progress (internode_compatibility_mode: ");
        if (DseCompat.enabled())
        {
            message.append("DSE_6");
            if (!peers.isEmpty())
                message.append(", DSE 6 peers: ").append(peers);
        }
        else
        {
            message.append("NONE, but DSE 6 peers are in the cluster: ").append(peers);
        }
        return message.append("); ").append(remedy).toString();
    }

    /**
     * True while the schema is frozen: in {@code DSE_6} mode, unless {@code cassandra.internode_compatibility_mode.allow_ddl}
     * is set.
     */
    public static boolean schemaChangesFrozen()
    {
        return DseCompat.enabled() && !CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE_ALLOW_DDL.getBoolean();
    }

    /**
     * Refuses a schema change, counted under {@link #SCHEMA_CHANGE}: called by {@code AlterSchemaStatement} for a
     * statement that would change the schema, once it has found the schema frozen ({@link #schemaChangesFrozen()}); one
     * that changes nothing ({@code CREATE ... IF NOT EXISTS} of an existing object, {@code DROP ... IF EXISTS} of a
     * missing one), as applications issue at their start, runs as without the freeze.
     *
     * @throws InvalidRequestException always
     */
    public static void refuseSchemaChange() throws InvalidRequestException
    {
        DseCompatRefusals.refused(SCHEMA_CHANGE);
        throw new InvalidRequestException(SCHEMA_FROZEN + " (or start the node with -D" +
                                          CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE_ALLOW_DDL.getKey() + "=true)");
    }

    /**
     * The status line logged periodically: the DSE peers known (live or on record), the entries of the record that are
     * not addresses, which keep a start with {@code NONE} refused as well, and the refusals so far, from one snapshot of
     * the counters.
     */
    public static String statusLine()
    {
        Set<InetAddressAndPort> peers = knownDsePeers();
        Set<String> unparsed = DsePeersRecord.unparsedEntries();
        Map<String, Long> refusals = DseCompatRefusals.snapshot();
        long total = 0;
        for (long count : refusals.values())
            total += count;
        StringBuilder line = new StringBuilder("DSE 6 upgrade in progress (internode_compatibility_mode: DSE_6): ");
        if (peers.isEmpty() && unparsed.isEmpty())
            line.append("no peer speaks the pre-4.0 dialect any more and none is on record; once every node runs this software, drop the mode " +
                        "and delete the upgrade marker");
        else if (peers.isEmpty())
            line.append("no peer speaks the pre-4.0 dialect any more");
        else
            line.append(peers.size()).append(" peer(s) speaking the pre-4.0 dialect or on record as DSE 6 peers: ").append(peers);
        if (!unparsed.isEmpty())
            line.append("; the record of DSE 6 peers ").append(DsePeersRecord.path()).append(" has entries that are not addresses, which keep ")
                .append("a start with internode_compatibility_mode NONE refused until they are removed by hand: ").append(unparsed);
        line.append("; requests refused so far: ").append(total);
        if (total > 0)
            line.append(' ').append(refusals);
        return line.toString();
    }

    /** Starts the periodic status line, once at server start in {@code DSE_6} mode (a no-op otherwise). */
    public static void startStatusLog()
    {
        if (!DseCompat.enabled() || !STATUS_LOG_STARTED.compareAndSet(false, true))
            return;
        ScheduledExecutors.scheduledTasks.scheduleWithFixedDelay(DseUpgradeGuards::logStatus,
                                                                 STATUS_LOG_INTERVAL_MINUTES, STATUS_LOG_INTERVAL_MINUTES, TimeUnit.MINUTES);
        logger.info("The DSE 6 upgrade status will be logged every {} minutes", STATUS_LOG_INTERVAL_MINUTES);
    }

    /** One status line; a failure is logged and never cancels the next runs. */
    @VisibleForTesting
    static void logStatus()
    {
        try
        {
            logger.warn(statusLine());
        }
        catch (RuntimeException e)
        {
            logger.error("Unable to compute the DSE 6 upgrade status", e);
        }
    }

    @VisibleForTesting
    static boolean statusLogStarted()
    {
        return STATUS_LOG_STARTED.get();
    }
}
