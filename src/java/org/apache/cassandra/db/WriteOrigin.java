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

package org.apache.cassandra.db;

import java.util.function.Function;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;

/**
 * Where a replica-side apply came from.
 * <p>
 * A replica applying a mutation cannot otherwise tell whether the write was coordinated by a node in its
 * own datacenter or in a remote one: every inbound {@code MUTATION_REQ} is applied with
 * {@link WriteOptions#DEFAULT}, exactly like a write this node coordinates itself. That distinction is not
 * derivable from the {@link Mutation} — it lives only in the {@link Message} — so it is captured here by the
 * verb handlers and carried to {@link CassandraWriteContext}, which secondary indexes already receive in
 * {@code Index.Group#indexerFor}.
 * <p>
 * Two different endpoints of the inbound message answer two different questions:
 * <ul>
 *   <li>{@link Message#respondTo()} is the <em>coordinator</em>. For a write relayed inside a remote
 *       datacenter, {@code ForwardingInfo} handling in {@link MutationVerbHandler} puts the original
 *       coordinator in {@code RESPOND_TO}, so this is the true origin on both the direct and the forwarded
 *       leg. It drives {@link #isCrossDatacenter()}, which is true on <em>every</em> replica in the
 *       receiving datacenter.</li>
 *   <li>{@link Message#from()} is the <em>sender</em>. It is never serialized — the receiver stamps it with
 *       the connection peer — so it is the relay node for a forwarded message and the remote coordinator for
 *       a message delivered straight to this replica. It drives
 *       {@link #isDirectFromRemoteDatacenter()}.</li>
 * </ul>
 * <b>Read {@link #isDirectFromRemoteDatacenter()}'s contract before inferring anything from it.</b> It is
 * tempting to read it as "I am this datacenter's single entry point for the write", and for an ordinary
 * write that is exactly what it means — but only because {@code MUTATION_REQ} is the one verb that fans out
 * inside the receiving datacenter. Every other verb here is sent point-to-point to each replica, so all of
 * them see it as true.
 * <p>
 * Instances are immutable and are created once per inbound message, not per partition update.
 */
public final class WriteOrigin
{
    private static final Logger logger = LoggerFactory.getLogger(WriteOrigin.class);

    /**
     * A write with no inbound message behind it: coordinated by this node on behalf of a client, or applied
     * by an internal path such as commit log replay, batchlog replay of a locally owned mutation, or a
     * paxos commit applied where it was proposed. Never cross-datacenter -- this constant means the write
     * genuinely originated here, not "we could not tell"; that is {@link #UNKNOWN}.
     */
    public static final WriteOrigin LOCAL = new WriteOrigin(null, null, false, false);

    /**
     * The origin could not be determined: the snitch is missing or misbehaving, this node's own datacenter
     * is not configured, or placing an endpoint threw. The write may well have come from another
     * datacenter -- there is just no way to tell -- so this is deliberately NOT {@link #LOCAL}, whose
     * contract is "genuinely originated here".
     * <p>
     * Both boolean accessors answer false, because answering true to either would be a guess; a consumer
     * that must distinguish "provably local" from "undetermined" checks {@link #isUnknown()}.
     */
    public static final WriteOrigin UNKNOWN = new WriteOrigin(null, null, false, false);

    private final InetAddressAndPort coordinator;
    private final String datacenter;
    private final boolean crossDatacenter;
    private final boolean directFromRemoteDatacenter;

    private WriteOrigin(InetAddressAndPort coordinator,
                        String datacenter,
                        boolean crossDatacenter,
                        boolean directFromRemoteDatacenter)
    {
        this.coordinator = coordinator;
        this.datacenter = datacenter;
        this.crossDatacenter = crossDatacenter;
        this.directFromRemoteDatacenter = directFromRemoteDatacenter;
    }

    /**
     * Derives the origin of an inbound mutation-carrying message.
     * <p>
     * Never throws: any failure yields {@link #UNKNOWN} -- not {@link #LOCAL}, because a write we could
     * not place may well be cross-datacenter, and LOCAL promises it is not.
     * <p>
     * Note that "the snitch cannot place this endpoint" is mostly <em>not</em> such a failure. Most snitches
     * in this tree ({@code GossipingPropertyFileSnitch}, the cloud metadata snitches) answer an endpoint they
     * do not know with a synthetic datacenter name rather than null, so an endpoint missing from the local
     * topology reads as a real — and therefore remote — datacenter. Consumers must treat
     * {@link #isCrossDatacenter()} as best-effort during a topology change, not as ground truth.
     */
    public static WriteOrigin fromMessage(Message<?> message)
    {
        try
        {
            // A message this node addressed to itself is not an inbound write in any meaningful sense:
            // paths that reuse their verb handler for local execution (PaxosCommit#executeOnSelf) would
            // otherwise report the node as its own coordinator instead of LOCAL.
            if (FBUtilities.getBroadcastAddressAndPort().equals(message.respondTo()))
                return LOCAL;

            IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();
            if (snitch == null)
                return UNKNOWN;

            String localDatacenter = DatabaseDescriptor.getLocalDataCenter();
            if (localDatacenter == null)
                localDatacenter = snitch.getLocalDatacenter();

            return origin(message.respondTo(), message.from(), localDatacenter, snitch::getDatacenter);
        }
        catch (Throwable t)
        {
            JVMStabilityInspector.inspectThrowable(t);
            // A snitch that throws for an endpoint (PropertyFileSnitch does, for one it has no entry for)
            // must not fail the write. Degrade to "undetermined" and say so once per problem endpoint
            // rather than on every mutation -- this runs on the mutation stage.
            logger.debug("Could not determine the origin of a {} from {}", message.verb(), message.from(), t);
            return UNKNOWN;
        }
    }

    /**
     * Derives the origin of an apply learned from a single peer, for verb handlers whose payload is not
     * a {@link Mutation} and whose verbs never carry {@code FORWARD_TO} -- the paxos commit family, where
     * {@code respondTo() == from()} always, so the peer is both coordinator and sender. Same never-throws
     * contract as {@link #fromMessage}.
     */
    public static WriteOrigin fromPeer(InetAddressAndPort peer)
    {
        try
        {
            // See fromMessage: a delivery from this node itself is a local apply, not an inbound one.
            if (FBUtilities.getBroadcastAddressAndPort().equals(peer))
                return LOCAL;

            IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();
            if (snitch == null)
                return UNKNOWN;

            String localDatacenter = DatabaseDescriptor.getLocalDataCenter();
            if (localDatacenter == null)
                localDatacenter = snitch.getLocalDatacenter();

            return origin(peer, peer, localDatacenter, snitch::getDatacenter);
        }
        catch (Throwable t)
        {
            JVMStabilityInspector.inspectThrowable(t);
            logger.debug("Could not determine the origin of an apply from {}", peer, t);
            return UNKNOWN;
        }
    }

    /**
     * The datacenter-placement logic, free of any snitch or configuration lookup: the coordinator and
     * the sender of the message, this node's datacenter, and the topology to place them with.
     * <p>
     * {@code datacenterOf} may return null for an endpoint it cannot place. Undetermined cases resolve
     * <em>away</em> from claiming something we cannot support, and are honest about which kind of
     * "cannot" they are: no coordinator at all means the write genuinely started here ({@link #LOCAL}),
     * while a coordinator or a local datacenter we cannot place means we simply do not know
     * ({@link #UNKNOWN}). An unplaceable sender leaves {@link #isDirectFromRemoteDatacenter()} false
     * rather than asserting a delivery path we did not observe.
     * (In practice most snitches here never return null — see {@link #fromMessage}.)
     * <p>
     * Public so that code reacting to an origin -- a secondary index, typically -- can build one
     * deterministically in a unit test instead of standing up a cluster.
     */
    public static WriteOrigin origin(@Nullable InetAddressAndPort coordinator,
                                     @Nullable InetAddressAndPort sender,
                                     @Nullable String localDatacenter,
                                     Function<InetAddressAndPort, String> datacenterOf)
    {
        if (coordinator == null)
            return LOCAL;

        if (localDatacenter == null)
            return UNKNOWN;

        String coordinatorDatacenter = datacenterOf.apply(coordinator);
        if (coordinatorDatacenter == null)
            return UNKNOWN;

        if (localDatacenter.equals(coordinatorDatacenter))
            return new WriteOrigin(coordinator, coordinatorDatacenter, false, false);

        // The write crossed a datacenter boundary. It reached this node directly from that datacenter only
        // if the sender is the one that sat on the far side of the boundary; a message relayed by a peer of
        // this datacenter was sent by that peer. A sender we cannot place proves nothing, so say no.
        String senderDatacenter = sender == null ? null : datacenterOf.apply(sender);
        boolean direct = senderDatacenter != null && !localDatacenter.equals(senderDatacenter);

        return new WriteOrigin(coordinator, coordinatorDatacenter, true, direct);
    }

    /**
     * The node that coordinated the write, or null when it was initiated locally (see {@link #LOCAL}).
     * For a hint this is the node that held the hint, and for a read repair the node that ran the read —
     * that is, the node the apply came from, which is not necessarily the node the client talked to.
     */
    @Nullable
    public InetAddressAndPort coordinator()
    {
        return coordinator;
    }

    /** The coordinator's datacenter, or null when the write was initiated locally. */
    @Nullable
    public String datacenter()
    {
        return datacenter;
    }

    /** Whether the write was coordinated in a datacenter other than this node's. */
    public boolean isCrossDatacenter()
    {
        return crossDatacenter;
    }

    /**
     * Whether the message reached this node <em>directly</em> from a node in the coordinator's remote
     * datacenter, rather than being relayed to it by a peer of its own datacenter. False for every locally
     * coordinated write.
     * <p>
     * This is a statement about the delivery path, not an election. Whether it also identifies a
     * <em>unique</em> node per receiving datacenter depends entirely on how the verb fans out:
     * <ul>
     *   <li>{@code MUTATION_REQ} — <b>unique</b>. {@code StorageProxy.sendMessagesToNonlocalDC} sends one
     *       message per remote datacenter and lets the receiver fan it out via {@code ForwardingInfo}, so
     *       exactly one replica per receiving datacenter sees this true. (A datacenter with a single
     *       contacted replica gets no {@code ForwardingInfo}, and that replica is still the only one.)</li>
     *   <li>{@code READ_REPAIR_REQ}, {@code HINT_REQ}, {@code PAXOS2_COMMIT_REMOTE_REQ} — <b>not unique</b>.
     *       {@code BlockingPartitionRepair}, {@code HintsDispatcher} and {@code PaxosCommit} each send
     *       point-to-point to every destination, so <em>every</em> replica in the receiving datacenter sees
     *       this true.</li>
     * </ul>
     * Do not use it to elect one node per datacenter unless you have restricted yourself to the standard
     * write path and are willing to have that coupling break silently if the transport changes.
     */
    public boolean isDirectFromRemoteDatacenter()
    {
        return directFromRemoteDatacenter;
    }

    /** True only for {@link #UNKNOWN}: the origin could not be determined, as opposed to provably local. */
    public boolean isUnknown()
    {
        return this == UNKNOWN;
    }

    @Override
    public String toString()
    {
        if (this == UNKNOWN)
            return "WriteOrigin{unknown}";
        if (coordinator == null)
            return "WriteOrigin{local}";

        return "WriteOrigin{coordinator=" + coordinator
               + ", dc=" + datacenter
               + ", crossDc=" + crossDatacenter
               + ", direct=" + directFromRemoteDatacenter
               + '}';
    }
}
