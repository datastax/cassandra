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

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.SimpleSnitch;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class WriteOriginTest
{
    private static final String LOCAL_DC = "dc1";
    private static final String REMOTE_DC = "dc2";

    private static InetAddressAndPort localCoordinator;
    private static InetAddressAndPort localPeer;
    private static InetAddressAndPort remoteCoordinator;
    private static InetAddressAndPort unplaced;

    private static final Map<InetAddressAndPort, String> TOPOLOGY = new HashMap<>();

    @BeforeClass
    public static void setUp() throws UnknownHostException
    {
        DatabaseDescriptor.daemonInitialization();

        localCoordinator = InetAddressAndPort.getByName("127.0.0.1");
        localPeer = InetAddressAndPort.getByName("127.0.0.2");
        remoteCoordinator = InetAddressAndPort.getByName("127.0.0.3");
        unplaced = InetAddressAndPort.getByName("127.0.0.4");

        TOPOLOGY.put(localCoordinator, LOCAL_DC);
        TOPOLOGY.put(localPeer, LOCAL_DC);
        TOPOLOGY.put(remoteCoordinator, REMOTE_DC);
    }

    private static WriteOrigin origin(InetAddressAndPort coordinator, InetAddressAndPort sender)
    {
        return WriteOrigin.origin(coordinator, sender, LOCAL_DC, TOPOLOGY::get);
    }

    @Test
    public void locallyInitiatedWriteHasNoOrigin()
    {
        WriteOrigin origin = origin(null, null);

        assertSame(WriteOrigin.LOCAL, origin);
        assertNull(origin.coordinator());
        assertNull(origin.datacenter());
        assertFalse(origin.isCrossDatacenter());
        assertFalse(origin.isDirectFromRemoteDatacenter());
    }

    @Test
    public void writeCoordinatedInThisDatacenterIsNotCrossDatacenter()
    {
        WriteOrigin origin = origin(localCoordinator, localCoordinator);

        assertEquals(localCoordinator, origin.coordinator());
        assertEquals(LOCAL_DC, origin.datacenter());
        assertFalse(origin.isCrossDatacenter());
        assertFalse(origin.isDirectFromRemoteDatacenter());
    }

    /**
     * The message that actually crossed the datacenter boundary: the remote coordinator is both the
     * coordinator and the sender.
     */
    @Test
    public void writeReceivedStraightFromARemoteCoordinatorIsDirect()
    {
        WriteOrigin origin = origin(remoteCoordinator, remoteCoordinator);

        assertEquals(remoteCoordinator, origin.coordinator());
        assertEquals(REMOTE_DC, origin.datacenter());
        assertTrue(origin.isCrossDatacenter());
        assertTrue(origin.isDirectFromRemoteDatacenter());
    }

    /**
     * The same write as seen by the replicas the relay forwarded it to: RESPOND_TO still names the remote
     * coordinator, but the sender is the local relay, so the message did not reach them directly.
     */
    @Test
    public void forwardedWriteIsCrossDatacenterButNotDirect()
    {
        WriteOrigin origin = origin(remoteCoordinator, localPeer);

        assertEquals(remoteCoordinator, origin.coordinator());
        assertEquals(REMOTE_DC, origin.datacenter());
        assertTrue(origin.isCrossDatacenter());
        assertFalse(origin.isDirectFromRemoteDatacenter());
    }

    /**
     * A sender the topology cannot place proves nothing about the delivery path, so we do not claim one.
     * The alternative -- assuming the boundary -- would hand every relayed replica a false "direct" during
     * the exact window (a node being replaced, an address just changed) when it is least verifiable.
     */
    @Test
    public void crossDatacenterWriteFromAnUnplacedSenderIsNotClaimedDirect()
    {
        WriteOrigin origin = origin(remoteCoordinator, unplaced);

        assertTrue(origin.isCrossDatacenter());
        assertFalse(origin.isDirectFromRemoteDatacenter());
    }

    /**
     * An unplaceable coordinator degrades to UNKNOWN -- not to LOCAL, whose contract is "provably
     * originated here", and not to a guessed remote datacenter.
     *
     * Note what this does NOT protect against: most snitches in this tree never return null, they return a
     * synthetic datacenter name ("UNKNOWN_DC") for an endpoint they cannot place, which compares unequal to
     * the local one and therefore reads as cross-datacenter. This branch only covers snitches that throw or
     * return null. isCrossDatacenter() is best-effort during a topology change, and says so.
     */
    @Test
    public void unplacedCoordinatorDegradesToUnknown()
    {
        WriteOrigin origin = origin(unplaced, unplaced);

        assertSame(WriteOrigin.UNKNOWN, origin);
        assertTrue(origin.isUnknown());
        // Conservative on both questions -- but distinguishable from LOCAL, which answers the same.
        assertFalse(origin.isCrossDatacenter());
        assertFalse(origin.isDirectFromRemoteDatacenter());
        assertFalse(WriteOrigin.LOCAL.isUnknown());
    }

    @Test
    public void unknownLocalDatacenterDegradesToUnknown()
    {
        assertSame(WriteOrigin.UNKNOWN, WriteOrigin.origin(remoteCoordinator, remoteCoordinator, null, TOPOLOGY::get));
    }

    /**
     * A missing sender is a determinable case, not an UNKNOWN one: the coordinator places fine, so the
     * cross-datacenter answer stands -- only the delivery path is unclaimed.
     */
    @Test
    public void missingSenderLeavesTheDeliveryPathUnclaimed()
    {
        WriteOrigin origin = origin(remoteCoordinator, null);

        assertTrue(origin.isCrossDatacenter());
        assertFalse(origin.isDirectFromRemoteDatacenter());
        assertFalse(origin.isUnknown());
    }

    /**
     * The environment-reading entry points, on their happy paths: fromMessage places both endpoints
     * through the installed snitch, and fromPeer is its single-endpoint equivalent for the paxos verbs.
     */
    @Test
    public void fromMessageAndFromPeerPlaceEndpointsThroughTheInstalledSnitch()
    {
        withSnitchAndLocalDc(placingSnitch(), LOCAL_DC, () -> {
            WriteOrigin fromMessage = WriteOrigin.fromMessage(Message.synthetic(remoteCoordinator, Verb.MUTATION_REQ, NoPayload.noPayload));
            assertEquals(remoteCoordinator, fromMessage.coordinator());
            assertTrue(fromMessage.isCrossDatacenter());
            assertTrue(fromMessage.isDirectFromRemoteDatacenter());

            WriteOrigin fromPeer = WriteOrigin.fromPeer(remoteCoordinator);
            assertEquals(remoteCoordinator, fromPeer.coordinator());
            assertTrue(fromPeer.isCrossDatacenter());
            assertTrue(fromPeer.isDirectFromRemoteDatacenter());

            // A delivery this node addressed to itself is a local apply, not an inbound one
            // (PaxosCommit#executeOnSelf reuses its request handler with the node's own address).
            InetAddressAndPort self = FBUtilities.getBroadcastAddressAndPort();
            assertSame(WriteOrigin.LOCAL, WriteOrigin.fromMessage(Message.synthetic(self, Verb.MUTATION_REQ, NoPayload.noPayload)));
            assertSame(WriteOrigin.LOCAL, WriteOrigin.fromPeer(self));
        });
    }

    /** No snitch installed at all: nothing can be placed, so the answer is "undetermined", not "local". */
    @Test
    public void missingSnitchDegradesToUnknown()
    {
        withSnitchAndLocalDc(null, LOCAL_DC, () -> {
            assertSame(WriteOrigin.UNKNOWN, WriteOrigin.fromMessage(Message.synthetic(remoteCoordinator, Verb.MUTATION_REQ, NoPayload.noPayload)));
            assertSame(WriteOrigin.UNKNOWN, WriteOrigin.fromPeer(remoteCoordinator));
        });
    }

    /**
     * This node's own datacenter cannot be resolved: neither the config nor the snitch knows it, so the
     * comparison that decides cross-datacenter has nothing to compare against.
     */
    @Test
    public void unresolvableLocalDatacenterDegradesToUnknown()
    {
        IEndpointSnitch noLocalDc = new SimpleSnitch()
        {
            @Override
            public String getLocalDatacenter()
            {
                return null;
            }
        };
        withSnitchAndLocalDc(noLocalDc, null, () -> {
            assertSame(WriteOrigin.UNKNOWN, WriteOrigin.fromMessage(Message.synthetic(remoteCoordinator, Verb.MUTATION_REQ, NoPayload.noPayload)));
            assertSame(WriteOrigin.UNKNOWN, WriteOrigin.fromPeer(remoteCoordinator));
        });
    }

    /**
     * A snitch that throws while placing an endpoint (PropertyFileSnitch does, for one it has no entry
     * for) must not fail the write -- and must not claim the write is local either.
     */
    @Test
    public void throwingSnitchDegradesToUnknown()
    {
        IEndpointSnitch throwing = new SimpleSnitch()
        {
            @Override
            public String getDatacenter(InetAddressAndPort endpoint)
            {
                throw new IllegalStateException("no topology entry for " + endpoint);
            }
        };
        withSnitchAndLocalDc(throwing, LOCAL_DC, () -> {
            assertSame(WriteOrigin.UNKNOWN, WriteOrigin.fromMessage(Message.synthetic(remoteCoordinator, Verb.MUTATION_REQ, NoPayload.noPayload)));
            assertSame(WriteOrigin.UNKNOWN, WriteOrigin.fromPeer(remoteCoordinator));
        });
    }

    /** Swaps the environment fromMessage/fromPeer read, restoring it whatever the body does. */
    private static void withSnitchAndLocalDc(IEndpointSnitch snitch, String localDc, Runnable body)
    {
        IEndpointSnitch previousSnitch = DatabaseDescriptor.getEndpointSnitch();
        String previousLocalDc = DatabaseDescriptor.getLocalDataCenter();
        DatabaseDescriptor.setEndpointSnitch(snitch);
        DatabaseDescriptor.setLocalDataCenter(localDc);
        try
        {
            body.run();
        }
        finally
        {
            DatabaseDescriptor.setEndpointSnitch(previousSnitch);
            DatabaseDescriptor.setLocalDataCenter(previousLocalDc);
        }
    }

    /** A snitch backed by the same explicit topology map the pure-factory tests use. */
    private static IEndpointSnitch placingSnitch()
    {
        return new SimpleSnitch()
        {
            @Override
            public String getDatacenter(InetAddressAndPort endpoint)
            {
                String dc = TOPOLOGY.get(endpoint);
                return dc != null ? dc : LOCAL_DC; // the local node itself is in LOCAL_DC
            }
        };
    }

    @Test
    public void mutationCarriesItsOriginAcrossFiltering()
    {
        WriteOrigin origin = origin(remoteCoordinator, remoteCoordinator);

        Mutation mutation = new Mutation("ks",
                                         DatabaseDescriptor.getPartitioner().decorateKey(ByteBufferUtil.bytes(1)),
                                         ImmutableMap.of(),
                                         0L);

        assertSame(WriteOrigin.LOCAL, mutation.origin());

        mutation.withOrigin(origin);
        assertSame(origin, mutation.origin());

        // Hint replay filters out truncated tables before applying; the origin must survive that copy.
        TableId truncated = TableId.fromString("00000000-0000-0000-0000-000000000001");
        assertSame(origin, mutation.without(Collections.singleton(truncated)).origin());

        // As must the merge a trigger's augmented mutations go through.
        assertSame(origin, Mutation.merge(Arrays.asList(mutation, mutation)).origin());

        // And the setter never installs null: LOCAL is the "no inbound message" value, not null.
        assertSame(WriteOrigin.LOCAL, mutation.withOrigin(null).origin());
    }
}
