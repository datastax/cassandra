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
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.utils.ByteBufferUtil;

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
        return WriteOrigin.create(coordinator, sender, LOCAL_DC, TOPOLOGY::get);
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
     * An unplaceable coordinator degrades to "local" rather than to a guessed remote datacenter.
     *
     * Note what this does NOT protect against: most snitches in this tree never return null, they return a
     * synthetic datacenter name ("UNKNOWN_DC") for an endpoint they cannot place, which compares unequal to
     * the local one and therefore reads as cross-datacenter. This branch only covers snitches that throw or
     * return null. isCrossDatacenter() is best-effort during a topology change, and says so.
     */
    @Test
    public void unplacedCoordinatorDegradesToLocal()
    {
        assertSame(WriteOrigin.LOCAL, origin(unplaced, unplaced));
    }

    @Test
    public void unknownLocalDatacenterDegradesToLocal()
    {
        assertSame(WriteOrigin.LOCAL, WriteOrigin.create(remoteCoordinator, remoteCoordinator, null, TOPOLOGY::get));
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
