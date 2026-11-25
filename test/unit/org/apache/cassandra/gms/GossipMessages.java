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
import java.util.List;
import java.util.Map;

import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.locator.InetAddressAndPort;

/** Test access to the package-private gossip message constructors and readers, for the tests of other packages. */
public final class GossipMessages
{
    private GossipMessages()
    {
    }

    public static GossipDigest digest(InetAddressAndPort endpoint, int generation, int maxVersion)
    {
        return new GossipDigest(endpoint, generation, maxVersion);
    }

    public static GossipDigestAck ack(List<GossipDigest> digests, Map<InetAddressAndPort, EndpointState> states)
    {
        return new GossipDigestAck(digests, states);
    }

    public static GossipDigestAck2 ack2(Map<InetAddressAndPort, EndpointState> states)
    {
        return new GossipDigestAck2(states);
    }

    /**
     * The endpoint states of an ack2 carrying {@code states}, written and read at {@code version}, as its verb handler
     * takes them from {@code sender} (at a pre-4.0 version in DSE 6 compatibility mode: after the sender is identified
     * by its own state, if the ack2 carries it, and translated from DSE's ordinals when it is a DSE node).
     */
    public static Map<InetAddressAndPort, EndpointState> receivedAck2(InetAddressAndPort sender, Map<InetAddressAndPort, EndpointState> states, int version) throws IOException
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            GossipDigestAck2.serializer.serialize(new GossipDigestAck2(states), out, version);
            try (DataInputBuffer in = new DataInputBuffer(out.getData()))
            {
                return GossipDigestAck2.serializer.deserialize(in, version).getEndpointStateMap(sender);
            }
        }
    }
}
