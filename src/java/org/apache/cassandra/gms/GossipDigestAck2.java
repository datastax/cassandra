/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.gms;

import java.io.*;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.DseCompat;

import static org.apache.cassandra.locator.InetAddressAndPort.Serializer.inetAddressAndPortSerializer;

/**
 * This ack gets sent out as a result of the receipt of a GossipDigestAckMessage. This the
 * last stage of the 3 way messaging of the Gossip protocol.
 */
public class GossipDigestAck2
{
    public static final IVersionedSerializer<GossipDigestAck2> serializer = new GossipDigestAck2Serializer();

    final Map<InetAddressAndPort, EndpointState> epStateMap;
    /** DSE 6 compatibility mode: the peer this ack is sent to (see {@link DseGossipStates.Recipient}); null otherwise. */
    @Nullable
    final DseGossipStates.Recipient recipient;
    /**
     * DSE 6 compatibility mode: true for an ack read at a pre-4.0 version, whose states still have the sender's ordinals
     * until {@link #getEndpointStateMap(InetAddressAndPort)} translates them.
     */
    final boolean readInDialect;

    GossipDigestAck2(Map<InetAddressAndPort, EndpointState> epStateMap)
    {
        this(epStateMap, null, false);
    }

    /** An ack to send to {@code to}. */
    GossipDigestAck2(Map<InetAddressAndPort, EndpointState> epStateMap, InetAddressAndPort to)
    {
        this(epStateMap, DseGossipStates.recipient(to), false);
    }

    private GossipDigestAck2(Map<InetAddressAndPort, EndpointState> epStateMap, @Nullable DseGossipStates.Recipient recipient, boolean readInDialect)
    {
        this.epStateMap = epStateMap;
        this.recipient = recipient;
        this.readInDialect = readInDialect;
    }

    /** A received ack, read at {@code version}. */
    static GossipDigestAck2 received(Map<InetAddressAndPort, EndpointState> epStateMap, int version)
    {
        return new GossipDigestAck2(epStateMap, null, DseCompat.isPre40Layout(version));
    }

    boolean writesDseOrdinals(int version)
    {
        return recipient != null && recipient.dseOrdinals(version);
    }

    /** The endpoint states written at {@code version}: none at a pre-4.0 version to a peer only presumed to be a DSE 6 node, else all. */
    Map<InetAddressAndPort, EndpointState> statesToWrite(int version)
    {
        return recipient != null && recipient.withholdsStates(version) ? Collections.emptyMap() : epStateMap;
    }

    /** The endpoint states of this ack received from {@code sender}, with this software's ordinals. */
    Map<InetAddressAndPort, EndpointState> getEndpointStateMap(InetAddressAndPort sender)
    {
        return readInDialect ? DseGossipStates.receivedFrom(sender, epStateMap, true) : epStateMap;
    }
}

class GossipDigestAck2Serializer implements IVersionedSerializer<GossipDigestAck2>
{
    public void serialize(GossipDigestAck2 ack2, DataOutputPlus out, int version) throws IOException
    {
        Map<InetAddressAndPort, EndpointState> states = ack2.statesToWrite(version);
        out.writeInt(states.size());
        for (Map.Entry<InetAddressAndPort, EndpointState> entry : states.entrySet())
        {
            InetAddressAndPort ep = entry.getKey();
            inetAddressAndPortSerializer.serialize(ep, out, version);
            EndpointStateSerializer.instance.serialize(entry.getValue(), out, version, ack2.writesDseOrdinals(version));
        }
    }

    public GossipDigestAck2 deserialize(DataInputPlus in, int version) throws IOException
    {
        int size = in.readInt();
        Map<InetAddressAndPort, EndpointState> epStateMap = new HashMap<>(size);

        for (int i = 0; i < size; ++i)
        {
            InetAddressAndPort ep = inetAddressAndPortSerializer.deserialize(in, version);
            EndpointState epState = EndpointState.serializer.deserialize(in, version);
            epStateMap.put(ep, epState);
        }
        return GossipDigestAck2.received(epStateMap, version);
    }

    public long serializedSize(GossipDigestAck2 ack2, int version)
    {
        Map<InetAddressAndPort, EndpointState> states = ack2.statesToWrite(version);
        long size = TypeSizes.sizeof(states.size());
        for (Map.Entry<InetAddressAndPort, EndpointState> entry : states.entrySet())
            size += inetAddressAndPortSerializer.serializedSize(entry.getKey(), version)
                    + EndpointStateSerializer.instance.serializedSize(entry.getValue(), version, ack2.writesDseOrdinals(version));
        return size;
    }
}

