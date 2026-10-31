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

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
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
 * This ack gets sent out as a result of the receipt of a GossipDigestSynMessage by an
 * endpoint. This is the 2 stage of the 3 way messaging in the Gossip protocol.
 */
public class GossipDigestAck
{
    public static final IVersionedSerializer<GossipDigestAck> serializer = new GossipDigestAckSerializer();

    final List<GossipDigest> gDigestList;
    final Map<InetAddressAndPort, EndpointState> epStateMap;
    /** DSE 6 compatibility mode: the peer this ack is sent to (see {@link DseGossipStates.Recipient}); null otherwise. */
    @Nullable
    final DseGossipStates.Recipient recipient;
    /**
     * DSE 6 compatibility mode: true for an ack read at a pre-4.0 version, whose states still have the sender's ordinals
     * until {@link #getEndpointStateMap(InetAddressAndPort)} translates them.
     */
    final boolean readInDialect;

    GossipDigestAck(List<GossipDigest> gDigestList, Map<InetAddressAndPort, EndpointState> epStateMap)
    {
        this(gDigestList, epStateMap, null, false);
    }

    /** An ack to send to {@code to}. */
    GossipDigestAck(List<GossipDigest> gDigestList, Map<InetAddressAndPort, EndpointState> epStateMap, InetAddressAndPort to)
    {
        this(gDigestList, epStateMap, DseGossipStates.recipient(to), false);
    }

    /** The answer to the shadow round of {@code to}: no digest, {@code epStateMap}, whose states are never left out. */
    static GossipDigestAck shadowRoundAnswer(Map<InetAddressAndPort, EndpointState> epStateMap, InetAddressAndPort to)
    {
        return new GossipDigestAck(Collections.emptyList(), epStateMap, DseGossipStates.shadowRoundRecipient(to), false);
    }

    private GossipDigestAck(List<GossipDigest> gDigestList, Map<InetAddressAndPort, EndpointState> epStateMap,
                            @Nullable DseGossipStates.Recipient recipient, boolean readInDialect)
    {
        this.gDigestList = gDigestList;
        this.epStateMap = epStateMap;
        this.recipient = recipient;
        this.readInDialect = readInDialect;
    }

    /** A received ack, read at {@code version}. */
    static GossipDigestAck received(List<GossipDigest> gDigestList, Map<InetAddressAndPort, EndpointState> epStateMap, int version)
    {
        return new GossipDigestAck(gDigestList, epStateMap, null, DseCompat.isPre40Layout(version));
    }

    boolean writesDseOrdinals(int version)
    {
        return recipient != null && recipient.dseOrdinals(version);
    }

    /**
     * The endpoint states written at {@code version}: none at a pre-4.0 version to a peer only presumed to be a DSE 6 node
     * (except in the answer to a shadow round), else all.
     */
    Map<InetAddressAndPort, EndpointState> statesToWrite(int version)
    {
        return recipient != null && recipient.withholdsStates(version) ? Collections.emptyMap() : epStateMap;
    }

    List<GossipDigest> getGossipDigestList()
    {
        return gDigestList;
    }

    /** The endpoint states of this ack received from {@code sender}, with this software's ordinals. */
    Map<InetAddressAndPort, EndpointState> getEndpointStateMap(InetAddressAndPort sender)
    {
        return readInDialect ? DseGossipStates.receivedFrom(sender, epStateMap, !gDigestList.isEmpty()) : epStateMap;
    }
}

class GossipDigestAckSerializer implements IVersionedSerializer<GossipDigestAck>
{
    public void serialize(GossipDigestAck gDigestAckMessage, DataOutputPlus out, int version) throws IOException
    {
        GossipDigestSerializationHelper.serialize(gDigestAckMessage.gDigestList, out, version);
        Map<InetAddressAndPort, EndpointState> states = gDigestAckMessage.statesToWrite(version);
        out.writeInt(states.size());
        for (Map.Entry<InetAddressAndPort, EndpointState> entry : states.entrySet())
        {
            InetAddressAndPort ep = entry.getKey();
            inetAddressAndPortSerializer.serialize(ep, out, version);
            EndpointStateSerializer.instance.serialize(entry.getValue(), out, version, gDigestAckMessage.writesDseOrdinals(version));
        }
    }

    public GossipDigestAck deserialize(DataInputPlus in, int version) throws IOException
    {
        List<GossipDigest> gDigestList = GossipDigestSerializationHelper.deserialize(in, version);
        int size = in.readInt();
        Map<InetAddressAndPort, EndpointState> epStateMap = new HashMap<InetAddressAndPort, EndpointState>(size);

        for (int i = 0; i < size; ++i)
        {
            InetAddressAndPort ep = inetAddressAndPortSerializer.deserialize(in, version);
            EndpointState epState = EndpointState.serializer.deserialize(in, version);
            epStateMap.put(ep, epState);
        }
        return GossipDigestAck.received(gDigestList, epStateMap, version);
    }

    public long serializedSize(GossipDigestAck ack, int version)
    {
        int size = GossipDigestSerializationHelper.serializedSize(ack.gDigestList, version);
        Map<InetAddressAndPort, EndpointState> states = ack.statesToWrite(version);
        size += TypeSizes.sizeof(states.size());
        for (Map.Entry<InetAddressAndPort, EndpointState> entry : states.entrySet())
            size += inetAddressAndPortSerializer.serializedSize(entry.getKey(), version)
                    + EndpointStateSerializer.instance.serializedSize(entry.getValue(), version, ack.writesDseOrdinals(version));
        return size;
    }
}
