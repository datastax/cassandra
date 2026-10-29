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
package org.apache.cassandra.net;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

import static org.apache.cassandra.locator.InetAddressAndPort.Serializer.inetAddressAndPortSerializer;
import static org.apache.cassandra.net.MessagingService.VERSION_40;
import static org.apache.cassandra.utils.vint.VIntCoding.computeUnsignedVIntSize;

/**
 * A container used to store a node -> message_id map for inter-DC write forwarding.
 * We pick one node in each external DC to forward the message to its local peers.
 *
 */
public final class ForwardingInfo implements Serializable
{
    final List<InetAddressAndPort> targets;
    final long[] messageIds;

    public ForwardingInfo(List<InetAddressAndPort> targets, long[] messageIds)
    {
        Preconditions.checkArgument(targets.size() == messageIds.length);
        this.targets = targets;
        this.messageIds = messageIds;
    }

    /**
     * @return {@code true} if all host are to use the same message id, {@code false} otherwise. Starting with 4.0 and
     * above, we should be reusing the same id, always, but it won't always be true until 3.0/3.11 are phased out.
     * In DSE 6 compatibility mode, a DSE 6 coordinator lists an id of its own for each target, as Cassandra 3.0
     * did.
     */
    public boolean useSameMessageID(long id)
    {
        for (int i = 0; i < messageIds.length; i++)
            if (id != messageIds[i])
                return false;

        return true;
    }

    /**
     * Apply the provided consumer to all (host, message_id) pairs.
     */
    public void forEach(BiConsumer<Long, InetAddressAndPort> biConsumer)
    {
        for (int i = 0; i < messageIds.length; i++)
            biConsumer.accept(messageIds[i], targets.get(i));
    }

    /**
     * Serializer; below {@link MessagingService#VERSION_40} (the pre-4.0 dialect of the DSE 6 compatibility mode)
     * the count and the message ids are 4-byte ints and the addresses carry no port, the {@code FWD_TO} layout of
     * Cassandra 3.0.
     */
    static final IVersionedSerializer<ForwardingInfo> serializer = new IVersionedSerializer<ForwardingInfo>()
    {
        public void serialize(ForwardingInfo forwardTo, DataOutputPlus out, int version) throws IOException
        {
            boolean pre40 = DseCompat.isPre40Layout(version);
            assert pre40 || version >= VERSION_40;
            long[] ids = forwardTo.messageIds;
            List<InetAddressAndPort> targets = forwardTo.targets;

            int count = ids.length;
            if (pre40)
                out.writeInt(count);
            else
                out.writeUnsignedVInt32(count);

            for (int i = 0; i < count; i++)
            {
                inetAddressAndPortSerializer.serialize(targets.get(i), out, version);
                if (pre40)
                    out.writeInt(Ints.checkedCast(ids[i]));
                else
                    out.writeUnsignedVInt(ids[i]);
            }
        }

        public long serializedSize(ForwardingInfo forwardTo, int version)
        {
            boolean pre40 = DseCompat.isPre40Layout(version);
            assert pre40 || version >= VERSION_40;
            long[] ids = forwardTo.messageIds;
            List<InetAddressAndPort> targets = forwardTo.targets;

            int count = ids.length;
            long size = pre40 ? TypeSizes.sizeof(count) : computeUnsignedVIntSize(count);

            for (int i = 0; i < count; i++)
            {
                size += inetAddressAndPortSerializer.serializedSize(targets.get(i), version);
                size += pre40 ? TypeSizes.INT_SIZE : computeUnsignedVIntSize(ids[i]);
            }

            return size;
        }

        public ForwardingInfo deserialize(DataInputPlus in, int version) throws IOException
        {
            boolean pre40 = DseCompat.isPre40Layout(version);
            assert pre40 || version >= VERSION_40;
            int count = pre40 ? in.readInt() : in.readUnsignedVInt32();

            long[] ids = new long[count];
            List<InetAddressAndPort> targets = new ArrayList<>(count);

            for (int i = 0; i < count; i++)
            {
                targets.add(inetAddressAndPortSerializer.deserialize(in, version));
                ids[i] = pre40 ? in.readInt() : in.readUnsignedVInt32();
            }

            return new ForwardingInfo(targets, ids);
        }
    };
}
