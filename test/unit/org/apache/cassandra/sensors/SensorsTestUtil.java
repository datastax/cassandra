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

package org.apache.cassandra.sensors;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;

public final class SensorsTestUtil
{
    private SensorsTestUtil()
    {
    }

    /**
     * Returns the sensor with the given context and type from the global registry
     * @param context the sensor context
     * @param type the sensor type
     * @return the requested sensor from the global registry
     */
    static Sensor getRegistrySensor(Context context, Type type)
    {
        return SensorsRegistry.instance.getOrCreateSensor(context, type).get();
    }

    /**
     * Returns the sensor registered in the thread local {@link RequestSensors}
     * @return the thread local read sensor
     */
    static Sensor getThreadLocalRequestSensor(Context context, Type type)
    {
        return RequestTracker.instance.get().getSensor(context, type).get();
    }

    static ColumnFamilyStore discardSSTables(String ks, String cf)
    {
        Keyspace keyspace = Keyspace.open(ks);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cf);
        cfs.discardSSTables(System.currentTimeMillis());
        return cfs;
    }

    static DataOutputBuffer serialize(Message message)
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            int messagingVersion = MessagingService.current_version;
            Message.serializer.serialize(message, out, messagingVersion);
            return out;
        }
        catch (IOException e)
        {
            throw new RuntimeException("Cannot serialize message " + message, e);
        }
    }

    static Message deserialize(DataOutputBuffer out, InetAddressAndPort peer)
    {
        try (DataInputBuffer in = new DataInputBuffer(out.buffer(), false))
        {
            int messagingVersion = MessagingService.current_version;
            return Message.serializer.deserialize(in, peer, messagingVersion);
        }
        catch (IOException e)
        {
            throw new RuntimeException("Cannot deserialize message from " + peer, e);
        }
    }

    static double bytesToDouble(byte[] bytes)
    {
        ByteBuffer readBytesBuffer = ByteBuffer.allocate(Double.BYTES);
        readBytesBuffer.put(bytes);
        readBytesBuffer.flip();
        return readBytesBuffer.getDouble();
    }
}
