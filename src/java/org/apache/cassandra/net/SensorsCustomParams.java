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

import java.nio.ByteBuffer;

import org.apache.cassandra.sensors.Sensor;
import org.apache.cassandra.sensors.Type;

/**
 * A utility class that contians the defintion of custom params added to the {@link Message} header to prpagate {@link Sensor} values from
 * writer to cooridnater and necessary methods to encode sensor values as appropriate for the internode mesage format.
 */
public final class SensorsCustomParams
{
    /**
     * The per-request read bytes value for a given keyspace.
     */
    public static final String READ_BYTES = Type.READ_BYTES.name();
    /**
     * The aggregated read bytes value for a given keyspace, across all requests.
     */
    public static final String READ_BYTES_TOTAL = String.format("%s_TOTAL", READ_BYTES);

    private SensorsCustomParams()
    {
    }

    /**
     * Utility method to encode senors value as byte buffer in the big endian order.
     */
    public static byte[] doubleAsBytes(double value)
    {
        ByteBuffer readBytesBuffer = ByteBuffer.allocate(Double.BYTES);
        readBytesBuffer.putDouble(value);

        return readBytesBuffer.array();
    }
}
