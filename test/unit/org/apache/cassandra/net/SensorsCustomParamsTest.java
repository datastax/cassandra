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

import org.junit.Test;

import org.apache.cassandra.sensors.RequestSensors;
import org.apache.cassandra.sensors.Sensor;
import org.apache.cassandra.sensors.Type;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;

public class SensorsCustomParamsTest
{

    @Test
    public void testHeaderNames()
    {
        assertEquals("READ_BYTES_REQUEST", SensorsCustomParams.READ_BYTES_REQUEST);
        assertEquals("READ_BYTES_TABLE", SensorsCustomParams.READ_BYTES_TABLE);
        assertEquals("READ_BYTES_RATE", SensorsCustomParams.READ_BYTES_RATE);
    }

    @Test
    public void testSensorValueAsBytes()
    {
        RequestSensors requestSensors = new RequestSensors(null);
        requestSensors.registerSensor(Type.READ_BYTES);
        double d = Double.MAX_VALUE;
        requestSensors.incrementSensor(Type.READ_BYTES, d);
        Sensor sensor = requestSensors.getSensor(Type.READ_BYTES).get();
        byte[] bytes = SensorsCustomParams.sensorValueAsBytes(sensor);
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        assertEquals(d, bb.getDouble(), 0.0);
    }

    @Test
    public void testSensorValueAsByteBuffer()
    {
        double d = Double.MAX_VALUE;
        ByteBuffer bb = SensorsCustomParams.sensorValueAsByteBuffer(d);
        // bb should already be flipped
        assertEquals(bb.position(), 0);
        assertEquals(d, ByteBufferUtil.toDouble(bb), 0.0);
    }
}
