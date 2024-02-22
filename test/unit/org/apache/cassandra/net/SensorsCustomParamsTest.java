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

import org.apache.cassandra.sensors.Context;
import org.apache.cassandra.sensors.Sensor;
import org.apache.cassandra.sensors.Type;

import static org.junit.Assert.assertEquals;

public class SensorsCustomParamsTest
{
    @Test
    public void testDoubleAsBytes()
    {
        double d = Double.MAX_VALUE;
        byte[] bytes = SensorsCustomParams.sensorValueAsBytes(d);
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        assertEquals(Double.MAX_VALUE, bb.getDouble(), 0.0);
    }

    @Test
    public void testEncodeKeyspaceAndTableInWriteByteRequestParam()
    {
        Sensor sensor = new TestSensor("ks1", "t1");
        String expectedParam = String.format("WRITE_BYTES_REQUEST.%s_%s", "ks1", "t1");
        String actualParam = SensorsCustomParams.encodeKeyspaceAndTableInWriteByteRequestParam(sensor);
        assertEquals(expectedParam, actualParam);
    }

    @Test
    public void testEncodeKeyspaceAndTableInWriteByteTableParam()
    {
        Sensor sensor = new TestSensor("ks1", "t1");
        String expectedParam = String.format("WRITE_BYTES_TABLE.%s_%s", "ks1", "t1");
        String actualParam = SensorsCustomParams.encodeKeyspaceAndTableInWriteByteTableParam(sensor);
        assertEquals(expectedParam, actualParam);
    }

    private static class TestSensor extends Sensor
    {
        public TestSensor(String keyspace, String table)
        {
            super(new Context(keyspace, table, "UUID"), Type.WRITE_BYTES);
        }
    }
}
