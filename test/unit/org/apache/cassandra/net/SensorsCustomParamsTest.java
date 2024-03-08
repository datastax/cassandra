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
    public void testEncodeTableInWriteByteRequestParam()
    {
        String table = "t1";
        String expectedParam = String.format("WRITE_BYTES_REQUEST.%s", "t1");
        String actualParam = SensorsCustomParams.encodeTableInWriteByteRequestParam(table);
        assertEquals(expectedParam, actualParam);
    }

    @Test
    public void testEncodeTableInWriteByteTableParam()
    {
        String table = "t1";
        String expectedParam = String.format("WRITE_BYTES_TABLE.%s", "t1");
        String actualParam = SensorsCustomParams.encodeTableInWriteByteTableParam(table);
        assertEquals(expectedParam, actualParam);
    }
}
