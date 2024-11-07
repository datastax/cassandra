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

package org.apache.cassandra.distributed.test.sensors;

import java.nio.ByteBuffer;
import java.util.Map;

import org.junit.Test;

import org.apache.cassandra.utils.ByteBufferUtil;
import org.assertj.core.api.Assertions;

public class SensorsTest extends AbstractSensorsTest
{
    @Test
    public void testSensorsInCQLResponseEnabled() throws Throwable
    {
        Map<String, ByteBuffer> customPayload = executeTest(true);
        for (String header : expectedHeaders)
        {
            double requestBytes = getBytesForHeader(customPayload, header);
            Assertions.assertThat(requestBytes).isGreaterThan(0D);
        }
    }

    @Test
    public void testSensorsInCQLResponseDisabled() throws Throwable
    {
        Map<String, ByteBuffer> customPayload = executeTest(false);
        // customPayload will be null if it has no headers. However, non-sensor headers could've been added. So here we check for nullability or non-existence of sensor headers
        if (customPayload != null)
        {
            for (String header : expectedHeaders)
            {
                Assertions.assertThat(customPayload).doesNotContainKey(header);
            }
        } // else do nothing as null customPayload means no sensors were added
    }

    private double getBytesForHeader(Map<String, ByteBuffer> customPayload, String expectedHeader)
    {
        Assertions.assertThat(customPayload).containsKey(expectedHeader);
        return ByteBufferUtil.toDouble(customPayload.get(expectedHeader));
    }
}