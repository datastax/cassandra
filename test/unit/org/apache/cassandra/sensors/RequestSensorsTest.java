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

import java.util.Optional;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class RequestSensorsTest
{
    private Context context1;
    private Type type1;
    private Context context2;
    private Type type2;
    private RequestSensors context1Sensors;
    private RequestSensors context2Sensors;
    private SensorsRegistry sensorsRegistry;

    @Before
    public void beforeTest()
    {
        sensorsRegistry = mock(SensorsRegistry.class);

        context1 = new Context("ks1", "t1", "id1");
        type1 = Type.READ_BYTES;

        context2 = new Context("ks2", "t2", "id2");
        type2 = Type.SEARCH_BYTES;

        context1Sensors = new RequestSensors(() -> sensorsRegistry, context1);
        context2Sensors = new RequestSensors(() -> sensorsRegistry, context2);
    }

    @Test
    public void testRegistration()
    {
        Optional<Sensor> sensor = context1Sensors.getSensor(type1);
        assertNull(sensor.orElse(null));

        context1Sensors.registerSensor(type1);

        sensor = context1Sensors.getSensor(type1);
        assertNotNull(sensor.orElse(null));

        context1Sensors.registerSensor(type1);
        assertEquals(sensor.get(), context1Sensors.getSensor(type1).get());
    }

    @Test
    public void testRegistrationWithDifferentType()
    {
        context1Sensors.registerSensor(type1);
        context1Sensors.registerSensor(type2);

        assertNotEquals(context1Sensors.getSensor(type1).get(), context1Sensors.getSensor(type2).get());
    }

    @Test
    public void testRegistrationWithDifferentContext()
    {
        context1Sensors.registerSensor(type1);
        context2Sensors.registerSensor(type1);

        assertNotEquals(context1Sensors.getSensor(type1).get(), context2Sensors.getSensor(type1).get());
    }

    @Test
    public void testIncrement()
    {
        context1Sensors.registerSensor(type1);
        context1Sensors.getSensor(type1).get().increment(1.0);
        assertEquals(1.0, context1Sensors.getSensor(type1).get().getValue(), 0);
    }

    @Test
    public void testSyncAll()
    {
        context1Sensors.registerSensor(type1);
        context1Sensors.registerSensor(type2);

        context1Sensors.getSensor(type1).get().increment(1.0);
        context1Sensors.getSensor(type2).get().increment(1.0);

        context1Sensors.syncAllSensors();
        verify(sensorsRegistry, times(1)).updateSensor(eq(context1), eq(type1), eq(1.0));
        verify(sensorsRegistry, times(1)).updateSensor(eq(context1), eq(type2), eq(1.0));
    }
}