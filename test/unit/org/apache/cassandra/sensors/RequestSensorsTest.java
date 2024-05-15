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

import static org.assertj.core.api.Assertions.assertThat;
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
    private RequestSensors sensors;
    private SensorsRegistry sensorsRegistry;

    @Before
    public void beforeTest()
    {
        sensorsRegistry = mock(SensorsRegistry.class);

        context1 = new Context("ks1", "t1", "id1");
        type1 = Type.READ_BYTES;

        context2 = new Context("ks2", "t2", "id2");
        type2 = Type.WRITE_BYTES;

        context1Sensors = new RequestSensors(() -> sensorsRegistry);
        context2Sensors = new RequestSensors(() -> sensorsRegistry);
        sensors = new RequestSensors(() -> sensorsRegistry);
    }

    @Test
    public void testRegistration()
    {
        Optional<Sensor> sensor = context1Sensors.getSensor(context1, type1);
        assertThat(sensor).isEmpty();

        context1Sensors.registerSensor(context1, type1);

        sensor = context1Sensors.getSensor(context1, type1);
        assertThat(sensor).isPresent();

        context1Sensors.registerSensor(context1, type1);
        assertThat(context1Sensors.getSensor(context1, type1)).isEqualTo(sensor);
    }

    @Test
    public void testRegistrationWithMultipleContexts()
    {
        Optional<Sensor> context1Sensor = sensors.getSensor(context1, type1);
        Optional<Sensor> context2Sensor = sensors.getSensor(context2, type1);
        assertThat(context1Sensor).isEmpty();
        assertThat(context2Sensor).isEmpty();

        sensors.registerSensor(context1, type1);
        sensors.registerSensor(context2, type1);

        context1Sensor = sensors.getSensor(context1, type1);
        assertThat(context1Sensor).isPresent();

        context2Sensor = sensors.getSensor(context2, type1);
        assertThat(context2Sensor).isPresent();

        assertThat(context1Sensor).isNotEqualTo(context2Sensor);

        sensors.registerSensor(context1, type1);
        assertThat(sensors.getSensor(context1, type1)).isEqualTo(context1Sensor);

        sensors.registerSensor(context2, type1);
        assertThat(sensors.getSensor(context2, type1)).isEqualTo(context2Sensor);
    }

    @Test
    public void testRegistrationWithDifferentType()
    {
        context1Sensors.registerSensor(context1, type1);
        context1Sensors.registerSensor(context2, type2);

        assertThat(context1Sensors.getSensor(context1, type1)).isNotEqualTo(context1Sensors.getSensor(context2, type2));
    }

    @Test
    public void testRegistrationWithDifferentContext()
    {
        context1Sensors.registerSensor(context1, type1);
        context2Sensors.registerSensor(context2, type1);

        assertThat(context1Sensors.getSensor(context1, type1)).isNotEqualTo(context2Sensors.getSensor(context2, type1));
    }

    @Test
    public void testIncrement()
    {
        context1Sensors.registerSensor(context1, type1);
        context1Sensors.incrementSensor(context1, type1, 1.0);
        assertThat(context1Sensors.getSensor(context1, type1)).hasValueSatisfying((s) -> assertThat(s.getValue()).isEqualTo(1.0));
    }

    @Test
    public void testIncrementWithMultipleContexts()
    {
        sensors.registerSensor(context1, type1);
        sensors.incrementSensor(context1, type1, 1.0);
        sensors.registerSensor(context2, type1);
        sensors.incrementSensor(context2, type1, 2.0);
        assertThat(sensors.getSensor(context1, type1)).hasValueSatisfying((s) -> assertThat(s.getValue()).isEqualTo(1.0));
        assertThat(sensors.getSensor(context2, type1)).hasValueSatisfying((s) -> assertThat(s.getValue()).isEqualTo(2.0));
    }

    @Test
    public void testSyncAll()
    {
        context1Sensors.registerSensor(context1, type1);
        context1Sensors.registerSensor(context1, type2);

        context1Sensors.incrementSensor(context1, type1, 1.0);
        context1Sensors.incrementSensor(context1, type2, 1.0);

        context1Sensors.syncAllSensors();
        verify(sensorsRegistry, times(1)).incrementSensor(eq(context1), eq(type1), eq(1.0));
        verify(sensorsRegistry, times(1)).incrementSensor(eq(context1), eq(type2), eq(1.0));

        // Syncing again doesn't update the sensor
        context1Sensors.syncAllSensors();
        verify(sensorsRegistry, times(1)).incrementSensor(eq(context1), eq(type1), eq(1.0));
        verify(sensorsRegistry, times(1)).incrementSensor(eq(context1), eq(type2), eq(1.0));

        // Unless updated:
        context1Sensors.incrementSensor(context1, type1, 1.0);
        context1Sensors.incrementSensor(context1, type2, 1.0);
        context1Sensors.syncAllSensors();
        verify(sensorsRegistry, times(2)).incrementSensor(eq(context1), eq(type1), eq(1.0));
        verify(sensorsRegistry, times(2)).incrementSensor(eq(context1), eq(type2), eq(1.0));
    }

    @Test
    public void testSyncAllWithMultipleContexts()
    {
        sensors.registerSensor(context1, type1);
        sensors.registerSensor(context1, type2);
        sensors.registerSensor(context2, type1);
        sensors.registerSensor(context2, type2);

        sensors.incrementSensor(context1, type1, 1.0);
        sensors.incrementSensor(context1, type2, 1.0);
        sensors.incrementSensor(context2, type1, 1.0);
        sensors.incrementSensor(context2, type2, 1.0);

        sensors.syncAllSensors();
        verify(sensorsRegistry, times(1)).incrementSensor(eq(context1), eq(type1), eq(1.0));
        verify(sensorsRegistry, times(1)).incrementSensor(eq(context1), eq(type2), eq(1.0));
        verify(sensorsRegistry, times(1)).incrementSensor(eq(context2), eq(type1), eq(1.0));
        verify(sensorsRegistry, times(1)).incrementSensor(eq(context2), eq(type2), eq(1.0));
    }

    @Test
    public void testGetSensors()
    {
        sensors.registerSensor(context1, type1);
        sensors.registerSensor(context1, type2);
        sensors.registerSensor(context2, type1);
        sensors.registerSensor(context2, type2);

        assertThat(sensors.getSensors(type1)).hasSize(2);
        assertThat(sensors.getSensors(type1)).containsExactlyInAnyOrder(sensors.getSensor(context1, type1).get(), sensors.getSensor(context2, type1).get());

        assertThat(sensors.getSensors(type2)).hasSize(2);
        assertThat(sensors.getSensors(type2)).containsExactlyInAnyOrder(sensors.getSensor(context1, type2).get(), sensors.getSensor(context2, type2).get());
    }
}