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

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class SensorsFactoryTest
{
    @Test
    public void testDefaultRequestSensors()
    {
        SensorsFactory factory = SensorsFactory.instance;
        RequestSensors sensors = factory.createRequestSensors("ks1", "ks2");

        assertThat(sensors).isInstanceOf(NoOpRequestSensors.class);
        assertThat(factory.createRequestSensors("ks1", "ks2")).isSameAs(sensors);
    }

    @Test
    public void testDefaultSensorEncoder()
    {
        SensorsFactory factory = SensorsFactory.instance;
        SensorEncoder encoder = factory.createSensorEncoder();
        Sensor sensor = new Sensor(new Context("ks1", "t1", "id1"), Type.READ_BYTES);

        assertThat(encoder.encodeRequestSensorName(sensor)).isEmpty();
        assertThat(encoder.encodeGlobalSensorName(sensor)).isEmpty();
        assertThat(encoder).isSameAs(factory.createSensorEncoder());
    }
}
