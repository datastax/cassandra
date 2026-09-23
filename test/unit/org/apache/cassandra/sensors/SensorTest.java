/*
 * Copyright IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.sensors;

import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class SensorTest
{
    private Context context;
    private Sensor sensor;

    @Before
    public void setUp()
    {
        context = new Context("ks", "t", "id");
        sensor = new Sensor(context, Type.READ_BYTES);
    }

    // ---- increment / getValue / reset ----

    @Test
    public void testInitialValueIsZero()
    {
        assertThat(sensor.getValue()).isEqualTo(0.0);
    }

    @Test
    public void testIncrement()
    {
        sensor.increment(10.0);
        assertThat(sensor.getValue()).isEqualTo(10.0);
    }

    @Test
    public void testIncrementAccumulates()
    {
        sensor.increment(10.0);
        sensor.increment(5.0);
        assertThat(sensor.getValue()).isEqualTo(15.0);
    }

    @Test
    public void testReset()
    {
        sensor.increment(10.0);
        sensor.reset();
        assertThat(sensor.getValue()).isEqualTo(0.0);
    }

    // ---- equals / hashCode ----

    @Test
    public void testEquals_sameContextAndType()
    {
        Sensor other = new Sensor(new Context("ks", "t", "id"), Type.READ_BYTES);
        assertThat(sensor).isEqualTo(other);
    }

    @Test
    public void testEquals_differentType()
    {
        Sensor other = new Sensor(context, Type.WRITE_BYTES);
        assertThat(sensor).isNotEqualTo(other);
    }

    @Test
    public void testEquals_differentContext()
    {
        Sensor other = new Sensor(new Context("ks2", "t", "id"), Type.READ_BYTES);
        assertThat(sensor).isNotEqualTo(other);
    }

    @Test
    public void testHashCode_consistentWithEquals()
    {
        Sensor other = new Sensor(new Context("ks", "t", "id"), Type.READ_BYTES);
        assertThat(sensor.hashCode()).isEqualTo(other.hashCode());
    }

}
