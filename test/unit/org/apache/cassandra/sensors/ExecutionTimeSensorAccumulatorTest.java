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

import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ExecutionTimeSensorAccumulatorTest
{
    private Context context1;
    private Context context2;
    private RequestSensors sensors;

    @Before
    public void setUp()
    {
        context1 = new Context("ks", "t1", "id1");
        context2 = new Context("ks", "t2", "id2");
        sensors = new ActiveRequestSensors();
        sensors.registerSensor(context1, Type.WRITE_EXECUTION_TIME);
        sensors.registerSensor(context2, Type.WRITE_EXECUTION_TIME);
    }

    // -------------------------------------------------------------------------
    // Basic single-context behaviour
    // -------------------------------------------------------------------------

    @Test
    public void testSingleContext_firesAtThreshold()
    {
        ExecutionTimeSensorAccumulator acc = new ExecutionTimeSensorAccumulator(2);

        acc.accumulate(context1, Type.WRITE_EXECUTION_TIME, 100.0);
        acc.onResponse(sensors); // count = 1, below threshold
        assertThat(sensorValue(context1, Type.WRITE_EXECUTION_TIME)).isEqualTo(0.0);

        acc.accumulate(context1, Type.WRITE_EXECUTION_TIME, 200.0);
        acc.onResponse(sensors); // count = 2 = threshold → fires
        assertThat(sensorValue(context1, Type.WRITE_EXECUTION_TIME)).isEqualTo(200.0); // max(100, 200)
    }

    @Test
    public void testSingleContext_beyondThresholdDoesNotFireAgain()
    {
        ExecutionTimeSensorAccumulator acc = new ExecutionTimeSensorAccumulator(1);

        acc.accumulate(context1, Type.WRITE_EXECUTION_TIME, 100.0);
        acc.onResponse(sensors); // fires → 100.0
        acc.accumulate(context1, Type.WRITE_EXECUTION_TIME, 999.0);
        acc.onResponse(sensors); // beyond threshold — no second fire
        assertThat(sensorValue(context1, Type.WRITE_EXECUTION_TIME)).isEqualTo(100.0);
    }

    // -------------------------------------------------------------------------
    // Multi-context behaviour (multi-table mutation)
    // -------------------------------------------------------------------------

    @Test
    public void testMultiContext_allContextsFiredAtThreshold()
    {
        ExecutionTimeSensorAccumulator acc = new ExecutionTimeSensorAccumulator(2);

        // Replica 1 reports times for both tables
        acc.accumulate(context1, Type.WRITE_EXECUTION_TIME, 100.0);
        acc.accumulate(context2, Type.WRITE_EXECUTION_TIME, 150.0);
        acc.onResponse(sensors); // count = 1

        assertThat(sensorValue(context1, Type.WRITE_EXECUTION_TIME)).isEqualTo(0.0);
        assertThat(sensorValue(context2, Type.WRITE_EXECUTION_TIME)).isEqualTo(0.0);

        // Replica 2 reports times for both tables
        acc.accumulate(context1, Type.WRITE_EXECUTION_TIME, 200.0);
        acc.accumulate(context2, Type.WRITE_EXECUTION_TIME, 50.0);
        acc.onResponse(sensors); // count = 2 = threshold → fires both

        assertThat(sensorValue(context1, Type.WRITE_EXECUTION_TIME)).isEqualTo(200.0); // max(100, 200)
        assertThat(sensorValue(context2, Type.WRITE_EXECUTION_TIME)).isEqualTo(150.0); // max(150, 50)
    }

    @Test
    public void testMultiContext_incrementsSensorAdditively()
    {
        // The sensor already has a value (e.g. from a prior CAS phase).
        // onResponse must increment, not set.
        sensors.incrementSensor(context1, Type.WRITE_EXECUTION_TIME, 500.0);

        ExecutionTimeSensorAccumulator acc = new ExecutionTimeSensorAccumulator(1);
        acc.accumulate(context1, Type.WRITE_EXECUTION_TIME, 300.0);
        acc.onResponse(sensors);

        assertThat(sensorValue(context1, Type.WRITE_EXECUTION_TIME)).isEqualTo(800.0); // 500 + 300
    }

    // -------------------------------------------------------------------------
    // Null sensors
    // -------------------------------------------------------------------------

    @Test
    public void testNullSensors_noOp()
    {
        ExecutionTimeSensorAccumulator acc = new ExecutionTimeSensorAccumulator(1);
        acc.accumulate(context1, Type.WRITE_EXECUTION_TIME, 100.0);
        acc.onResponse(null); // must not throw
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private double sensorValue(Context context, Type type)
    {
        return sensors.getSensor(context, type)
                      .map(Sensor::getValue)
                      .orElse(0.0);
    }
}
