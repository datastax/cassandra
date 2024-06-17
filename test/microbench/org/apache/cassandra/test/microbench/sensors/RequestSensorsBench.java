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

package org.apache.cassandra.test.microbench.sensors;

import org.apache.cassandra.sensors.Context;
import org.apache.cassandra.sensors.RequestSensors;
import org.apache.cassandra.sensors.Sensor;
import org.apache.cassandra.sensors.SensorsRegistry;
import org.apache.cassandra.sensors.Type;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

@Warmup(iterations = 1)
@Fork(value = 1)
@State(Scope.Benchmark)
public class RequestSensorsBench
{
    private static final RequestSensors requestSensors = new RequestSensors();

    private static final int NUM_SENSORS = 1000;

    @Setup
    public void registerSensors()
    {
        // Register sensors
        for (Type type : Type.values())
        {
            for (int i = 0; i < NUM_SENSORS; i++)
            {
                Context context = new Context("keyspace", "table" + i, "tableId" + i);
                requestSensors.registerSensor(context, type);
            }
        }
    }
    @Benchmark
    @Threads(50)
    public void syncAllSensors()
    {
        // pick a sensor at random
        Type type = Type.values()[(int) (Math.random() * Type.values().length)];
        int sensorIndex = (int) (Math.random() * NUM_SENSORS);
        Context context = new Context("keyspace", "table" + sensorIndex, "tableId" + sensorIndex);
        requestSensors.incrementSensor(context, type, Math.random());
        requestSensors.syncAllSensors();
        SensorsRegistry.instance.getSensor(context, type).ifPresent(Sensor::getValue);
    }
}
