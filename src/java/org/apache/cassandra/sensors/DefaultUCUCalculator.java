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

/**
 * Default implementation of {@link UCUCalculator}.
 * Replica-level UCU is computed as a weighted sum of READ_BYTES, WRITE_BYTES, and INDEX_WRITE_BYTES.
 */
public class DefaultUCUCalculator implements UCUCalculator
{
    public static final DefaultUCUCalculator instance = new DefaultUCUCalculator();

    private final double readWeight;
    private final double writeWeight;
    private final double indexWriteWeight;

    public DefaultUCUCalculator()
    {
        this(1.0, 1.0, 1.0);
    }

    public DefaultUCUCalculator(double readWeight, double writeWeight, double indexWriteWeight)
    {
        this.readWeight = readWeight;
        this.writeWeight = writeWeight;
        this.indexWriteWeight = indexWriteWeight;
    }

    @Override
    public double computeReplicaUCU(RequestSensors sensors, Context context)
    {
        if (sensors == null || context == null)
            return 0.0;

        double readBytes = sensors.getSensor(context, Type.READ_BYTES).map(Sensor::getValue).orElse(0.0);
        double writeBytes = sensors.getSensor(context, Type.WRITE_BYTES).map(Sensor::getValue).orElse(0.0);
        double indexWriteBytes = sensors.getSensor(context, Type.INDEX_WRITE_BYTES).map(Sensor::getValue).orElse(0.0);

        return (readBytes * readWeight) + (writeBytes * writeWeight) + (indexWriteBytes * indexWriteWeight);
    }
}
