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

public class UCUCalculatorTest
{
    private Context context;
    private RequestSensors requestSensors;

    @Before
    public void setUp()
    {
        context = new Context("ks", "tb", "table_id");
        requestSensors = new ActiveRequestSensors();
    }

    @Test
    public void testDefaultUCUCalculatorReplicaUCU()
    {
        DefaultUCUCalculator calculator = DefaultUCUCalculator.instance;

        requestSensors.registerSensor(context, Type.READ_BYTES);
        requestSensors.registerSensor(context, Type.WRITE_BYTES);
        requestSensors.registerSensor(context, Type.INDEX_WRITE_BYTES);

        requestSensors.incrementSensor(context, Type.READ_BYTES, 100.0);
        requestSensors.incrementSensor(context, Type.WRITE_BYTES, 200.0);
        requestSensors.incrementSensor(context, Type.INDEX_WRITE_BYTES, 50.0);

        double ucu = calculator.computeReplicaUCU(requestSensors, context);
        assertThat(ucu).isEqualTo(350.0);
    }

    @Test
    public void testCustomWeightsUCUCalculatorReplicaUCU()
    {
        // readWeight=2.0, writeWeight=1.5, indexWriteWeight=3.0
        DefaultUCUCalculator calculator = new DefaultUCUCalculator(2.0, 1.5, 3.0);

        requestSensors.registerSensor(context, Type.READ_BYTES);
        requestSensors.registerSensor(context, Type.WRITE_BYTES);
        requestSensors.registerSensor(context, Type.INDEX_WRITE_BYTES);

        requestSensors.incrementSensor(context, Type.READ_BYTES, 10.0);       // 10 * 2.0 = 20
        requestSensors.incrementSensor(context, Type.WRITE_BYTES, 20.0);      // 20 * 1.5 = 30
        requestSensors.incrementSensor(context, Type.INDEX_WRITE_BYTES, 5.0); // 5 * 3.0 = 15

        double ucu = calculator.computeReplicaUCU(requestSensors, context);
        assertThat(ucu).isEqualTo(65.0);
    }
}
