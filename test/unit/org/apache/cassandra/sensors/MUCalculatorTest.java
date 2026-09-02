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

public class MUCalculatorTest
{
    private static final double NANOS_PER_SECOND = 1_000_000_000.0;
    private static final double MU_SCALE = 4000.0;

    private Context context;
    private RequestSensors requestSensors;

    @Before
    public void setUp()
    {
        context = new Context("ks", "tb", "table_id");
        requestSensors = new ActiveRequestSensors();
    }

    // ── RMU tests ────────────────────────────────────────────────────────────

    @Test
    public void testRMU_bytesDominateWhenLatencyIsLow()
    {
        // baseline = 1 000 000 bytes/s, read_bytes = 500 000, latency = 100 ms
        // normalized_bytes = 500_000 / 1_000_000 = 0.5
        // normalized_latency = 0.1_000_000_000 / 1_000_000_000 = 0.1
        // RMU = max(0.1, 0.5) * 4000 = 2000.0
        DefaultMUCalculator calc = new DefaultMUCalculator(1_000_000, 1_000_000);
        requestSensors.registerSensor(context, Type.READ_BYTES);
        requestSensors.incrementSensor(context, Type.READ_BYTES, 500_000);

        long latency = (long) (0.1 * NANOS_PER_SECOND); // 100 ms
        double rmu = calc.computeRMU(requestSensors, context, latency);
        // bytes dominate: max(0.1, 0.5) = 0.5 → RMU = 0.5 * 4000 = 2000.0
        assertThat(rmu).isEqualTo(0.5 * MU_SCALE);
    }

    @Test
    public void testRMU_latencyDominatesWhenBytesAreLow()
    {
        // baseline = 1 000 000 bytes/s, read_bytes = 100 000, latency = 500 ms
        // normalized_bytes = 0.1, normalized_latency = 0.5
        // RMU = max(0.5, 0.1) * 4000 = 2000.0
        DefaultMUCalculator calc = new DefaultMUCalculator(1_000_000, 1_000_000);
        requestSensors.registerSensor(context, Type.READ_BYTES);
        requestSensors.incrementSensor(context, Type.READ_BYTES, 100_000);

        long latency = (long) (0.5 * NANOS_PER_SECOND); // 500 ms
        double rmu = calc.computeRMU(requestSensors, context, latency);
        assertThat(rmu).isEqualTo(0.5 * MU_SCALE);
    }

    @Test
    public void testRMU_exactlyOneMU_whenOneSecondAndBaselineBytes()
    {
        // 1 second latency, read_bytes == baseline_read_bytes → both normalized = 1.0
        // RMU = max(1.0, 1.0) * 4000 = 4000.0
        double baseline = 2_000_000;
        DefaultMUCalculator calc = new DefaultMUCalculator(baseline, baseline);
        requestSensors.registerSensor(context, Type.READ_BYTES);
        requestSensors.incrementSensor(context, Type.READ_BYTES, baseline);

        long latency = (long) NANOS_PER_SECOND; // exactly 1 second
        double rmu = calc.computeRMU(requestSensors, context, latency);
        assertThat(rmu).isEqualTo(MU_SCALE);
    }

    @Test
    public void testRMU_noBaselineConfigured_usesRawBytes()
    {
        // baseline <= 0: latency term dropped, result = read_bytes * MU_SCALE
        DefaultMUCalculator calc = new DefaultMUCalculator(-1, -1);
        requestSensors.registerSensor(context, Type.READ_BYTES);
        requestSensors.incrementSensor(context, Type.READ_BYTES, 250.0);

        double rmu = calc.computeRMU(requestSensors, context, (long) (2.0 * NANOS_PER_SECOND));
        assertThat(rmu).isEqualTo(250.0 * MU_SCALE);
    }

    @Test
    public void testRMU_nullSensors_returnsZero()
    {
        DefaultMUCalculator calc = new DefaultMUCalculator(1_000_000, 1_000_000);
        assertThat(calc.computeRMU(null, context, 1_000_000_000L)).isEqualTo(0.0);
    }

    @Test
    public void testRMU_nullContext_returnsZero()
    {
        DefaultMUCalculator calc = new DefaultMUCalculator(1_000_000, 1_000_000);
        assertThat(calc.computeRMU(requestSensors, null, 1_000_000_000L)).isEqualTo(0.0);
    }

    // ── WMU tests ────────────────────────────────────────────────────────────

    @Test
    public void testWMU_bytesDominateWhenLatencyIsLow()
    {
        // baseline = 1 000 000, write_bytes = 600 000, index_write_bytes = 200 000, latency = 200 ms
        // total_write_bytes = 800 000, normalized_bytes = 0.8, normalized_latency = 0.2
        // WMU = max(0.2, 0.8) * 4000 = 3200.0
        DefaultMUCalculator calc = new DefaultMUCalculator(1_000_000, 1_000_000);
        requestSensors.registerSensor(context, Type.WRITE_BYTES);
        requestSensors.registerSensor(context, Type.INDEX_WRITE_BYTES);
        requestSensors.incrementSensor(context, Type.WRITE_BYTES, 600_000);
        requestSensors.incrementSensor(context, Type.INDEX_WRITE_BYTES, 200_000);

        long latency = (long) (0.2 * NANOS_PER_SECOND); // 200 ms
        double wmu = calc.computeWMU(requestSensors, context, latency);
        assertThat(wmu).isEqualTo(0.8 * MU_SCALE);
    }

    @Test
    public void testWMU_latencyDominatesWhenBytesAreLow()
    {
        // baseline = 1 000 000, write_bytes = 30 000, index_write_bytes = 20 000, latency = 700 ms
        // total_write_bytes = 50 000, normalized_bytes = 0.05, normalized_latency = 0.7
        // WMU = max(0.7, 0.05) * 4000 = 2800.0
        DefaultMUCalculator calc = new DefaultMUCalculator(1_000_000, 1_000_000);
        requestSensors.registerSensor(context, Type.WRITE_BYTES);
        requestSensors.registerSensor(context, Type.INDEX_WRITE_BYTES);
        requestSensors.incrementSensor(context, Type.WRITE_BYTES, 30_000);
        requestSensors.incrementSensor(context, Type.INDEX_WRITE_BYTES, 20_000);

        long latency = (long) (0.7 * NANOS_PER_SECOND); // 700 ms
        double wmu = calc.computeWMU(requestSensors, context, latency);
        assertThat(wmu).isEqualTo(0.7 * MU_SCALE);
    }

    @Test
    public void testWMU_indexWriteBytesAddedToWriteBytes()
    {
        // Verify that index_write_bytes are included in the total byte count.
        // baseline = 1 000 000, write_bytes = 400 000, index_write_bytes = 400 000, latency = 0
        // total = 800 000, normalized_bytes = 0.8
        // WMU = 0.8 * 4000 = 3200.0
        // Without index_write_bytes it would be 0.4 * 4000 = 1600.0
        DefaultMUCalculator calc = new DefaultMUCalculator(1_000_000, 1_000_000);
        requestSensors.registerSensor(context, Type.WRITE_BYTES);
        requestSensors.registerSensor(context, Type.INDEX_WRITE_BYTES);
        requestSensors.incrementSensor(context, Type.WRITE_BYTES, 400_000);
        requestSensors.incrementSensor(context, Type.INDEX_WRITE_BYTES, 400_000);

        double wmu = calc.computeWMU(requestSensors, context, 0L);
        assertThat(wmu).isEqualTo(0.8 * MU_SCALE);
    }

    @Test
    public void testWMU_noBaselineConfigured_usesRawBytes()
    {
        // baseline <= 0: latency term dropped, result = (write_bytes + index_write_bytes) * MU_SCALE
        DefaultMUCalculator calc = new DefaultMUCalculator(-1, -1);
        requestSensors.registerSensor(context, Type.WRITE_BYTES);
        requestSensors.registerSensor(context, Type.INDEX_WRITE_BYTES);
        requestSensors.incrementSensor(context, Type.WRITE_BYTES, 200.0);
        requestSensors.incrementSensor(context, Type.INDEX_WRITE_BYTES, 100.0);

        double wmu = calc.computeWMU(requestSensors, context, (long) (2.0 * NANOS_PER_SECOND));
        assertThat(wmu).isEqualTo(300.0 * MU_SCALE);
    }

    @Test
    public void testWMU_nullSensors_returnsZero()
    {
        DefaultMUCalculator calc = new DefaultMUCalculator(1_000_000, 1_000_000);
        assertThat(calc.computeWMU(null, context, 1_000_000_000L)).isEqualTo(0.0);
    }

    @Test
    public void testWMU_nullContext_returnsZero()
    {
        DefaultMUCalculator calc = new DefaultMUCalculator(1_000_000, 1_000_000);
        assertThat(calc.computeWMU(requestSensors, null, 1_000_000_000L)).isEqualTo(0.0);
    }
}
