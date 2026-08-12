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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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

    // ---- setIf ----

    @Test
    public void testSetIf_updatesWhenPredicateIsTrue()
    {
        sensor.setIf(3.0, (current, candidate) -> candidate > current);
        assertThat(sensor.getValue()).isEqualTo(3.0);
    }

    @Test
    public void testSetIf_noOpWhenPredicateIsFalse()
    {
        sensor.increment(5.0);
        sensor.setIf(2.0, (current, candidate) -> candidate > current);
        assertThat(sensor.getValue()).isEqualTo(5.0);
    }

    @Test
    public void testSetIf_noOpWhenCandidateEqualsCurrentAndPredicateRejectsEqual()
    {
        sensor.increment(3.0);
        sensor.setIf(3.0, (current, candidate) -> candidate > current);
        assertThat(sensor.getValue()).isEqualTo(3.0);
    }

    @Test
    public void testSetIf_multipleUpdatesKeepMaximum()
    {
        sensor.setIf(2.0, (current, candidate) -> candidate > current);
        sensor.setIf(4.0, (current, candidate) -> candidate > current);
        sensor.setIf(1.0, (current, candidate) -> candidate > current);
        sensor.setIf(3.0, (current, candidate) -> candidate > current);
        assertThat(sensor.getValue()).isEqualTo(4.0);
    }

    @Test
    public void testSetIf_concurrentMaxUpdatesConvergeToHighestValue() throws InterruptedException
    {
        int threadCount = 8;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);

        double[] candidates = { 1.0, 2.0, 3.0, 4.0, 5.0, 3.0, 2.0, 1.0 };

        for (int i = 0; i < threadCount; i++)
        {
            double candidate = candidates[i];
            executor.submit(() -> {
                try
                {
                    startLatch.await();
                    sensor.setIf(candidate, (current, c) -> c > current);
                }
                catch (InterruptedException e)
                {
                    Thread.currentThread().interrupt();
                }
            });
        }

        startLatch.countDown();
        executor.shutdown();
        assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();

        assertThat(sensor.getValue()).isEqualTo(5.0);
    }
}
