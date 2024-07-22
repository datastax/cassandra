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

package org.apache.cassandra.test.microbench;


import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import org.apache.commons.math3.distribution.ZipfDistribution;

import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
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
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@Warmup(iterations = 1)
@Fork(value = 1)
@State(Scope.Benchmark)
public class RequestSensorBench
{
    private static final int NUM_SENSORS = 1000;
    private static final int THREADS = 50;
    private static final int NUM_FIXTURES = 20000;
    private static final ConcurrentMap<Integer, Context> contextFixtures = new ConcurrentHashMap();
    private static final Fixture[] fixtures = new Fixture[NUM_FIXTURES];
    private static final RequestSensors[] requestSensorsPool = new RequestSensors[THREADS];
    private static final Random randomGen = new Random(1234567890);
    private static final AtomicInteger threadIdx = new AtomicInteger();

    // Zipfian should more realisticly represent workload (few tenants generating most of the load)
    private static final ZipfDistribution zipfDistributionContext = new ZipfDistribution(NUM_SENSORS - 1, 1);

    private static class Fixture
    {
        Context context;
        Type type;
        double delta;

        Fixture(Context context, Type type, double delta)
        {
            this.context = context;
            this.delta = delta;
            this.type = type;
        }
    }


    @Setup
    public void generateFixtures()
    {
        SensorsRegistry.USE_STRIPED_LOCK = true;
        for (int i = 0; i < NUM_SENSORS; i++)
        {
            Context context = new Context("keyspace" + i, "table" + i, UUID.randomUUID().toString());
            SensorsRegistry.instance.onCreateKeyspace(KeyspaceMetadata.create(context.getKeyspace(), null));
            SensorsRegistry.instance.onCreateTable(TableMetadata.builder(context.getKeyspace(), context.getTable()).id(TableId.fromString(context.getTableId())).build());
            contextFixtures.put(i, context);
        }
        IntStream.range(0, THREADS).forEach( n -> requestSensorsPool[n]=new RequestSensors());
        IntStream.range(0, NUM_FIXTURES).forEach(n -> fixtures[n] = new Fixture((contextFixtures.get(zipfDistributionContext.sample())), Type.values()[randomGen.nextInt(Type.values().length)], Math.random()));
    }

    @State(Scope.Thread)
    public static class BenchState
    {
        RequestSensors requestSensors = requestSensorsPool[threadIdx.getAndIncrement()];
    }

    // A lots of CPU cycles spent in `syncAllSensors` with `latestSyncedValuePerSensor` map
    @Benchmark
    @Threads(THREADS)
    public void syncAllSensors(BenchState benchState)
    {
        RequestSensors requestSensors = benchState.requestSensors; // each thread has it's own RequestSensors
        for(int i = 0; i < NUM_FIXTURES; i++)
        {
            Fixture f = fixtures[i];
            requestSensors.registerSensor(f.context, f.type); // invoking every time simulates worst-case
            requestSensors.incrementSensor(f.context, f.type, f.delta);
            if (i % 5 == 0)
                // arbitrary picked to sync after every 5 increments
                requestSensors.syncAllSensors();
        }
    }

    // Due to using `syncAllSensorsNew` throughput increased between 8X-10X (depends on how often sync is called)
    @Benchmark
    @Threads(THREADS)
    public void syncAllSensorsNew(BenchState benchState)
    {
        RequestSensors requestSensors = benchState.requestSensors; // each thread has its own RequestSensors
        for(int i = 0; i < NUM_FIXTURES; i++)
        {
            Fixture f = fixtures[i];
            requestSensors.registerSensor(f.context, f.type);
            requestSensors.incrementSensor(f.context, f.type, f.delta);
            if (i % 5 == 0)
                // arbitrary picked to sync after every 5 increments
                requestSensors.syncAllSensorsNew();
        }
    }

    // Note: replacing existing lock with striped lock (1000 stripes) increases throughput by 3X
    // To benchmark striped version set SensorRegistry.USE_STRIPED_LOCK in benchmark setup function
    @Benchmark
    @Threads(THREADS)
    public void benchUsingSensorRegistryDirectly()
    {
        for(int i = 0; i < NUM_FIXTURES; i++)
        {
            Fixture f = fixtures[i];
            Sensor sensor = SensorsRegistry.instance.getOrCreateSensor(f.context, f.type).orElseThrow();
            sensor.increment(f.delta);
        }
    }

    public static void main(String... args) throws Exception {
        Options options = new OptionsBuilder().include(RequestSensorBench.class.getSimpleName()).build();
        new Runner(options).run();
    }
}
