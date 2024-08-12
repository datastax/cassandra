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
import org.apache.cassandra.sensors.ActiveRequestSensors;
import org.apache.cassandra.sensors.Context;
import org.apache.cassandra.sensors.RequestSensors;
import org.apache.cassandra.sensors.SensorsRegistry;
import org.apache.cassandra.sensors.Type;
import org.apache.cassandra.utils.Pair;
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
    private static final int THREADS = 100;
    private static final int SENSORS_PER_THREAD = 10;
    private static final int UPDATES_PER_THREAD = 100;
    private static final ConcurrentMap<Integer, Pair<Context, Type>> contextFixtures = new ConcurrentHashMap();
    private static final Fixture[][] fixtures = new Fixture[THREADS][SENSORS_PER_THREAD];
    private static final RequestSensors[] requestSensorsPool = new RequestSensors[THREADS];
    private static final Random randomGen = new Random(1234567890);
    private static final AtomicInteger threadIdx = new AtomicInteger();

    // Zipfian should more realisticly represent workload (few tenants generating most of the load)
    private static final ZipfDistribution zipfDistributionContext = new ZipfDistribution(NUM_SENSORS - 1, 1);

    private static class Fixture
    {
        Context context;
        Type type;

        Fixture(Context context, Type type)
        {
            this.context = context;
            this.type = type;
        }
    }

    @Setup
    public void generateFixtures()
    {
        for (int i = 0; i < NUM_SENSORS; i++)
        {
            Context context = new Context("keyspace" + i, "table" + i, UUID.randomUUID().toString());
            SensorsRegistry.instance.onCreateKeyspace(KeyspaceMetadata.create(context.getKeyspace(), null));
            SensorsRegistry.instance.onCreateTable(TableMetadata.builder(context.getKeyspace(), context.getTable()).id(TableId.fromString(context.getTableId())).build());
            contextFixtures.put(i, Pair.create(context, Type.values()[randomGen.nextInt(Type.values().length)]));
        }

        IntStream.range(0, THREADS).forEach(t -> {
            requestSensorsPool[t] = new ActiveRequestSensors();
            Pair<Context, Type> contextTypePair = contextFixtures.get(zipfDistributionContext.sample());
            IntStream.range(0, SENSORS_PER_THREAD).forEach(s -> fixtures[t][s] = new Fixture(contextTypePair.left, contextTypePair.right));
        });
    }

    @State(Scope.Thread)
    public static class BenchState
    {
        int idx = threadIdx.getAndIncrement();
        RequestSensors requestSensors = requestSensorsPool[idx];
    }

    @Benchmark
    @Threads(THREADS)
    public void syncAllSensors(BenchState benchState)
    {
        RequestSensors requestSensors = benchState.requestSensors;
        for(int i = 0; i < SENSORS_PER_THREAD; i++)
        {
            Fixture f = fixtures[benchState.idx][i];
            requestSensors.registerSensor(f.context, f.type);
        }
        for (int i = 0; i < UPDATES_PER_THREAD; i++)
        {
            Fixture f = fixtures[benchState.idx][i % SENSORS_PER_THREAD];
            requestSensors.incrementSensor(f.context, f.type, 1);
        }
        requestSensors.syncAllSensors();
    }

    @Benchmark
    @Threads(THREADS)
    public void benchUsingSensorRegistryDirectly(BenchState benchState)
    {
        for (int i = 0; i < SENSORS_PER_THREAD; i++)
        {
            Fixture f = fixtures[benchState.idx][i];
            SensorsRegistry.instance.getOrCreateSensor(f.context, f.type);
        }
        for (int i = 0; i < UPDATES_PER_THREAD; i++)
        {
            Fixture f = fixtures[benchState.idx][i % SENSORS_PER_THREAD];
            SensorsRegistry.instance.getSensor(f.context, f.type).get().increment(1);
        }
    }

    public static void main(String... args) throws Exception
    {
        Options options = new OptionsBuilder().include(RequestSensorBench.class.getSimpleName()).build();
        new Runner(options).run();
    }
}
