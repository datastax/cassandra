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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.MetricRegistry;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Benchmark to assess the performance impact of relaxing the MetricName validation
 * that prevents scope from containing name (CassandraMetricsRegistry.java line 782).
 *
 * The current check exists to prevent "performance issues" but this benchmark
 * quantifies what those issues actually are. The operations measured are:
 *
 * 1. MetricName string construction via MetricRegistry.name()
 * 2. HashMap lookup with the constructed metric name key
 * 3. The scope.contains(name) check itself
 *
 * We compare "normal" metric names (scope does not contain name) vs.
 * "colliding" metric names (scope contains name, as would happen with
 * a table named "WA" and the "WA" metric).
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 4, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 8, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(value = 2)
@Threads(4)
@State(Scope.Benchmark)
public class MetricNameBench
{
    private static final String GROUP = "org.apache.cassandra.metrics";
    private static final String TYPE = "CompactionCosts";

    // Metric names used by Controller.Metrics
    private static final String[] METRIC_NAMES = { "WA", "ReadIOCost", "WriteIOCost", "TotIOCost" };

    @Param({ "normal", "colliding" })
    String scenario;

    // scope that does NOT contain any metric name
    private String normalScope;
    // scope that DOES contain a metric name ("WA")
    private String collidingScope;

    private String activeScope;

    // Pre-built metric name strings for lookup benchmarks
    private String[] metricKeys;

    // Simulates the ConcurrentHashMap inside MetricRegistry
    private ConcurrentHashMap<String, Object> registry;

    @Setup(Level.Trial)
    public void setup()
    {
        normalScope = "system_auth.roles";
        collidingScope = "myks.WAjGzCwq9GDaCHW1tzif";

        activeScope = scenario.equals("colliding") ? collidingScope : normalScope;

        // Build metric name strings and populate the registry
        metricKeys = new String[METRIC_NAMES.length];
        registry = new ConcurrentHashMap<>();

        for (int i = 0; i < METRIC_NAMES.length; i++)
        {
            metricKeys[i] = MetricRegistry.name(GROUP, TYPE, METRIC_NAMES[i], activeScope);
            registry.put(metricKeys[i], new Object());
        }

        // Add background entries to simulate a realistic registry size
        for (int i = 0; i < 5000; i++)
        {
            String key = MetricRegistry.name(GROUP, TYPE, "metric" + i, "ks.tbl" + i);
            registry.put(key, new Object());
        }
    }

    /**
     * Measures the cost of constructing a metric name string via MetricRegistry.name().
     * This is called every time a metric is registered, looked up, or removed.
     */
    @Benchmark
    public void metricNameConstruction(Blackhole bh)
    {
        for (String metricName : METRIC_NAMES)
        {
            bh.consume(MetricRegistry.name(GROUP, TYPE, metricName, activeScope));
        }
    }

    /**
     * Measures ConcurrentHashMap.get() with the constructed metric name as key.
     * This represents the core lookup path in MetricRegistry.
     */
    @Benchmark
    public void registryLookup(Blackhole bh)
    {
        for (String key : metricKeys)
        {
            bh.consume(registry.get(key));
        }
    }

    /**
     * Measures the full cycle: construct name then look up in registry.
     * This is the realistic hot path when accessing a metric.
     */
    @Benchmark
    public void constructAndLookup(Blackhole bh)
    {
        for (String metricName : METRIC_NAMES)
        {
            String key = MetricRegistry.name(GROUP, TYPE, metricName, activeScope);
            bh.consume(registry.get(key));
        }
    }

    /**
     * Measures the cost of the scope.contains(name) validation check itself.
     * This is the check we might remove/relax.
     */
    @Benchmark
    public void scopeContainsCheck(Blackhole bh)
    {
        for (String metricName : METRIC_NAMES)
        {
            bh.consume(activeScope.contains(metricName));
        }
    }

    /**
     * Measures MBean name construction (StringBuilder-based).
     * This mirrors DefaultNameFactory.createDefaultMBeanName().
     */
    @Benchmark
    public void mbeanNameConstruction(Blackhole bh)
    {
        for (String metricName : METRIC_NAMES)
        {
            StringBuilder sb = new StringBuilder();
            sb.append(GROUP);
            sb.append(":type=");
            sb.append(TYPE);
            sb.append(",scope=");
            sb.append(activeScope);
            sb.append(",name=");
            sb.append(metricName);
            bh.consume(sb.toString());
        }
    }
}
