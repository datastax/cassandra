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

package org.apache.cassandra.sensors.memory;

import java.lang.management.ManagementFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.sensors.ActiveRequestSensors;
import org.apache.cassandra.sensors.Context;
import org.apache.cassandra.sensors.Sensor;
import org.apache.cassandra.sensors.SensorsRegistry;
import org.apache.cassandra.sensors.Type;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.memory.MemoryUtil;

/**
 * A convenience class to manage memory sensors.
 */
public final class MemorySensors
{
    private static final Logger logger = LoggerFactory.getLogger(MemorySensors.class);
    private static final NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(logger, 1, TimeUnit.MINUTES);

    /**
     * If we settle on the Context.all() approach, we might as we add a special rule in CNDB to handle this case so
     * we can selectively enable and do the "sensors idiomatic" way of invoking SensorsFactory.instance.createRequestSensors(Context.all().getKeyspace);
     *
     */
    private static final ActiveRequestSensors activeRequestSensors = new ActiveRequestSensors();
    private static final int SENSORS_REGISTRY_SYNC_INTERVAL_SECONDS = 10;

    private static final int MEMORY_SNAPSHOT_INTERVAL_SECONDS = 30;

    /**
     * Expose all snapshots in gauge like metrics (or the final OOM prediction) for auto-scaler to consume.
     */
    private static final AtomicLong ON_HEAP_ALLOCATED_SNAPSHOT = new AtomicLong();
    private static final AtomicLong OFF_HEAP_ALLOCATED_SNAPSHOT = new AtomicLong();
    private static final AtomicLong UNSAFE_ALLOCATED_SNAPSHOT = new AtomicLong();

    /**
     * Presumably, those don't change without a restart, so we can keep them as final statics.
     * Each one of those need analysis to know what is the most accurate and efficient way to measure. Here I'm just
     * adding the most naive way for each one to iterate on.
     */
    private static final long ON_HEAP_MAX_MEMORY = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax();

    /**
     * return -1 on Mac unit test although -XX:MaxDirectMemorySize is set in the test
     */
    private static final long OFF_HEAP_MAX_MEMORY = ManagementFactory.getMemoryMXBean().getNonHeapMemoryUsage().getMax();
    private static final long UNSAFE_MAX_MEMORY = ((com.sun.management.OperatingSystemMXBean)
                                                     java.lang.management.ManagementFactory.getOperatingSystemMXBean()).getTotalPhysicalMemorySize();

    /**
     * Perhaps add a start/stop methods to control fom CNDB and tests
     */
    static
    {
        activeRequestSensors.registerSensor(Context.all(), Type.ON_HEAP_BYTES);
        activeRequestSensors.registerSensor(Context.all(), Type.OFF_HEAP_BYTES);
        activeRequestSensors.registerSensor(Context.all(), Type.UNSAFE_BYTES);

        // although memory sensors are effectively monotonic (sense they outlive any request), here we sync them
        // to the registry SensorsMetrics in CNDB register to Sensors Register events
        ScheduledExecutors.scheduledTasks.scheduleAtFixedRate(activeRequestSensors::syncAllSensors,
                                                              SENSORS_REGISTRY_SYNC_INTERVAL_SECONDS,
                                                              SENSORS_REGISTRY_SYNC_INTERVAL_SECONDS,
                                                              TimeUnit.SECONDS);

        ScheduledExecutors.scheduledTasks.scheduleAtFixedRate(() -> ON_HEAP_ALLOCATED_SNAPSHOT.set(ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed()),
                                                              MEMORY_SNAPSHOT_INTERVAL_SECONDS,
                                                              MEMORY_SNAPSHOT_INTERVAL_SECONDS,
                                                              TimeUnit.SECONDS);

        ScheduledExecutors.scheduledTasks.scheduleAtFixedRate(() -> OFF_HEAP_ALLOCATED_SNAPSHOT.set(ManagementFactory.getMemoryMXBean().getNonHeapMemoryUsage().getUsed()),
                                                              MEMORY_SNAPSHOT_INTERVAL_SECONDS,
                                                              MEMORY_SNAPSHOT_INTERVAL_SECONDS,
                                                              TimeUnit.SECONDS);

        ScheduledExecutors.scheduledTasks.scheduleAtFixedRate(() -> UNSAFE_ALLOCATED_SNAPSHOT.set(MemoryUtil.allocated()),
                                                              MEMORY_SNAPSHOT_INTERVAL_SECONDS,
                                                              MEMORY_SNAPSHOT_INTERVAL_SECONDS,
                                                              TimeUnit.SECONDS);

        ScheduledExecutors.scheduledTasks.scheduleAtFixedRate(MemorySensors::LoggingOOMPredictor,
                                                              1,
                                                              1,
                                                              TimeUnit.MINUTES);
    }

    private MemorySensors()
    {

    }

    public static void incrementOnHeapBytes(long bytes)
    {
        activeRequestSensors.incrementSensor(Context.all(), Type.ON_HEAP_BYTES, bytes);
    }

    public static void incrementOffHeapBytes(long bytes)
    {
        activeRequestSensors.incrementSensor(Context.all(), Type.OFF_HEAP_BYTES, bytes);
    }

    public static void incrementUnsafeBytes(long bytes)
    {
        activeRequestSensors.incrementSensor(Context.all(), Type.UNSAFE_BYTES, bytes);
    }

    /**
     * Perhaps we need an OOM Predictor class/interface with different implementations and output (some would push
     * to logs (and metrics, or autoscaler can do the math on metrics) other can be polled for traffic shaping purposes)
     */
    private static void LoggingOOMPredictor()
    {
        int W = 60; // look ahead window in seconds, fix its readability/visibility/configurability
        double MPercentage = 0.95d; // percentage of memory allocated, to calculate a threshold for OOM predictions

        long onHeapMThreshold = (long) (ON_HEAP_MAX_MEMORY * MPercentage);
        long offHeapMThreshold = (long) (OFF_HEAP_MAX_MEMORY * MPercentage);
        // for unsafe, for now subtract what's allocated on heap and off heap from the total physical memory
        long unsafeEffectiveMaxMemory = UNSAFE_MAX_MEMORY - ON_HEAP_ALLOCATED_SNAPSHOT.get() - OFF_HEAP_MAX_MEMORY;
        long unsafeHeapMThreshold = (long) (unsafeEffectiveMaxMemory * MPercentage);

        double onHeapAllocationRate = SensorsRegistry.instance.getSensorRate(Context.all(), Type.ON_HEAP_BYTES);
        double offHeapAllocationRate = SensorsRegistry.instance.getSensorRate(Context.all(), Type.OFF_HEAP_BYTES);
        double unsafeAllocationRate = SensorsRegistry.instance.getSensorRate(Context.all(), Type.UNSAFE_BYTES);

        long onHeapUsed = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();
        long offHeapUsed = ManagementFactory.getMemoryMXBean().getNonHeapMemoryUsage().getUsed();
        long unsafeUsed = MemoryUtil.allocated();

        boolean onHeapOOM = onHeapUsed + onHeapAllocationRate * W > onHeapMThreshold;
        boolean offHeapOOM = offHeapUsed + offHeapAllocationRate * W > offHeapMThreshold;
        boolean unsafeOOM = unsafeUsed + unsafeAllocationRate * W > unsafeHeapMThreshold;

        noSpamLogger.info("OOM prediction report with W={}s and M=0.95% of max available memory\n" +
                          "    prediction: onHeapOOM={}, offHeapOOM={}, unsafeOOM={}\n" +
                          "    variables: onHeapMaxMemory={}, offHeapMaxMemory={}, unsafeMaxMemory={}, unsafeEffectiveMaxMemory={}\n" +
                          "               onHeapUsed={}, offHeapUsed={}, unsafeUsed={}\n" +
                          "               onHeapBytesSensorValue={}, offHeapBytesSensorValue={}, unsafeBytesSensorValue={}\n" +
                          "               onHeapAllocationRate={}, offHeapAllocationRate={}, unsafeAllocationRate={}\n",
                          W,
                          onHeapOOM, offHeapOOM, unsafeOOM,
                          FBUtilities.prettyPrintMemory(ON_HEAP_MAX_MEMORY), FBUtilities.prettyPrintMemory(OFF_HEAP_MAX_MEMORY), FBUtilities.prettyPrintMemory(UNSAFE_MAX_MEMORY), FBUtilities.prettyPrintMemory(unsafeEffectiveMaxMemory),
                          FBUtilities.prettyPrintMemory(onHeapUsed), FBUtilities.prettyPrintMemory(offHeapUsed), FBUtilities.prettyPrintMemory(unsafeUsed),
                          FBUtilities.prettyPrintMemory(activeRequestSensors.getSensor(Context.all(), Type.ON_HEAP_BYTES).map(Sensor::getValue).orElse(-1d).longValue()),
                          FBUtilities.prettyPrintMemory(activeRequestSensors.getSensor(Context.all(), Type.OFF_HEAP_BYTES).map(Sensor::getValue).orElse(-1d).longValue()),
                          FBUtilities.prettyPrintMemory((activeRequestSensors.getSensor(Context.all(), Type.UNSAFE_BYTES).map(Sensor::getValue).orElse(-1d)).longValue()),
                          FBUtilities.prettyPrintMemoryPerSecond((long) onHeapAllocationRate), FBUtilities.prettyPrintMemoryPerSecond((long) offHeapAllocationRate), FBUtilities.prettyPrintMemoryPerSecond((long) unsafeAllocationRate));
    }
}
