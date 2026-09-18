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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.utils.NoSpamLogger;

/**
 * Active (non-no-op) implementation of {@link RequestSensors} that groups {@link Sensor}s associated to a given
 * request/response and related {@link Context}. This is the main entry point to create and modify sensors.
 * More specifically:
 * <ul>
 *     <li>Create a new sensor associated to the request/response via {@link #registerSensor(Context, Type)}.</li>
 *     <li>Increment the sensor value for the request/response via {@link #incrementSensor(Context, Type, double)}.</li>
 *     <li>Sync this request/response sensor value to the {@link SensorsRegistry} via {@link #syncAllSensors()}.</li>
 * </ul>
 * Sensor values related to a given request/response are isolated from other sensors, and the "same" sensor
 * (for a given context and type) registered to different requests/responses will have a different value: in other words,
 * there is no automatic synchronization or coordination across sensor values belonging to different
 * {@link RequestSensors} objects, hence {@link #syncAllSensors()} MUST be invoked to propagate the sensors values
 * at a global level to the {@link SensorsRegistry}.
 * <p>
 * Every production instance is scoped to a non-empty set of allowed keyspaces, supplied by
 * {@link ActiveSensorsFactory#createRequestSensors} at creation time (see {@link #getKeyspaces()}).
 * Any call to {@link #registerSensor(Context, Type)} whose {@link Context} refers to a keyspace outside
 * that set is silently ignored and a warning is logged — <em>unless</em> the context is a
 * {@link Context#isRequestContext() request context} or the keyspace is a system keyspace (e.g.
 * {@code system}, {@code system_schema}, {@code system_auth}, …), both of which are always permitted.
 * This exemption exists so that internal Paxos/CAS operations, which write to {@code system.paxos}
 * regardless of the user keyspace, are never accidentally suppressed.
 * <p>
 * Instances of this class should be created via the configured {@link SensorsFactory}.
 * The no-arg, {@code Supplier}-only, and {@code (Set<String>, Supplier<SensorsRegistry>)} constructors are visible for testing only.
 */
public class ActiveRequestSensors implements RequestSensors
{
    private static final Logger logger = LoggerFactory.getLogger(ActiveRequestSensors.class);

    private final Supplier<SensorsRegistry> sensorsRegistry;

    /**
     * The set of keyspaces this instance is allowed to track sensors for, or {@code null} to allow all keyspaces.
     * A sensor whose context belongs to a keyspace not in this set is silently dropped (with a warning) unless the
     * context is a request context or the keyspace is a system keyspace.
     */
    private final Set<String> allowedKeyspaces;

    // Using Map of array values for performance reasons to avoid wrapping key into another Object (.eg. Pair(context,type)).
    // Note that array values can contain NULL so be careful to filter NULLs when iterating over array
    private final HashMap<Context, Sensor[]> sensors = new LinkedHashMap<>();

    private final Map<Sensor, Double> latestSyncedValuePerSensor = new HashMap<>();

    @VisibleForTesting
    public ActiveRequestSensors()
    {
        this(null, () -> SensorsRegistry.instance);
    }

    @VisibleForTesting
    public ActiveRequestSensors(Supplier<SensorsRegistry> sensorsRegistry)
    {
        this(null, sensorsRegistry);
    }

    @VisibleForTesting
    public ActiveRequestSensors(Set<String> allowedKeyspaces, Supplier<SensorsRegistry> sensorsRegistry)
    {
        this.allowedKeyspaces = allowedKeyspaces == null ? null : ImmutableSet.copyOf(allowedKeyspaces);
        this.sensorsRegistry = sensorsRegistry;
    }

    public ActiveRequestSensors(Set<String> allowedKeyspaces)
    {
        this(allowedKeyspaces, () -> SensorsRegistry.instance);
    }

    public synchronized void registerSensor(Context context, Type type)
    {
        if (!isAllowed(context))
        {
            NoSpamLogger.getLogger(logger, 1, TimeUnit.MINUTES)
                        .warn("Ignoring sensor registration for context {} and type {}: keyspace '{}' is not in the allowed keyspaces {}",
                              context, type, context.getKeyspace().orElse(null), allowedKeyspaces);

            return;
        }
        Sensor[] typeSensors = sensors.computeIfAbsent(context, key ->
        {
            Sensor[] newTypeSensors = new Sensor[Type.values().length];
            newTypeSensors[type.ordinal()] = new Sensor(context, type);
            return newTypeSensors;
        });
        if (typeSensors[type.ordinal()] == null)
            typeSensors[type.ordinal()] = new Sensor(context, type);
    }

    public synchronized Optional<Sensor> getSensor(Context context, Type type)
    {
        return Optional.ofNullable(getSensorFast(context, type));
    }

    public synchronized Set<Sensor> getSensors(Predicate<Sensor> filter)
    {
        return sensors.values().stream().flatMap(Arrays::stream).filter(Objects::nonNull).filter(filter).collect(Collectors.toSet());
    }

    public synchronized void incrementSensor(Context context, Type type, double value)
    {
        Sensor sensor = getSensorFast(context, type);
        if (sensor != null)
            sensor.increment(value);
    }

    @Override
    public final Set<String> getKeyspaces()
    {
        return allowedKeyspaces == null ? Collections.emptySet() : Collections.unmodifiableSet(allowedKeyspaces);
    }

    public synchronized void syncAllSensors()
    {
        sensors.values().forEach(types -> {
            for (int i = 0; i < types.length; i++)
            {
                if (types[i] != null)
                {
                    Sensor sensor = types[i];
                    double current = latestSyncedValuePerSensor.getOrDefault(sensor, 0d);
                    double update = sensor.getValue() - current;
                    if (update == 0d)
                        continue;

                    latestSyncedValuePerSensor.put(sensor, sensor.getValue());
                    sensorsRegistry.get().incrementSensor(sensor.getContext(), sensor.getType(), update);
                }
            }
        });
    }

    /**
     * Returns {@code true} if a sensor for the given context may be registered on this instance.
     * <p>
     * Registration is always allowed when:
     * <ul>
     *   <li>no keyspace restriction was set ({@code allowedKeyspaces} null or empty)</li>
     *   <li>the context is a request context (no keyspace identity), or</li>
     *   <li>the context keyspace is a system keyspace (e.g. {@code system}, {@code system_schema},
     *       {@code system_auth}, …), or</li>
     *   <li>the context keyspace is explicitly contained in {@code allowedKeyspaces}.</li>
     * </ul>
     */
    private boolean isAllowed(Context context)
    {
        if (allowedKeyspaces == null || allowedKeyspaces.isEmpty() || context.isRequestContext())
            return true;
        String keyspace = context.getKeyspace().orElse(null);
        return keyspace != null && (allowedKeyspaces.contains(keyspace) || SchemaConstants.isSystemKeyspace(keyspace));
    }

    /**
     * To get best perfromance we are not returning Optional here
     */
    @Nullable
    private Sensor getSensorFast(Context context, Type type)
    {
        Sensor[] typeSensors = sensors.get(context);
        if (typeSensors != null)
            return typeSensors[type.ordinal()];

        return null;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ActiveRequestSensors other = (ActiveRequestSensors) o;
        return Objects.equals(sensors, other.sensors);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(sensors);
    }

    @Override
    public String toString()
    {
        return "ActiveRequestSensors{" +
               "sensors=" + sensors +
               '}';
    }
}
