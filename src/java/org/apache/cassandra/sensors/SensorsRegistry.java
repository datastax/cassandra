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
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Striped;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaChangeListener;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.concurrent.Timer;

/**
 * This class tracks {@link Sensor}s at a "global" level, allowing to:
 * <ul>
 *     <li>Getting or creating (if not existing) sensors of a given {@link Context} and {@link Type}.</li>
 *     <li>Accessing sensors by keyspace, table id or type.</li>
 * </ul>
 * The returned sensors are global, meaning that their value spans across requests/responses, but cannot be modified either
 * directly or indirectly via this class (whose update methods are package protected). In order to modify a sensor value,
 * it must be registered to a request/response via {@link RequestSensors#registerSensor(Context, Type)} and incremented via
 * {@link RequestSensors#incrementSensor(Context, Type, double)}, then synced via {@link RequestSensors#syncAllSensors()}, which
 * will update the related global sensors.
 * <br/><br/>
 * Given sensors are tied to a context, that is to a given keyspace and table, their global instance will be deleted
 * if the related keyspace/table is dropped.
 * <br/><br/>
 * It's also possible to:
 * <ul>
 *     <li>
 *         Register listeners via the {@link #registerListener(SensorsRegistryListener)} method.
 *         Such listeners will get notified on creation and removal of sensors.
 *     </li>
 *     <li>
 *         Unregister listeners via the {@link #unregisterListener(SensorsRegistryListener)} method.
 *         Such listeners will not be notified anymore about creation or removal of sensors.
 *     </li>
 * </ul>
 */
public class SensorsRegistry implements SchemaChangeListener
{
    private static final int LOCK_SRIPES = 1024;

    public static final SensorsRegistry instance = new SensorsRegistry();
    private static final Logger logger = LoggerFactory.getLogger(SensorsRegistry.class);

    private final Timer asyncUpdater = Timer.INSTANCE;

    private final Striped<ReadWriteLock> stripedUpdateLock = Striped.readWriteLock(LOCK_SRIPES); // we stripe per keyspace

    private final Set<String> keyspaces = Sets.newConcurrentHashSet();
    private final Set<String> tableIds = Sets.newConcurrentHashSet();

    // Using Map of array values for performance reasons to avoid wrapping key into another Object, e.g. Pair(context,type)).
    // Note that array values can contain NULL so be careful to filter NULLs when iterating over array
    private final ConcurrentMap<Context, Sensor[]> identity = new ConcurrentHashMap<>();

    private final ConcurrentMap<String, Set<Sensor>> byKeyspace = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Set<Sensor>> byTableId = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Set<Sensor>> byType = new ConcurrentHashMap<>();

    private final CopyOnWriteArrayList<SensorsRegistryListener> listeners = new CopyOnWriteArrayList<>();

    private SensorsRegistry()
    {
        Schema.instance.registerListener(this);
    }

    public void registerListener(SensorsRegistryListener listener)
    {
        listeners.add(listener);
        logger.debug("Listener {} registered", listener);
    }

    public void unregisterListener(SensorsRegistryListener listener)
    {
        listeners.remove(listener);
        logger.debug("Listener {} unregistered", listener);
    }

    public Optional<Sensor> getSensor(Context context, Type type)
    {
        return Optional.ofNullable(getSensorFast(context, type));
    }

    public Optional<Sensor> getOrCreateSensor(Context context, Type type)
    {
        return Optional.ofNullable(getOrCreateSensorFast(context, type));
    }

    protected void incrementSensor(Context context, Type type, double value)
    {
        Sensor sensor = getOrCreateSensorFast(context, type);
        if (sensor != null)
            sensor.increment(value);
    }

    protected Future<Void> incrementSensorAsync(Context context, Type type, double value, long delay, TimeUnit unit)
    {
        return asyncUpdater.onTimeout(() ->
                                      getOrCreateSensor(context, type).ifPresent(s -> s.increment(value)),
                                      delay, unit);
    }

    public Set<Sensor> getSensorsByKeyspace(String keyspace)
    {
        return Optional.ofNullable(byKeyspace.get(keyspace)).orElseGet(() -> ImmutableSet.of());
    }

    public Set<Sensor> getSensorsByTableId(String tableId)
    {
        return Optional.ofNullable(byTableId.get(tableId)).orElseGet(() -> ImmutableSet.of());
    }

    public Set<Sensor> getSensorsByType(Type type)
    {
        return Optional.ofNullable(byType.get(type.name())).orElseGet(() -> ImmutableSet.of());
    }

    public void removeSensorsByKeyspace(String keyspaceName)
    {
        stripedUpdateLock.getAt(getLockStripe(keyspaceName.hashCode())).writeLock().lock();
        try
        {
            byKeyspace.remove(keyspaceName);

            Set<Sensor> removed = removeSensorArrays(ImmutableSet.of(identity.values()), s -> s.getContext().getKeyspace().equals(keyspaceName));
            removed.forEach(this::notifyOnSensorRemoved);

            removeSensor(byTableId.values(), s -> s.getContext().getKeyspace().equals(keyspaceName));
            removeSensor(byType.values(), s -> s.getContext().getKeyspace().equals(keyspaceName));
        }
        finally
        {
            stripedUpdateLock.getAt(getLockStripe(keyspaceName.hashCode())).writeLock().unlock();
        }
    }

    public void removeSensorsByTableId(String keyspaceName, String tableId)
    {
        stripedUpdateLock.getAt(getLockStripe(keyspaceName.hashCode())).writeLock().lock();
        try
        {
            Set<Sensor> removed = removeSensorArrays(ImmutableSet.of(identity.values()), s -> s.getContext().getTableId().equals(tableId));
            removed.forEach(this::notifyOnSensorRemoved);

            byTableId.remove(tableId);
            removeSensor(byType.values(), s -> s.getContext().getTableId().equals(tableId));
        }
        finally
        {
            stripedUpdateLock.getAt(getLockStripe(keyspaceName.hashCode())).writeLock().unlock();
        }
    }

    @Override
    public void onCreateKeyspace(KeyspaceMetadata keyspace)
    {
        keyspaces.add(keyspace.name);
    }

    @Override
    public void onCreateTable(TableMetadata table)
    {
        tableIds.add(table.id.toString());
    }

    @Override
    public void onDropKeyspace(KeyspaceMetadata keyspace, boolean dropData)
    {
        stripedUpdateLock.getAt(getLockStripe(keyspace.name.hashCode())).writeLock().lock();
        try
        {
            keyspaces.remove(keyspace.name);
            byKeyspace.remove(keyspace.name);

            Set<Sensor> removed = removeSensorArrays(ImmutableSet.of(identity.values()), s -> s.getContext().getKeyspace().equals(keyspace.name));
            removed.forEach(this::notifyOnSensorRemoved);

            removeSensor(byTableId.values(), s -> s.getContext().getKeyspace().equals(keyspace.name));
            removeSensor(byType.values(), s -> s.getContext().getKeyspace().equals(keyspace.name));
        }
        finally
        {
            stripedUpdateLock.getAt(getLockStripe(keyspace.name.hashCode())).writeLock().unlock();
        }
    }

    @Override
    public void onDropTable(TableMetadata table, boolean dropData)
    {
        stripedUpdateLock.getAt(getLockStripe(table.keyspace.hashCode())).writeLock().lock();
        try
        {
            String tableId = table.id.toString();
            tableIds.remove(tableId);
            byTableId.remove(tableId);

            Set<Sensor> removed = removeSensorArrays(ImmutableSet.of(identity.values()), s -> s.getContext().getTableId().equals(tableId));
            removed.forEach(this::notifyOnSensorRemoved);

            removeSensor(byKeyspace.values(), s -> s.getContext().getTableId().equals(tableId));
            removeSensor(byType.values(), s -> s.getContext().getTableId().equals(tableId));
        }
        finally
        {
            stripedUpdateLock.getAt(getLockStripe(table.keyspace.hashCode())).writeLock().unlock();
        }
    }

    private static int getLockStripe(int hashCode)
    {
        return Math.abs(hashCode) % LOCK_SRIPES;
    }

    /**
     * Remove sensors from a collection of candidates based on the given predicate
     *
     * @param candidates the candidates to remove from
     * @param accept the predicate used to select the sensors to remove
     * @return the set of removed sensors
     */
    private Set<Sensor> removeSensor(Collection<? extends Collection<Sensor>> candidates, Predicate<Sensor> accept)
    {
        Set<Sensor> removed = new HashSet<>();

        for (Collection<Sensor> sensors : candidates)
        {
            Iterator<Sensor> sensorIt = sensors.iterator();
            while (sensorIt.hasNext())
            {
                Sensor sensor = sensorIt.next();
                if (!accept.test(sensor))
                    continue;

                sensorIt.remove();
                removed.add(sensor);
            }
        }

        return removed;
    }

    /**
     * To get best perfromance we are not returning Optional here
     */
    @Nullable
    private Sensor getSensorFast(Context context, Type type)
    {
        Sensor[] typeSensors = identity.get(context);
        return  typeSensors != null ? typeSensors[type.ordinal()] : null;
    }

    /**
     * To get best perfromance we are not returning Optional here
     */
    @Nullable
    private Sensor getOrCreateSensorFast(Context context, Type type)
    {
        Sensor sensor = getSensorFast(context, type);
        if (sensor != null)
            return sensor;

        stripedUpdateLock.getAt(getLockStripe(context.getKeyspace().hashCode())).readLock().lock();
        try
        {
            if (!keyspaces.contains(context.getKeyspace()) || !tableIds.contains(context.getTableId()))
                return null;

            Sensor[] typeSensors = identity.compute(context, (key, types) -> {
                Sensor[] computed = types != null ? types : new Sensor[Type.values().length];
                if (computed[type.ordinal()] == null)
                {
                    computed[type.ordinal()] = new Sensor(context, type);
                    notifyOnSensorCreated(computed[type.ordinal()]);
                }
                return computed;
            });
            sensor = typeSensors[type.ordinal()];

            Set<Sensor> keyspaceSet = byKeyspace.get(sensor.getContext().getKeyspace());
            keyspaceSet = keyspaceSet != null ? keyspaceSet : byKeyspace.computeIfAbsent(sensor.getContext().getKeyspace(), (ignored) -> Sets.newConcurrentHashSet());
            keyspaceSet.add(sensor);

            Set<Sensor> tableSet = byTableId.get(sensor.getContext().getTableId());
            tableSet = tableSet != null ? tableSet : byTableId.computeIfAbsent(sensor.getContext().getTableId(), (ignored) -> Sets.newConcurrentHashSet());
            tableSet.add(sensor);

            Set<Sensor> opSet = byType.get(sensor.getType().name());
            opSet = opSet != null ? opSet : byType.computeIfAbsent(sensor.getType().name(), (ignored) -> Sets.newConcurrentHashSet());
            opSet.add(sensor);

            return sensor;
        }
        finally
        {
            stripedUpdateLock.getAt(getLockStripe(context.getKeyspace().hashCode())).readLock().unlock();
        }
    }

    /**
     * Removes array of sensors if any sensor in the array matches the predicate.
     * This function is used by `identity` map that holds an array of Sensors (each item in the array maps to Type)
     */
    private Set<Sensor> removeSensorArrays(Collection<? extends Collection<Sensor[]>> candidates, Predicate<Sensor> accept)
    {
        Set<Sensor> removed = new HashSet<>();

        for (Collection<Sensor[]> sensors : candidates)
        {
            Iterator<Sensor[]> sensorIt = sensors.iterator();
            while (sensorIt.hasNext())
            {
                List<Sensor> typeSensors = Arrays.stream(sensorIt.next()).filter(Objects::nonNull).collect(Collectors.toList());
                if (typeSensors.size() > 0 && accept.test(typeSensors.get(0)))
                {
                    removed.addAll(typeSensors);
                    sensorIt.remove();
                }
            }
        }

        return removed;
    }

    @VisibleForTesting
    public void clear()
    {
        keyspaces.clear();
        tableIds.clear();
        identity.clear();
        byKeyspace.clear();
        byTableId.clear();
        byType.clear();
    }

    private void notifyOnSensorCreated(Sensor sensor)
    {
        tryNotifyListeners(sensor, SensorsRegistryListener::onSensorCreated, "created");
    }

    private void notifyOnSensorRemoved(Sensor sensor)
    {
        tryNotifyListeners(sensor, SensorsRegistryListener::onSensorRemoved, "removed");
    }

    private void tryNotifyListeners(Sensor sensor, BiConsumer<SensorsRegistryListener, Sensor> notification, String action)
    {
        for (SensorsRegistryListener l: listeners)
        {
            try
            {
                notification.accept(l, sensor);
                logger.trace("Listener {} correctly notified on sensor {} being {}", l, sensor, action);
            }
            catch (Throwable t)
            {
                logger.error("Failed to notify listener {} on sensor {} being {}", l, sensor, action);
            }
        }
    }
}
