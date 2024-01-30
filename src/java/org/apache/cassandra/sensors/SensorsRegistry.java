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

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaChangeListener;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.MonotonicClock;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.Timer;

import static org.apache.cassandra.utils.MonotonicClock.approxTime;

/**
 * This class tracks {@link Sensor}s at a "global" level, allowing to:
 * <ul>
 *     <li>Getting or creating (if not existing) sensors of a given {@link Context} and {@link Type}.</li>
 *     <li>Accessing sensors by keyspace, table id or type.</li>
 * </ul>
 * The returned sensors are global, meaning that their value spans across requests/responses, but cannot be modified either
 * directly or indirectly via this class (whose update methods are package protected). In order to modify a sensor value,
 * it must be registered to a request/response via {@link RequestSensors#registerSensor(Type)} and incremented via
 * {@link RequestSensors#incrementSensor(Type, double)}, then synced via {@link RequestSensors#syncAllSensors()}, which
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
    public static final SensorsRegistry instance = new SensorsRegistry();

    /**
     * Used to calculate the rate of a given sensor, as sensor values are monotonically increasing at each {@link RequestSensors#syncAllSensors()} call,
     * This value is utilized by {@link SensorsRegistry#updateSensor(Context, Type, double)} to simplify the logic required to
     * calculate sensor rate when calling {@link SensorsRegistry#getSensorRate(Sensor)}:
     * <ul>
     *    <li>
     *        Spares the need for a background job to snapshot sensor value at each SENSOR_RATE_WINDOW_IN_SECONDS interval
     *    </li>
     *    <li>
     *        Spares the need of storing sensor value history over time.
     *    </li>
     * </ul>
     *  Defaults to 60 seconds.
     */
    public static final String SENSORS_RATE_WINDOW_IN_SECONDS_SYSTEM_PROPERTY = "cassandra.sensors.rate_window_in_seconds";

    private static final Logger logger = LoggerFactory.getLogger(SensorsRegistry.class);

    private final Timer asyncUpdater = Timer.INSTANCE;
    private final ReadWriteLock updateLock = new ReentrantReadWriteLock();

    private final Set<String> keyspaces = Sets.newConcurrentHashSet();
    private final Set<String> tableIds = Sets.newConcurrentHashSet();

    private final ConcurrentMap<Pair<Context, Type>, Sensor> identity = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Set<Sensor>> byKeyspace = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Set<Sensor>> byTableId = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Set<Sensor>> byType = new ConcurrentHashMap<>();
    private final CopyOnWriteArrayList<SensorsRegistryListener> listeners = new CopyOnWriteArrayList<>();
    private final ConcurrentMap<String, SensorsRegisterAggregator> aggregatorPyType = new ConcurrentHashMap<>();

    private final long sensorRateWindowInNanos =  TimeUnit.SECONDS.toNanos(Integer.getInteger(SENSORS_RATE_WINDOW_IN_SECONDS_SYSTEM_PROPERTY, 60));

    private MonotonicClock clock = approxTime;

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
        return Optional.ofNullable(identity.get(Pair.create(context, type)));
    }

    public Optional<Sensor> getOrCreateSensor(Context context, Type type)
    {
        updateLock.readLock().lock();

        try
        {
            if (!keyspaces.contains(context.getKeyspace()) || !tableIds.contains(context.getTableId()))
                return Optional.empty();

            // Create a candidate sensor and try inserting in the identity map: this is to make sure concurrent calls will
            // use the same sensor below
            Sensor sensor = identity.computeIfAbsent(Pair.create(context, type), (ignored) -> {
                Sensor created = new Sensor(context, type);
                notifyOnSensorCreated(created);
                return created;
            });

            Set<Sensor> keyspaceSet = byKeyspace.computeIfAbsent(sensor.getContext().getKeyspace(), (ignored) -> Sets.newConcurrentHashSet());
            keyspaceSet.add(sensor);
            Set<Sensor> tableSet = byTableId.computeIfAbsent(sensor.getContext().getTableId(), (ignored) -> Sets.newConcurrentHashSet());
            tableSet.add(sensor);
            Set<Sensor> opSet = byType.computeIfAbsent(sensor.getType().name(), (ignored) -> Sets.newConcurrentHashSet());
            opSet.add(sensor);

            return Optional.of(sensor);
        }
        finally
        {
            updateLock.readLock().unlock();
        }
    }

    /**
     * Register a new sensor aggregator function for a given type. Overwrites any previously registered aggregator for the same type.
     */
    public void registerSensorAggregator(SensorsRegisterAggregator aggregator, Type type)
    {
        aggregatorPyType.put(type.name(), aggregator);
        logger.debug("Aggregator {} for type {} registered", aggregator, type);
    }

    protected void updateSensor(Context context, Type type, double value)
    {
        long now = this.clock.now();
        getOrCreateSensor(context, type).ifPresent(s -> s.increment(value, sensor -> now - s.getLastSnapshotTime() > this.sensorRateWindowInNanos, now));
    }

    protected Future<Void> updateSensorAsync(Context context, Type type, double value, long delay, TimeUnit unit)
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

    /**
     * Aggregate all sensors of a given type using a registered aggregator function.
     * If no aggregator is registered for the given type, it defaults to a sum of all sensor values.
     */
    public double aggregateSensorsByType(Type type)
    {
        SensorsRegisterAggregator aggregator = aggregatorPyType.getOrDefault(type.name(), SensorsRegisterAggregator.DEFAULT);
        return byType.get(type.name())
                     .stream()
                     .filter(aggregator.aggregatorFilter())
                     .map(aggregator.aggregatorFn())
                     .reduce(0.0, Double::sum);
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
        updateLock.writeLock().lock();
        try
        {
            keyspaces.remove(keyspace.name);
            byKeyspace.remove(keyspace.name);

            Set<Sensor> removed = removeSensor(ImmutableSet.of(identity.values()), s -> s.getContext().getKeyspace().equals(keyspace.name));
            removed.forEach(this::notifyOnSensorRemoved);

            removeSensor(byTableId.values(), s -> s.getContext().getKeyspace().equals(keyspace.name));
            removeSensor(byType.values(), s -> s.getContext().getKeyspace().equals(keyspace.name));
        }
        finally
        {
            updateLock.writeLock().unlock();
        }
    }

    @Override
    public void onDropTable(TableMetadata table, boolean dropData)
    {
        updateLock.writeLock().lock();
        try
        {
            String tableId = table.id.toString();
            tableIds.remove(tableId);
            byTableId.remove(tableId);

            Set<Sensor> removed = removeSensor(ImmutableSet.of(identity.values()), s -> s.getContext().getTableId().equals(tableId));
            removed.forEach(this::notifyOnSensorRemoved);

            removeSensor(byKeyspace.values(), s -> s.getContext().getTableId().equals(tableId));
            removeSensor(byType.values(), s -> s.getContext().getTableId().equals(tableId));
        }
        finally
        {
            updateLock.writeLock().unlock();
        }
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
     * @return the delta of the sensor value since the last snapshot. Please note that if the sensor is idle,
     * the rate will over report until a full SENSOR_AGGREGATION_WINDOW_IN_NANOS has passed since the last snapshot.
     */
    @VisibleForTesting
    public double getSensorRate(Sensor sensor)
    {
        return this.clock.now() - sensor.getLastSnapshotTime() > this.sensorRateWindowInNanos
               ? 0 // Handles the case where the sensor rate is read but no recent snapshots has been taken due to lack of sensor updates
               : sensor.getValue() - sensor.getLastSnapshotValue();
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

    @VisibleForTesting
    public void setClock(MonotonicClock clock)
    {
        this.clock = clock;
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
