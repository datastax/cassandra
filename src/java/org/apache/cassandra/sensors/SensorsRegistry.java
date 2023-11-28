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
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Predicate;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaChangeListener;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.Timer;

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
 */
public class SensorsRegistry implements SchemaChangeListener
{
    public static final SensorsRegistry instance = new SensorsRegistry();

    private final Timer asyncUpdater = Timer.INSTANCE;

    private final ReadWriteLock updateLock = new ReentrantReadWriteLock();

    private final Set<String> keyspaces = Sets.newConcurrentHashSet();
    private final Set<String> tableIds = Sets.newConcurrentHashSet();

    private final ConcurrentMap<Pair<Context, Type>, Sensor> identity = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Set<Sensor>> byKeyspace = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Set<Sensor>> byTableId = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Set<Sensor>> byType = new ConcurrentHashMap<>();

    private SensorsRegistry()
    {
        Schema.instance.registerListener(this);
    }

    public Optional<Sensor> getOrCreateSensor(Context context, Type type)
    {
        if (updateLock.readLock().tryLock() && keyspaces.contains(context.getKeyspace()) && tableIds.contains(context.getTableId()))
        {
            try
            {
                // Create a candidate sensor and try inserting in the identity map: this is to make sure concurrent calls will
                // use the same sensor below
                Sensor candidate = new Sensor(context, type);
                Sensor sensor = identity.computeIfAbsent(Pair.create(candidate.getContext(), candidate.getType()),
                                                         (ignored) -> candidate);

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
        return Optional.empty();
    }

    protected void updateSensor(Context context, Type type, double value)
    {
        getOrCreateSensor(context, type).ifPresent(s -> s.increment(value));
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
            removeSensor(ImmutableSet.of(identity.values()), s -> s.getContext().getKeyspace().equals(keyspace.name));
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
            removeSensor(ImmutableSet.of(identity.values()), s -> s.getContext().getTableId().equals(tableId));
            removeSensor(byKeyspace.values(), s -> s.getContext().getTableId().equals(tableId));
            removeSensor(byType.values(), s -> s.getContext().getTableId().equals(tableId));
        }
        finally
        {
            updateLock.writeLock().unlock();
        }
    }

    private void removeSensor(Collection<? extends Collection<Sensor>> candidates, Predicate<Sensor> accept)
    {
        for (Collection<Sensor> sensors : candidates)
        {
            Iterator<Sensor> sensorIt = sensors.iterator();
            while (sensorIt.hasNext())
            {
                Sensor sensor = sensorIt.next();
                if (!accept.test(sensor))
                    break;

                sensorIt.remove();
            }
        }
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
}
