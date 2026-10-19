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

package org.apache.cassandra.db.counters;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.base.Supplier;
import com.google.common.util.concurrent.Striped;

import org.apache.cassandra.config.DatabaseDescriptor;

import static org.apache.cassandra.config.CassandraRelevantProperties.COUNTER_LOCK_FAIR_LOCK;
import static org.apache.cassandra.config.CassandraRelevantProperties.COUNTER_LOCK_NUM_STRIPES_PER_THREAD;
/**
 * Legacy implementation of {@link CounterLockManager} that uses a fixed set of locks.
 * On a workload with many different counters it is likely to see two counters sharing the same lock.
 */
public class StripedCounterLockManager implements CounterLockManager
{
    private final Striped<java.util.concurrent.locks.Lock> locks;

    StripedCounterLockManager()
    {
        int numStripes = COUNTER_LOCK_NUM_STRIPES_PER_THREAD.getInt() * DatabaseDescriptor.getConcurrentCounterWriters();
        if (COUNTER_LOCK_FAIR_LOCK.getBoolean())
        {
            try
            {
                Class<?> stripedClass = Striped.class;

                // Get the custom method Striped.custom
                Method customMethod = stripedClass.getDeclaredMethod("custom", int.class, Supplier.class);
                customMethod.setAccessible(true);

                Supplier<java.util.concurrent.locks.Lock> lockSupplier = () -> new ReentrantLock(true);
                locks = (Striped<java.util.concurrent.locks.Lock>) customMethod.invoke(null, numStripes, lockSupplier);
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }
        else
        {
            locks = Striped.lock(numStripes);
        }
    }

    @Override
    public List<LockHandle> grabLocks(Iterable<Integer> keys)
    {
        List<LockHandle> result = new ArrayList<>();
        Iterable<java.util.concurrent.locks.Lock> locks = this.locks.bulkGet(keys);
        locks.forEach(l -> result.add(new LockImpl(l)));
        return result;
    }

    @Override
    public boolean hasNumKeys()
    {
        return false;
    }

    private static class LockImpl implements LockHandle
    {
        private final java.util.concurrent.locks.Lock lock;
        private boolean acquired;

        public LockImpl(java.util.concurrent.locks.Lock lock)
        {
            this.lock = lock;
        }

        @Override
        public void release()
        {
            if (acquired)
                lock.unlock();
        }

        @Override
        public boolean tryLock(long timeout, TimeUnit timeUnit) throws InterruptedException
        {
            acquired = lock.tryLock(timeout, timeUnit);
            return acquired;
        }
    }
}
