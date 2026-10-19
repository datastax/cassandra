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

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.cassandra.config.DatabaseDescriptor;

/**
 * Implemetation of {@link CounterLockManager} that uses a cache of locks.
 * Note: this implemetation tries to reduce the chance of having two counters lock each other, but as the counters
 * are identified by the hash of the primary key of the row, it is still possible to
 * have some cross-counter contention for counters with different primary keys but the same hash.
 * <p>
 * This code is copied from
 * <a href="https://github.com/diennea/herddb/blob/master/herddb-utils/src/main/java/herddb/utils/LocalLockManager.java">LocalLockManager</a> from the HerdDB project (Apache 2 licensed).
 */
public class CachedCounterLockManager implements CounterLockManager
{
    private final static int EXPECTED_CONCURRENCY = DatabaseDescriptor.getConcurrentCounterWriters() * 16;
    /**
     * The mapping function in {@link #makeLockForKey(Integer)} relies on the ConcurrentHashMap guarantee that the remapping function is run only once per compute, and that it is run atomically
     */
    private final ConcurrentHashMap<Integer, RefCountedLock> locks = new ConcurrentHashMap<>(EXPECTED_CONCURRENCY);

    @Override
    public List<LockHandle> grabLocks(Iterable<Integer> keys)
    {
        // we must return the locks in order to avoid deadlocks
        // please note that the list may contain duplicates
        return StreamSupport.stream(keys.spliterator(), false)
                            .sorted()
                            .map(this::makeLockForKey)
                            .collect(Collectors.toList());
    }

    private ReentrantLock makeLock()
    {
        return new ReentrantLock();
    }

    private LockHandleImpl makeLockForKey(Integer key)
    {
        RefCountedLock instance = locks.compute(key, (k, existing) -> {
            if (existing != null)
            {
                existing.count++;
                return existing;
            }
            else
            {
                return new RefCountedLock(makeLock(), 1);
            }
        });
        return new LockHandleImpl(key, instance);
    }

    private void releaseLockForKey(RefCountedLock instance, Integer key) throws IllegalStateException
    {
        locks.compute(key, (Integer t, RefCountedLock u) -> {
            if (instance != u)
            {
                throw new IllegalStateException("trying to release un-owned lock");
            }
            if (--u.count == 0)
            {
                return null;
            }
            else
            {
                return u;
            }
        });
    }

    @Override
    public boolean hasNumKeys()
    {
        return true;
    }

    @Override
    public int getNumKeys()
    {
        return locks.size();
    }

    /**
     * This class is not thread safe, it is expected to be used by a single thread.
     */
    private class LockHandleImpl implements LockHandle
    {
        private boolean acquired;
        private final Integer key;
        private final RefCountedLock handle;

        private LockHandleImpl(Integer key, RefCountedLock handle)
        {
            this.key = key;
            this.handle = handle;
        }

        @Override
        public void release()
        {
            if (acquired)
                handle.lock.unlock();
            releaseLockForKey(handle, key);
        }

        @Override
        public boolean tryLock(long timeout, TimeUnit timeUnit) throws InterruptedException
        {
            return acquired = handle.lock.tryLock(timeout, timeUnit);
        }

        @Override
        public String toString()
        {
            return "{key=" + key + '}';
        }
    }

    private static class RefCountedLock
    {

        private final ReentrantLock lock;
        private int count;

        private RefCountedLock(ReentrantLock lock, int count)
        {
            this.lock = lock;
            this.count = count;
        }

        @Override
        public String toString()
        {
            return "RefCountedStampedLock{" + "lock=" + lock + ", count=" + count + '}';
        }
    }
}
