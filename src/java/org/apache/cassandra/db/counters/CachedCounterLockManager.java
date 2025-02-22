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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.StampedLock;

import com.google.common.collect.Iterables;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.CounterMutation;

public class CachedCounterLockManager implements CounterLockManager
{
    private static final Logger logger = LoggerFactory.getLogger(CachedCounterLockManager.class);
    private final static int EXPECTED_CONCURRENCY = DatabaseDescriptor.getConcurrentCounterWriters() * 16;

    @Override
    public Iterable<Lock> grabLocks(Iterable<Integer> keys)
    {
        return Iterables.transform(keys, k -> {
            return makeLockForKey(k);
        });
    }

    private StampedLock makeLock()
    {
        return new StampedLock();
    }


    private final ConcurrentMap<Integer, RefCountedStampedLock> locks = new ConcurrentHashMap<>(EXPECTED_CONCURRENCY);

    private LockHandle makeLockForKey(Integer key)
    {
        RefCountedStampedLock instance = locks.compute(key, (k, existing) -> {
            if (existing != null)
            {
                existing.count++;
                return existing;
            }
            else
            {
                return new RefCountedStampedLock(makeLock(), 1);
            }
        });
        return new LockHandle(key, instance);
    }

    private void returnLockForKey(RefCountedStampedLock instance, Integer key) throws IllegalStateException
    {
        locks.compute(key, (Integer t, RefCountedStampedLock u) -> {
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

    public void clear()
    {
        this.locks.clear();
    }

    public int getNumKeys()
    {
        return locks.size();
    }

    private class LockHandle implements Lock
    {

        public volatile long stamp;
        public final Integer key;
        public final RefCountedStampedLock handle;

        public LockHandle(Integer key, RefCountedStampedLock handle)
        {
            this.key = key;
            this.handle = handle;
        }

        @Override
        public void release()
        {
            if (stamp != 0)
                handle.lock.unlockWrite(stamp);
            returnLockForKey(handle, key);
        }

        @Override
        public boolean tryLock(long timeout, TimeUnit timeUnit) throws InterruptedException
        {
            stamp = handle.lock.tryWriteLock(timeout, timeUnit);
            return stamp != 0;
        }
    }

    private static class RefCountedStampedLock
    {

        private final StampedLock lock;
        private int count;

        public RefCountedStampedLock(StampedLock lock, int count)
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
