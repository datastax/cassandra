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
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.config.CassandraRelevantProperties;

/**
 * Interface for managing locks for CounterMutation.
 * CounterMutation needs to ensure that each local counter is accessed by only one thread at a time.
 * Please note that the id of the counter is an integer hash of the primary key of the row.
 */
public interface CounterLockManager
{
    boolean USE_STRIPED_COUNTER_LOCK_MANAGER = CassandraRelevantProperties.USE_STRIPED_COUNTER_LOCK_MANAGER.getBoolean();
    CounterLockManager instance = USE_STRIPED_COUNTER_LOCK_MANAGER ? new StripedCounterLockManager() : new CachedCounterLockManager();

    /**
     * Handle to a lock for a particular key.
     * Some expectations:
     * - instances of this class are not thread-safe
     * - it is not required that the underlying lock is reentrant
     */
    interface LockHandle
    {
        /**
         * Try to get the lock. This method can be called at most once.
         *
         * @param timeout  timeout.
         * @param timeUnit time unit.
         * @return false in case the lock could not be acquired within the timeout.
         * @throws InterruptedException in case the thread is interrupted while waiting for the lock.
         */
        boolean tryLock(long timeout, TimeUnit timeUnit) throws InterruptedException;

        /**
         * Unlock the lock if it was acquired and release the handle. This method is to be called even if the acquire method failed or even if tryLock has never been called.
         * This method is to be called only once.
         */
        void release();
    }

    /**
     * Grab locks for the given keys. The returned handles must be released by calling {@link LockHandle#release()}.
     * The returned list is re-ordered in order to prevent deadlocks.
     * It is expected that the caller will release the locks in the inverse order they were acquired.
     * The initial set may contain duplicates, it is expected that this method will return a list with the same number of elements.
     *
     * @param keys list of keys, the Iterable is scanned only once in order to prevent side effects.
     * @return a list of lock handles. The List can be iterated multiple times without side effects.
     */
    List<LockHandle> grabLocks(Iterable<Integer> keys);

    /**
     * Check if the implementation can return the number of keys that are handled by the lock manager.
     * This method is useful only for testing.
     *
     * @return true if the implementation can return the number of keys.
     */
    boolean hasNumKeys();

    /**
     * Get the number of keys that are handled by the lock manager.
     * This method is useful only for testing.
     *
     * @return the number of keys.
     */
    default int getNumKeys()
    {
        throw new UnsupportedOperationException();
    }
}
