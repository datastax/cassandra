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

import java.util.concurrent.TimeUnit;

public interface CounterLockManager
{
    boolean USE_STRIPED_COUNTER_LOCK_MANAGER = Boolean.getBoolean("cassandra.use_striped_counter_lock_manager");
    CounterLockManager instance = USE_STRIPED_COUNTER_LOCK_MANAGER ? new StripedCounterLockManager() : new CachedCounterLockManager();

    interface Lock
    {
        /**
         * Try to get the lock.
         *
         * @param timeout  timeout.
         * @param timeUnit time unit.
         * @return false in case the lock could not be acquired within the timeout.
         * @throws InterruptedException
         */
        boolean tryLock(long timeout, TimeUnit timeUnit) throws InterruptedException;

        /**
         * Unlock the lock. This method is to be called even if the acquire method failed.
         * This method is to be called only once.
         */
        void release();
    }

    Iterable<Lock> grabLocks(Iterable<Integer> keys);
}
