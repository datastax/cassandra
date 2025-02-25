/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.db.counters;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;


import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit Tests for {@link CounterLockManager} implementations
 */
public class CounterLockManagerTest
{
    @BeforeClass
    public static void setUpClass()
    {
        DatabaseDescriptor.setConfig(new Config());
    }

    @Test
    public void basicTestCachedCounterLockManager() throws Exception
    {
        basicTest(new CachedCounterLockManager());
    }


    @Test
    public void basicTestStripedCounterLockManager() throws Exception
    {
        basicTest(new StripedCounterLockManager());
    }


    private static void basicTest(CounterLockManager manager) throws Exception
    {
        List<Integer> keys = List.of(1, 2, 3, 4, 5);
        List<CounterLockManager.LockHandle> lockHandleHandles = manager.grabLocks(keys);
        for (CounterLockManager.LockHandle l : lockHandleHandles)
            assertThat(l.tryLock(1, TimeUnit.SECONDS)).isTrue();

        if (manager.hasNumKeys())
            assertThat(manager.getNumKeys()).isEqualTo(keys.size());

        lockHandleHandles.forEach(CounterLockManager.LockHandle::release);

        if (manager.hasNumKeys())
            assertThat(manager.getNumKeys()).isZero();

        // double release is not allowed (expecting IllegalMonitorStateException)
        for (CounterLockManager.LockHandle l : lockHandleHandles)
            assertThatThrownBy(l::release).isInstanceOf(IllegalMonitorStateException.class);

        // the number of keys is still zero
        if (manager.hasNumKeys())
            assertThat(manager.getNumKeys()).isZero();
    }


    @Test
    public void lockTimeoutTestCachedCounterLockManager() throws Exception
    {
        lockTimeout(new CachedCounterLockManager());
    }

    @Test
    public void lockTimeoutTestStripedCounterLockManager() throws Exception
    {
        lockTimeout(new StripedCounterLockManager());
    }

    private static void lockTimeout(CounterLockManager manager) throws Exception
    {
        List<Integer> keys = List.of(1, 2, 3, 4, 5);
        List<CounterLockManager.LockHandle> lockHandleHandles = manager.grabLocks(keys);
        for (CounterLockManager.LockHandle l : lockHandleHandles)
            assertThat(l.tryLock(1, TimeUnit.SECONDS)).isTrue();

        if (manager.hasNumKeys())
            assertThat(manager.getNumKeys()).isEqualTo(keys.size());

        // some implementations use reentrant locks, so we have to try to get the lock from another thread
        int count = CompletableFuture.supplyAsync(() -> {
            try
            {
                int numAcquired = 0;
                List<CounterLockManager.LockHandle> newLockHandleHandles = manager.grabLocks(keys);
                try
                {
                    for (CounterLockManager.LockHandle l : newLockHandleHandles)
                    {
                        if (l.tryLock(1, TimeUnit.SECONDS))
                            numAcquired++;
                    }
                }
                finally
                {
                    newLockHandleHandles.forEach(CounterLockManager.LockHandle::release);
                }
                return numAcquired;
            }
            catch (InterruptedException e)
            {
                throw new RuntimeException(e);
            }
        }).join();
        assertThat(count).isZero();

        lockHandleHandles.forEach(CounterLockManager.LockHandle::release);

        if (manager.hasNumKeys())
            assertThat(manager.getNumKeys()).isZero();

        count = CompletableFuture.supplyAsync(() -> {
            try
            {
                int numAcquired = 0;
                List<CounterLockManager.LockHandle> newLockHandleHandles = manager.grabLocks(keys);
                try
                {
                    for (CounterLockManager.LockHandle l : newLockHandleHandles)
                    {
                        if (l.tryLock(1, TimeUnit.SECONDS))
                            numAcquired++;
                    }
                }
                finally
                {
                    newLockHandleHandles.forEach(CounterLockManager.LockHandle::release);
                }
                return numAcquired;
            }
            catch (InterruptedException e)
            {
                throw new RuntimeException(e);
            }
        }).join();
        assertThat(count).isEqualTo(keys.size());

        if (manager.hasNumKeys())
            assertThat(manager.getNumKeys()).isZero();
    }

    @Test
    public void testInterruptedExceptionCachedCounterLockManager() throws Exception
    {
        testInterruptedException(new CachedCounterLockManager());
    }

    @Test
    public void testInterruptedExceptionStripedCounterLockManager() throws Exception
    {
        testInterruptedException(new StripedCounterLockManager());
    }

    private static void testInterruptedException(CounterLockManager manager) throws Exception
    {
        List<Integer> keys = List.of(1, 2, 3, 4, 5);
        List<CounterLockManager.LockHandle> lockHandleHandles = manager.grabLocks(keys);
        for (CounterLockManager.LockHandle l : lockHandleHandles)
            assertThat(l.tryLock(1, TimeUnit.SECONDS)).isTrue();

        if (manager.hasNumKeys())
            assertThat(manager.getNumKeys()).isEqualTo(keys.size());

        CompletableFuture<Void> result = new CompletableFuture<>();
        Thread otherThread = new Thread(() -> {
            List<CounterLockManager.LockHandle> newLockHandleHandles = manager.grabLocks(keys);
            try
            {
                try
                {
                    for (CounterLockManager.LockHandle l : newLockHandleHandles)
                    {
                        // the first of these locks will be interrupted
                        l.tryLock(1, TimeUnit.HOURS);
                    }
                    result.complete(null);
                }
                catch (InterruptedException error)
                {
                    result.completeExceptionally(error);
                }
            }
            finally
            {
                // in any case all the locks have to be released
                newLockHandleHandles.forEach(CounterLockManager.LockHandle::release);
            }
        });

        otherThread.start();
        otherThread.interrupt();
        assertThatThrownBy(result::join).hasCauseInstanceOf(InterruptedException.class);

        lockHandleHandles.forEach(CounterLockManager.LockHandle::release);

        if (manager.hasNumKeys())
            assertThat(manager.getNumKeys()).isZero();
    }
}
