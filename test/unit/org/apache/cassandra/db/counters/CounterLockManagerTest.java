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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.BeforeClass;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit Tests for {@link CounterLockManager} implementations
 */
public class CounterLockManagerTest
{
    private static final Logger logger = LoggerFactory.getLogger(CounterLockManagerTest.class);

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
        // please note that keys are not sorted and there is one duplicate
        List<Integer> keys = List.of(1, 2, 3, 4, 5, 4);
        List<CounterLockManager.LockHandle> lockHandleHandles = manager.grabLocks(keys);
        for (CounterLockManager.LockHandle l : lockHandleHandles)
            assertThat(l.tryLock(1, TimeUnit.SECONDS)).isTrue();

        if (manager.hasNumKeys())
            assertThat(manager.getNumKeys()).isEqualTo(keys.size() - 1); // one key is duplicated

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


    @Test
    public void stressTestCachedCounterLockManager() throws Exception
    {
        stressTest(new CachedCounterLockManager());
    }

    @Test
    public void stressTestStripedCounterLockManager() throws Exception
    {
        stressTest(new StripedCounterLockManager());
    }

    private static void stressTest(CounterLockManager manager) throws Exception
    {
        Random randon = new Random(12313);
        int numThreads = 40;
        int numKeys = 20;
        int numIterations = 1000;
        AtomicReference<Throwable> oneError = new AtomicReference<>();
        CountDownLatch allDone = new CountDownLatch(numIterations);
        ExecutorService threadPool = Executors.newFixedThreadPool(numThreads);
        AtomicInteger threadId = new AtomicInteger();
        try
        {
            for (int i = 0; i < numIterations; i++)
            {
                threadPool.submit(() -> {
                    String id = "t" + threadId.incrementAndGet();
                    int numKeysToLock = randon.nextInt(numKeys);

                    List<Integer> keys = new ArrayList<>(numKeysToLock);
                    try
                    {
                        for (int k = 0; k < numKeysToLock; k++)
                            keys.add(randon.nextInt(numKeys));

                        //logger.info("Thread {} grabbing locks for  {} keys, {}", id, numKeysToLock, keys);

                        List<CounterLockManager.LockHandle> lockHandles = manager.grabLocks(keys);
                        //logger.info("Thread {} got locks {}", id, lockHandles);
                        try
                        {
                            for (CounterLockManager.LockHandle l : lockHandles)
                                assertThat(l.tryLock(1, TimeUnit.MINUTES)).isTrue();

                            //logger.info("Thread {} locked {}", id, lockHandles);

                            // simlate some work
                            Thread.sleep(randon.nextInt(10));
                        }
                        finally
                        {
                            // release in inverse order, like Cassandra does
                            // iterate over all locks in reverse order and unlock them
                            for (int j = lockHandles.size() - 1; j >= 0; j--)
                                lockHandles.get(j).release();

                            //logger.info("Thread {} released {} lockHandles, {}", id, numKeysToLock, lockHandles);
                        }
                    }
                    catch (Throwable error)
                    {
                        logger.error("Iteration {} failed to acquire {}", id, keys, error);
                        oneError.set(error);
                    }
                    finally
                    {
                        allDone.countDown();
                    }
                });
            }
            assertThat(allDone.await(10, TimeUnit.MINUTES)).isTrue();
            assertThat(oneError.get()).isNull();

            // the number of keys is zero in the end
            if (manager.hasNumKeys())
                assertThat(manager.getNumKeys()).isZero();
        }
        finally
        {
            threadPool.shutdown();
        }
    }
}
