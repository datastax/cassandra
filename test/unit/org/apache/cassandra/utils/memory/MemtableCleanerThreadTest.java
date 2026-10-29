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

package org.apache.cassandra.utils.memory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

public class MemtableCleanerThreadTest
{
    private static final long TIMEOUT_SECONDS = 5;
    private static final long TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS);

    @Mock
    private MemtablePool pool;

//    @Mock
    private MemtableCleaner cleaner;
    private MemtableCleanerThread<MemtablePool> cleanerThread;

    @Before
    public void setup()
    {
        MockitoAnnotations.initMocks(this);
    }

    private void startThread(MemtableCleaner cleaner)
    {
        cleanerThread = new MemtableCleanerThread<>(pool, cleaner);
    }

    private void stopThread() throws InterruptedException
    {
        cleanerThread.shutdownNow();

        assertTrue(cleanerThread.awaitTermination(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS));
    }

    private void waitForPendingTasks()
    {
        // wait for a bit because the cleaner latch completes before the pending tasks are decremented
        FBUtilities.sleepQuietly(TIMEOUT_MILLIS);

        assertEquals(0, cleanerThread.numPendingTasks());
    }

    /**
     * A future completed with a null result and no error (e.g. a cleaner that returns no value) must not
     * cause a NullPointerException when auto-unboxing the result, and must not trigger a spurious retry.
     */
    @Test
    public void testCleanApplyNoOpWhenNullResult() throws Exception
    {
        when(pool.needsCleaning()).thenReturn(false);
        AsyncPromise<Boolean> fut = new AsyncPromise<>();
        cleaner = () -> fut;

        MemtableCleanerThread.Clean<MemtablePool> clean = new MemtableCleanerThread.Clean<>(pool, cleaner);

        // null result with no error: should return null without NPE and without signalling the wait queue
        Boolean res = clean.apply(null, null);

        assertNull(res);
    }

    @Test
    public void testCleanerInvoked() throws Exception
    {
        CountDownLatch cleanerExecutedLatch = new CountDownLatch(1);
        AsyncPromise<Boolean > fut = new AsyncPromise<>();
        AtomicBoolean needsCleaning = new AtomicBoolean(false);

        when(pool.needsCleaning()).thenAnswer(invocation -> needsCleaning.get());
        cleaner = () -> {
            needsCleaning.set(false);
            cleanerExecutedLatch.countDown();
            return fut;
        };

        // start the thread with needsCleaning returning false, the cleaner should not be invoked
        needsCleaning.set(false);
        startThread(cleaner);
        try
        {
            assertFalse(cleanerExecutedLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            assertEquals(1, cleanerExecutedLatch.getCount());
            assertEquals(0, cleanerThread.numPendingTasks());

            // now invoke the cleaner
            needsCleaning.set(true);
            cleanerThread.trigger();
            assertTrue(cleanerExecutedLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            assertEquals(0, cleanerExecutedLatch.getCount());
            assertEquals(1, cleanerThread.numPendingTasks());

            // now complete the cleaning task
            needsCleaning.set(false);
            fut.setSuccess(true);
            waitForPendingTasks();
        }
        finally
        {
            stopThread();
        }
    }

    /**
     * When a cleaning task fails the future completes with a null result and an error. If the pool still needs
     * cleaning, the cleaner thread must be woken up to try again, without anybody calling {@code trigger()}.
     */
    @Test
    public void testCleanerRetriedAfterErrorWhenPoolStillNeedsCleaning() throws Exception
    {
        // each invocation of the cleaner returns its own future and counts down its own latch
        AsyncPromise<Boolean> firstCleaning = new AsyncPromise<>();
        AsyncPromise<Boolean> secondCleaning = new AsyncPromise<>();
        CountDownLatch firstInvocation = new CountDownLatch(1);
        CountDownLatch secondInvocation = new CountDownLatch(1);
        // counted down once the cleaner thread has called needsCleaning() twice after the first invocation,
        // meaning it has entered the wait-path and passed the double-check — i.e. it is now parked
        CountDownLatch threadParkedLatch = new CountDownLatch(1);
        AtomicInteger numTimeCleanerInvoked = new AtomicInteger(0);
        AtomicBoolean needsCleaning = new AtomicBoolean(true);
        AtomicInteger needsCleaningCallCount = new AtomicInteger(0); // private to the mock answer
        // set to the target call count from inside the cleaner lambda (on the cleaner thread), so the
        // snapshot is taken immediately after firstInvocation with no race against the test thread
        AtomicInteger parkThreshold = new AtomicInteger(-1);

        when(pool.needsCleaning()).thenAnswer(invocation -> {
            int count = needsCleaningCallCount.incrementAndGet();
            int threshold = parkThreshold.get();
            if (threshold >= 0 && count >= threshold)
                threadParkedLatch.countDown();
            return needsCleaning.get();
        });

        cleaner = () -> {
            needsCleaning.set(false); // as a real cleaner would, the first call starts a flush that frees memory
            if (numTimeCleanerInvoked.incrementAndGet() == 1)
            {
                // set the threshold here, on the cleaner thread, so needsCleaningCallCount is current
                parkThreshold.set(needsCleaningCallCount.get() + 2);
                firstInvocation.countDown();
                return firstCleaning;
            }
            secondInvocation.countDown();
            return secondCleaning;
        };

        // the pool needs cleaning, so the cleaner is invoked as soon as the thread starts
        startThread(cleaner);
        try
        {
            assertTrue(firstInvocation.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));

            // wait until the cleaner thread has called needsCleaning() twice after the first invocation:
            // once to enter the wait-path and once for the double-check inside it; at that point the
            // thread is guaranteed to be parked
            assertTrue("Timed out waiting for cleaner thread to park",
                       threadParkedLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            assertEquals(1, numTimeCleanerInvoked.get());

            // the first cleaning fails, while the pool needs cleaning again: the cleaner thread must be woken up
            needsCleaning.set(true);
            firstCleaning.setFailure(new RuntimeException("Simulated cleaning failure to verify retry"));
            assertTrue(secondInvocation.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            assertEquals(2, numTimeCleanerInvoked.get());

            secondCleaning.setSuccess(false);
        }
        finally
        {
            stopThread();
        }
    }

    @Test
    public void testCleanerError() throws Exception
    {
        AtomicReference<CountDownLatch> cleanerLatch = new AtomicReference<>(new CountDownLatch(1));
        AtomicReference<AsyncPromise<Boolean>> fut = new AtomicReference<>(new AsyncPromise<>());
        AtomicBoolean needsCleaning = new AtomicBoolean(false);
        AtomicInteger numTimeCleanerInvoked = new AtomicInteger(0);

        when(pool.needsCleaning()).thenAnswer(invocation -> needsCleaning.get());

        cleaner = () -> {
            needsCleaning.set(false);
            numTimeCleanerInvoked.incrementAndGet();
            cleanerLatch.get().countDown();
            return fut.get();
        };

        // start the thread with needsCleaning returning true, the cleaner should be invoked
        needsCleaning.set(true);
        startThread(cleaner);
        try
        {
            assertTrue(cleanerLatch.get().await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            assertEquals(0, cleanerLatch.get().getCount());
            assertEquals(1, cleanerThread.numPendingTasks());
            assertEquals(1, numTimeCleanerInvoked.get());

            // complete the cleaning task with an error, no other cleaning task should be invoked
            cleanerLatch.set(new CountDownLatch(1));
            AsyncPromise<Boolean> oldFut = fut.get();
            fut.set(new AsyncPromise<>());
            needsCleaning.set(false);
            oldFut.setFailure(new RuntimeException("Test"));
            assertFalse(cleanerLatch.get().await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            assertEquals(1, cleanerLatch.get().getCount());
            assertEquals(1, numTimeCleanerInvoked.get());

            // now trigger cleaning again and verify that a new task is invoked
            cleanerLatch.set(new CountDownLatch(1));
            fut.set(new AsyncPromise<>());
            needsCleaning.set(true);
            cleanerThread.trigger();
            assertTrue(cleanerLatch.get().await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            assertEquals(0, cleanerLatch.get().getCount());
            assertEquals(2, numTimeCleanerInvoked.get());

            //  complete the cleaning task with false (nothing should be scheduled)
            cleanerLatch.set(new CountDownLatch(1));
            oldFut = fut.get();
            fut.set(new AsyncPromise<>());
            needsCleaning.set(false);
            oldFut.setSuccess(false);
            assertFalse(cleanerLatch.get().await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            assertEquals(1, cleanerLatch.get().getCount());
            assertEquals(2, numTimeCleanerInvoked.get());

            // now trigger cleaning again and verify that a new task is invoked
            cleanerLatch.set(new CountDownLatch(1));
            fut.set(new AsyncPromise<>());
            needsCleaning.set(true);
            cleanerThread.trigger();
            assertTrue(cleanerLatch.get().await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            assertEquals(0, cleanerLatch.get().getCount());
            assertEquals(3, numTimeCleanerInvoked.get());
        }
        finally
        {
            stopThread();
        }
    }
}