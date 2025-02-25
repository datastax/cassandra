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

package org.apache.cassandra.io.memtable;

import com.google.common.util.concurrent.AtomicDouble;

import org.apache.cassandra.concurrent.ScheduledExecutorPlus;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DataStorageSpec;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.notifications.INotification;
import org.apache.cassandra.notifications.SSTableAddingNotification;

import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.db.memtable.AbstractAllocatorMemtable.MEMORY_POOL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * this is a long-ish test that shows that writes do not block anymore
 * after flush is failing in the notification subscriber
 * <p>
 * without the fix the test fails typically within a couple of seconds
 * by default the test duration is set to 120s
 */
public class FlushFailingOnNotificationSubscriberTest extends CQLTester
{
    private static final int APPROXIMATE_TEST_DURATION_SECONDS = 120;
    private static final double FLUSH_FAILURE_PROBABILITY = 0.25;
    private final AtomicInteger numUserFlushes = new AtomicInteger();
    private final AtomicInteger numFailedFlushes = new AtomicInteger();

    volatile long maxTimeSinceCleanup = 0;
    volatile long lastTimePoolNeededCleaning = System.nanoTime();

    static AtomicDouble failFlushProbability = new AtomicDouble(FLUSH_FAILURE_PROBABILITY);

    @BeforeClass
    public static void setup()
    {
        Config conf = DatabaseDescriptor.getRawConfig();
        // frequent flushes
        conf.memtable_allocation_type = Config.MemtableAllocationType.offheap_objects;
        conf.memtable_cleanup_threshold = 0.15f;
        conf.memtable_heap_space = new DataStorageSpec.IntMebibytesBound(2);
        conf.memtable_offheap_space = new DataStorageSpec.IntMebibytesBound(2);

        CQLTester.setUpClass();
    }

    @Test
    public void flushFailingOnSSTableAddingNotificationVSWritesTest() throws InterruptedException, ExecutionException, TimeoutException
    {
        try
        {
            ScheduledExecutorPlus scheduledExecutor = executorFactory().scheduled("forced flush");

            createTable(KEYSPACE, "CREATE TABLE %s (pk int PRIMARY KEY, value int)", "failedflushtest");

            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            cfs.getTracker().subscribeLateConsumer(this::maybeThrowWhenFlushAddsSSTables);

            int flushPeriodSec = 15;

            scheduledExecutor.scheduleAtFixedRate(this::successfulUserFlush, flushPeriodSec, flushPeriodSec, SECONDS);
            scheduledExecutor.scheduleAtFixedRate(() -> {
                updateMaxTimeSinceCleanup();
                logState();
            }, 1, 1, SECONDS);

            int idx = 1;
            while (numUserFlushes.get() < APPROXIMATE_TEST_DURATION_SECONDS / flushPeriodSec)
            {
                final int fidx = idx;
                Future<UntypedResultSet> query = scheduledExecutor.submit(() -> execute("INSERT INTO %s (pk, value) VALUES (?, ?)", fidx, fidx));
                try
                {
                    query.get(1, TimeUnit.SECONDS);
                }
                catch (TimeoutException e)
                {
                    logger.info("write timed out at iteration {}", idx);
                    logState();
                    throw new RuntimeException("Test failed because a write got stuck", e);
                }

                idx++;
                if (MEMORY_POOL.needsCleaning())
                {
                    lastTimePoolNeededCleaning = System.nanoTime();
                }
                else
                {
                    updateMaxTimeSinceCleanup();
                }

                if (MEMORY_POOL.getNumPendingtasks() > 2)
                {
                    logger.info("Flushes seem to be backing up, sleeping at iteration {} to slow down writes", idx);
                    logState();
                    Thread.sleep(100);
                }

                assertEquals("write blocked on allocation", 0, MEMORY_POOL.blockedOnAllocating.getCount());
            }
            scheduledExecutor.shutdown();
            assertTrue(scheduledExecutor.awaitTermination(1, TimeUnit.MINUTES));
            successfulUserFlush();

            // check the amount of memory used for the memtables is less than half of the limit
            // it is expected that if this resource leaks then these assertions won't hold
            assertTrue(MEMORY_POOL.onHeap.used() < 1_000_000);
            assertTrue(MEMORY_POOL.offHeap.used() < 1_000_000);
            // assert no memtables are stuck in a reclaiming state
            assertEquals(0, MEMORY_POOL.onHeap.getReclaiming());
            assertEquals(0, MEMORY_POOL.offHeap.getReclaiming());
            // check that memtable cleanup is scheduled sufficiently often
            assertTrue("memory pool did not clean for more than 10s: " + maxTimeSinceCleanup + " ms", maxTimeSinceCleanup < 10_000);
            // and that there were no writes that actually got blocked due to memory pressure
            assertEquals("write blocked on allocation", 0, MEMORY_POOL.blockedOnAllocating.getCount());
        }
        finally
        {
            logger.info("The ultimate system state:");
            logState();
            // If the test managed to reproduce the problem then writing may be blocked now.
            // This means that @After in CQLTester will hang.
            // To prevent this let's unblock writes by running a successful flush, which will
            // free the current memtable.
            successfulUserFlush();
        }
    }

    private void updateMaxTimeSinceCleanup()
    {
        maxTimeSinceCleanup = Math.max(maxTimeSinceCleanup, (System.nanoTime() - lastTimePoolNeededCleaning) / TimeUnit.MILLISECONDS.toNanos(1));
    }

    private void logState()
    {
        logger.info(" --- STATE ---\n" +
                    "Max time since pool needed cleaning: {} ms\n" +
                    "Num failed flushes: {}\n" +
                    "Memory pool: onHeap.used={} ({} %), onHeap.getReclaiming={} ({} %)\n" +
                    "Memory pool: offHeap.used={} ({} %), offHeap.getReclaiming={} ({} %)\n" +
                    "Total size of sstables: {} bytes\n" +
                    "Num blocked allocations: {}",
                    maxTimeSinceCleanup,
                    numFailedFlushes,
                    MEMORY_POOL.onHeap.used(), 100 * MEMORY_POOL.onHeap.usedRatio(), MEMORY_POOL.onHeap.getReclaiming(), 100 * MEMORY_POOL.onHeap.reclaimingRatio(),
                    MEMORY_POOL.offHeap.used(), 100 * MEMORY_POOL.offHeap.usedRatio(), MEMORY_POOL.offHeap.getReclaiming(), 100 * MEMORY_POOL.offHeap.reclaimingRatio(),
                    getCurrentColumnFamilyStore().getLiveSSTables().stream().mapToLong(SSTableReader::bytesOnDisk).sum(),
                    MEMORY_POOL.blockedOnAllocating.getCount());
    }

    private void maybeThrowWhenFlushAddsSSTables(INotification notification, Object sender)
    {
        logger.info("Consuming notification {}", notification);
        if (notification instanceof SSTableAddingNotification)
        {
            SSTableAddingNotification addingNotification = (SSTableAddingNotification) notification;
            if (addingNotification.operationType == OperationType.FLUSH && addingNotification.memtable().get().metadata().name.equals("failedflushtest")
                && failFlushProbability.get() > Math.random())
            {
                logger.info("Throwing exception for notification {}", notification);
                numFailedFlushes.incrementAndGet();
                throw new RuntimeException("hey I just broke your flush, haven't I?");
            }
        }
    }

    private void successfulUserFlush()
    {
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.getAllMemtables().forEach(m -> logger.info("pre flush memtable: {}", m));
        logState();
        try
        {
            failFlushProbability.set(0.0);
            getCurrentColumnFamilyStore().forceFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS).get();
            failFlushProbability.set(FLUSH_FAILURE_PROBABILITY);
        }
        catch (InterruptedException | ExecutionException e)
        {
            throw new RuntimeException(e);
        }
        finally
        {
            cfs.getAllMemtables().forEach(m -> logger.info("post flush memtable: {}", m));
            logState();
            numUserFlushes.incrementAndGet();
        }
    }
}
