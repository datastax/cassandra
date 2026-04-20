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

package org.apache.cassandra.db.compaction;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.google.common.base.Predicate;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.MockSchema;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Throwables;

import static org.apache.cassandra.db.compaction.TableOperation.StopTrigger.UNIT_TESTS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


/// Randomized test for compaction cancellation that verifies:
/// - Tasks can be cancelled both when queued and when active
/// - Cancelled tasks always run their cleanup
/// - No leaks occur in ActiveOperations queues
/// - The right tasks are cancelled based on predicates
public class RandomizedCancelCompactionsTest extends CQLTester
{
    @BeforeClass
    public static void setUpClass()
    {
        CassandraRelevantProperties.CESSATION_WAIT_SECONDS.setInt(1);
        CQLTester.setUpClass();
    }

    private static final Logger logger = LoggerFactory.getLogger(RandomizedCancelCompactionsTest.class);
    
    private static final int TEST_DURATION_SECONDS = 40;
    private static final int MAX_CONCURRENT_COMPACTIONS = 40;
    private static final int EXECUTOR_THREADS = 4; // Limited threads to keep tasks in queue
    private static final int SSTABLE_COUNT = 500;
    private static final int MAX_COMPACTION_SLEEP_MS = 2000;
    private static final int MIN_COMPACTION_SLEEP_MS = 100;
    private static final double QUICK_EXIT_CHANCE = 0.05;
    
    @Test
    public void testRandomizedCancellation() throws Exception
    {
        ColumnFamilyStore cfs = MockSchema.newCFS();
        List<SSTableReader> sstables = createSSTables(cfs, SSTABLE_COUNT, 0);
        
        Random random = new Random(System.currentTimeMillis());
        ExecutorService executor = Executors.newFixedThreadPool(EXECUTOR_THREADS);
        
        // Track all tasks and their states
        CopyOnWriteArrayList<RandomizedCompactionTask> allTasks = new CopyOnWriteArrayList<>();
        AtomicInteger taskIdCounter = new AtomicInteger(0);
        AtomicBoolean testRunning = new AtomicBoolean(true);
        
        // Statistics
        AtomicInteger tasksCreated = new AtomicInteger(0);
        AtomicInteger tasksCreationFailed = new AtomicInteger(0);
        AtomicInteger tasksCompleted = new AtomicInteger(0);
        AtomicInteger tasksCancelled = new AtomicInteger(0);
        AtomicInteger tasksCancelledBeforeStart = new AtomicInteger(0);
        AtomicInteger tasksCancelledAfterStart = new AtomicInteger(0);
        AtomicInteger cancellationAttempts = new AtomicInteger(0);
        AtomicInteger cancellationSuccess = new AtomicInteger(0);

        long startTime = System.currentTimeMillis();
        long endTime = startTime + TimeUnit.SECONDS.toMillis(TEST_DURATION_SECONDS);
        
        try
        {
            // Thread 1: Randomly create compaction tasks
            Future<?> taskCreator = executor.submit(() -> {
                while (testRunning.get() && System.currentTimeMillis() < endTime)
                {
                    try
                    {
                        // Limit concurrent tasks - count active tasks without stream
                        int activeTasks = 0;
                        for (RandomizedCompactionTask t : allTasks)
                        {
                            if (!t.isFinished())
                                activeTasks++;
                        }
                        
                        if (activeTasks < MAX_CONCURRENT_COMPACTIONS && cfs.isCompactionActive())
                        {
                            // Select random subset of sstables (1-3 sstables per task)
                            int count = random.nextInt(3) + 1;
                            Set<SSTableReader> taskSSTables = new HashSet<>();
                            for (int i = 0; i < count; i++)
                            {
                                taskSSTables.add(sstables.get(random.nextInt(sstables.size())));
                            }
                            
                            int taskId = taskIdCounter.incrementAndGet();
                            RandomizedCompactionTask task = new RandomizedCompactionTask(
                                cfs, 
                                taskSSTables, 
                                taskId,
                                random.nextDouble() < QUICK_EXIT_CHANCE ? (random.nextBoolean() ? 0 : -1)
                                                                        : random.nextInt(MAX_COMPACTION_SLEEP_MS - MIN_COMPACTION_SLEEP_MS) + MIN_COMPACTION_SLEEP_MS,
                                tasksCompleted,
                                tasksCancelled,
                                tasksCancelledBeforeStart,
                                tasksCancelledAfterStart
                            );
                            
                            // Check if task creation succeeded (tryModify returned non-null transaction)
                            allTasks.add(task);
                            task.start(executor);
                            tasksCreated.incrementAndGet();
                            logger.debug("Created task {} with {} sstables", taskId, taskSSTables.size());
                        }
                        
                        // Random delay before creating next task
                        Thread.sleep(random.nextInt(50) + 10);
                    }
                    catch (InterruptedException e)
                    {
                        Thread.currentThread().interrupt();
                        break;
                    }
                    catch (WasNullException e)
                    {
                        tasksCreationFailed.incrementAndGet();
                    }
                    catch (Exception e)
                    {
                        logger.error("Error creating task", e);
                        fail("Unexpected error creating task");
                    }
                }
            });
            
            // Thread 2: Randomly cancel compactions using runWithCompactionsDisabled
            Future<?> taskCanceller = executor.submit(() -> {
                while (testRunning.get() && System.currentTimeMillis() < endTime)
                {
                    try
                    {
                        // Wait a bit before attempting cancellation
                        Thread.sleep(random.nextInt(1000) + 500);
                        
                        // Select a random token range to cancel
                        long rangeStart = random.nextInt(SSTABLE_COUNT * 10);
                        long rangeEnd = rangeStart + random.nextInt(500) + 10;
                        Range<Token> range = new Range<>(token(rangeStart), token(rangeEnd));
                        
                        cancellationAttempts.incrementAndGet();
                        logger.debug("Attempting cancellation for range [{}, {}]", rangeStart, rangeEnd);
                        
                        CountDownLatch cancelLatch = new CountDownLatch(1);
                        Predicate<SSTableReader> predicate = (sstable) -> sstable.intersects(Collections.singleton(range));
                        if (cfs.runWithCompactionsDisabled(
                            () -> {
                                logger.debug("Successful cancellation for range [{}, {}]", rangeStart, rangeEnd);
                                cancellationSuccess.incrementAndGet();
                                assertEquals("No sstables intersecting range may be compacting inside runWithCompactionsDisabled block",
                                             0, cfs.getCompactingSSTables().stream().filter(predicate).count());
                                return true;
                            },
                            predicate,
                            false,
                            false,
                            false,
                            UNIT_TESTS
                        ) == null)
                            logger.warn("Failed cancellation for range [{}, {}]", rangeStart, rangeEnd);
                    }
                    catch (InterruptedException e)
                    {
                        Thread.currentThread().interrupt();
                        break;
                    }
                    catch (Exception e)
                    {
                        logger.error("Error during cancellation", e);
                    }
                }
            });
            
            // Wait for test duration
            taskCreator.get(TEST_DURATION_SECONDS + 5, TimeUnit.SECONDS);
            taskCanceller.get(TEST_DURATION_SECONDS + 5, TimeUnit.SECONDS);
            
            // Stop test
            testRunning.set(false);
            
            // Wait for all tasks to complete
            logger.info("Waiting for all tasks to complete...");
            long waitStart = System.currentTimeMillis();
            while (allTasks.stream().anyMatch(t -> !t.isFinished()))
            {
                if (System.currentTimeMillis() - waitStart > 10000)
                {
                    fail("Tasks did not complete within timeout");
                }
                Thread.sleep(100);
            }
            
            // Verify all tasks
            logger.info("Verifying task states...");
            for (RandomizedCompactionTask task : allTasks)
            {
                assertTrue("Task " + task.taskId + " should be finished", task.isFinished());
                assertTrue("Task " + task.taskId + " should have run cleanup", task.cleanupRan.get());
                
                // Only verify wasStopRequested for tasks that were cancelled after becoming active
                // Tasks cancelled before start or during initialization won't have this flag set
                if (task.wasCancelled.get() && task.wasStopRequested.get())
                {
                    // If wasStopRequested is true, the task must have been cancelled after starting
                    assertTrue("Task " + task.taskId + " with stop requested should have started", 
                               task.started.get());
                }
            }
            
            // Verify no leaks in ActiveOperations
            List<AbstractCompactionTask> scheduledTestTasks = CompactionManager.instance.active.getScheduledTasks()
                .stream()
                .filter(RandomizedCompactionTask.class::isInstance)
                .collect(Collectors.toList());
            
            assertEquals("No operations should be in scheduled list after test", 0, scheduledTestTasks.size());
            
            // Verify no test tasks remain in ActiveOperations
            List<TableOperation> remainingOps = CompactionManager.instance.active.getTableOperations()
                .stream()
                .filter(op -> op.getProgress().table().orElse("unknown").equalsIgnoreCase(cfs.name))
                .collect(Collectors.toList());
            
            assertEquals("No test operations should remain in ActiveOperations", 0, remainingOps.size());

            assertEquals("No sstables should be still compacting", 0, cfs.getCompactingSSTables().size());
            
            // Print statistics
            logger.info("Test completed:");
            logger.info("  Cancellation attempts: {}", cancellationAttempts.get());
            logger.info("    - Successful: {}", cancellationSuccess.get());
            logger.info("  Tasks created: {}", tasksCreated.get());
            logger.info("  Task creation failures: {}", tasksCreationFailed.get());
            logger.info("  Tasks completed: {}", tasksCompleted.get());
            logger.info("  Tasks cancelled: {}", tasksCancelled.get());
            logger.info("    - Cancelled before start: {}", tasksCancelledBeforeStart.get());
            logger.info("    - Cancelled after start: {}", tasksCancelledAfterStart.get());
            logger.info("  Total tasks: {}", allTasks.size());
            
            // Verify counts
            assertEquals("All tasks should be accounted for", 
                         tasksCompleted.get() + tasksCancelled.get(), 
                         allTasks.size());
            assertEquals("Cancelled tasks breakdown should match total", 
                         tasksCancelledBeforeStart.get() + tasksCancelledAfterStart.get(), 
                         tasksCancelled.get());

            assertEquals("Cancellations should all succeed", cancellationAttempts.get(), cancellationSuccess.get());
        }
        finally
        {
            testRunning.set(false);
            executor.shutdownNow();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }
    }
    
    private Token token(long t)
    {
        return new Murmur3Partitioner.LongToken(t);
    }
    
    private List<SSTableReader> createSSTables(ColumnFamilyStore cfs, int count, int startGeneration)
    {
        List<SSTableReader> sstables = new ArrayList<>();
        for (int i = 0; i < count; i++)
        {
            long first = i * 10;
            long last = (i + 1) * 10 - 1;
            sstables.add(MockSchema.sstable(startGeneration + i, 0, true, first, last, cfs));
        }
        cfs.disableAutoCompaction();
        cfs.addSSTables(sstables);
        return sstables;
    }

    static class WasNullException extends RuntimeException
    {

    }
    
    /**
     * A compaction task that sleeps for a random amount of time instead of waiting on signals.
     * Tracks its lifecycle for verification.
     */
    private static class RandomizedCompactionTask extends AbstractCompactionTask
    {
        private static final Logger logger = LoggerFactory.getLogger(RandomizedCompactionTask.class);
        
        private final ColumnFamilyStore cfs;
        private final Set<SSTableReader> sstables;
        private final int taskId;
        private final int sleepTimeMs;
        private final AtomicInteger completedCounter;
        private final AtomicInteger cancelledCounter;
        private final AtomicInteger cancelledBeforeStartCounter;
        private final AtomicInteger cancelledAfterStartCounter;
        
        private CompactionController controller;
        private CompactionIterator ci;
        private List<ISSTableScanner> scanners;
        private Closeable closeable;
        
        // State tracking
        private final AtomicBoolean started = new AtomicBoolean(false);
        private final AtomicBoolean finished = new AtomicBoolean(false);
        private final AtomicBoolean cleanupRan = new AtomicBoolean(false);
        private final AtomicBoolean wasCancelled = new AtomicBoolean(false);
        private final AtomicBoolean wasStopRequested = new AtomicBoolean(false);
        
        public RandomizedCompactionTask(ColumnFamilyStore cfs, 
                                       Set<SSTableReader> sstables, 
                                       int taskId,
                                       int sleepTimeMs,
                                       AtomicInteger completedCounter,
                                       AtomicInteger cancelledCounter,
                                       AtomicInteger cancelledBeforeStartCounter,
                                       AtomicInteger cancelledAfterStartCounter)
        {
            super(cfs, checkNotNull(cfs.getTracker().tryModify(sstables, OperationType.COMPACTION)));
            this.cfs = cfs;
            this.sstables = sstables;
            this.taskId = taskId;
            this.sleepTimeMs = sleepTimeMs;
            this.completedCounter = completedCounter;
            this.cancelledCounter = cancelledCounter;
            this.cancelledBeforeStartCounter = cancelledBeforeStartCounter;
            this.cancelledAfterStartCounter = cancelledAfterStartCounter;
        }
        
        static <T> T checkNotNull(T value)
        {
            if (value == null)
                throw new WasNullException();
            return value;
        }

        public void runMayThrow() throws InterruptedException
        {
            try
            {
                started.set(true);
                
                if (transaction == null)
                {
                    // Task was cancelled before it could start
                    logger.debug("Task {} cancelled before start", taskId);
                    wasCancelled.set(true);
                    cancelledCounter.incrementAndGet();
                    cancelledBeforeStartCounter.incrementAndGet();
                    return;
                }

                if (sleepTimeMs <= 0)
                {
                    // quick exit, to test task not moving to active
                    logger.debug("Task {} performing quick exit{}", taskId, sleepTimeMs < 0 ? " with exception" : "");
                    completedCounter.incrementAndGet();
                    if (sleepTimeMs < 0)
                        throw new RuntimeException("test");
                    else
                        return;
                }

                scanners = sstables.stream().map(SSTableReader::getScanner).collect(Collectors.toList());
                controller = new CompactionController(cfs, sstables, Integer.MIN_VALUE);
                ci = new CompactionIterator(transaction.opType(), scanners, controller, FBUtilities.nowInSeconds(), UUID.randomUUID());
                TableOperation op = ci.getOperation();
                closeable = opObserver.onOperationStart(op);
                switchToActive();
                
                logger.debug("Task {} started, sleeping for {}ms", taskId, sleepTimeMs);
                
                // Sleep in small increments to check for cancellation
                int slept = 0;
                while (slept < sleepTimeMs)
                {
                    if (op.isStopRequested())
                    {
                        wasStopRequested.set(true);
                        logger.debug("Task {} stop requested after {}ms", taskId, slept);
                        break;
                    }
                    Thread.sleep(Math.min(100, sleepTimeMs - slept));
                    slept += 100;
                }
                
                if (wasStopRequested.get())
                {
                    wasCancelled.set(true);
                    cancelledCounter.incrementAndGet();
                    cancelledAfterStartCounter.incrementAndGet();
                    logger.debug("Task {} cancelled after start", taskId);
                }
                else
                {
                    completedCounter.incrementAndGet();
                    logger.debug("Task {} completed normally", taskId);
                }
            }
            finally
            {
                complete();
                finished.set(true);
            }
        }
        
        private void complete()
        {
            cleanupRan.set(true);
            
            if (controller != null)
                controller.close();
            if (ci != null)
                ci.close();
            if (scanners != null)
                scanners.forEach(ISSTableScanner::close);
            if (closeable != null)
                Throwables.maybeFail(Throwables.close(null, closeable));
        }
        
        @Override
        public void cancelledOnStart()
        {
            started.set(true);
            finished.set(true);
            cleanupRan.set(true);
            wasCancelled.set(true);
            cancelledCounter.incrementAndGet();
            cancelledBeforeStartCounter.incrementAndGet();
            logger.debug("Task {} cancelled on start", taskId);
        }
        
        public void start(ExecutorService executor)
        {
            executor.submit(() -> execute(CompactionManager.instance.active));
        }
        
        public boolean isFinished()
        {
            return finished.get();
        }

        @Override
        public long getSpaceOverhead()
        {
            return sstables.stream().mapToLong(SSTableReader::onDiskLength).sum();
        }
    }
}
