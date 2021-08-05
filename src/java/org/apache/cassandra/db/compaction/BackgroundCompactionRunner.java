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

import java.io.File;
import java.io.IOError;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ConcurrentHashMultiset;
import com.google.common.collect.Multiset;
import com.google.common.primitives.Longs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.DebuggableScheduledThreadPoolExecutor;
import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.FSDiskFullWriteError;
import org.apache.cassandra.io.FSError;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.Throwables;

public class BackgroundCompactionRunner implements Runnable
{
    private final static Logger logger = LoggerFactory.getLogger(BackgroundCompactionRunner.class);

    public enum RequestResult
    {
        /**
         * when the compaction check was done and there were no compaction tasks for the CFS
         */
        NOT_NEEDED,

        /**
         * when the compaction was aborted for the CFS because the CF got dropped in the meantime
         */
        ABORTED,

        /**
         * compaction tasks were completed for the CFS
         */
        COMPLETED
    }

    private final DebuggableScheduledThreadPoolExecutor checkExecutor;

    /**
     * Each time we start a compaction, the CFS is added to this set; it is removed when the compaction finishes
     * or when it is cancelled
     */
    private final Multiset<ColumnFamilyStore> ongoingCompactions = ConcurrentHashMultiset.create();

    /**
     * CFSs for which a compaction was requested mapped to the promise returned to the requesting code
     */
    private final ConcurrentMap<ColumnFamilyStore, FutureRequestResult> compactionRequests = new ConcurrentHashMap<>();

    private final AtomicInteger currentlyBackgroundUpgrading = new AtomicInteger(0);

    private final Random random = new Random();

    private final DebuggableThreadPoolExecutor compactionExecutor;

    private final ActiveOperations activeOperations;


    BackgroundCompactionRunner(DebuggableThreadPoolExecutor compactionExecutor, ActiveOperations activeOperations)
    {
        this(compactionExecutor, new DebuggableScheduledThreadPoolExecutor("BackgroundTaskExecutor"), activeOperations);
    }

    @VisibleForTesting
    BackgroundCompactionRunner(DebuggableThreadPoolExecutor compactionExecutor, DebuggableScheduledThreadPoolExecutor checkExecutor, ActiveOperations activeOperations)
    {
        this.compactionExecutor = compactionExecutor;
        this.checkExecutor = checkExecutor;
        this.activeOperations = activeOperations;
    }

    /**
     * This extends and behave like a {@link CompletableFuture}, with the exception that one cannot call
     * {@link #cancel}, {@link #complete} and {@link #completeExceptionally} (they throw {@link UnsupportedOperationException}).
     */
    public static class FutureRequestResult extends CompletableFuture<RequestResult>
    {
        @Override
        public boolean complete(RequestResult t)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean cancel(boolean interruptIfRunning)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean completeExceptionally(Throwable throwable)
        {
            throw new UnsupportedOperationException();
        }

        private void completeInternal(RequestResult t)
        {
            super.complete(t);
        }

        private void completeExceptionallyInternal(Throwable throwable)
        {
            super.completeExceptionally(throwable);
        }
    }

    /**
     * Marks a CFS for compaction. Since marked, it will become a possible candidate for compaction. The mark will be
     * cleared when we actually run the compaction for CFS.
     *
     * @return a promise which will be completed when the mark is cleared. The returned future should not be cancelled or
     *         completed by the caller.
     */
    CompletableFuture<RequestResult> requestCompaction(ColumnFamilyStore cfs)
    {
        logger.trace("Requested background compaction for {}.{}", cfs.getKeyspaceName(), cfs.getTableName());
        FutureRequestResult p = compactionRequests.computeIfAbsent(cfs, ignored -> new FutureRequestResult());
        if (!maybeScheduleNextCheck())
        {
            logger.info("Executor has been shut down, background compactions check will not be scheduled");
            p.completeInternal(RequestResult.ABORTED);
        }
        return p;
    }

    void shutdown()
    {
        checkExecutor.shutdown();
        compactionRequests.values().forEach(promise -> promise.completeInternal(RequestResult.ABORTED));
        // it's okay to complete a CompletableFuture more than one on race between request, run and shutdown
    }

    int getOngoingCompactionsCount(ColumnFamilyStore cfs)
    {
        return ongoingCompactions.count(cfs);
    }

    @Override
    public void run()
    {
        logger.trace("Running background compactions check");

        // We shuffle the CFSs for which the compaction was requested so that with each run we traverse those CFSs
        // in different order and make each CFS have equal chance to be selected
        ArrayList<ColumnFamilyStore> compactionRequestsList = new ArrayList<>(compactionRequests.keySet());
        Collections.shuffle(compactionRequestsList, random);

        for (ColumnFamilyStore cfs : compactionRequestsList)
        {
            // When the executor is fully occupied, we delay acting on this request until a thread is available. This
            // helps make a better decision what exactly to compact (e.g. if we issue the request now we may select n
            // sstables, while by the time this request actually has a thread to execute on more may have accumulated
            // and it may be better to compact all).
            if (compactionExecutor.getActiveTaskCount() >= compactionExecutor.getMaximumPoolSize())
            {
                logger.trace("Background compaction is still running for {}.{} - the check will be done after completion",
                             cfs.getKeyspaceName(), cfs.getTableName());
                continue;
            }

            FutureRequestResult promise = compactionRequests.remove(cfs);
            assert promise != null : "Background compaction checker must be single-threaded";

            if (promise.isDone())
            {
                // A shutdown request may abort processing while we are still processing
                assert promise.join() == RequestResult.ABORTED : "Background compaction checker must be single-threaded";

                logger.trace("The request for {}.{} was aborted due to shutdown",
                             cfs.getKeyspaceName(), cfs.getTableName());
                continue;
            }

            if (!cfs.isValid())
            {
                logger.trace("Aborting compaction for dropped CF {}.{}", cfs.getKeyspaceName(), cfs.getTableName());
                promise.completeInternal(RequestResult.ABORTED);
                continue;
            }

            logger.trace("Running a background task check for {}.{} with {}",
                         cfs.keyspace.getName(),
                         cfs.name,
                         cfs.getCompactionStrategy().getName());

            // When there are tasks to be done for a CFS, we add the CFS to ongoingCompactions for the time the tasks
            // are being executed, and then remove it. When there is no task to be run, we do not add the CFS to the
            // ongoingCompactions.
            // The removal of the CFS from ongoingCompactions is tied to the completion of the tasks, therefore CFS has
            // to be added to ongoingCompactions before the task is started because otherwise addition and removal could
            // be reordered, and we would have a race.
            // The supplier is used because it allows determining whether there are tasks to be run before starting
            // them. We can also keep all the modifications of the ongoingCompactions in a single place rather than
            // having them spread across the other methods
            Supplier<CompletableFuture<?>[]> compactionTasks = lazyRunCompactionTasks(cfs);
            if (compactionTasks == null)
                compactionTasks = lazyRunUpgradeTasks(cfs);

            if (compactionTasks != null)
            {
                ongoingCompactions.add(cfs);

                CompletableFuture<?>[] tasksResults = compactionTasks.get();

                CompletableFuture.allOf(tasksResults).handle((ignored, throwable) -> {
                    ongoingCompactions.remove(cfs);

                    if (throwable != null)
                    {
                        logger.warn(String.format("Aborting compaction of %s.%s due to error", cfs.getKeyspaceName(), cfs.getTableName()), throwable);
                        handleCompactionError(throwable, cfs);
                        promise.completeExceptionallyInternal(throwable);
                    }
                    else
                    {
                        logger.trace("Finished compaction for {}.{}", cfs.getKeyspaceName(), cfs.getTableName());
                        promise.completeInternal(RequestResult.COMPLETED);
                    }

                    // if at least one compaction task completes successfully, the set of sstables may be changed,
                    // therefore we check for new compaction possibilities
                    if (Arrays.stream(tasksResults).anyMatch(f -> !f.isCancelled() && !f.isCompletedExceptionally()))
                        requestCompaction(cfs);

                    return null;
                });
            }
            else
            {
                promise.completeInternal(RequestResult.NOT_NEEDED);
            }
        }
    }

    private boolean maybeScheduleNextCheck()
    {
        if (checkExecutor.getQueue().isEmpty())
        {
            try
            {
                checkExecutor.execute(this);
            }
            catch (RejectedExecutionException ex)
            {
                if (checkExecutor.isShutdown())
                    logger.info("Executor has been shut down, background compactions check will not be scheduled");
                else
                    logger.error("Failed to submit background compactions check", ex);

                return false;
            }
        }

        return true;
    }

    private Supplier<CompletableFuture<?>[]> lazyRunCompactionTasks(ColumnFamilyStore cfs)
    {
        Collection<AbstractCompactionTask> compactionTasks = cfs.getCompactionStrategy()
                                                                .getNextBackgroundTasks(CompactionManager.getDefaultGcBefore(cfs, FBUtilities.nowInSeconds()));
        if (!compactionTasks.isEmpty())
        {
            return () -> {
                logger.debug("Running compaction tasks: {}", compactionTasks);
                return compactionTasks.stream()
                                      .map(task -> CompletableFuture.runAsync(() -> task.execute(activeOperations), compactionExecutor))
                                      .toArray(CompletableFuture<?>[]::new);
            };
        }
        else
        {
            logger.debug("No compaction tasks for {}.{}", cfs.getKeyspaceName(), cfs.getTableName());
            return null;
        }
    }

    private Supplier<CompletableFuture<?>[]> lazyRunUpgradeTasks(ColumnFamilyStore cfs)
    {
        AbstractCompactionTask upgradeTask = getUpgradeSSTableTask(cfs);

        if (upgradeTask != null)
        {
            return () -> {
                logger.debug("Running upgrade task: {}", upgradeTask);
                CompletableFuture<Object> result = CompletableFuture.runAsync(() -> upgradeTask.execute(activeOperations), compactionExecutor)
                                                                    .handle((ignored1, ignored2) -> {
                                                                        currentlyBackgroundUpgrading.decrementAndGet();
                                                                        return null;
                                                                    });
                return new CompletableFuture[]{ result };
            };
        }
        else
        {
            logger.debug("No upgrade tasks for {}.{}", cfs.getKeyspaceName(), cfs.getTableName());
            return null;
        }
    }

    /**
     * Finds the oldest (by modification date) non-latest-version sstable on disk and creates an upgrade task for it
     */
    @VisibleForTesting
    public AbstractCompactionTask getUpgradeSSTableTask(ColumnFamilyStore cfs)
    {
        logger.trace("Checking for upgrade tasks {}.{}", cfs.getKeyspaceName(), cfs.getTableName());

        if (!DatabaseDescriptor.automaticSSTableUpgrade())
        {
            logger.trace("Automatic sstable upgrade is disabled - will not try to upgrade sstables of {}.{}",
                         cfs.getKeyspaceName(),
                         cfs.getTableName());
            return null;
        }

        if (currentlyBackgroundUpgrading.incrementAndGet() <= DatabaseDescriptor.maxConcurrentAutoUpgradeTasks())
        {
            Set<SSTableReader> compacting = cfs.getTracker().getCompacting();
            List<SSTableReader> potentialUpgrade = cfs.getLiveSSTables()
                                                      .stream()
                                                      .filter(s -> !compacting.contains(s) && !s.descriptor.version.isLatestVersion())
                                                      .sorted((o1, o2) -> {
                                                          File f1 = new File(o1.descriptor.filenameFor(Component.DATA));
                                                          File f2 = new File(o2.descriptor.filenameFor(Component.DATA));
                                                          return Longs.compare(f1.lastModified(), f2.lastModified());
                                                      }).collect(Collectors.toList());
            for (SSTableReader sstable : potentialUpgrade)
            {
                LifecycleTransaction txn = cfs.getTracker().tryModify(sstable, OperationType.UPGRADE_SSTABLES);
                if (txn != null)
                {
                    logger.debug("Found tasks for automatic sstable upgrade of {}", sstable);
                    return cfs.getCompactionStrategy().createCompactionTask(txn, Integer.MIN_VALUE, Long.MAX_VALUE);
                }
            }
        }
        else
        {
            logger.trace("Skipped upgrade task for {}.{} because the limit {} of concurrent upgrade tasks has been reached",
                         cfs.getKeyspaceName(),
                         cfs.getTableName(),
                         DatabaseDescriptor.maxConcurrentAutoUpgradeTasks());
        }

        currentlyBackgroundUpgrading.decrementAndGet();
        return null;
    }

    private static void handleCompactionError(Throwable t, ColumnFamilyStore cfs)
    {
        t = Throwables.unwrapped(t);
        // FSDiskFullWriteErrors caught during compaction are expected to be recoverable, so we don't explicitly
        // trigger the disk failure policy because of them (see CASSANDRA-12385).
        if (t instanceof IOError && !(t instanceof FSDiskFullWriteError))
        {
            logger.error("Potentially unrecoverable error during background compaction of table {}", cfs, t);
            // Strictly speaking it's also possible to hit a read-related IOError during compaction, although the
            // chances for that are much lower than the chances for write-related IOError. If we want to handle that,
            // we might have to rely on error message parsing...
            t = t instanceof FSError ? t : new FSWriteError(t);
            JVMStabilityInspector.inspectThrowable(t);
        }
        else
        {
            logger.error("Exception during background compaction of table {}", cfs, t);
        }
    }
}
