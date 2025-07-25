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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.TabularData;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Collections2;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Meter;
import io.netty.util.concurrent.FastThreadLocal;
import org.apache.cassandra.cache.AutoSavingCache;
import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.concurrent.JMXEnabledThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.DiskBoundaries;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.compaction.BackgroundCompactionRunner.RequestResult;
import org.apache.cassandra.db.lifecycle.ILifecycleTransaction;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.lifecycle.SSTableIntervalTree;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.db.lifecycle.WrappedLifecycleTransaction;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.view.ViewBuilderTask;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.index.SecondaryIndexBuilder;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.IndexSummaryRedistribution;
import org.apache.cassandra.io.sstable.SSTableRewriter;
import org.apache.cassandra.io.sstable.ScannerList;
import org.apache.cassandra.io.sstable.SnapshotDeletingTask;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.locator.RangesAtEndpoint;
import org.apache.cassandra.metrics.CompactionMetrics;
import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.schema.CompactionParams.TombstoneOption;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.MBeanWrapper;
import org.apache.cassandra.utils.NonThrowingCloseable;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.cassandra.utils.WrappedRunnable;
import org.apache.cassandra.utils.concurrent.Refs;

import static java.util.Collections.singleton;
import static org.apache.cassandra.service.ActiveRepairService.NO_PENDING_REPAIR;
import static org.apache.cassandra.service.ActiveRepairService.UNREPAIRED_SSTABLE;

/**
 * <p>
 * A singleton which manages a private executor of ongoing compactions.
 * </p>
 * Scheduling for compaction is accomplished by swapping sstables to be compacted into
 * a set via Tracker. New scheduling attempts will ignore currently compacting
 * sstables.
 */
public class CompactionManager implements CompactionManagerMBean
{
    public static final String MBEAN_OBJECT_NAME = "org.apache.cassandra.db:type=CompactionManager";
    private static final Logger logger = LoggerFactory.getLogger(CompactionManager.class);
    public static final CompactionManager instance;

    public static final int NO_GC = Integer.MIN_VALUE;
    public static final int GC_ALL = Integer.MAX_VALUE;

    // A thread local that tells us if the current thread is owned by the compaction manager. Used
    // by CounterContext to figure out if it should log a warning for invalid counter shards.
    public static final FastThreadLocal<Boolean> isCompactionManager = new FastThreadLocal<Boolean>()
    {
        @Override
        protected Boolean initialValue()
        {
            return false;
        }
    };
    private static final int ACQUIRE_GRANULARITY =
    Integer.getInteger(Config.PROPERTY_PREFIX + "compaction_rate_limit_granularity_in_kb", 128) * 1024;
    private static final String CONTROLLER_CONFIG_JSON_SUFFIX = "-controller-config.JSON";

    static
    {
        instance = new CompactionManager();

        MBeanWrapper.instance.registerMBean(instance, MBEAN_OBJECT_NAME);

        /*Schedule periodic reports to run every minute*/
        ScheduledExecutors.scheduledTasks.scheduleAtFixedRate(CompactionManager::periodicReports, 1, 1, TimeUnit.MINUTES);

        /*Delete any controller-config.JSON files that correspond to a table that no longer exists*/
        cleanupControllerConfig();

        /*Store Controller Config for UCS every hour*/
        ScheduledExecutors.scheduledTasks.scheduleAtFixedRate(CompactionManager::storeControllerConfig, 10, 60, TimeUnit.MINUTES);
    }

    private final CompactionExecutor executor = new CompactionExecutor();
    private final ValidationExecutor validationExecutor = new ValidationExecutor();
    private final CompactionExecutor cacheCleanupExecutor = new CacheCleanupExecutor();
    private final CompactionExecutor viewBuildExecutor = new ViewBuildExecutor();

    private final CompactionMetrics metrics = new CompactionMetrics(executor, validationExecutor, viewBuildExecutor);

    public final ActiveOperations active = new ActiveOperations();

    private final BackgroundCompactionRunner backgroundCompactionRunner = new BackgroundCompactionRunner(executor, active);

    // used to temporarily pause non-strategy managed compactions (like index summary redistribution)
    private final AtomicInteger globalCompactionPauseCount = new AtomicInteger(0);

    private final RateLimiter compactionRateLimiter = RateLimiter.create(Double.MAX_VALUE);

    protected static void periodicReports()
    {
        for (String keyspace : Schema.instance.getKeyspaces())
        {
            if (Schema.instance.getKeyspaceInstance(keyspace) != null)
            {
                for (ColumnFamilyStore cfs : Schema.instance.getKeyspaceInstance(keyspace).getColumnFamilyStores())
                {
                    CompactionStrategy strat = cfs.getCompactionStrategy();
                    strat.periodicReport();
                }
            }
        }
    }

    @VisibleForTesting
    public static void storeControllerConfig()
    {
        for (String keyspace : Schema.instance.getKeyspaces())
        {
            //don't store config files for system tables
            if (Schema.instance.getKeyspaceInstance(keyspace) != null && !SchemaConstants.isSystemKeyspace(keyspace))
            {
                for (ColumnFamilyStore cfs : Schema.instance.getKeyspaceInstance(keyspace).getColumnFamilyStores())
                {
                    CompactionStrategy strat = cfs.getCompactionStrategy();
                    if (strat instanceof UnifiedCompactionContainer)
                    {
                        UnifiedCompactionStrategy ucs = (UnifiedCompactionStrategy) ((UnifiedCompactionContainer) strat).getStrategies().get(0);
                        ucs.storeControllerConfig();
                    }
                }
            }
        }
    }

    @VisibleForTesting
    public static void cleanupControllerConfig()
    {
        Pattern fileNamePattern = Pattern.compile(CONTROLLER_CONFIG_JSON_SUFFIX, Pattern.LITERAL);
        Pattern keyspaceNameSeparator = Pattern.compile("\\.");
        File dir = DatabaseDescriptor.getMetadataDirectory();
        if (dir != null)
        {
            for (File file : dir.tryList())
            {
                if (file.name().contains(CONTROLLER_CONFIG_JSON_SUFFIX))
                {
                    String[] names = keyspaceNameSeparator.split(fileNamePattern.matcher(file.name()).replaceAll(""));
                    if (names.length == 2)
                    {
                        try
                        {
                            //table exists so keep the file
                            Schema.instance.getKeyspaceInstance(names[0]).getColumnFamilyStore(names[1]);
                        }
                        catch (NullPointerException e)
                        {
                            //table does not exist so delete the file
                            logger.debug("Removing " + file + " because it does not correspond to an existing table");
                            file.delete();
                        }
                    }
                    else if (names.length == 3) // if keyspace/table names are long, we include table id as a 3rd component while the keyspace and table names are abbreviated
                    {
                        TableId tableId = TableId.fromHexString(names[2]);
                        //table exists so keep the file
                        if (Schema.instance.getTableMetadata(tableId) == null)
                        {
                            //table does not exist so delete the file
                            logger.debug("Removing " + file + " because it does not correspond to an existing table");
                            file.delete();
                        }
                    }
                }
            }
        }
    }

    public CompactionMetrics getMetrics()
    {
        return metrics;
    }

    /**
     * Gets compaction rate limiter.
     * Rate unit is bytes per sec.
     *
     * @return RateLimiter with rate limit set
     */
    public RateLimiter getRateLimiter()
    {
        setRate(DatabaseDescriptor.getCompactionThroughputMbPerSec());
        return compactionRateLimiter;
    }

    /**
     * Sets the rate for the rate limiter. When compaction_throughput_mb_per_sec is 0 or node is bootstrapping,
     * this sets the rate to Double.MAX_VALUE bytes per second.
     * @param throughPutMbPerSec throughput to set in mb per second
     */
    public void setRate(final double throughPutMbPerSec)
    {
        double throughput = throughPutMbPerSec * 1024.0 * 1024.0;
        // if throughput is set to 0, throttling is disabled
        if (throughput == 0 || StorageService.instance.isBootstrapMode())
            throughput = Double.MAX_VALUE;
        if (compactionRateLimiter.getRate() != throughput)
            compactionRateLimiter.setRate(throughput);
    }

    public Meter getCompactionThroughput()
    {
        return metrics.bytesCompactedThroughput;
    }

    /**
     * Call this whenever a compaction might be needed on the given column family store.
     * It's okay to over-call (within reason) if a call is unnecessary, it will
     * turn into a no-op in the bucketing/candidate-scan phase.
     */
    public CompletableFuture<RequestResult> submitBackground(final ColumnFamilyStore cfs)
    {
        return backgroundCompactionRunner.markForCompactionCheck(cfs);
    }

    public void submitBackground(Set<ColumnFamilyStore> cfss)
    {
        backgroundCompactionRunner.markForCompactionCheck(cfss);
    }

    public int getOngoingBackgroundCompactionsCount()
    {
        return backgroundCompactionRunner.getOngoingCompactionsCount();
    }

    public CompletableFuture<?>[] startCompactionTasks(ColumnFamilyStore cfs, Collection<AbstractCompactionTask> tasks)
    {
        return backgroundCompactionRunner.startCompactionTasks(cfs, tasks);
    }

    public int getOngoingBackgroundUpgradesCount()
    {
        return backgroundCompactionRunner.getOngoingUpgradesCount();
    }

    public boolean isCompacting(Iterable<ColumnFamilyStore> cfses, Predicate<SSTableReader> sstablePredicate)
    {
        for (ColumnFamilyStore cfs : cfses)
            if (cfs.getTracker().getCompacting().stream().anyMatch(sstablePredicate))
                return true;
        return false;
    }

    @VisibleForTesting
    public boolean hasOngoingOrPendingTasks()
    {
        if (!active.getTableOperations().isEmpty())
            return true;

        int pendingTasks = executor.getPendingTaskCount() +
                           validationExecutor.getPendingTaskCount() +
                           viewBuildExecutor.getPendingTaskCount() +
                           cacheCleanupExecutor.getPendingTaskCount();
        if (pendingTasks > 0)
            return true;

        int activeTasks = executor.getActiveTaskCount() +
                          validationExecutor.getActiveTaskCount() +
                          viewBuildExecutor.getActiveTaskCount() +
                          cacheCleanupExecutor.getActiveTaskCount();

        return activeTasks > 0;
    }
    /**
     * Shutdowns both compaction and validation executors, cancels running compaction / validation,
     * and waits for tasks to complete if tasks were not cancelable.
     */
    public void forceShutdown()
    {
        // shutdown executors to prevent further submission
        backgroundCompactionRunner.shutdown();
        executor.shutdown();
        validationExecutor.shutdown();
        viewBuildExecutor.shutdown();
        cacheCleanupExecutor.shutdown();

        // interrupt compactions and validations
        for (TableOperation operationSource : active.getTableOperations())
        {
            operationSource.stop(TableOperation.StopTrigger.SHUTDOWN);
        }

        // wait for tasks to terminate
        // compaction tasks are interrupted above, so it shuold be fairy quick
        // until not interrupted tasks to complete.
        for (ExecutorService exec : Arrays.asList(executor, validationExecutor, viewBuildExecutor, cacheCleanupExecutor))
        {
            try
            {
                if (!exec.awaitTermination(1, TimeUnit.MINUTES))
                    logger.warn("Failed to wait for compaction executors shutdown");
            }
            catch (InterruptedException e)
            {
                logger.error("Interrupted while waiting for tasks to be terminated", e);
            }
        }
    }

    public void finishCompactionsAndShutdown(long timeout, TimeUnit unit) throws InterruptedException
    {
        backgroundCompactionRunner.shutdown();
        executor.shutdown();
        executor.awaitTermination(timeout, unit);
    }

    /**
     * Run an operation over all sstables using jobs threads
     *
     * @param cfs the column family store to run the operation on
     * @param operation the operation to run
     * @param jobs the number of threads to use - 0 means use all available. It never uses more than concurrent_compactors threads
     * @return status of the operation
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @SuppressWarnings("resource")
    private AllSSTableOpStatus parallelAllSSTableOperation(final ColumnFamilyStore cfs,
                                                           final OneSSTableOperation operation,
                                                           int jobs,
                                                           OperationType operationType,
                                                           TableOperation.StopTrigger trigger)
    {
        logger.info("Starting {} for {}.{}", operationType, cfs.keyspace.getName(), cfs.getTableName());
        List<LifecycleTransaction> transactions = new ArrayList<>();
        List<Future<?>> futures = new ArrayList<>();
        try (LifecycleTransaction compacting = cfs.markAllCompacting(operationType, trigger))
        {
            if (compacting == null)
                return AllSSTableOpStatus.UNABLE_TO_CANCEL;

            Iterable<SSTableReader> sstables = Lists.newArrayList(operation.filterSSTables(compacting));
            if (Iterables.isEmpty(sstables))
            {
                logger.info("No sstables to {} for {}.{}", operationType.name(), cfs.keyspace.getName(), cfs.name);
                return AllSSTableOpStatus.SUCCESSFUL;
            }

            for (final SSTableReader sstable : sstables)
            {
                final LifecycleTransaction txn = compacting.split(singleton(sstable));
                transactions.add(txn);
                Callable<Object> callable = new Callable<Object>()
                {
                    @Override
                    public Object call() throws Exception
                    {
                        operation.execute(txn);
                        return this;
                    }
                };
                Future<?> fut = executor.submitIfRunning(callable, "paralell sstable operation");
                if (!fut.isCancelled())
                    futures.add(fut);
                else
                    return AllSSTableOpStatus.ABORTED;

                if (jobs > 0 && futures.size() == jobs)
                {
                    Future<?> f = FBUtilities.waitOnFirstFuture(futures);
                    futures.remove(f);
                }
            }
            FBUtilities.waitOnFutures(futures);
            assert compacting.originals().isEmpty();
            logger.info("Finished {} for {}.{} successfully", operationType, cfs.keyspace.getName(), cfs.getTableName());
            return AllSSTableOpStatus.SUCCESSFUL;
        }
        finally
        {
            // wait on any unfinished futures to make sure we don't close an ongoing transaction
            try
            {
                FBUtilities.waitOnFutures(futures);
            }
            catch (Throwable t)
            {
               // these are handled/logged in CompactionExecutor#afterExecute
            }
            Throwable fail = Throwables.close(null, transactions);
            if (fail != null)
                logger.error("Failed to cleanup lifecycle transactions ({} for {}.{})", operationType, cfs.keyspace.getName(), cfs.getTableName(), fail);
        }
    }

    private static interface OneSSTableOperation
    {
        Iterable<SSTableReader> filterSSTables(LifecycleTransaction transaction);
        void execute(LifecycleTransaction input) throws IOException;
    }

    public enum AllSSTableOpStatus
    {
        SUCCESSFUL(0),
        ABORTED(1),
        UNABLE_TO_CANCEL(2);

        public final int statusCode;

        AllSSTableOpStatus(int statusCode)
        {
            this.statusCode = statusCode;
        }
    }

    public AllSSTableOpStatus performScrub(final ColumnFamilyStore cfs, final boolean skipCorrupted, final boolean checkData,
                                           int jobs)
    throws InterruptedException, ExecutionException
    {
        return performScrub(cfs, skipCorrupted, checkData, false, jobs);
    }

    public AllSSTableOpStatus performScrub(final ColumnFamilyStore cfs, final boolean skipCorrupted, final boolean checkData,
                                           final boolean reinsertOverflowedTTL, int jobs)
    throws InterruptedException, ExecutionException
    {
        return parallelAllSSTableOperation(cfs, new OneSSTableOperation()
        {
            @Override
            public Iterable<SSTableReader> filterSSTables(LifecycleTransaction input)
            {
                return input.originals();
            }

            @Override
            public void execute(LifecycleTransaction input)
            {
                scrubOne(cfs, input, skipCorrupted, checkData, reinsertOverflowedTTL, active);
            }
        }, jobs, OperationType.SCRUB, TableOperation.StopTrigger.SCRUB);
    }

    public AllSSTableOpStatus performVerify(ColumnFamilyStore cfs, Verifier.Options options) throws InterruptedException, ExecutionException
    {
        assert !cfs.isIndex();
        return parallelAllSSTableOperation(cfs, new OneSSTableOperation()
        {
            @Override
            public Iterable<SSTableReader> filterSSTables(LifecycleTransaction input)
            {
                return input.originals();
            }

            @Override
            public void execute(LifecycleTransaction input)
            {
                verifyOne(cfs, input.onlyOne(), options, active);
            }
        }, 0, OperationType.VERIFY, TableOperation.StopTrigger.VERIFY);
    }

    public AllSSTableOpStatus performSSTableRewrite(final ColumnFamilyStore cfs, final boolean excludeCurrentVersion, int jobs) throws InterruptedException, ExecutionException
    {
        return parallelAllSSTableOperation(cfs, new OneSSTableOperation()
        {
            @Override
            public Iterable<SSTableReader> filterSSTables(LifecycleTransaction transaction)
            {
                List<SSTableReader> sortedSSTables = Lists.newArrayList(transaction.originals());
                Collections.sort(sortedSSTables, SSTableReader.sizeComparator.reversed());
                Iterator<SSTableReader> iter = sortedSSTables.iterator();
                while (iter.hasNext())
                {
                    SSTableReader sstable = iter.next();
                    if (excludeCurrentVersion && sstable.descriptor.version.equals(sstable.descriptor.getFormat().getLatestVersion()))
                    {
                        transaction.cancel(sstable);
                        iter.remove();
                    }
                }
                return sortedSSTables;
            }

            @Override
            public void execute(LifecycleTransaction txn)
            {
                AbstractCompactionTask task = cfs.getCompactionStrategy().createCompactionTask(txn, NO_GC, Long.MAX_VALUE);
                task.setUserDefined(true);
                task.setCompactionType(OperationType.UPGRADE_SSTABLES);
                task.execute(active);
            }
        }, jobs, OperationType.UPGRADE_SSTABLES, TableOperation.StopTrigger.UPGRADE_SSTABLES);
    }

    public AllSSTableOpStatus performCleanup(final ColumnFamilyStore cfStore, int jobs) throws InterruptedException, ExecutionException
    {
        assert !cfStore.isIndex();
        Keyspace keyspace = cfStore.keyspace;

        // if local ranges is empty, it means no data should remain
        final RangesAtEndpoint replicas = StorageService.instance.getLocalReplicas(keyspace.getName());
        final Set<Range<Token>> allRanges = replicas.ranges();
        final Set<Range<Token>> transientRanges = replicas.onlyTransient().ranges();
        final Set<Range<Token>> fullRanges = replicas.onlyFull().ranges();
        final boolean hasIndexes = cfStore.indexManager.hasIndexes();

        return parallelAllSSTableOperation(cfStore, new OneSSTableOperation()
        {
            @Override
            public Iterable<SSTableReader> filterSSTables(LifecycleTransaction transaction)
            {
                List<SSTableReader> sortedSSTables = Lists.newArrayList(transaction.originals());
                Iterator<SSTableReader> sstableIter = sortedSSTables.iterator();
                int totalSSTables = 0;
                int skippedSStables = 0;
                while (sstableIter.hasNext())
                {
                    SSTableReader sstable = sstableIter.next();
                    boolean needsCleanupFull = needsCleanup(sstable, fullRanges);
                    boolean needsCleanupTransient = !transientRanges.isEmpty() && sstable.isRepaired() && needsCleanup(sstable, transientRanges);
                    //If there are no ranges for which the table needs cleanup either due to lack of intersection or lack
                    //of the table being repaired.
                    totalSSTables++;
                    if (!needsCleanupFull && !needsCleanupTransient)
                    {
                        logger.debug("Skipping {} ([{}, {}]) for cleanup; all rows should be kept. Needs cleanup full ranges: {} Needs cleanup transient ranges: {} Repaired: {}",
                                    sstable,
                                    sstable.first.getToken(),
                                    sstable.last.getToken(),
                                    needsCleanupFull,
                                    needsCleanupTransient,
                                    sstable.isRepaired());
                        sstableIter.remove();
                        transaction.cancel(sstable);
                        skippedSStables++;
                    }
                }
                logger.info("Skipping cleanup for {}/{} sstables for {}.{} since they are fully contained in owned ranges (full ranges: {}, transient ranges: {})",
                            skippedSStables, totalSSTables, cfStore.keyspace.getName(), cfStore.getTableName(), fullRanges, transientRanges);
                sortedSSTables.sort(SSTableReader.sizeComparator);
                return sortedSSTables;
            }

            @Override
            public void execute(LifecycleTransaction txn) throws IOException
            {
                CleanupStrategy cleanupStrategy = CleanupStrategy.get(cfStore, allRanges, transientRanges, txn.onlyOne().isRepaired(), FBUtilities.nowInSeconds());
                doCleanupOne(cfStore, txn, cleanupStrategy, replicas.ranges(), hasIndexes);
            }
        }, jobs, OperationType.CLEANUP, TableOperation.StopTrigger.CLEANUP);
    }

    public AllSSTableOpStatus performGarbageCollection(final ColumnFamilyStore cfStore, TombstoneOption tombstoneOption, int jobs) throws InterruptedException, ExecutionException
    {
        assert !cfStore.isIndex();

        return parallelAllSSTableOperation(cfStore, new OneSSTableOperation()
        {
            @Override
            public Iterable<SSTableReader> filterSSTables(LifecycleTransaction transaction)
            {
                Iterable<SSTableReader> originals = transaction.originals();
                if (cfStore.onlyPurgeRepairedTombstones())
                    originals = Iterables.filter(originals, SSTableReader::isRepaired);
                List<SSTableReader> sortedSSTables = Lists.newArrayList(originals);
                Collections.sort(sortedSSTables, SSTableReader.maxTimestampAscending);
                return sortedSSTables;
            }

            @Override
            public void execute(LifecycleTransaction txn)
            {
                logger.debug("Garbage collecting {}", txn.originals());
                AbstractCompactionTask task = CompactionTask.forGarbageCollection(cfStore,
                                                                                  txn,
                                                                                  getDefaultGcBefore(cfStore, FBUtilities.nowInSeconds()),
                                                                                  tombstoneOption);
                task.execute(active);
            }
        }, jobs, OperationType.GARBAGE_COLLECT, TableOperation.StopTrigger.GARBAGE_COLLECT);
    }

    public AllSSTableOpStatus relocateSSTables(final ColumnFamilyStore cfs, int jobs) throws ExecutionException, InterruptedException
    {
        if (!cfs.getPartitioner().splitter().isPresent())
        {
            logger.info("Partitioner does not support splitting");
            return AllSSTableOpStatus.ABORTED;
        }

        if (StorageService.instance.getLocalReplicas(cfs.keyspace.getName()).isEmpty())
        {
            logger.info("Relocate cannot run before a node has joined the ring");
            return AllSSTableOpStatus.ABORTED;
        }

        final DiskBoundaries diskBoundaries = cfs.getDiskBoundaries();

        return parallelAllSSTableOperation(cfs, new OneSSTableOperation()
        {
            @Override
            public Iterable<SSTableReader> filterSSTables(LifecycleTransaction transaction)
            {
                Set<SSTableReader> originals = Sets.newHashSet(transaction.originals());
                Set<SSTableReader> needsRelocation = originals.stream().filter(s -> !inCorrectLocation(s)).collect(Collectors.toSet());
                transaction.cancel(Sets.difference(originals, needsRelocation));

                Map<Integer, List<SSTableReader>> groupedByDisk = groupByDiskIndex(needsRelocation);

                int maxSize = 0;
                for (List<SSTableReader> diskSSTables : groupedByDisk.values())
                    maxSize = Math.max(maxSize, diskSSTables.size());

                List<SSTableReader> mixedSSTables = new ArrayList<>();

                for (int i = 0; i < maxSize; i++)
                    for (List<SSTableReader> diskSSTables : groupedByDisk.values())
                        if (i < diskSSTables.size())
                            mixedSSTables.add(diskSSTables.get(i));

                return mixedSSTables;
            }

            public Map<Integer, List<SSTableReader>> groupByDiskIndex(Set<SSTableReader> needsRelocation)
            {
                return needsRelocation.stream().collect(Collectors.groupingBy((s) -> diskBoundaries.getDiskIndexFromKey(s)));
            }

            private boolean inCorrectLocation(SSTableReader sstable)
            {
                if (!cfs.getPartitioner().splitter().isPresent())
                    return true;

                // Compare the expected data directory for the sstable with its current data directory
                Directories.DataDirectory currentDirectory = cfs.getDirectories().getDataDirectoryForFile(sstable.descriptor);
                return diskBoundaries.isInCorrectLocation(sstable, currentDirectory);
            }

            @Override
            public void execute(LifecycleTransaction txn)
            {
                logger.debug("Relocating {}", txn.originals());
                AbstractCompactionTask task = cfs.getCompactionStrategy().createCompactionTask(txn, NO_GC, Long.MAX_VALUE);
                task.setUserDefined(true);
                task.setCompactionType(OperationType.RELOCATE);
                task.execute(active);
            }
        }, jobs, OperationType.RELOCATE, TableOperation.StopTrigger.RELOCATE);
    }

    /**
     * Splits the given token ranges of the given sstables into a pending repair silo
     */
    public ListenableFuture<?> submitPendingAntiCompaction(ColumnFamilyStore cfs,
                                                           RangesAtEndpoint tokenRanges,
                                                           Refs<SSTableReader> sstables,
                                                           LifecycleTransaction txn,
                                                           UUID sessionId,
                                                           BooleanSupplier isCancelled)
    {
        Runnable runnable = new WrappedRunnable()
        {
            protected void runMayThrow() throws Exception
            {
                try (TableMetrics.TableTimer.Context ctx = cfs.metric.anticompactionTime.time())
                {
                    performAnticompaction(cfs, tokenRanges, sstables, txn, sessionId, isCancelled);
                }
            }
        };

        ListenableFuture<?> task = null;
        try
        {
            task = executor.submitIfRunning(runnable, "pending anticompaction");
            return task;
        }
        finally
        {
            if (task == null || task.isCancelled())
            {
                sstables.release();
                txn.abort();
            }
        }
    }

    /**
     * for sstables that are fully contained in the given ranges, just rewrite their metadata with
     * the pending repair id and remove them from the transaction
     */
    private static void mutateFullyContainedSSTables(ColumnFamilyStore cfs,
                                                     Refs<SSTableReader> refs,
                                                     Iterator<SSTableReader> sstableIterator,
                                                     Collection<Range<Token>> ranges,
                                                     LifecycleTransaction txn,
                                                     UUID sessionID,
                                                     boolean isTransient) throws IOException
    {
        if (ranges.isEmpty())
            return;

        List<Range<Token>> normalizedRanges = Range.normalize(ranges);

        Set<SSTableReader> fullyContainedSSTables = findSSTablesToAnticompact(sstableIterator, normalizedRanges, sessionID);

        cfs.metric.bytesMutatedAnticompaction.inc(CompactionSSTable.getTotalDataBytes(fullyContainedSSTables));
        cfs.mutateRepaired(fullyContainedSSTables, UNREPAIRED_SSTABLE, sessionID, isTransient);
        // since we're just re-writing the sstable metdata for the fully contained sstables, we don't want
        // them obsoleted when the anti-compaction is complete. So they're removed from the transaction here
        txn.cancel(fullyContainedSSTables);
        refs.release(fullyContainedSSTables);
    }

    /**
     * Make sure the {validatedForRepair} are marked for compaction before calling this.
     *
     * Caller must reference the validatedForRepair sstables (via ParentRepairSession.getActiveRepairedSSTableRefs(..)).
     *
     * @param cfs
     * @param replicas token ranges to be repaired
     * @param validatedForRepair SSTables containing the repaired ranges. Should be referenced before passing them.
     * @param sessionID the repair session we're anti-compacting for
     * @param isCancelled function that indicates if active anti-compaction should be canceled
     * @throws InterruptedException
     * @throws IOException
     */
    public void performAnticompaction(ColumnFamilyStore cfs,
                                      RangesAtEndpoint replicas,
                                      Refs<SSTableReader> validatedForRepair,
                                      LifecycleTransaction txn,
                                      UUID sessionID,
                                      BooleanSupplier isCancelled) throws IOException
    {
        try
        {
            ActiveRepairService.ParentRepairSession prs = ActiveRepairService.instance.getParentRepairSession(sessionID);
            Preconditions.checkArgument(!prs.isPreview(), "Cannot anticompact for previews");
            Preconditions.checkArgument(!replicas.isEmpty(), "No ranges to anti-compact");

            if (logger.isInfoEnabled())
                logger.info("{} Starting anticompaction for {}.{} on {}/{} sstables", PreviewKind.NONE.logPrefix(sessionID), cfs.keyspace.getName(), cfs.getTableName(), validatedForRepair.size(), cfs.getLiveSSTables().size());
            if (logger.isTraceEnabled())
                logger.trace("{} Starting anticompaction for ranges {}", PreviewKind.NONE.logPrefix(sessionID), replicas);

            Set<SSTableReader> sstables = new HashSet<>(validatedForRepair);
            validateSSTableBoundsForAnticompaction(sessionID, sstables, replicas);
            mutateFullyContainedSSTables(cfs, validatedForRepair, sstables.iterator(), replicas.onlyFull().ranges(), txn, sessionID, false);
            mutateFullyContainedSSTables(cfs, validatedForRepair, sstables.iterator(), replicas.onlyTransient().ranges(), txn, sessionID, true);

            assert txn.originals().equals(sstables);
            if (!sstables.isEmpty())
                doAntiCompaction(cfs, replicas, txn, sessionID, isCancelled);
            txn.finish();
        }
        finally
        {
            validatedForRepair.release();
            txn.close();
        }

        logger.info("{} Completed anticompaction successfully", PreviewKind.NONE.logPrefix(sessionID));
    }

    static void validateSSTableBoundsForAnticompaction(UUID sessionID,
                                                       Collection<SSTableReader> sstables,
                                                       RangesAtEndpoint ranges)
    {
        List<Range<Token>> normalizedRanges = Range.normalize(ranges.ranges());
        for (SSTableReader sstable : sstables)
        {
            Bounds<Token> bounds = new Bounds<>(sstable.first.getToken(), sstable.last.getToken());

            if (!Iterables.any(normalizedRanges, r -> (r.contains(bounds.left) && r.contains(bounds.right)) || r.intersects(bounds)))
            {
                // this should never happen - in PendingAntiCompaction#getSSTables we select all sstables that intersect the repaired ranges, that can't have changed here
                String message = String.format("%s SSTable %s (%s) does not intersect repaired ranges %s, this sstable should not have been included.",
                                               PreviewKind.NONE.logPrefix(sessionID), sstable, bounds, normalizedRanges);
                logger.error(message);
                throw new IllegalStateException(message);
            }
        }

    }

    @VisibleForTesting
    static Set<SSTableReader> findSSTablesToAnticompact(Iterator<SSTableReader> sstableIterator, List<Range<Token>> normalizedRanges, UUID parentRepairSession)
    {
        Set<SSTableReader> fullyContainedSSTables = new HashSet<>();
        while (sstableIterator.hasNext())
        {
            SSTableReader sstable = sstableIterator.next();

            Bounds<Token> sstableBounds = new Bounds<>(sstable.first.getToken(), sstable.last.getToken());

            for (Range<Token> r : normalizedRanges)
            {
                // ranges are normalized - no wrap around - if first and last are contained we know that all tokens are contained in the range
                if (r.contains(sstable.first.getToken()) && r.contains(sstable.last.getToken()))
                {
                    logger.info("{} SSTable {} fully contained in range {}, mutating repairedAt to unrepaired instead of anticompacting", PreviewKind.NONE.logPrefix(parentRepairSession), sstable, r);
                    fullyContainedSSTables.add(sstable);
                    sstableIterator.remove();
                    break;
                }
                else if (r.intersects(sstableBounds))
                {
                    logger.info("{} SSTable {} ({}) will be anticompacted on range {}", PreviewKind.NONE.logPrefix(parentRepairSession), sstable, sstableBounds, r);
                }
            }
        }
        return fullyContainedSSTables;
    }

    public void performMaximal(final ColumnFamilyStore cfStore, boolean splitOutput)
    {
        FBUtilities.waitOnFutures(submitMaximal(cfStore, getDefaultGcBefore(cfStore, FBUtilities.nowInSeconds()), splitOutput));
    }

    public void performMaximal(final ColumnFamilyStore cfStore, boolean splitOutput, int parallelism)
    {
        FBUtilities.waitOnFutures(submitMaximal(cfStore, getDefaultGcBefore(cfStore, FBUtilities.nowInSeconds()), splitOutput, parallelism, active));
    }

    public List<Future<?>> submitMaximal(final ColumnFamilyStore cfStore, final int gcBefore, boolean splitOutput)
    {
        return submitMaximal(cfStore, gcBefore, splitOutput, active);
    }

    public List<Future<?>> submitMaximal(final ColumnFamilyStore cfStore,
                                         final int gcBefore,
                                         boolean splitOutput,
                                         TableOperationObserver obs)
    {
        return submitMaximal(cfStore, gcBefore, splitOutput, -1, obs);
    }

    @VisibleForTesting
    @SuppressWarnings("resource") // the tasks are executed in parallel on the executor, making sure that they get closed
    public List<Future<?>> submitMaximal(final ColumnFamilyStore cfStore,
                                         final int gcBefore,
                                         boolean splitOutput,
                                         int permittedParallelism,
                                         TableOperationObserver obs)
    {
        // The default parallelism is half the number of compaction threads to leave enough room for other compactions.
        if (permittedParallelism < 0)
            permittedParallelism = getCoreCompactorThreads() / 2;
        else if (permittedParallelism == 0)
            permittedParallelism = Integer.MAX_VALUE;

        // here we compute the task off the compaction executor, so having that present doesn't
        // confuse runWithCompactionsDisabled -- i.e., we don't want to deadlock ourselves, waiting
        // for ourselves to finish/acknowledge cancellation before continuing.

        CompactionTasks tasks = cfStore.getCompactionStrategy().getMaximalTasks(gcBefore, splitOutput, permittedParallelism);

        if (tasks.isEmpty())
            return Collections.emptyList();

        List<Future<?>> futures = new ArrayList<>();

        int nonEmptyTasks = 0;
        for (final AbstractCompactionTask task : tasks)
        {
            if (task.transaction.originals().size() > 0)
                nonEmptyTasks++;

            Runnable runnable = new WrappedRunnable()
            {
                protected void runMayThrow()
                {
                    task.execute(obs);
                }
            };

            Future<?> fut = executor.submitIfRunning(runnable, "maximal task");
            if (!fut.isCancelled())
                futures.add(fut);
            else
            {
                Throwable error = task.rejected(new RejectedExecutionException("rejected by executor"));
                if (error != null)
                    futures.add(Futures.immediateFailedFuture(error));
            }
        }
        if (nonEmptyTasks > 1)
            logger.info("Major compaction of {}.{} will not result in a single sstable - " +
                        "repaired and unrepaired data is kept separate, compaction runs per data_file_directory, " +
                        "and some compaction strategies will construct multiple non-overlapping sstables.",
                        cfStore.getKeyspaceName(), cfStore.getTableName());

        return futures;
    }

    /**
     * Forces a major compaction of specified token ranges of the specified column family.
     * <p>
     * The token ranges will be interpreted as closed intervals to match the closed interval defined by the first and
     * last keys of a sstable, even though the {@link Range} class is suppossed to be half-open by definition.
     *
     * @param cfStore The column family store to be compacted.
     * @param ranges The token ranges to be compacted, interpreted as closed intervals.
     */
    public void forceCompactionForTokenRange(ColumnFamilyStore cfStore, Collection<Range<Token>> ranges)
    {
        Callable<CompactionTasks> taskCreator = () -> {
            Collection<SSTableReader> sstables = sstablesInBounds(cfStore, ranges);
            if (sstables == null || sstables.isEmpty())
            {
                logger.debug("No sstables found for the provided token range");
                return CompactionTasks.empty();
            }
            return cfStore.getCompactionStrategy().getUserDefinedTasks(sstables, getDefaultGcBefore(cfStore, FBUtilities.nowInSeconds()));
        };

        try (CompactionTasks tasks = cfStore.runWithCompactionsDisabled(taskCreator,
                                                                        (sstable) -> new Bounds<>(sstable.first.getToken(), sstable.last.getToken()).intersects(ranges),
                                                                        false,
                                                                        false,
                                                                        false,
                                                                        TableOperation.StopTrigger.COMPACTION))
        {
            if (tasks.isEmpty())
                return;

            Runnable runnable = new WrappedRunnable()
            {
                protected void runMayThrow()
                {
                    for (AbstractCompactionTask task : tasks)
                        if (task != null)
                            task.execute(active);
                }
            };

            FBUtilities.waitOnFuture(executor.submitIfRunning(runnable, "force compaction for token range"));
        }
    }

    /**
     * Returns the sstables of the specified column family store that intersect with the specified token ranges.
     * <p>
     * The token ranges will be interpreted as closed intervals to match the closed interval defined by the first and
     * last keys of a sstable, even though the {@link Range} class is suppossed to be half-open by definition.
     */
    private static Collection<SSTableReader> sstablesInBounds(ColumnFamilyStore cfs, Collection<Range<Token>> tokenRangeCollection)
    {
        final Set<SSTableReader> sstables = new HashSet<>();
        Iterable<SSTableReader> liveTables = cfs.getTracker().getView().select(SSTableSet.LIVE);
        SSTableIntervalTree tree = SSTableIntervalTree.build(liveTables);

        for (Range<Token> tokenRange : tokenRangeCollection)
        {
            if (!AbstractBounds.strictlyWrapsAround(tokenRange.left, tokenRange.right))
            {
                Iterable<SSTableReader> ssTableReaders = View.sstablesInBounds(tokenRange.left.minKeyBound(), tokenRange.right.maxKeyBound(), tree);
                Iterables.addAll(sstables, ssTableReaders);
            }
            else
            {
                // Searching an interval tree will not return the correct results for a wrapping range
                // so we have to unwrap it first
                for (Range<Token> unwrappedRange : tokenRange.unwrap())
                {
                    Iterable<SSTableReader> ssTableReaders = View.sstablesInBounds(unwrappedRange.left.minKeyBound(), unwrappedRange.right.maxKeyBound(), tree);
                    Iterables.addAll(sstables, ssTableReaders);
                }
            }
        }
        return sstables;
    }

    public void forceUserDefinedCompaction(String dataFiles)
    {
        String[] filenames = dataFiles.split(",");
        Multimap<ColumnFamilyStore, Descriptor> descriptors = ArrayListMultimap.create();

        for (String filename : filenames)
        {
            // extract keyspace and columnfamily name from filename
            Descriptor desc = Descriptor.fromFilename(filename.trim());
            if (Schema.instance.getTableMetadataRef(desc) == null)
            {
                logger.warn("Schema does not exist for file {}. Skipping.", filename);
                continue;
            }
            // group by keyspace/columnfamily
            ColumnFamilyStore cfs = Keyspace.open(desc.ksname).getColumnFamilyStore(desc.cfname);
            descriptors.put(cfs, cfs.getDirectories().find(new File(filename.trim()).name()));
        }

        List<Future<?>> futures = new ArrayList<>(descriptors.size());
        int nowInSec = FBUtilities.nowInSeconds();
        for (ColumnFamilyStore cfs : descriptors.keySet())
            futures.add(submitUserDefined(cfs, descriptors.get(cfs), getDefaultGcBefore(cfs, nowInSec)));
        FBUtilities.waitOnFutures(futures);
    }

    public void forceUserDefinedCleanup(String dataFiles)
    {
        String[] filenames = dataFiles.split(",");
        HashMap<ColumnFamilyStore, Descriptor> descriptors = Maps.newHashMap();

        for (String filename : filenames)
        {
            // extract keyspace and columnfamily name from filename
            Descriptor desc = Descriptor.fromFilename(filename.trim());
            if (Schema.instance.getTableMetadataRef(desc) == null)
            {
                logger.warn("Schema does not exist for file {}. Skipping.", filename);
                continue;
            }
            // group by keyspace/columnfamily
            ColumnFamilyStore cfs = Keyspace.open(desc.ksname).getColumnFamilyStore(desc.cfname);
            desc = cfs.getDirectories().find(new File(filename.trim()).name());
            if (desc != null)
                descriptors.put(cfs, desc);
        }

        if (!StorageService.instance.isJoined())
        {
            logger.error("Cleanup cannot run before a node has joined the ring");
            return;
        }

        for (Map.Entry<ColumnFamilyStore,Descriptor> entry : descriptors.entrySet())
        {
            ColumnFamilyStore cfs = entry.getKey();
            Keyspace keyspace = cfs.keyspace;
            final RangesAtEndpoint replicas = StorageService.instance.getLocalReplicas(keyspace.getName());
            final Set<Range<Token>> allRanges = replicas.ranges();
            final Set<Range<Token>> transientRanges = replicas.onlyTransient().ranges();
            boolean hasIndexes = cfs.indexManager.hasIndexes();
            SSTableReader sstable = lookupSSTable(cfs, entry.getValue());

            if (sstable == null)
            {
                logger.warn("Will not clean {}, it is not an active sstable", entry.getValue());
            }
            else
            {
                CleanupStrategy cleanupStrategy = CleanupStrategy.get(cfs, allRanges, transientRanges, sstable.isRepaired(), FBUtilities.nowInSeconds());
                try (LifecycleTransaction txn = cfs.getTracker().tryModify(sstable, OperationType.CLEANUP))
                {
                    doCleanupOne(cfs, txn, cleanupStrategy, allRanges, hasIndexes);
                }
                catch (IOException e)
                {
                    logger.error("forceUserDefinedCleanup failed: {}", e.getLocalizedMessage());
                }
            }
        }
    }


    public Future<?> submitUserDefined(final ColumnFamilyStore cfs, final Collection<Descriptor> dataFiles, final int gcBefore)
    {
        Runnable runnable = new WrappedRunnable()
        {
            protected void runMayThrow() throws Exception
            {
                // look up the sstables now that we're on the compaction executor, so we don't try to re-compact
                // something that was already being compacted earlier.
                Collection<SSTableReader> sstables = new ArrayList<>(dataFiles.size());
                for (Descriptor desc : dataFiles)
                {
                    // inefficient but not in a performance sensitive path
                    SSTableReader sstable = lookupSSTable(cfs, desc);
                    if (sstable == null)
                    {
                        logger.info("Will not compact {}: it is not an active sstable", desc);
                    }
                    else
                    {
                        sstables.add(sstable);
                    }
                }

                if (sstables.isEmpty())
                {
                    logger.info("No files to compact for user defined compaction");
                }
                else
                {
                    try (CompactionTasks tasks = cfs.getCompactionStrategy().getUserDefinedTasks(sstables, gcBefore))
                    {
                        for (AbstractCompactionTask task : tasks)
                        {
                            if (task != null)
                                task.execute(active);
                        }
                    }
                }
            }
        };

        return executor.submitIfRunning(runnable, "user defined task");
    }

    // This acquire a reference on the sstable
    // This is not efficient, do not use in any critical path
    private SSTableReader lookupSSTable(final ColumnFamilyStore cfs, Descriptor descriptor)
    {
        for (SSTableReader sstable : cfs.getSSTables(SSTableSet.CANONICAL))
        {
            if (sstable.descriptor.equals(descriptor))
                return sstable;
        }
        return null;
    }

    public Future<?> submitValidation(Callable<Object> validation)
    {
        return validationExecutor.submitIfRunning(validation, "validation");
    }

    /* Used in tests. */
    public void disableAutoCompaction()
    {
        for (String ksname : Schema.instance.getNonSystemKeyspaces().names())
        {
            for (ColumnFamilyStore cfs : Keyspace.open(ksname).getColumnFamilyStores())
                cfs.disableAutoCompaction();
        }
    }

    @VisibleForTesting
    void scrubOne(ColumnFamilyStore cfs, LifecycleTransaction modifier, boolean skipCorrupted, boolean checkData, boolean reinsertOverflowedTTL, TableOperationObserver activeCompactions)
    {
        try (Scrubber scrubber = new Scrubber(cfs, modifier, skipCorrupted, checkData, reinsertOverflowedTTL);
             NonThrowingCloseable c = activeCompactions.onOperationStart(scrubber.getScrubInfo()))
        {
            scrubber.scrub();
        }
    }

    @VisibleForTesting
    void verifyOne(ColumnFamilyStore cfs, SSTableReader sstable, Verifier.Options options, TableOperationObserver activeCompactions)
    {
        try (Verifier verifier = new Verifier(cfs, sstable, false, options);
             NonThrowingCloseable c = activeCompactions.onOperationStart(verifier.getVerifyInfo()))
        {
            verifier.verify();
        }
    }

    /**
     * Determines if a cleanup would actually remove any data in this SSTable based
     * on a set of owned ranges.
     */
    @VisibleForTesting
    public static boolean needsCleanup(SSTableReader sstable, Collection<Range<Token>> ownedRanges)
    {
        if (ownedRanges.isEmpty())
        {
            return true; // all data will be cleaned
        }

        // unwrap and sort the ranges by LHS token
        List<Range<Token>> sortedRanges = Range.normalize(ownedRanges);

        // see if there are any keys LTE the token for the start of the first range
        // (token range ownership is exclusive on the LHS.)
        Range<Token> firstRange = sortedRanges.get(0);
        if (sstable.first.getToken().compareTo(firstRange.left) <= 0)
            return true;

        // then, iterate over all owned ranges and see if the next key beyond the end of the owned
        // range falls before the start of the next range
        for (int i = 0; i < sortedRanges.size(); i++)
        {
            Range<Token> range = sortedRanges.get(i);
            if (range.right.isMinimum())
            {
                // we split a wrapping range and this is the second half.
                // there can't be any keys beyond this (and this is the last range)
                return false;
            }

            DecoratedKey firstBeyondRange = sstable.firstKeyBeyond(range.right.maxKeyBound());
            if (firstBeyondRange == null)
            {
                // we ran off the end of the sstable looking for the next key; we don't need to check any more ranges
                return false;
            }

            if (i == (sortedRanges.size() - 1))
            {
                // we're at the last range and we found a key beyond the end of the range
                return true;
            }

            Range<Token> nextRange = sortedRanges.get(i + 1);
            if (firstBeyondRange.getToken().compareTo(nextRange.left) <= 0)
            {
                // we found a key in between the owned ranges
                return true;
            }
        }

        return false;
    }

    /**
     * This function goes over a file and removes the keys that the node is not responsible for
     * and only keeps keys that this node is responsible for.
     *
     * @throws IOException
     */
    private void doCleanupOne(final ColumnFamilyStore cfs,
                              LifecycleTransaction txn,
                              CleanupStrategy cleanupStrategy,
                              Collection<Range<Token>> allRanges,
                              boolean hasIndexes) throws IOException
    {
        assert !cfs.isIndex();

        SSTableReader sstable = txn.onlyOne();

        // if ranges is empty and no index, entire sstable is discarded
        if (!hasIndexes && !new Bounds<>(sstable.first.getToken(), sstable.last.getToken()).intersects(allRanges))
        {
            txn.obsoleteOriginals();
            txn.finish();
            logger.info("SSTable {} ([{}, {}]) does not intersect the owned ranges ({}), dropping it", sstable, sstable.first.getToken(), sstable.last.getToken(), allRanges);
            return;
        }

        long start = System.nanoTime();

        long totalkeysWritten = 0;

        long expectedBloomFilterSize = Math.max(cfs.metadata().params.minIndexInterval,
                                               SSTableReader.getApproximateKeyCount(txn.originals()));
        if (logger.isTraceEnabled())
            logger.trace("Expected bloom filter size : {}", expectedBloomFilterSize);

        logger.info("Cleaning up {}", sstable);

        File compactionFileLocation = sstable.descriptor.directory;
        RateLimiter limiter = getRateLimiter();
        double compressionRatio = sstable.getCompressionRatio();
        if (compressionRatio == MetadataCollector.NO_COMPRESSION_RATIO)
            compressionRatio = 1.0;

        List<SSTableReader> finished;

        int nowInSec = FBUtilities.nowInSeconds();
        try (SSTableRewriter writer = SSTableRewriter.construct(cfs, txn, false, sstable.maxDataAge);
             ISSTableScanner scanner = cleanupStrategy.getScanner(sstable);
             CompactionController controller = new CompactionController(cfs, txn.originals(), getDefaultGcBefore(cfs, nowInSec));
             Refs<SSTableReader> refs = Refs.ref(singleton(sstable));
             CompactionIterator ci = new CompactionIterator(OperationType.CLEANUP, Collections.singletonList(scanner), controller, nowInSec, UUIDGen.getTimeUUID()))
        {
            StatsMetadata metadata = sstable.getSSTableMetadata();
            writer.switchWriter(createWriter(cfs, compactionFileLocation, expectedBloomFilterSize, metadata.repairedAt, metadata.pendingRepair, metadata.isTransient, sstable, txn));
            long lastBytesScanned = 0;

            while (ci.hasNext())
            {
                try (UnfilteredRowIterator partition = ci.next();
                     UnfilteredRowIterator notCleaned = cleanupStrategy.cleanup(partition))
                {
                    if (notCleaned == null)
                        continue;

                    if (writer.append(notCleaned))
                        totalkeysWritten++;

                    long bytesScanned = scanner.getBytesScanned();

                    if (compactionRateLimiterAcquire(limiter, bytesScanned, lastBytesScanned, compressionRatio))
                        lastBytesScanned = bytesScanned;
                }
            }

            // flush to ensure we don't lose the tombstones on a restart, since they are not commitlog'd
            cfs.indexManager.flushAllIndexesBlocking();

            finished = writer.finish();
        }

        if (!finished.isEmpty())
        {
            String format = "Cleaned up to %s.  %s to %s (~%d%% of original) for %,d keys.  Time: %,dms.";
            long dTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            long startsize = sstable.onDiskLength();
            long endsize = 0;
            for (SSTableReader newSstable : finished)
                endsize += newSstable.onDiskLength();
            double ratio = (double) endsize / (double) startsize;
            logger.info(String.format(format, finished.get(0).getFilename(), FBUtilities.prettyPrintMemory(startsize),
                                      FBUtilities.prettyPrintMemory(endsize), (int) (ratio * 100), totalkeysWritten, dTime));
        }

    }

    protected boolean compactionRateLimiterAcquire(RateLimiter limiter, long bytesScanned, long lastBytesScanned, double compressionRatio)
    {
        if (DatabaseDescriptor.getCompactionThroughputMbPerSec() == 0)
            return false;

        long lengthRead = (long) ((bytesScanned - lastBytesScanned) * compressionRatio) + 1;
        // Acquire at 128k granularity. At worst we'll exceed the limit a bit, but acquire is quite expensive.
        if (lengthRead < ACQUIRE_GRANULARITY)
            return false;

        return actuallyAcquire(limiter, lengthRead);
    }

    private boolean actuallyAcquire(RateLimiter limiter, long lengthRead)
    {
        metrics.bytesCompactedThroughput.mark(lengthRead);
        while (lengthRead >= Integer.MAX_VALUE)
        {
            limiter.acquire(Integer.MAX_VALUE);
            lengthRead -= Integer.MAX_VALUE;
        }
        if (lengthRead > 0)
        {
            limiter.acquire((int) lengthRead);
        }
        return true;
    }

    private static abstract class CleanupStrategy
    {
        protected final Collection<Range<Token>> ranges;
        protected final int nowInSec;

        protected CleanupStrategy(Collection<Range<Token>> ranges, int nowInSec)
        {
            this.ranges = ranges;
            this.nowInSec = nowInSec;
        }

        public static CleanupStrategy get(ColumnFamilyStore cfs, Collection<Range<Token>> ranges, Collection<Range<Token>> transientRanges, boolean isRepaired, int nowInSec)
        {
            if (cfs.indexManager.hasIndexes())
            {
                if (!transientRanges.isEmpty())
                {
                    //Shouldn't have been possible to create this situation
                    throw new AssertionError("Can't have indexes and transient ranges");
                }
                return new Full(cfs, ranges, nowInSec);
            }
            return new Bounded(cfs, ranges, transientRanges, isRepaired, nowInSec);
        }

        public abstract ISSTableScanner getScanner(SSTableReader sstable);
        public abstract UnfilteredRowIterator cleanup(UnfilteredRowIterator partition);

        private static final class Bounded extends CleanupStrategy
        {
            private final Collection<Range<Token>> transientRanges;
            private final boolean isRepaired;

            public Bounded(final ColumnFamilyStore cfs, Collection<Range<Token>> ranges, Collection<Range<Token>> transientRanges, boolean isRepaired, int nowInSec)
            {
                super(ranges, nowInSec);
                instance.cacheCleanupExecutor.submit(new Runnable()
                {
                    @Override
                    public void run()
                    {
                        cfs.cleanupCache();
                    }
                });
                this.transientRanges = transientRanges;
                this.isRepaired = isRepaired;
            }

            @Override
            public ISSTableScanner getScanner(SSTableReader sstable)
            {
                //If transient replication is enabled and there are transient ranges
                //then cleanup should remove any partitions that are repaired and in the transient range
                //as they should already be synchronized at other full replicas.
                //So just don't scan the portion of the table containing the repaired transient ranges
                Collection<Range<Token>> rangesToScan = ranges;
                if (isRepaired)
                {
                    rangesToScan = Collections2.filter(ranges, range -> !transientRanges.contains(range));
                }
                return sstable.getScanner(rangesToScan);
            }

            @Override
            public UnfilteredRowIterator cleanup(UnfilteredRowIterator partition)
            {
                return partition;
            }
        }

        private static final class Full extends CleanupStrategy
        {
            private final ColumnFamilyStore cfs;

            public Full(ColumnFamilyStore cfs, Collection<Range<Token>> ranges, int nowInSec)
            {
                super(ranges, nowInSec);
                this.cfs = cfs;
            }

            @Override
            public ISSTableScanner getScanner(SSTableReader sstable)
            {
                return sstable.getScanner();
            }

            @Override
            public UnfilteredRowIterator cleanup(UnfilteredRowIterator partition)
            {
                if (Range.isInRanges(partition.partitionKey().getToken(), ranges))
                    return partition;

                cfs.invalidateCachedPartition(partition.partitionKey());

                cfs.indexManager.deletePartition(partition, nowInSec);
                return null;
            }
        }
    }

    public static SSTableWriter createWriter(CompactionRealm cfs,
                                             File compactionFileLocation,
                                             long expectedBloomFilterSize,
                                             long repairedAt,
                                             UUID pendingRepair,
                                             boolean isTransient,
                                             SSTableReader sstable,
                                             LifecycleTransaction txn)
    {
        FileUtils.createDirectory(compactionFileLocation);

        return SSTableWriter.create(cfs.metadataRef(),
                                    cfs.newSSTableDescriptor(compactionFileLocation),
                                    expectedBloomFilterSize,
                                    repairedAt,
                                    pendingRepair,
                                    isTransient,
                                    sstable.getSSTableLevel(),
                                    sstable.header,
                                    cfs.getIndexManager().listIndexGroups(),
                                    txn);
    }

    public static SSTableWriter createWriterForAntiCompaction(ColumnFamilyStore cfs,
                                                              File compactionFileLocation,
                                                              int expectedBloomFilterSize,
                                                              long repairedAt,
                                                              UUID pendingRepair,
                                                              boolean isTransient,
                                                              Collection<SSTableReader> sstables,
                                                              ILifecycleTransaction txn)
    {
        FileUtils.createDirectory(compactionFileLocation);
        int minLevel = Integer.MAX_VALUE;
        // if all sstables have the same level, we can compact them together without creating overlap during anticompaction
        // note that we only anticompact from unrepaired sstables, which is not leveled, but we still keep original level
        // after first migration to be able to drop the sstables back in their original place in the repaired sstable manifest
        for (SSTableReader sstable : sstables)
        {
            if (minLevel == Integer.MAX_VALUE)
                minLevel = sstable.getSSTableLevel();

            if (minLevel != sstable.getSSTableLevel())
            {
                minLevel = 0;
                break;
            }
        }
        return SSTableWriter.create(cfs.newSSTableDescriptor(compactionFileLocation),
                                    (long) expectedBloomFilterSize,
                                    repairedAt,
                                    pendingRepair,
                                    isTransient,
                                    cfs.metadata,
                                    new MetadataCollector(sstables, cfs.metadata().comparator, minLevel),
                                    SerializationHeader.make(cfs.metadata(), sstables),
                                    cfs.indexManager.listIndexGroups(),
                                    txn);
    }

    /**
     * Splits up an sstable into two new sstables. The first of the new tables will store repaired ranges, the second
     * will store the non-repaired ranges. Once anticompation is completed, the original sstable is marked as compacted
     * and subsequently deleted.
     * @param cfs
     * @param txn a transaction over the repaired sstables to anticompact
     * @param ranges full and transient ranges to be placed into one of the new sstables. The repaired table will be tracked via
     *   the {@link StatsMetadata#pendingRepair} field.
     * @param pendingRepair the repair session we're anti-compacting for
     * @param isCancelled function that indicates if active anti-compaction should be canceled
     */
    private void doAntiCompaction(ColumnFamilyStore cfs,
                                  RangesAtEndpoint ranges,
                                  LifecycleTransaction txn,
                                  UUID pendingRepair,
                                  BooleanSupplier isCancelled)
    {
        int originalCount = txn.originals().size();
        logger.info("Performing anticompaction on {} sstables for {}", originalCount, pendingRepair);

        //Group SSTables
        Set<SSTableReader> sstables = txn.originals();

        // Repairs can take place on both unrepaired (incremental + full) and repaired (full) data.
        // Although anti-compaction could work on repaired sstables as well and would result in having more accurate
        // repairedAt values for these, we still avoid anti-compacting already repaired sstables, as we currently don't
        // make use of any actual repairedAt value and splitting up sstables just for that is not worth it at this point.
        Set<SSTableReader> unrepairedSSTables = sstables.stream().filter((s) -> !s.isRepaired()).collect(Collectors.toSet());
        cfs.metric.bytesAnticompacted.inc(CompactionSSTable.getTotalDataBytes(unrepairedSSTables));
        Collection<Collection<CompactionSSTable>> groupedSSTables = cfs.getCompactionStrategy().groupSSTablesForAntiCompaction(unrepairedSSTables);

        // iterate over sstables to check if the full / transient / unrepaired ranges intersect them.
        int antiCompactedSSTableCount = 0;
        for (Collection<CompactionSSTable> sstableGroup : groupedSSTables)
        {
            try (LifecycleTransaction groupTxn = txn.split(Collections2.transform(sstableGroup, SSTableReader.class::cast)))
            {
                int antiCompacted = antiCompactGroup(cfs, ranges, groupTxn, pendingRepair, isCancelled);
                antiCompactedSSTableCount += antiCompacted;
            }
        }
        String format = "Anticompaction completed successfully, anticompacted from {} to {} sstable(s) for {}.";
        logger.info(format, originalCount, antiCompactedSSTableCount, pendingRepair);
    }

    @VisibleForTesting
    int antiCompactGroup(ColumnFamilyStore cfs,
                         RangesAtEndpoint ranges,
                         LifecycleTransaction txn,
                         UUID pendingRepair,
                         BooleanSupplier isCancelled)
    {
        Preconditions.checkArgument(!ranges.isEmpty(), "need at least one full or transient range");
        long groupMaxDataAge = -1;

        for (Iterator<SSTableReader> i = txn.originals().iterator(); i.hasNext();)
        {
            SSTableReader sstable = i.next();
            if (groupMaxDataAge < sstable.maxDataAge)
                groupMaxDataAge = sstable.maxDataAge;
        }

        if (txn.originals().size() == 0)
        {
            logger.info("No valid anticompactions for this group, All sstables were compacted and are no longer available");
            return 0;
        }

        logger.info("Anticompacting {} in {}.{} for {}", txn.originals(), cfs.keyspace.getName(), cfs.getTableName(), pendingRepair);
        Set<SSTableReader> sstableAsSet = txn.originals();

        File destination = cfs.getDirectories().getWriteableLocationAsFile(cfs.getExpectedCompactedFileSize(sstableAsSet, OperationType.ANTICOMPACTION));
        int nowInSec = FBUtilities.nowInSeconds();
        RateLimiter limiter = getRateLimiter();

        /**
         * HACK WARNING
         *
         * We have multiple writers operating over the same Transaction, producing different sets of sstables that all
         * logically replace the transaction's originals.  The SSTableRewriter assumes it has exclusive control over
         * the transaction state, and this will lead to temporarily inconsistent sstable/tracker state if we do not
         * take special measures to avoid it.
         *
         * Specifically, if a number of rewriter have prepareToCommit() invoked in sequence, then two problematic things happen:
         *   1. The obsoleteOriginals() call of the first rewriter immediately remove the originals from the tracker, despite
         *      their having been only partially replaced.  To avoid this, we must either avoid obsoleteOriginals() or checkpoint()
         *   2. The LifecycleTransaction may only have prepareToCommit() invoked once, and this will checkpoint() also.
         *
         * Similarly commit() would finalise partially complete on-disk state.
         *
         * To avoid these problems, we introduce a SharedTxn that proxies all calls onto the underlying transaction
         * except prepareToCommit(), checkpoint(), obsoleteOriginals(), and commit().
         * We then invoke these methods directly once each of the rewriter has updated the transaction
         * with their share of replacements.
         *
         * Note that for the same essential reason we also explicitly disable early open.
         * By noop-ing checkpoint we avoid any of the problems with early open, but by continuing to explicitly
         * disable it we also prevent any of the extra associated work from being performed.
         */
        class SharedTxn extends WrappedLifecycleTransaction
        {
            public SharedTxn(ILifecycleTransaction delegate) { super(delegate); }
            public Throwable commit(Throwable accumulate) { return accumulate; }
            public void prepareToCommit() {}
            public void checkpoint() {}
            public void obsoleteOriginals() {}
            public void close() {}
        }

        CompactionStrategy strategy = cfs.getCompactionStrategy();
        try (SharedTxn sharedTxn = new SharedTxn(txn);
             SSTableRewriter fullWriter = SSTableRewriter.constructWithoutEarlyOpening(sharedTxn, false, groupMaxDataAge);
             SSTableRewriter transWriter = SSTableRewriter.constructWithoutEarlyOpening(sharedTxn, false, groupMaxDataAge);
             SSTableRewriter unrepairedWriter = SSTableRewriter.constructWithoutEarlyOpening(sharedTxn, false, groupMaxDataAge);

             ScannerList scanners = strategy.getScanners(txn.originals());
             CompactionController controller = new CompactionController(cfs, sstableAsSet, getDefaultGcBefore(cfs, nowInSec));
             CompactionIterator ci = getAntiCompactionIterator(scanners.scanners, controller, nowInSec, UUIDGen.getTimeUUID(), isCancelled))
        {
            TableOperation op = ci.getOperation();
            try (NonThrowingCloseable cls = active.onOperationStart(op))
            {
                int expectedBloomFilterSize = Math.max(cfs.metadata().params.minIndexInterval, (int)(SSTableReader.getApproximateKeyCount(sstableAsSet)));

                fullWriter.switchWriter(CompactionManager.createWriterForAntiCompaction(cfs, destination, expectedBloomFilterSize, UNREPAIRED_SSTABLE, pendingRepair, false, sstableAsSet, txn));
                transWriter.switchWriter(CompactionManager.createWriterForAntiCompaction(cfs, destination, expectedBloomFilterSize, UNREPAIRED_SSTABLE, pendingRepair, true, sstableAsSet, txn));
                unrepairedWriter.switchWriter(CompactionManager.createWriterForAntiCompaction(cfs, destination, expectedBloomFilterSize, UNREPAIRED_SSTABLE, NO_PENDING_REPAIR, false, sstableAsSet, txn));

                Predicate<Token> fullChecker = !ranges.onlyFull().isEmpty() ? new Range.OrderedRangeContainmentChecker(ranges.onlyFull().ranges()) : t -> false;
                Predicate<Token> transChecker = !ranges.onlyTransient().isEmpty() ? new Range.OrderedRangeContainmentChecker(ranges.onlyTransient().ranges()) : t -> false;
                double compressionRatio = scanners.getCompressionRatio();
                if (compressionRatio == MetadataCollector.NO_COMPRESSION_RATIO)
                    compressionRatio = 1.0;

                long lastBytesScanned = 0;
                while (ci.hasNext())
                {
                    try (UnfilteredRowIterator partition = ci.next())
                    {
                        Token token = partition.partitionKey().getToken();
                        // if this row is contained in the full or transient ranges, append it to the appropriate sstable
                        if (fullChecker.test(token))
                        {
                            fullWriter.append(partition);
                        }
                        else if (transChecker.test(token))
                        {
                            transWriter.append(partition);
                        }
                        else
                        {
                            // otherwise, append it to the unrepaired sstable
                            unrepairedWriter.append(partition);
                        }
                        long bytesScanned = scanners.getTotalBytesScanned();
                        if (compactionRateLimiterAcquire(limiter, bytesScanned, lastBytesScanned, compressionRatio))
                            lastBytesScanned = bytesScanned;
                    }
                }
            }

            fullWriter.prepareToCommit();
            transWriter.prepareToCommit();
            unrepairedWriter.prepareToCommit();
            txn.checkpoint();
            txn.obsoleteOriginals();
            txn.prepareToCommit();

            List<SSTableReader> fullSSTables = new ArrayList<>(fullWriter.finished());
            List<SSTableReader> transSSTables = new ArrayList<>(transWriter.finished());
            List<SSTableReader> unrepairedSSTables = new ArrayList<>(unrepairedWriter.finished());

            fullWriter.commit();
            transWriter.commit();
            unrepairedWriter.commit();
            txn.commit();
            logger.info("Anticompacted {} in {}.{} to full = {}, transient = {}, unrepaired = {} for {}",
                        sstableAsSet,
                        cfs.keyspace.getName(),
                        cfs.getTableName(),
                        fullSSTables,
                        transSSTables,
                        unrepairedSSTables,
                        pendingRepair);
            return fullSSTables.size() + transSSTables.size() + unrepairedSSTables.size();
        }
        catch (CompactionInterruptedException e)
        {
            if (isCancelled.getAsBoolean())
            {
                logger.info("Anticompaction has been canceled for session {}", pendingRepair);
                logger.trace(e.getMessage(), e);
            }
            else
            {
                logger.info("Anticompaction for session {} has been stopped by request.", pendingRepair);
            }
            throw e;
        }
        catch (Throwable e)
        {
            JVMStabilityInspector.inspectThrowable(e);
            logger.error("Error anticompacting " + txn + " for " + pendingRepair, e);
            throw e;
        }
    }

    @VisibleForTesting
    public static CompactionIterator getAntiCompactionIterator(List<ISSTableScanner> scanners, CompactionController controller, int nowInSec, UUID timeUUID, BooleanSupplier isCancelled)
    {
        return new CompactionIterator(OperationType.ANTICOMPACTION, scanners, controller, nowInSec, timeUUID) {
            @Override
            public TableOperation createOperation(CompactionProgress progress)
            {
                return getAntiCompactionOperation(super.createOperation(progress), isCancelled);
            }
        };
    }

    @VisibleForTesting
    public static TableOperation getAntiCompactionOperation(TableOperation compaction, BooleanSupplier isCancelled)
    {
        return new AbstractTableOperation()
        {
            @Override
            public boolean isGlobal()
            {
                return false;
            }

            @Override
            public Progress getProgress()
            {
                return compaction.getProgress();
            }

            @Override
            public void stop(StopTrigger trigger)
            {
                compaction.stop(trigger);
            }

            @Override
            public boolean isStopRequested()
            {
                return compaction.isStopRequested() || isCancelled.getAsBoolean();
            }

            @Override
            public StopTrigger trigger()
            {
                return compaction.trigger();
            }
        };
    }

    @VisibleForTesting
    ListenableFuture<?> submitIndexBuild(final SecondaryIndexBuilder builder, TableOperationObserver activeCompactions)
    {
        Runnable runnable = new Runnable()
        {
            public void run()
            {
                try (NonThrowingCloseable c = activeCompactions.onOperationStart(builder))
                {
                    builder.build();
                }
            }
        };

        return executor.submitIfRunning(runnable, "index build");
    }

    /**
     * Is not scheduled, because it is performing disjoint work from sstable compaction.
     */
    public ListenableFuture<?> submitIndexBuild(final SecondaryIndexBuilder builder)
    {
        return submitIndexBuild(builder, active);
    }

    public Future<?> submitCacheWrite(final AutoSavingCache.Writer writer)
    {
        return submitCacheWrite(writer, active);
    }

    Future<?> submitCacheWrite(final AutoSavingCache.Writer writer, TableOperationObserver activeCompactions)
    {
        Runnable runnable = new Runnable()
        {
            public void run()
            {
                if (!AutoSavingCache.flushInProgress.add(writer.cacheType()))
                {
                    logger.trace("Cache flushing was already in progress: skipping {}", writer.getProgress());
                    return;
                }
                try
                {
                    try (NonThrowingCloseable c = activeCompactions.onOperationStart(writer))
                    {
                        writer.saveCache();
                    }
                }
                finally
                {
                    AutoSavingCache.flushInProgress.remove(writer.cacheType());
                }
            }
        };

        return executor.submitIfRunning(runnable, "cache write");
    }

    public List<SSTableReader> runIndexSummaryRedistribution(IndexSummaryRedistribution redistribution) throws IOException
    {
        return runIndexSummaryRedistribution(redistribution, active);
    }

    @VisibleForTesting
    List<SSTableReader> runIndexSummaryRedistribution(IndexSummaryRedistribution redistribution, TableOperationObserver activeCompactions) throws IOException
    {
        try(Closeable c = activeCompactions.onOperationStart(redistribution))
        {
            return redistribution.redistributeSummaries();
        }
    }

    public static int getDefaultGcBefore(ColumnFamilyStore cfs, int nowInSec)
    {
        // 2ndary indexes have ExpiringColumns too, so we need to purge tombstones deleted before now. We do not need to
        // add any GcGrace however since 2ndary indexes are local to a node.
        return cfs.isIndex() ? nowInSec : cfs.gcBefore(nowInSec);
    }

    public ListenableFuture<Long> submitViewBuilder(final ViewBuilderTask task)
    {
        return submitViewBuilder(task, active);
    }

    @VisibleForTesting
    ListenableFuture<Long> submitViewBuilder(final ViewBuilderTask task, TableOperationObserver activeCompactions)
    {
        return viewBuildExecutor.submitIfRunning(() -> {
            try(Closeable c = activeCompactions.onOperationStart(task))
            {
                return task.call();
            }
        }, "view build");
    }

    public int getActiveCompactions()
    {
        return active.getTableOperations().size();
    }

    static class CompactionExecutor extends JMXEnabledThreadPoolExecutor
    {
        // We will use this because getActiveCount returns only approximate number and should be used for anything but
        // metering
        private final AtomicInteger exactRunningTasks = new AtomicInteger();

        protected CompactionExecutor(int minThreads, int maxThreads, String name, BlockingQueue<Runnable> queue)
        {
            super(minThreads, maxThreads, 60, TimeUnit.SECONDS, queue, new NamedThreadFactory(name, Thread.MIN_PRIORITY), "internal");
        }

        private CompactionExecutor(int threadCount, String name)
        {
            this(threadCount, threadCount, name, new LinkedBlockingQueue<Runnable>());
        }

        public CompactionExecutor()
        {
            this(Math.max(1, DatabaseDescriptor.getConcurrentCompactors()), "CompactionExecutor");
        }

        protected void beforeExecute(Thread t, Runnable r)
        {
            // can't set this in Thread factory, so we do it redundantly here
            isCompactionManager.set(true);
            super.beforeExecute(t, r);
            exactRunningTasks.incrementAndGet();
        }

        // modified from DebuggableThreadPoolExecutor so that CompactionInterruptedExceptions are not logged
        @Override
        public void afterExecute(Runnable r, Throwable t)
        {
            exactRunningTasks.decrementAndGet();

            DebuggableThreadPoolExecutor.maybeResetLocalSessionWrapper(r);

            if (t == null)
                t = DebuggableThreadPoolExecutor.extractThrowable(r);

            if (t != null)
            {
                if (t instanceof CompactionInterruptedException)
                {
                    logger.info(t.getMessage());
                    if (t.getSuppressed() != null && t.getSuppressed().length > 0)
                        logger.warn("Interruption of compaction encountered exceptions:", t);
                    else
                        logger.trace("Full interruption stack trace:", t);
                }
                else
                {
                    DebuggableThreadPoolExecutor.handleOrLog(t);
                }
            }

            // Snapshots cannot be deleted on Windows while segments of the root element are mapped in NTFS. Compactions
            // unmap those segments which could free up a snapshot for successful deletion.
            SnapshotDeletingTask.rescheduleFailedTasks();
        }

        @Override
        public int getActiveTaskCount()
        {
            return exactRunningTasks.get();
        }

        public ListenableFuture<?> submitIfRunning(Runnable task, String name)
        {
            return submitIfRunning(Executors.callable(task, null), name);
        }

        /**
         * Submit the task but only if the executor has not been shutdown.If the executor has
         * been shutdown, or in case of a rejected execution exception return a cancelled future.
         *
         * @param task - the task to submit
         * @param name - the task name to use in log messages
         *
         * @return the future that will deliver the task result, or a future that has already been
         *         cancelled if the task could not be submitted.
         */
        public <T> ListenableFuture<T> submitIfRunning(Callable<T> task, String name)
        {
            if (isShutdown())
            {
                logger.info("Executor has been shut down, not submitting {}", name);
                return Futures.immediateCancelledFuture();
            }

            try
            {
                ListenableFutureTask<T> ret = ListenableFutureTask.create(task);
                execute(ret);
                return ret;
            }
            catch (RejectedExecutionException ex)
            {
                if (isShutdown())
                    logger.info("Executor has shut down, could not submit {}", name);
                else
                    logger.error("Failed to submit {}", name, ex);

                return Futures.immediateCancelledFuture();
            }
        }
    }

    // TODO: pull out relevant parts of CompactionExecutor and move to ValidationManager
    public static class ValidationExecutor extends CompactionExecutor
    {
        // CompactionExecutor, and by extension ValidationExecutor, use DebuggableThreadPoolExecutor's
        // default RejectedExecutionHandler which blocks the submitting thread when the work queue is
        // full. The calling thread in this case is AntiEntropyStage, so in most cases we don't actually
        // want to block when the ValidationExecutor is saturated as this prevents progress on all
        // repair tasks and may cause repair sessions to time out. Also, it can lead to references to
        // heavyweight validation responses containing merkle trees being held for extended periods which
        // increases GC pressure. Using LinkedBlockingQueue instead of the default SynchronousQueue allows
        // tasks to be submitted without blocking the caller, but will always prefer queueing to creating
        // new threads if the pool already has at least `corePoolSize` threads already running. For this
        // reason we set corePoolSize to the maximum desired concurrency, but allow idle core threads to
        // be terminated.

        public ValidationExecutor()
        {
            super(DatabaseDescriptor.getConcurrentValidations(),
                  DatabaseDescriptor.getConcurrentValidations(),
                  "ValidationExecutor",
                  new LinkedBlockingQueue());

            allowCoreThreadTimeOut(true);
        }

        public void adjustPoolSize()
        {
            setMaximumPoolSize(DatabaseDescriptor.getConcurrentValidations());
            setCorePoolSize(DatabaseDescriptor.getConcurrentValidations());
        }
    }

    private static class ViewBuildExecutor extends CompactionExecutor
    {
        public ViewBuildExecutor()
        {
            super(DatabaseDescriptor.getConcurrentViewBuilders(), "ViewBuildExecutor");
        }
    }

    private static class CacheCleanupExecutor extends CompactionExecutor
    {
        public CacheCleanupExecutor()
        {
            super(1, "CacheCleanupExecutor");
        }
    }

    public void incrementAborted()
    {
        metrics.compactionsAborted.inc();
    }

    public void incrementFailed()
    {
        metrics.totalCompactionsFailed.inc();
    }

    public void incrementCompactionsReduced()
    {
        metrics.compactionsReduced.inc();
    }

    public void incrementSstablesDropppedFromCompactions(long num)
    {
        metrics.sstablesDropppedFromCompactions.inc(num);
    }

    public void incrementRemovedExpiredSSTables(long num)
    {
        metrics.removedExpiredSSTables.mark(num);
    }

    public void incrementDeleteOnlyCompactions()
    {
        metrics.deleteOnlyCompactions.mark();
    }


    public List<Map<String, String>> getCompactions()
    {
        List<TableOperation> operationSources = active.getTableOperations();
        List<Map<String, String>> out = new ArrayList<Map<String, String>>(operationSources.size());
        for (TableOperation op : operationSources)
            out.add(op.getProgress().asMap());
        return out;
    }

    public List<String> getCompactionSummary()
    {
        List<TableOperation> operationSources = active.getTableOperations();
        List<String> out = new ArrayList<String>(operationSources.size());
        for (TableOperation ci : operationSources)
            out.add(ci.getProgress().toString());
        return out;
    }

    public TabularData getCompactionHistory()
    {
        try
        {
            return SystemKeyspace.getCompactionHistory();
        }
        catch (OpenDataException e)
        {
            throw new RuntimeException(e);
        }
    }

    public long getTotalBytesCompacted()
    {
        return metrics.bytesCompacted.getCount();
    }

    public long getTotalCompactionsCompleted()
    {
        return metrics.totalCompactionsCompleted.getCount();
    }

    public int getPendingTasks()
    {
        return metrics.pendingTasks.getValue();
    }

    public long getCompletedTasks()
    {
        return metrics.completedTasks.getValue();
    }

    public void stopCompaction(String type)
    {
        OperationType operation = OperationType.valueOf(type);
        for (TableOperation operationSource : active.getTableOperations())
        {
            if (operationSource.getProgress().operationType() == operation)
                operationSource.stop(TableOperation.StopTrigger.USER_STOP);
        }
    }

    public void stopCompactionById(String compactionId)
    {
        for (TableOperation operationSource : active.getTableOperations())
        {
            UUID holderId = operationSource.getProgress().operationId();
            if (holderId != null && holderId.equals(UUID.fromString(compactionId)))
                operationSource.stop(TableOperation.StopTrigger.USER_STOP);
        }
    }

    public void setConcurrentCompactors(int value)
    {
        if (value > executor.getCorePoolSize())
        {
            // we are increasing the value
            executor.setMaximumPoolSize(value);
            executor.setCorePoolSize(value);
        }
        else if (value < executor.getCorePoolSize())
        {
            // we are reducing the value
            executor.setCorePoolSize(value);
            executor.setMaximumPoolSize(value);
        }
    }

    public void setConcurrentValidations()
    {
        validationExecutor.adjustPoolSize();
    }

    public void setConcurrentViewBuilders(int value)
    {
        if (value > viewBuildExecutor.getCorePoolSize())
        {
            // we are increasing the value
            viewBuildExecutor.setMaximumPoolSize(value);
            viewBuildExecutor.setCorePoolSize(value);
        }
        else if (value < viewBuildExecutor.getCorePoolSize())
        {
            // we are reducing the value
            viewBuildExecutor.setCorePoolSize(value);
            viewBuildExecutor.setMaximumPoolSize(value);
        }
    }

    public int getCoreCompactorThreads()
    {
        return executor.getCorePoolSize();
    }

    public void setCoreCompactorThreads(int number)
    {
        executor.setCorePoolSize(number);
    }

    public int getMaximumCompactorThreads()
    {
        return executor.getMaximumPoolSize();
    }

    public void setMaximumCompactorThreads(int number)
    {
        executor.setMaximumPoolSize(number);
    }

    public int getCoreValidationThreads()
    {
        return validationExecutor.getCorePoolSize();
    }

    public void setCoreValidationThreads(int number)
    {
        validationExecutor.setCorePoolSize(number);
    }

    public int getMaximumValidatorThreads()
    {
        return validationExecutor.getMaximumPoolSize();
    }

    public void setMaximumValidatorThreads(int number)
    {
        validationExecutor.setMaximumPoolSize(number);
    }

    public boolean getDisableSTCSInL0()
    {
        return DatabaseDescriptor.getDisableSTCSInL0();
    }

    public void setDisableSTCSInL0(boolean disabled)
    {
        if (disabled != DatabaseDescriptor.getDisableSTCSInL0())
            logger.info("Changing STCS in L0 disabled from {} to {}", DatabaseDescriptor.getDisableSTCSInL0(), disabled);
        DatabaseDescriptor.setDisableSTCSInL0(disabled);
    }

    public int getCoreViewBuildThreads()
    {
        return viewBuildExecutor.getCorePoolSize();
    }

    public void setCoreViewBuildThreads(int number)
    {
        viewBuildExecutor.setCorePoolSize(number);
    }

    public int getMaximumViewBuildThreads()
    {
        return viewBuildExecutor.getMaximumPoolSize();
    }

    public void setMaximumViewBuildThreads(int number)
    {
        viewBuildExecutor.setMaximumPoolSize(number);
    }

    public boolean getAutomaticSSTableUpgradeEnabled()
    {
        return DatabaseDescriptor.automaticSSTableUpgrade();
    }

    public void setAutomaticSSTableUpgradeEnabled(boolean enabled)
    {
        DatabaseDescriptor.setAutomaticSSTableUpgradeEnabled(enabled);
    }

    public int getMaxConcurrentAutoUpgradeTasks()
    {
        return DatabaseDescriptor.maxConcurrentAutoUpgradeTasks();
    }

    public void setMaxConcurrentAutoUpgradeTasks(int value)
    {
        try
        {
            DatabaseDescriptor.setMaxConcurrentAutoUpgradeTasks(value);
        }
        catch (ConfigurationException e)
        {
            throw new RuntimeException(e.getMessage());
        }
    }

    /**
     * Try to stop all of the compactions for given tables.
     *
     * Note that this method does not wait for all compactions to finish; you'll need to loop against
     * isCompacting if you want that behavior.
     *
     * @param tables The tables to try to stop compaction upon.
     * @param opPredicate Predicate to define which compaction operation to stop, based on its type.
     * @param readerPredicate Predicate to define which compaction to stop based on candidate sstables.
     * @param waitForInterruption whether to wait until interrupted compaction has fully stopped
     *
     * @return True if any compaction has been interrupted false otherwise.
     */
    public boolean interruptCompactionFor(Iterable<TableMetadata> tables, Predicate<OperationType> opPredicate, Predicate<SSTableReader> readerPredicate,
                                          boolean waitForInterruption, TableOperation.StopTrigger trigger)
    {
        assert tables != null;

        // interrupt in-progress compactions
        Set<TableOperation> interrupted = new HashSet<>();
        for (TableOperation operationSource : active.getTableOperations())
        {
            TableOperation.Progress info = operationSource.getProgress();

            if (Iterables.contains(tables, info.metadata()) && opPredicate.test(info.operationType()))
            {
                operationSource.stop(trigger);
                interrupted.add(operationSource);
            }
        }

        if (waitForInterruption)
        {
            // wait at most 2 minutes
            long start = System.nanoTime();
            long wait = TimeUnit.MINUTES.toNanos(2);

            for (TableOperation operation : interrupted)
            {
                while (active.isActive(operation) && System.nanoTime() - start < wait)
                    Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);

                if (active.isActive(operation))
                    throw new RuntimeException(String.format("Compaction task (%s) didn't finish within 2 minutes", operation.getProgress()));
            }
        }

        return !interrupted.isEmpty();
    }

    /**
     * Try to stop all of the compactions for given ColumnFamilies.
     *
     * Note that this method does not wait for all compactions to finish; you'll need to loop against
     * isCompacting if you want that behavior.
     *
     * @param columnFamilies The ColumnFamilies to try to stop compaction upon.
     * @param sstablePredicate the sstable predicate to match on
     * @param interruptValidation true if validation operations for repair should also be interrupted
     * @return True if any compaction has been interrupted false otherwise.
     */
    public boolean interruptCompactionFor(Iterable<TableMetadata> columnFamilies,
                                          Predicate<SSTableReader> sstablePredicate,
                                          boolean interruptValidation,
                                          TableOperation.StopTrigger trigger)
    {
        assert columnFamilies != null;

        // interrupt in-progress compactions
        boolean interrupted = false;
        for (TableOperation operationSource : active.getTableOperations())
        {
            TableOperation.Progress info = operationSource.getProgress();
            if ((info.operationType() == OperationType.VALIDATION) && !interruptValidation)
                continue;

            if (info.metadata() == null || Iterables.contains(columnFamilies, info.metadata()))
            {
                if (operationSource.shouldStop(sstablePredicate))
                {
                    operationSource.stop(trigger);
                    interrupted = true;
                }
            }
        }
        return interrupted;
    }

    public boolean interruptCompactionFor(Iterable<TableMetadata> tables, TableOperation.StopTrigger trigger)
    {
        return interruptCompactionFor(tables, Predicates.alwaysTrue(), true, trigger);
    }

    public void interruptCompactionForCFs(Iterable<ColumnFamilyStore> cfss,
                                          Predicate<SSTableReader> sstablePredicate,
                                          boolean interruptValidation,
                                          TableOperation.StopTrigger trigger)
    {
        List<TableMetadata> metadata = new ArrayList<>();
        for (ColumnFamilyStore cfs : cfss)
            metadata.add(cfs.metadata());

        interruptCompactionFor(metadata, sstablePredicate, interruptValidation, trigger);
    }

    public void waitForCessation(Iterable<ColumnFamilyStore> cfss, Predicate<SSTableReader> sstablePredicate)
    {
        long start = System.nanoTime();
        long delay = TimeUnit.MINUTES.toNanos(1);

        while (System.nanoTime() - start < delay)
        {
            if (CompactionManager.instance.isCompacting(cfss, sstablePredicate))
                Uninterruptibles.sleepUninterruptibly(1, TimeUnit.MILLISECONDS);
            else
                break;
        }
    }


    public List<TableOperation.Progress> getSSTableTasks()
    {
        return active.getTableOperations()
                     .stream()
                     .map(TableOperation::getProgress)
                     .filter(progress -> progress.operationType() != OperationType.COUNTER_CACHE_SAVE
                                     && progress.operationType() != OperationType.KEY_CACHE_SAVE
                                     && progress.operationType() != OperationType.ROW_CACHE_SAVE)
                     .collect(Collectors.toList());
    }

    /**
     * Return whether "global" compactions should be paused, used by ColumnFamilyStore#runWithCompactionsDisabled
     *
     * a global compaction is one that includes several/all tables, currently only IndexSummaryBuilder
     */
    public boolean isGlobalCompactionPaused()
    {
        return globalCompactionPauseCount.get() > 0;
    }

    public CompactionPauser pauseGlobalCompaction()
    {
        CompactionPauser pauser = globalCompactionPauseCount::decrementAndGet;
        globalCompactionPauseCount.incrementAndGet();
        return pauser;
    }

    public interface CompactionPauser extends AutoCloseable
    {
        public void close();
    }
}
