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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.RateLimiter;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.compaction.unified.UnifiedCompactionTask;
import org.apache.cassandra.db.compaction.writers.CompactionAwareWriter;
import org.apache.cassandra.db.compaction.writers.DefaultCompactionWriter;
import org.apache.cassandra.db.lifecycle.ILifecycleTransaction;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.sstable.ScannerList;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.concurrent.Refs;

import static org.apache.cassandra.config.CassandraRelevantProperties.COMPACTION_HISTORY_ENABLED;
import static org.apache.cassandra.config.CassandraRelevantProperties.CURSORS_ENABLED;
import static org.apache.cassandra.db.compaction.CompactionManager.compactionRateLimiterAcquire;
import static org.apache.cassandra.utils.FBUtilities.prettyPrintMemory;
import static org.apache.cassandra.utils.FBUtilities.prettyPrintMemoryPerSecond;

public class CompactionTask extends AbstractCompactionTask
{
    protected static final Logger logger = LoggerFactory.getLogger(CompactionTask.class);

    protected final int gcBefore;
    protected final boolean keepOriginals;
    /** for trace logging purposes only */
    private static final AtomicLong totalBytesCompacted = new AtomicLong();

    // The compaction strategy is not necessarily available for all compaction tasks (e.g. GC or sstable splitting)
    @Nullable
    private final CompactionStrategy strategy;

    public CompactionTask(CompactionRealm realm,
                          ILifecycleTransaction txn,
                          int gcBefore,
                          boolean keepOriginals,
                          @Nullable CompactionStrategy strategy)
    {
        this(realm, txn, gcBefore, keepOriginals, strategy, strategy);
    }

    public CompactionTask(CompactionRealm realm,
                          ILifecycleTransaction txn,
                          int gcBefore,
                          boolean keepOriginals,
                          @Nullable CompactionStrategy strategy,
                          CompactionObserver observer)
    {
        super(realm, txn);
        this.gcBefore = gcBefore;
        this.keepOriginals = keepOriginals;
        this.strategy = strategy;

        if (observer != null)
            addObserver(observer);

        logger.debug("Created compaction task with id {} and strategy {}", txn.opIdString(), strategy);
    }

    /**
     * Create a compaction task for deleted data collection.
     */
    public static AbstractCompactionTask forGarbageCollection(CompactionRealm realm, ILifecycleTransaction txn, int gcBefore, CompactionParams.TombstoneOption tombstoneOption)
    {
        AbstractCompactionTask task = new CompactionTask(realm, txn, gcBefore, false, null)
        {
            @Override
            protected CompactionController getCompactionController(Set<SSTableReader> toCompact)
            {
                return new CompactionController(realm, toCompact, gcBefore, null, tombstoneOption);
            }

            @Override
            protected int getLevel()
            {
                return txn.onlyOne().getSSTableLevel();
            }
        };
        task.setUserDefined(true);
        task.setCompactionType(OperationType.GARBAGE_COLLECT);
        return task;
    }

    private static long addToTotalBytesCompacted(long bytesCompacted)
    {
        return totalBytesCompacted.addAndGet(bytesCompacted);
    }

    /*
     *  Find the maximum size file in the list .
     */
    private SSTableReader getMaxSizeFile(Iterable<SSTableReader> sstables)
    {
        long maxSize = 0L;
        SSTableReader maxFile = null;
        for (SSTableReader sstable : sstables)
        {
            if (sstable.onDiskLength() > maxSize)
            {
                maxSize = sstable.onDiskLength();
                maxFile = sstable;
            }
        }
        return maxFile;
    }

    @VisibleForTesting
    public boolean reduceScopeForLimitedSpace(Set<SSTableReader> nonExpiredSSTables, long expectedSize)
    {
        if (partialCompactionsAcceptable() && nonExpiredSSTables.size() > 1)
        {
            // Try again w/o the largest one.
            SSTableReader removedSSTable = getMaxSizeFile(nonExpiredSSTables);
            logger.warn("insufficient space to compact all requested files. {}MB required, {} for compaction {} - removing largest SSTable: {}",
                        (float) expectedSize / 1024 / 1024,
                        StringUtils.join(nonExpiredSSTables, ", "),
                        transaction.opIdString(),
                        removedSSTable);
            // Note that we have removed files that are still marked as compacting.
            // This suboptimal but ok since the caller will unmark all the sstables at the end.
            transaction.cancel(removedSSTable);
            nonExpiredSSTables.remove(removedSSTable);
            return true;
        }
        return false;
    }

    /**
     * For internal use and testing only.  The rest of the system should go through the submit* methods,
     * which are properly serialized.
     * Caller is in charge of marking/unmarking the sstables as compacting.
     */
    @Override
    protected void runMayThrow() throws Exception
    {
        // The collection of sstables passed may be empty (but not null); even if
        // it is not empty, it may compact down to nothing if all rows are deleted.
        assert transaction != null;

        if (inputSSTables().isEmpty())
            return;

        if (DatabaseDescriptor.isSnapshotBeforeCompaction())
            realm.snapshotWithoutMemtable(System.currentTimeMillis() + "-compact-" + realm.getTableName());

        // The set of sstables given here may be later modified by buildCompactionCandidatesForAvailableDiskSpace() and
        // the compaction iterators in CompactionController and OverlapTracker will reflect the updated set of sstables.
        try (CompactionController controller = getCompactionController(inputSSTables());
             CompactionOperation operation = createCompactionOperation(controller, strategy))
        {
            operation.execute();
        }
    }

    /**
     * @return The token range that the operation should compact. This is usually null, but if we have a parallelizable
     * multi-task operation (see {@link UnifiedCompactionStrategy#createAndAddTasks}), it will specify a subrange.
     */
    protected Range<Token> tokenRange()
    {
        return null;
    }

    /**
     * If this is a partial compaction, its progress reports are shared between tasks. This method returns the shared
     * progress object.
     */
    protected SharedCompactionProgress sharedProgress()
    {
        return null;
    }

    /**
     * @return The set of input sstables for this compaction. This must be a subset of the transaction originals and
     * must reflect any removal of sstables from the originals set for correct overlap tracking.
     * See {@link UnifiedCompactionTask} for an example.
     */
    protected Set<SSTableReader> inputSSTables()
    {
        return transaction.originals();
    }

    /**
     * @return True if the task should try to limit the operation size to the available space by removing sstables from
     * the compacting set. This cannot be done if this is part of a multi-task operation with a shared transaction.
     */
    protected boolean shouldReduceScopeForSpace()
    {
        return true;
    }

    private CompactionOperation createCompactionOperation(CompactionController controller, CompactionStrategy strategy)
    {
        Set<CompactionSSTable> fullyExpiredSSTables = controller.getFullyExpiredSSTables();
        Set<SSTableReader> actuallyCompact = new HashSet<>(inputSSTables());
        actuallyCompact.removeAll(fullyExpiredSSTables);
        // select SSTables to compact based on available disk space.
        if (shouldReduceScopeForSpace() && !buildCompactionCandidatesForAvailableDiskSpace(actuallyCompact, !fullyExpiredSSTables.isEmpty()))
        {
            // The set of sstables has changed (one or more were excluded due to limited available disk space).
            // We need to recompute the overlaps between sstables. The iterators used in the compaction controller
            // and tracker will reflect the changed set of sstables made by LifecycleTransaction.cancel(),
            // so refreshing the overlaps will be based on the updated set of sstables.
            controller.refreshOverlaps();
        }

        // sanity check: sstables to compact is a subset of the transaction originals
        assert transaction.originals().containsAll(actuallyCompact);
        // sanity check: all sstables must belong to the same table
        assert !Iterables.any(transaction.originals(), sstable -> !sstable.descriptor.cfname.equals(realm.getTableName()));


        // Cursors currently don't support:
        boolean compactByIterators = !CURSORS_ENABLED.getBoolean()
                                     ||strategy != null && !strategy.supportsCursorCompaction()  // strategy does not support it
                                     || controller.shouldProvideTombstoneSources()  // garbagecollect
                                     || realm.getIndexManager().hasIndexes()  // indexes
                                     || realm.metadata().enforceStrictLiveness();   // strict liveness

        logger.debug("Compacting in {} by {}: {} {} {} {} {}",
                     realm.toString(),
                     compactByIterators ? "iterators" : "cursors",
                     CURSORS_ENABLED.getBoolean() ? "" : "cursors disabled",
                     strategy == null ? "no table compaction strategy"
                                      : !strategy.supportsCursorCompaction() ? "no cursor support"
                                                                             : "",
                     controller.shouldProvideTombstoneSources() ? "tombstone sources" : "",
                     realm.getIndexManager().hasIndexes() ? "has indexes" : "",
                     realm.metadata().enforceStrictLiveness() ? "strict liveness" : "");

        if (compactByIterators)
            return new CompactionOperationIterator(controller, actuallyCompact, fullyExpiredSSTables.size());
        else
            return new CompactionOperationCursor(controller, actuallyCompact, fullyExpiredSSTables.size());
    }

    /**
     *  The compaction operation is a special case of an {@link AbstractTableOperation} and takes care of executing the
     *  actual compaction and releasing any resources when the compaction is finished.
     *  <p/>
     *  This class also extends {@link AbstractTableOperation} for reporting compaction-specific progress information.
     */
    public abstract class CompactionOperation implements AutoCloseable, CompactionProgress
    {
        final CompactionController controller;
        final UUID taskId;
        final String taskIdString;
        final RateLimiter limiter;
        private final long startNanos;
        private final long startTime;
        final Set<SSTableReader> actuallyCompact;
        private final int fullyExpiredSSTablesCount;
        private final long inputDiskSize;

        // resources that are updated and may be read by another thread
        volatile Collection<SSTableReader> newSStables;
        volatile long totalKeysWritten;
        volatile long estimatedKeys;

        // resources that are updated but only read by this thread
        boolean completed;
        long lastCheckObsoletion;

        // resources that need closing
        Refs<SSTableReader> sstableRefs;
        TableOperation op;
        Closeable obsCloseable;
        CompactionAwareWriter writer;

        /**
         * Create a new compaction operation.
         * <p/>
         *
         * @param controller the compaction controller is needed by the scanners and compaction iterator to manage options
         * @param actuallyCompact the set of sstables to compact (excludes any fully expired ones)
         * @param fullyExpiredSSTablesCount the number of fully expired sstables (used in metrics)
         */
        private CompactionOperation(CompactionController controller, Set<SSTableReader> actuallyCompact, int fullyExpiredSSTablesCount)
        {
            this.controller = controller;
            this.actuallyCompact = actuallyCompact;
            this.taskId = transaction.opId();
            this.taskIdString = transaction.opIdString();

            this.limiter = CompactionManager.instance.getRateLimiter();
            this.startNanos = System.nanoTime();
            this.startTime = System.currentTimeMillis();
            this.newSStables = Collections.emptyList();
            this.fullyExpiredSSTablesCount = fullyExpiredSSTablesCount;
            this.totalKeysWritten = 0;
            this.estimatedKeys = 0;
            this.completed = false;

            Directories dirs = getDirectories();

            try
            {
                // resources that need closing, must be created last in case of exceptions and released if there is an exception in the c.tor
                this.sstableRefs = Refs.ref(actuallyCompact);
                this.inputDiskSize = getTotalOnDiskBytes(actuallyCompact, tokenRange());
                this.op = initializeSource(tokenRange());
                this.writer = getCompactionAwareWriter(realm, dirs, actuallyCompact);
                this.obsCloseable = opObserver.onOperationStart(op);
                CompactionProgress progress = this;
                var sharedProgress = sharedProgress();
                if (sharedProgress != null)
                {
                    sharedProgress.addSubtask(this);
                    progress = sharedProgress;
                }

                for (var obs : getCompObservers())
                    obs.onInProgress(progress);
            }
            catch (Throwable t)
            {
                close(t);
                throw new AssertionError(t); // unreachable (close will throw when t is not null). Added for static analysis.
            }
        }

        private long getTotalOnDiskBytes(Set<SSTableReader> actuallyCompact, Range<Token> tokenRange)
        {
            if (tokenRange == null)
                return CompactionSSTable.getTotalBytes(actuallyCompact);

            var rangeList = ImmutableList.of(tokenRange);
            long total = 0;
            for (SSTableReader rdr : actuallyCompact)
                total += rdr.onDiskSizeForPartitionPositions(rdr.getPositionsForRanges(rangeList));
            return total;
        }

        abstract TableOperation initializeSource(Range<Token> tokenRange) throws Throwable;

        private void execute()
        {
            try
            {
                // new sstables from flush can be added during a compaction, but only the compaction can remove them,
                // so in our single-threaded compaction world this is a valid way of determining if we're compacting
                // all the sstables (that existed when we started)
                if (logger.isDebugEnabled())
                {
                    debugLogCompactingMessage(taskIdString);
                }

                if (!controller.realm.isCompactionActive())
                    throw new CompactionInterruptedException(op.getProgress(), op.trigger());

                estimatedKeys = writer.estimatedKeys();

                execute0();

                // point of no return
                newSStables = writer.finish();

                completed = true;
            }
            catch (Throwable t)
            {
                Throwables.maybeFail(onError(t));
            }
        }

        private Throwable onError(Throwable e)
        {
            if (e instanceof AssertionError)
            {
                // Add additional information to help operators.
                AssertionError error = new AssertionError(
                String.format("Illegal input has been generated, most probably due to corruption in the input sstables\n" +
                              "\t%s\n" +
                              "Try scrubbing the sstables by running\n" +
                              "\tnodetool scrub %s %s\n",
                              transaction.originals(),
                              realm.getKeyspaceName(),
                              realm.getTableName()));
                error.addSuppressed(e);
                return error;
            }

            return e;
        }

        void maybeStopOrUpdateState()
        {
            op.throwIfStopRequested();

            long now = System.nanoTime();
            if (now - lastCheckObsoletion > TimeUnit.MINUTES.toNanos(1L))
            {
                controller.maybeRefreshOverlaps();
                lastCheckObsoletion = now;
            }
        }

        abstract void execute0();

        //
        // Closeable
        //

        @Override
        public void close()
        {
            close(null);
        }

        public void close(Throwable errorsSoFar)
        {
            Throwable err = Throwables.close(errorsSoFar, obsCloseable, writer, sstableRefs);

            if (transaction.isOffline())
            {
                if (completed)
                {
                    // update basic metrics
                    realm.metrics().incBytesCompacted(adjustedInputDiskSize(),
                                                      outputDiskSize(),
                                                      System.nanoTime() - startNanos);
                }
                Throwables.maybeFail(err);
                return;
            }

            if (completed)
            {
                boolean shouldSignalCompletion = true;
                var sharedProgress = sharedProgress();
                if (sharedProgress != null)
                    shouldSignalCompletion = sharedProgress.completeSubtask(this);

                if (shouldSignalCompletion)
                {
                    if (COMPACTION_HISTORY_ENABLED.getBoolean())
                    {
                        updateCompactionHistory(taskId, realm.getKeyspaceName(), realm.getTableName(), this);
                    }
                    CompactionManager.instance.incrementRemovedExpiredSSTables(fullyExpiredSSTablesCount);
                    if (!transaction.originals().isEmpty() && actuallyCompact.isEmpty())
                        // this CompactionOperation only deleted fully expired SSTables without compacting anything
                        CompactionManager.instance.incrementDeleteOnlyCompactions();
                }

                if (logger.isDebugEnabled())
                    debugLogCompactionSummaryInfo(taskIdString, System.nanoTime() - startNanos, totalKeysWritten, newSStables, this);
                if (logger.isTraceEnabled())
                    traceLogCompactionSummaryInfo(totalKeysWritten, estimatedKeys, this);
                if (strategy != null)
                    strategy.getCompactionLogger().compaction(startTime,
                                                              transaction.originals(),
                                                              tokenRange(),
                                                              System.currentTimeMillis(),
                                                              newSStables);

                // update the metrics
                realm.metrics().incBytesCompacted(adjustedInputDiskSize(),
                                                  outputDiskSize(),
                                                  System.nanoTime() - startNanos);
            }

            Throwables.maybeFail(err);
        }


        //
        // TableOperation.Progress methods
        //

        @Override
        public Optional<String> keyspace()
        {
            return Optional.of(metadata().keyspace);
        }

        @Override
        public Optional<String> table()
        {
            return Optional.of(metadata().name);
        }

        @Override
        public TableMetadata metadata()
        {
            return realm.metadata();
        }

        @Override
        public OperationType operationType()
        {
            return compactionType;
        }

        @Override
        public UUID operationId()
        {
            return taskId;
        }

        @Override
        public TableOperation.Unit unit()
        {
            return TableOperation.Unit.BYTES;
        }

        @Override
        public Set<SSTableReader> sstables()
        {
            return transaction.originals();
        }

        //
        // CompactionProgress
        //

        @Override
        @Nullable
        public CompactionStrategy strategy()
        {
            return CompactionTask.this.strategy;
        }

        @Override
        public Collection<SSTableReader> inSSTables()
        {
            // TODO should we use transaction.originals() and include the expired sstables?
            // This would be more correct but all the metrics we get from CompactionIterator will not be compatible
            return actuallyCompact;
        }
        @Override
        public Collection<SSTableReader> outSSTables()
        {
            return newSStables;
        }

        @Override
        public long inputDiskSize()
        {
            return inputDiskSize;
        }

        @Override
        public long outputDiskSize()
        {
            return CompactionSSTable.getTotalBytes(newSStables);
        }

        @Override
        public long uncompressedBytesWritten()
        {
            return writer.bytesWritten();
        }

        @Override
        public long startTimeNanos()
        {
            return startNanos;
        }
    }

    /**
     *  The compaction operation is a special case of an {@link AbstractTableOperation} and takes care of executing the
     *  actual compaction and releasing any resources when the compaction is finished.
     *  <p/>
     *  This class also extends {@link AbstractTableOperation} for reporting compaction-specific progress information.
     */
    public final class CompactionOperationIterator extends CompactionOperation
    {
        // resources that need closing
        private ScannerList scanners;
        private CompactionIterator compactionIterator;

        /**
         * Create a new compaction operation.
         * <p/>
         * @param controller the compaction controller is needed by the scanners and compaction iterator to manage options
         */
        CompactionOperationIterator(CompactionController controller, Set<SSTableReader> actuallyCompact, int fullyExpiredSSTablesCount)
        {
            super(controller, actuallyCompact, fullyExpiredSSTablesCount);
        }

        @Override
        TableOperation initializeSource(Range<Token> tokenRange)
        {
            var rangeList = tokenRange != null ? ImmutableList.of(tokenRange) : null;
            this.scanners = strategy != null ? strategy.getScanners(actuallyCompact, rangeList)
                                             : ScannerList.of(actuallyCompact, rangeList);
            this.compactionIterator = new CompactionIterator(compactionType, scanners.scanners, controller, FBUtilities.nowInSeconds(), taskId);
            return compactionIterator.getOperation();
        }

        void execute0()
        {
            double compressionRatio = compactionIterator.getCompressionRatio();
            if (compressionRatio == MetadataCollector.NO_COMPRESSION_RATIO)
                compressionRatio = 1.0;

            long lastBytesScanned = 0;

            while (compactionIterator.hasNext())
            {
                UnfilteredRowIterator partition = compactionIterator.next();
                if (writer.append(partition))
                    totalKeysWritten++;

                long bytesScanned = compactionIterator.getTotalBytesScanned();

                // Rate limit the scanners, and account for compression
                if (compactionRateLimiterAcquire(limiter, bytesScanned, lastBytesScanned, compressionRatio))
                    lastBytesScanned = bytesScanned;

                maybeStopOrUpdateState();
            }
        }

        @Override
        public void close(Throwable errorsSoFar)
        {
            super.close(Throwables.close(errorsSoFar, compactionIterator, scanners));
        }

        /**
         * @return the number of bytes read by the compaction iterator. For compressed or encrypted sstables,
         *         this is the number of bytes processed by the iterator after decompression, so this is the current
         *         position in the uncompressed sstable files.
         */
        @Override
        public long completed()
        {
            return compactionIterator.bytesRead();
        }

        /**
         * @return the initial number of bytes for input sstables. For compressed or encrypted sstables,
         *         this is the number of bytes after decompression, so this is the uncompressed length of sstable files.
         */
        public long total()
        {
            return compactionIterator.totalBytes();
        }


        @Override
        public long inputUncompressedSize()
        {
            return compactionIterator.totalBytes();
        }

        @Override
        public long adjustedInputDiskSize()
        {
            return compactionIterator.getTotalCompressedSize();
        }

        @Override
        public long uncompressedBytesRead()
        {
            return compactionIterator.bytesRead();
        }

        @Override
        public long uncompressedBytesRead(int level)
        {
            return compactionIterator.bytesRead(level);
        }

        @Override
        public long partitionsRead()
        {
            return compactionIterator.totalSourcePartitions();
        }

        @Override
        public long rowsRead()
        {
            return compactionIterator.totalSourceRows();
        }

        @Override
        public long[] partitionsHistogram()
        {
            return compactionIterator.mergedPartitionsHistogram();
        }

        @Override
        public long[] rowsHistogram()
        {
            return compactionIterator.mergedRowsHistogram();
        }

    }

    /**
     *  Cursor version of the above.
     */
    public final class CompactionOperationCursor extends CompactionOperation
    {
        // resources that need closing
        private CompactionCursor compactionCursor;

        /**
         * Create a new compaction operation.
         * <p/>
         * @param controller the compaction controller is needed by the scanners and compaction iterator to manage options
         */
        CompactionOperationCursor(CompactionController controller, Set<SSTableReader> actuallyCompact, int fullyExpiredSSTablesCount)
        {
            super(controller, actuallyCompact, fullyExpiredSSTablesCount);
        }

        @Override
        TableOperation initializeSource(Range<Token> tokenRange)
        {
            this.compactionCursor = new CompactionCursor(compactionType, actuallyCompact, tokenRange, controller, limiter, FBUtilities.nowInSeconds(), taskId);
            return compactionCursor.createOperation();
        }

        void execute0()
        {
            try
            {
                writeLoop:
                while (true)
                {
                    op.throwIfStopRequested();

                    switch (compactionCursor.copyOne(writer))
                    {
                        case EXHAUSTED:
                            break writeLoop;
                        case PARTITION:
                            ++totalKeysWritten;
                            maybeStopOrUpdateState();
                            break;
                    }
                }
            }
            catch (IOException e)
            {
                throw new FSWriteError(e, writer.getCurrentFileName());
            }
        }

        @Override
        public void close(Throwable errorsSoFar)
        {
            super.close(Throwables.close(errorsSoFar, compactionCursor));
        }

        /**
         * @return the number of bytes read by the compaction iterator. For compressed or encrypted sstables,
         *         this is the number of bytes processed by the iterator after decompression, so this is the current
         *         position in the uncompressed sstable files.
         */
        @Override
        public long completed()
        {
            return compactionCursor.bytesRead();
        }

        /**
         * @return the initial number of bytes for input sstables. For compressed or encrypted sstables,
         *         this is the number of bytes after decompression, so this is the uncompressed length of sstable files.
         */
        public long total()
        {
            return compactionCursor.totalBytes();
        }

        @Override
        public long inputUncompressedSize()
        {
            return compactionCursor.totalBytes();
        }

        @Override
        public long adjustedInputDiskSize()
        {
            return inputDiskSize();
        }

        @Override
        public long uncompressedBytesRead()
        {
            return compactionCursor.bytesRead();
        }

        @Override
        public long uncompressedBytesRead(int level)
        {
            // Cursors don't implement LCS per-level progress tracking.
            return 0L;
        }

        @Override
        public long partitionsRead()
        {
            return compactionCursor.totalSourcePartitions();
        }

        @Override
        public long rowsRead()
        {
            return compactionCursor.totalSourceRows();
        }

        @Override
        public long[] partitionsHistogram()
        {
            return compactionCursor.mergedPartitionsHistogram();
        }

        @Override
        public long[] rowsHistogram()
        {
            return compactionCursor.mergedRowsHistogram();
        }
    }

    public CompactionAwareWriter getCompactionAwareWriter(CompactionRealm realm,
                                                          Directories directories,
                                                          Set<SSTableReader> nonExpiredSSTables)
    {
        return new DefaultCompactionWriter(realm, directories, transaction, nonExpiredSSTables, keepOriginals, getLevel());
    }

    protected Directories getDirectories()
    {
        return realm.getDirectories();
    }

    public static long getMinRepairedAt(Set<SSTableReader> actuallyCompact)
    {
        long minRepairedAt= Long.MAX_VALUE;
        for (SSTableReader sstable : actuallyCompact)
            minRepairedAt = Math.min(minRepairedAt, sstable.getSSTableMetadata().repairedAt);
        if (minRepairedAt == Long.MAX_VALUE)
            return ActiveRepairService.UNREPAIRED_SSTABLE;
        return minRepairedAt;
    }

    public static UUID getPendingRepair(Set<SSTableReader> sstables)
    {
        if (sstables.isEmpty())
        {
            return ActiveRepairService.NO_PENDING_REPAIR;
        }
        Set<UUID> ids = new HashSet<>();
        for (SSTableReader sstable: sstables)
            ids.add(sstable.getSSTableMetadata().pendingRepair);

        if (ids.size() != 1)
        {
            if (!SKIP_REPAIR_STATE_CHECKING)
                throw new RuntimeException(String.format("Attempting to compact pending repair sstables with sstables from other repair, or sstables not pending repair: %s", ids));
            // otherwise we should continue but mark the result as unrepaired
            return ActiveRepairService.NO_PENDING_REPAIR;
        }

        return ids.iterator().next();
    }

    public static boolean getIsTransient(Set<SSTableReader> sstables)
    {
        if (sstables.isEmpty())
        {
            return false;
        }

        boolean isTransient = sstables.iterator().next().isTransient();

        if (!Iterables.all(sstables, sstable -> sstable.isTransient() == isTransient))
        {
            throw new RuntimeException("Attempting to compact transient sstables with non transient sstables");
        }

        return isTransient;
    }

    /**
     * Checks if we have enough disk space to execute the compaction.  Drops the largest sstable out of the Task until
     * there's enough space (in theory) to handle the compaction.  Does not take into account space that will be taken by
     * other compactions.
     *
     * @return true if there is enough disk space to execute the complete compaction, false if some sstables are excluded.
     *         If SSTables are excluded, they are removed from the transaction as well as the nonExpiredSSTables set.
     */
    protected boolean buildCompactionCandidatesForAvailableDiskSpace(Set<SSTableReader> nonExpiredSSTables, boolean containsExpired)
    {
        if(!realm.isCompactionDiskSpaceCheckEnabled() && compactionType == OperationType.COMPACTION)
        {
            logger.info("Compaction space check is disabled - trying to compact all sstables");
            return true;
        }

        int sstablesRemoved = 0;

        while(!nonExpiredSSTables.isEmpty())
        {
            // Only consider write size of non expired SSTables
            long expectedWriteSize = realm.getExpectedCompactedFileSize(nonExpiredSSTables, compactionType);
            long estimatedSSTables = strategy != null && strategy.getMaxSSTableBytes() > 0
                                     ? Math.max(1, expectedWriteSize / strategy.getMaxSSTableBytes())
                                     : 1;

            if(realm.getDirectories().hasAvailableDiskSpace(estimatedSSTables, expectedWriteSize))
                break;

            if (!reduceScopeForLimitedSpace(nonExpiredSSTables, expectedWriteSize))
            {
                // we end up here if we can't take any more sstables out of the compaction.
                // usually means we've run out of disk space

                // but we can still remove expired SSTables
                if (partialCompactionsAcceptable() && containsExpired)
                {
                    for (SSTableReader rdr : nonExpiredSSTables)
                        transaction.cancel(rdr);
                    nonExpiredSSTables.clear();
                    assert transaction.originals().size() > 0;
                    break;
                }

                String msg = String.format("Not enough space for compaction, estimated sstables = %d, expected write size = %d", estimatedSSTables, expectedWriteSize);
                logger.warn(msg);
                CompactionManager.instance.incrementAborted();
                throw new RuntimeException(msg);
            }

            sstablesRemoved++;
            logger.warn("Not enough space for compaction, {}MB estimated.  Reducing scope.",
                        (float) expectedWriteSize / 1024 / 1024);
        }

        if(sstablesRemoved > 0)
        {
            CompactionManager.instance.incrementCompactionsReduced();
            CompactionManager.instance.incrementSstablesDropppedFromCompactions(sstablesRemoved);
            return false;
        }
        return true;
    }

    protected int getLevel()
    {
        return 0;
    }

    protected CompactionController getCompactionController(Set<SSTableReader> toCompact)
    {
        return new CompactionController(realm, toCompact, gcBefore);
    }

    protected boolean partialCompactionsAcceptable()
    {
        return !isUserDefined;
    }

    public static long getMaxDataAge(Collection<SSTableReader> sstables)
    {
        long max = 0;
        for (SSTableReader sstable : sstables)
        {
            if (sstable.maxDataAge > max)
                max = sstable.maxDataAge;
        }
        return max;
    }

    private void debugLogCompactionSummaryInfo(String taskId,
                                               long durationInNano,
                                               long totalKeysWritten,
                                               Collection<SSTableReader> newSStables,
                                               CompactionProgress progress)
    {
        // log a bunch of statistics about the result and save to system table compaction_history
        long dTime = TimeUnit.NANOSECONDS.toMillis(durationInNano);

        long totalMergedPartitions = 0;
        long[] mergedPartitionCounts = progress.partitionsHistogram();
        StringBuilder mergeSummary = new StringBuilder(mergedPartitionCounts.length * 10);
        mergeSummary.append('{');
        for (int i = 0; i < mergedPartitionCounts.length; i++)
        {
            long mergedPartitionCount = mergedPartitionCounts[i];
            if (mergedPartitionCount != 0)
            {
                totalMergedPartitions += mergedPartitionCount * (i + 1);
                mergeSummary.append(i).append(':').append(mergedPartitionCount).append(", ");
            }
        }
        mergeSummary.append('}');

        StringBuilder newSSTableNames = new StringBuilder(newSStables.size() * 100);
        for (SSTableReader reader : newSStables)
            newSSTableNames.append(reader.descriptor.baseFileUri()).append(',');
        logger.debug("Compacted ({}{}) {} sstables to [{}] to level={}. {} to {} (~{}% of original) in {}ms. " +
                     "Read Throughput = {}, Write Throughput = {}, Row Throughput = ~{}/s, Partition Throughput = ~{}/s." +
                     " {} total partitions merged to {}. Partition merge counts were {}.",
                     taskId,
                     tokenRange() != null ? " range " + tokenRange() : "",
                     transaction.originals().size(),
                     newSSTableNames,
                     getLevel(),
                     prettyPrintMemory(progress.adjustedInputDiskSize()),
                     prettyPrintMemory(progress.outputDiskSize()),
                     (int) (progress.sizeRatio() * 100),
                     dTime,
                     prettyPrintMemoryPerSecond(progress.adjustedInputDiskSize(), durationInNano),
                     prettyPrintMemoryPerSecond(progress.outputDiskSize(), durationInNano),
                     progress.rowsRead() / (TimeUnit.NANOSECONDS.toSeconds(durationInNano) + 1),
                     (int) progress.partitionsRead() / (TimeUnit.NANOSECONDS.toSeconds(progress.durationInNanos()) + 1),
                     totalMergedPartitions,
                     totalKeysWritten,
                     mergeSummary);
    }

    private void debugLogCompactingMessage(String taskId)
    {
        Set<SSTableReader> originals = transaction.originals();
        StringBuilder ssTableLoggerMsg = new StringBuilder(originals.size() * 100);
        ssTableLoggerMsg.append("Compacting (").append(taskId);
        if (tokenRange() != null)
            ssTableLoggerMsg.append(" range ").append(tokenRange());
        ssTableLoggerMsg.append(") [");
        for (SSTableReader sstr : originals)
        {
            ssTableLoggerMsg.append(sstr.getFilename());
            if (sstr.getSSTableLevel() != 0)
                ssTableLoggerMsg.append(":level=")
                                .append(sstr.getSSTableLevel());
            ssTableLoggerMsg.append(", ");
        }
        ssTableLoggerMsg.append(']');

        logger.debug(ssTableLoggerMsg.toString());
    }


    private static void updateCompactionHistory(UUID id,
                                                String keyspaceName,
                                                String columnFamilyName,
                                                CompactionProgress progress)
    {
        long[] mergedPartitionsHistogram = progress.partitionsHistogram();
        Map<Integer, Long> mergedPartitions = new HashMap<>(mergedPartitionsHistogram.length);
        for (int i = 0; i < mergedPartitionsHistogram.length; i++)
        {
            long count = mergedPartitionsHistogram[i];
            if (count == 0)
                continue;

            int rows = i + 1;
            mergedPartitions.put(rows, count);
        }
        SystemKeyspace.updateCompactionHistory(id,
                                               keyspaceName,
                                               columnFamilyName,
                                               System.currentTimeMillis(),
                                               progress.adjustedInputDiskSize(),
                                               progress.outputDiskSize(),
                                               mergedPartitions);
    }

    private void traceLogCompactionSummaryInfo(long totalKeysWritten,
                                               long estimatedKeys,
                                               CompactionProgress progress)
    {
        logger.trace("CF Total Bytes Compacted: {}", prettyPrintMemory(addToTotalBytesCompacted(progress.outputDiskSize())));
        logger.trace("Actual #keys: {}, Estimated #keys:{}, Err%: {}",
                     totalKeysWritten,
                     estimatedKeys,
                     ((double) (totalKeysWritten - estimatedKeys) / totalKeysWritten));
    }
}
