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
package org.apache.cassandra.db.memtable;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.AbstractUnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.BasePartitionUpdater;
import org.apache.cassandra.db.tries.InMemoryBaseTrie;

import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.tries.TrieSpaceExhaustedException;
import org.apache.cassandra.index.transactions.UpdateTransaction;

import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.tries.Direction;
import org.apache.cassandra.metrics.TrieMemtableMetricsView;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.MemtableAllocator;
import org.github.jamm.Unmetered;

/**
 * Base class for trie-based memtable implementations (Stage2 and Stage3), providing common functionality
 * for managing shards, metrics, and lifecycle operations.
 */
public abstract class AbstractTrieMemtable extends AbstractShardedMemtable
{
    private static final Logger logger = LoggerFactory.getLogger(AbstractTrieMemtable.class);

    // Set to true when the memtable requests a switch (e.g. for trie size limit being reached) to ensure only one
    // thread calls cfs.switchMemtableIfCurrent.
    protected final AtomicBoolean switchRequested = new AtomicBoolean(false);

    @Unmetered
    protected final TrieMemtableMetricsView metrics;

    protected AbstractTrieMemtable(AtomicReference<CommitLogPosition> commitLogLowerBound,
                                   TableMetadataRef metadataRef,
                                   Owner owner,
                                   Integer shardCountOption)
    {
        super(commitLogLowerBound, metadataRef, owner, shardCountOption);
        this.metrics = TrieMemtableMetricsView.getOrCreate(metadataRef.keyspace, metadataRef.name);
    }

    @Override
    public void signalFlushRequired(ColumnFamilyStore.FlushReason flushReason, boolean skipIfSignaled)
    {
        if (!switchRequested.getAndSet(true) || !skipIfSignaled)
        {
            logger.info("Scheduling flush for table {} due to {}", this.metadata.get(), flushReason);
            owner.signalFlushRequired(this, flushReason);
        }
    }

    @Override
    protected long performPut(PartitionUpdate update, UpdateTransaction indexer, OpOrder.Group opGroup)
    {
        DecoratedKey key = update.partitionKey();
        AbstractMemtableShard shard = getShards()[boundaries.getShardForKey(key)];
        long colUpdateTimeDelta = shard.put(update, indexer, opGroup);

        if (shard.data.reachedAllocatedSizeThreshold())
            signalFlushRequired(ColumnFamilyStore.FlushReason.TRIE_LIMIT, true);

        return colUpdateTimeDelta;
    }

    @Override
    public long getMinTimestamp()
    {
        long min = Long.MAX_VALUE;
        for (AbstractMemtableShard shard : getShards())
            min = EncodingStats.mergeMinTimestamp(min, shard.stats);
        return min != EncodingStats.NO_STATS.minTimestamp ? min : NO_MIN_TIMESTAMP;
    }

    @Override
    public long getMinLocalDeletionTime()
    {
        long min = Long.MAX_VALUE;
        for (AbstractMemtableShard shard : getShards())
            min = EncodingStats.mergeMinLocalDeletionTime(min, shard.stats);
        return min;
    }

    @Override
    public DecoratedKey minPartitionKey()
    {
        AbstractMemtableShard[] shards = getShards();
        for (int i = 0; i < shards.length; i++)
        {
            AbstractMemtableShard shard = shards[i];
            if (!shard.isClean())
                return shard.minPartitionKey();
        }
        return null;
    }

    @Override
    public DecoratedKey maxPartitionKey()
    {
        AbstractMemtableShard[] shards = getShards();
        for (int i = shards.length - 1; i >= 0; i--)
        {
            AbstractMemtableShard shard = shards[i];
            if (!shard.isClean())
                return shard.maxPartitionKey();
        }
        return null;
    }

    @Override
    RegularAndStaticColumns columns()
    {
        for (AbstractMemtableShard shard : getShards())
            columnsCollector.update(shard.columns);
        return columnsCollector.get();
    }

    @Override
    EncodingStats encodingStats()
    {
        for (AbstractMemtableShard shard : getShards())
            statsCollector.update(shard.stats);
        return statsCollector.get();
    }

    @Override
    public boolean isClean()
    {
        for (AbstractMemtableShard shard : getShards())
            if (!shard.isClean())
                return false;
        return true;
    }

    @VisibleForTesting
    @Override
    public void switchOut(OpOrder.Barrier writeBarrier, AtomicReference<CommitLogPosition> commitLogUpperBound)
    {
        super.switchOut(writeBarrier, commitLogUpperBound);

        for (AbstractMemtableShard shard : getShards())
            shard.allocator.setDiscarding();
    }

    @Override
    public void discard()
    {
        super.discard();
        // metrics here are not thread safe, but I think we can live with that
        metrics.lastFlushShardDataSizes.reset();
        for (AbstractMemtableShard shard : getShards())
        {
            metrics.lastFlushShardDataSizes.update(shard.liveDataSize());
        }
        // the buffer release is a longer-running process, do it in a separate loop to not make the metrics update wait
        for (AbstractMemtableShard shard : getShards())
        {
            shard.allocator.setDiscarded();
            shard.discardBuffers();
        }
    }

    @Override
    public void addMemoryUsageTo(MemoryUsage stats)
    {
        super.addMemoryUsageTo(stats);
        for (AbstractMemtableShard shard : getShards())
        {
            stats.ownsOnHeap += shard.allocator.onHeap().owns();
            stats.ownsOffHeap += shard.allocator.offHeap().owns();
            stats.ownershipRatioOnHeap += shard.allocator.onHeap().ownershipRatio();
            stats.ownershipRatioOffHeap += shard.allocator.offHeap().ownershipRatio();
        }
    }

    @Override
    public long getLiveDataSize()
    {
        long total = 0L;
        for (AbstractMemtableShard shard : getShards())
            total += shard.liveDataSize();
        return total;
    }

    @Override
    public long operationCount()
    {
        long total = 0L;
        for (AbstractMemtableShard shard : getShards())
            total += shard.currentOperations();
        return total;
    }

    @Override
    public long partitionCount()
    {
        int total = 0;
        for (AbstractMemtableShard shard : getShards())
            total += shard.partitionCount();
        return total;
    }

    @Override
    @VisibleForTesting
    public long unusedReservedOnHeapMemory()
    {
        long size = 0;
        for (AbstractMemtableShard shard : getShards())
        {
            size += shard.unusedReservedMemory();
            size += shard.allocator.unusedReservedOnHeapMemory();
        }
        size += this.allocator.unusedReservedOnHeapMemory();
        return size;
    }

    protected static DecoratedKey getPartitionKeyFromPath(TableMetadata metadata, ByteComparable path, ByteComparable.Version version)
    {
        return BufferDecoratedKey.fromByteComparable(path, version, metadata.partitioner);
    }

    protected static ByteComparable toComparableBound(PartitionPosition position, boolean before)
    {
        return position == null || position.isMinimum() ? null : position.asComparableBound(before);
    }

    /**
     * Get the array of shards for this memtable.
     */
    protected abstract AbstractMemtableShard[] getShards();

    /**
     * Release all recycled content references, including the ones waiting in still incomplete recycling lists.
     * This is a test method and can cause null pointer exceptions if used on a live trie.
     */
    @VisibleForTesting
    protected void releaseReferencesUnsafe()
    {
        for (AbstractMemtableShard<?, ?> shard : getShards())
            shard.data.releaseReferencesUnsafe();
    }

    /**
     * Base class for memtable unfiltered partition iterators.
     */
    protected static class MemtableUnfilteredPartitionIterator
    extends AbstractUnfilteredPartitionIterator
    implements Memtable.MemtableUnfilteredPartitionIterator
    {
        private final TableMetadata metadata;
        private final Iterator<? extends Partition> iter;
        private final ColumnFilter columnFilter;
        private final DataRange dataRange;
        private final long minLocalDeletionTime;

        protected MemtableUnfilteredPartitionIterator(TableMetadata metadata,
                                                      Iterator<? extends Partition> iter,
                                                      ColumnFilter columnFilter,
                                                      DataRange dataRange,
                                                      long minLocalDeletionTime)
        {
            this.metadata = metadata;
            this.iter = iter;
            this.columnFilter = columnFilter;
            this.dataRange = dataRange;
            this.minLocalDeletionTime = minLocalDeletionTime;
        }

        public long getMinLocalDeletionTime()
        {
            return minLocalDeletionTime;
        }

        public TableMetadata metadata()
        {
            return metadata;
        }

        public boolean hasNext()
        {
            return iter.hasNext();
        }

        public UnfilteredRowIterator next()
        {
            Partition partition = iter.next();
            DecoratedKey key = partition.partitionKey();
            ClusteringIndexFilter filter = dataRange.clusteringIndexFilter(key);

            return filter.getUnfilteredRowIterator(columnFilter, partition);
        }
    }

    /**
     * Base class for memtable shards, providing common fields and operations.
     * @param <T> The type of the trie data structure used by this shard (must extend InMemoryBaseTrie)
     * @param <U> The type of the updater used to apply partition updates (must extend BasePartitionUpdater)
     */
    public static abstract class AbstractMemtableShard<T extends InMemoryBaseTrie<?>, U extends BasePartitionUpdater>
    {
        /// Content map for the given shard.
        @VisibleForTesting
        protected final T data;
        // The following fields are volatile as we have to make sure that when we
        // collect results from all sub-ranges, the thread accessing the value
        // is guaranteed to see the changes to the values.

        // The smallest timestamp for all partitions stored in this shard
        protected volatile long minTimestamp = Long.MAX_VALUE;

        protected volatile long liveDataSize = 0;

        protected volatile long currentOperations = 0;

        protected volatile int partitionCount = 0;

        @Unmetered
        protected final ReentrantLock writeLock = new ReentrantLock(TrieMemtable.SHARD_LOCK_FAIRNESS);

        protected volatile RegularAndStaticColumns columns;

        protected volatile EncodingStats stats;

        @Unmetered  // total pool size should not be included in memtable's deep size
        protected final MemtableAllocator allocator;

        @Unmetered
        protected final TrieMemtableMetricsView metrics;

        protected final TableMetadataRef metadata;

        @VisibleForTesting
        protected AbstractMemtableShard(TableMetadataRef metadata, MemtableAllocator allocator, TrieMemtableMetricsView metrics, T data)
        {
            this.metadata = metadata;
            this.columns = RegularAndStaticColumns.NONE;
            this.stats = EncodingStats.NO_STATS;
            this.allocator = allocator;
            this.metrics = metrics;
            this.data = data;
        }

        public boolean isClean()
        {
            return data.isEmpty();
        }

        protected void updateMinTimestamp(long timestamp)
        {
            if (timestamp < minTimestamp)
                minTimestamp = timestamp;
        }

        protected void updateLiveDataSize(long size)
        {
            liveDataSize += size;
        }

        protected void updateCurrentOperations(long op)
        {
            currentOperations += op;
        }

        public int partitionCount()
        {
            return partitionCount;
        }

        public long liveDataSize()
        {
            return liveDataSize;
        }

        public long currentOperations()
        {
            return currentOperations;
        }

        protected abstract DecoratedKey firstPartitionKey(Direction direction);

        public DecoratedKey minPartitionKey()
        {
            return firstPartitionKey(Direction.FORWARD);
        }

        public DecoratedKey maxPartitionKey()
        {
            return firstPartitionKey(Direction.REVERSE);
        }

        public long unusedReservedMemory()
        {
            return data.unusedReservedOnHeapMemory();
        }

        public void discardBuffers()
        {
            data.discardBuffers();
        }

        /**
         * Check if the data trie is empty.
         */
        protected boolean isDataEmpty()
        {
            return data.isEmpty();
        }

        /**
         * Create an updater for applying partition updates.
         */
        protected abstract U createUpdater(PartitionUpdate update, UpdateTransaction indexer, OpOrder.Group opGroup);

        /**
         * Apply the update using the updater.
         */
        protected abstract void applyUpdate(U updater, PartitionUpdate update) throws TrieSpaceExhaustedException;

        /**
         * Get partitions added from updater.
         */
        protected abstract int getUpdaterPartitionsAdded(U updater);

        /**
         * Common put implementation for applying partition updates.
         */
        public long put(PartitionUpdate update, UpdateTransaction indexer, OpOrder.Group opGroup)
        {
            U updater = createUpdater(update, indexer, opGroup);
            acquireWriteLock();
            try
            {
                indexer.start();
                // Add the initial trie size on the first operation. This technically isn't correct (other shards
                // do take their memory share even if they are empty) but doing it during construction may cause
                // the allocator to block while we are trying to flush a memtable and become a deadlock.
                long onHeap = isDataEmpty() ? 0 : data.usedSizeOnHeap();
                long offHeap = isDataEmpty() ? 0 : data.usedSizeOffHeap();

                try
                {
                    applyUpdate(updater, update);
                }
                catch (TrieSpaceExhaustedException e)
                {
                    // This should never really happen as a flush would be triggered long before this limit is reached.
                    throw new AssertionError(e);
                }
                finally
                {
                    allocator.offHeap().adjust(data.usedSizeOffHeap() - offHeap, opGroup);
                    allocator.onHeap().adjust((data.usedSizeOnHeap() - onHeap) + updater.heapSize, opGroup);
                    partitionCount += getUpdaterPartitionsAdded(updater);
                }
            }
            finally
            {
                indexer.commit();
                updateMinTimestamp(update.stats().minTimestamp);
                updateLiveDataSize(updater.dataSize);
                updateCurrentOperations(update.operationCount());

                columns = columns.mergeTo(update.columns());
                stats = stats.mergeWith(update.stats());

                writeLock.unlock();
            }
            return updater.colUpdateTimeDelta;
        }

        /**
         * Common logic for acquiring write lock with metrics tracking.
         */
        protected void acquireWriteLock()
        {
            boolean locked = writeLock.tryLock();
            if (locked)
            {
                metrics.uncontendedPuts.inc();
            }
            else
            {
                metrics.contendedPuts.inc();
                long lockStartTime = Clock.Global.nanoTime();
                writeLock.lock();
                metrics.contentionTime.addNano(Clock.Global.nanoTime() - lockStartTime);
            }
        }
    }
}
