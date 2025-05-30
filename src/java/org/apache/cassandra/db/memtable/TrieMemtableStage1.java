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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.AbstractUnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.BTreePartitionData;
import org.apache.cassandra.db.partitions.BTreePartitionUpdate;
import org.apache.cassandra.db.partitions.BTreePartitionUpdater;
import org.apache.cassandra.db.partitions.ImmutableBTreePartition;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.tries.Direction;
import org.apache.cassandra.db.tries.InMemoryTrie;
import org.apache.cassandra.db.tries.Trie;
import org.apache.cassandra.db.tries.TrieSpaceExhaustedException;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.IncludingExcludingBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.metrics.TrieMemtableMetricsView;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.Cloner;
import org.apache.cassandra.utils.memory.EnsureOnHeap;
import org.apache.cassandra.utils.memory.MemtableAllocator;
import org.github.jamm.Unmetered;

/**
 * Previous TrieMemtable implementation, provided for two reasons:
 * <ul>
 * <li> to easily compare current and earlier implementations of the trie memtable
 * <li> to have an option to change a database back to the older implementation if we find a bug or a performance problem
 *   with the new code.
 *   </ul>
 * <p>
 * To switch a table to this version, use
 * <code><pre>
 *   ALTER TABLE ... WITH memtable = {'class': 'TrieMemtableStage1'}
 * </pre></code>
 * or add
 * <code><pre>
 *   memtable:
 *     class: TrieMemtableStage1
 * </pre></code>
 * in <code>cassandra.yaml</code> to switch a node to it as default.
 *
 */
public class TrieMemtableStage1 extends AbstractAllocatorMemtable
{
    private static final Logger logger = LoggerFactory.getLogger(TrieMemtableStage1.class);

    public static final Factory FACTORY = new TrieMemtableStage1.Factory();

    static final ByteComparable.Version BYTE_COMPARABLE_VERSION = ByteComparable.Version.OSS41;

    /** If keys is below this length, we will use a recursive procedure for inserting data in the memtable trie. */
    @VisibleForTesting
    public static final int MAX_RECURSIVE_KEY_LENGTH = 128;

    // Set to true when the memtable requests a switch (e.g. for trie size limit being reached) to ensure only one
    // thread calls cfs.switchMemtableIfCurrent.
    private AtomicBoolean switchRequested = new AtomicBoolean(false);


    // The boundaries for the keyspace as they were calculated when the memtable is created.
    // The boundaries will be NONE for system keyspaces or if StorageService is not yet initialized.
    // The fact this is fixed for the duration of the memtable lifetime, guarantees we'll always pick the same core
    // for the a given key, even if we race with the StorageService initialization or with topology changes.
    @Unmetered
    private final ShardBoundaries boundaries;

    /**
     * Core-specific memtable regions. All writes must go through the specific core. The data structures used
     * are concurrent-read safe, thus reads can be carried out from any thread.
     */
    private final MemtableShard[] shards;

    /**
     * A merged view of the memtable map. Used for partition range queries and flush.
     * For efficiency we serve single partition requests off the shard which offers more direct InMemoryTrie methods.
     */
    private final Trie<BTreePartitionData> mergedTrie;

    @Unmetered
    private final TrieMemtableMetricsView metrics;

    /**
     * Keeps an estimate of the average row size in this memtable, computed from a small sample of rows.
     * Because computing this estimate is potentially costly, as it requires iterating the rows,
     * the estimate is updated only whenever the number of operations on the memtable increases significantly from the
     * last update. This estimate is not very accurate but should be ok for planning or diagnostic purposes.
     */
    private volatile MemtableAverageRowSize estimatedAverageRowSize;

    // only to be used by init(), to setup the very first memtable for the cfs
    TrieMemtableStage1(AtomicReference<CommitLogPosition> commitLogLowerBound, TableMetadataRef metadataRef, Owner owner)
    {
        super(commitLogLowerBound, metadataRef, owner);
        this.boundaries = owner.localRangeSplits(TrieMemtable.SHARD_COUNT);
        this.metrics = TrieMemtableMetricsView.getOrCreate(metadataRef.keyspace, metadataRef.name);
        this.shards = generatePartitionShards(boundaries.shardCount(), metadataRef, metrics);
        this.mergedTrie = makeMergedTrie(shards);
        logger.trace("Created memtable with {} shards", this.shards.length);
    }

    private static MemtableShard[] generatePartitionShards(int splits,
                                                           TableMetadataRef metadata,
                                                           TrieMemtableMetricsView metrics)
    {
        if (splits == 1)
            return new MemtableShard[] { new MemtableShard(metadata, metrics) };

        MemtableShard[] partitionMapContainer = new MemtableShard[splits];
        for (int i = 0; i < splits; i++)
            partitionMapContainer[i] = new MemtableShard(metadata, metrics);

        return partitionMapContainer;
    }

    private static Trie<BTreePartitionData> makeMergedTrie(MemtableShard[] shards)
    {
        List<Trie<BTreePartitionData>> tries = new ArrayList<>(shards.length);
        for (MemtableShard shard : shards)
            tries.add(shard.data);
        return Trie.mergeDistinct(tries);
    }

    protected Factory factory()
    {
        return FACTORY;
    }

    public boolean isClean()
    {
        for (MemtableShard shard : shards)
            if (!shard.isEmpty())
                return false;
        return true;
    }

    @VisibleForTesting
    @Override
    public void switchOut(OpOrder.Barrier writeBarrier, AtomicReference<CommitLogPosition> commitLogUpperBound)
    {
        super.switchOut(writeBarrier, commitLogUpperBound);

        for (MemtableShard shard : shards)
            shard.allocator.setDiscarding();
    }

    @Override
    public void discard()
    {
        super.discard();
        // metrics here are not thread safe, but I think we can live with that
        metrics.lastFlushShardDataSizes.reset();
        for (MemtableShard shard : shards)
        {
            metrics.lastFlushShardDataSizes.update(shard.liveDataSize());
        }
        for (MemtableShard shard : shards)
        {
            shard.allocator.setDiscarded();
            shard.data.discardBuffers();
        }
    }

    /**
     * Should only be called by ColumnFamilyStore.apply via Keyspace.apply, which supplies the appropriate
     * OpOrdering.
     *
     * commitLogSegmentPosition should only be null if this is a secondary index, in which case it is *expected* to be null
     */
    public long put(PartitionUpdate update, UpdateTransaction indexer, OpOrder.Group opGroup)
    {
        DecoratedKey key = update.partitionKey();
        MemtableShard shard = shards[boundaries.getShardForKey(key)];
        long colUpdateTimeDelta = shard.put(key, update, indexer, opGroup);

        if (shard.data.reachedAllocatedSizeThreshold())
            signalFlushRequired(ColumnFamilyStore.FlushReason.TRIE_LIMIT, true);

        return colUpdateTimeDelta;
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
    public void addMemoryUsageTo(MemoryUsage stats)
    {
        super.addMemoryUsageTo(stats);
        for (MemtableShard shard : shards)
        {
            stats.ownsOnHeap += shard.allocator.onHeap().owns();
            stats.ownsOffHeap += shard.allocator.offHeap().owns();
            stats.ownershipRatioOnHeap += shard.allocator.onHeap().ownershipRatio();
            stats.ownershipRatioOffHeap += shard.allocator.offHeap().ownershipRatio();
        }
    }

    /**
     * Technically we should scatter gather on all the core threads because the size in following calls are not
     * using volatile variables, but for metrics purpose this should be good enough.
     */
    @Override
    public long getLiveDataSize()
    {
        long total = 0L;
        for (MemtableShard shard : shards)
            total += shard.liveDataSize();
        return total;
    }

    @Override
    public long getOperations()
    {
        long total = 0L;
        for (MemtableShard shard : shards)
            total += shard.currentOperations();
        return total;
    }

    @Override
    public long partitionCount()
    {
        int total = 0;
        for (MemtableShard shard : shards)
            total += shard.partitionCount();
        return total;
    }

    public int getShardCount()
    {
        return shards.length;
    }

    public long rowCount(final ColumnFilter columnFilter, final DataRange dataRange)
    {
        int total = 0;
        for (MemtableUnfilteredPartitionIterator iter = makePartitionIterator(columnFilter, dataRange); iter.hasNext(); )
        {
            for (UnfilteredRowIterator it = iter.next(); it.hasNext(); )
            {
                Unfiltered uRow = it.next();
                if (uRow.isRow())
                    total++;
            }
        }

        return total;
    }

    @Override
    public long getEstimatedAverageRowSize()
    {
        if (estimatedAverageRowSize == null || currentOperations.get() > estimatedAverageRowSize.operations * 1.5)
            estimatedAverageRowSize = new MemtableAverageRowSize(this);
        return estimatedAverageRowSize.rowSize;
    }

    /**
     * Returns the minTS if one available, otherwise NO_MIN_TIMESTAMP.
     *
     * EncodingStats uses a synthetic epoch TS at 2015. We don't want to leak that (CASSANDRA-18118) so we return NO_MIN_TIMESTAMP instead.
     *
     * @return The minTS or NO_MIN_TIMESTAMP if none available
     */
    @Override
    public long getMinTimestamp()
    {
        long min = Long.MAX_VALUE;
        for (MemtableShard shard : shards)
            min =  EncodingStats.mergeMinTimestamp(min, shard.stats);
        return min != EncodingStats.NO_STATS.minTimestamp ? min : NO_MIN_TIMESTAMP;
    }

    @Override
    public DecoratedKey minPartitionKey()
    {
        for (int i = 0; i < shards.length; i++)
        {
            MemtableShard shard = shards[i];
            if (!shard.isEmpty())
                return shard.minPartitionKey();
        }
        return null;
    }

    @Override
    public DecoratedKey maxPartitionKey()
    {
        for (int i = shards.length - 1; i >= 0; i--)
        {
            MemtableShard shard = shards[i];
            if (!shard.isEmpty())
                return shard.maxPartitionKey();
        }
        return null;
    }

    @Override
    RegularAndStaticColumns columns()
    {
        for (MemtableShard shard : shards)
            columnsCollector.update(shard.columns);
        return columnsCollector.get();
    }

    @Override
    EncodingStats encodingStats()
    {
        for (MemtableShard shard : shards)
            statsCollector.update(shard.stats);
        return statsCollector.get();
    }

    public MemtableUnfilteredPartitionIterator makePartitionIterator(final ColumnFilter columnFilter, final DataRange dataRange)
    {
        AbstractBounds<PartitionPosition> keyRange = dataRange.keyRange();

        PartitionPosition left = keyRange.left;
        PartitionPosition right = keyRange.right;
        if (left.isMinimum())
            left = null;
        if (right.isMinimum())
            right = null;

        boolean isBound = keyRange instanceof Bounds;
        boolean includeStart = isBound || keyRange instanceof IncludingExcludingBounds;
        boolean includeStop = isBound || keyRange instanceof Range;

        Trie<BTreePartitionData> subMap = mergedTrie.subtrie(left, includeStart, right, includeStop);

        return new MemtableUnfilteredPartitionIterator(metadata(),
                                                       allocator.ensureOnHeap(),
                                                       subMap,
                                                       columnFilter,
                                                       dataRange);
    }

    public Partition getPartition(DecoratedKey key)
    {
        int shardIndex = boundaries.getShardForKey(key);
        BTreePartitionData data = shards[shardIndex].data.get(key);
        if (data != null)
            return createPartition(metadata(), allocator.ensureOnHeap(), key, data);
        else
            return null;
    }

    private static MemtablePartition createPartition(TableMetadata metadata, EnsureOnHeap ensureOnHeap, DecoratedKey key, BTreePartitionData data)
    {
        return new MemtablePartition(metadata, ensureOnHeap, key, data);
    }

    private static MemtablePartition getPartitionFromTrieEntry(TableMetadata metadata, EnsureOnHeap ensureOnHeap, Map.Entry<? extends ByteComparable, BTreePartitionData> en)
    {
        DecoratedKey key = BufferDecoratedKey.fromByteComparable(en.getKey(),
                                                                 BYTE_COMPARABLE_VERSION,
                                                                 metadata.partitioner);
        return createPartition(metadata, ensureOnHeap, key, en.getValue());
    }

    private static DecoratedKey getPartitionKeyFromPath(TableMetadata metadata, ByteComparable path)
    {
        return BufferDecoratedKey.fromByteComparable(path, BYTE_COMPARABLE_VERSION, metadata.partitioner);
    }

    public FlushCollection<MemtablePartition> getFlushSet(PartitionPosition from, PartitionPosition to)
    {
        Trie<BTreePartitionData> toFlush = mergedTrie.subtrie(from, true, to, false);
        long keySize = 0;
        int keyCount = 0;

        for (Iterator<Map.Entry<ByteComparable.Preencoded, BTreePartitionData>> it = toFlush.entryIterator(); it.hasNext(); )
        {
            Map.Entry<ByteComparable.Preencoded, BTreePartitionData> en = it.next();
            byte[] keyBytes = DecoratedKey.keyFromByteComparable(en.getKey(), BYTE_COMPARABLE_VERSION, metadata().partitioner);
            keySize += keyBytes.length;
            keyCount++;
        }
        long partitionKeySize = keySize;
        int partitionCount = keyCount;

        return new AbstractFlushCollection<MemtablePartition>()
        {
            public Memtable memtable()
            {
                return TrieMemtableStage1.this;
            }

            public PartitionPosition from()
            {
                return from;
            }

            public PartitionPosition to()
            {
                return to;
            }

            public long partitionCount()
            {
                return partitionCount;
            }

            public Iterator<MemtablePartition> iterator()
            {
                return Iterators.transform(toFlush.entryIterator(),
                                           // During flushing we are certain the memtable will remain at least until
                                           // the flush completes. No copying to heap is necessary.
                                           entry -> getPartitionFromTrieEntry(metadata(), EnsureOnHeap.NOOP, entry));
            }

            public long partitionKeySize()
            {
                return partitionKeySize;
            }
        };
    }

    static class MemtableShard
    {
        // The following fields are volatile as we have to make sure that when we
        // collect results from all sub-ranges, the thread accessing the value
        // is guaranteed to see the changes to the values.

        // The smallest timestamp for all partitions stored in this shard
        private volatile long minTimestamp = Long.MAX_VALUE;

        private volatile long liveDataSize = 0;

        private volatile long currentOperations = 0;

        private volatile int partitionCount = 0;

        @Unmetered
        private ReentrantLock writeLock = new ReentrantLock();

        // Content map for the given shard. This is implemented as a memtable trie which uses the prefix-free
        // byte-comparable ByteSource representations of the keys to address the partitions.
        //
        // This map is used in a single-producer, multi-consumer fashion: only one thread will insert items but
        // several threads may read from it and iterate over it. Iterators are created when a the first item of
        // a flow is requested for example, and then used asynchronously when sub-sequent items are requested.
        //
        // Therefore, iterators should not throw ConcurrentModificationExceptions if the underlying map is modified
        // during iteration, they should provide a weakly consistent view of the map instead.
        //
        // Also, this data is backed by memtable memory, when accessing it callers must specify if it can be accessed
        // unsafely, meaning that the memtable will not be discarded as long as the data is used, or whether the data
        // should be copied on heap for off-heap allocators.
        @VisibleForTesting
        final InMemoryTrie<BTreePartitionData> data;

        RegularAndStaticColumns columns;

        EncodingStats stats;

        private final MemtableAllocator allocator;

        @Unmetered
        private final TrieMemtableMetricsView metrics;

        private TableMetadataRef metadata;

        MemtableShard(TableMetadataRef metadata, TrieMemtableMetricsView metrics)
        {
            this(metadata, AbstractAllocatorMemtable.MEMORY_POOL.newAllocator(), metrics);
        }

        @VisibleForTesting
        MemtableShard(TableMetadataRef metadata, MemtableAllocator allocator, TrieMemtableMetricsView metrics)
        {
            this.data = InMemoryTrie.shortLived(BYTE_COMPARABLE_VERSION, TrieMemtable.BUFFER_TYPE);
            this.columns = RegularAndStaticColumns.NONE;
            this.stats = EncodingStats.NO_STATS;
            this.allocator = allocator;
            this.metrics = metrics;
            this.metadata = metadata;
        }

        public long put(DecoratedKey key, PartitionUpdate update, UpdateTransaction indexer, OpOrder.Group opGroup)
        {
            Cloner cloner = allocator.cloner(opGroup);
            BTreePartitionUpdater updater = new BTreePartitionUpdater(allocator, cloner, opGroup, indexer);
            boolean locked = writeLock.tryLock();
            if (locked)
            {
                metrics.uncontendedPuts.inc();
            }
            else
            {
                metrics.contendedPuts.inc();
                long lockStartTime = System.nanoTime();
                writeLock.lock();
                metrics.contentionTime.addNano(System.nanoTime() - lockStartTime);
            }
            try
            {
                try
                {
                    // Add the initial trie size on the first operation. This technically isn't correct (other shards
                    // do take their memory share even if they are empty) but doing it during construction may cause
                    // the allocator to block while we are trying to flush a memtable and become a deadlock.
                    long onHeap = data.isEmpty() ? 0 : data.usedSizeOnHeap();
                    long offHeap = data.isEmpty() ? 0 : data.usedSizeOffHeap();
                    // Use the fast recursive put if we know the key is small enough to not cause a stack overflow.
                    try
                    {
                        data.putSingleton(key,
                                          BTreePartitionUpdate.asBTreeUpdate(update),
                                          updater::mergePartitions,
                                          key.getKeyLength() < MAX_RECURSIVE_KEY_LENGTH);
                    }
                    catch (TrieSpaceExhaustedException e)
                    {
                        // This should never really happen as a flush would be triggered long before this limit is reached.
                        throw Throwables.propagate(e);
                    }
                    allocator.offHeap().adjust(data.usedSizeOffHeap() - offHeap, opGroup);
                    allocator.onHeap().adjust(data.usedSizeOnHeap() - onHeap, opGroup);
                    partitionCount += updater.partitionsAdded;
                }
                finally
                {
                    updateMinTimestamp(update.stats().minTimestamp);
                    updateLiveDataSize(updater.dataSize);
                    updateCurrentOperations(update.operationCount());

                    columns = columns.mergeTo(update.columns());
                    stats = stats.mergeWith(update.stats());
                }
            }
            finally
            {
                writeLock.unlock();
            }
            return updater.colUpdateTimeDelta;
        }

        public boolean isEmpty()
        {
            return data.isEmpty();
        }

        private void updateMinTimestamp(long timestamp)
        {
            if (timestamp < minTimestamp)
                minTimestamp = timestamp;
        }

        void updateLiveDataSize(long size)
        {
            liveDataSize = liveDataSize + size;
        }

        private void updateCurrentOperations(long op)
        {
            currentOperations = currentOperations + op;
        }

        public int partitionCount()
        {
            return partitionCount;
        }

        long liveDataSize()
        {
            return liveDataSize;
        }

        long currentOperations()
        {
            return currentOperations;
        }

        private DecoratedKey firstPartitionKey(Direction direction)
        {
            Iterator<Map.Entry<ByteComparable.Preencoded, BTreePartitionData>> iter = data.entryIterator(direction);
            if (!iter.hasNext())
                return null;

            Map.Entry<ByteComparable.Preencoded, BTreePartitionData> entry = iter.next();
            return getPartitionKeyFromPath(metadata.get(), entry.getKey());
        }

        public DecoratedKey minPartitionKey()
        {
            return firstPartitionKey(Direction.FORWARD);
        }

        public DecoratedKey maxPartitionKey()
        {
            return firstPartitionKey(Direction.REVERSE);
        }
    }

    static class MemtableUnfilteredPartitionIterator extends AbstractUnfilteredPartitionIterator implements Memtable.MemtableUnfilteredPartitionIterator
    {
        private final TableMetadata metadata;
        private final EnsureOnHeap ensureOnHeap;
        private final Trie<BTreePartitionData> source;
        private final Iterator<Map.Entry<ByteComparable.Preencoded, BTreePartitionData>> iter;
        private final ColumnFilter columnFilter;
        private final DataRange dataRange;

        public MemtableUnfilteredPartitionIterator(TableMetadata metadata,
                                                   EnsureOnHeap ensureOnHeap,
                                                   Trie<BTreePartitionData> source,
                                                   ColumnFilter columnFilter,
                                                   DataRange dataRange)
        {
            this.metadata = metadata;
            this.ensureOnHeap = ensureOnHeap;
            this.iter = source.entryIterator();
            this.source = source;
            this.columnFilter = columnFilter;
            this.dataRange = dataRange;
        }

        public int getMinLocalDeletionTime()
        {
            int minLocalDeletionTime = Integer.MAX_VALUE;
            for (BTreePartitionData partition : source.values())
                minLocalDeletionTime = EncodingStats.mergeMinLocalDeletionTime(minLocalDeletionTime, partition.stats);

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
            Partition partition = getPartitionFromTrieEntry(metadata(), ensureOnHeap, iter.next());
            DecoratedKey key = partition.partitionKey();
            ClusteringIndexFilter filter = dataRange.clusteringIndexFilter(key);

            return filter.getUnfilteredRowIterator(columnFilter, partition);
        }
    }

    static class MemtablePartition extends ImmutableBTreePartition
    {

        private final EnsureOnHeap ensureOnHeap;

        private MemtablePartition(TableMetadata table, EnsureOnHeap ensureOnHeap, DecoratedKey key, BTreePartitionData data)
        {
            super(table, key, data);
            this.ensureOnHeap = ensureOnHeap;
        }

        @Override
        protected boolean canHaveShadowedData()
        {
            // The BtreePartitionData we store in the memtable are build iteratively by BTreePartitionData.add(), which
            // doesn't make sure there isn't shadowed data, so we'll need to eliminate any.
            return true;
        }


        @Override
        public DeletionInfo deletionInfo()
        {
            return ensureOnHeap.applyToDeletionInfo(super.deletionInfo());
        }

        @Override
        public Row staticRow()
        {
            return ensureOnHeap.applyToStatic(super.staticRow());
        }

        @Override
        public DecoratedKey partitionKey()
        {
            return ensureOnHeap.applyToPartitionKey(super.partitionKey());
        }

        @Override
        public Row getRow(Clustering<?> clustering)
        {
            return ensureOnHeap.applyToRow(super.getRow(clustering));
        }

        @Override
        public Row lastRow()
        {
            return ensureOnHeap.applyToRow(super.lastRow());
        }

        @Override
        public UnfilteredRowIterator unfilteredIterator(ColumnFilter selection, Slices slices, boolean reversed)
        {
            return unfilteredIterator(holder(), selection, slices, reversed);
        }

        @Override
        public UnfilteredRowIterator unfilteredIterator(ColumnFilter selection, NavigableSet<Clustering<?>> clusteringsInQueryOrder, boolean reversed)
        {
            return ensureOnHeap
                            .applyToPartition(super.unfilteredIterator(selection, clusteringsInQueryOrder, reversed));
        }

        @Override
        public UnfilteredRowIterator unfilteredIterator()
        {
            return unfilteredIterator(ColumnFilter.selection(super.columns()), Slices.ALL, false);
        }

        @Override
        public UnfilteredRowIterator unfilteredIterator(BTreePartitionData current, ColumnFilter selection, Slices slices, boolean reversed)
        {
            return ensureOnHeap
                            .applyToPartition(super.unfilteredIterator(current, selection, slices, reversed));
        }

        @Override
        public Iterator<Row> rowIterator()
        {
            return ensureOnHeap.applyToPartition(super.rowIterator());
        }
    }

    static class Factory implements Memtable.Factory
    {
        public Memtable create(AtomicReference<CommitLogPosition> commitLogLowerBound,
                               TableMetadataRef metadaRef,
                               Owner owner)
        {
            return new TrieMemtableStage1(commitLogLowerBound, metadaRef, owner);
        }

        @Override
        public TableMetrics.ReleasableMetric createMemtableMetrics(TableMetadataRef metadataRef)
        {
            TrieMemtableMetricsView metrics = TrieMemtableMetricsView.getOrCreate(metadataRef.keyspace, metadataRef.name);
            return metrics::release;
        }
    }

    @Override
    @VisibleForTesting
    public long unusedReservedOnHeapMemory()
    {
        long size = 0;
        for (MemtableShard shard : shards)
        {
            size += shard.data.unusedReservedOnHeapMemory();
            size += shard.allocator.unusedReservedOnHeapMemory();
        }
        return size;
    }
}
