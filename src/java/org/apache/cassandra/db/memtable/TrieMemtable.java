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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.MutableDeletionInfo;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.AbstractUnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.TrieBackedPartition;
import org.apache.cassandra.db.partitions.TriePartitionUpdate;
import org.apache.cassandra.db.partitions.TriePartitionUpdater;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.tries.Direction;
import org.apache.cassandra.db.tries.InMemoryTrie;
import org.apache.cassandra.db.tries.Trie;
import org.apache.cassandra.db.tries.TrieEntriesWalker;
import org.apache.cassandra.db.tries.TrieSpaceExhaustedException;
import org.apache.cassandra.db.tries.TrieTailsIterator;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.IncludingExcludingBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.metrics.TrieMemtableMetricsView;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MBeanWrapper;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.EnsureOnHeap;
import org.apache.cassandra.utils.memory.HeapCloner;
import org.apache.cassandra.utils.memory.MemtableAllocator;
import org.github.jamm.Unmetered;

public class TrieMemtable extends AbstractAllocatorMemtable
{
    private static final Logger logger = LoggerFactory.getLogger(TrieMemtable.class);
    public static final String TRIE_MEMTABLE_CONFIG_OBJECT_NAME = "org.apache.cassandra.db:type=TrieMemtableConfig";

    public static final Factory FACTORY = new TrieMemtable.Factory();

    /** Buffer type to use for memtable tries (on- vs off-heap) */
    public static final BufferType BUFFER_TYPE;

    static
    {
        switch (DatabaseDescriptor.getMemtableAllocationType())
        {
        case unslabbed_heap_buffers:
        case heap_buffers:
            BUFFER_TYPE = BufferType.ON_HEAP;
            break;
        case offheap_buffers:
        case offheap_objects:
            BUFFER_TYPE = BufferType.OFF_HEAP;
            break;
        default:
            throw new AssertionError();
        }

        MBeanWrapper.instance.registerMBean(new TrieMemtableConfig(), TRIE_MEMTABLE_CONFIG_OBJECT_NAME, MBeanWrapper.OnException.LOG);
    }

    /**
     * Force copy checker (see InMemoryTrie.ApplyState) ensuring all modifications apply atomically and consistently to
     * the whole partition.
     */
    public static final Predicate<InMemoryTrie.NodeFeatures<Object>> FORCE_COPY_PARTITION_BOUNDARY = features -> isPartitionBoundary(features.content());

    public static final Predicate<Object> IS_PARTITION_BOUNDARY = TrieMemtable::isPartitionBoundary;

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
    private final Trie<Object> mergedTrie;

    @Unmetered
    private final TrieMemtableMetricsView metrics;

    /**
     * Keeps an estimate of the average row size in this memtable, computed from a small sample of rows.
     * Because computing this estimate is potentially costly, as it requires iterating the rows,
     * the estimate is updated only whenever the number of operations on the memtable increases significantly from the
     * last update. This estimate is not very accurate but should be ok for planning or diagnostic purposes.
     */
    private volatile MemtableAverageRowSize estimatedAverageRowSize;

    @VisibleForTesting
    public static final String SHARD_COUNT_PROPERTY = "cassandra.trie.memtable.shard.count";

    public static volatile int SHARD_COUNT = Integer.getInteger(SHARD_COUNT_PROPERTY, autoShardCount());

    private static int autoShardCount()
    {
        return 4 * FBUtilities.getAvailableProcessors();
    }

    // only to be used by init(), to setup the very first memtable for the cfs
    TrieMemtable(AtomicReference<CommitLogPosition> commitLogLowerBound, TableMetadataRef metadataRef, Owner owner)
    {
        super(commitLogLowerBound, metadataRef, owner);
        this.boundaries = owner.localRangeSplits(SHARD_COUNT);
        this.metrics = TrieMemtableMetricsView.getOrCreate(metadataRef.keyspace, metadataRef.name);
        this.shards = generatePartitionShards(boundaries.shardCount(), metadataRef, metrics, owner.readOrdering());
        this.mergedTrie = makeMergedTrie(shards);
        logger.trace("Created memtable with {} shards", this.shards.length);
    }

    private static MemtableShard[] generatePartitionShards(int splits,
                                                           TableMetadataRef metadata,
                                                           TrieMemtableMetricsView metrics,
                                                           OpOrder opOrder)
    {
        if (splits == 1)
            return new MemtableShard[] { new MemtableShard(metadata, metrics, opOrder) };

        MemtableShard[] partitionMapContainer = new MemtableShard[splits];
        for (int i = 0; i < splits; i++)
            partitionMapContainer[i] = new MemtableShard(metadata, metrics, opOrder);

        return partitionMapContainer;
    }

    private static Trie<Object> makeMergedTrie(MemtableShard[] shards)
    {
        List<Trie<Object>> tries = new ArrayList<>(shards.length);
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
        long colUpdateTimeDelta = shard.put(update, indexer, opGroup);

        if (shard.data.reachedAllocatedSizeThreshold() && !switchRequested.getAndSet(true))
        {
            logger.info("Scheduling flush due to trie size limit reached.");
            owner.signalFlushRequired(this, ColumnFamilyStore.FlushReason.MEMTABLE_LIMIT);
        }

        return colUpdateTimeDelta;
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

    public int getMinLocalDeletionTime()
    {
        int min = Integer.MAX_VALUE;
        for (MemtableShard shard : shards)
            min =  EncodingStats.mergeMinLocalDeletionTime(min, shard.stats);
        return min;
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

    static boolean isPartitionBoundary(Object content)
    {
        // In the trie we use PartitionData for the root of a partition, but PartitionUpdates come with DeletionInfo.
        // Both are descendants of DeletionInfo.
        return content instanceof DeletionInfo;
    }

    public MemtableUnfilteredPartitionIterator makePartitionIterator(final ColumnFilter columnFilter, final DataRange dataRange)
    {
        AbstractBounds<PartitionPosition> keyRange = dataRange.keyRange();

        boolean isBound = keyRange instanceof Bounds;
        boolean includeStart = isBound || keyRange instanceof IncludingExcludingBounds;
        boolean includeStop = isBound || keyRange instanceof Range;

        Trie<Object> subMap = mergedTrie.subtrie(toComparableBound(keyRange.left, includeStart),
                                                 toComparableBound(keyRange.right, !includeStop));

        return new MemtableUnfilteredPartitionIterator(metadata(),
                                                       allocator.ensureOnHeap(),
                                                       subMap,
                                                       columnFilter,
                                                       dataRange,
                                                       getMinLocalDeletionTime());
        // Note: the minLocalDeletionTime reported by the iterator is the memtable's minLocalDeletionTime. This is okay
        // because we only need to report a lower bound that will eventually advance, and calculating a more precise
        // bound would be an unnecessary expense.
    }

    private static ByteComparable toComparableBound(PartitionPosition position, boolean before)
    {
        return position.isMinimum() ? null : position.asComparableBound(before);
    }

    public Partition getPartition(DecoratedKey key)
    {
        int shardIndex = boundaries.getShardForKey(key);
        Trie<Object> trie = shards[shardIndex].data.tailTrie(key);
        return createPartition(metadata(), allocator.ensureOnHeap(), key, trie);
    }

    private static TrieBackedPartition createPartition(TableMetadata metadata, EnsureOnHeap ensureOnHeap, DecoratedKey key, Trie<Object> trie)
    {
        if (trie == null)
            return null;
        PartitionData holder = (PartitionData) trie.get(ByteComparable.EMPTY);
        // If we found a matching path in the trie, it must be the root of this partition (because partition keys are
        // prefix-free, it can't be a prefix for a different path, or have another partition key as prefix) and contain
        // PartitionData (because the attachment of a new or modified partition to the trie is atomic).
        assert holder != null : "Entry for " + key + " without associated PartitionData";

        return TrieBackedPartition.create(key,
                                          holder.columns(),
                                          holder.stats(),
                                          holder.rowCountIncludingStatic(),
                                          trie,
                                          metadata,
                                          ensureOnHeap);
    }

    private static DecoratedKey getPartitionKeyFromPath(TableMetadata metadata, ByteComparable path)
    {
        return BufferDecoratedKey.fromByteComparable(path,
                                                     TrieBackedPartition.BYTE_COMPARABLE_VERSION,
                                                     metadata.partitioner);
    }

    /**
     * Metadata object signifying the root node of a partition. Holds the deletion information as well as a link
     * to the owning subrange, which is used for compiling statistics and column sets.
     *
     * Descends from MutableDeletionInfo to permit tail tries to be passed directly to TrieBackedPartition.
     */
    public static class PartitionData extends MutableDeletionInfo
    {
        @Unmetered
        public final MemtableShard owner;

        private int rowCountIncludingStatic;

        public static final long HEAP_SIZE = ObjectSizes.measure(new PartitionData(DeletionInfo.LIVE, null));

        public PartitionData(DeletionInfo deletion,
                             MemtableShard owner)
        {
            super(deletion.getPartitionDeletion(), deletion.copyRanges(HeapCloner.instance));
            this.owner = owner;
            this.rowCountIncludingStatic = 0;
        }

        public PartitionData(PartitionData existing,
                             DeletionInfo update)
        {
            // Start with the update content, to properly copy it
            this(update, existing.owner);
            rowCountIncludingStatic = existing.rowCountIncludingStatic;
            add(existing);
        }

        public RegularAndStaticColumns columns()
        {
            return owner.columns;
        }

        public EncodingStats stats()
        {
            return owner.stats;
        }

        public int rowCountIncludingStatic()
        {
            return rowCountIncludingStatic;
        }

        public void markInsertedRows(int howMany)
        {
            rowCountIncludingStatic += howMany;
        }

        @Override
        public String toString()
        {
            return "partition " + super.toString();
        }

        @Override
        public long unsharedHeapSize()
        {
            return super.unsharedHeapSize() + HEAP_SIZE - MutableDeletionInfo.EMPTY_SIZE;
        }
    }


    class KeySizeAndCountCollector extends TrieEntriesWalker<Object, Void>
    {
        long keySize = 0;
        int keyCount = 0;

        @Override
        public Void complete()
        {
            return null;
        }

        @Override
        protected void content(Object content, byte[] bytes, int byteLength)
        {
            // This is used with processSkippingBranches which should ensure that we only see the partition roots.
            assert content instanceof PartitionData;
            ++keyCount;
            byte[] keyBytes = DecoratedKey.keyFromByteSource(ByteSource.preencoded(bytes, 0, byteLength),
                                                             TrieBackedPartition.BYTE_COMPARABLE_VERSION,
                                                             metadata().partitioner);
            keySize += keyBytes.length;
        }
    }

    public FlushCollection<TrieBackedPartition> getFlushSet(PartitionPosition from, PartitionPosition to)
    {
        Trie<Object> toFlush = mergedTrie.subtrie(from, true, to, false);

        var counter = new KeySizeAndCountCollector(); // need to jump over tails keys
        toFlush.processSkippingBranches(counter, Direction.FORWARD);
        int partitionCount = counter.keyCount;
        long partitionKeySize = counter.keySize;

        return new AbstractFlushCollection<TrieBackedPartition>()
        {
            public Memtable memtable()
            {
                return TrieMemtable.this;
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

            public Iterator<TrieBackedPartition> iterator()
            {
                return new PartitionIterator(toFlush, metadata(), EnsureOnHeap.NOOP);
            }

            public long partitionKeySize()
            {
                return partitionKeySize;
            }
        };
    }

    public static class MemtableShard
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
        // several threads may read from it and iterate over it. Iterators (especially partition range iterators)
        // may operate for a long period of time and thus iterators should not throw ConcurrentModificationExceptions
        // if the underlying map is modified during iteration, they should provide a weakly consistent view of the map
        // instead.
        //
        // Also, this data is backed by memtable memory, when accessing it callers must specify if it can be accessed
        // unsafely, meaning that the memtable will not be discarded as long as the data is used, or whether the data
        // should be copied on heap for off-heap allocators.
        @VisibleForTesting
        final InMemoryTrie<Object> data;

        RegularAndStaticColumns columns;

        EncodingStats stats;

        private final MemtableAllocator allocator;

        @Unmetered
        private final TrieMemtableMetricsView metrics;

        private final TableMetadataRef metadata;

        MemtableShard(TableMetadataRef metadata, TrieMemtableMetricsView metrics, OpOrder opOrder)
        {
            this(metadata, AbstractAllocatorMemtable.MEMORY_POOL.newAllocator(), metrics, opOrder);
        }

        @VisibleForTesting
        MemtableShard(TableMetadataRef metadata, MemtableAllocator allocator, TrieMemtableMetricsView metrics, OpOrder opOrder)
        {
            this.metadata = metadata;
            this.data = InMemoryTrie.longLived(TrieBackedPartition.BYTE_COMPARABLE_VERSION, BUFFER_TYPE, opOrder);
            this.columns = RegularAndStaticColumns.NONE;
            this.stats = EncodingStats.NO_STATS;
            this.allocator = allocator;
            this.metrics = metrics;
        }

        public long put(PartitionUpdate update, UpdateTransaction indexer, OpOrder.Group opGroup)
        {
            TriePartitionUpdater updater = new TriePartitionUpdater(allocator.cloner(opGroup), indexer, metadata.get(), this);
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
                    indexer.start();
                    // Add the initial trie size on the first operation. This technically isn't correct (other shards
                    // do take their memory share even if they are empty) but doing it during construction may cause
                    // the allocator to block while we are trying to flush a memtable and become a deadlock.
                    long onHeap = data.isEmpty() ? 0 : data.usedSizeOnHeap();
                    long offHeap = data.isEmpty() ? 0 : data.usedSizeOffHeap();
                    // Use the fast recursive put if we know the key is small enough to not cause a stack overflow.
                    try
                    {
                        data.apply(TriePartitionUpdate.asMergableTrie(update),
                                   updater,
                                   FORCE_COPY_PARTITION_BOUNDARY);
                    }
                    catch (TrieSpaceExhaustedException e)
                    {
                        // This should never really happen as a flush would be triggered long before this limit is reached.
                        throw new AssertionError(e);
                    }
                    allocator.offHeap().adjust(data.usedSizeOffHeap() - offHeap, opGroup);
                    allocator.onHeap().adjust((data.usedSizeOnHeap() - onHeap) + updater.heapSize, opGroup);
                    partitionCount += updater.partitionsAdded;
                }
                finally
                {
                    indexer.commit();
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
            Iterator<Map.Entry<ByteComparable, PartitionData>> iter = data.filteredEntryIterator(direction, PartitionData.class);
            if (!iter.hasNext())
                return null;

            Map.Entry<ByteComparable, PartitionData> entry = iter.next();
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

    static class PartitionIterator extends TrieTailsIterator<Object, TrieBackedPartition>
    {
        final TableMetadata metadata;
        final EnsureOnHeap ensureOnHeap;
        PartitionIterator(Trie<Object> source, TableMetadata metadata, EnsureOnHeap ensureOnHeap)
        {
            super(source, Direction.FORWARD, PartitionData.class::isInstance);
            this.metadata = metadata;
            this.ensureOnHeap = ensureOnHeap;
        }

        @Override
        protected TrieBackedPartition mapContent(Object content, Trie<Object> tailTrie, byte[] bytes, int byteLength)
        {
            PartitionData pd = (PartitionData) content;
            DecoratedKey key = getPartitionKeyFromPath(metadata,
                                                       ByteComparable.preencoded(TrieBackedPartition.BYTE_COMPARABLE_VERSION,
                                                                                 bytes, 0, byteLength));
            return TrieBackedPartition.create(key,
                                              pd.columns(),
                                              pd.stats(),
                                              pd.rowCountIncludingStatic(),
                                              tailTrie,
                                              metadata,
                                              ensureOnHeap);
        }
    }

    static class MemtableUnfilteredPartitionIterator extends AbstractUnfilteredPartitionIterator implements Memtable.MemtableUnfilteredPartitionIterator
    {
        private final TableMetadata metadata;
        private final Iterator<TrieBackedPartition> iter;
        private final ColumnFilter columnFilter;
        private final DataRange dataRange;
        private final int minLocalDeletionTime;

        public MemtableUnfilteredPartitionIterator(TableMetadata metadata,
                                                   EnsureOnHeap ensureOnHeap,
                                                   Trie<Object> source,
                                                   ColumnFilter columnFilter,
                                                   DataRange dataRange,
                                                   int minLocalDeletionTime)
        {
            this.iter = new PartitionIterator(source, metadata, ensureOnHeap);
            this.metadata = metadata;
            this.columnFilter = columnFilter;
            this.dataRange = dataRange;
            this.minLocalDeletionTime = minLocalDeletionTime;
        }

        public int getMinLocalDeletionTime()
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

    static class Factory implements Memtable.Factory
    {
        public Memtable create(AtomicReference<CommitLogPosition> commitLogLowerBound,
                               TableMetadataRef metadaRef,
                               Owner owner)
        {
            return new TrieMemtable(commitLogLowerBound, metadaRef, owner);
        }

        @Override
        public PartitionUpdate.Factory partitionUpdateFactory()
        {
            return TriePartitionUpdate.FACTORY;
        }

        @Override
        public TableMetrics.ReleasableMetric createMemtableMetrics(TableMetadataRef metadataRef)
        {
            TrieMemtableMetricsView metrics = TrieMemtableMetricsView.getOrCreate(metadataRef.keyspace, metadataRef.name);
            return metrics::release;
        }
    }

    @Override
    public long unusedReservedOnHeapMemory()
    {
        long size = 0;
        for (MemtableShard shard : shards)
        {
            size += shard.data.unusedReservedOnHeapMemory();
            size += shard.allocator.unusedReservedOnHeapMemory();
        }
        size += this.allocator.unusedReservedOnHeapMemory();
        return size;
    }

    /**
     * Release all recycled content references, including the ones waiting in still incomplete recycling lists.
     * This is a test method and can cause null pointer exceptions if used on a live trie.
     */
    @VisibleForTesting
    void releaseReferencesUnsafe()
    {
        for (MemtableShard shard : shards)
            shard.data.releaseReferencesUnsafe();
    }

    @VisibleForTesting
    public static class TrieMemtableConfig implements TrieMemtableConfigMXBean
    {
        @Override
        public void setShardCount(String shardCount)
        {
            if ("auto".equalsIgnoreCase(shardCount))
            {
                SHARD_COUNT = autoShardCount();
            }
            else
            {
                try
                {
                    SHARD_COUNT = Integer.valueOf(shardCount);
                }
                catch (NumberFormatException ex)
                {
                    logger.warn("Unable to parse {} as valid value for shard count; leaving it as {}",
                                shardCount, SHARD_COUNT);
                    return;
                }
            }
            logger.info("Requested setting shard count to {}; set to: {}", shardCount, SHARD_COUNT);
        }

        @Override
        public String getShardCount()
        {
            return "" + SHARD_COUNT;
        }
    }

}
