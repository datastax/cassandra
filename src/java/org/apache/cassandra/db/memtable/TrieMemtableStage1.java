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
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.PartitionPosition;
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
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
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
import org.apache.cassandra.io.sstable.SSTableReadsListener;
import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.metrics.TrieMemtableMetricsView;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.EnsureOnHeap;
import org.apache.cassandra.utils.memory.MemtableAllocator;

/// Trie memtable implementation. Improves memory usage, garbage collection efficiency and lookup performance.
/// The implementation is described in detail in the paper:
///       https://www.vldb.org/pvldb/vol15/p3359-lambov.pdf
///
/// The configuration takes a single parameter:
/// - shards: the number of shards to split into, defaulting to the number of CPU cores.
///
/// Also see [Memtable_API.md](./Memtable_API.md).
///
/// This is stage 1 of the TrieMemtable implementation, provided for two reasons:
///
///   -  to easily compare current and earlier implementations of the trie memtable
///   -  to have an option to change a database back to the older implementation if we find a bug or a performance
///      problem with the new code.
///
///
/// To switch a table to this version, use
/// ```
///   ALTER TABLE ... WITH memtable = {'class': 'TrieMemtableStage1'}
/// ```
/// or add
/// ```
///   memtable:
///     class: TrieMemtableStage1
/// ```
/// in `cassandra.yaml` to switch a node to it as default.
///
public class TrieMemtableStage1 extends AbstractTrieMemtable
{
    private static final Logger logger = LoggerFactory.getLogger(TrieMemtableStage1.class);

    public static final Memtable.Factory FACTORY = new TrieMemtableStage1.Factory();

    static final ByteComparable.Version BYTE_COMPARABLE_VERSION = ByteComparable.Version.OSS41;

    /** If keys is below this length, we will use a recursive procedure for inserting data in the memtable trie. */
    @VisibleForTesting
    public static final int MAX_RECURSIVE_KEY_LENGTH = 128;

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

    // only to be used by init(), to setup the very first memtable for the cfs
    TrieMemtableStage1(AtomicReference<CommitLogPosition> commitLogLowerBound, TableMetadataRef metadataRef, Owner owner)
    {
        this(commitLogLowerBound, metadataRef, owner, null);
    }

    TrieMemtableStage1(AtomicReference<CommitLogPosition> commitLogLowerBound, TableMetadataRef metadataRef, Owner owner, Integer shardCountOption)
    {
        super(commitLogLowerBound, metadataRef, owner, shardCountOption);
        this.shards = generatePartitionShards(this.boundaries.shardCount(), metadataRef, this.metrics);
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

    @Override
    protected MemtableShard[] getShards()
    {
        return shards;
    }

    @Override
    public UnfilteredRowIterator rowIterator(DecoratedKey key, Slices slices, ColumnFilter columnFilter, boolean reversed, SSTableReadsListener listener)
    {
        Partition p = getPartition(key);
        if (p == null)
            return null;
        else
            return p.unfilteredIterator(columnFilter, slices, reversed);
    }

    @Override
    public UnfilteredRowIterator rowIterator(DecoratedKey key)
    {
        Partition p = getPartition(key);
        return p != null ? p.unfilteredIterator() : null;
    }

    @Override
    public UnfilteredPartitionIterator partitionIterator(final ColumnFilter columnFilter,
                                                         final DataRange dataRange,
                                                         SSTableReadsListener readsListener)
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

        Trie<BTreePartitionData> subMap = mergedTrie.subtrie(toComparableBound(left, includeStart),
                                                             toComparableBound(right, !includeStop));

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

    public FlushablePartitionSet<MemtablePartition> getFlushSet(PartitionPosition from, PartitionPosition to)
    {
        Trie<BTreePartitionData> toFlush = mergedTrie.subtrie(toComparableBound(from, true), toComparableBound(to, true));
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

        return new AbstractFlushablePartitionSet<>()
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

            public long partitionKeysSize()
            {
                return partitionKeySize;
            }
        };
    }

    static class MemtableShard extends AbstractMemtableShard<InMemoryTrie<BTreePartitionData>, BTreePartitionUpdater>
    {

        MemtableShard(TableMetadataRef metadata, TrieMemtableMetricsView metrics)
        {
            this(metadata, AbstractAllocatorMemtable.MEMORY_POOL.newAllocator(metadata.toString()), metrics);
        }

        @VisibleForTesting
        MemtableShard(TableMetadataRef metadata, MemtableAllocator allocator, TrieMemtableMetricsView metrics)
        {
            super(metadata, allocator, metrics, InMemoryTrie.shortLived(BYTE_COMPARABLE_VERSION, TrieMemtable.BUFFER_TYPE));
        }

        @Override
        protected int getUpdaterPartitionsAdded(BTreePartitionUpdater updater)
        {
            return updater.partitionsAdded;
        }

        @Override
        protected BTreePartitionUpdater createUpdater(PartitionUpdate update, UpdateTransaction indexer, OpOrder.Group opGroup)
        {
            return new BTreePartitionUpdater(allocator, allocator.cloner(opGroup), opGroup, indexer);
        }

        @Override
        protected void applyUpdate(BTreePartitionUpdater updater, PartitionUpdate update) throws TrieSpaceExhaustedException
        {
            DecoratedKey key = update.partitionKey();
            data.putSingleton(key,
                              BTreePartitionUpdate.asBTreeUpdate(update),
                              updater::mergePartitions,
                              key.getKeyLength() < MAX_RECURSIVE_KEY_LENGTH);
        }

        @Override
        protected DecoratedKey firstPartitionKey(Direction direction)
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

        public long getMinLocalDeletionTime()
        {
            long minLocalDeletionTime = Long.MAX_VALUE;
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
}
