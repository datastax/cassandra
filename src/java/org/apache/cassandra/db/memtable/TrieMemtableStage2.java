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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.MutableDeletionInfo;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.TrieBackedPartitionStage2;
import org.apache.cassandra.db.partitions.TriePartitionUpdateStage2;
import org.apache.cassandra.db.partitions.TriePartitionUpdaterStage2;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.tries.Direction;
import org.apache.cassandra.db.tries.InMemoryBaseTrie;
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
import org.apache.cassandra.io.sstable.SSTableReadsListener;
import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.metrics.TrieMemtableMetricsView;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.EnsureOnHeap;
import org.apache.cassandra.utils.memory.HeapCloner;
import org.apache.cassandra.utils.memory.MemtableAllocator;
import org.github.jamm.Unmetered;

/// Previous TrieMemtable implementation, provided for two reasons:
///
///   -  to easily compare current and earlier implementations of the trie memtable
///   -  to have an option to change a database back to the older implementation if we find a bug or a performance
///      problem with the new code.
///
///
/// To switch a table to this version, use
/// ```
///   ALTER TABLE ... WITH memtable = {'class': 'TrieMemtableStage2'}
/// ```
/// or add
/// ```
///   memtable:
///     class: TrieMemtableStage2
/// ```
/// in `cassandra.yaml` to switch a node to it as default.
public class TrieMemtableStage2 extends AbstractTrieMemtable
{
    private static final Logger logger = LoggerFactory.getLogger(TrieMemtableStage2.class);

    /** Buffer type to use for memtable tries (on- vs off-heap) */
    public static final BufferType BUFFER_TYPE = DatabaseDescriptor.getMemtableAllocationType().toBufferType();

    /**
     * Force copy checker (see InMemoryTrie.ApplyState) ensuring all modifications apply atomically and consistently to
     * the whole partition.
     */
    public static final Predicate<InMemoryBaseTrie.NodeFeatures<Object>> FORCE_COPY_PARTITION_BOUNDARY = features -> isPartitionBoundary(features.content());

    public static final Predicate<Object> IS_PARTITION_BOUNDARY = TrieMemtableStage2::isPartitionBoundary;

    /**
     * Sharded memtable sections. Each is responsible for a contiguous range of the token space (between boundaries[i]
     * and boundaries[i+1]) and is written to by one thread at a time, while reads are carried out concurrently
     * (including with any write).
     */
    private final MemtableShard[] shards;

    /**
     * A merged view of the memtable map. Used for partition range queries and flush.
     * For efficiency we serve single partition requests off the shard which offers more direct InMemoryTrie methods.
     */
    private final Trie<Object> mergedTrie;

    TrieMemtableStage2(AtomicReference<CommitLogPosition> commitLogLowerBound, TableMetadataRef metadataRef, Owner owner, Integer shardCountOption)
    {
        super(commitLogLowerBound, metadataRef, owner, shardCountOption);
        this.shards = generatePartitionShards(boundaries.shardCount(), metadataRef, metrics, owner.readOrdering());
        this.mergedTrie = makeMergedTrie(shards);
        logger.trace("Created memtable with {} shards", this.shards.length);
    }

    @Override
    protected AbstractMemtableShard[] getShards()
    {
        return shards;
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

    static boolean isPartitionBoundary(Object content)
    {
        // In the trie we use PartitionData for the root of a partition, but PartitionUpdates come with DeletionInfo.
        // Both are descendants of DeletionInfo.
        return content instanceof DeletionInfo;
    }

    @Override
    public UnfilteredPartitionIterator partitionIterator(final ColumnFilter columnFilter,
                                                         final DataRange dataRange,
                                                         SSTableReadsListener readsListener)
    {
        AbstractBounds<PartitionPosition> keyRange = dataRange.keyRange();

        boolean isBound = keyRange instanceof Bounds;
        boolean includeStart = isBound || keyRange instanceof IncludingExcludingBounds;
        boolean includeStop = isBound || keyRange instanceof Range;

        Trie<Object> subMap = mergedTrie.subtrie(toComparableBound(keyRange.left, includeStart),
                                                 toComparableBound(keyRange.right, !includeStop));

        TableMetadata metadata = metadata();
        return new MemtableUnfilteredPartitionIterator(metadata,
                                                       new PartitionIterator(subMap, metadata, allocator.ensureOnHeap()),
                                                       columnFilter,
                                                       dataRange,
                                                       getMinLocalDeletionTime());
        // Note: the minLocalDeletionTime reported by the iterator is the memtable's minLocalDeletionTime. This is okay
        // because we only need to report a lower bound that will eventually advance, and calculating a more precise
        // bound would be an unnecessary expense.
    }

    public Partition getPartition(DecoratedKey key)
    {
        int shardIndex = boundaries.getShardForKey(key);
        Trie<Object> trie = shards[shardIndex].data.tailTrie(key);
        return createPartition(metadata(), allocator.ensureOnHeap(), key, trie);
    }

    private static TrieBackedPartitionStage2 createPartition(TableMetadata metadata, EnsureOnHeap ensureOnHeap, DecoratedKey key, Trie<Object> trie)
    {
        if (trie == null)
            return null;
        PartitionData holder = (PartitionData) trie.get(ByteComparable.EMPTY);
        // If we found a matching path in the trie, it must be the root of this partition (because partition keys are
        // prefix-free, it can't be a prefix for a different path, or have another partition key as prefix) and contain
        // PartitionData (because the attachment of a new or modified partition to the trie is atomic).
        assert holder != null : "Entry for " + key + " without associated PartitionData";

        return TrieBackedPartitionStage2.create(key,
                                                holder.columns(),
                                                holder.stats(),
                                                holder.rowCountIncludingStatic(),
                                                trie,
                                                metadata,
                                                ensureOnHeap);
    }

    @Override
    public UnfilteredRowIterator rowIterator(DecoratedKey key, Slices slices, ColumnFilter selectedColumns, boolean reversed, SSTableReadsListener listener)
    {
        Partition p = getPartition(key);
        if (p == null)
            return null;
        else
            return p.unfilteredIterator(selectedColumns, slices, reversed);
    }

    @Override
    public UnfilteredRowIterator rowIterator(DecoratedKey key)
    {
        Partition p = getPartition(key);
        return p != null ? p.unfilteredIterator() : null;
    }



    /**
     * Metadata object signifying the root node of a partition. Holds the deletion information as well as a link
     * to the owning subrange, which is used for compiling statistics and column sets.
     *
     * Descends from MutableDeletionInfo to permit tail tries to be passed directly to TrieBackedPartitionStage2.
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
                                                             TrieBackedPartitionStage2.BYTE_COMPARABLE_VERSION,
                                                             metadata().partitioner);
            keySize += keyBytes.length;
        }
    }

    public FlushablePartitionSet<TrieBackedPartitionStage2> getFlushSet(PartitionPosition from, PartitionPosition to)
    {
        Trie<Object> toFlush = mergedTrie.subtrie(toComparableBound(from, true), toComparableBound(to, true));

        var counter = new KeySizeAndCountCollector(); // need to jump over tails keys
        toFlush.processSkippingBranches(Direction.FORWARD, counter);
        int partitionCount = counter.keyCount;
        long partitionKeySize = counter.keySize;

        return new AbstractFlushablePartitionSet<TrieBackedPartitionStage2>()
        {
            public Memtable memtable()
            {
                return TrieMemtableStage2.this;
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

            public Iterator<TrieBackedPartitionStage2> iterator()
            {
                return new PartitionIterator(toFlush, metadata(), EnsureOnHeap.NOOP);
            }

            public long partitionKeysSize()
            {
                return partitionKeySize;
            }
        };
    }

    public static class MemtableShard extends AbstractMemtableShard<InMemoryTrie<Object>, TriePartitionUpdaterStage2>
    {
        MemtableShard(TableMetadataRef metadata, TrieMemtableMetricsView metrics, OpOrder opOrder)
        {
            this(metadata, AbstractAllocatorMemtable.MEMORY_POOL.newAllocator(metadata.toString()), metrics, opOrder);
        }

        @VisibleForTesting
        MemtableShard(TableMetadataRef metadata, MemtableAllocator allocator, TrieMemtableMetricsView metrics, OpOrder opOrder)
        {
            super(metadata, allocator, metrics, opOrder, 
                  InMemoryTrie.longLived(TrieBackedPartitionStage2.BYTE_COMPARABLE_VERSION, BUFFER_TYPE, opOrder));
        }

        @Override
        protected TriePartitionUpdaterStage2 createUpdater(PartitionUpdate update, UpdateTransaction indexer, OpOrder.Group opGroup)
        {
            return new TriePartitionUpdaterStage2(allocator.cloner(opGroup), indexer, metadata.get(), this);
        }

        @Override
        protected void applyUpdate(TriePartitionUpdaterStage2 updater, PartitionUpdate update) throws TrieSpaceExhaustedException
        {
            updater.apply(data, TriePartitionUpdateStage2.asMergableTrie(update));
        }

        @Override
        protected int getUpdaterPartitionsAdded(TriePartitionUpdaterStage2 updater)
        {
            return updater.partitionsAdded;
        }

        @Override
        protected DecoratedKey firstPartitionKey(Direction direction)
        {
            Iterator<Map.Entry<ByteComparable.Preencoded, PartitionData>> iter = data.filteredEntryIterator(direction, PartitionData.class);
            if (!iter.hasNext())
                return null;

            Map.Entry<ByteComparable.Preencoded, PartitionData> entry = iter.next();
            return getPartitionKeyFromPath(metadata.get(), entry.getKey(), TrieBackedPartitionStage2.BYTE_COMPARABLE_VERSION);
        }
    }

    static class PartitionIterator extends TrieTailsIterator.Plain<Object, TrieBackedPartitionStage2>
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
        protected TrieBackedPartitionStage2 mapContent(Object content, Trie<Object> tailTrie, byte[] bytes, int byteLength)
        {
            PartitionData pd = (PartitionData) content;
            DecoratedKey key = AbstractTrieMemtable.getPartitionKeyFromPath(metadata,
                                                                            ByteComparable.preencoded(TrieBackedPartitionStage2.BYTE_COMPARABLE_VERSION,
                                                                                 bytes, 0, byteLength),
                                                                            TrieBackedPartitionStage2.BYTE_COMPARABLE_VERSION);
            return TrieBackedPartitionStage2.create(key,
                                              pd.columns(),
                                              pd.stats(),
                                              pd.rowCountIncludingStatic(),
                                              tailTrie,
                                              metadata,
                                              ensureOnHeap);
        }
    }

    public static Memtable.Factory factory(Map<String, String> optionsCopy)
    {
        String shardsString = optionsCopy.remove(SHARDS_OPTION);
        Integer shardCount = shardsString != null ? Integer.parseInt(shardsString) : null;
        return new Factory(shardCount);
    }


    static class Factory implements Memtable.Factory
    {
        final Integer shardCount;

        Factory(Integer shardCount)
        {
            this.shardCount = shardCount;
        }

        public Memtable create(AtomicReference<CommitLogPosition> commitLogLowerBound,
                               TableMetadataRef metadaRef,
                               Owner owner)
        {
            return new TrieMemtableStage2(commitLogLowerBound, metadaRef, owner, shardCount);
        }

        @Override
        public PartitionUpdate.Factory partitionUpdateFactory()
        {
            return TriePartitionUpdateStage2.FACTORY;
        }

        @Override
        public TableMetrics.ReleasableMetric createMemtableMetrics(TableMetadataRef metadataRef)
        {
            TrieMemtableMetricsView metrics = TrieMemtableMetricsView.getOrCreate(metadataRef.keyspace, metadataRef.name);
            return metrics::release;
        }
    }
}
