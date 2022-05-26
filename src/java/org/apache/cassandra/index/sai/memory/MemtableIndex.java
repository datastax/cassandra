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

package org.apache.cassandra.index.sai.memory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.LongAdder;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.memtable.ShardBoundaries;
import org.apache.cassandra.db.memtable.TrieMemtable;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.PrimaryKeys;
import org.apache.cassandra.index.sai.utils.RangeConcatIterator;
import org.apache.cassandra.index.sai.utils.RangeIterator;
import org.apache.cassandra.utils.MergeIterator;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.Reducer;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.concurrent.OpOrder;

import static java.util.function.Function.identity;

public class MemtableIndex
{
    private final ShardBoundaries boundaries;
    private final MemoryIndex[] rangeIndexes;
    private final AbstractType<?> validator;
    private final ClusteringComparator clusteringComparator;
    private final LongAdder writeCount = new LongAdder();
    private final LongAdder estimatedOnHeapMemoryUsed = new LongAdder();
    private final LongAdder estimatedOffHeapMemoryUsed = new LongAdder();

    public MemtableIndex(IndexContext indexContext)
    {
        this.boundaries = indexContext.owner().localRangeSplits(TrieMemtable.SHARD_COUNT);
        this.rangeIndexes = new MemoryIndex[boundaries.shardCount()];
        this.validator = indexContext.getValidator();
        this.clusteringComparator = indexContext.comparator();
        for (int shard = 0; shard < boundaries.shardCount(); shard++)
        {
            this.rangeIndexes[shard] = new MultiBlockIndex(indexContext);
        }
    }

    public long writeCount()
    {
        return writeCount.sum();
    }

    public long estimatedOnHeapMemoryUsed()
    {
        return estimatedOnHeapMemoryUsed.sum();
    }

    public long estimatedOffHeapMemoryUsed()
    {
        return estimatedOffHeapMemoryUsed.sum();
    }

    public boolean isEmpty()
    {
        return getMinTerm() == null;
    }

    public ByteBuffer getMinTerm()
    {
        return Arrays.stream(rangeIndexes)
                     .map(MemoryIndex::getMinTerm)
                     .filter(Objects::nonNull)
                     .min(Comparator.comparing(identity(), validator))
                     .orElse(null);
    }

    public ByteBuffer getMaxTerm()
    {
        return Arrays.stream(rangeIndexes)
                     .map(MemoryIndex::getMaxTerm)
                     .filter(Objects::nonNull)
                     .max(Comparator.comparing(identity(), validator))
                     .orElse(null);
    }

    public void index(DecoratedKey key, Clustering clustering, ByteBuffer value, Memtable memtable, OpOrder.Group opGroup)
    {
        if (value == null || value.remaining() == 0)
            return;

        rangeIndexes[boundaries.getShardForKey(key)].add(key,
                                                         clustering,
                                                         value,
                                                         allocatedBytes -> {
                                                             memtable.markExtraOnHeapUsed(allocatedBytes, opGroup);
                                                             estimatedOnHeapMemoryUsed.add(allocatedBytes);
                                                         },
                                                         allocatedBytes -> {
                                                             memtable.markExtraOffHeapUsed(allocatedBytes, opGroup);
                                                             estimatedOffHeapMemoryUsed.add(allocatedBytes);
                                                         });
        writeCount.increment();
    }

    public RangeIterator search(Expression expression, AbstractBounds<PartitionPosition> keyRange)
    {
        RangeConcatIterator.Builder builder = RangeConcatIterator.builder();

        for (MemoryIndex index : rangeIndexes)
        {
            builder.add(index.search(expression, keyRange));
        }
        return builder.build();
    }

    /**
     * NOTE: returned data may contain partition key not within the provided min and max which are only used to find
     * corresponding subranges. We don't do filtering here to avoid unnecessary token comparison. In case of JBOD,
     * min/max should align exactly at token boundaries. In case of tiered-storage, keys within min/max may not
     * belong to the given sstable.
     *
     * @param min minimum partition key used to find min subrange
     * @param max maximum partition key used to find max subrange
     *
     * @return iterator of indexed term to primary keys mapping in sorted by indexed term and primary key.
     */
    public Iterator<Pair<ByteComparable, Iterator<ByteComparable>>> iterator(DecoratedKey min, DecoratedKey max)
    {
        int minSubrange = min == null ? 0 : boundaries.getShardForKey(min);
        int maxSubrange = max == null ? rangeIndexes.length - 1 : boundaries.getShardForKey(max);

        List<Iterator<Pair<ByteComparable, Iterable<ByteComparable>>>> rangeLists = new ArrayList<>(maxSubrange - minSubrange + 1);
        for (int i = minSubrange; i <= maxSubrange; i++)
            rangeLists.add(rangeIndexes[i].iterator());

        return MergeIterator.get(rangeLists, (o1, o2) -> ByteComparable.compare(o1.left, o2.left, ByteComparable.Version.OSS41),
                                 new PrimaryKeysMergeReducer(rangeIndexes.length, clusteringComparator));
    }

    private static class PrimaryKeysMergeReducer extends Reducer<Pair<ByteComparable, Iterable<ByteComparable>>, Pair<ByteComparable, Iterator<ByteComparable>>>
    {
        private final Pair<ByteComparable, Iterable<ByteComparable>>[] toMerge;
        private final int size;
        private final Comparator<ByteComparable> comparator;

        @SuppressWarnings("unchecked")
        PrimaryKeysMergeReducer(int size, ClusteringComparator clusteringComparator)
        {
            this.toMerge = new Pair[size];
            this.size = size;
            this.comparator = (o1, o2) -> ByteComparable.compare(o1, o2, ByteComparable.Version.OSS41);
        }

        @Override
        public void reduce(int idx, Pair<ByteComparable, Iterable<ByteComparable>> current)
        {
            toMerge[idx] = current;
        }

        @Override
        public Pair<ByteComparable, Iterator<ByteComparable>> getReduced()
        {
            ByteComparable term = getTerm();

            List<Iterator<ByteComparable>> keyIterators = new ArrayList<>(size);
            for (Pair<ByteComparable, Iterable<ByteComparable>> p : toMerge)
                if (p != null && p.right != null && p.right.iterator().hasNext())
                    keyIterators.add(p.right.iterator());

            Iterator<ByteComparable> primaryKeys = MergeIterator.get(keyIterators, comparator, Reducer.getIdentity());
            return Pair.create(term, primaryKeys);
        }

        @Override
        public void onKeyChange()
        {
            for (int i = 0; i < size; i++)
                toMerge[i] = null;
        }

        private ByteComparable getTerm()
        {
            for (Pair<ByteComparable, Iterable<ByteComparable>> p : toMerge)
            {
                if (p != null)
                    return p.left;
            }
            throw new IllegalStateException("Term must exist in IndexMemtable.");
        }
    }
}
