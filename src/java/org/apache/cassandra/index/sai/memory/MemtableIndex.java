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
import java.util.Iterator;
import java.util.List;
import javax.annotation.Nullable;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.disk.vector.VectorMemtableIndex;
import org.apache.cassandra.index.sai.iterators.KeyRangeIterator;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.MemtableOrdering;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.concurrent.OpOrder;

public interface MemtableIndex extends MemtableOrdering
{
    Memtable getMemtable();

    long writeCount();

    long estimatedOnHeapMemoryUsed();

    long estimatedOffHeapMemoryUsed();

    boolean isEmpty();

    // Returns the minimum indexed term in the combined memory indexes.
    // This can be null if the indexed memtable was empty. Users of the
    // {@code MemtableIndex} requiring a non-null minimum term should
    // use the {@link MemtableIndex#isEmpty} method.
    // Note: Individual index shards can return null here if the index
    // didn't receive any terms within the token range of the shard
    @Nullable
    ByteBuffer getMinTerm();

    // Returns the maximum indexed term in the combined memory indexes.
    // This can be null if the indexed memtable was empty. Users of the
    // {@code MemtableIndex} requiring a non-null maximum term should
    // use the {@link MemtableIndex#isEmpty} method.
    // Note: Individual index shards can return null here if the index
    // didn't receive any terms within the token range of the shard
    @Nullable
    ByteBuffer getMaxTerm();

    void index(DecoratedKey key, Clustering clustering, ByteBuffer value, Memtable memtable, OpOrder.Group opGroup);

    void update(DecoratedKey key, Clustering clustering, ByteBuffer oldValue, ByteBuffer newValue, Memtable memtable, OpOrder.Group opGroup);
    void update(DecoratedKey key, Clustering clustering, Iterator<ByteBuffer> oldValues, Iterator<ByteBuffer> newValues, Memtable memtable, OpOrder.Group opGroup);

    KeyRangeIterator search(QueryContext queryContext, Expression expression, AbstractBounds<PartitionPosition> keyRange);

    /**
     * Estimates the number of rows that would be returned by this index given the predicate.
     * It is extrapolated from the first shard.
     * Note that this is not a guarantee of the number of rows that will actually be returned.
     *
     * @param expression predicate to match
     * @param keyRange   the key range to search within
     * @return an approximate number of the matching rows
     */
    long estimateMatchingRowsCountUsingFirstShard(Expression expression, AbstractBounds<PartitionPosition> keyRange);

    /**
     * Estimates the number of rows that would be returned by this index given the predicate.
     * It estimates from all relevant shards individually.
     * Note that this is not a guarantee of the number of rows that will actually be returned.
     *
     * @param expression predicate to match
     * @param keyRange   the key range to search within
     * @return an estimated number of the matching rows
     */
    long estimateMatchingRowsCountUsingAllShards(Expression expression, AbstractBounds<PartitionPosition> keyRange);

    Iterator<Pair<ByteComparable.Preencoded, List<MemoryIndex.PkWithFrequency>>> iterator(DecoratedKey min, DecoratedKey max);

    static MemtableIndex createIndex(IndexContext indexContext, Memtable mt)
    {
        return indexContext.isVector() ? new VectorMemtableIndex(indexContext, mt) : new TrieMemtableIndex(indexContext, mt);
    }

    /**
     * @return num of rows in the memtable index
     */
    int getRowCount();

    /**
     * Approximate total count of terms in the memtable index.
     * The count is approximate because some deletions are not accounted for in the current implementation.
     *
     * @return total count of terms for indexes rows.
     */
    long getApproximateTermCount();
}
