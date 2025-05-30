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
import java.util.function.LongConsumer;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.iterators.KeyRangeIterator;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.plan.Orderer;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.PrimaryKeyWithSortKey;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

public abstract class MemoryIndex
{
    protected final IndexContext indexContext;

    protected MemoryIndex(IndexContext indexContext)
    {
        this.indexContext = indexContext;
    }

    public abstract void add(DecoratedKey key,
                             Clustering clustering,
                             ByteBuffer value,
                             LongConsumer onHeapAllocationsTracker,
                             LongConsumer offHeapAllocationsTracker);

    /**
     * Update the index value for the given key and clustering by removing the old value and adding the new value.
     * This is meant to be used when the indexed column is any type other than a non-frozen collection.
     */
    public abstract void update(DecoratedKey key,
                                Clustering clustering,
                                ByteBuffer oldValue,
                                ByteBuffer newValue,
                                LongConsumer onHeapAllocationsTracker,
                                LongConsumer offHeapAllocationsTracker);

    /**
     * Update the index value for the given key and clustering by removing the old values and adding the new values.
     * This is meant to be used when the indexed column is a non-frozen collection.
     */
    public abstract void update(DecoratedKey key,
                                Clustering clustering,
                                Iterator<ByteBuffer> oldValues,
                                Iterator<ByteBuffer> newValues,
                                LongConsumer onHeapAllocationsTracker,
                                LongConsumer offHeapAllocationsTracker);

    public abstract CloseableIterator<PrimaryKeyWithSortKey> orderBy(Orderer orderer, Expression slice);

    public abstract KeyRangeIterator search(Expression expression, AbstractBounds<PartitionPosition> keyRange);

    public abstract long estimateMatchingRowsCount(Expression expression, AbstractBounds<PartitionPosition> keyRange);

    public abstract ByteBuffer getMinTerm();

    public abstract ByteBuffer getMaxTerm();

    /**
     * @return num of rows in the memory index
     */
    public abstract int indexedRows();

    /**
     * Iterate all Term->PrimaryKeys mappings in sorted order
     */
    public abstract Iterator<Pair<ByteComparable.Preencoded, List<PkWithFrequency>>> iterator();


    public static class PkWithFrequency
    {
        public final PrimaryKey pk;
        public final int frequency;

        public PkWithFrequency(PrimaryKey pk, int frequency)
        {
            this.pk = pk;
            this.frequency = frequency;
        }
    }
}
