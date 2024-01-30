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

package org.apache.cassandra.index.sai.utils;

import java.util.Collection;
import java.util.List;
import java.util.PriorityQueue;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

import org.apache.cassandra.index.sai.SSTableIndex;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.CloseableIterator;

/**
 * An iterator over {@link ScoredPrimaryKey} that merges multiple iterators into a single iterator by taking the
 * scores of the top element of each iterator and returning the {@link ScoredPrimaryKey} with the
 * highest score.
 */
public class MergeScoredPrimaryKeyIterator extends AbstractIterator<ScoredPrimaryKey>
{
    private final PriorityQueue<PeekingIterator<ScoredPrimaryKey>> pq;
    private final List<CloseableIterator<ScoredPrimaryKey>> iteratorsToBeClosed;
    private final Collection<SSTableIndex> indexesToBeClosed;

    public MergeScoredPrimaryKeyIterator(List<CloseableIterator<ScoredPrimaryKey>> iterators, Collection<SSTableIndex> referencedIndexes)
    {
        int size = !iterators.isEmpty() ? iterators.size() : 1;
        this.pq = new PriorityQueue<>(size, (o1, o2) -> Float.compare(o2.peek().score, o1.peek().score));
        for (CloseableIterator<ScoredPrimaryKey> iterator : iterators)
            if (iterator.hasNext())
                pq.add(Iterators.peekingIterator(iterator));
        iteratorsToBeClosed = iterators;
        indexesToBeClosed = referencedIndexes;
    }

    @Override
    protected ScoredPrimaryKey computeNext()
    {
        if (pq.isEmpty())
            return endOfData();

        // Get the iterator with the highest score
        var nextIter = pq.poll();
        assert nextIter != null;
        var nextKey = nextIter.next();
        // If the iterator has more elements, add it back to the queue
        if (nextIter.hasNext())
            pq.add(nextIter);

        return nextKey;
    }

    @Override
    public void close()
    {
        for (CloseableIterator<ScoredPrimaryKey> iterator : iteratorsToBeClosed)
            FileUtils.closeQuietly(iterator);
        for (SSTableIndex index : indexesToBeClosed)
            index.release();
    }
}
