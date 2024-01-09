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

import java.util.List;
import java.util.PriorityQueue;

import org.apache.cassandra.io.util.FileUtils;

public class ScoredPriorityQueue extends AbstractIterator<ScoredPrimaryKey> implements AutoCloseable
{
    private final PriorityQueue<ScoreOrderedIterator> pq;
    // Defer closing the iterators because the PKs might be materialized after this.
    // todo should we just force load here to close earlier?
    private final List<ScoreOrderedIterator> toBeClosed;

    public ScoredPriorityQueue(List<ScoreOrderedIterator> iterators)
    {
        // todo we need a "better" priority queue??
        assert !iterators.isEmpty();
        this.pq = new PriorityQueue<>(iterators.size(), (o1, o2) -> Float.compare(o2.peek().score, o1.peek().score));
        for (ScoreOrderedIterator iterator : iterators)
            if (iterator.hasNext())
                pq.add(iterator);
        toBeClosed = iterators;
    }
    @Override
    protected ScoredPrimaryKey computeNext()
    {
        if (pq.isEmpty())
            return endOfData();
        var nextIter = pq.poll();
        var next = nextIter.next();
        if (nextIter.hasNext())
            pq.add(nextIter);
        return next;
    }

    @Override
    public void close()
    {
        for (ScoreOrderedIterator iterator : toBeClosed)
            FileUtils.closeQuietly(iterator);
    }
}
