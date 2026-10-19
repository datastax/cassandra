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

package org.apache.cassandra.utils;

import java.util.Comparator;

import org.apache.lucene.util.PriorityQueue;

/**
 * Version of lucene's priority queue that accepts a comparator.
 * <p>
 * This priority queue has several performance advantages compared to java's:
 * <ul>
 * <li>it can efficiently order items added through {@code addAll} using the O(n) bottom-up heapification process</li>
 * <li>it implements an {@code updateTop} method which is much more efficient than {@code poll} + {@code add} for e.g.
 * advancing a source and keeping it in the queue</li>
 * </ul>
 * <p>
 * Use this class when elements need to be added to the queue after the initial construction. In case all elements are
 * predetermined, a {@link SortingIterator} is usually preferable as it also implements skipping and deduplication.
 * When sorting multiple iterators into one, a {@link MergeIterator} or the underlying {@link Merger} may provide a
 * simpler solution. Finally, when operating on integer iterators, we have a special-case {@link IntMerger}.
 */
public class LucenePriorityQueue<T> extends PriorityQueue<T>
{
    final Comparator<? super T> comparator;

    public LucenePriorityQueue(int size, Comparator<? super T> comparator)
    {
        super(size);
        this.comparator = comparator;
    }

    @Override
    protected boolean lessThan(T t, T t1)
    {
        return comparator.compare(t, t1) < 0;
    }
}
