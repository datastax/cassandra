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

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;

/**
 * This class selects the smallest k items from a stream.
 * <p>
 * This is implemented as a binary heap with reversed comparator which keeps track of k items and keeps the largest of
 * them on top of the heap. When a new item arrives, it is checked against the top: if it is larger or equal, it can
 * be ignored as we already have k better items; if not, it replaces the top item and is pushed down to restore the
 * properties of the heap.
 * <p>
 * This process has a time complexity of O(n log k) for n > k and uses O(k) space. Duplicates are not removed and are
 * returned in arbitrary order.
 * <p>
 * If the number of items required is not known in advance, {@link SortingIterator} can be used instead to get an
 * arbitrary number of ordered items at the expense of keeping track of all of them (using O(n + k log n) time and O(n)
 * space).
 */
public class TopKSelector<T> extends BinaryHeap
{
    private final Comparator<? super T> comparator;
    private int size;

    public TopKSelector(Comparator<? super T> comparator, int limit)
    {
        super(new Object[limit]);
        this.comparator = comparator;
        size = 0;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected boolean greaterThan(Object a, Object b)
    {
        // Top-k uses an inverted comparator, so that the largest item, the one we should compare with and replace
        // if something smaller is added, sits at the top. This is also the comparator suitable for doing the final
        // heapsort steps required to arrange the end result in sort order.
        return comparator.compare((T) a, (T) b) < 0;
    }

    public void add(T newItem)
    {
        if (newItem == null)
            return;

        if (size < heap.length)
        {
            heap[size] = newItem;
            if (++size == heap.length)
                heapify();
        }
        else
        {
            if (greaterThan(newItem, top()))
                replaceTop(newItem);
        }
    }

    public void addAll(Iterable<? extends T> items)
    {
        for (T item : items)
            add(item);
    }

    @Override
    protected int size()
    {
        return size;
    }

    private void maybeHeapify()
    {
        if (size < heap.length)
            heapify();
    }

    /**
     * Get a copy of the top K elements.
     * After this call the collector can be reused.
     */
    public List<T> get()
    {
        return new ArrayList<>(getShared());
    }

    /**
     * Get a copy of the top K elements, applying the given transformation.
     * After this call the collector can be reused.
     */
    public <R> List<R> getTransformed(Function<T, R> transformer)
    {
        return getTransformedSliced(transformer, 0);
    }

    /**
     * Get a copy of the lowest size-startIndex elements, applying the given transformation.
     * The top startIndex elements will remain in the selector.
     */
    public <R> List<R> getTransformedSliced(Function<T, R> transformer, int startIndex)
    {
        return new ArrayList<>(getTransformedSlicedShared(transformer, startIndex));
    }

    /**
     * Get a shared list of the top K elements.
     * If the selector is not used further, this is a quicker alternative to get().
     */
    public List<T> getShared()
    {
        maybeHeapify();
        heapSort();
        int completedSize = size;
        size = 0;
        return getUnsortedShared(completedSize);
    }

    /**
     * Get a shared list of the top K elements in unsorted order.
     * This avoids the final sort phase (and heapification if there are fewer than K elements).
     */
    public List<T> getUnsortedShared()
    {
        return getUnsortedShared(size);
    }

    private List<T> getUnsortedShared(int size)
    {
        return new AbstractList<T>()
        {
            @Override
            public T get(int i)
            {
                return (T) heap[i];
            }

            @Override
            public int size()
            {
                return size;
            }
        };
    }

    /**
     * Get a shared list of the lowest size-startIndex elements, applying the given transformation.
     * If the selector is not used further, this is a quicker alternative to getTransformedSliced().
     */
    public <R> List<R> getTransformedSlicedShared(Function<T, R> transformer, int startIndex)
    {
        int selectedSize = size() - startIndex;
        if (selectedSize <= 0)
            return List.of();
        maybeHeapify();

        heapSortFrom(startIndex);
        size = startIndex; // the rest of the top items remain heapified and can be extracted later
        return new AbstractList<R>()
        {
            @Override
            public R get(int i)
            {
                return transformer.apply((T) heap[i + startIndex]);
            }

            @Override
            public int size()
            {
                return selectedSize;
            }
        };
    }
}
