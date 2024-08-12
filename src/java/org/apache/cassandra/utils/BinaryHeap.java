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

import com.google.common.base.Preconditions;

/**
 * A base binary heap implementation with fixed size, supporting only operations that push
 * data down in the heap (i.e. after the initial initialization of the heap from a collection
 * of items, only top/smallest items can be modified (removed or replaced)).
 * <p>
 * This class's purpose is to implement various sources of sorted entries, e.g.
 * for merging iterators (see e.g. {@link TrieMemoryIndex.SortingSingletonOrSetIterator}, producing
 * a sorted iterator from an unsorted list of items ({@link SortingIterator}) or selecting the
 * top items from data of unbounded size ({@link TopKSelector}).
 * <p>
 * As it does not support adding elements after the initial construction, the class does not
 * implement a priority queue, where items need to be repeatedly added and removed. If a priority
 * queue is required, consider using {@link LucenePriorityQueue}.
 * <p>
 * By default, the implementation supports nulls among the source entries (by comparing them greater
 * than all other elements and using null as a marker of completion) and achieves removal by
 * replacing items with null. This adds slight overhead for simple comparators (e.g. ints), but
 * significantly improves performance when the comparator is complex.
 */
public abstract class BinaryHeap
{
    // Note: This class is tested via its descendants by SortingIteratorTest and TopKSelectorTest.

    protected final Object[] heap;

    /**
     * Create a binary heap with the given array. The data must be heapified before being used.
     */
    protected BinaryHeap(Object[] data)
    {
        Preconditions.checkArgument(data.length > 0, "Binary heap needs at least one item.");
        this.heap = data;
        // Note that we can't perform any preparation here because the subclass defining greaterThan may have not been
        // initialized yet.
    }

    /**
     * Compare two objects and return true iff the first is greater.
     * The method must treat nulls as greater than non-null objects.
     */
    protected abstract boolean greaterThan(Object a, Object b);

    /**
     * Get the size. Usually just the heap length because we don't count removed elements, but some descendants may
     * choose to control size differently.
     */
    protected int size()
    {
        return heap.length;
    }

    /**
     * Advance an item. Return null if there are no further entries.
     * The default implementations assumes entries are single items and always returns null.
     * Override it to implement merging of sorted iterators.
     * @param item The heap item to advance
     */
    protected Object advanceItem(Object item)
    {
        return null;
    }

    /**
     * Advance an item to the closest entry greater than or equal to the target.
     * Return null if no such entry exists.
     * The default implementations assumes entries are single items and always returns null.
     * Override it to implement merging of sorted seeking iterators.
     * @param item The heap item to advance
     * @param targetKey The comparison key
     */
    protected Object advanceItemTo(Object item, Object targetKey)
    {
        return null;
    }

    /**
     * Turn the current list of items into a binary heap by using the initial heap construction
     * of the heapsort algorithm with complexity O(size()). Done recursively to improve caching on
     * larger heaps.
     */
    protected void heapify()
    {
        heapifyRecursively(0, size());
    }

    protected boolean isEmpty()
    {
        return heap[0] == null;
    }

    /**
     * Return the next element in the heap without advancing.
     */
    protected Object top()
    {
        return heap[0];
    }

    /**
     * Get and remove the next element in the heap.
     * If the heap contains duplicates, they will be returned in an arbitrary order.
     */
    protected Object pop()
    {
        Object item = heap[0];
        heapifyDown(advanceItem(item), 0);
        return item;
    }

    /**
     * Get and replace the top item with a new one.
     */
    protected Object replaceTop(Object newItem)
    {
        Object item = heap[0];
        heapifyDown(newItem, 0);
        return item;
    }

    /**
     * Get the next element and skip over all items equal to it.
     * Calling this instead of {@link #pop} results in deduplication of the list
     * of entries.
     */
    protected Object popAndSkipEqual()
    {
        Object item = heap[0];
        advanceBeyond(item, item);
        return item;
    }

    protected void advanceBeyond(Object targetKey, Object topItem)
    {
        Object advanced = advanceItem(topItem);
        // avoid recomparing top element
        int size = size();
        if (1 < size)
        {
            if (2 < size)
                applyAdvance(targetKey, 2, ADVANCE_BEYOND, size);
            applyAdvance(targetKey, 1, ADVANCE_BEYOND, size);
        }
        heapifyDown(advanced, 0);
    }

    /**
     * Skip to the first element that is greater than or equal to the given key.
     */
    protected void advanceTo(Object targetKey)
    {
        applyAdvance(targetKey, 0, ADVANCE_TO, size());
    }

    /**
     * Interface used to specify an advancing operation for {@link #applyAdvance}.
     */
    protected interface AdvanceOperation
    {
        /**
         * Return true if the necessary condition is satisfied by this heap entry.
         * The condition is assumed to also be satisfied for all descendants of the
         * entry (as they are equal or greater).
         */
        boolean shouldStop(BinaryHeap self, Object heapEntry, Object targetKey);

        /**
         * Apply the relevant advancing operation and return the entry to use.
         */
        Object advanceItem(BinaryHeap self, Object heapEntry, Object targetKey);
    }

    static final AdvanceOperation ADVANCE_BEYOND = new AdvanceOperation()
    {
        @Override
        public boolean shouldStop(BinaryHeap self, Object heapEntry, Object targetKey)
        {
            return self.greaterThan(heapEntry, targetKey);
        }

        @Override
        public Object advanceItem(BinaryHeap self, Object heapEntry, Object targetKey)
        {
            return self.advanceItem(heapEntry);
        }
    };

    static final AdvanceOperation ADVANCE_TO = new AdvanceOperation()
    {
        @Override
        public boolean shouldStop(BinaryHeap self, Object heapEntry, Object targetKey)
        {
            return !self.greaterThan(targetKey, heapEntry);
        }

        @Override
        public Object advanceItem(BinaryHeap self, Object heapEntry, Object targetKey)
        {
            return self.advanceItemTo(heapEntry, targetKey);
        }
    };

    /**
     * Recursively apply the advance operation to all elements in the subheap rooted at the given heapIndex
     * that do not satisfy the shouldStop condition, and restore the heap ordering on the way back from the recursion.
     */
    private void applyAdvance(Object targetKey, int heapIndex, AdvanceOperation advanceOperation, int size)
    {
        if (advanceOperation.shouldStop(this, heap[heapIndex], targetKey))
            return;

        if (heapIndex * 2 + 1 < size)
        {
            if (heapIndex * 2 + 2 < size)
                applyAdvance(targetKey, heapIndex * 2 + 2, advanceOperation, size);
            applyAdvance(targetKey, heapIndex * 2 + 1, advanceOperation, size);

            Object advanced = advanceOperation.advanceItem(this, heap[heapIndex], targetKey);
            heapifyDown(advanced, heapIndex);
        }
        else
        {
            Object advanced = advanceOperation.advanceItem(this, heap[heapIndex], targetKey);
            heap[heapIndex] = advanced;
        }
    }

    /**
     * Perform the initial heapification of the data. This could be achieved with the method above (with shouldStop
     * always false and advanceItem returning the item unchanged), but a direct implementation is much simpler and
     * performs better.
     */

    private void heapifyRecursively(int heapIndex, int size)
    {
        if (heapIndex * 2 + 1 < size)
        {
            if (heapIndex * 2 + 2 < size)
                heapifyRecursively(heapIndex * 2 + 2, size);
            heapifyRecursively(heapIndex * 2 + 1, size);

            heapifyDown(heap[heapIndex], heapIndex);
        }
    }

    /**
     * Push the given state down in the heap from the given index until it finds its proper place among
     * the subheap rooted at that position.
     */
    private void heapifyDown(Object item, int index)
    {
        heapifyDownUpTo(item, index, size());
    }

    /**
     * Push the given state down in the heap from the given index until it finds its proper place among
     * the subheap rooted at that position.
     */
    private void heapifyDownUpTo(Object item, int index, int size)
    {
        while (true)
        {
            int next = index * 2 + 1;
            if (next >= size)
                break;
            // Select the smaller of the two children to push down to.
            if (next + 1 < size && greaterThan(heap[next], heap[next + 1]))
                ++next;
            // If the child is greater or equal, the invariant has been restored.
            if (!greaterThan(item, heap[next]))
                break;
            heap[index] = heap[next];
            index = next;
        }
        heap[index] = item;
    }

    /**
     * Sort the heap by repeatedly popping the top item and placing it at the end of the heap array.
     * The result will contain the elements in the heap sorted in descending order.
     * The heap must be heapified before calling this method.
     */
    protected void heapSort()
    {
        // Sorting the ones from 1 will also make put the right value in heap[0]
        heapSortFrom(1);
    }

    /**
     * Partially sort the heap by repeatedly popping the top item and placing it at the end of the heap array,
     * until the given start position is reached. This results in a partial sorting where the smallest items
     * (according to the comparator) are placed at positions of the heap between start and size in descending order,
     * and the items before that are left heapified.
     * The heap must be heapified up to the size before calling this method.
     * Used to fetch items after a certain offset in a top-k selection.
     */
    protected void heapSortFrom(int start)
    {
        // Data must already be heapified up to that size, comparator must be reverse
        for (int i = size() - 1; i >= start; --i)
        {
            Object top = heap[0];
            heapifyDownUpTo(heap[i], 0, i);
            heap[i] = top;
        }
    }

    /**
     * A binary heap that uses a comparator to determine the order of elements, implementing the necessary handling
     * of nulls.
     */
    public static class WithComparator<T> extends BinaryHeap
    {
        final Comparator<? super T> comparator;

        public WithComparator(Comparator<? super T> comparator, Object[] data)
        {
            super(data);
            this.comparator = comparator;
        }

        @Override
        @SuppressWarnings("unchecked")
        protected boolean greaterThan(Object a, Object b)
        {
            // nulls are treated as greater than non-nulls to be placed at the end of the sequence
            if (a == null || b == null)
                return b != null;
            return comparator.compare((T) a, (T) b) > 0;
        }
    }

    /**
     * Create a mermaid graph for the current state of the heap. Used to create visuals for documentation/slides.
     */
    String toMermaid()
    {
        StringBuilder builder = new StringBuilder();
        builder.append("flowchart\n");
        int size = size();
        for (int i = 0; i < size; ++i)
            builder.append("  s" + i + "(" + heap[i] + ")\n");
        builder.append("\n");
        for (int i = 0; i * 2 + 1 < size; ++i)
        {
            builder.append("  s" + i + " ---|<=| s" + (i * 2 + 1) + "\n");
            if (i * 2 + 2 < size)
                builder.append("  s" + i + " ---|<=| s" + (i * 2 + 2) + "\n");
        }
        return builder.toString();
    }
}
