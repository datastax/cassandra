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


import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Collection;
import java.util.function.Consumer;

/**
 * <p>
 * Integer version of the {@link Merger} class, throwing away the {@code equalParent} optimization as it is
 * not beneficial for the integer comparisons.
 * </p><p>
 * This class merges sorted integer streams by direct value comparison. For simplicity, it assumes the user has
 * some external method of recognizing completion (e.g. using {@code Integer.MAX_VALUE} as sentinel). The class
 * will not advance any of the source iterators until a request for data has been made. If a source's input has been
 * processed and returned by the merger, the next value will only be requested when the merger is asked for the next.
 * </p><p>
 * The most straightforward way to implement merging of iterators is to use a {@code PriorityQueue},
 * {@code poll} it to find the next item to consume, then {@code add} the iterator back after advancing.
 * This is not very efficient as {@code poll} and {@code add} in all cases require at least
 * {@code log(size)} comparisons and swaps (usually more than {@code 2*log(size)}) per consumed item, even
 * if the input is suitable for fast iteration.
 * </p><p>
 * The implementation below makes use of the fact that replacing the top element in a binary heap can be
 * done much more efficiently than separately removing it and placing it back, especially in the cases where
 * the top iterator is to be used again very soon (e.g. when there are large sections of the output where
 * only a limited number of input iterators overlap, which is normally the case in many practically useful
 * situations, e.g. levelled compaction).
 * </p><p>
 * The implementation builds and maintains a binary heap of sources (stored in an array), where we do not
 * add items after the initial construction. Instead we advance the smallest element (which is at the top
 * of the heap) and push it down to find its place for its new position. Should this source be exhausted,
 * we swap it with the last source in the heap and proceed by pushing that down in the heap.
 * </p><p>
 * Duplicate values in multiple sources are merged together, but duplicates in any individual source are not resolved.
 * In the case where we have multiple sources with matching positions, {@link #advance} advances all equal sources and
 * then restores the heap structure in one operation over the heap. The latter is done equivalently to the process of
 * initial construction of a min-heap using back-to-front heapification as done in the classic heapsort algorithm. It
 * only needs to heapify subheaps whose top item is advanced (i.e. one whose position matches the current), and we can
 * do that recursively from bottom to top.
 * </p><p>
 * To make it easier to advance efficienty in single-sourced branches of tries, we extract the current smallest
 * source (the head) separately, and start any advance with comparing that to the heap's first. When the smallest
 * source remains the same (e.g. in branches coming from a single source) this makes it possible to advance with
 * just one comparison instead of two at the expense of increasing the number by one in the general case.
 * </p>
 */
public abstract class IntMerger<S>
{
    /**
     * The current smallest item from the sources, tracked separately to improve performance in single-source
     * sections of the input.
     */
    protected int headItem;
    /**
     * The source corresponding to the smallest item.
     */
    protected S headSource;

    /**
     * Binary heap of the current items from each source. The smallest element is at position 0.
     * Every element i is smaller than or equal to its two children, i.e.<br/>
     *     {@code item[i] <= item[i*2 + 1] && item[i] <= item[i*2 + 2]}
     */
    private final int[] items;
    /**
     * Binary heap of the sources.
     * <p/>
     * Sources are moved up and down the heap together with the items, i.e. the source index corresponds to the item
     * index in these two arrays.
     */
    private final S[] sources;

    boolean started;

    /** Advance the given source by one item and return it. */
    protected abstract int advanceSource(S s) throws IOException;
    /** Skip the given source to the smallest item that is greater or equal to the given target, and return that item. */
    protected abstract int skipSource(S s, int target) throws IOException;

    protected IntMerger(Collection<? extends S> inputs, Class<S> sourceClass)
    {
        int count = inputs.size();

        // Get sources for all inputs. Put one of them in head and the rest in the heap.
        @SuppressWarnings("unchecked")
        S[] s = (S[]) Array.newInstance(sourceClass, count - 1);
        sources = s;
        items = new int[count - 1];
        int i = -1;
        for (S source : inputs)
        {
            if (i >= 0)
                sources[i] = source;
            else
                headSource = source;
            ++i;
        }
        // Do not fetch items until requested.
        started = false;
    }

    /**
     * Advance the merged state and return the next item.
     */
    protected int advance() throws IOException
    {
        if (started)
            advanceHeap(headItem, 0);
        else
            initializeHeap();

        return headItem = maybeSwapHead(advanceSource(headSource));
    }

    /**
     * Descend recursively in the subheap structure from the given index to all children that match the given position.
     * On the way back from the recursion, advance each matching iterator and restore the heap invariants.
     */
    private void advanceHeap(int advancedItem, int index) throws IOException
    {
        if (index >= items.length)
            return;

        if (items[index] != advancedItem)
            return;

        // If any of the children are at the same position, they also need advancing and their subheap
        // invariant to be restored.
        advanceHeap(advancedItem, index * 2 + 1);
        advanceHeap(advancedItem, index * 2 + 2);

        // On the way back from the recursion, advance and form a heap from the (already advanced and well-formed)
        // children and the current node.
        advanceSourceAndHeapify(index);
        // The heap rooted at index is now advanced and well-formed.
    }


    /**
     * Advance the source at the given index and restore the heap invariant for its subheap, assuming its child subheaps
     * are already well-formed.
     */
    private void advanceSourceAndHeapify(int index) throws IOException
    {
        // Advance the source.
        S source = sources[index];
        int next = advanceSource(source);

        // Place current node in its proper position, pulling any smaller child up. This completes the construction
        // of the subheap rooted at this index.
        heapifyDown(source, next, index);
    }

    /**
     * Push the given state down in the heap from the given index until it finds its proper place among
     * the subheap rooted at that position.
     */
    private void heapifyDown(S source, int item, int index)
    {
        while (true)
        {
            int next = index * 2 + 1;
            if (next >= items.length)
                break;
            // Select the smaller of the two children to push down to.
            int nextItem = items[next];
            if (next + 1 < items.length)
            {
                int nextP1Item = items[next + 1];
                if (nextItem > nextP1Item)
                {
                    nextItem = nextP1Item;
                    ++next;
                }
            }
            // If the child is greater or equal, the invariant has been restored.
            if (item <= nextItem)
                break;
            items[index] = nextItem;
            sources[index] = sources[next];
            index = next;
        }
        items[index] = item;
        sources[index] = source;
    }

    /**
     * Check if the head is greater than the top element in the heap, and if so, swap them and push down the new
     * top until its proper place.
     */
    private int maybeSwapHead(int newHeadItem)
    {
        int heap0Item = items[0];
        if (newHeadItem <= heap0Item)
            return newHeadItem;   // head is still smallest

        // otherwise we need to swap heap and heap[0]
        S newHeap0 = headSource;
        headSource = sources[0];
        heapifyDown(newHeap0, newHeadItem, 0);
        return heap0Item;
    }

    /**
     * Initialize the heap for the retrieving the first item. We do this in a separate method because we don't yet have
     * target items with which to compare in the methods above.
     */
    private void initializeHeap() throws IOException
    {
        for (int i = items.length - 1; i >= 0; --i)
            advanceSourceAndHeapify(i);

        started = true;
    }

    /**
     * Skip the merged iterator to the smallest value equal to or greater than the target and return the next item.
     */
    protected int skipTo(int target) throws IOException
    {
        // We need to advance all sources that stand before the requested position.
        // If a child source does not need to advance as it is at the skip position or greater, neither of the ones
        // below it in the heap hierarchy do as they can't have an earlier position.
        if (started)
            skipHeap(target, 0);
        else
            initializeSkipping(target);

        return headItem = maybeSwapHead(skipSource(headSource, target));
    }


    /**
     * Descend recursively in the subheap structure from the given index to all children that are smaller than the
     * requested position.
     * On the way back from the recursion, skip each matching iterator and restore the heap invariants.
     */
    private void skipHeap(int target, int index) throws IOException
    {
        if (index >= items.length)
            return;

        if (items[index] >= target)
            return;

        // If any of the children are at a smaller position, they also need advancing and their subheap
        // invariant to be restored.
        skipHeap(target, index * 2 + 1);
        skipHeap(target, index * 2 + 2);

        // On the way back from the recursion, advance and form a heap from the (already advanced and well-formed)
        // children and the current node.
        skipSourceAndHeapify(index, target);

        // The heap rooted at index is now advanced and well-formed.
    }

    /**
     * Skip the source at the given index and restore the heap invariant for its subheap, assuming its child subheaps
     * are already well-formed.
     */
    private void skipSourceAndHeapify(int index, int target) throws IOException
    {
        // Advance the source.
        S source = sources[index];
        int next = skipSource(source, target);

        // Place current node in its proper position, pulling any smaller child up. This completes the construction
        // of the subheap rooted at this index.
        heapifyDown(source, next, index);
    }

    /**
     * Initialize the heap by skipping to the given target. We do this in a separate method because we don't yet have
     * items with which to compare in the methods above.
     */
    private void initializeSkipping(int target) throws IOException
    {
        for (int i = items.length - 1; i >= 0; --i)
            skipSourceAndHeapify(i, target);

        started = true;
    }

    /**
     * Apply a method to all sources.
     */
    protected void applyToAllSources(Consumer<S> op)
    {
        for (int i = sources.length - 1; i >= 0; --i)
            op.accept(sources[i]);
        op.accept(headSource);
    }

    // currentItem(), forEachCurrentSource() methods can be easily implemented if required
}

