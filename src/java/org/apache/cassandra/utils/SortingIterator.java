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

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Function;

/**
 * An iterator that lists a set of items in order.
 * <p>
 * This is intended for use where we would normally read only a small subset of the elements, or where we would skip
 * over large sections of the sorted set. To implement this efficiently, we put the data in a binary heap and extract
 * elements as the iterator is queried, effectively performing heapsort. We also implement a quicker skipTo operation
 * where we remove all smaller elements and restore the heap for all of them in one step.
 * <p>
 * As in heapsort, the first stage of the process has complexity O(n), and every next item is extracted in O(log n)
 * steps. skipTo works in O(m.log n) steps (where m is the number of skipped items), but is also limited to O(n) when m
 * is large by the same argument as the initial heap construction.
 * <p>
 * The class accepts and stores nulls as non-present values, which turns out to be quite a bit more efficient for
 * iterating these sets when the comparator is complex at the expense of a small slowdown for simple comparators. The
 * reason for this is that we can remove entries by replacing them with nulls and letting these descend the heap, which
 * avoids half the comparisons compared to using one of the largest live elements.
 * <p>
 * If the number of items necessary is small and known in advance, it may be preferable to use {@link TopKSelector}
 * which keeps a smaller memory footprint.
 */
public class SortingIterator<T> extends BinaryHeap.WithComparator<T> implements Iterator<T>
{
    SortingIterator(Comparator<? super T> comparator, Object[] data)
    {
        super(comparator, data);
        heapify();
    }

    /**
     * Create a sorting iterator from a list of sources.
     * Duplicates will be returned in arbitrary order.
     */
    public static <T> SortingIterator<T> create(Comparator<? super T> comparator, Collection<T> sources)
    {
        return new SortingIterator<>(comparator, sources.isEmpty() ? new Object[1] : sources.toArray());
    }

    /**
     * Create a closeable sorting iterator from a list of sources, calling the given method on close.
     * Duplicates will be returned in arbitrary order.
     */
    public static <T, V> CloseableIterator<T> createCloseable(Comparator<? super T> comparator, Collection<V> sources, Function<V, T> mapper, Runnable onClose)
    {
        return new Builder<>(sources, mapper).closeable(comparator, onClose);
    }

    /**
     * Create a sorting and deduplicating iterator from a list of sources.
     * Duplicate values will only be reported once, using an arbitrarily-chosen representative.
     */
    public static <T> SortingIterator<T> createDeduplicating(Comparator<? super T> comparator, Collection<T> sources)
    {
        return new Deduplicating<>(comparator, sources.isEmpty() ? new Object[1] : sources.toArray());
    }

    @Override
    protected Object advanceItem(Object item)
    {
        return null;
    }

    @Override
    protected Object advanceItemTo(Object item, Object targetKey)
    {
        return null;
    }

    @SuppressWarnings("unchecked")
    public T peek()
    {
        return (T) super.top();
    }

    @Override
    public boolean hasNext()
    {
        return !isEmpty();
    }

    @SuppressWarnings("unchecked")
    @Override
    public T next()
    {
        Object item = pop();
        if (item == null)
            throw new NoSuchElementException();
        return (T) item;
    }

    /**
     * Skip to the first element that is greater than or equal to the given key.
     */
    public void skipTo(T targetKey)
    {
        advanceTo(targetKey);
    }

    public static class Closeable<T> extends SortingIterator<T> implements CloseableIterator<T>
    {
        final Runnable onClose;

        public <V> Closeable(Comparator<? super T> comparator,
                             Object[] data,
                             Runnable onClose)
        {
            super(comparator, data);
            this.onClose = onClose;
        }

        @Override
        public void close()
        {
            onClose.run();
        }
    }

    public static class Deduplicating<T> extends SortingIterator<T>
    {
        public Deduplicating(Comparator<? super T> comparator, Object[] data)
        {
            super(comparator, data);
        }

        @Override
        public T next()
        {
            Object item = popAndSkipEqual();
            if (item == null)
                throw new NoSuchElementException();
            return (T) item;
        }
    }

    public static class Builder<T>
    {
        Object[] data;
        int count;

        public Builder()
        {
            this(16);
        }

        public Builder(int initialSize)
        {
            data = new Object[Math.max(initialSize, 1)]; // at least one element so that we don't need to special-case empty
            count = 0;
        }

        public <V> Builder(Collection<V> collection, Function<V, T> mapper)
        {
            this(collection.size());
            for (V item : collection)
                data[count++] = mapper.apply(item); // this may be null, which the iterator will properly handle
        }

        public Builder<T> add(T element)
        {
            if (element != null)   // avoid growing if we don't need to
            {
                if (count == data.length)
                    data = Arrays.copyOf(data, data.length * 2);
                data[count++] = element;
            }
            return this;
        }

        public Builder<T> addAll(Collection<? extends T> collection)
        {
            if (count + collection.size() > data.length)
                data = Arrays.copyOf(data, count + collection.size());
            for (T item : collection)
                data[count++] = item;
            return this;
        }

        public <V> Builder<T> addAll(Collection<V> collection, Function<V, ? extends T> mapper)
        {
            if (count + collection.size() > data.length)
                data = Arrays.copyOf(data, count + collection.size());
            for (V item : collection)
                data[count++] = mapper.apply(item); // this may be null, which the iterator will properly handle
            return this;
        }

        public int size()
        {
            return count; // Note: may include null elements, depending on how data is added
        }

        /**
         * Build a sorting iterator from the data added so far.
         * The returned iterator will report duplicates in arbitrary order.
         */
        public SortingIterator<T> build(Comparator<? super T> comparator)
        {
            return new SortingIterator<>(comparator, data);    // this will have nulls at the end, which is okay
        }

        /**
         * Build a closeable sorting iterator from the data added so far.
         * The returned iterator will report duplicates in arbitrary order.
         */
        public Closeable<T> closeable(Comparator<? super T> comparator, Runnable onClose)
        {
            return new Closeable<>(comparator, data, onClose);
        }

        /**
         * Build a sorting and deduplicating iterator from the data added so far.
         * The returned iterator will only report equal items once, using an arbitrarily-chosen representative.
         */
        public SortingIterator<T> deduplicating(Comparator<? super T> comparator)
        {
            return new Deduplicating<>(comparator, data);
        }

        // This does not offer build methods that trim the array to count (i.e. Arrays.copyOf(data, count) instead of
        // data), because it is only meant for short-lived operations where the iterator is not expected to live much
        // longer than the builder and thus both the builder and iterator will almost always expire in the same GC cycle
        // and thus the cost of trimming is not offset by any gains.
    }
}
