/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.cql3.selection;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import com.google.common.math.IntMath;

import org.apache.cassandra.utils.TopKSelector;

import static org.apache.cassandra.db.filter.DataLimits.NO_LIMIT;

/**
 * Builds a list of query result rows applying the specified order, limit and offset.
 */
public abstract class SortedRowsBuilder
{
    public final int limit;
    public final int offset;
    public final int fetchLimit; // limit + offset, saturated to Integer.MAX_VALUE

    @SuppressWarnings("UnstableApiUsage")
    private SortedRowsBuilder(int limit, int offset)
    {
        assert limit > 0 && offset >= 0;
        this.limit = limit;
        this.offset = offset;
        this.fetchLimit = IntMath.saturatedAdd(limit, offset);
    }

    /**
     * Adds the specified row to this builder. The row might be ignored if it's over the specified limit and offset.
     *
     * @param row the row to add
     */
    public abstract void add(List<ByteBuffer> row);

    /**
     * @return a list of query result rows based on the specified order, limit and offset.
     */
    public abstract List<List<ByteBuffer>> build();

    /**
     * Returns a new row builder that keeps insertion order.
     *
     * @return a rows builder that keeps insertion order.
     */
    public static SortedRowsBuilder create()
    {
        return new WithInsertionOrder(Integer.MAX_VALUE, 0);
    }

    /**
     * Returns a new row builder that keeps insertion order.
     *
     * @param limit the query limit
     * @param offset the query offset
     * @return a rows builder that keeps insertion order.
     */
    public static SortedRowsBuilder create(int limit, int offset)
    {
        return new WithInsertionOrder(limit, offset);
    }

    /**
     * Returns a new row builder that orders the added rows based on the specified {@link Comparator}.
     *
     * @param limit the query limit
     * @param offset the query offset
     * @param comparator the comparator to use for ordering
     * @return a rows builder that orders results based on a comparator.
     */
    public static SortedRowsBuilder create(int limit, int offset, Comparator<List<ByteBuffer>> comparator)
    {
        return new WithHybridSort(limit, offset, comparator);
    }

    /**
     * {@link SortedRowsBuilder} that keeps insertion order.
     * </p>
     * It keeps at most {@code limit} rows in memory.
     */
    private static class WithInsertionOrder extends SortedRowsBuilder
    {
        private final List<List<ByteBuffer>> rows = new ArrayList<>();
        private int toSkip = offset;

        private WithInsertionOrder(int limit, int offset)
        {
            super(limit, offset);
        }

        @Override
        public void add(List<ByteBuffer> row)
        {
            if (toSkip-- <= 0 && rows.size() < limit)
                rows.add(row);
        }

        @Override
        public List<List<ByteBuffer>> build()
        {
            return rows;
        }
    }

    /**
     * {@link SortedRowsBuilder} that orders rows based on the provided comparator.
     * </p>
     * It simply stores all the rows in a list, and sorts and trims it when {@link #build()} is called. As such, it can
     * consume a bunch of resources if the number of rows is high. However, it has good performance for cases where the
     * number of rows is close to {@code limit + offset}, as it's the case of partition-directed queries.
     */
    public static class WithListSort extends SortedRowsBuilder
    {
        private final List<List<ByteBuffer>> rows = new ArrayList<>();
        private final Comparator<List<ByteBuffer>> comparator;

        private WithListSort(int limit,
                             int offset,
                             Comparator<List<ByteBuffer>> comparator)
        {
            super(limit, offset);
            this.comparator = comparator;
        }

        @Override
        public void add(List<ByteBuffer> row)
        {
            rows.add(row);
        }

        @Override
        public List<List<ByteBuffer>> build()
        {
            rows.sort(comparator);
            return rows.subList(Math.min(offset, rows.size()),
                                Math.min(fetchLimit, rows.size()));
        }
    }

    /**
     * {@link SortedRowsBuilder} that orders rows based on the provided comparator.
     * </p>
     * It uses a heap to keep at most {@code limit + offset} rows in memory.
     */
    public static class WithHeapSort extends SortedRowsBuilder
    {
        private final TopKSelector<List<ByteBuffer>> heap;

        private WithHeapSort(int limit, int offset, Comparator<List<ByteBuffer>> comparator)
        {
            super(limit, offset);
            this.heap = new TopKSelector<>(comparator, fetchLimit);
        }

        @Override
        public void add(List<ByteBuffer> row)
        {
            heap.add(row);
        }

        public void addAll(Iterable<List<ByteBuffer>> rows)
        {
            heap.addAll(rows);
        }

        @Override
        public List<List<ByteBuffer>> build()
        {
            return heap.getSliced(offset);
        }
    }

    /**
     * {@link SortedRowsBuilder} that tries to combine the benefits of {@link WithListSort} and {@link WithHeapSort}.
     * </p>
     * {@link WithListSort} is faster for the first rows, but then it becomes slower than {@link WithHeapSort} as the
     * number of rows grows. Also, {@link WithHeapSort} has constant {@code limit + offset} memory usage, whereas
     * {@link WithListSort} memory usage grows linearly with the number of added rows.
     * </p>
     * This uses a {@link WithListSort} to sort the first {@code (limit + offset) * }{@link #SWITCH_FACTOR} rows,
     * and then it switches to a {@link WithHeapSort} if more rows are added.
     * </p>
     * It keeps at most {@link #SWITCH_FACTOR} {@code * (limit + offset)} rows in memory.
     */
    public static class WithHybridSort extends SortedRowsBuilder
    {
        /**
         * Factor of {@code limit + offset} at which we switch from list to heap.
         */
        public static final int SWITCH_FACTOR = 4;

        private final int threshold; // at what number of rows we switch from list to heap, -1 means no switch

        private WithListSort list;
        private WithHeapSort heap;

        @SuppressWarnings("UnstableApiUsage")
        private WithHybridSort(int limit, int offset, Comparator<List<ByteBuffer>> comparator)
        {
            super(limit, offset);
            this.list = new WithListSort(limit, offset, comparator);

            // The heap approach is only useful when the limit is smaller than the number of collected rows.
            // If there is no limit we will return all the collected rows, so we can simply use the list approach.
            this.threshold = limit == NO_LIMIT ? -1 : IntMath.saturatedMultiply(fetchLimit, SWITCH_FACTOR);
        }

        @Override
        public void add(List<ByteBuffer> row)
        {
            // start using the heap if the list is full
            if (list != null && threshold > 0 && list.rows.size() >= threshold)
            {
                heap = new WithHeapSort(limit, offset, list.comparator);
                heap.addAll(list.rows);
                list = null;
            }

            if (list != null)
                list.add(row);
            else
                heap.add(row);
        }

        @Override
        public List<List<ByteBuffer>> build()
        {
            return list != null ? list.build() : heap.build();
        }
    }
}
