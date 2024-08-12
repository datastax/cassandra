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
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import com.google.common.math.IntMath;

import org.apache.cassandra.index.Index;
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
        return WithHybridSort.create(limit, offset, comparator);
    }

    /**
     * Returns a new row builder that orders the added rows based on the specified {@link Index.Scorer}.
     *
     * @param limit the query limit
     * @param offset the query offset
     * @param scorer the index scorer to use for ordering
     * {@link SortedRowsBuilder} that orders results based on a secondary index scorer.
     */
    public static SortedRowsBuilder create(int limit, int offset, Index.Scorer scorer)
    {
        return WithHybridSort.create(limit, offset, scorer);
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
     * </p>
     * The rows can be decorated with any other value used for the comparator, so it doesn't need to recalculate that
     * value in every comparison.
     */
    public static class WithListSort<T> extends SortedRowsBuilder
    {
        private final List<T> rows = new ArrayList<>();
        private final Function<List<ByteBuffer>, T> decorator;
        private final Function<T, List<ByteBuffer>> undecorator;
        private final Comparator<T> comparator;

        public static WithListSort<List<ByteBuffer>> create(int limit, int offset, Comparator<List<ByteBuffer>> comparator)
        {
            return new WithListSort<>(limit, offset, r -> r, r -> r, comparator);
        }

        public static WithListSort<RowWithScore> create(int limit, int offset, Index.Scorer scorer)
        {
            return new WithListSort<>(limit, offset,
                                      r -> new RowWithScore(r, scorer.score(r)),
                                      rs -> rs.row,
                                      (x, y) -> scorer.reversed()
                                                ? Float.compare(y.score, x.score)
                                                : Float.compare(x.score, y.score));
        }

        private WithListSort(int limit,
                             int offset,
                             Function<List<ByteBuffer>, T> decorator,
                             Function<T, List<ByteBuffer>> undecorator,
                             Comparator<T> comparator)
        {
            super(limit, offset);
            this.comparator = comparator;
            this.decorator = decorator;
            this.undecorator = undecorator;
        }

        @Override
        public void add(List<ByteBuffer> row)
        {
            rows.add(decorator.apply(row));
        }

        @Override
        public List<List<ByteBuffer>> build()
        {
            rows.sort(comparator);

            // trim the results and undecorate them
            List<List<ByteBuffer>> result = new ArrayList<>();
            for (int i = offset; i < Math.min(fetchLimit, rows.size()); i++)
            {
                result.add(undecorator.apply(rows.get(i)));
            }

            return result;
        }

        /**
         * A row decorated with its score assigned by a {@link Index.Scorer},
         * so we don't need to recalculate that score in every comparison.
         */
        private static final class RowWithScore
        {
            private final List<ByteBuffer> row;
            private final float score;

            private RowWithScore(List<ByteBuffer> row, float score)
            {
                this.row = row;
                this.score = score;
            }
        }
    }

    /**
     * {@link SortedRowsBuilder} that orders rows based on the provided comparator.
     * </p>
     * It's possible for the comparison to produce ties. To deal with these ties, the rows are decorated with their
     * position in the sequence of calls to {@link #add(List)}, so we can use that identifying position to solve ties by
     * favoring the row that was inserted first.
     * </p>
     * The rows can be decorated with any other value used for the comparator, so it doesn't need to recalculate that
     * value in every comparison.
     * </p>
     * It keeps at most {@code limit + offset} rows in memory.
     */
    public static class WithHeapSort<T extends WithHeapSort.RowWithId> extends SortedRowsBuilder
    {
        private final BiFunction<List<ByteBuffer>, Integer, T> decorator;

        private TopKSelector<T> heap;
        private int numAddedRows = 0;

        public static WithHeapSort<RowWithId> create(int limit, int offset, Comparator<List<ByteBuffer>> comparator)
        {
            return new WithHeapSort<>(limit, offset,
                                      RowWithId::new,
                                      (x, y) -> comparator.compare(x.row, y.row));
        }

        public static WithHeapSort<RowWithIdAndScore> create(int limit, int offset, Index.Scorer scorer)
        {
            return new WithHeapSort<>(limit, offset,
                                      (row, id) -> new RowWithIdAndScore(row, id, scorer.score(row)),
                                      (x, y) -> scorer.reversed()
                                                ? Float.compare(y.score, x.score)
                                                : Float.compare(x.score, y.score));
        }

        private WithHeapSort(int limit,
                             int offset,
                             BiFunction<List<ByteBuffer>, Integer, T> decorator,
                             Comparator<T> comparator)
        {
            super(limit, offset);
            this.decorator = decorator;
            this.heap = new TopKSelector<>(comparator, fetchLimit);
        }

        @Override
        public void add(List<ByteBuffer> row)
        {
            T decoratedRow = decorator.apply(row, numAddedRows++);
            heap.add(decoratedRow);
        }

        @Override
        public List<List<ByteBuffer>> build()
        {
            return heap.getTransformedSliced(r -> r.row, offset);
        }

        /**
         * A row decorated with its position in the sequence of calls to {@link #add(List)},
         * so we can use it to solve ties in comparisons.
         */
        private static class RowWithId
        {
            protected final List<ByteBuffer> row;
            protected final int id;

            private RowWithId(List<ByteBuffer> row, int id)
            {
                this.row = row;
                this.id = id;
            }
        }

        /**
         * A {@link RowWithId} that is also decorated with a score assigned by a {@link Index.Scorer},
         * so we don't need to recalculate that score in every comparison.
         */
        private static final class RowWithIdAndScore extends RowWithId
        {
            private final float score;

            private RowWithIdAndScore(List<ByteBuffer> row, int id, float score)
            {
                super(row, id);
                this.score = score;
            }
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
    public static class WithHybridSort<L, Q extends WithHeapSort.RowWithId> extends SortedRowsBuilder
    {
        /** Factor of {@code limit + offset} at which we switch from list to heap. */
        public static final int SWITCH_FACTOR = 4;

        private WithListSort<L> list;
        private final Supplier<WithHeapSort<Q>> heapSupplier;
        private final int threshold; // at what number of rows we switch from list to heap, -1 means no switch

        private WithHeapSort<Q> heap;

        public static SortedRowsBuilder create(int limit, int offset, Comparator<List<ByteBuffer>> comparator)
        {
            return new WithHybridSort<>(limit, offset,
                                        WithListSort.create(limit, offset, comparator),
                                        () -> WithHeapSort.create(limit, offset, comparator));
        }

        public static SortedRowsBuilder create(int limit, int offset, Index.Scorer scorer)
        {
            return new WithHybridSort<>(limit, offset,
                                        WithListSort.create(limit, offset, scorer),
                                        () -> WithHeapSort.create(limit, offset, scorer));
        }

        @SuppressWarnings("UnstableApiUsage")
        private WithHybridSort(int limit, int offset,
                               WithListSort<L> list,
                               Supplier<WithHeapSort<Q>> heapSupplier)
        {
            super(limit, offset);
            this.list = list;
            this.heapSupplier = heapSupplier;

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
                heap = heapSupplier.get();
                for (L r : list.rows)
                    heap.add(list.undecorator.apply(r));
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
