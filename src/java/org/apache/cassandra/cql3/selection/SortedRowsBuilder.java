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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.cassandra.index.Index;
import org.apache.lucene.util.PriorityQueue;

/**
 * Builds a list of query result rows applying the specified order, limit and offset.
 * </p>
 * It's functionally equivalent to adding all the rows to a list, sorting it based on the specified order, removing the
 * first offset elements, and then removing the last elements until the list size satisfies the limit.
 * </p>
 * The advantage of this class is that it doesn't need to keep all the rows in memory. If the order is based on
 * insertion order, it won't keep more than {@code limit} rows in memory. If the order is based on a comparator or an
 * {@link Index.Scorer}, it won't keep more than {@code limit + offset} rows in memory.
 */
public abstract class SortedRowsBuilder
{
    public final int limit;
    public final int offset;

    private SortedRowsBuilder(int limit, int offset)
    {
        assert limit > 0 && offset >= 0;
        this.limit = limit;
        this.offset = offset;
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
        return WithListAndPriorityQueue.create(limit, offset, comparator);
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
        return WithListAndPriorityQueue.create(limit, offset, scorer);
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
     * consume a bunch of resources if the number of rows is high. However, it has good performance for cases where
     * number of rows is close to {@code limit + offset}, as it's the case of partition-directed queries.
     * </p>
     * The rows can be decorated with any other value used for the comparator, so it doesn't need to recalculate that
     * value in every comparison.
     */
    public static class WithList<T> extends SortedRowsBuilder
    {
        private final List<T> rows = new ArrayList<>();
        private final Function<List<ByteBuffer>, T> decorator;
        private final Function<T, List<ByteBuffer>> undecorator;
        private final Comparator<T> comparator;
        private boolean built = false;

        public static WithList<List<ByteBuffer>> create(int limit, int offset, Comparator<List<ByteBuffer>> comparator)
        {
            return new WithList<>(limit, offset, r -> r, r -> r, comparator);
        }

        public static WithList<RowWithScore> create(int limit, int offset, Index.Scorer scorer)
        {
            return new WithList<>(limit, offset,
                                  r -> new RowWithScore(r, scorer.score(r)),
                                  rs -> rs.row,
                                  (x, y) -> scorer.reversed()
                                            ? Float.compare(y.score, x.score)
                                            : Float.compare(x.score, y.score));
        }

        private WithList(int limit,
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
            assert !built : "Cannot add more rows after calling build()";
            rows.add(decorator.apply(row));
        }

        @Override
        public List<List<ByteBuffer>> build()
        {
            built = true;

            rows.sort(comparator);

            // trim the results and undecorate them
            List<List<ByteBuffer>> result = new ArrayList<>();
            for (int i = 0; i < rows.size(); i++)
            {
                if (i < offset)
                    continue;
                if (i >= limit + offset)
                    break;
                result.add(undecorator.apply(rows.get(i)));
            }

            return result;
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
    public static class WithPriorityQueue<T extends RowWithId> extends SortedRowsBuilder
    {
        private final BiFunction<List<ByteBuffer>, Integer, T> decorator;
        private final PriorityQueue<T> rows;
        private int numAddedRows = 0;
        private boolean built = false;

        public static WithPriorityQueue<RowWithId> create(int limit, int offset, Comparator<List<ByteBuffer>> comparator)
        {
            return new WithPriorityQueue<>(limit, offset,
                                           RowWithId::new,
                                           (x, y) -> comparator.compare(x.row, y.row));
        }

        public static WithPriorityQueue<RowWithScoreAndId> create(int limit, int offset, Index.Scorer scorer)
        {
            return new WithPriorityQueue<>(limit, offset,
                                           (row, id) -> new RowWithScoreAndId(row, id, scorer.score(row)),
                                           (x, y) -> scorer.reversed()
                                                     ? Float.compare(y.score, x.score)
                                                     : Float.compare(x.score, y.score));
        }

        private WithPriorityQueue(int limit,
                                  int offset,
                                  BiFunction<List<ByteBuffer>, Integer, T> decorator,
                                  Comparator<T> comparator)
        {
            super(limit, offset);
            this.decorator = decorator;
            rows = new PriorityQueue<>(limit + offset)
            {
                @Override
                protected boolean lessThan(T t1, T t2)
                {
                    // Reverse compare rows, so the worst stays at the top of the queue.
                    // Ties are solved by favoring the row which id indicates that it was inserted first.
                    int cmp = comparator.compare(t1, t2);
                    return cmp == 0 ? t1.id > t2.id : cmp > 0;
                }
            };
        }

        @Override
        public void add(List<ByteBuffer> row)
        {
            assert !built : "Cannot add more rows after calling build()";
            T candidate = decorator.apply(row, numAddedRows++);
            rows.insertWithOverflow(candidate);
        }

        @Override
        public List<List<ByteBuffer>> build()
        {
            built = true;

            int toPeek = rows.size() - offset;
            if (toPeek <= 0)
                return Collections.emptyList();

            ArrayList<List<ByteBuffer>> result = new ArrayList<>(toPeek);
            while (toPeek-- > 0)
                result.add(rows.pop().row);

            Collections.reverse(result);
            return result;
        }
    }

    /**
     * {@link SortedRowsBuilder} that tries to combine the benefits of {@link WithList} and {@link WithPriorityQueue}.
     * </p>
     * It uses a {@link WithList} to store the first {@code limit + offset} rows, and then swaps to a {@link WithPriorityQueue}
     * if more rows are added.
     */
    public static class WithListAndPriorityQueue<L, Q extends RowWithId> extends SortedRowsBuilder
    {
        private final Supplier<WithPriorityQueue<Q>> queueSupplier;
        private WithList<L> list;
        private WithPriorityQueue<Q> queue;

        public static SortedRowsBuilder create(int limit, int offset, Comparator<List<ByteBuffer>> comparator)
        {
            return new WithListAndPriorityQueue<>(limit, offset,
                                                  WithList.create(limit, offset, comparator),
                                                  () -> WithPriorityQueue.create(limit, offset, comparator));
        }

        public static SortedRowsBuilder create(int limit, int offset, Index.Scorer scorer)
        {
            return new WithListAndPriorityQueue<>(limit, offset,
                                                  WithList.create(limit, offset, scorer),
                                                  () -> WithPriorityQueue.create(limit, offset, scorer));
        }

        private WithListAndPriorityQueue(int limit, int offset, WithList<L> list, Supplier<WithPriorityQueue<Q>> queueSupplier)
        {
            super(limit, offset);
            this.list = list;
            this.queueSupplier = queueSupplier;
        }

        @Override
        public void add(List<ByteBuffer> row)
        {
            if (list != null && list.rows.size() < limit + offset)
            {
                list.add(row);
            }
            else
            {
                if (queue == null)
                    swapToQueue();
                queue.add(row);
            }
        }

        /** Initializes the queue with the contents of the list and drops the list. */
        private void swapToQueue()
        {
            queue = queueSupplier.get();
            for (int i = limit + offset - 1; i >= 0; i--)
            {
                L listRow = list.rows.remove(i);
                List<ByteBuffer> row = list.undecorator.apply(listRow);

                Q queueRow = queue.decorator.apply(row, i);
                queue.rows.add(queueRow);
                queue.numAddedRows++;
            }
            list = null;
        }

        @Override
        public List<List<ByteBuffer>> build()
        {
            return list != null ? list.build() : queue.build();
        }
    }

    /**
     * A row decorated with its position in the sequence of calls to {@link #add(List)}, so we can use it to solve ties
     * in comparisons.
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

    /**
     * A {@link RowWithId} that is also decorated with a score assigned by a {@link Index.Scorer},
     * so we don't need to recalculate that score in every comparison.
     */
    private static final class RowWithScoreAndId extends RowWithId
    {
        private final float score;

        private RowWithScoreAndId(List<ByteBuffer> row, int id, float score)
        {
            super(row, id);
            this.score = score;
        }
    }
}
