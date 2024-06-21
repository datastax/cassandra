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
import java.util.PriorityQueue;

import org.apache.cassandra.index.Index;

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
    protected final int limit;
    protected final int offset;

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
        return new WithComparator(limit, offset, comparator);
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
        return new WithScore(limit, offset, scorer);
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
     * {@link SortedRowsBuilder} that orders the added rows based on a {@link Comparator}.
     */
    private static class WithComparator extends WithCompare<WithCompare.DecoratedRow>
    {
        private final Comparator<List<ByteBuffer>> comparator;

        private WithComparator(int limit, int offset, Comparator<List<ByteBuffer>> comparator)
        {
            super(limit, offset);
            this.comparator = comparator;
        }

        @Override
        DecoratedRow decorate(List<ByteBuffer> row, int id)
        {
            return new DecoratedRow(row, id);
        }

        @Override
        public int compare(DecoratedRow one, DecoratedRow other)
        {
            return comparator.compare(one.row, other.row);
        }
    }

    /**
     * {@link SortedRowsBuilder} that orders the added rows based on a {@link Index.Scorer}.
     * </p>
     * The rows are decorated with their scores, so we don't have to recompute them while sorting.
     */
    private static class WithScore extends WithCompare<WithScore.ScoredRow>
    {
        private final Index.Scorer scorer;

        private WithScore(int limit, int offset, Index.Scorer scorer)
        {
            super(limit, offset);
            this.scorer = scorer;
        }

        @Override
        ScoredRow decorate(List<ByteBuffer> row, int id)
        {
            return new ScoredRow(row, id, scorer.score(row));
        }

        @Override
        public int compare(ScoredRow one, ScoredRow other)
        {
            return scorer.reversed()
                   ? Float.compare(other.score, one.score)
                   : Float.compare(one.score, other.score);
        }

        static final class ScoredRow extends DecoratedRow
        {
            private final float score;

            ScoredRow(List<ByteBuffer> row, int id, float score)
            {
                super(row, id);
                this.score = score;
            }
        }
    }

    /**
     * {@link SortedRowsBuilder} that orders rows based on the {@link #compare(DecoratedRow, DecoratedRow)} method.
     * </p>
     * It's possible for {@link #compare(DecoratedRow, DecoratedRow)} to produce ties. To deal with these ties, the rows
     * are decorated with their position in the sequence of calls to {@link #add(List)}, so we can use that identifying
     * position to solve ties by favoring the row that was inserted first.
     * </p>
     * It keeps at most {@code limit + offset} rows in memory.
     */
    private static abstract class WithCompare<T extends WithCompare.DecoratedRow> extends SortedRowsBuilder
    {
        private final PriorityQueue<T> queue = new PriorityQueue<>(this::compareReversedWithIdx);
        private int numAddedRows = 0;
        private boolean built = false;

        private WithCompare(int limit, int offset)
        {
            super(limit, offset);
        }

        abstract T decorate(List<ByteBuffer> row, int id);

        private int compareReversedWithIdx(T one, T other)
        {
            int cmp = compare(other, one);
            return cmp != 0 ? cmp : Integer.compare(other.id, one.id);
        }

        abstract int compare(T indexedRow, T other);

        @Override
        public void add(List<ByteBuffer> row)
        {
            assert !built : "Cannot add more rows after calling build()";
            T candidate = decorate(row, numAddedRows++);
            queue.offer(candidate);
            if (queue.size() > limit + offset)
                queue.poll();
        }

        @Override
        public List<List<ByteBuffer>> build()
        {
            built = true;

            int toPeek = queue.size() - offset;
            if (toPeek <= 0)
                return Collections.emptyList();

            ArrayList<List<ByteBuffer>> rows = new ArrayList<>(toPeek);
            while (!queue.isEmpty() && toPeek-- > 0)
                rows.add(queue.poll().row);

            Collections.reverse(rows);
            return rows;
        }

        /**
         * A row decorated with its position in the sequence of calls to {@link #add(List)},
         * so we can use it to solve ties in {@link #compareReversedWithIdx(DecoratedRow, DecoratedRow)}.
         */
        static class DecoratedRow
        {
            protected final List<ByteBuffer> row;
            protected final int id;

            DecoratedRow(List<ByteBuffer> row, int id)
            {
                this.row = row;
                this.id = id;
            }
        }
    }
}
