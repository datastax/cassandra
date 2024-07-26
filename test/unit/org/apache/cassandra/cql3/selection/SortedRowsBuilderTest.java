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
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nullable;

import org.junit.Test;

import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.index.Index;
import org.assertj.core.api.Assertions;

/**
 * Tests for {@link SortedRowsBuilder}.
 */
public class SortedRowsBuilderTest
{
    private static final Comparator<List<ByteBuffer>> comparator = (o1, o2) -> Int32Type.instance.compare(o1.get(0), o2.get(0));
    private static final Comparator<List<ByteBuffer>> reverseComparator = comparator.reversed();

    @Test
    public void testRowBuilder()
    {
        test();
        test(0);
        test(0, 0, 0, 0);
        test(0, 1, 2, 3, 5, 6, 7, 8);
        test(8, 7, 6, 5, 3, 2, 1, 0);
        test(1, 6, 2, 0, 7, 3, 5, 4);
        test(1, 6, 2, 0, 7, 3, 5, 4, 4, 5, 3, 7, 0, 2, 6, 1);
    }

    private static void test(int... values)
    {
        List<List<ByteBuffer>> rows = toRows(values);

        List<Integer> limits = IntStream.range(1, values.length + 1).boxed().collect(Collectors.toList());
        limits.add(Integer.MAX_VALUE);
        limits.add(Integer.MAX_VALUE - 1);

        List<Integer> offsets = IntStream.range(0, values.length).boxed().collect(Collectors.toList());
        offsets.add(Integer.MAX_VALUE);
        offsets.add(Integer.MAX_VALUE - 1);

        for (int limit : limits)
        {
            for (int offset : offsets)
            {
                boolean cannotUseHeap = limit >= SortedRowsBuilder.WithHeapSort.MAX_SIZE - offset;

                // with insertion order
                test(rows, () -> SortedRowsBuilder.create(limit, offset), null, false);

                // with comparator
                test(rows, () -> SortedRowsBuilder.create(limit, offset, comparator), comparator, false);
                test(rows, () -> SortedRowsBuilder.create(limit, offset, reverseComparator), reverseComparator, false);
                test(rows, () -> SortedRowsBuilder.WithListSort.create(limit, offset, comparator), comparator, false);
                test(rows, () -> SortedRowsBuilder.WithListSort.create(limit, offset, reverseComparator), reverseComparator, false);
                test(rows, () -> SortedRowsBuilder.WithHeapSort.create(limit, offset, comparator), comparator, cannotUseHeap);
                test(rows, () -> SortedRowsBuilder.WithHeapSort.create(limit, offset, reverseComparator), reverseComparator, cannotUseHeap);
                test(rows, () -> SortedRowsBuilder.WithHybridSort.create(limit, offset, comparator), comparator, false);
                test(rows, () -> SortedRowsBuilder.WithHybridSort.create(limit, offset, reverseComparator), reverseComparator, false);

                // with index scorer
                test(rows, () -> SortedRowsBuilder.create(limit, offset, scorer(false)), comparator, false);
                test(rows, () -> SortedRowsBuilder.create(limit, offset, scorer(true)), reverseComparator, false);
                test(rows, () -> SortedRowsBuilder.WithListSort.create(limit, offset, scorer(false)), comparator, false);
                test(rows, () -> SortedRowsBuilder.WithListSort.create(limit, offset, scorer(true)), reverseComparator, false);
                test(rows, () -> SortedRowsBuilder.WithHeapSort.create(limit, offset, scorer(false)), comparator, cannotUseHeap);
                test(rows, () -> SortedRowsBuilder.WithHeapSort.create(limit, offset, scorer(true)), reverseComparator, cannotUseHeap);
                test(rows, () -> SortedRowsBuilder.WithHybridSort.create(limit, offset, scorer(false)), comparator, false);
                test(rows, () -> SortedRowsBuilder.WithHybridSort.create(limit, offset, scorer(true)), reverseComparator, false);
            }
        }
    }

    private static void test(List<List<ByteBuffer>> rows,
                             Supplier<SortedRowsBuilder> builderSupplier,
                             @Nullable Comparator<List<ByteBuffer>> comparator,
                             boolean shouldFail)
    {
        if (shouldFail)
        {
            Assertions.assertThatThrownBy(builderSupplier::get)
                      .hasMessageContaining(SortedRowsBuilder.WithHeapSort.LIMIT_REQUIRED_ERROR);
            return;
        }

        SortedRowsBuilder builder = builderSupplier.get();
        int offset = builder.offset;
        int fetchLimit = builder.fetchLimit;

        // get the expected values...
        List<List<ByteBuffer>> expecetedRows = new ArrayList<>(rows);
        if (comparator != null)
            expecetedRows.sort(comparator);
        expecetedRows = expecetedRows.subList(Math.min(offset, expecetedRows.size()),
                                              Math.min(fetchLimit, expecetedRows.size()));
        List<Integer> expectedValues = fromRows(expecetedRows);

        // get the actual values...
        rows.forEach(builder::add);
        List<Integer> actualValues = fromRows(builder.build());

        // ...and compare
        Assertions.assertThat(actualValues).isEqualTo(expectedValues);
    }

    private static List<List<ByteBuffer>> toRows(int... values)
    {
        List<List<ByteBuffer>> rows = new ArrayList<>();
        for (int value : values)
            rows.add(Collections.singletonList(Int32Type.instance.decompose(value)));
        return rows;
    }

    private static List<Integer> fromRows(List<List<ByteBuffer>> rows)
    {
        List<Integer> values = new ArrayList<>();
        for (List<ByteBuffer> row : rows)
            values.add(Int32Type.instance.compose(row.get(0)));
        return values;
    }

    private static Index.Scorer scorer(boolean reversed)
    {
        return new Index.Scorer()
        {
            @Override
            public float score(List<ByteBuffer> row)
            {
                return Int32Type.instance.compose(row.get(0));
            }

            @Override
            public boolean reversed()
            {
                return reversed;
            }
        };
    }
}
