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

import org.junit.Test;

import static org.junit.Assert.*;

import java.util.*;

import com.google.common.base.Predicates;

public class SortingIteratorTest
{
    // Most of this test is ChatGPT-generated.

    @Test
    public void testSortingIterator_withFixedData()
    {
        List<Integer> data = List.of(4, 1, 3, 2);
        Iterator<Integer> iterator = SortingIterator.create(Comparator.naturalOrder(), data);

        List<Integer> sorted = new ArrayList<>();
        while (iterator.hasNext())
        {
            sorted.add(iterator.next());
        }

        assertEquals(List.of(1, 2, 3, 4), sorted);
    }

    @Test
    public void testSortingIterator_withEmptyData()
    {
        List<Integer> data = List.of();
        Iterator<Integer> iterator = SortingIterator.create(Comparator.naturalOrder(), data);

        assertFalse(iterator.hasNext());
    }

    @Test
    public void testNextWithoutHasNext()
    {
        List<String> data = List.of("apple", "orange", "banana");
        Iterator<String> iterator = SortingIterator.create(Comparator.naturalOrder(), data);

        assertEquals("apple", iterator.next());
        assertEquals("banana", iterator.next());
        assertEquals("orange", iterator.next());
        assertFalse(iterator.hasNext());
    }

    @Test(expected = NoSuchElementException.class)
    public void testNoSuchElementException()
    {
        List<Integer> data = List.of(1, 3, 2);
        Iterator<Integer> iterator = SortingIterator.create(Comparator.naturalOrder(), data);

        iterator.next();
        iterator.next();
        iterator.next();
        iterator.next(); // Should throw NoSuchElementException
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testUnsupportedOperationException()
    {
        List<Integer> data = List.of(1, 2, 3);
        Iterator<Integer> iterator = SortingIterator.create(Comparator.naturalOrder(), data);

        iterator.remove(); // Should throw UnsupportedOperationException
    }


    @Test
    public void testWithDuplicates()
    {
        List<Integer> data = List.of(4, 1, 2, 5, 3, 4, 2, 4, 4);
        var iterator = SortingIterator.create(Comparator.naturalOrder(), data);

        List<Integer> result = new ArrayList<>();
        while (iterator.hasNext())
        {
            result.add(iterator.next());
        }

        assertEquals(List.of(1, 2, 2, 3, 4, 4, 4, 4, 5), result);
    }

    @Test
    public void testSkipTo_existingKey()
    {
        List<Integer> data = List.of(5, 3, 1, 9, 7);
        var iterator = SortingIterator.create(Comparator.naturalOrder(), data);

        iterator.skipTo(5);

        assertTrue(iterator.hasNext());
        assertEquals((Integer) 5, iterator.next());
    }

    @Test
    public void testSkipTo_nonExistingKey()
    {
        List<Integer> data = List.of(1, 5, 7, 9, 3);
        var iterator = SortingIterator.create(Comparator.naturalOrder(), data);

        iterator.skipTo(6);

        assertTrue(iterator.hasNext());
        assertEquals((Integer) 7, iterator.next());
    }

    @Test
    public void testSkipTo_beyondLastKey()
    {
        List<Integer> data = List.of(9, 3, 1, 7, 5);
        var iterator = SortingIterator.create(Comparator.naturalOrder(), data);

        iterator.skipTo(10);

        assertFalse(iterator.hasNext());
    }

    @Test
    public void testSkipTo_firstKey()
    {
        List<Integer> data = List.of(1, 5, 3, 9, 7);
        var iterator = SortingIterator.create(Comparator.naturalOrder(), data);

        iterator.skipTo(1);

        assertTrue(iterator.hasNext());
        assertEquals((Integer) 1, iterator.next());
    }

    @Test
    public void testSkipTo_beforeFirstKey()
    {
        List<Integer> data = List.of(3, 9, 1, 7, 5);
        var iterator = SortingIterator.create(Comparator.naturalOrder(), data);

        iterator.skipTo(0);

        assertTrue(iterator.hasNext());
        assertEquals((Integer) 1, iterator.next());
    }


    @Test
    public void testSkipTo_withDuplicates()
    {
        List<Integer> data = List.of(3, 4, 1, 4, 2, 4, 5, 2, 4);
        var iterator = SortingIterator.create(Comparator.naturalOrder(), data);

        iterator.skipTo(2);
        assertTrue(iterator.hasNext());
        assertEquals((Integer) 2, iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals((Integer) 2, iterator.next());

        iterator.skipTo(4);
        assertTrue(iterator.hasNext());
        assertEquals((Integer) 4, iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals((Integer) 4, iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals((Integer) 4, iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals((Integer) 4, iterator.next());

        iterator.skipTo(5);
        assertTrue(iterator.hasNext());
        assertEquals((Integer) 5, iterator.next());
    }

    @Test
    public void testSkipTo_onEmptyCollection()
    {
        List<Integer> data = List.of();
        var iterator = SortingIterator.create(Comparator.naturalOrder(), data);

        iterator.skipTo(5);

        assertFalse(iterator.hasNext());
    }

    @Test
    public void testRandomizedSkipTo()
    {
        Random random = new Random();
        int size = random.nextInt(100) + 50; // List size between 50 and 150
        List<Integer> data = new ArrayList<>();

        // Generate random data
        for (int i = 0; i < size; i++)
        {
            data.add(random.nextInt(200)); // Values between 0 and 199
        }

        // Create the iterator
        var iterator = SortingIterator.create(Comparator.naturalOrder(), data);

        Collections.sort(data);

        // Track the current index
        int currentIndex = 0;

        // Perform random skipTo operations
        for (int i = 0; i < 10; i++)
        { // Perform 10 random skipTo operations
            int targetKey = random.nextInt(200); // Random target key between 0 and 199
            iterator.skipTo(targetKey);

            // Find the expected position, considering the current position
            int expectedIndex = Integer.MAX_VALUE;
            for (int j = currentIndex; j < data.size(); j++)
            {
                if (data.get(j) >= targetKey)
                {
                    expectedIndex = j;
                    break;
                }
            }

            if (expectedIndex >= data.size())
            {
                // If no element is greater than or equal to targetKey, iterator should be exhausted
                assertFalse(iterator.hasNext());
                currentIndex = expectedIndex;
            }
            else
            {
                // Otherwise, the next element should be the expected one
                assertTrue(iterator.hasNext());
                assertEquals(data.get(expectedIndex), iterator.next());
                currentIndex = expectedIndex + 1;
            }
        }
    }


    @Test
    public void testSortingIterator_withRandomData()
    {
        Random random = new Random();
        int size = 10000;
        List<Integer> data = new ArrayList<>();
        for (int i = 0; i < size; i++)
        {
            data.add(random.nextInt(size));
        }

        Iterator<Integer> iterator = SortingIterator.create(Comparator.naturalOrder(), data);
        List<Integer> sorted = new ArrayList<>();
        while (iterator.hasNext())
        {
            sorted.add(iterator.next());
        }

        List<Integer> expected = new ArrayList<>(data);
        Collections.sort(expected);

        assertEquals(expected, sorted);
    }

    @Test
    public void testSortingIterator_skipToRandomData()
    {
        Random random = new Random();
        int size = 10000;
        List<Integer> data = new ArrayList<>();
        for (int i = 0; i < size; i++)
        {
            data.add(random.nextInt(size));
        }

        List<Integer> sorted = new ArrayList<>(data);
        Collections.sort(sorted);
        List<Integer> expected = new ArrayList<>();

        var iterator = SortingIterator.create(Comparator.naturalOrder(), data);
        List<Integer> iterated = new ArrayList<>();

        int skipDistanceMax = 200;
        for (int i = 0; i < size; i += random.nextInt(skipDistanceMax))
        {
            int targetMax = sorted.get(i);
            int targetMin = i > 0 ? sorted.get(i - 1) : 0;
            if (targetMin == targetMax)
                continue;
            int target = random.nextInt(targetMax - targetMin) + targetMin + 1; // (targetMin + 1; targetMax]
            iterator.skipTo(target);
            for (int c = random.nextInt(5); c >= 0; --c)
            {
                if (i >= size)
                {
                    assert !iterator.hasNext();
                    break;
                }
                iterated.add(iterator.next());
                expected.add(sorted.get(i++));
            }
        }

        assertEquals(expected, iterated);
    }

    @Test
    public void testDeduplicateRemovesDuplicates()
    {
        List<Integer> data = List.of(4, 1, 2, 5, 3, 4, 2, 4, 4);
        var iterator = SortingIterator.createDeduplicating(Comparator.naturalOrder(), data);

        List<Integer> result = new ArrayList<>();
        while (iterator.hasNext())
        {
            result.add(iterator.next());
        }

        assertEquals(List.of(1, 2, 3, 4, 5), result);
    }

    @Test
    public void testSkipTo_deduplicate()
    {
        List<Integer> data = List.of(3, 4, 1, 4, 2, 4, 5, 2, 4);
        var iterator = SortingIterator.createDeduplicating(Comparator.naturalOrder(), data);

        iterator.skipTo(3);
        assertTrue(iterator.hasNext());
        assertEquals((Integer) 3, iterator.next());

        iterator.skipTo(4);
        assertTrue(iterator.hasNext());
        assertEquals((Integer) 4, iterator.next());

        iterator.skipTo(5);
        assertTrue(iterator.hasNext());
        assertEquals((Integer) 5, iterator.next());
    }

    @Test
    public void testSkipTo_deduplicateWithNonExistingTarget()
    {
        List<Integer> data = List.of(4, 5, 1, 3, 4, 4, 2, 4, 2);
        var iterator = SortingIterator.createDeduplicating(Comparator.naturalOrder(), data);

        iterator.skipTo(2); // Skip to the first occurrence of 2
        assertTrue(iterator.hasNext());
        assertEquals((Integer) 2, iterator.next());
        // The above must have consumed all 2s
        assertTrue(iterator.hasNext());
        assertEquals((Integer) 3, iterator.next());

        iterator.skipTo(6); // Skip to a non-existing key, should end iteration
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testEmptyCollectionDeduplicate()
    {
        List<Integer> data = List.of();
        var iterator = SortingIterator.createDeduplicating(Comparator.naturalOrder(), data);

        assertFalse(iterator.hasNext());
    }

    @Test
    public void testSingleElementDeduplicate()
    {
        List<Integer> data = List.of(42, 42, 42);
        var iterator = SortingIterator.createDeduplicating(Comparator.naturalOrder(), data);

        assertTrue(iterator.hasNext());
        assertEquals((Integer) 42, iterator.next());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testRandomizedDeduplicate()
    {
        Random random = new Random();
        int size = random.nextInt(100) + 50; // List size between 50 and 150
        List<Integer> data = new ArrayList<>();

        // Generate random data with potential duplicates
        for (int i = 0; i < size; i++)
        {
            data.add(random.nextInt(50)); // Values between 0 and 49, likely to have duplicates
        }

        // Construct through Builder for coverage
        var iterator = new SortingIterator.Builder<>(data, x -> x)
                           .deduplicating(Comparator.naturalOrder());

        // Using a set to verify uniqueness of the iterator output
        Set<Integer> seenElements = new HashSet<>();
        while (iterator.hasNext())
        {
            Integer element = iterator.next();
            assertFalse(seenElements.contains(element)); // Ensure no duplicates are returned
            seenElements.add(element);
        }

        // Verify that all elements in the set were indeed from the original data (in sorted, unique form)
        Set<Integer> expectedElements = new TreeSet<>(data); // TreeSet to sort and remove duplicates
        assertEquals(expectedElements, seenElements);
    }

    @Test
    public void testSortingIterator_randomWithNulls()
    {
        Random random = new Random();
        int size = 10000;
        List<Integer> data = new ArrayList<>();
        for (int i = 0; i < size; i++)
        {
            data.add(random.nextInt(10) != 0 ? random.nextInt(size) : null);
        }

        Iterator<Integer> iterator = SortingIterator.create(Comparator.naturalOrder(), data);
        List<Integer> sorted = new ArrayList<>();
        while (iterator.hasNext())
        {
            sorted.add(iterator.next());
        }

        List<Integer> expected = new ArrayList<>(data);
        expected.removeIf(Predicates.isNull());
        Collections.sort(expected);

        assertEquals(expected, sorted);
    }

    @Test
    public void testSortingIterator_skipToRandomDataWithNulls()
    {
        Random random = new Random();
        int size = 10000;
        List<Integer> data = new ArrayList<>();
        for (int i = 0; i < size; i++)
        {
            data.add(random.nextInt(10) != 0 ? random.nextInt(size) : null);
        }

        List<Integer> sorted = new ArrayList<>(data);
        sorted.removeIf(Predicates.isNull());
        Collections.sort(sorted);
        size = sorted.size();
        List<Integer> expected = new ArrayList<>();

        var iterator = SortingIterator.create(Comparator.naturalOrder(), data);
        List<Integer> iterated = new ArrayList<>();

        int skipDistanceMax = 200;
        for (int i = 0; i < size; i += random.nextInt(skipDistanceMax))
        {
            int targetMax = sorted.get(i);
            int targetMin = i > 0 ? sorted.get(i - 1) : 0;
            if (targetMin == targetMax)
                continue;
            int target = random.nextInt(targetMax - targetMin) + targetMin + 1; // (targetMin + 1; targetMax]
            iterator.skipTo(target);
            for (int c = random.nextInt(5); c >= 0; --c)
            {
                if (i >= size)
                {
                    assert !iterator.hasNext();
                    break;
                }
                iterated.add(iterator.next());
                expected.add(sorted.get(i++));
            }
        }

        assertEquals(expected, iterated);
    }

    /**
     * Dump the evolution of a heap of 15 elements as mermaid graph sources for visualizations/documentation.
     */
    @Test
    public void makeMermaids()
    {
        int size = 15;
        Random rand = new Random(52);
        Integer[] array = new Integer[size];
        for (int i = 0; i < size; ++i)
            array[i] = rand.nextDouble() < 0.03 ? null : rand.nextInt(100);
        System.out.println(Arrays.toString(array));

        var sorter = new SortingIterator<Integer>(Comparator.naturalOrder(), array);
        var sorted = new ArrayList<Integer>();
        while (sorter.hasNext())
        {
            System.out.println(sorter.toMermaid());
            sorted.add(sorter.next());
        }
        System.out.println(sorted);
    }
}
