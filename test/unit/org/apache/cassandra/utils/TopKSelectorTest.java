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

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.base.Predicates;

import static org.junit.Assert.*;

public class TopKSelectorTest
{

    private TopKSelector<Integer> selector;

    @Before
    public void setUp()
    {
        selector = new TopKSelector<>(Integer::compareTo, 5);
    }

    @Test
    public void testBasicFunctionality()
    {
        List<Integer> data = List.of(10, 20, 5, 7, 1, 3, 9, 15, 25);

        for (int num : data)
        {
            selector.add(num);
        }

        List<Integer> topK = selector.get();

        // Expecting the smallest 5 elements in sorted order
        assertEquals(List.of(1, 3, 5, 7, 9), topK);
    }

    @Test
    public void testTopKSmallerThanK()
    {
        List<Integer> data = List.of(10, 20, 5);

        for (int num : data)
        {
            selector.add(num);
        }

        List<Integer> topK = selector.get();

        // Expecting all elements in sorted order because the total number is less than k
        assertEquals(List.of(5, 10, 20), topK);
    }

    @Test
    public void testTopKWithDuplicates()
    {
        List<Integer> data = List.of(10, 20, 5, 5, 7, 10, 1, 3, 15, 25);

        for (int num : data)
        {
            selector.add(num);
        }

        List<Integer> topK = selector.get();

        // Expecting the smallest 5 elements in sorted order
        assertEquals(List.of(1, 3, 5, 5, 7), topK);
    }


    @Test
    public void testTopKWithNulls()
    {
        List<Integer> data = Arrays.asList(10, null, 5, 5, 7, 10, 1, 3, null, 25);  // List.of does not like nulls

        for (Integer num : data)
        {
            selector.add(num);
        }

        List<Integer> topK = selector.get();

        // Expecting the smallest 5 elements in sorted order
        assertEquals(List.of(1, 3, 5, 5, 7), topK);
    }


    @Test
    public void testTopKWithNullsShort()
    {
        List<Integer> data = Arrays.asList(10, null, 5, 7, null);

        for (Integer num : data)
        {
            selector.add(num);
        }

        List<Integer> topK = selector.getShared();

        // Expecting the non-null elements in sorted order
        assertEquals(List.of(5, 7, 10), topK);
    }

    @Test
    public void testTopKWithNegativeNumbers()
    {
        List<Integer> data = List.of(-10, -20, -5, -7, -1, -3, -9, -15, -25);

        for (int num : data)
        {
            selector.add(num);
        }

        // also test transformation
        List<Integer> topK = selector.getTransformed(x -> -x);

        // Expecting the smallest (most negative) 5 elements in sorted order, transformed to -x
        assertEquals(List.of(25, 20, 15, 10, 9), topK);
    }

    @Test
    public void testEmptyInput()
    {
        List<Integer> topK = selector.get();

        // Expecting an empty list because no elements were added
        assertEquals(List.of(), topK);
    }

    @Test
    public void testSingleElementInput()
    {
        selector.add(42);

        List<Integer> topK = selector.get();

        // Expecting the single element in the list
        assertEquals(List.of(42), topK);
    }

    @Test
    public void testRandomizedInput()
    {
        Random random = new Random();
        int size = 1000;
        int k = random.nextInt(20) + 1;

        TopKSelector<Integer> randomSelector = new TopKSelector<>(Integer::compareTo, k);
        List<Integer> all = new ArrayList<>();
        for (int i = 0; i < size; i++)
        {
            Integer newItem = random.nextInt(10000); // Random values between 0 and 9999
            randomSelector.add(newItem);
            all.add(newItem);
        }

        List<Integer> topK = randomSelector.get();
        List<Integer> sortedTopK = all.stream().sorted().limit(k).collect(Collectors.toList());

        // Ensure the top k elements are sorted
        assertEquals(sortedTopK, topK);

        // Ensure the size of the top k list is k
        assertEquals(k, topK.size());
    }

    @Test
    public void testRandomizedInputNulls()
    {
        Random random = new Random();
        int size = 1000;
        int k = random.nextInt(20) + 1;

        TopKSelector<Integer> randomSelector = new TopKSelector<>(Integer::compareTo, k);
        List<Integer> all = new ArrayList<>();
        for (int i = 0; i < size; i++)
        {
            Integer newItem = random.nextDouble() < 0.05 ? null : random.nextInt(10000); // Random values between 0 and 9999
            randomSelector.add(newItem);
            all.add(newItem);
        }

        List<Integer> topK = randomSelector.get();
        List<Integer> sortedTopK = all.stream().filter(Predicates.notNull()).sorted().limit(k).collect(Collectors.toList());

        // Ensure the top k elements are sorted
        assertEquals(sortedTopK, topK);

        // Ensure the size of the top k list is k
        assertEquals(k, topK.size());
    }


    @Test
    public void testRandomizedInputTransformedSliced()
    {
        Random random = new Random();
        int size = 1000;
        int offset = random.nextInt(20) + 1;
        int k = random.nextInt(20);

        TopKSelector<Integer> randomSelector = new TopKSelector<>(Integer::compareTo, k + offset);
        List<Integer> all = new ArrayList<>();
        for (int i = 0; i < size; i++)
        {
            Integer newItem = random.nextInt(10000); // Random values between 0 and 9999
            randomSelector.add(newItem);
            all.add(newItem);
        }

        Function<Integer, Integer> transformer = x -> x + 5;
        List<Integer> topK = randomSelector.getTransformedSlicedShared(transformer, offset);
        List<Integer> sortedTopK = all.stream().sorted().map(transformer).skip(offset).limit(k).collect(Collectors.toList());

        // Ensure the top k elements are sorted
        assertEquals(sortedTopK, topK);

        // Ensure the size of the top k list is k
        assertEquals(k, topK.size());

        // Get the rest of the items now
        List<Integer> remainder = randomSelector.getShared();
        List<Integer> sortedRemainder = all.stream().sorted().limit(offset).collect(Collectors.toList());
        assertEquals(sortedRemainder, remainder);
    }

    @Test
    public void testAddMoreThanKElements()
    {
        for (int i = 0; i < 20; i++)
        {
            selector.add(i);
        }

        List<Integer> topK = selector.get();

        // Expecting the smallest 5 elements in sorted order
        assertEquals(List.of(0, 1, 2, 3, 4), topK);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testZeroK()
    {
        var selector = new TopKSelector<>(Integer::compareTo, 0);
        selector.addAll(List.of(10, 20, 3, 4, 5));
        assertEquals(List.of(), selector.get());
    }


    @Test
    public void testGetAndRestart()
    {
        testBasicFunctionality();
        // Reusing the selector which is reset by the get() call
        testTopKWithNegativeNumbers();
        // Also test resetting after getTransformed
        testBasicFunctionality();
    }
}
