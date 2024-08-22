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

package org.apache.cassandra.index.sai.utils;

import java.util.function.IntConsumer;

import org.agrona.collections.IntIntConsumer;

/**
 * A simple array of int pairs that avoids boxing. Implemented as an alternative to the
 * {@link org.agrona.collections.Int2IntHashMap} for uses that do not require lookups.
 */
public class IntIntPairArray
{
    private final int capacity;
    private int size;
    private final int[] array;

    /**
     * Create a new IntIntPairArray with the given capacity.
     * @param capacity
     */
    public IntIntPairArray(int capacity)
    {
        assert capacity < Integer.MAX_VALUE / 2 : "capacity is too large " + capacity;
        this.capacity = capacity;
        this.size = 0;
        this.array = new int[capacity * 2];
    }

    /**
     * Add a pair to the array.
     * @param x the first value
     * @param y the second value
     */
    public void add(int x, int y)
    {
        if (size == capacity)
            throw new IndexOutOfBoundsException(size);
        array[size * 2] = x;
        array[size * 2 + 1] = y;
        size++;
    }

    /**
     * The number of pairs in the array.
     * @return the number of pairs in the array
     */
    public int size()
    {
        return size;
    }

    /**
     * Iterate over the pairs in the array, calling the consumer for each pair.
     * @param consumer the consumer to call for each pair
     */
    public void forEachIntPair(IntIntConsumer consumer)
    {
        for (int i = 0; i < size; i++)
            consumer.accept(array[i * 2], array[i * 2 + 1]);
    }

    /**
     * Calls the consumer for each right value in each pair of the array.
     * @param consumer the consumer to call for each right value
     */
    public void forEachRightInt(IntConsumer consumer)
    {
        for (int i = 0; i < size; i++)
            consumer.accept(array[i * 2 + 1]);
    }
}
