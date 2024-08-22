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

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class IntIntPairArrayTest
{
    @Test
    public void testOperationsOnIntIntPairArray()
    {
        IntIntPairArray array = new IntIntPairArray(2);
        assertEquals(0, array.size());
        array.add(1, 2);
        array.add(3, 4);
        assertEquals(2, array.size());

        // Validate the iteration
        var accumulator = new AtomicInteger();
        array.forEachRightInt(accumulator::addAndGet);
        assertEquals(6, accumulator.get());

        accumulator.set(0);
        array.forEachIntPair((x,y) -> {
            accumulator.addAndGet(x);
            accumulator.addAndGet(y);
        });
        assertEquals(10, accumulator.get());
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testAddToFullArray()
    {
        IntIntPairArray array = new IntIntPairArray(1);
        array.add(1, 2);
        array.add(3, 4);
    }

    @Test(expected = AssertionError.class)
    public void testCapacityTooLarge()
    {
        new IntIntPairArray(Integer.MAX_VALUE / 2 + 1);
    }
}
