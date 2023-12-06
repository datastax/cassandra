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

package org.apache.cassandra.metrics;

import org.junit.Test;

import org.apache.cassandra.metrics.PairedSlidingWindowReservoir.IntIntPair;

import static org.junit.Assert.assertEquals;

public class LinearFitTest
{
    @Test
    public void testInterceptSlopeFor()
    {
        var values = new IntIntPair[10];
        for (int i = 0; i < values.length; i++)
            values[i] = new IntIntPair(i * 2, i);

        var pair = LinearFit.interceptSlopeFor(values);
        assertEquals(0.0, pair.left, 0.01);
        assertEquals(2.0, pair.right, 0.01);

        values = new IntIntPair[10];
        for (int i = 0; i < values.length; i++)
            values[i] = new IntIntPair(1 + i * 2, i);

        pair = LinearFit.interceptSlopeFor(values);
        assertEquals(1.0, pair.left, 0.01d);
        assertEquals(2.0, pair.right, 0.01d);

        values = new IntIntPair[10];
        for (int i = 0; i < values.length; i++)
            values[i] = new IntIntPair(-1 + i * 2, i);

        pair = LinearFit.interceptSlopeFor(values);
        assertEquals(-1.0, pair.left, 0.01d);
        assertEquals(2.0, pair.right, 0.01d);
    }

    @Test
    public void testInterceptSlopeWithNoise()
    {
        var values = new IntIntPair[] {
            new IntIntPair(66, 1),
            new IntIntPair(108, 2),
            new IntIntPair(70, 3),
            new IntIntPair(112, 4),
            new IntIntPair(74, 5),
            new IntIntPair(116, 6),
            new IntIntPair(78, 7),
            new IntIntPair(120, 8),
            new IntIntPair(82, 9)
        };
        var pair = LinearFit.interceptSlopeFor(values);
        // verified with sklearn
        assertEquals(2.0, pair.right, 0.01);
        assertEquals(81.78, pair.left, 0.01);
    }

    @Test
    public void testInterceptSlopeWithMoreNoise()
    {
        var values = new IntIntPair[] {
            new IntIntPair(1366, 931),
            new IntIntPair(822, 973),
            new IntIntPair(1308, 200),
            new IntIntPair(708, 332),
            new IntIntPair(1186, 677),
            new IntIntPair(7112, 401),
            new IntIntPair(166, 111),
            new IntIntPair(734, 503),
            new IntIntPair(78, 738),
            new IntIntPair(8120, 829)
        };
        var pair = LinearFit.interceptSlopeFor(values);
        // verified with sklearn
        assertEquals(1.31, pair.right, 0.01);
        assertEquals(1412.35, pair.left, 0.01);
    }
}
