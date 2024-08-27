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

package org.apache.cassandra.index.sai.disk.v6;

import java.io.IOException;
import java.nio.ByteOrder;

import org.junit.Test;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.index.sai.disk.ModernResettableByteBuffersIndexOutput;
import org.apache.cassandra.index.sai.disk.oldlucene.ByteArrayIndexInput;

import static org.junit.Assert.*;

public class TermsDistributionTest
{

    @Test
    public void testEmpty()
    {
        AbstractType<Integer> type = Int32Type.instance;
        TermsDistribution td = new TermsDistribution.Builder(type, 10, 10).build();
        assertEquals(0, td.estimateNumRowsMatchingExact(type.decompose(1)));
        assertEquals(0, td.estimateNumRowsInRange(type.decompose(0), type.decompose(1000)));
    }

    @Test
    public void testExactMatch()
    {
        AbstractType<Integer> type = Int32Type.instance;
        var builder = new TermsDistribution.Builder(type, 10, 10);
        for (int i = 0; i < 1000; i++)
            builder.add(type.decompose(i), 1);
        var td = builder.build();

        // in range:
        assertEquals(1, td.estimateNumRowsMatchingExact(type.decompose(0)));
        assertEquals(1, td.estimateNumRowsMatchingExact(type.decompose(17)));
        assertEquals(1, td.estimateNumRowsMatchingExact(type.decompose(999)));

        // out of range:
        assertEquals(0, td.estimateNumRowsMatchingExact(type.decompose(-1)));
        assertEquals(0, td.estimateNumRowsMatchingExact(type.decompose(1000)));
    }

    @Test
    public void testRangeMatch()
    {
        AbstractType<Integer> type = Int32Type.instance;
        var builder = new TermsDistribution.Builder(type, 10, 10);
        for (int i = 0; i < 1000; i++)
            builder.add(type.decompose(i), 1);
        var td = builder.build();

        // in range:
        assertEquals(10, td.estimateNumRowsInRange(type.decompose(-1), type.decompose(9)));
        assertEquals(10, td.estimateNumRowsInRange(type.decompose(0), type.decompose(10)));
        assertEquals(30, td.estimateNumRowsInRange(type.decompose(15), type.decompose(45)));

        // partially in range
        assertEquals(1000, td.estimateNumRowsInRange(type.decompose(-1), type.decompose(1000)));
        assertEquals(11, td.estimateNumRowsInRange(type.decompose(-1000), type.decompose(10)));
        assertEquals(9, td.estimateNumRowsInRange(type.decompose(990), type.decompose(200000)));

        // out of range:
        assertEquals(0, td.estimateNumRowsInRange(type.decompose(-10), type.decompose(-1)));
        assertEquals(0, td.estimateNumRowsInRange(type.decompose(1000), type.decompose(1003)));

        // test inclusiveness / exclusiveness:
        assertEquals(9, td.estimateNumRowsInRange(type.decompose(0), false, type.decompose(10), false));
        assertEquals(10, td.estimateNumRowsInRange(type.decompose(0), true, type.decompose(10), false));
        assertEquals(10, td.estimateNumRowsInRange(type.decompose(0), false, type.decompose(10), true));
        assertEquals(11, td.estimateNumRowsInRange(type.decompose(0), true, type.decompose(10), true));

        // test one side ranges:
        assertEquals(10, td.estimateNumRowsInRange(null, type.decompose(9)));
        assertEquals(10, td.estimateNumRowsInRange(null, true, type.decompose(10), false));
        assertEquals(10, td.estimateNumRowsInRange(null, false, type.decompose(10), false));
        assertEquals(11, td.estimateNumRowsInRange(null, false, type.decompose(10), true));
        assertEquals(11, td.estimateNumRowsInRange(null, false, type.decompose(10), true));
        assertEquals(10, td.estimateNumRowsInRange(type.decompose(990), true, null, true));
        assertEquals(10, td.estimateNumRowsInRange(type.decompose(990), true, null, false));
        assertEquals(9, td.estimateNumRowsInRange(type.decompose(990), false, null, false));
        assertEquals(9, td.estimateNumRowsInRange(type.decompose(990), false, null, false));
    }

    @Test
    public void testMostFrequentItems()
    {
        int frequentValue = 33; // whatever between 2 and 998
        int frequentCount = 100; // whatever > 1

        AbstractType<Integer> type = Int32Type.instance;
        var builder = new TermsDistribution.Builder(type, 10, 10);
        for (int i = 0; i < 1000; i++)
            builder.add(type.decompose(i), (i == frequentValue) ? frequentCount : 1);
        var td = builder.build();

        // Exact match the frequent term:
        assertEquals(frequentCount, td.estimateNumRowsMatchingExact(type.decompose(frequentValue)));
        assertEquals(frequentCount, td.estimateNumRowsInRange(type.decompose(frequentValue), true, type.decompose(frequentValue), true));

        // A range starting or ending at the frequent term:
        assertEquals(frequentCount, td.estimateNumRowsInRange(type.decompose(frequentValue - 1), false, type.decompose(frequentValue), true));
        assertEquals(frequentCount, td.estimateNumRowsInRange(type.decompose(frequentValue), true, type.decompose(frequentValue + 1), false));

        // A range containing the frequent term:
        assertEquals(frequentCount + 1, td.estimateNumRowsInRange(type.decompose(frequentValue - 1), type.decompose(frequentValue + 1)));
        assertEquals(frequentCount + 3, td.estimateNumRowsInRange(type.decompose(frequentValue - 2), type.decompose(frequentValue + 2)));

        // Ranges not containing the frequent term.
        // Frequencies of terms next to the frequent term must not be affected by the frequent term:
        assertEquals(1, td.estimateNumRowsMatchingExact(type.decompose(frequentValue - 1)));
        assertEquals(1, td.estimateNumRowsMatchingExact(type.decompose(frequentValue + 1)));
        assertEquals(1, td.estimateNumRowsInRange(type.decompose(frequentValue - 2), type.decompose(frequentValue - 1)));
        assertEquals(1, td.estimateNumRowsInRange(type.decompose(frequentValue), type.decompose(frequentValue + 1)));
    }


    @Test
    public void testFractionalBuckets()
    {
        // Test if we get reasonable range estimates when selecting a fraction of a single bucket:

        AbstractType<Double> type = DoubleType.instance;
        var builder = new TermsDistribution.Builder(type, 13, 13);
        var COUNT = 100000;
        for (int i = 0; i < COUNT; i++)
            builder.add(type.decompose((double) i / COUNT), 1);
        var td = builder.build();

        assertEquals(COUNT * 0.5, td.estimateNumRowsInRange(type.decompose(0.0), type.decompose(0.5)), 1);
        assertEquals(COUNT * 0.1, td.estimateNumRowsInRange(type.decompose(0.0), type.decompose(0.1)), 1);
        assertEquals(COUNT * 0.01, td.estimateNumRowsInRange(type.decompose(0.0), type.decompose(0.01)), 1);
        assertEquals(COUNT * 0.001, td.estimateNumRowsInRange(type.decompose(0.0), type.decompose(0.001)), 1);
        assertEquals(COUNT * 0.002, td.estimateNumRowsInRange(type.decompose(0.0), type.decompose(0.002)), 1);
        assertEquals(COUNT * 0.0005, td.estimateNumRowsInRange(type.decompose(0.0), type.decompose(0.0005)), 1);
        assertEquals(COUNT * 0.0002, td.estimateNumRowsInRange(type.decompose(0.0), type.decompose(0.0002)), 1);
        assertEquals(COUNT * 0.0001, td.estimateNumRowsInRange(type.decompose(0.0), type.decompose(0.0001)), 1);
        assertEquals(COUNT * 0.00005, td.estimateNumRowsInRange(type.decompose(0.0), type.decompose(0.00005)), 1);
        assertEquals(COUNT * 0.00002, td.estimateNumRowsInRange(type.decompose(0.0), type.decompose(0.00002)), 1);
        assertEquals(COUNT * 0.00001, td.estimateNumRowsInRange(type.decompose(0.0), type.decompose(0.00001)), 1);

        assertEquals(COUNT * 0.5, td.estimateNumRowsInRange(type.decompose(0.5), type.decompose(1.0)), 1);
        assertEquals(COUNT * 0.1, td.estimateNumRowsInRange(type.decompose(0.5), type.decompose(0.6)), 1);
        assertEquals(COUNT * 0.01, td.estimateNumRowsInRange(type.decompose(0.5), type.decompose(0.51)), 1);
        assertEquals(COUNT * 0.001, td.estimateNumRowsInRange(type.decompose(0.5), type.decompose(0.501)), 1);
        assertEquals(COUNT * 0.002, td.estimateNumRowsInRange(type.decompose(0.5), type.decompose(0.502)), 1);
        assertEquals(COUNT * 0.0005, td.estimateNumRowsInRange(type.decompose(0.5), type.decompose(0.5005)), 1);
        assertEquals(COUNT * 0.0002, td.estimateNumRowsInRange(type.decompose(0.5), type.decompose(0.5002)), 1);
        assertEquals(COUNT * 0.0001, td.estimateNumRowsInRange(type.decompose(0.5), type.decompose(0.5001)), 1);
        assertEquals(COUNT * 0.00005, td.estimateNumRowsInRange(type.decompose(0.5), type.decompose(0.50005)), 1);
        assertEquals(COUNT * 0.00002, td.estimateNumRowsInRange(type.decompose(0.5), type.decompose(0.50002)), 1);
        assertEquals(COUNT * 0.00001, td.estimateNumRowsInRange(type.decompose(0.5), type.decompose(0.50001)), 1);
    }


    @Test
    public void testSerde() throws IOException
    {
        AbstractType<Double> type = DoubleType.instance;
        var builder = new TermsDistribution.Builder(type, 10, 10);
        var COUNT = 100000;
        for (int i = 0; i < COUNT; i++)
            builder.add(type.decompose((double) i / COUNT), 1);
        var td = builder.build();

        try (var out = new ModernResettableByteBuffersIndexOutput(1024, ""))
        {
            td.write(out);
            var input = out.toArrayCopy();
            var tdCopy = TermsDistribution.read(new ByteArrayIndexInput("", input, ByteOrder.LITTLE_ENDIAN), type);

            assertEquals(td.numPoints, tdCopy.numPoints);
            assertEquals(td.numRows, tdCopy.numRows);
        }
    }
}