/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.utils;

import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.metrics.DecayingEstimatedHistogramReservoir;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class EstimatedHistogramTest
{
    @Test
    public void testSimple()
    {
        {
            // 0 and 1 map to the same, first bucket
            EstimatedHistogram histogram = new EstimatedHistogram();
            histogram.add(0);
            assertEquals(1, histogram.get(0));
            histogram.add(1);
            assertEquals(2, histogram.get(0));
        }
        {
            // 0 and 1 map to different buckets
            EstimatedHistogram histogram = new EstimatedHistogram(90, true);
            histogram.add(0);
            assertEquals(1, histogram.get(0));
            histogram.add(1);
            assertEquals(1, histogram.get(0));
            assertEquals(1, histogram.get(1));
        }
    }

    @Test
    public void testOverflow()
    {
        EstimatedHistogram histogram = new EstimatedHistogram(1);
        histogram.add(100);
        assert histogram.isOverflowed();
        assertEquals(Long.MAX_VALUE, histogram.max());
    }

    @Test
    public void testMinMax()
    {
        EstimatedHistogram histogram = new EstimatedHistogram();
        histogram.add(16);
        assertEquals(15, histogram.min());
        assertEquals(17, histogram.max());
    }

    @Test
    public void testMean()
    {
        {
            EstimatedHistogram histogram = new EstimatedHistogram();
            for (int i = 0; i < 40; i++)
                histogram.add(0);
            for (int i = 0; i < 20; i++)
                histogram.add(1);
            for (int i = 0; i < 10; i++)
                histogram.add(2);
            assertEquals(70, histogram.count());
            assertEquals(2, histogram.mean());
        }
        {
            EstimatedHistogram histogram = new EstimatedHistogram(90, true);
            for (int i = 0; i < 40; i++)
                histogram.add(0);
            for (int i = 0; i < 20; i++)
                histogram.add(1);
            for (int i = 0; i < 10; i++)
                histogram.add(2);
            assertEquals(70, histogram.count());
            assertEquals(1, histogram.mean());
        }
    }

    @Test
    public void testFindingCorrectBuckets()
    {
        EstimatedHistogram histogram = new EstimatedHistogram();
        histogram.add(23282687);
        assert !histogram.isOverflowed();
        assertEquals(1, histogram.getBuckets(false)[histogram.buckets.length() - 2]);

        histogram.add(9);
        assertEquals(1, histogram.getBuckets(false)[8]);

        histogram.add(20);
        histogram.add(21);
        histogram.add(22);
        assertEquals(2, histogram.getBuckets(false)[13]);
        assertEquals(5021848, histogram.mean());
    }

    @Test
    public void testPercentile()
    {
        {
            EstimatedHistogram histogram = new EstimatedHistogram();
            // percentile of empty histogram is 0
            assertEquals(0, histogram.percentile(0.99));

            histogram.add(1);
            // percentile of a histogram with one element should be that element
            assertEquals(1, histogram.percentile(0.99));

            histogram.add(10);
            assertEquals(10, histogram.percentile(0.99));
        }

        {
            EstimatedHistogram histogram = new EstimatedHistogram();

            histogram.add(1);
            histogram.add(2);
            histogram.add(3);
            histogram.add(4);
            histogram.add(5);

            assertEquals(0, histogram.percentile(0.00));
            assertEquals(3, histogram.percentile(0.50));
            assertEquals(3, histogram.percentile(0.60));
            assertEquals(5, histogram.percentile(1.00));
        }

        {
            EstimatedHistogram histogram = new EstimatedHistogram();

            for (int i = 11; i <= 20; i++)
                histogram.add(i);

            // Right now the histogram looks like:
            //    10   12   14   17   20
            //     0    2    2    3    3
            // %:  0   20   40   70  100
            assertEquals(12, histogram.percentile(0.01));
            assertEquals(14, histogram.percentile(0.30));
            assertEquals(17, histogram.percentile(0.50));
            assertEquals(17, histogram.percentile(0.60));
            assertEquals(20, histogram.percentile(0.80));
        }
        {
            EstimatedHistogram histogram = new EstimatedHistogram(90, true);
            histogram.add(0);
            histogram.add(0);
            histogram.add(1);

            assertEquals(0, histogram.percentile(0.5));
            assertEquals(1, histogram.percentile(0.99));
        }
    }

    @Test
    public void testClearOverflow()
    {
        EstimatedHistogram histogram = new EstimatedHistogram(1);
        histogram.add(100);
        assertTrue(histogram.isOverflowed());
        histogram.clearOverflow();
        assertFalse(histogram.isOverflowed());
    }

    @Test
    public void testDSEBoundaries()
    {
        boolean bucketBoundaries = CassandraRelevantProperties.USE_DSE_COMPATIBLE_HISTOGRAM_BOUNDARIES.getBoolean();
        try
        {
            CassandraRelevantProperties.USE_DSE_COMPATIBLE_HISTOGRAM_BOUNDARIES.setBoolean(true);
            // these boundaries were computed in DSE
            long[] dseBoundaries = new long[]{ 1, 2, 3, 4, 5, 6, 7, 8, 10, 12, 14, 16, 20, 24, 28, 32, 40,
                                               48, 56, 64, 80, 96, 112, 128, 160, 192, 224, 256, 320, 384, 448, 512, 640, 768, 896, 1024, 1280, 1536, 1792,
                                               2048, 2560, 3072, 3584, 4096, 5120, 6144, 7168, 8192, 10240, 12288, 14336, 16384, 20480, 24576, 28672, 32768,
                                               40960, 49152, 57344, 65536, 81920, 98304, 114688, 131072, 163840, 196608, 229376, 262144, 327680, 393216,
                                               458752, 524288, 655360, 786432, 917504, 1048576, 1310720, 1572864, 1835008, 2097152, 2621440, 3145728, 3670016,
                                               4194304, 5242880, 6291456, 7340032, 8388608, 10485760, 12582912, 14680064, 16777216, 20971520, 25165824,
                                               29360128, 33554432, 41943040, 50331648, 58720256, 67108864, 83886080, 100663296, 117440512, 134217728,
                                               167772160, 201326592, 234881024, 268435456, 335544320, 402653184, 469762048, 536870912, 671088640, 805306368,
                                               939524096, 1073741824, 1342177280, 1610612736, 1879048192, 2147483648L, 2684354560L, 3221225472L, 3758096384L,
                                               4294967296L, 5368709120L, 6442450944L, 7516192768L, 8589934592L, 10737418240L, 12884901888L, 15032385536L, 17179869184L,
                                               21474836480L, 25769803776L, 30064771072L, 34359738368L, 42949672960L, 51539607552L, 60129542144L, 68719476736L,
                                               85899345920L, 103079215104L, 120259084288L, 137438953472L, 171798691840L, 206158430208L, 240518168576L, 274877906944L,
                                               343597383680L, 412316860416L, 481036337152L, 549755813888L, 687194767360L, 824633720832L, 962072674304L, 1099511627776L,
                                               1374389534720L, 1649267441664L, 1924145348608L, 2199023255552L, 2748779069440L, 3298534883328L, 3848290697216L,
                                               4398046511104L, 5497558138880L, 6597069766656L, 7696581394432L, 8796093022208L, 10995116277760L, 13194139533312L,
                                               15393162788864L, 17592186044416L, 21990232555520L, 26388279066624L, 30786325577728L, 35184372088832L, 43980465111040L,
                                               52776558133248L, 61572651155456L, 70368744177664L, 87960930222080L, 105553116266496L, 123145302310912L,
                                               140737488355328L, 175921860444160L };

            // the code below is O(n^2) so that we don't need to assume that boundaries are independent
            // of the histogram size; this is not a problem since the number of boundaries is small
            for (int size = 1; size <= dseBoundaries.length; size++)
            {
                EstimatedHistogram histogram = new EstimatedHistogram(size);
                // compute subarray of dseBoundaries of size `size`
                long[] subarray = new long[size];
                System.arraycopy(dseBoundaries, 0, subarray, 0, size);
                Assert.assertArrayEquals(subarray, histogram.getBucketOffsets());
            }
        }
        finally
        {
            CassandraRelevantProperties.USE_DSE_COMPATIBLE_HISTOGRAM_BOUNDARIES.setBoolean(bucketBoundaries);
        }
    }
}
