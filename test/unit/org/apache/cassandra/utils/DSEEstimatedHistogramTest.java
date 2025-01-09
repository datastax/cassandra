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

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;

/**
 * Test for any estimated histogram that uses DSE compatible boundaries. This needs to be a separate test class because
 * the property value is inlined as static final in the EstimatedHistogram class.
 */
public class DSEEstimatedHistogramTest
{
    @BeforeClass
    public static void setup()
    {
        CassandraRelevantProperties.USE_DSE_COMPATIBLE_HISTOGRAM_BOUNDARIES.setBoolean(true);
    }

    @Test
    public void testDSEBoundaries()
    {
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
}
