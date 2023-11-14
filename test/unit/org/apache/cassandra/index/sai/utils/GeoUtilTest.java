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

import java.util.logging.Logger;

import org.junit.Test;

import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.cql.GeoDistanceAccuracyTest;
import org.apache.cassandra.utils.memory.SlabPool;
import org.apache.lucene.util.SloppyMath;
import org.apache.lucene.util.VectorUtil;

import static org.junit.Assert.assertTrue;

public class GeoUtilTest
{
    private static final Logger logger = Logger.getLogger(GeoUtilTest.class.getName());
    @Test
    public void haversineBenchmark()
    {
        // Run 1 million iterations
        int iterations = 1000000;
        double strictHaversineDuration = 0;
        double sloppyHaversineDuration = 0;
        for (int i = 0; i < iterations; i++)
        {
            float searchLat = SAITester.getRandom().nextFloatBetween(-90, 90);
            float searchLon = SAITester.getRandom().nextFloatBetween(-180, 180);
            float pointLat = SAITester.getRandom().nextFloatBetween(-90, 90);
            float pointLon = SAITester.getRandom().nextFloatBetween(-180, 180);

            // Get the haversine distance using a strict algorithm
            var nowStrict = System.nanoTime();
            GeoDistanceAccuracyTest.strictHaversineDistance(searchLat, searchLon, pointLat, pointLon);
            strictHaversineDuration += System.nanoTime() - nowStrict;

            // Calculate the sloppy distance (the one used in the code)
            var nowSloppy = System.nanoTime();
            SloppyMath.haversinMeters(searchLat, searchLon, pointLat, pointLon);
            sloppyHaversineDuration += System.nanoTime() - nowSloppy;
        }

        double strictHaversineAverage = strictHaversineDuration / iterations;
        double sloppyHaversineAverage = sloppyHaversineDuration / iterations;
        logger.info("Average duration for strict haversine: " + strictHaversineAverage);
        logger.info("Average duration for sloppy haversine: " + sloppyHaversineAverage);
        assertTrue("Sloppy haversine distance should be at least as fast as strict haversine distance.",
                   sloppyHaversineAverage <= strictHaversineAverage);
    }
}
