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

import org.junit.Test;

import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.cql.GeoDistanceAccuracyTest;
import org.apache.cassandra.utils.memory.SlabPool;
import org.apache.lucene.util.SloppyMath;
import org.apache.lucene.util.VectorUtil;

public class GeoUtilTest
{
    @Test
    public void verifyDistanceFormulaAssumptions()
    {
        // Run 1 million iterations
        int iterations = 1000000;
        double sqDistDuration = 0;
        double haversineDuration = 0;
        for (int i = 0; i < iterations; i++)
        {
            // VSTODO make this work for the whole globe?
            float searchLat = SAITester.getRandom().nextFloatBetween(-90, 90);
            float searchLon = SAITester.getRandom().nextFloatBetween(0, 180);
            float pointLat = SAITester.getRandom().nextFloatBetween(-90, 90);
            float pointLon = SAITester.getRandom().nextFloatBetween(0, 180);

            // Get the haversine distance using a strict algorithm
            double strictDistance = GeoDistanceAccuracyTest.strictHaversineDistance(searchLat, searchLon, pointLat, pointLon);
            // This represents the maximum square distance that we can use to get the correct results when using
            // the square distance. Therefore, this value should always be less than or equal to the
            // square distance between the two points. Otherwise, there will be false postives.
            double maxSquareDistance = GeoUtil.maximumSquareDistanceForCorrectLatLongSimilarity((float) strictDistance);

            // Calculate the square distance.
            var nowSq = System.nanoTime();
            double squareDistance = VectorUtil.squareDistance(new float[]{ searchLat, searchLon }, new float[]{ pointLat, pointLon });
            sqDistDuration += System.nanoTime() - nowSq;

            // Calculate the sloppy distance (the one used in the code). This should always be greater than or equal
            // to the maxSquareDistance to ensure correctness.
            var nowH = System.nanoTime();
            double sloppyDistance = SloppyMath.haversinMeters(searchLat, searchLon, pointLat, pointLon);
            haversineDuration += System.nanoTime() - nowH;

            assertLessThanOrEqual(maxSquareDistance, squareDistance);
            assertLessThanOrEqual(maxSquareDistance, sloppyDistance);
        }
        System.out.println("Square distance average duration: " + sqDistDuration / iterations);
        System.out.println("Haversine average duration: " + haversineDuration / iterations);
    }

    private void assertLessThanOrEqual(double a, double b)
    {
        assert a <= b : String.format("%f is not less than or equal to %f", a, b);
    }
}
