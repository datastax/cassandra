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

package org.apache.cassandra.index.sai.cql;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Test;

import org.apache.cassandra.utils.Pair;
import org.apache.lucene.geo.GeoUtils;

import static org.junit.Assert.assertTrue;

public class GeoSearchAccuracyTest extends VectorTester
{
    // Number indicates that 98% of the search results are truly within the searched distance
    // In testing, it appeared to be around 99.9% for NYC. The accuracy improves as the latitude
    // approaches the equator.
    private final static float EXPECTED_ACCURACY = 0.98f;

    @Test
    public void testRandomVectorsAgainstHaversineDistance()
    {
        createTable("CREATE TABLE %s (pk int, val vector<float, 2>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function' : 'euclidean'}");
        waitForIndexQueryable();
        int numVectors = 20000;
        var vectors = IntStream.range(0, numVectors).mapToObj(s -> Pair.create(s, createRandomNYCVector())).collect(Collectors.toList());

        // Insert the vectors
        for (var vector : vectors)
            execute("INSERT INTO %s (pk, val) VALUES (?, ?)", vector.left, vector(vector.right));

        double accuracy = 0;
        int queryCount = 100;
        for (int i = 0; i < queryCount; i++)
        {
            var searchVector = createRandomNYCVector();
            // Pick a random distance between 1km and 10km
            var distanceInMeters = getRandom().nextIntBetween(1000, 10000);
            // Get the "correct" results using the great circle distance or the haversine formula
            var closeVectors = vectors.stream()
                                      .filter(v -> isWithinDistance(v.right, searchVector, distanceInMeters))
                                      .collect(Collectors.toList());

            var results = execute("SELECT pk FROM %s WHERE GEO_DISTANCE(val, ?) < " + distanceInMeters, vector(searchVector));

            // The current algorithm for searching by latitude and longitude produces a superset of results. We expect
            // all the results that are within the great circle distance to also be within the search distance.
            var expected = closeVectors.stream().map(v -> v.left).collect(Collectors.toSet());
            var actual = results.stream().map(r -> r.getInt("pk")).collect(Collectors.toSet());
            assertTrue("Actual should be a superset of expected", actual.containsAll(expected));
            if (!actual.isEmpty())
                accuracy += (double) expected.size() / actual.size();
        }
        double observedAccuracy = accuracy / queryCount;
        logger.info("Observed accuracy: {}", observedAccuracy);
        assertTrue("Accuracy should be greater than " + EXPECTED_ACCURACY + " but found " + observedAccuracy,
                   observedAccuracy > EXPECTED_ACCURACY);
    }

    private boolean isWithinDistance(float[] vector, float[] searchVector, int distanceInMeters)
    {
        return strictHaversineDistance(vector[0], vector[1], searchVector[0], searchVector[1]) < distanceInMeters;
    }

    private float[] createRandomNYCVector()
    {
        // Approximate bounding box for contiguous US locations
        var lat = getRandom().nextFloatBetween(39, 41);
        var lon = getRandom().nextFloatBetween(-74, -72);
        return new float[] {lat, lon};
    }

    // In the production code, we use a haversine distance formula from lucene, which prioritizes speed over some
    // accuracy. This is the strict formula.
    private double strictHaversineDistance(float lat1, float lon1, float lat2, float lon2)
    {
        double phi1 = lat1 * Math.PI/180; // phi, lambda in radians
        double phi2 = lat2 * Math.PI/180;
        double deltaPhi = (lat2 - lat1) * Math.PI/180;
        double deltaLambda = (lon2 - lon1) * Math.PI/180;

        double a = Math.sin(deltaPhi / 2.0) * Math.sin(deltaPhi / 2.0) +
                   Math.cos(phi1) * Math.cos(phi2) *
                   Math.sin(deltaLambda / 2.0) * Math.sin(deltaLambda / 2.0);
        double c = 2.0 * Math.atan2(Math.sqrt(a), Math.sqrt(1.0 - a));

        return GeoUtils.EARTH_MEAN_RADIUS_METERS * c; // in meters
    }
}
