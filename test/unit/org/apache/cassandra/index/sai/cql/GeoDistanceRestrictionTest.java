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

import org.junit.Test;

public class GeoDistanceRestrictionTest extends VectorTester
{
    @Test
    public void testBasicGeoDistanceQuery() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, v vector<float, 2>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function' : 'euclidean'}");

        // Distances computed using GeoDistanceAccuracyTest#strictHaversineDistance
        execute("INSERT INTO %s (pk, v) VALUES (0, [1, 2])"); // distance is 555661 m from [5,5]
        execute("INSERT INTO %s (pk, v) VALUES (1, [4, 4])"); // distance is 157010 m from [5,5]
        execute("INSERT INTO %s (pk, v) VALUES (2, [5, 5])"); // distance is 0 m from [5,5]
        execute("INSERT INTO %s (pk, v) VALUES (3, [6, 6])"); // distance is 156891 m from [5,5]
        execute("INSERT INTO %s (pk, v) VALUES (4, [8, 9])"); // distance is 553647 m from [5,5]
        execute("INSERT INTO %s (pk, v) VALUES (5, [10, 10])"); // distance is 782780 m from [5,5]

        beforeAndAfterFlush(() -> {
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE GEO_DISTANCE(v, [5,5]) < 157000"),
                                    row(2), row(3));
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE GEO_DISTANCE(v, [5,5]) < 157011"),
                                    row(1), row(2), row(3));
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE GEO_DISTANCE(v, [5,5]) <= 600000"),
                                    row(0), row(1), row(2), row(3), row(4));
        });
    }

    @Test
    public void testPointCloseToBondaryAt1DegreeLatitude() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, v vector<float, 2>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function' : 'euclidean'}");

        // Points chosen to be close to the boundary of the search radius. The assertion failed based on earlier
        // versions of the math used to determine whether the square distance was sufficient to short circuit
        // the logic and skip performing the haversine distance calculation.
        execute("INSERT INTO %s (pk, v) VALUES (0, [0.99, 0])"); // distance is 110.1 km from [0,0]
        execute("INSERT INTO %s (pk, v) VALUES (1, [0.998, 0])"); // distance is 110.9 km from [0,0]
        execute("INSERT INTO %s (pk, v) VALUES (2, [0.9982, 0])"); // distance is 110995 m from [0,0]
        execute("INSERT INTO %s (pk, v) VALUES (3, [0.9983, 0])"); // distance is 111006.05 m from [0,0]
        execute("INSERT INTO %s (pk, v) VALUES (4, [0.999, 0])"); // distance is 111.1 km from [0,0]

        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT pk FROM %s WHERE GEO_DISTANCE(v, [0,0]) < 111000"),
                       row(1), row(0), row(2));
            assertRows(execute("SELECT pk FROM %s WHERE GEO_DISTANCE(v, [0,0]) <= 111006"),
                       row(1), row(0), row(2));
            assertRows(execute("SELECT pk FROM %s WHERE GEO_DISTANCE(v, [0,0]) < 111007"),
                       row(1), row(0), row(2), row(3));
        });
    }


    @Test
    public void testPointCloseToBondaryAtOneTenthDegreeLatitude() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, v vector<float, 2>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function' : 'euclidean'}");

        execute("INSERT INTO %s (pk, v) VALUES (0, [0.10999, 0])"); // distance is 12230.3 m from [0,0]
        execute("INSERT INTO %s (pk, v) VALUES (1, [0.11000, 0])"); // distance is 12231.4 m from [0,0]

        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT pk FROM %s WHERE GEO_DISTANCE(v, [0,0]) < 12231"), row(0));
            assertRows(execute("SELECT pk FROM %s WHERE GEO_DISTANCE(v, [0,0]) <= 12231"), row(0));
        });
    }

    @Test
    public void testPointCloseToBondaryAtOneTenThousandthsDegreeLatitude() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, v vector<float, 2>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function' : 'euclidean'}");

        execute("INSERT INTO %s (pk, v) VALUES (0, [0.00009, 0])"); // distance is 10.007 m from [0,0]
        execute("INSERT INTO %s (pk, v) VALUES (1, [0.00010, 0])"); // distance is 11.120 m from [0,0]

        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT pk FROM %s WHERE GEO_DISTANCE(v, [0,0]) < 11"), row(0));
            assertRows(execute("SELECT pk FROM %s WHERE GEO_DISTANCE(v, [0,0]) <= 11"), row(0));
        });
    }

    @Test
    public void testIntersectedPredicateWithGeoDistanceQuery() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, num int, v vector<float, 2>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function' : 'euclidean'}");
        createIndex("CREATE CUSTOM INDEX ON %s(num) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (pk, num, v) VALUES (0, 0, [1, 2])"); // distance is 555661 m from [5,5]
        execute("INSERT INTO %s (pk, num, v) VALUES (1, 1, [4, 4])"); // distance is 157010 m from [5,5]
        execute("INSERT INTO %s (pk, num, v) VALUES (2, 2, [5, 5])"); // distance is 0 m from [5,5]
        execute("INSERT INTO %s (pk, num, v) VALUES (3, 3, [6, 6])"); // distance is 156891 m from [5,5]
        execute("INSERT INTO %s (pk, num, v) VALUES (4, 4, [8, 9])"); // distance is 553647 m from [5,5]
        execute("INSERT INTO %s (pk, num, v) VALUES (5, 5, [10, 10])"); // distance is 782780 m from [5,5]

        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT pk FROM %s WHERE GEO_DISTANCE(v, [5,5]) < 200000 AND num < 2"), row(1));
            assertRows(execute("SELECT pk FROM %s WHERE GEO_DISTANCE(v, [5,5]) <= 600000 AND num > 3"), row(4));
            assertRows(execute("SELECT pk FROM %s WHERE GEO_DISTANCE(v, [5,5]) <= 200000 AND num = 3"), row(3));
        });
    }

    @Test
    public void testGeoDistanceTopKQuery() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, point vector<float, 2>, v vector<float, 3>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(point) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function' : 'euclidean'}");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (pk, point, v) VALUES (0, [1, 2], [1, 2, 1])"); // distance is 555661 m from [5,5]
        execute("INSERT INTO %s (pk, point, v) VALUES (1, [4, 4], [4, 4, 1])"); // distance is 157010 m from [5,5]
        execute("INSERT INTO %s (pk, point, v) VALUES (2, [5, 5], [5, 5, 1])"); // distance is 0 m from [5,5]
        execute("INSERT INTO %s (pk, point, v) VALUES (3, [6, 6], [6, 6, 1])"); // distance is 156891 m from [5,5]
        execute("INSERT INTO %s (pk, point, v) VALUES (4, [8, 9], [8, 9, 1])"); // distance is 553647 m from [5,5]
        execute("INSERT INTO %s (pk, point, v) VALUES (5, [10, 10], [10, 10, 1])"); // distance is 782780 m from [5,5]

        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT pk FROM %s WHERE GEO_DISTANCE(point, [0,0]) < 400000 ORDER BY v ANN OF [0, 1, 2] LIMIT 1"), row(0));
        });
    }

    @Test
    public void testPreparedIntersectedPredicateWithGeoDistanceQuery() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, num int, v vector<float, 2>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function' : 'euclidean'}");
        createIndex("CREATE CUSTOM INDEX ON %s(num) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (pk, num, v) VALUES (0, 0, [1, 2])"); // distance is 555661 m from [5,5]
        execute("INSERT INTO %s (pk, num, v) VALUES (1, 1, [4, 4])"); // distance is 157010 m from [5,5]
        execute("INSERT INTO %s (pk, num, v) VALUES (2, 2, [5, 5])"); // distance is 0 m from [5,5]
        execute("INSERT INTO %s (pk, num, v) VALUES (3, 3, [6, 6])"); // distance is 156891 m from [5,5]
        execute("INSERT INTO %s (pk, num, v) VALUES (4, 4, [8, 9])"); // distance is 553647 m from [5,5]
        execute("INSERT INTO %s (pk, num, v) VALUES (5, 5, [10, 10])"); // distance is 782780 m from [5,5]

        var query = "SELECT pk FROM %s WHERE GEO_DISTANCE(v, ?) < ? AND num < ?";
        prepare(query);

        beforeAndAfterFlush(() -> {
            assertRows(execute(query, vector(5,5), 200000f, 2), row(1));
            assertRows(execute(query, vector(5,5), 200000.0f, 2), row(1));
        });
    }

    @Test
    public void testNestedGeoDistanceQueries() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, num int, v vector<float, 2>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function' : 'euclidean'}");
        createIndex("CREATE CUSTOM INDEX ON %s(num) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (pk, v) VALUES (0, [1, 2])"); // distance is 555661 m from [5,5]
        execute("INSERT INTO %s (pk, v) VALUES (1, [4, 4])"); // distance is 157010 m from [5,5]
        execute("INSERT INTO %s (pk, v) VALUES (2, [5, 5])"); // distance is 0 m from [5,5]
        execute("INSERT INTO %s (pk, v) VALUES (3, [6, 6])"); // distance is 156891 m from [5,5]
        execute("INSERT INTO %s (pk, v) VALUES (4, [8, 9])"); // distance is 553647 m from [5,5]
        execute("INSERT INTO %s (pk, v) VALUES (5, [10, 10])"); // distance is 782780 m from [5,5]

        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT pk FROM %s WHERE GEO_DISTANCE(v, [10,10]) < 1 OR GEO_DISTANCE(v, [1,2]) < 1"),
                       row(5), row(0));
        });
    }

    @Test
    public void testLongRangeGeoDistanceWithRealLocationsQuery() throws Throwable
    {
        createTable("CREATE TABLE %s (city text primary key, coordinates vector<float, 2>)");
        createIndex("CREATE CUSTOM INDEX ON %s(coordinates) USING 'StorageAttachedIndex' WITH OPTIONS = { 'similarity_function' : 'euclidean' }");

        // coordinates are [latitude, longitude]
        execute("INSERT INTO %s (city, coordinates) VALUES ('Washington DC', [38.8951, -77.0364])");
        execute("INSERT INTO %s (city, coordinates) VALUES ('New York City', [40.7128, -74.0060])");
        execute("INSERT INTO %s (city, coordinates) VALUES ('San Francisco', [37.7749, -122.4194])");
        execute("INSERT INTO %s (city, coordinates) VALUES ('Los Angeles', [34.0522, -118.2437])");
        execute("INSERT INTO %s (city, coordinates) VALUES ('Chicago', [41.8781, -87.6298])");
        execute("INSERT INTO %s (city, coordinates) VALUES ('Boston', [42.3601, -71.0589])");

        beforeAndAfterFlush(() -> {
            // Cities within 5 meters of Boston
            assertRowsIgnoringOrder(execute("SELECT city FROM %s WHERE GEO_DISTANCE(coordinates, [42.3601, -71.0589]) < 5"),
                                    row("Boston"));


            assertRowsIgnoringOrder(execute("SELECT city FROM %s WHERE GEO_DISTANCE(coordinates, [42.3601, -71.0589]) < 5 LIMIT 1"),
                                    row("Boston"));

            // Cities within 328.4 km of Washington DC
            assertRowsIgnoringOrder(execute("SELECT city FROM %s WHERE GEO_DISTANCE(coordinates, [38.8951, -77.0364]) < 328400"),
                                    row("New York City"), row("Washington DC"));

            // Cities within 500 km of New York City
            assertRowsIgnoringOrder(execute("SELECT city FROM %s WHERE GEO_DISTANCE(coordinates, [40.7128, -74.0060]) < 500000"),
                                    row("Boston"), row("New York City"), row("Washington DC"));

            // Cities within 1000 km of New York City
            assertRowsIgnoringOrder(execute("SELECT city FROM %s WHERE GEO_DISTANCE(coordinates, [40.7128, -74.0060]) < 500000"),
                                    row("Boston"), row("New York City"), row("Washington DC"));
        });
    }

    @Test
    public void testCloseRangeGeoDistanceWithRealLocationsQuery() throws Throwable
    {
        createTable("CREATE TABLE %s (city text primary key, coordinates vector<float, 2>)");
        createIndex("CREATE CUSTOM INDEX ON %s(coordinates) USING 'StorageAttachedIndex' WITH OPTIONS = { 'similarity_function' : 'euclidean' }");

        // coordinates are [latitude, longitude]
        // These are from NYC's Central Park
        execute("INSERT INTO %s (city, coordinates) VALUES ('Rec Center', [40.791186,-73.959591])");
        execute("INSERT INTO %s (city, coordinates) VALUES ('Baseball Field 11', [40.791597,-73.958059])");
        execute("INSERT INTO %s (city, coordinates) VALUES ('Baseball Field 7', [40.792847,-73.957105])");
        execute("INSERT INTO %s (city, coordinates) VALUES ('Baseball Field 6', [40.793018,-73.957565])");
        execute("INSERT INTO %s (city, coordinates) VALUES ('Baseball Field 5', [40.793193,-73.958644])");

        beforeAndAfterFlush(() -> {
            // Point within 40 meters of field 6
            assertRowsIgnoringOrder(execute("SELECT city FROM %s WHERE GEO_DISTANCE(coordinates, [40.793018,-73.957565]) < 40"),
                                    row("Baseball Field 6"));

            // Point within 43 meters of field 6 (field 7 is 43.14 meters away)
            assertRowsIgnoringOrder(execute("SELECT city FROM %s WHERE GEO_DISTANCE(coordinates, [40.793018,-73.957565]) < 43.5"),
                                    row("Baseball Field 6"), row("Baseball Field 7"));

            // Point within 95 meters of field 6 (field 5 is 93 meters away)
            assertRowsIgnoringOrder(execute("SELECT city FROM %s WHERE GEO_DISTANCE(coordinates, [40.793018,-73.957565]) < 95"),
                                    row("Baseball Field 6"), row("Baseball Field 7"), row("Baseball Field 5"));
        });
    }

    @Test
    public void testGeoAndANNOnSameColumn() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, v vector<float, 2>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function' : 'euclidean'}");

        // Distances computed using https://www.nhc.noaa.gov/gccalc.shtml
        execute("INSERT INTO %s (pk, v) VALUES (0, [1, 2])"); // distance is 555 km from [5,5]
        execute("INSERT INTO %s (pk, v) VALUES (1, [4, 4])"); // distance is 157 km from [5,5]
        execute("INSERT INTO %s (pk, v) VALUES (2, [5, 5])"); // distance is 0 km from [5,5]
        execute("INSERT INTO %s (pk, v) VALUES (3, [6, 6])"); // distance is 157 km from [5,5]
        execute("INSERT INTO %s (pk, v) VALUES (4, [8, 9])"); // distance is 553 from [5,5]
        execute("INSERT INTO %s (pk, v) VALUES (5, [10, 10])"); // distance is 782 km from [5,5]

        beforeAndAfterFlush(() -> {
            // GEO_DISTANCE gets all rows and then the limit gets the top 3
            assertRows(execute("select pk from %s WHERE geo_distance(v,[5,5]) <= 1000000 ORDER BY v ANN of [5,5] limit 3"),
                                    row(2), row(1), row(3));
        });

        // Delete a row
        execute("DELETE FROM %s WHERE pk = 2");

        // VSTODO this test asserts a slightly surprising result. PK 0 is further from the search point than PK 4, but by
        // euclidean distance it is closer, so 0 is the third result. We still run the test because it
        // validates that geo_distance and ANN OF two can be used at once. We have a ticket open to consider adding a
        // HAVERSINE similarity function, which would give us correct results.
        beforeAndAfterFlush(() -> {
            // GEO_DISTANCE gets all rows and then the limit gets the top 3
            assertRows(execute("select pk from %s WHERE geo_distance(v,[5,5]) <= 1000000 ORDER BY v ANN of [5,5] limit 3"),
                       row(1), row(3), row(0));
        });
    }

    @Test
    public void testGeoDistanceNearAntiMerridianQueriesForLargeDistances() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, num int, v vector<float, 2>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function' : 'euclidean'}");
        createIndex("CREATE CUSTOM INDEX ON %s(num) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (pk, num, v) VALUES (0, 1, [0, -179])");
        execute("INSERT INTO %s (pk, num, v) VALUES (1, 1, [0, 179])");
        execute("INSERT INTO %s (pk, num, v) VALUES (2, 1, [45, -179])");
        execute("INSERT INTO %s (pk, num, v) VALUES (3, 1, [45, 179])");
        execute("INSERT INTO %s (pk, num, v) VALUES (4, 1, [90, -179])");
        execute("INSERT INTO %s (pk, num, v) VALUES (5, 1, [90, 179])");
        execute("INSERT INTO %s (pk, num, v) VALUES (6, 1, [0, 0])");

        beforeAndAfterFlush(() -> {
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE GEO_DISTANCE(v, [0, -179]) < 6000000"),
                       row(0), row(1), row(2), row(3));

            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE GEO_DISTANCE(v, [45, 179]) < 6000000"),
                                    row(0), row(1), row(2), row(3), row(4), row(5));

            // Search using AND and OR to cover different code paths
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE GEO_DISTANCE(v, [0, -179]) < 6000000 AND num = 1"),
                                    row(0), row(1), row(2), row(3));

            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE GEO_DISTANCE(v, [45, 179]) < 6000000 OR num = 0"),
                                    row(0), row(1), row(2), row(3), row(4), row(5));
        });
    }

    @Test
    public void testGeoDistanceNearAntiMerridianQueriesForCloseDistances() throws Throwable
    {
        createTable("CREATE TABLE %s (location text PRIMARY KEY, coords vector<float, 2>)");
        createIndex("CREATE CUSTOM INDEX ON %s(coords) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function' : 'euclidean'}");

        // Here are the distances (these distances a not transitive, but do provide general motivation for the
        // results observed in the test)
        // Suva to Tubou is 292 km
        // Suva to Dakuiloa is 328 km
        // Tubou to Dakuiloa is 41.3 km
        execute("INSERT INTO %s (location, coords) VALUES ('suva', [-18.1236146,178.4217888])"); // Suva, Fiji
        execute("INSERT INTO %s (location, coords) VALUES ('tubou', [-18.2357357,-178.8109825])"); // Tubou, Fiji
        execute("INSERT INTO %s (location, coords) VALUES ('dakuiloa', [-18.4452221,-178.4884278])"); // Dakuiloa, Fiji

        beforeAndAfterFlush(() -> {
            // Search from Suva
            assertRowsIgnoringOrder(execute("SELECT location FROM %s WHERE GEO_DISTANCE(coords, [-18.1236146,178.4217888]) < 293000"),
                                    row("suva"), row("tubou"));
            // Search from Tubou
            assertRowsIgnoringOrder(execute("SELECT location FROM %s WHERE GEO_DISTANCE(coords, [-18.2357357,-178.8109825]) < 293000"),
                                    row("suva"), row("tubou"), row("dakuiloa"));
            // Search from Dakuiloa
            assertRowsIgnoringOrder(execute("SELECT location FROM %s WHERE GEO_DISTANCE(coords, [-18.4452221,-178.4884278]) < 293000"),
                                    row("tubou"), row("dakuiloa"));
            // Search from Dakuiloa with a smaller radius
            assertRowsIgnoringOrder(execute("SELECT location FROM %s WHERE GEO_DISTANCE(coords, [-18.4452221,-178.4884278]) < 40000"),
                                    row("dakuiloa"));
            // Search from a point in between all three on the antimeridian
            assertRowsIgnoringOrder(execute("SELECT location FROM %s WHERE GEO_DISTANCE(coords, [-18.3,-180]) < 170000"),
                                    row("suva"), row("tubou"), row("dakuiloa"));
            // Search from a point in between all three on the antimeridian
            assertRowsIgnoringOrder(execute("SELECT location FROM %s WHERE GEO_DISTANCE(coords, [-18.3,180]) < 170000"),
                                    row("suva"), row("tubou"), row("dakuiloa"));
        });
    }

    @Test
    public void testSearchesNearPoles() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, coords vector<float, 2>)");
        createIndex("CREATE CUSTOM INDEX ON %s(coords) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function' : 'euclidean'}");

        execute("INSERT INTO %s (pk, coords) VALUES (0, [90,0])");
        execute("INSERT INTO %s (pk, coords) VALUES (1, [89.99999, 0])");
        execute("INSERT INTO %s (pk, coords) VALUES (2, [89.99999, 179])");
        execute("INSERT INTO %s (pk, coords) VALUES (3, [89.99999, -179])");
        execute("INSERT INTO %s (pk, coords) VALUES (4, [89.999, 0])");
        execute("INSERT INTO %s (pk, coords) VALUES (5, [89.999, 179])");
        execute("INSERT INTO %s (pk, coords) VALUES (5, [89.999, -179])");

        beforeAndAfterFlush(() -> {
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE GEO_DISTANCE(coords, [90, 0]) < 0.01"),
                                    row(0));
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE GEO_DISTANCE(coords, [90, 0]) < 1"),
                                    row(0), row(1), row(2), row(3));
        });

    }
}
