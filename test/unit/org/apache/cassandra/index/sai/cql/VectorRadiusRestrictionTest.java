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

public class VectorRadiusRestrictionTest extends VectorTester
{
    @Test
    public void testBasicGeoDistanceQuery() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, v vector<float, 2>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function' : 'euclidean'}");
        waitForIndexQueryable();

        // Distances computed using https://www.nhc.noaa.gov/gccalc.shtml
        execute("INSERT INTO %s (pk, v) VALUES (0, [1, 2])"); // distance is 555 km from [5,5]
        execute("INSERT INTO %s (pk, v) VALUES (1, [4, 4])"); // distance is 157 km from [5,5]
        execute("INSERT INTO %s (pk, v) VALUES (2, [5, 5])"); // distance is 0 km from [5,5]
        execute("INSERT INTO %s (pk, v) VALUES (3, [6, 6])"); // distance is 157 km from [5,5]
        execute("INSERT INTO %s (pk, v) VALUES (4, [8, 9])"); // distance is 553 from [5,5]
        execute("INSERT INTO %s (pk, v) VALUES (5, [10, 10])"); // distance is 782 km from [5,5]

        beforeAndAfterFlush(() -> {
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE GEO_DISTANCE(v, [5,5]) < 157000"),
                                    row(1), row(2), row(3));
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE GEO_DISTANCE(v, [5,5]) <= 600000"),
                                    row(0), row(1), row(2), row(3), row(4));
        });
    }

    @Test
    public void testIntersectedPredicateWithGeoDistanceQuery() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, num int, v vector<float, 2>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function' : 'euclidean'}");
        createIndex("CREATE CUSTOM INDEX ON %s(num) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();

        // Distances computed using https://www.nhc.noaa.gov/gccalc.shtml
        execute("INSERT INTO %s (pk, v) VALUES (0, [1, 2])"); // distance is 555 km from [5,5]
        execute("INSERT INTO %s (pk, v) VALUES (1, [4, 4])"); // distance is 157 km from [5,5]
        execute("INSERT INTO %s (pk, v) VALUES (2, [5, 5])"); // distance is 0 km from [5,5]
        execute("INSERT INTO %s (pk, v) VALUES (3, [6, 6])"); // distance is 157 km from [5,5]
        execute("INSERT INTO %s (pk, v) VALUES (4, [8, 9])"); // distance is 553 from [5,5]
        execute("INSERT INTO %s (pk, v) VALUES (5, [10, 10])"); // distance is 782 km from [5,5]

        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT pk FROM %s WHERE GEO_DISTANCE(v, [5,5]) < 200000 AND num < 2"), row(1));
            assertRows(execute("SELECT pk FROM %s WHERE GEO_DISTANCE(v, [5,5]) <= 200000 AND num > 3"), row(4));
            assertRows(execute("SELECT pk FROM %s WHERE GEO_DISTANCE(v, [5,5]) <= 200000 AND num = 3"), row(3));
        });
    }

    @Test
    public void testPreparedIntersectedPredicateWithGeoDistanceQuery() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, num int, v vector<float, 2>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function' : 'euclidean'}");
        createIndex("CREATE CUSTOM INDEX ON %s(num) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();

        // Distances computed using https://www.nhc.noaa.gov/gccalc.shtml
        execute("INSERT INTO %s (pk, v) VALUES (0, [1, 2])"); // distance is 555 km from [5,5]
        execute("INSERT INTO %s (pk, v) VALUES (1, [4, 4])"); // distance is 157 km from [5,5]
        execute("INSERT INTO %s (pk, v) VALUES (2, [5, 5])"); // distance is 0 km from [5,5]
        execute("INSERT INTO %s (pk, v) VALUES (3, [6, 6])"); // distance is 157 km from [5,5]
        execute("INSERT INTO %s (pk, v) VALUES (4, [8, 9])"); // distance is 553 from [5,5]
        execute("INSERT INTO %s (pk, v) VALUES (5, [10, 10])"); // distance is 782 km from [5,5]

        var query = "SELECT pk FROM %s WHERE GEO_DISTANCE(v, ?) < ? AND num < ?";
        prepare(query);

        beforeAndAfterFlush(() -> {
            // TODO this is failing, and also fails with a different error when the distance is 5.0.
            assertRows(execute(query, vector(5,5), 200000, 2), row(1));
        });
    }

    @Test
    public void testNestedGeoDistanceQueries() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, num int, v vector<float, 2>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function' : 'euclidean'}");
        createIndex("CREATE CUSTOM INDEX ON %s(num) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();

        // Distances computed using https://www.nhc.noaa.gov/gccalc.shtml
        execute("INSERT INTO %s (pk, v) VALUES (0, [1, 2])"); // distance is 555 km from [5,5]
        execute("INSERT INTO %s (pk, v) VALUES (1, [4, 4])"); // distance is 157 km from [5,5]
        execute("INSERT INTO %s (pk, v) VALUES (2, [5, 5])"); // distance is 0 km from [5,5]
        execute("INSERT INTO %s (pk, v) VALUES (3, [6, 6])"); // distance is 157 km from [5,5]
        execute("INSERT INTO %s (pk, v) VALUES (4, [8, 9])"); // distance is 553 from [5,5]
        execute("INSERT INTO %s (pk, v) VALUES (5, [10, 10])"); // distance is 782 km from [5,5]

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
        waitForIndexQueryable();

        // coordinates are [latitude, longitude]
        execute("INSERT INTO %s (city, coordinates) VALUES ('Washington DC', [38.8951, -77.0364])");
        execute("INSERT INTO %s (city, coordinates) VALUES ('New York City', [40.7128, -74.0060])");
        execute("INSERT INTO %s (city, coordinates) VALUES ('San Francisco', [37.7749, -122.4194])");
        execute("INSERT INTO %s (city, coordinates) VALUES ('Los Angeles', [34.0522, -118.2437])");
        execute("INSERT INTO %s (city, coordinates) VALUES ('Chicago', [41.8781, -87.6298])");
        execute("INSERT INTO %s (city, coordinates) VALUES ('Boston', [42.3601, -71.0589])");

        // Cities within 5 meters of Boston
        assertRowsIgnoringOrder(execute("SELECT city FROM %s WHERE GEO_DISTANCE(coordinates, [42.3601, -71.0589]) < 5"),
                   row("Boston"));

        // Cities within 328 km of Washington DC
        assertRowsIgnoringOrder(execute("SELECT city FROM %s WHERE GEO_DISTANCE(coordinates, [38.8951, -77.0364]) < 328000"),
                                row("New York City"), row("Washington DC"));

        // Cities within 500 km of New York City
        assertRowsIgnoringOrder(execute("SELECT city FROM %s WHERE GEO_DISTANCE(coordinates, [40.7128, -74.0060]) < 500000"),
                   row("Boston"), row("New York City"), row("Washington DC"));

        // Cities within 1000 km of New York City
        assertRowsIgnoringOrder(execute("SELECT city FROM %s WHERE GEO_DISTANCE(coordinates, [40.7128, -74.0060]) < 500000"),
                                row("Boston"), row("New York City"), row("Washington DC"));
    }
}
