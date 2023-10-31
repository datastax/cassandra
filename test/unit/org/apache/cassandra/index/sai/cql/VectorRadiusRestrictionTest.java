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
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();

        execute("INSERT INTO %s (pk, v) VALUES (0, [1, 2])"); // distance is 5 from [5,5]
        execute("INSERT INTO %s (pk, v) VALUES (1, [4, 4])"); // distance is root 2 from [5,5]
        execute("INSERT INTO %s (pk, v) VALUES (2, [5, 5])"); // distance is 0 from [5,5]
        execute("INSERT INTO %s (pk, v) VALUES (3, [6, 6])"); // distance is root 2 from [5,5]
        execute("INSERT INTO %s (pk, v) VALUES (4, [8, 9])"); // distance is 5 from [5,5]
        execute("INSERT INTO %s (pk, v) VALUES (5, [10, 10])"); // distance is greater than 5 from [5,5]

        beforeAndAfterFlush(() -> {
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE GEO_DISTANCE(v, [5,5]) < 5"),
                                    row(1), row(2), row(3));
            assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE GEO_DISTANCE(v, [5,5]) <= 5"),
                                    row(0), row(1), row(2), row(3), row(4));
        });
    }

    @Test
    public void testIntersectedPredicateWithGeoDistanceQuery() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, num int, v vector<float, 2>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(num) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();

        execute("INSERT INTO %s (pk, num, v) VALUES (0, 0, [1, 2])"); // distance is 5 from [5,5]
        execute("INSERT INTO %s (pk, num, v) VALUES (1, 1, [4, 4])"); // distance is root 2 from [5,5]
        execute("INSERT INTO %s (pk, num, v) VALUES (2, 2, [5, 5])"); // distance is 0 from [5,5]
        execute("INSERT INTO %s (pk, num, v) VALUES (3, 3, [6, 6])"); // distance is root 2 from [5,5]
        execute("INSERT INTO %s (pk, num, v) VALUES (4, 4, [8, 9])"); // distance is 5 from [5,5]
        execute("INSERT INTO %s (pk, num, v) VALUES (5, 5, [10, 10])"); // distance is greater than 5 from [5,5]

        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT pk FROM %s WHERE GEO_DISTANCE(v, [5,5]) < 5 AND num < 2"), row(1));
            assertRows(execute("SELECT pk FROM %s WHERE GEO_DISTANCE(v, [5,5]) <= 5 AND num > 3"), row(4));
            assertRows(execute("SELECT pk FROM %s WHERE GEO_DISTANCE(v, [5,5]) <= 5 AND num = 3"), row(3));
        });
    }

    @Test
    public void testPreparedIntersectedPredicateWithGeoDistanceQuery() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, num int, v vector<float, 2>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(num) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();

        execute("INSERT INTO %s (pk, num, v) VALUES (0, 0, [1, 2])"); // distance is 5 from [5,5]
        execute("INSERT INTO %s (pk, num, v) VALUES (1, 1, [4, 4])"); // distance is root 2 from [5,5]
        execute("INSERT INTO %s (pk, num, v) VALUES (2, 2, [5, 5])"); // distance is 0 from [5,5]
        execute("INSERT INTO %s (pk, num, v) VALUES (3, 3, [6, 6])"); // distance is root 2 from [5,5]
        execute("INSERT INTO %s (pk, num, v) VALUES (4, 4, [8, 9])"); // distance is 5 from [5,5]
        execute("INSERT INTO %s (pk, num, v) VALUES (5, 5, [10, 10])"); // distance is greater than 5 from [5,5]

        var query = "SELECT pk FROM %s WHERE GEO_DISTANCE(v, ?) < ? AND num < ?";
        prepare(query);

        beforeAndAfterFlush(() -> {
            // TODO this is failing, and also fails with a different error when the distance is 5.0.
            assertRows(execute(query, vector(5,5), 5, 2), row(1));
        });
    }

    @Test
    public void testNestedGeoDistanceQueries() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, num int, v vector<float, 2>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(num) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();

        execute("INSERT INTO %s (pk, num, v) VALUES (0, 0, [1, 2])"); // distance is 5 from [5,5]
        execute("INSERT INTO %s (pk, num, v) VALUES (1, 1, [4, 4])"); // distance is root 2 from [5,5]
        execute("INSERT INTO %s (pk, num, v) VALUES (2, 2, [5, 5])"); // distance is 0 from [5,5]
        execute("INSERT INTO %s (pk, num, v) VALUES (3, 3, [6, 6])"); // distance is root 2 from [5,5]
        execute("INSERT INTO %s (pk, num, v) VALUES (4, 4, [8, 9])"); // distance is 5 from [5,5]
        execute("INSERT INTO %s (pk, num, v) VALUES (5, 5, [10, 10])"); // distance is greater than 5 from [5,5]

        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT pk FROM %s WHERE GEO_DISTANCE(v, [10,10]) < 1 OR GEO_DISTANCE(v, [1,2]) < 1"),
                       row(5), row(0));
        });
    }
}
