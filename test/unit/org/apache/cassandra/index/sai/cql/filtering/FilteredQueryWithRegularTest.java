/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.index.sai.cql.filtering;

import java.util.Collection;

import org.junit.Test;
import org.junit.runners.Parameterized;

/**
 * {@link FilteredQueryTester} for indexes on regular, non-collection columns.
 */
public class FilteredQueryWithRegularTest extends FilteredQueryTester
{
    @Parameterized.Parameters(name = "indexes={0}")
    public static Collection<Object[]> parameters()
    {
        return parameters(new Index("x"), new Index("y"), new Index("z"));
    }

    @Override
    public void createTable()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, x int, y int, z int)");
    }

    @Override
    public void populateTable()
    {
        execute("INSERT INTO %s(k, x, y, z) values (0, 0, 0, 0)");
        execute("INSERT INTO %s(k, x, y, z) values (1, 0, 1, 0)");
        execute("INSERT INTO %s(k, x, y, z) values (2, 0, 2, 1)");
        execute("INSERT INTO %s(k, x, y, z) values (3, 0, 3, 1)");
    }

    @Test
    public void testQueries()
    {
        test("SELECT k FROM %s WHERE x=0",
             !hasIndex("x"),
             hasIndex("x"),
             row(0), row(1), row(2), row(3));
        test("SELECT k FROM %s WHERE x=1",
             !hasIndex("x"),
             hasIndex("x"));

        test("SELECT k FROM %s WHERE y IN (1, 2)",
             !hasIndex("y"),
             hasIndex("y"),
             row(1), row(2));
        test("SELECT k FROM %s WHERE y IN (4, 6)",
             !hasIndex("y"),
             hasIndex("y"));

        test("SELECT k FROM %s WHERE x=0 AND y IN (1, 2)",
             !hasAllIndexes("x", "y"),
             hasAnyIndexes("x", "y"),
             row(1), row(2));
        test("SELECT k FROM %s WHERE x=1 AND y IN (1, 2)",
             !hasAllIndexes("x", "y"),
             hasAnyIndexes("x", "y"));

        test("SELECT k FROM %s WHERE x=0 AND (y=1 OR y=2)",
             !hasAllIndexes("x", "y"),
             hasAnyIndexes("x", "y"),
             row(1), row(2));
        test("SELECT k FROM %s WHERE x=1 AND (y=1 OR y=2)",
             !hasAllIndexes("x", "y"),
             hasAnyIndexes("x", "y"));

        test("SELECT k FROM %s WHERE x=0 OR y=0",
             !hasAllIndexes("x", "y"),
             hasAllIndexes("x", "y"),
             row(0), row(1), row(2), row(3));
        test("SELECT k FROM %s WHERE x=1 OR y=0",
             !hasAllIndexes("x", "y"),
             hasAllIndexes("x", "y"),
             row(0));

        test("SELECT k FROM %s WHERE x=0 OR (y=0 AND z=0)",
             !hasAllIndexes("x", "y", "z"),
             hasAllIndexes("x", "y") || hasAllIndexes("x", "z"),
             row(0), row(1), row(2), row(3));
        test("SELECT k FROM %s WHERE x=1 OR (y=0 AND z=0)",
             !hasAllIndexes("x", "y", "z"),
             hasAllIndexes("x", "y") || hasAllIndexes("x", "z"),
             row(0));

        test("SELECT k FROM %s WHERE x=0 OR y=0 OR z=0",
             !hasAllIndexes("x", "y", "z"),
             hasAllIndexes("x", "y", "z"),
             row(0), row(1), row(2), row(3));
        test("SELECT k FROM %s WHERE x=1 OR y=0 OR z=0",
             !hasAllIndexes("x", "y", "z"),
             hasAllIndexes("x", "y", "z"),
             row(0), row(1));
    }
}
