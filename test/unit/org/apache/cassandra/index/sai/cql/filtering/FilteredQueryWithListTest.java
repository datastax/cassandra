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
 * {@link FilteredQueryTester} for indexes on list columns.
 */
public class FilteredQueryWithListTest extends FilteredQueryTester
{
    @Parameterized.Parameters(name = "indexes={0}")
    public static Collection<Object[]> parameters()
    {
        return parameters(new Index("v"), new Index("l"));
    }

    @Override
    public void createTable()
    {
        createTable("CREATE TABLE %s(k int PRIMARY KEY, v int, l list<int>)");
    }

    @Override
    public void populateTable()
    {
        execute("INSERT INTO %s(k, v, l) values (1, 0, [1])");
        execute("INSERT INTO %s(k, v, l) values (2, 0, [1, 2])");
        execute("INSERT INTO %s(k, v, l) values (3, 0, [1, 2, 3])");
        execute("INSERT INTO %s(k, v, l) values (4, 0, [2, 3])");
        execute("INSERT INTO %s(k, v, l) values (5, 0, [3])");
        execute("INSERT INTO %s(k, v, l) values (6, 0, [])");
    }

    @Test
    public void testQueries()
    {
        // contains
        test("SELECT k FROM %s WHERE l CONTAINS 1",
             !hasAllIndexes("l"),
             hasAllIndexes("l"),
             row(1), row(2), row(3));
        test("SELECT k FROM %s WHERE l CONTAINS 1 AND v = 0",
             !hasAllIndexes("l", "v"),
             hasAnyIndexes("l", "v"),
             row(1), row(2), row(3));
        test("SELECT k FROM %s WHERE l CONTAINS 1 AND v = 1",
             !hasAllIndexes("l", "v"),
             hasAnyIndexes("l", "v"));
        test("SELECT k FROM %s WHERE l CONTAINS 1 OR v = 0",
             !hasAllIndexes("l", "v"),
             hasAllIndexes("l", "v"),
             row(1), row(2), row(3), row(4), row(5), row(6));
        test("SELECT k FROM %s WHERE l CONTAINS 1 OR v = 1",
             !hasAllIndexes("l", "v"),
             hasAllIndexes("l", "v"),
             row(1), row(2), row(3));

        // not contains
        test("SELECT k FROM %s WHERE l NOT CONTAINS 1",
             !hasAllIndexes("l"),
             hasAllIndexes("l"),
             row(4), row(5), row(6));
        test("SELECT k FROM %s WHERE l NOT CONTAINS 1 AND v = 0",
             !hasAllIndexes("l", "v"),
             hasAnyIndexes("l", "v"),
             row(4), row(5), row(6));
        test("SELECT k FROM %s WHERE l NOT CONTAINS 1 AND v = 1",
             !hasAllIndexes("l", "v"),
             hasAnyIndexes("l", "v"));
        test("SELECT k FROM %s WHERE l NOT CONTAINS 1 OR v = 0",
             !hasAllIndexes("l", "v"),
             hasAllIndexes("l", "v"),
             row(1), row(2), row(3), row(4), row(5), row(6));
        test("SELECT k FROM %s WHERE l NOT CONTAINS 1 OR v = 1",
             !hasAllIndexes("l", "v"),
             hasAllIndexes("l", "v"),
             row(4), row(5), row(6));
    }
}
