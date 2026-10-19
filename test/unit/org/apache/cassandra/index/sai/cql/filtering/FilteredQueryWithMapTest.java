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
 * {@link FilteredQueryTester} for indexes on map columns.
 */
public class FilteredQueryWithMapTest extends FilteredQueryTester
{
    @Parameterized.Parameters(name = "indexes={0}")
    public static Collection<Object[]> parameters()
    {
        return parameters(
                new Index("v"),
                new Index("mk", "CREATE CUSTOM INDEX mk ON %s(keys(m)) USING 'StorageAttachedIndex'"),
                new Index("mv", "CREATE CUSTOM INDEX mv ON %s(values(m)) USING 'StorageAttachedIndex'"),
                new Index("me", "CREATE CUSTOM INDEX me ON %s(entries(m)) USING 'StorageAttachedIndex'"));
    }

    @Override
    public void createTable()
    {
        createTable("CREATE TABLE %s(k int PRIMARY KEY, v int, m map<int, int>)");
    }

    @Override
    public void populateTable()
    {
        execute("INSERT INTO %s(k, v, m) values (1, 0, {1:1, 2:2})");
        execute("INSERT INTO %s(k, v, m) values (2, 0, {1:1, 3:3})");
        execute("INSERT INTO %s(k, v, m) values (3, 0, {4:4, 5:5})");
        execute("INSERT INTO %s(k, v, m) values (4, 0, {1:10, 2:20})");
        execute("INSERT INTO %s(k, v, m) values (5, 0, {1:10, 3:30})");
        execute("INSERT INTO %s(k, v, m) values (6, 0, {4:40, 5:50})");
    }

    @Test
    public void testQueries()
    {
        // equals entries
        test("SELECT k FROM %s WHERE m[1] = 1",
             !hasIndex("me"),
             hasIndex("me"),
             row(1), row(2));
        test("SELECT k FROM %s WHERE m[1] = 1 AND v = 0",
             !hasAllIndexes("me", "v"),
             hasAnyIndexes("me", "v"),
             row(1), row(2));
        test("SELECT k FROM %s WHERE m[1] = 1 AND v = 1",
             !hasAllIndexes("me", "v"),
             hasAnyIndexes("me", "v"));
        test("SELECT k FROM %s WHERE m[1] = 1 OR v = 0",
             !hasAllIndexes("me", "v"),
             hasAllIndexes("me", "v"),
             row(1), row(2), row(3), row(4), row(5), row(6));
        test("SELECT k FROM %s WHERE m[1] = 1 OR v = 1",
             !hasAllIndexes("me", "v"),
             hasAllIndexes("me", "v"),
             row(1), row(2));

        // not equals entries
        test("SELECT k FROM %s WHERE m[1] != 1",
             !hasIndex("me"),
             hasIndex("me"),
             row(3), row(4), row(5), row(6));
        test("SELECT k FROM %s WHERE m[1] != 1 AND v = 0",
             !hasAllIndexes("me", "v"),
             hasAnyIndexes("me", "v"),
             row(3), row(4), row(5), row(6));
        test("SELECT k FROM %s WHERE m[1] != 1 AND v = 1",
             !hasAllIndexes("me", "v"),
             hasAnyIndexes("me", "v"));
        test("SELECT k FROM %s WHERE m[1] != 1 OR v = 0",
             !hasAllIndexes("me", "v"),
             hasAllIndexes("me", "v"),
             row(1), row(2), row(3), row(4), row(5), row(6));
        test("SELECT k FROM %s WHERE m[1] != 1 OR v = 1",
             !hasAllIndexes("me", "v"),
             hasAllIndexes("me", "v"),
             row(3), row(4), row(5), row(6));

        // range entries (>)
        test("SELECT k FROM %s WHERE m[3] > 3",
             !hasIndex("me"),
             hasIndex("me"),
             row(5));
        test("SELECT k FROM %s WHERE m[3] > 3 AND v = 0",
             !hasAllIndexes("me", "v"),
             hasAnyIndexes("me", "v"),
             row(5));
        test("SELECT k FROM %s WHERE m[3] > 3 AND v = 1",
             !hasAllIndexes("me", "v"),
             hasAnyIndexes("me", "v"));
        test("SELECT k FROM %s WHERE m[3] > 3 OR v = 0",
             !hasAllIndexes("me", "v"),
             hasAllIndexes("me", "v"),
             row(1), row(2), row(3), row(4), row(5), row(6));
        test("SELECT k FROM %s WHERE m[3] > 3 OR v = 1",
             !hasAllIndexes("me", "v"),
             hasAllIndexes("me", "v"),
             row(5));

        // range entries (<)
        test("SELECT k FROM %s WHERE m[3] < 30",
             !hasIndex("me"),
             hasIndex("me"),
             row(2));
        test("SELECT k FROM %s WHERE m[3] < 30 AND v = 0",
             !hasAllIndexes("me", "v"),
             hasAnyIndexes("me", "v"),
             row(2));
        test("SELECT k FROM %s WHERE m[3] < 30 AND v = 1",
             !hasAllIndexes("me", "v"),
             hasAnyIndexes("me", "v"));
        test("SELECT k FROM %s WHERE m[3] < 30 OR v = 0",
             !hasAllIndexes("me", "v"),
             hasAllIndexes("me", "v"),
             row(1), row(2), row(3), row(4), row(5), row(6));
        test("SELECT k FROM %s WHERE m[3] < 30 OR v = 1",
             !hasAllIndexes("me", "v"),
             hasAllIndexes("me", "v"),
             row(2));

        // range entries (>=)
        test("SELECT k FROM %s WHERE m[3] >= 3",
             !hasIndex("me"),
             hasIndex("me"),
             row(2), row(5));
        test("SELECT k FROM %s WHERE m[3] >= 3 AND v = 0",
             !hasAllIndexes("me", "v"),
             hasAnyIndexes("me", "v"),
             row(2), row(5));
        test("SELECT k FROM %s WHERE m[3] >= 3 AND v = 1",
             !hasAllIndexes("me", "v"),
             hasAnyIndexes("me", "v"));
        test("SELECT k FROM %s WHERE m[3] >= 3 OR v = 0",
             !hasAllIndexes("me", "v"),
             hasAllIndexes("me", "v"),
             row(1), row(2), row(3), row(4), row(5), row(6));
        test("SELECT k FROM %s WHERE m[3] >= 3 OR v = 1",
             !hasAllIndexes("me", "v"),
             hasAllIndexes("me", "v"),
             row(2), row(5));

        // range entries (<=)
        test("SELECT k FROM %s WHERE m[3] <= 30",
             !hasIndex("me"),
             hasIndex("me"),
             row(2), row(5));
        test("SELECT k FROM %s WHERE m[3] <= 30 AND v = 0",
             !hasAllIndexes("me", "v"),
             hasAnyIndexes("me", "v"),
             row(2), row(5));
        test("SELECT k FROM %s WHERE m[3] <= 30 AND v = 1",
             !hasAllIndexes("me", "v"),
             hasAnyIndexes("me", "v"));
        test("SELECT k FROM %s WHERE m[3] <= 30 OR v = 0",
             !hasAllIndexes("me", "v"),
             hasAllIndexes("me", "v"),
             row(1), row(2), row(3), row(4), row(5), row(6));
        test("SELECT k FROM %s WHERE m[3] <= 30 OR v = 1",
             !hasAllIndexes("me", "v"),
             hasAllIndexes("me", "v"),
             row(2), row(5));

        // contains keys
        test("SELECT k FROM %s WHERE m CONTAINS KEY 1",
             !hasAllIndexes("mk"),
             hasAllIndexes("mk"),
             row(1), row(2), row(4), row(5));
        test("SELECT k FROM %s WHERE m CONTAINS KEY 1 AND v = 0",
             !hasAllIndexes("mk", "v"),
             hasAnyIndexes("mk", "v"),
             row(1), row(2), row(4), row(5));
        test("SELECT k FROM %s WHERE m CONTAINS KEY 1 AND v = 1",
             !hasAllIndexes("mk", "v"),
             hasAnyIndexes("mk", "v"));
        test("SELECT k FROM %s WHERE m CONTAINS KEY 1 OR v = 0",
             !hasAllIndexes("mk", "v"),
             hasAllIndexes("mk", "v"),
             row(1), row(2), row(3), row(4), row(5), row(6));
        test("SELECT k FROM %s WHERE m CONTAINS KEY 1 OR v = 1",
             !hasAllIndexes("mk", "v"),
             hasAllIndexes("mk", "v"),
             row(1), row(2), row(4), row(5));

        // not contains keys
        test("SELECT k FROM %s WHERE m NOT CONTAINS KEY 4",
             !hasAllIndexes("mk"),
             hasAllIndexes("mk"),
             row(1), row(2), row(4), row(5));
        test("SELECT k FROM %s WHERE m NOT CONTAINS KEY 4 AND v = 0",
             !hasAllIndexes("mk", "v"),
             hasAnyIndexes("mk", "v"),
             row(1), row(2), row(4), row(5));
        test("SELECT k FROM %s WHERE m NOT CONTAINS KEY 4 AND v = 1",
             !hasAllIndexes("mk", "v"),
             hasAnyIndexes("mk", "v"));
        test("SELECT k FROM %s WHERE m NOT CONTAINS KEY 4 OR v = 0",
             !hasAllIndexes("mk", "v"),
             hasAllIndexes("mk", "v"),
             row(1), row(2), row(3), row(4), row(5), row(6));
        test("SELECT k FROM %s WHERE m NOT CONTAINS KEY 4 OR v = 1",
             !hasAllIndexes("mk", "v"),
             hasAllIndexes("mk", "v"),
             row(1), row(2), row(4), row(5));

        // contains values
        test("SELECT k FROM %s WHERE m CONTAINS 1",
             !hasAllIndexes("mv"),
             hasAllIndexes("mv"),
             row(1), row(2));
        test("SELECT k FROM %s WHERE m CONTAINS 1 AND v = 0",
             !hasAllIndexes("mv", "v"),
             hasAnyIndexes("mv", "v"),
             row(1), row(2));
        test("SELECT k FROM %s WHERE m CONTAINS 1 AND v = 1",
             !hasAllIndexes("mv", "v"),
             hasAnyIndexes("mv", "v"));
        test("SELECT k FROM %s WHERE m CONTAINS 1 OR v = 0",
             !hasAllIndexes("mv", "v"),
             hasAllIndexes("mv", "v"),
             row(1), row(2), row(3), row(4), row(5), row(6));
        test("SELECT k FROM %s WHERE m CONTAINS 1 OR v = 1",
             !hasAllIndexes("mv", "v"),
             hasAllIndexes("mv", "v"),
             row(1), row(2));

        // not contains values
        test("SELECT k FROM %s WHERE m NOT CONTAINS 5",
             !hasAllIndexes("mv"),
             hasAllIndexes("mv"),
             row(1), row(2), row(4), row(5), row(6));
        test("SELECT k FROM %s WHERE m NOT CONTAINS 5 AND v = 0",
             !hasAllIndexes("mv", "v"),
             hasAnyIndexes("mv", "v"),
             row(1), row(2), row(4), row(5), row(6));
        test("SELECT k FROM %s WHERE m NOT CONTAINS 5 AND v = 1",
             !hasAllIndexes("mv", "v"),
             hasAnyIndexes("mv", "v"));
        test("SELECT k FROM %s WHERE m NOT CONTAINS 5 OR v = 0",
             !hasAllIndexes("mv", "v"),
             hasAllIndexes("mv", "v"),
             row(1), row(2), row(3), row(4), row(5), row(6));
        test("SELECT k FROM %s WHERE m NOT CONTAINS 5 OR v = 1",
             !hasAllIndexes("mv", "v"),
             hasAllIndexes("mv", "v"),
             row(1), row(2), row(4), row(5), row(6));
    }
}
