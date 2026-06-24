/*
 * Copyright IBM Corp.
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

package org.apache.cassandra.index.internal;

import org.junit.Test;

import org.apache.cassandra.index.SkipIndexOnFullPrimaryKeysTester;

/**
 * {@link SkipIndexOnFullPrimaryKeysTester} for legacy table-based indexes.
 */
public class SkipIndexOnFullPrimaryKeysTest extends SkipIndexOnFullPrimaryKeysTester
{
    @Test
    public void testSkipsIndexSkinny()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, n int)");
        createIndex("CREATE INDEX n_idx ON %s(n)");
        execute("INSERT INTO %s(k, n) VALUES (0, 0)");
        execute("INSERT INTO %s(k, n) VALUES (1, 1)");
        execute("INSERT INTO %s(k, n) VALUES (2, 0)");
        execute("INSERT INTO %s(k, n) VALUES (3, 1)");

        // range queries, unbounded
        assertHasQueryPlan("SELECT k FROM %s WHERE n = 0", row(0), row(2));
        assertHasQueryPlan("SELECT k FROM %s WHERE n = 1", row(1), row(3));
        assertHasQueryPlan("SELECT k FROM %s WHERE n = 2");

        // range queries, with token range
        assertHasQueryPlan("SELECT k FROM %s WHERE token(k) >= token(1) AND n = 0", row(0), row(2));
        assertHasQueryPlan("SELECT k FROM %s WHERE token(k) >= token(1) AND n = 1", row(1), row(3));
        assertHasQueryPlan("SELECT k FROM %s WHERE token(k) >= token(1) AND n = 2");

        // single-partition queries
        assertHasNoQueryPlan("SELECT k FROM %s WHERE k = 0 AND n = 0", row(0));
        assertHasNoQueryPlan("SELECT k FROM %s WHERE k = 0 AND n = 1");
        assertHasNoQueryPlan("SELECT k FROM %s WHERE k = 0 AND n = 2");

        // single-partition queries with hints
        assertHasQueryPlan("SELECT k FROM %s WHERE k = 0 AND n = 0 WITH included_indexes = {n_idx}", row(0));
        assertHasQueryPlan("SELECT k FROM %s WHERE k = 0 AND n = 1 WITH included_indexes = {n_idx}");
        assertHasQueryPlan("SELECT k FROM %s WHERE k = 0 AND n = 2 WITH included_indexes = {n_idx}");

        // multi-partition queries, which are not supported, but leaving it here in case that changes (CNDB-14044)
        assertUnsupportedINOnPK("SELECT k FROM %s WHERE k IN (0, 1) AND n = 0");
        assertUnsupportedINOnPK("SELECT k FROM %s WHERE k IN (0, 1) AND n = 1");
        assertUnsupportedINOnPK("SELECT k FROM %s WHERE k IN (0, 1) AND n = 2");
    }

    @Test
    public void testSkipsIndexWide()
    {
        createTable("CREATE TABLE %s (k int, c int, n int, PRIMARY KEY(k, c))");
        createIndex("CREATE INDEX n_idx ON %s(n)");
        execute("INSERT INTO %s(k, c, n) VALUES (0, 0, 0)");
        execute("INSERT INTO %s(k, c, n) VALUES (0, 1, 1)");
        execute("INSERT INTO %s(k, c, n) VALUES (0, 2, 0)");
        execute("INSERT INTO %s(k, c, n) VALUES (0, 3, 1)");
        execute("INSERT INTO %s(k, c, n) VALUES (1, 0, 0)");
        execute("INSERT INTO %s(k, c, n) VALUES (1, 1, 1)");
        execute("INSERT INTO %s(k, c, n) VALUES (1, 2, 0)");
        execute("INSERT INTO %s(k, c, n) VALUES (1, 3, 1)");

        // range queries, unbounded
        assertHasQueryPlan("SELECT k, c FROM %s WHERE n = 0", row(1, 0), row(1, 2), row(0, 0), row(0, 2));
        assertHasQueryPlan("SELECT k, c FROM %s WHERE n = 1", row(1, 1), row(1, 3), row(0, 1), row(0, 3));
        assertHasQueryPlan("SELECT k, c FROM %s WHERE n = 2");

        // range queries, with token range
        assertHasQueryPlan("SELECT k, c FROM %s WHERE token(k) >= token(1) AND n = 0", row(1, 0), row(1, 2), row(0, 0), row(0, 2));
        assertHasQueryPlan("SELECT k, c FROM %s WHERE token(k) >= token(1) AND n = 1", row(1, 1), row(1, 3), row(0, 1), row(0, 3));
        assertHasQueryPlan("SELECT k, c FROM %s WHERE token(k) >= token(1) AND n = 2");

        // single-partition queries
        assertHasQueryPlan("SELECT k, c FROM %s WHERE k = 0 AND n = 0", row(0, 0), row(0, 2));
        assertHasQueryPlan("SELECT k, c FROM %s WHERE k = 0 AND n = 1", row(0, 1), row(0, 3));
        assertHasQueryPlan("SELECT k, c FROM %s WHERE k = 0 AND n = 2");

        // multi-partition queries, which are not supported, but leaving it here in case that changes (CNDB-14044)
        assertUnsupportedINOnPK("SELECT k, c FROM %s WHERE k IN (0, 1) AND n = 0");
        assertUnsupportedINOnPK("SELECT k, c FROM %s WHERE k IN (0, 1) AND n = 1");
        assertUnsupportedINOnPK("SELECT k, c FROM %s WHERE k IN (0, 1) AND n = 2");

        // single-row single-partition queries
        assertHasNoQueryPlan("SELECT k, c FROM %s WHERE k = 0 AND c = 0 AND n = 0", row(0, 0));
        assertHasNoQueryPlan("SELECT k, c FROM %s WHERE k = 0 AND c = 0 AND n = 1");

        // single-row single-partition queries with hints
        assertHasQueryPlan("SELECT k, c FROM %s WHERE k = 0 AND c = 0 AND n = 0 WITH included_indexes = {n_idx}", row(0, 0));
        assertHasQueryPlan("SELECT k, c FROM %s WHERE k = 0 AND c = 0 AND n = 1 WITH included_indexes = {n_idx}");
        assertHasQueryPlan("SELECT k, c FROM %s WHERE k = 0 AND c = 0 AND n = 2 WITH included_indexes = {n_idx}");

        // names single-row queries
        assertHasNoQueryPlan("SELECT k, c FROM %s WHERE k = 0 AND c IN (0, 1) AND n = 0", row(0, 0));
        assertHasNoQueryPlan("SELECT k, c FROM %s WHERE k = 0 AND c IN (0, 1) AND n = 1", row(0, 1));
        assertHasNoQueryPlan("SELECT k, c FROM %s WHERE k = 0 AND c IN (0, 1) AND n = 2");

        // names single-row queries with hints
        assertHasQueryPlan("SELECT k, c FROM %s WHERE k = 0 AND c IN (0, 1) AND n = 0 WITH included_indexes = {n_idx}", row(0, 0));
        assertHasQueryPlan("SELECT k, c FROM %s WHERE k = 0 AND c IN (0, 1) AND n = 1 WITH included_indexes = {n_idx}", row(0, 1));
        assertHasQueryPlan("SELECT k, c FROM %s WHERE k = 0 AND c IN (0, 1) AND n = 3 WITH included_indexes = {n_idx}");

        // slices single-partition queries
        assertHasQueryPlan("SELECT k, c FROM %s WHERE k = 0 AND c >= 0 AND c <= 1 AND n = 0", row(0, 0));
        assertHasQueryPlan("SELECT k, c FROM %s WHERE k = 0 AND c >= 0 AND c <= 1 AND n = 1", row(0, 1));
        assertHasQueryPlan("SELECT k, c FROM %s WHERE k = 0 AND c >= 0 AND c <= 1 AND n = 3");

        // slices single-partition queries with hints
        assertHasQueryPlan("SELECT k, c FROM %s WHERE k = 0 AND c >= 0 AND c <= 1 AND n = 0 WITH included_indexes = {n_idx}", row(0, 0));
        assertHasQueryPlan("SELECT k, c FROM %s WHERE k = 0 AND c >= 0 AND c <= 1 AND n = 1 WITH included_indexes = {n_idx}", row(0, 1));
        assertHasQueryPlan("SELECT k, c FROM %s WHERE k = 0 AND c >= 0 AND c <= 1 AND n = 3 WITH included_indexes = {n_idx}");
    }
}
