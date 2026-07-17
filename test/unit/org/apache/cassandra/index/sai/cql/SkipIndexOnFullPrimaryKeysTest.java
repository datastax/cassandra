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

package org.apache.cassandra.index.sai.cql;

import org.junit.Test;

import org.apache.cassandra.index.SkipIndexOnFullPrimaryKeysTester;

/**
 * {@link SkipIndexOnFullPrimaryKeysTester} for SAI indexes.
 */
public class SkipIndexOnFullPrimaryKeysTest extends SkipIndexOnFullPrimaryKeysTester
{
    @Test
    public void testSkipsIndexSkinny()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, n int, t text, v vector<float, 2>)");
        createIndex("CREATE CUSTOM INDEX n_idx ON %s(n) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX t_idx ON %s(t) USING 'StorageAttachedIndex' WITH OPTIONS = { 'index_analyzer': 'standard' }");
        createIndex("CREATE CUSTOM INDEX v_idx ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function' : 'euclidean'}");
        execute("INSERT INTO %s(k, n, v, t) VALUES (0, 0, [0, 1], 'foo')");
        execute("INSERT INTO %s(k, n, v, t) VALUES (1, 1, [0, 2], 'foo bar')");
        execute("INSERT INTO %s(k, n, v, t) VALUES (2, 0, [0, 3], 'bar')");
        execute("INSERT INTO %s(k, n, v, t) VALUES (3, 1, [0, 4], 'foo bar')");

        // range queries, unbounded
        assertHasQueryPlan("SELECT k FROM %s WHERE n = 0", row(0), row(2));
        assertHasQueryPlan("SELECT k FROM %s WHERE n = 1", row(1), row(3));
        assertHasQueryPlan("SELECT k FROM %s WHERE n = 2");
        assertHasQueryPlan("SELECT k FROM %s WHERE t : 'foo'", row(1), row(0), row(3));
        assertHasQueryPlan("SELECT k FROM %s WHERE t : 'bar'", row(1), row(2), row(3));
        assertHasQueryPlan("SELECT k FROM %s WHERE t : 'bob'");
        assertHasQueryPlan("SELECT k FROM %s WHERE t = 'foo'", row(1), row(0), row(3));
        assertHasQueryPlan("SELECT k FROM %s WHERE t = 'bar'", row(1), row(2), row(3));
        assertHasQueryPlan("SELECT k FROM %s WHERE t = 'bob'");
        assertHasQueryPlan("SELECT k FROM %s ORDER BY t BM25 OF 'foo' LIMIT 2", row(0), row(1));
        assertHasQueryPlan("SELECT k FROM %s ORDER BY t BM25 OF 'bar' LIMIT 2", row(2), row(1));
        assertHasQueryPlan("SELECT k FROM %s ORDER BY t BM25 OF 'bob' LIMIT 2");
        assertHasQueryPlan("SELECT k FROM %s ORDER BY v ANN OF [0, 1] LIMIT 2", row(0), row(1));
        assertHasQueryPlan("SELECT k FROM %s ORDER BY v ANN OF [0, 9] LIMIT 2", row(3), row(2));

        // range queries, with token range
        assertHasQueryPlan("SELECT k FROM %s WHERE token(k) >= token(1) AND n = 0", row(0), row(2));
        assertHasQueryPlan("SELECT k FROM %s WHERE token(k) >= token(1) AND n = 1", row(1), row(3));
        assertHasQueryPlan("SELECT k FROM %s WHERE token(k) >= token(1) AND n = 2");
        assertHasQueryPlan("SELECT k FROM %s WHERE token(k) >= token(1) AND t : 'foo'", row(1), row(0), row(3));
        assertHasQueryPlan("SELECT k FROM %s WHERE token(k) >= token(1) AND t : 'bar'", row(1), row(2), row(3));
        assertHasQueryPlan("SELECT k FROM %s WHERE token(k) >= token(1) AND t : 'bob'");
        assertHasQueryPlan("SELECT k FROM %s WHERE token(k) >= token(1) ORDER BY t BM25 OF 'foo' LIMIT 2", row(0), row(1));
        assertHasQueryPlan("SELECT k FROM %s WHERE token(k) >= token(1) ORDER BY t BM25 OF 'bar' LIMIT 2", row(2), row(1));
        assertHasQueryPlan("SELECT k FROM %s WHERE token(k) >= token(1) ORDER BY t BM25 OF 'bob' LIMIT 2");
        assertHasQueryPlan("SELECT k FROM %s WHERE token(k) >= token(1) ORDER BY v ANN OF [0, 1] LIMIT 2", row(0), row(1));
        assertHasQueryPlan("SELECT k FROM %s WHERE token(k) >= token(1) ORDER BY v ANN OF [0, 9] LIMIT 2", row(3), row(2));

        // single-partition queries
        assertHasNoQueryPlan("SELECT k FROM %s WHERE k = 0 AND n = 0", row(0));
        assertHasNoQueryPlan("SELECT k FROM %s WHERE k = 0 AND n = 1");
        assertHasNoQueryPlan("SELECT k FROM %s WHERE k = 0 AND n = 2");
        assertHasNoQueryPlan("SELECT k FROM %s WHERE k = 0 AND t : 'foo'", row(0));
        assertHasNoQueryPlan("SELECT k FROM %s WHERE k = 0 AND t : 'bar'");
        assertHasNoQueryPlan("SELECT k FROM %s WHERE k = 0 AND t : 'bob'");
        assertHasNoQueryPlan("SELECT k FROM %s WHERE k = 0 AND t = 'foo'", row(0));
        assertHasNoQueryPlan("SELECT k FROM %s WHERE k = 0 AND t = 'bar'");
        assertHasNoQueryPlan("SELECT k FROM %s WHERE k = 0 AND t = 'bob'");
        assertHasQueryPlan("SELECT k FROM %s WHERE k = 0 ORDER BY t BM25 OF 'foo' LIMIT 2", row(0));
        assertHasQueryPlan("SELECT k FROM %s WHERE k = 0 ORDER BY t BM25 OF 'bar' LIMIT 2");
        assertHasQueryPlan("SELECT k FROM %s WHERE k = 0 ORDER BY t BM25 OF 'bob' LIMIT 2");
        assertHasQueryPlan("SELECT k FROM %s WHERE k = 0 ORDER BY v ANN OF [0, 1] LIMIT 2", row(0));
        assertHasQueryPlan("SELECT k FROM %s WHERE k = 0 ORDER BY v ANN OF [0, 9] LIMIT 2", row(0));

        // single-partition queries with hints
        assertHasQueryPlan("SELECT k FROM %s WHERE k = 0 AND n = 0 WITH included_indexes = {n_idx}", row(0));
        assertHasQueryPlan("SELECT k FROM %s WHERE k = 0 AND n = 1 WITH included_indexes = {n_idx}");
        assertHasQueryPlan("SELECT k FROM %s WHERE k = 0 AND n = 2 WITH included_indexes = {n_idx}");
        assertHasQueryPlan("SELECT k FROM %s WHERE k = 0 AND t : 'foo' WITH included_indexes = {t_idx}", row(0));
        assertHasQueryPlan("SELECT k FROM %s WHERE k = 0 AND t : 'bar' WITH included_indexes = {t_idx}");
        assertHasQueryPlan("SELECT k FROM %s WHERE k = 0 AND t : 'bob' WITH included_indexes = {t_idx}");
        assertHasQueryPlan("SELECT k FROM %s WHERE k = 0 AND t = 'foo' WITH included_indexes = {t_idx}", row(0));
        assertHasQueryPlan("SELECT k FROM %s WHERE k = 0 AND t = 'bar' WITH included_indexes = {t_idx}");
        assertHasQueryPlan("SELECT k FROM %s WHERE k = 0 AND t = 'bob' WITH included_indexes = {t_idx}");
        assertHasQueryPlan("SELECT k FROM %s WHERE k = 0 ORDER BY t BM25 OF 'foo' LIMIT 2 WITH included_indexes = {t_idx}", row(0));
        assertHasQueryPlan("SELECT k FROM %s WHERE k = 0 ORDER BY t BM25 OF 'bar' LIMIT 2 WITH included_indexes = {t_idx}");
        assertHasQueryPlan("SELECT k FROM %s WHERE k = 0 ORDER BY t BM25 OF 'bob' LIMIT 2 WITH included_indexes = {t_idx}");
        assertHasQueryPlan("SELECT k FROM %s WHERE k = 0 ORDER BY v ANN OF [0, 1] LIMIT 2 WITH included_indexes = {v_idx}", row(0));
        assertHasQueryPlan("SELECT k FROM %s WHERE k = 0 ORDER BY v ANN OF [0, 9] LIMIT 2 WITH included_indexes = {v_idx}", row(0));

        // multi-partition queries, which are not supported, but leaving it here in case that changes (CNDB-14044)
        assertUnsupportedINOnPK("SELECT k FROM %s WHERE k IN (0, 1) AND n = 0");
        assertUnsupportedINOnPK("SELECT k FROM %s WHERE k IN (0, 1) AND n = 1");
        assertUnsupportedINOnPK("SELECT k FROM %s WHERE k IN (0, 1) AND n = 2");
        assertUnsupportedINOnPK("SELECT k FROM %s WHERE k IN (0, 1) AND t : 'foo'");
        assertUnsupportedINOnPK("SELECT k FROM %s WHERE k IN (0, 1) AND t : 'bar'");
        assertUnsupportedINOnPK("SELECT k FROM %s WHERE k IN (0, 1) AND t : 'bob'");
        assertUnsupportedINOnPK("SELECT k FROM %s WHERE k IN (0, 1) AND t = 'foo'");
        assertUnsupportedINOnPK("SELECT k FROM %s WHERE k IN (0, 1) AND t = 'bar'");
        assertUnsupportedINOnPK("SELECT k FROM %s WHERE k IN (0, 1) AND t = 'bob'");
        assertUnsupportedINOnPK("SELECT k FROM %s WHERE k IN (0, 1) ORDER BY t BM25 OF 'foo' LIMIT 2");
        assertUnsupportedINOnPK("SELECT k FROM %s WHERE k IN (0, 1) ORDER BY t BM25 OF 'bar' LIMIT 2");
        assertUnsupportedINOnPK("SELECT k FROM %s WHERE k IN (0, 1) ORDER BY t BM25 OF 'bob' LIMIT 2");
        assertUnsupportedINOnPK("SELECT k FROM %s WHERE k IN (0, 1) ORDER BY v ANN OF [0, 1] LIMIT 2");
        assertUnsupportedINOnPK("SELECT k FROM %s WHERE k IN (0, 1) ORDER BY v ANN OF [0, 9] LIMIT 2");
    }

    @Test
    public void testSkipsIndexWide()
    {
        createTable("CREATE TABLE %s (k int, c int, n int, t text, v vector<float, 2>, PRIMARY KEY(k, c))");
        createIndex("CREATE CUSTOM INDEX n_idx ON %s(n) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX t_idx ON %s(t) USING 'StorageAttachedIndex' WITH OPTIONS = { 'index_analyzer': 'standard' }");
        createIndex("CREATE CUSTOM INDEX v_idx ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function' : 'euclidean'}");
        execute("INSERT INTO %s(k, c, n, v, t) VALUES (0, 0, 0, [0, 1], 'foo')");
        execute("INSERT INTO %s(k, c, n, v, t) VALUES (0, 1, 1, [0, 2], 'foo bar')");
        execute("INSERT INTO %s(k, c, n, v, t) VALUES (0, 2, 0, [0, 3], 'bar')");
        execute("INSERT INTO %s(k, c, n, v, t) VALUES (0, 3, 1, [0, 4], 'foo bar')");
        execute("INSERT INTO %s(k, c, n, v, t) VALUES (1, 0, 0, [0, 5], 'foo')");
        execute("INSERT INTO %s(k, c, n, v, t) VALUES (1, 1, 1, [0, 6], 'foo bar')");
        execute("INSERT INTO %s(k, c, n, v, t) VALUES (1, 2, 0, [0, 7], 'bar')");
        execute("INSERT INTO %s(k, c, n, v, t) VALUES (1, 3, 1, [0, 8], 'foo bar')");

        // range queries, unbounded
        assertHasQueryPlan("SELECT k, c FROM %s WHERE n = 0", row(1, 0), row(1, 2), row(0, 0), row(0, 2));
        assertHasQueryPlan("SELECT k, c FROM %s WHERE n = 1", row(1, 1), row(1, 3), row(0, 1), row(0, 3));
        assertHasQueryPlan("SELECT k, c FROM %s WHERE n = 2");
        assertHasQueryPlan("SELECT k, c FROM %s WHERE t : 'foo'", row(1, 0), row(1, 1), row(1, 3), row(0, 0), row(0, 1), row(0, 3));
        assertHasQueryPlan("SELECT k, c FROM %s WHERE t : 'bar'", row(1, 1), row(1, 2), row(1, 3), row(0, 1), row(0, 2), row(0, 3));
        assertHasQueryPlan("SELECT k, c FROM %s WHERE t : 'bob'");
        assertHasQueryPlan("SELECT k, c FROM %s WHERE t = 'foo'", row(1, 0), row(1, 1), row(1, 3), row(0, 0), row(0, 1), row(0, 3));
        assertHasQueryPlan("SELECT k, c FROM %s WHERE t = 'bar'", row(1, 1), row(1, 2), row(1, 3), row(0, 1), row(0, 2), row(0, 3));
        assertHasQueryPlan("SELECT k, c FROM %s WHERE t = 'bob'");
        assertHasQueryPlan("SELECT k, c FROM %s ORDER BY t BM25 OF 'foo' LIMIT 2", row(1, 0), row(0, 0));
        assertHasQueryPlan("SELECT k, c FROM %s ORDER BY t BM25 OF 'bar' LIMIT 2", row(1, 2), row(0, 2));
        assertHasQueryPlan("SELECT k, c FROM %s ORDER BY t BM25 OF 'bob' LIMIT 2");
        assertHasQueryPlan("SELECT k, c FROM %s ORDER BY v ANN OF [0, 1] LIMIT 2", row(0, 0), row(0, 1));
        assertHasQueryPlan("SELECT k, c FROM %s ORDER BY v ANN OF [0, 9] LIMIT 2", row(1, 3), row(1, 2));

        // range queries, with token range
        assertHasQueryPlan("SELECT k, c FROM %s WHERE token(k) >= token(1) AND n = 0", row(1, 0), row(1, 2), row(0, 0), row(0, 2));
        assertHasQueryPlan("SELECT k, c FROM %s WHERE token(k) >= token(1) AND n = 1", row(1, 1), row(1, 3), row(0, 1), row(0, 3));
        assertHasQueryPlan("SELECT k, c FROM %s WHERE token(k) >= token(1) AND n = 2");
        assertHasQueryPlan("SELECT k, c FROM %s WHERE token(k) >= token(1) AND t : 'foo'", row(1, 0), row(1, 1), row(1, 3), row(0, 0), row(0, 1), row(0, 3));
        assertHasQueryPlan("SELECT k, c FROM %s WHERE token(k) >= token(1) AND t : 'bar'", row(1, 1), row(1, 2), row(1, 3), row(0, 1), row(0, 2), row(0, 3));
        assertHasQueryPlan("SELECT k, c FROM %s WHERE token(k) >= token(1) AND t : 'bob'");
        assertHasQueryPlan("SELECT k, c FROM %s WHERE token(k) >= token(1) AND t = 'foo'", row(1, 0), row(1, 1), row(1, 3), row(0, 0), row(0, 1), row(0, 3));
        assertHasQueryPlan("SELECT k, c FROM %s WHERE token(k) >= token(1) AND t = 'bar'", row(1, 1), row(1, 2), row(1, 3), row(0, 1), row(0, 2), row(0, 3));
        assertHasQueryPlan("SELECT k, c FROM %s WHERE token(k) >= token(1) AND t = 'bob'");
        assertHasQueryPlan("SELECT k, c FROM %s WHERE token(k) >= token(1) ORDER BY t BM25 OF 'foo' LIMIT 2", row(1, 0), row(0, 0));
        assertHasQueryPlan("SELECT k, c FROM %s WHERE token(k) >= token(1) ORDER BY t BM25 OF 'bar' LIMIT 2", row(1, 2), row(0, 2));
        assertHasQueryPlan("SELECT k, c FROM %s WHERE token(k) >= token(1) ORDER BY t BM25 OF 'bob' LIMIT 2");
        assertHasQueryPlan("SELECT k, c FROM %s WHERE token(k) >= token(1) ORDER BY v ANN OF [0, 1] LIMIT 2", row(0, 0), row(0, 1));
        assertHasQueryPlan("SELECT k, c FROM %s WHERE token(k) >= token(1) ORDER BY v ANN OF [0, 9] LIMIT 2", row(1, 3), row(1, 2));

        // single-partition queries
        assertHasQueryPlan("SELECT k, c FROM %s WHERE k = 0 AND n = 0", row(0, 0), row(0, 2));
        assertHasQueryPlan("SELECT k, c FROM %s WHERE k = 0 AND n = 1", row(0, 1), row(0, 3));
        assertHasQueryPlan("SELECT k, c FROM %s WHERE k = 0 AND n = 2");
        assertHasQueryPlan("SELECT k, c FROM %s WHERE k = 0 AND t : 'foo'", row(0, 0), row(0, 1), row(0, 3));
        assertHasQueryPlan("SELECT k, c FROM %s WHERE k = 0 AND t : 'bar'", row(0, 1), row(0, 2), row(0, 3));
        assertHasQueryPlan("SELECT k, c FROM %s WHERE k = 0 AND t : 'bob'");
        assertHasQueryPlan("SELECT k, c FROM %s WHERE k = 0 AND t = 'foo'", row(0, 0), row(0, 1), row(0, 3));
        assertHasQueryPlan("SELECT k, c FROM %s WHERE k = 0 AND t = 'bar'", row(0, 1), row(0, 2), row(0, 3));
        assertHasQueryPlan("SELECT k, c FROM %s WHERE k = 0 AND t = 'bob'");
        assertHasQueryPlan("SELECT k, c FROM %s WHERE k = 0 ORDER BY t BM25 OF 'foo' LIMIT 2", row(0, 0), row(0, 1));
        assertHasQueryPlan("SELECT k, c FROM %s WHERE k = 0 ORDER BY t BM25 OF 'bar' LIMIT 2", row(0, 2), row(0, 1));
        assertHasQueryPlan("SELECT k, c FROM %s WHERE k = 0 ORDER BY t BM25 OF 'bob' LIMIT 2");
        assertHasQueryPlan("SELECT k, c FROM %s WHERE k = 0 ORDER BY v ANN OF [0, 1] LIMIT 2", row(0, 0), row(0, 1));
        assertHasQueryPlan("SELECT k, c FROM %s WHERE k = 0 ORDER BY v ANN OF [0, 9] LIMIT 2", row(0, 3), row(0, 2));

        // multi-partition queries, which are not supported, but leaving it here in case that changes (CNDB-14044)
        assertUnsupportedINOnPK("SELECT k, c FROM %s WHERE k IN (0, 1) AND n = 0");
        assertUnsupportedINOnPK("SELECT k, c FROM %s WHERE k IN (0, 1) AND n = 1");
        assertUnsupportedINOnPK("SELECT k, c FROM %s WHERE k IN (0, 1) AND n = 2");
        assertUnsupportedINOnPK("SELECT k, c FROM %s WHERE k IN (0, 1) AND t : 'foo'");
        assertUnsupportedINOnPK("SELECT k, c FROM %s WHERE k IN (0, 1) AND t : 'bar'");
        assertUnsupportedINOnPK("SELECT k, c FROM %s WHERE k IN (0, 1) AND t : 'bob'");
        assertUnsupportedINOnPK("SELECT k, c FROM %s WHERE k IN (0, 1) AND t = 'foo'");
        assertUnsupportedINOnPK("SELECT k, c FROM %s WHERE k IN (0, 1) AND t = 'bar'");
        assertUnsupportedINOnPK("SELECT k, c FROM %s WHERE k IN (0, 1) AND t = 'bob'");
        assertUnsupportedINOnPK("SELECT k, c FROM %s WHERE k IN (0, 1) ORDER BY t BM25 OF 'foo' LIMIT 2");
        assertUnsupportedINOnPK("SELECT k, c FROM %s WHERE k IN (0, 1) ORDER BY t BM25 OF 'bar' LIMIT 2");
        assertUnsupportedINOnPK("SELECT k, c FROM %s WHERE k IN (0, 1) ORDER BY t BM25 OF 'bob' LIMIT 2");
        assertUnsupportedINOnPK("SELECT k, c FROM %s WHERE k IN (0, 1) ORDER BY v ANN OF [0, 1] LIMIT 2");
        assertUnsupportedINOnPK("SELECT k, c FROM %s WHERE k IN (0, 1) ORDER BY v ANN OF [0, 9] LIMIT 2");

        // single-row single-partition queries
        assertHasNoQueryPlan("SELECT k, c FROM %s WHERE k = 0 AND c = 0 AND n = 0", row(0, 0));
        assertHasNoQueryPlan("SELECT k, c FROM %s WHERE k = 0 AND c = 0 AND n = 1");
        assertHasNoQueryPlan("SELECT k, c FROM %s WHERE k = 0 AND c = 0 AND n = 2");
        assertHasNoQueryPlan("SELECT k, c FROM %s WHERE k = 0 AND c = 0 AND t : 'foo'", row(0, 0));
        assertHasNoQueryPlan("SELECT k, c FROM %s WHERE k = 0 AND c = 0 AND t : 'bar'");
        assertHasNoQueryPlan("SELECT k, c FROM %s WHERE k = 0 AND c = 0 AND t = 'foo'", row(0, 0));
        assertHasNoQueryPlan("SELECT k, c FROM %s WHERE k = 0 AND c = 0 AND t = 'bar'");

        // single-row single-partition queries with hints
        assertHasQueryPlan("SELECT k, c FROM %s WHERE k = 0 AND c = 0 AND n = 0 WITH included_indexes = {n_idx}", row(0, 0));
        assertHasQueryPlan("SELECT k, c FROM %s WHERE k = 0 AND c = 0 AND n = 1 WITH included_indexes = {n_idx}");
        assertHasQueryPlan("SELECT k, c FROM %s WHERE k = 0 AND c = 0 AND n = 2 WITH included_indexes = {n_idx}");
        assertHasQueryPlan("SELECT k, c FROM %s WHERE k = 0 AND c = 0 AND t : 'foo' WITH included_indexes = {t_idx}", row(0, 0));
        assertHasQueryPlan("SELECT k, c FROM %s WHERE k = 0 AND c = 0 AND t : 'bar' WITH included_indexes = {t_idx}");
        assertHasQueryPlan("SELECT k, c FROM %s WHERE k = 0 AND c = 0 AND t = 'foo' WITH included_indexes = {t_idx}", row(0, 0));
        assertHasQueryPlan("SELECT k, c FROM %s WHERE k = 0 AND c = 0 AND t = 'bar' WITH included_indexes = {t_idx}");

        // names single-row queries
        assertHasNoQueryPlan("SELECT k, c FROM %s WHERE k = 0 AND c IN (0, 1) AND n = 0", row(0, 0));
        assertHasNoQueryPlan("SELECT k, c FROM %s WHERE k = 0 AND c IN (0, 1) AND n = 1", row(0, 1));
        assertHasNoQueryPlan("SELECT k, c FROM %s WHERE k = 0 AND c IN (0, 1) AND n = 2");
        assertHasNoQueryPlan("SELECT k, c FROM %s WHERE k = 0 AND c IN (0, 1) AND t : 'foo'", row(0, 0), row(0, 1));
        assertHasNoQueryPlan("SELECT k, c FROM %s WHERE k = 0 AND c IN (0, 1) AND t : 'bar'", row(0, 1));
        assertHasNoQueryPlan("SELECT k, c FROM %s WHERE k = 0 AND c IN (0, 1) AND t = 'foo'", row(0, 0), row(0, 1));
        assertHasNoQueryPlan("SELECT k, c FROM %s WHERE k = 0 AND c IN (0, 1) AND t = 'bar'", row(0, 1));

        // names single-row queries with hints
        assertHasQueryPlan("SELECT k, c FROM %s WHERE k = 0 AND c IN (0, 1) AND n = 0 WITH included_indexes = {n_idx}", row(0, 0));
        assertHasQueryPlan("SELECT k, c FROM %s WHERE k = 0 AND c IN (0, 1) AND n = 1 WITH included_indexes = {n_idx}", row(0, 1));
        assertHasQueryPlan("SELECT k, c FROM %s WHERE k = 0 AND c IN (0, 1) AND n = 2 WITH included_indexes = {n_idx}");
        assertHasQueryPlan("SELECT k, c FROM %s WHERE k = 0 AND c IN (0, 1) AND t : 'foo' WITH included_indexes = {t_idx}", row(0, 0), row(0, 1));
        assertHasQueryPlan("SELECT k, c FROM %s WHERE k = 0 AND c IN (0, 1) AND t : 'bar' WITH included_indexes = {t_idx}", row(0, 1));
        assertHasQueryPlan("SELECT k, c FROM %s WHERE k = 0 AND c IN (0, 1) AND t = 'foo' WITH included_indexes = {t_idx}", row(0, 0), row(0, 1));
        assertHasQueryPlan("SELECT k, c FROM %s WHERE k = 0 AND c IN (0, 1) AND t = 'bar' WITH included_indexes = {t_idx}", row(0, 1));

        // slices single-partition queries
        assertHasQueryPlan("SELECT k, c FROM %s WHERE k = 0 AND c >= 0 AND c <= 1 AND n = 0", row(0, 0));
        assertHasQueryPlan("SELECT k, c FROM %s WHERE k = 0 AND c >= 0 AND c <= 1 AND n = 1", row(0, 1));
        assertHasQueryPlan("SELECT k, c FROM %s WHERE k = 0 AND c >= 0 AND c <= 1 AND n = 2");
        assertHasQueryPlan("SELECT k, c FROM %s WHERE k = 0 AND c >= 0 AND c <= 1 AND t : 'foo'", row(0, 0), row(0, 1));
        assertHasQueryPlan("SELECT k, c FROM %s WHERE k = 0 AND c >= 0 AND c <= 1 AND t : 'bar'", row(0, 1));
        assertHasQueryPlan("SELECT k, c FROM %s WHERE k = 0 AND c >= 0 AND c <= 1 AND t = 'foo'", row(0, 0), row(0, 1));
        assertHasQueryPlan("SELECT k, c FROM %s WHERE k = 0 AND c >= 0 AND c <= 1 AND t = 'bar'", row(0, 1));

        // slices single-partition queries with hints
        assertHasQueryPlan("SELECT k, c FROM %s WHERE k = 0 AND c >= 0 AND c <= 1 AND n = 0 WITH included_indexes = {n_idx}", row(0, 0));
        assertHasQueryPlan("SELECT k, c FROM %s WHERE k = 0 AND c >= 0 AND c <= 1 AND n = 1 WITH included_indexes = {n_idx}", row(0, 1));
        assertHasQueryPlan("SELECT k, c FROM %s WHERE k = 0 AND c >= 0 AND c <= 1 AND n = 2 WITH included_indexes = {n_idx}");
        assertHasQueryPlan("SELECT k, c FROM %s WHERE k = 0 AND c >= 0 AND c <= 1 AND t : 'foo' WITH included_indexes = {t_idx}", row(0, 0), row(0, 1));
        assertHasQueryPlan("SELECT k, c FROM %s WHERE k = 0 AND c >= 0 AND c <= 1 AND t : 'bar' WITH included_indexes = {t_idx}", row(0, 1));
        assertHasQueryPlan("SELECT k, c FROM %s WHERE k = 0 AND c >= 0 AND c <= 1 AND t = 'foo' WITH included_indexes = {t_idx}", row(0, 0), row(0, 1));
        assertHasQueryPlan("SELECT k, c FROM %s WHERE k = 0 AND c >= 0 AND c <= 1 AND t = 'bar' WITH included_indexes = {t_idx}", row(0, 1));
    }

    @Test
    public void testSkipsIndexWideMultiClustering()
    {
        createTable("CREATE TABLE %s (k int, c1 int, c2 int, n int, PRIMARY KEY(k, c1, c2))");
        createIndex("CREATE CUSTOM INDEX n_idx ON %s(n) USING 'StorageAttachedIndex'");
        execute("INSERT INTO %s(k, c1, c2, n) VALUES (0, 0, 0, 0)");
        execute("INSERT INTO %s(k, c1, c2, n) VALUES (0, 0, 1, 0)");
        execute("INSERT INTO %s(k, c1, c2, n) VALUES (0, 0, 2, 0)");
        execute("INSERT INTO %s(k, c1, c2, n) VALUES (0, 1, 0, 0)");
        execute("INSERT INTO %s(k, c1, c2, n) VALUES (0, 1, 1, 0)");
        execute("INSERT INTO %s(k, c1, c2, n) VALUES (0, 1, 2, 0)");
        execute("INSERT INTO %s(k, c1, c2, n) VALUES (1, 0, 0, 0)");
        execute("INSERT INTO %s(k, c1, c2, n) VALUES (1, 0, 1, 1)");
        execute("INSERT INTO %s(k, c1, c2, n) VALUES (1, 0, 2, 0)");
        execute("INSERT INTO %s(k, c1, c2, n) VALUES (1, 1, 0, 1)");
        execute("INSERT INTO %s(k, c1, c2, n) VALUES (1, 1, 1, 0)");
        execute("INSERT INTO %s(k, c1, c2, n) VALUES (1, 1, 2, 1)");

        // range queries, unbounded
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE n = 0",
                           row(1, 0, 0), row(1, 0, 2), row(1, 1, 1),
                           row(0, 0, 0), row(0, 0, 1), row(0, 0, 2), row(0, 1, 0), row(0, 1, 1), row(0, 1, 2));
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE n = 1", row(1, 0, 1), row(1, 1, 0), row(1, 1, 2));
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE n = 2");

        // single-partition queries
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 0 AND n = 0", row(0, 0, 0), row(0, 0, 1), row(0, 0, 2), row(0, 1, 0), row(0, 1, 1), row(0, 1, 2));
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 0 AND n = 1");
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 0 AND n = 2");
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 1 AND n = 0", row(1, 0, 0), row(1, 0, 2), row(1, 1, 1));
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 1 AND n = 1", row(1, 0, 1), row(1, 1, 0), row(1, 1, 2));
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 1 AND n = 2");
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 2 AND n = 0");
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 2 AND n = 1");
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 3 AND n = 2");

        // single-partition queries with eq restriction on first clustering column
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 0 AND c1 = 0 AND n = 0", row(0, 0, 0), row(0, 0, 1), row(0, 0, 2));
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 0 AND c1 = 0 AND n = 1");
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 0 AND c1 = 0 AND n = 2");
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 1 AND c1 = 0 AND n = 0", row(1, 0, 0), row(1, 0, 2));
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 1 AND c1 = 0 AND n = 1", row(1, 0, 1));
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 1 AND c1 = 0 AND n = 2");
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 2 AND c1 = 0 AND n = 0");
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 2 AND c1 = 0 AND n = 1");
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 3 AND c1 = 0 AND n = 2");
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 0 AND c1 = 1 AND n = 0", row(0, 1, 0), row(0, 1, 1), row(0, 1, 2));
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 0 AND c1 = 1 AND n = 1");
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 0 AND c1 = 1 AND n = 2");
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 1 AND c1 = 1 AND n = 0", row(1, 1, 1));
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 1 AND c1 = 1 AND n = 1", row(1, 1, 0), row(1, 1, 2));
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 1 AND c1 = 1 AND n = 2");
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 2 AND c1 = 1 AND n = 0");
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 2 AND c1 = 1 AND n = 1");
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 3 AND c1 = 1 AND n = 2");

        // single-partition queries with gt restriction on first clustering column
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 0 AND c1 > 0 AND n = 0", row(0, 1, 0), row(0, 1, 1), row(0, 1, 2));
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 0 AND c1 > 0 AND n = 1");
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 0 AND c1 > 0 AND n = 2");
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 1 AND c1 > 0 AND n = 0", row(1, 1, 1));
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 1 AND c1 > 0 AND n = 1", row(1, 1, 0), row(1, 1, 2));
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 1 AND c1 > 0 AND n = 2");
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 2 AND c1 > 0 AND n = 0");
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 2 AND c1 > 0 AND n = 1");
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 3 AND c1 > 0 AND n = 2");

        // single-partition queries with lt restriction on first clustering column
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 0 AND c1 < 1 AND n = 0", row(0, 0, 0), row(0, 0, 1), row(0, 0, 2));
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 0 AND c1 < 1 AND n = 1");
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 0 AND c1 < 1 AND n = 2");
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 1 AND c1 < 1 AND n = 0", row(1, 0, 0), row(1, 0, 2));
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 1 AND c1 < 1 AND n = 1", row(1, 0, 1));
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 1 AND c1 < 1 AND n = 2");
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 2 AND c1 < 1 AND n = 0");
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 2 AND c1 < 1 AND n = 1");
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 3 AND c1 < 1 AND n = 2");

        // single-partition queries with gte restriction on first clustering column
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 0 AND c1 >= 0 AND n = 0", row(0, 0, 0), row(0, 0, 1), row(0, 0, 2), row(0, 1, 0), row(0, 1, 1), row(0, 1, 2));
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 0 AND c1 >= 0 AND n = 1");
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 0 AND c1 >= 0 AND n = 2");
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 1 AND c1 >= 0 AND n = 0", row(1, 0, 0), row(1, 0, 2), row(1, 1, 1));
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 1 AND c1 >= 0 AND n = 1", row(1, 0, 1), row(1, 1, 0), row(1, 1, 2));
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 1 AND c1 >= 0 AND n = 2");
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 2 AND c1 >= 0 AND n = 0");
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 2 AND c1 >= 0 AND n = 1");
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 3 AND c1 >= 0 AND n = 2");

        // single-partition queries with lte restriction on first clustering column
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 0 AND c1 <= 1 AND n = 0", row(0, 0, 0), row(0, 0, 1), row(0, 0, 2), row(0, 1, 0), row(0, 1, 1), row(0, 1, 2));
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 0 AND c1 <= 1 AND n = 1");
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 0 AND c1 <= 1 AND n = 2");
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 1 AND c1 <= 1 AND n = 0", row(1, 0, 0), row(1, 0, 2), row(1, 1, 1));
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 1 AND c1 <= 1 AND n = 1", row(1, 0, 1), row(1, 1, 0), row(1, 1, 2));
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 1 AND c1 <= 1 AND n = 2");
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 2 AND c1 <= 1 AND n = 0");
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 2 AND c1 <= 1 AND n = 1");
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 3 AND c1 <= 1 AND n = 2");

        // single-partition queries with eq restriction on second clustering column
        assertHasNoQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 0 AND c1 = 0 AND c2 = 0 AND n = 0", row(0, 0, 0));
        assertHasNoQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 0 AND c1 = 0 AND c2 = 0 AND n = 1");
        assertHasNoQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 0 AND c1 = 0 AND c2 = 0 AND n = 2");
        assertHasNoQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 0 AND c1 = 0 AND c2 = 1 AND n = 0", row(0, 0, 1));
        assertHasNoQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 0 AND c1 = 0 AND c2 = 1 AND n = 1");
        assertHasNoQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 0 AND c1 = 0 AND c2 = 1 AND n = 2");
        assertHasNoQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 0 AND c1 = 0 AND c2 = 2 AND n = 0", row(0, 0, 2));
        assertHasNoQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 0 AND c1 = 0 AND c2 = 2 AND n = 1");
        assertHasNoQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 0 AND c1 = 0 AND c2 = 2 AND n = 2");
        assertHasNoQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 0 AND c1 = 1 AND c2 = 0 AND n = 0", row(0, 1, 0));
        assertHasNoQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 0 AND c1 = 1 AND c2 = 0 AND n = 1");
        assertHasNoQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 0 AND c1 = 1 AND c2 = 0 AND n = 2");
        assertHasNoQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 0 AND c1 = 1 AND c2 = 1 AND n = 0", row(0, 1, 1));
        assertHasNoQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 0 AND c1 = 1 AND c2 = 1 AND n = 1");
        assertHasNoQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 0 AND c1 = 1 AND c2 = 1 AND n = 2");
        assertHasNoQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 0 AND c1 = 1 AND c2 = 2 AND n = 0", row(0, 1, 2));
        assertHasNoQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 0 AND c1 = 1 AND c2 = 2 AND n = 1");
        assertHasNoQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 0 AND c1 = 1 AND c2 = 2 AND n = 2");
        assertHasNoQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 1 AND c1 = 0 AND c2 = 0 AND n = 0", row(1, 0, 0));
        assertHasNoQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 1 AND c1 = 0 AND c2 = 0 AND n = 1");
        assertHasNoQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 1 AND c1 = 0 AND c2 = 0 AND n = 2");
        assertHasNoQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 1 AND c1 = 0 AND c2 = 1 AND n = 0");
        assertHasNoQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 1 AND c1 = 0 AND c2 = 1 AND n = 1", row(1, 0, 1));
        assertHasNoQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 1 AND c1 = 0 AND c2 = 1 AND n = 2");
        assertHasNoQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 1 AND c1 = 0 AND c2 = 2 AND n = 0", row(1, 0, 2));
        assertHasNoQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 1 AND c1 = 0 AND c2 = 2 AND n = 1");
        assertHasNoQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 1 AND c1 = 0 AND c2 = 2 AND n = 2");
        assertHasNoQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 1 AND c1 = 1 AND c2 = 0 AND n = 0");
        assertHasNoQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 1 AND c1 = 1 AND c2 = 0 AND n = 1", row(1, 1, 0));
        assertHasNoQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 1 AND c1 = 1 AND c2 = 0 AND n = 2");
        assertHasNoQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 1 AND c1 = 1 AND c2 = 1 AND n = 0", row(1, 1, 1));
        assertHasNoQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 1 AND c1 = 1 AND c2 = 1 AND n = 1");
        assertHasNoQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 1 AND c1 = 1 AND c2 = 1 AND n = 2");
        assertHasNoQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 1 AND c1 = 1 AND c2 = 2 AND n = 0");
        assertHasNoQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 1 AND c1 = 1 AND c2 = 2 AND n = 1", row(1, 1, 2));
        assertHasNoQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 1 AND c1 = 1 AND c2 = 2 AND n = 2");
        assertHasNoQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 2 AND c1 = 0 AND c2 = 0 AND n = 0");
        assertHasNoQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 2 AND c1 = 0 AND c2 = 0 AND n = 1");
        assertHasNoQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 2 AND c1 = 0 AND c2 = 1 AND n = 0");
        assertHasNoQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 2 AND c1 = 0 AND c2 = 1 AND n = 1");
        assertHasNoQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 2 AND c1 = 0 AND c2 = 2 AND n = 0");
        assertHasNoQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 2 AND c1 = 0 AND c2 = 2 AND n = 1");
        assertHasNoQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 2 AND c1 = 1 AND c2 = 0 AND n = 0");
        assertHasNoQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 2 AND c1 = 1 AND c2 = 0 AND n = 1");
        assertHasNoQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 2 AND c1 = 1 AND c2 = 1 AND n = 0");
        assertHasNoQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 2 AND c1 = 1 AND c2 = 1 AND n = 1");
        assertHasNoQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 2 AND c1 = 1 AND c2 = 2 AND n = 0");
        assertHasNoQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 2 AND c1 = 1 AND c2 = 2 AND n = 1");

        // single-partition queries with gt restriction on second clustering column
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 0 AND c1 = 0 AND c2 > 0 AND n = 0", row(0, 0, 1), row(0, 0, 2));
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 0 AND c1 = 0 AND c2 > 0 AND n = 1");
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 0 AND c1 = 0 AND c2 > 0 AND n = 2");
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 0 AND c1 = 1 AND c2 > 0 AND n = 0", row(0, 1, 1), row(0, 1, 2));
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 0 AND c1 = 1 AND c2 > 0 AND n = 1");
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 0 AND c1 = 1 AND c2 > 0 AND n = 2");
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 1 AND c1 = 0 AND c2 > 0 AND n = 0", row(1, 0, 2));
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 1 AND c1 = 0 AND c2 > 0 AND n = 1", row(1, 0, 1));
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 1 AND c1 = 0 AND c2 > 0 AND n = 2");
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 1 AND c1 = 1 AND c2 > 0 AND n = 0", row(1, 1, 1));
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 1 AND c1 = 1 AND c2 > 0 AND n = 1", row(1, 1, 2));
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 1 AND c1 = 1 AND c2 > 0 AND n = 2");
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 2 AND c1 = 0 AND c2 > 0 AND n = 0");
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 2 AND c1 = 0 AND c2 > 0 AND n = 1");
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 2 AND c1 = 1 AND c2 > 0 AND n = 0");
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 2 AND c1 = 1 AND c2 > 0 AND n = 1");

        // single-partition queries with lt restriction on second clustering column
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 0 AND c1 = 0 AND c2 < 2 AND n = 0", row(0, 0, 0), row(0, 0, 1));
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 0 AND c1 = 0 AND c2 < 2 AND n = 1");
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 0 AND c1 = 0 AND c2 < 2 AND n = 2");
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 0 AND c1 = 1 AND c2 < 2 AND n = 0", row(0, 1, 0), row(0, 1, 1));
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 0 AND c1 = 1 AND c2 < 2 AND n = 1");
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 0 AND c1 = 1 AND c2 < 2 AND n = 2");
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 1 AND c1 = 0 AND c2 < 2 AND n = 0", row(1, 0, 0));
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 1 AND c1 = 0 AND c2 < 2 AND n = 1", row(1, 0, 1));
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 1 AND c1 = 0 AND c2 < 2 AND n = 2");
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 1 AND c1 = 1 AND c2 < 2 AND n = 0", row(1, 1, 1));
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 1 AND c1 = 1 AND c2 < 2 AND n = 1", row(1, 1, 0));
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 1 AND c1 = 1 AND c2 < 2 AND n = 2");
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 2 AND c1 = 0 AND c2 < 2 AND n = 0");
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 2 AND c1 = 0 AND c2 < 2 AND n = 1");
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 2 AND c1 = 1 AND c2 < 2 AND n = 0");
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 2 AND c1 = 1 AND c2 < 2 AND n = 1");

        // single-partition queries with gte restriction on second clustering column
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 0 AND c1 = 0 AND c2 >= 0 AND n = 0", row(0, 0, 0), row(0, 0, 1), row(0, 0, 2));
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 0 AND c1 = 0 AND c2 >= 0 AND n = 1");
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 0 AND c1 = 0 AND c2 >= 0 AND n = 2");
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 0 AND c1 = 1 AND c2 >= 0 AND n = 0", row(0, 1, 0), row(0, 1, 1), row(0, 1, 2));
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 0 AND c1 = 1 AND c2 >= 0 AND n = 1");
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 0 AND c1 = 1 AND c2 >= 0 AND n = 2");
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 1 AND c1 = 0 AND c2 >= 0 AND n = 0", row(1, 0, 0), row(1, 0, 2));
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 1 AND c1 = 0 AND c2 >= 0 AND n = 1", row(1, 0, 1));
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 1 AND c1 = 0 AND c2 >= 0 AND n = 2");
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 1 AND c1 = 1 AND c2 >= 0 AND n = 0", row(1, 1, 1));
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 1 AND c1 = 1 AND c2 >= 0 AND n = 1", row(1, 1, 0), row(1, 1, 2));
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 1 AND c1 = 1 AND c2 >= 0 AND n = 2");
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 2 AND c1 = 0 AND c2 >= 0 AND n = 0");
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 2 AND c1 = 0 AND c2 >= 0 AND n = 1");
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 2 AND c1 = 1 AND c2 >= 0 AND n = 0");
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 2 AND c1 = 1 AND c2 >= 0 AND n = 1");

        // single-partition queries with lte restriction on second clustering column
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 0 AND c1 = 0 AND c2 <= 2 AND n = 0", row(0, 0, 0), row(0, 0, 1), row(0, 0, 2));
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 0 AND c1 = 0 AND c2 <= 2 AND n = 1");
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 0 AND c1 = 0 AND c2 <= 2 AND n = 2");
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 0 AND c1 = 1 AND c2 <= 2 AND n = 0", row(0, 1, 0), row(0, 1, 1), row(0, 1, 2));
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 0 AND c1 = 1 AND c2 <= 2 AND n = 1");
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 0 AND c1 = 1 AND c2 <= 2 AND n = 2");
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 1 AND c1 = 0 AND c2 <= 2 AND n = 0", row(1, 0, 0), row(1, 0, 2));
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 1 AND c1 = 0 AND c2 <= 2 AND n = 1", row(1, 0, 1));
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 1 AND c1 = 0 AND c2 <= 2 AND n = 2");
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 1 AND c1 = 1 AND c2 <= 2 AND n = 0", row(1, 1, 1));
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 1 AND c1 = 1 AND c2 <= 2 AND n = 1", row(1, 1, 0), row(1, 1, 2));
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 1 AND c1 = 1 AND c2 <= 2 AND n = 2");
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 2 AND c1 = 0 AND c2 <= 2 AND n = 0");
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 2 AND c1 = 0 AND c2 <= 2 AND n = 1");
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 2 AND c1 = 1 AND c2 <= 2 AND n = 0");
        assertHasQueryPlan("SELECT k, c1, c2 FROM %s WHERE k = 2 AND c1 = 1 AND c2 <= 2 AND n = 1");
    }

    @Test
    public void testCollections()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, s set<int>, l list<int>, m map<int, int>)");
        createIndex("CREATE CUSTOM INDEX s_idx ON %s(s) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX l_idx ON %s(l) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX m_k_idx ON %s(KEYS(m)) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX m_v_idx ON %s(VALUES(m)) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX m_e_idx ON %s(ENTRIES(m)) USING 'StorageAttachedIndex'");
        execute("INSERT INTO %s(k, s, l, m) VALUES (1, {1, 2, 3}, [1, 2, 3], {1:10, 2:20, 3:30})");
        execute("INSERT INTO %s(k, s, l, m) VALUES (2, {2, 3, 4}, [2, 3, 4], {2:20, 3:30, 4:40})");
        execute("INSERT INTO %s(k, s, l, m) VALUES (3, {4, 5 ,6}, [4, 5 ,6], {4:40, 5:50 ,6:60})");

        // range queries, unbounded
        assertHasQueryPlan("SELECT k FROM %s WHERE s CONTAINS 1", row(1));
        assertHasQueryPlan("SELECT k FROM %s WHERE s CONTAINS 2", row(1), row(2));
        assertHasQueryPlan("SELECT k FROM %s WHERE s CONTAINS 3", row(1), row(2));
        assertHasQueryPlan("SELECT k FROM %s WHERE s CONTAINS 4", row(2), row(3));
        assertHasQueryPlan("SELECT k FROM %s WHERE s NOT CONTAINS 1", row(2), row(3));
        assertHasQueryPlan("SELECT k FROM %s WHERE s NOT CONTAINS 2", row(3));
        assertHasQueryPlan("SELECT k FROM %s WHERE s NOT CONTAINS 3", row(3));
        assertHasQueryPlan("SELECT k FROM %s WHERE s NOT CONTAINS 4", row(1));
        assertHasQueryPlan("SELECT k FROM %s WHERE l CONTAINS 1", row(1));
        assertHasQueryPlan("SELECT k FROM %s WHERE l CONTAINS 2", row(1), row(2));
        assertHasQueryPlan("SELECT k FROM %s WHERE l CONTAINS 3", row(1), row(2));
        assertHasQueryPlan("SELECT k FROM %s WHERE l CONTAINS 4", row(2), row(3));
        assertHasQueryPlan("SELECT k FROM %s WHERE l NOT CONTAINS 1", row(2), row(3));
        assertHasQueryPlan("SELECT k FROM %s WHERE l NOT CONTAINS 2", row(3));
        assertHasQueryPlan("SELECT k FROM %s WHERE l NOT CONTAINS 3", row(3));
        assertHasQueryPlan("SELECT k FROM %s WHERE l NOT CONTAINS 4", row(1));
        assertHasQueryPlan("SELECT k FROM %s WHERE m CONTAINS KEY 1", row(1));
        assertHasQueryPlan("SELECT k FROM %s WHERE m CONTAINS KEY 2", row(1), row(2));
        assertHasQueryPlan("SELECT k FROM %s WHERE m CONTAINS KEY 3", row(1), row(2));
        assertHasQueryPlan("SELECT k FROM %s WHERE m CONTAINS KEY 4", row(2), row(3));
        assertHasQueryPlan("SELECT k FROM %s WHERE m NOT CONTAINS KEY 1", row(2), row(3));
        assertHasQueryPlan("SELECT k FROM %s WHERE m NOT CONTAINS KEY 2", row(3));
        assertHasQueryPlan("SELECT k FROM %s WHERE m NOT CONTAINS KEY 3", row(3));
        assertHasQueryPlan("SELECT k FROM %s WHERE m NOT CONTAINS KEY 4", row(1));
        assertHasQueryPlan("SELECT k FROM %s WHERE m CONTAINS 10", row(1));
        assertHasQueryPlan("SELECT k FROM %s WHERE m CONTAINS 20", row(1), row(2));
        assertHasQueryPlan("SELECT k FROM %s WHERE m CONTAINS 30", row(1), row(2));
        assertHasQueryPlan("SELECT k FROM %s WHERE m CONTAINS 40", row(2), row(3));
        assertHasQueryPlan("SELECT k FROM %s WHERE m NOT CONTAINS 10", row(2), row(3));
        assertHasQueryPlan("SELECT k FROM %s WHERE m NOT CONTAINS 20", row(3));
        assertHasQueryPlan("SELECT k FROM %s WHERE m NOT CONTAINS 30", row(3));
        assertHasQueryPlan("SELECT k FROM %s WHERE m NOT CONTAINS 40", row(1));
        assertHasQueryPlan("SELECT k FROM %s WHERE m[1] = 10", row(1));
        assertHasQueryPlan("SELECT k FROM %s WHERE m[2] = 20", row(1), row(2));
        assertHasQueryPlan("SELECT k FROM %s WHERE m[3] = 30", row(1), row(2));
        assertHasQueryPlan("SELECT k FROM %s WHERE m[4] = 40", row(2), row(3));
        assertHasQueryPlan("SELECT k FROM %s WHERE m[1] != 10", row(2), row(3));
        assertHasQueryPlan("SELECT k FROM %s WHERE m[2] != 20", row(3));
        assertHasQueryPlan("SELECT k FROM %s WHERE m[3] != 30", row(3));
        assertHasQueryPlan("SELECT k FROM %s WHERE m[4] != 40", row(1));

        // single-partition queries
        assertHasNoQueryPlan("SELECT k FROM %s WHERE k = 1 AND s CONTAINS 1", row(1));
        assertHasNoQueryPlan("SELECT k FROM %s WHERE k = 1 AND s CONTAINS 2", row(1));
        assertHasNoQueryPlan("SELECT k FROM %s WHERE k = 1 AND s CONTAINS 3", row(1));
        assertHasNoQueryPlan("SELECT k FROM %s WHERE k = 1 AND s CONTAINS 4");
        assertHasNoQueryPlan("SELECT k FROM %s WHERE k = 1 AND s NOT CONTAINS 1");
        assertHasNoQueryPlan("SELECT k FROM %s WHERE k = 1 AND s NOT CONTAINS 2");
        assertHasNoQueryPlan("SELECT k FROM %s WHERE k = 1 AND s NOT CONTAINS 3");
        assertHasNoQueryPlan("SELECT k FROM %s WHERE k = 1 AND s NOT CONTAINS 4", row(1));
        assertHasNoQueryPlan("SELECT k FROM %s WHERE k = 1 AND l CONTAINS 1", row(1));
        assertHasNoQueryPlan("SELECT k FROM %s WHERE k = 1 AND l CONTAINS 2", row(1));
        assertHasNoQueryPlan("SELECT k FROM %s WHERE k = 1 AND l CONTAINS 3", row(1));
        assertHasNoQueryPlan("SELECT k FROM %s WHERE k = 1 AND l CONTAINS 4");
        assertHasNoQueryPlan("SELECT k FROM %s WHERE k = 1 AND l NOT CONTAINS 1");
        assertHasNoQueryPlan("SELECT k FROM %s WHERE k = 1 AND l NOT CONTAINS 2");
        assertHasNoQueryPlan("SELECT k FROM %s WHERE k = 1 AND l NOT CONTAINS 3");
        assertHasNoQueryPlan("SELECT k FROM %s WHERE k = 1 AND l NOT CONTAINS 4", row(1));
        assertHasNoQueryPlan("SELECT k FROM %s WHERE k = 1 AND m CONTAINS KEY 1", row(1));
        assertHasNoQueryPlan("SELECT k FROM %s WHERE k = 1 AND m CONTAINS KEY 2", row(1));
        assertHasNoQueryPlan("SELECT k FROM %s WHERE k = 1 AND m CONTAINS KEY 3", row(1));
        assertHasNoQueryPlan("SELECT k FROM %s WHERE k = 1 AND m CONTAINS KEY 4");
        assertHasNoQueryPlan("SELECT k FROM %s WHERE k = 1 AND m NOT CONTAINS KEY 1");
        assertHasNoQueryPlan("SELECT k FROM %s WHERE k = 1 AND m NOT CONTAINS KEY 2");
        assertHasNoQueryPlan("SELECT k FROM %s WHERE k = 1 AND m NOT CONTAINS KEY 3");
        assertHasNoQueryPlan("SELECT k FROM %s WHERE k = 1 AND m NOT CONTAINS KEY 4", row(1));
        assertHasNoQueryPlan("SELECT k FROM %s WHERE k = 1 AND m CONTAINS 10", row(1));
        assertHasNoQueryPlan("SELECT k FROM %s WHERE k = 1 AND m CONTAINS 20", row(1));
        assertHasNoQueryPlan("SELECT k FROM %s WHERE k = 1 AND m CONTAINS 30", row(1));
        assertHasNoQueryPlan("SELECT k FROM %s WHERE k = 1 AND m CONTAINS 40");
        assertHasNoQueryPlan("SELECT k FROM %s WHERE k = 1 AND m NOT CONTAINS 10");
        assertHasNoQueryPlan("SELECT k FROM %s WHERE k = 1 AND m NOT CONTAINS 20");
        assertHasNoQueryPlan("SELECT k FROM %s WHERE k = 1 AND m NOT CONTAINS 30");
        assertHasNoQueryPlan("SELECT k FROM %s WHERE k = 1 AND m NOT CONTAINS 40", row(1));
        assertHasNoQueryPlan("SELECT k FROM %s WHERE k = 1 AND m[1] = 10", row(1));
        assertHasNoQueryPlan("SELECT k FROM %s WHERE k = 1 AND m[2] = 20", row(1));
        assertHasNoQueryPlan("SELECT k FROM %s WHERE k = 1 AND m[3] = 30", row(1));
        assertHasNoQueryPlan("SELECT k FROM %s WHERE k = 1 AND m[4] = 40");
        assertHasNoQueryPlan("SELECT k FROM %s WHERE k = 1 AND m[1] != 10");
        assertHasNoQueryPlan("SELECT k FROM %s WHERE k = 1 AND m[2] != 20");
        assertHasNoQueryPlan("SELECT k FROM %s WHERE k = 1 AND m[3] != 30");
        assertHasNoQueryPlan("SELECT k FROM %s WHERE k = 1 AND m[4] != 40", row(1));
    }
}
