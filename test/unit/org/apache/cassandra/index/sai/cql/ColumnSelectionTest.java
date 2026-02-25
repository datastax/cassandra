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

package org.apache.cassandra.index.sai.cql;

import org.junit.Test;

public class ColumnSelectionTest extends VectorTester
{
    /**
     * Tests that we can select any type of column in any table with SAI indexes. See CNDB-14997 for further details.
     */
    @Test
    public void testColumnSelection() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "k1 text, k2 text, " +
                    "c1 text, c2 text, " +
                    "s1 text static, s2 text static, " +
                    "r1 text, r2 vector<float, 2>, " +
                    "PRIMARY KEY((k1, k2), c1, c2))");
        createIndex("CREATE CUSTOM INDEX ON %s(r1) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(r2) USING 'StorageAttachedIndex'");

        Object[] row = row("k1", "k2", "c1", "c2", "s1", "s2", "r1", vector(1, 2));
        execute("INSERT INTO %s (k1, k2, c1, c2, s1, s2, r1, r2) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", row);

        beforeAndAfterFlush(() -> {

            // filtering
            assertRows(execute("SELECT * FROM %s WHERE r1='r1'"), row);
            assertRows(execute("SELECT k1 FROM %s WHERE r1='r1'"), row(row[0]));
            assertRows(execute("SELECT k2 FROM %s WHERE r1='r1'"), row(row[1]));
            assertRows(execute("SELECT c1 FROM %s WHERE r1='r1'"), row(row[2]));
            assertRows(execute("SELECT c2 FROM %s WHERE r1='r1'"), row(row[3]));
            assertRows(execute("SELECT s1 FROM %s WHERE r1='r1'"), row(row[4]));
            assertRows(execute("SELECT s2 FROM %s WHERE r1='r1'"), row(row[5]));
            assertRows(execute("SELECT r1 FROM %s WHERE r1='r1'"), row(row[6]));
            assertRows(execute("SELECT r2 FROM %s WHERE r1='r1'"), row(row[7]));
            assertRows(execute("SELECT k1, c1, s1, r1 FROM %s WHERE r1='r1'"), row(row[0], row[2], row[4], row[6]));
            assertRows(execute("SELECT k2, c2, s2, r2 FROM %s WHERE r1='r1'"), row(row[1], row[3], row[5], row[7]));

            // generic ordering
            assertRows(execute("SELECT * FROM %s ORDER BY r1 LIMIT 10"), row);
            assertRows(execute("SELECT k1 FROM %s ORDER BY r1 LIMIT 10"), row(row[0]));
            assertRows(execute("SELECT k2 FROM %s ORDER BY r1 LIMIT 10"), row(row[1]));
            assertRows(execute("SELECT c1 FROM %s ORDER BY r1 LIMIT 10"), row(row[2]));
            assertRows(execute("SELECT c2 FROM %s ORDER BY r1 LIMIT 10"), row(row[3]));
            assertRows(execute("SELECT s1 FROM %s ORDER BY r1 LIMIT 10"), row(row[4]));
            assertRows(execute("SELECT s2 FROM %s ORDER BY r1 LIMIT 10"), row(row[5]));
            assertRows(execute("SELECT r1 FROM %s ORDER BY r1 LIMIT 10"), row(row[6]));
            assertRows(execute("SELECT r2 FROM %s ORDER BY r1 LIMIT 10"), row(row[7]));
            assertRows(execute("SELECT k1, c1, s1, r1 FROM %s ORDER BY r1 LIMIT 10"), row(row[0], row[2], row[4], row[6]));
            assertRows(execute("SELECT k2, c2, s2, r2 FROM %s ORDER BY r1 LIMIT 10"), row(row[1], row[3], row[5], row[7]));

            // ANN ordering
            assertRows(execute("SELECT * FROM %s ORDER BY r2 ANN OF [1, 2] LIMIT 10"), row);
            assertRows(execute("SELECT k1 FROM %s ORDER BY r2 ANN OF [1, 2] LIMIT 10"), row(row[0]));
            assertRows(execute("SELECT k2 FROM %s ORDER BY r2 ANN OF [1, 2] LIMIT 10"), row(row[1]));
            assertRows(execute("SELECT c1 FROM %s ORDER BY r2 ANN OF [1, 2] LIMIT 10"), row(row[2]));
            assertRows(execute("SELECT c2 FROM %s ORDER BY r2 ANN OF [1, 2] LIMIT 10"), row(row[3]));
            assertRows(execute("SELECT s1 FROM %s ORDER BY r2 ANN OF [1, 2] LIMIT 10"), row(row[4]));
            assertRows(execute("SELECT s2 FROM %s ORDER BY r2 ANN OF [1, 2] LIMIT 10"), row(row[5]));
            assertRows(execute("SELECT r1 FROM %s ORDER BY r2 ANN OF [1, 2] LIMIT 10"), row(row[6]));
            assertRows(execute("SELECT r2 FROM %s ORDER BY r2 ANN OF [1, 2] LIMIT 10"), row(row[7]));
            assertRows(execute("SELECT k1, c1, s1, r1 FROM %s ORDER BY r2 ANN OF [1, 2] LIMIT 10"), row(row[0], row[2], row[4], row[6]));
            assertRows(execute("SELECT k2, c2, s2, r2 FROM %s ORDER BY r2 ANN OF [1, 2] LIMIT 10"), row(row[1], row[3], row[5], row[7]));
        });
    }
}
