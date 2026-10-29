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

import org.apache.cassandra.index.sai.SAITester;

/**
 * Tests that ranges whose lower bound is after their upper bound (e.g. {@code x > 5 AND x < 3}), or whose bounds
 * are equal but not both inclusive, match nothing, rather than failing the memtable index trie slices, which
 * require ordered bounds.
 */
public class UnorderedRangeBoundsTest extends SAITester
{
    @Test
    public void testInt() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, x int)");
        createIndex("CREATE CUSTOM INDEX ON %s(x) USING 'StorageAttachedIndex'");
        for (int i = 0; i < 10; i++)
            execute("INSERT INTO %s (pk, x) VALUES (?, ?)", i, i);

        beforeAndAfterFlush(() -> {
            assertRowCount(execute("SELECT pk FROM %s WHERE x > 5 AND x < 3"), 0);
            assertRowCount(execute("SELECT pk FROM %s WHERE x >= 5 AND x <= 3"), 0);
            assertRowCount(execute("SELECT pk FROM %s WHERE x > 5 AND x < 5"), 0);
            assertRowCount(execute("SELECT pk FROM %s WHERE x >= 5 AND x < 5"), 0);
            assertRows(execute("SELECT pk FROM %s WHERE x >= 5 AND x <= 5"), row(5));
        });
    }
}
