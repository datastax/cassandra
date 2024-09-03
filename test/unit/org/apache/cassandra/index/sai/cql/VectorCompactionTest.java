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

import java.util.ArrayList;
import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.index.sai.SAIUtil;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.disk.vector.CassandraOnHeapGraph;

@RunWith(Parameterized.class)
public class VectorCompactionTest extends VectorTester
{
    @Parameterized.Parameter
    public Version version;

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data()
    {
        return Stream.of(Version.CA, Version.DC).map(v -> new Object[]{ v}).collect(Collectors.toList());
    }

    @Before
    @Override
    public void setup() throws Throwable
    {
        super.setup();
        SAIUtil.setLatestVersion(version);
    }

    @Test
    public void testCompactionWithEnoughRowsForPQAndDeleteARow()
    {
        createTable("CREATE TABLE %s (pk int, vec vector<float, 2>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(vec) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();

        disableCompaction();

        for (int i = 0; i <= CassandraOnHeapGraph.MIN_PQ_ROWS; i++)
            execute("INSERT INTO %s (pk, vec) VALUES (?, ?)", i, vector(i, i + 1));
        flush();

        // By deleting a row, we trigger a key histogram to round its estimate to 0 instead of 1 rows per key, and
        // that broke compaction, so we test that here.
        execute("DELETE FROM %s WHERE pk = 0");
        flush();

        // Run compaction, it fails if compaction is not successful
        compact();

        // Confirm we can query the data
        assertRowCount(execute("SELECT * FROM %s ORDER BY vec ANN OF [1,2] LIMIT 1"), 1);
    }

    @Test
    public void testOneToManyCompaction()
    {
        for (int sstables = 2; sstables <= 3; sstables++)
        {
            testOneToManyCompactionInternal(10, sstables);
            testOneToManyCompactionInternal(CassandraOnHeapGraph.MIN_PQ_ROWS, sstables);
        }
    }

    public void testOneToManyCompactionInternal(int vectorsPerSstable, int sstables)
    {
        // Exercise the one-to-many path in compaction
        createTable("CREATE TABLE %s (pk int, vec vector<float, 2>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(vec) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();

        disableCompaction();

        var R = getRandom();
        double duplicateChance = R.nextDouble() * 0.2;
        int j = 0;
        for (int i = 0; i < sstables; i++)
        {
            var vectorsInserted = new ArrayList<Vector<Float>>();
            while (vectorsInserted.size() < vectorsPerSstable)
            {
                Vector<Float> v;
                if (R.nextDouble() < duplicateChance && !vectorsInserted.isEmpty())
                {
                    v = vectorsInserted.get(R.nextIntBetween(0, vectorsInserted.size() - 1));
                }
                else
                {
                    v = randomVectorBoxed(2);
                    vectorsInserted.add(v);
                }
                assert v != null;
                execute("INSERT INTO %s (pk, vec) VALUES (?, ?)", j++, v);
            }
            flush();
        }

        // Run compaction, it fails if compaction is not successful
        compact();

        // Confirm we can query the data
        assertRowCount(execute("SELECT * FROM %s ORDER BY vec ANN OF [1,2] LIMIT 1"), 1);
    }
}
