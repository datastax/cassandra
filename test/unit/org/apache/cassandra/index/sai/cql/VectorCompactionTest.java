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
import java.util.stream.Collectors;

import org.junit.Test;

import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.index.sai.disk.v3.V3OnDiskFormat;
import org.apache.cassandra.index.sai.disk.v5.V5VectorPostingsWriter;
import org.apache.cassandra.index.sai.disk.vector.CompactionGraph;

import static org.apache.cassandra.index.sai.disk.vector.CassandraOnHeapGraph.MIN_PQ_ROWS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class VectorCompactionTest extends VectorTester.Versioned
{
    @Test
    public void testCompactionWithEnoughRowsForPQAndDeleteARow()
    {
        createTable();
        disableCompaction();

        for (int i = 0; i <= MIN_PQ_ROWS; i++)
            execute("INSERT INTO %s (pk, v) VALUES (?, ?)", i, vector(i, i + 1));
        flush();

        // By deleting a row, we trigger a key histogram to round its estimate to 0 instead of 1 rows per key, and
        // that broke compaction, so we test that here.
        execute("DELETE FROM %s WHERE pk = 0");
        flush();

        // Run compaction, it fails if compaction is not successful
        compact();

        // Confirm we can query the data
        assertRowCount(execute("SELECT * FROM %s ORDER BY v ANN OF [1,2] LIMIT 1"), 1);
    }

    @Test
    public void testPQRefine()
    {
        createTable();
        disableCompaction();

        var vectors = new ArrayList<float[]>();
        // 3 sstables
        for (int j = 0; j < 3; j++)
        {
            for (int i = 0; i <= MIN_PQ_ROWS; i++)
            {
                var pk = j * MIN_PQ_ROWS + i;
                var v = create2DVector();
                vectors.add(v);
                execute("INSERT INTO %s (pk, v) VALUES (?, ?)", pk, vector(v));
            }
            flush();
        }

        CompactionGraph.PQ_TRAINING_SIZE = 2 * MIN_PQ_ROWS;
        compact();

        // Confirm we can query the data with reasonable recall
        double recall = 0;
        int ITERS = 10;
        for (int i = 0; i < ITERS; i++)
        {
            var q = create2DVector();
            var result = execute("SELECT pk, v FROM %s ORDER BY v ANN OF ? LIMIT 20", vector(q));
            var ann = result.stream().map(row -> {
                var vList = row.getVector("v", FloatType.instance, 2);
                return new float[]{ vList.get(0), vList.get(1) };
            }).collect(Collectors.toList());
            recall += computeRecall(vectors, q, ann, VectorSimilarityFunction.COSINE);
        }
        recall /= ITERS;
        assert recall >= 0.9 : recall;
    }

    @Test
    public void testOneToManyCompaction()
    {
        for (int sstables = 2; sstables <= 3; sstables++)
        {
            testOneToManyCompactionInternal(10, sstables);
            testOneToManyCompactionInternal(MIN_PQ_ROWS, sstables);
        }
    }

    // Exercise the one-to-many path in compaction
    public void testOneToManyCompactionInternal(int vectorsPerSstable, int sstables)
    {
        createTable();

        disableCompaction();

        insertOneToManyRows(vectorsPerSstable, sstables);

        // queries should behave sanely before and after compaction
        validateQueries();
        compact();
        validateQueries();
    }

    @Test
    public void testOneToManyCompactionTooManyHoles()
    {
        int sstables = 2;
        testOneToManyCompactionInternal(10, sstables);
        testOneToManyCompactionHolesInternal(MIN_PQ_ROWS, sstables);
    }

    public void testOneToManyCompactionHolesInternal(int vectorsPerSstable, int sstables)
    {
        createTable();

        disableCompaction();

        insertOneToManyRows(vectorsPerSstable, sstables);

        // this should be done after writing data so that we exercise the "we thought we were going to use the
        // one-to-many-path but there were too many holes so we changed plans" code path
        V5VectorPostingsWriter.GLOBAL_HOLES_ALLOWED = 0.0;

        // queries should behave sanely before and after compaction
        validateQueries();
        compact();
        validateQueries();
    }

    private void insertOneToManyRows(int vectorsPerSstable, int sstables)
    {
        var R = getRandom();
        double duplicateChance = R.nextDouble() * 0.2;
        int j = 0;
        for (int i = 0; i < sstables; i++)
        {
            var vectorsInserted = new ArrayList<Vector<Float>>();
            var duplicateExists = false;
            while (vectorsInserted.size() < vectorsPerSstable || !duplicateExists)
            {
                Vector<Float> v;
                if (R.nextDouble() < duplicateChance && !vectorsInserted.isEmpty())
                {
                    v = vectorsInserted.get(R.nextIntBetween(0, vectorsInserted.size() - 1));
                    duplicateExists = true;
                }
                else
                {
                    v = randomVectorBoxed(2);
                    vectorsInserted.add(v);
                }
                assert v != null;
                execute("INSERT INTO %s (pk, v) VALUES (?, ?)", j++, v);
            }
            flush();
        }
    }

    private void createTable()
    {
        createTable("CREATE TABLE %s (pk int, v vector<float, 2>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
    }

    private void validateQueries()
    {
        for (int i = 0; i < 10; i++)
        {
            var q = randomVectorBoxed(2);
            var r = execute("SELECT pk, similarity_cosine(v, ?) as similarity FROM %s ORDER BY v ANN OF ? LIMIT 10", q, q);
            float lastSimilarity = Float.MAX_VALUE;
            assertEquals(10, r.size());
            for (var row : r)
            {
                float similarity = (float) row.getFloat("similarity");
                assertTrue(similarity <= lastSimilarity);
                lastSimilarity = similarity;
            }
        }
    }

    private static float[] create2DVector() {
        var R = getRandom();
        return new float[] { R.nextFloatBetween(-100, 100), R.nextFloatBetween(-100, 100) };
    }
}
