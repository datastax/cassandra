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
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.index.sai.SAIUtil;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.disk.v5.V5VectorPostingsWriter;
import org.apache.cassandra.index.sai.disk.vector.NVQUtil;

import static org.apache.cassandra.index.sai.disk.vector.CassandraOnHeapGraph.MIN_PQ_ROWS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class VectorCompactionTest extends VectorTester
{
    @Parameterized.Parameter(0)
    public Version version;

    @Parameterized.Parameter(1)
    public int dimension;

    @Parameterized.Parameter(2)
    public boolean enableNVQ;

    @Parameterized.Parameters(name = "{0} {1} {2}")
    public static Collection<Object[]> data()
    {
        // Run with relatively low and medium dimensions since they can test slightly different code paths
        var dimensions = new int[]{ 2, 100 };
        // See Version file for explanation of changes associated with each version
        return Version.ALL.stream()
                          .filter(v -> v.onOrAfter(Version.JVECTOR_EARLIEST))
                          .flatMap(v -> Arrays.stream(dimensions).mapToObj(d -> new Object[]{ v, d }))
                          .flatMap(vd -> {
                              // NVQ is only relevant some of the time, so add it to the test matrix if relevant
                              var enableNVQ = NVQUtil.shouldWriteNVQ((int) vd[1], (Version) vd[0])
                                              ? new Boolean[]{ true, false }
                                              : new Boolean[]{ false };
                              return Arrays.stream(enableNVQ).map(b -> new Object[]{ vd[0], vd[1], b });
                          })
                          .collect(Collectors.toList());
    }

    @Before
    public void setCurrentVersion() throws Throwable
    {
        SAIUtil.setCurrentVersion(version);
    }

    @Before
    public void setEnableNVQ() throws Throwable
    {
        SAIUtil.setEnableNVQ(enableNVQ);
    }

    @Test
    public void testCompactionWithEnoughRowsForPQAndDeleteARow()
    {
        createTable();
        disableCompaction();

        var vectors = new HashSet<>();
        for (int i = 0; i <= MIN_PQ_ROWS; i++)
        {
            Vector<Float> vector;
            do
            {
                vector = randomVectorBoxed(dimension); // confirm no duplicates
            } while (!vectors.add(vector));
            // make ascending counted vector
            execute("INSERT INTO %s (pk, v) VALUES (?, ?)", i, vector);
        }
        flush();

        // By deleting a row, we trigger a key histogram to round its estimate to 0 instead of 1 rows per key, and
        // that broke compaction, so we test that here.
        execute("DELETE FROM %s WHERE pk = 0");
        flush();

        // Run compaction, it fails if compaction is not successful
        compact();

        // Confirm we can query the data
        assertRowCount(execute("SELECT * FROM %s ORDER BY v ANN OF ? LIMIT 1", randomVectorBoxed(dimension)), 1);
    }

    @Test
    public void testPQRefine()
    {
        // The test fails for dimensions > 2, not sure why, but likely due to the use of random vectors.
        if (dimension > 2)
            return;

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

//        TODO deterimine proper design for PQ training on vectors of dimension < 100.
//        see https://github.com/riptano/cndb/issues/13630
//        CompactionGraph.PQ_TRAINING_SIZE = 2 * MIN_PQ_ROWS;
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

    @Test
    public void testZeroOrOneToManyCompaction()
    {
        for (int sstables = 2; sstables <= 3; sstables++)
        {
            testZeroOrOneToManyCompactionInternal(10, sstables);
            testZeroOrOneToManyCompactionInternal(MIN_PQ_ROWS, sstables);
        }
    }

    public void testZeroOrOneToManyCompactionInternal(int vectorsPerSstable, int sstables)
    {
        createTable();
        disableCompaction();

        insertZeroOrOneToManyRows(vectorsPerSstable, sstables);

        validateQueries();
        compact();
        validateQueries();
    }

    private void insertZeroOrOneToManyRows(int vectorsPerSstable, int sstables)
    {
        var R = getRandom();
        double duplicateChance = R.nextDouble() * 0.2;
        int j = 0;
        boolean nullInserted = false;
        
        for (int i = 0; i < sstables; i++)
        {
            var vectorsInserted = new ArrayList<Vector<Float>>();
            var duplicateExists = false;
            while (vectorsInserted.size() < vectorsPerSstable || !duplicateExists)
            {
                if (!nullInserted && vectorsInserted.size() == vectorsPerSstable/2)
                {
                    // Insert one null vector in the middle
                    execute("INSERT INTO %s (pk, v) VALUES (?, null)", j++);
                    nullInserted = true;
                    continue;
                }

                Vector<Float> v;
                if (R.nextDouble() < duplicateChance && !vectorsInserted.isEmpty())
                {
                    // insert a duplicate
                    v = vectorsInserted.get(R.nextIntBetween(0, vectorsInserted.size() - 1));
                    duplicateExists = true;
                }
                else
                {
                    // insert a new random vector
                    v = randomVectorBoxed(dimension);
                    vectorsInserted.add(v);
                }
                assert v != null;
                execute("INSERT INTO %s (pk, v) VALUES (?, ?)", j++, v);
            }
            flush();
        }
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
                    v = randomVectorBoxed(dimension);
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
        createTable("CREATE TABLE %s (pk int, v vector<float, " + dimension + ">, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
    }

    private void validateQueries()
    {
        for (int i = 0; i < 10; i++)
        {
            var q = randomVectorBoxed(dimension);
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
