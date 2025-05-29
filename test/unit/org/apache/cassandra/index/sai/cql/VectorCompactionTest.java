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
import java.util.HashSet;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Test;

import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.disk.v2.V2VectorIndexSearcher;
import org.apache.cassandra.index.sai.disk.v5.V5VectorPostingsWriter;
import org.apache.cassandra.index.sai.disk.vector.VectorCompression;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.index.sai.disk.vector.CassandraOnHeapGraph.MIN_PQ_ROWS;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class VectorCompactionTest extends VectorTester.Versioned
{
    @Before
    public void resetDisabledReads()
    {
        CassandraRelevantProperties.SAI_INDEX_READS_DISABLED.setBoolean(false);
    }

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
    public void testDelayedIndexCreation()
    {
        createTable("CREATE TABLE %s (pk int, v vector<float, 2>, PRIMARY KEY(pk))");
        disableCompaction();

        for (int i = 0; i <= MIN_PQ_ROWS; i++)
            execute("INSERT INTO %s (pk, v) VALUES (?, ?)", i, vector(i, i + 1));
        flush();

        // Disable reads, then create index asynchronously since it won't become queryable until reads are enabled
        CassandraRelevantProperties.SAI_INDEX_READS_DISABLED.setBoolean(true);
        createIndexAsync("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        CassandraRelevantProperties.SAI_INDEX_READS_DISABLED.setBoolean(false);
        reloadSSTableIndex();
        // Because it was async, we wait here to ensure it worked.
        waitForTableIndexesQueryable();

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

    @Test
    public void testSaiIndexReadsDisabledWithEmptyVectorIndex()
    {
        createTable();

        CassandraRelevantProperties.SAI_INDEX_READS_DISABLED.setBoolean(true);

        // Insert then delete a row, trigger compaction to produce the empty index
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 0, vector(0, 1));
        flush();
        execute("DELETE FROM %s WHERE pk = 0");
        flush();
        compact();

        // Now make index queryable
        CassandraRelevantProperties.SAI_INDEX_READS_DISABLED.setBoolean(false);
        reloadSSTableIndex();

        // Confirm we can query the data (in an early implementation of the feature in this PR, this assertion failed)
        assertRowCount(execute("SELECT * FROM %s ORDER BY v ANN OF [1,2] LIMIT 1"), 0);
    }

    @SuppressWarnings("resource")
    @Test
    public void testSaiIndexReadsDisabled()
    {
        createTable("CREATE TABLE %s (pk int, v vector<float, 2>, PRIMARY KEY(pk))");
        var indexName = createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");

        // We'll manage compaction manually for better control
        disableCompaction();

        for (int i = 0; i < MIN_PQ_ROWS; i++)
            execute("INSERT INTO %s (pk, v) VALUES (?, ?)", i, randomVectorBoxed(2));

        flush();

        // Only add a handful of rows to the second sstable so we have to iterate to the older sstable in the
        // pq selection logic
        for (int i = MIN_PQ_ROWS; i < MIN_PQ_ROWS + 10; i++)
            execute("INSERT INTO %s (pk, v) VALUES (?, ?)", i, randomVectorBoxed(2));

        flush();

        // Delete 100 rows so that we could only create a PQ if we use the original data.
        for (int i = 0; i < 100; i++)
            execute("DELETE FROM %s WHERE pk = ?",  i);

        flush();

        // Mimic setting of disabling reads, only takes effect on sstable index load, so reload
        CassandraRelevantProperties.SAI_INDEX_READS_DISABLED.setBoolean(true);
        reloadSSTableIndex();

        // Confirm that V1MetadataOnlySearchableIndex has valid metadata.
        var metadataOnlyIndex = (StorageAttachedIndex) getIndexManager(keyspace(), indexName).listIndexes().iterator().next();
        var metadataOnlySSTableIndexes = metadataOnlyIndex.getIndexContext().getView().getIndexes();
        assertEquals(3, metadataOnlySSTableIndexes.size());
        metadataOnlySSTableIndexes.forEach(i -> {
            // Skip the empty index
            if (i.isEmpty())
                return;

            assertFalse(i.areSegmentsLoaded());
            assertEquals(0, i.indexFileCacheSize());
            assertThrows(UnsupportedOperationException.class, i::getSegments);
            // This is vector specific, so if we broaden the functionality of V1MetadataOnlySearchableIndex, we might
            // need to update this.
            assertArrayEquals(new byte[] { 0, 0, 0, 0 }, ByteBufferUtil.getArray(i.minTerm()));
            assertArrayEquals(new byte[] { 0, 0, 0, 0 }, ByteBufferUtil.getArray(i.maxTerm()));
            // Confirm count matches sstable row id range (the range is inclusive)
            var sstableDiff = i.maxSSTableRowId() - i.minSSTableRowId() + 1;
            assertEquals(i.getRowCount(), sstableDiff);

            var postingsStructures = i.getPostingsStructures().collect(Collectors.toList());
            if (Version.current().onOrAfter(Version.DC))
            {
                assertEquals(1, postingsStructures.size());
                assertEquals(V5VectorPostingsWriter.Structure.ONE_TO_ONE, postingsStructures.get(0));
            }
            else
            {
                assertEquals(0, postingsStructures.size());
            }
        });

        // Now compact (this exercises the code, but doesn't actually prove that we used the past segment's PQ)
        compact();

        // Reload and make queryable
        CassandraRelevantProperties.SAI_INDEX_READS_DISABLED.setBoolean(false);
        reloadSSTableIndex();

        // Assert that we have a product quantization
        var index = (StorageAttachedIndex) getIndexManager(keyspace(), indexName).listIndexes().iterator().next();
        var sstableIndexes = index.getIndexContext().getView().getIndexes();
        assertEquals(1, sstableIndexes.size());
        var searcher = (V2VectorIndexSearcher) sstableIndexes.iterator().next().getSegments().iterator().next().getIndexSearcher();
        // We have a PQ even though we have fewer than MIN_PQ_ROWS because we used the original seed.
        // We only test like this to confirm that it worked, not because this is a strictly required behavior.
        // Further, it's unlikely we'll have so few rows.
        assertEquals(VectorCompression.CompressionType.PRODUCT_QUANTIZATION, searcher.getCompression().type);
        assertEquals(V5VectorPostingsWriter.Structure.ONE_TO_ONE, searcher.getPostingsStructure());

        // Confirm we can query the data
        var result = execute( "SELECT * FROM %s ORDER BY v ANN OF ? LIMIT 5", randomVectorBoxed(2));
        assertEquals(5, result.size());
    }

    @SuppressWarnings("resource")
    @Test
    public void testSaiVectorIndexPostingStructure()
    {
        createTable("CREATE TABLE %s (pk int, v vector<float, 2>, PRIMARY KEY(pk))");
        var indexName = createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");

        // We'll manage compaction manually for better control
        disableCompaction();

        // first sstable has one-to-one
        for (int i = 0; i < MIN_PQ_ROWS; i++)
            execute("INSERT INTO %s (pk, v) VALUES (?, ?)", i, randomVectorBoxed(2));

        flush();

        // second sstable has one-to-many
        for (int i = MIN_PQ_ROWS; i < MIN_PQ_ROWS * 3; i += 2)
        {
            var dupedVector = randomVectorBoxed(2);
            execute("INSERT INTO %s (pk, v) VALUES (?, ?)", i, dupedVector);
            execute("INSERT INTO %s (pk, v) VALUES (?, ?)", i + 1, dupedVector);
        }

        flush();

        // third sstable has zero or one-to-many
        for (int i = MIN_PQ_ROWS * 3; i < MIN_PQ_ROWS * 4; i += 2)
            execute("INSERT INTO %s (pk, v) VALUES (?, ?)", i, randomVectorBoxed(2));
        // this row doesn't get a vector, to force it to the zero or one to many case
        execute("INSERT INTO %s (pk) VALUES (?)", MIN_PQ_ROWS * 4);
        flush();

        // Confirm that V1MetadataOnlySearchableIndex has valid metadata.
        var metadataOnlyIndex = (StorageAttachedIndex) getIndexManager(keyspace(), indexName).listIndexes().iterator().next();
        var metadataOnlySSTableIndexes = metadataOnlyIndex.getIndexContext().getView().getIndexes();
        assertEquals(3, metadataOnlySSTableIndexes.size());

        var structures = new HashSet<V5VectorPostingsWriter.Structure>();
        metadataOnlySSTableIndexes.forEach(i -> {
            assertTrue(i.areSegmentsLoaded());
            structures.addAll(i.getPostingsStructures().collect(Collectors.toList()));
        });

        if (version.onOrAfter(Version.DC))
        {
            assertEquals(3, structures.size());
            assertTrue(structures.contains(V5VectorPostingsWriter.Structure.ONE_TO_ONE));
            assertTrue(structures.contains(V5VectorPostingsWriter.Structure.ONE_TO_MANY));
            assertTrue(structures.contains(V5VectorPostingsWriter.Structure.ZERO_OR_ONE_TO_MANY));
        }
        else
        {
            assertTrue(structures.isEmpty());
        }

        // Run compaction without disabling reads to test that path too
        compact();

        // Confirm what is happening now.
        var compactedSSTableIndexes = metadataOnlyIndex.getIndexContext().getView().getIndexes();
        assertEquals(1, compactedSSTableIndexes.size());
        compactedSSTableIndexes.forEach(i -> {
            assertTrue(i.areSegmentsLoaded());
            assertEquals(1, i.getSegmentMetadatas().size());
            var postingsStructures = i.getPostingsStructures().collect(Collectors.toList());
            if (version.onOrAfter(Version.DC))
            {
                assertEquals(1, postingsStructures.size());
                assertEquals(V5VectorPostingsWriter.Structure.ZERO_OR_ONE_TO_MANY, postingsStructures.get(0));
            }
            else
            {
                assertEquals(0, postingsStructures.size());
            }
        });

        // Confirm we can query the data
        var result = execute( "SELECT * FROM %s ORDER BY v ANN OF ? LIMIT 5", randomVectorBoxed(2));
        assertEquals(5, result.size());
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
                    v = randomVectorBoxed(2);
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
