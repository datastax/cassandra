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
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import io.github.jbellis.jvector.quantization.ASHVectors;
import io.github.jbellis.jvector.quantization.AsymmetricHashing;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.disk.v2.V2VectorIndexSearcher;
import org.apache.cassandra.index.sai.disk.vector.CassandraOnHeapGraph;
import org.apache.cassandra.index.sai.disk.vector.VectorCompression;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Indexes with {@code enable_ash}: their memtable indexes build and search with ASH codes, flushes and compactions
 * write ASH codes, and new memtable indexes start with the newest flushed segment's model.
 */
@RunWith(Parameterized.class)
public class VectorAshTest extends VectorTester.VersionedWithChecksums
{
    @Test
    public void enableAshOptionTest()
    {
        // enable_ash is accepted as an index option and read into the index config
        createTable("CREATE TABLE %s (pk int, vec vector<float, 4>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(vec) USING 'StorageAttachedIndex' " +
                    "WITH OPTIONS = {'enable_ash' : 'true', 'similarity_function' : 'dot_product'}");

        StorageAttachedIndex saiIndex =
        (StorageAttachedIndex) getCurrentColumnFamilyStore().indexManager.listIndexes().iterator().next();
        assertTrue(saiIndex.getIndexContext().getIndexWriterConfig().isAshEnabled());
    }

    @Test
    public void enableAshIndexFindsStoredVectors()
    {
        // With enable_ash the memtable index builds and searches with ASH codes and reranks with the stored vectors;
        // a flushed segment searches with the ASH codes written at flush. Random unit vectors of 64 dimensions are
        // nearly orthogonal, so a stored vector is its own nearest neighbor; graph search on random vectors misses
        // a few even with exact scores, so nearly all, not every one, must find themselves among their 10 nearest,
        // before and after the memtable is flushed.
        createTable("CREATE TABLE %s (pk int, vec vector<float, 64>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(vec) USING 'StorageAttachedIndex' " +
                    "WITH OPTIONS = {'enable_ash' : 'true', 'similarity_function' : 'dot_product'}");

        List<float[]> vectors = new ArrayList<>();
        for (int i = 0; i < 2000; i++)
        {
            float[] v = randomVector(64);
            normalize(v);
            vectors.add(v);
            execute("INSERT INTO %s (pk, vec) VALUES (?, ?)", i, vector(v));
        }

        assertThat(fractionFindingThemselves(vectors)).isGreaterThanOrEqualTo(0.9);
        flush();
        var sai = (StorageAttachedIndex) getCurrentColumnFamilyStore().indexManager.listIndexes().iterator().next();
        for (var index : sai.getIndexContext().getView().getIndexes())
            for (var segment : index.getSegments())
                assertEquals(VectorCompression.CompressionType.ASH, ((V2VectorIndexSearcher) segment.getIndexSearcher()).getCompression().type);
        assertThat(fractionFindingThemselves(vectors)).isGreaterThanOrEqualTo(0.9);
    }

    @Test
    public void enableAshMemtableStartsWithFlushedModel()
    {
        // A new memtable index of an enable_ash index starts with the ASH model of the newest flushed segment, so only
        // an index with no flushed segment starts with exact scores.
        createTable("CREATE TABLE %s (pk int, vec vector<float, 64>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(vec) USING 'StorageAttachedIndex' " +
                    "WITH OPTIONS = {'enable_ash' : 'true', 'similarity_function' : 'dot_product'}");
        var sai = (StorageAttachedIndex) getCurrentColumnFamilyStore().indexManager.listIndexes().iterator().next();
        var context = sai.getIndexContext();
        assertNull(new CassandraOnHeapGraph<>(context, true, null).ashModel());

        List<float[]> vectors = new ArrayList<>();
        for (int i = 0; i < 2000; i++)
        {
            float[] v = randomVector(64);
            normalize(v);
            vectors.add(v);
            execute("INSERT INTO %s (pk, vec) VALUES (?, ?)", i, vector(v));
        }
        flush();
        // A flush can write several sstables, each segment with its own model; the new index takes one of them.
        var segmentModels = new ArrayList<AsymmetricHashing>();
        for (var index : context.getView().getIndexes())
            for (var segment : index.getSegments())
                segmentModels.add(((ASHVectors) ((V2VectorIndexSearcher) segment.getIndexSearcher()).graph.getCompressedVectors()).getCompressor());
        var inherited = new CassandraOnHeapGraph<>(context, true, null).ashModel();
        assertTrue(segmentModels.stream().anyMatch(m -> m == inherited));

        // The next memtable encodes with that model from its first vector, and searches find its vectors.
        for (int i = 2000; i < 2200; i++)
        {
            float[] v = randomVector(64);
            normalize(v);
            vectors.add(v);
            execute("INSERT INTO %s (pk, vec) VALUES (?, ?)", i, vector(v));
        }
        assertThat(fractionFindingThemselves(vectors)).isGreaterThanOrEqualTo(0.9);
    }

    @Test
    public void enableAshCompactionWritesAshCodes()
    {
        // Compacting segments of an enable_ash index builds the new segment with the inherited ASH model, so it
        // searches with ASH codes too.
        createTable("CREATE TABLE %s (pk int, vec vector<float, 64>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(vec) USING 'StorageAttachedIndex' " +
                    "WITH OPTIONS = {'enable_ash' : 'true', 'similarity_function' : 'dot_product'}");
        disableCompaction();

        List<float[]> vectors = new ArrayList<>();
        for (int i = 0; i < 4000; i++)
        {
            float[] v = randomVector(64);
            normalize(v);
            vectors.add(v);
            execute("INSERT INTO %s (pk, vec) VALUES (?, ?)", i, vector(v));
            if (i == 1999)
                flush();
        }
        flush();
        compact();

        var sai = (StorageAttachedIndex) getCurrentColumnFamilyStore().indexManager.listIndexes().iterator().next();
        var indexes = sai.getIndexContext().getView().getIndexes();
        assertEquals(1, indexes.size());
        for (var index : indexes)
            for (var segment : index.getSegments())
                assertEquals(VectorCompression.CompressionType.ASH, ((V2VectorIndexSearcher) segment.getIndexSearcher()).getCompression().type);
        assertThat(fractionFindingThemselves(vectors)).isGreaterThanOrEqualTo(0.9);
    }

    @Test
    public void ashDimensionsSetsTheCodeSize()
    {
        // ash_dimensions sets the projected dimensions of the ASH codes, which flushed segments keep.
        createTable("CREATE TABLE %s (pk int, vec vector<float, 64>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(vec) USING 'StorageAttachedIndex' " +
                    "WITH OPTIONS = {'enable_ash' : 'true', 'ash_dimensions' : '48', 'similarity_function' : 'dot_product'}");

        List<float[]> vectors = new ArrayList<>();
        for (int i = 0; i < 2000; i++)
        {
            float[] v = randomVector(64);
            normalize(v);
            vectors.add(v);
            execute("INSERT INTO %s (pk, vec) VALUES (?, ?)", i, vector(v));
        }
        flush();
        var sai = (StorageAttachedIndex) getCurrentColumnFamilyStore().indexManager.listIndexes().iterator().next();
        for (var index : sai.getIndexContext().getView().getIndexes())
            for (var segment : index.getSegments())
                assertEquals(48, ((ASHVectors) ((V2VectorIndexSearcher) segment.getIndexSearcher()).graph.getCompressedVectors()).getCompressor().quantizedDim);
        assertThat(fractionFindingThemselves(vectors)).isGreaterThanOrEqualTo(0.9);
    }

    private double fractionFindingThemselves(List<float[]> vectors)
    {
        int found = 0, queries = 0;
        for (int i = 0; i < vectors.size(); i += 10, queries++)
        {
            for (var row : execute("SELECT pk FROM %s ORDER BY vec ANN OF ? LIMIT 10", vector(vectors.get(i))))
            {
                if (row.getInt("pk") == i)
                {
                    found++;
                    break;
                }
            }
        }
        return (double) found / queries;
    }
}
