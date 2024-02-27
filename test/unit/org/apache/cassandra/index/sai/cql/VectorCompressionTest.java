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

import java.io.IOException;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.disk.v3.V3VectorIndexSearcher;
import org.apache.cassandra.index.sai.disk.vector.VectorCompression;
import org.apache.cassandra.index.sai.disk.vector.VectorSourceModel;

import static org.apache.cassandra.index.sai.disk.vector.VectorCompression.CompressionType.BINARY_QUANTIZATION;
import static org.apache.cassandra.index.sai.disk.vector.VectorCompression.CompressionType.NONE;
import static org.apache.cassandra.index.sai.disk.vector.VectorCompression.CompressionType.PRODUCT_QUANTIZATION;
import static org.junit.Assert.assertTrue;

public class VectorCompressionTest extends VectorTester
{
    @Test
    public void testAda002() throws IOException
    {
        // ADA002 is always 1536
        testOne(VectorSourceModel.ADA002, 1536, new VectorCompression(BINARY_QUANTIZATION, 1536 / 8));
    }

    @Test
    public void testGecko() throws IOException
    {
        // GECKO is always 768
        testOne(VectorSourceModel.GECKO, 768, new VectorCompression(PRODUCT_QUANTIZATION, 768 / 4));
    }

    @Test
    public void testOpenAiV3Large() throws IOException
    {
        // V3_LARGE can be truncated
        for (int i = 1; i < 3; i++)
        {
            int D = 3072 / i;
            testOne(VectorSourceModel.OPENAI_V3_LARGE, D, new VectorCompression(PRODUCT_QUANTIZATION, D / 16));
        }
    }

    @Test
    public void testOpenAiV3Small() throws IOException
    {
        // V3_SMALL can be truncated
        for (int i = 1; i < 3; i++)
        {
            int D = 1536 / i;
            testOne(VectorSourceModel.OPENAI_V3_SMALL, D, new VectorCompression(PRODUCT_QUANTIZATION, D / 16));
        }
    }

    @Test
    public void testBert() throws IOException
    {
        // BERT is more of a family than a specific model
        for (int i : List.of(128, 256, 512, 1024))
        {
            testOne(VectorSourceModel.BERT, i, new VectorCompression(PRODUCT_QUANTIZATION, i / 4));
        }
    }

    @Test
    public void testOther() throws IOException
    {
        // Glove
        testOne(VectorSourceModel.OTHER, 25, new VectorCompression(PRODUCT_QUANTIZATION, 25));
        testOne(VectorSourceModel.OTHER, 50, new VectorCompression(PRODUCT_QUANTIZATION, 32));
        testOne(VectorSourceModel.OTHER, 100, new VectorCompression(PRODUCT_QUANTIZATION, 50));
        testOne(VectorSourceModel.OTHER, 200, new VectorCompression(PRODUCT_QUANTIZATION, 100));
        // Ada002
        testOne(VectorSourceModel.OTHER, 1536, new VectorCompression(BINARY_QUANTIZATION, 1536 / 8));
        // Something unknown and large
        testOne(VectorSourceModel.OTHER, 2000, new VectorCompression(PRODUCT_QUANTIZATION, 2000 / 8));
    }

    @Test
    public void testFewRows() throws IOException
    {
        testOne(1, VectorSourceModel.OTHER, 200, new VectorCompression(NONE, 200 * 8));
        // BQ still works with tiny dataset
        testOne(1, VectorSourceModel.ADA002, 1536, new VectorCompression(BINARY_QUANTIZATION, 1536 / 8));
    }

    private void testOne(VectorSourceModel model, int originalDimension, VectorCompression expectedCompression) throws IOException
    {
        testOne(1024, model, originalDimension, expectedCompression);
    }

    private void testOne(int rows, VectorSourceModel model, int originalDimension, VectorCompression expectedCompression) throws IOException
    {
        createTable("CREATE TABLE %s " + String.format("(pk int, v vector<float, %d>, PRIMARY KEY(pk))", originalDimension));
        createIndex("CREATE CUSTOM INDEX ON %s(v) " + String.format("USING 'StorageAttachedIndex' WITH OPTIONS = {'source_model': '%s'}", model));
        waitForIndexQueryable();

        for (int i = 0; i < rows; i++)
            execute("INSERT INTO %s (pk, v) VALUES (?, ?)", i, randomVector(originalDimension));
        flush();
        // the larger models may flush mid-test automatically, so compact to make sure that we
        // end up with a single sstable (otherwise PQ might conclude there aren't enough vectors to train on)
        compact();
        waitForCompactionsFinished();

        // get a View of the sstables that contain indexed data
        var sim = getCurrentColumnFamilyStore().indexManager;
        var index = (StorageAttachedIndex) sim.listIndexes().iterator().next();
        var view = index.getIndexContext().getView();

        // there should be one sstable with one segment
        assert view.size() == 1 : "Expected a single sstable after compaction";
        var ssti = view.iterator().next();
        var segments = ssti.getSegments();
        assert segments.size() == 1 : "Expected a single segment";

        // open a Searcher for the segment so we can check that its compression is what we expected
        try (var segment = segments.iterator().next();
             var searcher = (V3VectorIndexSearcher) segment.getIndexSearcher())
        {
            var cv = searcher.getCompressedVectors();
            var msg = String.format("Expected %s but got %s", expectedCompression,
                                    cv == null ? "NONE" : cv.getClass().getSimpleName() + '@' + cv.getCompressedSize());
            assertTrue(msg, expectedCompression.matches(cv));
        }
    }
}
