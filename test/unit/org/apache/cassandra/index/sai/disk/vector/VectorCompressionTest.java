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

package org.apache.cassandra.index.sai.disk.vector;

import java.io.IOException;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.cql.VectorTester;
import org.apache.cassandra.index.sai.disk.v3.V3VectorIndexSearcher;

import static org.apache.cassandra.index.sai.disk.vector.VectorCompression.CompressionType.NONE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class VectorCompressionTest extends VectorTester
{
    @Test
    public void testAda002() throws IOException
    {
        // ADA002 is always 1536
        testOne(VectorSourceModel.ADA002, 1536, VectorSourceModel.ADA002.compressionProvider.apply(1536));
    }

    @Test
    public void testGecko() throws IOException
    {
        // GECKO is always 768
        testOne(VectorSourceModel.GECKO, 768, VectorSourceModel.GECKO.compressionProvider.apply(768));
    }

    @Test
    public void testOpenAiV3Large() throws IOException
    {
        // V3_LARGE can be truncated
        for (int i = 1; i < 3; i++)
        {
            int D = 3072 / i;
            testOne(VectorSourceModel.OPENAI_V3_LARGE, D, VectorSourceModel.OPENAI_V3_LARGE.compressionProvider.apply(D));
        }
    }

    @Test
    public void testOpenAiV3Small() throws IOException
    {
        // V3_SMALL can be truncated
        for (int i = 1; i < 3; i++)
        {
            int D = 1536 / i;
            testOne(VectorSourceModel.OPENAI_V3_SMALL, D, VectorSourceModel.OPENAI_V3_SMALL.compressionProvider.apply(D));
        }
    }

    @Test
    public void testBert() throws IOException
    {
        // BERT is more of a family than a specific model
        for (int dimension : List.of(128, 256, 512, 1024))
        {
            testOne(VectorSourceModel.BERT, dimension, VectorSourceModel.BERT.compressionProvider.apply(dimension));
        }
    }

    @Test
    public void testNV_QA_4() throws IOException
    {
        // NV_QA_4 is anecdotally 1024 based on reviewing https://build.nvidia.com/nvidia/embed-qa-4. Couldn't
        // find supporting documentation for this number, though.
        testOne(VectorSourceModel.NV_QA_4, 1024, VectorSourceModel.NV_QA_4.compressionProvider.apply(1024));
    }

    @Test
    public void testOther() throws IOException
    {
        // 25..200 -> Glove dimensions
        // 1536 -> Ada002
        // 2000 -> something unknown and large
        for (int dimension : List.of(25, 50, 100, 200, 1536, 2000))
            testOne(VectorSourceModel.OTHER, dimension, VectorSourceModel.OTHER.compressionProvider.apply(dimension));
    }

    @Test
    public void testFewRows() throws IOException
    {
        // with fewer than MIN_PQ_ROWS we expect to observe no compression no matter
        // what the source model would prefer
        testOne(1, VectorSourceModel.OTHER, 200, VectorCompression.NO_COMPRESSION);
    }

    private void testOne(VectorSourceModel model, int originalDimension, VectorCompression expectedCompression) throws IOException
    {
        testOne(CassandraOnHeapGraph.MIN_PQ_ROWS, model, originalDimension, expectedCompression);
    }

    private void testOne(int rows, VectorSourceModel model, int originalDimension, VectorCompression expectedCompression) throws IOException
    {
        createTable("CREATE TABLE %s " + String.format("(pk int, v vector<float, %d>, PRIMARY KEY(pk))", originalDimension));

        for (int i = 0; i < rows; i++)
            execute("INSERT INTO %s (pk, v) VALUES (?, ?)", i, randomVectorBoxed(originalDimension));
        flush();
        // the larger models may flush mid-test automatically, so compact to make sure that we
        // end up with a single sstable (otherwise PQ might conclude there aren't enough vectors to train on)
        compact();
        waitForCompactionsFinished();

        // create index after compaction so we don't have to wait for it to (potentially) build twice
        createIndex("CREATE CUSTOM INDEX ON %s(v) " + String.format("USING 'StorageAttachedIndex' WITH OPTIONS = {'source_model': '%s'}", model));
        waitForIndexQueryable();

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
            var vc = searcher.getCompression();
            var msg = String.format("Expected %s but got %s", expectedCompression, vc);
            assertEquals(msg, expectedCompression, vc);
            if (vc.type != NONE)
            {
                assertEquals((int) (100 * VectorSourceModel.tapered2x(100) * model.overqueryProvider.apply(vc)),
                             model.rerankKFor(100, vc));
            }
        }
    }
}
