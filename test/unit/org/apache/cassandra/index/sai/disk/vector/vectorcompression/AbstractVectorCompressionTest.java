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

package org.apache.cassandra.index.sai.disk.vector.vectorcompression;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.cql.VectorTester;
import org.apache.cassandra.index.sai.disk.v2.V2VectorIndexSearcher;
import org.apache.cassandra.index.sai.disk.vector.CassandraOnHeapGraph;
import org.apache.cassandra.index.sai.disk.vector.VectorCompression;
import org.apache.cassandra.index.sai.disk.vector.VectorSourceModel;
import org.assertj.core.api.Assertions;
import org.junit.Ignore;

import static org.apache.cassandra.index.sai.disk.vector.VectorCompression.CompressionType.NONE;
import static org.junit.Assert.assertEquals;

@Ignore
public abstract class AbstractVectorCompressionTest extends VectorTester
{
    protected void testOne(VectorSourceModel model, int originalDimension, VectorCompression expectedCompression) throws IOException
    {
        testOne(CassandraOnHeapGraph.MIN_PQ_ROWS, model, originalDimension, expectedCompression);
    }

    protected void testOne(int rows, VectorSourceModel model, int originalDimension, VectorCompression expectedCompression) throws IOException
    {
        createTable(String.format("CREATE TABLE %%s (pk int, v vector<float, %d>, PRIMARY KEY(pk)) " +
                                  "WITH compaction = {'class': 'UnifiedCompactionStrategy', 'num_shards': 1, 'enabled': false}",
                                  originalDimension));

        for (int i = 0; i < rows; i++)
            execute("INSERT INTO %s (pk, v) VALUES (?, ?)", i, randomVectorBoxed(originalDimension));
        flush();
        // the larger models may flush mid-test automatically, so compact to make sure that we
        // end up with a single sstable (otherwise PQ might conclude there aren't enough vectors to train on)
        compact();
        waitForCompactionsFinished();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        Assertions.assertThat(cfs.getLiveSSTables()).hasSize(1); // Expected a single sstable after compaction

        // create index after compaction, so we don't have to wait for it to (potentially) build twice,
        // and give it extra time to build large models
        String indexName = createIndexAsync(String.format("CREATE CUSTOM INDEX ON %%s(v) USING 'StorageAttachedIndex'" +
                                                          " WITH OPTIONS = {'source_model': '%s'}", model));
        waitForIndexQueryable(KEYSPACE, indexName, 5, TimeUnit.MINUTES);

        // get a View of the sstables that contain indexed data
        var index = (StorageAttachedIndex) cfs.indexManager.getIndexByName(indexName);
        var view = index.getIndexContext().getView();

        // there should be one sstable with one segment
        Assertions.assertThat(view).hasSize(1); // Expected a single sstable after compaction
        var ssti = view.iterator().next();
        var segments = ssti.getSegments();
        Assertions.assertThat(segments).hasSize(1); // Expected a single segment

        // open a Searcher for the segment, so we can check that its compression is what we expected
        try (var segment = segments.iterator().next();
             var searcher = (V2VectorIndexSearcher) segment.getIndexSearcher())
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

// Made with Bob
