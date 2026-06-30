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

import org.junit.Test;

import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.disk.v1.SegmentBuilder;
import org.apache.cassandra.index.sai.disk.v2.V2VectorIndexSearcher;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class VectorSiftSmallMultiSegmentBuildTest extends VectorSiftSmallTester
{
    // exercise the path where we use the PQ from the first segment (constructed on-heap)
    // to construct the others off-heap
    @Test
    public void testMultiSegmentBuild() throws Throwable
    {
        var baseVectors = readFvecs(String.format("test/data/%s/%s_base.fvecs", DATASET, DATASET));
        var queryVectors = readFvecs(String.format("test/data/%s/%s_query.fvecs", DATASET, DATASET));
        var groundTruth = readIvecs(String.format("test/data/%s/%s_groundtruth.ivecs", DATASET, DATASET));

        // Create table without index
        createTable();

        // we're going to compact manually, so disable background compactions to avoid interference
        disableCompaction();

        insertVectors(baseVectors, 0);
        // single big sstable before creating index
        flush();
        compact();

        SegmentBuilder.updateLastValidSegmentRowId(2000); // 2000 rows per segment, enough for PQ to be created
        createIndex();

        // verify that we got the expected number of segments and that PQ is present in all of them
        var sim = getCurrentColumnFamilyStore().getIndexManager();
        var index = (StorageAttachedIndex) sim.listIndexes().iterator().next();
        var searchableIndex = index.getIndexContext().getView().getIndexes().iterator().next();
        var segments = searchableIndex.getSegments();
        assertEquals(5, segments.size());
        for (int i = 0; i < 5; i++)
            assertNotNull(((V2VectorIndexSearcher) segments.get(0).getIndexSearcher()).getPQ());

        var recall = testRecall(100, queryVectors, groundTruth);
        assertTrue("Post-compaction recall is " + recall, recall > 0.975);
    }
}
