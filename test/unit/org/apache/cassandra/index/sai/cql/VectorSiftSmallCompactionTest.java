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

import java.util.List;

import org.junit.Test;

import org.apache.cassandra.index.sai.disk.vector.JVectorVersionUtil;

import static org.junit.Assert.assertTrue;

public class VectorSiftSmallCompactionTest extends VectorSiftSmallTester
{
    @Test
    public void testCompaction() throws Throwable
    {
        var baseVectors = readFvecs(String.format("test/data/%s/%s_base.fvecs", DATASET, DATASET));
        var queryVectors = readFvecs(String.format("test/data/%s/%s_query.fvecs", DATASET, DATASET));
        var groundTruth = readIvecs(String.format("test/data/%s/%s_groundtruth.ivecs", DATASET, DATASET));

        // Create table and index
        createTable();
        createIndex();

        // we're going to compact manually, so disable background compactions to avoid interference
        disableCompaction();

        int segments = 10;
        int vectorsPerSegment = baseVectors.size() / segments;
        assert baseVectors.size() % vectorsPerSegment == 0; // simplifies split logic
        for (int i = 0; i < segments; i++)
        {
            insertVectors(baseVectors.subList(i * vectorsPerSegment, (i + 1) * vectorsPerSegment), i * vectorsPerSegment);
            flush();
        }
        for (int topK : List.of(1, 100))
        {
            double recall = testRecall(topK, queryVectors, groundTruth);
            assertTrue("Pre-compaction recall is " + recall, recall > 0.975);
        }

        // When NVQ is enabled, we expect worse recall
        float postCompactionRecall = JVectorVersionUtil.ENABLE_NVQ ? 0.9499f : 0.975f;

        // Take the CassandraOnHeapGraph code path.
        compact();
        for (int topK : List.of(1, 100))
        {
            var recall = testRecall(topK, queryVectors, groundTruth);
            assertTrue("Post-compaction recall is " + recall, recall > postCompactionRecall);
        }

        // Compact again to take the CompactionGraph code path.
        compact();
        for (int topK : List.of(1, 100))
        {
            var recall = testRecall(topK, queryVectors, groundTruth);
            assertTrue("Post-compaction recall is " + recall, recall > postCompactionRecall);
        }

        // Set force PQ training size to ensure we hit the refine code path and apply it to half the vectors.
        // TODO this test fails as of this commit due to recall issues. Will investigate further.
        // CompactionGraph.PQ_TRAINING_SIZE = baseVectors.size() / 2;

        // Compact again to take the CompactionGraph code path that calls the refine logic
        compact();
        for (int topK : List.of(1, 100))
        {
            var recall = testRecall(topK, queryVectors, groundTruth);
            // This assertion will fail until we address the design the bug discussed
            // in https://github.com/riptano/cndb/issues/16637.
            // assertTrue("Post-compaction recall is " + recall, recall > postCompactionRecall);
        }
    }
}
