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

import static org.junit.Assert.assertTrue;

public class VectorSiftSmallTest extends VectorSiftSmallTester
{
    @Test
    public void testSiftSmall() throws Throwable
    {
        var baseVectors = readFvecs(String.format("test/data/%s/%s_base.fvecs", DATASET, DATASET));
        var queryVectors = readFvecs(String.format("test/data/%s/%s_query.fvecs", DATASET, DATASET));
        var groundTruth = readIvecs(String.format("test/data/%s/%s_groundtruth.ivecs", DATASET, DATASET));

        // Create table and index
        createTable();
        createIndex();

        insertVectors(baseVectors, 0);
        double memoryRecall = testRecall(100, queryVectors, groundTruth);
        assertTrue("Memory recall is " + memoryRecall, memoryRecall > 0.975);

        // Run a few queries with increasing rerank_k to validate that recall increases
        ensureIncreasingRerankKIncreasesRecall(queryVectors, groundTruth);

        // Run queries with and without pruning to validate recall improves
        ensureDisablingPruningIncreasesRecall(queryVectors, groundTruth);

        flush();
        var diskRecall = testRecall(100, queryVectors, groundTruth);
        assertTrue("Disk recall is " + diskRecall, diskRecall > 0.975);

        // Run a few queries with increasing rerank_k to validate that recall increases
        ensureIncreasingRerankKIncreasesRecall(queryVectors, groundTruth);
    }

    protected void ensureIncreasingRerankKIncreasesRecall(List<float[]> queryVectors, List<List<Integer>> groundTruth)
    {
        // Validate that the recall increases as we increase the rerank_k parameter
        double previousRecall = 0;
        int limit = 10;
        int strictlyIncreasedCount = 0;

        // First test with rerank_k = 0, which should have the worst recall
        double zeroRerankRecall = testRecall(limit, queryVectors, groundTruth, 0, null);

        // Testing shows that we achieve 100% recall at about rerank_k = 45, so no need to go higher
        for (int rerankK = limit; rerankK <= 50; rerankK += 5)
        {
            var recall = testRecall(limit, queryVectors, groundTruth, rerankK, null);
            // All recalls should be better than rerank_k = 0
            assertTrue("Recall for rerank_k = " + rerankK + " should be at least as good as with rerank_k = 0",
                       recall >= zeroRerankRecall);

            // Recall varies, so we can only assert that it does not get worse on a per-run basis. However, it should
            // get better strictly at least some of the time
            assertTrue("Recall for rerank_k = " + rerankK + " is " + recall, recall >= previousRecall);
            if (recall > previousRecall)
                strictlyIncreasedCount++;
            previousRecall = recall;
        }
        // This is a conservative assertion to prevent it from being too fragile. At the time of writing this test,
        // we observed a strict increase of 6 times for in memory and 5 times for on disk.
        assertTrue("Recall should have strictly increased at least 4 times but only increased " + strictlyIncreasedCount + " times",
                   strictlyIncreasedCount > 3);
    }

    protected void ensureDisablingPruningIncreasesRecall(List<float[]> queryVectors, List<List<Integer>> groundTruth)
    {
        int limit = 10;
        // Test with pruning enabled
        double recallWithPruning = testRecall(limit, queryVectors, groundTruth, null, true);

        // Test with pruning disabled
        double recallWithoutPruning = testRecall(limit, queryVectors, groundTruth, null, false);

        // Recall should be at least as good when pruning is disabled
        assertTrue("Recall without pruning (" + recallWithoutPruning +
                   ") should be at least as good as recall with pruning (" + recallWithPruning + ')',
                   recallWithoutPruning >= recallWithPruning);
    }
}
