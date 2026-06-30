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

import java.util.Arrays;

import org.junit.Test;

import org.apache.cassandra.cql3.UntypedResultSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class VectorSiftSmallRerankKZeroOrderMatchesFullPrecisionSimilarityTest extends VectorSiftSmallTester
{

    // Note: test only fails when scores are sent from replica to coordinator.
    @Test
    public void testRerankKZeroOrderMatchesFullPrecisionSimilarity() throws Throwable
    {
        var baseVectors = readFvecs(String.format("test/data/%s/%s_base.fvecs", DATASET, DATASET));
        var queryVectors = readFvecs(String.format("test/data/%s/%s_query.fvecs", DATASET, DATASET));

        // Create table and index
        createTable();
        createIndex();

        // Flush because in memory index uses FP vectors, therefore ignoring rerank_k = 0
        insertVectors(baseVectors, 0);
        flush();

        // Test with a subset of query vectors to keep test runtime reasonable, but query with a high limit to
        // increase probability for incorrect ordering
        int numQueriesToTest = 10;
        int limit = 100;

        for (int queryIdx = 0; queryIdx < numQueriesToTest; queryIdx++)
        {
            float[] queryVector = queryVectors.get(queryIdx);
            String queryVectorAsString = Arrays.toString(queryVector);

            // Execute query with rerank_k = 0 and get the similarity scores computed by the coordinator
            String query = String.format("SELECT pk, similarity_euclidean(val, %s) as similarity FROM %%s ORDER BY val ANN OF %s LIMIT %d WITH ann_options = {'rerank_k': 0}",
                                         queryVectorAsString, queryVectorAsString, limit);
            UntypedResultSet result = execute(query);

            // Verify that results are in descending order of similarity score
            // (Euclidean similarity is 1.0 / (1.0 + distance²), so higher score = more similar)
            float lastSimilarity = Float.MAX_VALUE;
            assertEquals(limit, result.size());
            for (UntypedResultSet.Row row : result)
            {
                float similarity = row.getFloat("similarity");
                assertTrue(String.format("Query %d: Similarity scores should be in descending order (higher score = more similar). " +
                                         "Previous: %.10f, Current: %.10f",
                                         queryIdx, lastSimilarity, similarity),
                           similarity <= lastSimilarity);
                lastSimilarity = similarity;
            }
        }
    }
}
