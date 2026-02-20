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

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import org.junit.Test;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.disk.v1.SegmentBuilder;
import org.apache.cassandra.index.sai.disk.v2.V2VectorIndexSearcher;
import org.apache.cassandra.index.sai.disk.vector.CompactionGraph;
import org.apache.cassandra.index.sai.disk.vector.JVectorVersionUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class VectorSiftSmallTest extends VectorTester.Versioned
{
    private static final String DATASET = "siftsmall"; // change to "sift" for larger dataset. requires manual download

    @Override
    public void setup() throws Throwable
    {
        super.setup();
    }

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

    private void ensureIncreasingRerankKIncreasesRecall(List<float[]> queryVectors, List<List<Integer>> groundTruth)
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

    private void ensureDisablingPruningIncreasesRecall(List<float[]> queryVectors, List<List<Integer>> groundTruth)
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

    public static ArrayList<float[]> readFvecs(String filePath) throws IOException
    {
        var vectors = new ArrayList<float[]>();
        try (var dis = new DataInputStream(new BufferedInputStream(new FileInputStream(filePath))))
        {
            while (dis.available() > 0)
            {
                var dimension = Integer.reverseBytes(dis.readInt());
                assert dimension > 0 : dimension;
                var buffer = new byte[dimension * Float.BYTES];
                dis.readFully(buffer);
                var byteBuffer = ByteBuffer.wrap(buffer).order(ByteOrder.LITTLE_ENDIAN);

                var vector = new float[dimension];
                for (var i = 0; i < dimension; i++)
                {
                    vector[i] = byteBuffer.getFloat();
                }
                vectors.add(vector);
            }
        }
        return vectors;
    }

    private static ArrayList<List<Integer>> readIvecs(String filename)
    {
        var groundTruthTopK = new ArrayList<List<Integer>>();

        try (var dis = new DataInputStream(new FileInputStream(filename)))
        {
            while (dis.available() > 0)
            {
                var numNeighbors = Integer.reverseBytes(dis.readInt());
                List<Integer> neighbors = new ArrayList<>(numNeighbors);

                for (var i = 0; i < numNeighbors; i++)
                {
                    var neighbor = Integer.reverseBytes(dis.readInt());
                    neighbors.add(neighbor);
                }

                groundTruthTopK.add(neighbors);
            }
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }

        return groundTruthTopK;
    }

    public double testRecall(int topK, List<float[]> queryVectors, List<List<Integer>> groundTruth)
    {
        return testRecall(topK, queryVectors, groundTruth, null, null);
    }

    public double testRecall(int topK, List<float[]> queryVectors, List<List<Integer>> groundTruth, Integer rerankK, Boolean usePruning)
    {
        AtomicInteger topKfound = new AtomicInteger(0);

        // Perform query and compute recall
        var stream = IntStream.range(0, queryVectors.size()).parallel();
        stream.forEach(i -> {
            float[] queryVector = queryVectors.get(i);
            String queryVectorAsString = Arrays.toString(queryVector);

            try {
                StringBuilder query = new StringBuilder()
                    .append("SELECT pk FROM %s ORDER BY val ANN OF ")
                    .append(queryVectorAsString)
                    .append(" LIMIT ")
                    .append(topK);

                if (rerankK != null || usePruning != null)
                {
                    query.append(" WITH ann_options = {");
                    List<String> options = new ArrayList<>();

                    if (rerankK != null)
                        options.add("'rerank_k': " + rerankK);

                    if (usePruning != null)
                        options.add("'use_pruning': " + usePruning);

                    query.append(String.join(", ", options));
                    query.append('}');
                }

                UntypedResultSet result = execute(query.toString());
                var gt = groundTruth.get(i);
                assert topK <= gt.size();
                // we don't care about order within the topK but we do need to restrict the size first
                var gtSet = new HashSet<>(gt.subList(0, topK));

                int n = (int)result.stream().filter(row -> gtSet.contains(row.getInt("pk"))).count();
                topKfound.addAndGet(n);
            } catch (Throwable throwable) {
                throw new RuntimeException(throwable);
            }
        });

        return (double) topKfound.get() / (queryVectors.size() * topK);
    }

    private void createTable()
    {
        createTable("CREATE TABLE %s (pk int, val vector<float, 128>, PRIMARY KEY(pk))");
    }

    private void createIndex()
    {
        // we need a long timeout because we are adding many vectors
        String index = createIndexAsync("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function' : 'euclidean', 'enable_hierarchy': 'true'}");
        waitForIndexQueryable(KEYSPACE, index, 5, TimeUnit.MINUTES);
    }

    private void insertVectors(List<float[]> vectors, int baseRowId)
    {
        IntStream.range(0, vectors.size()).parallel().forEach(i -> {
            float[] arrayVector = vectors.get(i);
            try {
                execute("INSERT INTO %s (pk, val) VALUES (?, ?)", baseRowId + i, vector(arrayVector));
            } catch (Throwable throwable) {
                throw new RuntimeException(throwable);
            }
        });
    }
}
