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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import org.junit.Test;

import org.apache.cassandra.cql3.UntypedResultSet;

import static org.junit.Assert.assertTrue;

public class VectorSiftSmallTest extends VectorTester
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
        createTable(KEYSPACE, "CREATE TABLE %s (pk int, val vector<float, 128>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function' : 'euclidean'}");
        waitForIndexQueryable();

        insertVectors(baseVectors, 0);
        double memoryRecall = testRecall(queryVectors, groundTruth);
        assertTrue("Memory recall is " + memoryRecall, memoryRecall > 0.975);

        flush();
        var diskRecall = testRecall(queryVectors, groundTruth);
        assertTrue("Disk recall is " + diskRecall, diskRecall > 0.975);
    }

    @Test
    public void testCompaction() throws Throwable
    {
        var baseVectors = readFvecs(String.format("test/data/%s/%s_base.fvecs", DATASET, DATASET));
        var queryVectors = readFvecs(String.format("test/data/%s/%s_query.fvecs", DATASET, DATASET));
        var groundTruth = readIvecs(String.format("test/data/%s/%s_groundtruth.ivecs", DATASET, DATASET));

        // Create table and index
        createTable(KEYSPACE, "CREATE TABLE %s (pk int, val vector<float, 128>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function' : 'euclidean'}");
        waitForIndexQueryable();

        // we're going to compact manually, so disable background compactions to avoid interference
        disableCompaction();

        int segments = 5;
        int vectorsPerSegment = baseVectors.size() / segments;
        assert baseVectors.size() % vectorsPerSegment == 0; // simplifies split logic
        for (int i = 0; i < segments; i++)
        {
            insertVectors(baseVectors.subList(i * vectorsPerSegment, (i + 1) * vectorsPerSegment), i * vectorsPerSegment);
            flush();
        }
        double memoryRecall = testRecall(queryVectors, groundTruth);
        assertTrue("Pre-compaction recall is " + memoryRecall, memoryRecall > 0.975);

        compact();
        var diskRecall = testRecall(queryVectors, groundTruth);
        assertTrue("Post-compaction recall is " + diskRecall, diskRecall > 0.975);
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

    private static ArrayList<HashSet<Integer>> readIvecs(String filename)
    {
        var groundTruthTopK = new ArrayList<HashSet<Integer>>();

        try (var dis = new DataInputStream(new FileInputStream(filename)))
        {
            while (dis.available() > 0)
            {
                var numNeighbors = Integer.reverseBytes(dis.readInt());
                var neighbors = new HashSet<Integer>(numNeighbors);

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

    public double testRecall(List<float[]> queryVectors, List<HashSet<Integer>> groundTruth)
    {
        AtomicInteger topKfound = new AtomicInteger(0);
        int topK = 100;

        // Perform query and compute recall
        var stream = IntStream.range(0, queryVectors.size()).parallel();
        stream.forEach(i -> {
            float[] queryVector = queryVectors.get(i);
            String queryVectorAsString = Arrays.toString(queryVector);

            try {
                UntypedResultSet result = execute("SELECT pk FROM %s ORDER BY val ANN OF " + queryVectorAsString + " LIMIT " + topK);
                var gt = groundTruth.get(i);

                int n = (int)result.stream().filter(row -> gt.contains(row.getInt("pk"))).count();
                topKfound.addAndGet(n);
            } catch (Throwable throwable) {
                throw new RuntimeException(throwable);
            }
        });

        return (double) topKfound.get() / (queryVectors.size() * topK);
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
