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

import org.junit.Ignore;

import org.apache.cassandra.cql3.UntypedResultSet;

@Ignore
public abstract class VectorSiftSmallTester extends VectorTester.Versioned
{
    protected static final String DATASET = "siftsmall"; // change to "sift" for larger dataset. requires manual download

    @Override
    public void setup() throws Throwable
    {
        super.setup();
    }

    protected static ArrayList<float[]> readFvecs(String filePath) throws IOException
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

    protected static ArrayList<List<Integer>> readIvecs(String filename)
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

    protected double testRecall(int topK, List<float[]> queryVectors, List<List<Integer>> groundTruth)
    {
        return testRecall(topK, queryVectors, groundTruth, null, null);
    }

    protected double testRecall(int topK, List<float[]> queryVectors, List<List<Integer>> groundTruth, Integer rerankK, Boolean usePruning)
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

    protected void createTable()
    {
        createTable("CREATE TABLE %s (pk int, val vector<float, 128>, PRIMARY KEY(pk))");
    }

    protected void createIndex()
    {
        // we need a long timeout because we are adding many vectors
        String index = createIndexAsync("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function' : 'euclidean', 'enable_hierarchy': 'true'}");
        waitForIndexQueryable(KEYSPACE, index, 5, TimeUnit.MINUTES);
    }

    protected void insertVectors(List<float[]> vectors, int baseRowId)
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
