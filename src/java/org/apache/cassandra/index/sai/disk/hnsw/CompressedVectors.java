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

package org.apache.cassandra.index.sai.disk.hnsw;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.index.sai.disk.hnsw.pq.ProductQuantization;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.lucene.index.VectorSimilarityFunction;

public class CompressedVectors
{
    public static final boolean DISABLE_INMEMORY_VECTORS = Boolean.parseBoolean(System.getProperty("cassandra.index.sai.vector_search.disable_inmemory_vectors", "false"));

    private final ProductQuantization pq;
    private List<byte[]> compressedVectors;

    private CompressedVectors(ProductQuantization pq, List<byte[]> compressedVectors) throws IOException
    {
        this.pq = pq;
        this.compressedVectors = compressedVectors;
    }

    public static CompressedVectors load(FileHandle fh, long offset) throws IOException
    {
        try (var in = fh.createReader())
        {
            in.seek(offset);
            if (in.read() == 0 || DISABLE_INMEMORY_VECTORS) {
                // there were too few vectors to bother compressiong
                return null;
            }

            // pq codebooks
            var pq = ProductQuantization.load(in);

            // read the vectors
            int size = in.readInt();
            var compressedVectors = new ArrayList<byte[]>(size);
            int compressedDimension = in.readInt();
            for (int i = 0; i < size; i++)
            {
                byte[] vector = new byte[compressedDimension];
                in.readFully(vector);
                compressedVectors.add(vector);
            }

            return new CompressedVectors(pq, compressedVectors);
        }
    }

    public float decodedSimilarity(int ordinal, float[] v, VectorSimilarityFunction similarityFunction)
    {
        switch (similarityFunction)
        {
            case DOT_PRODUCT:
                return (1 + pq.decodedDotProduct(compressedVectors.get(ordinal), v)) / 2;
            default:
                // VSTODO implement other similarity functions efficiently
                var decoded = new float[pq.vectorDimension()];
                pq.decode(compressedVectors.get(ordinal), decoded);
                return similarityFunction.compare(decoded, v);
        }
    }
}
