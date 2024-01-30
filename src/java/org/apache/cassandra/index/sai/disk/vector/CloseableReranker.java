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

import java.io.Closeable;

import io.github.jbellis.jvector.graph.NodeSimilarity;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import org.apache.cassandra.io.util.FileUtils;

/**
 * A {@link NodeSimilarity.Reranker} that closes the underlying {@link VectorSupplier} when closed.
 */
public class CloseableReranker implements NodeSimilarity.Reranker, Closeable
{
    private final VectorSimilarityFunction similarityFunction;
    private final float[] queryVector;
    private final VectorSupplier vectorSupplier;

    public CloseableReranker(VectorSimilarityFunction similarityFunction, float[] queryVector, VectorSupplier view)
    {
        this.similarityFunction = similarityFunction;
        this.queryVector = queryVector;
        this.vectorSupplier = view;
    }

    @Override
    public float similarityTo(int i)
    {
        return similarityFunction.compare(queryVector, vectorSupplier.getVectorForOrdinal(i));
    }

    @Override
    public void close()
    {
        FileUtils.closeQuietly(vectorSupplier);
    }
}
