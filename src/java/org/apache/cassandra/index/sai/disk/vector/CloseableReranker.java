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

import io.github.jbellis.jvector.graph.similarity.ScoreFunction;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import org.apache.cassandra.io.util.FileUtils;

/**
 * A {@link NodeSimilarity.Reranker} that closes the underlying {@link VectorSupplier} when closed.
 */
public class CloseableReranker implements ScoreFunction.ExactScoreFunction, Closeable
{
    private final VectorSupplier vectorSupplier;
    private final ExactScoreFunction scoreFunction;

    public CloseableReranker(VectorSimilarityFunction similarityFunction, VectorFloat<?> queryVector, VectorSupplier supplier)
    {
        this.vectorSupplier = supplier;
        this.scoreFunction = supplier.getScoreFunction(queryVector, similarityFunction);
    }

    @Override
    public float similarityTo(int i)
    {
        return scoreFunction.similarityTo(i);
    }

    @Override
    public void close()
    {
        FileUtils.closeQuietly(vectorSupplier);
    }
}
