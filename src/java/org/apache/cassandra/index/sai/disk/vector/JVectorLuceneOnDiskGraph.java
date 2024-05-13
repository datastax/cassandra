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
import java.io.IOException;
import java.util.function.IntConsumer;

import org.slf4j.Logger;

import io.github.jbellis.jvector.graph.similarity.ScoreFunction;
import io.github.jbellis.jvector.pq.CompressedVectors;
import io.github.jbellis.jvector.util.Bits;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.disk.v1.PerIndexFiles;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.utils.RowIdWithScore;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.CloseableIterator;

/**
 * A common interface between Lucene and JVector graph indexes
 */
public abstract class JVectorLuceneOnDiskGraph implements AutoCloseable
{
    private static final Logger logger = org.slf4j.LoggerFactory.getLogger(JVectorLuceneOnDiskGraph.class);

    protected final PerIndexFiles indexFiles;
    protected final SegmentMetadata.ComponentMetadataMap componentMetadatas;

    protected JVectorLuceneOnDiskGraph(SegmentMetadata.ComponentMetadataMap componentMetadatas, PerIndexFiles indexFiles)
    {
        this.componentMetadatas = componentMetadatas;
        this.indexFiles = indexFiles;
    }

    public abstract long ramBytesUsed();

    public abstract int size();

    public abstract OrdinalsView getOrdinalsView() throws IOException;
    public abstract VectorSupplier getVectorSupplier() throws IOException;

    /** returns null if no compression was performed */
    public abstract CompressedVectors getCompressedVectors();

    /**
     * See CassandraDiskANN::search
     */
    public abstract CloseableIterator<RowIdWithScore> search(VectorFloat<?> queryVector, int limit, int rerankK, float threshold, Bits bits, QueryContext context, IntConsumer nodesVisited);

    public abstract void close() throws IOException;

    public static interface VectorSupplier extends AutoCloseable
    {
        /**
         * Returns the score function for the given query vector.
         */
        ScoreFunction.ExactScoreFunction getScoreFunction(VectorFloat<?> queryVector, VectorSimilarityFunction similarityFunction);

        /**
         * Close the vectors view, logging any exceptions.
         */
        @Override
        void close();
    }

    /**
     * An ExactScoreFunction that closes the underlying {@link VectorSupplier} when closed.
     */
    public static class CloseableReranker implements ScoreFunction.ExactScoreFunction, Closeable
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
}
