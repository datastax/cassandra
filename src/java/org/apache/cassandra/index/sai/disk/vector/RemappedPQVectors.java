/*
 * Copyright IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.index.sai.disk.vector;

import java.io.IOException;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkElementIndex;

import io.github.jbellis.jvector.disk.IndexWriter;
import io.github.jbellis.jvector.graph.disk.OrdinalMapper;
import io.github.jbellis.jvector.graph.similarity.ScoreFunction;
import io.github.jbellis.jvector.quantization.PQVectors;
import io.github.jbellis.jvector.quantization.ProductQuantization;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.types.ByteSequence;
import io.github.jbellis.jvector.vector.types.VectorFloat;

/**
 * A remapped view over an existing {@link PQVectors} instance.
 * Ordinals are mapped from new ordinals to old ordinals
 * in the source {@link PQVectors} instance using the provided {@link OrdinalMapper}.
 */
public class RemappedPQVectors extends PQVectors
{
    private static final String ORDINAL_DESC = "ordinal";
    private static final String MAPPED_ORDINAL_DESC = "mapped ordinal";

    private final PQVectors source;
    private final OrdinalMapper mapper;

    public RemappedPQVectors(PQVectors source, OrdinalMapper mapper)
    {
        super(source.getCompressor());
        this.source = Objects.requireNonNull(source);
        this.mapper = Objects.requireNonNull(mapper);
    }

    @Override
    public int count()
    {
        return mapper.maxOrdinal() + 1;
    }

    @Override
    protected int validChunkCount()
    {
        throw new UnsupportedOperationException("RemappedPQVectors is a lazy view and does not manage chunks directly");
    }

    @Override
    public ByteSequence<?> get(int ordinal)
    {
        checkElementIndex(ordinal, count(), ORDINAL_DESC);
        int oldOrdinal = mapper.newToOld(ordinal);
        checkElementIndex(oldOrdinal, source.count(), MAPPED_ORDINAL_DESC);
        return source.get(oldOrdinal);
    }

    @Override
    public void write(IndexWriter out, int version) throws IOException
    {
        ProductQuantization pq = getCompressor();
        // pq codebooks
        pq.write(out, version);

        // compressed vectors
        int totalCount = count();
        out.writeInt(totalCount);
        int subspaceCount = pq.getSubspaceCount();
        out.writeInt(subspaceCount);

        for (int i = 0; i < totalCount; i++)
        {
            ByteSequence<?> seq = get(i);
            out.write((byte[]) seq.get(), seq.offset(), seq.length());
        }
    }

    @Override
    public ScoreFunction.ApproximateScoreFunction precomputedScoreFunctionFor(VectorFloat<?> q, VectorSimilarityFunction similarityFunction)
    {
        ScoreFunction.ApproximateScoreFunction sourceScoreFunction = source.precomputedScoreFunctionFor(q, similarityFunction);
        return newOrdinal -> {
            checkElementIndex(newOrdinal, count(), ORDINAL_DESC);
            int oldOrdinal = mapper.newToOld(newOrdinal);
            checkElementIndex(oldOrdinal, source.count(), MAPPED_ORDINAL_DESC);
            return sourceScoreFunction.similarityTo(oldOrdinal);
        };
    }

    @Override
    public ScoreFunction.ApproximateScoreFunction scoreFunctionFor(VectorFloat<?> q, VectorSimilarityFunction similarityFunction)
    {
        ScoreFunction.ApproximateScoreFunction sourceScoreFunction = source.scoreFunctionFor(q, similarityFunction);
        return newOrdinal -> {
            checkElementIndex(newOrdinal, count(), ORDINAL_DESC);
            int oldOrdinal = mapper.newToOld(newOrdinal);
            checkElementIndex(oldOrdinal, source.count(), MAPPED_ORDINAL_DESC);
            return sourceScoreFunction.similarityTo(oldOrdinal);
        };
    }

    @Override
    public ScoreFunction.ApproximateScoreFunction diversityFunctionFor(int node1, VectorSimilarityFunction similarityFunction)
    {
        checkElementIndex(node1, count(), ORDINAL_DESC);
        int oldNode1 = mapper.newToOld(node1);
        checkElementIndex(oldNode1, source.count(), MAPPED_ORDINAL_DESC);

        ScoreFunction.ApproximateScoreFunction sourceDiversityFunction = source.diversityFunctionFor(oldNode1, similarityFunction);
        return node2 -> {
            checkElementIndex(node2, count(), ORDINAL_DESC);
            int oldNode2 = mapper.newToOld(node2);
            checkElementIndex(oldNode2, source.count(), MAPPED_ORDINAL_DESC);
            return sourceDiversityFunction.similarityTo(oldNode2);
        };
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (!(o instanceof PQVectors)) return false;
        PQVectors that = (PQVectors) o;

        if (!Objects.equals(getCompressor(), that.getCompressor())) return false;
        if (this.count() != that.count()) return false;
        for (int i = 0; i < this.count(); i++)
        {
            ByteSequence<?> thisNode = this.get(i);
            ByteSequence<?> thatNode = that.get(i);
            if (!thisNode.equals(thatNode)) return false;
        }
        return true;
    }

    @Override
    public int hashCode()
    {
        int result = 1;
        result = 31 * result + getCompressor().hashCode();
        result = 31 * result + count();

        for (int i = 0; i < count(); i++)
            result = 31 * result + get(i).hashCode();

        return result;
    }

    @Override
    public long ramBytesUsed()
    {
        return source.ramBytesUsed();
    }
}
