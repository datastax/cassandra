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

import java.util.Comparator;
import java.util.NoSuchElementException;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import io.github.jbellis.jvector.graph.GraphIndex;
import io.github.jbellis.jvector.graph.NodesIterator;
import io.github.jbellis.jvector.graph.similarity.ScoreFunction;
import io.github.jbellis.jvector.util.Bits;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import org.apache.cassandra.utils.SortingIterator;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class BruteForceRowIdIteratorTest
{
    private static final VectorTypeSupport vts = VectorizationProvider.getInstance().getVectorTypeSupport();

    @Test
    public void testBruteForceRowIdIteratorForEmptyPQAndTopKEqualsLimit()
    {
        var queryVector = vts.createFloatVector(new float[] { 1f, 0f });
        var pq = SortingIterator.create(BruteForceRowIdIterator.RowWithApproximateScore::compare, ImmutableList.of());
        var topK = 10;
        var limit = 10;

        // Should work for an empty pq
        var view = new TestView();
        CloseableReranker reranker = new CloseableReranker(VectorSimilarityFunction.COSINE, queryVector, view);
        var iter = new BruteForceRowIdIterator(pq, reranker, limit, topK);
        assertFalse(iter.hasNext());
        assertThrows(NoSuchElementException.class, iter::next);
        assertFalse(view.isClosed);
        iter.close();
        assertTrue(view.isClosed);
    }

    private static class TestView implements GraphIndex.ScoringView
    {
        private boolean isClosed = false;

        @Override
        public ScoreFunction.ExactScoreFunction rerankerFor(VectorFloat<?> queryVector, VectorSimilarityFunction vsf)
        {
            return ordinal -> vsf.compare(queryVector, vts.createFloatVector(new float[] { ordinal }));
        }

        @Override
        public void close()
        {
            isClosed = true;
        }

        //
        // unused by BruteForceRowIdIterator
        //

        @Override
        public ScoreFunction.ApproximateScoreFunction approximateScoreFunctionFor(VectorFloat<?> vectorFloat, VectorSimilarityFunction vectorSimilarityFunction)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public NodesIterator getNeighborsIterator(int i)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public int size()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public int entryNode()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Bits liveNodes()
        {
            throw new UnsupportedOperationException();
        }
    }
}
