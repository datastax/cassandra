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

import java.util.NoSuchElementException;
import java.util.PriorityQueue;

import org.junit.Test;

import io.github.jbellis.jvector.graph.NodeSimilarity;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;

public class BruteForceRowIdIteratorTest
{
    @Test
    public void testBruteForceRowIdIteratorForEmptyPQAndTopKEqualsLimit()
    {
        var queryVector = new float[] { 1f, 0f };
        var pq = new PriorityQueue<BruteForceRowIdIterator.RowWithApproximateScore>(10);
        var topK = 10;
        var limit = 10;

        // Should work for an empty pq
        NodeSimilarity.Reranker reranker = o -> VectorSimilarityFunction.COSINE.compare(queryVector, identityMapper(o));
        var iter = new BruteForceRowIdIterator(pq, reranker, limit, topK);
        assertFalse(iter.hasNext());
        assertThrows(NoSuchElementException.class, iter::next);
    }

    private float[] identityMapper(int rowId)
    {
        return new float[] { rowId };
    }
}
