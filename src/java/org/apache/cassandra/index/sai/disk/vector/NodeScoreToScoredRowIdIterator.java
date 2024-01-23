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

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.PrimitiveIterator;
import java.util.stream.IntStream;

import io.github.jbellis.jvector.graph.SearchResult;
import org.apache.cassandra.utils.CloseableIterator;

/**
 * An iterator over {@link ScoredRowId} sorted by score descending. The iterator converts ordinals (node ids) to
 * segment row ids and pairs them with the score given by the index.
 */
public class NodeScoreToScoredRowIdIterator implements CloseableIterator<ScoredRowId>
{
    private final CloseableIterator<SearchResult.NodeScore> nodeScores;
    private final RowIdsView rowIdsView;

    private PrimitiveIterator.OfInt segmentRowIdIterator = IntStream.empty().iterator();
    private float currentScore;

    public NodeScoreToScoredRowIdIterator(CloseableIterator<SearchResult.NodeScore> nodeScores, RowIdsView rowIdsView)
    {
        this.nodeScores = nodeScores;
        this.rowIdsView = rowIdsView;
    }

    @Override
    public boolean hasNext()
    {
        try
        {
            if (segmentRowIdIterator.hasNext())
                return true;

            while (nodeScores.hasNext())
            {
                SearchResult.NodeScore result = nodeScores.next();
                currentScore = result.score;
                var ordinal = result.node;
                segmentRowIdIterator = rowIdsView.getSegmentRowIdsMatching(ordinal);
                if (segmentRowIdIterator.hasNext())
                    return true;
            }
            return false;
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ScoredRowId next()
    {
        if (!hasNext())
            throw new NoSuchElementException();
        return new ScoredRowId(segmentRowIdIterator.nextInt(), currentScore);
    }

    @Override
    public void close()
    {
        rowIdsView.close();
        nodeScores.close();
    }
}
