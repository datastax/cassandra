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

import java.util.Arrays;
import java.util.Iterator;
import java.util.function.IntConsumer;

import javax.annotation.Nullable;

import io.github.jbellis.jvector.graph.GraphSearcher;
import io.github.jbellis.jvector.graph.SearchResult;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.AbstractIterator;

/**
 * An iterator over {@link SearchResult.NodeScore} backed by a {@link SearchResult} that resumes search
 * when the backing {@link SearchResult} is exhausted.
 */
public class AutoResumingNodeScoreIterator extends AbstractIterator<SearchResult.NodeScore>
{
    private final GraphSearcher<float[]> searcher;
    private final int topK;
    private final boolean inMemory;
    private final AutoCloseable onClose;
    private final IntConsumer nodesVisitedConsumer;
    private Iterator<SearchResult.NodeScore> nodeScores;
    private int cumulativeNodesVisited;

    /**
     * Create a new {@link AutoResumingNodeScoreIterator} that iterates over the provided {@link SearchResult}.
     * If the {@link SearchResult} is consumed, it retrieves the next {@link SearchResult} until the search returns
     * no more results.
     * @param searcher the {@link GraphSearcher} to use to resume search.
     * @param result the first {@link SearchResult} to iterate over
     * @param nodesVisitedConsumer a consumer that accepts the total number of nodes visited
     * @param topK the limit to pass to the {@link GraphSearcher} when resuming search
     * @param inMemory whether the graph is in memory or on disk (used for trace logging)
     * @param onClose an {@link AutoCloseable} object to close when this iterator is closed
     */
    public AutoResumingNodeScoreIterator(GraphSearcher<float[]> searcher,
                                         SearchResult result,
                                         IntConsumer nodesVisitedConsumer,
                                         int topK,
                                         boolean inMemory,
                                         @Nullable AutoCloseable onClose)
    {
        this.searcher = searcher;
        this.nodeScores = Arrays.stream(result.getNodes()).iterator();
        this.cumulativeNodesVisited = result.getVisitedCount();
        this.nodesVisitedConsumer = nodesVisitedConsumer;
        this.topK = topK;
        this.inMemory = inMemory;
        this.onClose = onClose;
    }

    @Override
    protected SearchResult.NodeScore computeNext()
    {
        if (nodeScores.hasNext())
            return nodeScores.next();

        var nextResult = searcher.resume(topK);
        maybeLogTrace(nextResult);
        cumulativeNodesVisited += nextResult.getVisitedCount();
        // If the next result is empty, we are done searching.
        nodeScores = Arrays.stream(nextResult.getNodes()).iterator();
        return nodeScores.hasNext() ? nodeScores.next() : endOfData();
    }

    private void maybeLogTrace(SearchResult result)
    {
        if (!Tracing.isTracing())
            return;
        String msg = inMemory ? "ANN resumed search and visited {} in-memory nodes to return {} results"
                              : "DiskANN resumed search and visited {} nodes to return {} results";
        Tracing.trace(msg, result.getVisitedCount(), result.getNodes().length);
    }

    @Override
    public void close()
    {
        nodesVisitedConsumer.accept(cumulativeNodesVisited);
        FileUtils.closeQuietly(onClose);
    }
}