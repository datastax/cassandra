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

import io.github.jbellis.jvector.graph.GraphSearcher;
import io.github.jbellis.jvector.graph.SearchResult;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.metrics.ColumnQueryMetrics;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.AbstractIterator;

import static java.lang.Math.max;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

/**
 * An iterator over {@link SearchResult.NodeScore} backed by a {@link SearchResult} that resumes search
 * when the backing {@link SearchResult} is exhausted.
 */
public class AutoResumingNodeScoreIterator extends AbstractIterator<SearchResult.NodeScore>
{
    private final GraphSearcher searcher;
    private final GraphSearcherAccessManager accessManager;
    private final int limit;
    private final int rerankK;
    private final boolean inMemory;
    private final String source;
    private final QueryContext context;
    private final ColumnQueryMetrics.VectorIndexMetrics columnQueryMetrics;
    private final IntConsumer nodesVisitedConsumer;
    private Iterator<SearchResult.NodeScore> nodeScores;
    private int cumulativeNodesVisited;

    /**
     * Create a new {@link AutoResumingNodeScoreIterator} that iterates over the provided {@link SearchResult}.
     * If the {@link SearchResult} is consumed, it retrieves the next {@link SearchResult} until the search returns
     * no more results.
     * @param searcher the {@link GraphSearcher} to use to resume search.
     * @param result the first {@link SearchResult} to iterate over
     * @param context the {@link QueryContext} to use to record metrics
     * @param columnQueryMetrics object to record metrics
     * @param nodesVisitedConsumer a consumer that accepts the total number of nodes visited
     * @param limit the limit to pass to the {@link GraphSearcher} when resuming search
     * @param rerankK the rerankK to pass to the {@link GraphSearcher} when resuming search
     * @param inMemory whether the graph is in memory or on disk (used for trace logging)
     * @param source the source of the search (used for trace logging)
     */
    public AutoResumingNodeScoreIterator(GraphSearcher searcher,
                                         GraphSearcherAccessManager accessManager,
                                         SearchResult result,
                                         QueryContext context,
                                         ColumnQueryMetrics.VectorIndexMetrics columnQueryMetrics,
                                         IntConsumer nodesVisitedConsumer,
                                         int limit,
                                         int rerankK,
                                         boolean inMemory,
                                         String source)
    {
        this.searcher = searcher;
        this.accessManager = accessManager;
        this.nodeScores = Arrays.stream(result.getNodes()).iterator();
        this.context = context;
        this.columnQueryMetrics = columnQueryMetrics;
        this.cumulativeNodesVisited = 0;
        this.nodesVisitedConsumer = nodesVisitedConsumer;
        this.limit = max(1, limit / 2); // we shouldn't need as many results on resume
        this.rerankK = rerankK;
        this.inMemory = inMemory;
        this.source = source;
    }

    @Override
    protected SearchResult.NodeScore computeNext()
    {
        if (nodeScores.hasNext())
            return nodeScores.next();

        long start = nanoTime();

        // Search deeper into the graph
        var nextResult = searcher.resume(limit, rerankK);

        // Record metrics
        long elapsed = nanoTime() - start;
        columnQueryMetrics.onSearchResult(nextResult, elapsed, true);
        context.addAnnGraphSearchLatency(elapsed);
        cumulativeNodesVisited += nextResult.getVisitedCount();

        if (Tracing.isTracing())
        {
            String msg = inMemory ? "Memory based ANN resume for {}/{} visited {} nodes, reranked {} to return {} results from {}"
                                  : "Disk based ANN resume for {}/{} visited {} nodes, reranked {} to return {} results from {}";
            Tracing.trace(msg, limit, rerankK, nextResult.getVisitedCount(), nextResult.getRerankedCount(), nextResult.getNodes().length, source);
        }

        // If the next result is empty, we are done searching.
        nodeScores = Arrays.stream(nextResult.getNodes()).iterator();
        return nodeScores.hasNext() ? nodeScores.next() : endOfData();
    }

    @Override
    public void close()
    {
        nodesVisitedConsumer.accept(cumulativeNodesVisited);
        accessManager.release();
    }
}