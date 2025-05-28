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
package org.apache.cassandra.index.sai.metrics;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import io.github.jbellis.jvector.graph.SearchResult;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

public abstract class ColumnQueryMetrics extends AbstractMetrics
{
    protected ColumnQueryMetrics(String keyspace, String table, String indexName)
    {
        super(keyspace, table, indexName, "ColumnQueryMetrics");
    }

    public static class TrieIndexMetrics extends ColumnQueryMetrics implements QueryEventListener.TrieIndexEventListener
    {
        private static final String TRIE_POSTINGS_TYPE = "Postings";

        /**
         * Trie index metrics.
         */
        public final Timer termsTraversalTotalTime;

        public final QueryEventListener.PostingListEventListener postingsListener;

        public TrieIndexMetrics(String keyspace, String table, String indexName)
        {
            super(keyspace, table, indexName);

            termsTraversalTotalTime = Metrics.timer(createMetricName("TermsLookupLatency"));

            Meter postingDecodes = Metrics.meter(createMetricName("PostingDecodes", TRIE_POSTINGS_TYPE));

            postingsListener = new PostingListEventsMetrics(postingDecodes);
        }

        @Override
        public void onSegmentHit() { }

        @Override
        public void onTraversalComplete(long traversalTotalTime, TimeUnit unit)
        {
            termsTraversalTotalTime.update(traversalTotalTime, unit);
        }

        @Override
        public QueryEventListener.PostingListEventListener postingListEventListener()
        {
            return postingsListener;
        }
    }

    public static class BKDIndexMetrics extends ColumnQueryMetrics implements QueryEventListener.BKDIndexEventListener
    {
        private static final String BKD_POSTINGS_TYPE = "KDTreePostings";

        /**
         * BKD index metrics.
         */
        public final Timer intersectionLatency;
        public final Meter postingsNumPostings;
        public final Meter intersectionEarlyExits;

        private final QueryEventListener.PostingListEventListener postingsListener;

        public BKDIndexMetrics(String keyspace, String table, String indexName)
        {
            super(keyspace, table, indexName);

            intersectionLatency = Metrics.timer(createMetricName("KDTreeIntersectionLatency"));
            intersectionEarlyExits = Metrics.meter(createMetricName("KDTreeIntersectionEarlyExits"));

            postingsNumPostings = Metrics.meter(createMetricName("NumPostings", BKD_POSTINGS_TYPE));

            Meter postingDecodes = Metrics.meter(createMetricName("PostingDecodes", BKD_POSTINGS_TYPE));

            postingsListener = new PostingListEventsMetrics(postingDecodes);
        }

        @Override
        public void onIntersectionComplete(long intersectionTotalTime, TimeUnit unit)
        {
            intersectionLatency.update(intersectionTotalTime, unit);
        }

        @Override
        public void onIntersectionEarlyExit()
        {
            intersectionEarlyExits.mark();
        }

        @Override
        public void postingListsHit(int count)
        {
            postingsNumPostings.mark(count);
        }

        @Override
        public void onSegmentHit() { }

        @Override
        public QueryEventListener.PostingListEventListener postingListEventListener()
        {
            return postingsListener;
        }
    }

    private static class PostingListEventsMetrics implements QueryEventListener.PostingListEventListener
    {
        private final Meter postingDecodes;

        private PostingListEventsMetrics(Meter postingDecodes)
        {
            this.postingDecodes = postingDecodes;
        }

        @Override
        public void onAdvance() { }

        @Override
        public void postingDecoded(long postingsDecoded)
        {
            postingDecodes.mark(postingsDecoded);
        }
    }

    /**
     * Example VectorIndexMetrics for tracking ANN/Vector index–related metrics.
     * You will also need a corresponding QueryEventListener implementation
     * (e.g. VectorIndexEventListener) that calls these methods.
     */
    public static class VectorIndexMetrics extends ColumnQueryMetrics implements QueryEventListener.VectorIndexEventListener
    {
        // Vector index meatrics
        // Note that the counters will essentially give us a number of operations per second. We lose the notion
        // of per query counts, but we can back into an average by dividing by the number of queries.
        public final Counter annNodesVisited;
        public final Counter annNodesReranked;
        public final Counter annNodesExpanded;
        public final Counter annNodesExpandedBaseLayer;
        public final Counter annGraphSearches;
        public final Counter annGraphResumes;
        public final Timer   annGraphSearchLatency; // Note that this timer measures individual graph search latency

        public final Counter bruteForceNodesVisited;
        public final Counter bruteForceNodesReranked;

        // While not query metrics, these are vector specific metrics for the column.
        public final LongAdder quantizationMemoryBytes;
        public final LongAdder ordinalsMapMemoryBytes;
        public final LongAdder onDiskGraphsCount;
        public final LongAdder onDiskGraphVectorsCount;

        public VectorIndexMetrics(String keyspace, String table, String indexName)
        {
            super(keyspace, table, indexName);

            // Initialize Counters and Timer for ANN search
            annNodesVisited = Metrics.counter(createMetricName("ANNNodesVisited"));
            annNodesReranked = Metrics.counter(createMetricName("ANNNodesReranked"));
            annNodesExpanded = Metrics.counter(createMetricName("ANNNodesExpanded"));
            annNodesExpandedBaseLayer = Metrics.counter(createMetricName("ANNNodesExpandedBaseLayer"));
            annGraphSearches = Metrics.counter(createMetricName("ANNGraphSearches"));
            annGraphResumes = Metrics.counter(createMetricName("ANNGraphResumes"));
            annGraphSearchLatency = Metrics.timer(createMetricName("ANNGraphSearchLatency"));

            // Initialize Counters for brute-force fallback (if applicable)
            bruteForceNodesVisited = Metrics.counter(createMetricName("BruteForceNodesVisited"));
            bruteForceNodesReranked = Metrics.counter(createMetricName("BruteForceNodesReranked"));

            // Initialize Gauge for PQ bytes. Ignoring codahale metrics for now.
            quantizationMemoryBytes = new LongAdder();
            ordinalsMapMemoryBytes = new LongAdder();
            onDiskGraphVectorsCount = new LongAdder();
            onDiskGraphsCount = new LongAdder();
        }

        @Override
        public void onGraphLoaded(long quantizationBytes, long ordinalsMapCachedBytes, long vectorsLoaded)
        {
            this.quantizationMemoryBytes.add(quantizationBytes);
            this.ordinalsMapMemoryBytes.add(ordinalsMapCachedBytes);
            this.onDiskGraphVectorsCount.add(vectorsLoaded);
            this.onDiskGraphsCount.increment();
        }

        @Override
        public void onGraphClosed(long quantizationBytes, long ordinalsMapCachedBytes, long vectorsLoaded)
        {
            this.quantizationMemoryBytes.add(-quantizationBytes);
            this.ordinalsMapMemoryBytes.add(-ordinalsMapCachedBytes);
            this.onDiskGraphVectorsCount.add(-vectorsLoaded);
            this.onDiskGraphsCount.decrement();
        }

        @Override
        public void onSearchResult(SearchResult result, long latencyNs, boolean isResume)
        {
            annNodesVisited.inc(result.getVisitedCount());
            annNodesReranked.inc(result.getRerankedCount());
            annNodesExpanded.inc(result.getExpandedCount());
            annNodesExpandedBaseLayer.inc(result.getExpandedCountBaseLayer());
            annGraphSearchLatency.update(latencyNs, TimeUnit.NANOSECONDS);
            if (isResume)
                annGraphResumes.inc();
            else
                annGraphSearches.inc();
        }

        // These are the approximate similarity comparisons
        @Override
        public void onBruteForceNodesVisited(int visited)
        {
            bruteForceNodesVisited.inc(visited);
        }

        // These are the exact similarity comparisons
        @Override
        public void onBruteForceNodesReranked(int visited)
        {
            bruteForceNodesReranked.inc(visited);
        }
    }
}
