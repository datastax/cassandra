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

package org.apache.cassandra.db;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import com.google.common.annotations.VisibleForTesting;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Snapshot;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.index.sai.utils.AbortedOperationException;
import org.apache.cassandra.metrics.DecayingEstimatedHistogramReservoir;

/**
 * Tracks state relevant to the execution of a single query, including metrics and timeout monitoring.
 */
public class QueryContext
{
    static class SearchResultMetric
    {
        private final LongAdder searchesCount = new LongAdder();
        private final Histogram visitsHistogram = createHistogram();
        private final Histogram resultsHistogram = createHistogram();

        public void addSearches(long visits, long results)
        {
            searchesCount.add(1);
            visitsHistogram.update(visits);
            resultsHistogram.update(results);
        }

        public long getSearchesCount()
        {
            return searchesCount.longValue();
        }

        public Snapshot getVisitsHistogram()
        {
            return visitsHistogram.getSnapshot();
        }

        public Snapshot getResultsHistogram()
        {
            return resultsHistogram.getSnapshot();
        }

        @Override
        public String toString()
        {
            return String.format("executed %d times, visits: %s, results: %s",
                                 searchesCount.longValue(),
                                 printHisto(getVisitsHistogram()),
                                 printHisto(getResultsHistogram()));
        }
    }

    private static Histogram createHistogram()
    {
        return new Histogram(new DecayingEstimatedHistogramReservoir(true));
    }

    public static String printHisto(Snapshot val)
    {
        return String.format("(p50=%.2f, p99=%.2f, max=%d, stdev=%.2f)",
                             val.getMedian(), val.get99thPercentile(), val.getMax(), val.getStdDev());
    }

    private static final boolean DISABLE_TIMEOUT = Boolean.getBoolean("cassandra.sai.test.disable.timeout");

    protected final long queryStartTimeNanos;

    public final long executionQuotaNano;

    private final LongAdder sstablesHit = new LongAdder();
    private final LongAdder segmentsHit = new LongAdder();
    private final LongAdder partitionsRead = new LongAdder();
    private final LongAdder rowsFiltered = new LongAdder();

    private final LongAdder trieSegmentsHit = new LongAdder();

    private final LongAdder bkdPostingListsHit = new LongAdder();
    private final LongAdder bkdSegmentsHit = new LongAdder();

    private final LongAdder bkdPostingsSkips = new LongAdder();
    private final LongAdder bkdPostingsDecodes = new LongAdder();

    private final LongAdder triePostingsSkips = new LongAdder();
    private final LongAdder triePostingsDecodes = new LongAdder();

    private final LongAdder tokenSkippingCacheHits = new LongAdder();
    private final LongAdder tokenSkippingLookups = new LongAdder();

    private final LongAdder queryTimeouts = new LongAdder();

    private final LongAdder hnswVectorsAccessed = new LongAdder();
    private final LongAdder hnswVectorCacheHits = new LongAdder();

    private final LongAdder shadowedKeysLoopCount = new LongAdder();
    private final LongAdder shadowedPrimaryKeysCount = new LongAdder();


    private final SearchResultMetric diskannSearches = new SearchResultMetric();
    private final Histogram diskAnnLatenciesMicros = createHistogram();
    private final SearchResultMetric diskhnswSearches = new SearchResultMetric();
    private final Histogram diskHnswLatenciesMicros = createHistogram();
    private final SearchResultMetric heapannSearches = new SearchResultMetric();
    private final Histogram heapAnnLatenciesMicros = createHistogram();

    private final Histogram searchLatenciesMicros = createHistogram();

    private final Histogram sstablesPerIndexQueried = createHistogram();

    @VisibleForTesting
    public QueryContext()
    {
        this(DatabaseDescriptor.getRangeRpcTimeout(TimeUnit.MILLISECONDS));
    }

    public QueryContext(long executionQuotaMs)
    {
        this.executionQuotaNano = TimeUnit.MILLISECONDS.toNanos(executionQuotaMs);
        this.queryStartTimeNanos = System.nanoTime();
    }

    public long totalQueryTimeNs()
    {
        return System.nanoTime() - queryStartTimeNanos;
    }

    // setters

    public void markDiskHnswLatencies(long latencyNanos)
    {
        diskHnswLatenciesMicros.update(TimeUnit.NANOSECONDS.toMicros(latencyNanos));
    }

    public void markDiskAnnLatencies(long latencyNanos)
    {
        diskAnnLatenciesMicros.update(TimeUnit.NANOSECONDS.toMicros(latencyNanos));
    }

    public void markHeapAnnLatencies(long latencyNanos)
    {
        heapAnnLatenciesMicros.update(TimeUnit.NANOSECONDS.toMicros(latencyNanos));
    }

    public void addIndexesQueried(long sstablesPerIndex)
    {
        sstablesPerIndexQueried.update(sstablesPerIndex);
    }

    public void addDiskannSearches(long nodes, long results)
    {
        diskannSearches.addSearches(nodes, results);
    }

    public void addDiskhnswSearches(long nodes, long results)
    {
        diskhnswSearches.addSearches(nodes, results);
    }

    public void addHeapannSearches(long nodes, long results)
    {
        heapannSearches.addSearches(nodes, results);
    }

    public void addShadowedPrimaryKeysCount(long val)
    {
        shadowedPrimaryKeysCount.add(val);
    }

    public void addSstablesHit(long val)
    {
        sstablesHit.add(val);
    }
    public void addSegmentsHit(long val) {
        segmentsHit.add(val);
    }
    public void addPartitionsRead(long val)
    {
        partitionsRead.add(val);
    }
    public void addRowsFiltered(long val)
    {
        rowsFiltered.add(val);
    }
    public void addTrieSegmentsHit(long val)
    {
        trieSegmentsHit.add(val);
    }
    public void addBkdPostingListsHit(long val)
    {
        bkdPostingListsHit.add(val);
    }
    public void addBkdSegmentsHit(long val)
    {
        bkdSegmentsHit.add(val);
    }
    public void addBkdPostingsSkips(long val)
    {
        bkdPostingsSkips.add(val);
    }
    public void addBkdPostingsDecodes(long val)
    {
        bkdPostingsDecodes.add(val);
    }
    public void addTriePostingsSkips(long val)
    {
        triePostingsSkips.add(val);
    }
    public void addTriePostingsDecodes(long val)
    {
        triePostingsDecodes.add(val);
    }
    public void addTokenSkippingCacheHits(long val)
    {
        tokenSkippingCacheHits.add(val);
    }
    public void addTokenSkippingLookups(long val)
    {
        tokenSkippingLookups.add(val);
    }
    public void addQueryTimeouts(long val)
    {
        queryTimeouts.add(val);
    }
    public void addHnswVectorsAccessed(long val)
    {
        hnswVectorsAccessed.add(val);
    }
    public void addHnswVectorCacheHits(long val)
    {
        hnswVectorCacheHits.add(val);
    }

    public void addShadowedKeysLoopCount(long val)
    {
        shadowedKeysLoopCount.add(val);
    }

    // getters

    public long shadowedPrimaryKeysCount()
    {
        return shadowedPrimaryKeysCount.longValue();
    }

    public long sstablesHit()
    {
        return sstablesHit.longValue();
    }
    public long segmentsHit() {
        return segmentsHit.longValue();
    }
    public long partitionsRead()
    {
        return partitionsRead.longValue();
    }
    public long rowsFiltered()
    {
        return rowsFiltered.longValue();
    }
    public long trieSegmentsHit()
    {
        return trieSegmentsHit.longValue();
    }
    public long bkdPostingListsHit()
    {
        return bkdPostingListsHit.longValue();
    }
    public long bkdSegmentsHit()
    {
        return bkdSegmentsHit.longValue();
    }
    public long bkdPostingsSkips()
    {
        return bkdPostingsSkips.longValue();
    }
    public long bkdPostingsDecodes()
    {
        return bkdPostingsDecodes.longValue();
    }
    public long triePostingsSkips()
    {
        return triePostingsSkips.longValue();
    }
    public long triePostingsDecodes()
    {
        return triePostingsDecodes.longValue();
    }
    public long tokenSkippingCacheHits()
    {
        return tokenSkippingCacheHits.longValue();
    }
    public long tokenSkippingLookups()
    {
        return tokenSkippingLookups.longValue();
    }
    public long queryTimeouts()
    {
        return queryTimeouts.longValue();
    }
    public long hnswVectorsAccessed()
    {
        return hnswVectorsAccessed.longValue();
    }
    public long hnswVectorCacheHits()
    {
        return hnswVectorCacheHits.longValue();
    }

    public SearchResultMetric diskannSearches()
    {
        return diskannSearches;
    }

    public SearchResultMetric heapannSearches()
    {
        return heapannSearches;
    }

    public SearchResultMetric diskhnswSearches()
    {
        return diskhnswSearches;
    }

    public void checkpoint()
    {
        searchLatenciesMicros.update(TimeUnit.NANOSECONDS.toMicros(totalQueryTimeNs()));
        if (totalQueryTimeNs() >= executionQuotaNano && !DISABLE_TIMEOUT)
        {
            addQueryTimeouts(1);
            throw new AbortedOperationException();
        }
    }

    public long numSearches()
    {
        return searchLatenciesMicros.getCount();
    }

    public Snapshot searchLatenciesMicros()
    {
        return searchLatenciesMicros.getSnapshot();
    }

    public Snapshot diskhnswLatenciesMicros()
    {
        return diskHnswLatenciesMicros.getSnapshot();
    }

    public Snapshot diskAnnLatenciesMicros()
    {
        return diskAnnLatenciesMicros.getSnapshot();
    }

    public Snapshot heapAnnLatenciesMicros()
    {
        return heapAnnLatenciesMicros.getSnapshot();
    }

    public long numIndexesQueried()
    {
        return sstablesPerIndexQueried.getCount();
    }

    public Snapshot sstablesPerIndexQueried()
    {
        return sstablesPerIndexQueried.getSnapshot();
    }

    public long shadowedKeysLoopCount()
    {
        return shadowedKeysLoopCount.longValue();
    }

}
