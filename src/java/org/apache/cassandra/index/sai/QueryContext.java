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

package org.apache.cassandra.index.sai;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.index.sai.utils.AbortedOperationException;
import org.apache.cassandra.utils.MonotonicClock;

import static java.lang.Math.max;

/**
 * Tracks state relevant to the execution of a single query, including metrics and timeout monitoring.
 */
@NotThreadSafe
public class QueryContext
{
    public static final boolean DISABLE_TIMEOUT = CassandraRelevantProperties.TEST_SAI_DISABLE_TIMEOUT.getBoolean();

    protected final long queryStartTimeNanos;

    public final long executionQuotaNano;

    private final LongAdder sstablesHit = new LongAdder();
    private final LongAdder segmentsHit = new LongAdder();
    private final LongAdder partitionsRead = new LongAdder();
    private final LongAdder rowsPreFiltered = new LongAdder();
    private final LongAdder rowsFiltered = new LongAdder();
    private final LongAdder trieSegmentsHit = new LongAdder();

    private final LongAdder bkdPostingListsHit = new LongAdder();
    private final LongAdder bkdSegmentsHit = new LongAdder();

    private final LongAdder bkdPostingsSkips = new LongAdder();
    private final LongAdder bkdPostingsDecodes = new LongAdder();

    private final LongAdder triePostingsSkips = new LongAdder();
    private final LongAdder triePostingsDecodes = new LongAdder();

    private final LongAdder queryTimeouts = new LongAdder();

    private final LongAdder annGraphSearchLatency = new LongAdder();

    private float annRerankFloor = 0.0f; // only called from single-threaded setup code

    private final LongAdder shadowedPrimaryKeyCount = new LongAdder();
    private final LongAdder postFilteringReadLatency = new LongAdder();

    // Determines the order of using indexes for filtering and sorting.
    // Null means the query execution order hasn't been decided yet.
    private FilterSortOrder filterSortOrder = null;

    @VisibleForTesting
    public QueryContext()
    {
        this(DatabaseDescriptor.getRangeRpcTimeout(TimeUnit.MILLISECONDS));
    }

    public QueryContext(long executionQuotaMs)
    {
        this.executionQuotaNano = TimeUnit.MILLISECONDS.toNanos(executionQuotaMs);
        this.queryStartTimeNanos = MonotonicClock.Global.approxTime.now();
    }

    public long totalQueryTimeNs()
    {
        return MonotonicClock.Global.approxTime.now() - queryStartTimeNanos;
    }

    // setters
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
    public void addRowsPreFiltered(long val)
    {
        rowsPreFiltered.add(val);
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
    public void addQueryTimeouts(long val)
    {
        queryTimeouts.add(val);
    }

    public void addAnnGraphSearchLatency(long val)
    {
        annGraphSearchLatency.add(val);
    }

    public void addPostFilteringReadLatency(long val)
    {
        postFilteringReadLatency.add(val);
    }

    public void setFilterSortOrder(FilterSortOrder filterSortOrder)
    {
        this.filterSortOrder = filterSortOrder;
    }

    // getters

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
    public long rowsPreFiltered()
    {
        return rowsPreFiltered.longValue();
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
    public long queryTimeouts()
    {
        return queryTimeouts.longValue();
    }
    public long annGraphSearchLatency()
    {
        return annGraphSearchLatency.longValue();
    }

    public FilterSortOrder filterSortOrder()
    {
        return filterSortOrder;
    }

    public void checkpoint()
    {
        if (totalQueryTimeNs() >= executionQuotaNano && !DISABLE_TIMEOUT)
        {
            addQueryTimeouts(1);
            throw new AbortedOperationException();
        }
    }

    public void addShadowed(long count)
    {
        shadowedPrimaryKeyCount.add(count);
    }

    /**
     * @return shadowed primary keys, in ascending order
     */
    public long getShadowedPrimaryKeyCount()
    {
        return shadowedPrimaryKeyCount.longValue();
    }

    public float getAnnRerankFloor()
    {
        return annRerankFloor;
    }

    public void updateAnnRerankFloor(float observedFloor)
    {
        if (observedFloor < Float.POSITIVE_INFINITY)
            annRerankFloor = max(annRerankFloor, observedFloor);
    }

    public long getPostFilteringReadLatency()
    {
        return postFilteringReadLatency.longValue();
    }

    /**
     * Determines the order of filtering and sorting operations.
     * Currently used only by vector search.
     */
    public enum FilterSortOrder
    {
        /** First get the matching keys from the non-vector indexes, then use vector index to return the top K by similarity order */
        SEARCH_THEN_ORDER,

        /** First get the candidates in ANN order from the vector index, then fetch the rows and filter them until we find K matching the predicates */
        SCAN_THEN_FILTER
    }

    public Snapshot snapshot()
    {
        return new Snapshot(this);
    }

    /**
     * A snapshot of all relevant metrics in a {@link QueryContext} at a specific point in time.
     * This class memoizes the values of those metrics so that they can be reused by multiple metrics instances,
     * without calculating the same values once and again.
     * Also, this class should be more lightweight than the full {@link QueryContext}, in case of needing to retain it
     * for long-ish periods of time, as in the case of the slow query logger, which tracks the metrics of the slowest
     * queries over a fixed period of time.
     */
    public static class Snapshot
    {
        public final long totalQueryTimeNs;
        public final long sstablesHit;
        public final long segmentsHit;
        public final long partitionsRead;
        public final long rowsFiltered;
        public final long rowsPreFiltered;
        public final long trieSegmentsHit;
        public final long bkdPostingListsHit;
        public final long bkdSegmentsHit;
        public final long bkdPostingsSkips;
        public final long bkdPostingsDecodes;
        public final long triePostingsSkips;
        public final long triePostingsDecodes;
        public final long queryTimeouts;
        public final long annGraphSearchLatency;
        public final long shadowedPrimaryKeyCount;
        public final long postFilteringReadLatency;
        public final FilterSortOrder filterSortOrder;

        /**
         * Creates a snapshot of all the metrics in the given {@link QueryContext}.
         *
         * @param context the query context to snapshot
         */
        private Snapshot(QueryContext context)
        {
            totalQueryTimeNs = context.totalQueryTimeNs();
            sstablesHit = context.sstablesHit();
            segmentsHit = context.segmentsHit();
            partitionsRead = context.partitionsRead();
            rowsFiltered = context.rowsFiltered();
            rowsPreFiltered = context.rowsPreFiltered();
            trieSegmentsHit = context.trieSegmentsHit();
            bkdPostingListsHit = context.bkdPostingListsHit();
            bkdSegmentsHit = context.bkdSegmentsHit();
            bkdPostingsSkips = context.bkdPostingsSkips();
            bkdPostingsDecodes = context.bkdPostingsDecodes();
            triePostingsSkips = context.triePostingsSkips();
            triePostingsDecodes = context.triePostingsDecodes();
            queryTimeouts = context.queryTimeouts();
            annGraphSearchLatency = context.annGraphSearchLatency();
            shadowedPrimaryKeyCount = context.getShadowedPrimaryKeyCount();
            postFilteringReadLatency = context.getPostFilteringReadLatency();
            filterSortOrder = context.filterSortOrder();
        }
    }
}
