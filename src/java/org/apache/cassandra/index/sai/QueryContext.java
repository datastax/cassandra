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
import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.index.sai.utils.AbortedOperationException;
import org.apache.cassandra.utils.MonotonicClock;

import static java.lang.Math.max;

/**
 * Tracks state relevant to the execution of a single query, including metrics and timeout monitoring.
 */
@NotThreadSafe // this should only be manipulated by the single thread running the query it belongs to
public class QueryContext
{
    public static final boolean DISABLE_TIMEOUT = Boolean.getBoolean("cassandra.sai.test.disable.timeout");

    protected final long queryStartTimeNanos;

    public final long executionQuotaNano;

    private long sstablesHit = 0;
    private long segmentsHit = 0;

    /**
     * The partition/row keys that will be used to fetch rows from the base table.
     * They will be either partition keys in AA, or row keys in the later row-aware disk formats.
     */
    private long keysFetched = 0;

    /** The number of live partitions fetched from the storage engine, before post-filtering. */
    private long partitionsFetched = 0;

    /** The number of live partitions returned to the coordinator, after post-filtering. */
    private long partitionsReturned = 0;

    /** The number of deleted partitions that are fetched. */
    private long partitionTombstonesFetched = 0;

    /** The number of live rows fetched from the storage engine, before post-filtering. */
    private long rowsFetched = 0;

    /** The number of live rows returned to the coordinator, after post-filtering. */
    private long rowsReturned = 0;

    /** The number of deleted individual rows or ranges of rows that are fetched. */
    private long rowTombstonesFetched = 0;

    private long trieSegmentsHit = 0;

    private long bkdPostingListsHit = 0;
    private long bkdSegmentsHit = 0;

    private long bkdPostingsSkips = 0;
    private long bkdPostingsDecodes = 0;

    private long triePostingsSkips = 0;
    private long triePostingsDecodes = 0;

    private long queryTimeouts = 0;

    private long annGraphSearchLatency = 0;

    private float annRerankFloor = 0.0f; // only called from single-threaded setup code

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
        this.queryStartTimeNanos = MonotonicClock.approxTime.now();
    }

    public long totalQueryTimeNs()
    {
        return MonotonicClock.approxTime.now() - queryStartTimeNanos;
    }

    // setters
    public void addSstablesHit(long val)
    {
        sstablesHit += val;
    }

    public void addSegmentsHit(long val) {
        segmentsHit += val;
    }

    public void addKeysFetched(long val)
    {
        keysFetched += val;
    }

    public void addPartitionsFetched(long val)
    {
        partitionsFetched += val;
    }

    public void addPartitionsReturned(long val)
    {
        partitionsReturned += val;
    }

    public void addPartitionTombstonesFetched(long val)
    {
        partitionTombstonesFetched += val;
    }

    public void addRowsFetched(long val)
    {
        rowsFetched += val;
    }

    public void addRowsReturned(long val)
    {
        rowsReturned += val;
    }

    public void addRowTombstonesFetched(long val)
    {
        rowTombstonesFetched += val;
    }

    public void addTrieSegmentsHit(long val)
    {
        trieSegmentsHit += val;
    }

    public void addBkdPostingListsHit(long val)
    {
        bkdPostingListsHit += val;
    }

    public void addBkdSegmentsHit(long val)
    {
        bkdSegmentsHit += val;
    }

    public void addBkdPostingsSkips(long val)
    {
        bkdPostingsSkips += val;
    }

    public void addBkdPostingsDecodes(long val)
    {
        bkdPostingsDecodes += val;
    }

    public void addTriePostingsSkips(long val)
    {
        triePostingsSkips += val;
    }

    public void addTriePostingsDecodes(long val)
    {
        triePostingsDecodes += val;
    }

    public void addQueryTimeouts(long val)
    {
        queryTimeouts += val;
    }

    public void addAnnGraphSearchLatency(long val)
    {
        annGraphSearchLatency += val;
    }

    public void setFilterSortOrder(FilterSortOrder filterSortOrder)
    {
        this.filterSortOrder = filterSortOrder;
    }

    // getters

    public long sstablesHit()
    {
        return sstablesHit;
    }

    public long segmentsHit()
    {
        return segmentsHit;
    }

    public long keysFetched()
    {
        return keysFetched;
    }

    public long partitionsFetched()
    {
        return partitionsFetched;
    }

    public long partitionsReturned()
    {
        return partitionsReturned;
    }

    public long partitionTombstonesFetched()
    {
        return partitionTombstonesFetched;
    }

    public long rowsFetched()
    {
        return rowsFetched;
    }

    public long rowsReturned()
    {
        return rowsReturned;
    }

    public long rowTombstonesFetched()
    {
        return rowTombstonesFetched;
    }

    public long trieSegmentsHit()
    {
        return trieSegmentsHit;
    }

    public long bkdPostingListsHit()
    {
        return bkdPostingListsHit;
    }

    public long bkdSegmentsHit()
    {
        return bkdSegmentsHit;
    }

    public long bkdPostingsSkips()
    {
        return bkdPostingsSkips;
    }

    public long bkdPostingsDecodes()
    {
        return bkdPostingsDecodes;
    }

    public long triePostingsSkips()
    {
        return triePostingsSkips;
    }

    public long triePostingsDecodes()
    {
        return triePostingsDecodes;
    }

    public long queryTimeouts()
    {
        return queryTimeouts;
    }

    public long annGraphSearchLatency()
    {
        return annGraphSearchLatency;
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

    public float getAnnRerankFloor()
    {
        return annRerankFloor;
    }

    public void updateAnnRerankFloor(float observedFloor)
    {
        if (observedFloor < Float.POSITIVE_INFINITY)
            annRerankFloor = max(annRerankFloor, observedFloor);
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
        public final long keysFetched;
        public final long partitionsFetched;
        public final long partitionsReturned;
        public final long partitionTombstonesFetched;
        public final long rowsFetched;
        public final long rowsReturned;
        public final long rowTombstonesFetched;
        public final long trieSegmentsHit;
        public final long bkdPostingListsHit;
        public final long bkdSegmentsHit;
        public final long bkdPostingsSkips;
        public final long bkdPostingsDecodes;
        public final long triePostingsSkips;
        public final long triePostingsDecodes;
        public final long queryTimeouts;
        public final long annGraphSearchLatency;
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
            keysFetched = context.keysFetched();
            partitionsFetched = context.partitionsFetched();
            partitionsReturned = context.partitionsReturned();
            partitionTombstonesFetched = context.partitionTombstonesFetched();
            rowsFetched = context.rowsFetched();
            rowsReturned = context.rowsReturned();
            rowTombstonesFetched = context.rowTombstonesFetched();
            trieSegmentsHit = context.trieSegmentsHit();
            bkdPostingListsHit = context.bkdPostingListsHit();
            bkdSegmentsHit = context.bkdSegmentsHit();
            bkdPostingsSkips = context.bkdPostingsSkips();
            bkdPostingsDecodes = context.bkdPostingsDecodes();
            triePostingsSkips = context.triePostingsSkips();
            triePostingsDecodes = context.triePostingsDecodes();
            queryTimeouts = context.queryTimeouts();
            annGraphSearchLatency = context.annGraphSearchLatency();
            filterSortOrder = context.filterSortOrder();
        }
    }
}
