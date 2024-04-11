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

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.index.sai.utils.AbortedOperationException;

/**
 * Tracks state relevant to the execution of a single query, including metrics and timeout monitoring.
 */
@NotThreadSafe
public class QueryContext
{
    private static final boolean DISABLE_TIMEOUT = Boolean.getBoolean("cassandra.sai.test.disable.timeout");

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

    private final LongAdder annNodesVisited = new LongAdder();

    private final LongAdder shadowedPrimaryKeyCount = new LongAdder();

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
        this.queryStartTimeNanos = System.nanoTime();
    }

    public long totalQueryTimeNs()
    {
        return System.nanoTime() - queryStartTimeNanos;
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
    public void addAnnNodesVisited(long val)
    {
        annNodesVisited.add(val);
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
    public long annNodesVisited()
    {
        return annNodesVisited.longValue();
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

    /**
     * Determines the order of filtering and sorting operations.
     * Currently used only by vector search.
     */
    public enum FilterSortOrder
    {
        /** First get the matching keys from the non-vector indexes, then use vector index to sort them */
        FILTER_THEN_SORT,

        /** First get the candidates in ANN order from the vector index, then fetch the rows and post-filter them */
        SORT_THEN_FILTER
    }
}
