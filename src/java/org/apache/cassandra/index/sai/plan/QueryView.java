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

package org.apache.cassandra.index.sai.plan;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.googlecode.concurrenttrees.common.Iterables;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.SSTableIndex;
import org.apache.cassandra.index.sai.memory.MemtableIndex;
import org.apache.cassandra.index.sai.utils.RangeUtil;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.MonotonicClock;
import org.apache.cassandra.utils.NoSpamLogger;

public class QueryView implements AutoCloseable
{
    final ColumnFamilyStore.RefViewFragment view;
    final Set<SSTableIndex> referencedIndexes;
    final Set<MemtableIndex> memtableIndexes;
    final IndexContext indexContext;

    public QueryView(ColumnFamilyStore.RefViewFragment view,
                     Set<SSTableIndex> referencedIndexes,
                     Set<MemtableIndex> memtableIndexes,
                     IndexContext indexContext)
    {

        this.view = view;
        this.referencedIndexes = referencedIndexes;
        this.memtableIndexes = memtableIndexes;
        this.indexContext = indexContext;
    }

    @Override
    public void close()
    {
        view.release();
        referencedIndexes.forEach(SSTableIndex::release);
    }

    /**
     * Returns the total count of rows in all sstables in this view
     */
    public long getTotalSStableRows()
    {
        return view.sstables.stream().mapToLong(SSTableReader::getTotalRows).sum();
    }

    /**
     * Build a query specific view of the memtables, sstables, and indexes for a query.
     * For use with SAI ordered queries to ensure that the view is consistent over the lifetime of the query,
     * which is particularly important for validation of a cell's source memtable/sstable.
     */
    static class Builder
    {
        private static final Logger logger = LoggerFactory.getLogger(Builder.class);

        private final ColumnFamilyStore cfs;
        private final IndexContext indexContext;
        private final AbstractBounds<PartitionPosition> range;
        private final QueryContext queryContext;

        Builder(IndexContext indexContext, AbstractBounds<PartitionPosition> range, QueryContext queryContext)
        {
            this.cfs = indexContext.columnFamilyStore();
            this.indexContext = indexContext;
            this.range = range;
            this.queryContext = queryContext;
        }

        /**
         * Acquire references to all the memtables, memtable indexes, sstables, and sstable indexes required for the
         * given expression.
         */
        protected QueryView build()
        {
            var referencedIndexes = new HashSet<SSTableIndex>();

            // We must use the canonical view in order for the equality check for source sstable/memtable
            // to work correctly.
            var filter = RangeUtil.coversFullRing(range)
                         ? View.selectFunction(SSTableSet.CANONICAL)
                         : View.select(SSTableSet.CANONICAL, s -> RangeUtil.intersects(s, range));


            try
            {
                // Keeps track of which memtables we've already tried to match the index to.
                // If we fail to match the index to the memtable for the first time, this is not necessarily an
                // error, because the memtable could be flushed and its index removed between the moment we
                // got the view and the moment we did the lookup.
                // However, if we get the same memtable in the view again, and there is no index,
                // then the missing index is not due to a concurrent modification but likely a bug, so
                // we log an error and break the loop. This makes debugging easier (infinite looping is much worse
                // than erroring out).
                var processedMemtables = new HashSet<Memtable>();

                outer:
                while (true)
                {
                    // Prevent an infinite loop
                    queryContext.checkpoint();

                    // Lock a consistent view of memtables and sstables.
                    // A consistent view is required for correctness of order by and vector queries.
                    var refViewFragment = cfs.selectAndReference(filter);

                    // Lookup the indexes corresponding to memtables:
                    var memtableIndexes = new HashSet<MemtableIndex>();

                    for (Memtable memtable : refViewFragment.memtables)
                    {
                        // Empty memtables have no index but that's not a problem, we can ignore them.
                        if (memtable.isClean())
                            continue;

                        MemtableIndex index = indexContext.getMemtableIndex(memtable);
                        if (index == null)
                        {
                            // We can end up here if a flush happened right after we referenced the refViewFragment
                            // but before looking up the memtable index.
                            // In that case, we need to retry with the updated view
                            // (we expect the updated view to not contain this memtable).
                            refViewFragment.release();

                            if (!processedMemtables.contains(memtable))
                            {
                                // Protect from infinite looping in case we have a permanent inconsistency between
                                // the index set and the memtable set.
                                processedMemtables.add(memtable);
                                continue outer;
                            }
                            else
                            {
                                // So the memtable still exists in the second attempt, but it is not empty
                                // and has no index. Oh, that looks like a bug.
                                throw new IllegalStateException("Index not found for memtable: " + memtable);
                            }
                        }
                        memtableIndexes.add(index);
                    }

                    // Lookup and reference the indexes corresponding to the sstables:
                    for (SSTableReader sstable : refViewFragment.sstables)
                    {
                        SSTableIndex index = indexContext.getSSTableIndex(sstable.descriptor);

                        // If there is no index for given sstable, we don't retry, because we should never
                        // release an index before we release the sstable. The sstables are already referenced
                        // in the refViewFragment, so that protects indexes from being released. Hence, we should
                        // always get an index and if we don't, it is a bug.
                        if (index == null)
                        {
                            refViewFragment.release();
                            throw new IllegalStateException("Index not found for sstable: " + sstable);
                        }
                        if (!index.reference())
                        {
                            refViewFragment.release();
                            throw new IllegalStateException("Index for sstable " + sstable + " found but already released and cannot be referenced");
                        }
                        referencedIndexes.add(index);
                    }

                    // freeze referencedIndexes and memtableIndexes, so we can safely give access to them
                    // without risking something messes them up
                    // (this was added after KeyRangeTermIterator messed them up which led to a bug)
                    return new QueryView(refViewFragment,
                                         Collections.unmodifiableSet(referencedIndexes),
                                         Collections.unmodifiableSet(memtableIndexes),
                                         indexContext);
                }
            }
            finally
            {
                if (Tracing.isTracing())
                {
                    var groupedIndexes = referencedIndexes.stream().collect(
                    Collectors.groupingBy(i -> i.getIndexContext().getIndexName(), Collectors.counting()));
                    var summary = groupedIndexes.entrySet().stream()
                                                .map(e -> String.format("%s (%s sstables)", e.getKey(), e.getValue()))
                                                .collect(Collectors.joining(", "));
                    Tracing.trace("Querying storage-attached indexes {}", summary);
                }
            }
        }
    }
}
