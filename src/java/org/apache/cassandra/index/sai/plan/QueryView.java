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

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import org.apache.cassandra.io.sstable.Descriptor;
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
         * Denotes a situation when there exist no index for an active memtable or sstable.
         * This can happen e.g. when the index gets dropped while running the query.
         */
        static class MissingIndexException extends RuntimeException
        {
            final IndexContext context;

            public MissingIndexException(IndexContext context, String message)
            {
                super(message);
                this.context = context;
            }
        }

        /**
         * Acquire references to all the memtables, memtable indexes, sstables, and sstable indexes required for the
         * given expression.
         */
        protected QueryView build() throws MissingIndexException
        {
            var referencedIndexes = new HashSet<SSTableIndex>();
            ColumnFamilyStore.RefViewFragment refViewFragment = null;

            // We must use the canonical view in order for the equality check for source sstable/memtable
            // to work correctly.
            var filter = RangeUtil.coversFullRing(range)
                         ? View.selectFunction(SSTableSet.CANONICAL)
                         : View.select(SSTableSet.CANONICAL, s -> RangeUtil.intersects(s, range));


            try
            {
                // Keeps track of which memtables we've already tried to match the index to.
                // If we fail to match the index to the memtable for the first time, we have to retry
                // because the memtable could be flushed and its index removed between the moment we
                // got the view and the moment we did the lookup.
                // If we get the same memtable in the view again, and there is no index,
                // then the missing index is not due to a concurrent modification, but it doesn't contain indexed
                // data, so we can ignore it.
                var processedMemtables = new HashSet<Memtable>();


                var start = MonotonicClock.approxTime.now();
                Memtable unmatchedMemtable = null;
                Descriptor unmatchedSStable = null;

                // This loop will spin only if there is a mismatch between the view managed by IndexViewManager
                // and the view managed by Cassandra Tracker. Such a mismatch can happen at the moment when
                // the sstable or memtable sets are updated, e.g. on flushes or compactions. The mismatch
                // should last only until all Tracker notifications get processed by SAI
                // (which doesn't involve I/O and should be very fast). We expect the mismatch to resolve in order
                // of nanoceconds, but the timeout is large enough just in case of unpredictable performance hiccups.
                outer:
                while (!MonotonicClock.approxTime.isAfter(start + TimeUnit.MILLISECONDS.toNanos(2000)))
                {
                    // cleanup after the previous iteration if we're retrying
                    release(referencedIndexes);
                    release(refViewFragment);

                    // Prevent exceeding the query timeout
                    queryContext.checkpoint();

                    // Lock a consistent view of memtables and sstables.
                    // A consistent view is required for correctness of order by and vector queries.
                    refViewFragment = cfs.selectAndReference(filter);
                    var indexView = indexContext.getView();

                    // Lookup the indexes corresponding to memtables:
                    var memtableIndexes = new HashSet<MemtableIndex>();
                    for (Memtable memtable : refViewFragment.memtables)
                    {
                        // Empty memtables have no index but that's not a problem, we can ignore them.
                        if (memtable.getLiveDataSize() == 0)
                            continue;

                        MemtableIndex index = indexContext.getMemtableIndex(memtable);
                        if (index != null)
                        {
                            memtableIndexes.add(index);
                        }
                        else if (indexContext.isDropped())
                        {
                            // Index was dropped deliberately by the user.
                            // We cannot recover here.
                            refViewFragment.release();
                            throw new MissingIndexException(indexContext, "Index " + indexContext.getIndexName() +
                                                                          " not found for memtable: " + memtable);
                        }
                        else if (!processedMemtables.contains(memtable))
                        {
                            // We can end up here if a flush happened right after we referenced the refViewFragment
                            // but before looking up the memtable index.
                            // In that case, we need to retry with the updated view
                            // (we expect the updated view to not contain this memtable).

                            // Remember this metable to protect from infinite looping in case we have a permanent
                            // inconsistency between the index set and the memtable set.
                            processedMemtables.add(memtable);

                            unmatchedMemtable = memtable;
                            continue outer;
                        }
                        // If the memtable was non-empty, the index context hasn't been dropped, but the
                        // index doesn't exist on the second attempt, then his means there is no indexed data
                        // in that memtable. In this case we just continue without it.
                        // Memtable indexes are created lazily, on the first insert, therefore a missing index
                        // is a normal situation.
                    }

                    // Lookup and reference the indexes corresponding to the sstables:
                    for (SSTableReader sstable : refViewFragment.sstables)
                    {
                        // If the IndexViewManager never saw this sstable, then we need to spin.
                        // Let's hope in the next iteration we get the indexView based on the same sstable set
                        // as the refViewFragment.
                        if (!indexView.containsSSTable(sstable))
                        {
                            if (MonotonicClock.approxTime.isAfter(start + 100))
                                NoSpamLogger.log(logger, NoSpamLogger.Level.WARN, 1, TimeUnit.SECONDS,
                                                 "Spinning trying to get the index for sstable {} because index view is out of sync", sstable.descriptor);

                            unmatchedSStable = sstable.descriptor;
                            continue outer;
                        }

                        SSTableIndex index = indexView.getSSTableIndex(sstable.descriptor);

                        // The IndexViewManager got the update about this sstable, but there is no index for the sstable
                        // (e.g. index was dropped or got corrupt, etc.). In this case retrying won't fix it.
                        if (index == null)
                            throw new MissingIndexException(indexContext, "Index " + indexContext.getIndexName() +
                                                                          " not found for sstable: " + sstable.descriptor);

                        if (!indexInRange(index))
                            continue;

                        // It is unlikely but possible the index got unreferenced just between the moment we grabbed the
                        // refViewFragment and getting here. In that case we won't be able to reference it and we have
                        // to retry.
                        if (!index.reference())
                        {

                            if (MonotonicClock.approxTime.isAfter(start + 100))
                                NoSpamLogger.log(logger, NoSpamLogger.Level.WARN, 1, TimeUnit.SECONDS,
                                                 "Spinning trying to get the index for sstable {} because index was released", sstable.descriptor);

                            unmatchedSStable = sstable.descriptor;
                            continue outer;
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


                if (unmatchedMemtable != null)
                    throw new MissingIndexException(indexContext, "Index " + indexContext.getIndexName() +
                                                                  " not found for memtable " + unmatchedMemtable);
                if (unmatchedSStable != null)
                    throw new MissingIndexException(indexContext, "Index " + indexContext.getIndexName() +
                                                                  " not found for sstable " + unmatchedSStable);

                // This should be unreachable, because whenever we retry, we always set unmatchedMemtable
                // or unmatchedSSTable, so we'd log a better message above.
                throw new MissingIndexException(indexContext, "Failed to build QueryView for index " + indexContext.getIndexName());
            }
            catch (MissingIndexException e)
            {
                release(referencedIndexes);
                release(refViewFragment);
                throw e;
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

        private void release(ColumnFamilyStore.RefViewFragment refViewFragment)
        {
            if (refViewFragment != null)
                refViewFragment.release();
        }

        private void release(Collection<SSTableIndex> indexes)
        {
            for (var index : indexes)
                index.release();
            indexes.clear();
        }

        // I've removed the concept of "most selective index" since we don't actually have per-sstable
        // statistics on that; it looks like it was only used to check bounds overlap, so computing
        // an actual global bounds should be an improvement.  But computing global bounds as an intersection
        // of individual bounds is messy because you can end up with more than one range.
        private boolean indexInRange(SSTableIndex index)
        {
            SSTableReader sstable = index.getSSTable();
            if (range instanceof Bounds && range.left.equals(range.right) && (!range.left.isMinimum()) && range.left instanceof DecoratedKey)
            {
                if (!sstable.getBloomFilter().isPresent((DecoratedKey)range.left))
                    return false;
            }
            return range.left.compareTo(sstable.last) <= 0 && (range.right.isMinimum() || sstable.first.compareTo(range.right) <= 0);
        }
    }
}
