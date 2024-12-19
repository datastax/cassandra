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
         * <p>
         * Will retry if the active sstables change concurrently.
         */
        protected QueryView build()
        {
            var referencedIndexes = new HashSet<SSTableIndex>();
            long failingSince = -1L;
            try
            {
                outer:
                while (true)
                {
                    // Prevent an infinite loop
                    queryContext.checkpoint();

                    // Acquire live memtable index and memtable references first to avoid missing an sstable due to flush.
                    // Copy the memtable indexes to avoid concurrent modification.
                    var memtableIndexes = new HashSet<>(indexContext.getLiveMemtables().values());

                    // We must use the canonical view in order for the equality check for source sstable/memtable
                    // to work correctly.
                    var filter = RangeUtil.coversFullRing(range)
                                 ? View.selectFunction(SSTableSet.CANONICAL)
                                 : View.select(SSTableSet.CANONICAL, s -> RangeUtil.intersects(s, range));
                    var refViewFragment = cfs.selectAndReference(filter);
                    var memtables = Iterables.toSet(refViewFragment.memtables);
                    // Confirm that all the memtables associated with the memtable indexes we already have are still live.
                    // There might be additional memtables that are not associated with the expression because tombstones
                    // are not indexed.
                    for (MemtableIndex memtableIndex : memtableIndexes)
                    {
                        if (!memtables.contains(memtableIndex.getMemtable()))
                        {
                            refViewFragment.release();
                            continue outer;
                        }
                    }

                    Set<SSTableIndex> indexes = getIndexesForExpression(indexContext);
                    // Attempt to reference each of the indexes, and thn confirm that the sstable associated with the index
                    // is in the refViewFragment. If it isn't in the refViewFragment, we will get incorrect results, so
                    // we release the indexes and refViewFragment and try again.
                    for (SSTableIndex index : indexes)
                    {
                        var success = index.reference();
                        if (success)
                            referencedIndexes.add(index);

                        if (!success || !refViewFragment.sstables.contains(index.getSSTable()))
                        {
                            referencedIndexes.forEach(SSTableIndex::release);
                            referencedIndexes.clear();
                            refViewFragment.release();

                            // Log about the failures
                            if (failingSince <= 0)
                            {
                                failingSince = MonotonicClock.approxTime.now();
                            }
                            else if (MonotonicClock.approxTime.now() - failingSince > TimeUnit.MILLISECONDS.toNanos(100))
                            {
                                failingSince = MonotonicClock.approxTime.now();
                                if (success)
                                    NoSpamLogger.log(logger, NoSpamLogger.Level.WARN, 1, TimeUnit.SECONDS,
                                                     "Spinning trying to capture index reader for {}, but it was released.", index);
                                else
                                    NoSpamLogger.log(logger, NoSpamLogger.Level.WARN, 1, TimeUnit.SECONDS,
                                                     "Spinning trying to capture readers for {}, but : {}, ", refViewFragment.sstables, index.getSSTable());
                            }
                            continue outer;
                        }
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

        /**
         * Get the index
         */
        private Set<SSTableIndex> getIndexesForExpression(IndexContext indexContext)
        {
            if (!indexContext.isIndexed())
                throw new IllegalArgumentException("Expression is not indexed");

            // Get all the indexes in the range.
            return indexContext.getView().getIndexes().stream().filter(this::indexInRange).collect(Collectors.toSet());
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
