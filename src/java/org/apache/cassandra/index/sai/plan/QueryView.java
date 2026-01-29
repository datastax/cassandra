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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SSTableIndex;
import org.apache.cassandra.index.sai.memory.MemtableIndex;
import org.apache.cassandra.index.sai.view.View;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReaderWithFilter;
import org.apache.cassandra.tracing.Tracing;


public class QueryView implements AutoCloseable
{
    final View saiView;
    final ColumnFamilyStore.ViewFragment viewFragment;
    public final Set<SSTableIndex> sstableIndexes;
    public final Set<MemtableIndex> memtableIndexes;

    public QueryView(View saiView,
                     ColumnFamilyStore.ViewFragment viewFragment,
                     Set<SSTableIndex> sstableIndexes,
                     Set<MemtableIndex> memtableIndexes)
    {
        this.saiView = saiView;
        this.viewFragment = viewFragment;
        this.sstableIndexes = sstableIndexes;
        this.memtableIndexes = memtableIndexes;
    }

    @Override
    public void close()
    {
        saiView.release();
    }

    /**
     * Returns the total count of rows in all sstables in this view
     */
    public long getTotalSStableRows()
    {
        return viewFragment.sstables.stream().mapToLong(SSTableReader::getTotalRows).sum();
    }

    /**
     * Build a query specific view of the memtables, sstables, and indexes for a query.
     * For use with SAI ordered queries to ensure that the view is consistent over the lifetime of the query,
     * which is particularly important for validation of a cell's source memtable/sstable.
     */
    static class Builder
    {
        private static final Logger logger = LoggerFactory.getLogger(Builder.class);

        private final IndexContext indexContext;
        private final AbstractBounds<PartitionPosition> range;

        Builder(IndexContext indexContext, AbstractBounds<PartitionPosition> range)
        {
            this.indexContext = indexContext;
            this.range = range;
        }

        /**
         * Denotes a situation when there exist no index for an active memtable or sstable.
         * This can happen e.g. when the index gets dropped while running the query.
         */
        static class MissingIndexException extends RuntimeException
        {
            final boolean isDropped;
            final String indexName;

            private MissingIndexException(IndexContext context)
            {
                super();
                this.isDropped = context.isDropped();
                this.indexName = context.getIndexName();
            }

            @Override
            public String getMessage()
            {
                return isDropped ? "Index " + indexName + " was dropped."
                                 : "Unable to acquire lock on index view: " + indexName + '.';
            }
        }

        /**
         * Acquire references to all the memtables, memtable indexes, sstables, and sstable indexes required for the
         * given expression.
         */
        protected QueryView build() throws MissingIndexException
        {
            var sstableIndexes = new HashSet<SSTableIndex>();
            View saiView = null;
            try
            {
                // Get memtables first in case we are in the middle of flushing one.
                // Note that we get the memtables from the index context, which is updated via notifications after
                // the index context's view is updated, which guarantees a complete and correct view of the table in
                // favor of possibly duplicated search on a recently flushed memtable and its corresponding sstable.
                var memtableIndexes = new HashSet<>(indexContext.getLiveMemtables().values());
                // This is an atomic operation to get an already referenced view of all current local indexes for the table
                saiView = indexContext.getReferencedView(TimeUnit.SECONDS.toNanos(5));
                if (saiView == null)
                    throw new MissingIndexException(indexContext);

                // Now that we referenced a view, need to confirm that the view we referenced isn't somehow invalid.
                if (!indexContext.isIndexed())
                    throw new MissingIndexException(indexContext);

                var sstableReaders = new ArrayList<SSTableReader>(saiView.size());
                // These are already referenced because they are referenced by the same view we just referenced.
                for (var index : saiView.getIndexes())
                {
                    if (!indexInRange(index))
                        continue;
                    sstableIndexes.add(index);
                    sstableReaders.add(index.getSSTable());
                }

                var memtables = new ArrayList<Memtable>(memtableIndexes.size());
                for (var index : memtableIndexes)
                {
                    var memtable = index.getMemtable();
                    memtables.add(memtable);
                }

                var viewFragment = new ColumnFamilyStore.ViewFragment(sstableReaders, memtables);
                return new QueryView(saiView, viewFragment, sstableIndexes, memtableIndexes);
            }
            catch (Exception e)
            {
                if (saiView != null)
                    saiView.release();
                throw e;
            }
            finally
            {
                if (Tracing.isTracing())
                {
                    var groupedIndexes = sstableIndexes.stream().collect(
                    Collectors.groupingBy(i -> i.getIndexContext().getIndexName(), Collectors.counting()));
                    var summary = groupedIndexes.entrySet().stream()
                                                .map(e -> String.format("%s (%s sstables)", e.getKey(), e.getValue()))
                                                .collect(Collectors.joining(", "));
                    Tracing.trace("Querying storage-attached indexes {}", summary);
                }
            }
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
                if (!((SSTableReaderWithFilter) sstable).getFilter().isPresent((DecoratedKey)range.left))
                    return false;
            }
            return range.left.compareTo(sstable.last) <= 0 && (range.right.isMinimum() || sstable.first.compareTo(range.right) <= 0);
        }
    }
}
