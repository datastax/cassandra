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
import java.util.Iterator;
import java.util.List;

import com.google.common.collect.Iterators;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.AbstractUnfilteredRowIterator;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.exceptions.RequestTimeoutException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.SSTableIndex;
import org.apache.cassandra.index.sai.metrics.TableQueryMetrics;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.AbstractIterator;

public class StorageAttachedIndexSearcher implements Index.Searcher
{
    private final QueryController controller;
    private final QueryContext queryContext;
    private final PrimaryKey.PrimaryKeyFactory keyFactory;

    public StorageAttachedIndexSearcher(ColumnFamilyStore cfs,
                                        TableQueryMetrics tableQueryMetrics,
                                        ReadCommand command,
                                        List<RowFilter.Expression> expressions,
                                        long executionQuotaMs)
    {
        this.queryContext = new QueryContext(executionQuotaMs);
        this.controller = new QueryController(cfs, command, expressions, queryContext, tableQueryMetrics);
        this.keyFactory = PrimaryKey.factory(cfs.metadata.get());
    }

    @Override
    public UnfilteredPartitionIterator search(ReadExecutionController executionController) throws RequestTimeoutException
    {
        return new ResultRetriever(analyze(), controller, executionController, queryContext, keyFactory);
    }

    /**
     * Converts expressions into filter tree and reference {@link SSTableIndex}s used for query.
     *
     * @return operation
     */
    private Operation analyze()
    {
        return Operation.initTreeBuilder(controller).complete();
    }

    /**
     * Converts expressions into filter tree (which is currently just a single AND).
     *
     * Filter tree allows us to do a couple of important optimizations
     * namely, group flattening for AND operations (query rewrite), expression bounds checks,
     * "satisfies by" checks for resulting rows with an early exit.
     *
     * @return root of the filter tree.
     */
    //TODO How does this get applied in OS
    private FilterTree analyzeFilter()
    {
        return Operation.initTreeBuilder(controller).completeFilter();
    }

    private static class ResultRetriever extends AbstractIterator<UnfilteredRowIterator> implements UnfilteredPartitionIterator
    {
        private final PrimaryKey startPrimaryKey;
        private final PrimaryKey lastPrimaryKey;
        private final Iterator<DataRange> keyRanges;
        private AbstractBounds<PartitionPosition> current;

        private final Operation operation;
        private final QueryController controller;
        private final ReadExecutionController executionController;
        private final QueryContext queryContext;
        private final PrimaryKey.PrimaryKeyFactory keyFactory;

        private PrimaryKey currentKey = null;
        private PrimaryKey lastKey;

        private ResultRetriever(Operation operation,
                                QueryController controller,
                                ReadExecutionController executionController,
                                QueryContext queryContext,
                                PrimaryKey.PrimaryKeyFactory keyFactory)
        {
            this.keyRanges = controller.dataRanges().iterator();
            this.current = keyRanges.next().keyRange();

            this.operation = operation;
            this.controller = controller;
            this.executionController = executionController;
            this.queryContext = queryContext;
            this.keyFactory = keyFactory;

            //TODO These ought to take the decorated keys in which case we ought to be able to
            //be able to get a sstable row id for them from the primary key map
            this.startPrimaryKey = keyFactory.createKey(controller.mergeRange().left.getToken());
            this.lastPrimaryKey = keyFactory.createKey(controller.mergeRange().right.getToken());
       }

        @Override
        public UnfilteredRowIterator computeNext()
        {
            if (operation == null)
                return endOfData();

            operation.skipTo(startPrimaryKey);
            if (!operation.hasNext())
                return endOfData();
            currentKey = operation.next();

            // IMPORTANT: The correctness of the entire query pipeline relies on the fact that we consume a token
            // and materialize its keys before moving on to the next token in the flow. This sequence must not be broken
            // with toList() or similar. (Both the union and intersection flow constructs, to avoid excessive object
            // allocation, reuse their token mergers as they process individual positions on the ring.)
            while (true)
            {
                if (!lastPrimaryKey.partitionKey().getToken().isMinimum() && lastPrimaryKey.compareTo(currentKey) < 0)
                    return endOfData();

                while (current != null)
                {
                    if (current.contains(currentKey.partitionKey()))
                    {
                        UnfilteredRowIterator partition = apply(currentKey);
                        if (partition != null)
                            return partition;
                        break;
                    }
                    // bigger than current range
                    else if (!current.right.isMinimum() && current.right.compareTo(currentKey.partitionKey()) <= 0)
                    {
                        if (keyRanges.hasNext())
                            current = keyRanges.next().keyRange();
                        else
                            return endOfData();
                    }
                    // smaller than current range
                    else
                    {
                        // we already knew that key is not included in "current" abstract bounds,
                        // so "left" may have the same partition position as "key" when "left" is exclusive.
                        assert current.left.compareTo(currentKey.partitionKey()) >= 0;
                        operation.skipTo(keyFactory.createKey(current.left.getToken()));
                        break;
                    }
                }
                if (!operation.hasNext())
                    return endOfData();
                currentKey = operation.next();
            }
        }

        public UnfilteredRowIterator apply(PrimaryKey key)
        {
            if ((lastKey != null && lastKey.compareTo(key) == 0) || !controller.needsRow(key))
            {
                return null;
            }

            lastKey = key;

            // SPRC should only return UnfilteredRowIterator, but it returns UnfilteredPartitionIterator due to Flow.
            try (UnfilteredRowIterator partition = controller.getPartition(key, executionController))
            {
                queryContext.partitionsRead++;

                return applyIndexFilter(key, partition, operation.filterTree, queryContext);
            }
        }

        private static UnfilteredRowIterator applyIndexFilter(PrimaryKey key, UnfilteredRowIterator partition, FilterTree tree, QueryContext queryContext)
        {
            Row staticRow = partition.staticRow();
            List<Unfiltered> clusters = new ArrayList<>();

            while (partition.hasNext())
            {
                Unfiltered row = partition.next();

                queryContext.rowsFiltered++;
                if (tree.satisfiedBy(key.partitionKey(), row, staticRow))
                    clusters.add(row);
            }

            if (clusters.isEmpty())
            {
                queryContext.rowsFiltered++;
                if (tree.satisfiedBy(key.partitionKey(), staticRow, staticRow))
                    clusters.add(staticRow);
            }

            /*
             * If {@code clusters} is empty, which means either all clustering row and static row pairs failed,
             *       or static row and static row pair failed. In both cases, we should not return any partition.
             * If {@code clusters} is not empty, which means either there are some clustering row and static row pairs match the filters,
             *       or static row and static row pair matches the filters. In both cases, we should return a partition with static row,
             *       and remove the static row marker from the {@code clusters} for the latter case.
             */
            if (clusters.isEmpty())
                return null;

            return new PartitionIterator(partition, staticRow, Iterators.filter(clusters.iterator(), u -> !((Row)u).isStatic()));
        }

        private static class PartitionIterator extends AbstractUnfilteredRowIterator
        {
            private final Iterator<Unfiltered> rows;

            public PartitionIterator(UnfilteredRowIterator partition, Row staticRow, Iterator<Unfiltered> content)
            {
                super(partition.metadata(),
                      partition.partitionKey(),
                      partition.partitionLevelDeletion(),
                      partition.columns(),
                      staticRow,
                      partition.isReverseOrder(),
                      partition.stats());

                rows = content;
            }

            @Override
            protected Unfiltered computeNext()
            {
                return rows.hasNext() ? rows.next() : endOfData();
            }
        }

        @Override
        public TableMetadata metadata()
        {
            return controller.metadata();
        }

        public void close()
        {
            FileUtils.closeQuietly(operation);
            controller.finish();
        }
    }
}
