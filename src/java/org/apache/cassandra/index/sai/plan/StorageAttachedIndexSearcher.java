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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.AbstractUnfilteredRowIterator;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.RequestTimeoutException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.disk.format.IndexFeatureSet;
import org.apache.cassandra.index.sai.iterators.KeyRangeIterator;
import org.apache.cassandra.index.sai.metrics.TableQueryMetrics;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.RangeUtil;
import org.apache.cassandra.index.sai.utils.PrimaryKeyWithSortKey;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;

public class StorageAttachedIndexSearcher implements Index.Searcher
{
    private static final Logger logger = LoggerFactory.getLogger(StorageAttachedIndexSearcher.class);

    private final ReadCommand command;
    private final QueryController controller;
    private final QueryContext queryContext;

    public StorageAttachedIndexSearcher(ColumnFamilyStore cfs,
                                        TableQueryMetrics tableQueryMetrics,
                                        ReadCommand command,
                                        Orderer orderer,
                                        IndexFeatureSet indexFeatureSet,
                                        long executionQuotaMs)
    {
        this.command = command;
        this.queryContext = new QueryContext(executionQuotaMs);
        this.controller = new QueryController(cfs, command, orderer, indexFeatureSet, queryContext, tableQueryMetrics);
    }

    @Override
    public ReadCommand command()
    {
        return command;
    }

    @Override
    @SuppressWarnings("unchecked")
    public UnfilteredPartitionIterator search(ReadExecutionController executionController) throws RequestTimeoutException
    {
        int retries = 0;
        while (true)
        {
            try
            {
                FilterTree filterTree = analyzeFilter();
                Plan plan = controller.buildPlan();
                Iterator<? extends PrimaryKey> keysIterator = controller.buildIterator(plan);

                // Can't check for `command.isTopK()` because the planner could optimize sorting out
                Orderer ordering = plan.ordering();
                if (ordering != null)
                {
                    assert !(keysIterator instanceof KeyRangeIterator);
                    var scoredKeysIterator = (CloseableIterator<PrimaryKeyWithSortKey>) keysIterator;
                    var result = new ScoreOrderedResultRetriever(scoredKeysIterator, filterTree, controller,
                                                                 executionController, queryContext, command.limits().count());
                    return (UnfilteredPartitionIterator) new TopKProcessor(command).filter(result);
                }
                else
                {
                    assert keysIterator instanceof KeyRangeIterator;
                    return new ResultRetriever((KeyRangeIterator) keysIterator, filterTree, controller, executionController, queryContext);
                }
            }
            catch (QueryView.Builder.MissingIndexException e)
            {
                // If an index was dropped while we were preparing the plan or between preparing the plan
                // and creating the result retriever, we can retry without that index,
                // because there may be other indexes that could be used to run the query.
                // And if there are no good indexes left, we'd get a good contextual request error message.
                if (e.context.isDropped() && retries < 8)
                {
                    logger.debug("Index " + e.context.getIndexName() + " dropped while preparing the query plan. Retrying.");
                    retries++;
                    continue;
                }

                // If we end up here, this is either a bug or a problem with an index (corrupted / missing components?).
                controller.abort();
                logger.error("Index not found", e);
                throw invalidRequest("Index missing or corrupt: " + e.context.getIndexName());
            }
            catch (Throwable t)
            {
                controller.abort();
                throw t;
            }
        }
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
    private FilterTree analyzeFilter()
    {
        return controller.buildFilter();
    }

    private static class ResultRetriever extends AbstractIterator<UnfilteredRowIterator> implements UnfilteredPartitionIterator
    {
        private final PrimaryKey firstPrimaryKey;
        private final Iterator<DataRange> keyRanges;
        private AbstractBounds<PartitionPosition> currentKeyRange;

        private final KeyRangeIterator operation;
        private final FilterTree filterTree;
        private final QueryController controller;
        private final ReadExecutionController executionController;
        private final QueryContext queryContext;
        private final PrimaryKey.Factory keyFactory;

        private PrimaryKey lastKey;

        private ResultRetriever(KeyRangeIterator operation,
                                FilterTree filterTree,
                                QueryController controller,
                                ReadExecutionController executionController,
                                QueryContext queryContext)
        {
            this.keyRanges = controller.dataRanges().iterator();
            this.currentKeyRange = keyRanges.next().keyRange();

            this.operation = operation;
            this.filterTree = filterTree;
            this.controller = controller;
            this.executionController = executionController;
            this.queryContext = queryContext;
            this.keyFactory = controller.primaryKeyFactory();

            this.firstPrimaryKey = controller.firstPrimaryKey();
        }

        @Override
        public UnfilteredRowIterator computeNext()
        {
            // IMPORTANT: The correctness of the entire query pipeline relies on the fact that we consume a token
            // and materialize its keys before moving on to the next token in the flow. This sequence must not be broken
            // with toList() or similar. (Both the union and intersection flow constructs, to avoid excessive object
            // allocation, reuse their token mergers as they process individual positions on the ring.)

            if (operation == null)
                return endOfData();

            // If being called for the first time, skip to the beginning of the range.
            // We can't put this code in the constructor because it may throw and the caller
            // may not be prepared for that.
            if (lastKey == null)
                operation.skipTo(firstPrimaryKey);

            // Theoretically we wouldn't need this if the caller of computeNext always ran the
            // returned iterators to the completion. Unfortunately, we have no control over the caller behavior here.
            // Hence, we skip to the next partition in order to comply to the unwritten partition iterator contract
            // saying this iterator must not return the same partition twice.
            skipToNextPartition();

            UnfilteredRowIterator iterator = nextRowIterator(this::nextSelectedKeyInRange);
            return iterator != null
                   ? iteratePartition(iterator)
                   : endOfData();
        }

        /**
         * Tries to obtain a row iterator for one of the supplied keys by repeatedly calling
         * {@link ResultRetriever#apply} until it gives a non-null result.
         * The keySupplier should return the next key with every call to get() and
         * null when there are no more keys to try.
         *
         * @return an iterator or null if all keys were tried with no success
         */
        private @Nullable UnfilteredRowIterator nextRowIterator(@Nonnull Supplier<PrimaryKey> keySupplier)
        {
            UnfilteredRowIterator iterator = null;
            while (iterator == null)
            {
                PrimaryKey key = keySupplier.get();
                if (key == null)
                    return null;
                iterator = apply(key);
            }
            return iterator;
        }

        /**
         * Returns the next available key contained by one of the keyRanges.
         * If the next key falls out of the current key range, it skips to the next key range, and so on.
         * If no more keys or no more ranges are available, returns null.
         */
        private @Nullable PrimaryKey nextKeyInRange()
        {
            PrimaryKey key = nextKey();

            while (key != null && !(currentKeyRange.contains(key.partitionKey())))
            {
                if (!currentKeyRange.right.isMinimum() && currentKeyRange.right.compareTo(key.partitionKey()) <= 0)
                {
                    // currentKeyRange before the currentKey so need to move currentKeyRange forward
                    currentKeyRange = nextKeyRange();
                    if (currentKeyRange == null)
                        return null;
                }
                else
                {
                    // the following condition may be false if currentKeyRange.left is not inclusive,
                    // and key == currentKeyRange.left; in this case we should not try to skipTo the beginning
                    // of the range because that would be requesting the key to go backwards
                    // (in some implementations, skipTo can go backwards, and we don't want that)
                    if (currentKeyRange.left.getToken().compareTo(key.token()) > 0)
                    {
                        // key before the current range, so let's move the key forward
                        skipTo(currentKeyRange.left.getToken());
                    }
                    key = nextKey();
                }
            }
            return key;
        }

        /**
         * Returns the next available key contained by one of the keyRanges and selected by the queryController.
         * If the next key falls out of the current key range, it skips to the next key range, and so on.
         * If no more keys acceptd by the controller are available, returns null.
         */
         private @Nullable PrimaryKey nextSelectedKeyInRange()
        {
            PrimaryKey key;
            do
            {
                key = nextKeyInRange();
            }
            while (key != null && !controller.selects(key));
            return key;
        }

        /**
         * Retrieves the next primary key that belongs to the given partition and is selected by the query controller.
         * The underlying key iterator is advanced only if the key belongs to the same partition.
         * <p>
         * Returns null if:
         * <ul>
         *   <li>there are no more keys</li>
         *   <li>the next key is beyond the upper bound</li>
         *   <li>the next key belongs to a different partition</li>
         * </ul>
         * </p>
         */
        private @Nullable PrimaryKey nextSelectedKeyInPartition(DecoratedKey partitionKey)
        {
            PrimaryKey key;
            do
            {
                if (!operation.hasNext())
                    return null;
                PrimaryKey minKey = operation.peek();
                if (!minKey.token().equals(partitionKey.getToken()))
                    return null;
                if (minKey.partitionKey() != null && !minKey.partitionKey().equals(partitionKey))
                    return null;

                key = nextKey();
            }
            while (key != null && !controller.selects(key));
            return key;
        }

        /**
         * Gets the next key from the underlying operation.
         * Returns null if there are no more keys <= lastPrimaryKey.
         */
        private @Nullable PrimaryKey nextKey()
        {
            return operation.hasNext() ? operation.next() : null;
        }

        /**
         * Gets the next key range from the underlying range iterator.
         */
        private @Nullable AbstractBounds<PartitionPosition> nextKeyRange()
        {
            return keyRanges.hasNext() ? keyRanges.next().keyRange() : null;
        }

        /**
         * Convenience function to skip to a given token.
         */
        private void skipTo(@Nonnull Token token)
        {
            operation.skipTo(keyFactory.createTokenOnly(token));
        }

        /**
         * Skips to the key that belongs to a different partition than the last key we fetched.
         */
        private void skipToNextPartition()
        {
            if (lastKey == null)
                return;
            DecoratedKey lastPartitionKey = lastKey.partitionKey();
            while (operation.hasNext() && operation.peek().partitionKey().equals(lastPartitionKey))
                operation.next();
        }


        /**
         * Returns an iterator over the rows in the partition associated with the given iterator.
         * Initially, it retrieves the rows from the given iterator until it runs out of data.
         * Then it iterates the primary keys obtained from the index until the end of the partition
         * and lazily constructs new row itertors for each of the key. At a given time, only one row iterator is open.
         *
         * The rows are retrieved in the order of primary keys provided by the underlying index.
         * The iterator is complete when the next key to be fetched belongs to different partition
         * (but the iterator does not consume that key).
         *
         * @param startIter an iterator positioned at the first row in the partition that we want to return
         */
        private @Nonnull UnfilteredRowIterator iteratePartition(@Nonnull UnfilteredRowIterator startIter)
        {
            return new AbstractUnfilteredRowIterator(
                startIter.metadata(),
                startIter.partitionKey(),
                startIter.partitionLevelDeletion(),
                startIter.columns(),
                startIter.staticRow(),
                startIter.isReverseOrder(),
                startIter.stats())
            {
                private UnfilteredRowIterator currentIter = startIter;
                private final DecoratedKey partitionKey = startIter.partitionKey();

                @Override
                protected Unfiltered computeNext()
                {
                    while (!currentIter.hasNext())
                    {
                        currentIter.close();
                        currentIter = nextRowIterator(() -> nextSelectedKeyInPartition(partitionKey));
                        if (currentIter == null)
                            return endOfData();
                    }
                    return currentIter.next();
                }

                @Override
                public void close()
                {
                    FileUtils.closeQuietly(currentIter);
                    super.close();
                }
            };
        }

        public UnfilteredRowIterator apply(PrimaryKey key)
        {
            // Key reads are lazy, delayed all the way to this point.
            // We don't want key.equals(lastKey) because some PrimaryKey implementations consider more than just
            // partition key and clustering for equality. This can break lastKey skipping, which is necessary for
            // correctness when PrimaryKey doesn't have a clustering (as otherwise, the same partition may get
            // filtered and considered as a result multiple times).
            // we need a non-null partitionKey here, as we want to construct a SinglePartitionReadCommand
            Preconditions.checkNotNull(key.partitionKey(), "Partition key must not be null");
            if (lastKey != null && key.partitionKey().equals(lastKey.partitionKey()) && key.clustering().equals(lastKey.clustering()))
                return null;
            lastKey = key;

            UnfilteredRowIterator partition = controller.getPartition(key, executionController);
            queryContext.addPartitionsRead(1);
            queryContext.checkpoint();
            return applyIndexFilter(partition, filterTree, queryContext);
        }

        @Override
        public TableMetadata metadata()
        {
            return controller.metadata();
        }

        @Override
        public void close()
        {
            FileUtils.closeQuietly(operation);
            controller.finish();
        }
    }

    /**
     * A result retriever that consumes an iterator primary keys sorted by some score, materializes the row for each
     * primary key (currently, each primary key is required to be fully qualified and should only point to one row),
     * apply the filter tree to the row to test that the real row satisfies the WHERE clause, and finally tests
     * that the row is valid for the ORDER BY clause. The class performs some optimizations to avoid materializing
     * rows unnecessarily. See the class for more details.
     * <p>
     * The resulting {@link UnfilteredRowIterator} objects are not guaranteed to be in any particular order. It is
     * the responsibility of the caller to sort the results if necessary.
     */
    public static class ScoreOrderedResultRetriever extends AbstractIterator<UnfilteredRowIterator> implements UnfilteredPartitionIterator
    {
        private final ColumnFamilyStore.RefViewFragment view;
        private final List<AbstractBounds<PartitionPosition>> keyRanges;
        private final boolean coversFullRing;
        private final CloseableIterator<PrimaryKeyWithSortKey> scoredPrimaryKeyIterator;
        private final FilterTree filterTree;
        private final QueryController controller;
        private final ReadExecutionController executionController;
        private final QueryContext queryContext;

        private final HashSet<PrimaryKey> processedKeys;
        private final Queue<UnfilteredRowIterator> pendingRows;

        // The limit requested by the query. We cannot load more than softLimit rows in bulk because we only want
        // to fetch the topk rows where k is the limit. However, we allow the iterator to fetch more rows than the
        // soft limit to avoid confusing behavior. When the softLimit is reached, the iterator will fetch one row
        // at a time.
        private final int softLimit;
        private int returnedRowCount = 0;

        private ScoreOrderedResultRetriever(CloseableIterator<PrimaryKeyWithSortKey> scoredPrimaryKeyIterator,
                                            FilterTree filterTree,
                                            QueryController controller,
                                            ReadExecutionController executionController,
                                            QueryContext queryContext,
                                            int limit)
        {
            IndexContext context = controller.getOrderer().context;
            this.view = controller.getQueryView(context).view;
            this.keyRanges = controller.dataRanges().stream().map(DataRange::keyRange).collect(Collectors.toList());
            this.coversFullRing = keyRanges.size() == 1 && RangeUtil.coversFullRing(keyRanges.get(0));

            this.scoredPrimaryKeyIterator = scoredPrimaryKeyIterator;
            this.filterTree = filterTree;
            this.controller = controller;
            this.executionController = executionController;
            this.queryContext = queryContext;

            this.processedKeys = new HashSet<>(limit);
            this.pendingRows = new ArrayDeque<>(limit);
            this.softLimit = limit;
        }

        @Override
        public UnfilteredRowIterator computeNext()
        {
            if (pendingRows.isEmpty())
                fillPendingRows();
            returnedRowCount++;
            // Because we know ordered keys are fully qualified, we do not iterate partitions
            return !pendingRows.isEmpty() ? pendingRows.poll() : endOfData();
        }

        /**
         * Fills the pendingRows queue to generate a queue of row iterators for the supplied keys by repeatedly calling
         * {@link #readAndValidatePartition} until it gives enough non-null results.
         */
        private void fillPendingRows()
        {
            // We always want to get at least 1.
            int rowsToRetrieve = Math.max(1, softLimit - returnedRowCount);
            var keys = new HashMap<PrimaryKey, List<PrimaryKeyWithSortKey>>();
            // We want to get the first unique `rowsToRetrieve` keys to materialize
            // Don't pass the priority queue here because it is more efficient to add keys in bulk
            fillKeys(keys, rowsToRetrieve, null);
            // Sort the primary keys by PrK order, just in case that helps with cache and disk efficiency
            var primaryKeyPriorityQueue = new PriorityQueue<>(keys.keySet());

            while (!keys.isEmpty())
            {
                var primaryKey = primaryKeyPriorityQueue.poll();
                var primaryKeyWithSortKeys = keys.remove(primaryKey);
                var partitionIterator = readAndValidatePartition(primaryKey, primaryKeyWithSortKeys);
                if (partitionIterator != null)
                    pendingRows.add(partitionIterator);
                else
                    // The current primaryKey did not produce a partition iterator. We know the caller will need
                    // `rowsToRetrieve` rows, so we get the next unique key and add it to the queue.
                    fillKeys(keys, 1, primaryKeyPriorityQueue);
            }
        }

        /**
         * Fills the keys map with the next `count` unique primary keys that are in the keys produced by calling
         * {@link #nextSelectedKeyInRange()}. We map PrimaryKey to List<PrimaryKeyWithSortKey> because the same
         * primary key can be in the result set multiple times, but with different source tables.
         * @param keys the map to fill
         * @param count the number of unique PrimaryKeys to consume from the iterator
         * @param primaryKeyPriorityQueue the priority queue to add new keys to. If the queue is null, we do not add
         *                                keys to the queue.
         */
        private void fillKeys(Map<PrimaryKey, List<PrimaryKeyWithSortKey>> keys, int count, PriorityQueue<PrimaryKey> primaryKeyPriorityQueue)
        {
            int initialSize = keys.size();
            while (keys.size() - initialSize < count)
            {
                var primaryKeyWithSortKey = nextSelectedKeyInRange();
                if (primaryKeyWithSortKey == null)
                    return;
                var nextPrimaryKey = primaryKeyWithSortKey.primaryKey();
                var accumulator = keys.computeIfAbsent(nextPrimaryKey, k -> new ArrayList<>());
                if (primaryKeyPriorityQueue != null && accumulator.isEmpty())
                    primaryKeyPriorityQueue.add(nextPrimaryKey);
                accumulator.add(primaryKeyWithSortKey);
            }
        }

        /**
         * Determine if the key is in one of the queried key ranges. We do not iterate through results in
         * {@link PrimaryKey} order, so we have to check each range.
         * @param key
         * @return true if the key is in one of the queried key ranges
         */
        private boolean isInRange(DecoratedKey key)
        {
            if (coversFullRing)
                return true;

            for (AbstractBounds<PartitionPosition> range : keyRanges)
                if (range.contains(key))
                    return true;
            return false;
        }

        /**
         * Returns the next available key contained by one of the keyRanges and selected by the queryController.
         * If the next key falls out of the current key range, it skips to the next key range, and so on.
         * If no more keys acceptd by the controller are available, returns null.
         */
        private @Nullable PrimaryKeyWithSortKey nextSelectedKeyInRange()
        {
            while (scoredPrimaryKeyIterator.hasNext())
            {
                var key = scoredPrimaryKeyIterator.next();
                if (isInRange(key.partitionKey()) && controller.selects(key))
                    return key;
            }
            return null;
        }

        public UnfilteredRowIterator readAndValidatePartition(PrimaryKey key, List<PrimaryKeyWithSortKey> primaryKeys)
        {
            // If we've already processed the key, we can skip it. Because the score ordered iterator does not
            // deduplicate rows, we could see dupes if a row is in the ordering index multiple times. This happens
            // in the case of dupes and of overwrites.
            if (processedKeys.contains(key))
                return null;

            try (UnfilteredRowIterator partition = controller.getPartition(key, view, executionController))
            {
                queryContext.addPartitionsRead(1);
                queryContext.checkpoint();
                var staticRow = partition.staticRow();
                UnfilteredRowIterator clusters = applyIndexFilter(partition, filterTree, queryContext);

                if (clusters == null || !clusters.hasNext())
                {
                    processedKeys.add(key);
                    return null;
                }

                var now = FBUtilities.nowInSeconds();
                boolean isRowValid = false;
                var row = clusters.next();
                assert !clusters.hasNext() : "Expected only one row per partition";
                if (!row.isRangeTombstoneMarker())
                {
                    for (PrimaryKeyWithSortKey primaryKeyWithSortKey : primaryKeys)
                    {
                        // Each of these primary keys are equal, but they have different source tables. Therefore,
                        // we check to see if the row is valid for any of them, and if it is, we return the row.
                        if (primaryKeyWithSortKey.isIndexDataValid((Row) row, now))
                        {
                            isRowValid = true;
                            // We can only count the key as processed once we know it was valid for one of the
                            // primary keys.
                            processedKeys.add(key);
                            break;
                        }
                    }
                }
                return isRowValid ? new PrimaryKeyIterator(partition, staticRow, row) : null;
            }
        }

        public static class PrimaryKeyIterator extends AbstractUnfilteredRowIterator
        {
            private boolean consumed = false;
            private final Unfiltered row;

            public PrimaryKeyIterator(UnfilteredRowIterator partition, Row staticRow, Unfiltered content)
            {
                super(partition.metadata(),
                      partition.partitionKey(),
                      partition.partitionLevelDeletion(),
                      partition.columns(),
                      staticRow,
                      partition.isReverseOrder(),
                      partition.stats());

                row = content;
            }

            @Override
            protected Unfiltered computeNext()
            {
                if (consumed)
                    return endOfData();
                consumed = true;
                return row;
            }
        }

        @Override
        public TableMetadata metadata()
        {
            return controller.metadata();
        }

        public void close()
        {
            FileUtils.closeQuietly(scoredPrimaryKeyIterator);
            controller.finish();
        }
    }

    private static UnfilteredRowIterator applyIndexFilter(UnfilteredRowIterator partition, FilterTree tree, QueryContext queryContext)
    {
        FilteringPartitionIterator filtered = new FilteringPartitionIterator(partition, tree, queryContext);
        if (!filtered.hasNext() && !filtered.matchesStaticRow())
        {
            // shadowed by expired TTL or row tombstone or range tombstone
            queryContext.addShadowed(1);
            filtered.close();
            return null;
        }
        return filtered;
    }

    /**
     * Filters the rows in the partition so that only non-static rows that match given filter are returned.
     */
    private static class FilteringPartitionIterator extends AbstractUnfilteredRowIterator
    {
        private final FilterTree filter;
        private final QueryContext queryContext;
        private final UnfilteredRowIterator rows;

        private final DecoratedKey key;
        private final Row staticRow;

        public FilteringPartitionIterator(UnfilteredRowIterator partition, FilterTree filter, QueryContext queryContext)
        {
            super(partition.metadata(),
                  partition.partitionKey(),
                  partition.partitionLevelDeletion(),
                  partition.columns(),
                  partition.staticRow(),
                  partition.isReverseOrder(),
                  partition.stats());

            this.rows = partition;
            this.filter = filter;
            this.queryContext = queryContext;
            this.key = partition.partitionKey();
            this.staticRow = partition.staticRow();
        }

        public boolean matchesStaticRow()
        {
            queryContext.addRowsFiltered(1);
            return filter.isSatisfiedBy(key, staticRow, staticRow);
        }

        @Override
        protected Unfiltered computeNext()
        {
            while (rows.hasNext())
            {
                Unfiltered row = rows.next();
                queryContext.addRowsFiltered(1);

                if (!row.isRow() || ((Row)row).isStatic())
                    continue;

                if (filter.isSatisfiedBy(key, row, staticRow))
                    return row;
            }
            return endOfData();
        }

        @Override
        public void close()
        {
            super.close();
            rows.close();
        }
    }
}
