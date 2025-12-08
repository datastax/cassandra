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

import java.io.IOError;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.util.concurrent.FastThreadLocal;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.MessageParams;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.AbstractUnfilteredRowIterator;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.ColumnData;
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
import org.apache.cassandra.index.sai.utils.PrimaryKeyWithScore;
import org.apache.cassandra.index.sai.utils.PrimaryKeyWithSortKey;
import org.apache.cassandra.index.sai.utils.RangeUtil;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.net.ParamType;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.btree.BTree;

public class StorageAttachedIndexSearcher implements Index.Searcher
{
    protected static final Logger logger = LoggerFactory.getLogger(StorageAttachedIndexSearcher.class);

    private static final int PARTITION_ROW_BATCH_SIZE = CassandraRelevantProperties.SAI_PARTITION_ROW_BATCH_SIZE.getInt();

    private final ReadCommand command;
    private final QueryController controller;
    private final QueryContext queryContext;
    private final TableQueryMetrics tableQueryMetrics;

    private static final FastThreadLocal<List<PrimaryKey>> nextKeys = new FastThreadLocal<>()
    {
        @Override
        protected List<PrimaryKey> initialValue()
        {
            return new ArrayList<>(PARTITION_ROW_BATCH_SIZE);
        }
    };

    public StorageAttachedIndexSearcher(ColumnFamilyStore cfs,
                                        TableQueryMetrics tableQueryMetrics,
                                        ReadCommand command,
                                        Orderer orderer,
                                        IndexFeatureSet indexFeatureSet,
                                        long executionQuotaMs)
    {
        this.command = command;
        this.queryContext = new QueryContext(executionQuotaMs);
        this.controller = new QueryController(cfs, command, orderer, indexFeatureSet, queryContext);
        this.tableQueryMetrics = tableQueryMetrics;
    }

    @Override
    public ReadCommand command()
    {
        return command;
    }

    @VisibleForTesting
    public final Set<String> plannedIndexes()
    {
        try
        {
            Plan plan = controller.buildPlan();
            Set<String> indexes = new HashSet<>();
            plan.forEach(node -> {
                if (node instanceof Plan.IndexScan)
                {
                    Plan.IndexScan indexScan = (Plan.IndexScan) node;
                    indexes.add(indexScan.getIndexName());
                }
                if (node instanceof Plan.ScoredIndexScan)
                {
                    Plan.ScoredIndexScan indexScan = (Plan.ScoredIndexScan) node;
                    indexes.add(indexScan.getIndexName());
                }
                return Plan.ControlFlow.Continue;
            });
            return indexes;
        }
        finally
        {
            // we need to call this to clean up the resources opened by the plan
            controller.abort();
        }
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
                maybeTriggerReferencedIndexesGuardrail(filterTree);

                Plan plan = controller.buildPlan();
                Iterator<? extends PrimaryKey> keysIterator = controller.buildIterator(plan);

                // Can't check for `command.isTopK()` because the planner could optimize sorting out
                Orderer ordering = plan.ordering();
                if (ordering == null)
                {
                    assert keysIterator instanceof KeyRangeIterator;
                    return new ResultRetriever((KeyRangeIterator) keysIterator, filterTree, executionController);
                }

                assert !(keysIterator instanceof KeyRangeIterator);
                var scoredKeysIterator = (CloseableIterator<PrimaryKeyWithSortKey>) keysIterator;
                var result = new ScoreOrderedResultRetriever(scoredKeysIterator, filterTree, executionController,
                                                             command.limits().count(),
                                                             ordering.context.getDefinition());
                return new TopKProcessor(command).filter(result);
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
                // Throwing IOError here because we want the coordinator to handle it as any other serious storage error
                // and report it up to the user as failed query. It is better to fail than to return an incomplete
                // result set.
                throw new IOError(e);
            }
            catch (Throwable t)
            {
                controller.abort();
                throw t;
            }
        }
    }

    private void maybeTriggerReferencedIndexesGuardrail(FilterTree filterTree)
    {
        if (!Guardrails.saiSSTableIndexesPerQuery.enabled())
            return;

        int numReferencedIndexes = filterTree.numSSTableIndexes();

        if (Guardrails.saiSSTableIndexesPerQuery.failsOn(numReferencedIndexes, null))
        {
            String msg = String.format("Query %s attempted to read from too many indexes (%s) but max allowed is %s; " +
                                       "query aborted (see sai_sstable_indexes_per_query_fail_threshold)",
                                       command.toRedactedCQLString(),
                                       numReferencedIndexes,
                                       Guardrails.CONFIG_PROVIDER.getOrCreate(null).getSaiSSTableIndexesPerQueryFailThreshold());
            Tracing.trace(msg);
            MessageParams.add(ParamType.TOO_MANY_REFERENCED_INDEXES_FAIL, numReferencedIndexes);
            throw new QueryReferencingTooManyIndexesException(msg);
        }
        else if (Guardrails.saiSSTableIndexesPerQuery.warnsOn(numReferencedIndexes, null))
        {
            MessageParams.add(ParamType.TOO_MANY_REFERENCED_INDEXES_WARN, numReferencedIndexes);
        }
    }

    /**
     * Converts expressions into filter tree (which is currently just a single AND).
     * </p>
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

    private class ResultRetriever extends AbstractIterator<UnfilteredRowIterator> implements UnfilteredPartitionIterator
    {
        private final PrimaryKey firstPrimaryKey;
        private final Iterator<DataRange> keyRanges;
        private AbstractBounds<PartitionPosition> currentKeyRange;

        private final KeyRangeIterator operation;
        private final FilterTree filterTree;
        private final ReadExecutionController executionController;
        private final PrimaryKey.Factory keyFactory;
        private final int partitionRowBatchSize;

        private PrimaryKey lastKey;

        private ResultRetriever(KeyRangeIterator operation,
                                FilterTree filterTree,
                                ReadExecutionController executionController)
        {
            this.keyRanges = controller.dataRanges().iterator();
            this.currentKeyRange = keyRanges.next().keyRange();

            this.operation = operation;
            this.filterTree = filterTree;
            this.executionController = executionController;
            this.keyFactory = controller.primaryKeyFactory();

            this.firstPrimaryKey = controller.firstPrimaryKey();

            // Ensure we don't fetch larger batches than the provided LIMIT to avoid fetching keys we won't use:
            this.partitionRowBatchSize = Math.min(PARTITION_ROW_BATCH_SIZE, command.limits().count());
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

            UnfilteredRowIterator iterator = nextRowIterator(this::nextSelectedKeysInRange);
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
        private @Nullable UnfilteredRowIterator nextRowIterator(@Nonnull Supplier<List<PrimaryKey>> keySupplier)
        {
            UnfilteredRowIterator iterator = null;
            while (iterator == null)
            {
                List<PrimaryKey> keys = keySupplier.get();
                if (keys.isEmpty())
                    return null;
                iterator = apply(keys);
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

        private boolean isEqualToLastKey(PrimaryKey key)
        {
            // We don't want key.equals(lastKey) because some PrimaryKey implementations consider more than just
            // partition key and clustering for equality. This can break lastKey skipping, which is necessary for
            // correctness when PrimaryKey doesn't have a clustering (as otherwise, the same partition may get
            // filtered and considered as a result multiple times).
            return lastKey != null &&
                   Objects.equals(lastKey.partitionKey(), key.partitionKey()) &&
                   (lastKey.hasEmptyClustering() || key.hasEmptyClustering() || Objects.equals(lastKey.clustering(), key.clustering()));
        }

        private void fillNextSelectedKeysInPartition(DecoratedKey partitionKey, List<PrimaryKey> nextPrimaryKeys)
        {
            while (operation.hasNext()
                   && operation.peek().partitionKey().equals(partitionKey)
                   && nextPrimaryKeys.size() < partitionRowBatchSize)
            {
                PrimaryKey key = nextKey();

                if (key == null)
                    break;

                if (!controller.selects(key) || isEqualToLastKey(key))
                    continue;

                nextPrimaryKeys.add(key);
                lastKey = key;
            }
        }

        /**
         * Retrieves the next batch of primary keys (i.e. up to {@link #partitionRowBatchSize} of them) that are
         * contained by one of the query key ranges and selected by the {@link QueryController}. If the next key falls
         * out of the current key range, it skips to the next key range, and so on. If no more keys accepted by
         * the controller are available, and empty list is returned.
         *
         * @return a list of up to {@link #partitionRowBatchSize} primary keys
         */
        private List<PrimaryKey> nextSelectedKeysInRange()
        {
            List<PrimaryKey> threadLocalNextKeys = nextKeys.get();
            threadLocalNextKeys.clear();
            PrimaryKey firstKey;

            do
            {
                firstKey = nextKeyInRange();

                if (firstKey == null)
                    return Collections.emptyList();
            }
            while (!controller.selects(firstKey) || isEqualToLastKey(firstKey));

            lastKey = firstKey;
            threadLocalNextKeys.add(firstKey);
            fillNextSelectedKeysInPartition(firstKey.partitionKey(), threadLocalNextKeys);
            return threadLocalNextKeys;
        }

        /**
         * Retrieves the next batch of primary keys (i.e. up to {@link #partitionRowBatchSize} of them) that belong to
         * the given partition and are selected by the query controller, advancing the underlying iterator only while
         * the next key belongs to that partition.
         *
         * @return a list of up to {@link #partitionRowBatchSize} primary keys within the given partition
         */
        private List<PrimaryKey> nextSelectedKeysInPartition(DecoratedKey partitionKey)
        {
            List<PrimaryKey> threadLocalNextKeys = nextKeys.get();
            threadLocalNextKeys.clear();
            fillNextSelectedKeysInPartition(partitionKey, threadLocalNextKeys);
            return threadLocalNextKeys;
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
         * </p>
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
                        currentIter = nextRowIterator(() -> nextSelectedKeysInPartition(partitionKey));
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

        public UnfilteredRowIterator apply(List<PrimaryKey> keys)
        {
            long startTimeNanos = Clock.Global.nanoTime();
            UnfilteredRowIterator partition = controller.getPartition(keys, executionController);
            queryContext.addPartitionsRead(1);
            queryContext.checkpoint();
            UnfilteredRowIterator filtered = applyIndexFilter(partition, filterTree, queryContext);

            // Note that we record the duration of the read after post-filtering, which actually
            // materializes the rows from disk.
            queryContext.addPostFilteringReadLatency(Clock.Global.nanoTime() - startTimeNanos);
            return filtered;
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
            if (tableQueryMetrics != null)
                tableQueryMetrics.record(queryContext, command);
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
    public class ScoreOrderedResultRetriever extends AbstractIterator<UnfilteredRowIterator> implements UnfilteredPartitionIterator
    {
        private final ColumnFamilyStore.ViewFragment view;
        private final List<AbstractBounds<PartitionPosition>> keyRanges;
        private final boolean coversFullRing;
        private final CloseableIterator<PrimaryKeyWithSortKey> scoredPrimaryKeyIterator;
        private final FilterTree filterTree;
        private final ReadExecutionController executionController;

        private final HashSet<PrimaryKey> processedKeys;
        private final Queue<UnfilteredRowIterator> pendingRows;

        // Null indicates we are not sending the synthetic score column to the coordinator
        @Nullable
        private final ColumnMetadata syntheticScoreColumn;

        // The limit requested by the query. We cannot load more than softLimit rows in bulk because we only want
        // to fetch the topk rows where k is the limit. However, we allow the iterator to fetch more rows than the
        // soft limit to avoid confusing behavior. When the softLimit is reached, the iterator will fetch one row
        // at a time.
        private final int softLimit;
        private int returnedRowCount = 0;

        private ScoreOrderedResultRetriever(CloseableIterator<PrimaryKeyWithSortKey> scoredPrimaryKeyIterator,
                                            FilterTree filterTree,
                                            ReadExecutionController executionController, int limit,
                                            ColumnMetadata orderedColumn)
        {
            IndexContext context = controller.getOrderer().context;
            this.view = controller.getQueryView(context).viewFragment;
            this.keyRanges = controller.dataRanges().stream().map(DataRange::keyRange).collect(Collectors.toList());
            this.coversFullRing = keyRanges.size() == 1 && RangeUtil.coversFullRing(keyRanges.get(0));

            this.scoredPrimaryKeyIterator = scoredPrimaryKeyIterator;
            this.filterTree = filterTree;
            this.executionController = executionController;

            this.processedKeys = new HashSet<>(limit);
            this.pendingRows = new ArrayDeque<>(limit);
            this.softLimit = limit;

            // When +score is added on the coordinator side, it's represented as a PrecomputedColumnFilter
            // even in a 'SELECT *' because WCF is not capable of representing synthetic columns.
            // This can be simplified when we remove ANN_USE_SYNTHETIC_SCORE
            var tempColumn = ColumnMetadata.syntheticScoreColumn(orderedColumn, FloatType.instance);
            var isScoreFetched = controller.command().columnFilter().fetchesExplicitly(tempColumn);
            this.syntheticScoreColumn = isScoreFetched ? tempColumn : null;
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
            // Group PKs by source sstable/memtable
            var groupedKeys = new HashMap<PrimaryKey, List<PrimaryKeyWithSortKey>>();
            // We always want to get at least 1.
            int rowsToRetrieve = Math.max(1, softLimit - returnedRowCount);
            // We want to get the first unique `rowsToRetrieve` keys to materialize
            // Don't pass the priority queue here because it is more efficient to add keys in bulk
            fillKeys(groupedKeys, rowsToRetrieve, null);
            // Sort the primary keys by PrK order, just in case that helps with cache and disk efficiency
            var primaryKeyPriorityQueue = new PriorityQueue<>(groupedKeys.keySet());

            // drain groupedKeys into pendingRows
            while (!groupedKeys.isEmpty())
            {
                var pk = primaryKeyPriorityQueue.poll();
                var sourceKeys = groupedKeys.remove(pk);
                var partitionIterator = readAndValidatePartition(pk, sourceKeys);
                if (partitionIterator != null)
                    pendingRows.add(partitionIterator);
                else
                    // The current primaryKey did not produce a partition iterator. We know the caller will need
                    // `rowsToRetrieve` rows, so we get the next unique key and add it to the queue.
                    fillKeys(groupedKeys, 1, primaryKeyPriorityQueue);
            }
        }

        /**
         * Fills the `groupedKeys` Map with the next `count` unique primary keys that are in the keys produced by calling
         * {@link #nextSelectedKeyInRange()}. We map PrimaryKey to {@literal List<PrimaryKeyWithSortKey>} because the same
         * primary key can be in the result set multiple times, but with different source tables.
         * @param groupedKeys the map to fill
         * @param count the number of unique PrimaryKeys to consume from the iterator
         * @param primaryKeyPriorityQueue the priority queue to add new keys to. If the queue is null, we do not add
         *                                keys to the queue.
         */
        private void fillKeys(Map<PrimaryKey, List<PrimaryKeyWithSortKey>> groupedKeys, int count, PriorityQueue<PrimaryKey> primaryKeyPriorityQueue)
        {
            int initialSize = groupedKeys.size();
            while (groupedKeys.size() - initialSize < count)
            {
                var primaryKeyWithSortKey = nextSelectedKeyInRange();
                if (primaryKeyWithSortKey == null)
                    return;
                var nextPrimaryKey = primaryKeyWithSortKey.primaryKey();
                var accumulator = groupedKeys.computeIfAbsent(nextPrimaryKey, k -> new ArrayList<>());
                if (primaryKeyPriorityQueue != null && accumulator.isEmpty())
                    primaryKeyPriorityQueue.add(nextPrimaryKey);
                accumulator.add(primaryKeyWithSortKey);
            }
        }

        /**
         * Determine if the key is in one of the queried key ranges. We do not iterate through results in
         * {@link PrimaryKey} order, so we have to check each range.
         *
         * @param key a partition key
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

        /**
         * Reads and validates a partition for a given primary key against its sources.
         * <p>
         * @param pk The primary key of the partition to read and validate
         * @param sourceKeys A list of PrimaryKeyWithSortKey objects associated with the primary key.
         *                   Multiple sort keys can exist for the same primary key when data comes from different
         *                   sstables or memtables.
         *
         * @return An UnfilteredRowIterator containing the validated partition data, or null if:
         *         - The key has already been processed
         *         - The partition does not pass index filters
         *         - The partition contains no valid rows
         *         - The row data does not match the index metadata for any of the provided primary keys
         */
        public UnfilteredRowIterator readAndValidatePartition(PrimaryKey pk, List<PrimaryKeyWithSortKey> sourceKeys)
        {
            // If we've already processed the key, we can skip it. Because the score ordered iterator does not
            // deduplicate rows, we could see dupes if a row is in the ordering index multiple times. This happens
            // in the case of dupes and of overwrites.
            if (processedKeys.contains(pk))
                return null;

            try (UnfilteredRowIterator partition = controller.getPartition(pk, view, executionController))
            {
                queryContext.addPartitionsRead(1);
                queryContext.checkpoint();
                UnfilteredRowIterator clusters = applyIndexFilter(partition, filterTree, queryContext);

                if (clusters == null)
                    return null;

                var now = FBUtilities.nowInSeconds();
                var staticRow = partition.staticRow();
                boolean isStaticValid = false;

                // Each of the primary keys are equal, but they have different source tables.
                // Therefore, we check to see if the static row is valid for any of them.
                for (PrimaryKeyWithSortKey sourceKey : sourceKeys)
                {
                    if (sourceKey.isIndexDataValid(staticRow, now))
                    {
                        // If there are no regular rows, return the static row only
                        if (!clusters.hasNext())
                            return new PrimaryKeyIterator(partition, staticRow, null, sourceKey, syntheticScoreColumn);

                        isStaticValid = true;
                        break;
                    }
                }

                // If the static row isn't valid, we can skip the partition.
                if (!isStaticValid)
                    return null;

                var row = clusters.next();
                if (!row.isRangeTombstoneMarker())
                {
                    for (PrimaryKeyWithSortKey sourceKey : sourceKeys)
                    {
                        // Each of these primary keys are equal, but they have different source tables.
                        // Only one can be valid.
                        if (sourceKey.isIndexDataValid((Row) row, now))
                        {
                            // We can only count the pk as processed once we know it was valid for one of the
                            // scored keys.
                            processedKeys.add(pk);
                            return new PrimaryKeyIterator(partition, staticRow, row, sourceKey, syntheticScoreColumn);
                        }
                    }
                }
                return null;
            }
        }

        @Override
        public TableMetadata metadata()
        {
            return controller.metadata();
        }

        @Override
        public void close()
        {
            FileUtils.closeQuietly(scoredPrimaryKeyIterator);
            controller.finish();
            if (tableQueryMetrics != null)
                tableQueryMetrics.record(queryContext, command);
        }

        public class PrimaryKeyIterator extends AbstractUnfilteredRowIterator
        {
            private boolean consumed = false;

            @Nullable
            private final Unfiltered row;

            public PrimaryKeyIterator(UnfilteredRowIterator partition,
                                      Row staticRow,
                                      @Nullable Unfiltered content,
                                      PrimaryKeyWithSortKey primaryKeyWithSortKey,
                                      ColumnMetadata syntheticScoreColumn)
            {
                super(partition.metadata(),
                      partition.partitionKey(),
                      partition.partitionLevelDeletion(),
                      partition.columns(),
                      staticRow,
                      partition.isReverseOrder(),
                      partition.stats());

                if (content == null || !content.isRow() || !(primaryKeyWithSortKey instanceof PrimaryKeyWithScore))
                {
                    this.row = content;
                    return;
                }

                if (syntheticScoreColumn == null)
                {
                    this.row = content;
                    return;
                }

                // Clone the original Row
                Row originalRow = (Row) content;
                ArrayList<ColumnData> columnData = new ArrayList<>(originalRow.columnCount() + 1);
                columnData.addAll(originalRow.columnData());

                // inject +score as a new column
                var pkWithScore = (PrimaryKeyWithScore) primaryKeyWithSortKey;
                columnData.add(BufferCell.live(syntheticScoreColumn,
                                               FBUtilities.nowInSeconds(),
                                               FloatType.instance.decompose(pkWithScore.indexScore)));

                this.row = BTreeRow.create(originalRow.clustering(),
                                           originalRow.primaryKeyLivenessInfo(),
                                           originalRow.deletion(),
                                           BTree.builder(ColumnData.comparator)
                                                .auto(true)
                                                .addAll(columnData)
                                                .build());
            }

            @Override
            protected Unfiltered computeNext()
            {
                if (consumed || row == null)
                    return endOfData();
                consumed = true;
                return row;
            }
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
