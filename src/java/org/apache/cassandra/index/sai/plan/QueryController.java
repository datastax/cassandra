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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.MultiRangeReadCommand;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ClusteringIndexNamesFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.SSTableIndex;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.disk.format.IndexFeatureSet;
import org.apache.cassandra.index.sai.metrics.TableQueryMetrics;
import org.apache.cassandra.index.sai.utils.AbortedOperationException;
import org.apache.cassandra.index.sai.utils.OrderingFilterRangeIterator;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.RangeIntersectionIterator;
import org.apache.cassandra.index.sai.utils.RangeIterator;
import org.apache.cassandra.index.sai.utils.RangeUnionIterator;
import org.apache.cassandra.index.sai.utils.MergeScoredPrimaryKeyIterator;
import org.apache.cassandra.index.sai.utils.ScoredPrimaryKey;
import org.apache.cassandra.index.sai.utils.SoftLimitUtil;
import org.apache.cassandra.index.sai.utils.TermIterator;
import org.apache.cassandra.index.sai.view.View;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.concurrent.Ref;

import static org.apache.cassandra.config.CassandraRelevantProperties.SAI_VECTOR_SEARCH_ORDER_CHUNK_SIZE;

public class QueryController
{
    private static final Logger logger = LoggerFactory.getLogger(QueryController.class);

    /**
     * How likely we want the soft limit to be high enough, so that we get sufficient number of rows
     * without having to retry. The closer this is to 1.0, the higher the soft limit will be relative to
     * the exact limit (i.e. we'll ask for more rows speculatively in case we would have to throw some of them out due
     * to tombstones / updates / expired TTLs / not matching the post-filter).
     * Setting it too high may cause the queries to be more expensive, because they would be fetching too many rows.
     * Setting it too low will cause frequent retries.
     */
    private static final double SOFT_LIMIT_CONFIDENCE = 0.90;

    /**
     * Constants used in cost-based query optimization.
     * Those costs are abstract, they don't represent any physical resource unit.
     * What matters are ratios between them, not the absolute values.
     */
    static class Costs
    {
        /**
         * The cost to get a single PrimaryKey from the index, *without* looking it up in the sstable.
         */
        static final float INDEX_KEY_FETCH_COST = 1.0f;
        /**
         * How much additional effort it costs to get a PrimaryKey from the index if we have to intersect indexes.
         * Intersections are more costly because of index skipping.
         */
        static final float INDEX_INTERSECTION_PENALTY = 4.0f;
        /**
         * The cost to fetch a full row from the storage and apply filters to it.
         * Deserializing rows is costly.
         * In the future this should be replaced by a better model taking into account the data size
         * and number of columns.
         */
        static final float ROW_MATERIALIZE_COST = 100.0f;

        /**
         * Additional row materialization penalty per each cell in the row.
         * <p>
         * The cost of a row cell depends on how the cell is used, e.g. the cost of a cell that is used in the
         * row filter is much higher than the cost of the cell that isn't fetched from the storage. Unfortunately
         * at the moment we don't have stats precise enough to distinguish those cases, so we just assume the column
         * is fetched from storage and used in the filter. The cost is so low anyway that it really
         * matters only for collections.
         */
        static final float CELL_MATERIALIZE_COST = 0.4f;

        /** Additional row materialization penalty per each deserialized byte of the row */
        static final float BYTE_MATERIALIZE_COST = 0.005f;
    }

    public static final int ORDER_CHUNK_SIZE = SAI_VECTOR_SEARCH_ORDER_CHUNK_SIZE.getInt();

    private final ColumnFamilyStore cfs;
    private final ReadCommand command;
    private final int limit;
    private final QueryContext queryContext;
    private final TableQueryMetrics tableQueryMetrics;
    private final RowFilter.FilterElement filterOperation;
    private final IndexFeatureSet indexFeatureSet;
    private final List<DataRange> ranges;
    private final AbstractBounds<PartitionPosition> mergeRange;

    private final PrimaryKey.Factory keyFactory;
    private final PrimaryKey firstPrimaryKey;

    public QueryController(ColumnFamilyStore cfs,
                           ReadCommand command,
                           RowFilter.FilterElement filterOperation,
                           IndexFeatureSet indexFeatureSet,
                           QueryContext queryContext,
                           TableQueryMetrics tableQueryMetrics)
    {
        this.cfs = cfs;
        this.command = command;
        this.queryContext = queryContext;
        this.limit = command.limits().count();
        this.tableQueryMetrics = tableQueryMetrics;
        this.filterOperation = filterOperation;
        this.indexFeatureSet = indexFeatureSet;
        this.ranges = dataRanges(command);
        DataRange first = ranges.get(0);
        DataRange last = ranges.get(ranges.size() - 1);
        this.mergeRange = ranges.size() == 1 ? first.keyRange() : first.keyRange().withNewRight(last.keyRange().right);

        this.keyFactory = PrimaryKey.factory(cfs.metadata().comparator, indexFeatureSet);
        this.firstPrimaryKey = keyFactory.createTokenOnly(mergeRange.left.getToken());
    }

    public PrimaryKey.Factory primaryKeyFactory()
    {
        return keyFactory;
    }

    public PrimaryKey firstPrimaryKey()
    {
        return firstPrimaryKey;
    }


    public TableMetadata metadata()
    {
        return command.metadata();
    }

    RowFilter.FilterElement filterOperation()
    {
        return this.filterOperation;
    }

    /**
     * @return token ranges used in the read command
     */
    List<DataRange> dataRanges()
    {
        return ranges;
    }

    /**
     * Note: merged range may contain subrange that no longer belongs to the local node after range movement.
     * It should only be used as an optimization to reduce search space. Use {@link #dataRanges()} instead to filter data.
     *
     * @return merged token range
     */
    AbstractBounds<PartitionPosition> mergeRange()
    {
        return mergeRange;
    }

    /**
     * @return indexed {@code ColumnContext} if index is found; otherwise return non-indexed {@code ColumnContext}.
     */
    public IndexContext getContext(RowFilter.Expression expression)
    {
        StorageAttachedIndex index = getBestIndexFor(expression);

        if (index != null)
            return index.getIndexContext();

        return new IndexContext(cfs.metadata().keyspace,
                                cfs.metadata().name,
                                cfs.metadata().partitionKeyType,
                                cfs.metadata().comparator,
                                expression.column(),
                                IndexTarget.Type.VALUES,
                                null,
                                cfs);
    }

    public UnfilteredRowIterator getPartition(PrimaryKey key, ReadExecutionController executionController)
    {
        if (key == null)
            throw new IllegalArgumentException("non-null key required");

        SinglePartitionReadCommand partition = getPartitionReadCommand(key, executionController);
        return executePartitionReadCommand(partition, executionController);
    }

    public SinglePartitionReadCommand getPartitionReadCommand(PrimaryKey key, ReadExecutionController executionController)
    {
        if (key == null)
            throw new IllegalArgumentException("non-null key required");

        return SinglePartitionReadCommand.create(cfs.metadata(),
                                                 command.nowInSec(),
                                                 command.columnFilter(),
                                                 RowFilter.NONE,
                                                 DataLimits.NONE,
                                                 key.partitionKey(),
                                                 makeFilter(key));
    }

    public UnfilteredRowIterator executePartitionReadCommand(SinglePartitionReadCommand command, ReadExecutionController executionController)
    {
        return command.queryMemtableAndDisk(cfs, executionController);
    }

    public RangeIterator buildIterator()
    {
        // VSTODO we can clean this up when we break ordering out
        var nonOrderingExpressions = filterOperation.expressions().stream()
                                                    .filter(e -> e.operator() != Operator.ANN)
                                                    .collect(Collectors.toList());
        if (nonOrderingExpressions.isEmpty() && filterOperation.children().isEmpty())
            return RangeIterator.empty();
        return Operation.Node.buildTree(nonOrderingExpressions, filterOperation.children(), filterOperation.isDisjunction())
                             .analyzeTree(this)
                             .rangeIterator(this);
    }

    public CloseableIterator<ScoredPrimaryKey> buildScoredPrimaryKeyIterator()
    {
        var filterOperation = filterOperation();
        var orderings = filterOperation.expressions()
                                       .stream().filter(e -> e.operator() == Operator.ANN).collect(Collectors.toList());
        assert orderings.size() == 1;
        if (filterOperation.expressions().size() == 1 && filterOperation.children().isEmpty() && orderings.size() == 1)
            // If we only have one expression, we just use the ANN index to order and limit.
            return getTopKRows(orderings.get(0));

        var whereClauseIter = buildIterator();

        // A query is no longer re-run, so this method is always called before initializing the query context's
        // filter sort order.
        assert queryContext.filterSortOrder() == null;
        QueryContext.FilterSortOrder order = decideFilterSortOrder(filterOperation, whereClauseIter);
        queryContext.setFilterSortOrder(order);

        if (queryContext.filterSortOrder() == QueryContext.FilterSortOrder.SORT_THEN_FILTER)
        {
            queryContext.setPostFilterSelectivityEstimate(estimateSelectivity(whereClauseIter));
            FileUtils.closeQuietly(whereClauseIter);
            return getTopKRows(orderings.get(0));
        }

        return getTopKRows(whereClauseIter, orderings.get(0));
    }

    private QueryContext.FilterSortOrder decideFilterSortOrder(RowFilter.FilterElement filter, RangeIterator iter)
    {
        double sortThenFilterCost = estimateSortThenFilterCost(iter);
        double filterThenSortCost = estimateFilterThenSortCost(filter, iter);
        QueryContext.FilterSortOrder order = sortThenFilterCost < filterThenSortCost
               ? QueryContext.FilterSortOrder.SORT_THEN_FILTER
               : QueryContext.FilterSortOrder.FILTER_THEN_SORT;
        logger.debug("Decided filter sort order {} (costs: SORT_THEN_FILTER = {}, FILTER_THEN_SORT = {})",
                     order, sortThenFilterCost, filterThenSortCost);
        return order;
    }

    private float estimateFilterThenSortCost(RowFilter.FilterElement filter, RangeIterator iter)
    {
        // Unions are cheap but intersections have higher costs because of skipping on the iterators,
        // so we add a penalty for each intersection used in the filter.
        float intersectionPenalty = intersectionPenalty(filter);
        // TODO: account for shadowed keys once we collect stats
        long primaryKeysFetchedFromIndex = iter.getMaxKeys();
        long materializedRows = Math.min(getExactLimit(), primaryKeysFetchedFromIndex);
        return primaryKeysFetchedFromIndex * Costs.INDEX_KEY_FETCH_COST * (1.0f + intersectionPenalty)
               + estimateRowMaterializationCost(materializedRows);
    }

    private float estimateSortThenFilterCost(RangeIterator iter)
    {
        float selectivity = estimateSelectivity(iter);
        int materializedRows = SoftLimitUtil.softLimit(getExactLimit(), SOFT_LIMIT_CONFIDENCE, selectivity);
        return estimateRowMaterializationCost(materializedRows);
    }

    private float estimateRowMaterializationCost(long rows)
    {
        float avgCellsPerRow = avgCellsPerRow();
        float avgBytesPerRow = avgRowSizeInBytes();
        return rows * (
            Costs.ROW_MATERIALIZE_COST
            + avgCellsPerRow * Costs.CELL_MATERIALIZE_COST
            + avgBytesPerRow * Costs.BYTE_MATERIALIZE_COST
        );
    }

    private float avgCellsPerRow()
    {
        long cells = 0;
        long rows = 0;
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            rows += sstable.getTotalRows();
            cells += sstable.getEstimatedCellPerPartitionCount().mean() * sstable.getEstimatedCellPerPartitionCount().count();
        }
        return rows == 0 ? 0.0f : ((float) cells) / rows;
    }

    private float avgRowSizeInBytes()
    {
        long totalLength = 0;
        long rows = 0;
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            rows += sstable.getTotalRows();
            totalLength += sstable.uncompressedLength();
        }
        return rows == 0 ? 0.0f : ((float) totalLength) / rows;
    }

    /**
     * Estimates additional cost of performing index intersections (AND operator).
     * If there are no intersections in the filter tree, returns 0.0.
     */
    private static float intersectionPenalty(RowFilter.FilterElement elem)
    {
        // TODO: This is very crude cost estimation. Ideally we should take into account the selectivity of each
        // individual filter expression. The worst case can be when there are multiple intersected predicates, where
        // each has low selectivity (selects many keys), but only very few keys match all of them. Then many posting
        // list entries must be traversed before we get a key and the cost is high. This code does not take it into
        // account.
       return intersectionsCount(elem) * Costs.INDEX_INTERSECTION_PENALTY;
    }

    /**
     * Returns the number of intersections in the filter expression tree (includes children).
     * <p>
     * Examples:
     * <ul>
     *     <li>A OR B is counted as 0</li>
     *     <li>A AND B is counted as 1</li>
     *     <li>A AND B AND C is counted as 2</li>
     *     <li>A OR B AND C is counted as 1</li>
     * </ul>
     */
    private static int intersectionsCount(RowFilter.FilterElement elem)
    {
        int nonOrderingExpressionsCount = (int) elem.expressions().stream().filter(e -> e.operator() != Operator.ANN).count();
        int intersectedExpressionsCount = elem.isDisjunction() ? 0 : nonOrderingExpressionsCount;
        int intersectionsCount = Math.max(0, intersectedExpressionsCount - 1);

        for (RowFilter.FilterElement child : elem.children())
            intersectionsCount += intersectionsCount(child);

        return intersectionsCount;
    }

    public FilterTree buildFilter()
    {
        return Operation.Node.buildTree(filterOperation()).buildFilter(this);
    }

    /**
     * Build a {@link RangeIterator.Builder} from the given list of expressions by applying given operation (OR/AND).
     * Building of such builder involves index search, results of which are persisted in the internal resources list
     *
     * @param op The operation type to coalesce expressions with.
     * @param expressions The expressions to build range iterator from (expressions with not results are ignored).
     *
     * @return range iterator builder based on given expressions and operation type.
     */
    public RangeIterator buildRangeIteratorForExpressions(Operation.OperationType op, Collection<Expression> expressions)
    {
        assert !expressions.isEmpty() : "expressions should not be empty for " + op + " in " + filterOperation;

        // VSTODO move ANN out of expressions and into its own abstraction? That will help get generic ORDER BY support
        Collection<Expression> exp = expressions.stream().filter(e -> e.operation != Expression.Op.ANN).collect(Collectors.toList());
        boolean defer = op == Operation.OperationType.OR || RangeIntersectionIterator.shouldDefer(exp.size());
        RangeIterator.Builder builder = op == Operation.OperationType.OR
                                        ? RangeUnionIterator.builder()
                                        : RangeIntersectionIterator.builder(RangeIntersectionIterator.INTERSECTION_CLAUSE_LIMIT);

        Set<Map.Entry<Expression, NavigableSet<SSTableIndex>>> view = referenceAndGetView(op, exp).entrySet();

        try
        {
            for (Map.Entry<Expression, NavigableSet<SSTableIndex>> e : view)
            {
                @SuppressWarnings("resource") // RangeIterators are closed by releaseIndexes
                RangeIterator index = TermIterator.build(e.getKey(), e.getValue(), mergeRange, queryContext, defer, Integer.MAX_VALUE);

                builder.add(index);
            }
        }
        catch (Throwable t)
        {
            // all sstable indexes in view have been referenced, need to clean up when exception is thrown
            FileUtils.closeQuietly(builder.ranges());
            view.forEach(e -> e.getValue().forEach(SSTableIndex::release));
            throw t;
        }
        return builder.build();
    }

    // This is an ANN only query
    public CloseableIterator<ScoredPrimaryKey> getTopKRows(RowFilter.Expression expression)
    {
        assert expression.operator() == Operator.ANN;
        var planExpression = new Expression(getContext(expression))
                             .add(Operator.ANN, expression.getIndexValue().duplicate());

        // search memtable before referencing sstable indexes; otherwise we may miss newly flushed memtable index
        var memtableResults = getContext(expression).orderMemtable(queryContext, planExpression, mergeRange, limit);

        var queryView = new QueryViewBuilder(Collections.singleton(planExpression), mergeRange).build();

        try
        {
            var sstableResults = orderSstables(queryView, Collections.emptyList());
            sstableResults.addAll(memtableResults);
            return new MergeScoredPrimaryKeyIterator(sstableResults, queryView.referencedIndexes);
        }
        catch (Throwable t)
        {
            // all sstable indexes in view have been referenced, need to clean up when exception is thrown
            queryView.referencedIndexes.forEach(SSTableIndex::release);
            FileUtils.closeQuietly(memtableResults);
            throw t;
        }
    }

    // This is a hybrid query. We apply all other predicates before ordering and limiting.
    public CloseableIterator<ScoredPrimaryKey> getTopKRows(RangeIterator source, RowFilter.Expression expression)
    {
        List<CloseableIterator<ScoredPrimaryKey>> scoredPrimaryKeyIterators = new ArrayList<>();
        List<SSTableIndex> indexesToRelease = new ArrayList<>();
        try (var iter = new OrderingFilterRangeIterator<>(source, ORDER_CHUNK_SIZE, queryContext, list -> this.getTopKRows(list, expression)))
        {
            while (iter.hasNext())
            {
                var next = iter.next();
                scoredPrimaryKeyIterators.addAll(next.iterators);
                indexesToRelease.addAll(next.referencedIndexes);
            }
        }
        return new MergeScoredPrimaryKeyIterator(scoredPrimaryKeyIterators, indexesToRelease);
    }

    private IteratorsAndIndexes getTopKRows(List<PrimaryKey> sourceKeys, RowFilter.Expression expression)
    {
        Tracing.logAndTrace(logger, "SAI predicates produced {} keys", sourceKeys.size());

        // Filter out PKs now. Each PK is passed to every segment of the ANN index, so filtering shadowed keys
        // eagerly can save some work when going from PK to row id for on disk segments.
        // Since the result is shared with multiple streams, we use an unmodifiable list.
        var planExpression = new Expression(this.getContext(expression));
        planExpression.add(Operator.ANN, expression.getIndexValue().duplicate());

        // search memtable before referencing sstable indexes; otherwise we may miss newly flushed memtable index
        var memtableResults = this.getContext(expression)
                                  .orderResultsBy(queryContext, sourceKeys, planExpression, limit);
        var queryView = new QueryViewBuilder(Collections.singleton(planExpression), mergeRange).build();

        try
        {
            var sstableScoredPrimaryKeyIterators = orderSstables(queryView, sourceKeys);
            sstableScoredPrimaryKeyIterators.addAll(memtableResults);
            if (sstableScoredPrimaryKeyIterators.isEmpty())
            {
                // We release here because an empty vector index will produce 0 iterators
                // but still needs to be released.
                // VSTODO Maybe we can remove empty indexes from the view.
                queryView.referencedIndexes.forEach(SSTableIndex::release);
                return new IteratorsAndIndexes(Collections.emptyList(), Collections.emptySet());
            }
            return new IteratorsAndIndexes(sstableScoredPrimaryKeyIterators, queryView.referencedIndexes);
        }
        catch (Throwable t)
        {
            // all sstable indexes in view have been referenced, need to clean up when exception is thrown
            queryView.referencedIndexes.forEach(SSTableIndex::release);
            FileUtils.closeQuietly(memtableResults);
            throw t;
        }

    }

    /**
     * Create the list of iterators over {@link ScoredPrimaryKey} from the given {@link QueryViewBuilder.QueryView}.
     * @param queryView The view to use to create the iterators.
     * @param sourceKeys The source keys to use to create the iterators. Use an empty list to search all keys.
     * @return The list of iterators over {@link ScoredPrimaryKey}.
     */
    private List<CloseableIterator<ScoredPrimaryKey>> orderSstables(QueryViewBuilder.QueryView queryView, List<PrimaryKey> sourceKeys)
    {
        List<CloseableIterator<ScoredPrimaryKey>> results = new ArrayList<>();
        for (var e : queryView.view.values())
        {
            QueryViewBuilder.IndexExpression annIndexExpression = null;
            try
            {
                assert e.size() == 1 : "only one index is expected in ANN expression, found " + e.size() + " in " + e;
                annIndexExpression = e.get(0);
                var iterators = sourceKeys.isEmpty() ? annIndexExpression.index.orderBy(annIndexExpression.expression, mergeRange, queryContext, limit)
                                                     : annIndexExpression.index.orderResultsBy(queryContext, sourceKeys, annIndexExpression.expression, limit);
                results.addAll(iterators);
            }
            catch (Throwable ex)
            {
                // Close any iterators that were successfully opened before the exception
                FileUtils.closeQuietly(results);
                if (logger.isDebugEnabled() && !(ex instanceof AbortedOperationException) && annIndexExpression != null)
                {
                    var msg = String.format("Failed search on index %s, aborting query.", annIndexExpression.index.getSSTable());
                    logger.debug(annIndexExpression.index.getIndexContext().logMessage(msg), ex);
                }
                throw Throwables.cleaned(ex);
            }
        }
        return results;
    }

    public int getExactLimit()
    {
        return command.limits().count();
    }

    public IndexFeatureSet indexFeatureSet()
    {
        return indexFeatureSet;
    }

    /**
     * Returns whether this query is selecting the {@link PrimaryKey}.
     * The query selects the key if any of the following statements is true:
     *  1. The query is not row-aware
     *  2. The table associated with the query is not using clustering keys
     *  3. The clustering index filter for the command wants the row.
     *
     *  Item 3 is important in paged queries where the {@link org.apache.cassandra.db.filter.ClusteringIndexSliceFilter} for
     *  subsequent paged queries may not select rows that are returned by the index
     *  search because that is initially partition based.
     *
     * @param key The {@link PrimaryKey} to be tested
     * @return true if the key is selected by the query
     */
    public boolean selects(PrimaryKey key)
    {
        return !indexFeatureSet.isRowAware() ||
               key.hasEmptyClustering() ||
               command.clusteringIndexFilter(key.partitionKey()).selects(key.clustering());
    }

    private StorageAttachedIndex getBestIndexFor(RowFilter.Expression expression)
    {
        return cfs.indexManager.getBestIndexFor(expression, StorageAttachedIndex.class).orElse(null);
    }

    // Note: This method assumes that the selects method has already been called for the
    // key to avoid having to (potentially) call selects twice
    private ClusteringIndexFilter makeFilter(PrimaryKey key)
    {
        ClusteringIndexFilter clusteringIndexFilter = command.clusteringIndexFilter(key.partitionKey());

        if (!indexFeatureSet.isRowAware() || key.hasEmptyClustering())
            return clusteringIndexFilter;
        else
            return new ClusteringIndexNamesFilter(FBUtilities.singleton(key.clustering(), cfs.metadata().comparator),
                                                  clusteringIndexFilter.isReversed());
    }

    private static void releaseQuietly(SSTableIndex index)
    {
        try
        {
            index.release();
        }
        catch (Throwable e)
        {
            logger.error(index.getIndexContext().logMessage("Failed to release index on SSTable {}"), index.getSSTable().descriptor, e);
        }
    }

    /**
     * Used to release all resources and record metrics when query finishes.
     */
    public void finish()
    {
        if (tableQueryMetrics != null) tableQueryMetrics.record(queryContext);
    }

    /**
     * Try to reference all SSTableIndexes before querying on disk indexes.
     *
     * If we attempt to proceed into {@link TermIterator#build(Expression, Set, AbstractBounds, QueryContext, boolean, int)}
     * without first referencing all indexes, a concurrent compaction may decrement one or more of their backing
     * SSTable {@link Ref} instances. This will allow the {@link SSTableIndex} itself to be released and will fail the query.
     */
    private Map<Expression, NavigableSet<SSTableIndex>> referenceAndGetView(Operation.OperationType op, Collection<Expression> expressions)
    {
        SortedSet<String> indexNames = new TreeSet<>();
        try
        {
            while (true)
            {
                List<SSTableIndex> referencedIndexes = new ArrayList<>();
                boolean failed = false;

                Map<Expression, NavigableSet<SSTableIndex>> view = getView(op, expressions);

                for (SSTableIndex index : view.values().stream().flatMap(Collection::stream).collect(Collectors.toList()))
                {
                    indexNames.add(index.getIndexContext().getIndexName());

                    if (index.reference())
                    {
                        referencedIndexes.add(index);
                    }
                    else
                    {
                        failed = true;
                        break;
                    }
                }

                if (failed)
                {
                    // TODO: This might be a good candidate for a table/index group metric in the future...
                    referencedIndexes.forEach(QueryController::releaseQuietly);
                }
                else
                {
                    return view;
                }
            }
        }
        finally
        {
            Tracing.trace("Querying storage-attached indexes {}", indexNames);
        }
    }

    private Map<Expression, NavigableSet<SSTableIndex>> getView(Operation.OperationType op, Collection<Expression> expressions)
    {
        // first let's determine the primary expression if op is AND
        Pair<Expression, NavigableSet<SSTableIndex>> primary = (op == Operation.OperationType.AND) ? calculatePrimary(expressions) : null;

        Map<Expression, NavigableSet<SSTableIndex>> indexes = new HashMap<>();
        for (Expression e : expressions)
        {
            // NO_EQ and non-index column query should only act as FILTER BY for satisfiedBy(Row) method
            // because otherwise it likely to go through the whole index.
            if (!e.context.isIndexed())
            {
                continue;
            }

            // primary expression, we'll have to add as is
            if (primary != null && e.equals(primary.left))
            {
                indexes.put(primary.left, primary.right);

                continue;
            }

            View view = e.context.getView();

            NavigableSet<SSTableIndex> readers = new TreeSet<>(SSTableIndex.COMPARATOR);
            if (primary != null && primary.right.size() > 0)
            {
                for (SSTableIndex index : primary.right)
                    readers.addAll(view.match(index.minKey(), index.maxKey()));
            }
            else
            {
                readers.addAll(applyScope(view.match(e)));
            }

            indexes.put(e, readers);
        }

        return indexes;
    }

    private Pair<Expression, NavigableSet<SSTableIndex>> calculatePrimary(Collection<Expression> expressions)
    {
        Expression expression = null;
        NavigableSet<SSTableIndex> primaryIndexes = null;

        for (Expression e : expressions)
        {
            if (!e.context.isIndexed())
                continue;

            View view = e.context.getView();

            NavigableSet<SSTableIndex> indexes = new TreeSet<>(SSTableIndex.COMPARATOR);
            indexes.addAll(applyScope(view.match(e)));

            if (expression == null || primaryIndexes.size() > indexes.size())
            {
                primaryIndexes = indexes;
                expression = e;
            }
        }

        return expression == null ? null : Pair.create(expression, primaryIndexes);
    }

    private Set<SSTableIndex> applyScope(Set<SSTableIndex> indexes)
    {
        return Sets.filter(indexes, index -> {
            SSTableReader sstable = index.getSSTable();
            if (mergeRange instanceof Bounds && mergeRange.left.equals(mergeRange.right) && (!mergeRange.left.isMinimum()) && mergeRange.left instanceof DecoratedKey)
            {
                if (!sstable.getBloomFilter().isPresent((DecoratedKey)mergeRange.left))
                    return false;
            }
            return mergeRange.left.compareTo(sstable.last) <= 0 && (mergeRange.right.isMinimum() || sstable.first.compareTo(mergeRange.right) <= 0);
        });
    }

    /**
     * Returns the {@link DataRange} list covered by the specified {@link ReadCommand}.
     *
     * @param command a read command
     * @return the data ranges covered by {@code command}
     */
    private static List<DataRange> dataRanges(ReadCommand command)
    {
        if (command instanceof SinglePartitionReadCommand)
        {
            SinglePartitionReadCommand cmd = (SinglePartitionReadCommand) command;
            DecoratedKey key = cmd.partitionKey();
            return Lists.newArrayList(new DataRange(new Bounds<>(key, key), cmd.clusteringIndexFilter()));
        }
        else if (command instanceof PartitionRangeReadCommand)
        {
            PartitionRangeReadCommand cmd = (PartitionRangeReadCommand) command;
            return Lists.newArrayList(cmd.dataRange());
        }
        else if (command instanceof MultiRangeReadCommand)
        {
            MultiRangeReadCommand cmd = (MultiRangeReadCommand) command;
            return cmd.ranges();
        }
        else
        {
            throw new AssertionError("Unsupported read command type: " + command.getClass().getName());
        }
    }

    /**
     * Returns the fraction of the total rows of the table returned by the index
     *
     * @param iterator iterator over the keys from the index(es)
     */
    private float estimateSelectivity(RangeIterator iterator)
    {
        long totalRowCount = estimateTotalAvailableRows();
        return estimateSelectivity(iterator, totalRowCount);
    }

    private float estimateSelectivity(RangeIterator iterator, long totalRowCount)
    {
        float selectivity;

        if (iterator.getMaxKeys() == 0)
            return 0.0f;

        // For intersection and union we assume predicates are independent
        // (column values are not correlated with each other).
        // This assumption is not neccesarily true, but we have no statistical information to do any better.
        if (iterator instanceof RangeIntersectionIterator)
        {
            RangeIntersectionIterator intersectionIterator = (RangeIntersectionIterator) iterator;
            selectivity = 1.0f;
            for (RangeIterator range : intersectionIterator.ranges)
                selectivity *= estimateSelectivity(range, totalRowCount);
        }
        else if (iterator instanceof RangeUnionIterator)
        {
            selectivity = 0.0f;
            RangeUnionIterator unionIterator = (RangeUnionIterator) iterator;
            for (RangeIterator range : unionIterator.ranges)
                selectivity = 1.0f - (1.0f - selectivity) * (1.0f - estimateSelectivity(range, totalRowCount));
        }
        else
        {
            selectivity = Math.min((float) iterator.getMaxKeys() / totalRowCount, 1.0f);
        }

        return selectivity;
    }

    /**
     * Returns number of rows indexed accross all ssables and memtables
     */
    private long estimateTotalAvailableRows()
    {
        if (queryContext.totalAvailableRows() != null)
            return queryContext.totalAvailableRows();

        long memtableRows = StreamSupport.stream(cfs.getAllMemtables().spliterator(), false)
                                         .mapToLong(Memtable::rowCount)
                                         .sum();
        long sstableRows = cfs.getLiveSSTables()
                              .stream()
                              .mapToLong(SSTableReader::getTotalRows)
                              .sum();
        long totalRows = memtableRows + sstableRows;
        queryContext.setTotalAvailableRows(totalRows);
        return totalRows;
    }

    private static class IteratorsAndIndexes
    {
        final List<CloseableIterator<ScoredPrimaryKey>> iterators;
        final Set<SSTableIndex> referencedIndexes;

        IteratorsAndIndexes(List<CloseableIterator<ScoredPrimaryKey>> iterators, Set<SSTableIndex> indexes)
        {
            this.iterators = iterators;
            this.referencedIndexes = indexes;
        }
    }
}
