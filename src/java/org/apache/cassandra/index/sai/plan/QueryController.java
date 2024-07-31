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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
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
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.rows.BaseRowIterator;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.SSTableIndex;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.disk.format.IndexFeatureSet;
import org.apache.cassandra.index.sai.disk.v1.Segment;
import org.apache.cassandra.index.sai.disk.vector.VectorMemtableIndex;
import org.apache.cassandra.index.sai.memory.MemtableIndex;
import org.apache.cassandra.index.sai.metrics.TableQueryMetrics;
import org.apache.cassandra.index.sai.utils.AbortedOperationException;
import org.apache.cassandra.index.sai.utils.OrderingFilterRangeIterator;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.RangeIntersectionIterator;
import org.apache.cassandra.index.sai.utils.RangeIterator;
import org.apache.cassandra.index.sai.utils.PrimaryKeyWithSortKey;
import org.apache.cassandra.index.sai.utils.RowWithSourceTable;
import org.apache.cassandra.index.sai.utils.RangeUtil;
import org.apache.cassandra.index.sai.utils.SoftLimitUtil;
import org.apache.cassandra.index.sai.utils.TermIterator;
import org.apache.cassandra.index.sai.view.View;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MergeIterator;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.Reducer;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.concurrent.Ref;

import static java.lang.Math.max;
import static org.apache.cassandra.config.CassandraRelevantProperties.SAI_VECTOR_SEARCH_ORDER_CHUNK_SIZE;

public class QueryController implements Plan.Executor, Plan.CostEstimator
{
    private static final Logger logger = LoggerFactory.getLogger(QueryController.class);

    public static final int ORDER_CHUNK_SIZE = SAI_VECTOR_SEARCH_ORDER_CHUNK_SIZE.getInt();

    /**
     * Controls whether we optimize query plans.
     * 0 disables the optimizer.
     * 1 enables the optimizer and tells the optimizer to respect the intersection clause limit.
     * Higher values enable the optimizer and disable the hard intersection clause limit.
     * Note: the config is not final to simplify testing.
     */
    @VisibleForTesting
    public static int QUERY_OPT_LEVEL = Integer.getInteger("cassandra.sai.query.optimization.level", 1);

    private final ColumnFamilyStore cfs;
    private final ReadCommand command;
    private final Orderer orderer;
    private final QueryContext queryContext;
    private final TableQueryMetrics tableQueryMetrics;
    private final IndexFeatureSet indexFeatureSet;
    private final List<DataRange> ranges;
    private final AbstractBounds<PartitionPosition> mergeRange;

    private final PrimaryKey.Factory keyFactory;
    private final PrimaryKey firstPrimaryKey;

    final Plan.Factory planFactory;

    /**
     * Holds the primary key iterators for indexed expressions in the query (i.e. leaves of the expression tree).
     * We will construct the final iterator from those.
     * We need a MultiMap because the same Expression can occur more than once in a query.
     * <p>
     * Longer explanation why this is needed:
     * In order to construct a Plan for a query, we need predicate selectivity estimates. But at the moment
     * of writing this code, the only way to estimate an index predicate selectivity is to look at the posting
     * list(s) in the index, by obtaining a {@link RangeIterator} and callling {@link RangeIterator#getMaxKeys()} on it.
     * Hence, we need to create the iterators before creating the Plan.
     * But later when we assemble the final key iterator according to the optimized Plan, we need those iterators
     * again. In order to avoid recreating them, which would be costly, we just keep them here in this map.
     */
    private final Multimap<Expression, RangeIterator> keyIterators = ArrayListMultimap.create();

    static
    {
        logger.info(String.format("Query plan optimization is %s (level = %d)",
                                  QUERY_OPT_LEVEL > 0 ? "enabled" : "disabled",
                                  QUERY_OPT_LEVEL));
    }

    @VisibleForTesting
    public QueryController(ColumnFamilyStore cfs,
                           ReadCommand command,
                           IndexFeatureSet indexFeatureSet,
                           QueryContext queryContext,
                           TableQueryMetrics tableQueryMetrics)
    {
        this(cfs, command, null, indexFeatureSet, queryContext, tableQueryMetrics);
    }

    public QueryController(ColumnFamilyStore cfs,
                           ReadCommand command,
                           Orderer orderer,
                           IndexFeatureSet indexFeatureSet,
                           QueryContext queryContext,
                           TableQueryMetrics tableQueryMetrics)
    {
        this.cfs = cfs;
        this.command = command;
        this.orderer = orderer;
        this.queryContext = queryContext;
        this.tableQueryMetrics = tableQueryMetrics;
        this.indexFeatureSet = indexFeatureSet;
        this.ranges = dataRanges(command);
        DataRange first = ranges.get(0);
        DataRange last = ranges.get(ranges.size() - 1);
        this.mergeRange = ranges.size() == 1 ? first.keyRange() : first.keyRange().withNewRight(last.keyRange().right);

        this.keyFactory = PrimaryKey.factory(cfs.metadata().comparator, indexFeatureSet);
        this.firstPrimaryKey = keyFactory.createTokenOnly(mergeRange.left.getToken());
        var tableMetrics = new Plan.TableMetrics(estimateTotalAvailableRows(ranges),
                                                 avgCellsPerRow(),
                                                 avgRowSizeInBytes(),
                                                 cfs.getLiveSSTables().size());
        this.planFactory = new Plan.Factory(tableMetrics, this);
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
        // NOTE: we cannot remove the order by filter expression here yet because it is used in the FilterTree class
        // to filter out shadowed rows.
        return this.command.rowFilter().root();
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
                                determineIndexTargetType(expression),
                                null,
                                cfs);
    }

    /**
     * Determines the {@link IndexTarget.Type} for the expression. In this case we are only interested in map types and
     * the operator being used in the expression.
     */
    public static IndexTarget.Type determineIndexTargetType(RowFilter.Expression expression)
    {
        AbstractType<?> type  = expression.column().type;
        IndexTarget.Type indexTargetType = IndexTarget.Type.SIMPLE;
        if (type.isCollection() && type.isMultiCell())
        {
            CollectionType<?> collection = ((CollectionType<?>) type);
            if (collection.kind == CollectionType.Kind.MAP)
            {
                Operator operator = expression.operator();
                switch (operator)
                {
                    case EQ:
                    case NEQ:
                    case LT:
                    case LTE:
                    case GT:
                    case GTE:
                        indexTargetType = IndexTarget.Type.KEYS_AND_VALUES;
                        break;
                    case CONTAINS:
                    case NOT_CONTAINS:
                        indexTargetType = IndexTarget.Type.VALUES;
                        break;
                    case CONTAINS_KEY:
                    case NOT_CONTAINS_KEY:
                        indexTargetType = IndexTarget.Type.KEYS;
                        break;
                    default:
                        throw new InvalidRequestException("Invalid operator " + operator + " for map type");
                }
            }
        }
        return indexTargetType;
    }

    /**
     * Get an iterator over the rows for this partition key. Builds a search view that includes all memtables and all
     * {@link SSTableSet#LIVE} sstables.
     * @param key
     * @param executionController
     * @return
     */
    public UnfilteredRowIterator getPartition(PrimaryKey key, ReadExecutionController executionController)
    {
        if (key == null)
            throw new IllegalArgumentException("non-null key required");

        SinglePartitionReadCommand partition = getPartitionReadCommand(key, executionController);
        return partition.queryMemtableAndDisk(cfs, executionController);
    }

    /**
     * Get an iterator over the rows for this partition key. Restrict the search to the specified view.
     * @param key
     * @param executionController
     * @return
     */
    public UnfilteredRowIterator getPartition(PrimaryKey key, ColumnFamilyStore.ViewFragment view, ReadExecutionController executionController)
    {
        if (key == null)
            throw new IllegalArgumentException("non-null key required");

        SinglePartitionReadCommand partition = getPartitionReadCommand(key, executionController);

        // Class to transform the row to include its source table.
        Function<Object, Transformation<BaseRowIterator<?>>> rowTransformer = (Object sourceTable) -> new Transformation<>()
        {
            @Override
            protected Row applyToRow(Row row)
            {
                return new RowWithSourceTable(row, sourceTable);
            }
        };

        return partition.queryMemtableAndDisk(cfs, view, rowTransformer, executionController);
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

    private Plan buildPlan()
    {
        Plan.KeysIteration keysIterationPlan = buildKeysIterationPlan();
        Plan.RowsIteration rowsIteration = planFactory.fetch(keysIterationPlan);
        rowsIteration = planFactory.recheckFilter(command.rowFilter(), rowsIteration);
        rowsIteration = planFactory.limit(rowsIteration, command.limits().rows());

        Plan optimizedPlan;
        optimizedPlan = QUERY_OPT_LEVEL > 0
                        ? rowsIteration.optimize()
                        : rowsIteration;
        optimizedPlan = RangeIntersectionIterator.INTERSECTION_CLAUSE_LIMIT > 0 && QUERY_OPT_LEVEL <= 1
                        ? optimizedPlan.limitIntersectedClauses(RangeIntersectionIterator.INTERSECTION_CLAUSE_LIMIT)
                        : optimizedPlan;

        if (optimizedPlan.contains(node -> node instanceof Plan.AnnIndexScan))
            queryContext.setFilterSortOrder(QueryContext.FilterSortOrder.SORT_THEN_FILTER);
        if (optimizedPlan.contains(node -> node instanceof Plan.KeysSort))
            queryContext.setFilterSortOrder(QueryContext.FilterSortOrder.FILTER_THEN_SORT);

        if (logger.isTraceEnabled())
            logger.trace("Query execution plan:\n" + optimizedPlan.toStringRecursive());

        if (Tracing.isTracing())
        {
            Tracing.trace("Query execution plan:\n" + optimizedPlan.toStringRecursive());
            List<Plan.IndexScan> origIndexScans = keysIterationPlan.nodesOfType(Plan.IndexScan.class);
            List<Plan.IndexScan> selectedIndexScans = optimizedPlan.nodesOfType(Plan.IndexScan.class);
            Tracing.trace("Selecting {} {} of {} out of {} indexes",
                          selectedIndexScans.size(),
                          selectedIndexScans.size() > 1 ? "indexes with cardinalities" : "index with cardinality",
                          selectedIndexScans.stream().map(s -> "" + ((long) s.expectedKeys())).collect(Collectors.joining(", ")),
                          origIndexScans.size());
        }
        return optimizedPlan;
    }

    private Plan.KeysIteration buildKeysIterationPlan()
    {
        // Remove the ORDER BY filter expression from the filter tree, as it is added below.
        var filterElement = filterOperation().filter(e -> !Orderer.isFilterExpressionOrderer(e));
        Plan.KeysIteration keysIterationPlan = Operation.Node.buildTree(filterElement)
                                                             .analyzeTree(this)
                                                             .plan(this);

        // Because the orderer has a specific queue view
        if (orderer != null)
            keysIterationPlan = planFactory.sort(keysIterationPlan, orderer);

        assert keysIterationPlan != planFactory.everything; // This would mean we have no WHERE nor ANN clauses at all
        return keysIterationPlan;
    }

    public Iterator<? extends PrimaryKey> buildIterator()
    {
        try
        {
            Plan plan = buildPlan();
            Plan.KeysIteration keysIteration = plan.firstNodeOfType(Plan.KeysIteration.class);
            assert keysIteration != null : "No index scan found";

            // We need to estimate how many keys we'll be fetching for better performance.
            // Some indexes may make a good use of knowing the number of requested keys in advance,
            // because incremental re-requesting for more keys may be more expensive than getting them
            // all at once.
            Plan.Limit limit = plan.firstNodeOfType(Plan.Limit.class);
            int hardLimit = limit != null ? limit.limit : Integer.MAX_VALUE;

            // If there is filtering in the plan, we may need to fetch more keys
            // from the index, so we need to reflect that in the limit.
            // (In general, it costs more to over-fetch than under-fetch, because resume is cheap, so we don't
            // need to use a high confidenceLevel.)
            Plan.Filter filter = plan.firstNodeOfType(Plan.Filter.class);
            int softLimit = (filter != null)
                          ? SoftLimitUtil.softLimit(hardLimit, 0.5, filter.selectivity())
                          : hardLimit;

            return keysIteration.execute(this, softLimit);
        }
        finally
        {
            // Because we optimize the plan, it is possible that there exist iterators that we
            // constructed but which weren't used by the final plan.
            // Let's close them here, so they don't hold the resources.
            closeUnusedIterators();
        }
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


    public FilterTree buildFilter()
    {
        return Operation.Node.buildTree(filterOperation()).analyzeTree(this).filterTree();
    }

    /**
     * Build a {@link Plan} from the given list of expressions by applying given operation (OR/AND).
     * Building of such builder involves index search, results of which are persisted in the internal resources list
     *
     * @param builder The plan node builder which receives the built index scans
     * @param expressions The expressions to build the plan from
     */
    public void buildPlanForExpressions(Plan.Builder builder, Collection<Expression> expressions)
    {
        Operation.OperationType op = builder.type;
        assert !expressions.isEmpty() : "expressions should not be empty for " + op + " in " + command.rowFilter().root();

        // VSTODO move ANN out of expressions and into its own abstraction? That will help get generic ORDER BY support
        Collection<Expression> exp = expressions.stream().filter(e -> e.operation != Expression.Op.ORDER_BY).collect(Collectors.toList());

        // we cannot use indexes with OR if we have a mix of indexed and non-indexed columns (see CNDB-10142)
        if (op == Operation.OperationType.OR && !exp.stream().allMatch(e -> e.context.isIndexed()))
        {
            builder.add(planFactory.everything);
            return;
        }

        boolean defer = builder.type == Operation.OperationType.OR || RangeIntersectionIterator.shouldDefer(exp.size());

        Set<Map.Entry<Expression, NavigableSet<SSTableIndex>>> view = referenceAndGetView(op, expressions).entrySet();
        try
        {
            var viewIterator = view.iterator();
            while (viewIterator.hasNext())
            {
                var e = viewIterator.next();
                Expression predicate = e.getKey();
                RangeIterator iterator = TermIterator.build(predicate, e.getValue(), mergeRange, queryContext, defer, Integer.MAX_VALUE);

                // The returned iterator owns the set of indexes now and will release them on close,
                // so let's remove it from the view to avoid double-release.
                viewIterator.remove();

                // Cache the iterator for when the plan node needs it for the execution
                keyIterators.put(predicate, iterator);

                long keysCount = Math.min(iterator.getMaxKeys(), planFactory.tableMetrics.rows);
                Plan.KeysIteration plan = planFactory.indexScan(predicate, keysCount);
                builder.add(plan);
            }
        }
        catch (Throwable t)
        {
            // Release the references to the indexes that we didn't get the iterator for
            view.forEach(e -> e.getValue().forEach(SSTableIndex::release));
            throw t;
        }
    }

    @Override
    public Iterator<? extends PrimaryKey> getKeysFromIndex(Expression predicate)
    {
        Collection<RangeIterator> rangeIterators = keyIterators.get(predicate);
        // This should be never empty, because we put iterators in this map when we create the IndexScan nodes of the Plan
        assert !rangeIterators.isEmpty() : "No iterator found for predicate: " + predicate;

        RangeIterator iterator = rangeIterators.iterator().next();
        keyIterators.remove(predicate, iterator);  // remove so we never accidentally reuse the same iterator
        return iterator;
    }

    /**
     * Use the configured {@link Orderer} to create an iterator that sorts the whole table by a specific column.
     */
    @Override
    public CloseableIterator<PrimaryKeyWithSortKey> getTopKRows(int softLimit)
    {
        List<CloseableIterator<PrimaryKeyWithSortKey>> memtableResults = new ArrayList<>();
        try
        {
            for (MemtableIndex index : queryContext.view.memtableIndexes)
                memtableResults.addAll(index.orderBy(queryContext, orderer, mergeRange, softLimit));

            var sstableResults = orderSstables(queryContext.view, Collections.emptyList(), softLimit);
            sstableResults.addAll(memtableResults);
            return MergeIterator.getNonReducingCloseable(sstableResults, orderer.getComparator());
        }
        catch (Throwable t)
        {
            FileUtils.closeQuietly(memtableResults);
            throw t;
        }
    }

    /**
     * Use the configured {@link Orderer} to sort the rows from the given source iterator.
     */
    public CloseableIterator<PrimaryKeyWithSortKey> getTopKRows(RangeIterator source, int softLimit)
    {
        List<CloseableIterator<PrimaryKeyWithSortKey>> scoredPrimaryKeyIterators = new ArrayList<>();
        OrderingFilterRangeIterator<List<CloseableIterator<PrimaryKeyWithSortKey>>> iter = null;
        try
        {
            // We cannot close the source iterator eagerly because it produces partially loaded PrimaryKeys
            // that might not be needed until a deeper search into the ordering index, which happens after
            // we exit this block.
            iter = new OrderingFilterRangeIterator<>(source,
                                                     ORDER_CHUNK_SIZE,
                                                     queryContext,
                                                     list -> this.getTopKRows(list, softLimit));
            while (iter.hasNext())
            {
                var next = iter.next();
                scoredPrimaryKeyIterators.addAll(next);
            }
            var merged = MergeIterator.getNonReducingCloseable(scoredPrimaryKeyIterators, orderer.getComparator());
            return CloseableIterator.withOnClose(merged, iter::close);
        }
        catch (Throwable t)
        {
            FileUtils.closeQuietly(iter);
            FileUtils.closeQuietly(scoredPrimaryKeyIterators);
            throw t;
        }
    }

    private List<CloseableIterator<PrimaryKeyWithSortKey>> getTopKRows(List<PrimaryKey> sourceKeys, int softLimit)
    {
        Tracing.logAndTrace(logger, "SAI predicates produced {} keys", sourceKeys.size());
        var memtableResults = queryContext.view.memtableIndexes.stream()
                                                               .map(index -> index.orderResultsBy(queryContext, sourceKeys, orderer, softLimit))
                                                               .collect(Collectors.toList());
        try
        {
            var sstableScoredPrimaryKeyIterators = orderSstables(queryContext.view, sourceKeys, softLimit);
            sstableScoredPrimaryKeyIterators.addAll(memtableResults);
            return sstableScoredPrimaryKeyIterators;
        }
        catch (Throwable t)
        {
            FileUtils.closeQuietly(memtableResults);
            throw t;
        }

    }

    /**
     * Create the list of iterators over {@link PrimaryKeyWithSortKey} from the given {@link QueryViewBuilder.QueryView}.
     * @param queryView The view to use to create the iterators.
     * @param sourceKeys The source keys to use to create the iterators. Use an empty list to search all keys.
     * @return The list of iterators over {@link PrimaryKeyWithSortKey}.
     */
    private List<CloseableIterator<PrimaryKeyWithSortKey>> orderSstables(QueryViewBuilder.QueryView queryView, List<PrimaryKey> sourceKeys, int softLimit)
    {
        List<CloseableIterator<PrimaryKeyWithSortKey>> results = new ArrayList<>();
        long totalRows = queryView.view.sstables.stream().mapToLong(SSTableReader::getTotalRows).sum();
        for (var index : queryView.referencedIndexes)
        {
            try
            {
                var iterators = sourceKeys.isEmpty() ? index.orderBy(orderer, mergeRange, queryContext, softLimit, totalRows)
                                                     : index.orderResultsBy(queryContext, sourceKeys, orderer, softLimit, totalRows);
                results.addAll(iterators);
            }
            catch (Throwable ex)
            {
                // Close any iterators that were successfully opened before the exception
                FileUtils.closeQuietly(results);
                if (logger.isDebugEnabled() && !(ex instanceof AbortedOperationException))
                {
                    var msg = String.format("Failed search on index %s, aborting query.", index.getSSTable());
                    logger.debug(index.getIndexContext().logMessage(msg), ex);
                }
                throw Throwables.cleaned(ex);
            }
        }
        return results;
    }

    public IndexFeatureSet indexFeatureSet()
    {
        return indexFeatureSet;
    }

    public Orderer getOrderer()
    {
        return orderer;
    }

    /**
     * Returns whether this query is selecting the {@link PrimaryKey}.
     * The query selects the key if any of the following statements is true:
     *  1. The query is not row-aware
     *  2. The table associated with the query is not using clustering keys
     *  3. The clustering index filter for the command wants the row.
     *
     *  Item 3 is important in paged queries where the {@link ClusteringIndexSliceFilter} for
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
        closeUnusedIterators();
        if (tableQueryMetrics != null) tableQueryMetrics.record(queryContext);
    }

    private void closeUnusedIterators()
    {
        Iterator<Map.Entry<Expression, RangeIterator>> entries = keyIterators.entries().iterator();
        while (entries.hasNext())
        {
            FileUtils.closeQuietly(entries.next().getValue());
            entries.remove();
        }
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
     * Returns the total count of rows in the sstables which overlap with any of the given ranges
     * and all live memtables.
     */
    private long estimateTotalAvailableRows(List<DataRange> ranges)
    {
        long rows = 0;

        for (Memtable memtable : cfs.getAllMemtables())
            rows += estimateMemtableRowCount(memtable);

        for (SSTableReader sstable : cfs.getLiveSSTables())
            for (DataRange range : ranges)
                if (RangeUtil.intersects(sstable, range.keyRange()))
                    rows += sstable.getTotalRows();

        return rows;
    }

    private static long estimateMemtableRowCount(Memtable memtable)
    {
        long rowSize = memtable.getEstimatedAverageRowSize();
        return rowSize > 0 ? memtable.getLiveDataSize() / rowSize : 0;
    }

    @Override
    public int estimateAnnNodesVisited(Orderer ordering, int limit, long candidates)
    {
        IndexContext context = ordering.context;
        Collection<MemtableIndex> memtables = context.getLiveMemtables().values();
        View queryView = context.getView();

        int annNodesCount = 0;
        for (MemtableIndex index : memtables)
        {
            // TODO: maybe limit and candidates should be scaled proportionally by the size of the memtable?
            int memtableCandidates = (int) Math.min(Integer.MAX_VALUE, candidates);
            annNodesCount += ((VectorMemtableIndex) index).estimateAnnNodesVisited(limit, memtableCandidates);
        }

        long totalRows = 0;
        for (SSTableIndex index : queryView.getIndexes())
            totalRows += index.getSSTable().getTotalRows();

        for (SSTableIndex index : queryView.getIndexes())
        {
            for (Segment segment : index.getSegments())
            {
                if (!segment.intersects(mergeRange))
                    continue;
                int segmentLimit = segment.proportionalAnnLimit(limit, totalRows);
                int segmentCandidates = max(1, (int) (candidates * (double) segment.metadata.numRows / totalRows));
                annNodesCount += segment.estimateAnnNodesVisited(segmentLimit, segmentCandidates);
            }
        }
        return annNodesCount;
    }
}
