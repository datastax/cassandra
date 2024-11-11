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

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.analyzer.AnalyzerEqOperatorSupport;
import org.apache.cassandra.index.sai.disk.format.IndexFeatureSet;
import org.apache.cassandra.index.sai.metrics.TableQueryMetrics;

public class StorageAttachedIndexQueryPlan implements Index.QueryPlan
{
    private final ColumnFamilyStore cfs;
    private final TableQueryMetrics queryMetrics;

    /**
     * postIndexFilter comprised by those expressions in the read command row filter that can't be handled by
     * {@link FilterTree#isSatisfiedBy(DecoratedKey, Unfiltered, Row)}. That includes expressions targeted
     * at {@link RowFilter.UserExpression}s like those used by RLAC.
     */
    private final RowFilter postIndexFilter;
    private final Set<Index> indexes;
    private final IndexFeatureSet indexFeatureSet;
    private final Orderer orderer;

    private StorageAttachedIndexQueryPlan(ColumnFamilyStore cfs,
                                          TableQueryMetrics queryMetrics,
                                          RowFilter filter,
                                          ImmutableSet<Index> indexes,
                                          IndexFeatureSet indexFeatureSet)
    {
        this.cfs = cfs;
        this.queryMetrics = queryMetrics;
        this.postIndexFilter = filter.restrict(RowFilter.Expression::isUserDefined);
        this.indexes = indexes;
        this.indexFeatureSet = indexFeatureSet;
        this.orderer = Orderer.from(cfs.getIndexManager(), filter);
    }

    @Nullable
    public static StorageAttachedIndexQueryPlan create(ColumnFamilyStore cfs,
                                                       TableQueryMetrics queryMetrics,
                                                       Set<StorageAttachedIndex> allIndexes,
                                                       RowFilter rowFilter)
    {
        // collect the indexes that can be used with the provided row filter
        Set<StorageAttachedIndex> selectedIndexes = new HashSet<>();
        if (!selectedIndexes(rowFilter.root(), allIndexes, selectedIndexes))
            return null;

        // collect the features of the selected indexes
        IndexFeatureSet.Accumulator accumulator = new IndexFeatureSet.Accumulator();
        for (StorageAttachedIndex index : selectedIndexes)
            accumulator.accumulate(index.getIndexContext().indexFeatureSet());

        return new StorageAttachedIndexQueryPlan(cfs,
                                                 queryMetrics,
                                                 rowFilter,
                                                 ImmutableSet.copyOf(selectedIndexes),
                                                 accumulator.complete());
    }

    /**
     * Collects the indexes that can be used with the specified filtering tree without doing a full index scan.
     * </p>
     * The selected indexes are those that can satisfy at least one of the expressions of the filter, and that
     * aren't part of an OR operation that contains not indexed expressions, unless that OR operation is nested inside
     * an AND operation that has at least one indexed operation.
     * </p>
     * For example, for {@code x AND y} we can use any index in {@code x}, {@code y}, or both.
     * </p>
     * For {@code x OR y} we can't use a single index on {@code x} or {@code y} because we would need to do a full index
     * scan because of the unidexed expression. However, if both columns were indexed, we could use those two indexes.
     * </p>
     * For {@code (x OR y) AND z}, where {@code x} and {@code z} are indexed, we can use the index on {@code z}, even
     * though we will ignore the index on {@code x}.
     *
     * @param element a row filter tree node
     * @param allIndexes all the indexes in the index group
     * @param selectedIndexes the set of indexes where we'll add those indexes can be used with the specified expression
     * @return {@code true} if this has collected any indexes, {@code false} otherwise
     */
    private static boolean selectedIndexes(RowFilter.FilterElement element,
                                           Set<StorageAttachedIndex> allIndexes,
                                           Set<StorageAttachedIndex> selectedIndexes)
    {
        if (element.isDisjunction()) // OR, all restrictions should have an index
        {
            Set<StorageAttachedIndex> orIndexes = new HashSet<>();
            for (RowFilter.Expression expression : element.expressions())
            {
                if (!selectedIndexes(expression, allIndexes, orIndexes))
                    return false;
            }
            for (RowFilter.FilterElement child : element.children())
            {
                if (!selectedIndexes(child, allIndexes, orIndexes))
                    return false;
            }
            selectedIndexes.addAll(orIndexes);
            return !orIndexes.isEmpty();
        }
        else // AND, only one restriction needs to have an index
        {
            boolean hasIndex = false;
            for (RowFilter.Expression expression : element.expressions())
            {
                hasIndex |= selectedIndexes(expression, allIndexes, selectedIndexes);
            }
            for (RowFilter.FilterElement child : element.children())
            {
                hasIndex |= selectedIndexes(child, allIndexes, selectedIndexes);
            }
            return hasIndex;
        }
    }

    /**
     * Collects the indexes that can be used with the specified expression.
     *
     * @param expression a row filter expression
     * @param allIndexes all the indexes in the index group
     * @param selectedIndexes the set of indexes where we'll add those indexes can be used with the specified expression
     * @return {@code true} if this has collected any indexes, {@code false} otherwise
     */
    private static boolean selectedIndexes(RowFilter.Expression expression,
                                           Set<StorageAttachedIndex> allIndexes,
                                           Set<StorageAttachedIndex> selectedIndexes)
    {
        // we ignore user-defined expressions here because we don't have a way to translate their #isSatifiedBy
        // method, they will be included in the filter returned by QueryPlan#postIndexQueryFilter()
        if (expression.isUserDefined())
            return false;

        boolean hasIndex = false;
        for (StorageAttachedIndex index : allIndexes)
        {
            if (index.supportsExpression(expression.column(), expression.operator()))
            {
                selectedIndexes.add(index);
                hasIndex = true;
            }
        }
        return hasIndex;
    }

    @Override
    public Set<Index> getIndexes()
    {
        return indexes;
    }

    @Override
    public long getEstimatedResultRows()
    {
        // this is temporary (until proper QueryPlan is integrated into Cassandra)
        // and allows us to priority storage-attached indexes if any in the query since they
        // are going to be more efficient, to query and intersect, than built-in indexes.
        return Long.MIN_VALUE;
    }

    @Override
    public boolean shouldEstimateInitialConcurrency()
    {
        return false;
    }

    @Override
    public void validate(ReadCommand command) throws InvalidRequestException
    {
        // Maybe warn about EQ restrictions on analyzed columns
        AnalyzerEqOperatorSupport.maybeWarn(command.rowFilter(), indexes);

        // Validate index by index
        Index.QueryPlan.super.validate(command);
    }

    @Override
    public Index.Searcher searcherFor(ReadCommand command)
    {
        return new StorageAttachedIndexSearcher(cfs,
                                                queryMetrics,
                                                command,
                                                orderer,
                                                indexFeatureSet,
                                                DatabaseDescriptor.getRangeRpcTimeout(TimeUnit.MILLISECONDS));
    }

    /**
     * Called on coordinator after merging replica responses before returning to client
     */
    @Override
    public Function<PartitionIterator, PartitionIterator> postProcessor(ReadCommand command)
    {
        if (!isTopK())
            return partitions -> partitions;

        // in case of top-k query, filter out rows that are not actually global top-K
        return partitions -> (PartitionIterator) new TopKProcessor(command).filter(partitions);
    }

    /**
     * @return a filter with all the expressions that are user-defined
     */
    @Override
    public RowFilter postIndexQueryFilter()
    {
        return postIndexFilter;
    }

    @Override
    public boolean supportsMultiRangeReadCommand()
    {
        return true;
    }

    @Override
    public boolean isTopK()
    {
        return orderer != null;
    }
}
