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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.SSTableIndex;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.ColumnMetadata.Kind;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.index.sai.plan.Operation.OperationType;

/**
 * Tree-like structure to filter base table data using indexed expressions and non-user-defined filters.
 *
 * This is needed because:
 * 1. SAI doesn't index tombstones, base data may have been shadowed.
 * 2. SAI indexes partition offset, not all rows in partition match index condition.
 * 3. Replica filter protecting may fetch data that doesn't match index expressions.
 */
public class FilterTree
{
    protected final OperationType op;
    protected final ListMultimap<ColumnMetadata, Expression> expressions;
    protected final List<FilterTree> children = new ArrayList<>();

    FilterTree(OperationType operation,
               ListMultimap<ColumnMetadata, Expression> expressions)
    {
        this.op = operation;
        this.expressions = expressions;
    }

    void addChild(FilterTree child)
    {
        children.add(child);
    }

    public boolean isSatisfiedBy(DecoratedKey key, Unfiltered currentCluster, Row staticRow)
    {
        boolean result = localSatisfiedBy(key, currentCluster, staticRow);

        if (shouldReturnNow(result))
            return result;

        for (FilterTree child : children)
        {
            result = op.apply(result, child.isSatisfiedBy(key, currentCluster, staticRow));
            if (shouldReturnNow(result))
                return result;
        }

        return result;
    }

    private boolean localSatisfiedBy(DecoratedKey key, Unfiltered currentCluster, Row staticRow)
    {
        if (currentCluster == null || !currentCluster.isRow())
            return false;

        final long now = FBUtilities.nowInSeconds();
        boolean result = op == OperationType.AND;

        Iterator<ColumnMetadata> columnIterator = expressions.keySet().iterator();
        while(columnIterator.hasNext())
        {
            ColumnMetadata column = columnIterator.next();
            Row row = column.kind == Kind.STATIC ? staticRow : (Row)currentCluster;

            // If there is a column with multiple expressions that can mean an OR or (in the case of map
            // collections) it can mean different map indexes.
            List<Expression> filters = expressions.get(column);

            // We do a reverse iteration over the filters because NOT_EQ operations will be at the end
            // of the filter list and we want to check them first.
            ListIterator<Expression> filterIterator = filters.listIterator(filters.size());
            while(filterIterator.hasPrevious())
            {
                Expression filter = filterIterator.previous();

                if (filter.context.isMultiValued())
                {
                    Iterator<ByteBuffer> valueIterator = filter.context.getValuesOf(key, row, now);
                    result = op.apply(result, filter.isSatisfiedBy(valueIterator));
                }
                else
                {
                    ByteBuffer value = filter.context.getValueOf(key, row, now);
                    result = op.apply(result, filter.isSatisfiedBy(value));
                }

                if (shouldReturnNow(result))
                    return result;
            }
        }
        return result;
    }

    /**
     * When evaluating an AND expression, if the current result is false, we can return immediately.
     * When evaluating an OR expression, if the current result is true, we can return immediately.
     * @param result the current result
     * @return true if it is valid to return the current result
     */
    private boolean shouldReturnNow(boolean result) {
        return (op == OperationType.AND && !result) || (op == OperationType.OR && result);
    }

    /**
     * @return the number of unique SSTable indexes that are referenced by the expressions in this filter tree.
     */
    public int numSSTableIndexes()
    {
        Set<SSTableIndex> referencedIndexes = new HashSet<>();
        sstableIndexes(referencedIndexes);
        return referencedIndexes.size();
    }

    protected void sstableIndexes(Set<SSTableIndex> indexes)
    {
        for (Expression expression : expressions.values())
            indexes.addAll(expression.context.getView().getIndexes());
        for (FilterTree child : children)
            child.sstableIndexes(indexes);
    }

    /**
     * The filter of a {@code CONTAINS ELEMENT} group: it evaluates the conditions of the group on each element of
     * the collection, as {@link RowFilter.ElementMatchExpression} does (the same evaluation as the filtering without
     * index and as the coordinator's, so that the results are the same). The indexes only select its candidate rows
     * (see {@link Operation.ElementNode}): the rows where each positive condition is satisfied by some element, which
     * this filter rejects when no single element satisfies the group. The rows it evaluates and rejects are counted in
     * the {@link QueryContext} when the group's candidates are the source of the keys (no OR above the group): they
     * approximate the false positives of the candidates (the planner may still read other keys, e.g. if it drops the
     * candidates' index scans from an intersection, and the rows of a partition are all evaluated, the key being the
     * partition in AA). The filter of the candidates is kept only for the indexes it references (see
     * {@link #numSSTableIndexes()}).
     */
    static class ElementFilter extends FilterTree
    {
        private final RowFilter.ElementMatchExpression expression;
        private final TableMetadata metadata;
        // the context of the query: its time, and the counters of the rows evaluated and rejected by the filter
        private final QueryContext queryContext;
        // whether to count the rows: when the group's candidates are the source of the keys of the query
        private final boolean counts;
        // the filter tree of the candidates, never evaluated, for the indexes it references
        @Nullable
        private final FilterTree candidates;

        ElementFilter(RowFilter.ElementMatchExpression expression,
                      TableMetadata metadata,
                      QueryContext queryContext,
                      boolean counts,
                      @Nullable FilterTree candidates)
        {
            super(OperationType.AND, ArrayListMultimap.create());
            this.expression = expression;
            this.metadata = metadata;
            this.queryContext = queryContext;
            this.counts = counts;
            this.candidates = candidates;
        }

        @Override
        protected void sstableIndexes(Set<SSTableIndex> indexes)
        {
            if (candidates != null)
                candidates.sstableIndexes(indexes);
        }

        @Override
        public boolean isSatisfiedBy(DecoratedKey key, Unfiltered currentCluster, Row staticRow)
        {
            if (currentCluster == null || !currentCluster.isRow())
                return false;

            // the static row alone (the check of a partition without matching rows) has no clustering nor regular
            // values: it never satisfies a group on them, not even a NOT CONTAINS ELEMENT
            ColumnMetadata column = expression.column();
            boolean isStaticRowCheck = ((Row) currentCluster).isStatic();
            if (isStaticRowCheck && !column.isStatic() && !column.isPartitionKey())
                return false;

            // the time of the read command, as the filtering without index and the coordinator use
            Row row = column.isStatic() ? staticRow : (Row) currentCluster;
            boolean satisfied = expression.isSatisfiedBy(metadata, key, row, queryContext.nowInSec());
            // the rows, not the check of the static row of a partition without matching rows
            if (counts && !isStaticRowCheck)
            {
                queryContext.addElementFilterCandidates(1);
                if (!satisfied)
                    queryContext.addElementFilterRejections(1);
            }
            return satisfied;
        }
    }
}
