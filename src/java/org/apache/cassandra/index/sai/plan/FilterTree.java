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
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import com.google.common.collect.ListMultimap;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.ColumnMetadata.Kind;
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

        final int now = FBUtilities.nowInSeconds();
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

                if (TypeUtil.isNonFrozenCollection(column.type))
                {
                    Iterator<ByteBuffer> valueIterator = filter.context.getValuesOf(row, now);
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
}
