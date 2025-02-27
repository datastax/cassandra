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
import java.util.Arrays;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.utils.PrimaryKeyWithSortKey;
import org.apache.cassandra.index.sai.utils.TypeUtil;

/**
 * An SAI Orderer represents an index based order by clause.
 */
public class Orderer
{
    // The list of operators that are valid for order by clauses.
    static final EnumSet<Operator> ORDER_BY_OPERATORS = EnumSet.of(Operator.ANN,
                                                                   Operator.BM25,
                                                                   Operator.ORDER_BY_ASC,
                                                                   Operator.ORDER_BY_DESC);

    public final IndexContext context;
    public final Operator operator;
    public final ByteBuffer term;
    private float[] vector;
    private List<ByteBuffer> queryTerms;

    /**
     * Create an orderer for the given index context, operator, and term.
     * @param context the index context, used to build the view of memtables and sstables for query execution.
     * @param operator the operator for the order by clause.
     * @param term the term to order by (not always relevant)
     */
    public Orderer(IndexContext context, Operator operator, ByteBuffer term)
    {
        this.context = context;
        assert ORDER_BY_OPERATORS.contains(operator) : "Invalid operator for order by clause " + operator;
        this.operator = operator;
        this.term = term;
    }

    public String getIndexName()
    {
        return context.getIndexName();
    }

    public boolean isAscending()
    {
        // Note: ANN is always descending.
        return operator == Operator.ORDER_BY_ASC;
    }

    public Comparator<? super PrimaryKeyWithSortKey> getComparator()
    {
        // ANN/BM25's PrimaryKeyWithSortKey is always descending, so we use the natural order for the priority queue
        return (isAscending() || isANN() || isBM25()) ? Comparator.naturalOrder() : Comparator.reverseOrder();
    }

    public boolean isLiteral()
    {
        return context.isLiteral();
    }

    public boolean isANN()
    {
        return operator == Operator.ANN;
    }

    public boolean isBM25()
    {
        return operator == Operator.BM25;
    }

    @Nullable
    public static Orderer from(SecondaryIndexManager indexManager, RowFilter filter)
    {
        var expressions = filter.root().expressions().stream().filter(Orderer::isFilterExpressionOrderer).collect(Collectors.toList());
        if (expressions.isEmpty())
            return null;
        var orderRowFilter = expressions.get(0);
        var index = indexManager.getBestIndexFor(orderRowFilter, StorageAttachedIndex.class)
                                .orElseThrow(() -> new IllegalStateException("No index found for order by clause"));
        return new Orderer(index.getIndexContext(), orderRowFilter.operator(), orderRowFilter.getIndexValue());
    }

    public static boolean isFilterExpressionOrderer(RowFilter.Expression expression)
    {
        return ORDER_BY_OPERATORS.contains(expression.operator());
    }

    @Override
    public String toString()
    {
        String direction = isAscending() ? "ASC" : "DESC";
        if (isANN())
            return context.getColumnName() + " ANN OF " + Arrays.toString(getVectorTerm()) + ' ' + direction;
        if (isBM25())
            return context.getColumnName() + " BM25 OF " + TypeUtil.getString(term, context.getValidator()) + ' ' + direction;
        return context.getColumnName() + ' ' + direction;
    }

    public float[] getVectorTerm()
    {
        if (vector == null)
            vector = TypeUtil.decomposeVector(context.getValidator(), term);
        return vector;
    }

    public List<ByteBuffer> getQueryTerms()
    {
        if (queryTerms != null)
            return queryTerms;

        var queryAnalyzer = context.getQueryAnalyzerFactory().create();
        // Split query into terms
        var uniqueTerms = new HashSet<ByteBuffer>();
        queryAnalyzer.reset(term);
        try
        {
            queryAnalyzer.forEachRemaining(uniqueTerms::add);
        }
        finally
        {
            queryAnalyzer.end();
        }
        queryTerms = new ArrayList<>(uniqueTerms);
        return queryTerms;
    }
}
