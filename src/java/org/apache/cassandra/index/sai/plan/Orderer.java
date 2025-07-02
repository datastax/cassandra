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
import org.apache.cassandra.db.filter.ANNOptions;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.disk.vector.VectorCompression;
import org.apache.cassandra.index.sai.utils.DocBm25Stats;
import org.apache.cassandra.index.sai.utils.PrimaryKeyWithSortKey;
import org.apache.cassandra.index.sai.utils.TypeUtil;

import static org.apache.cassandra.index.sai.disk.v3.V3OnDiskFormat.JVECTOR_USE_PRUNING_DEFAULT;

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

    // Vector search parameters
    private float[] vector;
    private final ANNOptions annOptions;

    // BM25 search parameter
    private List<ByteBuffer> queryTerms;

    // BM25 aggregated statistics
    public final DocBm25Stats bm25stats = new DocBm25Stats();

    /**
     * Create an orderer for the given index context, operator, and term.
     * @param context the index context, used to build the view of memtables and sstables for query execution.
     * @param operator the operator for the order by clause.
     * @param term the term to order by (not always relevant)
     * @param annOptions optional options for ANN queries
     */
    public Orderer(IndexContext context, Operator operator, ByteBuffer term, ANNOptions annOptions)
    {
        this.context = context;
        assert ORDER_BY_OPERATORS.contains(operator) : "Invalid operator for order by clause " + operator;
        this.operator = operator;
        this.annOptions = annOptions;
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

    /**
     * Provide rerankK for ANN queries. Use the user provided rerankK if available, otherwise use the model's default
     * based on the limit and compression type.
     *
     * @param limit the query limit or the proportional segment limit to use when calculating a reasonable rerankK
     *              default value
     * @param vc the compression type of the vectors in the index
     * @return the rerankK value to use in ANN search
     */
    public int rerankKFor(int limit, VectorCompression vc)
    {
        assert isANN() : "rerankK is only valid for ANN queries";
        return annOptions.rerankK != null
               ? annOptions.rerankK
               : context.getIndexWriterConfig().getSourceModel().rerankKFor(limit, vc);
    }

    /**
     * Whether to use pruning to speed up the ANN search. If the AnnOption does not specify a value for usePruning,
     * we use the default value, which is currently configured as an environment variable.
     *
     * @return the usePruning value to use in ANN search
     */
    public boolean usePruning()
    {
        assert isANN() : "usePruning is only valid for ANN queries";
        return annOptions.usePruning != null ? annOptions.usePruning : JVECTOR_USE_PRUNING_DEFAULT;
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
        var orderExpression = expressions.get(0);
        var index = indexManager.getBestIndexFor(orderExpression, StorageAttachedIndex.class)
                                .orElseThrow(() -> new IllegalStateException("No index found for order by clause"));

        return new Orderer(index.getIndexContext(), orderExpression.operator(), orderExpression.getIndexValue(), filter.annOptions());
    }

    public static boolean isFilterExpressionOrderer(RowFilter.Expression expression)
    {
        return ORDER_BY_OPERATORS.contains(expression.operator());
    }

    @Override
    public String toString()
    {
        String direction = isAscending() ? "ASC" : "DESC";
        String annOptionsString = annOptions != null ? annOptions.toCQLString() : "";
        if (isANN())
            return context.getColumnName() + " ANN OF " + Arrays.toString(getVectorTerm()) + ' ' + direction + annOptionsString;
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
