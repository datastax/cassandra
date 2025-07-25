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
package org.apache.cassandra.db.filter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import com.google.common.base.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.restrictions.ExternalRestriction;
import org.apache.cassandra.cql3.restrictions.Restrictions;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.context.*;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.guardrails.Guardrails;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.IndexRegistry;
import org.apache.cassandra.index.sai.utils.GeoUtil;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.lucene.util.SloppyMath;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkBindValueSet;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkFalse;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkNotNull;

/**
 * A filter on which rows a given query should include or exclude.
 * <p>
 * This corresponds to the restrictions on rows that are not handled by the query
 * {@link ClusteringIndexFilter}. Some of the expressions of this filter may
 * be handled by a 2ndary index, and the rest is simply filtered out from the
 * result set (the later can only happen if the query was using ALLOW FILTERING).
 */
public class RowFilter
{
    private static final Logger logger = LoggerFactory.getLogger(RowFilter.class);

    public static final Serializer serializer = new Serializer();
    public static final RowFilter NONE = new RowFilter(FilterElement.NONE, IndexHints.NONE);

    public final FilterElement root;
    public final IndexHints indexHints;

    protected RowFilter(FilterElement root, IndexHints indexHints)
    {
        this.root = root;
        this.indexHints = indexHints;
    }

    /**
     * @return all the expressions in this filter expression tree by traversing it in pre-order
     */
    public List<Expression> expressions()
    {
        return root.traversedExpressions();
    }

    /**
     * @return {@code true} if this filter contains any expression with an ANN operator, {@code false} otherwise.
     */
    public boolean hasANN()
    {
        for (Expression expression : root.expressions()) // ANN expressions are always on the first tree level
        {
            if (expression.operator == Operator.ANN)
                return true;
        }
        return false;
    }

    /**
     * @return the {@link ANNOptions} of the ANN expression in this filter, or {@link ANNOptions#NONE} if there is
     * no ANN expression.
     */
    public ANNOptions annOptions()
    {
        for (Expression expression : root.expressions()) // ANN expressions are always on the first tree level
        {
            if (expression.operator == Operator.ANN)
                return expression.annOptions();
        }
        return ANNOptions.NONE;
    }

    /**
     * @return {@code true} if this filter contains any disjunction, {@code false} otherwise.
     */
    public boolean containsDisjunctions()
    {
        return root.containsDisjunctions();
    }

    /**
     * Checks if some of the expressions apply to clustering or regular columns.
     * @return {@code true} if some of the expressions apply to clustering or regular columns, {@code false} otherwise.
     */
    public boolean hasExpressionOnClusteringOrRegularColumns()
    {
        for (Expression expression : expressions())
        {
            ColumnMetadata column = expression.column();
            if (column.isClusteringColumn() || column.isRegular())
                return true;
        }
        return false;
    }

    protected Transformation<BaseRowIterator<?>> filter(TableMetadata metadata, int nowInSec)
    {
        FilterElement partitionLevelOperation = root.partitionLevelTree();
        FilterElement rowLevelOperation = root.rowLevelTree();

        final boolean filterNonStaticColumns = !rowLevelOperation.isEmpty();

        return new Transformation<>()
        {
            DecoratedKey pk;

            @Override
            protected BaseRowIterator<?> applyToPartition(BaseRowIterator<?> partition)
            {
                pk = partition.partitionKey();

                // Short-circuit all partitions that won't match based on static and partition keys
                if (!partitionLevelOperation.isSatisfiedBy(metadata, partition.partitionKey(), partition.staticRow()))
                {
                    partition.close();
                    return null;
                }

                BaseRowIterator<?> iterator = partition instanceof UnfilteredRowIterator
                                              ? Transformation.apply((UnfilteredRowIterator) partition, this)
                                              : Transformation.apply((RowIterator) partition, this);

                if (filterNonStaticColumns && !iterator.hasNext())
                {
                    iterator.close();
                    return null;
                }

                return iterator;
            }

            @Override
            public Row applyToRow(Row row)
            {
                Row purged = row.purge(DeletionPurger.PURGE_ALL, nowInSec, metadata.enforceStrictLiveness());
                if (purged == null)
                    return null;

                if (!rowLevelOperation.isSatisfiedBy(metadata, pk, purged))
                    return null;

                return row;
            }
        };
    }

    /**
     * Filters the provided iterator so that only the row satisfying the expression of this filter
     * are included in the resulting iterator.
     *
     * @param iter the iterator to filter
     * @param nowInSec the time of query in seconds.
     * @return the filtered iterator.
     */
    public UnfilteredPartitionIterator filter(UnfilteredPartitionIterator iter, int nowInSec)
    {
        return root.isEmpty() ? iter : Transformation.apply(iter, filter(iter.metadata(), nowInSec));
    }

    /**
     * Filters the provided iterator so that only the row satisfying the expression of this filter
     * are included in the resulting iterator.
     *
     * @param iter the iterator to filter
     * @param nowInSec the time of query in seconds.
     * @return the filtered iterator.
     */
    public PartitionIterator filter(PartitionIterator iter, TableMetadata metadata, int nowInSec)
    {
        return root.isEmpty() ? iter : Transformation.apply(iter, filter(metadata, nowInSec));
    }

    /**
     * Whether the provided row in the provided partition satisfies this filter.
     *
     * @param metadata the table metadata.
     * @param partitionKey the partition key for partition to test.
     * @param row the row to test.
     * @param nowInSec the current time in seconds (to know what is live and what isn't).
     * @return {@code true} if {@code row} in partition {@code partitionKey} satisfies this row filter.
     */
    public boolean isSatisfiedBy(TableMetadata metadata, DecoratedKey partitionKey, Row row, int nowInSec)
    {
        // We purge all tombstones as the expressions isSatisfiedBy methods expects it
        Row purged = row.purge(DeletionPurger.PURGE_ALL, nowInSec, metadata.enforceStrictLiveness());
        if (purged == null)
            return root.isEmpty();

        return root.isSatisfiedBy(metadata, partitionKey, purged);
    }

    /**
     * Returns true if all the expressions within this filter that apply to the partition key are satisfied by
     * the given key, false otherwise.
     */
    public boolean partitionKeyRestrictionsAreSatisfiedBy(DecoratedKey key, AbstractType<?> keyValidator)
    {
        for (Expression e : expressions())
        {
            if (!e.column.isPartitionKey())
                continue;

            ByteBuffer value = keyValidator instanceof CompositeType
                             ? ((CompositeType) keyValidator).split(key.getKey())[e.column.position()]
                             : key.getKey();

            if (!e.isSatisfiedBy(e.column.type, value))
                return false;
        }
        return true;
    }

    /**
     * Returns true if all the expressions within this filter that apply to the clustering key are satisfied by
     * the given Clustering, false otherwise.
     */
    public boolean clusteringKeyRestrictionsAreSatisfiedBy(Clustering<?> clustering)
    {
        for (Expression e : expressions())
        {
            if (!e.column.isClusteringColumn())
                continue;

            ByteBuffer value = clustering.bufferAt(e.column.position());

            if (!e.isSatisfiedBy(e.column.type, value))
                return false;
        }
        return true;
    }

    /**
     * Returns this filter but without the provided expression. This method
     * *assumes* that the filter contains the provided expression.
     */
    public RowFilter without(Expression expression)
    {
        assert root.contains(expression);
        if (root.size() == 1)
            return RowFilter.NONE;

        return new RowFilter(root.filter(e -> !e.equals(expression)), indexHints);
    }

    public RowFilter withoutExpressions()
    {
        return NONE;
    }

    /**
     * @return this filter pruning all its disjunction branches
     */
    public RowFilter withoutDisjunctions()
    {
        return new RowFilter(root.withoutDisjunctions(), indexHints);
    }

    public RowFilter restrict(Predicate<Expression> filter)
    {
        return new RowFilter(root.filter(filter), indexHints);
    }

    public boolean isEmpty()
    {
        return root.isEmpty();
    }

    @Override
    public String toString()
    {
        return root.toString();
    }

    public static Builder builder()
    {
        return new Builder(null, IndexHints.NONE);
    }

    public static Builder builder(IndexRegistry indexRegistry, IndexHints indexHints)
    {
        return new Builder(indexRegistry, indexHints);
    }

    public static class Builder
    {
        private FilterElement.Builder current = new FilterElement.Builder(false);

        private final IndexRegistry indexRegistry;
        private final IndexHints indexHints;

        public Builder(IndexRegistry indexRegistry, IndexHints indexHints)
        {
            this.indexRegistry = indexRegistry;
            this.indexHints = indexHints;
        }

        public RowFilter build()
        {
            return new RowFilter(current.build(), indexHints);
        }

        public RowFilter buildFromRestrictions(StatementRestrictions restrictions,
                                               TableMetadata table,
                                               QueryOptions options,
                                               QueryState queryState,
                                               ANNOptions annOptions)
        {
            FilterElement root = doBuild(restrictions, table, options, annOptions);

            if (Guardrails.queryFilters.enabled(queryState))
                Guardrails.queryFilters.guard(root.numFilteredValues(), "Select query", false, queryState);

            return new RowFilter(root, indexHints);
        }

        private FilterElement doBuild(StatementRestrictions restrictions,
                                      TableMetadata table,
                                      QueryOptions options,
                                      ANNOptions annOptions)
        {
            FilterElement.Builder element = new FilterElement.Builder(restrictions.isDisjunction());
            this.current = element;

            for (Restrictions restrictionSet : restrictions.filterRestrictions().getRestrictions())
                restrictionSet.addToRowFilter(this, indexRegistry, options, annOptions, indexHints);

            for (ExternalRestriction expression : restrictions.filterRestrictions().getExternalExpressions())
                addAllAsConjunction(b -> expression.addToRowFilter(b, table, options));

            for (StatementRestrictions child : restrictions.children())
                element.children.add(doBuild(child, table, options, annOptions));

            // Optimize out any conjunctions / disjunctions with TRUE.
            // This is not needed for correctness.
            if (restrictions.isDisjunction())
            {
                // `OR TRUE` swallows all other restrictions in disjunctions.
                // Therefore, replace this node with an always true element.
                if (element.children.stream().anyMatch(FilterElement::isAlwaysTrue))
                    element = new FilterElement.Builder(false);
            }
            else
            {
                // `AND TRUE` does nothing in conjunctions, so remove it.
                element.children.removeIf(FilterElement::isAlwaysTrue);
            }

            return element.build();
        }

        /**
         * Adds multiple filter expressions to this {@link RowFilter.Builder} and joins them with AND (conjunction),
         * regardless of the current mode (conjunction / disjunction) of the {@link RowFilter.Builder}.
         * <p>
         *
         * This wrapper method makes sure we pass a {@code RowFilter.Builder} that is always in conjunction mode to the
         * respective {@code addToRowFilterDelegate} method. If multiple expressions are added to the row filter, this 
         * method makes sure they are joined with AND in their own {@link FilterElement}.
         *
         * @param addToRowFilterDelegate a function that adds expressions / child filter elements
         *                               to a provided {@link RowFilter.Builder}, and expects all
         *                               added expressions to be joined with AND operator
         */
        public void addAllAsConjunction(Consumer<Builder> addToRowFilterDelegate)
        {
            if (current.isDisjunction)
            {
                // If we're in disjunction mode, we must not pass the current builder to addToRowFilter.
                // We create a new conjunction sub-builder instead and add all expressions there.
                var builder = new Builder(indexRegistry, indexHints);
                addToRowFilterDelegate.accept(builder);

                if (builder.current.expressions.size() == 1 && builder.current.children.isEmpty())
                {
                    // Optimization:
                    // if there is one expression, we can just add it directly to the current FilterElement
                    // making the result tree flatter
                    current.expressions.add(builder.current.expressions.get(0));
                }
                else if (builder.current.children.size() == 1 && builder.current.expressions.isEmpty())
                {
                    // Optimization:
                    // if there is one child, we can just add it directly to the current FilterElement,
                    // making the result tree flatter
                    current.children.add(builder.current.children.get(0));
                }
                else
                {
                    // More expressions means we have to create a new child node (AND) for them.
                    // Also note that we use this for adding zero expressions/children as well.
                    // A conjunction with no restrictions means selecting everything, so if we didn't add an empty
                    // AND node in such case, we could end up with a filter that misses to match some rows.
                    current.children.add(builder.current.build());
                }
            }
            else
            {
                // Just an optimisation. If we're already in the conjunction mode, we don't need to create
                // a sub-builder; we can just use this one to collect the expressions.
                addToRowFilterDelegate.accept(this);
            }
        }

        /**
         * Adds the specified simple filter expression to this builder.
         *
         * @param def the filtered column
         * @param op the filtering operator, shouldn't be {@link Operator#ANN}.
         * @param value the filtered value
         * @return the added expression
         */
        public SimpleExpression add(ColumnMetadata def, Operator op, ByteBuffer value)
        {
            assert op != Operator.ANN : "ANN expressions should be added with the addANNExpression method";
            SimpleExpression expression = new SimpleExpression(def, op, value, analyzer(def, op, value), null);
            add(expression);
            return expression;
        }

        /**
         * Adds the specified ANN expression to this builder.
         *
         * @param def the column for ANN ordering
         * @param value the value for ANN ordering
         * @param annOptions the ANN options
         */
        public void addANNExpression(ColumnMetadata def, ByteBuffer value, ANNOptions annOptions)
        {
            add(new SimpleExpression(def, Operator.ANN, value, null, annOptions));
        }

        public void addMapComparison(ColumnMetadata def, ByteBuffer key, Operator op, ByteBuffer value)
        {
            add(new MapComparisonExpression(def, key, op, value));
        }

        @Nullable
        private Index.Analyzer analyzer(ColumnMetadata def, Operator op, ByteBuffer value)
        {
            return indexRegistry == null ? null : indexRegistry.getAnalyzerFor(def, op, value, indexHints).orElse(null);
        }

        public void addGeoDistanceExpression(ColumnMetadata def, ByteBuffer point, Operator op, ByteBuffer distance)
        {
            var primaryGeoDistanceExpression = new GeoDistanceExpression(def, point, op, distance);
            // The following logic optionally adds a second search expression in the event that the query area
            // crosses then antimeridian.
            if (primaryGeoDistanceExpression.crossesAntimeridian())
            {
                // The primry GeoDistanceExpression includes points on/over the antimeridian. Since we search
                // using the lat/lon coordinates, we must create a shifted expression that will collect
                // results on the other side of the antimeridian.
                var shiftedGeoDistanceExpression = primaryGeoDistanceExpression.buildShiftedExpression();
                if (current.isDisjunction)
                {
                    // We can add both expressions to this level of the tree because it is a disjunction.
                    add(primaryGeoDistanceExpression);
                    add(shiftedGeoDistanceExpression);
                }
                else
                {
                    // We need to add a new level to the tree so that we can get all results that match the primary
                    // or the shifted expressions.
                    var builder = new FilterElement.Builder(true);
                    primaryGeoDistanceExpression.validate();
                    shiftedGeoDistanceExpression.validate();
                    builder.expressions.add(primaryGeoDistanceExpression);
                    builder.expressions.add(shiftedGeoDistanceExpression);
                    current.children.add(builder.build());
                }
            }
            else
            {
                add(primaryGeoDistanceExpression);
            }
        }

        public void addCustomIndexExpression(TableMetadata metadata, IndexMetadata targetIndex, ByteBuffer value)
        {
            add(CustomExpression.build(metadata, targetIndex, value));
        }

        public Builder add(Expression expression)
        {
            expression.validate();
            current.expressions.add(expression);
            return this;
        }

        public void addUserExpression(UserExpression e)
        {
            current.expressions.add(e);
        }
    }

    public static class FilterElement
    {
        public static final Serializer serializer = new Serializer();

        public static final FilterElement NONE = new FilterElement(false, Collections.emptyList(), Collections.emptyList());

        private final boolean isDisjunction;

        private final List<Expression> expressions;

        private final List<FilterElement> children;

        public FilterElement(boolean isDisjunction, List<Expression> expressions, List<FilterElement> children)
        {
            this.isDisjunction = isDisjunction;
            this.expressions = expressions;
            this.children = children;
        }

        public boolean isDisjunction()
        {
            return isDisjunction;
        }

        private boolean containsDisjunctions()
        {
            if (isDisjunction)
                return true;

            for (FilterElement child : children)
                if (child.containsDisjunctions())
                    return true;

            return false;
        }

        public List<Expression> expressions()
        {
            return expressions;
        }

        private List<Expression> traversedExpressions()
        {
            List<Expression> allExpressions = new ArrayList<>(expressions);
            for (FilterElement child : children)
                allExpressions.addAll(child.traversedExpressions());
            return allExpressions;
        }

        private FilterElement withoutDisjunctions()
        {
            if (isDisjunction)
                return NONE;

            FilterElement.Builder builder = new Builder(false);
            builder.expressions.addAll(expressions);

            for (FilterElement child : children)
            {
                if (!child.isDisjunction)
                    builder.children.add(child);
            }

            return builder.build();
        }

        public FilterElement filter(Predicate<Expression> filter)
        {
            FilterElement.Builder builder = new Builder(isDisjunction);

            expressions.stream().filter(filter).forEach(builder.expressions::add);

            children.stream().map(c -> c.filter(filter)).forEach(builder.children::add);

            return builder.build();
        }

        public List<FilterElement> children()
        {
            return children;
        }

        public boolean isEmpty()
        {
            return expressions.isEmpty() && children.isEmpty();
        }

        public boolean isAlwaysTrue()
        {
            return !isDisjunction && isEmpty();
        }

        public boolean contains(Expression expression)
        {
            return expressions.contains(expression) || children.stream().anyMatch(c -> contains(expression));
        }

        public FilterElement partitionLevelTree()
        {
            return new FilterElement(isDisjunction,
                                     expressions.stream()
                                                  .filter(e -> e.column.isStatic() || e.column.isPartitionKey())
                                                  .collect(Collectors.toList()),
                                     children.stream()
                                               .map(FilterElement::partitionLevelTree)
                                               .collect(Collectors.toList()));
        }

        public FilterElement rowLevelTree()
        {
            return new FilterElement(isDisjunction,
                                     expressions.stream()
                                                  .filter(e -> !e.column.isStatic() && !e.column.isPartitionKey())
                                                  .collect(Collectors.toList()),
                                     children.stream()
                                               .map(FilterElement::rowLevelTree)
                                               .collect(Collectors.toList()));
        }

        public int size()
        {
            return expressions.size() + children.stream().mapToInt(FilterElement::size).sum();
        }

        public boolean isSatisfiedBy(TableMetadata table, DecoratedKey key, Row row)
        {
            if (isEmpty())
                return true;
            if (isDisjunction)
            {
                for (Expression e : expressions)
                    if (e.isSatisfiedBy(table, key, row))
                        return true;
                for (FilterElement child : children)
                    if (child.isSatisfiedBy(table, key, row))
                        return true;
                return false;
            }
            else
            {
                for (Expression e : expressions)
                    if (!e.isSatisfiedBy(table, key, row))
                        return false;
                for (FilterElement child : children)
                    if (!child.isSatisfiedBy(table, key, row))
                        return false;
                return true;
            }
        }

        /**
         * Returns the number of values that this filter will filter out after applying any index analyzers.
         */
        private int numFilteredValues()
        {
            int result = 0;

            for (Expression expression : expressions)
                result += expression.numFilteredValues();

            for (FilterElement child : children)
                result += child.numFilteredValues();

            return result;
        }

        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < expressions.size(); i++)
            {
                if (sb.length() > 0)
                    sb.append(isDisjunction ? " OR " : " AND ");
                sb.append(expressions.get(i));
            }
            for (int i = 0; i < children.size(); i++)
            {
                if (sb.length() > 0)
                    sb.append(isDisjunction ? " OR " : " AND ");
                sb.append('(');
                sb.append(children.get(i));
                sb.append(')');
            }
            return sb.toString();
        }

        public static class Builder
        {
            private final boolean isDisjunction;
            private final List<Expression> expressions = new ArrayList<>();
            private final List<FilterElement> children = new ArrayList<>();

            public Builder(boolean isDisjunction)
            {
                this.isDisjunction = isDisjunction;
            }

            public FilterElement build()
            {
                return new FilterElement(isDisjunction, expressions, children);
            }
        }

        public static class Serializer
        {
            public void serialize(FilterElement operation, DataOutputPlus out, int version) throws IOException
            {
                assert (!operation.isDisjunction && operation.children().isEmpty()) || version >= MessagingService.VERSION_DS_10 :
                "Attempting to serialize a disjunct row filter to a node that doesn't support disjunction";

                out.writeUnsignedVInt(operation.expressions.size());
                for (Expression expr : operation.expressions)
                    Expression.serializer.serialize(expr, out, version);

                if (version < MessagingService.VERSION_DS_10)
                    return;

                out.writeBoolean(operation.isDisjunction);
                out.writeUnsignedVInt(operation.children.size());
                for (FilterElement child : operation.children)
                    serialize(child, out, version);
            }

            public FilterElement deserialize(DataInputPlus in, int version, TableMetadata metadata, IndexHints indexHints) throws IOException
            {
                int size = (int)in.readUnsignedVInt();
                List<Expression> expressions = new ArrayList<>(size);
                for (int i = 0; i < size; i++)
                    expressions.add(Expression.serializer.deserialize(in, version, metadata, indexHints));

                if (version < MessagingService.VERSION_DS_10)
                    return new FilterElement(false, expressions, Collections.emptyList());

                boolean isDisjunction = in.readBoolean();
                size = (int)in.readUnsignedVInt();
                List<FilterElement> children = new ArrayList<>(size);
                for (int i  = 0; i < size; i++)
                    children.add(deserialize(in, version, metadata, indexHints));
                return new FilterElement(isDisjunction, expressions, children);
            }

            public long serializedSize(FilterElement operation, int version)
            {
                long size = TypeSizes.sizeofUnsignedVInt(operation.expressions.size());
                for (Expression expr : operation.expressions)
                    size += Expression.serializer.serializedSize(expr, version);

                if (version < MessagingService.VERSION_DS_10)
                    return size;

                size++; // isDisjunction boolean
                size += TypeSizes.sizeofUnsignedVInt(operation.children.size());
                for (FilterElement child : operation.children)
                    size += serializedSize(child, version);
                return size;
            }
        }
    }

    public static abstract class Expression
    {
        public static final Serializer serializer = new Serializer();

        // Note: the val of this enum is used for serialization,
        // and this is why we have some UNUSEDX for values we don't use anymore
        // (we could clean those on a major protocol update, but it's not worth
        // the trouble for now)
        // VECTOR
        protected enum Kind
        {
            SIMPLE(0), MAP_COMPARISON(1), UNUSED1(2), CUSTOM(3), USER(4), VECTOR_RADIUS(100);
            private final int val;
            Kind(int v) { val = v; }
            public int getVal() { return val; }
            public static Kind fromVal(int val)
            {
                switch (val)
                {
                    case 0: return SIMPLE;
                    case 1: return MAP_COMPARISON;
                    case 2: return UNUSED1;
                    case 3: return CUSTOM;
                    case 4: return USER;
                    case 100: return VECTOR_RADIUS;
                    default: throw new IllegalArgumentException("Unknown index expression kind: " + val);
                }
            }
        }

        protected abstract Kind kind();

        protected final ColumnMetadata column;
        protected final Operator operator;
        protected final ByteBuffer value;

        protected Expression(ColumnMetadata column, Operator operator, ByteBuffer value)
        {
            this.column = column;
            this.operator = operator;
            this.value = value;
        }

        public boolean isCustom()
        {
            return kind() == Kind.CUSTOM;
        }

        public boolean isUserDefined()
        {
            return kind() == Kind.USER;
        }

        public ColumnMetadata column()
        {
            return column;
        }

        public Operator operator()
        {
            return operator;
        }

        @Nullable
        public Index.Analyzer analyzer()
        {
            return null;
        }

        protected boolean isSatisfiedBy(AbstractType<?> type, ByteBuffer foundValue)
        {
            if (foundValue == null)
                return false;

            Index.Analyzer analyzer = analyzer();

            // Note that CQL expression are always of the form 'x < 4', i.e. the tested value is on the left.
            return analyzer == null
                   ? operator.isSatisfiedBy(type, foundValue, value)
                   : operator.isSatisfiedByAnalyzed(type, analyzer.indexedTokens(foundValue), analyzer.queriedTokens());
        }

        @Nullable
        public ANNOptions annOptions()
        {
            return null;
        }

        /**
         * Checks if the operator of this <code>IndexExpression</code> is a <code>CONTAINS</code> operator.
         *
         * @return <code>true</code> if the operator of this <code>IndexExpression</code> is a <code>CONTAINS</code>
         * operator, <code>false</code> otherwise.
         */
        public boolean isContains()
        {
            return Operator.CONTAINS == operator;
        }

        /**
         * Checks if the operator of this <code>IndexExpression</code> is a <code>CONTAINS_KEY</code> operator.
         *
         * @return <code>true</code> if the operator of this <code>IndexExpression</code> is a <code>CONTAINS_KEY</code>
         * operator, <code>false</code> otherwise.
         */
        public boolean isContainsKey()
        {
            return Operator.CONTAINS_KEY == operator;
        }

        /**
         * Checks the operator of this {@code IndexExpression} is any of the variations of {@code [NOT] CONTAINS [KEY]}.
         *
         * @return {@code true} if this operator is any kind of contains operator, {@code false} otherwise.
         */
        public boolean isAnyContains()
        {
            return operator.isAnyContains();
        }

        /**
         * If this expression is used to query an index, the value to use as
         * partition key for that index query.
         */
        public ByteBuffer getIndexValue()
        {
            return value;
        }

        public void validate()
        {
            checkNotNull(value, "Unsupported null value for column %s", column.name);
            checkBindValueSet(value, "Unsupported unset value for column %s", column.name);
        }

        @Deprecated
        public void validateForIndexing()
        {
            checkFalse(value.remaining() > FBUtilities.MAX_UNSIGNED_SHORT,
                       "Index expression values may not be larger than 64K");
        }

        /**
         * Returns whether the provided row satisfied this expression or not.
         *
         * @param metadata the metadata of the queried table
         * @param partitionKey the partition key for row to check.
         * @param row the row to check. It should *not* contain deleted cells
         * (i.e. it should come from a RowIterator).
         * @return whether the row is satisfied by this expression.
         */
        public abstract boolean isSatisfiedBy(TableMetadata metadata, DecoratedKey partitionKey, Row row);

        /**
         * Returns the number of values that this expression will check after applying any index analyzers.
         */
        protected int numFilteredValues()
        {
            return 1;
        }

        protected ByteBuffer getValue(TableMetadata metadata, DecoratedKey partitionKey, Row row)
        {
            switch (column.kind)
            {
                case PARTITION_KEY:
                    return metadata.partitionKeyType instanceof CompositeType
                         ? CompositeType.extractComponent(partitionKey.getKey(), column.position())
                         : partitionKey.getKey();
                case CLUSTERING:
                    return row.clustering().bufferAt(column.position());
                default:
                    Cell<?> cell = row.getCell(column);
                    return cell == null ? null : cell.buffer();
            }
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o)
                return true;

            if (!(o instanceof Expression))
                return false;

            Expression that = (Expression)o;

            return this.kind() == that.kind()
                && this.operator == that.operator
                && Objects.equal(this.column.name, that.column.name)
                && Objects.equal(this.value, that.value);
        }

        @Override
        public int hashCode()
        {
            return Objects.hashCode(column.name, operator, value);
        }

        public static class Serializer
        {
            public void serialize(Expression expression, DataOutputPlus out, int version) throws IOException
            {
                out.writeByte(expression.kind().getVal());

                // Custom expressions include neither a column or operator, but all
                // other expressions do.
                if (expression.kind() == Kind.CUSTOM)
                {
                    IndexMetadata.serializer.serialize(((CustomExpression)expression).targetIndex, out, version);
                    ByteBufferUtil.writeWithShortLength(expression.value, out);
                    return;
                }

                if (expression.kind() == Kind.USER)
                {
                    UserExpression.serialize((UserExpression)expression, out, version);
                    return;
                }

                ByteBufferUtil.writeWithShortLength(expression.column.name.bytes, out);
                expression.operator.writeTo(out);

                switch (expression.kind())
                {
                    case SIMPLE:
                        ByteBufferUtil.writeWithShortLength(expression.value, out);
                        if (expression.operator == Operator.ANN)
                            ANNOptions.serializer.serialize(expression.annOptions(), out, version);
                        break;
                    case MAP_COMPARISON:
                        MapComparisonExpression mexpr = (MapComparisonExpression)expression;
                        ByteBufferUtil.writeWithShortLength(mexpr.key, out);
                        ByteBufferUtil.writeWithShortLength(mexpr.value, out);
                        break;
                    case VECTOR_RADIUS:
                        GeoDistanceExpression gexpr = (GeoDistanceExpression) expression;
                        gexpr.distanceOperator.writeTo(out);
                        ByteBufferUtil.writeWithShortLength(gexpr.distance, out);
                        ByteBufferUtil.writeWithShortLength(gexpr.value, out);
                        break;
                }
            }

            public Expression deserialize(DataInputPlus in, int version, TableMetadata metadata, IndexHints indexHints) throws IOException
            {
                Kind kind = Kind.fromVal(in.readByte());

                // custom expressions (3.0+ only) do not contain a column or operator, only a value
                if (kind == Kind.CUSTOM)
                {
                    return CustomExpression.build(metadata,
                                                  IndexMetadata.serializer.deserialize(in, version, metadata),
                                                  ByteBufferUtil.readWithShortLength(in));
                }

                if (kind == Kind.USER)
                    return UserExpression.deserialize(in, version, metadata);

                ByteBuffer name = ByteBufferUtil.readWithShortLength(in);
                Operator operator = Operator.readFrom(in);
                ColumnMetadata column = metadata.getColumn(name);
                IndexRegistry indexRegistry = IndexRegistry.obtain(metadata);

                // Compact storage tables, when used with thrift, used to allow falling through this withouot throwing an
                // exception. However, since thrift was removed in 4.0, this behaviour was not restored in CASSANDRA-16217
                if (column == null)
                    throw new RuntimeException("Unknown (or dropped) column " + UTF8Type.instance.getString(name) + " during deserialization");

                switch (kind)
                {
                    case SIMPLE:
                        ByteBuffer value = ByteBufferUtil.readWithShortLength(in);
                        ANNOptions annOptions = operator == Operator.ANN ? ANNOptions.serializer.deserialize(in, version) : null;
                        Index.Analyzer analyzer = indexRegistry.getAnalyzerFor(column, operator, value, indexHints).orElse(null);
                        return new SimpleExpression(column, operator, value, analyzer, annOptions);
                    case MAP_COMPARISON:
                        ByteBuffer key = ByteBufferUtil.readWithShortLength(in);
                        ByteBuffer val = ByteBufferUtil.readWithShortLength(in);
                        return new MapComparisonExpression(column, key, operator, val);
                    case VECTOR_RADIUS:
                        Operator boundaryOperator = Operator.readFrom(in);
                        ByteBuffer distance = ByteBufferUtil.readWithShortLength(in);
                        ByteBuffer searchVector = ByteBufferUtil.readWithShortLength(in);
                        return new GeoDistanceExpression(column, searchVector, boundaryOperator, distance);
                }
                throw new AssertionError();
            }

            public long serializedSize(Expression expression, int version)
            {
                long size = 1; // kind byte

                // Custom expressions include neither a column or operator, but all
                // other expressions do.
                if (expression.kind() != Kind.CUSTOM && expression.kind() != Kind.USER)
                    size += ByteBufferUtil.serializedSizeWithShortLength(expression.column().name.bytes)
                            + expression.operator.serializedSize();

                switch (expression.kind())
                {
                    case SIMPLE:
                        size += ByteBufferUtil.serializedSizeWithShortLength((expression).value);
                        if (expression.operator == Operator.ANN)
                            size += ANNOptions.serializer.serializedSize(expression.annOptions(), version);
                        break;
                    case MAP_COMPARISON:
                        MapComparisonExpression mexpr = (MapComparisonExpression)expression;
                        size += ByteBufferUtil.serializedSizeWithShortLength(mexpr.key)
                              + ByteBufferUtil.serializedSizeWithShortLength(mexpr.value);
                        break;
                    case CUSTOM:
                        size += IndexMetadata.serializer.serializedSize(((CustomExpression)expression).targetIndex, version)
                               + ByteBufferUtil.serializedSizeWithShortLength(expression.value);
                        break;
                    case USER:
                        size += UserExpression.serializedSize((UserExpression)expression, version);
                        break;
                    case VECTOR_RADIUS:
                        GeoDistanceExpression geoDistanceRelation = (GeoDistanceExpression) expression;
                        size += ByteBufferUtil.serializedSizeWithShortLength(geoDistanceRelation.distance)
                                + ByteBufferUtil.serializedSizeWithShortLength(geoDistanceRelation.value)
                                + geoDistanceRelation.distanceOperator.serializedSize();
                        break;
                }
                return size;
            }
        }
    }

    /**
     * An expression of the form 'column' 'op' 'value'.
     */
    public static class SimpleExpression extends Expression
    {
        @Nullable
        protected final Index.Analyzer analyzer;

        @Nullable
        private final ANNOptions annOptions;

        public SimpleExpression(ColumnMetadata column,
                                Operator operator,
                                ByteBuffer value,
                                @Nullable Index.Analyzer analyzer,
                                @Nullable ANNOptions annOptions)
        {
            super(column, operator, value);
            this.analyzer = analyzer;
            this.annOptions = annOptions;
        }

        @Override
        @Nullable
        public Index.Analyzer analyzer()
        {
            return analyzer;
        }

        @Override
        public int numFilteredValues()
        {
            return analyzer == null
                    ? super.numFilteredValues()
                    : analyzer.queriedTokens().size();
        }

        @Nullable
        public ANNOptions annOptions()
        {
            return annOptions;
        }

        @Override
        public boolean isSatisfiedBy(TableMetadata metadata, DecoratedKey partitionKey, Row row)
        {
            // We support null conditions for LWT (in ColumnCondition) but not for RowFilter.
            // TODO: we should try to merge both code someday.
            assert value != null;

            switch (operator)
            {
                case EQ:
                case IN:
                case NOT_IN:
                case LT:
                case LTE:
                case GTE:
                case GT:
                    {
                        assert !column.isComplex() : "Only CONTAINS and CONTAINS_KEY are supported for 'complex' types";

                        ByteBuffer foundValue = getValue(metadata, partitionKey, row);

                        if (foundValue == null)
                            return false;

                        // In order to support operators on Counter types, their value has to be extracted from internal
                        // representation. See CASSANDRA-11629
                        if (column.type.isCounter())
                        {
                            ByteBuffer counterValue = LongType.instance.decompose(CounterContext.instance().total(foundValue, ByteBufferAccessor.instance));
                            return isSatisfiedBy(LongType.instance, counterValue);
                        }
                        else
                        {
                            return isSatisfiedBy(column.type, foundValue);
                        }
                    }
                case NEQ:
                case LIKE_PREFIX:
                case LIKE_SUFFIX:
                case LIKE_CONTAINS:
                case LIKE_MATCHES:
                case ANALYZER_MATCHES:
                case ANN:
                case BM25:
                    {
                        assert !column.isComplex() : "Only CONTAINS and CONTAINS_KEY are supported for 'complex' types";
                        ByteBuffer foundValue = getValue(metadata, partitionKey, row);
                        // Note that CQL expression are always of the form 'x < 4', i.e. the tested value is on the left.
                        return isSatisfiedBy(column.type, foundValue);
                    }
                case CONTAINS:
                    return contains(metadata, partitionKey, row);
                case CONTAINS_KEY:
                    return containsKey(metadata, partitionKey, row);
                case NOT_CONTAINS:
                    return !contains(metadata, partitionKey, row);
                case NOT_CONTAINS_KEY:
                    return !containsKey(metadata, partitionKey, row);
            }
            throw new AssertionError("Unsupported operator: " + operator);
        }

        private boolean contains(TableMetadata metadata, DecoratedKey partitionKey, Row row)
        {
            assert column.type.isCollection();

            CollectionType<?> type = (CollectionType<?>) column.type;

            if (column.isComplex())
            {
                ComplexColumnData complexData = row.getComplexColumnData(column);
                if (complexData != null)
                {
                    AbstractType<?> elementType = type.kind == CollectionType.Kind.SET ? type.nameComparator() : type.valueComparator();
                    for (Cell<?> cell : complexData)
                    {
                        ByteBuffer elementValue = type.kind == CollectionType.Kind.SET ? cell.path().get(0) : cell.buffer();
                        if (analyzer != null)
                        {
                            List<ByteBuffer> elementTokens = analyzer.indexedTokens(elementValue);
                            List<ByteBuffer> queriedTokens = analyzer.queriedTokens();
                            if (Operator.ANALYZER_MATCHES.isSatisfiedByAnalyzed(elementType, elementTokens, queriedTokens))
                                return true;
                        }
                        else
                        {
                            if (Operator.EQ.isSatisfiedBy(elementType, elementValue, value))
                                return true;
                        }
                    }
                }
                return false;
            }
            else
            {
                assert analyzer == null : "Analyzers are not supported on frozen collections";
                ByteBuffer foundValue = getValue(metadata, partitionKey, row);
                return foundValue != null && Operator.CONTAINS.isSatisfiedBy(type, foundValue, value);
            }
        }

        private boolean containsKey(TableMetadata metadata, DecoratedKey partitionKey, Row row)
        {
            assert column.type.isCollection() && column.type instanceof MapType;
            MapType<?, ?> mapType = (MapType<?, ?>) column.type;
            if (column.isComplex())
            {
                if (analyzer != null)
                {
                    for (Cell<?> cell : row.getComplexColumnData(column))
                    {
                        AbstractType<?> elementType = mapType.nameComparator();
                        ByteBuffer elementValue = cell.path().get(0);
                        List<ByteBuffer> elementTokens = analyzer.indexedTokens(elementValue);
                        List<ByteBuffer> queriedTokens = analyzer.queriedTokens();
                        if (Operator.ANALYZER_MATCHES.isSatisfiedByAnalyzed(elementType, elementTokens, queriedTokens))
                            return true;
                    }
                    return false;
                }
                return row.getCell(column, CellPath.create(value)) != null;
            }
            else
            {
                assert analyzer == null : "Analyzers are not supported on frozen collections";
                ByteBuffer foundValue = getValue(metadata, partitionKey, row);
                return foundValue != null && Operator.CONTAINS_KEY.isSatisfiedBy(mapType, foundValue, value);
            }
        }

        @Override
        public String toString()
        {
            AbstractType<?> type = column.type;
            switch (operator)
            {
                case CONTAINS:
                case NOT_CONTAINS:
                    assert type instanceof CollectionType;
                    CollectionType<?> ct = (CollectionType<?>)type;
                    type = ct.kind == CollectionType.Kind.SET ? ct.nameComparator() : ct.valueComparator();
                    break;
                case CONTAINS_KEY:
                case NOT_CONTAINS_KEY:
                    assert type instanceof MapType;
                    type = ((MapType<?, ?>)type).nameComparator();
                    break;
                case IN:
                case NOT_IN:
                    type = ListType.getInstance(type, false);
                    break;
                case ORDER_BY_ASC:
                case ORDER_BY_DESC:
                    // These don't have a value, so we return here to prevent an error calling type.getString(value)
                    return String.format("%s %s", column.name, operator);
                default:
                    break;
            }
            var valueString = type.getString(value);
            if (valueString.length() > 9)
                valueString = valueString.substring(0, 6) + "...";
            return String.format("%s %s %s", column.name, operator, valueString);
        }

        @Override
        protected Kind kind()
        {
            return Kind.SIMPLE;
        }
    }

    /**
     * An expression of the form 'column' ['key'] OPERATOR 'value' (which is only
     * supported when 'column' is a map) and where the operator can be {@link Operator#EQ}, {@link Operator#NEQ},
     * {@link Operator#LT}, {@link Operator#LTE}, {@link Operator#GT}, or {@link Operator#GTE}.
     */
    public static class MapComparisonExpression extends Expression
    {
        private final ByteBuffer key;
        private ByteBuffer indexValue = null;

        public MapComparisonExpression(ColumnMetadata column,
                                       ByteBuffer key,
                                       Operator operator,
                                       ByteBuffer value)
        {
            super(column, operator, value);
            assert column.type instanceof MapType && (operator == Operator.EQ || operator == Operator.NEQ || operator.isSlice());
            this.key = key;
        }

        @Override
        public void validate() throws InvalidRequestException
        {
            checkNotNull(key, "Unsupported null map key for column %s", column.name);
            checkBindValueSet(key, "Unsupported unset map key for column %s", column.name);
            checkNotNull(value, "Unsupported null map value for column %s", column.name);
            checkBindValueSet(value, "Unsupported unset map value for column %s", column.name);
        }

        @Override
        public ByteBuffer getIndexValue()
        {
            if (indexValue == null)
                indexValue = CompositeType.build(ByteBufferAccessor.instance, key, value);
            return indexValue;
        }

        /**
         * Returns whether the provided row satisfies this expression. For equality, it validates that the row contains
         * the exact key/value pair. For inequalities, it validates that the row contains the key, then that the value
         * satisfies the inequality.
         *
         * @param metadata the metadata of the queried table
         * @param partitionKey the partition key for row to check.
         * @param row the row to check. It should *not* contain deleted cells
         * (i.e. it should come from a RowIterator).
         * @return whether the row is satisfied by this expression.
         */
        @Override
        public boolean isSatisfiedBy(TableMetadata metadata, DecoratedKey partitionKey, Row row)
        {
            return isSatisfiedByEq(metadata, partitionKey, row) ^ (operator == Operator.NEQ);
        }

        private boolean isSatisfiedByEq(TableMetadata metadata, DecoratedKey partitionKey, Row row)
        {
            assert key != null;
            // We support null conditions for LWT (in ColumnCondition) but not for RowFilter.
            // TODO: we should try to merge both code someday.
            assert value != null;

            if (row.isStatic() != column.isStatic())
                return true;

            int comp;
            MapType<?, ?> mt = (MapType<?, ?>)column.type;
            if (column.isComplex())
            {
                Cell<?> cell = row.getCell(column, CellPath.create(key));
                if (cell == null)
                    return false;
                comp = mt.valueComparator().compare(cell.buffer(), value);
            }
            else
            {
                ByteBuffer serializedMap = getValue(metadata, partitionKey, row);
                if (serializedMap == null)
                    return false;

                ByteBuffer foundValue = mt.getSerializer().getSerializedValue(serializedMap, key, mt.getKeysType());
                if (foundValue == null)
                    return false;
                comp = mt.valueComparator().compare(foundValue, value);
            }
            switch (operator) {
                case EQ:
                case NEQ: // NEQ is inverted in calling method. We do this to simplify handling of null cells.
                    return comp == 0;
                case LT:
                    return comp < 0;
                case LTE:
                    return comp <= 0;
                case GT:
                    return comp > 0;
                case GTE:
                    return comp >= 0;
                default:
                    throw new AssertionError("Unsupported operator: " + operator);
            }
        }

        @Override
        public String toString()
        {
            MapType<?, ?> mt = (MapType<?, ?>)column.type;
            return String.format("%s[%s] %s %s", column.name, mt.nameComparator().getString(key), operator, mt.valueComparator().getString(value));
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o)
                return true;

            if (!(o instanceof MapComparisonExpression))
                return false;

            MapComparisonExpression that = (MapComparisonExpression)o;

            return Objects.equal(this.column.name, that.column.name)
                && this.operator == that.operator
                && Objects.equal(this.key, that.key)
                && Objects.equal(this.value, that.value);
        }

        @Override
        public int hashCode()
        {
            return Objects.hashCode(column.name, operator, key, value);
        }

        @Override
        protected Kind kind()
        {
            return Kind.MAP_COMPARISON;
        }

        /**
         * Get the lower bound for this expression. When the expression is EQ, GT, or GTE, the lower bound is the
         * expression itself. When the expression is LT or LTE, the lower bound is the map's key becuase
         * {@link ByteBuffer} comparisons will work correctly.
         * @return the lower bound for this expression.
         */
        public ByteBuffer getLowerBound()
        {
            switch (operator) {
                case EQ:
                case GT:
                case GTE:
                    return this.getIndexValue();
                case LT:
                case LTE:
                    return CompositeType.extractFirstComponentAsTrieSearchPrefix(getIndexValue(), true);
                default:
                    throw new AssertionError("Unsupported operator: " + operator);
            }
        }

        /**
         * Get the upper bound for this expression. When the expression is EQ, LT, or LTE, the upper bound is the
         * expression itself. When the expression is GT or GTE, the upper bound is the map's key with the last byte
         * set to 1 so that {@link ByteBuffer} comparisons will work correctly.
         * @return the upper bound for this express
         */
        public ByteBuffer getUpperBound()
        {
            switch (operator) {
                case GT:
                case GTE:
                    return CompositeType.extractFirstComponentAsTrieSearchPrefix(getIndexValue(), false);
                case EQ:
                case LT:
                case LTE:
                    return this.getIndexValue();
                default:
                    throw new AssertionError("Unsupported operator: " + operator);
            }
        }
    }

    public static class GeoDistanceExpression extends Expression
    {
        private final ByteBuffer distance;
        private final Operator distanceOperator;
        private final float searchRadiusMeters;
        private final float searchLat;
        private final float searchLon;
        // Whether this is a shifted expression, which is used to handle crossing the antimeridian
        private final boolean isShifted;

        public GeoDistanceExpression(ColumnMetadata column, ByteBuffer point, Operator operator, ByteBuffer distance)
        {
            this(column, point, operator, distance, false);
        }

        private GeoDistanceExpression(ColumnMetadata column, ByteBuffer point, Operator operator, ByteBuffer distance, boolean isShifted)
        {
            super(column, Operator.BOUNDED_ANN, point);
            assert column.type instanceof VectorType && (operator == Operator.LTE || operator == Operator.LT);
            this.isShifted = isShifted;
            this.distanceOperator = operator;
            this.distance = distance;
            searchRadiusMeters = FloatType.instance.compose(distance);
            var pointVector = TypeUtil.decomposeVector(column.type, point);
            // This is validated earlier in the parser because the column requires size 2, so only assert on it
            assert pointVector.length == 2 : "GEO_DISTANCE requires search vector to have 2 dimensions.";
            searchLat = pointVector[0];
            searchLon = pointVector[1];
        }

        public boolean crossesAntimeridian()
        {
            return GeoUtil.crossesAntimeridian(searchLat, searchLon, searchRadiusMeters);
        }

        /**
         * @return a new {@link GeoDistanceExpression} that is shifted by 360 degrees and can correctly search
         * on the opposite side of the antimeridian.
         */
        public GeoDistanceExpression buildShiftedExpression()
        {
            float shiftedLon = searchLon > 0 ? searchLon - 360 : searchLon + 360;
            var newPoint = VectorType.getInstance(FloatType.instance, 2)
                                     .decompose(List.of(searchLat, shiftedLon));
            return new GeoDistanceExpression(column, newPoint, distanceOperator, distance, true);
        }

        public Operator getDistanceOperator()
        {
            return distanceOperator;
        }

        public ByteBuffer getDistance()
        {
            return distance;
        }

        @Override
        public void validate() throws InvalidRequestException
        {
            checkBindValueSet(distance, "Unsupported unset distance for column %s", column.name);
            checkBindValueSet(value, "Unsupported unset vector value for column %s", column.name);

            if (searchRadiusMeters <= 0)
                throw new InvalidRequestException("GEO_DISTANCE radius must be positive, got " + searchRadiusMeters);

            if (searchLat < -90 || searchLat > 90)
                throw new InvalidRequestException("GEO_DISTANCE latitude must be between -90 and 90 degrees, got " + searchLat);
            if (!isShifted && (searchLon < -180 || searchLon > 180))
                throw new InvalidRequestException("GEO_DISTANCE longitude must be between -180 and 180 degrees, got " + searchLon);
        }

        @Override
        public boolean isSatisfiedBy(TableMetadata metadata, DecoratedKey partitionKey, Row row)
        {
            ByteBuffer foundValue = getValue(metadata, partitionKey, row);
            if (foundValue == null)
                return false;
            var foundVector = TypeUtil.decomposeVector(column.type, foundValue);
            double haversineDistance = SloppyMath.haversinMeters(foundVector[0], foundVector[1], searchLat, searchLon);
            switch (distanceOperator)
            {
                case LTE:
                    return haversineDistance <= searchRadiusMeters;
                case LT:
                    return haversineDistance < searchRadiusMeters;
                default:
                    throw new AssertionError("Unsupported operator: " + operator);
            }
        }

        @Override
        public String toString()
        {
            return String.format("GEO_DISTANCE(%s, %s) %s %s", column.name, column.type.getString(value),
                                 distanceOperator, FloatType.instance.getString(distance));
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o)
                return true;

            if (!(o instanceof GeoDistanceExpression))
                return false;

            GeoDistanceExpression that = (GeoDistanceExpression)o;

            return Objects.equal(this.column.name, that.column.name)
                   && this.distanceOperator == that.distanceOperator
                   && Objects.equal(this.distance, that.distance)
                   && Objects.equal(this.value, that.value);
        }

        @Override
        public int hashCode()
        {
            return Objects.hashCode(column.name, distanceOperator, value, distance);
        }

        @Override
        protected Kind kind()
        {
            return Kind.VECTOR_RADIUS;
        }
    }

    /**
     * A custom index expression for use with 2i implementations which support custom syntax and which are not
     * necessarily linked to a single column in the base table.
     */
    public static class CustomExpression extends Expression
    {
        private final IndexMetadata targetIndex;
        private final TableMetadata table;

        public CustomExpression(TableMetadata table, IndexMetadata targetIndex, ByteBuffer value)
        {
            // The operator is not relevant, but Expression requires it so for now we just hardcode EQ
            super(makeDefinition(table, targetIndex), Operator.EQ, value);
            this.targetIndex = targetIndex;
            this.table = table;
        }

        public static CustomExpression build(TableMetadata metadata, IndexMetadata targetIndex, ByteBuffer value)
        {
            // delegate the expression creation to the target custom index
            return Keyspace.openAndGetStore(metadata).indexManager.getIndex(targetIndex).customExpressionFor(metadata, value);
        }

        private static ColumnMetadata makeDefinition(TableMetadata table, IndexMetadata index)
        {
            // Similarly to how we handle non-defined columns in thift, we create a fake column definition to
            // represent the target index. This is definitely something that can be improved though.
            return ColumnMetadata.regularColumn(table, ByteBuffer.wrap(index.name.getBytes()), BytesType.instance);
        }

        public IndexMetadata getTargetIndex()
        {
            return targetIndex;
        }

        public ByteBuffer getValue()
        {
            return value;
        }

        @Override
        public String toString()
        {
            return String.format("expr(%s, %s)",
                                 targetIndex.name,
                                 Keyspace.openAndGetStore(table)
                                         .indexManager
                                         .getIndex(targetIndex)
                                         .customExpressionValueType());
        }

        @Override
        protected Kind kind()
        {
            return Kind.CUSTOM;
        }

        // Filtering by custom expressions isn't supported yet, so just accept any row
        @Override
        public boolean isSatisfiedBy(TableMetadata metadata, DecoratedKey partitionKey, Row row)
        {
            return true;
        }
    }

    /**
     * A user defined filtering expression. These may be added to RowFilter programmatically by a
     * QueryHandler implementation. No concrete implementations are provided and adding custom impls
     * to the classpath is a task for operators (needless to say, this is something of a power
     * user feature). Care must also be taken to register implementations, via the static register
     * method during system startup. An implementation and its corresponding Deserializer must be
     * registered before sending or receiving any messages containing expressions of that type.
     * Use of custom filtering expressions in a mixed version cluster should be handled with caution
     * as the order in which types are registered is significant: if continuity of use during upgrades
     * is important, new types should registered last and obsoleted types should still be registered (
     * or dummy implementations registered in their place) to preserve consistent identifiers across
     * the cluster).
     * </p>
     * During serialization, the identifier for the Deserializer implementation is prepended to the
     * implementation specific payload. To deserialize, the identifier is read first to obtain the
     * Deserializer, which then provides the concrete expression instance.
     */
    public static abstract class UserExpression extends Expression
    {
        private static final DeserializerRegistry deserializers = new DeserializerRegistry();
        private static final class DeserializerRegistry
        {
            private final AtomicInteger counter = new AtomicInteger(0);
            private final ConcurrentMap<Integer, Deserializer> deserializers = new ConcurrentHashMap<>();
            private final ConcurrentMap<Class<? extends UserExpression>, Integer> registeredClasses = new ConcurrentHashMap<>();

            public void registerUserExpressionClass(Class<? extends UserExpression> expressionClass,
                                                    UserExpression.Deserializer deserializer)
            {
                int id = registeredClasses.computeIfAbsent(expressionClass, (cls) -> counter.getAndIncrement());
                deserializers.put(id, deserializer);

                logger.debug("Registered user defined expression type {} and serializer {} with identifier {}",
                             expressionClass.getName(), deserializer.getClass().getName(), id);
            }

            public Integer getId(UserExpression expression)
            {
                return registeredClasses.get(expression.getClass());
            }

            public Deserializer getDeserializer(int id)
            {
                return deserializers.get(id);
            }
        }

        public static abstract class Deserializer
        {
            protected abstract UserExpression deserialize(DataInputPlus in,
                                                          int version,
                                                          TableMetadata metadata) throws IOException;
        }

        public static void register(Class<? extends UserExpression> expressionClass, Deserializer deserializer)
        {
            deserializers.registerUserExpressionClass(expressionClass, deserializer);
        }

        private static UserExpression deserialize(DataInputPlus in, int version, TableMetadata metadata) throws IOException
        {
            int id = in.readInt();
            Deserializer deserializer = deserializers.getDeserializer(id);
            assert deserializer != null : "No user defined expression type registered with id " + id;
            return deserializer.deserialize(in, version, metadata);
        }

        private static void serialize(UserExpression expression, DataOutputPlus out, int version) throws IOException
        {
            Integer id = deserializers.getId(expression);
            assert id != null : "User defined expression type " + expression.getClass().getName() + " is not registered";
            out.writeInt(id);
            expression.serialize(out, version);
        }

        private static long serializedSize(UserExpression expression, int version)
        {   // 4 bytes for the expression type id
            return 4 + expression.serializedSize(version);
        }

        protected UserExpression(ColumnMetadata column, Operator operator, ByteBuffer value)
        {
            super(column, operator, value);
        }

        @Override
        protected Kind kind()
        {
            return Kind.USER;
        }

        protected abstract void serialize(DataOutputPlus out, int version) throws IOException;
        protected abstract long serializedSize(int version);
    }

    public static class Serializer
    {
        public void serialize(RowFilter filter, DataOutputPlus out, int version) throws IOException
        {
            out.writeBoolean(false); // Old "is for thrift" boolean
            IndexHints.serializer.serialize(filter.indexHints, out, version); // hints first because the expressions might need them
            FilterElement.serializer.serialize(filter.root, out, version);
        }

        public RowFilter deserialize(DataInputPlus in, int version, TableMetadata metadata) throws IOException
        {
            in.readBoolean(); // Unused
            IndexHints indexHints = IndexHints.serializer.deserialize(in, version, metadata);
            FilterElement operation = FilterElement.serializer.deserialize(in, version, metadata, indexHints);
            return new RowFilter(operation, indexHints);
        }

        public long serializedSize(RowFilter filter, int version)
        {
            return 1 // unused boolean
                   + FilterElement.serializer.serializedSize(filter.root, version)
                   + IndexHints.serializer.serializedSize(filter.indexHints, version);
        }
    }
}
