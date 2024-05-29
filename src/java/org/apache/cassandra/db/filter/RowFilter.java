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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.restrictions.ExternalRestriction;
import org.apache.cassandra.cql3.restrictions.Restrictions;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionPurger;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.BaseRowIterator;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.IndexRegistry;
import org.apache.cassandra.index.sai.utils.GeoUtil;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.NoSpamLogger;
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
public class RowFilter implements Iterable<RowFilter.Expression>
{
    private static final Logger logger = LoggerFactory.getLogger(RowFilter.class);
    private static final NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(logger, 1, TimeUnit.MINUTES);

    public static final Serializer serializer = new Serializer();
    private static final RowFilter NONE = new RowFilter(FilterElement.NONE, false);

    protected final FilterElement root;

    private final boolean needsReconciliation;

    protected RowFilter(FilterElement root, boolean needsReconciliation)
    {
        this.root = root;
        this.needsReconciliation = needsReconciliation;
    }

    public static RowFilter none()
    {
        return NONE;
    }

    public FilterElement root()
    {
        return root;
    }

    public List<Expression> getExpressions()
    {
        warnIfFilterIsATree();
        return root.expressions;
    }

    /**
     * @return true if this filter belongs to a read that requires reconciliation at the coordinator
     * @see StatementRestrictions#getRowFilter(IndexRegistry, QueryOptions)
     */
    public boolean needsReconciliation()
    {
        return needsReconciliation;
    }

    /**
     * If this filter belongs to a read that requires reconciliation at the coordinator, and it contains an intersection
     * on two or more non-key (and therefore mutable) columns, we cannot strictly apply it to local, unrepaired rows.
     * When this occurs, we must downgrade the intersection of expressions to a union and leave the coordinator to 
     * filter strictly before sending results to the client.
     * 
     * @return true if strict filtering is safe
     *
     * @see <a href="https://issues.apache.org/jira/browse/CASSANDRA-19018">CASSANDRA-19018</a>
     */
    public boolean isStrict()
    {
        return !needsReconciliation || getExpressions().stream().filter(e -> !e.column.isPrimaryKeyColumn()).count() <= 1;
    }

    /**
     * @return *all* the expressions from the RowFilter tree in pre-order.
     */
    public Stream<Expression> getExpressionsPreOrder()
    {
        return root.getExpressionsPreOrder();
    }

    /**
     * Checks if some of the expressions apply to clustering or regular columns.
     * @return {@code true} if some of the expressions apply to clustering or regular columns, {@code false} otherwise.
     */
    public boolean hasExpressionOnClusteringOrRegularColumns()
    {
        warnIfFilterIsATree();
        for (Expression expression : root)
        {
            ColumnMetadata column = expression.column();
            if (column.isClusteringColumn() || column.isRegular())
                return true;
        }
        return false;
    }

    /**
     * Note that the application of this transformation does not yet take {@link #isStrict()} into account. This means
     * that even when strict filtering is not safe, expressions will be applied as intersections rather than unions.
     * The filter will always be evaluated strictly in conjunction with replica filtering protection at the 
     * coordinator, however, even after CASSANDRA-19007 is addressed.
     * 
     * @see <a href="https://issues.apache.org/jira/browse/CASSANDRA-190007">CASSANDRA-19007</a>
     */
    protected Transformation<BaseRowIterator<?>> filter(TableMetadata metadata, long nowInSec)
    {
        List<Expression> partitionLevelExpressions = new ArrayList<>();
        List<Expression> rowLevelExpressions = new ArrayList<>();
        for (Expression e: root.expressions())
        {
            if (e.column.isStatic() || e.column.isPartitionKey())
                partitionLevelExpressions.add(e);
            else
                rowLevelExpressions.add(e);
        }

        long numberOfRegularColumnExpressions = rowLevelExpressions.size();
        final boolean filterNonStaticColumns = numberOfRegularColumnExpressions > 0;

        return new Transformation<>()
        {
            DecoratedKey pk;

            @Override
            protected BaseRowIterator<?> applyToPartition(BaseRowIterator<?> partition)
            {
                pk = partition.partitionKey();

                // Short-circuit all partitions that won't match based on static and partition keys
                for (Expression e : partitionLevelExpressions)
                    if (!e.isSatisfiedBy(metadata, partition.partitionKey(), partition.staticRow()))
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

                for (Expression e : rowLevelExpressions)
                    if (!e.isSatisfiedBy(metadata, pk, purged))
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
    public UnfilteredPartitionIterator filter(UnfilteredPartitionIterator iter, long nowInSec)
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
    public PartitionIterator filter(PartitionIterator iter, TableMetadata metadata, long nowInSec)
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
    public boolean isSatisfiedBy(TableMetadata metadata, DecoratedKey partitionKey, Row row, long nowInSec)
    {
        // We purge all tombstones as the expressions isSatisfiedBy methods expects it
        Row purged = row.purge(DeletionPurger.PURGE_ALL, nowInSec, metadata.enforceStrictLiveness());
        if (purged == null)
            return root.isEmpty();

        return root.isSatisfiedBy(metadata, partitionKey, purged);
    }

    /**
     * Returns true if all of the expressions within this filter that apply to the partition key are satisfied by
     * the given key, false otherwise.
     */
    public boolean partitionKeyRestrictionsAreSatisfiedBy(DecoratedKey key, AbstractType<?> keyValidator)
    {
        warnIfFilterIsATree();
        for (Expression e : root)
        {
            if (!e.column.isPartitionKey())
                continue;

            ByteBuffer value = keyValidator instanceof CompositeType
                             ? ((CompositeType) keyValidator).split(key.getKey())[e.column.position()]
                             : key.getKey();
            if (!e.operator().isSatisfiedBy(e.column.type, value, e.value))
                return false;
        }
        return true;
    }

    /**
     * Returns true if all of the expressions within this filter that apply to the clustering key are satisfied by
     * the given Clustering, false otherwise.
     */
    public boolean clusteringKeyRestrictionsAreSatisfiedBy(Clustering<?> clustering)
    {
        warnIfFilterIsATree();
        for (Expression e : root)
        {
            if (!e.column.isClusteringColumn())
                continue;

            if (!e.operator().isSatisfiedBy(e.column.type, clustering.bufferAt(e.column.position()), e.value))
            {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns this filter but without the provided expression. This method
     * *assumes* that the filter contains the provided expression.
     */
    public RowFilter without(Expression expression)
    {
        warnIfFilterIsATree();
        assert root.contains(expression);
        if (root.size() == 1)
            return RowFilter.none();

        return new CQLFilter(root.filter(e -> !e.equals(expression)), needsReconciliation);
    }

    /**
     * Returns a copy of this filter but without the provided expression. If this filter doesn't contain the specified
     * expression this method will just return an identical copy of this filter.
     */
    public RowFilter without(ColumnMetadata column, Operator op, ByteBuffer value)
    {
        if (isEmpty())
            return this;

        return new CQLFilter(root.filter(e -> !e.column().equals(column) || e.operator() != op || !e.value.equals(value)), needsReconciliation);
    }

    public RowFilter withoutExpressions()
    {
        return new CQLFilter(root.filter(e -> false), needsReconciliation);
    }

    public RowFilter restrict(Predicate<Expression> filter)
    {
        return new CQLFilter(root.filter(filter), needsReconciliation);
    }

    public boolean isEmpty()
    {
        return root.isEmpty();
    }

    public Iterator<Expression> iterator()
    {
        warnIfFilterIsATree();
        return root.iterator();
    }

    @Override
    public String toString()
    {
        return toString(false);
    }

    /**
     * Returns a CQL representation of this row filter.
     *
     * @return a CQL representation of this row filter
     */
    public String toCQLString()
    {
        return toString(true);
    }

    private String toString(boolean cql)
    {
        return root.toString(cql);
    }

    private void warnIfFilterIsATree()
    {
        if (!root.children.isEmpty())
        {
            noSpamLogger.warn("RowFilter is a tree, but we're using it as a list of top-levels expressions", new Exception("stacktrace of a potential misuse"));
        }
    }

    public static Builder builder(boolean needsReconciliation)
    {
        return new Builder(needsReconciliation);
    }

    public static class Builder
    {
        private FilterElement.Builder current = new FilterElement.Builder(false);
        boolean needsReconciliation = false;

        public Builder(boolean needsReconciliation)
        {
            this.needsReconciliation = needsReconciliation;
        }

        public RowFilter build()
        {
            return new CQLFilter(current.build(), needsReconciliation);
        }

        public RowFilter buildFromRestrictions(StatementRestrictions restrictions, IndexRegistry indexManager, TableMetadata table, QueryOptions options)
        {
            return new CQLFilter(doBuild(restrictions, indexManager, table, options), needsReconciliation);
        }

        private FilterElement doBuild(StatementRestrictions restrictions, IndexRegistry indexManager, TableMetadata table, QueryOptions options)
        {
            FilterElement.Builder element = new FilterElement.Builder(restrictions.isDisjunction());
            this.current = element;

            for (Restrictions restrictionSet : restrictions.filterRestrictions().getRestrictions())
                restrictionSet.addToRowFilter(this, indexManager, options);

            for (ExternalRestriction expression : restrictions.filterRestrictions().getExternalExpressions())
                expression.addToRowFilter(this, table, options);

            for (StatementRestrictions child : restrictions.children())
                element.children.add(doBuild(child, indexManager, table, options));

            return element.build();
        }

        public SimpleExpression add(ColumnMetadata def, Operator op, ByteBuffer value)
        {
            SimpleExpression expression = new SimpleExpression(def, op, value);
            add(expression);
            return expression;
        }

        public void addMapComparison(ColumnMetadata def, ByteBuffer key, Operator op, ByteBuffer value)
        {
            add(new MapComparisonExpression(def, key, op, value));
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

    public static class FilterElement implements Iterable<Expression>
    {
        public static final Serializer serializer = new Serializer();

        public static final FilterElement NONE = new FilterElement(false, Collections.emptyList(), Collections.emptyList());

        private boolean isDisjunction;

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

        public List<Expression> expressions()
        {
            return expressions;
        }

        public Iterator<Expression> iterator()
        {
            List<Expression> allExpressions = new ArrayList<>(expressions);
            for (FilterElement child : children)
                allExpressions.addAll(child.expressions);
            return allExpressions.iterator();
        }

        public FilterElement filter(Predicate<Expression> filter)
        {
            FilterElement.Builder builder = new Builder(isDisjunction);

            expressions.stream().filter(filter).forEach(e -> builder.expressions.add(e));

            children.stream().map(c -> c.filter(filter)).forEach(c -> builder.children.add(c));

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

        public String toString(boolean cql)
        {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < expressions.size(); i++)
            {
                if (sb.length() > 0)
                    sb.append(isDisjunction ? " OR " : " AND ");
                sb.append(expressions.get(i).toString(cql));
            }
            for (int i = 0; i < children.size(); i++)
            {
                if (sb.length() > 0)
                    sb.append(isDisjunction ? " OR " : " AND ");
                sb.append("(");
                sb.append(children.get(i).toString(cql));
                sb.append(")");
            }
            return sb.toString();
        }

        @Override
        public String toString()
        {
            return toString(false);
        }

        public Stream<Expression> getExpressionsPreOrder()
        {
            return Stream.concat(expressions.stream(),
                                 children.stream().flatMap(FilterElement::getExpressionsPreOrder));
        }

        public static class Builder
        {
            private boolean isDisjunction;
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
                assert (!operation.isDisjunction && operation.children().isEmpty()) || version >= MessagingService.Version.VERSION_SG_10.value :
                "Attempting to serialize a disjunct row filter to a node that doesn't support disjunction";

                out.writeBoolean(operation.isDisjunction);
                out.writeUnsignedVInt32(operation.expressions.size());
                for (Expression expr : operation.expressions)
                    Expression.serializer.serialize(expr, out, version);
                out.writeUnsignedVInt32(operation.children.size());
                for (FilterElement child : operation.children)
                    serialize(child, out, version);
            }

            public FilterElement deserialize(DataInputPlus in, int version, TableMetadata metadata) throws IOException
            {
                boolean isDisjunction = in.readBoolean();
                int size = (int)in.readUnsignedVInt32();
                List<Expression> expressions = new ArrayList<>(size);
                for (int i = 0; i < size; i++)
                    expressions.add(Expression.serializer.deserialize(in, version, metadata));
                size = (int)in.readUnsignedVInt32();
                List<FilterElement> children = new ArrayList<>(size);
                for (int i  = 0; i < size; i++)
                    children.add(deserialize(in, version, metadata));
                return new FilterElement(isDisjunction, expressions, children);
            }

            public long serializedSize(FilterElement operation, int version)
            {
                long size = 1 + TypeSizes.sizeofUnsignedVInt(operation.expressions.size());
                for (Expression expr : operation.expressions)
                    size += Expression.serializer.serializedSize(expr, version);
                size += TypeSizes.sizeofUnsignedVInt(operation.children.size());
                for (FilterElement child : operation.children)
                    size += serializedSize(child, version);
                return size;
            }
        }
    }

    private static class CQLFilter extends RowFilter
    {
        private static final CQLFilter NONE = new CQLFilter(FilterElement.NONE, false);

        private CQLFilter(FilterElement operation, boolean needsReconciliation)
        {
            super(operation, needsReconciliation);
        }

        protected Transformation<BaseRowIterator<?>> filter(TableMetadata metadata, long nowInSec)
        {
            FilterElement partitionLevelOperation = root.partitionLevelTree();
            FilterElement rowLevelOperation = root.rowLevelTree();

            final boolean filterNonStaticColumns = rowLevelOperation.size() > 0;

            return new Transformation<>()
            {
                DecoratedKey pk;

                @SuppressWarnings("resource")
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
    }

    public static abstract class Expression
    {
        public static final Serializer serializer = new Serializer();

        // Note: the val of this enum is used for serialization,
        // and this is why we have some UNUSEDX for values we don't use anymore
        // (we could clean those on a major protocol update, but it's not worth
        // the trouble for now)
        // VECTOR
        protected enum Kind {
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

        /** @deprecated See CASSANDRA-6377 */
        @Deprecated(since = "3.5")
        public void validateForIndexing()
        {
            checkFalse(value.remaining() > FBUtilities.MAX_UNSIGNED_SHORT,
                       "Index expression values may not be larger than 64K");
        }

        /**
         * Returns whether the provided row satisfied this expression or not.
         *
         *
         * @param metadata
         * @param partitionKey the partition key for row to check.
         * @param row the row to check. It should *not* contain deleted cells
         * (i.e. it should come from a RowIterator).
         * @return whether the row is satisfied by this expression.
         */
        public abstract boolean isSatisfiedBy(TableMetadata metadata, DecoratedKey partitionKey, Row row);

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

            return Objects.equal(this.kind(), that.kind())
                && Objects.equal(this.column.name, that.column.name)
                && Objects.equal(this.operator, that.operator)
                && Objects.equal(this.value, that.value);
        }

        @Override
        public int hashCode()
        {
            return Objects.hashCode(column.name, operator, value);
        }

        @Override
        public String toString()
        {
            return toString(false);
        }

        /**
         * Returns a CQL representation of this expression.
         *
         * @return a CQL representation of this expression
         */
        public String toCQLString()
        {
            return toString(true);
        }

        protected abstract String toString(boolean cql);

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

            public Expression deserialize(DataInputPlus in, int version, TableMetadata metadata) throws IOException
            {
                Kind kind = Kind.fromVal(in.readByte());

                // custom expressions (3.0+ only) do not contain a column or operator, only a value
                if (kind == Kind.CUSTOM)
                {
                    return new CustomExpression(metadata,
                            IndexMetadata.serializer.deserialize(in, version, metadata),
                            ByteBufferUtil.readWithShortLength(in));
                }

                if (kind == Kind.USER)
                    return UserExpression.deserialize(in, version, metadata);

                ByteBuffer name = ByteBufferUtil.readWithShortLength(in);
                Operator operator = Operator.readFrom(in);
                ColumnMetadata column = metadata.getColumn(name);

                // Compact storage tables, when used with thrift, used to allow falling through this withouot throwing an
                // exception. However, since thrift was removed in 4.0, this behaviour was not restored in CASSANDRA-16217
                if (column == null)
                    throw new RuntimeException("Unknown (or dropped) column " + UTF8Type.instance.getString(name) + " during deserialization");

                switch (kind)
                {
                    case SIMPLE:
                        return new SimpleExpression(column, operator, ByteBufferUtil.readWithShortLength(in));
                    case MAP_COMPARISON:
                        ByteBuffer key = ByteBufferUtil.readWithShortLength(in);
                        ByteBuffer value = ByteBufferUtil.readWithShortLength(in);
                        return new MapComparisonExpression(column, key, operator, value);
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
                        size += ByteBufferUtil.serializedSizeWithShortLength(((SimpleExpression)expression).value);
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
        public SimpleExpression(ColumnMetadata column, Operator operator, ByteBuffer value)
        {
            super(column, operator, value);
        }

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
                        assert !column.isComplex() : "Only CONTAINS and CONTAINS_KEY are supported for collection types";

                        // In order to support operators on Counter types, their value has to be extracted from internal
                        // representation. See CASSANDRA-11629
                        if (column.type.isCounter())
                        {
                            ByteBuffer foundValue = getValue(metadata, partitionKey, row);
                            if (foundValue == null)
                                return false;

                            ByteBuffer counterValue = LongType.instance.decompose(CounterContext.instance().total(foundValue, ByteBufferAccessor.instance));
                            return operator.isSatisfiedBy(LongType.instance, counterValue, value);
                        }
                        else
                        {
                            // Note that CQL expression are always of the form 'x < 4', i.e. the tested value is on the left.
                            ByteBuffer foundValue = getValue(metadata, partitionKey, row);
                            return foundValue != null && operator.isSatisfiedBy(column.type, foundValue, value);
                        }
                    }
                case NEQ:
                case LIKE_PREFIX:
                case LIKE_SUFFIX:
                case LIKE_CONTAINS:
                case LIKE_MATCHES:
                case ANN:
                    {
                        assert !column.isComplex() : "Only CONTAINS and CONTAINS_KEY are supported for collection types";
                        ByteBuffer foundValue = getValue(metadata, partitionKey, row);
                        // Note that CQL expression are always of the form 'x < 4', i.e. the tested value is on the left.
                        return foundValue != null && operator.isSatisfiedBy(column.type, foundValue, value);
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
            CollectionType<?> type = (CollectionType<?>)column.type;
            if (column.isComplex())
            {
                ComplexColumnData complexData = row.getComplexColumnData(column);
                if (complexData != null)
                {
                    for (Cell<?> cell : complexData)
                    {
                        if (type.kind == CollectionType.Kind.SET)
                        {
                            if (type.nameComparator().compare(cell.path().get(0), value) == 0)
                                return true;
                        }
                        else
                        {
                            if (type.valueComparator().compare(cell.buffer(), value) == 0)
                                return true;
                        }
                    }
                }
                return false;
            }
            else
            {
                ByteBuffer foundValue = getValue(metadata, partitionKey, row);
                if (foundValue == null)
                    return false;

                switch (type.kind)
                {
                    case LIST:
                        ListType<?> listType = (ListType<?>)type;
                        return listType.compose(foundValue).contains(listType.getElementsType().compose(value));
                    case SET:
                        SetType<?> setType = (SetType<?>)type;
                        return setType.compose(foundValue).contains(setType.getElementsType().compose(value));
                    case MAP:
                        MapType<?,?> mapType = (MapType<?, ?>)type;
                        return mapType.compose(foundValue).containsValue(mapType.getValuesType().compose(value));
                }
                throw new AssertionError();
            }
        }

        private boolean containsKey(TableMetadata metadata, DecoratedKey partitionKey, Row row)
        {
            assert column.type.isCollection() && column.type instanceof MapType;
            MapType<?, ?> mapType = (MapType<?, ?>)column.type;
            if (column.isComplex())
            {
                return row.getCell(column, CellPath.create(value)) != null;
            }
            else
            {
                ByteBuffer foundValue = getValue(metadata, partitionKey, row);
                return foundValue != null && mapType.getSerializer().getSerializedValue(foundValue, value, mapType.getKeysType()) != null;
            }
        }

        @Override
        protected String toString(boolean cql)
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
                default:
                    break;
            }
            return cql
                 ? String.format("%s %s %s", column.name.toCQLString(), operator, type.toCQLString(value) )
                 : String.format("%s %s %s", column.name.toString(), operator, type.getString(value));
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

        public MapComparisonExpression(ColumnMetadata column, ByteBuffer key, Operator operator, ByteBuffer value)
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
         * @param metadata
         * @param partitionKey the partition key for row to check.
         * @param row the row to check. It should *not* contain deleted cells
         * (i.e. it should come from a RowIterator).
         * @return whether the row is satisfied by this expression.
         */
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
        protected String toString(boolean cql)
        {
            MapType<?, ?> mt = (MapType<?, ?>) column.type;
            AbstractType<?> nt = mt.nameComparator();
            AbstractType<?> vt = mt.valueComparator();
            return cql
                 ? String.format("%s[%s] %s %s", column.name.toCQLString(), nt.toCQLString(key), operator, vt.toCQLString(value))
                 : String.format("%s[%s] %s %s", column.name.toString(), nt.getString(key), operator, vt.getString(value));
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
                && Objects.equal(this.operator, that.operator)
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
         * Build a new {@link GeoDistanceExpression} that is shifted by 360 degrees and can correctly search
         * on the opposite side of the antimeridian.
         * @return
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
            return toString(false);
        }

        @Override
        protected String toString(boolean cql)
        {
            return String.format("GEO_DISTANCE(%s, %s) %s %s", cql ? column.name.toCQLString() : column.name.toString(),
                                 column.type.getString(value),
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
                   && Objects.equal(this.distanceOperator, that.distanceOperator)
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
        protected String toString(boolean cql)
        {
            return String.format("expr(%s, %s)",
                                 cql ? ColumnIdentifier.maybeQuote(targetIndex.name) : targetIndex.name,
                                 Keyspace.openAndGetStore(table)
                                         .indexManager
                                         .getIndex(targetIndex)
                                         .customExpressionValueType());
        }

        protected Kind kind()
        {
            return Kind.CUSTOM;
        }

        // Filtering by custom expressions isn't supported yet, so just accept any row
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
     *
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

        protected static abstract class Deserializer
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
            FilterElement.serializer.serialize(filter.root, out, version);

        }

        public RowFilter deserialize(DataInputPlus in, int version, TableMetadata metadata, boolean needsReconciliation) throws IOException
        {
            in.readBoolean(); // Unused
            FilterElement operation = FilterElement.serializer.deserialize(in, version, metadata);
            return new CQLFilter(operation, needsReconciliation);
        }

        public long serializedSize(RowFilter filter, int version)
        {
            long size = 1 // unused boolean
                        + FilterElement.serializer.serializedSize(filter.root, version);
            return size;
        }
    }
}
