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

package org.apache.cassandra.cql3;

import org.apache.cassandra.cql3.restrictions.SingleColumnRestriction;
import org.apache.cassandra.cql3.restrictions.SingleRestriction;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;

/**
 * A single element of an ORDER BY clause.
 * <code>ORDER BY ordering1 [, ordering2 [, ...]] </code>
 * <p>
 * An ordering comprises an expression that produces the values to compare against each other
 * and a sorting direction (ASC, DESC).
 */
public class Ordering
{
    public final Expression expression;
    public final Direction direction;

    public Ordering(Expression expression, Direction direction)
    {
        this.expression = expression;
        this.direction = direction;
    }

    public interface Expression
    {
        boolean hasNonClusteredOrdering();

        SingleRestriction toRestriction();

        ColumnMetadata getColumn();

        default boolean isScored()
        {
            return false;
        }
    }

    /**
     * Represents a single column in
     * <code>ORDER BY column</code>
     */
    public static class SingleColumn implements Expression
    {
        public final ColumnMetadata column;
        private final Ordering.Direction direction;

        public SingleColumn(ColumnMetadata column, Ordering.Direction direction)
        {
            this.column = column;
            this.direction = direction;
        }

        @Override
        public boolean hasNonClusteredOrdering()
        {
            return !column.isClusteringColumn();
        }

        @Override
        public SingleRestriction toRestriction()
        {
            return new SingleColumnRestriction.OrderRestriction(column, direction == Direction.DESC ? Operator.ORDER_BY_DESC : Operator.ORDER_BY_ASC);
        }

        @Override
        public ColumnMetadata getColumn()
        {
            return column;
        }
    }

    /**
     * An expression used in Approximate Nearest Neighbor ordering.
     * <code>ORDER BY column ANN OF value</code>
     */
    public static class Ann implements Expression
    {
        final ColumnMetadata column;
        final Term vectorValue;
        final Direction direction;

        public Ann(ColumnMetadata column, Term vectorValue, Direction direction)
        {
            this.column = column;
            this.vectorValue = vectorValue;
            this.direction = direction;
        }

        @Override
        public boolean hasNonClusteredOrdering()
        {
            return true;
        }

        @Override
        public SingleRestriction toRestriction()
        {
            return new SingleColumnRestriction.AnnRestriction(column, vectorValue);
        }

        @Override
        public ColumnMetadata getColumn()
        {
            return column;
        }

        @Override
        public boolean isScored()
        {
            return SelectStatement.ANN_USE_SYNTHETIC_SCORE;
        }
    }

    /**
     * An expression used in BM25 ordering.
     * <code>ORDER BY column BM25 OF value</code>
     */
    public static class Bm25 implements Expression
    {
        final ColumnMetadata column;
        final Term queryValue;
        final Direction direction;

        public Bm25(ColumnMetadata column, Term queryValue, Direction direction)
        {
            this.column = column;
            this.queryValue = queryValue;
            this.direction = direction;
        }

        @Override
        public boolean hasNonClusteredOrdering()
        {
            return true;
        }

        @Override
        public SingleRestriction toRestriction()
        {
            return new SingleColumnRestriction.Bm25Restriction(column, queryValue);
        }

        @Override
        public ColumnMetadata getColumn()
        {
            return column;
        }

        @Override
        public boolean isScored()
        {
            return true;
        }
    }

    public enum Direction
    {ASC, DESC}


    /**
     * Represents the AST of a single element in the ORDER BY clause.
     * This comes directly out of CQL parser.
     */
    public static class Raw
    {

        final Expression expression;
        final Ordering.Direction direction;

        public Raw(Expression expression, Ordering.Direction direction)
        {
            this.expression = expression;
            this.direction = direction;
        }

        /**
         * Resolves column identifiers against the table schema.
         * Binds markers (?) to columns.
         */
        public Ordering bind(TableMetadata table, VariableSpecifications boundNames)
        {
            return new Ordering(expression.bind(table, boundNames, direction), direction);
        }

        public interface Expression
        {
            Ordering.Expression bind(TableMetadata table, VariableSpecifications boundNames, Ordering.Direction direction);
        }

        public static class SingleColumn implements Expression
        {
            final ColumnIdentifier column;

            SingleColumn(ColumnIdentifier column)
            {
                this.column = column;
            }

            @Override
            public Ordering.Expression bind(TableMetadata table, VariableSpecifications boundNames, Ordering.Direction direction)
            {
                return new Ordering.SingleColumn(table.getExistingColumn(column), direction);
            }
        }

        public static class Ann implements Expression
        {
            final ColumnIdentifier columnId;
            final Term.Raw vectorValue;

            Ann(ColumnIdentifier column, Term.Raw vectorValue)
            {
                this.columnId = column;
                this.vectorValue = vectorValue;
            }

            @Override
            public Ordering.Expression bind(TableMetadata table, VariableSpecifications boundNames, Direction direction)
            {
                ColumnMetadata column = table.getExistingColumn(columnId);
                Term value = vectorValue.prepare(table.keyspace, column);
                value.collectMarkerSpecification(boundNames);
                return new Ordering.Ann(column, value, direction);
            }
        }

        public static class Bm25 implements Expression
        {
            final ColumnIdentifier columnId;
            final Term.Raw queryValue;

            Bm25(ColumnIdentifier column, Term.Raw queryValue)
            {
                this.columnId = column;
                this.queryValue = queryValue;
            }

            @Override
            public Ordering.Expression bind(TableMetadata table, VariableSpecifications boundNames, Direction direction)
            {
                ColumnMetadata column = table.getExistingColumn(columnId);
                Term value = queryValue.prepare(table.keyspace, column);
                value.collectMarkerSpecification(boundNames);
                return new Ordering.Bm25(column, value, direction);
            }
        }
    }
}



