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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;

import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.analyzer.AbstractAnalyzer;
import org.apache.cassandra.index.sai.utils.TreeFormatter;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.serializers.ListSerializer;
import org.apache.cassandra.transport.ProtocolVersion;

public class Operation
{
    public enum OperationType
    {
        AND, OR;

        public boolean apply(boolean a, boolean b)
        {
            switch (this)
            {
                case OR:
                    return a | b;

                case AND:
                    return a & b;

                default:
                    throw new AssertionError();
            }
        }
    }

    @VisibleForTesting
    protected static ListMultimap<ColumnMetadata, Expression> analyzeGroup(QueryController controller,
                                                                           OperationType op,
                                                                           List<RowFilter.Expression> expressions)
    {
        ListMultimap<ColumnMetadata, Expression> analyzed = ArrayListMultimap.create();
        Map<ColumnMetadata, Boolean> columnIsMultiExpression = new HashMap<>();

        // sort all of the expressions in the operation by name and priority of the logical operator
        // this gives us an efficient way to handle inequality and combining into ranges without extra processing
        // and converting expressions from one type to another.
        expressions.sort((a, b) -> {
            int cmp = a.column().compareTo(b.column());
            return cmp == 0 ? -Integer.compare(getPriority(a.operator()), getPriority(b.operator())) : cmp;
        });

        for (final RowFilter.Expression e : expressions)
        {
            IndexContext indexContext = controller.getContext(e);
            List<Expression> perColumn = analyzed.get(e.column());

            AbstractAnalyzer.AnalyzerFactory analyzerFactory = indexContext.getQueryAnalyzerFactory();
            AbstractAnalyzer analyzer = analyzerFactory.create();
            try
            {
                analyzer.reset(e.getIndexValue());

                // EQ/LIKE_*/NOT_EQ can have multiple expressions e.g. text = "Hello World",
                // becomes text = "Hello" AND text = "World" because "space" is always interpreted as a split point (by analyzer),
                // CONTAINS/CONTAINS_KEY are always treated as multiple expressions since they currently only targetting
                // collections, NOT_EQ is made an independent expression only in case of pre-existing multiple EQ expressions, or
                // if there is no EQ operations and NOT_EQ is met or a single NOT_EQ expression present,
                // in such case we know exactly that there would be no more EQ/RANGE expressions for given column
                // since NOT_EQ has the lowest priority.
                boolean isMultiExpression = columnIsMultiExpression.getOrDefault(e.column(), Boolean.FALSE);
                switch (e.operator())
                {
                    // case BM25: leave it at the default of `false`
                    case EQ:
                        // EQ operator will always be a multiple expression because it is being used by map entries
                        isMultiExpression = indexContext.isNonFrozenCollection();

                        // EQ wil behave like ANALYZER_MATCHES for analyzed columns if the analyzer supports EQ queries
                        isMultiExpression |= indexContext.isAnalyzed() && analyzerFactory.supportsEquals();
                        break;
                    case CONTAINS:
                    case CONTAINS_KEY:
                    case NOT_CONTAINS:
                    case NOT_CONTAINS_KEY:
                    case LIKE_PREFIX:
                    case LIKE_MATCHES:
                    case ANALYZER_MATCHES:
                        isMultiExpression = true;
                        break;
                    case NEQ:
                        // NEQ operator will always be a multiple expression if it is the only operator
                        // (e.g. multiple NEQ expressions)
                        isMultiExpression = isMultiExpression || perColumn.isEmpty();
                        break;
                }
                columnIsMultiExpression.put(e.column(), isMultiExpression);

                if (isMultiExpression)
                {
                    if (!analyzer.hasNext())
                    {
                        perColumn.add(new Expression(indexContext).add(e.operator(), ByteBuffer.allocate(0)));
                    }
                    else
                    {
                        // The hasNext implementation has a side effect, so we need to call next before calling hasNext
                        do
                        {
                            final ByteBuffer token = analyzer.next();
                            perColumn.add(new Expression(indexContext).add(e.operator(), token.duplicate()));
                        }
                        while (analyzer.hasNext());
                    }
                }
                else if (e instanceof RowFilter.GeoDistanceExpression)
                {
                    var distance = ((RowFilter.GeoDistanceExpression) e);
                    var expression = new Expression(indexContext)
                                     .add(distance.getDistanceOperator(), distance.getDistance().duplicate())
                                     .add(Operator.BOUNDED_ANN, e.getIndexValue().duplicate());
                    perColumn.add(expression);
                }
                else
                // "range" or not-equals operator, combines both bounds together into the single expression,
                // if operation of the group is AND, otherwise we are forced to create separate expressions,
                // not-equals is combined with the range iff operator is AND.
                {
                    Expression range;
                    if (perColumn.size() == 0 || op != OperationType.AND || e instanceof RowFilter.MapComparisonExpression)
                    {
                        range = new Expression(indexContext);
                        perColumn.add(range);
                    }
                    else
                    {
                        range = Iterables.getLast(perColumn);
                    }

                    if (!TypeUtil.isLiteral(indexContext.getValidator()))
                    {
                        range.add(e.operator(), e.getIndexValue().duplicate());
                    }
                    else if (e instanceof RowFilter.MapComparisonExpression)
                    {
                        var map = (RowFilter.MapComparisonExpression) e;
                        var operator = map.operator();
                        switch (operator) {
                            case EQ:
                            case NEQ:
                                range.add(operator, map.getIndexValue().duplicate());
                                break;
                            case GT:
                            case GTE:
                                range.add(operator, map.getLowerBound().duplicate());
                                range.add(Operator.LTE, map.getUpperBound().duplicate());
                                break;
                            case LT:
                            case LTE:
                                range.add(Operator.GTE, map.getLowerBound().duplicate());
                                range.add(operator, map.getUpperBound().duplicate());
                                break;
                            default:
                                throw new InvalidRequestException("Unexpected operator: " + operator);
                        }
                    }
                    else
                    {
                        while (analyzer.hasNext())
                        {
                            ByteBuffer term = analyzer.next();
                            range.add(e.operator(), term.duplicate());
                        }
                    }
                }
            }
            finally
            {
                analyzer.end();
            }
        }

        return analyzed;
    }

    private static int getPriority(org.apache.cassandra.cql3.Operator op)
    {
        switch (op)
        {
            case EQ:
                return 7;

            case CONTAINS:
            case CONTAINS_KEY:
                return 6;

            case LIKE_PREFIX:
            case LIKE_MATCHES:
                return 5;

            case GTE:
            case GT:
                return 4;

            case LTE:
            case LT:
                return 3;

            case NOT_CONTAINS:
            case NOT_CONTAINS_KEY:
                return 2;

            case NEQ:
                return 1;

            default:
                return 0;
        }
    }

    public static abstract class Node
    {
        ListMultimap<ColumnMetadata, Expression> expressionMap;

        boolean canFilter()
        {
            return (expressionMap != null && !expressionMap.isEmpty()) || !children().isEmpty() ;
        }

        List<Node> children()
        {
            return Collections.emptyList();
        }

        void add(Node child)
        {
            throw new UnsupportedOperationException();
        }

        RowFilter.Expression expression()
        {
            throw new UnsupportedOperationException();
        }

        /**
         * Analyze the tree, potentially flattening it and storing the result in expressionMap.
         */
        abstract void analyze(QueryController controller);

        abstract FilterTree filterTree();

        abstract Plan.KeysIteration plan(QueryController controller);

        static Node buildTree(QueryController controller, List<RowFilter.Expression> expressions, List<RowFilter.FilterElement> children, boolean isDisjunction)
        {
            OperatorNode node = isDisjunction ? new OrNode() : new AndNode();
            for (RowFilter.Expression expression : expressions)
                node.add(buildExpression(controller, expression, isDisjunction));
            for (RowFilter.FilterElement child : children)
                node.add(buildTree(controller, child));
            return node;
        }

        static Node buildTree(QueryController controller, RowFilter.FilterElement filterOperation)
        {
            return buildTree(controller, filterOperation.expressions(), filterOperation.children(), filterOperation.isDisjunction());
        }

        static Node buildExpression(QueryController controller, RowFilter.Expression expression, boolean isDisjunction)
        {
            if (expression.operator() == Operator.IN)
            {
                OperatorNode node = new OrNode();
                int size = ListSerializer.readCollectionSize(expression.getIndexValue(), ByteBufferAccessor.instance, ProtocolVersion.V3);
                int offset = ListSerializer.sizeOfCollectionSize(size, ProtocolVersion.V3);
                for (int index = 0; index < size; index++)
                {
                    node.add(new ExpressionNode(new RowFilter.SimpleExpression(expression.column(),
                                                                               Operator.EQ,
                                                                               ListSerializer.readValue(expression.getIndexValue(),
                                                                                                        ByteBufferAccessor.instance,
                                                                                                        offset,
                                                                                                        ProtocolVersion.V3),
                                                                               expression.analyzer(),
                                                                               expression.annOptions())));
                    offset += TypeSizes.INT_SIZE + ByteBufferAccessor.instance.getInt(expression.getIndexValue(), offset);
                }
                if (node.children().size() == 1)
                    return node.children().get(0);
                if (node.children().isEmpty())
                    return new EmptyNode();
                return node;
            }
            else if (isDisjunction && (expression.operator() == Operator.ANALYZER_MATCHES ||
                                       expression.operator() == Operator.EQ && controller.getContext(expression).isAnalyzed()))
            {
                // In case of having a tokenizing query_analyzer (such as NGram) with OR, we need to split the
                // expression into multiple expressions and intersect them.
                // The additional node in case of no tokenization will be taken care of in Plan.Factory#intersection()
                OperatorNode node = new AndNode();
                node.add(new ExpressionNode(expression));
                return node;
            }
            else
                return new ExpressionNode(expression);
        }

        Node analyzeTree(QueryController controller)
        {
            analyze(controller);
            return this;
        }

        @VisibleForTesting
        FilterTree buildFilter(QueryController controller)
        {
            analyze(controller);
            return filterTree();
        }

        /**
         * Formats the whole operation tree as a pretty tree.
         */
        public final String toStringRecursive()
        {
            TreeFormatter<Node> formatter = new TreeFormatter<>(Node::toString, Node::children);
            return formatter.format(this);
        }
    }

    static abstract class OperatorNode extends Node
    {
        List<Node> children = new ArrayList<>();

        @Override
        public List<Node> children()
        {
            return children;
        }

        @Override
        public void add(Node child)
        {
            children.add(child);
        }

        abstract protected OperationType operationType();
        abstract protected Plan.Builder planBuilder(QueryController controller);

        // expression list is the children that are leaf nodes... we could figure that out here...
        @Override
        public void analyze(QueryController controller)
        {
            // This operation flattens the tree where possible and stores the result in expressionMap
            List<RowFilter.Expression> expressionList = new ArrayList<>();
            for (Node child : children)
            {
                if (child instanceof ExpressionNode)
                    expressionList.add(child.expression());
                else
                    child.analyze(controller);
            }
            expressionMap = analyzeGroup(controller, operationType(), expressionList);
        }

        @Override
        FilterTree filterTree()
        {
            assert expressionMap != null;
            var tree = new FilterTree(operationType(), expressionMap);
            for (Node child : children())
                if (child.canFilter())
                    tree.addChild(child.filterTree());
            return tree;
        }

        @Override
        Plan.KeysIteration plan(QueryController controller)
        {
            var builder = planBuilder(controller);
            if (!expressionMap.isEmpty())
                controller.buildPlanForExpressions(builder, expressionMap.values());
            for (Node child : children)
                if (child.canFilter())
                    builder.add(child.plan(controller));
            return builder.build();
        }
    }

    public static class AndNode extends OperatorNode
    {
        @Override
        protected OperationType operationType()
        {
            return OperationType.AND;
        }

        @Override
        protected Plan.Builder planBuilder(QueryController controller)
        {
            return controller.planFactory.intersectionBuilder();
        }

        @Override
        public String toString()
        {
            return "AndNode";
        }
    }

    public static class OrNode extends OperatorNode
    {
        @Override
        protected OperationType operationType()
        {
            return OperationType.OR;
        }

        @Override
        protected Plan.Builder planBuilder(QueryController controller)
        {
            return controller.planFactory.unionBuilder();
        }

        @Override
        public String toString()
        {
            return "OrNode";
        }
    }

    public static class ExpressionNode extends Node
    {
        RowFilter.Expression expression;

        @Override
        public void analyze(QueryController controller)
        {
            expressionMap = analyzeGroup(controller, OperationType.AND, Collections.singletonList(expression));
        }

        @Override
        FilterTree filterTree()
        {
            assert expressionMap != null;
            return new FilterTree(OperationType.AND, expressionMap);
        }

        public ExpressionNode(RowFilter.Expression expression)
        {
            this.expression = expression;
        }

        @Override
        public RowFilter.Expression expression()
        {
            return expression;
        }

        @Override
        Plan.KeysIteration plan(QueryController controller)
        {
            assert canFilter() : "Cannot process query with no expressions";
            Plan.Builder builder = controller.planFactory.intersectionBuilder();
            controller.buildPlanForExpressions(builder, expressionMap.values());
            return builder.build();
        }

        @Override
        public String toString()
        {
            return "ExpressionNode{expression=" + expression + '}';
        }
    }

    public static class EmptyNode extends Node
    {
        // A FilterTree that filters out all rows
        private static final FilterTree EMPTY_TREE = new FilterTree(OperationType.OR, ArrayListMultimap.create());

        @Override
        boolean canFilter()
        {
            return true;
        }

        @Override
        void analyze(QueryController controller)
        {
        }

        @Override
        FilterTree filterTree()
        {
            return EMPTY_TREE;
        }

        @Override
        Plan.KeysIteration plan(QueryController controller)
        {
            return controller.planFactory.nothing;
        }

        @Override
        public String toString()
        {
            return "EmptyNode";
        }
    }

}
