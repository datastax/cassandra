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

package org.apache.cassandra.cql3.restrictions;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.Relation;

/**
 * This is a representation of a complex query as a tree of expression elements. An element in the
 * tree can have one or more relations or custom index expressions associated with it and
 * one or more child expression elements.
 *
 *
 */
public class ExpressionTree
{
    public static final ExpressionTree EMPTY = new ExpressionTree(ExpressionElement.EMPTY);

    private final ExpressionElement root;

    private ExpressionTree(ExpressionElement root)
    {
        this.root = root;
    }

    public boolean isEmpty()
    {
        return root == null;
    }

    public ExpressionElement root()
    {
        return root;
    }

    public ExpressionTree rename(ColumnIdentifier from, ColumnIdentifier to)
    {
        return new ExpressionTree(root.rename(from, to));
    }

    @Override
    public String toString()
    {
        return root.toString();
    }

    /**
     * This receives fragments from the parse operation and builds them into the final <code>ExpressionTree</code>.
     *
     * The received fragments are:
     * <ul>
     *     <li><code>add(Relation)</code> - adds a new relation to the current <code>ParseState</code></li>
     *     <li><code>add(CustomIndexExpression)</code> - adds a new custom index expression to the current <code>ParseState</code></li>
     *     <li><code>startEnclosure</code> - responds to a '(' and pushes the current <code>ParseState</code> onto the precedence stack</li>
     *     <li><code>endEnclosure</code> - responds to a ')' and pulls the <code>ParseState</code> associated with the
     *     matching <code>startEnclosure</code>. It will pull any intermediate precedence states off the stack until it
     *     reaches the matching enclosure state</li>
     *     <li><code>setCurrentOperator</code> - changes the operator in the <code>ParseState</code>. If this new operator is
     *     of a higher precedence than the current operator, the last expression is popped from the <code>ParseState</code> and
     *     the state is pushed onto the precedence stack</li>
     *     <li><code>build</code> - always the last call. This builds the resultant <code>ExpressionTree</code> from the
     *     precedence stack and the current <code>ParseState</code></li>
     * </ul>
     */
    public static class Builder
    {
        private final Deque<ParseState> precedenceStack = new ArrayDeque<>();
        private ParseState parseState = new ParseState();

        public void add(Relation relation)
        {
            parseState.push(new RelationElement(relation));
        }

        public void add(CustomIndexExpression customIndexExpression)
        {
            parseState.push(new CustomIndexExpressionElement(customIndexExpression));
        }

        public void startEnclosure()
        {
            pushStack(PushState.ENCLOSURE);
        }

        public void endEnclosure()
        {
            do
            {
                ExpressionElement expression = generate();
                parseState = precedenceStack.pop();
                parseState.push(expression);
            }
            while (parseState.enclosure == PushState.PRECEDENCE);
        }

        public void setCurrentOperator(String value)
        {
            Operator operator = Operator.valueOf(value.toUpperCase());
            if (parseState.isChangeOfOperator(operator))
            {
                if (parseState.higherPrecedence(operator))
                {
                    // Where we have a = 1 OR b = 1 AND c = 1. When the operator changes to AND
                    // we need to pop b = 1 from the parseState, push the parseState containing
                    // a = 1 OR and then add b = 1 to the new parseState
                    ExpressionElement last = parseState.pop();
                    pushStack(PushState.PRECEDENCE);
                    parseState.push(last);
                }
                else
                {
                    ExpressionElement element = generate();
                    if (!precedenceStack.isEmpty() && precedenceStack.peek().enclosure == PushState.PRECEDENCE)
                        parseState = precedenceStack.pop();
                    else
                        parseState.clear();
                    parseState.push(element);
                }
            }
            parseState.operator = operator;
        }

        public ExpressionTree build()
        {
            while (!precedenceStack.isEmpty())
            {
                ExpressionElement expression = generate();
                parseState = precedenceStack.pop();
                parseState.push(expression);
            }
            return new ExpressionTree(generate());
        }

        private void pushStack(PushState enclosure)
        {
            parseState.enclosure = enclosure;
            precedenceStack.push(parseState);
            parseState = new ParseState();
        }

        private ExpressionElement generate()
        {
            if (parseState.size() == 1)
                return parseState.pop();
            return parseState.asContainer();
        }
    }

    /**
     * Represents the state of the parsing operation at a point of enclosure or precedence change.
     */
    public static class ParseState
    {
        Operator operator = Operator.NONE;
        PushState enclosure = PushState.NONE;
        Deque<ExpressionElement> expressionElements = new ArrayDeque<>();

        void push(ExpressionElement element)
        {
            expressionElements.add(element);
        }

        ExpressionElement pop()
        {
            return expressionElements.removeLast();
        }

        int size()
        {
            return expressionElements.size();
        }

        ParseState clear()
        {
            expressionElements.clear();
            return this;
        }

        boolean isChangeOfOperator(Operator operator)
        {
            return this.operator != operator && expressionElements.size() > 1;
        }

        boolean higherPrecedence(Operator operator)
        {
            return operator.compareTo(this.operator) > 0;
        }

        ContainerElement asContainer()
        {
            return operator == Operator.OR ? new OrElement().add(expressionElements) : new AndElement().add(expressionElements);
        }
    }

    enum Operator
    {
        NONE, OR, AND;

        public String joinValue()
        {
            return " " + name() + " ";
        }
    }

    /**
     * This is the reason why the <code>ParseState</code> was pushed onto the precedence stack.
     */
    enum PushState
    {
        NONE, PRECEDENCE, ENCLOSURE
    }

    public static abstract class ExpressionElement
    {
        private static final ExpressionElement EMPTY = new EmptyElement();

        public List<ContainerElement> operations()
        {
            return Collections.emptyList();
        }

        public boolean isDisjunction()
        {
            return false;
        }

        public List<Relation> relations()
        {
            return Collections.emptyList();
        }

        public List<CustomIndexExpression> expressions()
        {
            return Collections.emptyList();
        }

        public boolean containsCustomExpressions()
        {
            return false;
        }

        public abstract String toEncapsulatedString();

        public ExpressionElement rename(ColumnIdentifier from, ColumnIdentifier to)
        {
            return this;
        }
    }

    public static abstract class VariableElement extends ExpressionElement
    {
        @Override
        public String toEncapsulatedString()
        {
            return toString();
        }
    }

    public static class EmptyElement extends VariableElement
    {
    }

    public static class RelationElement extends VariableElement
    {
        private final Relation relation;

        public RelationElement(Relation relation)
        {
            this.relation = relation;
        }

        @Override
        public List<Relation> relations()
        {
            return Lists.newArrayList(relation);
        }

        @Override
        public ExpressionElement rename(ColumnIdentifier from, ColumnIdentifier to)
        {
            return new RelationElement(relation.renameIdentifier(from, to));
        }

        @Override
        public String toString()
        {
            return relation.toString();
        }
    }

    public static class CustomIndexExpressionElement extends VariableElement
    {
        private final CustomIndexExpression customIndexExpression;

        public CustomIndexExpressionElement(CustomIndexExpression customIndexExpression)
        {
            this.customIndexExpression = customIndexExpression;
        }

        @Override
        public List<CustomIndexExpression> expressions()
        {
            return Lists.newArrayList(customIndexExpression);
        }

        @Override
        public boolean containsCustomExpressions()
        {
            return true;
        }

        @Override
        public String toString()
        {
            return customIndexExpression.toString();
        }
    }

    public static abstract class ContainerElement extends ExpressionElement
    {
        protected final List<ExpressionElement> children = new ArrayList<>();

        @Override
        public List<ContainerElement> operations()
        {
            return children.stream()
                           .filter(c -> (c instanceof ContainerElement))
                           .map(r -> ((ContainerElement)r))
                           .collect(Collectors.toList());
        }

        public ContainerElement add(Deque<ExpressionElement> children)
        {
            this.children.addAll(children);
            return this;
        }

        protected abstract Operator operator();

        @Override
        public List<Relation> relations()
        {
            return children.stream()
                           .filter(c -> (c instanceof RelationElement))
                           .map(r -> (((RelationElement)r).relation))
                           .collect(Collectors.toList());
        }

        @Override
        public List<CustomIndexExpression> expressions()
        {
            return children.stream()
                           .filter(c -> (c instanceof CustomIndexExpressionElement))
                           .map(r -> (((CustomIndexExpressionElement)r).customIndexExpression))
                           .collect(Collectors.toList());
        }

        @Override
        public boolean containsCustomExpressions()
        {
            return children.stream().anyMatch(ExpressionElement::containsCustomExpressions);
        }

        @Override
        public ExpressionElement rename(ColumnIdentifier from, ColumnIdentifier to)
        {
            AndElement element = new AndElement();
            children.stream().map(c -> c.rename(from, to)).forEach(c -> element.children.add(c));
            return element;
        }

        @Override
        public String toString()
        {
            return children.stream().map(ExpressionElement::toEncapsulatedString).collect(Collectors.joining(operator().joinValue()));
        }

        @Override
        public String toEncapsulatedString()
        {
            return children.stream().map(ExpressionElement::toEncapsulatedString).collect(Collectors.joining(operator().joinValue(), "(", ")"));
        }

    }

    public static class AndElement extends ContainerElement
    {
        @Override
        protected Operator operator()
        {
            return Operator.AND;
        }
    }

    public static class OrElement extends ContainerElement
    {
        @Override
        protected Operator operator()
        {
            return Operator.OR;
        }

        @Override
        public boolean isDisjunction()
        {
            return true;
        }
    }
}
