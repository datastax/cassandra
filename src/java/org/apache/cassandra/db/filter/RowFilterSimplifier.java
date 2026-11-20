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

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;

import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.filter.RowFilter.CellBounds;
import org.apache.cassandra.db.filter.RowFilter.Expression;
import org.apache.cassandra.db.filter.RowFilter.FilterElement;
import org.apache.cassandra.db.filter.RowFilter.SimpleExpression;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.schema.TableMetadata;

/// Simplifies a [FilterElement] by comparing its expressions with the expressions of its direct child elements of the
/// opposite type (an AND with its OR children, or an OR with its AND children).
///
/// All the simplifications here return exactly the same rows as the original element, including the rows where the
/// compared columns are null. Note that [Expression#isSatisfiedBy] returns `false` for null values, for all the
/// operators including `!=`, so `a != 0` is not the negation of `a = 0`. This is why `a = 0 OR (a != 0 AND b = 1)` is
/// not simplified to `a = 0 OR b = 1`.
///
/// The supported simplifications are:
/// * Absorption: `x OR (x AND y)` is replaced by `x`, and `x AND (x OR y)` is replaced by `x`.
/// * Contradiction: in `x AND (e OR y)`, `e` is removed if `x` and `e` can never be satisfied by the same value.
///   For example, `a = 0 AND (a != 0 OR b = 1)` is replaced by `a = 0 AND b = 1`. This is valid for any column,
///   because if `x` is satisfied the value is not null, so `e` can't be satisfied either.
///
/// The simplifications only look at the expressions of the element and at the expressions of its direct children.
final class RowFilterSimplifier
{
    private RowFilterSimplifier()
    {
    }

    /// Simplifies the specified element.
    ///
    /// @param element the element to simplify
    /// @param table the table of the queried data
    /// @param hints the index hints of the query
    /// @return the specified element if there is nothing to simplify, [FilterElement#REJECT_ALL] if the element is
    /// found to be unsatisfiable, or a simplified copy of the element otherwise
    static FilterElement simplify(FilterElement element, TableMetadata table, IndexHints hints)
    {
        List<Expression> parentExpressions = element.expressions();
        if (parentExpressions.isEmpty() || element.children().isEmpty())
            return element;

        // The expressions of the new element. It starts with the original ones, but it can get more expressions
        // moved from the simplified children.
        List<Expression> expressions = new ArrayList<>(parentExpressions);
        List<FilterElement> children = new ArrayList<>(element.children().size());
        boolean changed = false;

        for (FilterElement child : element.children())
        {
            // only the children of the opposite type can be simplified
            if (child.isDisjunction() == element.isDisjunction())
            {
                children.add(child);
                continue;
            }

            if (isAbsorbed(parentExpressions, child))
            {
                changed = true;
                continue;
            }

            // the contradictions are only valid in conjunctions, see above
            if (element.isDisjunction())
            {
                children.add(child);
                continue;
            }

            List<Expression> remaining = new ArrayList<>(child.expressions().size());
            for (Expression expression : child.expressions())
            {
                if (!contradictsAny(parentExpressions, expression, table, hints))
                    remaining.add(expression);
            }

            if (remaining.size() == child.expressions().size())
            {
                children.add(child);
                continue;
            }

            changed = true;

            // An empty disjunction is always false, and so is the conjunction that contains it.
            if (remaining.isEmpty() && child.children().isEmpty())
                return FilterElement.REJECT_ALL;

            // If there is a single expression left it is moved to the parent, unless the parent already has an
            // expression on the same column. That would produce combinations of restrictions on the same column
            // that CQL doesn't allow, such as `a > 5 AND a > 7`.
            if (remaining.size() == 1 && child.children().isEmpty() && !hasExpressionOnColumn(expressions, remaining.get(0)))
                expressions.add(remaining.get(0));
            else
                children.add(new FilterElement(child.isDisjunction(), remaining, child.children()));
        }

        return changed ? new FilterElement(element.isDisjunction(), expressions, children) : element;
    }

    /// Returns whether the specified child can be removed because it contains any of the expressions of its parent.
    /// In `x OR (x AND y)` and `x AND (x OR y)` the child is redundant, because it can't change the result of its parent.
    private static boolean isAbsorbed(List<Expression> parentExpressions, FilterElement child)
    {
        for (Expression expression : child.expressions())
        {
            if (isComparable(expression) && parentExpressions.contains(expression))
                return true;
        }
        return false;
    }

    /// Returns whether the specified expression can be compared to other expressions.
    /// The ordering and the custom expressions are not predicates on the values of a column.
    private static boolean isComparable(Expression expression)
    {
        return !expression.isOrderingExpression() && !expression.isCustom() && !expression.isUserDefined();
    }

    /// Returns whether the specified expression of a disjunction can be removed because it contradicts any of the
    /// specified expressions of its parent conjunction.
    private static boolean contradictsAny(List<Expression> parentExpressions,
                                          Expression expression,
                                          TableMetadata table,
                                          IndexHints hints)
    {
        for (Expression parentExpression : parentExpressions)
        {
            if (areMutuallyExclusive(parentExpression, expression, table, hints))
                return true;
        }
        return false;
    }

    private static boolean hasExpressionOnColumn(List<Expression> expressions, Expression expression)
    {
        for (Expression other : expressions)
        {
            if (other.column().equals(expression.column()))
                return true;
        }
        return false;
    }

    /// Returns the type used to compare the values of the specified expressions, or `null` if the expressions can't be
    /// compared. Only simple expressions on the same column with an `=`, `!=` or slice operator can be compared.
    @Nullable
    private static AbstractType<?> comparableType(Expression left, Expression right, TableMetadata table, IndexHints hints)
    {
        if (!(left instanceof SimpleExpression) || !(right instanceof SimpleExpression))
            return null;

        if (!left.column().equals(right.column()) || left.analyzer() != null || right.analyzer() != null)
            return null;

        AbstractType<?> type = CellBounds.boundValueType(left, table, hints);
        if (type == null || CellBounds.boundValueType(right, table, hints) == null)
            return null;

        return type;
    }

    /// Returns whether it's impossible for the same value to satisfy both specified expressions.
    /// Only simple expressions on the same column with an `=`, `!=` or slice operator are considered.
    /// For example, `x = 1` and `x != 1`, `x = 1` and `x = 2`, or `x < 1` and `x > 2` are mutually exclusive.
    static boolean areMutuallyExclusive(Expression left, Expression right, TableMetadata table, IndexHints hints)
    {
        AbstractType<?> type = comparableType(left, right, table, hints);
        if (type == null)
            return false;

        Operator leftOperator = left.operator();
        Operator rightOperator = right.operator();

        if (leftOperator == Operator.EQ)
            return !rightOperator.isSatisfiedBy(type, left.value, right.value);

        if (rightOperator == Operator.EQ)
            return !leftOperator.isSatisfiedBy(type, right.value, left.value);

        // a lower bound and an upper bound can define an empty range, such as `x < 1 AND x > 2`
        if (leftOperator.isSlice() && rightOperator.isSlice() && isLowerBound(leftOperator) != isLowerBound(rightOperator))
        {
            CellBounds bounds = new CellBounds(type);
            bounds.add(leftOperator, left.value);
            bounds.add(rightOperator, right.value);
            return bounds.isUnsatisfiable();
        }

        return false;
    }

    private static boolean isLowerBound(Operator operator)
    {
        return operator == Operator.GT || operator == Operator.GTE;
    }
}
