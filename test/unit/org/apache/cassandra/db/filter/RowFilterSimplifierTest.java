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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.filter.RowFilter.Expression;
import org.apache.cassandra.db.filter.RowFilter.FilterElement;
import org.apache.cassandra.db.marshal.CounterColumnType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/// Unit tests for [RowFilterSimplifier] that build the filter elements directly, so they can cover combinations of
/// expressions that CQL validation doesn't allow, and expressions that are not plain comparisons.
/// The simplification of the filters of CQL queries is tested by [RowFilterAbsorptionTest].
public class RowFilterSimplifierTest
{
    private static TableMetadata table;
    private static ColumnMetadata a;
    private static ColumnMetadata b;
    private static ColumnMetadata c;
    private static ColumnMetadata ck;
    private static ColumnMetadata mapColumn;
    private static ColumnMetadata counterColumn;

    @BeforeClass
    public static void setupClass()
    {
        DatabaseDescriptor.daemonInitialization();

        // a virtual table is enough, and it doesn't require the table to be part of the schema
        table = TableMetadata.builder("ks", "vt")
                             .kind(TableMetadata.Kind.VIRTUAL)
                             .addPartitionKeyColumn("k", Int32Type.instance)
                             .addClusteringColumn("ck", Int32Type.instance)
                             .addRegularColumn("a", Int32Type.instance)
                             .addRegularColumn("b", Int32Type.instance)
                             .addRegularColumn("c", Int32Type.instance)
                             .addRegularColumn("m", MapType.getInstance(UTF8Type.instance, Int32Type.instance, true))
                             .addRegularColumn("cnt", CounterColumnType.instance)
                             .build();
        a = table.getColumn(ByteBufferUtil.bytes("a"));
        b = table.getColumn(ByteBufferUtil.bytes("b"));
        c = table.getColumn(ByteBufferUtil.bytes("c"));
        ck = table.getColumn(ByteBufferUtil.bytes("ck"));
        mapColumn = table.getColumn(ByteBufferUtil.bytes("m"));
        counterColumn = table.getColumn(ByteBufferUtil.bytes("cnt"));
    }

    @Test
    public void testMutuallyExclusiveWithEquality()
    {
        assertExclusive(expr(a, Operator.EQ, 0), expr(a, Operator.NEQ, 0));
        assertExclusive(expr(a, Operator.EQ, 0), expr(a, Operator.EQ, 1));
        assertExclusive(expr(a, Operator.EQ, 0), expr(a, Operator.GT, 0));
        assertExclusive(expr(a, Operator.EQ, 0), expr(a, Operator.LT, 0));
        assertExclusive(expr(a, Operator.EQ, 1), expr(a, Operator.LT, 1));
        assertExclusive(expr(a, Operator.EQ, 1), expr(a, Operator.GT, 2));

        assertNotExclusive(expr(a, Operator.EQ, 0), expr(a, Operator.EQ, 0));
        assertNotExclusive(expr(a, Operator.EQ, 0), expr(a, Operator.NEQ, 1));
        assertNotExclusive(expr(a, Operator.EQ, 0), expr(a, Operator.GTE, 0));
        assertNotExclusive(expr(a, Operator.EQ, 0), expr(a, Operator.LTE, 0));
        assertNotExclusive(expr(a, Operator.EQ, 1), expr(a, Operator.GT, 0));
        assertNotExclusive(expr(a, Operator.EQ, 1), expr(a, Operator.LT, 2));
    }

    @Test
    public void testMutuallyExclusiveWithSlices()
    {
        assertExclusive(expr(a, Operator.LT, 1), expr(a, Operator.GT, 2));
        assertExclusive(expr(a, Operator.LT, 1), expr(a, Operator.GT, 1));
        assertExclusive(expr(a, Operator.LT, 1), expr(a, Operator.GTE, 1));
        assertExclusive(expr(a, Operator.LTE, 1), expr(a, Operator.GT, 1));

        assertNotExclusive(expr(a, Operator.LTE, 1), expr(a, Operator.GTE, 1));
        assertNotExclusive(expr(a, Operator.LT, 2), expr(a, Operator.GT, 1));

        // slices of the same direction always overlap
        assertNotExclusive(expr(a, Operator.GT, 0), expr(a, Operator.GT, 1));
        assertNotExclusive(expr(a, Operator.LT, 0), expr(a, Operator.LT, 1));

        // inequalities don't exclude anything but their own value
        assertNotExclusive(expr(a, Operator.NEQ, 0), expr(a, Operator.NEQ, 0));
        assertNotExclusive(expr(a, Operator.NEQ, 0), expr(a, Operator.GT, 0));
        assertNotExclusive(expr(a, Operator.NEQ, 0), expr(a, Operator.LT, 0));
    }

    @Test
    public void testMutuallyExclusiveIgnoresOtherExpressions()
    {
        // different columns
        assertNotExclusive(expr(a, Operator.EQ, 0), expr(b, Operator.NEQ, 0));

        // other operators
        assertNotExclusive(expr(a, Operator.EQ, 0), expr(a, Operator.IN, 1));
        assertNotExclusive(expr(a, Operator.EQ, 0), expr(a, Operator.NOT_IN, 0));
        assertNotExclusive(expr(a, Operator.EQ, 0), expr(a, Operator.ANN, 1));

        // map elements
        assertNotExclusive(map("k", Operator.EQ, 0), map("k", Operator.NEQ, 0));
        assertNotExclusive(map("k", Operator.EQ, 0), map("k", Operator.EQ, 1));
        assertNotExclusive(expr(a, Operator.EQ, 0), map("k", Operator.NEQ, 0));

        // not comparable types
        assertNotExclusive(expr(counterColumn, Operator.EQ, 0L), expr(counterColumn, Operator.NEQ, 0L));
    }

    @Test
    public void testNothingToSimplify()
    {
        // no children or no expressions
        assertUnchanged(conjunction(exprs(expr(a, Operator.EQ, 0)), children()));
        assertUnchanged(conjunction(exprs(), children(disjunction(exprs(expr(a, Operator.EQ, 0), expr(b, Operator.EQ, 1)), children()))));
        assertUnchanged(FilterElement.REJECT_NONE);
        assertUnchanged(FilterElement.REJECT_ALL);

        // unrelated expressions
        assertUnchanged(conjunction(exprs(expr(a, Operator.EQ, 0)),
                                    children(disjunction(exprs(expr(b, Operator.EQ, 0), expr(c, Operator.EQ, 1)), children()))));
        assertUnchanged(disjunction(exprs(expr(a, Operator.EQ, 0)),
                                    children(conjunction(exprs(expr(b, Operator.EQ, 0), expr(c, Operator.EQ, 1)), children()))));

        // children of the same type are not simplified
        assertUnchanged(conjunction(exprs(expr(a, Operator.EQ, 0)),
                                    children(conjunction(exprs(expr(a, Operator.EQ, 0), expr(b, Operator.EQ, 1)), children()))));
        assertUnchanged(disjunction(exprs(expr(a, Operator.EQ, 0)),
                                    children(disjunction(exprs(expr(a, Operator.EQ, 0), expr(b, Operator.EQ, 1)), children()))));
    }

    @Test
    public void testAbsorption()
    {
        Expression x = expr(a, Operator.EQ, 0);
        Expression y = expr(b, Operator.EQ, 1);
        Expression z = expr(c, Operator.EQ, 2);

        // x OR (x AND y) -> x
        assertSimplified(disjunction(exprs(x), children(conjunction(exprs(x, y), children()))),
                         disjunction(exprs(x), children()));
        // x AND (x OR y) -> x
        assertSimplified(conjunction(exprs(x), children(disjunction(exprs(y, x), children()))),
                         conjunction(exprs(x), children()));

        // only the absorbed children are removed
        assertSimplified(conjunction(exprs(x), children(disjunction(exprs(x, y), children()),
                                                        disjunction(exprs(y, z), children()))),
                         conjunction(exprs(x), children(disjunction(exprs(y, z), children()))));

        // the absorbed child can have nested elements
        assertSimplified(disjunction(exprs(x), children(conjunction(exprs(x), children(disjunction(exprs(y, z), children()))))),
                         disjunction(exprs(x), children()));
    }

    @Test
    public void testAbsorptionIsNotAppliedToOrderingExpressions()
    {
        for (Operator operator : Arrays.asList(Operator.ORDER_BY_ASC, Operator.ORDER_BY_DESC, Operator.BM25, Operator.ANN))
        {
            Expression ordering = expr(a, operator, 0);
            Expression y = expr(b, Operator.EQ, 1);

            assertUnchanged(disjunction(exprs(ordering), children(conjunction(exprs(ordering, y), children()))));
            assertUnchanged(conjunction(exprs(ordering), children(disjunction(exprs(ordering, y), children()))));
        }
    }

    @Test
    public void testContradictionMovesTheRemainingExpression()
    {
        Expression x = expr(a, Operator.EQ, 0);
        Expression notX = expr(a, Operator.NEQ, 0);
        Expression y = expr(b, Operator.EQ, 1);

        // x AND (!x OR y) -> x AND y
        assertSimplified(conjunction(exprs(x), children(disjunction(exprs(notX, y), children()))),
                         conjunction(exprs(x, y), children()));

        // the other children are kept
        Expression z = expr(c, Operator.EQ, 2);
        FilterElement other = disjunction(exprs(y, z), children());
        assertSimplified(conjunction(exprs(x), children(disjunction(exprs(notX, y), children()), other)),
                         conjunction(exprs(x, y), children(other)));
    }

    @Test
    public void testContradictionKeepsSeveralRemainingExpressionsNested()
    {
        Expression x = expr(a, Operator.EQ, 0);
        Expression notX = expr(a, Operator.NEQ, 0);
        Expression y = expr(b, Operator.EQ, 1);
        Expression z = expr(c, Operator.EQ, 2);

        assertSimplified(conjunction(exprs(x), children(disjunction(exprs(notX, y, z), children()))),
                         conjunction(exprs(x), children(disjunction(exprs(y, z), children()))));
    }

    /// The remaining expression is not moved to the parent if the parent already has an expression on the same column,
    /// because that could produce combinations that CQL doesn't allow, such as multiple lower bounds.
    @Test
    public void testContradictionDoesNotMoveExpressionsToColumnsWithExpressions()
    {
        // a > 5 AND (a < 3 OR a > 7) -> a > 5 AND (a > 7)
        assertSimplified(conjunction(exprs(expr(a, Operator.GT, 5)),
                                     children(disjunction(exprs(expr(a, Operator.LT, 3), expr(a, Operator.GT, 7)), children()))),
                         conjunction(exprs(expr(a, Operator.GT, 5)),
                                     children(disjunction(exprs(expr(a, Operator.GT, 7)), children()))));

        // a != 0 AND (a = 0 OR a != 1) -> a != 0 AND (a != 1)
        assertSimplified(conjunction(exprs(expr(a, Operator.NEQ, 0)),
                                     children(disjunction(exprs(expr(a, Operator.EQ, 0), expr(a, Operator.NEQ, 1)), children()))),
                         conjunction(exprs(expr(a, Operator.NEQ, 0)),
                                     children(disjunction(exprs(expr(a, Operator.NEQ, 1)), children()))));

        // the column of any of the expressions of the parent is considered, not only the contradicting one
        assertSimplified(conjunction(exprs(expr(a, Operator.EQ, 0), expr(b, Operator.GT, 5)),
                                     children(disjunction(exprs(expr(a, Operator.NEQ, 0), expr(b, Operator.GT, 7)), children()))),
                         conjunction(exprs(expr(a, Operator.EQ, 0), expr(b, Operator.GT, 5)),
                                     children(disjunction(exprs(expr(b, Operator.GT, 7)), children()))));
    }

    @Test
    public void testContradictionWithNestedElements()
    {
        Expression x = expr(a, Operator.EQ, 0);
        Expression notX = expr(a, Operator.NEQ, 0);
        FilterElement nested = conjunction(exprs(expr(b, Operator.EQ, 1), expr(c, Operator.EQ, 1)), children());

        // the nested elements of the disjunction are not looked at, and they prevent moving the remaining expression
        assertSimplified(conjunction(exprs(x), children(disjunction(exprs(notX), children(nested)))),
                         conjunction(exprs(x), children(disjunction(exprs(), children(nested)))));
        assertSimplified(conjunction(exprs(x), children(disjunction(exprs(notX, expr(b, Operator.EQ, 0)), children(nested)))),
                         conjunction(exprs(x), children(disjunction(exprs(expr(b, Operator.EQ, 0)), children(nested)))));
    }

    @Test
    public void testContradictionOfAllTheExpressionsRejectsEverything()
    {
        Expression x = expr(a, Operator.EQ, 0);

        FilterElement result = RowFilterSimplifier.simplify(conjunction(exprs(x),
                                                                        children(disjunction(exprs(expr(a, Operator.NEQ, 0), expr(a, Operator.EQ, 1)), children()))),
                                                            table, IndexHints.NONE);
        assertSame(FilterElement.REJECT_ALL, result);

        // even if there are other children
        result = RowFilterSimplifier.simplify(conjunction(exprs(x),
                                                          children(disjunction(exprs(expr(b, Operator.EQ, 0), expr(c, Operator.EQ, 1)), children()),
                                                                   disjunction(exprs(expr(a, Operator.NEQ, 0)), children()))),
                                              table, IndexHints.NONE);
        assertSame(FilterElement.REJECT_ALL, result);
    }

    /// `a = 0 OR (a != 0 AND b = 1)` can't be replaced with `a = 0 OR b = 1`, because a null `a` satisfies neither
    /// `a = 0` nor `a != 0`, so the rows with a null `a` and `b = 1` would be wrongly included.
    @Test
    public void testTautologyIsNotAppliedToNullableColumns()
    {
        assertUnchanged(disjunction(exprs(expr(a, Operator.EQ, 0)),
                                    children(conjunction(exprs(expr(a, Operator.NEQ, 0), expr(b, Operator.EQ, 1)), children()))));
        assertUnchanged(disjunction(exprs(expr(a, Operator.EQ, 0)),
                                    children(conjunction(exprs(expr(a, Operator.EQ, 1), expr(b, Operator.EQ, 1)), children()))));
        assertNoTautology(expr(a, Operator.EQ, 0), expr(a, Operator.NEQ, 0));
        assertNoTautology(expr(a, Operator.LT, 1), expr(a, Operator.GTE, 1));
    }

    @Test
    public void testTautologyWithInequalities()
    {
        assertTautology(expr(ck, Operator.NEQ, 1), expr(ck, Operator.EQ, 1));
        assertTautology(expr(ck, Operator.NEQ, 1), expr(ck, Operator.NEQ, 2));
        assertTautology(expr(ck, Operator.NEQ, 1), expr(ck, Operator.GT, 0));
        assertTautology(expr(ck, Operator.NEQ, 1), expr(ck, Operator.LT, 2));
        assertTautology(expr(ck, Operator.NEQ, 1), expr(ck, Operator.GTE, 1));
        assertTautology(expr(ck, Operator.NEQ, 1), expr(ck, Operator.LTE, 1));

        assertNoTautology(expr(ck, Operator.NEQ, 1), expr(ck, Operator.NEQ, 1));
        assertNoTautology(expr(ck, Operator.NEQ, 1), expr(ck, Operator.EQ, 2));
        assertNoTautology(expr(ck, Operator.NEQ, 1), expr(ck, Operator.GT, 1));
        assertNoTautology(expr(ck, Operator.NEQ, 1), expr(ck, Operator.LT, 1));
    }

    @Test
    public void testTautologyWithSlices()
    {
        // overlapping
        assertTautology(expr(ck, Operator.LT, 2), expr(ck, Operator.GT, 1));
        assertTautology(expr(ck, Operator.LTE, 2), expr(ck, Operator.GTE, 1));
        assertTautology(expr(ck, Operator.LT, 5), expr(ck, Operator.GTE, 0));

        // touching at an included value
        assertTautology(expr(ck, Operator.LT, 1), expr(ck, Operator.GTE, 1));
        assertTautology(expr(ck, Operator.LTE, 1), expr(ck, Operator.GT, 1));
        assertTautology(expr(ck, Operator.LTE, 1), expr(ck, Operator.GTE, 1));

        // touching at an excluded value, or leaving a gap
        assertNoTautology(expr(ck, Operator.LT, 1), expr(ck, Operator.GT, 1));
        assertNoTautology(expr(ck, Operator.LT, 1), expr(ck, Operator.GT, 2));
        assertNoTautology(expr(ck, Operator.LTE, 1), expr(ck, Operator.GT, 2));

        // slices of the same direction never cover all the values
        assertNoTautology(expr(ck, Operator.GT, 0), expr(ck, Operator.GT, 1));
        assertNoTautology(expr(ck, Operator.LT, 0), expr(ck, Operator.LT, 1));

        // equalities don't cover all the values
        assertNoTautology(expr(ck, Operator.EQ, 1), expr(ck, Operator.GT, 1));
        assertNoTautology(expr(ck, Operator.EQ, 1), expr(ck, Operator.EQ, 2));
    }

    @Test
    public void testTautologyIgnoresOtherExpressions()
    {
        // different columns
        assertNoTautology(expr(ck, Operator.NEQ, 1), expr(a, Operator.EQ, 1));

        // other operators
        assertNoTautology(expr(ck, Operator.NEQ, 1), expr(ck, Operator.IN, 1));
        assertNoTautology(expr(ck, Operator.NEQ, 1), expr(ck, Operator.NOT_IN, 1));
    }

    @Test
    public void testTautologyRemovesTheExpression()
    {
        Expression x = expr(ck, Operator.EQ, 1);
        Expression notX = expr(ck, Operator.NEQ, 1);
        Expression y = expr(b, Operator.EQ, 1);
        Expression z = expr(c, Operator.EQ, 2);

        // x OR (!x AND y) -> x OR y
        assertSimplified(disjunction(exprs(x), children(conjunction(exprs(notX, y), children()))),
                         disjunction(exprs(x, y), children()));

        // x OR (!x AND y AND z) -> x OR (y AND z)
        assertSimplified(disjunction(exprs(x), children(conjunction(exprs(notX, y, z), children()))),
                         disjunction(exprs(x), children(conjunction(exprs(y, z), children()))));

        // the other children are kept
        FilterElement other = conjunction(exprs(y, z), children());
        assertSimplified(disjunction(exprs(x), children(conjunction(exprs(notX, y), children()), other)),
                         disjunction(exprs(x, y), children(other)));

        // the remaining expression is not moved to a column with other expressions in the parent
        assertSimplified(disjunction(exprs(expr(ck, Operator.LT, 2)),
                                     children(conjunction(exprs(expr(ck, Operator.GT, 1), expr(ck, Operator.GT, 3)), children()))),
                         disjunction(exprs(expr(ck, Operator.LT, 2)),
                                     children(conjunction(exprs(expr(ck, Operator.GT, 3)), children()))));
    }

    /// If all the expressions of the conjunction are covered by the parent, the conjunction is always true.
    /// It's not removed, because an empty filter would also return the partitions that only have a static row.
    @Test
    public void testTautologyOfAllTheExpressionsIsNotApplied()
    {
        Expression x = expr(ck, Operator.EQ, 1);

        assertUnchanged(disjunction(exprs(x),
                                    children(conjunction(exprs(expr(ck, Operator.NEQ, 1)), children()))));
        assertUnchanged(disjunction(exprs(x),
                                    children(conjunction(exprs(expr(a, Operator.EQ, 0), expr(b, Operator.EQ, 1)), children()),
                                             conjunction(exprs(expr(ck, Operator.NEQ, 1)), children()))));
    }

    /// A conjunction with nested elements is not empty when its expressions are removed.
    @Test
    public void testTautologyWithNestedElements()
    {
        Expression x = expr(ck, Operator.EQ, 1);
        FilterElement nested = disjunction(exprs(expr(b, Operator.EQ, 1), expr(c, Operator.EQ, 1)), children());

        assertSimplified(disjunction(exprs(x), children(conjunction(exprs(expr(ck, Operator.NEQ, 1)), children(nested)))),
                         disjunction(exprs(x), children(conjunction(exprs(), children(nested)))));
    }

    private static void assertTautology(Expression left, Expression right)
    {
        assertTrue(message(left, right), RowFilterSimplifier.isTautology(left, right, table, IndexHints.NONE));
        assertTrue(message(right, left), RowFilterSimplifier.isTautology(right, left, table, IndexHints.NONE));
    }

    private static void assertNoTautology(Expression left, Expression right)
    {
        assertFalse(message(left, right), RowFilterSimplifier.isTautology(left, right, table, IndexHints.NONE));
        assertFalse(message(right, left), RowFilterSimplifier.isTautology(right, left, table, IndexHints.NONE));
    }

    private static void assertExclusive(Expression left, Expression right)
    {
        assertTrue(message(left, right), RowFilterSimplifier.areMutuallyExclusive(left, right, table, IndexHints.NONE));
        assertTrue(message(right, left), RowFilterSimplifier.areMutuallyExclusive(right, left, table, IndexHints.NONE));
    }

    private static void assertNotExclusive(Expression left, Expression right)
    {
        assertFalse(message(left, right), RowFilterSimplifier.areMutuallyExclusive(left, right, table, IndexHints.NONE));
        assertFalse(message(right, left), RowFilterSimplifier.areMutuallyExclusive(right, left, table, IndexHints.NONE));
    }

    /// Describes the expressions without rendering their values, which are not always valid for their operators.
    private static String message(Expression left, Expression right)
    {
        return left.column().name + " " + left.operator() + " and " + right.column().name + " " + right.operator();
    }

    private static void assertUnchanged(FilterElement element)
    {
        assertSame(element, RowFilterSimplifier.simplify(element, table, IndexHints.NONE));
    }

    private static void assertSimplified(FilterElement element, FilterElement expected)
    {
        assertEquals(describe(expected), describe(RowFilterSimplifier.simplify(element, table, IndexHints.NONE)));
    }

    /// Describes the element, including the order of its expressions and children.
    private static String describe(FilterElement element)
    {
        StringBuilder builder = new StringBuilder(element.isDisjunction() ? "OR[" : "AND[");
        for (Expression expression : element.expressions())
            builder.append(expression).append(';');
        for (FilterElement child : element.children())
            builder.append(describe(child)).append(';');
        return builder.append(']').toString();
    }

    private static FilterElement conjunction(List<Expression> expressions, List<FilterElement> children)
    {
        return new FilterElement(false, expressions, children);
    }

    private static FilterElement disjunction(List<Expression> expressions, List<FilterElement> children)
    {
        return new FilterElement(true, expressions, children);
    }

    private static List<Expression> exprs(Expression... expressions)
    {
        return expressions.length == 0 ? Collections.emptyList() : Arrays.asList(expressions);
    }

    private static List<FilterElement> children(FilterElement... elements)
    {
        return elements.length == 0 ? Collections.emptyList() : Arrays.asList(elements);
    }

    private static Expression expr(ColumnMetadata column, Operator operator, int value)
    {
        return new RowFilter.SimpleExpression(column, operator, ByteBufferUtil.bytes(value), null, null);
    }

    private static Expression expr(ColumnMetadata column, Operator operator, long value)
    {
        return new RowFilter.SimpleExpression(column, operator, ByteBufferUtil.bytes(value), null, null);
    }

    private static Expression map(String key, Operator operator, int value)
    {
        return new RowFilter.MapComparisonExpression(mapColumn, ByteBufferUtil.bytes(key), operator, ByteBufferUtil.bytes(value));
    }
}
