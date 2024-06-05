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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import com.google.common.collect.Lists;
import org.junit.Test;

import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.index.sai.utils.LongIterator;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.RangeIterator;
import org.mockito.Mockito;

import static org.apache.cassandra.index.sai.plan.Plan.CostCoefficients.ANN_SCORED_KEY_COST;
import static org.apache.cassandra.index.sai.plan.Plan.CostCoefficients.ROW_COST;
import static org.apache.cassandra.index.sai.plan.Plan.CostCoefficients.SAI_KEY_COST;
import static org.junit.Assert.*;

public class PlanTest
{
    private final static RowFilter.Expression ordering = Mockito.mock(RowFilter.Expression.class);
    private final static RowFilter.Expression pred1 = filerPred("pred1", Operator.LT);
    private final static RowFilter.Expression pred2 = filerPred("pred2", Operator.LT);
    private final static RowFilter.Expression pred3 = filerPred("pred3", Operator.LT);
    private final static RowFilter.Expression pred4 = filerPred("pred4", Operator.LT);

    private final static Expression saiPred1 = saiPred("pred1", Expression.Op.RANGE);
    private final static  Expression saiPred2 = saiPred("pred2", Expression.Op.RANGE);
    private final static  Expression saiPred3 = saiPred("pred3", Expression.Op.RANGE);
    private final static  Expression saiPred4 = saiPred("pred4", Expression.Op.RANGE);
    
    private final static RowFilter rowFilter1 = RowFilter.builder().add(pred1).build();
    private final static RowFilter rowFilter12 = RowFilter.builder().add(pred1).add(pred2).build();
    private final static RowFilter rowFilter123 = RowFilter.builder().add(pred1).add(pred2).add(pred3).build();

    private final Plan.TableMetrics table1M = new Plan.TableMetrics(1_000_000, 7, 128, 3);
    private final Plan.TableMetrics table10M = new Plan.TableMetrics(10_000_000, 7, 128, 8);
    
    private final Plan.Factory factory = new Plan.Factory(table1M);


    private static Expression saiPred(String column, Expression.Op operation)
    {
        Expression pred = Mockito.mock(Expression.class);
        Mockito.when(pred.toString()).thenReturn(operation.toString() + '(' + column + ')');
        Mockito.when(pred.getIndexName()).thenReturn(column + "_idx");
        Mockito.when(pred.getOp()).thenReturn(operation);
        return pred;
    }

    private static RowFilter.Expression filerPred(String column, Operator operation)
    {
        RowFilter.Expression pred = Mockito.mock(RowFilter.Expression.class);
        Mockito.when(pred.toString()).thenReturn(column + ' ' + operation + " X");
        Mockito.when(pred.operator()).thenReturn(operation);
        return pred;
    }

    @Test
    public void empty()
    {
        Plan.KeysIteration plan = factory.numericIndexScan(saiPred1, 0);
        assertTrue(plan instanceof Plan.NumericIndexScan);
        assertEquals(0.0, plan.expectedKeys(), 0.01);
        assertEquals(0.0, plan.selectivity(), 0.01);
        assertEquals(0.0, plan.costPerKey(), 0.01);
    }

    @Test
    public void single()
    {
        Plan.KeysIteration plan = factory.numericIndexScan(saiPred1, (long)(0.5 * factory.tableMetrics.rows));
        assertTrue(plan instanceof Plan.NumericIndexScan);
        assertEquals(0.5 * factory.tableMetrics.rows, plan.expectedKeys(), 0.01);
        assertEquals(0.5, plan.selectivity(), 0.01);
        assertEquals(SAI_KEY_COST, plan.costPerKey(), 0.01);
    }

    @Test
    public void intersection()
    {
        Plan.KeysIteration a1 = factory.numericIndexScan(saiPred1, (long)(0.2 * factory.tableMetrics.rows));
        Plan.KeysIteration a2 = factory.numericIndexScan(saiPred2, (long)(0.5 * factory.tableMetrics.rows));
        Plan.KeysIteration plan1 = factory.intersection(Lists.newArrayList(a1, a2));

        assertTrue(plan1 instanceof Plan.Intersection);
        assertEquals(0.1, plan1.selectivity(), 0.01);
        assertTrue(plan1.costPerKey() > a1.costPerKey());
        assertTrue(plan1.costPerKey() > a2.costPerKey());

        Plan.KeysIteration b1 = factory.numericIndexScan(saiPred1, (long)(0.01 * factory.tableMetrics.rows));
        Plan.KeysIteration b2 = factory.numericIndexScan(saiPred2, (long)(0.01 * factory.tableMetrics.rows));
        Plan.KeysIteration plan2 = factory.intersection(Lists.newArrayList(b1, b2));

        assertTrue(plan2 instanceof Plan.Intersection);
        assertEquals(0.0001, plan2.selectivity(), 1e-9);
        assertEquals(0.0001 * factory.tableMetrics.rows, plan2.expectedKeys(), 1e-9);
        assertTrue(plan2.costPerKey() > plan1.costPerKey());
    }

    @Test
    public void intersectionWithEmpty()
    {
        Plan.KeysIteration a1 = factory.numericIndexScan(saiPred1, 0);
        Plan.KeysIteration a2 = factory.numericIndexScan(saiPred2, (long)(0.01 * factory.tableMetrics.rows));
        Plan.KeysIteration plan = factory.intersection(Lists.newArrayList(a1, a2));
        assertEquals(0.0, plan.selectivity(), 0.0001);
        assertEquals(0.0, plan.expectedKeys(), 0.0001);
        assertTrue(plan.fullCost() <= SAI_KEY_COST * 2);
    }

    @Test
    public void intersectionWithNothing()
    {
        Plan.KeysIteration a1 = factory.nothing;
        Plan.KeysIteration a2 = factory.numericIndexScan(saiPred2, (long)(0.01 * factory.tableMetrics.rows));
        Plan.KeysIteration plan = factory.intersection(Lists.newArrayList(a1, a2));
        assertEquals(0.0, plan.selectivity(), 0.0001);
        assertEquals(0.0, plan.expectedKeys(), 0.0001);
        assertTrue(plan.fullCost() <= SAI_KEY_COST * 2);
    }

    @Test
    public void intersectionWithEverything()
    {
        Plan.KeysIteration a1 = factory.everything;
        Plan.KeysIteration a2 = factory.numericIndexScan(saiPred2, (long)(0.01 * factory.tableMetrics.rows));
        Plan.KeysIteration plan = factory.intersection(Lists.newArrayList(a1, a2));
        assertSame(a2, plan);

        assertSame(factory.everything, factory.intersection(Lists.newArrayList()));
        assertSame(factory.everything, factory.intersection(Lists.newArrayList(factory.everything)));
        assertSame(factory.everything, factory.intersection(Lists.newArrayList(factory.everything, factory.everything)));
    }

    @Test
    public void intersectionWithUnion()
    {
        Plan.KeysIteration s1 = factory.numericIndexScan(saiPred1, (long) (0.01 * factory.tableMetrics.rows));
        Plan.KeysIteration s2 = factory.numericIndexScan(saiPred2, (long) (0.01 * factory.tableMetrics.rows));
        Plan.KeysIteration s3 = factory.numericIndexScan(saiPred3, (long) (0.01 * factory.tableMetrics.rows));
        Plan.KeysIteration u = factory.union(Lists.newArrayList(s2, s3));
        Plan.KeysIteration i1 = factory.intersection(Lists.newArrayList(s1, u));
        Plan.KeysIteration i2 = factory.intersection(Lists.newArrayList(s1, s2));
        assertTrue(i1.initCost() > i2.initCost());
        assertTrue(i1.fullCost() > i2.fullCost());
    }

    @Test
    public void nestedIntersections()
    {
        Plan.KeysIteration s1 = factory.numericIndexScan(saiPred1, (long) (0.01 * factory.tableMetrics.rows));
        Plan.KeysIteration s2 = factory.numericIndexScan(saiPred2, (long) (0.01 * factory.tableMetrics.rows));
        Plan.KeysIteration s3 = factory.numericIndexScan(saiPred3, (long) (0.01 * factory.tableMetrics.rows));
        Plan.KeysIteration nested = factory.intersection(Lists.newArrayList(s2, s3));
        Plan.KeysIteration i1 = factory.intersection(Lists.newArrayList(s1, nested));
        Plan.KeysIteration i2 = factory.intersection(Lists.newArrayList(s1, s2, s3));
        assertEquals(i1.initCost(), i2.initCost(), 0.01);
        assertTrue(i1.fullCost() > i2.fullCost());
    }

    @Test
    public void rangeScanVsPointLookupIntersection()
    {
        // Intersecting range scans is

        Plan.KeysIteration n1 = factory.numericIndexScan(saiPred("a", Expression.Op.RANGE),
                                                         (long)(0.1 * factory.tableMetrics.rows));
        Plan.KeysIteration n2 = factory.numericIndexScan(saiPred("b", Expression.Op.RANGE),
                                                         (long)(0.1 * factory.tableMetrics.rows));
        Plan.KeysIteration ni = factory.intersection(Lists.newArrayList(n1, n2));

        Plan.KeysIteration l1 = factory.literalIndexScan(saiPred("c", Expression.Op.EQ),
                                                         (long)(0.1 * factory.tableMetrics.rows));
        Plan.KeysIteration l2 = factory.literalIndexScan(saiPred("d", Expression.Op.EQ),
                                                         (long)(0.1 * factory.tableMetrics.rows));
        Plan.KeysIteration li = factory.intersection(Lists.newArrayList(l1, l2));

        assertEquals(li.expectedKeys(), ni.expectedKeys(), 0.01);
        assertTrue(li.fullCost() < ni.fullCost());
    }

    @Test
    public void intersectThree()
    {
        Plan.KeysIteration s1 = factory.numericIndexScan(saiPred1, (long) (0.5 * factory.tableMetrics.rows));
        Plan.KeysIteration s2 = factory.numericIndexScan(saiPred2, (long) (0.8 * factory.tableMetrics.rows));
        Plan.KeysIteration s3 = factory.numericIndexScan(saiPred3, (long) (0.8 * factory.tableMetrics.rows));
        Plan.KeysIteration intersect2 = factory.intersection(Lists.newArrayList(s1, s2));
        Plan.KeysIteration intersect3 = factory.intersection(Lists.newArrayList(s1, s2, s3));

        assertTrue(intersect3 instanceof Plan.Intersection);
        assertEquals(0.32, intersect3.selectivity(), 0.01);
        assertEquals(0.32 * factory.tableMetrics.rows, intersect3.expectedKeys(), 0.01);
        assertTrue(intersect3.fullCost() > intersect2.fullCost());
    }

    @Test
    public void union()
    {
        Plan.KeysIteration s1 = factory.numericIndexScan(saiPred1, (long) (0.5 * factory.tableMetrics.rows));
        Plan.KeysIteration s2 = factory.numericIndexScan(saiPred2, (long) (0.5 * factory.tableMetrics.rows));
        Plan.KeysIteration plan = factory.union(Lists.newArrayList(s1, s2));

        assertTrue(plan instanceof Plan.Union);
        assertEquals(0.75, plan.selectivity(), 0.01);
        assertEquals(0.75 * factory.tableMetrics.rows, plan.expectedKeys(), 0.01);
        assertEquals( 1.333333333 * (SAI_KEY_COST), plan.costPerKey(), 0.01);
        assertTrue(plan.fullCost() >= s1.fullCost() + s2.fullCost());
    }

    @Test
    public void unionWithNoting()
    {
        Plan.KeysIteration s1 = factory.nothing;
        Plan.KeysIteration s2 = factory.numericIndexScan(saiPred2, (long) (0.5 * factory.tableMetrics.rows));
        Plan.KeysIteration plan = factory.union(Lists.newArrayList(s1, s2));

        assertSame(s2, plan);
        assertSame(factory.nothing, factory.union(Collections.emptyList()));
    }

    @Test
    public void unionWithEverything()
    {
        Plan.KeysIteration s1 = factory.everything;
        Plan.KeysIteration s2 = factory.numericIndexScan(saiPred2, (long) (0.5 * factory.tableMetrics.rows));
        Plan.KeysIteration plan = factory.union(Lists.newArrayList(s1, s2));

        assertSame(factory.everything, plan);
    }

    @Test
    public void fetch()
    {
        Plan.KeysIteration i = factory.numericIndexScan(saiPred1, (long) (0.5 * factory.tableMetrics.rows));
        Plan.RowsIteration s = factory.fetch(i);
        assertEquals(0.5 * factory.tableMetrics.rows, s.expectedRows(), 0.01);
        assertTrue(s.fullCost() > 0.5 * factory.tableMetrics.rows * (SAI_KEY_COST + ROW_COST));
    }

    @Test
    public void limit()
    {
        Plan.KeysIteration i = factory.numericIndexScan(saiPred1, (long) (0.5 * factory.tableMetrics.rows));
        Plan.RowsIteration s = factory.fetch(i);
        Plan.RowsIteration l = factory.limit(s, 10);
        assertEquals(10, l.expectedRows(), 0.01);
        assertTrue(l.fullCost() < s.fullCost());
    }

    @Test
    public void filter()
    {
        Plan.KeysIteration i = factory.numericIndexScan(saiPred1, (long) (0.5 * factory.tableMetrics.rows));
        Plan.RowsIteration s = factory.fetch(i);
        Plan.RowsIteration f = factory.filter(RowFilter.builder().add(pred1).build(), s, 0.25);
        assertEquals(0.25 * factory.tableMetrics.rows, f.expectedRows(), 0.01);
        assertEquals(0.25, f.selectivity(), 0.01);
        assertTrue(f.costPerRow() > s.costPerRow());
    }

    @Test
    public void filterAndLimit()
    {
        Plan.KeysIteration i = factory.numericIndexScan(saiPred1, (long) (0.5 * factory.tableMetrics.rows));
        Plan.RowsIteration s = factory.fetch(i);
        Plan.RowsIteration f = factory.filter(RowFilter.builder().add(pred1).build(), s, 0.25);
        Plan.RowsIteration l = factory.limit(f, 10);
        assertEquals(10, l.expectedRows(), 0.01);
        assertEquals(l.costPerRow(), f.costPerRow(), 0.01);
    }

    @Test
    public void annSort()
    {
        Plan.KeysIteration i = factory.numericIndexScan(saiPred1, (long) (0.5 * factory.tableMetrics.rows));
        Plan.KeysIteration s = factory.annSort(i, ordering);

        assertEquals(0.5 * factory.tableMetrics.rows, s.expectedKeys(), 0.01);
        assertTrue(s.initCost() >= i.fullCost());
    }

    @Test
    public void annScan()
    {
        Plan.KeysIteration i = factory.annScan(ordering);
        assertEquals(factory.tableMetrics.rows, i.expectedKeys(), 0.01);
        assertEquals(0.0, i.initCost(), 0.01);
        assertEquals(factory.tableMetrics.rows * ANN_SCORED_KEY_COST, i.fullCost(), 0.01);
    }

    @Test
    public void findNodeByType()
    {
        Plan.KeysIteration s1 = factory.numericIndexScan(saiPred1, (long) (0.5 * factory.tableMetrics.rows));
        Plan.KeysIteration s2 = factory.numericIndexScan(saiPred2, (long) (0.2 * factory.tableMetrics.rows));
        Plan.KeysIteration s3 = factory.numericIndexScan(saiPred3, (long) (0.1 * factory.tableMetrics.rows));
        RowFilter rowFilter = RowFilter.builder().add(pred1).add(pred2).add(pred3).build();

        Plan.KeysIteration union = factory.union(Lists.newArrayList(factory.intersection(Lists.newArrayList(s1, s2)), s3));
        Plan.KeysIteration sort = factory.annSort(union, ordering);
        Plan.RowsIteration fetch = factory.fetch(sort);
        Plan.RowsIteration filter = factory.recheckFilter(rowFilter, fetch);
        Plan.RowsIteration limit = factory.limit(filter, 3);

        assertEquals(List.of(s2, s1, s3), limit.nodesOfType(Plan.NumericIndexScan.class));
        assertEquals(List.of(union), limit.nodesOfType(Plan.Union.class));
        assertEquals(List.of(fetch), limit.nodesOfType(Plan.Fetch.class));
        assertEquals(List.of(filter), limit.nodesOfType(Plan.Filter.class));
        assertEquals(List.of(limit), limit.nodesOfType(Plan.Limit.class));
    }


    @Test
    public void removeSubplan()
    {
        Plan.KeysIteration s1 = factory.numericIndexScan(saiPred1, 20);
        Plan.KeysIteration s2 = factory.numericIndexScan(saiPred2, 30);
        Plan.KeysIteration s3 = factory.numericIndexScan(saiPred3, 50);
        Plan.KeysIteration plan1 = factory.intersection(Lists.newArrayList(s1, s2, s3));
        Plan plan2 = plan1.remove(s2.id);

        assertNotSame(plan1, plan2);
        assertEquals(plan1.id, plan2.id);  // although the result plan is different object, the nodes must retain their ids
        assertTrue(plan2 instanceof Plan.Intersection);
        assertEquals(Lists.newArrayList(s1, s3), plan2.subplans());
        assertNotSame(plan1.cost(), plan2.cost());

        Plan plan3 = plan2.remove(s1.id);
        assertEquals(s3, plan3);

        Plan plan4 = plan3.remove(s3.id);
        assertTrue(plan4 instanceof Plan.Everything);
    }

    @Test
    public void removeNestedSubplan()
    {
        Plan.KeysIteration s1 = factory.numericIndexScan(saiPred1, 50);
        Plan.KeysIteration s2 = factory.numericIndexScan(saiPred2, 30);
        Plan.KeysIteration s3 = factory.numericIndexScan(saiPred3, 80);
        Plan.KeysIteration s4 = factory.numericIndexScan(saiPred4, 50);

        Plan.KeysIteration sub1 = factory.intersection(Lists.newArrayList(s1, s2));
        Plan.KeysIteration sub2 = factory.intersection(Lists.newArrayList(s3, s4));
        Plan.KeysIteration plan1 = factory.union(Lists.newArrayList(sub1, sub2));
        Plan plan2 = plan1.remove(s2.id).remove(s3.id);

        Plan reference = factory.union(Lists.newArrayList(s1, s4));

        assertNotSame(plan1, plan2);
        assertEquals(reference.cost(), plan2.cost());
        assertTrue(plan2 instanceof Plan.Union);
    }

    @Test
    public void intersectionClauseLimit()
    {
        Plan.KeysIteration s1 = factory.numericIndexScan(saiPred1, 3);
        Plan.KeysIteration s2 = factory.numericIndexScan(saiPred2, 4);
        Plan.KeysIteration s3 = factory.literalIndexScan(saiPred3, 1);
        Plan.KeysIteration s4 = factory.literalIndexScan(saiPred4, 2);
        Plan.KeysIteration intersect = factory.intersection(Lists.newArrayList(s1, s2, s3, s4));
        Plan.RowsIteration plan = factory.limit(factory.fetch(intersect), 3);

        Plan plan4 = plan.limitIntersectedClauses(4);
        assertSame(plan, plan4);

        Plan plan3 = plan.limitIntersectedClauses(3);
        Plan.Intersection intersection3 = plan3.firstNodeOfType(Plan.Intersection.class);
        assertNotNull(intersection3);
        assertEquals(List.of(s3, s4, s1), intersection3.subplans());

        Plan plan2 = plan.limitIntersectedClauses(2);
        Plan.Intersection intersection2 = plan2.firstNodeOfType(Plan.Intersection.class);
        assertNotNull(intersection2);
        assertEquals(List.of(s3, s4), intersection2.subplans());

        Plan plan1 = plan.limitIntersectedClauses(1);
        Plan.Fetch fetch = plan1.firstNodeOfType(Plan.Fetch.class);
        assertNotNull(fetch);
        assertSame(s3, fetch.subplans().get(0));
    }

    @Test
    public void rangeIterator()
    {
        Plan.KeysIteration s1 = factory.numericIndexScan(saiPred1, 3);
        Plan.KeysIteration s2 = factory.numericIndexScan(saiPred2, 3);
        Plan.KeysIteration s3 = factory.literalIndexScan(saiPred3, 1);
        Plan.KeysIteration plan = factory.union(Lists.newArrayList(factory.intersection(Lists.newArrayList(s1, s2)), s3));

        Map<Expression, RangeIterator> iterators = new HashMap<>();
        iterators.put(saiPred1, new LongIterator(new long[] { 1L, 2L, 3L }));
        iterators.put(saiPred2, new LongIterator(new long[] { 1L, 2L, 5L }));
        iterators.put(saiPred3, new LongIterator(new long[] { 100L }));

        Plan.Executor executor = new Plan.Executor()
        {
            @Override
            public Iterator<? extends PrimaryKey> getKeysFromIndex(Expression predicate)
            {
                return iterators.get(predicate);
            }

            @Override
            public Iterator<? extends PrimaryKey> getTopKRows(RowFilter.Expression ordering)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public Iterator<? extends PrimaryKey> getTopKRows(RangeIterator keys, RowFilter.Expression ordering)
            {
                throw new UnsupportedOperationException();
            }
        };

        RangeIterator iterator = (RangeIterator) plan.execute(executor);
        assertEquals(LongIterator.convert(1L, 2L, 100L), LongIterator.convert(iterator));
    }

    @Test
    public void builder()
    {
        Plan plan1 = factory.intersectionBuilder()
                            .add(factory.numericIndexScan(saiPred1, 50))
                            .add(factory.numericIndexScan(saiPred2, 50))
                            .build();
        assertTrue(plan1 instanceof Plan.Intersection);
        assertEquals(2, plan1.subplans().size());

        Plan plan2 = factory.unionBuilder()
                            .add(factory.numericIndexScan(saiPred3, 50))
                            .add(factory.numericIndexScan(saiPred4, 50))
                            .build();
        assertTrue(plan2 instanceof Plan.Union);
        assertEquals(2, plan2.subplans().size());
    }

    @Test
    public void prettyPrint()
    {
        Locale.setDefault(Locale.ENGLISH);
        Plan.KeysIteration s1 = factory.numericIndexScan(saiPred1, (long) (0.5 * factory.tableMetrics.rows));
        Plan.KeysIteration s2 = factory.numericIndexScan(saiPred2, (long) (0.002 * factory.tableMetrics.rows));
        Plan.KeysIteration s3 = factory.literalIndexScan(saiPred3, (long) (0.001 * factory.tableMetrics.rows));
        Plan.KeysIteration union = factory.union(Lists.newArrayList(factory.intersection(Lists.newArrayList(s1, s2)), s3));
        Plan.KeysIteration sort = factory.annSort(union, ordering);
        Plan.RowsIteration fetch = factory.fetch(sort);
        Plan.RowsIteration filter = factory.recheckFilter(RowFilter.builder().add(pred1).add(pred2).add(pred3).build(), fetch);
        Plan.RowsIteration limit = factory.limit(filter, 3);

        String prettyStr = limit.toStringRecursive();

        assertEquals("Limit (sel: 0.000003000, rows: 3.0, cost/row: 213.4, cost: 61099.3..61739.7)\n" +
                     " └─ Filter pred1 < X AND pred2 < X AND pred3 < X (sel: 0.001999000, rows: 1999.0, cost/row: 213.4, cost: 61099.3..487765.9)\n" +
                     "     └─ Fetch (sel: 0.001999000, rows: 1999.0, cost/row: 213.4, cost: 61099.3..487765.9)\n" +
                     "         └─ AnnSort (sel: 0.001999000, keys: 1999.0, cost/key: 10.0, cost: 61099.3..81089.3)\n" +
                     "             └─ Union (sel: 0.001999000, keys: 1999.0, cost/key: 25.6, cost: 3.0..51104.3)\n" +
                     "                 ├─ Intersection (sel: 0.001000000, keys: 1000.0, cost/key: 50.1, cost: 2.0..50103.3)\n" +
                     "                 │   ├─ NumericIndexScan of pred2_idx using RANGE(pred2) (sel: 0.002000000, keys: 2000.0, cost/key: 1.0, cost: 1.0..2001.0)\n" +
                     "                 │   └─ NumericIndexScan of pred1_idx using RANGE(pred1) (sel: 0.500000000, keys: 500000.0, cost/key: 1.0, cost: 1.0..500001.0)\n" +
                     "                 └─ LiteralIndexScan of pred3_idx using RANGE(pred3) (sel: 0.001000000, keys: 1000.0, cost/key: 1.0, cost: 1.0..1001.0)\n", prettyStr);
    }

    @Test
    public void removeNeedlessIntersections()
    {
        // If one of the intersection branches has bad selectivity (here 90%), then performing the intersection
        // makes no sense, because it will cause more effort to perform the intersection than to fetch the additional
        // rows that weren't filtered out
        Plan.KeysIteration s1 = factory.numericIndexScan(saiPred1, (long) (0.99 * factory.tableMetrics.rows));
        Plan.KeysIteration s2 = factory.numericIndexScan(saiPred2, (long) (0.001 * factory.tableMetrics.rows));
        Plan.KeysIteration s3 = factory.numericIndexScan(saiPred3, (long) (0.95 * factory.tableMetrics.rows));
        Plan.KeysIteration s4 = factory.numericIndexScan(saiPred4, (long) (0.97 * factory.tableMetrics.rows));
        Plan.KeysIteration intersect = factory.intersection(Lists.newArrayList(s1, s2, s3, s4));
        Plan.RowsIteration origPlan = factory.fetch(intersect);

        Plan optimizedPlan = origPlan.optimize();
        assertEquals(List.of(s2, s3, s4, s1), intersect.subplans()); // subplans must be sorted by selectivity
        assertEquals(List.of(s2), optimizedPlan.subplans()); // look ma, no intersection under the fetch node
    }

    @Test
    public void optimizeIntersectionWithEmpty()
    {
        Plan.KeysIteration a1 = factory.numericIndexScan(saiPred1, 0);
        Plan.KeysIteration a2 = factory.numericIndexScan(saiPred2, (long)(0.01 * factory.tableMetrics.rows));
        Plan.KeysIteration plan = factory.intersection(Lists.newArrayList(a1, a2));
        Plan optimized = plan.optimize();
        assertSame(optimized, a1);
    }

    @Test
    public void leaveGoodIntersection()
    {
        // If both intersection selectivities are good, then the intersection shouldn't be removed at all
        Plan.KeysIteration s1 = factory.numericIndexScan(saiPred1, (long) (0.0001 * factory.tableMetrics.rows));
        Plan.KeysIteration s2 = factory.numericIndexScan(saiPred2, (long) (0.2 * factory.tableMetrics.rows));
        Plan.KeysIteration intersect = factory.intersection(Lists.newArrayList(s1, s2));
        Plan.RowsIteration origPlan = factory.fetch(intersect);

        Plan optimizedPlan = origPlan.optimize();
        assertSame(origPlan, optimizedPlan);
    }

    @Test
    public void removeNeedlessIntersectionUnderFilter()
    {
        Plan.KeysIteration s1 = factory.numericIndexScan(saiPred1, (long) (0.0001 * factory.tableMetrics.rows));
        Plan.KeysIteration s2 = factory.numericIndexScan(saiPred2, (long) (0.9 * factory.tableMetrics.rows));
        Plan.KeysIteration intersect = factory.intersection(Lists.newArrayList(s1, s2));
        Plan.RowsIteration fetch = factory.fetch(intersect);
        RowFilter rowFilter = RowFilter.builder().add(pred1).add(pred2).build();
        Plan.RowsIteration origPlan = factory.recheckFilter(rowFilter, fetch);

        Plan.RowsIteration optimizedPlan = (Plan.RowsIteration) origPlan.optimize();
        assertFalse(optimizedPlan.contains(p -> p instanceof Plan.Intersection));
        assertEquals(origPlan.cost().expectedRows, optimizedPlan.cost().expectedRows, 0.001);
    }

    @Test
    public void leaveGoodIntersectionUnderFilter()
    {
        Plan.KeysIteration s1 = factory.numericIndexScan(saiPred1, (long) (0.1 * factory.tableMetrics.rows));
        Plan.KeysIteration s2 = factory.numericIndexScan(saiPred2, (long) (0.5 * factory.tableMetrics.rows));
        Plan.KeysIteration intersect = factory.intersection(Lists.newArrayList(s1, s2));
        Plan.RowsIteration fetch = factory.fetch(intersect);
        RowFilter rowFilter = RowFilter.builder().add(pred1).add(pred2).build();
        Plan.RowsIteration origPlan = factory.recheckFilter(rowFilter, fetch);

        Plan optimizedPlan = origPlan.optimize();
        assertSame(origPlan, optimizedPlan);
    }

    @Test
    public void replaceAnnSortWithAnnScan()
    {
        // This is a simulation of a typical hybrid vector query
        // SELECT * FROM ... WHERE matches_lot_of_rows ORDER BY v ANN OF ... LIMIT n;
        // If the predicate matches a significant portion of the data and n is small,
        // then we should switch to scanning the ANN index only and post-filtering.
        // This allows us to perform such query lazily and the cost should be proportional to n.
        // Important: this requires hight number of rows in the table, so that the cost of fetching all keys from the index
        // is significantly larger than the cost of fetching a few result rows from storage.
        Plan.KeysIteration indexScan = factory.numericIndexScan(saiPred1, (long) (0.1 * factory.tableMetrics.rows));
        Plan.KeysIteration sort = factory.annSort(indexScan, ordering);
        Plan.RowsIteration fetch = factory.fetch(sort);
        Plan.RowsIteration postFilter = factory.recheckFilter(rowFilter1, fetch);
        Plan.RowsIteration origPlan = factory.limit(postFilter, 3);

        Plan optimizedPlan = origPlan.optimize();
        assertTrue(optimizedPlan.contains(p -> p instanceof Plan.AnnScan));
        assertEquals(0.0, optimizedPlan.cost().initCost(), 0.0);

        // The optimized plan should finish before the original plan even gets the first row out ;)
        assertTrue(optimizedPlan.cost().fullCost() < origPlan.cost().initCost());
    }

    @Test
    public void replaceIntersectionAndAnnSortWithAnnScan()
    {
        // Similar like the previous test, but now with an intersection:
        Plan.KeysIteration indexScan1 = factory.numericIndexScan(saiPred1, (long) (0.1 * factory.tableMetrics.rows));
        Plan.KeysIteration indexScan2 = factory.numericIndexScan(saiPred2, (long) (0.2 * factory.tableMetrics.rows));
        Plan.KeysIteration intersection = factory.intersection(Lists.newArrayList(indexScan1, indexScan2));
        Plan.KeysIteration sort = factory.annSort(intersection, ordering);
        Plan.RowsIteration fetch = factory.fetch(sort);
        Plan.RowsIteration postFilter = factory.recheckFilter(rowFilter12, fetch);
        Plan.RowsIteration origPlan = factory.limit(postFilter, 3);

        Plan optimizedPlan = origPlan.optimize();
        assertTrue(optimizedPlan.contains(p -> p instanceof Plan.AnnScan));
        assertEquals(0.0, optimizedPlan.cost().initCost(), 0.0);

        // The optimized plan should finish before the original plan even gets the first row out ;)
        assertTrue(optimizedPlan.cost().fullCost() < origPlan.cost().initCost());
    }

    @Test
    public void removeIntersectionBelowAnnSort()
    {
        Plan.KeysIteration indexScan1 = factory.numericIndexScan(saiPred1, (long) (0.001 * factory.tableMetrics.rows));
        Plan.KeysIteration indexScan2 = factory.numericIndexScan(saiPred2, (long) (0.9 * factory.tableMetrics.rows));
        Plan.KeysIteration intersection = factory.intersection(Lists.newArrayList(indexScan1, indexScan2));
        Plan.KeysIteration sort = factory.annSort(intersection, ordering);
        Plan.RowsIteration fetch = factory.fetch(sort);
        Plan.RowsIteration postFilter = factory.recheckFilter(rowFilter12, fetch);
        Plan.RowsIteration origPlan = factory.limit(postFilter, 3);

        Plan optimizedPlan = origPlan.optimize();
        assertFalse(optimizedPlan.contains(p -> p instanceof Plan.Intersection));  // no intersection
        assertFalse(optimizedPlan.contains(p -> p instanceof Plan.AnnScan));    // no direct ANN index scan
        assertTrue(optimizedPlan.contains(p -> p instanceof Plan.AnnSort));
        assertTrue(optimizedPlan.contains(p -> p instanceof Plan.NumericIndexScan));
    }

    @Test
    public void reduceNumberOfIntersectionsBelowAnnSort()
    {
        Plan.KeysIteration indexScan1 = factory.numericIndexScan(saiPred1, (long) (0.01 * factory.tableMetrics.rows));
        Plan.KeysIteration indexScan2 = factory.numericIndexScan(saiPred2, (long) (0.01 * factory.tableMetrics.rows));
        Plan.KeysIteration indexScan3 = factory.numericIndexScan(saiPred3, (long) (0.5 * factory.tableMetrics.rows));
        Plan.KeysIteration intersection = factory.intersection(Lists.newArrayList(indexScan1, indexScan2, indexScan3));
        Plan.KeysIteration sort = factory.annSort(intersection, ordering);
        Plan.RowsIteration fetch = factory.fetch(sort);
        Plan.RowsIteration postFilter = factory.recheckFilter(rowFilter123, fetch);
        Plan.RowsIteration origPlan = factory.limit(postFilter, 3);

        Plan optimizedPlan = origPlan.optimize();

        Plan.Intersection optimizedIntersection = optimizedPlan.firstNodeOfType(Plan.Intersection.class);
        assertNotNull(optimizedIntersection);
        assertEquals(List.of(indexScan1, indexScan2), optimizedIntersection.subplans());
    }

    @Test
    public void leaveThreeIntersectionsBelowAnnSort()
    {
        Plan.KeysIteration indexScan1 = factory.numericIndexScan(saiPred1, (long) (0.01 * factory.tableMetrics.rows));
        Plan.KeysIteration indexScan2 = factory.numericIndexScan(saiPred2, (long) (0.01 * factory.tableMetrics.rows));
        Plan.KeysIteration indexScan3 = factory.numericIndexScan(saiPred3, (long) (0.01 * factory.tableMetrics.rows));
        Plan.KeysIteration intersection = factory.intersection(Lists.newArrayList(indexScan1, indexScan2, indexScan3));
        Plan.KeysIteration sort = factory.annSort(intersection, ordering);
        Plan.RowsIteration fetch = factory.fetch(sort);
        Plan.RowsIteration postFilter = factory.recheckFilter(rowFilter123, fetch);
        Plan.RowsIteration origPlan = factory.limit(postFilter, 3);

        Plan optimizedPlan = origPlan.optimize();
        Plan.Intersection optimizedIntersection = optimizedPlan.firstNodeOfType(Plan.Intersection.class);
        assertNotNull(optimizedIntersection);
        assertEquals(List.of(indexScan1, indexScan2, indexScan3), optimizedIntersection.subplans());
    }

    @Test
    public void leaveIntersectionsBelowAnnSort()
    {
        Plan.KeysIteration indexScan1 = factory.numericIndexScan(saiPred1, (long) (0.001 * factory.tableMetrics.rows));
        Plan.KeysIteration indexScan2 = factory.numericIndexScan(saiPred2, (long) (0.01 * factory.tableMetrics.rows));
        Plan.KeysIteration intersection = factory.intersection(Lists.newArrayList(indexScan1, indexScan2));
        Plan.KeysIteration sort = factory.annSort(intersection, ordering);
        Plan.RowsIteration fetch = factory.fetch(sort);
        Plan.RowsIteration postFilter = factory.recheckFilter(rowFilter12, fetch);
        Plan.RowsIteration origPlan = factory.limit(postFilter, 3);

        Plan optimizedPlan = origPlan.optimize();
        assertSame(origPlan, optimizedPlan);
    }

    @Test
    public void optimizeIntersectionsUnderLimit()
    {
        testIntersectionsUnderLimit(table10M, List.of(0.0), List.of(1));
        testIntersectionsUnderLimit(table10M, List.of(0.0, 0.0), List.of(1));
        testIntersectionsUnderLimit(table10M, List.of(0.0, 0.1), List.of(1));
        testIntersectionsUnderLimit(table10M, List.of(0.0, 0.0, 0.0), List.of(1));
        testIntersectionsUnderLimit(table10M, List.of(0.0, 0.0, 0.1), List.of(1));
        testIntersectionsUnderLimit(table10M, List.of(0.0, 0.0, 0.0, 0.0), List.of(1));

        testIntersectionsUnderLimit(table1M, List.of(1.0), List.of(1));
        testIntersectionsUnderLimit(table1M, List.of(0.5), List.of(1));
        testIntersectionsUnderLimit(table1M, List.of(0.1), List.of(1));
        testIntersectionsUnderLimit(table1M, List.of(0.0), List.of(1));

        testIntersectionsUnderLimit(table10M, List.of(1.0), List.of(1));
        testIntersectionsUnderLimit(table10M, List.of(0.5), List.of(1));
        testIntersectionsUnderLimit(table10M, List.of(0.1), List.of(1));
        testIntersectionsUnderLimit(table10M, List.of(0.0), List.of(1));


        testIntersectionsUnderLimit(table10M, List.of(0.1, 1.0), List.of(1));
        testIntersectionsUnderLimit(table10M, List.of(0.1, 0.2), List.of(2));

        testIntersectionsUnderLimit(table10M, List.of(0.9, 0.9), List.of(1));
        testIntersectionsUnderLimit(table10M, List.of(0.2, 0.2), List.of(2));
        testIntersectionsUnderLimit(table10M, List.of(0.1, 0.1), List.of(2));

        testIntersectionsUnderLimit(table10M, List.of(0.01, 1.0), List.of(1));
        testIntersectionsUnderLimit(table10M, List.of(0.001, 1.0), List.of(1));

        testIntersectionsUnderLimit(table10M, List.of(0.1, 0.1, 1.0), List.of(2));
        testIntersectionsUnderLimit(table10M, List.of(0.1, 0.1, 0.5), List.of(2, 3));
        testIntersectionsUnderLimit(table10M, List.of(0.1, 0.1, 0.2), List.of(2, 3));
        testIntersectionsUnderLimit(table10M, List.of(0.1, 0.1, 0.1), List.of(3));

        testIntersectionsUnderLimit(table10M, List.of(0.01, 0.01, 1.0), List.of(2));
        testIntersectionsUnderLimit(table10M, List.of(0.01, 0.01, 0.5), List.of(2));
        testIntersectionsUnderLimit(table10M, List.of(0.01, 0.01, 0.2), List.of(2, 3));
        testIntersectionsUnderLimit(table10M, List.of(0.01, 0.01, 0.1), List.of(2, 3));
        testIntersectionsUnderLimit(table10M, List.of(0.01, 0.01, 0.05), List.of(2, 3));
        testIntersectionsUnderLimit(table10M, List.of(0.01, 0.01, 0.02), List.of(3));
        testIntersectionsUnderLimit(table10M, List.of(0.01, 0.01, 0.01), List.of(3));

        testIntersectionsUnderLimit(table10M, List.of(0.001, 1.0, 1.0), List.of(1));
        testIntersectionsUnderLimit(table10M, List.of(0.001, 0.7, 1.0), List.of(1));
        testIntersectionsUnderLimit(table10M, List.of(0.001, 0.5, 1.0), List.of(1, 2));
        testIntersectionsUnderLimit(table10M, List.of(0.001, 0.2, 1.0), List.of(1, 2));
        testIntersectionsUnderLimit(table10M, List.of(0.001, 0.1, 1.0), List.of(2));
        testIntersectionsUnderLimit(table10M, List.of(0.001, 0.05, 1.0), List.of(2));
        testIntersectionsUnderLimit(table10M, List.of(0.001, 0.02, 1.0), List.of(2));
        testIntersectionsUnderLimit(table10M, List.of(0.001, 0.001, 1.0), List.of(2));

        testIntersectionsUnderLimit(table10M, List.of(0.0001, 1.0, 1.0), List.of(1));
        testIntersectionsUnderLimit(table10M, List.of(0.0001, 0.5, 1.0), List.of(1));
        testIntersectionsUnderLimit(table10M, List.of(0.0001, 0.2, 1.0), List.of(1, 2));
        testIntersectionsUnderLimit(table10M, List.of(0.0001, 0.1, 1.0), List.of(1, 2));
        testIntersectionsUnderLimit(table10M, List.of(0.0001, 0.05), List.of(2));
        testIntersectionsUnderLimit(table10M, List.of(0.0001, 0.02, 1.0), List.of(2));
        testIntersectionsUnderLimit(table10M, List.of(0.0001, 0.001, 1.0), List.of(2));


        testIntersectionsUnderLimit(table10M, List.of(0.0001, 0.0001, 0.0001), List.of(3));
        testIntersectionsUnderLimit(table10M, List.of(0.001, 0.001, 0.001), List.of(3));
        testIntersectionsUnderLimit(table10M, List.of(0.002, 0.002, 0.002), List.of(3));
        testIntersectionsUnderLimit(table10M, List.of(0.005, 0.005, 0.005), List.of(3));
        testIntersectionsUnderLimit(table10M, List.of(0.008, 0.008, 0.008), List.of(3));
    }

    private void testIntersectionsUnderLimit(Plan.TableMetrics metrics, List<Double> selectivities, List<Integer> expectedIndexScanCount)
    {
        Plan.Factory factory = new Plan.Factory(metrics);
        List<Plan.KeysIteration> indexScans = new ArrayList<>(selectivities.size());
        RowFilter.Builder rowFilterBuilder = RowFilter.builder();
        RowFilter.Expression[] predicates = new RowFilter.Expression[] { pred1, pred2, pred3, pred4 };
        Expression[] saiPredicates = new Expression[] { saiPred1, saiPred2, saiPred3, saiPred4 };
        for (int i = 0; i < selectivities.size(); i++)
        {
            indexScans.add(factory.numericIndexScan(saiPredicates[i], (long) (selectivities.get(i) * metrics.rows)));
            rowFilterBuilder.add(predicates[i]);
        }

        Plan.KeysIteration intersection = factory.intersection(indexScans);
        Plan.RowsIteration fetch = factory.fetch(intersection);
        Plan.RowsIteration postFilter = factory.recheckFilter(rowFilterBuilder.build(), fetch);
        Plan.RowsIteration origPlan = factory.limit(postFilter, 3);

        Plan optimizedPlan = origPlan.optimize();
        List<Plan.NumericIndexScan> resultIndexScans = optimizedPlan.nodesOfType(Plan.NumericIndexScan.class);
        assertTrue("original:\n" + origPlan.toStringRecursive() + "optimized:\n" + optimizedPlan.toStringRecursive(),
                     expectedIndexScanCount.contains(resultIndexScans.size()));
    }

    @Test
    public void optimizeIntersectionsUnderAnnSort()
    {
        testIntersectionsUnderAnnSort(table1M, Expression.Op.RANGE, List.of(0.0), List.of(1));
        testIntersectionsUnderAnnSort(table1M, Expression.Op.RANGE, List.of(0.0, 0.0), List.of(1));
        testIntersectionsUnderAnnSort(table1M, Expression.Op.RANGE, List.of(0.0, 0.0, 0.1), List.of(1));

        testIntersectionsUnderAnnSort(table1M, Expression.Op.RANGE, List.of(1.0), List.of(0));
        testIntersectionsUnderAnnSort(table1M, Expression.Op.RANGE, List.of(0.1), List.of(0));
        testIntersectionsUnderAnnSort(table1M, Expression.Op.RANGE, List.of(0.05), List.of(0));
        testIntersectionsUnderAnnSort(table1M, Expression.Op.RANGE, List.of(0.02), List.of(0, 1));
        testIntersectionsUnderAnnSort(table1M, Expression.Op.RANGE, List.of(0.01), List.of(0, 1));
        testIntersectionsUnderAnnSort(table1M, Expression.Op.RANGE, List.of(0.001), List.of(1));
        testIntersectionsUnderAnnSort(table1M, Expression.Op.RANGE, List.of(0.0001), List.of(1));
        testIntersectionsUnderAnnSort(table1M, Expression.Op.RANGE, List.of(0.00001), List.of(1));

        testIntersectionsUnderAnnSort(table1M, Expression.Op.RANGE, List.of(0.9, 0.9), List.of(0));
        testIntersectionsUnderAnnSort(table1M, Expression.Op.RANGE, List.of(0.1, 0.9), List.of(0));
        testIntersectionsUnderAnnSort(table1M, Expression.Op.RANGE, List.of(0.001, 0.9), List.of(1));
        testIntersectionsUnderAnnSort(table1M, Expression.Op.RANGE, List.of(0.001, 0.1), List.of(1));
        testIntersectionsUnderAnnSort(table1M, Expression.Op.RANGE, List.of(0.001, 0.002), List.of(2));
        testIntersectionsUnderAnnSort(table1M, Expression.Op.RANGE, List.of(0.001, 0.001), List.of(2));

        testIntersectionsUnderAnnSort(table1M, Expression.Op.RANGE, List.of(0.001, 1.0), List.of(1));
        testIntersectionsUnderAnnSort(table1M, Expression.Op.RANGE, List.of(0.001, 0.5), List.of(1));
        testIntersectionsUnderAnnSort(table1M, Expression.Op.RANGE, List.of(0.001, 0.2), List.of(1));
        testIntersectionsUnderAnnSort(table1M, Expression.Op.RANGE, List.of(0.001, 0.1), List.of(1));
        testIntersectionsUnderAnnSort(table1M, Expression.Op.RANGE, List.of(0.001, 0.05), List.of(1, 2));
        testIntersectionsUnderAnnSort(table1M, Expression.Op.RANGE, List.of(0.001, 0.02), List.of(2));
        testIntersectionsUnderAnnSort(table1M, Expression.Op.RANGE, List.of(0.001, 0.001), List.of(2));
        testIntersectionsUnderAnnSort(table1M, Expression.Op.RANGE, List.of(0.0001, 0.0001), List.of(2));
        testIntersectionsUnderAnnSort(table1M, Expression.Op.RANGE, List.of(0.00001, 0.00001), List.of(2));

        testIntersectionsUnderAnnSort(table1M, Expression.Op.RANGE, List.of(0.1, 1.0, 1.0), List.of(0));
        testIntersectionsUnderAnnSort(table1M, Expression.Op.RANGE, List.of(0.1, 0.1, 1.0), List.of(0));
        testIntersectionsUnderAnnSort(table1M, Expression.Op.RANGE, List.of(0.1, 0.1, 1.0), List.of(0));
        testIntersectionsUnderAnnSort(table1M, Expression.Op.RANGE, List.of(0.1, 0.1, 0.5), List.of(0));
        testIntersectionsUnderAnnSort(table1M, Expression.Op.RANGE, List.of(0.1, 0.1, 0.2), List.of(0));

        testIntersectionsUnderAnnSort(table1M, Expression.Op.RANGE, List.of(0.01, 1.0, 1.0), List.of(1));

        testIntersectionsUnderAnnSort(table1M, Expression.Op.RANGE, List.of(0.01, 0.01, 1.0), List.of(1, 2));
        testIntersectionsUnderAnnSort(table1M, Expression.Op.RANGE, List.of(0.01, 0.01, 0.5), List.of(2));
        testIntersectionsUnderAnnSort(table1M, Expression.Op.RANGE, List.of(0.01, 0.01, 0.2), List.of(2));
        testIntersectionsUnderAnnSort(table1M, Expression.Op.RANGE, List.of(0.01, 0.01, 0.1), List.of(2, 3));
        testIntersectionsUnderAnnSort(table1M, Expression.Op.RANGE, List.of(0.01, 0.01, 0.05), List.of(2, 3));
        testIntersectionsUnderAnnSort(table1M, Expression.Op.RANGE, List.of(0.01, 0.01, 0.02), List.of(2, 3));
        testIntersectionsUnderAnnSort(table1M, Expression.Op.RANGE, List.of(0.01, 0.01, 0.01), List.of(2, 3));

        testIntersectionsUnderAnnSort(table1M, Expression.Op.RANGE, List.of(0.001, 1.0, 1.0), List.of(1));
        testIntersectionsUnderAnnSort(table1M, Expression.Op.RANGE, List.of(0.001, 0.5, 1.0), List.of(1));
        testIntersectionsUnderAnnSort(table1M, Expression.Op.RANGE, List.of(0.001, 0.2, 1.0), List.of(1));
        testIntersectionsUnderAnnSort(table1M, Expression.Op.RANGE, List.of(0.001, 0.1, 1.0), List.of(1));
        testIntersectionsUnderAnnSort(table1M, Expression.Op.RANGE, List.of(0.001, 0.05, 1.0), List.of(1, 2));
        testIntersectionsUnderAnnSort(table1M, Expression.Op.RANGE, List.of(0.001, 0.02, 1.0), List.of(2));
        testIntersectionsUnderAnnSort(table1M, Expression.Op.RANGE, List.of(0.001, 0.01, 1.0), List.of(2));

        testIntersectionsUnderAnnSort(table10M, Expression.Op.RANGE, List.of(0.001, 1.0, 1.0), List.of(1));
        testIntersectionsUnderAnnSort(table10M, Expression.Op.RANGE, List.of(0.001, 0.5, 1.0), List.of(1));
        testIntersectionsUnderAnnSort(table10M, Expression.Op.RANGE, List.of(0.001, 0.2, 1.0), List.of(1));
        testIntersectionsUnderAnnSort(table10M, Expression.Op.RANGE, List.of(0.001, 0.1, 1.0), List.of(1));
        testIntersectionsUnderAnnSort(table10M, Expression.Op.RANGE, List.of(0.001, 0.05, 1.0), List.of(1));
        testIntersectionsUnderAnnSort(table10M, Expression.Op.RANGE, List.of(0.001, 0.02, 1.0), List.of(1));
        testIntersectionsUnderAnnSort(table10M, Expression.Op.RANGE, List.of(0.001, 0.01, 1.0), List.of(1));
        testIntersectionsUnderAnnSort(table10M, Expression.Op.RANGE, List.of(0.001, 0.005, 1.0), List.of(1, 2));
        testIntersectionsUnderAnnSort(table10M, Expression.Op.RANGE, List.of(0.001, 0.002, 1.0), List.of(2));
        testIntersectionsUnderAnnSort(table10M, Expression.Op.RANGE, List.of(0.001, 0.001, 1.0), List.of(2));

        testIntersectionsUnderAnnSort(table1M, Expression.Op.RANGE, List.of(0.0001, 0.0001, 0.0001, 0.0001), List.of(3));
        testIntersectionsUnderAnnSort(table1M, Expression.Op.RANGE, List.of(0.0001, 0.0001, 0.0001), List.of(3));
        testIntersectionsUnderAnnSort(table1M, Expression.Op.RANGE, List.of(0.001, 0.001, 0.001), List.of(3));
        testIntersectionsUnderAnnSort(table1M, Expression.Op.RANGE, List.of(0.002, 0.002, 0.002), List.of(3));
        testIntersectionsUnderAnnSort(table1M, Expression.Op.RANGE, List.of(0.005, 0.005, 0.005), List.of(3));
        testIntersectionsUnderAnnSort(table1M, Expression.Op.RANGE, List.of(0.008, 0.008, 0.008), List.of(3));
        testIntersectionsUnderAnnSort(table1M, Expression.Op.RANGE, List.of(0.01, 0.01, 0.01), List.of(3));

        testIntersectionsUnderAnnSort(table10M, Expression.Op.RANGE, List.of(0.0001, 0.0001, 0.0001, 0.0001), List.of(3));
        testIntersectionsUnderAnnSort(table10M, Expression.Op.RANGE, List.of(0.0001, 0.0001, 0.0001), List.of(3));
        testIntersectionsUnderAnnSort(table10M, Expression.Op.RANGE, List.of(0.001, 0.001, 0.001), List.of(3));
        testIntersectionsUnderAnnSort(table10M, Expression.Op.RANGE, List.of(0.002, 0.002, 0.002), List.of(3));
        testIntersectionsUnderAnnSort(table10M, Expression.Op.RANGE, List.of(0.005, 0.005, 0.005), List.of(3));
        testIntersectionsUnderAnnSort(table10M, Expression.Op.RANGE, List.of(0.008, 0.008, 0.008), List.of(3));
        testIntersectionsUnderAnnSort(table10M, Expression.Op.RANGE, List.of(0.01, 0.01, 0.01), List.of(2, 3));
    }

    @Test
    public void optimizeLiteralIntersectionsUnderAnnSort()
    {
        testIntersectionsUnderAnnSort(table10M, Expression.Op.EQ, List.of(0.001, 1.0), List.of(1));
        testIntersectionsUnderAnnSort(table10M, Expression.Op.EQ, List.of(0.001, 0.5), List.of(1));
        testIntersectionsUnderAnnSort(table10M, Expression.Op.EQ, List.of(0.001, 0.2), List.of(1));
        testIntersectionsUnderAnnSort(table10M, Expression.Op.EQ, List.of(0.001, 0.1), List.of(1));
        testIntersectionsUnderAnnSort(table10M, Expression.Op.EQ, List.of(0.001, 0.05), List.of(1));
        testIntersectionsUnderAnnSort(table10M, Expression.Op.EQ, List.of(0.001, 0.02), List.of(1));
        testIntersectionsUnderAnnSort(table10M, Expression.Op.EQ, List.of(0.001, 0.01), List.of(1));
        testIntersectionsUnderAnnSort(table10M, Expression.Op.EQ, List.of(0.001, 0.005), List.of(1, 2));
        testIntersectionsUnderAnnSort(table10M, Expression.Op.EQ, List.of(0.001, 0.002), List.of(2));
        testIntersectionsUnderAnnSort(table10M, Expression.Op.EQ, List.of(0.001, 0.001), List.of(2));
    }

    private void testIntersectionsUnderAnnSort(Plan.TableMetrics metrics,
                                               Expression.Op operation,
                                               List<Double> selectivities,
                                               List<Integer> expectedIndexScanCount)
    {
        Plan.Factory factory = new Plan.Factory(metrics);
        List<Plan.KeysIteration> indexScans = new ArrayList<>(selectivities.size());
        RowFilter.Builder rowFilterBuilder = RowFilter.builder();
        for (int i = 0; i < selectivities.size(); i++)
        {
            String column = "p" + i;
            Plan.KeysIteration indexScan = factory.numericIndexScan(saiPred(column, operation),
                                                                    (long) (selectivities.get(i) * metrics.rows));
            indexScans.add(indexScan);
            rowFilterBuilder.add(filerPred(column, operation == Expression.Op.RANGE ? Operator.LT : Operator.EQ));
        }

        Plan.KeysIteration intersection = factory.intersection(indexScans);
        Plan.KeysIteration sort = factory.annSort(intersection, ordering);
        Plan.RowsIteration fetch = factory.fetch(sort);
        Plan.RowsIteration postFilter = factory.recheckFilter(rowFilterBuilder.build(), fetch);
        Plan.RowsIteration origPlan = factory.limit(postFilter, 3);

        Plan optimizedPlan = origPlan.optimize();
        List<Plan.IndexScan> resultIndexScans = optimizedPlan.nodesOfType(Plan.IndexScan.class);
        assertTrue("original:\n" + origPlan.toStringRecursive() + "optimized:\n" + optimizedPlan.toStringRecursive(),
                     expectedIndexScanCount.contains(resultIndexScans.size()));
    }
    
}