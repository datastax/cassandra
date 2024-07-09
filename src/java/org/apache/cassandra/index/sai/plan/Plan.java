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
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cache.ChunkCache;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.RangeIntersectionIterator;
import org.apache.cassandra.index.sai.utils.RangeIterator;
import org.apache.cassandra.index.sai.utils.RangeUnionIterator;
import org.apache.cassandra.index.sai.utils.SoftLimitUtil;
import org.apache.cassandra.index.sai.utils.TreeFormatter;
import org.apache.cassandra.io.util.FileUtils;

import static java.lang.Math.max;
import static org.apache.cassandra.index.sai.plan.Plan.CostCoefficients.*;

/**
 * The common base class for query execution plan nodes.
 * The top-level node is considered to be the execution plan of the query.
 *
 * <h1>Structure</h1>
 * A query plan is an immutable tree constisting of nodes representing physical data operations,
 * e.g. index scans, intersections, unions, filtering, limiting, sorting, etc.
 * Nodes of type {@link KeysIteration} operate on streams of keys, and nodes of type {@link RowsIteration} operate
 * on streams of rows. You should build a plan bottom-up by using static methods in {@link Plan.Factory}.
 * Nodes don't have pointers to parent nodes on purpose – this way multiple plans can share subtrees.
 *
 * <h1>Cost estimation</h1>
 * A plan can estimate its execution cost and result set size which is useful to select the best plan among
 * the semantically equivalent candidate plans. Operations represented by nodes may be pipelined, so their actual
 * runtime cost may depend on how many rows are read from the top level node. Upon construction, each plan node
 * gets an {@link Access} object which describes the way how the node results are going to be used by the parent nodes:
 * how many rows will be requested or what skip operations are going to be performed on the iterator.
 * The access objects get propagated down the tree to the leaves. This way we get an accurate cost of execution
 * at the leave nodes, taking into account any top-level limit or intersections.
 * <p>
 * Some nodes cannot be pipelined, e.g. nodes that represent sorting. To make cost estimation for such nodes possible,
 * each node maintains an initial cost (initCost) of the operation - that is the cost of preparation before the first
 * result row or key can be returned. Sorting nodes can have that cost very high.
 *
 * <h1>Optimization</h1>
 * This class also offers a few methods for modifying the plans (e.g. removing nodes) and a method allowing
 * to automatically improve the plan – see {@link #optimize()}. Whenever we talk about "modification" or "updates"
 * we always mean constructing a new plan. All updates are non-destructive. Each node has a unique numeric
 * identifier in the tree. Because a modification requires creating some new nodes, identifiers allow to find
 * corresponding nodes in the modified plan, even if they addresses changed (they are different java objects).
 *
 * <h1>Execution</h1>
 * The plan tree may store additional context information to be executable, i.e. to produce the iterator over the result
 * keys or rows - see {@link KeysIteration#execute}. However, the purpose of the plan nodes is not to perform
 * the actual computation of the result set. Instead, it should delegate the control to other modules responsible
 * for data retrieval. The plan only sets up the execution, but must not contain the execution logic.
 * For the sake of good testability, plan trees must be creatable, estimatable and optimizable also without
 * creating any of the objects used by the execution engine.
 *
 * <h1>Example</h1>
 * The CQL query
 * <pre>
 * SELECT * FROM table WHERE  a < 0.01 AND b < 0.2 LIMIT 10
 * </pre>
 *
 * can be represented by the following query execution plan:
 * <pre>
 * Limit 10 (rows: 10.0, cost/row: 265.2, cost: 80.0..2732.2)
 *  └─ Filter a < 0.2 AND b < 0.01 (sel: 1.000000000) (rows: 10.0, cost/row: 265.2, cost: 80.0..2732.2)
 *      └─ Fetch (rows: 10.0, cost/row: 265.2, cost: 80.0..2732.2)
 *          └─ Intersection (keys: 10.0, cost/key: 58.2, cost: 80.0..662.1)
 *              ├─ NumericIndexScan of vector_b_idx using Expression{ ... } (sel: 0.010010000, step: 1.0) (keys: 50.2, cost/key: 1.0, cost: 40.0..90.2)
 *              └─ NumericIndexScan of vector_a_idx using Expression{ ... } (sel: 0.199230000, step: 19.9) (keys: 50.2, cost/key: 10.6, cost: 40.0..571.9)
 * </pre>
 */
@NotThreadSafe
abstract public class Plan
{
    private static final Logger logger = LoggerFactory.getLogger(Plan.class);

    /**
     * Identifier of the plan tree node.
     * Used to identify the nodes of the plan.
     * Preserved during plan transformations.
     * <p>
     * Identifiers are more useful than object's identity (address) because plans can be transformed functionally
     * and as the result of that process we may get new node objects.
     * Identifiers allow us to match nodes in the transformed plan to the original.
     */
    final int id;

    /**
     * Reference to the factory gives access to common data shared among all nodes,
     * e.g. total number of keys in the table and the cost parameters.
     * It also allows to modify plan trees, e.g. create new nodes or recreate this node with different parameters.
     */
    final Factory factory;

    /**
     * Describes how this node is going to be used.
     * Very likely affects the cost.
     */
    final Access access;

    /**
     * Lazily caches the estimated fraction of the table data that the result of this plan is expected to match.
     */
    private double selectivity = -1;


    private Plan(Factory factory, int id, Access access)
    {
        this.id = id;
        this.factory = factory;
        this.access = access;
    }

    /** selectivity comparisons to 0 will probably cause bugs, use this instead */
    protected static boolean isEffectivelyZero(double a) {
        assert a >= 0;
        return a < 1e-9;
    }

    /** dividing by extremely tiny numbers can cause overflow so clamp the minimum to 1e-9 */
    protected static double boundedSelectivity(double selectivity) {
        assert 0 <= selectivity && selectivity <= 1.0;
        return Math.max(1e-9, selectivity);
    }

    /**
     * Returns a new list containing subplans of this node.
     * The list can be later freely modified by the caller and does not affect the original plan.
     * <p>
     * Performance warning: This allocates a fresh list on the heap.
     * If you only want to iterate the subplan nodes, it is recommended to use {@link #forEachSubplan(Function)}
     * or {@link #withUpdatedSubplans(Function)} which offer better performance and less GC pressure.
     */
    final List<Plan> subplans()
    {
        List<Plan> result = new ArrayList<>();
        forEachSubplan(subplan -> {
            result.add(subplan);
            return ControlFlow.Continue;
        });
        return result;
    }

    /**
     * Returns a new list of nodes of given type.
     * The tree is traversed in depth-first order.
     * This node is included in the search.
     * The list can be later freely modified by the caller and does not affect the original plan.
     * <p>
     * Performance warning: This allocates a fresh list on the heap.
     * If you only want to iterate the subplan nodes, it is recommended to use {@link #forEachSubplan(Function)}
     * which should offer better performance and less GC pressure.
     */
    @SuppressWarnings("unchecked")
    final <T extends Plan> List<T> nodesOfType(Class<T> nodeType)
    {
        List<T> result = new ArrayList<>();
        forEach(node -> {
            if (nodeType.isAssignableFrom(node.getClass()))
                result.add((T) node);
            return ControlFlow.Continue;
        });
        return result;
    }

    /**
     * Returns the first node of the given type.
     * Searches the tree in depth-first order.
     * This node is included in the search.
     * If node of given type is not found, returns null.
     */
    @SuppressWarnings("unchecked")
    final <T extends Plan> @Nullable T firstNodeOfType(Class<T> nodeType)
    {
        Plan[] result = new Plan[] { null };
        forEach(node -> {
            if (nodeType.isAssignableFrom(node.getClass()))
            {
                result[0] = node;
                return ControlFlow.Break;
            }
            return ControlFlow.Continue;
        });
        return (T) result[0];
    }

    /**
     * Calls a function recursively for each node of given type in the tree.
     * If the function returns {@link ControlFlow#Break} then the traversal is aborted.
     * @return {@link ControlFlow#Continue} if traversal hasn't been aborted, {@link ControlFlow#Break} otherwise.
     */
    final ControlFlow forEach(Function<Plan, ControlFlow> function)
    {
        return (function.apply(this) == ControlFlow.Continue)
               ? forEachSubplan(subplan -> subplan.forEach(function))
               : ControlFlow.Break;
    }

    /**
     * Calls a function for each child node of this plan.
     * The function should return {@link ControlFlow#Continue} to indicate the iteration should be continued
     * and {@link ControlFlow#Break} to abort it.
     *
     * @return the value returned by the last invocation of the function
     */
    abstract ControlFlow forEachSubplan(Function<Plan, ControlFlow> function);


    /** Controls tree traversals, see {@link #forEach(Function)} and {@link #forEachSubplan(Function)}  */
    enum ControlFlow { Continue, Break }

    /**
     * Runs the updater function on each subplan and if the updater returns a new subplan, then reconstructs this
     * plan from the modified subplans.
     * <p>
     * Accepting a list of sub-plans would be a valid alternative design of this API,
     * but that would require constructing a list on the heap by the caller for each updated node,
     * and that would be potentially wasteful as most of the node types have at most one subplan and don't use
     * lists internally.
     *
     * @param updater a function to be called on each subplan; if no update is needed, should return the argument
     * @return a new plan if any of the subplans has been replaced, this otherwise
     */
    protected abstract Plan withUpdatedSubplans(Function<Plan, Plan> updater);

    /**
     * Returns an object describing detailed cost information about running this plan.
     * The actual type of the Cost depends in practice on the type of the result set returned by the node.
     * The results of this method are supposed to be cached. The method is idempotent.
     * The cost usually depends on the Access value.
     */
    protected abstract Cost cost();

    /**
     * Estimates the probability of a random key or row of the table to be included in the result set
     * if the result was iterated fully with no skipping and if it did not have any limits.
     * This property is independent of the way how result set is used.
     */
    protected abstract double estimateSelectivity();

    /**
     * Formats the whole plan as a pretty tree
     */
    public final String toStringRecursive()
    {
        TreeFormatter<Plan> formatter = new TreeFormatter<>(Plan::toString, Plan::subplans);
        return formatter.format(this);
    }

    /**
     * Returns the string representation of this node only
     */
    public final String toString()
    {
        String description = description();
        return (description.isEmpty())
            ? String.format("%s (%s)", getClass().getSimpleName(), cost())
            : String.format("%s %s (%s)", getClass().getSimpleName(), description, cost());
    }

    /**
     * Returns additional information specific to the node.
     * The information is included in the output of {@link #toString()} and {@link #toStringRecursive()}.
     * It is up to subclasses to implement it.
     */
    protected String description()
    {
        return "";
    }

    /**
     * Returns an optimized plan.
     * <p>
     * The current optimization algorithm repeatedly cuts down one leaf of the plan tree
     * and recomputes the nodes above it. Then it returns the best plan from candidates obtained that way.
     * The expected running time is proportional to the height of the plan tree multiplied by the number of the leaves.
     */
    public final Plan optimize()
    {
        if (logger.isTraceEnabled())
            logger.trace("Optimizing plan:\n{}", this.toStringRecursive());

        Plan bestPlanSoFar = this;
        List<Leaf> leaves = nodesOfType(Leaf.class);

        // Remove leaves one by one, starting from the ones with the worst selectivity
        leaves.sort(Comparator.comparingDouble(Plan::selectivity).reversed());
        for (Leaf leaf : leaves)
        {
            Plan candidate = bestPlanSoFar.remove(leaf.id);
            if (logger.isTraceEnabled())
                logger.trace("Candidate query plan:\n{}", candidate.toStringRecursive());

            if (candidate.fullCost() <= bestPlanSoFar.fullCost())
                bestPlanSoFar = candidate;
        }

        if (logger.isTraceEnabled())
            logger.trace("Optimized plan:\n{}", bestPlanSoFar.toStringRecursive());
        return bestPlanSoFar;
    }

    /**
     * Modifies all intersections to not intersect more clauses than the given limit.
     */
    public final Plan limitIntersectedClauses(int clauseLimit)
    {
        Plan result = this;
        if (result instanceof Intersection)
        {
            Plan.Intersection intersection = (Plan.Intersection) result;
            result = intersection.stripSubplans(clauseLimit);
        }
        return result.withUpdatedSubplans(p -> p.limitIntersectedClauses(clauseLimit));
    }

    /** Returns true if the plan contains a node matching the condition */
    final boolean contains(Function<Plan, Boolean> condition)
    {
        ControlFlow res = forEach(node -> (condition.apply(node)) ? ControlFlow.Break : ControlFlow.Continue);
        return res == ControlFlow.Break;
    }

    /**
     * Returns a new plan with the given node removed.
     * Searches for the subplan to remove recursively down the tree.
     * If the new plan is different, its estimates are also recomputed.
     * If *this* plan matches the id, then the {@link Everything} node is returned.
     *
     * <p>
     * The purpose of this method is to optimise the plan.
     * Sometimes not doing an intersection and post-filtering instead can be faster, so by removing child nodes from
     * intersections we can potentially get a better plan.
     */
    final Plan remove(int id)
    {
        // If id is the same, replace this node with "everything"
        // because a query with no filter expression returns all rows
        // (removing restrictions should widen the result set)
        return (this.id == id)
                ? factory.everything
                : withUpdatedSubplans(subplan -> subplan.remove(id));
    }

    /**
     * Returns the estimated cost of preparation steps
     * that must be done before returning the first row / key
     */
    public final double initCost()
    {
        return cost().initCost();
    }

    public final double iterCost()
    {
        return cost().iterCost();
    }

    /**
     * Returns the estimated cost of running the plan to completion, i.e. exhausting
     * the key or row iterator returned by it
     */
    public final double fullCost()
    {
        return cost().fullCost();
    }

    /**
     * Returns the estimated fraction of the table data that the result of this plan is expected to match
     */
    public final double selectivity()
    {
        if (selectivity == -1)
            selectivity = estimateSelectivity();
        assert 0.0 <= selectivity && selectivity <= 1.0 : "Invalid selectivity: " + selectivity;
        return selectivity;
    }

    protected interface Cost
    {
        double initCost();
        double iterCost();
        double fullCost();
    }

    protected static final class KeysIterationCost implements Cost
    {
        final double expectedKeys;
        final double initCost;
        final double iterCost;

        public KeysIterationCost(double expectedKeys, double initCost, double iterCost)
        {
            this.expectedKeys = expectedKeys;
            this.initCost = initCost;
            this.iterCost = iterCost;
        }

        @Override
        public double initCost()
        {
            return initCost;
        }

        @Override
        public double iterCost()
        {
            return iterCost;
        }

        @Override
        public double fullCost()
        {
            return initCost + iterCost;
        }

        public double costPerKey()
        {
            return expectedKeys == 0 ? 0.0 : iterCost / expectedKeys;
        }

        public String toString()
        {
            return String.format("keys: %.1f, cost/key: %.1f, cost: %.1f..%.1f",
                                 expectedKeys, costPerKey(), initCost, fullCost());
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            KeysIterationCost that = (KeysIterationCost) o;
            return Double.compare(expectedKeys, that.expectedKeys) == 0
                   && Double.compare(initCost, that.initCost) == 0
                   && Double.compare(iterCost, that.iterCost) == 0;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(expectedKeys, initCost, iterCost);
        }
    }

    protected static final class RowsIterationCost implements Cost
    {
        final double expectedRows;
        final double initCost;
        final double iterCost;

        public RowsIterationCost(double expectedRows, double initCost, double iterCost)
        {
            this.expectedRows = expectedRows;
            this.initCost = initCost;
            this.iterCost = iterCost;
        }

        @Override
        public double initCost()
        {
            return initCost;
        }

        @Override
        public double iterCost()
        {
            return iterCost;
        }

        @Override
        public double fullCost()
        {
            return initCost + iterCost;
        }

        public double costPerRow()
        {
            return expectedRows == 0 ? 0.0 : iterCost / expectedRows;
        }

        public String toString()
        {
            return String.format("rows: %.1f, cost/row: %.1f, cost: %.1f..%.1f",
                                 expectedRows, costPerRow(), initCost, fullCost());
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            RowsIterationCost that = (RowsIterationCost) o;
            return Double.compare(expectedRows, that.expectedRows) == 0
                   && Double.compare(initCost, that.initCost) == 0
                   && Double.compare(iterCost, that.iterCost) == 0;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(expectedRows, initCost, iterCost);
        }
    }

    /**
     * Common base class for all plan nodes that iterate over primary keys.
     */
    public abstract static class KeysIteration extends Plan
    {
        /**
         * Caches the estimated cost to avoid frequent recomputation
         */
        private KeysIterationCost cost;

        protected KeysIteration(Factory factory, int id, Access access)
        {
            super(factory, id, access);
        }

        @Override
        protected final KeysIterationCost cost()
        {
            if (cost == null)
                cost = estimateCost();
            return cost;
        }

        protected abstract KeysIterationCost estimateCost();

        protected abstract Iterator<? extends PrimaryKey> execute(Executor executor);

        protected abstract KeysIteration withAccess(Access patterns);
        
        final double expectedKeys()
        {
            return cost().expectedKeys;
        }

        final double costPerKey()
        {
            return cost().costPerKey();
        }
    }

    /**
     * Leaves of the plan tree cannot have subplans.
     * This class exists purely for DRY purpose.
     */
    abstract static class Leaf extends KeysIteration
    {
        protected Leaf(Factory factory, int id, Access accesses)
        {
            super(factory, id, accesses);
        }

        @Override
        protected ControlFlow forEachSubplan(Function<Plan, ControlFlow> function)
        {
            return ControlFlow.Continue;
        }

        @Override
        protected final Plan withUpdatedSubplans(Function<Plan, Plan> updater)
        {
            // There are no subplans so it is a noop
            return this;
        }
    }

    /**
     * Represents an index scan that returns an empty range
     */
    static class Nothing extends Leaf
    {
        protected Nothing(int id, Factory factory)
        {
            super(factory, id, null);
        }

        @Nonnull
        @Override
        protected KeysIterationCost estimateCost()
        {
            return new KeysIterationCost(0, 0.0, 0.0);
        }

        @Override
        protected double estimateSelectivity()
        {
            return 0;
        }

        @Override
        protected RangeIterator execute(Executor executor)
        {
            return RangeIterator.empty();
        }

        @Override
        protected Nothing withAccess(Access patterns)
        {
            // limit does not matter for Nothing node because it always returns 0 keys
            return this;
        }
    }

    /**
     * Represents an index scan that returns all keys in the table.
     * This is a virtual node that has no real representation in the database system.
     * It is useful in query optimization.
     */
    static class Everything extends Leaf
    {
        protected Everything(int id, Factory factory, Access accesses)
        {
            super(factory, id, accesses);
        }

        @Nonnull
        @Override
        protected KeysIterationCost estimateCost()
        {
            // We set the cost to infinity so this node is never present in the optimized plan.
            // We don't want to have those nodes in the final plan,
            // because currently we have no way to execute it efficiently.
            // In the future we may want to change it, when we have a way to return all rows without using an index.
            return new KeysIterationCost(access.totalCount(factory.tableMetrics.rows),
                                         Double.POSITIVE_INFINITY,
                                         Double.POSITIVE_INFINITY);
        }

        @Override
        protected double estimateSelectivity()
        {
            return 1.0;
        }

        @Override
        protected RangeIterator execute(Executor executor)
        {
            // Not supported because it doesn't make a lot of sense.
            // A direct scan of table data would be certainly faster.
            // Everything node is not supposed to be executed. However, it is useful for analyzing various plans,
            // e.g. we may get such node after removing some nodes from a valid, executable plan.
            throw new UnsupportedOperationException("Returning an iterator over all keys is not supported.");
        }

        @Override
        protected Everything withAccess(Access access)
        {
            return Objects.equals(access, this.access)
                   ? this
                   : new Everything(id, factory, access);
        }
    }

    abstract static class IndexScan extends Leaf
    {
        protected final Expression predicate;
        protected final long matchingKeysCount;

        public IndexScan(Factory factory, int id, Expression predicate, long matchingKeysCount, Access access)
        {
            super(factory, id, access);
            this.predicate = predicate;
            this.matchingKeysCount = matchingKeysCount;
        }

        @Override
        protected final String description()
        {
            return String.format("of %s using %s (sel: %.9f, step: %.1f)", predicate.getIndexName(), predicate, selectivity(), access.meanDistance());
        }

        @Override
        protected final KeysIterationCost estimateCost()
        {
            double expectedKeys = access.totalCount(matchingKeysCount);
            double costPerKey = access.unitCost(SAI_KEY_COST, this::estimateCostPerSkip);
            double initCost = SAI_OPEN_COST * factory.tableMetrics.sstables;
            double iterCost = expectedKeys * costPerKey;
            return new KeysIterationCost(expectedKeys, initCost, iterCost);
        }

        @Override
        protected double estimateSelectivity()
        {
            return factory.tableMetrics.rows > 0
                   ? ((double) matchingKeysCount / factory.tableMetrics.rows)
                   : 0.0;
        }

        private double estimateCostPerSkip(double step)
        {
            // This is the first very rough approximation of the cost model for skipTo operation.
            // It is likely not a very accurate model.
            // We know for sure that the cost goes up the bigger the skip distance (= the more key we skip over)
            // and also the bigger the merged posting list is. A range scan of the numeric
            // index may require merging posting lists from many index nodes. Intuitively, the more keys we match,
            // the higher number of posting lists are merged. Also, the further we skip, the higher number
            // of posting lists must be advanced, and we're also more likely to hit a non-cached chunk.
            // From a few experiments I did, I conclude those costs grow sublinearly.
            // In the future we probably will need to take more index metrics into account
            // (e.g. number of distinct values).

            double keysPerSSTable = (double) matchingKeysCount / factory.tableMetrics.sstables;

            double skipCostFactor;
            double postingsCountFactor;
            double postingsCountExponent;
            double skipDistanceFactor;
            double skipDistanceExponent;
            if (predicate.getOp() == Expression.Op.RANGE)
            {
                skipCostFactor = RANGE_SCAN_SKIP_COST;
                postingsCountFactor = RANGE_SCAN_SKIP_COST_POSTINGS_COUNT_FACTOR;
                postingsCountExponent = RANGE_SCAN_SKIP_COST_POSTINGS_COUNT_EXPONENT;
                skipDistanceFactor = RANGE_SCAN_SKIP_COST_DISTANCE_FACTOR;
                skipDistanceExponent = RANGE_SCAN_SKIP_COST_DISTANCE_EXPONENT;
            }
            else
            {
                skipCostFactor = POINT_LOOKUP_SKIP_COST;
                postingsCountFactor = 0.0;
                postingsCountExponent = 1.0;
                skipDistanceFactor = POINT_LOOKUP_SKIP_COST_DISTANCE_FACTOR;
                skipDistanceExponent = POINT_LOOKUP_SKIP_COST_DISTANCE_EXPONENT;
            }

            // divide by exponent so the derivative at 1.0 equals postingsCountFactor
            double dKeys = postingsCountFactor / postingsCountExponent;
            double postingsCountPenalty = dKeys * Math.pow(keysPerSSTable, postingsCountExponent);

            // divide by exponent so the derivative at 1.0 equals skipDistanceFactor
            double dPostings = skipDistanceFactor / skipDistanceExponent;
            double distancePenalty = dPostings * Math.pow(step, skipDistanceExponent);

            return skipCostFactor
                   * (1.0 + distancePenalty)
                   * (1.0 + postingsCountPenalty)
                   * factory.tableMetrics.sstables;
        }

        @Override
        protected Iterator<? extends PrimaryKey> execute(Executor executor)
        {
            return executor.getKeysFromIndex(predicate);
        }
    }
    /**
     * Represents a scan over a numeric storage attached index.
     */
    static class NumericIndexScan extends IndexScan
    {
        public NumericIndexScan(Factory factory, int id, Expression predicate, long matchingKeysCount, Access access)
        {
            super(factory, id, predicate, matchingKeysCount, access);
        }

        @Override
        protected NumericIndexScan withAccess(Access access)
        {
            return Objects.equals(this.access, access)
                   ? this
                   : new NumericIndexScan(factory, id, predicate, matchingKeysCount, access);
        }
    }

    /**
     * Represents a scan over a literal storage attached index
     */
    static class LiteralIndexScan extends IndexScan
    {
        public LiteralIndexScan(Factory factory, int id, Expression predicate, long matchingKeysCount, Access access)
        {
            super(factory, id, predicate, matchingKeysCount, access);
        }

        @Override
        protected LiteralIndexScan withAccess(Access access)
        {
            return Objects.equals(this.access, access)
                   ? this
                   : new LiteralIndexScan(factory, id, predicate, matchingKeysCount, this.access);
        }
    }

    /**
     * Union of multiple primary key streams.
     * This is a fairly cheap operation - its cost is basically a sum of costs of the subplans.
     */
    static final class Union extends KeysIteration
    {
        private final LazyTransform<List<KeysIteration>> subplansSupplier;

        Union(Factory factory, int id, List<KeysIteration> subplans, Access access)
        {
            super(factory, id, access);
            Preconditions.checkArgument(!subplans.isEmpty(), "Subplans must not be empty");

            // We propagate Access lazily just before we need the subplans.
            // This is because there may be several requests to change the access pattern from the top,
            // and we don't want to reconstruct the whole subtree each time
            this.subplansSupplier = new LazyTransform<>(subplans, this::propagateAccess);
        }

        /**
         * Adjusts the counts for each subplan to account for the other subplans.
         * As explained in `estimateSelectivity`, the union of (for instance) two subplans
         * that each select 50% of the keys is 75%, not 100%.  Thus, we need to reduce the counts
         * to remove estimated overlapping keys.
         */
        private List<KeysIteration> propagateAccess(List<KeysIteration> subplans)
        {
            if (isEffectivelyZero(selectivity()))
            {
                // all subplan selectivity should also be ~0
                for (var subplan: subplans)
                    assert isEffectivelyZero(subplan.selectivity());
                return subplans;
            }

            ArrayList<KeysIteration> newSubplans = new ArrayList<>(subplans.size());
            for (KeysIteration subplan : subplans)
            {
                Access access = this.access.scaleCount(subplan.selectivity() / selectivity());
                newSubplans.add(subplan.withAccess(access));
            }
            return newSubplans;
        }

        @Override
        protected double estimateSelectivity()
        {
            // Assume independence (lack of correlation) of subplans.
            // We multiply the probabilities of *not* selecting a key.
            // Because selectivity is usage-independent, we can use the original subplans,
            // to avoid forcing pushdown of Access information down.
            double inverseSelectivity = 1.0;
            for (KeysIteration plan : subplansSupplier.orig)
                inverseSelectivity *= (1.0 - plan.selectivity());
            return 1.0 - inverseSelectivity;
        }

        @Override
        protected ControlFlow forEachSubplan(Function<Plan, ControlFlow> function)
        {
            for (Plan s : subplansSupplier.get())
            {
                if (function.apply(s) == ControlFlow.Break)
                    return ControlFlow.Break;
            }
            return ControlFlow.Continue;
        }

        @Override
        protected Plan withUpdatedSubplans(Function<Plan, Plan> updater)
        {
            List<KeysIteration> subplans = subplansSupplier.get();
            ArrayList<KeysIteration> newSubplans = new ArrayList<>(subplans.size());
            for (Plan subplan : subplans)
                newSubplans.add((KeysIteration) updater.apply(subplan));

            return newSubplans.equals(subplans)
                   ? this
                   : factory.union(newSubplans, id).withAccess(access);
        }

        @Override
        protected Union withAccess(Access access)
        {
            return Objects.equals(access, this.access)
                ? this
                : new Union(factory, id, subplansSupplier.orig, access);
        }

        @Override
        protected KeysIterationCost estimateCost()
        {
            double initCost = 0.0;
            double iterCost = 0.0;
            List<KeysIteration> subplans = subplansSupplier.get();
            for (int i = 0; i < subplans.size(); i++)
            {
                KeysIteration subplan = subplans.get(i);
                // Initialization must be done all branches before we can start iterating
                initCost += subplan.initCost();
                iterCost += subplan.iterCost();
            }
            double expectedKeys = access.totalCount(factory.tableMetrics.rows * selectivity());
            return new KeysIterationCost(expectedKeys, initCost, iterCost);
        }

        @Override
        protected RangeIterator execute(Executor executor)
        {
            RangeIterator.Builder builder = RangeUnionIterator.builder();
            try
            {
                for (KeysIteration plan : subplansSupplier.get())
                    builder.add((RangeIterator) plan.execute(executor));
                return builder.build();
            }
            catch (Throwable t)
            {
                FileUtils.closeQuietly(builder.ranges());
                throw t;
            }
        }
    }

    /**
     * Intersection of multiple primary key streams.
     * This is quite complex operation where many keys from all underlying streams must be read in order
     * to return one matching key. Therefore, expect the cost of this operation to be significantly higher than
     * the costs of the subplans.
     */
    static final class Intersection extends KeysIteration
    {
        private final LazyTransform<List<KeysIteration>> subplansSupplier;

        private Intersection(Factory factory, int id, List<KeysIteration> subplans, Access access)
        {
            super(factory, id, access);
            Preconditions.checkArgument(!subplans.isEmpty(), "Subplans must not be empty");

            // We propagate Access lazily just before we need the subplans.
            // This is because there may be several requests to change the access pattern from the top,
            // and we don't want to reconstruct the whole subtree each time
            this.subplansSupplier = new LazyTransform<>(subplans, this::propagateAccess);
        }

        /**
         * In an intersection operation, the goal is to find the common elements between the results
         * of multiple subplans.  This requires taking into account not only the selectivity but also
         * the match probabilities between subplans.
         * <p>
         * VSTODO explain what's going on in more detail.
         */
        private ArrayList<KeysIteration> propagateAccess(List<KeysIteration> subplans)
        {
            double loops = isEffectivelyZero(selectivity()) ? 1.0 : subplans.get(0).selectivity() / selectivity();

            ArrayList<KeysIteration> newSubplans = new ArrayList<>(subplans.size());
            newSubplans.add(subplans.get(0).withAccess(access.scaleDistance(loops).convolute(loops, 1.0)));

            double matchProbability = 1.0;
            for (int i = 1; i < subplans.size(); i++)
            {
                KeysIteration subplan = subplans.get(i);
                double cumulativeSelectivity = subplans.get(0).selectivity() * matchProbability;
                double skipDistance = subplan.selectivity() / boundedSelectivity(cumulativeSelectivity);
                Access subAccess = access.scaleDistance(subplan.selectivity() / boundedSelectivity(selectivity()))
                                         .convolute(loops * matchProbability, skipDistance)
                                         .forceSkip();
                newSubplans.add(subplan.withAccess(subAccess));
                matchProbability *= subplan.selectivity();
            }
            return newSubplans;
        }

        @Override
        protected double estimateSelectivity()
        {
            double selectivity = 1.0;
            for (KeysIteration plan : subplansSupplier.orig)
                selectivity *= plan.selectivity();
            return selectivity;
        }

        @Override
        protected ControlFlow forEachSubplan(Function<Plan, ControlFlow> function)
        {
            for (Plan s : subplansSupplier.get())
            {
                if (function.apply(s) == ControlFlow.Break)
                    return ControlFlow.Break;
            }
            return ControlFlow.Continue;
        }

        @Override
        protected Plan withUpdatedSubplans(Function<Plan, Plan> updater)
        {
            List<KeysIteration> subplans = subplansSupplier.get();
            ArrayList<KeysIteration> newSubplans = new ArrayList<>(subplans.size());
            for (Plan subplan : subplans)
                newSubplans.add((KeysIteration) updater.apply(subplan));

            return newSubplans.equals(subplans)
                   ? this
                   : factory.intersection(newSubplans, id).withAccess(access);
        }

        @Override
        protected Intersection withAccess(Access access)
        {
            return Objects.equals(access, this.access)
                ? this
                : new Intersection(factory, id, subplansSupplier.orig, access);
        }

        @Override
        protected KeysIterationCost estimateCost()
        {
            List<KeysIteration> subplans = subplansSupplier.get();
            assert !subplans.isEmpty() : "Expected at least one subplan here. An intersection of 0 plans should have been optimized out.";

            double initCost = 0.0;
            double iterCost = 0.0;
            for (KeysIteration subplan : subplans)
            {
                initCost += subplan.initCost();
                iterCost += subplan.iterCost();
            }
            double expectedKeyCount = access.totalCount(factory.tableMetrics.rows * selectivity());
            return new KeysIterationCost(expectedKeyCount, initCost, iterCost);
        }

        @Override
        protected RangeIterator execute(Executor executor)
        {
            RangeIterator.Builder builder = RangeIntersectionIterator.builder();
            try
            {
                for (KeysIteration plan : subplansSupplier.get())
                    builder.add((RangeIterator) plan.execute(executor));

                return builder.build();
            }
            catch (Throwable t)
            {
                FileUtils.closeQuietly(builder.ranges());
                throw t;
            }
        }

        /**
         * Limits the number of intersected subplans
         */
        public Plan stripSubplans(int clauseLimit)
        {
            if (subplansSupplier.orig.size() <= clauseLimit)
                return this;
            List<Plan.KeysIteration> newSubplans = new ArrayList<>(subplansSupplier.orig.subList(0, clauseLimit));
            return factory.intersection(newSubplans, id).withAccess(access);
        }
    }

    /**
     * Sorts keys in ANN order.
     * Must fetch all keys from the source before sorting, so it has a high initial cost.
     */
    static class AnnSort extends KeysIteration
    {
        private final KeysIteration source;
        final RowFilter.Expression ordering;


        protected AnnSort(Factory factory, int id, KeysIteration source, RowFilter.Expression ordering, Access access)
        {
            super(factory, id, access);
            this.source = source;
            this.ordering = ordering;
        }

        @Override
        protected ControlFlow forEachSubplan(Function<Plan, ControlFlow> function)
        {
            return function.apply(source);
        }

        @Override
        protected Plan withUpdatedSubplans(Function<Plan, Plan> updater)
        {
            return factory.annSort((KeysIteration) updater.apply(source), ordering, id).withAccess(access);
        }

        @Override
        protected double estimateSelectivity()
        {
            return source.selectivity();
        }

        @Override
        protected KeysIterationCost estimateCost()
        {
            double expectedKeys = access.totalCount(source.expectedKeys());
            double initCost = ANN_OPEN_COST * factory.tableMetrics.sstables
                              + source.fullCost()
                              + source.expectedKeys() * CostCoefficients.ANN_INPUT_KEY_COST;
            return new KeysIterationCost(expectedKeys,
                                         initCost,
                                          expectedKeys * CostCoefficients.ANN_SCORED_KEY_COST);
        }

        @Override
        protected Iterator<? extends PrimaryKey> execute(Executor executor)
        {
            RangeIterator sourceIterator = (RangeIterator) source.execute(executor);
            int softLimit = Math.round((float) access.totalCount);
            return executor.getTopKRows(sourceIterator, ordering, softLimit);
        }

        @Override
        protected AnnSort withAccess(Access access)
        {
            return Objects.equals(access, this.access)
                ? this
                : new AnnSort(factory, id, source, ordering, access);
        }
    }

    /**
     * Returns all keys in ANN order.
     * Contrary to {@link AnnSort}, there is no input node here and the output is generated lazily.
     */
    static class AnnScan extends Leaf
    {
        final RowFilter.Expression ordering;

        protected AnnScan(Factory factory, int id, RowFilter.Expression ordering, Access access)
        {
            super(factory, id, access);
            this.ordering = ordering;
        }

        @Override
        protected KeysIterationCost estimateCost()
        {
            int keysCount = keysCount();
            int initNodesCount = factory.costEstimator.estimateAnnNodesVisited(ordering,
                                                                               keysCount,
                                                                               factory.tableMetrics.rows);
            double initCost = ANN_OPEN_COST * factory.tableMetrics.sstables + initNodesCount * ANN_NODE_COST;
            return new KeysIterationCost(keysCount,
                                         initCost,
                                         keysCount * CostCoefficients.ANN_SCORED_KEY_COST);
        }

        @Override
        protected Iterator<? extends PrimaryKey> execute(Executor executor)
        {
            // Divide soft limit by 2, so we can terminate search earlier if it
            // occurs that we collected enough rows. keysCount() gives us the
            // average expected number of rows that will be needed but
            // many queries will need fewer than that.
            return executor.getTopKRows(ordering, max(1, keysCount() / 2));
        }

        @Override
        protected KeysIteration withAccess(Access access)
        {
            return Objects.equals(access, this.access)
                   ? this
                   : new AnnScan(factory, id, ordering, access);
        }

        @Override
        protected double estimateSelectivity()
        {
            return 1.0;
        }

        private int keysCount()
        {
            return Math.round((float) access.totalCount(factory.tableMetrics.rows));
        }
    }


    abstract public static class RowsIteration extends Plan
    {
        private RowsIterationCost cost;

        private RowsIteration(Factory factory, int id, Access access)
        {
            super(factory, id, access);
        }

        @Override
        protected RowsIterationCost cost()
        {
            if (cost == null)
                cost = estimateCost();
            return cost;
        }

        protected abstract RowsIterationCost estimateCost();

        protected abstract RowsIteration withAccess(Access patterns);

        final double costPerRow()
        {
            return cost().costPerRow();
        }

        final double expectedRows()
        {
            return cost().expectedRows;
        }
    }

    /**
     * Retrieves rows from storage based on the stream of primary keys
     */
    static final class Fetch extends RowsIteration
    {
        private final LazyTransform<KeysIteration> source;

        private Fetch(Factory factory, int id, KeysIteration keysIteration, Access access)
        {
            super(factory, id, access);
            this.source = new LazyTransform<>(keysIteration, k -> k.withAccess(access));
        }


        @Override
        protected ControlFlow forEachSubplan(Function<Plan, ControlFlow> function)
        {
            return function.apply(source.get());
        }

        @Override
        protected Fetch withUpdatedSubplans(Function<Plan, Plan> updater)
        {
            Plan.KeysIteration updatedSource = (KeysIteration) updater.apply(source.get());
            return updatedSource == source.get() ? this : new Fetch(factory, id, updatedSource, access);
        }

        @Override
        protected double estimateSelectivity()
        {
            return source.orig.selectivity();
        }

        @Override
        protected RowsIterationCost estimateCost()
        {
            double rowFetchCost = CostCoefficients.ROW_COST
                                  + CostCoefficients.ROW_CELL_COST * factory.tableMetrics.avgCellsPerRow
                                  + CostCoefficients.ROW_BYTE_COST * factory.tableMetrics.avgBytesPerRow;

            KeysIteration src = source.get();
            double expectedKeys = access.totalCount(src.expectedKeys());
            return new RowsIterationCost(expectedKeys,
                                         src.initCost(),
                                         src.iterCost() + expectedKeys * rowFetchCost);
        }

        @Override
        protected Fetch withAccess(Access access)
        {
            return Objects.equals(access, this.access)
                ? this
                : new Fetch(factory, id, source.orig, access);
        }
    }

    /**
     * Filters rows.
     * In order to return one row in the result set it may need to retrieve many rows from the source node.
     * Hence, it will typically have higher cost-per-row than the source node, and will return fewer rows.
     */
    static class Filter extends RowsIteration
    {
        private final RowFilter filter;
        private final LazyTransform<RowsIteration> source;
        private final double targetSelectivity;

        Filter(Factory factory, int id, RowFilter filter, RowsIteration source, double targetSelectivity, Access access)
        {
            super(factory, id, access);
            this.filter = filter;
            this.source = new LazyTransform<>(source, this::propagateAccess);
            this.targetSelectivity = targetSelectivity;
        }

        /**
         * Scale the access pattern of the source to reflect that we will need
         * to keep pulling rows from it until the Filter is satisfied.
         */
        private RowsIteration propagateAccess(RowsIteration source)
        {
            Access scaledAccess = access.scaleCount(source.selectivity() / boundedSelectivity(targetSelectivity));
            return source.withAccess(scaledAccess);
        }

        @Override
        protected ControlFlow forEachSubplan(Function<Plan, ControlFlow> function)
        {
            return function.apply(source.get());
        }

        @Override
        protected Plan withUpdatedSubplans(Function<Plan, Plan> updater)
        {
            Plan.RowsIteration updatedSource = (RowsIteration) updater.apply(source.get());
            return updatedSource == source.get()
                   ? this
                   : new Filter(factory, id, filter, updatedSource, targetSelectivity, access);
        }

        @Override
        protected double estimateSelectivity()
        {
            return targetSelectivity;
        }

        @Override
        protected RowsIterationCost estimateCost()
        {
            double expectedRows = access.totalCount(factory.tableMetrics.rows * targetSelectivity);
            return new RowsIterationCost(expectedRows,
                                         source.get().initCost(),
                                         source.get().iterCost());
        }

        @Override
        protected Filter withAccess(Access access)
        {
            return Objects.equals(access, this.access)
                ? this
                : new Filter(factory, id, filter, source.orig, targetSelectivity, access);
        }

        @Override
        protected String description()
        {
            return String.format("%s (sel: %.9f)", filter, selectivity() / source.get().selectivity());
        }
    }

    /**
     * Limits the number of returned rows to a fixed number.
     * Unlike {@link Filter} it does not affect the cost-per-row.
     */
    static class Limit extends RowsIteration
    {
        private final LazyTransform<RowsIteration> source;
        private final long limit;

        private Limit(Factory factory, int id, RowsIteration source, long limit, Access access)
        {
            super(factory, id, access);
            this.limit = limit;
            this.source = new LazyTransform<>(source, s -> s.withAccess(access.limit(limit)));
        }

        @Override
        protected ControlFlow forEachSubplan(Function<Plan, ControlFlow> function)
        {
            return function.apply(source.get());
        }

        @Override
        protected Plan withUpdatedSubplans(Function<Plan, Plan> updater)
        {
            Plan.RowsIteration updatedSource = (RowsIteration) updater.apply(source.get());
            return updatedSource == source.get() ? this : new Limit(factory, id, updatedSource, limit, access);
        }

        @Override
        protected double estimateSelectivity()
        {
            return source.orig.selectivity();
        }

        @Override
        protected RowsIterationCost estimateCost()
        {
            RowsIteration src = source.get();
            double expectedRows = access.totalCount(src.expectedRows());
            double iterCost = (limit >= src.expectedRows())
                              ? src.iterCost()
                              : src.iterCost() * limit / src.expectedRows();
            return new RowsIterationCost(expectedRows, src.initCost(), iterCost);
        }

        @Override
        protected RowsIteration withAccess(Access access)
        {
            return Objects.equals(access, this.access)
                   ? this
                   : new Limit(factory, id, source.orig, limit, access);
        }

        @Override
        protected String description()
        {
            return "" + limit;
        }
    }

    /**
     * Constructs plan nodes.
     * Contains data common for all plan nodes.
     * Performs very lightweight local optimizations.
     * E.g. requesting an intersection/union of only one subplan will result in returning the subplan directly
     * and no intersection/union will be created.
     */
    @NotThreadSafe
    public static final class Factory
    {
        /** Table metrics that affect cost estimates, e.g. row count, sstable count etc */
        public final TableMetrics tableMetrics;

        public final CostEstimator costEstimator;

        /** A plan returning no keys */
        public final KeysIteration nothing;

        /** A plan returning all keys in the table */
        public final KeysIteration everything;

        /** Default access pattern is to read all rows/keys without skipping until the end of the iterator */
        private final Access defaultAccess;

        /** Id of the next new node created by this factory */
        private int nextId = 0;

        /**
         * Creates a factory that produces Plan nodes.
         * @param tableMetrics allows the planner to adapt the cost estimates to the actual amount of data stored in the table
         */
        public Factory(TableMetrics tableMetrics, CostEstimator costEstimator)
        {
            this.tableMetrics = tableMetrics;
            this.costEstimator = costEstimator;
            this.nothing = new Nothing(-1, this);
            this.defaultAccess = Access.sequential(tableMetrics.rows);
            this.everything = new Everything(-1, this, defaultAccess);
        }

        /**
         * Constructs a plan node representing a direct scan of a numeric index.
         * @param predicate the expression matching the rows that we want to search in the index;
         *                  this is needed for identifying this node, it doesn't affect the cost
         * @param matchingKeysCount the number of row keys expected to be returned by the index scan,
         *                        i.e. keys of rows that match the search predicate
         */
        public KeysIteration numericIndexScan(Expression predicate, long matchingKeysCount)
        {
            Preconditions.checkNotNull(predicate, "predicate must not be null");
            Preconditions.checkArgument(matchingKeysCount >= 0, "matchingKeyCount must not be negative");
            Preconditions.checkArgument(matchingKeysCount <= tableMetrics.rows, "matchingKeyCount must not exceed totalKeyCount");
            return new NumericIndexScan(this, nextId++, predicate, matchingKeysCount, defaultAccess);
        }

        /**
         * Constructs a plan node representing a direct scan of a literal index.
         *
         * @param predicate the expression matching the rows that we want to search in the index;
         *                  this is needed for identifying this node, it doesn't affect the cost
         * @param matchingKeysCount the number of row keys expected to be returned by the index scan,
         *                        i.e. keys of rows that match the search predicate - this affects the cost estimates
         */
        public KeysIteration literalIndexScan(Expression predicate, long matchingKeysCount)
        {
            Preconditions.checkNotNull(predicate, "predicate must not be null");
            Preconditions.checkArgument(matchingKeysCount >= 0, "matchingKeyCount must not be negative");
            Preconditions.checkArgument(matchingKeysCount <= tableMetrics.rows, "matchingKeyCount must not exceed totalKeyCount");
            return new LiteralIndexScan(this, nextId++, predicate, matchingKeysCount, defaultAccess);
        }

        /**
         * Constructs a plan node representing a union of two key sets.
         * @param subplans a list of subplans for unioned key sets
         */
        public KeysIteration union(List<KeysIteration> subplans)
        {
            return union(subplans, nextId++);
        }

        private KeysIteration union(List<KeysIteration> subplans, int id)
        {
            if (subplans.contains(everything))
                return everything;
            if (subplans.contains(nothing))
                subplans.removeIf(s -> s == nothing);
            if (subplans.size() == 1)
                return subplans.get(0);
            if (subplans.isEmpty())
                return nothing;

            return new Union(this, id, subplans, defaultAccess);
        }

        /**
         * Constructs a plan node representing an intersection of two key sets.
         * @param subplans a list of subplans for intersected key sets
         */
        public KeysIteration intersection(List<KeysIteration> subplans)
        {
            return intersection(subplans, nextId++);
        }

        private KeysIteration intersection(List<KeysIteration> subplans, int id)
        {
            if (subplans.contains(nothing))
                return nothing;
            if (subplans.contains(everything))
                subplans.removeIf(c -> c == everything);
            if (subplans.size() == 1)
                return subplans.get(0);
            if (subplans.isEmpty())
                return everything;

            subplans.sort(Comparator.comparing(KeysIteration::selectivity));
            return new Intersection(this, id, subplans, defaultAccess);
        }

        public Builder unionBuilder()
        {
            return new Builder(this, Operation.OperationType.OR);
        }

        public Builder intersectionBuilder()
        {
            return new Builder(this, Operation.OperationType.AND);
        }

        /**
         * Constructs a node that sorts keys using DiskANN index
         */
        public KeysIteration annSort(KeysIteration source, RowFilter.Expression ordering)
        {
            return annSort(source, ordering, nextId++);
        }

        private KeysIteration annSort(@Nonnull KeysIteration source, @Nonnull RowFilter.Expression ordering, int id)
        {
            return (source instanceof Everything)
                ? new AnnScan(this, id, ordering, defaultAccess)
                : new AnnSort(this, id, source, ordering, defaultAccess);
        }

        /**
         * Constructs a node that scans the DiskANN index and returns key in ANN order
         */
        public KeysIteration annScan(@Nonnull RowFilter.Expression ordering)
        {
            return new AnnScan(this, nextId++, ordering, defaultAccess);
        }

        /**
         * Constructs a node that lazily fetches the rows from storage, based on the primary key iterator.
         */
        public RowsIteration fetch(@Nonnull KeysIteration keysIterationPlan)
        {
            return new Fetch(this, nextId++, keysIterationPlan, defaultAccess);
        }

        /**
         * Constructs a filter node with fixed target selectivity set to the selectivity of the source node.
         * @see Plan.Factory#filter
         */
        public RowsIteration recheckFilter(@Nonnull RowFilter filter, @Nonnull RowsIteration source)
        {
            return new Filter(this, nextId++, filter, source, source.selectivity(), defaultAccess);
        }

        /**
         * Constructs a filter node with fixed target selectivity.
         * <p>
         * Fixed target selectivity means that the expected number of rows returned by this node is always
         * targetSelectivity/totalRows, regardless of the number of the input rows.
         * Changing the number of the input rows by replacing the subplan
         * with a subplan of different selectivity does not cause this node to return a different number
         * of rows (however, it may change the cost per row estimate).
         * </p><p>
         * This property is useful for constructing so-called "recheck filters" – filters that
         * are not any weaker than the filters in the subplan. If a recheck filter is present, we can freely reduce
         * selectivity of the subplan by e.g. removing intersection nodes, and we still get exactly same number of rows
         * in the result set.
         * </p>
         * @param filter defines which rows are accepted
         * @param source source plan providing the input rows
         * @param targetSelectivity a value in range [0.0, 1.0], but not greater than the selectivity of source
         */
        public RowsIteration filter(@Nonnull RowFilter filter, @Nonnull RowsIteration source, double targetSelectivity)
        {
            RowsIterationCost sourceCost = source.cost();
            Preconditions.checkArgument(targetSelectivity >= 0.0, "selectivity must not be negative");
            Preconditions.checkArgument(targetSelectivity <= source.selectivity(), "selectivity must not exceed source selectivity of " + source.selectivity());
            return new Filter(this, nextId++, filter, source, targetSelectivity, defaultAccess);
        }

        /**
         * Constructs a plan node that fetches only a limited number of rows.
         * It is likely going to have lower fullCost than the fullCost of its input.
         */
        public RowsIteration limit(@Nonnull RowsIteration source, long limit)
        {
            return new Limit(this, nextId++, source, limit, defaultAccess);
        }
    }

    public static class TableMetrics
    {
        public final long rows;
        public final double avgCellsPerRow;
        public final double avgBytesPerRow;
        public final int sstables;

        public TableMetrics(long rows, double avgCellsPerRow, double avgBytesPerRow, int sstables)
        {
            this.rows = rows;
            this.avgCellsPerRow = avgCellsPerRow;
            this.avgBytesPerRow = avgBytesPerRow;
            this.sstables = sstables;
        }
    }

    /**
     * Executes the plan
     */
    public interface Executor
    {
        Iterator<? extends PrimaryKey> getKeysFromIndex(Expression predicate);
        Iterator<? extends PrimaryKey> getTopKRows(RowFilter.Expression ordering, int softLimit);
        Iterator<? extends PrimaryKey> getTopKRows(RangeIterator keys, RowFilter.Expression ordering, int softLimit);
    }

    /**
     * Outsources more complex cost estimates to external components.
     * Some components may collect stats on previous data execution and deliver more accurate estimates based
     * on that state.
     */
    public interface CostEstimator
    {
        /**
         * Returns the expected number of ANN index nodes that must be visited to get the list of candidates for top K.
         *
         * @param ordering   allows to identify the proper index
         * @param limit      number of rows to fetch
         * @param candidates number of candidate rows that satisfy the expression predicates
         */
        int estimateAnnNodesVisited(RowFilter.Expression ordering, int limit, long candidates);
    }

    /**
     * Data-independent cost coefficients.
     * They are likely going to change whenever storage engine algorithms change.
     */
    public static class CostCoefficients
    {
        /** The constant cost of performing skipTo on posting lists returned from range scans */
        public final static double RANGE_SCAN_SKIP_COST = 0.2;

        /** The coefficient controlling the increase of the skip cost with the distance of the skip. */
        public final static double RANGE_SCAN_SKIP_COST_DISTANCE_FACTOR = 0.1;
        public final static double RANGE_SCAN_SKIP_COST_DISTANCE_EXPONENT = 0.5;

        /** The coefficient controlling the increase of the skip cost with the total size of the posting list. */
        public final static double RANGE_SCAN_SKIP_COST_POSTINGS_COUNT_FACTOR = 0.03;
        public final static double RANGE_SCAN_SKIP_COST_POSTINGS_COUNT_EXPONENT = 0.33;

        /** The constant cost of performing skipTo on literal indexes */
        public final static double POINT_LOOKUP_SKIP_COST = 0.5;

        /** The coefficient controlling the increase of the skip cost with the total size of the posting list for point lookup queries. */
        public final static double POINT_LOOKUP_SKIP_COST_DISTANCE_FACTOR = 0.1;
        public final static double POINT_LOOKUP_SKIP_COST_DISTANCE_EXPONENT = 0.5;

        /** Cost to open the per-sstable index, read metadata and obtain the iterators. */
        public final static double SAI_OPEN_COST = 10.0;

        /** Cost to advance the index iterator to the next key and load the key. Common for literal and numeric indexes. */
        public final static double SAI_KEY_COST = 1.0;

        /** Cost to open the vector index and get ready for the search */
        public final static double ANN_OPEN_COST = 10.0;

        /** Additional overhead needed by processing each input key fed to the ANN index searcher */
        public final static double ANN_INPUT_KEY_COST = 3.0;

        /** Cost to get a scored key from DiskANN */
        public final static double ANN_SCORED_KEY_COST = 10.0;

        /** Cost to visit a DiskANN index node */
        public final static double ANN_NODE_COST = 20.0;

        /** Cost to fetch one row from storage */
        public final static double ROW_COST = 80.0;

        /** Additional cost added to row fetch cost per each row cell */
        public final static double ROW_CELL_COST = 0.4;

        /** Additional cost added to row fetch cost per each serialized byte of the row */
        public final static double ROW_BYTE_COST = 0.005;

    }

    /** Convenience builder for building intersection and union nodes */
    public static class Builder
    {
        final Factory factory;
        final Operation.OperationType type;
        final List<KeysIteration> subplans;

        Builder(Factory context, Operation.OperationType type)
        {
            this.factory = context;
            this.type = type;
            this.subplans = new ArrayList<>(4);
        }

        public Builder add(KeysIteration subplan)
        {
            subplans.add(subplan);
            return this;
        }

        public KeysIteration build()
        {
            if (type == Operation.OperationType.AND)
                return factory.intersection(subplans);
            if (type == Operation.OperationType.OR)
                return factory.union(subplans);

            // Should never hit this
            throw new AssertionError("Unexpected builder type: " + type);
        }
    }

    /**
     * how much cheaper is it to perform an in-memory approximate similarity
     * compared to loading the full resolution vector from disk
     * @return 0..1, lower is cheaper
     */
    public static double memoryToDiskFactor()
    {
        double hitRate = ChunkCache.instance == null ? 1.0 : ChunkCache.instance.metrics.hitRate();
        return 0.25 * (Double.isFinite(hitRate) ? max(0.1, hitRate) : 1.0);
    }
    
    /**
     * Describes the usage pattern of the result of the plan node.
     * Modelled by the number of accesses and the distribution of skip distances.
     * Distance = 1.0 means sequential scan over all rows/keys.
     */
    protected static final class Access
    {
        final static Access EMPTY = Access.sequential(0);

        final double[] counts;
        final double[] distances;

        final double totalCount;
        final double totalDistance;
        final boolean forceSkip;


        private Access(double[] count, double[] distance, boolean forceSkip)
        {
            assert count.length == distance.length;
            this.counts = count;
            this.distances = distance;
            this.forceSkip = forceSkip;

            double totalDistance = 0.0;
            double totalCount = 0.0;
            for (int i = 0; i < counts.length; i++)
            {
                totalCount += counts[i];
                totalDistance += counts[i] * distances[i];
            }

            this.totalDistance = totalDistance;
            this.totalCount = totalCount;
        }

        static Access sequential(double count)
        {
            return new Access(new double[] { count }, new double[] { 1.0 }, false);
        }

        /** Scales the counts so that the total count does not exceed given limit */
        Access limit(long limit)
        {
            double totalCount = 0.0;
            for (int i = 0; i < counts.length; i++)
                totalCount += counts[i];

            return limit > totalCount
               ? this
               : this.scaleCount(limit / totalCount);
        }

        /** Multiplies all counts by a constant without changing the distribution */
        Access scaleCount(double factor)
        {
            assert Double.isFinite(factor) : "Count multiplier must not be finite; got " + factor;

            double[] counts = Arrays.copyOf(this.counts, this.counts.length);
            double[] skipDistances = Arrays.copyOf(this.distances, this.distances.length);
            for (int i = 0; i < counts.length; i++)
                counts[i] *= factor;
            return new Access(counts, skipDistances, forceSkip);
        }

        /**
         * Multiplies all skip distances by a constant
         * (if constant is > 1, it spreads accesses further away from each other)
         */
        Access scaleDistance(double factor)
        {
            assert Double.isFinite(factor) : "Distance multiplier must not be finite; got " + factor;

            double[] counts = Arrays.copyOf(this.counts, this.counts.length);
            double[] skipDistances = Arrays.copyOf(this.distances, this.distances.length);
            for (int i = 0; i < counts.length; i++)
                skipDistances[i] *= factor;
            return new Access(counts, skipDistances, forceSkip);
        }

        /**
         * Multiplicates each access at the last level to count accesses with given skip distance.
         * <p>
         * Example (a star denotes a single access):
         * <pre>
         * Access.sequential(4).scaleDistance(6):
         * *     *     *     *
         * Access.sequential(4).scaleDistance(6).convolute(3, 1):
         * ***   ***   ***   ***
         * </pre>
         * */
        Access convolute(double count, double skipDistance)
        {
            assert !Double.isNaN(count) : "Count must not be NaN";
            assert !Double.isNaN(skipDistance) : "Skip distance must not be NaN";

            double[] counts = Arrays.copyOf(this.counts, this.counts.length + 1);
            double[] skipDistances = Arrays.copyOf(this.distances, this.distances.length + 1);

            if (count <= 1.0)
                return scaleCount(count);

            counts[counts.length - 1] = (count - 1) * totalCount;
            skipDistances[skipDistances.length - 1] = skipDistance;

            // Because we added new accesses, we need to adjust the distance of the remaining points
            // in a way that the total distance stays the same:
            for (int i = 0; i < skipDistances.length - 1; i++)
                skipDistances[i] -= (count - 1) * skipDistance;

            return new Access(counts, skipDistances, forceSkip);
        }

        /** Forces using skipTo cost even if skipping distance is not greater than 1 item */
        Access forceSkip()
        {
            return new Access(counts, distances, true);
        }

        /** Returns the total expected number of items (rows or keys) to be retrieved from the node */
        double totalCount(double availableCount)
        {
            return totalCount == 0 || totalDistance <= availableCount
                   ? totalCount
                   : availableCount / totalDistance * totalCount;
        }

        /**
         * Computes the expected cost of fetching one item (row or key).
         * This is computed as an arithmetic mean of costs of skipping by each distance, weighted by counts.
         * @param nextCost the cost of fetching one item from the plan node as a function of the skip distance
         *                 (measured in rows or keys)
         */
        double unitCost(double nextCost, Function<Double, Double> skipCostFn)
        {
            if (totalCount == 0)
                return 0.0;  // we don't want NaNs ;)

            double totalCost = 0.0;
            double totalWeight = 0.0;
            for (int i = 0; i < counts.length; i++)
            {
                double skipCost = (distances[i] > 1.0 || forceSkip) ? skipCostFn.apply(distances[i]) : 0.0;
                totalCost += counts[i] * (nextCost + skipCost);
                totalWeight += counts[i];
            }
            return totalCost / totalWeight;
        }

        public double meanDistance()
        {
            return totalCount > 0.0 ? totalDistance / totalCount : 0.0;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Access that = (Access) o;
            return Arrays.equals(counts, that.counts) && Arrays.equals(distances, that.distances);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(Arrays.hashCode(counts), Arrays.hashCode(distances));
        }
    }

    /**
     * Applies given function to given object lazily, only when the result is needed.
     * Caches the result for subsequent executions.
     */
    static class LazyTransform<T>
    {
        final T orig;
        final Function<T, T> transform;
        private T result;

        LazyTransform(T orig, Function<T, T> transform)
        {
            this.orig = orig;
            this.transform = transform;
        }

        public T get()
        {
            if (result == null)
                result = transform.apply(orig);
            return result;
        }
    }
}
