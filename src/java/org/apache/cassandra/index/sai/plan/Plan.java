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
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.base.Preconditions;

import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.RangeIntersectionIterator;
import org.apache.cassandra.index.sai.utils.RangeIterator;
import org.apache.cassandra.index.sai.utils.RangeUnionIterator;
import org.apache.cassandra.index.sai.utils.TreeFormatter;
import org.apache.cassandra.io.util.FileUtils;

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
 * runtime cost may depend on how many rows are read from the top level node. However, in this design, the cost of
 * the node can depend only on the node itself and its children (subplans), but not the parent nodes. This invariant
 * vastly simplifies the design and allows us to avoid mutating nodes after construction. In order to make
 * the cost of operations like LIMIT correct, the cost computations are performed mostly in terms of marginal costs like
 * cost-per-key or cost-per-row instead of total cost. This way the total cost of execution can be computed by
 * multiplying the marginal costs by the number of rows retrieved. Hence, the LIMIT node and the nodes above it
 * may have a lower full cost than the nodes below it.
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
 * SELECT * FROM table WHERE score1 < 0.001 AND score2 < 0.1 ORDER BY vector ANN OF [34.02, 64.2, ... ] LIMIT 3
 * </pre>
 *
 * can be represented by the following query execution plan:
 * <pre>
 * Limit (sel: 0.000003000, rows: 3.0, cost/row: 105.0, cost: 9600.0..9915.0)
 *  └─ Filter score1 < 0.001 AND score2 < 0.1 (sel: 0.000100000, rows: 100.0, cost/row: 105.0, cost: 9600.0..20100.0)
 *      └─ Fetch (sel: 0.000100000, rows: 100.0, cost/row: 105.0, cost: 9600.0..20100.0)
 *          └─ AnnSort vector (sel: 0.000100000, keys: 100.0, cost/key: 5.0, cost: 9600.0..10100.0)
 *              └─ Intersection (sel: 0.000100000, keys: 100.0, cost/key: 96.0, cost: 0.0..9600.0)
 *                  ├─ NumericIndexScan score1 (sel: 0.001000000, keys: 1000.0, cost/key: 1.0, cost: 0.0..1000.0)
 *                  └─ NumericIndexScan score2 (sel: 0.100000000, keys: 100000.0, cost/key: 1.0, cost: 0.0..100000.0)*
 * </pre>
 */
abstract public class Plan
{
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


    private Plan(Factory factory, int id)
    {
        this.id = id;
        this.factory = factory;
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
     * Returns an object describing detailed cost information about running this plan,
     * e.g. estimates of the number of rows/keys or data size.
     * The actual type of the Cost depends in practice on the type of the result set returned by the node.
     * The results of this method are supposed to be cached. The method is idempotent.
     */
    protected abstract Cost cost();

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
        Plan bestPlanSoFar = this;
        List<Leaf> leaves = nodesOfType(Leaf.class);

        // Remove leaves one by one, starting from the ones with the worst selectivity
        leaves.sort(Comparator.comparingDouble(Plan::selectivity).reversed());
        for (Leaf leaf : leaves)
        {
            Plan candidate = bestPlanSoFar.remove(leaf.id);
            if (candidate.fullCost() <= bestPlanSoFar.fullCost())
                bestPlanSoFar = candidate;
        }
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
        return cost().selectivity();
    }

    protected interface Cost
    {
        double selectivity();
        double initCost();
        double iterCost();
        double fullCost();
    }

    protected static final class KeysIterationCost implements Cost
    {
        final double expectedKeys;
        final double selectivity;
        final double initCost;
        final double iterCost;

        public KeysIterationCost(double expectedKeys, double selectivity, double initCost, double iterCost)
        {
            this.expectedKeys = expectedKeys;
            this.selectivity = selectivity;
            this.initCost = initCost;
            this.iterCost = iterCost;
        }

        @Override
        public double selectivity()
        {
            return selectivity;
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
            return String.format("sel: %.9f, keys: %.1f, cost/key: %.1f, cost: %.1f..%.1f",
                                 selectivity, expectedKeys, costPerKey(), initCost, fullCost());
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            KeysIterationCost that = (KeysIterationCost) o;
            return Double.compare(expectedKeys, that.expectedKeys) == 0
                   && Double.compare(selectivity, that.selectivity) == 0
                   && Double.compare(initCost, that.initCost) == 0
                   && Double.compare(iterCost, that.iterCost) == 0;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(expectedKeys, selectivity, initCost, iterCost);
        }
    }

    protected static final class RowsIterationCost implements Cost
    {
        final double expectedRows;
        final double selectivity;
        final double initCost;
        final double iterCost;


        public RowsIterationCost(double expectedRows, double selectivity, double initCost, double iterCost)
        {
            this.expectedRows = expectedRows;
            this.selectivity = selectivity;
            this.initCost = initCost;
            this.iterCost = iterCost;
        }

        @Override
        public double selectivity()
        {
            return selectivity;
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
            return String.format("sel: %.9f, rows: %.1f, cost/row: %.1f, cost: %.1f..%.1f",
                                 selectivity, expectedRows, costPerRow(), initCost, fullCost());
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            RowsIterationCost that = (RowsIterationCost) o;
            return Double.compare(expectedRows, that.expectedRows) == 0
                   && Double.compare(selectivity, that.selectivity) == 0
                   && Double.compare(initCost, that.initCost) == 0
                   && Double.compare(iterCost, that.iterCost) == 0;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(expectedRows, selectivity, initCost, iterCost);
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

        protected KeysIteration(Factory factory, int id)
        {
            super(factory, id);
        }

        @Override
        protected final KeysIterationCost cost()
        {
            if (cost == null)
                cost = estimateCost();
            return cost;
        }

        protected abstract KeysIterationCost estimateCost();

        /**
         * Estimates the cost of advancing the key set iterator to a different key.
         * This has to be a function, because the skipping cost may depend on the amount of data
         * and the distance of the skip.
         *
         * @param distance distance of the skip expressed as the fraction of the whole key set;
         *                 the distance of 1.0 means jumping to the end of the key stream,
         *                 the distance of 1.0 / expectedKeys() means jumping to the next key.
         * @return estimated cost
         */
        protected abstract double estimateCostPerSkip(double distance);

        protected abstract Iterator<? extends PrimaryKey> execute(Executor executor);


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
        protected Leaf(Factory factory, int id)
        {
            super(factory, id);
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
            super(factory, id);
        }

        @Nonnull
        @Override
        protected KeysIterationCost estimateCost()
        {
            return new KeysIterationCost(0, 0.0, 0.0, 0.0);
        }

        @Override
        protected double estimateCostPerSkip(double distance)
        {
            return 0.0;
        }

        @Override
        protected RangeIterator execute(Executor executor)
        {
            return RangeIterator.empty();
        }
    }

    /**
     * Represents an index scan that returns all keys in the table.
     * This is a virtual node that has no real representation in the database system.
     * It is useful in query optimization.
     */
    static class Everything extends Leaf
    {
        protected Everything(int id, Factory factory)
        {
            super(factory, id);
        }

        @Nonnull
        @Override
        protected KeysIterationCost estimateCost()
        {
            // We set the cost to infinity so this node is never present in the optimized plan.
            // In the future we may want to change it, when we have a way to return all rows without using an index.
            return new KeysIterationCost(factory.tableMetrics.rows,
                                         1.0,
                                         Double.POSITIVE_INFINITY,
                                         Double.POSITIVE_INFINITY);
        }

        @Override
        protected double estimateCostPerSkip(double distance)
        {
            return Double.POSITIVE_INFINITY;
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
    }

    abstract static class IndexScan extends Leaf
    {
        protected final Expression predicate;
        protected final long matchingKeysCount;

        public IndexScan(Factory factory, int id, Expression predicate, long matchingKeysCount)
        {
            super(factory, id);
            this.predicate = predicate;
            this.matchingKeysCount = matchingKeysCount;
        }

        @Override
        protected final String description()
        {
            return "of " + predicate.getIndexName() + " using " + predicate;
        }

        @Override
        protected final KeysIterationCost estimateCost()
        {
            double expectedKeys = (double) matchingKeysCount;
            double costPerKey = CostCoefficients.SAI_KEY_COST;
            double iterCost = expectedKeys * costPerKey;
            double selectivity = factory.tableMetrics.rows > 0 ? (expectedKeys / factory.tableMetrics.rows) : 0.0;
            return new KeysIterationCost(expectedKeys, selectivity, costPerKey, iterCost);
        }

        @Override
        protected final double estimateCostPerSkip(double distance)
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

            double expectedKeysPerSSTable = expectedKeys() / factory.tableMetrics.sstables;

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
            double postingsCountPenalty = dKeys * Math.pow(expectedKeysPerSSTable, postingsCountExponent);

            double distanceInPostings = Math.min(expectedKeys(), distance * expectedKeys());
            // divide by exponent so the derivative at 1.0 equals skipDistanceFactor
            double dPostings = skipDistanceFactor / skipDistanceExponent;
            double distancePenalty = dPostings * Math.pow(distanceInPostings, skipDistanceExponent);

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
        public NumericIndexScan(Factory factory, int id, Expression predicate, long matchingKeysCount)
        {
            super(factory, id, predicate, matchingKeysCount);
        }
    }

    /**
     * Represents a scan over a literal storage attached index
     */
    static class LiteralIndexScan extends IndexScan
    {
        public LiteralIndexScan(Factory factory, int id, Expression predicate, long matchingKeysCount)
        {
            super(factory, id, predicate, matchingKeysCount);
        }
    }

    /**
     * Union of multiple primary key streams.
     * This is a fairly cheap operation - its cost is basically a sum of costs of the subplans.
     */
    static final class Union extends KeysIteration
    {
        private final List<KeysIteration> subplans;

        Union(Factory factory, int id, List<KeysIteration> subplans)
        {
            super(factory, id);
            Preconditions.checkArgument(!subplans.isEmpty(), "Subplans must not be empty");
            this.subplans = Collections.unmodifiableList(subplans);
        }

        @Override
        protected ControlFlow forEachSubplan(Function<Plan, ControlFlow> function)
        {
            for (Plan s : subplans)
            {
                if (function.apply(s) == ControlFlow.Break)
                    return ControlFlow.Break;
            }
            return ControlFlow.Continue;
        }

        @Override
        protected Plan withUpdatedSubplans(Function<Plan, Plan> updater)
        {
            ArrayList<KeysIteration> newSubplans = new ArrayList<>(subplans.size());
            for (Plan subplan : subplans)
                newSubplans.add((KeysIteration) updater.apply(subplan));

            return newSubplans.equals(subplans)
                   ? this
                   : factory.union(newSubplans, id);
        }

        @Override
        protected KeysIterationCost estimateCost()
        {
            double selectivity = 0.0;
            double initCost = 0.0;
            double iterCost = 0.0;
            for (int i = 0; i < subplans.size(); i++)
            {
                KeysIteration subplan = subplans.get(i);
                // Assume independence (lack of correlation) of subplans.
                // We multiply the probabilities of *not* selecting a key multiply.
                selectivity = 1.0 - (1.0 - selectivity) * (1.0 - subplan.selectivity());
                // Initialization must be done all branches before we can start iterating
                initCost += subplan.initCost();
                iterCost += subplan.iterCost();
            }
            double expectedKeys = factory.tableMetrics.rows * selectivity;
            return new KeysIterationCost(expectedKeys, selectivity, initCost, iterCost);
        }

        @Override
        protected RangeIterator execute(Executor executor)
        {
            RangeIterator.Builder builder = RangeUnionIterator.builder();
            try
            {
                for (KeysIteration plan : subplans)
                    builder.add((RangeIterator) plan.execute(executor));
                return builder.build();
            }
            catch (Throwable t)
            {
                FileUtils.closeQuietly(builder.ranges());
                throw t;
            }
        }

        @Override
        protected double estimateCostPerSkip(double distance)
        {
            // Skipping propagates to all the branches equally
            double costPerSkip = 0.0;
            for (KeysIteration plan : subplans)
                costPerSkip += plan.estimateCostPerSkip(distance);
            return costPerSkip;
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
        private final List<KeysIteration> subplans;

        private Intersection(Factory factory, int id, List<KeysIteration> subplans)
        {
            super(factory, id);
            Preconditions.checkArgument(!subplans.isEmpty(), "Subplans must not be empty");
            this.subplans = Collections.unmodifiableList(subplans);
        }

        @Override
        protected ControlFlow forEachSubplan(Function<Plan, ControlFlow> function)
        {
            for (Plan s : subplans)
            {
                if (function.apply(s) == ControlFlow.Break)
                    return ControlFlow.Break;
            }
            return ControlFlow.Continue;
        }

        @Override
        protected Plan withUpdatedSubplans(Function<Plan, Plan> updater)
        {
            ArrayList<KeysIteration> newSubplans = new ArrayList<>(subplans.size());
            for (Plan subplan : subplans)
                newSubplans.add((KeysIteration) updater.apply(subplan));

            return newSubplans.equals(subplans)
                   ? this
                   : factory.intersection(newSubplans, id);
        }

        @Override
        protected KeysIterationCost estimateCost()
        {
            assert !subplans.isEmpty() : "Expected at least one subplan here. An intersection of 0 plans should have been optimized out.";

            double selectivity = 1.0;
            double initCost = 0.0;
            double costPerKey = 0.0;
            for (KeysIteration subplan : subplans)
            {
                selectivity *= subplan.selectivity();
                initCost += subplan.initCost();
                costPerKey += subplan.costPerKey();
            }
            double expectedKeyCount = factory.tableMetrics.rows * selectivity;

            // Compute the additional cost of skipping keys:
            if (subplans.size() > 1)
            {
                Plan.KeysIteration subplan0 = subplans.get(0);

                // The expected number of main loop steps.
                double loops = selectivity != 0
                               ? Math.min(subplan0.expectedKeys(), subplan0.selectivity() / selectivity)
                               : 1.0;

                // To get the first key, we just need to advance the iterator to the next key, but if we don't get a match,
                // all subsequent attempts will cause skipping to the value given by the second iterator.
                costPerKey += Math.max(loops - 1, 0) * (subplan0.estimateCostPerSkip(0.0) + subplan0.costPerKey());

                // Additional cost of skipping on the remaining iterators.
                // Note that we may do fewer skips on the iterator at indexes >= 2 because we short circuit if there is no match.
                double matchProbability = 1.0;
                for (int i = 1; i < subplans.size(); i++)
                {
                    KeysIteration subplan = subplans.get(i);
                    double skipDistance = Math.min(1.0, 1.0 / (Math.max(1.0, subplan0.expectedKeys()) * matchProbability));
                    costPerKey += matchProbability * loops * (subplan.estimateCostPerSkip(skipDistance) + subplan.costPerKey());
                    matchProbability *= subplan.selectivity();
                }
            }

            return new KeysIterationCost(expectedKeyCount, selectivity, initCost, expectedKeyCount * costPerKey);
        }

        @Override
        protected RangeIterator execute(Executor executor)
        {
            RangeIterator.Builder builder = RangeIntersectionIterator.builder();
            try
            {
                for (KeysIteration plan : subplans)
                    builder.add((RangeIterator) plan.execute(executor));

                return builder.build();
            }
            catch (Throwable t)
            {
                FileUtils.closeQuietly(builder.ranges());
                throw t;
            }
        }

        @Override
        protected double estimateCostPerSkip(double distance)
        {
            // Skipping propagates to all the branches equally
            double costPerSkip = 0.0;
            for (KeysIteration plan : subplans)
                costPerSkip += plan.estimateCostPerSkip(distance);
            return costPerSkip;
        }

        /**
         * Limits the number of intersected subplans
         */
        public Plan stripSubplans(int clauseLimit)
        {
            if (subplans.size() <= clauseLimit)
                return this;
            List<Plan.KeysIteration> newSubplans = new ArrayList<>(subplans.subList(0, clauseLimit));
            return factory.intersection(newSubplans, id);
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


        protected AnnSort(Factory factory, int id, KeysIteration source, RowFilter.Expression ordering)
        {
            super(factory, id);
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
            return factory.annSort((KeysIteration) updater.apply(source), ordering, id);
        }

        @Override
        protected KeysIterationCost estimateCost()
        {
            return new KeysIterationCost(source.expectedKeys(),
                                         source.selectivity(),
                                         source.fullCost() + source.expectedKeys() * CostCoefficients.ANN_INPUT_KEY_COST,
                                         source.expectedKeys() * CostCoefficients.ANN_SCORED_KEY_COST);
        }

        @Override
        protected double estimateCostPerSkip(double distance)
        {
            throw new UnsupportedOperationException("AnnSort doesn't support skipping");
        }

        @Override
        protected Iterator<? extends PrimaryKey> execute(Executor executor)
        {
            return executor.getTopKRows((RangeIterator) source.execute(executor), ordering);
        }

    }

    /**
     * Returns all keys in ANN order.
     * Contrary to {@link AnnSort}, there is no input node here and the output is generated lazily.
     */
    static class AnnScan extends Leaf
    {
        final RowFilter.Expression ordering;

        protected AnnScan(Factory factory, int id, RowFilter.Expression ordering)
        {
            super(factory, id);
            this.ordering = ordering;
        }

        @Override
        protected KeysIterationCost estimateCost()
        {
            return new KeysIterationCost(factory.tableMetrics.rows,
                                         1.0,
                                         0.0,
                                         factory.tableMetrics.rows * CostCoefficients.ANN_SCORED_KEY_COST);
        }

        @Override
        protected double estimateCostPerSkip(double distance)
        {
            throw new UnsupportedOperationException("AnnScan doesn't support skipping");
        }

        @Override
        protected Iterator<? extends PrimaryKey> execute(Executor executor)
        {
            return executor.getTopKRows(ordering);
        }
    }


    abstract public static class RowsIteration extends Plan
    {
        private RowsIterationCost cost;

        private RowsIteration(Factory factory, int id)
        {
            super(factory, id);
        }

        @Override
        protected RowsIterationCost cost()
        {
            if (cost == null)
                cost = estimateCost();
            return cost;
        }

        protected abstract RowsIterationCost estimateCost();


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
        private final KeysIteration source;

        private Fetch(Factory factory, int id, KeysIteration keysIteration)
        {
            super(factory, id);
            this.source = keysIteration;
        }


        @Override
        protected ControlFlow forEachSubplan(Function<Plan, ControlFlow> function)
        {
            return function.apply(source);
        }

        @Override
        protected Fetch withUpdatedSubplans(Function<Plan, Plan> updater)
        {
            Plan.KeysIteration updatedSource = (KeysIteration) updater.apply(source);
            return updatedSource == source ? this : new Fetch(factory, id, updatedSource);
        }

        @Override
        protected RowsIterationCost estimateCost()
        {

            double rowFetchCost = CostCoefficients.ROW_COST
                                  + CostCoefficients.ROW_CELL_COST * factory.tableMetrics.avgCellsPerRow
                                  + CostCoefficients.ROW_BYTE_COST * factory.tableMetrics.avgBytesPerRow;

            return new RowsIterationCost(source.expectedKeys(),
                                         source.selectivity(),
                                         source.initCost(),
                                         source.iterCost() + source.expectedKeys() * rowFetchCost);
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
        private final RowsIteration source;
        private final double targetSelectivity;

        Filter(Factory factory, int id, RowFilter filter, RowsIteration source, double targetSelectivity)
        {
            super(factory, id);
            this.filter = filter;
            this.source = source;
            this.targetSelectivity = targetSelectivity;
        }

        @Override
        protected ControlFlow forEachSubplan(Function<Plan, ControlFlow> function)
        {
            return function.apply(source);
        }

        @Override
        protected Plan withUpdatedSubplans(Function<Plan, Plan> updater)
        {
            Plan.RowsIteration updatedSource = (RowsIteration) updater.apply(source);
            return updatedSource == source
                   ? this
                   : new Filter(factory, id, filter, updatedSource, targetSelectivity);
        }

        @Override
        protected RowsIterationCost estimateCost()
        {
            double expectedRows = factory.tableMetrics.rows * targetSelectivity;
            return new RowsIterationCost(expectedRows, targetSelectivity, source.initCost(), source.iterCost());
        }

        @Override
        protected String description()
        {
            return filter.toString();
        }
    }

    /**
     * Limits the number of returned rows to a fixed number.
     * Unlike {@link Filter} it does not affect the cost-per-row.
     */
    static class Limit extends RowsIteration
    {
        private final RowsIteration source;
        private final long limit;

        private Limit(Factory factory, int id, RowsIteration source, long limit)
        {
            super(factory, id);
            this.source = source;
            this.limit = limit;
        }

        @Override
        protected ControlFlow forEachSubplan(Function<Plan, ControlFlow> function)
        {
            return function.apply(source);
        }

        @Override
        protected Plan withUpdatedSubplans(Function<Plan, Plan> updater)
        {
            Plan.RowsIteration updatedSource = (RowsIteration) updater.apply(source);
            return updatedSource == source ? this : new Limit(factory, id, updatedSource, limit);
        }

        @Override
        protected RowsIterationCost estimateCost()
        {
            double expectedRows = Math.min(limit, source.cost().expectedRows);
            double selectivity = expectedRows / factory.tableMetrics.rows;
            double iterCost = (limit >= source.cost().expectedRows)
                              ? source.iterCost()
                              : source.iterCost() * limit / source.expectedRows();
            return new RowsIterationCost(expectedRows, selectivity, source.initCost(), iterCost);
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

        /** A plan returning no keys */
        public final KeysIteration nothing;

        /** A plan returning all keys in the table */
        public final KeysIteration everything;

        /** Id of the next new node created by this factory */
        private int nextId = 0;

        /**
         * Creates a factory that produces Plan nodes.
         * @param tableMetrics allows the planner to adapt the cost estimates to the actual amount of data stored in the table
         */
        public Factory(TableMetrics tableMetrics)
        {
            this.tableMetrics = tableMetrics;
            this.nothing = new Nothing(-1, this);
            this.everything = new Everything(-1, this);
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
            return new NumericIndexScan(this, nextId++, predicate, matchingKeysCount);
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
            return new LiteralIndexScan(this, nextId++, predicate, matchingKeysCount);
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

            return new Union(this, id, subplans);
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

            subplans.sort(Comparator.comparing(KeysIteration::expectedKeys));
            return new Intersection(this, id, subplans);
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
                ? new AnnScan(this, id, ordering)
                : new AnnSort(this, id, source, ordering);
        }

        /**
         * Constructs a node that scans the DiskANN index and returns key in ANN order
         */
        public KeysIteration annScan(@Nonnull RowFilter.Expression ordering)
        {
            return new AnnScan(this, nextId++, ordering);
        }

        /**
         * Constructs a node that lazily fetches the rows from storage, based on the primary key iterator.
         */
        public RowsIteration fetch(@Nonnull KeysIteration keysIterationPlan)
        {
            return new Fetch(this, nextId++, keysIterationPlan);
        }

        /**
         * Constructs a filter node with fixed target selectivity set to the selectivity of the source node.
         * @see Plan.Factory#filter
         */
        public RowsIteration recheckFilter(@Nonnull RowFilter filter, @Nonnull RowsIteration source)
        {
            return new Filter(this, nextId++, filter, source, source.cost().selectivity);
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
            Preconditions.checkArgument(targetSelectivity <= sourceCost.selectivity, "selectivity must not exceed source selectivity of " + sourceCost.selectivity);
            return new Filter(this, nextId++, filter, source, targetSelectivity);
        }

        /**
         * Constructs a plan node that fetches only a limited number of rows.
         * It is likely going to have lower fullCost than the fullCost of its input.
         */
        public RowsIteration limit(@Nonnull RowsIteration source, long limit)
        {
            return new Limit(this, nextId++, source, limit);
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
        Iterator<? extends PrimaryKey> getTopKRows(RowFilter.Expression ordering);
        Iterator<? extends PrimaryKey> getTopKRows(RangeIterator keys, RowFilter.Expression ordering);
    }

    /**
     * Data-independent cost coefficients.
     * They are likely going to change whenever storage engine algorithms change.
     */
    public static class CostCoefficients
    {
        /** The constant cost of performing skipTo on posting lists returned from range scans */
        public final static double RANGE_SCAN_SKIP_COST = 0.3;

        /** The coefficient controlling the increase of the skip cost with the distance of the skip. */
        public final static double RANGE_SCAN_SKIP_COST_DISTANCE_FACTOR = 0.1;
        public final static double RANGE_SCAN_SKIP_COST_DISTANCE_EXPONENT = 0.5;

        /** The coefficient controlling the increase of the skip cost with the total size of the posting list. */
        public final static double RANGE_SCAN_SKIP_COST_POSTINGS_COUNT_FACTOR = 0.03;
        public final static double RANGE_SCAN_SKIP_COST_POSTINGS_COUNT_EXPONENT = 0.33;

        /** The constant cost of performing skipTo on literal indexes */
        public final static double POINT_LOOKUP_SKIP_COST = 0.8;

        /** The coefficient controlling the increase of the skip cost with the total size of the posting list for point lookup queries. */
        public final static double POINT_LOOKUP_SKIP_COST_DISTANCE_FACTOR = 0.1;
        public final static double POINT_LOOKUP_SKIP_COST_DISTANCE_EXPONENT = 0.5;

        /** Cost to advance the index iterator to the next key and load the key. Common for literal and numeric indexes. */
        public final static double SAI_KEY_COST = 1.0;

        /** Additional overhead needed by processing each input key fed to the ANN index searcher */
        public final static double ANN_INPUT_KEY_COST = 5.0;

        /** Cost to get a scored key from DiskANN */
        public final static double ANN_SCORED_KEY_COST = 10.0;

        /** Cost to fetch one row from storage */
        public final static double ROW_COST = 200.0;

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

}
