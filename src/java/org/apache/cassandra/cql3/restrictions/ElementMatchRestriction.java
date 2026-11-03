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

import java.util.List;
import java.util.function.Predicate;

import javax.annotation.Nullable;

import org.apache.cassandra.cql3.FieldPath;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.db.MultiClusteringBuilder;
import org.apache.cassandra.db.filter.ANNOptions;
import org.apache.cassandra.db.filter.IndexHints;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.IndexRegistry;
import org.apache.cassandra.schema.ColumnMetadata;

import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;

/**
 * The restriction of a {@code CONTAINS ELEMENT} group ({@code addrs CONTAINS ELEMENT (city = 'Rome' AND zip = 100)}):
 * some element of a collection of UDTs or tuples satisfies all the conditions of the group, or, for
 * {@code NOT CONTAINS ELEMENT}, no element does (a row without elements satisfies it). The elements are those reached
 * by the {@link #fieldPath() prefix} (see {@link FieldPath#elementsOf}), the conditions are restrictions on the
 * absolute paths under the prefix, as if they were written outside the group ({@code addrs.city = 'Rome'}), but they
 * are evaluated on each element (see {@link RowFilter.ElementMatchExpression}).
 * <p>
 * Indexes: the positive conditions of a {@code CONTAINS ELEMENT} group, each on "some element", select a superset of
 * the rows satisfying the group (the candidates); the negative ones ({@code !=}, {@code NOT IN},
 * {@code NOT CONTAINS}) are element-level, they select nothing. So the group is served by the indexes if they serve
 * enough of its positive conditions to restrict the rows: one of an AND, all of an OR (see {@link #restricts}); the
 * other conditions, and the same-element check, are evaluated on the candidate rows. A {@code NOT CONTAINS ELEMENT}
 * group can't be served by the superset (it isn't subtracted), except if it's a single equality, which is the
 * {@code !=} on its field ({@code addrs NOT CONTAINS ELEMENT (city = 'Rome')} is {@code addrs.city != 'Rome'}): the
 * group is then replaced by that {@code !=} (its {@link #reduced} restriction).
 */
public final class ElementMatchRestriction extends SingleColumnRestriction
{
    public static final String EMPTY_GROUP_MESSAGE =
    "The CONTAINS ELEMENT group has no conditions once its values are bound (e.g. a NOT IN with an empty list): %s";

    private final boolean negated;
    private final Group root;
    // the restriction replacing the group: the != of a NOT CONTAINS ELEMENT group with a single equality
    @Nullable
    private final SingleColumnRestriction reduced;
    // the CQL form of the group, for messages
    private final String cql;

    /**
     * @param columnDef the column of the collection
     * @param prefix the path to the elements of the collection (see {@link FieldPath#elementsOf})
     * @param negated {@code true} for {@code NOT CONTAINS ELEMENT}
     * @param root the conditions on each element
     * @param reduced the restriction replacing the group, if any
     * @param cql the CQL form of the group
     */
    public ElementMatchRestriction(ColumnMetadata columnDef,
                                   FieldPath prefix,
                                   boolean negated,
                                   Group root,
                                   @Nullable SingleColumnRestriction reduced,
                                   String cql)
    {
        super(columnDef, prefix);
        assert !prefix.steps().isEmpty() && prefix.steps().get(prefix.steps().size() - 1).kind == FieldPath.Step.Kind.ELEMENTS;
        this.negated = negated;
        this.root = root;
        this.reduced = reduced;
        this.cql = cql;
    }

    public boolean isNegated()
    {
        return negated;
    }

    @Override
    public String targetName()
    {
        return cql;
    }

    @Override
    public void addFunctionsTo(List<Function> functions)
    {
        root.addFunctionsTo(functions);
    }

    @Override
    protected SingleRestriction doMergeWith(SingleRestriction otherRestriction)
    {
        // the restrictions on paths through collections are never merged (see RestrictionSet.Builder)
        throw new UnsupportedOperationException("CONTAINS ELEMENT restrictions are never merged: " + cql);
    }

    @Override
    MultiColumnRestriction toMultiColumnRestriction()
    {
        throw new UnsupportedOperationException("Cannot convert to multicolumn restriction");
    }

    @Override
    boolean canBeConvertedToMultiColumnRestriction()
    {
        return false;
    }

    @Override
    public MultiClusteringBuilder appendTo(MultiClusteringBuilder builder, QueryOptions options)
    {
        // a restriction on fields never selects clusterings
        throw new UnsupportedOperationException("CONTAINS ELEMENT restrictions don't select clusterings: " + cql);
    }

    @Override
    protected boolean isSupportedBy(Index index)
    {
        if (reduced != null)
            return reduced.isSupportedBy(index);
        return !negated && restricts(root, leaf -> leaf.isSupportedBy(index));
    }

    @Override
    public boolean hasSupportingIndex(IndexRegistry indexRegistry, IndexHints indexHints)
    {
        if (reduced != null)
            return reduced.hasSupportingIndex(indexRegistry, indexHints);
        return !negated && restricts(root, leaf -> leaf.hasSupportingIndex(indexRegistry, indexHints));
    }

    @Override
    @Nullable
    public Index findSupportingIndex(IndexRegistry indexRegistry, IndexHints indexHints)
    {
        // only used for clustering columns, whose fields are never clustering restrictions
        return null;
    }

    @Override
    public boolean needsFiltering(Index.Group indexGroup, IndexHints indexHints)
    {
        if (reduced != null)
            return reduced.needsFiltering(indexGroup, indexHints);
        return negated || !restricts(root, leaf -> !leaf.needsFiltering(indexGroup, indexHints));
    }

    /**
     * Whether the indexes accepted by the given predicate serve enough positive conditions of the group to restrict
     * the rows: in an AND, one of its positive conditions, or one of its children; in an OR, all of its conditions,
     * all of them positive, and all of its children.
     */
    private static boolean restricts(Group group, Predicate<SingleColumnRestriction> served)
    {
        if (group.isDisjunction)
        {
            if (group.leaves.isEmpty() && group.children.isEmpty())
                return false;
            for (Leaf leaf : group.leaves)
                if (!leaf.positive || !served.test(leaf.restriction))
                    return false;
            for (Group child : group.children)
                if (!restricts(child, served))
                    return false;
            return true;
        }

        for (Leaf leaf : group.leaves)
            if (leaf.positive && served.test(leaf.restriction))
                return true;
        for (Group child : group.children)
            if (restricts(child, served))
                return true;
        return false;
    }

    @Override
    public void addToRowFilter(RowFilter.Builder filter,
                               IndexRegistry indexRegistry,
                               QueryOptions options,
                               ANNOptions annOptions,
                               IndexHints indexHints)
    {
        if (reduced != null)
        {
            reduced.addToRowFilter(filter, indexRegistry, options, annOptions, indexHints);
            return;
        }

        RowFilter.FilterElement element = toFilterElement(filter, root, indexRegistry, options, annOptions, indexHints);
        // a NOT IN with an empty list (literal or bound) adds no expression: a group without conditions would be
        // satisfied by any element here, and refused by the replicas that deserialize it
        if (element.isEmpty())
            throw invalidRequest(EMPTY_GROUP_MESSAGE, cql);
        filter.addElementMatch(columnDef, fieldPath, negated, element);
    }

    /**
     * @return the filter of the given group, made of the expressions on the absolute paths of its conditions (see
     * {@link RowFilter.ElementMatchExpression}, which makes them relative to the elements)
     */
    private static RowFilter.FilterElement toFilterElement(RowFilter.Builder filter,
                                                           Group group,
                                                           IndexRegistry indexRegistry,
                                                           QueryOptions options,
                                                           ANNOptions annOptions,
                                                           IndexHints indexHints)
    {
        RowFilter.Builder builder = filter.nestedBuilder(group.isDisjunction);
        for (Leaf leaf : group.leaves)
            leaf.restriction.addToRowFilter(builder, indexRegistry, options, annOptions, indexHints);
        for (Group child : group.children)
            builder.addChild(toFilterElement(filter, child, indexRegistry, options, annOptions, indexHints));
        return builder.buildElement();
    }

    @Override
    public String toString()
    {
        return cql;
    }

    /**
     * A node of the conditions of a group: the AND (or the OR) of restrictions and of other nodes.
     */
    public static final class Group
    {
        final boolean isDisjunction;
        final List<Leaf> leaves;
        final List<Group> children;

        public Group(boolean isDisjunction, List<Leaf> leaves, List<Group> children)
        {
            this.isDisjunction = isDisjunction;
            this.leaves = List.copyOf(leaves);
            this.children = List.copyOf(children);
        }

        /**
         * @return the single condition of this group, {@code null} if it has more than one condition
         */
        @Nullable
        public Leaf singleLeaf()
        {
            if (leaves.size() == 1 && children.isEmpty())
                return leaves.get(0);
            if (leaves.isEmpty() && children.size() == 1)
                return children.get(0).singleLeaf();
            return null;
        }

        private void addFunctionsTo(List<Function> functions)
        {
            for (Leaf leaf : leaves)
                leaf.restriction.addFunctionsTo(functions);
            for (Group child : children)
                child.addFunctionsTo(functions);
        }
    }

    /**
     * A condition of a group: a restriction on the absolute path of a field of the elements, and whether it's
     * positive (it selects the rows where some element satisfies it, a superset of the rows satisfying the group)
     * or negative ({@code !=}, {@code NOT IN}, {@code NOT CONTAINS}).
     */
    public static final class Leaf
    {
        final SingleColumnRestriction restriction;
        final boolean positive;

        public Leaf(SingleColumnRestriction restriction, boolean positive)
        {
            this.restriction = restriction;
            this.positive = positive;
        }

        public SingleColumnRestriction restriction()
        {
            return restriction;
        }
    }
}
