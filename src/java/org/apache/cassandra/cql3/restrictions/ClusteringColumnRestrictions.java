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

import java.util.*;

import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.statements.Bound;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ANNOptions;
import org.apache.cassandra.db.filter.IndexHints;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.IndexRegistry;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.utils.btree.BTreeSet;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkFalse;
import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;

/**
 * A set of restrictions on the clustering key.
 */
final class ClusteringColumnRestrictions extends RestrictionSetWrapper
{
    /**
     * The composite type.
     */
    private final ClusteringComparator comparator;

    /**
     * The restrictions served by an index normalizing the values (e.g. a case-insensitive SAI index), which are
     * applied by the index, on the normalized values, rather than as clustering slices or names, on the raw values.
     */
    private final Set<SingleRestriction> normalizedByIndex;

    /**
     * The {@code !=} restrictions that no index serves but whose excluded values are normalized like those of an index
     * on the column (see {@link RowFilter#analyzerFor}). Their slices exclude the raw values, which are among the
     * normalized ones, and they're also added to the row filter, if any, to exclude the normalized values.
     */
    private final Set<SingleRestriction> normalizedExclusions;

    private ClusteringColumnRestrictions(ClusteringComparator comparator,
                                         RestrictionSet restrictionSet,
                                         Set<SingleRestriction> normalizedByIndex,
                                         Set<SingleRestriction> normalizedExclusions)
    {
        super(restrictionSet);
        this.comparator = comparator;
        this.normalizedByIndex = normalizedByIndex;
        this.normalizedExclusions = normalizedExclusions;
    }

    /**
     * @return whether some restrictions are applied by an index normalizing the values rather than as clustering
     * slices or names
     */
    public boolean hasRestrictionsNormalizedByIndex()
    {
        return !normalizedByIndex.isEmpty();
    }

    /**
     * @return whether some {@code !=} restrictions exclude normalized values when they're added to the row filter
     */
    public boolean hasNormalizedExclusions()
    {
        return !normalizedExclusions.isEmpty();
    }

    public NavigableSet<Clustering<?>> valuesAsClustering(QueryOptions options, ClientState state) throws InvalidRequestException
    {
        MultiClusteringBuilder builder = MultiClusteringBuilder.create(comparator);
        for (SingleRestriction restriction : restrictions())
        {
            restriction.appendTo(builder, options);

            if (hasIN() && Guardrails.inSelectCartesianProduct.enabled(state))
                Guardrails.inSelectCartesianProduct.guard(builder.buildSize(), "clustering key", false, state);

            if (builder.buildIsEmpty())
                break;
        }
        return builder.build();
    }

    public NavigableSet<ClusteringBound<?>> boundsAsClustering(Bound bound, QueryOptions options) throws InvalidRequestException
    {
        MultiClusteringBuilder builder = MultiClusteringBuilder.create(comparator);
        int keyPosition = 0;

        for (SingleRestriction restriction : restrictions())
        {
            if (handleInFilter(restriction, keyPosition))
                break;

            restriction.appendBoundTo(builder, bound, options);

            if (builder.buildIsEmpty())
                return BTreeSet.empty(comparator);

            // We allow slice restriction only on the last clustering column restricted by the query.
            // Any further column restrictions must be handled by indexes or filtering.
            if (restriction.isSlice())
                break;

            keyPosition = restriction.getLastColumn().position() + 1;
        }

        return builder.buildBound(bound.isStart());
    }

    /**
     * Checks if underlying restrictions would require filtering
     *
     * @return <code>true</code> if any underlying restrictions require filtering, <code>false</code>
     * otherwise
     */
    public boolean needFiltering()
    {
        int position = 0;

        for (SingleRestriction restriction : restrictions())
        {
            if (handleInFilter(restriction, position))
                return true;

            if (!restriction.isSlice())
                position = restriction.getLastColumn().position() + 1;
        }
        return hasContains();
    }

    @Override
    public void addToRowFilter(RowFilter.Builder filter,
                               IndexRegistry indexRegistry,
                               QueryOptions options,
                               ANNOptions annOptions,
                               IndexHints indexHints) throws InvalidRequestException
    {
        int position = 0;

        for (SingleRestriction restriction : restrictions())
        {
            // We ignore all the clustering columns that can be handled by slices.
            if (handleInFilter(restriction, position)
                || restriction.hasSupportingIndex(indexRegistry, indexHints)
                || normalizedExclusions.contains(restriction))
            {
                restriction.addToRowFilter(filter, indexRegistry, options, annOptions, indexHints);
            }
            else if (!restriction.isSlice())
            {
                position = restriction.getLastColumn().position() + 1;
            }
        }
    }

    private boolean handleInFilter(SingleRestriction restriction, int index)
    {
        return restriction.isContains()
               || restriction.isLIKE()
               || index != restriction.getFirstColumn().position()
               || normalizedByIndex.contains(restriction);
    }

    public static ClusteringColumnRestrictions.Builder builder(TableMetadata table, boolean allowFiltering)
    {
        return new Builder(table, allowFiltering, null, IndexHints.NONE);
    }

    public static ClusteringColumnRestrictions.Builder builder(TableMetadata table,
                                                               boolean allowFiltering,
                                                               IndexRegistry indexRegistry,
                                                               IndexHints indexHints)
    {
        return new Builder(table, allowFiltering, indexRegistry, indexHints);
    }

    public static class Builder
    {
        private final TableMetadata table;
        private final boolean allowFiltering;
        private final IndexRegistry indexRegistry;
        private final IndexHints indexHints;

        private final RestrictionSet.Builder restrictions = RestrictionSet.builder();

        private Builder(TableMetadata table, boolean allowFiltering, IndexRegistry indexRegistry, IndexHints indexHints)
        {
            this.table = table;
            this.allowFiltering = allowFiltering;
            this.indexRegistry = indexRegistry;
            this.indexHints = indexHints;
        }

        public ClusteringColumnRestrictions.Builder addRestriction(Restriction restriction)
        {
            return addRestriction(restriction, false);
        }

        public ClusteringColumnRestrictions.Builder addRestriction(Restriction restriction, boolean isDisjunction)
        {
            SingleRestriction newRestriction = (SingleRestriction) restriction;
            boolean isEmpty = restrictions.isEmpty();

            if (!isEmpty && !allowFiltering && (indexRegistry == null || !newRestriction.hasSupportingIndex(indexRegistry, indexHints)))
            {
                SingleRestriction lastRestriction = restrictions.lastRestriction();
                ColumnMetadata lastRestrictionStart = lastRestriction.getFirstColumn();
                ColumnMetadata newRestrictionStart = newRestriction.getFirstColumn();
                restrictions.addRestriction(newRestriction, isDisjunction);

                checkFalse(lastRestriction.isSlice() && newRestrictionStart.position() > lastRestrictionStart.position(),
                           "Clustering column \"%s\" cannot be restricted (preceding column \"%s\" is restricted by a non-EQ relation)",
                           newRestrictionStart.name,
                           lastRestrictionStart.name);

                if (newRestrictionStart.position() < lastRestrictionStart.position() && newRestriction.isSlice())
                    throw invalidRequest("PRIMARY KEY column \"%s\" cannot be restricted (preceding column \"%s\" is restricted by a non-EQ relation)",
                                         restrictions.nextColumn(newRestrictionStart).name,
                                         newRestrictionStart.name);
            }
            else
            {
                restrictions.addRestriction(newRestriction, isDisjunction);
            }

            return this;
        }

        public ClusteringColumnRestrictions build()
        {
            return build(false);
        }

        /**
         * @param useIndexes whether the restrictions can be served by the indexes (as in {@code SELECT} statements),
         * and so be applied as the indexes normalizing the values do
         */
        public ClusteringColumnRestrictions build(boolean useIndexes)
        {
            RestrictionSet restrictionSet = restrictions.build();
            Set<SingleRestriction> normalizedByIndex = Collections.newSetFromMap(new IdentityHashMap<>());
            Set<SingleRestriction> normalizedExclusions = Collections.newSetFromMap(new IdentityHashMap<>());
            if (useIndexes && indexRegistry != null)
            {
                for (SingleRestriction restriction : restrictionSet.restrictions())
                {
                    if (!(restriction instanceof SingleColumnRestriction))
                        continue;

                    Index index = ((SingleColumnRestriction) restriction).findSupportingIndex(indexRegistry, indexHints);
                    if (index != null && index.isAnalyzed())
                        normalizedByIndex.add(restriction);
                    else if (index == null
                             && restriction.isSlice()
                             && RowFilter.analyzerFor(indexRegistry, restriction.getFirstColumn(), Operator.NEQ, null, indexHints) != null)
                        normalizedExclusions.add(restriction);
                }
            }
            return new ClusteringColumnRestrictions(table.comparator, restrictionSet, normalizedByIndex, normalizedExclusions);
        }
    }
}
