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

import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.statements.Bound;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.MultiClusteringBuilder;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.index.IndexRegistry;
import org.apache.cassandra.service.ClientState;

/**
 * A set of single restrictions on the partition key.
 * <p>This class can only contain <code>SingleRestriction</code> instances. Token restrictions will be handled by
 * <code>TokenRestriction</code> class or by the <code>TokenFilter</code> class if the query contains a mix of token
 * restrictions and single column restrictions on the partition key.
 */
final class PartitionKeySingleRestrictionSet extends RestrictionSetWrapper implements PartitionKeyRestrictions
{
    /**
     * The composite type.
     */
    private final ClusteringComparator comparator;

    private PartitionKeySingleRestrictionSet(RestrictionSet restrictionSet, ClusteringComparator comparator)
    {
        super(restrictionSet);
        this.comparator = comparator;
    }

    @Override
    public PartitionKeyRestrictions mergeWith(Restriction restriction, IndexRegistry indexRegistry)
    {
        if (restriction.isOnToken())
        {
            if (isEmpty())
                return (PartitionKeyRestrictions) restriction;

            return TokenFilter.create(this, (TokenRestriction) restriction);
        }

        Builder builder = PartitionKeySingleRestrictionSet.builder(comparator);
        List<SingleRestriction> restrictions = restrictions();
        for (int i = 0; i < restrictions.size(); i++)
        {
            SingleRestriction r = restrictions.get(i);
            builder.addRestriction(r);
        }
        return builder.addRestriction(restriction)
                      .build(indexRegistry);
    }

    @Override
    public List<ByteBuffer> values(QueryOptions options, ClientState state)
    {
        MultiClusteringBuilder builder = MultiClusteringBuilder.create(comparator);
        List<SingleRestriction> restrictions = restrictions();
        for (int i = 0; i < restrictions.size(); i++)
        {
            SingleRestriction r = restrictions.get(i);
            r.appendTo(builder, options);

            if (hasIN() && Guardrails.inSelectCartesianProduct.enabled(state))
                Guardrails.inSelectCartesianProduct.guard(builder.buildSize(), "partition key", false, state);

            if (builder.buildIsEmpty())
                break;
        }
        return builder.buildSerializedPartitionKeys();
    }

    @Override
    public List<ByteBuffer> bounds(Bound bound, QueryOptions options)
    {
        MultiClusteringBuilder builder = MultiClusteringBuilder.create(comparator);
        List<SingleRestriction> restrictions = restrictions();
        for (int i = 0; i < restrictions.size(); i++)
        {
            SingleRestriction r = restrictions.get(i);
            r.appendBoundTo(builder, bound, options);
            if (builder.buildIsEmpty())
                return Collections.emptyList();
        }
        return builder.buildSerializedPartitionKeys();
    }

    @Override
    public boolean hasBound(Bound b)
    {
        if (isEmpty())
            return false;
        return restrictions.lastRestriction().hasBound(b);
    }

    @Override
    public boolean isInclusive(Bound b)
    {
        if (isEmpty())
            return false;
        return restrictions.lastRestriction().isInclusive(b);
    }

    @Override
    public void addToRowFilter(RowFilter.Builder filter,
                               IndexRegistry indexRegistry,
                               QueryOptions options)
    {
        List<SingleRestriction> restrictions = restrictions();
        for (int i = 0; i < restrictions.size(); i++)
        {
            SingleRestriction r = restrictions.get(i);
            r.addToRowFilter(filter, indexRegistry, options);
        }
    }

    @Override
    public boolean needFiltering(TableMetadata table)
    {
        if (isEmpty())
            return false;

        // slice or has unrestricted key component
        return hasUnrestrictedPartitionKeyComponents(table) || hasSlice() || hasContains();
    }

    @Override
    public boolean hasUnrestrictedPartitionKeyComponents(TableMetadata table)
    {
        return size() < table.partitionKeyColumns().size();
    }

    public static Builder builder(ClusteringComparator clusteringComparator)
    {
        return new Builder(clusteringComparator);
    }

    public static final class Builder
    {
        private final ClusteringComparator clusteringComparator;

        private final List<Restriction> restrictions = new ArrayList<>();

        private Builder(ClusteringComparator clusteringComparator)
        {
            this.clusteringComparator = clusteringComparator;
        }

        public Builder addRestriction(Restriction restriction)
        {
            restrictions.add(restriction);
            return this;
        }

        public PartitionKeyRestrictions build(IndexRegistry indexRegistry)
        {
            return build(indexRegistry, false);
        }

        public PartitionKeyRestrictions build(IndexRegistry indexRegistry, boolean isDisjunction)
        {
            RestrictionSet.Builder restrictionSet = RestrictionSet.builder();

            for (int i = 0; i < restrictions.size(); i++)
            {
                Restriction restriction = restrictions.get(i);

                // restrictions on tokens are handled in a special way
                if (restriction.isOnToken())
                    return buildWithTokens(restrictionSet, i, indexRegistry);

                restrictionSet.addRestriction((SingleRestriction) restriction, isDisjunction, indexRegistry);
            }

            return buildPartitionKeyRestrictions(restrictionSet);
        }

        private PartitionKeyRestrictions buildWithTokens(RestrictionSet.Builder restrictionSet, int i, IndexRegistry indexRegistry)
        {
            PartitionKeyRestrictions merged = buildPartitionKeyRestrictions(restrictionSet);

            for (; i < restrictions.size(); i++)
            {
                Restriction restriction = restrictions.get(i);

                merged = merged.mergeWith(restriction, indexRegistry);
            }

            return merged;
        }

        private PartitionKeySingleRestrictionSet buildPartitionKeyRestrictions(RestrictionSet.Builder restrictionSet)
        {
            return new PartitionKeySingleRestrictionSet(restrictionSet.build(), clusteringComparator);
        }
    }
}
