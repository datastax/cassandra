/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.db;

import java.util.List;

import org.apache.cassandra.cql3.CqlBuilder;
import org.apache.cassandra.schema.TableMetadata;

/**
 * A {@code ReadQuery} for multiple partitions, restricted by one of more data ranges.
 */
public interface MultiPartitionReadQuery extends ReadQuery
{
    List<DataRange> ranges();

    default void appendCQLWhereClause(CqlBuilder builder)
    {
        List<DataRange> ranges = ranges();
        if (ranges.size() == 1 && ranges.get(0).isUnrestricted() && rowFilter().isEmpty())
            return;

        // Append the data ranges.
        TableMetadata metadata = metadata();
        boolean hasRanges = appendRanges(builder);

        // Append the clustering index filter and the row filter.
        String filter = ranges.get(0).clusteringIndexFilter.toCQLString(metadata, rowFilter());
        if (!filter.isEmpty())
        {
            if (filter.startsWith("ORDER BY"))
                builder.append(" ");
            else if (hasRanges)
                builder.append(" AND ");
            else
                builder.append(" WHERE ");
            builder.append(filter);
        }
    }

    private boolean appendRanges(CqlBuilder builder)
    {
        List<DataRange> ranges = ranges();
        boolean hasRangeRestrictions = false;
        if (ranges().size() == 1)
        {
            String rangeString = ranges.get(0).toCQLString(metadata());
            if (!rangeString.isEmpty())
            {
                builder.append(" WHERE ").append(rangeString);
                hasRangeRestrictions = true;
            }
        }
        else
        {
            builder.append(" WHERE (");
            for (int i = 0; i < ranges.size(); i++)
            {
                if (i > 0)
                    builder.append(" OR ");
                builder.append(ranges.get(i).toCQLString(metadata()));
            }
            builder.append(')');
            hasRangeRestrictions = true;
        }
        return hasRangeRestrictions;
    }
}
