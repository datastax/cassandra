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

    default void appendCQLWhereClause(CqlBuilder builder, boolean redact)
    {
        // Append the data ranges.
        TableMetadata metadata = metadata();
        boolean hasRanges = appendRanges(builder, redact);

        // Append the clustering index filter and the row filter.
        String filter = ranges().get(0).clusteringIndexFilter.toCQLString(metadata, rowFilter(), redact);
        builder.appendRestrictions(filter, hasRanges);
    }

    private boolean appendRanges(CqlBuilder builder, boolean redact)
    {
        List<DataRange> ranges = ranges();
        if (ranges.size() == 1)
        {
            DataRange range = ranges.get(0);
            if (range.isUnrestricted(metadata()))
                return false;

            String rangeString = range.toCQLString(metadata(), rowFilter(), redact);
            if (!rangeString.isEmpty())
            {
                builder.append(" WHERE ").append(rangeString);
                return true;
            }
        }
        else
        {
            builder.append(" WHERE ").append('(');
            for (int i = 0; i < ranges.size(); i++)
            {
                if (i > 0)
                    builder.append(" OR ");
                builder.append(ranges.get(i).toCQLString(metadata(), rowFilter(), redact));
            }
            builder.append(')');
            return true;
        }
        return false;
    }
}
