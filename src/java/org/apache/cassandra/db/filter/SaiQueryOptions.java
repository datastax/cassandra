/*
 * Copyright IBM Corp.
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
package org.apache.cassandra.db.filter;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * User-provided directives about query optimizer options should be used by a {@code SELECT} query.
 * See {@code SaiQueryOptions.md} for further details.
 */


public class SaiQueryOptions
{
    private static final Logger logger = LoggerFactory.getLogger(SaiQueryOptions.class);

    public static final String SAI_QUERY_OPTIMIZATION_LEVEL = "sai_query_optimization_level";
    public static final String SAI_INTERSECTION_CLAUSE_LIMIT = "sai_intersection_clause_limit";
    public static final String SAI_USE_TERM_STATISTICS = "sai_use_term_statistics";
    public static final String SAI_HYBRID_SORT_ORDER = "sai_hybrid_sort_order";

    public enum HybridSortOrder
    {
        AUTO,
        SORT_THEN_FILTER,
        FILTER_THEN_SORT;

        public static HybridSortOrder fromString(String s)
        {
            if (s == null)
                return AUTO;
            try
            {
                return valueOf(s.toUpperCase());
            }
            catch (IllegalArgumentException e)
            {
                throw new InvalidRequestException(String.format("Invalid value '%s' for option '%s'. Must be 'auto', 'filter_then_sort', or 'sort_then_filter'.",
                        s, SAI_HYBRID_SORT_ORDER));
            }
        }
    }

    public static final SaiQueryOptions NONE = new SaiQueryOptions(null, null, null, null)
    {
        @Override
        public String toCQLString() {return StringUtils.EMPTY;}

        @Override
        public void validate(ClientState state, String keyspace)
        {
            // no validattion needed for None
        }

    };
    public static final Serializer serializer = new Serializer();

    @Nullable
    public final Integer queryOptimizationLevel;
    @Nullable
    public final HybridSortOrder hybridSortOrder;
    @Nullable
    public final Integer intersectionClauseLimit;
    @Nullable
    public final Boolean useTermStatistics;

    private SaiQueryOptions(@Nullable Integer queryOptimizationLevel,
                            @Nullable Integer intersectionClauseLimit,
                            @Nullable Boolean useTermStatistics,
                            @Nullable HybridSortOrder hybridSortOrder)
    {
        this.queryOptimizationLevel = queryOptimizationLevel;
        this.intersectionClauseLimit = intersectionClauseLimit;
        this.useTermStatistics = useTermStatistics;
        this.hybridSortOrder = hybridSortOrder;
    }

    public static SaiQueryOptions create(@Nullable Integer queryOptimizationLevel, @Nullable Integer intersectionClauseLimit, @Nullable Boolean useTermStatistics, @Nullable HybridSortOrder hybridSortOrder)
    {
        // if all the options are null, return NONE instance
        return queryOptimizationLevel == null && intersectionClauseLimit == null && useTermStatistics == null && hybridSortOrder == null ? NONE : new SaiQueryOptions(queryOptimizationLevel, intersectionClauseLimit, useTermStatistics, hybridSortOrder);
    }

    /**
     * Validates the SAI Query Options by checking that peers support the options.
     */
    public void validate(ClientState state, String keyspace)
    {
       assert keyspace != null;
       Set<InetAddressAndPort> badNodes = MessagingService.instance().endpointsWithConnectionsOnVersionBelow(keyspace, MessagingService.VERSION_DS_20);
       if (MessagingService.current_version < MessagingService.VERSION_DS_20)
           badNodes.add(FBUtilities.getBroadcastAddressAndPort());
       if (!badNodes.isEmpty())
           throw new InvalidRequestException("SAI Query Options are not supported in clusters below DS 20.");
    }

    /**
     *
     *
     * @param map the map of query options in the {@code WITH query_options} of a {@code SELECT} query
     * @return
     */
    public static SaiQueryOptions fromMap(Map<String, String> map)
    {
        Integer queryOptimizationLevel = null;
        Integer intersectionClauseLimit = null;
        Boolean useTermStatistics = null;
        HybridSortOrder hybridSortOrder = null;

        for (Map.Entry<String, String> entry : map.entrySet())
        {
            String key = entry.getKey();
            String value = entry.getValue();

           if (key.equals(SAI_QUERY_OPTIMIZATION_LEVEL))
           {
               queryOptimizationLevel = parseQueryOptimizationLevel((value));
           }
           else if (key.equals(SAI_INTERSECTION_CLAUSE_LIMIT))
           {
               intersectionClauseLimit = parseIntersectionClauseLimit(value);
           }
           else if (key.equals(SAI_USE_TERM_STATISTICS))
           {
               useTermStatistics = parseUseTermStatistics(value);
           }
           else if (key.equals(SAI_HYBRID_SORT_ORDER))
           {
               hybridSortOrder = parseHybridSortOrder(value);
           }
           else
           {
               throw new InvalidRequestException("Unknown SAI query option: " + key);
           }
        }
        return SaiQueryOptions.create(queryOptimizationLevel, intersectionClauseLimit, useTermStatistics, hybridSortOrder);
    }

    // parsing methods

    private static int parseQueryOptimizationLevel(String value)
    {
        int queryOptimizationLevel;
        try
        {
            queryOptimizationLevel = Integer.parseInt(value);
        }
        catch (NumberFormatException e)
        {
            throw new InvalidRequestException(String.format("Invalid '%s' provided. Expected an int value of either 0 or 1 but found '%s'.", SAI_QUERY_OPTIMIZATION_LEVEL, value));
        }
        if (queryOptimizationLevel < 0 || queryOptimizationLevel > 1)
            throw new InvalidRequestException(String.format("Invalid '%s' provided. Expected an int value of either 0 or 1 but found '%s'.", SAI_QUERY_OPTIMIZATION_LEVEL, value));

        return queryOptimizationLevel;
    }

    private static int parseIntersectionClauseLimit(String value)
    {
        int intersectionClauseLimit;
        try
        {
            intersectionClauseLimit = Integer.parseInt(value);
        }
        catch (NumberFormatException e)
        {
            throw new InvalidRequestException(String.format("Invalid '%s' provided. Expected a positive int but found '%s'.",  SAI_INTERSECTION_CLAUSE_LIMIT, value));
        }
        if (intersectionClauseLimit < 1)
            throw new InvalidRequestException(String.format("invalid value '%s' for option '%s'. Must be between 1 and %d (inclusive).",  value, SAI_INTERSECTION_CLAUSE_LIMIT, Integer.MAX_VALUE));

        return intersectionClauseLimit;
    }

    private static boolean parseUseTermStatistics(String value)
    {
        if (value.equalsIgnoreCase("true"))
            return true;
        if (value.equalsIgnoreCase("false"))
            return false;
        throw new InvalidRequestException(String.format("Invalid value '%s' for option '%s'. Must be 'true' or 'false'.",
                                                        value, SAI_USE_TERM_STATISTICS));
    }

    private static HybridSortOrder parseHybridSortOrder(String value)
    {
        return HybridSortOrder.fromString(value);
    }

    public int queryOptimizationLevel()
    {
        return queryOptimizationLevel != null ? queryOptimizationLevel : CassandraRelevantProperties.SAI_QUERY_OPT_LEVEL.getInt();
    }

    public int intersectionClauseLimit()
    {
        return intersectionClauseLimit != null ? intersectionClauseLimit : CassandraRelevantProperties.SAI_INTERSECTION_CLAUSE_LIMIT.getInt();
    }

    public boolean useTermStatistics()
    {
        return useTermStatistics != null ? useTermStatistics : CassandraRelevantProperties.SAI_QUERY_OPTIMIZATION_USE_TERM_STATISTICS.getBoolean();
    }

    public HybridSortOrder hybridSortOrder()
    {
        return hybridSortOrder != null ? hybridSortOrder : HybridSortOrder.AUTO;
    }

    public String toCQLString()
    {
        if (this == NONE)
            return StringUtils.EMPTY;

        List<String> entries = new ArrayList<>();
        if (queryOptimizationLevel != null)
            entries.add(String.format("'%s': %d", SAI_QUERY_OPTIMIZATION_LEVEL, queryOptimizationLevel));
        if (intersectionClauseLimit != null)
            entries.add(String.format("'%s': %d", SAI_INTERSECTION_CLAUSE_LIMIT, intersectionClauseLimit));
        if (useTermStatistics != null)
            entries.add(String.format("'%s': %b", SAI_USE_TERM_STATISTICS, useTermStatistics));
        if (hybridSortOrder != null)
            entries.add(String.format("'%s': '%s'", SAI_HYBRID_SORT_ORDER, hybridSortOrder));

        return '{' + String.join(", ", entries) + '}';
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SaiQueryOptions that = (SaiQueryOptions) o;
        return Objects.equals(queryOptimizationLevel, that.queryOptimizationLevel) &&
               Objects.equals(intersectionClauseLimit, that.intersectionClauseLimit) &&
               Objects.equals(useTermStatistics, that.useTermStatistics) &&
               hybridSortOrder == that.hybridSortOrder;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(queryOptimizationLevel, intersectionClauseLimit, useTermStatistics, hybridSortOrder);
    }

    /**
     * Serializer for {@link SaiQueryOptions}.
     * <p>
     * This serializer writes an int containing bit flags that indicate which options are present, allowing the future
     * addition of new options without increasing the messaging version.
     */
    public static class Serializer
    {
        private static final int QUERY_OPT_LEVEL_MASK = 1;
        private static final int INTERSECTION_CLAUSE_LIMIT_MASK = 2;
        private static final int USE_TERM_STATISTICS_MASK = 4;
        private static final int HYBRID_SORT_ORDER_MASK = 8;
        private static final int UNKNOWN_OPTIONS_MASK = ~(QUERY_OPT_LEVEL_MASK |
                                                          INTERSECTION_CLAUSE_LIMIT_MASK |
                                                          USE_TERM_STATISTICS_MASK |
                                                          HYBRID_SORT_ORDER_MASK);

        public void serialize(SaiQueryOptions options, DataOutputPlus out, int version) throws IOException
        {
            if (version < MessagingService.VERSION_DS_20)
            {
                if (options != NONE)
                    throw new IllegalStateException("Unable to serialize SAI query options with messaging version: " + version);
                return;
            }

            int flags = flags(options);
            out.writeInt(flags);

            if (options.queryOptimizationLevel != null)
                out.writeUnsignedVInt32(options.queryOptimizationLevel);
            if (options.intersectionClauseLimit != null)
                out.writeUnsignedVInt32(options.intersectionClauseLimit);
            if (options.useTermStatistics != null)
                out.writeBoolean(options.useTermStatistics);
            if (options.hybridSortOrder != null)
                out.writeUTF(options.hybridSortOrder.name());
        }

        public SaiQueryOptions deserialize(DataInputPlus in, int version) throws IOException
        {
            if (version < MessagingService.VERSION_DS_20)
                return SaiQueryOptions.NONE;

            int flags = in.readInt();
            if ((flags & UNKNOWN_OPTIONS_MASK) != 0)
                throw new IOException("Found unsupported SAI query options from a newer node.");

            Integer queryOptLevel = (flags & QUERY_OPT_LEVEL_MASK) != 0 ? (int) in.readUnsignedVInt() : null;
            Integer intersectionLimit = (flags & INTERSECTION_CLAUSE_LIMIT_MASK) != 0 ? (int) in.readUnsignedVInt() : null;
            Boolean useTermStats = (flags & USE_TERM_STATISTICS_MASK) != 0 ? in.readBoolean() : null;
            HybridSortOrder sortOrder = (flags & HYBRID_SORT_ORDER_MASK) != 0 ? HybridSortOrder.valueOf(in.readUTF()) : null;

            return SaiQueryOptions.create(queryOptLevel, intersectionLimit, useTermStats, sortOrder);
        }

        public long serializedSize(SaiQueryOptions options, int version)
        {
            if (version < MessagingService.VERSION_DS_20)
                return 0;

            int flags = flags(options);
            long size = TypeSizes.sizeof(flags);

            if (options.queryOptimizationLevel != null)
                size += TypeSizes.sizeofUnsignedVInt(options.queryOptimizationLevel);
            if (options.intersectionClauseLimit != null)
                size += TypeSizes.sizeofUnsignedVInt(options.intersectionClauseLimit);
            if (options.useTermStatistics != null)
                size += TypeSizes.sizeof(options.useTermStatistics);
            if (options.hybridSortOrder != null)
                size += TypeSizes.sizeof(options.hybridSortOrder.name());

            return size;
        }

        private static int flags(SaiQueryOptions options)
        {
            int flags = 0;
            if (options == NONE)
                return flags;

            if (options.queryOptimizationLevel != null)
                flags |= QUERY_OPT_LEVEL_MASK;
            if (options.intersectionClauseLimit != null)
                flags |= INTERSECTION_CLAUSE_LIMIT_MASK;
            if (options.useTermStatistics != null)
                flags |= USE_TERM_STATISTICS_MASK;
            if (options.hybridSortOrder != null)
                flags |= HYBRID_SORT_ORDER_MASK;

            return flags;
        }
    }
}