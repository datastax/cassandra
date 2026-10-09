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
import org.apache.cassandra.utils.FBUtilities;
import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * User-provided directives that override SAI query-optimizer settings for a single {@code SELECT} query.
 * <p>
 * Instances are immutable; {@link #NONE} is the sentinel used when no options were specified.
 * Each option falls back to the corresponding {@link CassandraRelevantProperties} JVM property when the
 * per-query value is absent, making those defaults dynamically updatable at runtime without a JVM restart.
 * <p>
 * See {@code OptimizerOptions.md} for further details.
 */
public class OptimizerOptions
{
    /** Enables or disables the SAI query optimizer for this query (0 = disabled, 1 = enabled). */
    public static final String QUERY_OPTIMIZATION_LEVEL = "query_optimization_level";
    /** Caps the number of index clauses that may be intersected for this query. */
    public static final String INTERSECTION_CLAUSE_LIMIT = "intersection_clause_limit";
    /** When {@code true}, the optimizer uses per-term statistics for selectivity estimates. */
    public static final String USE_TERM_STATISTICS = "use_term_statistics";
    /** Overrides the optimizer's hybrid sort-order plan choice for this query. */
    public static final String HYBRID_SORT_ORDER = "hybrid_sort_order";

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
                        s, HYBRID_SORT_ORDER));
            }
        }
    }

    public static final OptimizerOptions NONE = new OptimizerOptions(null, null, null, null)
    {
        @Override
        public String toCQLString() {return StringUtils.EMPTY;}

        @Override
        public void validate(String keyspace)
        {
            // no validation needed for NONE
        }
    };
    public static final Serializer serializer = new Serializer();

    /**
     * Per-query optimization level; {@code null} means fall back to
     * {@link CassandraRelevantProperties#SAI_QUERY_OPT_LEVEL}.
     */
    @Nullable
    private final Integer queryOptimizationLevel;
    /**
     * Per-query intersection clause cap; {@code null} means fall back to
     * {@link CassandraRelevantProperties#SAI_INTERSECTION_CLAUSE_LIMIT}.
     */
    @Nullable
    private final Integer intersectionClauseLimit;
    /**
     * Per-query term-statistics flag; {@code null} means fall back to
     * {@link CassandraRelevantProperties#SAI_QUERY_OPTIMIZATION_USE_TERM_STATISTICS}.
     */
    @Nullable
    private final Boolean useTermStatistics;
    /**
     * Per-query hybrid sort-order override; {@code null} means use {@link HybridSortOrder#AUTO}.
     */
    @Nullable
    private final HybridSortOrder hybridSortOrder;

    private OptimizerOptions(@Nullable Integer queryOptimizationLevel,
                             @Nullable Integer intersectionClauseLimit,
                             @Nullable Boolean useTermStatistics,
                             @Nullable HybridSortOrder hybridSortOrder)
    {
        this.queryOptimizationLevel = queryOptimizationLevel;
        this.intersectionClauseLimit = intersectionClauseLimit;
        this.useTermStatistics = useTermStatistics;
        this.hybridSortOrder = hybridSortOrder;
    }

    public static OptimizerOptions create(@Nullable Integer queryOptimizationLevel,
                                          @Nullable Integer intersectionClauseLimit,
                                          @Nullable Boolean useTermStatistics,
                                          @Nullable HybridSortOrder hybridSortOrder)
    {
        // if all the options are null, return NONE instance
        return queryOptimizationLevel == null &&
               intersectionClauseLimit == null &&
               useTermStatistics == null &&
               hybridSortOrder == null
               ? NONE
               : new OptimizerOptions(queryOptimizationLevel, intersectionClauseLimit, useTermStatistics, hybridSortOrder);
    }

    /**
     * Validates the optimizer options by checking that all peers support them.
     */
    public void validate(String keyspace)
    {
        assert keyspace != null;
        Set<InetAddressAndPort> badNodes = MessagingService.instance().endpointsWithConnectionsOnVersionBelow(keyspace, MessagingService.VERSION_DS_20);
        if (MessagingService.current_version < MessagingService.VERSION_DS_20)
            badNodes.add(FBUtilities.getBroadcastAddressAndPort());
        if (!badNodes.isEmpty())
            throw new InvalidRequestException("SAI optimizer options are not supported in clusters below DS 20.");
    }

    /**
     * Parses optimizer options from the map provided in the {@code WITH optimizer_options} clause of a
     * {@code SELECT} query.
     *
     * @param map the map of option key/value pairs
     * @return a new {@link OptimizerOptions} instance, or {@link #NONE} if the map is empty
     */
    public static OptimizerOptions fromMap(Map<String, String> map)
    {
        Integer queryOptimizationLevel = null;
        Integer intersectionClauseLimit = null;
        Boolean useTermStatistics = null;
        HybridSortOrder hybridSortOrder = null;

        for (Map.Entry<String, String> entry : map.entrySet())
        {
            String key = entry.getKey();
            String value = entry.getValue();

            switch (key)
            {
                case QUERY_OPTIMIZATION_LEVEL:
                    queryOptimizationLevel = parseQueryOptimizationLevel(value);
                    break;
                case INTERSECTION_CLAUSE_LIMIT:
                    intersectionClauseLimit = parseIntersectionClauseLimit(value);
                    break;
                case USE_TERM_STATISTICS:
                    useTermStatistics = parseUseTermStatistics(value);
                    break;
                case HYBRID_SORT_ORDER:
                    hybridSortOrder = HybridSortOrder.fromString(value);
                    break;
                default:
                    throw new InvalidRequestException("Unknown SAI optimizer option: " + key);
            }
        }
        return OptimizerOptions.create(queryOptimizationLevel, intersectionClauseLimit, useTermStatistics, hybridSortOrder);
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
            throw new InvalidRequestException(String.format("Invalid '%s' provided. Expected an int value of either 0 or 1 but found '%s'.", QUERY_OPTIMIZATION_LEVEL, value));
        }
        if (queryOptimizationLevel < 0 || queryOptimizationLevel > 1)
            throw new InvalidRequestException(String.format("Invalid '%s' provided. Expected an int value of either 0 or 1 but found '%s'.", QUERY_OPTIMIZATION_LEVEL, value));

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
            throw new InvalidRequestException(String.format("Invalid '%s' provided. Expected a positive int but found '%s'.", INTERSECTION_CLAUSE_LIMIT, value));
        }
        if (intersectionClauseLimit < 1)
            throw new InvalidRequestException(String.format("Invalid value '%s' for option '%s'. Must be between 1 and %d (inclusive).", value, INTERSECTION_CLAUSE_LIMIT, Integer.MAX_VALUE));

        return intersectionClauseLimit;
    }

    private static boolean parseUseTermStatistics(String value)
    {
        if (value.equalsIgnoreCase("true"))
            return true;
        if (value.equalsIgnoreCase("false"))
            return false;
        throw new InvalidRequestException(String.format("Invalid value '%s' for option '%s'. Must be 'true' or 'false'.",
                                                        value, USE_TERM_STATISTICS));
    }

    /**
     * Returns the effective optimization level: the per-query override if set, otherwise the value of the
     * {@code cassandra.sai.query.optimization.level} system property (dynamically updatable at runtime).
     */
    public int queryOptimizationLevel()
    {
        return queryOptimizationLevel != null ? queryOptimizationLevel : CassandraRelevantProperties.SAI_QUERY_OPT_LEVEL.getInt();
    }

    /**
     * Returns the effective intersection clause limit: the per-query override if set, otherwise the value of the
     * {@code cassandra.sai.intersection_clause_limit} system property (dynamically updatable at runtime).
     */
    public int intersectionClauseLimit()
    {
        return intersectionClauseLimit != null ? intersectionClauseLimit : CassandraRelevantProperties.SAI_INTERSECTION_CLAUSE_LIMIT.getInt();
    }

    /**
     * Returns the effective term-statistics flag: the per-query override if set, otherwise the value of the
     * {@code cassandra.sai.query_optimization.use_term_statistics} system property (dynamically updatable at runtime).
     */
    public boolean useTermStatistics()
    {
        return useTermStatistics != null ? useTermStatistics : CassandraRelevantProperties.SAI_QUERY_OPTIMIZATION_USE_TERM_STATISTICS.getBoolean();
    }

    /**
     * Returns the effective hybrid sort-order: the per-query override if set, otherwise {@link HybridSortOrder#AUTO}.
     */
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
            entries.add(String.format("'%s': %d", QUERY_OPTIMIZATION_LEVEL, queryOptimizationLevel));
        if (intersectionClauseLimit != null)
            entries.add(String.format("'%s': %d", INTERSECTION_CLAUSE_LIMIT, intersectionClauseLimit));
        if (useTermStatistics != null)
            entries.add(String.format("'%s': %b", USE_TERM_STATISTICS, useTermStatistics));
        if (hybridSortOrder != null)
            entries.add(String.format("'%s': '%s'", HYBRID_SORT_ORDER, hybridSortOrder));

        return '{' + String.join(", ", entries) + '}';
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (!(o instanceof OptimizerOptions)) return false;
        OptimizerOptions that = (OptimizerOptions) o;
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
     * Serializer for {@link OptimizerOptions}.
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

        public void serialize(OptimizerOptions options, DataOutputPlus out, int version) throws IOException
        {
            if (version < MessagingService.VERSION_DS_20)
            {
                if (options != NONE)
                    throw new IllegalStateException("Unable to serialize SAI optimizer options with messaging version: " + version);
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

        public OptimizerOptions deserialize(DataInputPlus in, int version) throws IOException
        {
            if (version < MessagingService.VERSION_DS_20)
                return OptimizerOptions.NONE;

            int flags = in.readInt();
            if ((flags & UNKNOWN_OPTIONS_MASK) != 0)
                throw new IOException("Found unsupported SAI optimizer options from a newer node.");

            Integer queryOptLevel = (flags & QUERY_OPT_LEVEL_MASK) != 0 ? (int) in.readUnsignedVInt() : null;
            Integer intersectionLimit = (flags & INTERSECTION_CLAUSE_LIMIT_MASK) != 0 ? (int) in.readUnsignedVInt() : null;
            Boolean useTermStats = (flags & USE_TERM_STATISTICS_MASK) != 0 ? in.readBoolean() : null;
            HybridSortOrder sortOrder = (flags & HYBRID_SORT_ORDER_MASK) != 0 ? HybridSortOrder.valueOf(in.readUTF()) : null;

            return OptimizerOptions.create(queryOptLevel, intersectionLimit, useTermStats, sortOrder);
        }

        public long serializedSize(OptimizerOptions options, int version)
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

        private static int flags(OptimizerOptions options)
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
