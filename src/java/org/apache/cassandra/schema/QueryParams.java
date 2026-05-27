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
package org.apache.cassandra.schema;

import java.util.HashMap;
import java.util.Map;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.exceptions.ConfigurationException;

import static java.lang.String.format;

/**
 * Controls default query parameters for queries run against tables.
 */
public final class QueryParams
{
    public static final String SAI_USE_TERM_STATISTICS = "sai_use_term_statistics";
    public static final String SAI_QUERY_OPTIMIZATION_LEVEL = "sai_query_optimization_level";
    public static final String SAI_INTERSECTION_CLAUSE_LIMIT = "sai_intersection_clause_limit";
    
    public static final QueryParams DEFAULT = new QueryParams(ImmutableMap.of());

    private final ImmutableMap<String, String> options;

    public static QueryParams fromMap(Map<String, String> opts)
    {
        Map<String, String> options = new HashMap<>(opts);
        QueryParams qo = new QueryParams(ImmutableMap.copyOf(options));
        qo.validate();
        return qo;
    }

    private QueryParams(ImmutableMap<String, String> options)
    {
        this.options = options;
    }

    public void validate()
    {
        // We'll be removing recognized options from the map to check for unrecognized options at the end,
        // so make a copy to avoid modifying the original
        Map<String, String> options = new HashMap<>(this.options);

        String saiUseTermStatistics = options.remove(SAI_USE_TERM_STATISTICS);
        if (saiUseTermStatistics != null
            && !saiUseTermStatistics.equalsIgnoreCase("true")
            && !saiUseTermStatistics.equalsIgnoreCase("false"))
        {
                throw new ConfigurationException(format("Invalid value '%s' for option '%s'. Must be 'true' or 'false'.",
                                                       saiUseTermStatistics, SAI_USE_TERM_STATISTICS));
        }

        // Validate sai_query_optimization_level if present (range: 0-1)
        validateIntegerInRange(SAI_QUERY_OPTIMIZATION_LEVEL, options.remove(SAI_QUERY_OPTIMIZATION_LEVEL), 0, 1);

        // Validate sai_intersection_clause_limit if present (range: 1-Integer.MAX_VALUE)
        validateIntegerInRange(SAI_INTERSECTION_CLAUSE_LIMIT, options.remove(SAI_INTERSECTION_CLAUSE_LIMIT), 1, Integer.MAX_VALUE);

        if (!options.isEmpty())
        {
            throw new ConfigurationException(format("Unrecognized query option(s): %s", options.keySet()));
        }
    }

    /**
     * Validates that the value is an integer within the specified range (inclusive).
     * @param optionKey the option key for error messages
     * @param value the value to validate
     * @param min the minimum allowed value (inclusive)
     * @param max the maximum allowed value (inclusive)
     * @throws ConfigurationException if the value is not a valid integer or is outside the range
     */
    private static void validateIntegerInRange(String optionKey, String value, int min, int max)
    {
        if (value != null)
        {
            try
            {
                int intValue = Integer.parseInt(value);
                if (intValue < min || intValue > max)
                {
                    throw new ConfigurationException(format("Invalid value '%s' for option '%s'. Must be between %d and %d (inclusive).",
                                                           value, optionKey, min, max));
                }
            }
            catch (NumberFormatException e)
            {
                throw new ConfigurationException(format("Invalid value '%s' for option '%s'. " +
                                                        "Must be a valid integer between %d and %d (inclusive).",
                                                        value, optionKey, min, max));
            }
        }
    }

    /**
     * Returns the SAI query optimization level.
     * If not set explicitly in the schema, returns the default value from {@link CassandraRelevantProperties}.
     * @see CassandraRelevantProperties#SAI_QUERY_OPTIMIZATION_LEVEL
     */
    public int saiQueryOptimizationLevel()
    {
        String defaultValue = CassandraRelevantProperties.SAI_QUERY_OPTIMIZATION_LEVEL.getString();
        String value = options.getOrDefault(SAI_QUERY_OPTIMIZATION_LEVEL, defaultValue);
        assert value != null;
        return Integer.parseInt(value);
    }

    /**
     * Returns the SAI intersection clause limit.
     * If not set explicitly in the schema, returns the default value from {@link CassandraRelevantProperties}.
     * @see CassandraRelevantProperties#SAI_INTERSECTION_CLAUSE_LIMIT
     */
    public int saiIntersectionClauseLimit()
    {
        String defaultValue = CassandraRelevantProperties.SAI_INTERSECTION_CLAUSE_LIMIT.getString();
        String value = options.getOrDefault(SAI_INTERSECTION_CLAUSE_LIMIT, defaultValue);
        assert value != null;
        return Integer.parseInt(value);
    }

    /**
     * Returns whether SAI should use term statistics for query optimization.
     * If not set explicitly in the schema, returns the default value from {@link CassandraRelevantProperties}.
     * @see CassandraRelevantProperties#SAI_QUERY_OPTIMIZATION_USE_TERM_STATISTICS
     */
    public boolean saiUseTermStatistics()
    {
        String defaultValue = CassandraRelevantProperties.SAI_QUERY_OPTIMIZATION_USE_TERM_STATISTICS.getString();
        String value = options.getOrDefault(SAI_USE_TERM_STATISTICS, defaultValue);
        assert value != null;
        return Boolean.parseBoolean(value);
    }

    public Map<String, String> asMap()
    {
        return options;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;

        if (!(obj instanceof QueryParams))
            return false;

        QueryParams other = (QueryParams) obj;
        return options.equals(other.options);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(options);
    }

    @Override
    public String toString()
    {
        return options.toString();
    }
}

