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
 * <p/>
 * CQL: {'sai_use_term_statistics' : 'true'|'false', 'sai_query_optimization_level': '0'|'1', 'sai_intersection_clause_limit': '12345'}
 */
public final class StorageAttachedIndexingParams
{
    public static final String USE_TERM_STATISTICS = "use_term_statistics";
    public static final String QUERY_OPTIMIZATION_LEVEL = "query_optimization_level";
    public static final String INTERSECTION_CLAUSE_LIMIT = "intersection_clause_limit";
    
    public static final StorageAttachedIndexingParams DEFAULT = new StorageAttachedIndexingParams(ImmutableMap.of());

    private final ImmutableMap<String, String> params;

    public static StorageAttachedIndexingParams fromMap(Map<String, String> opts)
    {
        Map<String, String> options = new HashMap<>(opts);
        StorageAttachedIndexingParams qo = new StorageAttachedIndexingParams(ImmutableMap.copyOf(options));
        qo.validate();
        return qo;
    }

    private StorageAttachedIndexingParams(ImmutableMap<String, String> params)
    {
        this.params = params;
    }

    public void validate()
    {
        // We'll be removing recognized options from the map to check for unrecognized options at the end,
        // so make a copy to avoid modifying the original
        Map<String, String> options = new HashMap<>(this.params);

        String useTermStatistics = options.remove(USE_TERM_STATISTICS);
        if (useTermStatistics != null
            && !useTermStatistics.equalsIgnoreCase("true")
            && !useTermStatistics.equalsIgnoreCase("false"))
        {
                throw new ConfigurationException(format("Invalid value '%s' for option '%s'. Must be 'true' or 'false'.",
                                                        useTermStatistics, USE_TERM_STATISTICS));
        }

        // Validate sai_query_optimization_level if present (range: 0-1)
        validateIntegerInRange(QUERY_OPTIMIZATION_LEVEL, options.remove(QUERY_OPTIMIZATION_LEVEL), 0, 1);

        // Validate sai_intersection_clause_limit if present (range: 1-Integer.MAX_VALUE)
        validateIntegerInRange(INTERSECTION_CLAUSE_LIMIT, options.remove(INTERSECTION_CLAUSE_LIMIT), 1, Integer.MAX_VALUE);

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
    private void validateIntegerInRange(String optionKey, String value, int min, int max)
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
                throw new ConfigurationException(format("Invalid value '%s' for option '%s'. Must be a valid integer.",
                                                       value, optionKey));
            }
        }
    }

    /**
     * Returns the SAI query optimization level.
     * If not set explicitly in the schema, returns the default value from {@link CassandraRelevantProperties}.
     * @see CassandraRelevantProperties#SAI_QUERY_OPTIMIZATION_LEVEL
     */
    public int queryOptimizationLevel()
    {
        String defaultValue = CassandraRelevantProperties.SAI_QUERY_OPTIMIZATION_LEVEL.getString();
        String value = params.getOrDefault(QUERY_OPTIMIZATION_LEVEL, defaultValue);
        assert value != null;
        return Integer.parseInt(value);
    }

    /**
     * Returns the SAI intersection clause limit.
     * If not set explicitly in the schema, returns the default value from {@link CassandraRelevantProperties}.
     * @see CassandraRelevantProperties#SAI_INTERSECTION_CLAUSE_LIMIT
     */
    public int intersectionClauseLimit()
    {
        String defaultValue = CassandraRelevantProperties.SAI_INTERSECTION_CLAUSE_LIMIT.getString();
        String value = params.getOrDefault(INTERSECTION_CLAUSE_LIMIT, defaultValue);
        assert value != null;
        return Integer.parseInt(value);
    }

    /**
     * Returns whether SAI should use term statistics for query optimization.
     * If not set explicitly in the schema, returns the default value from {@link CassandraRelevantProperties}.
     * @see CassandraRelevantProperties#SAI_QUERY_OPTIMIZATION_USE_TERM_STATISTICS
     */
    public boolean useTermStatistics()
    {
        String defaultValue = CassandraRelevantProperties.SAI_QUERY_OPTIMIZATION_USE_TERM_STATISTICS.getString();
        String value = params.getOrDefault(USE_TERM_STATISTICS, defaultValue);
        assert value != null;
        return Boolean.parseBoolean(value);
    }

    public Map<String, String> asMap()
    {
        return params;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;

        if (!(obj instanceof StorageAttachedIndexingParams))
            return false;

        StorageAttachedIndexingParams other = (StorageAttachedIndexingParams) obj;
        return params.equals(other.params);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(params);
    }

    @Override
    public String toString()
    {
        return params.toString();
    }
}