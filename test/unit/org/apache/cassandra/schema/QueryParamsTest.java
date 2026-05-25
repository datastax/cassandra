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
import java.util.Set;

import org.junit.Ignore;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.exceptions.ConfigurationException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


public class QueryParamsTest extends CQLTester
{
    @Test
    public void testDefaultQueryOptions()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        TableMetadata metadata = currentTableMetadata();
        assertTrue("Default sai_use_term_statistics should be true",
                    metadata.params.queryParams.saiUseTermStatistics());
    }

    @Test
    public void testSaiUseTermStatisticsTrue()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int) WITH query_options = {'sai_use_term_statistics': 'true'}");
        TableMetadata metadata = currentTableMetadata();
        assertTrue("sai_use_term_statistics should be true when set",
                   metadata.params.queryParams.saiUseTermStatistics());
    }

    @Test
    public void testSaiUseTermStatisticsFalse()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int) WITH query_options = {'sai_use_term_statistics': 'false'}");
        TableMetadata metadata = currentTableMetadata();
        assertFalse("sai_use_term_statistics should be false when explicitly set",
                    metadata.params.queryParams.saiUseTermStatistics());
    }

    @Test
    public void testAlterQueryOptions()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        TableMetadata metadata = currentTableMetadata();
        assertTrue("Initial value should be true", metadata.params.queryParams.saiUseTermStatistics());

        alterTable("ALTER TABLE %s WITH query_options = {'sai_use_term_statistics': 'true'}");
        metadata = currentTableMetadata();
        assertTrue("After ALTER, sai_use_term_statistics should be true",
                   metadata.params.queryParams.saiUseTermStatistics());

        alterTable("ALTER TABLE %s WITH query_options = {'sai_use_term_statistics': 'false'}");
        metadata = currentTableMetadata();
        assertFalse("After second ALTER, sai_use_term_statistics should be false",
                    metadata.params.queryParams.saiUseTermStatistics());
    }

    @Test
    public void testQueryOptionsInCqlOutput()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int) WITH query_options = {'sai_use_term_statistics': 'true'}");
        String cql = currentTableMetadata().toCqlString(false, false);
        assertTrue("CQL output should contain query_options", 
                   cql.contains("query_options"));
    }

    @Test
    public void testInvalidSaiUseTermStatisticsValue()
    {
        try
        {
            Map<String, String> options = new HashMap<>();
            options.put(QueryParams.SAI_USE_TERM_STATISTICS, "invalid");
            QueryParams.fromMap(options);
            fail("Should have thrown ConfigurationException for invalid value");
        }
        catch (ConfigurationException e)
        {
            assertTrue("Error message should mention invalid value",
                      e.getMessage().contains("Invalid value"));
        }
    }

    @Test
    public void testQueryParamsFromMap()
    {
        Map<String, String> options = new HashMap<>();
        options.put(QueryParams.SAI_USE_TERM_STATISTICS, "true");
        QueryParams qp = QueryParams.fromMap(options);
        assertTrue("sai_use_term_statistics should be true", qp.saiUseTermStatistics());

        options.put(QueryParams.SAI_USE_TERM_STATISTICS, "false");
        qp = QueryParams.fromMap(options);
        assertFalse("sai_use_term_statistics should be false", qp.saiUseTermStatistics());
    }

    @Test
    public void testDefaultSaiQueryOptimizationLevel()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        TableMetadata metadata = currentTableMetadata();
        // Default value from CassandraRelevantProperties.SAI_QUERY_OPTIMIZATION_LEVEL is "1"
        assertEquals("Default sai_query_optimization_level should be 1", 1, metadata.params.queryParams.saiQueryOptimizationLevel());
    }

    @Test
    public void testSaiQueryOptimizationLevelCustomValue()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int) WITH query_options = {'sai_query_optimization_level': 0}");
        TableMetadata metadata = currentTableMetadata();
        assertEquals("sai_query_optimization_level should be 0 when set", 0, metadata.params.queryParams.saiQueryOptimizationLevel());
    }

    @Test
    public void testAlterSaiQueryOptimizationLevel()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        TableMetadata metadata = currentTableMetadata();
        assertEquals("Initial value should be 1", 1, metadata.params.queryParams.saiQueryOptimizationLevel());

        alterTable("ALTER TABLE %s WITH query_options = {'sai_query_optimization_level': 0}");
        metadata = currentTableMetadata();
        assertEquals("After ALTER, sai_query_optimization_level should be 0", 0, metadata.params.queryParams.saiQueryOptimizationLevel());
    }

    @Test
    public void testInvalidSaiQueryOptimizationLevelValue()
    {
        try
        {
            Map<String, String> options = new HashMap<>();
            options.put(QueryParams.SAI_QUERY_OPTIMIZATION_LEVEL, "invalid");
            QueryParams.fromMap(options);
            fail("Should have thrown ConfigurationException for invalid value");
        }
        catch (ConfigurationException e)
        {
            assertTrue("Error message should mention invalid value",
                      e.getMessage().contains("Invalid value"));
        }
    }

    @Test
    public void testNegativeSaiQueryOptimizationLevelValue()
    {
        try
        {
            Map<String, String> options = new HashMap<>();
            options.put(QueryParams.SAI_QUERY_OPTIMIZATION_LEVEL, "-1");
            QueryParams.fromMap(options);
            fail("Should have thrown ConfigurationException for out of range value");
        }
        catch (ConfigurationException e)
        {
            assertTrue("Error message should mention valid range",
                      e.getMessage().contains("between 0 and 1"));
        }
    }

    @Test
    public void testSaiQueryOptimizationLevelOutOfRange()
    {
        try
        {
            Map<String, String> options = new HashMap<>();
            options.put(QueryParams.SAI_QUERY_OPTIMIZATION_LEVEL, "2");
            QueryParams.fromMap(options);
            fail("Should have thrown ConfigurationException for out of range value");
        }
        catch (ConfigurationException e)
        {
            assertTrue("Error message should mention valid range",
                      e.getMessage().contains("between 0 and 1"));
        }
    }

    @Test
    public void testSaiQueryOptimizationLevelBoundaryValues()
    {
        // Test minimum value (0)
        Map<String, String> options = new HashMap<>();
        options.put(QueryParams.SAI_QUERY_OPTIMIZATION_LEVEL, "0");
        QueryParams qp = QueryParams.fromMap(options);
        assertEquals("sai_query_optimization_level should be 0", 0, qp.saiQueryOptimizationLevel());

        // Test maximum value (1)
        options.put(QueryParams.SAI_QUERY_OPTIMIZATION_LEVEL, "1");
        qp = QueryParams.fromMap(options);
        assertEquals("sai_query_optimization_level should be 1", 1, qp.saiQueryOptimizationLevel());
    }

    @Test
    public void testDefaultSaiIntersectionClauseLimit()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        TableMetadata metadata = currentTableMetadata();
        // Default value from CassandraRelevantProperties.SAI_INTERSECTION_CLAUSE_LIMIT is "2"
        assertEquals("Default sai_intersection_clause_limit should be 2", 2, metadata.params.queryParams.saiIntersectionClauseLimit());
    }

    @Test
    public void testSaiIntersectionClauseLimitCustomValue()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int) WITH query_options = {'sai_intersection_clause_limit': 5}");
        TableMetadata metadata = currentTableMetadata();
        assertEquals("sai_intersection_clause_limit should be 5 when set", 5, metadata.params.queryParams.saiIntersectionClauseLimit());
    }

    @Test
    public void testAlterSaiIntersectionClauseLimit()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        TableMetadata metadata = currentTableMetadata();
        assertEquals("Initial value should be 2", 2, metadata.params.queryParams.saiIntersectionClauseLimit());

        alterTable("ALTER TABLE %s WITH query_options = {'sai_intersection_clause_limit': 10}");
        metadata = currentTableMetadata();
        assertEquals("After ALTER, sai_intersection_clause_limit should be 10", 10, metadata.params.queryParams.saiIntersectionClauseLimit());
    }

    @Test
    public void testInvalidSaiIntersectionClauseLimitValue()
    {
        try
        {
            Map<String, String> options = new HashMap<>();
            options.put(QueryParams.SAI_INTERSECTION_CLAUSE_LIMIT, "invalid");
            QueryParams.fromMap(options);
            fail("Should have thrown ConfigurationException for invalid value");
        }
        catch (ConfigurationException e)
        {
            assertTrue("Error message should mention invalid value",
                      e.getMessage().contains("Invalid value"));
        }
    }

    @Test
    public void testNegativeSaiIntersectionClauseLimitValue()
    {
        try
        {
            Map<String, String> options = new HashMap<>();
            options.put(QueryParams.SAI_INTERSECTION_CLAUSE_LIMIT, "-1");
            QueryParams.fromMap(options);
            fail("Should have thrown ConfigurationException for out of range value");
        }
        catch (ConfigurationException e)
        {
            assertTrue("Error message should mention valid range",
                      e.getMessage().contains("between 1 and"));
        }
    }

    @Test
    public void testZeroSaiIntersectionClauseLimitValue()
    {
        try
        {
            Map<String, String> options = new HashMap<>();
            options.put(QueryParams.SAI_INTERSECTION_CLAUSE_LIMIT, "0");
            QueryParams.fromMap(options);
            fail("Should have thrown ConfigurationException for zero value");
        }
        catch (ConfigurationException e)
        {
            assertTrue("Error message should mention valid range",
                      e.getMessage().contains("between 1 and"));
        }
    }

    @Test
    public void testSaiIntersectionClauseLimitBoundaryValues()
    {
        // Test minimum value (1)
        Map<String, String> options = new HashMap<>();
        options.put(QueryParams.SAI_INTERSECTION_CLAUSE_LIMIT, "1");
        QueryParams qp = QueryParams.fromMap(options);
        assertEquals("sai_intersection_clause_limit should be 1", 1, qp.saiIntersectionClauseLimit());

        // Test a large value
        options.put(QueryParams.SAI_INTERSECTION_CLAUSE_LIMIT, "1000");
        qp = QueryParams.fromMap(options);
        assertEquals("sai_intersection_clause_limit should be 1000", 1000, qp.saiIntersectionClauseLimit());
    }

    @Test
    public void testMultipleQueryOptionsFromMap()
    {
        Map<String, String> options = new HashMap<>();
        options.put(QueryParams.SAI_USE_TERM_STATISTICS, "false");
        options.put(QueryParams.SAI_QUERY_OPTIMIZATION_LEVEL, "0");
        options.put(QueryParams.SAI_INTERSECTION_CLAUSE_LIMIT, "5");
        QueryParams qp = QueryParams.fromMap(options);
        
        assertFalse("sai_use_term_statistics should be false", qp.saiUseTermStatistics());
        assertEquals("sai_query_optimization_level should be 0", 0, qp.saiQueryOptimizationLevel());
        assertEquals("sai_intersection_clause_limit should be 5", 5, qp.saiIntersectionClauseLimit());
    }

    @Test
    @Ignore("Enable this test after we persist query_options in the next release. " +
            "Query_options not persisted yet for backwards compatibility reasons.")
    public void testQueryOptionsPersistedAcrossKeyspaceReload() throws Throwable
    {
        // For this test to pass, one need to add the following line to SchemaKeyspace#addTableParamsToRowBuilder:
        // builder.add("query_options", params.queryParams.asMap());

        // Unfortunately we cannot do that now, because adding a new column data to the system schema is going to
        // break backwards compatibility. So we must add it in two stages. First, we add only the
        // metadata for the query_options column which allows this version to deserialize query_options,
        // and then in the next release we will save the actual data.

        // Create table with custom query options
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int) " +
                    "WITH query_options = {" +
                    "'sai_use_term_statistics': 'false', " +
                    "'sai_query_optimization_level': 0, " +
                    "'sai_intersection_clause_limit': 10}");

        // Verify options are set correctly
        TableMetadata metadata = currentTableMetadata();
        assertFalse("sai_use_term_statistics should be false", metadata.params.queryParams.saiUseTermStatistics());
        assertEquals("sai_query_optimization_level should be 0", 0, metadata.params.queryParams.saiQueryOptimizationLevel());
        assertEquals("sai_intersection_clause_limit should be 10", 10, metadata.params.queryParams.saiIntersectionClauseLimit());

        // Reload keyspace (simulate node restart)
        KeyspaceMetadata reloadedKeyspace = SchemaKeyspace.fetchKeyspaces(Set.of(metadata.keyspace))
                                                          .getNullable(metadata.keyspace);
        assertNotNull(reloadedKeyspace);
        TableMetadata reloadedMetadata = reloadedKeyspace.tables.get(currentTable()).orElse(null);
        assertNotNull("Table should exist in reloaded schema", reloadedMetadata);

        // Verify options are still correct after reload
        assertFalse("sai_use_term_statistics should still be false after reload",
                   reloadedMetadata.params.queryParams.saiUseTermStatistics());
        assertEquals("sai_query_optimization_level should still be 0 after reload",
                    0, reloadedMetadata.params.queryParams.saiQueryOptimizationLevel());
        assertEquals("sai_intersection_clause_limit should still be 10 after reload",
                    10, reloadedMetadata.params.queryParams.saiIntersectionClauseLimit());
    }
}

