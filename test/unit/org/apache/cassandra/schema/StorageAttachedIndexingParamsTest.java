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


public class StorageAttachedIndexingParamsTest extends CQLTester
{
    @Test
    public void testDefaultQueryOptions()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        TableMetadata metadata = currentTableMetadata();
        assertTrue("Default use_term_statistics should be true",
                    metadata.params.storageAttachedIndexingParams.useTermStatistics());
    }

    @Test
    public void testUseTermStatisticsTrue()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int) WITH storage_attached_indexing = {'use_term_statistics': 'true'}");
        TableMetadata metadata = currentTableMetadata();
        assertTrue("use_term_statistics should be true when set",
                   metadata.params.storageAttachedIndexingParams.useTermStatistics());
    }

    @Test
    public void testUseTermStatisticsFalse()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int) WITH storage_attached_indexing = {'use_term_statistics': 'false'}");
        TableMetadata metadata = currentTableMetadata();
        assertFalse("use_term_statistics should be false when explicitly set",
                    metadata.params.storageAttachedIndexingParams.useTermStatistics());
    }

    @Test
    public void testAlterQueryOptions()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        TableMetadata metadata = currentTableMetadata();
        assertTrue("Initial value should be true", metadata.params.storageAttachedIndexingParams.useTermStatistics());

        alterTable("ALTER TABLE %s WITH storage_attached_indexing = {'use_term_statistics': 'true'}");
        metadata = currentTableMetadata();
        assertTrue("After ALTER, use_term_statistics should be true",
                   metadata.params.storageAttachedIndexingParams.useTermStatistics());

        alterTable("ALTER TABLE %s WITH storage_attached_indexing = {'use_term_statistics': 'false'}");
        metadata = currentTableMetadata();
        assertFalse("After second ALTER, use_term_statistics should be false",
                    metadata.params.storageAttachedIndexingParams.useTermStatistics());
    }

    @Test
    public void testQueryOptionsInCqlOutput()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int) WITH storage_attached_indexing = {'use_term_statistics': 'true'}");
        String cql = currentTableMetadata().toCqlString(false, false);
        assertTrue("CQL output should contain storage_attached_indexing", 
                   cql.contains("storage_attached_indexing"));
    }

    @Test
    public void testInvalidUseTermStatisticsValue()
    {
        try
        {
            Map<String, String> options = new HashMap<>();
            options.put(StorageAttachedIndexingParams.USE_TERM_STATISTICS, "invalid");
            StorageAttachedIndexingParams.fromMap(options);
            fail("Should have thrown ConfigurationException for invalid value");
        }
        catch (ConfigurationException e)
        {
            assertTrue("Error message should mention invalid value",
                      e.getMessage().contains("Invalid value"));
        }
    }

    @Test
    public void testStorageAttachedIndexingParamsFromMap()
    {
        Map<String, String> options = new HashMap<>();
        options.put(StorageAttachedIndexingParams.USE_TERM_STATISTICS, "true");
        StorageAttachedIndexingParams qp = StorageAttachedIndexingParams.fromMap(options);
        assertTrue("use_term_statistics should be true", qp.useTermStatistics());

        options.put(StorageAttachedIndexingParams.USE_TERM_STATISTICS, "false");
        qp = StorageAttachedIndexingParams.fromMap(options);
        assertFalse("use_term_statistics should be false", qp.useTermStatistics());
    }

    @Test
    public void testDefaultQueryOptimizationLevel()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        TableMetadata metadata = currentTableMetadata();
        // Default value from CassandraRelevantProperties.SAI_QUERY_OPTIMIZATION_LEVEL is "1"
        assertEquals("Default query_optimization_level should be 1", 1, metadata.params.storageAttachedIndexingParams.queryOptimizationLevel());
    }

    @Test
    public void testQueryOptimizationLevelCustomValue()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int) WITH storage_attached_indexing = {'query_optimization_level': 0}");
        TableMetadata metadata = currentTableMetadata();
        assertEquals("query_optimization_level should be 0 when set", 0, metadata.params.storageAttachedIndexingParams.queryOptimizationLevel());
    }

    @Test
    public void testAlterQueryOptimizationLevel()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        TableMetadata metadata = currentTableMetadata();
        assertEquals("Initial value should be 1", 1, metadata.params.storageAttachedIndexingParams.queryOptimizationLevel());

        alterTable("ALTER TABLE %s WITH storage_attached_indexing = {'query_optimization_level': 0}");
        metadata = currentTableMetadata();
        assertEquals("After ALTER, query_optimization_level should be 0", 0, metadata.params.storageAttachedIndexingParams.queryOptimizationLevel());
    }

    @Test
    public void testInvalidQueryOptimizationLevelValue()
    {
        try
        {
            Map<String, String> options = new HashMap<>();
            options.put(StorageAttachedIndexingParams.QUERY_OPTIMIZATION_LEVEL, "invalid");
            StorageAttachedIndexingParams.fromMap(options);
            fail("Should have thrown ConfigurationException for invalid value");
        }
        catch (ConfigurationException e)
        {
            assertTrue("Error message should mention invalid value",
                      e.getMessage().contains("Invalid value"));
        }
    }

    @Test
    public void testNegativeQueryOptimizationLevelValue()
    {
        try
        {
            Map<String, String> options = new HashMap<>();
            options.put(StorageAttachedIndexingParams.QUERY_OPTIMIZATION_LEVEL, "-1");
            StorageAttachedIndexingParams.fromMap(options);
            fail("Should have thrown ConfigurationException for out of range value");
        }
        catch (ConfigurationException e)
        {
            assertTrue("Error message should mention valid range",
                      e.getMessage().contains("between 0 and 1"));
        }
    }

    @Test
    public void testQueryOptimizationLevelOutOfRange()
    {
        try
        {
            Map<String, String> options = new HashMap<>();
            options.put(StorageAttachedIndexingParams.QUERY_OPTIMIZATION_LEVEL, "2");
            StorageAttachedIndexingParams.fromMap(options);
            fail("Should have thrown ConfigurationException for out of range value");
        }
        catch (ConfigurationException e)
        {
            assertTrue("Error message should mention valid range",
                      e.getMessage().contains("between 0 and 1"));
        }
    }

    @Test
    public void testQueryOptimizationLevelBoundaryValues()
    {
        // Test minimum value (0)
        Map<String, String> options = new HashMap<>();
        options.put(StorageAttachedIndexingParams.QUERY_OPTIMIZATION_LEVEL, "0");
        StorageAttachedIndexingParams qp = StorageAttachedIndexingParams.fromMap(options);
        assertEquals("query_optimization_level should be 0", 0, qp.queryOptimizationLevel());

        // Test maximum value (1)
        options.put(StorageAttachedIndexingParams.QUERY_OPTIMIZATION_LEVEL, "1");
        qp = StorageAttachedIndexingParams.fromMap(options);
        assertEquals("query_optimization_level should be 1", 1, qp.queryOptimizationLevel());
    }

    @Test
    public void testDefaultIntersectionClauseLimit()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        TableMetadata metadata = currentTableMetadata();
        // Default value from CassandraRelevantProperties.SAI_INTERSECTION_CLAUSE_LIMIT is "2"
        assertEquals("Default intersection_clause_limit should be 2", 2, metadata.params.storageAttachedIndexingParams.intersectionClauseLimit());
    }

    @Test
    public void testIntersectionClauseLimitCustomValue()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int) WITH storage_attached_indexing = {'intersection_clause_limit': 5}");
        TableMetadata metadata = currentTableMetadata();
        assertEquals("intersection_clause_limit should be 5 when set", 5, metadata.params.storageAttachedIndexingParams.intersectionClauseLimit());
    }

    @Test
    public void testAlterIntersectionClauseLimit()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        TableMetadata metadata = currentTableMetadata();
        assertEquals("Initial value should be 2", 2, metadata.params.storageAttachedIndexingParams.intersectionClauseLimit());

        alterTable("ALTER TABLE %s WITH storage_attached_indexing = {'intersection_clause_limit': 10}");
        metadata = currentTableMetadata();
        assertEquals("After ALTER, intersection_clause_limit should be 10", 10, metadata.params.storageAttachedIndexingParams.intersectionClauseLimit());
    }

    @Test
    public void testInvalidIntersectionClauseLimitValue()
    {
        try
        {
            Map<String, String> options = new HashMap<>();
            options.put(StorageAttachedIndexingParams.INTERSECTION_CLAUSE_LIMIT, "invalid");
            StorageAttachedIndexingParams.fromMap(options);
            fail("Should have thrown ConfigurationException for invalid value");
        }
        catch (ConfigurationException e)
        {
            assertTrue("Error message should mention invalid value",
                      e.getMessage().contains("Invalid value"));
        }
    }

    @Test
    public void testNegativeIntersectionClauseLimitValue()
    {
        try
        {
            Map<String, String> options = new HashMap<>();
            options.put(StorageAttachedIndexingParams.INTERSECTION_CLAUSE_LIMIT, "-1");
            StorageAttachedIndexingParams.fromMap(options);
            fail("Should have thrown ConfigurationException for out of range value");
        }
        catch (ConfigurationException e)
        {
            assertTrue("Error message should mention valid range",
                      e.getMessage().contains("between 1 and"));
        }
    }

    @Test
    public void testZeroIntersectionClauseLimitValue()
    {
        try
        {
            Map<String, String> options = new HashMap<>();
            options.put(StorageAttachedIndexingParams.INTERSECTION_CLAUSE_LIMIT, "0");
            StorageAttachedIndexingParams.fromMap(options);
            fail("Should have thrown ConfigurationException for zero value");
        }
        catch (ConfigurationException e)
        {
            assertTrue("Error message should mention valid range",
                      e.getMessage().contains("between 1 and"));
        }
    }

    @Test
    public void testIntersectionClauseLimitBoundaryValues()
    {
        // Test minimum value (1)
        Map<String, String> options = new HashMap<>();
        options.put(StorageAttachedIndexingParams.INTERSECTION_CLAUSE_LIMIT, "1");
        StorageAttachedIndexingParams qp = StorageAttachedIndexingParams.fromMap(options);
        assertEquals("intersection_clause_limit should be 1", 1, qp.intersectionClauseLimit());

        // Test a large value
        options.put(StorageAttachedIndexingParams.INTERSECTION_CLAUSE_LIMIT, "1000");
        qp = StorageAttachedIndexingParams.fromMap(options);
        assertEquals("intersection_clause_limit should be 1000", 1000, qp.intersectionClauseLimit());
    }

    @Test
    public void testFromMap()
    {
        Map<String, String> options = new HashMap<>();
        options.put(StorageAttachedIndexingParams.USE_TERM_STATISTICS, "false");
        options.put(StorageAttachedIndexingParams.QUERY_OPTIMIZATION_LEVEL, "0");
        options.put(StorageAttachedIndexingParams.INTERSECTION_CLAUSE_LIMIT, "5");
        StorageAttachedIndexingParams qp = StorageAttachedIndexingParams.fromMap(options);
        
        assertFalse("use_term_statistics should be false", qp.useTermStatistics());
        assertEquals("query_optimization_level should be 0", 0, qp.queryOptimizationLevel());
        assertEquals("intersection_clause_limit should be 5", 5, qp.intersectionClauseLimit());
    }

    @Test
    @Ignore("Enable this test after we persist storage_attached_indexing in the next release. " +
            "storage_attached_indexing not persisted yet for backwards compatibility reasons.")
    public void testSAIParamsPersistedAcrossKeyspaceReload() throws Throwable
    {
        // For this test to pass, one need to add the following line to SchemaKeyspace#addTableParamsToRowBuilder:
        // builder.add("storage_attached_indexing", params.storageAttachedIndexingParams.asMap());

        // Unfortunately we cannot do that now, because adding a new column data to the system schema is going to
        // break backwards compatibility. So we must add it in two stages. First, we add only the
        // metadata for the storage_attached_indexing column which allows this version to deserialize storage_attached_indexing,
        // and then in the next release we will save the actual data.

        // Create table with custom query options
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int) " +
                    "WITH storage_attached_indexing = {" +
                    "'use_term_statistics': 'false', " +
                    "'query_optimization_level': 0, " +
                    "'intersection_clause_limit': 10}");

        // Verify options are set correctly
        TableMetadata metadata = currentTableMetadata();
        assertFalse("use_term_statistics should be false", metadata.params.storageAttachedIndexingParams.useTermStatistics());
        assertEquals("query_optimization_level should be 0", 0, metadata.params.storageAttachedIndexingParams.queryOptimizationLevel());
        assertEquals("intersection_clause_limit should be 10", 10, metadata.params.storageAttachedIndexingParams.intersectionClauseLimit());

        // Reload keyspace (simulate node restart)
        KeyspaceMetadata reloadedKeyspace = SchemaKeyspace.fetchKeyspaces(Set.of(metadata.keyspace))
                                                          .getNullable(metadata.keyspace);
        assertNotNull(reloadedKeyspace);
        TableMetadata reloadedMetadata = reloadedKeyspace.tables.get(currentTable()).orElse(null);
        assertNotNull("Table should exist in reloaded schema", reloadedMetadata);

        // Verify options are still correct after reload
        assertFalse("use_term_statistics should still be false after reload",
                   reloadedMetadata.params.storageAttachedIndexingParams.useTermStatistics());
        assertEquals("query_optimization_level should still be 0 after reload",
                    0, reloadedMetadata.params.storageAttachedIndexingParams.queryOptimizationLevel());
        assertEquals("intersection_clause_limit should still be 10 after reload",
                    10, reloadedMetadata.params.storageAttachedIndexingParams.intersectionClauseLimit());
    }
}