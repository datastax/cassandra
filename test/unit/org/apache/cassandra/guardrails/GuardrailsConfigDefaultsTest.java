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

package org.apache.cassandra.guardrails;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Test to verify that the default guardrail values are correctly set
 * for HCD, On Prem (DBAaS), and NoProfile configurations via applyConfig and enforceDefault.
 */
public class GuardrailsConfigDefaultsTest
{
    @BeforeClass
    public static void setup()
    {
        // Initialize DatabaseDescriptor with defaults for tests
        System.setProperty("cassandra.config", "cassandra.yaml");
        DatabaseDescriptor.daemonInitialization();
    }
    @Test
    public void testNoProfileDefaults()
    {
        testGuardrailDefaults(ProfileType.NO_PROFILE);
    }

    @Test
    public void testDbaasDefaults()
    {
        testGuardrailDefaults(ProfileType.DBAAS);
    }

    @Test
    public void testHcdDefaults()
    {
        testGuardrailDefaults(ProfileType.HCD);
    }

    private enum ProfileType
    {
        NO_PROFILE, DBAAS, HCD
    }

    private void testGuardrailDefaults(ProfileType profileType)
    {
        // Save current settings
        boolean previousDbaas = DatabaseDescriptor.isEmulateDbaasDefaults();
        boolean previousHcd = DatabaseDescriptor.isHcdGuardrailsDefaults();
        
        try
        {
            // Set the appropriate profile
            switch (profileType)
            {
                case NO_PROFILE:
                    DatabaseDescriptor.setEmulateDbaasDefaults(false);
                    DatabaseDescriptor.setHcdGuardrailsDefaults(false);
                    break;
                case DBAAS:
                    DatabaseDescriptor.setEmulateDbaasDefaults(true);
                    DatabaseDescriptor.setHcdGuardrailsDefaults(false);
                    break;
                case HCD:
                    DatabaseDescriptor.setEmulateDbaasDefaults(false);
                    DatabaseDescriptor.setHcdGuardrailsDefaults(true);
                    break;
            }

            // Create a fresh config and apply defaults
            GuardrailsConfig config = new GuardrailsConfig();
            config.applyConfig();
            
            // Validate the configuration to ensure it's valid
            config.validate();

            // Verify the defaults based on the profile
            verifyDefaultsForProfile(config, profileType);
        }
        finally
        {
            // Restore previous settings
            DatabaseDescriptor.setEmulateDbaasDefaults(previousDbaas);
            DatabaseDescriptor.setHcdGuardrailsDefaults(previousHcd);
        }
    }

    private void verifyDefaultsForProfile(GuardrailsConfig config, ProfileType profileType)
    {
        // Test read request guardrails
        switch (profileType)
        {
            case NO_PROFILE:
                verifyNoProfileDefaults(config);
                break;
            case DBAAS:
                verifyDbaasDefaults(config);
                break;
            case HCD:
                verifyHcdDefaults(config);
                break;
        }
    }

    private void verifyNoProfileDefaults(GuardrailsConfig config)
    {
        // Read request guardrails
        assertEquals("page_size_failure_threshold_in_kb", GuardrailsConfig.NO_LIMIT, (int) config.page_size_failure_threshold_in_kb);
        assertEquals("in_select_cartesian_product_failure_threshold", GuardrailsConfig.NO_LIMIT, (int) config.in_select_cartesian_product_failure_threshold);
        assertEquals("partition_keys_in_select_failure_threshold", GuardrailsConfig.NO_LIMIT, (int) config.partition_keys_in_select_failure_threshold);
        assertEquals("tombstone_warn_threshold", 1000, (int) config.tombstone_warn_threshold);
        assertEquals("tombstone_failure_threshold", 100000, (int) config.tombstone_failure_threshold);
        
        // Write request guardrails
        assertEquals("logged_batch_enabled", true, config.logged_batch_enabled.booleanValue());
        assertEquals("batch_size_warn_threshold_in_kb", 64, (int) config.batch_size_warn_threshold_in_kb);
        assertEquals("batch_size_fail_threshold_in_kb", 640, (int) config.batch_size_fail_threshold_in_kb);
        assertEquals("unlogged_batch_across_partitions_warn_threshold", 10, (int) config.unlogged_batch_across_partitions_warn_threshold);
        assertEquals("truncate_table_enabled", true, config.truncate_table_enabled.booleanValue());
        assertEquals("user_timestamps_enabled", true, config.user_timestamps_enabled.booleanValue());
        assertEquals("column_value_size_failure_threshold_in_kb", -1L, config.column_value_size_failure_threshold_in_kb.longValue());
        assertEquals("read_before_write_list_operations_enabled", true, config.read_before_write_list_operations_enabled.booleanValue());
        
        // Write consistency levels - NoProfile has empty set
        assertEquals("write_consistency_levels_disallowed", Collections.emptySet(), config.write_consistency_levels_disallowed);
        
        // Schema guardrails
        assertEquals("counter_enabled", true, config.counter_enabled.booleanValue());
        assertEquals("fields_per_udt_failure_threshold", -1L, config.fields_per_udt_failure_threshold.longValue());
        assertEquals("collection_size_warn_threshold_in_kb", -1L, config.collection_size_warn_threshold_in_kb.longValue());
        assertEquals("items_per_collection_warn_threshold", -1L, config.items_per_collection_warn_threshold.longValue());
        assertEquals("vector_dimensions_warn_threshold", -1, (int) config.vector_dimensions_warn_threshold);
        assertEquals("vector_dimensions_failure_threshold", 8192, (int) config.vector_dimensions_failure_threshold);
        assertEquals("columns_per_table_failure_threshold", -1L, config.columns_per_table_failure_threshold.longValue());
        assertEquals("secondary_index_per_table_failure_threshold", GuardrailsConfig.NO_LIMIT, (int) config.secondary_index_per_table_failure_threshold);
        assertEquals("sasi_indexes_per_table_failure_threshold", GuardrailsConfig.NO_LIMIT, (int) config.sasi_indexes_per_table_failure_threshold);
        assertEquals("materialized_view_per_table_failure_threshold", GuardrailsConfig.NO_LIMIT, (int) config.materialized_view_per_table_failure_threshold);
        assertEquals("tables_warn_threshold", -1L, config.tables_warn_threshold.longValue());
        assertEquals("tables_failure_threshold", -1L, config.tables_failure_threshold.longValue());
        
        // Table properties
        assertEquals("table_properties_disallowed", Collections.emptySet(), config.table_properties_disallowed);
        assertEquals("table_properties_ignored", Collections.emptySet(), config.table_properties_ignored);
        
        // Node status guardrails
        assertEquals("disk_usage_percentage_warn_threshold", GuardrailsConfig.NO_LIMIT, (int) config.disk_usage_percentage_warn_threshold);
        assertEquals("disk_usage_percentage_failure_threshold", GuardrailsConfig.NO_LIMIT, (int) config.disk_usage_percentage_failure_threshold);
        assertEquals("disk_usage_max_disk_size_in_gb", (long) GuardrailsConfig.NO_LIMIT, config.disk_usage_max_disk_size_in_gb.longValue());
        assertEquals("partition_size_warn_threshold_in_mb", 100, (int) config.partition_size_warn_threshold_in_mb);
        
        // SAI indexes guardrails
        assertEquals("sai_indexes_per_table_failure_threshold", GuardrailsConfig.DEFAULT_INDEXES_PER_TABLE_THRESHOLD, (int) config.sai_indexes_per_table_failure_threshold);
        assertEquals("sai_indexes_total_failure_threshold", GuardrailsConfig.DEFAULT_INDEXES_TOTAL_THRESHOLD, (int) config.sai_indexes_total_failure_threshold);
        
        // Offset guardrails
        assertEquals("offset_rows_warn_threshold", 10000, (int) config.offset_rows_warn_threshold);
        assertEquals("offset_rows_failure_threshold", 20000, (int) config.offset_rows_failure_threshold);
        
        // Query filter guardrails
        assertEquals("query_filters_warn_threshold", -1, (int) config.query_filters_warn_threshold);
        assertEquals("query_filters_fail_threshold", -1, (int) config.query_filters_fail_threshold);
    }

    private void verifyDbaasDefaults(GuardrailsConfig config)
    {
        // Read request guardrails
        assertEquals("page_size_failure_threshold_in_kb", 512, (int) config.page_size_failure_threshold_in_kb);
        assertEquals("in_select_cartesian_product_failure_threshold", 25, (int) config.in_select_cartesian_product_failure_threshold);
        assertEquals("partition_keys_in_select_failure_threshold", 20, (int) config.partition_keys_in_select_failure_threshold);
        assertEquals("tombstone_warn_threshold", 1000, (int) config.tombstone_warn_threshold);
        assertEquals("tombstone_failure_threshold", 100000, (int) config.tombstone_failure_threshold);
        
        // Write request guardrails
        assertEquals("logged_batch_enabled", true, config.logged_batch_enabled.booleanValue());
        assertEquals("batch_size_warn_threshold_in_kb", 64, (int) config.batch_size_warn_threshold_in_kb);
        assertEquals("batch_size_fail_threshold_in_kb", 640, (int) config.batch_size_fail_threshold_in_kb);
        assertEquals("unlogged_batch_across_partitions_warn_threshold", 10, (int) config.unlogged_batch_across_partitions_warn_threshold);
        assertEquals("truncate_table_enabled", true, config.truncate_table_enabled.booleanValue());
        assertEquals("user_timestamps_enabled", true, config.user_timestamps_enabled.booleanValue());
        assertEquals("column_value_size_failure_threshold_in_kb", 5 * 1024L, config.column_value_size_failure_threshold_in_kb.longValue());
        assertEquals("read_before_write_list_operations_enabled", false, config.read_before_write_list_operations_enabled.booleanValue());
        
        // Write consistency levels
        Set<String> expectedWriteConsistencyLevelsDisallowed = new LinkedHashSet<>(Arrays.asList("ANY", "ONE", "LOCAL_ONE"));
        assertEquals("write_consistency_levels_disallowed", expectedWriteConsistencyLevelsDisallowed, config.write_consistency_levels_disallowed);
        
        // Schema guardrails
        assertEquals("counter_enabled", true, config.counter_enabled.booleanValue());
        assertEquals("fields_per_udt_failure_threshold", 10L, config.fields_per_udt_failure_threshold.longValue());
        assertEquals("collection_size_warn_threshold_in_kb", 5 * 1024L, config.collection_size_warn_threshold_in_kb.longValue());
        assertEquals("items_per_collection_warn_threshold", 20L, config.items_per_collection_warn_threshold.longValue());
        assertEquals("vector_dimensions_warn_threshold", -1, (int) config.vector_dimensions_warn_threshold);
        assertEquals("vector_dimensions_failure_threshold", 8192, (int) config.vector_dimensions_failure_threshold);
        assertEquals("columns_per_table_failure_threshold", 50L, config.columns_per_table_failure_threshold.longValue());
        assertEquals("secondary_index_per_table_failure_threshold", 1, (int) config.secondary_index_per_table_failure_threshold);
        assertEquals("sasi_indexes_per_table_failure_threshold", 0, (int) config.sasi_indexes_per_table_failure_threshold);
        assertEquals("materialized_view_per_table_failure_threshold", 2, (int) config.materialized_view_per_table_failure_threshold);
        assertEquals("tables_warn_threshold", 100L, config.tables_warn_threshold.longValue());
        assertEquals("tables_failure_threshold", 200L, config.tables_failure_threshold.longValue());
        
        // Table properties
        assertEquals("table_properties_disallowed", Collections.emptySet(), config.table_properties_disallowed);
        assertNotNull("table_properties_ignored", config.table_properties_ignored);
        
        // Node status guardrails
        assertEquals("disk_usage_percentage_warn_threshold", 70, (int) config.disk_usage_percentage_warn_threshold);
        assertEquals("disk_usage_percentage_failure_threshold", 80, (int) config.disk_usage_percentage_failure_threshold);
        assertEquals("disk_usage_max_disk_size_in_gb", (long) GuardrailsConfig.NO_LIMIT, config.disk_usage_max_disk_size_in_gb.longValue());
        assertEquals("partition_size_warn_threshold_in_mb", 100, (int) config.partition_size_warn_threshold_in_mb);
        
        // SAI indexes guardrails
        assertEquals("sai_indexes_per_table_failure_threshold", GuardrailsConfig.DEFAULT_INDEXES_PER_TABLE_THRESHOLD, (int) config.sai_indexes_per_table_failure_threshold);
        assertEquals("sai_indexes_total_failure_threshold", GuardrailsConfig.DEFAULT_INDEXES_TOTAL_THRESHOLD, (int) config.sai_indexes_total_failure_threshold);
        
        // Offset guardrails
        assertEquals("offset_rows_warn_threshold", 10000, (int) config.offset_rows_warn_threshold);
        assertEquals("offset_rows_failure_threshold", 20000, (int) config.offset_rows_failure_threshold);
        
        // Query filter guardrails
        assertEquals("query_filters_warn_threshold", -1, (int) config.query_filters_warn_threshold);
        assertEquals("query_filters_fail_threshold", -1, (int) config.query_filters_fail_threshold);
    }

    private void verifyHcdDefaults(GuardrailsConfig config)
    {
        // Read request guardrails
        assertEquals("page_size_failure_threshold_in_kb", GuardrailsConfig.NO_LIMIT, (int) config.page_size_failure_threshold_in_kb);
        assertEquals("in_select_cartesian_product_failure_threshold", 25, (int) config.in_select_cartesian_product_failure_threshold);
        assertEquals("partition_keys_in_select_failure_threshold", 20, (int) config.partition_keys_in_select_failure_threshold);
        assertEquals("tombstone_warn_threshold", 1000, (int) config.tombstone_warn_threshold);
        assertEquals("tombstone_failure_threshold", 100000, (int) config.tombstone_failure_threshold);
        
        // Write request guardrails
        assertEquals("logged_batch_enabled", true, config.logged_batch_enabled.booleanValue());
        assertEquals("batch_size_warn_threshold_in_kb", 64, (int) config.batch_size_warn_threshold_in_kb);
        assertEquals("batch_size_fail_threshold_in_kb", 640, (int) config.batch_size_fail_threshold_in_kb);
        assertEquals("unlogged_batch_across_partitions_warn_threshold", 10, (int) config.unlogged_batch_across_partitions_warn_threshold);
        assertEquals("truncate_table_enabled", true, config.truncate_table_enabled.booleanValue());
        assertEquals("user_timestamps_enabled", true, config.user_timestamps_enabled.booleanValue());
        assertEquals("column_value_size_failure_threshold_in_kb", -1L, config.column_value_size_failure_threshold_in_kb.longValue());
        assertEquals("read_before_write_list_operations_enabled", true, config.read_before_write_list_operations_enabled.booleanValue());
        
        // Write consistency levels
        Set<String> expectedWriteConsistencyLevelsDisallowed = new LinkedHashSet<>(Arrays.asList("ANY"));
        assertEquals("write_consistency_levels_disallowed", expectedWriteConsistencyLevelsDisallowed, config.write_consistency_levels_disallowed);
        
        // Schema guardrails
        assertEquals("counter_enabled", true, config.counter_enabled.booleanValue());
        assertEquals("fields_per_udt_failure_threshold", 100L, config.fields_per_udt_failure_threshold.longValue());
        assertEquals("collection_size_warn_threshold_in_kb", 10240L, config.collection_size_warn_threshold_in_kb.longValue());
        assertEquals("items_per_collection_warn_threshold", 200L, config.items_per_collection_warn_threshold.longValue());
        assertEquals("vector_dimensions_warn_threshold", -1, (int) config.vector_dimensions_warn_threshold);
        assertEquals("vector_dimensions_failure_threshold", 8192, (int) config.vector_dimensions_failure_threshold);
        assertEquals("columns_per_table_failure_threshold", 200L, config.columns_per_table_failure_threshold.longValue());
        assertEquals("secondary_index_per_table_failure_threshold", 0, (int) config.secondary_index_per_table_failure_threshold);
        assertEquals("sasi_indexes_per_table_failure_threshold", 0, (int) config.sasi_indexes_per_table_failure_threshold);
        assertEquals("materialized_view_per_table_failure_threshold", 0, (int) config.materialized_view_per_table_failure_threshold);
        assertEquals("tables_warn_threshold", 100L, config.tables_warn_threshold.longValue());
        assertEquals("tables_failure_threshold", 200L, config.tables_failure_threshold.longValue());
        
        // Table properties
        assertEquals("table_properties_disallowed", Collections.emptySet(), config.table_properties_disallowed);
        assertEquals("table_properties_ignored", Collections.emptySet(), config.table_properties_ignored);
        
        // Node status guardrails
        assertEquals("disk_usage_percentage_warn_threshold", 70, (int) config.disk_usage_percentage_warn_threshold);
        assertEquals("disk_usage_percentage_failure_threshold", GuardrailsConfig.NO_LIMIT, (int) config.disk_usage_percentage_failure_threshold);
        assertEquals("disk_usage_max_disk_size_in_gb", (long) GuardrailsConfig.NO_LIMIT, config.disk_usage_max_disk_size_in_gb.longValue());
        assertEquals("partition_size_warn_threshold_in_mb", 100, (int) config.partition_size_warn_threshold_in_mb);
        
        // SAI indexes guardrails
        assertEquals("sai_indexes_per_table_failure_threshold", GuardrailsConfig.DEFAULT_INDEXES_PER_TABLE_THRESHOLD, (int) config.sai_indexes_per_table_failure_threshold);
        assertEquals("sai_indexes_total_failure_threshold", GuardrailsConfig.DEFAULT_INDEXES_TOTAL_THRESHOLD, (int) config.sai_indexes_total_failure_threshold);
        
        // Offset guardrails
        assertEquals("offset_rows_warn_threshold", 10000, (int) config.offset_rows_warn_threshold);
        assertEquals("offset_rows_failure_threshold", 20000, (int) config.offset_rows_failure_threshold);
        
        // Query filter guardrails
        assertEquals("query_filters_warn_threshold", -1, (int) config.query_filters_warn_threshold);
        assertEquals("query_filters_fail_threshold", -1, (int) config.query_filters_fail_threshold);
    }
}