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

package org.apache.cassandra.db.guardrails;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.GuardrailsOptions;
import org.apache.cassandra.db.ConsistencyLevel;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Test to verify that the default guardrail values are correctly set
 * for HCD, On Prem (DBAaS), and NoProfile configurations via applyConfig and enforceDefault.
 */
public class GuardrailsConfigDefaultsTest
{
    @BeforeClass
    public static void setup()
    {
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
        boolean previousDbaas = DatabaseDescriptor.isEmulateDbaasDefaults();
        boolean previousHcd = DatabaseDescriptor.isHcdGuardrailsDefaults();

        try
        {
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
            Config config = new Config();
            GuardrailsOptions guardrails = new GuardrailsOptions(config);
            guardrails.applyConfig();

            // Validate the configuration to ensure it's valid
            guardrails.validate();

            // Verify the defaults based on the profile
            verifyDefaultsForProfile(guardrails, profileType);
        }
        finally
        {
            // Restore previous settings
            DatabaseDescriptor.setEmulateDbaasDefaults(previousDbaas);
            DatabaseDescriptor.setHcdGuardrailsDefaults(previousHcd);
        }
    }

    private void verifyDefaultsForProfile(GuardrailsOptions guardrails, ProfileType profileType)
    {
        switch (profileType)
        {
            case NO_PROFILE:
                verifyNoProfileDefaults(guardrails);
                break;
            case DBAAS:
                verifyDbaasDefaults(guardrails);
                break;
            case HCD:
                verifyHcdDefaults(guardrails);
                break;
        }
    }

    private void verifyNoProfileDefaults(GuardrailsOptions guardrails)
    {
        Config config = DatabaseDescriptor.getRawConfig();

        // Read request guardrails
        assertEquals("page_size_fail_threshold", GuardrailsOptions.NO_LIMIT, guardrails.getPageSizeFailThreshold());
        assertEquals("in_select_cartesian_product_fail_threshold", GuardrailsOptions.NO_LIMIT, guardrails.getInSelectCartesianProductFailThreshold());
        assertEquals("partition_keys_in_select_fail_threshold", GuardrailsOptions.NO_LIMIT, guardrails.getPartitionKeysInSelectFailThreshold());
        assertEquals("tombstone_warn_threshold", 1000, config.tombstone_warn_threshold);
        assertEquals("tombstone_failure_threshold", 100000, config.tombstone_failure_threshold);

        // Write request guardrails
        assertEquals("logged_batch_enabled", true, guardrails.getLoggedBatchEnabled());
        assertEquals("batch_size_warn_threshold", 64, guardrails.getBatchSizeWarnThreshold() / 1024);
        assertEquals("batch_size_fail_threshold", 640, guardrails.getBatchSizeFailThreshold() / 1024);
        assertEquals("unlogged_batch_across_partitions_warn_threshold", 10, guardrails.getUnloggedBatchAcrossPartitionsWarnThreshold());
        assertEquals("drop_truncate_table_enabled", true, config.drop_truncate_table_enabled);
        assertEquals("user_timestamps_enabled", true, guardrails.getUserTimestampsEnabled());
        assertEquals("column_value_size_fail_threshold", GuardrailsOptions.NO_LIMIT, guardrails.getColumnValueSizeFailThreshold() == null ? GuardrailsOptions.NO_LIMIT : (int) (guardrails.getColumnValueSizeFailThreshold().toBytes() / 1024));
        assertEquals("read_before_write_list_operations_enabled", true, guardrails.getReadBeforeWriteListOperationsEnabled());

        // Write consistency levels - NoProfile has empty set
        assertEquals("write_consistency_levels_disallowed", Collections.emptySet(), guardrails.getWriteConsistencyLevelsDisallowed());

        // Schema guardrails
        assertEquals("counter_enabled", true, guardrails.getCounterEnabled());
        assertEquals("fields_per_udt_fail_threshold", GuardrailsOptions.NO_LIMIT, guardrails.getFieldsPerUDTFailThreshold());
        assertEquals("collection_size_warn_threshold", GuardrailsOptions.NO_LIMIT, guardrails.getCollectionSizeWarnThreshold() == null ? GuardrailsOptions.NO_LIMIT : (int) (guardrails.getCollectionSizeWarnThreshold().toBytes() / 1024));
        assertEquals("items_per_collection_warn_threshold", GuardrailsOptions.NO_LIMIT, guardrails.getItemsPerCollectionWarnThreshold());
        assertEquals("vector_dimensions_warn_threshold", GuardrailsOptions.NO_LIMIT, guardrails.getVectorDimensionsWarnThreshold());
        assertEquals("vector_dimensions_fail_threshold", 8192, guardrails.getVectorDimensionsFailThreshold());
        assertEquals("columns_per_table_fail_threshold", GuardrailsOptions.NO_LIMIT, guardrails.getColumnsPerTableFailThreshold());
        assertEquals("secondary_indexes_per_table_fail_threshold", GuardrailsOptions.NO_LIMIT, guardrails.getSecondaryIndexesPerTableFailThreshold());
        assertEquals("sasi_indexes_per_table_fail_threshold", GuardrailsOptions.NO_LIMIT, config.sasi_indexes_per_table_fail_threshold);
        assertEquals("materialized_views_per_table_fail_threshold", GuardrailsOptions.NO_LIMIT, guardrails.getMaterializedViewsPerTableFailThreshold());
        assertEquals("tables_warn_threshold", GuardrailsOptions.NO_LIMIT, guardrails.getTablesWarnThreshold());
        assertEquals("tables_fail_threshold", GuardrailsOptions.NO_LIMIT, guardrails.getTablesFailThreshold());

        // Table properties
        assertEquals("table_properties_disallowed", Collections.emptySet(), guardrails.getTablePropertiesDisallowed());
        assertEquals("table_properties_ignored", Collections.emptySet(), guardrails.getTablePropertiesIgnored());

        // Node status guardrails
        assertEquals("data_disk_usage_percentage_warn_threshold", GuardrailsOptions.NO_LIMIT, guardrails.getDataDiskUsagePercentageWarnThreshold());
        assertEquals("data_disk_usage_percentage_fail_threshold", GuardrailsOptions.NO_LIMIT, guardrails.getDataDiskUsagePercentageFailThreshold());
        assertEquals("data_disk_usage_max_disk_size", GuardrailsOptions.NO_LIMIT, guardrails.getDataDiskUsageMaxDiskSize() == null ? GuardrailsOptions.NO_LIMIT : (int) (guardrails.getDataDiskUsageMaxDiskSize().toBytes() / (1024L * 1024L * 1024L)));
        assertEquals("partition_size_warn_threshold", 100, guardrails.getPartitionSizeWarnThreshold().toBytes() / (1024 * 1024));

        // SAI indexes guardrails
        assertEquals("sai_indexes_per_table_fail_threshold", GuardrailsOptions.DEFAULT_INDEXES_PER_TABLE_THRESHOLD, guardrails.getStorageAttachedIndexesPerTableFailThreshold());
        assertEquals("sai_indexes_total_fail_threshold", GuardrailsOptions.DEFAULT_INDEXES_TOTAL_THRESHOLD, guardrails.getStorageAttachedIndexesTotalFailThreshold());

        // Offset guardrails
        assertEquals("offset_rows_warn_threshold", 10000, guardrails.getOffsetRowsWarnThreshold());
        assertEquals("offset_rows_fail_threshold", 20000, guardrails.getOffsetRowsFailThreshold());

        // Query filter guardrails
        assertEquals("query_filters_warn_threshold", GuardrailsOptions.NO_LIMIT, guardrails.getQueryFiltersWarnThreshold());
        assertEquals("query_filters_fail_threshold", GuardrailsOptions.NO_LIMIT, guardrails.getQueryFiltersFailThreshold());
    }

    private void verifyDbaasDefaults(GuardrailsOptions guardrails)
    {
        Config config = DatabaseDescriptor.getRawConfig();

        // Read request guardrails
        assertEquals("page_size_fail_threshold", 512, guardrails.getPageSizeFailThreshold());
        assertEquals("in_select_cartesian_product_fail_threshold", 25, guardrails.getInSelectCartesianProductFailThreshold());
        assertEquals("partition_keys_in_select_fail_threshold", 20, guardrails.getPartitionKeysInSelectFailThreshold());
        assertEquals("tombstone_warn_threshold", 1000, config.tombstone_warn_threshold);
        assertEquals("tombstone_failure_threshold", 100000, config.tombstone_failure_threshold);

        // Write request guardrails
        assertEquals("logged_batch_enabled", true, guardrails.getLoggedBatchEnabled());
        assertEquals("batch_size_warn_threshold", 64, guardrails.getBatchSizeWarnThreshold() / 1024);
        assertEquals("batch_size_fail_threshold", 640, guardrails.getBatchSizeFailThreshold() / 1024);
        assertEquals("unlogged_batch_across_partitions_warn_threshold", 10, guardrails.getUnloggedBatchAcrossPartitionsWarnThreshold());
        assertEquals("drop_truncate_table_enabled", true, config.drop_truncate_table_enabled);
        assertEquals("user_timestamps_enabled", true, guardrails.getUserTimestampsEnabled());
        assertEquals("column_value_size_fail_threshold", 5 * 1024 * 1024, guardrails.getColumnValueSizeFailThreshold().toBytes());
        assertEquals("read_before_write_list_operations_enabled", false, guardrails.getReadBeforeWriteListOperationsEnabled());

        // Write consistency levels
        assertEquals("write_consistency_levels_disallowed size", 3, guardrails.getWriteConsistencyLevelsDisallowed().size());
        assertTrue("write_consistency_levels_disallowed contains ANY", guardrails.getWriteConsistencyLevelsDisallowed().contains(ConsistencyLevel.ANY));
        assertTrue("write_consistency_levels_disallowed contains ONE", guardrails.getWriteConsistencyLevelsDisallowed().contains(ConsistencyLevel.ONE));
        assertTrue("write_consistency_levels_disallowed contains LOCAL_ONE", guardrails.getWriteConsistencyLevelsDisallowed().contains(ConsistencyLevel.LOCAL_ONE));

        // Schema guardrails
        assertEquals("counter_enabled", true, guardrails.getCounterEnabled());
        assertEquals("fields_per_udt_fail_threshold", 10, guardrails.getFieldsPerUDTFailThreshold());
        assertEquals("collection_size_warn_threshold", 5 * 1024 * 1024, guardrails.getCollectionSizeWarnThreshold().toBytes());
        assertEquals("items_per_collection_warn_threshold", 20, guardrails.getItemsPerCollectionWarnThreshold());
        assertEquals("vector_dimensions_warn_threshold", GuardrailsOptions.NO_LIMIT, guardrails.getVectorDimensionsWarnThreshold());
        assertEquals("vector_dimensions_fail_threshold", 8192, guardrails.getVectorDimensionsFailThreshold());
        assertEquals("columns_per_table_fail_threshold", 50, guardrails.getColumnsPerTableFailThreshold());
        assertEquals("secondary_indexes_per_table_fail_threshold", 1, guardrails.getSecondaryIndexesPerTableFailThreshold());
        assertEquals("sasi_indexes_per_table_fail_threshold", 0, config.sasi_indexes_per_table_fail_threshold);
        assertEquals("materialized_views_per_table_fail_threshold", 2, guardrails.getMaterializedViewsPerTableFailThreshold());
        assertEquals("tables_warn_threshold", 100, guardrails.getTablesWarnThreshold());
        assertEquals("tables_fail_threshold", 200, guardrails.getTablesFailThreshold());

        // Table properties
        assertEquals("table_properties_disallowed", Collections.emptySet(), guardrails.getTablePropertiesDisallowed());
        assertNotNull("table_properties_ignored", guardrails.getTablePropertiesIgnored());

        // Node status guardrails
        assertEquals("data_disk_usage_percentage_warn_threshold", 70, guardrails.getDataDiskUsagePercentageWarnThreshold());
        assertEquals("data_disk_usage_percentage_fail_threshold", 80, guardrails.getDataDiskUsagePercentageFailThreshold());
        assertEquals("data_disk_usage_max_disk_size", GuardrailsOptions.NO_LIMIT, guardrails.getDataDiskUsageMaxDiskSize() == null ? GuardrailsOptions.NO_LIMIT : (int) (guardrails.getDataDiskUsageMaxDiskSize().toBytes() / (1024L * 1024L * 1024L)));
        assertEquals("partition_size_warn_threshold", 100, guardrails.getPartitionSizeWarnThreshold().toBytes() / (1024 * 1024));

        // SAI indexes guardrails
        assertEquals("sai_indexes_per_table_fail_threshold", GuardrailsOptions.DEFAULT_INDEXES_PER_TABLE_THRESHOLD, guardrails.getStorageAttachedIndexesPerTableFailThreshold());
        assertEquals("sai_indexes_total_fail_threshold", GuardrailsOptions.DEFAULT_INDEXES_TOTAL_THRESHOLD, guardrails.getStorageAttachedIndexesTotalFailThreshold());

        // Offset guardrails
        assertEquals("offset_rows_warn_threshold", 10000, guardrails.getOffsetRowsWarnThreshold());
        assertEquals("offset_rows_fail_threshold", 20000, guardrails.getOffsetRowsFailThreshold());

        // Query filter guardrails
        assertEquals("query_filters_warn_threshold", GuardrailsOptions.NO_LIMIT, guardrails.getQueryFiltersWarnThreshold());
        assertEquals("query_filters_fail_threshold", GuardrailsOptions.NO_LIMIT, guardrails.getQueryFiltersFailThreshold());
    }

    private void verifyHcdDefaults(GuardrailsOptions guardrails)
    {
        Config config = DatabaseDescriptor.getRawConfig();

        // Read request guardrails
        assertEquals("page_size_fail_threshold", GuardrailsOptions.NO_LIMIT, guardrails.getPageSizeFailThreshold());
        assertEquals("in_select_cartesian_product_fail_threshold", 25, guardrails.getInSelectCartesianProductFailThreshold());
        assertEquals("partition_keys_in_select_fail_threshold", 20, guardrails.getPartitionKeysInSelectFailThreshold());
        assertEquals("tombstone_warn_threshold", 1000, config.tombstone_warn_threshold);
        assertEquals("tombstone_failure_threshold", 100000, config.tombstone_failure_threshold);

        // Write request guardrails
        assertEquals("logged_batch_enabled", true, guardrails.getLoggedBatchEnabled());
        assertEquals("batch_size_warn_threshold", 64, guardrails.getBatchSizeWarnThreshold() / 1024);
        assertEquals("batch_size_fail_threshold", 640, guardrails.getBatchSizeFailThreshold() / 1024);
        assertEquals("unlogged_batch_across_partitions_warn_threshold", 10, guardrails.getUnloggedBatchAcrossPartitionsWarnThreshold());
        assertEquals("drop_truncate_table_enabled", true, config.drop_truncate_table_enabled);
        assertEquals("user_timestamps_enabled", true, guardrails.getUserTimestampsEnabled());
        assertEquals("column_value_size_fail_threshold", GuardrailsOptions.NO_LIMIT, guardrails.getColumnValueSizeFailThreshold() == null ? GuardrailsOptions.NO_LIMIT : (int) (guardrails.getColumnValueSizeFailThreshold().toBytes() / 1024));
        assertEquals("read_before_write_list_operations_enabled", true, guardrails.getReadBeforeWriteListOperationsEnabled());

        // Write consistency levels
        assertEquals("write_consistency_levels_disallowed size", 1, guardrails.getWriteConsistencyLevelsDisallowed().size());
        assertTrue("write_consistency_levels_disallowed contains ANY", guardrails.getWriteConsistencyLevelsDisallowed().contains(ConsistencyLevel.ANY));

        // Schema guardrails
        assertEquals("counter_enabled", true, guardrails.getCounterEnabled());
        assertEquals("fields_per_udt_fail_threshold", 100, guardrails.getFieldsPerUDTFailThreshold());
        assertEquals("collection_size_warn_threshold", 10240 * 1024, guardrails.getCollectionSizeWarnThreshold().toBytes());
        assertEquals("items_per_collection_warn_threshold", 200, guardrails.getItemsPerCollectionWarnThreshold());
        assertEquals("vector_dimensions_warn_threshold", GuardrailsOptions.NO_LIMIT, guardrails.getVectorDimensionsWarnThreshold());
        assertEquals("vector_dimensions_fail_threshold", 8192, guardrails.getVectorDimensionsFailThreshold());
        assertEquals("columns_per_table_fail_threshold", 200, guardrails.getColumnsPerTableFailThreshold());
        assertEquals("secondary_indexes_per_table_fail_threshold", 0, guardrails.getSecondaryIndexesPerTableFailThreshold());
        assertEquals("sasi_indexes_per_table_fail_threshold", 0, config.sasi_indexes_per_table_fail_threshold);
        assertEquals("materialized_views_per_table_fail_threshold", 0, guardrails.getMaterializedViewsPerTableFailThreshold());
        assertEquals("tables_warn_threshold", 100, guardrails.getTablesWarnThreshold());
        assertEquals("tables_fail_threshold", 200, guardrails.getTablesFailThreshold());

        // Table properties
        assertEquals("table_properties_disallowed", Collections.emptySet(), guardrails.getTablePropertiesDisallowed());
        assertEquals("table_properties_ignored", Collections.emptySet(), guardrails.getTablePropertiesIgnored());

        // Node status guardrails
        assertEquals("data_disk_usage_percentage_warn_threshold", 70, guardrails.getDataDiskUsagePercentageWarnThreshold());
        assertEquals("data_disk_usage_percentage_fail_threshold", GuardrailsOptions.NO_LIMIT, guardrails.getDataDiskUsagePercentageFailThreshold());
        assertEquals("data_disk_usage_max_disk_size", GuardrailsOptions.NO_LIMIT, guardrails.getDataDiskUsageMaxDiskSize() == null ? GuardrailsOptions.NO_LIMIT : (int) (guardrails.getDataDiskUsageMaxDiskSize().toBytes() / (1024L * 1024L * 1024L)));
        assertEquals("partition_size_warn_threshold", 100, guardrails.getPartitionSizeWarnThreshold().toBytes() / (1024 * 1024));

        // SAI indexes guardrails
        assertEquals("sai_indexes_per_table_fail_threshold", GuardrailsOptions.DEFAULT_INDEXES_PER_TABLE_THRESHOLD, guardrails.getStorageAttachedIndexesPerTableFailThreshold());
        assertEquals("sai_indexes_total_fail_threshold", GuardrailsOptions.DEFAULT_INDEXES_TOTAL_THRESHOLD, guardrails.getStorageAttachedIndexesTotalFailThreshold());

        // Offset guardrails
        assertEquals("offset_rows_warn_threshold", 10000, guardrails.getOffsetRowsWarnThreshold());
        assertEquals("offset_rows_fail_threshold", 20000, guardrails.getOffsetRowsFailThreshold());

        // Query filter guardrails
        assertEquals("query_filters_warn_threshold", GuardrailsOptions.NO_LIMIT, guardrails.getQueryFiltersWarnThreshold());
        assertEquals("query_filters_fail_threshold", GuardrailsOptions.NO_LIMIT, guardrails.getQueryFiltersFailThreshold());
    }
}
