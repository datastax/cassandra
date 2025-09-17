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

package org.apache.cassandra.config;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.IntConsumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.statements.schema.TableAttributes;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.db.guardrails.GuardrailsConfig;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.TableParams;
import org.apache.cassandra.service.disk.usage.DiskUsageMonitor;

import static java.lang.String.format;
import static java.util.stream.Collectors.toSet;
import static org.apache.cassandra.config.DataStorageSpec.DataStorageUnit.KIBIBYTES;
import static org.apache.cassandra.config.DataStorageSpec.DataStorageUnit.MEBIBYTES;

/**
 * Configuration settings for guardrails populated from the Yaml file.
 *
 * <p>Note that the settings here must only be used to build the {@link GuardrailsConfig} class and not directly by the
 * code checking each guarded constraint. That code should use the higher level abstractions defined in
 * {@link Guardrails}).
 *
 * <p>We have 2 variants of guardrails, soft (warn) and hard (fail) limits, each guardrail having either one of the
 * variants or both. Note in particular that hard limits only make sense for guardrails triggering during query
 * execution. For other guardrails, say one triggering during compaction, aborting that compaction does not make sense.
 *
 * <p>Additionally, each individual setting should have a specific value (typically -1 for numeric settings),
 * that allows to disable the corresponding guardrail.
 */
public class GuardrailsOptions implements GuardrailsConfig
{
    private static final Logger logger = LoggerFactory.getLogger(GuardrailsOptions.class);

    public static final int NO_LIMIT = -1;
    public static final int UNSET = -2;
    public static final int DEFAULT_INDEXES_PER_TABLE_THRESHOLD = 10;
    public static final int DEFAULT_INDEXES_TOTAL_THRESHOLD = 100;

    private final Config config;

    public GuardrailsOptions(Config config)
    {
        this.config = config;
    }

    /**
     * Validate that the value provided for each guardrail setting is valid.
     *
     * @throws ConfigurationException if any of the settings has an invalid setting.
     */
    public void validate()
    {
        validateMaxIntThreshold(config.keyspaces_warn_threshold, config.keyspaces_fail_threshold, "keyspaces");
        validateMaxIntThreshold(config.tables_warn_threshold, config.tables_fail_threshold, "tables");
        validateMaxIntThreshold(config.columns_per_table_warn_threshold, config.columns_per_table_fail_threshold, "columns_per_table");
        validateMaxIntThreshold(config.secondary_indexes_per_table_warn_threshold, config.secondary_indexes_per_table_fail_threshold, "secondary_indexes_per_table");
        validateMaxIntThreshold(config.sai_indexes_per_table_warn_threshold, config.sai_indexes_per_table_fail_threshold, "sai_indexes_per_table");
        validateMaxIntThreshold(config.sai_indexes_total_warn_threshold, config.sai_indexes_total_fail_threshold, "sai_indexes_total");
        validateMaxIntThreshold(config.sasi_indexes_per_table_warn_threshold, config.sasi_indexes_per_table_fail_threshold, "sasi_indexes_per_table");
        validateMaxIntThreshold(config.materialized_views_per_table_warn_threshold, config.materialized_views_per_table_fail_threshold, "materialized_views_per_table", true);
        config.table_properties_warned = validateTableProperties(config.table_properties_warned, "table_properties_warned");
        config.table_properties_ignored = validateTableProperties(config.table_properties_ignored, "table_properties_ignored");
        config.table_properties_disallowed = validateTableProperties(config.table_properties_disallowed, "table_properties_disallowed");
        validateMaxIntThreshold(config.page_size_warn_threshold, config.page_size_fail_threshold, "page_size");
        validateSizeThreshold(config.page_weight_warn_threshold, config.page_weight_fail_threshold, false, "page_weight");
        validateMaxIntThreshold(config.partition_keys_in_select_warn_threshold, config.partition_keys_in_select_fail_threshold, "partition_keys_in_select");
        validateMaxIntThreshold(config.in_select_cartesian_product_warn_threshold, config.in_select_cartesian_product_fail_threshold, "in_select_cartesian_product");
        config.read_consistency_levels_warned = validateConsistencyLevels(config.read_consistency_levels_warned, "read_consistency_levels_warned");
        config.read_consistency_levels_disallowed = validateConsistencyLevels(config.read_consistency_levels_disallowed, "read_consistency_levels_disallowed");
        config.write_consistency_levels_warned = validateConsistencyLevels(config.write_consistency_levels_warned, "write_consistency_levels_warned");
        config.write_consistency_levels_disallowed = validateConsistencyLevels(config.write_consistency_levels_disallowed, "write_consistency_levels_disallowed");
        validateSizeThreshold(config.partition_size_warn_threshold, config.partition_size_fail_threshold, false, "partition_size");
        validateMaxLongThreshold(config.partition_tombstones_warn_threshold, config.partition_tombstones_fail_threshold, "partition_tombstones", false);
        validateSizeThreshold(config.column_value_size_warn_threshold, config.column_value_size_fail_threshold, false, "column_value_size");
        validateSizeThreshold(config.collection_size_warn_threshold, config.collection_size_fail_threshold, false, "collection_size");
        validateMaxIntThreshold(config.items_per_collection_warn_threshold, config.items_per_collection_fail_threshold, "items_per_collection");
        validateMaxIntThreshold(config.fields_per_udt_warn_threshold, config.fields_per_udt_fail_threshold, "fields_per_udt");
        validateMaxIntThreshold(config.vector_dimensions_warn_threshold, config.vector_dimensions_fail_threshold, "vector_dimensions");
        validateMaxIntThreshold(config.sai_ann_rerank_k_warn_threshold, config.sai_ann_rerank_k_fail_threshold, "sai_ann_rerank_k_max_value");
        validatePercentageThreshold(config.data_disk_usage_percentage_warn_threshold, config.data_disk_usage_percentage_fail_threshold, "data_disk_usage_percentage");
        validateDataDiskUsageMaxDiskSize(config.data_disk_usage_max_disk_size);
        validateMinRFThreshold(config.minimum_replication_factor_warn_threshold, config.minimum_replication_factor_fail_threshold);
        validateMaxRFThreshold(config.maximum_replication_factor_warn_threshold, config.maximum_replication_factor_fail_threshold);
        validateTimestampThreshold(config.maximum_timestamp_warn_threshold, config.maximum_timestamp_fail_threshold, "maximum_timestamp");
        validateTimestampThreshold(config.minimum_timestamp_warn_threshold, config.minimum_timestamp_fail_threshold, "minimum_timestamp");
        validateMaxLongThreshold(config.sai_sstable_indexes_per_query_warn_threshold,
                                 config.sai_sstable_indexes_per_query_fail_threshold,
                                 "sai_sstable_indexes_per_query",
                                 false);
        validateMaxIntThreshold(config.tombstone_warn_threshold, config.tombstone_failure_threshold, "tombstone_threshold");
    }

    /**
     * Apply guardrail defaults to settings not specified by yaml according
     * to {@link DatabaseDescriptor#isEmulateDbaasDefaults()} or {@link DatabaseDescriptor#isHcdGuardrailsDefaults()}
     */
    @VisibleForTesting
    public void applyConfig()
    {

        assert !(DatabaseDescriptor.isHcdGuardrailsDefaults() && DatabaseDescriptor.isEmulateDbaasDefaults())
                : "Cannot set both hcd_guardrail_defaults and emulate_dbaas_defaults to true";

        // for read requests
        enforceDefault("page_weight_fail_threshold", v -> config.page_weight_fail_threshold = v, null, new DataStorageSpec.IntBytesBound("512KiB"), null);

        enforceDefault("in_select_cartesian_product_fail_threshold", (IntConsumer) (v -> config.in_select_cartesian_product_fail_threshold = v), NO_LIMIT, 25, 25);
        enforceDefault("partition_keys_in_select_fail_threshold", (IntConsumer) (v -> config.partition_keys_in_select_fail_threshold = v), NO_LIMIT, 20, 20);

        enforceDefault("tombstone_warn_threshold", (IntConsumer) (v -> config.tombstone_warn_threshold = v), 1000, 1000, 1000);
        enforceDefault("tombstone_failure_threshold", (IntConsumer) (v -> config.tombstone_failure_threshold = v), 100000, 100000, 100000);

        // Default to no warning and failure at 4 times the maxTopK value
        int maxTopK = CassandraRelevantProperties.SAI_VECTOR_SEARCH_MAX_TOP_K.getInt();
        enforceDefault("sai_ann_rerank_k_warn_threshold", (IntConsumer) (v -> config.sai_ann_rerank_k_warn_threshold = v), -1, -1, -1);
        enforceDefault("sai_ann_rerank_k_fail_threshold", (IntConsumer) (v -> config.sai_ann_rerank_k_fail_threshold = v), 4 * maxTopK, 4 * maxTopK, 4 * maxTopK);

        // for write requests
        enforceDefault("logged_batch_enabled", v -> config.logged_batch_enabled = v, true, true, true);
        enforceDefault("batch_size_warn_threshold", v -> config.batch_size_warn_threshold = v, new DataStorageSpec.IntKibibytesBound("64KiB"), new DataStorageSpec.IntKibibytesBound("64KiB"), new DataStorageSpec.IntKibibytesBound("64KiB"));
        enforceDefault("batch_size_fail_threshold", v -> config.batch_size_fail_threshold = v, new DataStorageSpec.IntKibibytesBound("640KiB"), new DataStorageSpec.IntKibibytesBound("640KiB"), new DataStorageSpec.IntKibibytesBound("640KiB"));
        enforceDefault("unlogged_batch_across_partitions_warn_threshold", (IntConsumer) (v -> config.unlogged_batch_across_partitions_warn_threshold = v), 10, 10, 10);

        enforceDefault("drop_truncate_table_enabled", v -> config.drop_truncate_table_enabled = v, true, true, true);

        enforceDefault("user_timestamps_enabled", v -> config.user_timestamps_enabled = v, true, true, true);

        enforceDefault("column_value_size_fail_threshold", v -> config.column_value_size_fail_threshold = v, null, new DataStorageSpec.LongBytesBound(5 * 1024L, KIBIBYTES), null);

        enforceDefault("read_before_write_list_operations_enabled", v -> config.read_before_write_list_operations_enabled = v, true, false, true);

        // We use a LinkedHashSet just for the sake of preserving the ordering in error messages
        enforceDefault("write_consistency_levels_disallowed",
                       v -> config.write_consistency_levels_disallowed = ImmutableSet.copyOf(v),
                       Collections.<ConsistencyLevel>emptySet(),
                       new LinkedHashSet<>(Arrays.asList(ConsistencyLevel.ANY, ConsistencyLevel.ONE, ConsistencyLevel.LOCAL_ONE)),
                       new LinkedHashSet<>(Arrays.asList(ConsistencyLevel.ANY)));

        // for schema
        enforceDefault("counter_enabled", v -> config.counter_enabled = v, true, true, true);

        enforceDefault("fields_per_udt_fail_threshold", (IntConsumer) (v -> config.fields_per_udt_fail_threshold = v), -1, 10, 100);
        enforceDefault("collection_size_warn_threshold", v -> config.collection_size_warn_threshold = v, null, new DataStorageSpec.LongBytesBound(5 * 1024L, KIBIBYTES), new DataStorageSpec.LongBytesBound(10240L, KIBIBYTES));
        enforceDefault("items_per_collection_warn_threshold", v -> config.items_per_collection_warn_threshold = v, -1, 20, 200);

        enforceDefault("vector_dimensions_warn_threshold", v -> config.vector_dimensions_warn_threshold = v, -1, -1, -1);
        enforceDefault("vector_dimensions_fail_threshold", v -> config.vector_dimensions_fail_threshold = v, 8192, 8192, 8192);

        enforceDefault("columns_per_table_fail_threshold", v -> config.columns_per_table_fail_threshold = v, -1, 50, 200);
        enforceDefault("secondary_indexes_per_table_fail_threshold", v -> config.secondary_indexes_per_table_fail_threshold = v, NO_LIMIT, 1, 0);
        enforceDefault("sasi_indexes_per_table_fail_threshold", v -> config.sasi_indexes_per_table_fail_threshold = v, NO_LIMIT, NO_LIMIT, 0);
        enforceDefault("materialized_views_per_table_fail_threshold", v -> config.materialized_views_per_table_fail_threshold = v, NO_LIMIT, 2, 0);
        enforceDefault("tables_warn_threshold", v -> config.tables_warn_threshold = v, -1, 100, 100);
        enforceDefault("tables_fail_threshold", v -> config.tables_fail_threshold = v, -1, 200, 200);

        enforceDefault("table_properties_disallowed",
                       v -> config.table_properties_disallowed = ImmutableSet.copyOf(v),
                       Collections.<String>emptySet(),
                       Collections.<String>emptySet(),
                       Collections.<String>emptySet());

        enforceDefault("table_properties_ignored",
                       v -> config.table_properties_ignored = ImmutableSet.copyOf(v),
                       Collections.<String>emptySet(),
                       new LinkedHashSet<>(TableAttributes.allKeywords().stream()
                                                          .sorted()
                                                          .filter(p -> !p.equals(TableParams.Option.DEFAULT_TIME_TO_LIVE.toString()) &&
                                                                       !p.equals(TableParams.Option.COMMENT.toString()) &&
                                                                       !p.equals(TableParams.Option.EXTENSIONS.toString()))
                                                          .collect(Collectors.toList())),
                       Collections.<String>emptySet());

        // for node status
        enforceDefault("data_disk_usage_percentage_warn_threshold", v -> config.data_disk_usage_percentage_warn_threshold = v, NO_LIMIT, 70, 70);
        enforceDefault("data_disk_usage_percentage_fail_threshold", v -> config.data_disk_usage_percentage_fail_threshold = v, NO_LIMIT, 80, NO_LIMIT);
        enforceDefault("data_disk_usage_max_disk_size", v -> config.data_disk_usage_max_disk_size = v, null, (DataStorageSpec.LongBytesBound) null, (DataStorageSpec.LongBytesBound) null);

        enforceDefault("partition_size_warn_threshold", v -> config.partition_size_warn_threshold = v, new DataStorageSpec.LongBytesBound(100,MEBIBYTES), new DataStorageSpec.LongBytesBound(100, MEBIBYTES), new DataStorageSpec.LongBytesBound(100, MEBIBYTES));

        // SAI Table Failure threshold (maye be overridden via system property)
        int overrideTableFailureThreshold = CassandraRelevantProperties.INDEX_GUARDRAILS_TABLE_FAILURE_THRESHOLD.getInt(UNSET);
        if (overrideTableFailureThreshold != UNSET)
            config.sai_indexes_per_table_fail_threshold = overrideTableFailureThreshold;
        else
            enforceDefault("sai_indexes_per_table_fail_threshold", v -> config.sai_indexes_per_table_fail_threshold = v, DEFAULT_INDEXES_PER_TABLE_THRESHOLD, DEFAULT_INDEXES_PER_TABLE_THRESHOLD, DEFAULT_INDEXES_PER_TABLE_THRESHOLD);

        // SAI Table Failure threshold (maye be overridden via system property)
        int overrideTotalFailureThreshold = CassandraRelevantProperties.INDEX_GUARDRAILS_TOTAL_FAILURE_THRESHOLD.getInt(UNSET);
        if (overrideTotalFailureThreshold != UNSET)
            config.sai_indexes_total_fail_threshold = overrideTotalFailureThreshold;
        else
            enforceDefault("sai_indexes_total_fail_threshold", v -> config.sai_indexes_total_fail_threshold = v, DEFAULT_INDEXES_TOTAL_THRESHOLD, DEFAULT_INDEXES_TOTAL_THRESHOLD, DEFAULT_INDEXES_TOTAL_THRESHOLD);

        enforceDefault("offset_rows_warn_threshold", v -> config.offset_rows_warn_threshold = v, 10000, 10000, 10000);
        enforceDefault("offset_rows_fail_threshold", v -> config.offset_rows_fail_threshold = v, 20000, 20000, 20000);

        enforceDefault("query_filters_warn_threshold", v -> config.query_filters_warn_threshold = v, -1, -1, -1);
        enforceDefault("query_filters_fail_threshold", v -> config.query_filters_fail_threshold = v, -1, -1, -1);
    }

    /**
     * Enforce default value based on {@link DatabaseDescriptor#isEmulateDbaasDefaults()} if
     * it's not specified in yaml
     *
     * @param configName current config value defined in yaml
     * @param optionSetter setter to updated given config
     * @param noProfileDefault default value for on-prem
     * @param dbaasDefault default value for constellation DB-as-a-service
     * @param hcdDefault default value for HCD
     * @param <T>
     */
    private <T> void enforceDefault(String configName, Consumer<T> optionSetter, T noProfileDefault, T dbaasDefault, T hcdDefault)
    {
        if (config.userSetConfigKeys.contains(configName))
            return;

        if (DatabaseDescriptor.isEmulateDbaasDefaults())
            optionSetter.accept(dbaasDefault);
        else if (DatabaseDescriptor.isHcdGuardrailsDefaults())
            optionSetter.accept(hcdDefault);
        else
            optionSetter.accept(noProfileDefault);
    }

    private void enforceDefault(String configName, IntConsumer optionSetter, int noProfileDefault, int dbaasDefault, int hcdDefault)
    {
        if (config.userSetConfigKeys.contains(configName))
            return;

        if (DatabaseDescriptor.isEmulateDbaasDefaults())
            optionSetter.accept(dbaasDefault);
        else if (DatabaseDescriptor.isHcdGuardrailsDefaults())
            optionSetter.accept(hcdDefault);
        else
            optionSetter.accept(noProfileDefault);
    }

    private void enforceDefault(String configName, Consumer<Boolean> optionSetter, boolean noProfileDefault, boolean dbaasDefault, boolean hcdDefault)
    {
        if (config.userSetConfigKeys.contains(configName))
            return;

        if (DatabaseDescriptor.isEmulateDbaasDefaults())
            optionSetter.accept(dbaasDefault);
        else if (DatabaseDescriptor.isHcdGuardrailsDefaults())
            optionSetter.accept(hcdDefault);
        else
            optionSetter.accept(noProfileDefault);
    }

    @Override
    public int getKeyspacesWarnThreshold()
    {
        return config.keyspaces_warn_threshold;
    }

    @Override
    public int getKeyspacesFailThreshold()
    {
        return config.keyspaces_fail_threshold;
    }

    public void setKeyspacesThreshold(int warn, int fail)
    {
        validateMaxIntThreshold(warn, fail, "keyspaces");
        updatePropertyWithLogging("keyspaces_warn_threshold",
                                  warn,
                                  () -> config.keyspaces_warn_threshold,
                                  x -> config.keyspaces_warn_threshold = x);
        updatePropertyWithLogging("keyspaces_fail_threshold",
                                  fail,
                                  () -> config.keyspaces_fail_threshold,
                                  x -> config.keyspaces_fail_threshold = x);
    }

    @Override
    public int getTablesWarnThreshold()
    {
        return config.tables_warn_threshold;
    }

    @Override
    public int getTablesFailThreshold()
    {
        return config.tables_fail_threshold;
    }

    public void setTablesThreshold(int warn, int fail)
    {
        validateMaxIntThreshold(warn, fail, "tables");
        updatePropertyWithLogging("tables_warn_threshold",
                                  warn,
                                  () -> config.tables_warn_threshold,
                                  x -> config.tables_warn_threshold = x);
        updatePropertyWithLogging("tables_fail_threshold",
                                  fail,
                                  () -> config.tables_fail_threshold,
                                  x -> config.tables_fail_threshold = x);
    }

    @Override
    public int getColumnsPerTableWarnThreshold()
    {
        return config.columns_per_table_warn_threshold;
    }

    @Override
    public int getColumnsPerTableFailThreshold()
    {
        return config.columns_per_table_fail_threshold;
    }

    public void setColumnsPerTableThreshold(int warn, int fail)
    {
        validateMaxIntThreshold(warn, fail, "columns_per_table");
        updatePropertyWithLogging("columns_per_table_warn_threshold",
                                  warn,
                                  () -> config.columns_per_table_warn_threshold,
                                  x -> config.columns_per_table_warn_threshold = x);
        updatePropertyWithLogging("columns_per_table_fail_threshold",
                                  fail,
                                  () -> config.columns_per_table_fail_threshold,
                                  x -> config.columns_per_table_fail_threshold = x);
    }

    @Override
    public int getSecondaryIndexesPerTableWarnThreshold()
    {
        return config.secondary_indexes_per_table_warn_threshold;
    }

    @Override
    public int getSecondaryIndexesPerTableFailThreshold()
    {
        return config.secondary_indexes_per_table_fail_threshold;
    }

    public void setSecondaryIndexesPerTableThreshold(int warn, int fail)
    {
        validateMaxIntThreshold(warn, fail, "secondary_indexes_per_table");
        updatePropertyWithLogging("secondary_indexes_per_table_warn_threshold",
                                  warn,
                                  () -> config.secondary_indexes_per_table_warn_threshold,
                                  x -> config.secondary_indexes_per_table_warn_threshold = x);
        updatePropertyWithLogging("secondary_indexes_per_table_fail_threshold",
                                  fail,
                                  () -> config.secondary_indexes_per_table_fail_threshold,
                                  x -> config.secondary_indexes_per_table_fail_threshold = x);
    }

    @Override
    public int getSasiIndexesPerTableWarnThreshold()
    {
        return config.sasi_indexes_per_table_warn_threshold;
    }

    @Override
    public int getSasiIndexesPerTableFailThreshold()
    {
        return config.sasi_indexes_per_table_fail_threshold;
    }

    public void setSasiIndexesPerTableThreshold(int warn, int fail)
    {
        validateMaxIntThreshold(warn, fail, "sasi_indexes_per_table");
        updatePropertyWithLogging("sasi_indexes_per_table_warn_threshold",
                                  warn,
                                  () -> config.sasi_indexes_per_table_warn_threshold,
                                  x -> config.sasi_indexes_per_table_warn_threshold = x);
        updatePropertyWithLogging("sai_indexes_per_table_fail_threshold",
                                  fail,
                                  () -> config.sasi_indexes_per_table_fail_threshold,
                                  x -> config.sasi_indexes_per_table_fail_threshold = x);
    }

    @Override
    public int getStorageAttachedIndexesPerTableWarnThreshold()
    {
        return config.sai_indexes_per_table_warn_threshold;
    }

    @Override
    public int getStorageAttachedIndexesPerTableFailThreshold()
    {
        return config.sai_indexes_per_table_fail_threshold;
    }

    public void setStorageAttachedIndexesPerTableThreshold(int warn, int fail)
    {
        validateMaxIntThreshold(warn, fail, "sai_indexes_per_table");
        updatePropertyWithLogging("sai_indexes_per_table_warn_threshold",
                                  warn,
                                  () -> config.sai_indexes_per_table_warn_threshold,
                                  x -> config.sai_indexes_per_table_warn_threshold = x);
        updatePropertyWithLogging("sai_indexes_per_table_fail_threshold",
                                  fail,
                                  () -> config.sai_indexes_per_table_fail_threshold,
                                  x -> config.sai_indexes_per_table_fail_threshold = x);
    }

    @Override
    public int getStorageAttachedIndexesTotalWarnThreshold()
    {
        return config.sai_indexes_total_warn_threshold;
    }

    @Override
    public int getStorageAttachedIndexesTotalFailThreshold()
    {
        return config.sai_indexes_total_fail_threshold;
    }

    public void setStorageAttachedIndexesTotalThreshold(int warn, int fail)
    {
        validateMaxIntThreshold(warn, fail, "sai_indexes_total");
        updatePropertyWithLogging("sai_indexes_total_warn_threshold",
                                  warn,
                                  () -> config.sai_indexes_total_warn_threshold,
                                  x -> config.sai_indexes_total_warn_threshold = x);
        updatePropertyWithLogging("sai_indexes_total_fail_threshold",
                                  fail,
                                  () -> config.sai_indexes_total_fail_threshold,
                                  x -> config.sai_indexes_total_fail_threshold = x);
    }

    @Override
    public int getMaterializedViewsPerTableWarnThreshold()
    {
        return config.materialized_views_per_table_warn_threshold;
    }

    @Override
    public int getPartitionKeysInSelectWarnThreshold()
    {
        return config.partition_keys_in_select_warn_threshold;
    }

    @Override
    public int getPartitionKeysInSelectFailThreshold()
    {
        return config.partition_keys_in_select_fail_threshold;
    }

    public void setPartitionKeysInSelectThreshold(int warn, int fail)
    {
        validateMaxIntThreshold(warn, fail, "partition_keys_in_select");
        updatePropertyWithLogging("partition_keys_in_select_warn_threshold",
                                  warn,
                                  () -> config.partition_keys_in_select_warn_threshold,
                                  x -> config.partition_keys_in_select_warn_threshold = x);
        updatePropertyWithLogging("partition_keys_in_select_fail_threshold",
                                  fail,
                                  () -> config.partition_keys_in_select_fail_threshold,
                                  x -> config.partition_keys_in_select_fail_threshold = x);
    }

    @Override
    public int getMaterializedViewsPerTableFailThreshold()
    {
        return config.materialized_views_per_table_fail_threshold;
    }

    public void setMaterializedViewsPerTableThreshold(int warn, int fail)
    {
        validateMaxIntThreshold(warn, fail, "materialized_views_per_table");
        updatePropertyWithLogging("materialized_views_per_table_warn_threshold",
                                  warn,
                                  () -> config.materialized_views_per_table_warn_threshold,
                                  x -> config.materialized_views_per_table_warn_threshold = x);
        updatePropertyWithLogging("materialized_views_per_table_fail_threshold",
                                  fail,
                                  () -> config.materialized_views_per_table_fail_threshold,
                                  x -> config.materialized_views_per_table_fail_threshold = x);
    }

    @Override
    public int getPageSizeWarnThreshold()
    {
        return config.page_size_warn_threshold;
    }

    @Override
    public int getPageSizeFailThreshold()
    {
        return config.page_size_fail_threshold;
    }

    @Override
    @Nullable
    public DataStorageSpec.IntBytesBound getPageWeightWarnThreshold()
    {
        return config.page_weight_warn_threshold;
    }

    @Override
    @Nullable
    public DataStorageSpec.IntBytesBound getPageWeightFailThreshold()
    {
        return config.page_weight_fail_threshold;
    }

    public void setPageSizeThreshold(int warn, int fail)
    {
        validateMaxIntThreshold(warn, fail, "page_size");
        updatePropertyWithLogging("page_size_warn_threshold",
                                  warn,
                                  () -> config.page_size_warn_threshold,
                                  x -> config.page_size_warn_threshold = x);
        updatePropertyWithLogging("page_size_fail_threshold",
                                  fail,
                                  () -> config.page_size_fail_threshold,
                                  x -> config.page_size_fail_threshold = x);
    }

    public void setPageWeightThreshold(DataStorageSpec.IntBytesBound warn, DataStorageSpec.IntBytesBound fail)
    {
        validateSizeThreshold(warn, fail, false, "page_weight");
        updatePropertyWithLogging("page_weight_warn_threshold",
                                  warn,
                                  () -> config.page_weight_warn_threshold,
                                  x -> config.page_weight_warn_threshold = x);
        updatePropertyWithLogging("page_weight_fail_threshold",
                                  fail,
                                  () -> config.page_weight_fail_threshold,
                                  x -> config.page_weight_fail_threshold = x);
    }

    @Override
    public Set<String> getTablePropertiesWarned()
    {
        return config.table_properties_warned;
    }

    public void setTablePropertiesWarned(Set<String> properties)
    {
        updatePropertyWithLogging("table_properties_warned",
                                  validateTableProperties(properties, "table_properties_warned"),
                                  () -> config.table_properties_warned,
                                  x -> config.table_properties_warned = x);
    }

    @Override
    public Set<String> getTablePropertiesIgnored()
    {
        return config.table_properties_ignored;
    }

    public void setTablePropertiesIgnored(Set<String> properties)
    {
        updatePropertyWithLogging("table_properties_ignored",
                                  validateTableProperties(properties, "table_properties_ignored"),
                                  () -> config.table_properties_ignored,
                                  x -> config.table_properties_ignored = x);
    }

    @Override
    public Set<String> getTablePropertiesDisallowed()
    {
        return config.table_properties_disallowed;
    }

    public void setTablePropertiesDisallowed(Set<String> properties)
    {
        updatePropertyWithLogging("table_properties_disallowed",
                                  validateTableProperties(properties, "table_properties_disallowed"),
                                  () -> config.table_properties_disallowed,
                                  x -> config.table_properties_disallowed = x);
    }

    @Override
    public boolean getUserTimestampsEnabled()
    {
        return config.user_timestamps_enabled;
    }

    public void setUserTimestampsEnabled(boolean enabled)
    {
        updatePropertyWithLogging("user_timestamps_enabled",
                                  enabled,
                                  () -> config.user_timestamps_enabled,
                                  x -> config.user_timestamps_enabled = x);
    }

    @Override
    public boolean getGroupByEnabled()
    {
        return config.group_by_enabled;
    }

    public void setGroupByEnabled(boolean enabled)
    {
        updatePropertyWithLogging("group_by_enabled",
                                  enabled,
                                  () -> config.group_by_enabled,
                                  x -> config.group_by_enabled = x);
    }

    @Override
    public boolean getLoggedBatchEnabled()
    {
        return config.logged_batch_enabled;
    }

    public void setLoggedBatchEnabled(boolean enabled)
    {
        updatePropertyWithLogging("logged_batch_enabled",
                                  enabled,
                                  () -> config.logged_batch_enabled,
                                  x -> config.logged_batch_enabled = x);
    }

    @Override
    public boolean getDropTruncateTableEnabled()
    {
        return config.drop_truncate_table_enabled;
    }

    public void setDropTruncateTableEnabled(boolean enabled)
    {
        updatePropertyWithLogging("drop_truncate_table_enabled",
                                  enabled,
                                  () -> config.drop_truncate_table_enabled,
                                  x -> config.drop_truncate_table_enabled = x);
    }

    @Override
    public boolean getDropKeyspaceEnabled()
    {
        return config.drop_keyspace_enabled;
    }

    public void setDropKeyspaceEnabled(boolean enabled)
    {
        updatePropertyWithLogging("drop_keyspace_enabled",
                                  enabled,
                                  () -> config.drop_keyspace_enabled,
                                  x -> config.drop_keyspace_enabled = x);
    }

    @Override
    public boolean getSecondaryIndexesEnabled()
    {
        return config.secondary_indexes_enabled;
    }

    public void setSecondaryIndexesEnabled(boolean enabled)
    {
        updatePropertyWithLogging("secondary_indexes_enabled",
                                  enabled,
                                  () -> config.secondary_indexes_enabled,
                                  x -> config.secondary_indexes_enabled = x);
    }

    @Override
    public boolean getUncompressedTablesEnabled()
    {
        return config.uncompressed_tables_enabled;
    }

    public void setUncompressedTablesEnabled(boolean enabled)
    {
        updatePropertyWithLogging("uncompressed_tables_enabled",
                                  enabled,
                                  () -> config.uncompressed_tables_enabled,
                                  x -> config.uncompressed_tables_enabled = x);
    }

    @Override
    public boolean getCompactTablesEnabled()
    {
        return config.compact_tables_enabled;
    }

    public void setCompactTablesEnabled(boolean enabled)
    {
        updatePropertyWithLogging("compact_tables_enabled",
                                  enabled,
                                  () -> config.compact_tables_enabled,
                                  x -> config.compact_tables_enabled = x);
    }

    @Override
    public boolean getAlterTableEnabled()
    {
        return config.alter_table_enabled;
    }

    public void setAlterTableEnabled(boolean enabled)
    {
        updatePropertyWithLogging("alter_table_enabled",
                                  enabled,
                                  () -> config.alter_table_enabled,
                                  x -> config.alter_table_enabled = x);
    }

    @Override
    public boolean getReadBeforeWriteListOperationsEnabled()
    {
        return config.read_before_write_list_operations_enabled;
    }

    public void setReadBeforeWriteListOperationsEnabled(boolean enabled)
    {
        updatePropertyWithLogging("read_before_write_list_operations_enabled",
                                  enabled,
                                  () -> config.read_before_write_list_operations_enabled,
                                  x -> config.read_before_write_list_operations_enabled = x);
    }

    @Override
    public boolean getAllowFilteringEnabled()
    {
        return config.allow_filtering_enabled;
    }

    public void setAllowFilteringEnabled(boolean enabled)
    {
        updatePropertyWithLogging("allow_filtering_enabled",
                                  enabled,
                                  () -> config.allow_filtering_enabled,
                                  x -> config.allow_filtering_enabled = x);
    }

    @Override
    public boolean getSimpleStrategyEnabled()
    {
        return config.simplestrategy_enabled;
    }

    public void setSimpleStrategyEnabled(boolean enabled)
    {
        updatePropertyWithLogging("simplestrategy_enabled",
                                  enabled,
                                  () -> config.simplestrategy_enabled,
                                  x -> config.simplestrategy_enabled = x);
    }

    @Override
    public boolean getCounterEnabled()
    {
        return config.counter_enabled;
    }

    public void setCounterEnabled(boolean enabled)
    {
        updatePropertyWithLogging("counter_enabled",
                enabled,
                () -> config.counter_enabled,
                x -> config.counter_enabled = x);
    }

    @Override
    public int getInSelectCartesianProductWarnThreshold()
    {
        return config.in_select_cartesian_product_warn_threshold;
    }

    @Override
    public int getInSelectCartesianProductFailThreshold()
    {
        return config.in_select_cartesian_product_fail_threshold;
    }

    public void setInSelectCartesianProductThreshold(int warn, int fail)
    {
        validateMaxIntThreshold(warn, fail, "in_select_cartesian_product");
        updatePropertyWithLogging("in_select_cartesian_product_warn_threshold",
                                  warn,
                                  () -> config.in_select_cartesian_product_warn_threshold,
                                  x -> config.in_select_cartesian_product_warn_threshold = x);
        updatePropertyWithLogging("in_select_cartesian_product_fail_threshold",
                                  fail,
                                  () -> config.in_select_cartesian_product_fail_threshold,
                                  x -> config.in_select_cartesian_product_fail_threshold = x);
    }

    public Set<ConsistencyLevel> getReadConsistencyLevelsWarned()
    {
        return config.read_consistency_levels_warned;
    }

    public void setReadConsistencyLevelsWarned(Set<ConsistencyLevel> consistencyLevels)
    {
        updatePropertyWithLogging("read_consistency_levels_warned",
                                  validateConsistencyLevels(consistencyLevels, "read_consistency_levels_warned"),
                                  () -> config.read_consistency_levels_warned,
                                  x -> config.read_consistency_levels_warned = x);
    }

    @Override
    public Set<ConsistencyLevel> getReadConsistencyLevelsDisallowed()
    {
        return config.read_consistency_levels_disallowed;
    }

    public void setReadConsistencyLevelsDisallowed(Set<ConsistencyLevel> consistencyLevels)
    {
        updatePropertyWithLogging("read_consistency_levels_disallowed",
                                  validateConsistencyLevels(consistencyLevels, "read_consistency_levels_disallowed"),
                                  () -> config.read_consistency_levels_disallowed,
                                  x -> config.read_consistency_levels_disallowed = x);
    }

    @Override
    public Set<ConsistencyLevel> getWriteConsistencyLevelsWarned()
    {
        return config.write_consistency_levels_warned;
    }

    public void setWriteConsistencyLevelsWarned(Set<ConsistencyLevel> consistencyLevels)
    {
        updatePropertyWithLogging("write_consistency_levels_warned",
                                  validateConsistencyLevels(consistencyLevels, "write_consistency_levels_warned"),
                                  () -> config.write_consistency_levels_warned,
                                  x -> config.write_consistency_levels_warned = x);
    }

    @Override
    public Set<ConsistencyLevel> getWriteConsistencyLevelsDisallowed()
    {
        return config.write_consistency_levels_disallowed;
    }

    public void setWriteConsistencyLevelsDisallowed(Set<ConsistencyLevel> consistencyLevels)
    {
        updatePropertyWithLogging("write_consistency_levels_disallowed",
                                  validateConsistencyLevels(consistencyLevels, "write_consistency_levels_disallowed"),
                                  () -> config.write_consistency_levels_disallowed,
                                  x -> config.write_consistency_levels_disallowed = x);
    }

    @Override
    @Nullable
    public DataStorageSpec.LongBytesBound getPartitionSizeWarnThreshold()
    {
        return config.partition_size_warn_threshold;
    }

    @Override
    @Nullable
    public DataStorageSpec.LongBytesBound getPartitionSizeFailThreshold()
    {
        return config.partition_size_fail_threshold;
    }

    public void setPartitionSizeThreshold(@Nullable DataStorageSpec.LongBytesBound warn, @Nullable DataStorageSpec.LongBytesBound fail)
    {
        validateSizeThreshold(warn, fail, false, "partition_size");
        updatePropertyWithLogging("partition_size_warn_threshold",
                                  warn,
                                  () -> config.partition_size_warn_threshold,
                                  x -> config.partition_size_warn_threshold = x);
        updatePropertyWithLogging("partition_size_fail_threshold",
                                  fail,
                                  () -> config.partition_size_fail_threshold,
                                  x -> config.partition_size_fail_threshold = x);
    }

    @Override
    public long getPartitionTombstonesWarnThreshold()
    {
        return config.partition_tombstones_warn_threshold;
    }

    @Override
    public long getPartitionTombstonesFailThreshold()
    {
        return config.partition_tombstones_fail_threshold;
    }

    public void setPartitionTombstonesThreshold(long warn, long fail)
    {
        validateMaxLongThreshold(warn, fail, "partition_tombstones", false);
        updatePropertyWithLogging("partition_tombstones_warn_threshold",
                                  warn,
                                  () -> config.partition_tombstones_warn_threshold,
                                  x -> config.partition_tombstones_warn_threshold = x);
        updatePropertyWithLogging("partition_tombstones_fail_threshold",
                                  fail,
                                  () -> config.partition_tombstones_fail_threshold,
                                  x -> config.partition_tombstones_fail_threshold = x);
    }

    @Override
    @Nullable
    public DataStorageSpec.LongBytesBound getColumnValueSizeWarnThreshold()
    {
        return config.column_value_size_warn_threshold;
    }

    @Override
    @Nullable
    public DataStorageSpec.LongBytesBound getColumnValueSizeFailThreshold()
    {
        return config.column_value_size_fail_threshold;
    }

    public void setColumnValueSizeThreshold(@Nullable DataStorageSpec.LongBytesBound warn, @Nullable DataStorageSpec.LongBytesBound fail)
    {
        validateSizeThreshold(warn, fail, false, "column_value_size");
        updatePropertyWithLogging("column_value_size_warn_threshold",
                                  warn,
                                  () -> config.column_value_size_warn_threshold,
                                  x -> config.column_value_size_warn_threshold = x);
        updatePropertyWithLogging("column_value_size_fail_threshold",
                                  fail,
                                  () -> config.column_value_size_fail_threshold,
                                  x -> config.column_value_size_fail_threshold = x);
    }

    @Override
    @Nullable
    public DataStorageSpec.LongBytesBound getCollectionSizeWarnThreshold()
    {
        return config.collection_size_warn_threshold;
    }

    @Override
    @Nullable
    public DataStorageSpec.LongBytesBound getCollectionSizeFailThreshold()
    {
        return config.collection_size_fail_threshold;
    }

        public void setCollectionSizeThreshold(@Nullable DataStorageSpec.LongBytesBound warn, @Nullable DataStorageSpec.LongBytesBound fail)
    {
        validateSizeThreshold(warn, fail, false, "collection_size");
        updatePropertyWithLogging("collection_size_warn_threshold",
                                  warn,
                                  () -> config.collection_size_warn_threshold,
                                  x -> config.collection_size_warn_threshold = x);
        updatePropertyWithLogging("collection_size_fail_threshold",
                                  fail,
                                  () -> config.collection_size_fail_threshold,
                                  x -> config.collection_size_fail_threshold = x);
    }

    @Override
    public int getItemsPerCollectionWarnThreshold()
    {
        return config.items_per_collection_warn_threshold;
    }

    @Override
    public int getItemsPerCollectionFailThreshold()
    {
        return config.items_per_collection_fail_threshold;
    }

    public void setItemsPerCollectionThreshold(int warn, int fail)
    {
        validateMaxIntThreshold(warn, fail, "items_per_collection");
        updatePropertyWithLogging("items_per_collection_warn_threshold",
                                  warn,
                                  () -> config.items_per_collection_warn_threshold,
                                  x -> config.items_per_collection_warn_threshold = x);
        updatePropertyWithLogging("items_per_collection_fail_threshold",
                                  fail,
                                  () -> config.items_per_collection_fail_threshold,
                                  x -> config.items_per_collection_fail_threshold = x);
    }

    @Override
    public int getFieldsPerUDTWarnThreshold()
    {
        return config.fields_per_udt_warn_threshold;
    }

    @Override
    public int getFieldsPerUDTFailThreshold()
    {
        return config.fields_per_udt_fail_threshold;
    }

    public void setFieldsPerUDTThreshold(int warn, int fail)
    {
        validateMaxIntThreshold(warn, fail, "fields_per_udt");
        updatePropertyWithLogging("fields_per_udt_warn_threshold",
                                  warn,
                                  () -> config.fields_per_udt_warn_threshold,
                                  x -> config.fields_per_udt_warn_threshold = x);
        updatePropertyWithLogging("fields_per_udt_fail_threshold",
                                  fail,
                                  () -> config.fields_per_udt_fail_threshold,
                                  x -> config.fields_per_udt_fail_threshold = x);
    }

    @Override
    public int getVectorDimensionsWarnThreshold()
    {
        return config.vector_dimensions_warn_threshold;
    }

    @Override
    public int getVectorDimensionsFailThreshold()
    {
        return config.vector_dimensions_fail_threshold;
    }

    public void setVectorDimensionsThreshold(int warn, int fail)
    {
        validateMaxIntThreshold(warn, fail, "vector_dimensions");
        updatePropertyWithLogging("vector_dimensions_warn_threshold",
                                  warn,
                                  () -> config.vector_dimensions_warn_threshold,
                                  x -> config.vector_dimensions_warn_threshold = x);
        updatePropertyWithLogging("vector_dimensions_fail_threshold",
                                  fail,
                                  () -> config.vector_dimensions_fail_threshold,
                                  x -> config.vector_dimensions_fail_threshold = x);
    }

    @Override
    public int getSaiAnnRerankKWarnThreshold()
    {
        return config.sai_ann_rerank_k_warn_threshold;
    }

    @Override
    public int getSaiAnnRerankKFailThreshold()
    {
        return config.sai_ann_rerank_k_fail_threshold;
    }

    public void setSaiAnnRerankKThreshold(int warn, int fail)
    {
        validateMaxIntThreshold(warn, fail, "sai_ann_rerank_k_max_value");
        updatePropertyWithLogging("sai_ann_rerank_k_warn_threshold",
                                  warn,
                                  () -> config.sai_ann_rerank_k_warn_threshold,
                                  x -> config.sai_ann_rerank_k_warn_threshold = x);
        updatePropertyWithLogging("sai_ann_rerank_k_fail_threshold",
                                  fail,
                                  () -> config.sai_ann_rerank_k_fail_threshold,
                                  x -> config.sai_ann_rerank_k_fail_threshold = x);
    }

    public int getDataDiskUsagePercentageWarnThreshold()
    {
        return config.data_disk_usage_percentage_warn_threshold;
    }

    @Override
    public int getDataDiskUsagePercentageFailThreshold()
    {
        return config.data_disk_usage_percentage_fail_threshold;
    }

    public void setDataDiskUsagePercentageThreshold(int warn, int fail)
    {
        validatePercentageThreshold(warn, fail, "data_disk_usage_percentage");
        updatePropertyWithLogging("data_disk_usage_percentage_warn_threshold",
                                  warn,
                                  () -> config.data_disk_usage_percentage_warn_threshold,
                                  x -> config.data_disk_usage_percentage_warn_threshold = x);
        updatePropertyWithLogging("data_disk_usage_percentage_fail_threshold",
                                  fail,
                                  () -> config.data_disk_usage_percentage_fail_threshold,
                                  x -> config.data_disk_usage_percentage_fail_threshold = x);
    }

    @Override
    public DataStorageSpec.LongBytesBound getDataDiskUsageMaxDiskSize()
    {
        return config.data_disk_usage_max_disk_size;
    }

    public void setDataDiskUsageMaxDiskSize(@Nullable DataStorageSpec.LongBytesBound diskSize)
    {
        validateDataDiskUsageMaxDiskSize(diskSize);
        updatePropertyWithLogging("data_disk_usage_max_disk_size",
                                  diskSize,
                                  () -> config.data_disk_usage_max_disk_size,
                                  x -> config.data_disk_usage_max_disk_size = x);
    }

    @Override
    public int getMinimumReplicationFactorWarnThreshold()
    {
        return config.minimum_replication_factor_warn_threshold;
    }

    @Override
    public int getMinimumReplicationFactorFailThreshold()
    {
        return config.minimum_replication_factor_fail_threshold;
    }

    public void setMinimumReplicationFactorThreshold(int warn, int fail)
    {
        validateMinRFThreshold(warn, fail);
        updatePropertyWithLogging("minimum_replication_factor_warn_threshold",
                                  warn,
                                  () -> config.minimum_replication_factor_warn_threshold,
                                  x -> config.minimum_replication_factor_warn_threshold = x);
        updatePropertyWithLogging("minimum_replication_factor_fail_threshold",
                                  fail,
                                  () -> config.minimum_replication_factor_fail_threshold,
                                  x -> config.minimum_replication_factor_fail_threshold = x);
    }

    @Override
    public int getMaximumReplicationFactorWarnThreshold()
    {
        return config.maximum_replication_factor_warn_threshold;
    }

    @Override
    public int getMaximumReplicationFactorFailThreshold()
    {
        return config.maximum_replication_factor_fail_threshold;
    }

    public void setMaximumReplicationFactorThreshold(int warn, int fail)
    {
        validateMaxRFThreshold(warn, fail);
        updatePropertyWithLogging("maximum_replication_factor_warn_threshold",
                                  warn,
                                  () -> config.maximum_replication_factor_warn_threshold,
                                  x -> config.maximum_replication_factor_warn_threshold = x);
        updatePropertyWithLogging("maximum_replication_factor_fail_threshold",
                                  fail,
                                  () -> config.maximum_replication_factor_fail_threshold,
                                  x -> config.maximum_replication_factor_fail_threshold = x);
    }

    @Override
    public boolean getZeroTTLOnTWCSWarned()
    {
        return config.zero_ttl_on_twcs_warned;
    }

    @Override
    public void setZeroTTLOnTWCSWarned(boolean value)
    {
        updatePropertyWithLogging("zero_ttl_on_twcs_warned",
                                  value,
                                  () -> config.zero_ttl_on_twcs_warned,
                                  x -> config.zero_ttl_on_twcs_warned = x);
    }

    @Override
    public boolean getZeroTTLOnTWCSEnabled()
    {
        return config.zero_ttl_on_twcs_enabled;
    }

    @Override
    public void setZeroTTLOnTWCSEnabled(boolean value)
    {
        updatePropertyWithLogging("zero_ttl_on_twcs_enabled",
                                  value,
                                  () -> config.zero_ttl_on_twcs_enabled,
                                  x -> config.zero_ttl_on_twcs_enabled = x);
    }

    @Override
    public boolean getIntersectFilteringQueryWarned()
    {
        return config.intersect_filtering_query_warned;
    }

    @Override
    public void setIntersectFilteringQueryWarned(boolean value)
    {
        updatePropertyWithLogging("intersect_filtering_query_warned",
                                  value,
                                  () -> config.intersect_filtering_query_warned,
                                  x -> config.intersect_filtering_query_warned = x);
    }

    @Override
    public boolean getIntersectFilteringQueryEnabled()
    {
        return config.intersect_filtering_query_enabled;
    }

    public void setIntersectFilteringQueryEnabled(boolean value)
    {
        updatePropertyWithLogging("intersect_filtering_query_enabled",
                                  value,
                                  () -> config.intersect_filtering_query_enabled,
                                  x -> config.intersect_filtering_query_enabled = x);
    }

    @Override
    public  DurationSpec.LongMicrosecondsBound getMaximumTimestampWarnThreshold()
    {
        return config.maximum_timestamp_warn_threshold;
    }

    @Override
    public DurationSpec.LongMicrosecondsBound getMaximumTimestampFailThreshold()
    {
        return config.maximum_timestamp_fail_threshold;
    }

    @Override
    public void setMaximumTimestampThreshold(@Nullable DurationSpec.LongMicrosecondsBound warn,
                                             @Nullable DurationSpec.LongMicrosecondsBound fail)
    {
        validateTimestampThreshold(warn, fail, "maximum_timestamp");

        updatePropertyWithLogging("maximum_timestamp_warn_threshold",
                                  warn,
                                  () -> config.maximum_timestamp_warn_threshold,
                                  x -> config.maximum_timestamp_warn_threshold = x);

        updatePropertyWithLogging("maximum_timestamp_fail_threshold",
                                  fail,
                                  () -> config.maximum_timestamp_fail_threshold,
                                  x -> config.maximum_timestamp_fail_threshold = x);
    }

    @Override
    public  DurationSpec.LongMicrosecondsBound getMinimumTimestampWarnThreshold()
    {
        return config.minimum_timestamp_warn_threshold;
    }

    @Override
    public DurationSpec.LongMicrosecondsBound getMinimumTimestampFailThreshold()
    {
        return config.minimum_timestamp_fail_threshold;
    }

    @Override
    public void setMinimumTimestampThreshold(@Nullable DurationSpec.LongMicrosecondsBound warn,
                                             @Nullable DurationSpec.LongMicrosecondsBound fail)
    {
        validateTimestampThreshold(warn, fail, "minimum_timestamp");

        updatePropertyWithLogging("minimum_timestamp_warn_threshold",
                                  warn,
                                  () -> config.minimum_timestamp_warn_threshold,
                                  x -> config.minimum_timestamp_warn_threshold = x);

        updatePropertyWithLogging("minimum_timestamp_fail_threshold",
                                  fail,
                                  () -> config.minimum_timestamp_fail_threshold,
                                  x -> config.minimum_timestamp_fail_threshold = x);
    }

    @Override
    public int getSaiSSTableIndexesPerQueryWarnThreshold()
    {
        return config.sai_sstable_indexes_per_query_warn_threshold;
    }

    @Override
    public int getSaiSSTableIndexesPerQueryFailThreshold()
    {
        return config.sai_sstable_indexes_per_query_fail_threshold;
    }

    @Override
    public void setSaiSSTableIndexesPerQueryThreshold(int warn, int fail)
    {
        validateMaxIntThreshold(warn, fail, "sai_sstable_indexes_per_query");
        updatePropertyWithLogging("sai_sstable_indexes_per_query_warn_threshold",
                                  warn,
                                  () -> config.sai_sstable_indexes_per_query_warn_threshold,
                                  x -> config.sai_sstable_indexes_per_query_warn_threshold = x);

        updatePropertyWithLogging("sai_sstable_indexes_per_query_fail_threshold",
                                  fail,
                                  () -> config.sai_sstable_indexes_per_query_fail_threshold,
                                  x -> config.sai_sstable_indexes_per_query_fail_threshold = x);
    }

    @Override
    @Nullable
    public DataStorageSpec.LongBytesBound getSaiStringTermSizeWarnThreshold()
    {
        return config.sai_string_term_size_warn_threshold;
    }

    @Override
    @Nullable
    public DataStorageSpec.LongBytesBound getSaiStringTermSizeFailThreshold()
    {
        return config.sai_string_term_size_fail_threshold;
    }

    @Override
    public void setSaiStringTermSizeThreshold(@Nullable DataStorageSpec.LongBytesBound warn, @Nullable DataStorageSpec.LongBytesBound fail)
    {
        validateSizeThreshold(warn, fail, false, "sai_string_term_size");
        updatePropertyWithLogging("sai_string_term_size_warn_threshold",
                                  warn,
                                  () -> config.sai_string_term_size_warn_threshold,
                                  x -> config.sai_string_term_size_warn_threshold = x);
        updatePropertyWithLogging("sai_string_term_size_fail_threshold",
                                  fail,
                                  () -> config.sai_string_term_size_fail_threshold,
                                  x -> config.sai_string_term_size_fail_threshold = x);
    }

    @Override
    @Nullable
    public DataStorageSpec.LongBytesBound getSaiFrozenTermSizeWarnThreshold()
    {
        return config.sai_frozen_term_size_warn_threshold;
    }

    @Override
    @Nullable
    public DataStorageSpec.LongBytesBound getSaiFrozenTermSizeFailThreshold()
    {
        return config.sai_frozen_term_size_fail_threshold;
    }

    @Override
    public void setSaiFrozenTermSizeThreshold(@Nullable DataStorageSpec.LongBytesBound warn, @Nullable DataStorageSpec.LongBytesBound fail)
    {
        validateSizeThreshold(warn, fail, false, "sai_frozen_term_size");
        updatePropertyWithLogging("sai_frozen_term_size_warn_threshold",
                                  warn,
                                  () -> config.sai_frozen_term_size_warn_threshold,
                                  x -> config.sai_frozen_term_size_warn_threshold = x);
        updatePropertyWithLogging("sai_frozen_term_size_fail_threshold",
                                  fail,
                                  () -> config.sai_frozen_term_size_fail_threshold,
                                  x -> config.sai_frozen_term_size_fail_threshold = x);
    }

    @Override
    @Nullable
    public DataStorageSpec.LongBytesBound getSaiVectorTermSizeWarnThreshold()
    {
        return config.sai_vector_term_size_warn_threshold;
    }

    @Override
    @Nullable
    public DataStorageSpec.LongBytesBound getSaiVectorTermSizeFailThreshold()
    {
        return config.sai_vector_term_size_fail_threshold;
    }

    @Override
    public void setSaiVectorTermSizeThreshold(@Nullable DataStorageSpec.LongBytesBound warn, @Nullable DataStorageSpec.LongBytesBound fail)
    {
        validateSizeThreshold(warn, fail, false, "sai_vector_term_size");
        updatePropertyWithLogging("sai_vector_term_size_warn_threshold",
                                  warn,
                                  () -> config.sai_vector_term_size_warn_threshold,
                                  x -> config.sai_vector_term_size_warn_threshold = x);
        updatePropertyWithLogging("sai_vector_term_size_fail_threshold",
                                  fail,
                                  () -> config.sai_vector_term_size_fail_threshold,
                                  x -> config.sai_vector_term_size_fail_threshold = x);
    }

    @Override
    public boolean getNonPartitionRestrictedQueryEnabled()
    {
        return config.non_partition_restricted_index_query_enabled;
    }

    @Override
    public void setNonPartitionRestrictedQueryEnabled(boolean enabled)
    {
        updatePropertyWithLogging("non_partition_restricted_index_query_enabled",
                                  enabled,
                                  () -> config.non_partition_restricted_index_query_enabled,
                                  x -> config.non_partition_restricted_index_query_enabled = x);
    }
    
    public int getTombstoneWarnThreshold()
    {
        return config.tombstone_warn_threshold;
    }

    @Override
    public int getTombstoneFailThreshold()
    {
        return config.tombstone_failure_threshold;
    }

    public void setTombstonesThreshold(int warn, int fail)
    {
        validateMaxLongThreshold(warn, fail, "tombstones", false);
        updatePropertyWithLogging("tombstones_warn_threshold",
                                  warn,
                                  () -> config.tombstone_warn_threshold,
                                  x -> config.tombstone_warn_threshold = x);
        updatePropertyWithLogging("tombstones_fail_threshold",
                                  fail,
                                  () -> config.tombstone_failure_threshold,
                                  x -> config.tombstone_failure_threshold = x);
    }

    @Override
    public long getBatchSizeWarnThreshold()
    {
        return config.batch_size_warn_threshold.toBytesInLong();
    }

    @Override
    public long getBatchSizeFailThreshold()
    {
        return config.batch_size_fail_threshold.toBytesInLong();
    }

    @Override
    public long getUnloggedBatchAcrossPartitionsWarnThreshold()
    {
        return config.unlogged_batch_across_partitions_warn_threshold;
    }

    @Override
    public long getUnloggedBatchAcrossPartitionsFailThreshold()
    {
        return -1;
    }

    @Override
    public boolean getVectorTypeEnabled()
    {
        return config.vector_type_enabled;
    }

    @Override
    public void setVectorTypeEnabled(boolean enabled)
    {
        updatePropertyWithLogging("vector_type_enabled",
                                  enabled,
                                  () -> config.vector_type_enabled,
                                  x -> config.vector_type_enabled = x);
    }

    @Override
    public int getOffsetRowsWarnThreshold()
    {
        return config.offset_rows_warn_threshold;
    }

    @Override
    public int getOffsetRowsFailThreshold()
    {
        return config.offset_rows_fail_threshold;
    }

    public void setOffsetRowsThreshold(int warn, int fail)
    {
        validateMaxIntThreshold(warn, fail, "offset_rows", false);
        updatePropertyWithLogging("offset_rows_warn_threshold",
                                  warn,
                                  () -> config.offset_rows_warn_threshold,
                                  x -> config.offset_rows_warn_threshold = x);
        updatePropertyWithLogging("offset_rows_fail_threshold",
                                  fail,
                                  () -> config.offset_rows_fail_threshold,
                                  x -> config.offset_rows_fail_threshold = x);
    }

    @Override
    public int getQueryFiltersWarnThreshold()
    {
        return config.query_filters_warn_threshold;
    }

    @Override
    public int getQueryFiltersFailThreshold()
    {
        return config.query_filters_fail_threshold;
    }

    public void setQueryFiltersThreshold(int warn, int fail)
    {
        validateMaxIntThreshold(warn, fail, "query_filters", false);
        updatePropertyWithLogging("query_filters_warn_threshold",
                                  warn,
                                  () -> config.query_filters_warn_threshold,
                                  x -> config.query_filters_warn_threshold = x);
        updatePropertyWithLogging("query_filters_fail_threshold",
                                  fail,
                                  () -> config.query_filters_fail_threshold,
                                  x -> config.query_filters_fail_threshold = x);
    }

    private static <T> void updatePropertyWithLogging(String propertyName, T newValue, Supplier<T> getter, Consumer<T> setter)
    {
        T oldValue = getter.get();
        if (newValue == null || !newValue.equals(oldValue))
        {
            setter.accept(newValue);
            logger.info("Updated {} from {} to {}", propertyName, oldValue, newValue);
        }
    }

    private static void validatePositiveNumeric(long value, long maxValue, String name)
    {
        validatePositiveNumeric(value, maxValue, name, false);
    }

    private static void validatePositiveNumeric(long value, long maxValue, String name, boolean allowZero)
    {
        if (value == -1)
            return;

        if (value > maxValue)
            throw new IllegalArgumentException(format("Invalid value %d for %s: maximum allowed value is %d",
                                                      value, name, maxValue));

        if (!allowZero && value == 0)
            throw new IllegalArgumentException(format("Invalid value for %s: 0 is not allowed; " +
                                                      "if attempting to disable use -1", name));

        // We allow -1 as a general "disabling" flag. But reject anything lower to avoid mistakes.
        if (value < 0)
            throw new IllegalArgumentException(format("Invalid value %d for %s: negative values are not allowed, " +
                                                      "outside of -1 which disables the guardrail", value, name));
    }

    private static void validatePercentage(long value, String name)
    {
        validatePositiveNumeric(value, 100, name);
    }

    private static void validatePercentageThreshold(int warn, int fail, String name)
    {
        validatePercentage(warn, name + "_warn_threshold");
        validatePercentage(fail, name + "_fail_threshold");
        validateWarnLowerThanFail(warn, fail, name);
    }

    private static void validateMaxIntThreshold(int warn, int fail, String name)
    {
        validatePositiveNumeric(warn, Integer.MAX_VALUE, name + "_warn_threshold");
        validatePositiveNumeric(fail, Integer.MAX_VALUE, name + "_fail_threshold");
        validateWarnLowerThanFail(warn, fail, name);
    }

    private static void validateMaxIntThreshold(int warn, int fail, String name, boolean allowZero)
    {
        validatePositiveNumeric(warn, Integer.MAX_VALUE, name + "_warn_threshold", allowZero);
        validatePositiveNumeric(fail, Integer.MAX_VALUE, name + "_fail_threshold", allowZero);
        validateWarnLowerThanFail(warn, fail, name);
    }

    private static void validateMaxLongThreshold(long warn, long fail, String name, boolean allowZero)
    {
        validatePositiveNumeric(warn, Long.MAX_VALUE, name + "_warn_threshold", allowZero);
        validatePositiveNumeric(fail, Long.MAX_VALUE, name + "_fail_threshold", allowZero);
        validateWarnLowerThanFail(warn, fail, name);
    }

    private static void validateMinIntThreshold(int warn, int fail, String name)
    {
        validatePositiveNumeric(warn, Integer.MAX_VALUE, name + "_warn_threshold");
        validatePositiveNumeric(fail, Integer.MAX_VALUE, name + "_fail_threshold");
        validateWarnGreaterThanFail(warn, fail, name);
    }

    private static void validateMinRFThreshold(int warn, int fail)
    {
        validateMinIntThreshold(warn, fail, "minimum_replication_factor");

        if (fail > DatabaseDescriptor.getDefaultKeyspaceRF())
            throw new IllegalArgumentException(format("minimum_replication_factor_fail_threshold to be set (%d) " +
                                                      "cannot be greater than default_keyspace_rf (%d)",
                                                      fail, DatabaseDescriptor.getDefaultKeyspaceRF()));
    }

    private static void validateMaxRFThreshold(int warn, int fail)
    {
        validateMaxIntThreshold(warn, fail, "maximum_replication_factor");

        if (fail != -1 && fail < DatabaseDescriptor.getDefaultKeyspaceRF())
            throw new IllegalArgumentException(format("maximum_replication_factor_fail_threshold to be set (%d) " +
                                                      "cannot be lesser than default_keyspace_rf (%d)",
                                                      fail, DatabaseDescriptor.getDefaultKeyspaceRF()));
    }

    public static void validateTimestampThreshold(DurationSpec.LongMicrosecondsBound warn,
                                                  DurationSpec.LongMicrosecondsBound fail,
                                                  String name)
    {
        // this function is used for both upper and lower thresholds because lower threshold is relative
        // despite using MinThreshold we still want the warn threshold to be less than or equal to
        // the fail threshold.
        validateMaxLongThreshold(warn == null ? -1 : warn.toMicroseconds(),
                                 fail == null ? -1 : fail.toMicroseconds(),
                                 name, true);
    }

    private static void validateWarnLowerThanFail(long warn, long fail, String name)
    {
        if (warn == -1 || fail == -1)
            return;

        if (fail < warn)
            throw new IllegalArgumentException(format("The warn threshold %d for %s_warn_threshold should be lower " +
                                                      "than the fail threshold %d", warn, name, fail));
    }

    private static void validateWarnGreaterThanFail(long warn, long fail, String name)
    {
        if (warn == -1 || fail == -1)
            return;

        if (fail > warn)
            throw new IllegalArgumentException(format("The warn threshold %d for %s_warn_threshold should be greater " +
                                                      "than the fail threshold %d", warn, name, fail));
    }

    private static void validateSize(DataStorageSpec size, boolean allowZero, String name)
    {
        if (size == null)
            return;

        if (!allowZero && size.quantity() == 0)
            throw new IllegalArgumentException(format("Invalid value for %s: 0 is not allowed; " +
                                                      "if attempting to disable use an empty value",
                                                      name));
    }

    private static void validateSizeThreshold(DataStorageSpec warn, DataStorageSpec fail, boolean allowZero, String name)
    {
        validateSize(warn, allowZero, name + "_warn_threshold");
        validateSize(fail, allowZero, name + "_fail_threshold");
        validateWarnLowerThanFail(warn, fail, name);
    }

    private static void validateWarnLowerThanFail(DataStorageSpec warn, DataStorageSpec fail, String name)
    {
        if (warn == null || fail == null)
            return;

        if (fail.unit().toBytes(fail.quantity()) < warn.unit().toBytes(warn.quantity()))
            throw new IllegalArgumentException(format("The warn threshold %s for %s_warn_threshold should be lower " +
                                                      "than the fail threshold %s", warn, name, fail));
    }

    private static Set<String> validateTableProperties(Set<String> properties, String name)
    {
        if (properties == null)
            throw new IllegalArgumentException(format("Invalid value for %s: null is not allowed", name));

        Set<String> lowerCaseProperties = properties.stream().map(String::toLowerCase).collect(toSet());

        Set<String> diff = Sets.difference(lowerCaseProperties, TableAttributes.allKeywords());

        if (!diff.isEmpty())
            throw new IllegalArgumentException(format("Invalid value for %s: '%s' do not parse as valid table properties",
                                                      name, diff));

        return lowerCaseProperties;
    }

    private static Set<ConsistencyLevel> validateConsistencyLevels(Set<ConsistencyLevel> consistencyLevels, String name)
    {
        if (consistencyLevels == null)
            throw new IllegalArgumentException(format("Invalid value for %s: null is not allowed", name));

        return consistencyLevels.isEmpty() ? Collections.emptySet() : Sets.immutableEnumSet(consistencyLevels);
    }

    private static void validateDataDiskUsageMaxDiskSize(DataStorageSpec.LongBytesBound maxDiskSize)
    {
        if (maxDiskSize == null)
            return;

        validateSize(maxDiskSize, false, "data_disk_usage_max_disk_size");

        long diskSize = DiskUsageMonitor.totalDiskSpace();

        if (diskSize < maxDiskSize.toBytes())
            throw new IllegalArgumentException(format("Invalid value for data_disk_usage_max_disk_size: " +
                                                      "%s specified, but only %s are actually available on disk",
                                                      maxDiskSize, FileUtils.stringifyFileSize(diskSize)));
    }
}
