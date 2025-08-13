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
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.statements.schema.TableAttributes;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.TableParams;
import org.apache.cassandra.utils.units.SizeUnit;

import javax.xml.crypto.Data;

import static java.lang.String.format;

/**
 * Configuration settings for guardrails (populated from the Yaml file).
 *
 * <p>Note that the settings here must only be used by the {@link Guardrails} class and not directly by the code
 * checking each guarded constraint (which, again, should use the higher level abstractions defined in
 * {@link Guardrails}).
 *
 * <p>We have 2 variants of guardrails, soft (warn) and hard (fail) limits, each guardrail having either one of the
 * variant or both (note in particular that hard limits only make sense for guardrails triggering during query
 * execution. For other guardrails, say one triggering during compaction, failing does not make sense).
 *
 * <p>Additionally, each individual setting should have a specific value (typically -1 for numeric settings),
 * that allows to disable the corresponding guardrail.
 *
 * <p>The default values for each guardrail settings should reflect what is mandated for DCaaS.
 *
 * <p>For consistency, guardrails based on a simple numeric threshold should use the naming scheme
 * {@code <what_is_guarded>_warn_threshold} for soft limits and {@code <what_is_guarded>_failure_threshold} for hard
 * ones, and if the value has a unit, that unit should be added at the end (for instance,
 * {@code <what_is_guarded>_failure_threshold_in_kb}). For "boolean" guardrails that disable a feature, use
 * {@code <what_is_guarded_enabled}. Other type of guardrails can use appropriate suffixes but should start with
 * {@code <what is guarded>}.
 */
public class GuardrailsConfig
{
    public static final String INDEX_GUARDRAILS_TABLE_FAILURE_THRESHOLD = Config.PROPERTY_PREFIX + "index.guardrails.table_failure_threshold";
    public static final String INDEX_GUARDRAILS_TOTAL_FAILURE_THRESHOLD = Config.PROPERTY_PREFIX + "index.guardrails.total_failure_threshold";

    public static final int NO_LIMIT = -1;
    public static final int UNSET = -2;
    public static final int DEFAULT_INDEXES_PER_TABLE_THRESHOLD = 10;
    public static final int DEFAULT_INDEXES_TOTAL_THRESHOLD = 100;

    public volatile Long column_value_size_failure_threshold_in_kb = -1L;
    public volatile Long columns_per_table_failure_threshold = -1L;
    public volatile Long fields_per_udt_failure_threshold = -1L;
    public volatile Long collection_size_warn_threshold_in_kb = -1L;
    public volatile Long items_per_collection_warn_threshold = -1L;
    public volatile Boolean read_before_write_list_operations_enabled = true;
    public volatile Integer vector_dimensions_warn_threshold = -1;
    public volatile Integer vector_dimensions_failure_threshold = 8192;
    public volatile Integer sai_ann_rerank_k_warn_threshold = -1;
    public volatile Integer sai_ann_rerank_k_fail_threshold = 4 * CassandraRelevantProperties.SAI_VECTOR_SEARCH_MAX_TOP_K.getInt();

    // Legacy 2i guardrail
    public volatile Integer secondary_index_per_table_failure_threshold = -1;
    public volatile Integer sasi_indexes_per_table_failure_threshold = -1;
    // SAI indexes guardrail
    public volatile Integer sai_indexes_per_table_failure_threshold = DEFAULT_INDEXES_PER_TABLE_THRESHOLD;
    public volatile Integer sai_indexes_total_failure_threshold = DEFAULT_INDEXES_TOTAL_THRESHOLD;
    public volatile Integer materialized_view_per_table_failure_threshold = -1;

    public volatile Long tables_warn_threshold = -1L;
    public volatile Long tables_failure_threshold = -1L;
    // N.B. Not safe for concurrent modification
    public volatile Set<String> table_properties_disallowed = Collections.emptySet();
    public volatile Set<String> table_properties_ignored = Collections.emptySet();

    public volatile Boolean user_timestamps_enabled = true;

    public volatile Boolean logged_batch_enabled = true;

    public volatile Boolean truncate_table_enabled = true;

    public volatile Boolean counter_enabled = true;

    public volatile Set<String> write_consistency_levels_disallowed = Collections.emptySet();

    // For paging by bytes having a page bigger than this threshold will result in a failure
    // For paging by rows the result will be silently cut short if it is bigger than the threshold
    public volatile Integer page_size_failure_threshold_in_kb = NO_LIMIT;

    // Limit number of terms and their cartesian product in IN query
    public volatile Integer in_select_cartesian_product_failure_threshold = -1;
    public volatile Integer partition_keys_in_select_failure_threshold = -1;

    // represent percentage of disk space, -1 means disabled
    public volatile Integer disk_usage_percentage_warn_threshold = -1;
    public volatile Integer disk_usage_percentage_failure_threshold = -1;
    public volatile Long disk_usage_max_disk_size_in_gb = -1L;

    // When executing a scan, within or across a partition, we need to keep the
    // tombstones seen in memory so we can return them to the coordinator, which
    // will use them to make sure other replicas also know about the deleted rows.
    // With workloads that generate a lot of tombstones, this can cause performance
    // problems and even exaust the server heap.
    // (http://www.datastax.com/dev/blog/cassandra-anti-patterns-queues-and-queue-like-datasets)
    // Adjust the thresholds here if you understand the dangers and want to
    // scan more tombstones anyway. These thresholds may also be adjusted at runtime
    // using the StorageService mbean.
    public volatile Integer tombstone_warn_threshold = 1000;
    public volatile Integer tombstone_failure_threshold = 100000;

    // Log WARN on any multiple-partition batch size that exceeds this value. 64kb per batch by default.
    // Use caution when increasing the size of this threshold as it can lead to node instability.
    public volatile Integer batch_size_warn_threshold_in_kb = 64;
    // Fail any multiple-partition batch that exceeds this value. The calculated default is 640kb (10x warn threshold).
    public volatile Integer batch_size_fail_threshold_in_kb = 640;
    // Log WARN on any batches not of type LOGGED than span across more partitions than this limit.
    public volatile Integer unlogged_batch_across_partitions_warn_threshold = 10;

    public volatile Integer partition_size_warn_threshold_in_mb = 100;

    // Limit the offset used in SELECT queries
    public volatile Integer offset_rows_warn_threshold = 10000;
    public volatile Integer offset_rows_failure_threshold = 20000;

    // Limit the number of column value filters per SELECT query (after applying analyzers, in case they are used)
    public volatile Integer query_filters_warn_threshold = -1;
    public volatile Integer query_filters_fail_threshold = -1;

    /**
     * If {@link DatabaseDescriptor#isEmulateDbaasDefaults()} is true, apply cloud defaults to guardrails settings that
     * are not specified in yaml; otherwise, apply on-prem defaults to guardrails settings that are not specified in yaml;
     */
    @VisibleForTesting
    public void applyDefaults()  // FIXME check this is only defaults, e.g. system props like sai_indexes_per_table_failure_threshold
    {
        assert !(DatabaseDescriptor.isHcdGuardrailsDefaults() && DatabaseDescriptor.isEmulateDbaasDefaults())
                : "Cannot set both hcdGuardrailsDefaults and emulateDbaasDefaults";

        // for read requests
        enforceDefault(v -> page_size_failure_threshold_in_kb = v, NO_LIMIT, 512);

        enforceDefault(v -> in_select_cartesian_product_failure_threshold = v, 25, 25);
        enforceDefault(v -> partition_keys_in_select_failure_threshold = v, 20, 20);

        enforceDefault(v -> tombstone_warn_threshold = v, 1000, 1000);
        enforceDefault(v -> tombstone_failure_threshold = v, 100000, 100000);

        // Default to no warning and failure at 4 times the maxTopK value
        int maxTopK = CassandraRelevantProperties.SAI_VECTOR_SEARCH_MAX_TOP_K.getInt();
        enforceDefault(v -> sai_ann_rerank_k_warn_threshold = v, -1, -1);
        enforceDefault(v -> sai_ann_rerank_k_fail_threshold = v, 4 * maxTopK, 4 * maxTopK);

        // for write requests
        enforceDefault(v -> logged_batch_enabled = v, true, true);
        enforceDefault(v -> batch_size_warn_threshold_in_kb = v, 64, 64);
        enforceDefault(v -> batch_size_fail_threshold_in_kb = v, 640, 640);
        enforceDefault(v -> unlogged_batch_across_partitions_warn_threshold = v, 10, 10);

        enforceDefault(v -> truncate_table_enabled = v, true, true);

        enforceDefault(v -> user_timestamps_enabled = v, true, true);

        enforceDefault(v -> column_value_size_failure_threshold_in_kb = v, -1L, 5 * 1024L);

        enforceDefault(v -> read_before_write_list_operations_enabled = v, true, false);

        // We use a LinkedHashSet just for the sake of preserving the ordering in error messages
        enforceDefault(
                v -> write_consistency_levels_disallowed = ImmutableSet.copyOf(v),
                       Collections.<String>singleton("ANY"),
                       new LinkedHashSet<>(Arrays.asList("ANY", "ONE", "LOCAL_ONE")));

        // for schema
        enforceDefault(v -> counter_enabled = v, true, true);

        enforceDefault(v -> fields_per_udt_failure_threshold = v, 100L, 10L);
        enforceDefault(v -> collection_size_warn_threshold_in_kb = v, 10240L, 5 * 1024L);
        enforceDefault(v -> items_per_collection_warn_threshold = v, 200L, 20L);

        enforceDefault(v -> vector_dimensions_warn_threshold = v, -1, -1);
        enforceDefault(v -> vector_dimensions_failure_threshold = v, 8192, 8192);

        enforceDefault(v -> columns_per_table_failure_threshold = v, 200L, 50L);
        enforceDefault(v -> secondary_index_per_table_failure_threshold = v, 1, 1);
        enforceDefault(v -> sasi_indexes_per_table_failure_threshold = v, 0, 0);
        enforceDefault(v -> materialized_view_per_table_failure_threshold = v, 0, 2);
        enforceDefault(v -> tables_warn_threshold = v, 100L, 100L);
        enforceDefault(v -> tables_failure_threshold = v, 200L, 200L);

        enforceDefault(
                v -> table_properties_disallowed = ImmutableSet.copyOf(v),
                       Collections.<String>emptySet(),
                       Collections.<String>emptySet());

        enforceDefault(
                v -> table_properties_ignored = ImmutableSet.copyOf(v),
                       Collections.<String>emptySet(),
                       new LinkedHashSet<>(TableAttributes.allKeywords().stream()
                                                          .sorted()
                                                          .filter(p -> !p.equals(TableParams.Option.DEFAULT_TIME_TO_LIVE.toString()) &&
                                                                       !p.equals(TableParams.Option.COMMENT.toString()) &&
                                                                       !p.equals(TableParams.Option.EXTENSIONS.toString()))
                                                          .collect(Collectors.toList())));

        // for node status
        enforceDefault(v -> disk_usage_percentage_warn_threshold = v, 70, 70);
        enforceDefault(v -> disk_usage_percentage_failure_threshold = v, NO_LIMIT, 80);
        enforceDefault(v -> disk_usage_max_disk_size_in_gb = v, (long) NO_LIMIT, (long) NO_LIMIT);

        enforceDefault(v -> partition_size_warn_threshold_in_mb = v, 100, 100);

        // SAI Table Failure threshold (maye be overridden via system property)
        Integer overrideTableFailureThreshold = Integer.getInteger(INDEX_GUARDRAILS_TABLE_FAILURE_THRESHOLD, UNSET);
        if (overrideTableFailureThreshold != UNSET)
            sai_indexes_per_table_failure_threshold = overrideTableFailureThreshold;
        enforceDefault(v -> sai_indexes_per_table_failure_threshold = v, DEFAULT_INDEXES_PER_TABLE_THRESHOLD, DEFAULT_INDEXES_PER_TABLE_THRESHOLD);

        // SAI Table Failure threshold (maye be overridden via system property)
        Integer overrideTotalFailureThreshold = Integer.getInteger(INDEX_GUARDRAILS_TOTAL_FAILURE_THRESHOLD, UNSET);
        if (overrideTotalFailureThreshold != UNSET)
            sai_indexes_total_failure_threshold = overrideTotalFailureThreshold;
        enforceDefault(v -> sai_indexes_total_failure_threshold = v, DEFAULT_INDEXES_TOTAL_THRESHOLD, DEFAULT_INDEXES_TOTAL_THRESHOLD);

        enforceDefault(v -> offset_rows_warn_threshold = v, 10000, 10000);
        enforceDefault(v -> offset_rows_failure_threshold = v, 20000, 20000);

        enforceDefault(v -> query_filters_warn_threshold = v, -1, -1);
        enforceDefault(v -> query_filters_fail_threshold = v, -1, -1);
    }

    /**
     * Validate that the value provided for each guardrail setting is valid.
     *
     * @throws ConfigurationException if any of the settings has an invalid setting.
     */
    public void validate()
    {
        validateStrictlyPositiveInteger(column_value_size_failure_threshold_in_kb,
                                        "column_value_size_failure_threshold_in_kb");

        validateStrictlyPositiveInteger(columns_per_table_failure_threshold,
                                        "columns_per_table_failure_threshold");

        validateStrictlyPositiveInteger(fields_per_udt_failure_threshold,
                                        "fields_per_udt_failure_threshold");

        validateStrictlyPositiveInteger(collection_size_warn_threshold_in_kb,
                                        "collection_size_warn_threshold_in_kb");

        validateStrictlyPositiveInteger(items_per_collection_warn_threshold,
                                        "items_per_collection_warn_threshold");

        validateStrictlyPositiveInteger(vector_dimensions_warn_threshold, "vector_dimensions_warn_threshold");
        validateStrictlyPositiveInteger(vector_dimensions_failure_threshold, "vector_dimensions_failure_threshold");
        validateWarnLowerThanFail(vector_dimensions_warn_threshold, vector_dimensions_failure_threshold, "vector_dimensions");

        validateStrictlyPositiveInteger(sai_ann_rerank_k_warn_threshold, "sai_ann_rerank_k_warn_threshold");
        validateStrictlyPositiveInteger(sai_ann_rerank_k_fail_threshold, "sai_ann_rerank_k_fail_threshold");
        validateWarnLowerThanFail(sai_ann_rerank_k_warn_threshold, sai_ann_rerank_k_fail_threshold, "sai_ann_rerank_k");

        validateStrictlyPositiveInteger(tables_warn_threshold, "tables_warn_threshold");
        validateStrictlyPositiveInteger(tables_failure_threshold, "tables_failure_threshold");
        validateWarnLowerThanFail(tables_warn_threshold, tables_failure_threshold, "tables");

        validateDisallowedTableProperties();
        validateIgnoredTableProperties();

        validateStrictlyPositiveInteger(page_size_failure_threshold_in_kb, "page_size_failure_threshold_in_kb");

        validateStrictlyPositiveInteger(partition_size_warn_threshold_in_mb, "partition_size_warn_threshold_in_mb");

        validateStrictlyPositiveInteger(partition_keys_in_select_failure_threshold, "partition_keys_in_select_failure_threshold");

        validateStrictlyPositiveInteger(in_select_cartesian_product_failure_threshold, "in_select_cartesian_product_failure_threshold");

        validateDiskUsageThreshold();

        validateTombstoneThreshold(tombstone_warn_threshold, tombstone_failure_threshold);

        validateBatchSizeThreshold(batch_size_warn_threshold_in_kb, batch_size_fail_threshold_in_kb);
        validateStrictlyPositiveInteger(unlogged_batch_across_partitions_warn_threshold, "unlogged_batch_across_partitions_warn_threshold");

        for (String rawCL : write_consistency_levels_disallowed)
        {
            try
            {
                ConsistencyLevel.fromString(rawCL);
            }
            catch (Exception e)
            {
                throw new ConfigurationException(format("Invalid value for write_consistency_levels_disallowed guardrail: "
                                                        + "'%s' does not parse as a Consistency Level", rawCL));
            }
        }

        validateStrictlyPositiveInteger(offset_rows_warn_threshold, "offset_rows_warn_threshold");
        validateStrictlyPositiveInteger(offset_rows_failure_threshold, "offset_rows_failure_threshold");
        validateWarnLowerThanFail(offset_rows_warn_threshold, offset_rows_failure_threshold, "offset_rows_threshold");

        validateStrictlyPositiveInteger(query_filters_warn_threshold, "query_filters_warn_threshold");
        validateStrictlyPositiveInteger(query_filters_fail_threshold, "query_filters_fail_threshold");
        validateWarnLowerThanFail(query_filters_warn_threshold, query_filters_fail_threshold, "query_filters_threshold");
    }

    /**
     * This validation method should only be called after {@link DatabaseDescriptor#createAllDirectories()} has been called.
     */
    public void validateAfterDataDirectoriesExist()
    {
        validateDiskUsageMaxSize();
    }

    @VisibleForTesting
    public void validateDiskUsageMaxSize()
    {
        long totalDiskSizeInGb = 0L;
        for (Directories.DataDirectory directory : Directories.dataDirectories.getAllDirectories())
        {
            totalDiskSizeInGb += SizeUnit.BYTES.toGigaBytes(directory.getTotalSpace());
        }

        if (totalDiskSizeInGb == 0L)
        {
            totalDiskSizeInGb = Long.MAX_VALUE;
        }
        validatePositiveNumeric(disk_usage_max_disk_size_in_gb, totalDiskSizeInGb, false, "disk_usage_max_disk_size_in_gb");
    }

    /**
     * Enforce default value based on {@link DatabaseDescriptor#isEmulateDbaasDefaults()} if
     * it's not specified in yaml
     *
     * @param <T>
     * @param optionSetter  setter to updated given config
     * @param onPremDefault default value for on-prem
     * @param dbaasDefault  default value for constellation DB-as-a-service
     */
    private static <T> void enforceDefault(Consumer<T> optionSetter, T onPremDefault, T dbaasDefault)
    {
        assert !DatabaseDescriptor.isDaemonInitialized();
        if (!DatabaseDescriptor.isEmulateDbaasDefaults() && !DatabaseDescriptor.isHcdGuardrailsDefaults())
            return;

        optionSetter.accept(DatabaseDescriptor.isEmulateDbaasDefaults() ? dbaasDefault : onPremDefault);
    }

    /**
     * @return true if given disk usage threshold disables disk usage guardrail
     */
    public static boolean diskUsageGuardrailDisabled(double value)
    {
        return value < 0;
    }

    /**
     * Validate that the values provided for disk usage are valid.
     *
     * @throws ConfigurationException if any of the settings has an invalid setting.
     */
    @VisibleForTesting
    public void validateDiskUsageThreshold()
    {
        validatePositiveNumeric(disk_usage_percentage_warn_threshold, 100, false, "disk_usage_percentage_warn_threshold");
        validatePositiveNumeric(disk_usage_percentage_failure_threshold, 100, false, "disk_usage_percentage_failure_threshold");
        validateWarnLowerThanFail(disk_usage_percentage_warn_threshold, disk_usage_percentage_failure_threshold, "disk_usage_percentage");
    }

    public void validateTombstoneThreshold(long warnThreshold, long failureThreshold)
    {
        validateStrictlyPositiveInteger(warnThreshold, "tombstone_warn_threshold");
        validateStrictlyPositiveInteger(failureThreshold, "tombstone_failure_threshold");
        validateWarnLowerThanFail(warnThreshold, failureThreshold, "tombstone_threshold");
    }

    private void validateDisallowedTableProperties()
    {
        Set<String> diff = Sets.difference(table_properties_disallowed.stream().map(String::toLowerCase).collect(Collectors.toSet()),
                                           TableAttributes.allKeywords());

        if (!diff.isEmpty())
            throw new ConfigurationException(format("Invalid value for table_properties_disallowed guardrail: "
                                                    + "'%s' do not parse as valid table properties", diff.toString()));
    }

    private void validateIgnoredTableProperties()
    {
        Set<String> diff = Sets.difference(table_properties_ignored.stream().map(String::toLowerCase).collect(Collectors.toSet()),
                                           TableAttributes.allKeywords());

        if (!diff.isEmpty())
            throw new ConfigurationException(format("Invalid value for table_properties_ignored guardrail: "
                                                    + "'%s' do not parse as valid table properties", diff.toString()));
    }

    private void validateStrictlyPositiveInteger(long value, String name)
    {
        // We use 'long' for generality, but most numeric guardrail cannot effectively be more than a 'int' for various
        // internal reasons. Not that any should ever come close in practice ...
        // Also, in most cases, zero does not make sense (allowing 0 tables or columns is not exactly useful).
        validatePositiveNumeric(value, Integer.MAX_VALUE, false, name);
    }

    private void validatePositiveNumeric(long value, long maxValue, boolean allowZero, String name)
    {
        if (value > maxValue)
            throw new ConfigurationException(format("Invalid value %d for guardrail %s: maximum allowed value is %d",
                                                    value, name, maxValue));

        if (value == 0 && !allowZero)
            throw new ConfigurationException(format("Invalid value for guardrail %s: 0 is not allowed", name));

        // We allow -1 as a general "disabling" flag. But reject anything lower to avoid mistakes.
        if (value < -1L)
            throw new ConfigurationException(format("Invalid value %d for guardrail %s: negative values are not "
                                                    + "allowed, outside of -1 which disables the guardrail",
                                                    value, name));
    }

    private void validateWarnLowerThanFail(long warnValue, long failValue, String guardName)
    {
        if (warnValue == -1 || failValue == -1)
            return;

        if (failValue < warnValue)
            throw new ConfigurationException(format("The warn threshold %d for the %s guardrail should be lower "
                                                    + "than the failure threshold %d",
                                                    warnValue, guardName, failValue));
    }

    public void setTombstoneFailureThreshold(int threshold)
    {
        validateTombstoneThreshold(tombstone_warn_threshold, threshold);
        tombstone_failure_threshold = threshold;
    }

    public void setTombstoneWarnThreshold(int threshold)
    {
        validateTombstoneThreshold(threshold, tombstone_failure_threshold);
        tombstone_warn_threshold = threshold;
    }


    public void validateBatchSizeThreshold(long warnThreshold, long failureThreshold)
    {
        validateStrictlyPositiveInteger(warnThreshold, "batch_size_warn_threshold_in_kb");
        validateStrictlyPositiveInteger(failureThreshold, "batch_size_fail_threshold_in_kb");
        validateWarnLowerThanFail(warnThreshold, failureThreshold, "batch_size_threshold");
    }

    public int getBatchSizeWarnThreshold()
    {
        return batch_size_warn_threshold_in_kb * 1024;
    }

    public int getBatchSizeFailThreshold()
    {
        return batch_size_fail_threshold_in_kb * 1024;
    }

    public void setBatchSizeWarnThresholdInKB(int threshold)
    {
        validateBatchSizeThreshold(threshold, batch_size_fail_threshold_in_kb);
        batch_size_warn_threshold_in_kb = threshold;
    }

    public void setBatchSizeFailThresholdInKB(int threshold)
    {
        validateBatchSizeThreshold(batch_size_warn_threshold_in_kb, threshold);
        batch_size_fail_threshold_in_kb = threshold;
    }
}
