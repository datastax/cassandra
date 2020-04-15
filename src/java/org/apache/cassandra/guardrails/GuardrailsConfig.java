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
import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.ConfigurationException;

import static java.lang.String.format;

/**
 * Configuration settings for guardrails (populated from the Yaml file).
 *
 * <p>Note that the settings here must only be used by the {@link Guardrails} class and not directly by the code
 * checking each guarded constraint (which, again, should use the higher level abstractions defined in
 * {@link Guardrails}).
 *
 * <p>This contains a main setting, {@code enabled}, controlling if guardrails are globally active or not, and
 * individual setting to control each guardrail. We have 2 variants of guardrails, soft (warn) and hard (fail) limits,
 * each guardrail having either one of the variant or both (note in particular that hard limits only make sense for
 * guardrails triggering during query execution. For other guardrails, say one triggering during compaction, failing
 * does not make sense).
 *
 * <p>If {@code enabled == false}, no limits should be enforced, be it soft or hard. Additionally, each individual
 * setting should have a specific value (typically -1 for numeric settings), that allows to disable the corresponding
 * guardrail.
 *
 * <p>The default values for each guardrail settings should reflect what is mandated for C* aaS environment.
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
    public boolean enabled = false;

    public long column_value_size_failure_threshold_in_kb = 5 * 1024L; // 5MB
    public long columns_per_table_failure_threshold = 20;

    public long tables_warn_threshold = 100;
    public long tables_failure_threshold = 200;

    public boolean user_timestamps_enabled = false;

    // TODO: only allowing LOCAL_QUORUM as of current spec, but this can't be right ...
    // We use a LinkedHashSet just for the sake of preserving the ordering in error messages
    public Set<String> consistency_level_disallowed = new LinkedHashSet<>(Arrays.asList(
    "ANY", "ONE", "TWO", "THREE", "QUORUM", "ALL", "EACH_QUORUM", "SERIAL", "LOCAL_SERIAL", "LOCAL_ONE"));

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

        validateStrictlyPositiveInteger(tables_warn_threshold, "tables_warn_threshold");
        validateStrictlyPositiveInteger(tables_failure_threshold, "tables_failure_threshold");
        validateWarnLowerThanFail(tables_warn_threshold, tables_failure_threshold, "tables");

        for (String rawCL : consistency_level_disallowed)
        {
            try
            {
                ConsistencyLevel.fromString(rawCL);
            }
            catch (Exception e)
            {
                throw new ConfigurationException(format("Invalid value for consistency_level_disallowed guardrail: "
                                                        + "'%s' does not parse as a Consistency Level", rawCL));
            }
        }
    }

    private void validateStrictlyPositiveInteger(long value, String name)
    {
        // We use 'long' for generality, but most numeric guardrails cannot effectively be more than an 'int' for various
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
}
