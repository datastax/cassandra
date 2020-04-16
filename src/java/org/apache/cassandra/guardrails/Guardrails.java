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

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.guardrails.Guardrail.DisableFlag;
import org.apache.cassandra.guardrails.Guardrail.DisallowedValues;
import org.apache.cassandra.guardrails.Guardrail.Threshold;

import static java.lang.String.format;

/**
 * Entry point for Guardrails, storing the defined guardrails and provided a few global methods over them.
 */
public abstract class Guardrails
{
    private static final GuardrailsConfig config = DatabaseDescriptor.getGuardrailsConfig();

    // TODO: The guardrails whose name follows are provided for example sake, but are not
    //  yet implemented. Please remove the names from the list implemented and remove this message once empty:
    //  - columnValueSize
    //  - columnsPerTable
    //  - userTimestampsEnabled
    //  - disallowedConsistencies

    public static final Threshold columnValueSize = new Guardrail.SizeThreshold("column_value_size",
                                                                                () -> -1L, // not needed so far
                                                                                () -> config.column_value_size_failure_threshold_in_kb * 1024L,
                                                                                (x, what, v, t) -> format("Value of %s of size %s is greater than the maximum allowed (%s)",
                                                                                                          what, v, t));

    public static final Threshold columnsPerTable = new Threshold("columns_per_table",
                                                                  () -> -1L, // not needed so far
                                                                  () -> config.columns_per_table_failure_threshold,
                                                                  (x, what, v, t) -> format("Tables cannot have more than %s columns, but %s provided for table %s",
                                                                                            t, v, what));

    public static final DisableFlag userTimestampsEnabled = new DisableFlag("user_provided_timestamps",
                                                                            () -> !config.user_timestamps_enabled,
                                                                            "User provided timestamps (USING TIMESTAMP)");

    public static final DisallowedValues<ConsistencyLevel> disallowedConsistencies = new DisallowedValues<>("disallowed_write_consistency_levels",
                                                                                                            () -> config.consistency_level_disallowed,
                                                                                                            ConsistencyLevel::fromString,
                                                                                                            "Consistency Level");

    public static final Threshold secondaryIndexesPerTable = new Threshold("secondary_indexes_per_table",
                                                                           () -> -1,
                                                                           () -> config.secondary_index_per_table_failure_threshold,
                                                                           (x, what, v, t) -> format("Tables cannot have more than %s secondary indexes, failed to create secondary index %s",
                                                                                                     t, what));

    public static final Threshold materializedViewsPerTable = new Threshold("materialized_views_per_table",
                                                                            () -> -1,
                                                                            () -> config.materialized_view_per_table_failure_threshold,
                                                                            (x, what, v, t) -> format("Tables cannot have more than %s materialized views, failed to create materialized view %s",
                                                                                                      t, what));

    public static final Threshold tablesLimit = new Threshold("number_of_tables",
                                                              () -> config.tables_warn_threshold,
                                                              () -> config.tables_failure_threshold,
                                                              (isWarning, what, v, t) -> isWarning
                                                                                         ? format("Creating table %s, current number of tables %s exceeds warning threshold of %s.",
                                                                                                  what, v, t)
                                                                                         : format("Cannot have more than %s tables, failed to create table %s",
                                                                                                  t, what));

    public static final DisallowedValues<String> disallowedTableProperties = new DisallowedValues<>("disallowed_table_properties",
                                                                                                    () -> config.table_properties_disallowed,
                                                                                                    String::toLowerCase,
                                                                                                    "Table Properties");

    static final List<Listener> listeners = new CopyOnWriteArrayList<>();

    private Guardrails()
    {
    }

    /**
     * Whether guardrails are enabled globally or not.
     *
     * @return {@code true} if guardrails are enabled (applies based on their individual setting), {@code false}
     * otherwise (in which case no guardrail will trigger).
     */
    public static boolean enabled()
    {
        return config.enabled;
    }

    /**
     * Register a {@link Listener}.
     *
     * <p>Note that listeners are called in the order they are registered, and on the thread on which the guardrail
     * is triggered.
     *
     * @param listener the listener to register. If the same listener is registered twice (or more), its method will be
     *                 called twice (or more) for every trigger.
     */
    public static void register(Listener listener)
    {
        listeners.add(listener);
    }

    /**
     * Unregister a previously registered listener.
     *
     * @param listener the listener to unregister. If it was not registered before, this is a no-op. If it was
     *                 registered more than once, only one of the instance is unregistered.
     */
    public static void unregister(Listener listener)
    {
        listeners.remove(listener);
    }

    /**
     * Interface for external listener interested in being notified when a guardrail is triggered.
     *
     * <p>Listeners should be registered through the {@link #register} method to take effect.
     *
     * <p>Note: this is primarily target at Insights, to generate events when guardrails are triggered.
     */
    public interface Listener
    {
        /**
         * Called when a guardrail triggers a warning.
         *
         * <p>This method is called on the thread on which the guardrail is triggered, which can in particular be a
         * TPC thread. Overall, if any blocking work is to be done, the method should submit it asynchronously on a
         * separate dedicated thread.
         *
         * @param guardrailName a name describing the guardrail.
         * @param message       the message corresponding to the guardrail trigger.
         */
        public void onWarningTriggered(String guardrailName, String message);

        /**
         * Called when a guardrail triggers a failure.
         *
         * <p>This method is called on the thread on which the guardrail is triggered, which can in particular be a
         * TPC thread. Overall, if any blocking work is to be done, the method should submit it asynchronously on a
         * separate dedicated thread.
         *
         * @param guardrailName a name describing the guardrail.
         * @param message       the message corresponding to the guardrail trigger.
         */
        public void onFailureTriggered(String guardrailName, String message);
    }
}