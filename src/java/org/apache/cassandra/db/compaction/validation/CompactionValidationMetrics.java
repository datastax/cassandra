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
package org.apache.cassandra.db.compaction.validation;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import org.apache.cassandra.metrics.MicrometerMetrics;

/// Metrics for tracking compaction validation operations and results.
public class CompactionValidationMetrics extends MicrometerMetrics
{
    public static final CompactionValidationMetrics INSTANCE = new CompactionValidationMetrics();

    public Counter validationCount;
    public Counter validationWithoutAbsentKeys;
    public Counter absentKeys;
    public Counter potentialDataLosses;

    public CompactionValidationMetrics()
    {
        initializeMetrics();
    }

    @Override
    public synchronized void register(MeterRegistry newRegistry, Tags newTags)
    {
        super.register(newRegistry, newTags);
        initializeMetrics();
    }

    private void initializeMetrics()
    {
        this.validationCount = registryWithTags().left.counter("compaction_validation_total", registryWithTags().right);
        this.validationWithoutAbsentKeys = registryWithTags().left.counter("compaction_validation_without_absent_keys_total", registryWithTags().right);
        this.absentKeys = registryWithTags().left.counter("compaction_validation_absent_keys_count_from_output_total", registryWithTags().right);
        this.potentialDataLosses = registryWithTags().left.counter("compaction_validation_potential_data_loss_total", registryWithTags().right);
    }

    public void incrementValidation()
    {
        validationCount.increment();
    }

    public void incrementPotentialDataLosses()
    {
        potentialDataLosses.increment();
    }

    public void incrementValidationWithoutAbsentKeys()
    {
        validationWithoutAbsentKeys.increment();
    }

    public void incrementAbsentKeys(int keys)
    {
        absentKeys.increment(keys);
    }
}
