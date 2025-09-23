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
    public Counter validationWithoutMissingKeys;
    public Counter missingKeys;
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
        this.validationWithoutMissingKeys = registryWithTags().left.counter("compaction_validation_without_missing_key_total", registryWithTags().right);
        this.missingKeys = registryWithTags().left.counter("compaction_validation_missing_keys_total", registryWithTags().right);
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

    public void incrementValidationWithoutMissingKeys()
    {
        validationWithoutMissingKeys.increment();
    }

    public void incrementMissingKeys(int keys)
    {
        missingKeys.increment(keys);
    }
}
