/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package com.datastax.bdp.db.utils.leaks.detection;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;


/**
 * The parameters for tracking a resource. This class groups the following properties:
 * <p/>
 * Sampling probability: a number between 0 and 1 specifying the percentage of instances that should be tracked.
 * <p/>
 * Max stacks cache size: the size in MB of a cache that contains call stack traces.
 * <p/>
 * Number of access records: this determines the number of call stack traces that are recorded for each resource when
 * {@link LeaksTracker#record()} is called. This mechanism requires that the resource tracker is able to call this method,
 * which may not be possible for some resources such as {@link java.nio.ByteBuffer}.
 * These call stack traces are in addition to the creation call stack trace, which is always kept.
 * <p/>
 * Max stacks depth: the maximum depth to keep for a call stack trace. Normally only the bottom frames are sufficient for
 * debugging and so we can save space. Also, this makes the cache effective, increasing the depth may increase the
 * number of misses in the stacks cache, which means that a stack trace won't be available when a LEAK is logged.
 */
public class LeaksDetectionParams
{
    static final double DEFAULT_SAMPLING_PROBABILITY = Double.parseDouble(System.getProperty("dse.res_leak_sampling_prob", "0.01"));
    private static final int DEFAULT_NUM_ACCESS_RECORDS = Integer.getInteger("dse.res_leak_num_access_records", 0);
    private static final int MAX_STACK_DEPTH = Integer.getInteger("dse.res_leak_max_stack_depth", 30);
    private static final int MAX_STACK_CACHE_SIZE_MB = Integer.getInteger("dse.res_leak_max_stack_cache_size_mb", 32);

    /** non-final and public because of yaml config */
    public double sampling_probability;
    /** non-final and public because of yaml config */
    public int max_stacks_cache_size_mb;
    /** non-final and public because of yaml config */
    public int num_access_records;
    /** non-final and public because of yaml config */
    public int max_stack_depth;

    public LeaksDetectionParams()
    {
        this(DEFAULT_SAMPLING_PROBABILITY);
    }

    public LeaksDetectionParams(double sampling_probability)
    {
        this(sampling_probability, MAX_STACK_CACHE_SIZE_MB, DEFAULT_NUM_ACCESS_RECORDS, MAX_STACK_DEPTH);
    }

    public LeaksDetectionParams(double sampling_probability, int max_stacks_cache_size_mb, int num_access_records, int max_stack_depth)
    {
        Preconditions.checkArgument(sampling_probability >= 0 && sampling_probability <= 1, "Sampling probability must be between 0 and 1: {}", sampling_probability);
        Preconditions.checkArgument(max_stacks_cache_size_mb >= 4, "Max stacks cache size should be at least 4 MiB but got %s MiB", max_stacks_cache_size_mb);
        Preconditions.checkArgument(num_access_records >= 0, "Num access records should be positive or zero but got %s", num_access_records);
        Preconditions.checkArgument(max_stack_depth > 0, "Max stack depth should be positive but got %s", max_stack_depth);

        this.sampling_probability = sampling_probability;
        this.max_stacks_cache_size_mb = max_stacks_cache_size_mb;
        this.num_access_records = num_access_records;
        this.max_stack_depth = max_stack_depth;
    }

    @VisibleForTesting
    static LeaksDetectionParams create(double samplingProbability, int numAccessRecords)
    {
        return new LeaksDetectionParams(samplingProbability,
                                        MAX_STACK_CACHE_SIZE_MB,
                                        numAccessRecords,
                                        MAX_STACK_DEPTH);
    }

    LeaksDetectionParams copy()
    {
        return new LeaksDetectionParams(sampling_probability,
                                        max_stacks_cache_size_mb,
                                        num_access_records,
                                        max_stack_depth);
    }

    LeaksDetectionParams withSamplingProbability(double samplingProbability)
    {
        return new LeaksDetectionParams(samplingProbability,
                                        max_stacks_cache_size_mb,
                                        num_access_records,
                                        max_stack_depth);
    }

    LeaksDetectionParams withMaxStacksCacheSizeMb(int maxStacksCacheSizeMb)
    {
        return new LeaksDetectionParams(sampling_probability,
                                        maxStacksCacheSizeMb,
                                        num_access_records,
                                        max_stack_depth);
    }

    LeaksDetectionParams withNumAccessRecords(int numAccessRecords)
    {
        return new LeaksDetectionParams(sampling_probability,
                                        max_stacks_cache_size_mb,
                                        numAccessRecords,
                                        max_stack_depth);
    }

    LeaksDetectionParams withMaxStackDepth(int maxStackDepth)
    {
        return new LeaksDetectionParams(sampling_probability,
                                        max_stacks_cache_size_mb,
                                        num_access_records,
                                        maxStackDepth);
    }

    @Override
    public String toString()
    {
        return String.format("Sampling probability: %f, Max stacks cache size MB: %d, num. access records: %d, max stack depth: %d",
                             sampling_probability,
                             max_stacks_cache_size_mb,
                             num_access_records,
                             max_stack_depth);
    }
}
