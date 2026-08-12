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

package org.apache.cassandra.sensors;

/**
 * Discrete tier representation of ReadStage wall-clock execution time.
 * <p>
 * Tiers are used as a multiplier signal for rate limiters: a higher tier
 * indicates a more expensive query relative to bytes read.
 * Tier boundaries are expressed in nanoseconds and should be calibrated
 * against real workload latency data.
 *
 * @see Type#READ_LATENCY_TIER
 */
public enum ReadLatencyTier
{
    TIER_1(1, 0L, Bounds.MILLIS_1),
    TIER_2(2, Bounds.MILLIS_1, Bounds.MILLIS_10),
    TIER_3(3, Bounds.MILLIS_10, Bounds.MILLIS_50),
    TIER_4(4, Bounds.MILLIS_50, Bounds.MILLIS_200),
    TIER_5(5, Bounds.MILLIS_200, Long.MAX_VALUE);

    /**
     * Tier transition point constants. Names reflect the human-readable millisecond value of each boundary;
     * the stored values are in <strong>nanoseconds</strong> as required by {@link ReadLatencyTier#fromNanos(long)}.
     * <p>
     * Declared in a nested class so they are initialised before the enum constants that reference them
     * (Java enum fields cannot be referenced in their own constructor arguments).
     */
    public static final class Bounds
    {
        /**
         * 1 ms expressed in nanoseconds.
         */
        public static final long MILLIS_1 = 1_000_000L;
        /**
         * 10 ms expressed in nanoseconds.
         */
        public static final long MILLIS_10 = 10_000_000L;
        /**
         * 50 ms expressed in nanoseconds.
         */
        public static final long MILLIS_50 = 50_000_000L;
        /**
         * 200 ms expressed in nanoseconds.
         */
        public static final long MILLIS_200 = 200_000_000L;

        private Bounds()
        {
        }
    }

    private final int value;
    private final long lowerBoundNanos;   // inclusive
    private final long upperBoundNanos;   // exclusive

    ReadLatencyTier(int value, long lowerBoundNanos, long upperBoundNanos)
    {
        this.value = value;
        this.lowerBoundNanos = lowerBoundNanos;
        this.upperBoundNanos = upperBoundNanos;
    }

    /**
     * Returns the numeric tier value (1–5) suitable for use as a Sensor value.
     */
    public double value()
    {
        return value;
    }

    /**
     * Maps an elapsed execution time in nanoseconds to the corresponding tier.
     *
     * @param elapsedNanos wall-clock execution time in nanoseconds; must be >= 0
     * @return the matching {@link ReadLatencyTier}
     */
    public static ReadLatencyTier fromNanos(long elapsedNanos)
    {
        for (ReadLatencyTier tier : values())
        {
            if (elapsedNanos >= tier.lowerBoundNanos && elapsedNanos < tier.upperBoundNanos)
                return tier;
        }
        return TIER_5;  // unreachable given TIER_5 upper bound is Long.MAX_VALUE
    }
}
