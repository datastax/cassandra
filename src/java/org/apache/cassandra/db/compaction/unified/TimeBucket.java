/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.db.compaction.unified;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.exceptions.ConfigurationException;

/**
 * Represents a time-driven level definition within the Unified Compaction Strategy (UCS) configuration.
 *
 * <p>Time-driven levels allow the {@link org.apache.cassandra.db.compaction.UnifiedCompactionStrategy} to
 * partition SSTables into separate compaction arenas based on the age of the data (determined by the SSTable's
 * minimum timestamp). Each {@code TimeBucket} carries its own scaling parameters, which override the global
 * parameters for SSTables that fall within the corresponding time window.
 *
 * <p>Two modes of time bucketing are supported, and they are specified as suffix clauses appended to a
 * scaling-parameter segment (separated by {@code ;}) in the {@code scaling_parameters} option:
 *
 * <ul>
 *   <li><b>{@code until <duration>} — Transient young period (age-threshold mode).</b>
 *       SSTables whose age (current time minus the SSTable's minimum timestamp) is less than the duration are
 *       placed in a separate compaction arena with the scaling parameters that precede the {@code until} clause.
 *       This is useful for applying different compaction behaviors (such as tiered compaction) to young, hot data.
 *       <p>Example: {@code T4 until 1d; L4} — use {@code T4} (tiered compaction) for data younger than 1 day,
 *       and {@code L4} (leveled compaction) for older data.
 *   <li><b>{@code every <duration>} — Repeating time-window mode (TWCS-like).</b>
 *       Partitions older SSTables into fixed-size time windows of the specified duration. For each window, a
 *       separate compaction arena is created. The window index is determined by {@code floor(minTimestamp / durationUs)},
 *       so window boundaries align to multiples of the duration since the Unix epoch.
 *       <p>Example: {@code T4 every 1d} — partition all data into 1-day windows, and use tiered compaction
 *       with fanout 4 inside each window.
 * </ul>
 *
 * <p>Cassandra timestamps are stored in microseconds since the Unix epoch. Durations are expressed in human-readable
 * form using CQL duration format, such as {@code 1d}, {@code 7d}, {@code 1h}, {@code 30m}, {@code 2w} (weeks).
 *
 * <p>Multiple time buckets can be chained together. When parsing, the segments are checked in order:
 * <ol>
 *   <li>Zero or more {@code until <duration>} segments, which must be specified in strictly increasing duration order.</li>
 *   <li>At most one {@code every <duration>} segment at the end, or a plain unqualified
 *       scaling parameters segment at the end.</li>
 * </ol>
 *
 * <p>Concrete examples for {@code scaling_parameters} configuration:
 * <ul>
 *   <li>{@code "T4 every 1d"} — TWCS-like: separate arena per day, each using T4 tiered compaction.</li>
 *   <li>{@code "T4 until 2w; L1000000"} — T4 tiered compaction for fresh data (younger than 2 weeks), aggressive
 *       leveling for older data to trigger major compaction and clear tombstones.</li>
 *   <li>{@code "T4 until 1d; L1000000 every 1d"} — legacy TWCS-like behavior: uses tiered compaction (STCS) for the
 *       active window (younger than 1 day), and once the window is closed, compacts it once into a single SSTable
 *       using aggressive leveling (L1000000) inside every 1-day window.</li>
 * </ul>
 *
 * <p><b>Limitations:</b>
 * <ul>
 *   <li>Months and years are not supported as duration units since they are not of fixed length.</li>
 *   <li>Only the last segment can define the repeating {@code every} window or be unqualified.</li>
 * </ul>
 *
 * @see Controller#parseScalingParameterGroups(String)
 * @see org.apache.cassandra.db.compaction.ArenaSelector
 */
public final class TimeBucket
{
    /**
     * Pattern that matches human-readable duration tokens.
     */
    private static final Pattern DURATION_TOKEN_PATTERN =
        Pattern.compile("(\\d+)\\s*(weeks?|days?|hours?|minutes?|seconds?|w|d|h|m|s)", Pattern.CASE_INSENSITIVE);

    /**
     * Pattern for the {@code until <duration>} suffix.
     * {@code until} introduces a transient time span before the duration (younger data).
     */
    static final Pattern UNTIL_PATTERN =
        Pattern.compile("^\\s*(.+?)\\s+until\\s+(.+)$", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

    /**
     * Pattern for the {@code every <duration>} suffix.
     * {@code every} introduces a repeating time-window (TWCS-like) mode.
     */
    static final Pattern EVERY_PATTERN =
        Pattern.compile("^\\s*(.+?)\\s+every\\s+(.+)$", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

    /**
     * Duration of the time bucket in microseconds.
     */
    public final long durationUs;

    /**
     * Scaling parameters that apply to SSTables placed in this time bucket's arena.
     */
    public final int[] scalingParameters;

    /** Determines the semantics of the {@link #durationUs} field. */
    public final Mode mode;

    /** Precomputed formatted duration string to optimize arena naming on hot paths. */
    public final String formattedDuration;

    /**
     * The mode of a {@link TimeBucket}: transient young periods ({@link #UNTIL}) or repeating windows
     * ({@link #EVERY}).
     */
    public enum Mode
    {
        /**
         * Transient young period. SSTables whose age is less than {@code durationUs} relative to
         * the current wall-clock time are placed into this arena.
         */
        UNTIL,

        /**
         * Repeating fixed-size time windows aligned to multiples of the window duration since the epoch.
         * SSTables whose age is greater than all {@link #UNTIL} thresholds are grouped into repeating windows.
         */
        EVERY
    }

    /**
     * Creates a new {@code TimeBucket}.
     *
     * @param durationUs       the window length or transient age limit in microseconds
     * @param scalingParameters the scaling parameters (W values) for this bucket
     * @param mode             {@link Mode#UNTIL} for transient periods, {@link Mode#EVERY} for repeating windows
     */
    public TimeBucket(long durationUs, int[] scalingParameters, Mode mode)
    {
        if (durationUs <= 0)
            throw new ConfigurationException("Time bucket duration must be positive, got: " + durationUs + " us");
        this.durationUs = durationUs;
        this.scalingParameters = scalingParameters.clone();
        this.mode = mode;
        this.formattedDuration = formatDurationUs(durationUs);
    }

    /**
     * Returns a human-readable name for this time bucket that can be used as part of an arena name.
     *
     * @param windowIndex for {@link Mode#EVERY} buckets, the time window index; ignored for {@link Mode#UNTIL}
     * @return a short descriptive string
     */
    public String arenaName(long windowIndex)
    {
        if (mode == Mode.EVERY)
        {
            long epochMillis = Math.multiplyExact(windowIndex, durationUs) / 1000L;
            Instant instant = Instant.ofEpochMilli(epochMillis);
            DateTimeFormatter formatter;
            if (durationUs >= TimeUnit.DAYS.toMicros(1))
                formatter = DateTimeFormatter.ISO_LOCAL_DATE; // yyyy-MM-dd
            else
                formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm");
            
            return formatter.format(instant.atZone(ZoneOffset.UTC));
        }
        else
        {
            return "until_" + formattedDuration;
        }
    }

    /**
     * Given an SSTable's minimum timestamp (in Cassandra microseconds), computes the window index for
     * a {@link Mode#EVERY} bucket.
     *
     * @param minTimestampUs the SSTable's minimum timestamp in microseconds since the Unix epoch
     * @return the window index: {@code floor(minTimestampUs / durationUs)}
     * @throws UnsupportedOperationException if this bucket is not in {@link Mode#EVERY} mode
     */
    public long windowIndex(long minTimestampUs)
    {
        if (mode != Mode.EVERY)
            throw new UnsupportedOperationException("windowIndex() is only valid for EVERY-mode TimeBuckets");
        // Floor division — works correctly for both positive and negative timestamps.
        return Math.floorDiv(minTimestampUs, durationUs);
    }

    /**
     * Parses a human-readable duration string into microseconds.
     * Defer to {@link org.apache.cassandra.cql3.Duration} parser to match CQL formatting.
     *
     * @param durationStr the duration string to parse
     * @return the duration in microseconds
     * @throws ConfigurationException if the string does not match any recognised duration pattern
     */
    @VisibleForTesting
    public static long parseDurationUs(String durationStr)
    {
        try
        {
            org.apache.cassandra.cql3.Duration duration = org.apache.cassandra.cql3.Duration.from(durationStr.trim());
            if (duration.getMonths() != 0)
                throw new ConfigurationException("Months/years are not supported for time-driven levels: " + durationStr);
            long daysUs = duration.getDays() * TimeUnit.DAYS.toMicros(1);
            long nanosUs = duration.getNanoseconds() / 1000L;
            long total = daysUs + nanosUs;
            if (total <= 0)
                throw new ConfigurationException("Time bucket duration must be positive, got: " + durationStr);
            return total;
        }
        catch (ConfigurationException e)
        {
            throw e;
        }
        catch (Exception e)
        {
            throw new ConfigurationException("Invalid duration '" + durationStr + "': " + e.getMessage(), e);
        }
    }

    /**
     * Parses the full {@code scaling_parameters} value into a list of {@link TimeBucket} instances and the
     * "base" (most-recent) scaling parameters. The structure of the input is:
     *
     * <pre>
     * segment [; segment]*
     * </pre>
     *
     * where each segment is either:
     * <ul>
     *   <li>{@code <scalingParams> until <duration>} — transient young period</li>
     *   <li>{@code <scalingParams> every <duration>} — repeating time-window definition (last segment only)</li>
     *   <li>{@code <scalingParams>} — base parameters (unqualified, last segment only, applies to oldest data)</li>
     * </ul>
     *
     * @param str the raw value of the {@code scaling_parameters} option
     * @return a {@link ParseResult} containing the base parameters and any time bucket definitions
     * @throws ConfigurationException on any parse or semantic error
     */
    public static ParseResult parseScalingParameterGroups(String str)
    {
        String[] segments = str.split(";");
        int[] baseParams = null;
        List<TimeBucket> timeBuckets = new ArrayList<>();
        TimeBucket everyBucket = null;

        for (int i = 0; i < segments.length; i++)
        {
            String trimmedSegment = segments[i].trim();
            if (trimmedSegment.isEmpty())
                throw new ConfigurationException("Empty segment in scaling_parameters: '" + str + "'");

            Matcher untilMatcher = UNTIL_PATTERN.matcher(trimmedSegment);
            Matcher everyMatcher = EVERY_PATTERN.matcher(trimmedSegment);

            boolean isLast = (i == segments.length - 1);

            if (untilMatcher.matches())
            {
                if (isLast)
                    throw new ConfigurationException("The last segment cannot be an 'until' segment: '" + str + "'");
                
                int[] params = parseScalingParamsFragment(untilMatcher.group(1), str);
                long durationUs = parseDurationUs(untilMatcher.group(2));
                timeBuckets.add(new TimeBucket(durationUs, params, Mode.UNTIL));
            }
            else if (everyMatcher.matches())
            {
                if (!isLast)
                    throw new ConfigurationException("An 'every' segment must be the last segment: '" + str + "'");
                
                String paramsStr = everyMatcher.group(1);
                String durationStr = everyMatcher.group(2);
                int[] params = parseScalingParamsFragment(paramsStr, str);
                long durationUs = parseDurationUs(durationStr);
                
                everyBucket = new TimeBucket(durationUs, params, Mode.EVERY);
                baseParams = params;
            }
            else
            {
                if (!isLast)
                    throw new ConfigurationException("Only the last segment can be plain (unqualified) scaling parameters: '" + str + "'");
                
                baseParams = parseScalingParamsFragment(trimmedSegment, str);
            }
        }

        // Validate until durations are increasing
        long lastDurationUs = 0;
        for (TimeBucket tb : timeBuckets)
        {
            if (tb.durationUs <= lastDurationUs)
                throw new ConfigurationException("until durations must be strictly increasing in scaling_parameters: '" + str + "'");
            lastDurationUs = tb.durationUs;
        }

        if (everyBucket != null)
        {
            timeBuckets.add(everyBucket);
        }

        if (baseParams == null)
            throw new ConfigurationException("No base scaling parameters found in: '" + str + "'");

        return new ParseResult(baseParams, Collections.unmodifiableList(timeBuckets));
    }

    /**
     * Parses only the scaling-parameter portion of a segment.
     */
    private static int[] parseScalingParamsFragment(String fragment, String fullStr)
    {
        try
        {
            return Controller.parseScalingParameters(fragment.trim());
        }
        catch (ConfigurationException e)
        {
            throw new ConfigurationException("Invalid scaling parameters in segment of '" + fullStr + "': " + e.getMessage(), e);
        }
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(Controller.printScalingParameters(scalingParameters));
        sb.append(mode == Mode.UNTIL ? " until " : " every ");
        // Convert back to a human-readable duration.
        sb.append(formatDurationUs(durationUs));
        return sb.toString();
    }

    /**
     * Formats a duration in microseconds as a human-readable string using the largest applicable unit.
     *
     * @param durationUs duration in microseconds
     * @return formatted string, e.g. {@code "1d"}, {@code "2w"}, {@code "6h"}
     */
    @VisibleForTesting
    public static String formatDurationUs(long durationUs)
    {
        // Try weeks
        if (durationUs % TimeUnit.DAYS.toMicros(7) == 0)
            return (durationUs / TimeUnit.DAYS.toMicros(7)) + "w";
        if (durationUs % TimeUnit.DAYS.toMicros(1) == 0)
            return (durationUs / TimeUnit.DAYS.toMicros(1)) + "d";
        if (durationUs % TimeUnit.HOURS.toMicros(1) == 0)
            return (durationUs / TimeUnit.HOURS.toMicros(1)) + "h";
        if (durationUs % TimeUnit.MINUTES.toMicros(1) == 0)
            return (durationUs / TimeUnit.MINUTES.toMicros(1)) + "m";
        return (durationUs / TimeUnit.SECONDS.toMicros(1)) + "s";
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TimeBucket that = (TimeBucket) o;
        return durationUs == that.durationUs &&
               mode == that.mode &&
               Arrays.equals(scalingParameters, that.scalingParameters);
    }

    @Override
    public int hashCode()
    {
        int result = Objects.hash(durationUs, mode);
        result = 31 * result + Arrays.hashCode(scalingParameters);
        return result;
    }

    /**
     * The structured result of {@link #parseScalingParameterGroups(String)}.
     *
     * <ul>
     *   <li>{@link #baseScalingParameters} — the W values for the oldest data (or the {@code every}
     *       window data when all data is time-bucketed).</li>
     *   <li>{@link #timeBuckets} — ordered list of {@link TimeBucket} instances; may be empty if no
     *       time-based bucketing was requested.</li>
     * </ul>
     */
    public static final class ParseResult
    {
        /** Scaling parameters for the oldest or base data arena. */
        public final int[] baseScalingParameters;

        /**
         * Time bucket definitions in evaluation order (transient {@link Mode#UNTIL} buckets in ascending
         * duration order, followed by the repeating {@link Mode#EVERY} bucket if present).
         */
        public final List<TimeBucket> timeBuckets;

        ParseResult(int[] baseScalingParameters, List<TimeBucket> timeBuckets)
        {
            this.baseScalingParameters = baseScalingParameters;
            this.timeBuckets = timeBuckets;
        }

        /**
         * Returns {@code true} when no time-based bucketing has been requested.
         * In this case {@link #timeBuckets} is empty and behaviour is identical to the pre-existing
         * UCS without time-driven levels.
         */
        public boolean hasNoTimeBuckets()
        {
            return timeBuckets.isEmpty();
        }

        @Override
        public String toString()
        {
            return String.format("ParseResult{baseScalingParameters=%s, timeBuckets=%s}",
                                 Arrays.toString(baseScalingParameters),
                                 timeBuckets);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ParseResult that = (ParseResult) o;
            return Arrays.equals(baseScalingParameters, that.baseScalingParameters) &&
                   Objects.equals(timeBuckets, that.timeBuckets);
        }

        @Override
        public int hashCode()
        {
            int result = Objects.hashCode(timeBuckets);
            result = 31 * result + Arrays.hashCode(baseScalingParameters);
            return result;
        }
    }
}
