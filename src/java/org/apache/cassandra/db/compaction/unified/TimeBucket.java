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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.exceptions.ConfigurationException;

/**
 * Represents a time-driven level definition within the UCS (Unified Compaction Strategy) configuration.
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
 *   <li><b>{@code by <duration>} — Repeating time-window mode (TWCS-like).</b>
 *       Partitions all SSTables into fixed-size time windows whose length is {@code duration}. For each window
 *       a separate arena is created, and the scaling parameters that precede the {@code by} clause apply to all
 *       windows. The window index of an SSTable is determined by {@code floor(minTimestamp / windowSizeUs)},
 *       so window boundaries are aligned to multiples of {@code duration} since the Unix epoch.
 *       <p>Example: {@code "T6, T2 by 1d"} — use tiered T6/T2 settings inside every 1-day window.
 *   <li><b>{@code each <duration>} — Age-threshold mode (periodic major compaction).</b>
 *       Defines a single threshold boundary: SSTables whose minimum timestamp is older than {@code duration}
 *       relative to now are placed in a separate "old data" arena with the scaling parameters that follow the
 *       {@code each} clause, while the remaining SSTables use the parameters that precede the threshold. This
 *       is useful for ensuring tombstone removal via periodic major compaction of old data (see
 *       <a href="https://github.com/apache/cassandra/issues/18245">CASSANDRA-18245</a>).
 *       <p>Example: {@code "T4; L1000000 each 1d"} — use T4 for data newer than 1 day, L1000000 for older.
 * </ul>
 *
 * <p>Cassandra timestamps are stored in microseconds since the Unix epoch. Durations are expressed in human-
 * readable form such as {@code "1d"}, {@code "7d"}, {@code "1h"}, {@code "30m"}, {@code "2w"} (weeks), or
 * combinations like {@code "1d12h"}.
 *
 * <p>Multiple time buckets can be chained. In such cases the {@code each} boundaries take effect in ascending
 * age order, so the first matching (youngest) boundary takes precedence.
 *
 * <p>Concrete examples for {@code scaling_parameters}:
 * <ul>
 *   <li>{@code "T6, T2 by 1 day"}  — TWCS-like: separate arena per day, each uses T6/T2 scaling</li>
 *   <li>{@code "T4; L1000000 each 2 weeks"} — T4 for fresh data, aggressive leveling for data &gt; 2 weeks</li>
 *   <li>{@code "T6, T2 by 1 day; L1000000 each 2 weeks"} — combined: TWCS inside each day window, plus a
 *       catch-all major-compaction level for data older than 2 weeks</li>
 * </ul>
 *
 * @see Controller#parseScalingParameterGroups(String)
 * @see org.apache.cassandra.db.compaction.ArenaSelector
 */
public final class TimeBucket
{
    /**
     * Pattern that matches human-readable duration tokens.
     * Supported units: {@code w}/{@code week(s)}, {@code d}/{@code day(s)}, {@code h}/{@code hour(s)},
     * {@code m}/{@code minute(s)}, {@code s}/{@code second(s)}. Tokens may be concatenated: {@code "1d12h"}.
     */
    private static final Pattern DURATION_TOKEN_PATTERN =
        Pattern.compile("(\\d+)\\s*(weeks?|days?|hours?|minutes?|seconds?|w|d|h|m|s)", Pattern.CASE_INSENSITIVE);

    /**
     * Pattern for the {@code by <duration>} suffix.
     * {@code by} introduces a repeating time-window (TWCS-like) mode.
     */
    static final Pattern BY_PATTERN =
        Pattern.compile("^\\s*(.+?)\\s+by\\s+(.+)$", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

    /**
     * Pattern for the {@code each <duration>} suffix.
     * {@code each} introduces a single age-threshold boundary.
     */
    static final Pattern EACH_PATTERN =
        Pattern.compile("^\\s*(.+?)\\s+each\\s+(.+)$", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

    /**
     * Duration of the time bucket in microseconds.
     *
     * <ul>
     *   <li>For {@link Mode#BY} buckets this is the size of each repeating window.</li>
     *   <li>For {@link Mode#EACH} buckets this is the age threshold that divides the "recent" arena from the
     *       "old data" arena.</li>
     * </ul>
     *
     * Cassandra timestamps are stored in microseconds, so this value is likewise in microseconds.
     */
    public final long durationUs;

    /**
     * Scaling parameters that apply to SSTables placed in this time bucket's arena.
     * The array semantics are identical to those in {@link StaticController}: index corresponds to level index
     * within the arena, and the last element is repeated for all higher levels.
     */
    public final int[] scalingParameters;

    /** Determines the semantics of the {@link #durationUs} field. */
    public final Mode mode;

    /** Precomputed formatted duration string to optimize arena naming on hot paths. */
    public final String formattedDuration;

    /**
     * The mode of a {@link TimeBucket}: repeating windows ({@link #BY}) or a single age threshold
     * ({@link #EACH}).
     */
    public enum Mode
    {
        /**
         * Repeating fixed-size time windows aligned to multiples of the window duration since the epoch.
         * All SSTables whose {@code minTimestamp} falls in the same window are compacted together.
         * The window index is {@code floor(minTimestamp / durationUs)}.
         */
        BY,

        /**
         * A single age threshold. SSTables older than {@code durationUs} relative to the current wall-clock
         * time are placed into a separate arena with the associated scaling parameters.
         */
        EACH
    }

    /**
     * Creates a new {@code TimeBucket}.
     *
     * @param durationUs       the window length or age threshold in microseconds
     * @param scalingParameters the scaling parameters (W values) for this bucket
     * @param mode             {@link Mode#BY} for repeating windows, {@link Mode#EACH} for an age threshold
     */
    public TimeBucket(long durationUs, int[] scalingParameters, Mode mode)
    {
        if (durationUs <= 0)
            throw new ConfigurationException("Time bucket duration must be positive, got: " + durationUs + " us");
        this.durationUs = durationUs;
        this.scalingParameters = scalingParameters;
        this.mode = mode;
        this.formattedDuration = formatDurationUs(durationUs);
    }

    /**
     * Returns a human-readable name for this time bucket that can be used as part of an arena name.
     * For {@link Mode#BY} buckets the name includes the window index; for {@link Mode#EACH} buckets it
     * identifies the bucket as "old".
     *
     * @param windowIndex for {@link Mode#BY} buckets, the time window index; ignored for {@link Mode#EACH}
     * @return a short descriptive string
     */
    public String arenaName(long windowIndex)
    {
        if (mode == Mode.BY)
            return "tw_" + windowIndex;
        else
            return "old";
    }

    /**
     * Given an SSTable's minimum timestamp (in Cassandra microseconds), computes the window index for
     * a {@link Mode#BY} bucket.
     *
     * @param minTimestampUs the SSTable's minimum timestamp in microseconds since the Unix epoch
     * @return the window index: {@code floor(minTimestampUs / durationUs)}
     * @throws UnsupportedOperationException if this bucket is not in {@link Mode#BY} mode
     */
    public long windowIndex(long minTimestampUs)
    {
        if (mode != Mode.BY)
            throw new UnsupportedOperationException("windowIndex() is only valid for BY-mode TimeBuckets");
        // Floor division — works correctly for both positive and negative timestamps.
        return Math.floorDiv(minTimestampUs, durationUs);
    }

    /**
     * Determines whether an SSTable with the given minimum timestamp falls in the "old data" arena for a
     * {@link Mode#EACH} bucket, given the current time.
     *
     * @param minTimestampUs  the SSTable's minimum timestamp in microseconds since the Unix epoch
     * @param nowUs           the current wall-clock time in microseconds since the Unix epoch
     * @return {@code true} if the SSTable is older than the threshold
     * @throws UnsupportedOperationException if this bucket is not in {@link Mode#EACH} mode
     */
    public boolean isOld(long minTimestampUs, long nowUs)
    {
        if (mode != Mode.EACH)
            throw new UnsupportedOperationException("isOld() is only valid for EACH-mode TimeBuckets");
        return (nowUs - minTimestampUs) >= durationUs;
    }

    /**
     * Parses a human-readable duration string (e.g. {@code "1d"}, {@code "7d"}, {@code "2w"}, {@code "1h30m"})
     * into microseconds.
     *
     * <p>Supported units:
     * <ul>
     *   <li>{@code w} / {@code W} — weeks</li>
     *   <li>{@code d} / {@code D} — days</li>
     *   <li>{@code h} / {@code H} — hours</li>
     *   <li>{@code m} / {@code M} — minutes</li>
     *   <li>{@code s} / {@code S} — seconds</li>
     * </ul>
     *
     * @param durationStr the duration string to parse
     * @return the duration in microseconds
     * @throws ConfigurationException if the string does not match any recognised duration pattern
     */
    @VisibleForTesting
    public static long parseDurationUs(String durationStr)
    {
        String trimmed = durationStr.trim();
        Matcher m = DURATION_TOKEN_PATTERN.matcher(trimmed);
        long totalUs = 0;
        int lastEnd = 0;
        while (m.find())
        {
            if (m.start() != lastEnd)
                throw new ConfigurationException("Invalid duration '" + durationStr
                                                 + "'. Expected format like '1d', '7d', '2w', '1h30m'.");
            long value = Long.parseLong(m.group(1));
            String unit = m.group(2).toLowerCase();
            long multiplierUs;
            switch (unit)
            {
                case "w":
                case "week":
                case "weeks":
                    multiplierUs = TimeUnit.DAYS.toMicros(7);    break;
                case "d":
                case "day":
                case "days":
                    multiplierUs = TimeUnit.DAYS.toMicros(1);    break;
                case "h":
                case "hour":
                case "hours":
                    multiplierUs = TimeUnit.HOURS.toMicros(1);   break;
                case "m":
                case "minute":
                case "minutes":
                    multiplierUs = TimeUnit.MINUTES.toMicros(1); break;
                case "s":
                case "second":
                case "seconds":
                    multiplierUs = TimeUnit.SECONDS.toMicros(1); break;
                default:
                    throw new ConfigurationException("Unknown time unit '" + unit + "' in duration: " + durationStr);
            }
            totalUs += value * multiplierUs;
            lastEnd = m.end();
        }

        // Verify that we consumed the entire input (ignoring leading/trailing whitespace).
        if (totalUs == 0 || lastEnd != trimmed.length())
            throw new ConfigurationException("Invalid duration '" + durationStr
                                             + "'. Expected format like '1d', '7d', '2w', '1h30m'.");
        return totalUs;
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
     *   <li>{@code <scalingParams>} — the base parameters (for the most-recent data)</li>
     *   <li>{@code <scalingParams> by <duration>} — a repeating time-window definition</li>
     *   <li>{@code <scalingParams> each <duration>} — an age-threshold definition</li>
     * </ul>
     *
     * <p>The first segment (before any {@code ;}) always represents the base parameters or the parameters
     * associated with a {@code by} window. If it contains {@code by}, all SSTables are bucketed into repeating
     * time windows. Otherwise, the first segment's parameters apply to recent data that does not fall into
     * any age-threshold bucket.
     *
     * <p>Parsing rules:
     * <ul>
     *   <li>Segments are split on {@code ;}.</li>
     *   <li>Each segment may contain at most one {@code by} or one {@code each} keyword.</li>
     *   <li>At most one {@code by} segment is permitted across all segments.</li>
     *   <li>Multiple {@code each} segments are allowed and are ordered from smallest to largest duration
     *       (youngest to oldest threshold).</li>
     *   <li>If no time keywords appear at all the entire string is treated as plain scaling parameters with
     *       no time bucketing, maintaining full backward compatibility.</li>
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

        for (String segment : segments)
        {
            String trimmedSegment = segment.trim();
            if (trimmedSegment.isEmpty())
                throw new ConfigurationException("Empty segment in scaling_parameters: '" + str + "'");

            // Try to match 'by' first, then 'each'.
            Matcher byMatcher = BY_PATTERN.matcher(trimmedSegment);
            Matcher eachMatcher = EACH_PATTERN.matcher(trimmedSegment);

            if (byMatcher.matches())
            {
                // Ensure at most one 'by' segment exists.
                boolean alreadyHasBy = timeBuckets.stream().anyMatch(tb -> tb.mode == Mode.BY);
                if (alreadyHasBy)
                    throw new ConfigurationException("At most one 'by <duration>' segment is allowed in scaling_parameters: '" + str + "'");

                int[] params = parseScalingParamsFragment(byMatcher.group(1), str);
                long durationUs = parseDurationUs(byMatcher.group(2));
                // 'by' segments set both the base params and define the window size.
                if (baseParams == null)
                    baseParams = params;
                timeBuckets.add(new TimeBucket(durationUs, params, Mode.BY));
            }
            else if (eachMatcher.matches())
            {
                int[] params = parseScalingParamsFragment(eachMatcher.group(1), str);
                long durationUs = parseDurationUs(eachMatcher.group(2));
                timeBuckets.add(new TimeBucket(durationUs, params, Mode.EACH));
            }
            else
            {
                // Plain scaling params — this is the base segment (most-recent data).
                if (baseParams != null)
                    throw new ConfigurationException("Only one plain (non-time-qualified) scaling parameter segment is allowed in: '" + str + "'");
                baseParams = parseScalingParamsFragment(trimmedSegment, str);
            }
        }

        if (baseParams == null)
        {
            // All segments had 'by'/'each' — base falls back to the 'by' window params if present.
            boolean hasByBucket = timeBuckets.stream().anyMatch(tb -> tb.mode == Mode.BY);
            if (hasByBucket)
                baseParams = timeBuckets.stream().filter(tb -> tb.mode == Mode.BY).findFirst().get().scalingParameters;
            else
                throw new ConfigurationException("No base scaling parameters found in: '" + str + "'");
        }

        // Sort EACH buckets by duration descending (oldest threshold first) so that the longest duration
        // takes precedence when evaluating which bucket matches an SSTable.
        List<TimeBucket> eachBuckets = new ArrayList<>();
        for (TimeBucket tb : timeBuckets)
            if (tb.mode == Mode.EACH)
                eachBuckets.add(tb);
        eachBuckets.sort((a, b) -> Long.compare(b.durationUs, a.durationUs));

        List<TimeBucket> byBuckets = new ArrayList<>();
        for (TimeBucket tb : timeBuckets)
            if (tb.mode == Mode.BY)
                byBuckets.add(tb);

        // Combine: EACH buckets first (oldest first), then BY bucket (at the end).
        List<TimeBucket> ordered = new ArrayList<>(eachBuckets);
        ordered.addAll(byBuckets);

        return new ParseResult(baseParams, Collections.unmodifiableList(ordered));
    }

    /**
     * Parses only the scaling-parameter portion of a segment (i.e. the part before any {@code by} or
     * {@code each} keyword) using the same rules as {@link Controller#parseScalingParameters(String)}.
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
        sb.append(mode == Mode.BY ? " by " : " each ");
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
               java.util.Arrays.equals(scalingParameters, that.scalingParameters);
    }

    @Override
    public int hashCode()
    {
        int result = java.util.Objects.hash(durationUs, mode);
        result = 31 * result + java.util.Arrays.hashCode(scalingParameters);
        return result;
    }

    /**
     * The structured result of {@link #parseScalingParameterGroups(String)}.
     *
     * <ul>
     *   <li>{@link #baseScalingParameters} — the W values for the most-recent data (or the {@code by}
     *       window data when all data is time-bucketed).</li>
     *   <li>{@link #timeBuckets} — ordered list of {@link TimeBucket} instances; may be empty if no
     *       time-based bucketing was requested.</li>
     * </ul>
     */
    public static final class ParseResult
    {
        /** Scaling parameters for the "current" or "recent" data arena. */
        public final int[] baseScalingParameters;

        /**
         * Time bucket definitions in evaluation order (youngest threshold first for {@link Mode#EACH},
         * the single {@link Mode#BY} bucket if present).
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
                                 java.util.Arrays.toString(baseScalingParameters),
                                 timeBuckets);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ParseResult that = (ParseResult) o;
            return java.util.Arrays.equals(baseScalingParameters, that.baseScalingParameters) &&
                   java.util.Objects.equals(timeBuckets, that.timeBuckets);
        }

        @Override
        public int hashCode()
        {
            int result = java.util.Objects.hashCode(timeBuckets);
            result = 31 * result + java.util.Arrays.hashCode(baseScalingParameters);
            return result;
        }
    }
}
