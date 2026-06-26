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

package org.apache.cassandra.db.compaction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.DiskBoundaries;
import org.apache.cassandra.db.compaction.unified.Controller;
import org.apache.cassandra.db.compaction.unified.TimeBucket;

/**
 * Arena selector, used by UnifiedCompactionStrategy to distribute SSTables to separate compaction arenas.
 *
 * <p>This is used to:
 * <ul>
 *   <li>ensure that sstables that should not be compacted together (e.g. repaired with unrepaired)
 *       are separated;</li>
 *   <li>ensure that each disk's sstables are compacted separately;</li>
 *   <li>optionally, partition sstables into time-driven arenas when {@link TimeBucket} definitions are
 *       present in the controller configuration.</li>
 * </ul>
 *
 * <p>When time-driven levels are configured via the {@code scaling_parameters} option (using
 * {@code by <duration>} or {@code each <duration>} clauses), a {@link TimeEquivClassSplitter} is
 * appended to the equivalence-class chain. The splitter groups SSTables by their minimum timestamp:
 * <ul>
 *   <li>For {@link TimeBucket.Mode#BY} buckets: SSTables whose {@code minTimestamp} falls in the
 *       same fixed-size window (aligned to multiples of the window duration since the epoch) are
 *       placed into the same arena.</li>
 *   <li>For {@link TimeBucket.Mode#EACH} buckets: SSTables older than the threshold are placed into
 *       a separate "old data" arena with their own scaling parameters.</li>
 * </ul>
 *
 * <p>The current wall-clock time for {@link TimeBucket.Mode#EACH} evaluation is captured once at
 * construction time, so that the arena boundaries remain stable during a single compaction planning
 * cycle.
 */
public class ArenaSelector implements Comparator<CompactionSSTable>
{
    private final EquivClassSplitter[] classSplitters;
    final Controller controller;
    final DiskBoundaries diskBoundaries;

    public ArenaSelector(Controller controller, DiskBoundaries diskBoundaries)
    {
        this.controller = controller;
        this.diskBoundaries = diskBoundaries;

        ArrayList<EquivClassSplitter> ret = new ArrayList<>(3);

        ret.add(RepairEquivClassSplitter.INSTANCE);

        if (diskBoundaries.getNumBoundaries() > 1)
            ret.add(new DiskIndexEquivClassSplitter());

        // Add time-based splitting when time-bucket definitions are present.
        List<TimeBucket> timeBuckets = controller.getTimeBuckets();
        if (!timeBuckets.isEmpty())
        {
            // Capture now() once so the 'each' thresholds are evaluated consistently within this cycle.
            long nowUs = TimeUnit.MILLISECONDS.toMicros(controller.clock.translate().toMillisSinceEpoch(controller.clock.now()));
            ret.add(new TimeEquivClassSplitter(timeBuckets, nowUs));
        }

        classSplitters = ret.toArray(new EquivClassSplitter[0]);
    }

    @Override
    public int compare(CompactionSSTable o1, CompactionSSTable o2)
    {
        int res = 0;
        for (int i = 0; res == 0 && i < classSplitters.length; i++)
            res = classSplitters[i].compare(o1, o2);
        return res;
    }

    public String name(CompactionSSTable t)
    {
        return Arrays.stream(classSplitters)
                     .map(e -> e.name(t))
                     .collect(Collectors.joining("-"));
    }

    /**
     * An equivalence class is a function that compares two sstables and returns 0 when they fall in the same class.
     * For example, the repair status or disk index may define equivalence classes. See the concrete equivalence classes below.
     */
    private interface EquivClassSplitter extends Comparator<CompactionSSTable> {

        @Override
        int compare(CompactionSSTable a, CompactionSSTable b);

        /** Return a name that describes the equivalence class */
        String name(CompactionSSTable ssTableReader);
    }

    /**
     * Split sstables by their repair state: repaired, unrepaired, pending repair with a specific UUID (one group per pending repair).
     */
    private static final class RepairEquivClassSplitter implements EquivClassSplitter
    {
        public static final EquivClassSplitter INSTANCE = new RepairEquivClassSplitter();

        @Override
        public int compare(CompactionSSTable a, CompactionSSTable b)
        {
            // This is the same as name(a).compareTo(name(b))
            int af = repairClassValue(a);
            int bf = repairClassValue(b);
            if (af != 0 || bf != 0)
                return Integer.compare(af, bf);
            return a.getPendingRepair().compareTo(b.getPendingRepair());
        }

        private static int repairClassValue(CompactionSSTable a)
        {
            if (a.isRepaired())
                return 1;
            if (!a.isPendingRepair())
                return 2;
            else
                return 0;
        }

        @Override
        public String name(CompactionSSTable ssTableReader)
        {
            if (ssTableReader.isRepaired())
                return "repaired";
            else if (!ssTableReader.isPendingRepair())
                return "unrepaired";
            else
                return "pending_repair_" + ssTableReader.getPendingRepair();
        }
    }

    /**
     * Group sstables by their disk index.
     */
    private final class DiskIndexEquivClassSplitter implements EquivClassSplitter
    {
        @Override
        public int compare(CompactionSSTable a, CompactionSSTable b)
        {
            return Integer.compare(diskBoundaries.getDiskIndexFromKey(a), diskBoundaries.getDiskIndexFromKey(b));
        }

        @Override
        public String name(CompactionSSTable ssTableReader)
        {
            return "disk_" + diskBoundaries.getDiskIndexFromKey(ssTableReader);
        }
    }

    /**
     * Partitions SSTables into time-based arenas using the ordered list of {@link TimeBucket} definitions.
     *
     * <h3>Evaluation order</h3>
     * <p>The buckets are evaluated in the order they appear in the {@code timeBuckets} list, which is:
     * <ol>
     *   <li>{@link TimeBucket.Mode#EACH} buckets (zero or more, oldest threshold first) — each one
     *       creates a separate "old data" partition for SSTables whose age exceeds the threshold;</li>
     *   <li>{@link TimeBucket.Mode#BY} bucket (at most one) — determines the time-window index by dividing
     *       the SSTable's minimum timestamp by the window duration.</li>
     * </ol>
     *
     * <h3>Arena naming</h3>
     * <p>The arena name suffix returned by {@link #name(CompactionSSTable)} encodes which bucket the
     * SSTable belongs to:
     * <ul>
     *   <li>{@code tw_<windowIndex>} for {@link TimeBucket.Mode#BY} arenas</li>
     *   <li>{@code old_<thresholdDays>d} for {@link TimeBucket.Mode#EACH} arenas, where {@code thresholdDays}
     *       is the threshold duration rounded to days for readability</li>
     *   <li>{@code recent} if the SSTable does not fall into any old-data bucket</li>
     * </ul>
     *
     * <h3>Comparison logic</h3>
     * <p>Two SSTables compare as equal (value 0) only when both their time-bucket keys are identical.
     * If both fall in the same BY window they compare as 0 via a long comparison of window indices.
     * If one or both hit an EACH threshold, the first (oldest) matching threshold determines the key.
     */
    private static final class TimeEquivClassSplitter implements EquivClassSplitter
    {
        private static final Logger logger = LoggerFactory.getLogger(TimeEquivClassSplitter.class);

        private final List<TimeBucket> timeBuckets;
        /**
         * Wall-clock time in microseconds captured at ArenaSelector construction, used to evaluate
         * {@link TimeBucket.Mode#EACH} age thresholds consistently within a planning cycle.
         */
        private final long nowUs;

        /** Thread-safe caches to avoid redundant computations and logging during sorting and grouping. */
        private final java.util.concurrent.ConcurrentHashMap<CompactionSSTable, Long> keyCache = new java.util.concurrent.ConcurrentHashMap<>();
        private final java.util.concurrent.ConcurrentHashMap<CompactionSSTable, String> nameCache = new java.util.concurrent.ConcurrentHashMap<>();

        TimeEquivClassSplitter(List<TimeBucket> timeBuckets, long nowUs)
        {
            this.timeBuckets = timeBuckets;
            this.nowUs = nowUs;
            logger.info("TimeEquivClassSplitter initialized with nowUs = {} (time={}), buckets = {}",
                        nowUs, java.time.Instant.ofEpochMilli(nowUs / 1000), timeBuckets);
        }

        /**
         * Computes a stable, comparable key for this SSTable based on the configured time buckets.
         * The key is encoded as a {@code long} that preserves the ordering of time-based arenas:
         * <ul>
         *   <li>For {@link TimeBucket.Mode#BY}: returns the window index (positive or negative long).
         *       Two SSTables with the same window index produce the same key.</li>
         *   <li>For {@link TimeBucket.Mode#EACH}: if the SSTable's age exceeds the threshold the key
         *       encodes the threshold duration as a negative sentinel to keep "old" arenas distinct from
         *       "recent" data and from each other (youngest threshold = least negative key).</li>
         *   <li>If no time bucket matches the SSTable falls into the "recent" arena (key = 0).</li>
         * </ul>
         */
        private long timeKey(CompactionSSTable sstable)
        {
            return keyCache.computeIfAbsent(sstable, s -> {
                long minTimestampUs = s.getMinTimestamp();

                for (TimeBucket bucket : timeBuckets)
                {
                    if (bucket.mode == TimeBucket.Mode.BY)
                    {
                        // BY mode: all SSTables get a window-index key regardless of age.
                        // The window index can be any long; we encode it directly so that SSTables in the
                        // same window compare equal and SSTables in different windows compare different.
                        long wIdx = bucket.windowIndex(minTimestampUs);
                        if (logger.isDebugEnabled())
                        {
                            logger.debug("timeKey for sstable {} in keyspace {} table {}: matches BY bucket {}, minTimestampUs={}, nowUs={}, windowIndex={}",
                                        s.getDescriptor().filenameFor(org.apache.cassandra.io.sstable.Component.DATA),
                                        s.getKeyspaceName(), s.getColumnFamilyName(),
                                        bucket, minTimestampUs, nowUs, wIdx);
                        }
                        return wIdx;
                    }
                    else // EACH mode
                    {
                        boolean isOld = bucket.isOld(minTimestampUs, nowUs);
                        if (logger.isDebugEnabled())
                        {
                            logger.debug("timeKey for sstable {} in keyspace {} table {}: checking EACH bucket {}, minTimestampUs={}, nowUs={}, ageSecs={}, isOld={}",
                                        s.getDescriptor().filenameFor(org.apache.cassandra.io.sstable.Component.DATA),
                                        s.getKeyspaceName(), s.getColumnFamilyName(),
                                        bucket, minTimestampUs, nowUs, (nowUs - minTimestampUs) / 1000000.0, isOld);
                        }
                        if (isOld)
                        {
                            // Encode the threshold as a large negative number to ensure "old" arenas are
                            // ordered before (i.e. separate from) "recent" arenas.
                            // Use Long.MIN_VALUE/2 as base to avoid overflow and leave room for multiple thresholds.
                            // Older thresholds get more-negative keys (they appear first in the sorted list).
                            return -(bucket.durationUs + (Long.MAX_VALUE / 2));
                        }
                    }
                }
                // The SSTable is "recent" — it doesn't fall into any time bucket.
                if (logger.isDebugEnabled())
                {
                    logger.debug("timeKey for sstable {} in keyspace {} table {}: matches no buckets, categorized as recent",
                                s.getDescriptor().filenameFor(org.apache.cassandra.io.sstable.Component.DATA),
                                s.getKeyspaceName(), s.getColumnFamilyName());
                }
                return 0L;
            });
        }

        @Override
        public int compare(CompactionSSTable a, CompactionSSTable b)
        {
            return Long.compare(timeKey(a), timeKey(b));
        }

        @Override
        public String name(CompactionSSTable sstable)
        {
            return nameCache.computeIfAbsent(sstable, s -> {
                long minTimestampUs = s.getMinTimestamp();
                for (TimeBucket bucket : timeBuckets)
                {
                    if (bucket.mode == TimeBucket.Mode.BY)
                        return "tw_" + bucket.windowIndex(minTimestampUs);
                    else if (bucket.isOld(minTimestampUs, nowUs))
                        return "old_" + bucket.formattedDuration;
                }
                return "recent";
            });
        }
    }
}
