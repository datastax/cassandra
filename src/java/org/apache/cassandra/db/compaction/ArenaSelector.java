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

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.DiskBoundaries;
import org.apache.cassandra.db.compaction.unified.Controller;
import org.apache.cassandra.db.compaction.unified.TimeBucket;
import org.apache.cassandra.io.sstable.Component;

/**
 * Arena selector, used by UnifiedCompactionStrategy to distribute SSTables to separate compaction arenas.
 *
 * <p>This is used to:
 * <ul>
 *   <li>Ensure that SSTables that should not be compacted together (e.g. repaired with unrepaired)
 *       are separated.</li>
 *   <li>Ensure that each disk's SSTables are compacted separately.</li>
 *   <li>Optionally, partition SSTables into time-driven arenas when {@link TimeBucket} definitions are
 *       present in the controller configuration.</li>
 * </ul>
 *
 * <p>When time-driven levels are configured via the {@code scaling_parameters} option, a {@code TimeEquivClassSplitter}
 * is appended to the equivalence-class chain. The splitter groups SSTables by their minimum timestamp:
 * <ul>
 *   <li>For {@link TimeBucket.Mode#UNTIL} buckets: SSTables whose age is less than the bucket's duration
 *       are placed into the corresponding "until" arena.</li>
 *   <li>For {@link TimeBucket.Mode#EVERY} buckets: SSTables are partitioned into fixed-size repeating time windows
 *       (aligned to multiples of the window duration since the epoch).</li>
 * </ul>
 *
 * <p>The current wall-clock time for {@link TimeBucket.Mode#UNTIL} evaluation is captured once at
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
     *   <li>{@link TimeBucket.Mode#UNTIL} buckets (zero or more, ordered from youngest to oldest duration) —
     *       each creates a separate transient young arena.</li>
     *   <li>{@link TimeBucket.Mode#EVERY} bucket (at most one) — determines the time-window index by dividing
     *       the SSTable's minimum timestamp by the window duration.</li>
     * </ol>
     *
     * <h3>Arena naming</h3>
     * <p>The arena name suffix returned by {@link #name(CompactionSSTable)} encodes which bucket the
     * SSTable belongs to:
     * <ul>
     *   <li>{@code until_<duration>} for {@link TimeBucket.Mode#UNTIL} arenas.</li>
     *   <li>{@code every_<timestamp>} for {@link TimeBucket.Mode#EVERY} arenas, where {@code timestamp} is the
     *       start of the window formatted as a human-readable date.</li>
     *   <li>{@code base} if the SSTable does not fall into any time bucket (applies to the oldest data).</li>
     * </ul>
     *
     * <h3>Comparison logic</h3>
     * <p>Two SSTables compare as equal (value 0) only when both their time-bucket keys are identical.
     * If both fall in the same {@code EVERY} window, they compare as 0. If both fall in the same {@code UNTIL}
     * bucket or both match the base arena, they compare as 0.
     */
    private static final class TimeEquivClassSplitter implements EquivClassSplitter
    {
        private static final Logger logger = LoggerFactory.getLogger(TimeEquivClassSplitter.class);

        private final List<TimeBucket> timeBuckets;
        /**
         * Wall-clock time in microseconds captured at ArenaSelector construction, used to evaluate
         * {@link TimeBucket.Mode#UNTIL} age limits consistently within a planning cycle.
         */
        private final long nowUs;

        private final Function<CompactionSSTable, Long> computeTimeKeyRef;
        private final Function<CompactionSSTable, String> computeNameRef;

        /** Thread-safe caches to avoid redundant computations and logging during sorting and grouping. */
        private final ConcurrentHashMap<CompactionSSTable, Long> keyCache = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<CompactionSSTable, String> nameCache = new ConcurrentHashMap<>();

        TimeEquivClassSplitter(List<TimeBucket> timeBuckets, long nowUs)
        {
            this.timeBuckets = timeBuckets;
            this.nowUs = nowUs;
            this.computeTimeKeyRef = this::computeTimeKey;
            this.computeNameRef = this::computeName;
            if (logger.isTraceEnabled())
            {
                logger.trace("TimeEquivClassSplitter initialized with nowUs = {} (time={}), buckets = {}",
                            nowUs, Instant.ofEpochMilli(nowUs / 1000), timeBuckets);
            }
        }

        private long computeTimeKey(CompactionSSTable sstable)
        {
            long minTimestampUs = sstable.getMinTimestamp();

            for (TimeBucket bucket : timeBuckets)
            {
                if (bucket.mode == TimeBucket.Mode.UNTIL)
                {
                    boolean isYoung = (nowUs - minTimestampUs) < bucket.durationUs;
                    if (logger.isTraceEnabled())
                    {
                        logger.trace("timeKey for sstable {} in keyspace {} table {}: checking UNTIL bucket {}, minTimestampUs={}, nowUs={}, ageSecs={}, isYoung={}",
                                    sstable.getDescriptor().filenameFor(Component.DATA),
                                    sstable.getKeyspaceName(), sstable.getColumnFamilyName(),
                                    bucket, minTimestampUs, nowUs, (nowUs - minTimestampUs) / 1000000.0, isYoung);
                    }
                    if (isYoung)
                    {
                        return bucket.durationUs;
                    }
                }
                else if (bucket.mode == TimeBucket.Mode.EVERY)
                {
                    long wIdx = bucket.windowIndex(minTimestampUs);
                    if (logger.isTraceEnabled())
                    {
                        logger.trace("timeKey for sstable {} in keyspace {} table {}: matches EVERY bucket {}, minTimestampUs={}, nowUs={}, windowIndex={}",
                                    sstable.getDescriptor().filenameFor(Component.DATA),
                                    sstable.getKeyspaceName(), sstable.getColumnFamilyName(),
                                    bucket, minTimestampUs, nowUs, wIdx);
                    }
                    return (Long.MAX_VALUE / 2) + wIdx;
                }
            }

            if (logger.isTraceEnabled())
            {
                logger.trace("timeKey for sstable {} in keyspace {} table {}: matches no buckets, categorized as base",
                            sstable.getDescriptor().filenameFor(Component.DATA),
                            sstable.getKeyspaceName(), sstable.getColumnFamilyName());
            }
            return Long.MAX_VALUE;
        }

        private long timeKey(CompactionSSTable sstable)
        {
            return keyCache.computeIfAbsent(sstable, computeTimeKeyRef);
        }

        @Override
        public int compare(CompactionSSTable a, CompactionSSTable b)
        {
            return Long.compare(timeKey(a), timeKey(b));
        }

        private String computeName(CompactionSSTable sstable)
        {
            long minTimestampUs = sstable.getMinTimestamp();
            for (TimeBucket bucket : timeBuckets)
            {
                if (bucket.mode == TimeBucket.Mode.UNTIL)
                {
                    if ((nowUs - minTimestampUs) < bucket.durationUs)
                        return "until_" + bucket.formattedDuration;
                }
                else if (bucket.mode == TimeBucket.Mode.EVERY)
                {
                    return "every_" + bucket.arenaName(bucket.windowIndex(minTimestampUs));
                }
            }
            return "base";
        }

        @Override
        public String name(CompactionSSTable sstable)
        {
            return nameCache.computeIfAbsent(sstable, computeNameRef);
        }
    }
}
