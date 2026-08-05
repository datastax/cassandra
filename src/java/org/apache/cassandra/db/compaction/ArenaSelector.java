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
 * <p>When time-driven levels are configured via the {@code scaling_parameters} option (using
 * {@code until <duration>} or {@code every <duration>} clauses), a {@code TimeEquivClassSplitter}
 * is appended to the equivalence-class chain. The splitter groups SSTables by their minimum timestamp:
 * <ul>
 *   <li>For {@link TimeBucket.Mode#UNTIL} buckets: SSTables whose age is less than the bucket's duration
 *       are placed into the corresponding "until" arena.</li>
 *   <li>For {@link TimeBucket.Mode#EVERY} buckets: SSTables are partitioned into fixed-size repeating time windows
 *       (aligned to multiples of the window duration since the epoch).</li>
 * </ul>
 *
 * <p>The base selector (constructed via {@link #ArenaSelector(Controller, DiskBoundaries)}) contains only
 * the permanent equivalence classes (repair status and disk index). It is stored as a long-lived field in
 * {@link UnifiedCompactionStrategy} and rebuilt only when disk boundaries change. To obtain a per-cycle
 * selector that also includes time-bucket splitting, call {@link #withTimeBuckets(long)} which returns a
 * new selector with a {@code TimeEquivClassSplitter} appended, using a consistent wall-clock snapshot.
 *
 * <p><b>Major compaction scope:</b> Because time-driven arenas are treated as independent compaction
 * units, major compaction applies <em>within</em> each time bucket. SSTables from different time
 * buckets are never compacted together, even during a user-triggered major compaction. Each bucket's
 * SSTables are major-compacted independently, producing separate output files per bucket.
 */
public class ArenaSelector implements Comparator<CompactionSSTable>
{
    private final EquivClassSplitter[] classSplitters;
    final Controller controller;
    final DiskBoundaries diskBoundaries;

    /**
     * Constructs a base selector with only permanent equivalence classes (repair status and disk index).
     * This selector does not include time-bucket splitting; use {@link #withTimeBuckets(long)} to create
     * a per-cycle selector that includes time-driven arena partitioning.
     */
    public ArenaSelector(Controller controller, DiskBoundaries diskBoundaries)
    {
        this.controller = controller;
        this.diskBoundaries = diskBoundaries;

        ArrayList<EquivClassSplitter> ret = new ArrayList<>(3);

        ret.add(RepairEquivClassSplitter.INSTANCE);

        if (diskBoundaries.getNumBoundaries() > 1)
            ret.add(new DiskIndexEquivClassSplitter());

        classSplitters = ret.toArray(new EquivClassSplitter[0]);
    }

    /**
     * Private constructor for creating a selector with an extended set of splitters (base + time buckets).
     */
    private ArenaSelector(Controller controller, DiskBoundaries diskBoundaries, EquivClassSplitter[] classSplitters)
    {
        this.controller = controller;
        this.diskBoundaries = diskBoundaries;
        this.classSplitters = classSplitters;
    }

    /**
     * Returns a new {@code ArenaSelector} that includes time-bucket splitting in addition to the
     * permanent equivalence classes. The returned selector captures the given wall-clock time for
     * consistent {@link TimeBucket.Mode#UNTIL} evaluation within a single compaction planning cycle.
     *
     * <p>If no time buckets are configured, returns {@code this} (the base selector) unchanged.
     *
     * @param nowUs the current wall-clock time in microseconds since epoch, captured once per cycle
     * @return a new selector with time-bucket splitting, or {@code this} if no time buckets are configured
     */
    public ArenaSelector withTimeBuckets(long nowUs)
    {
        List<TimeBucket> timeBuckets = controller.getTimeBuckets();
        if (timeBuckets.isEmpty())
            return this;

        EquivClassSplitter[] extended = Arrays.copyOf(classSplitters, classSplitters.length + 1);
        extended[classSplitters.length] = new TimeEquivClassSplitter(timeBuckets, nowUs);
        return new ArenaSelector(controller, diskBoundaries, extended);
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

    public boolean shareNonTimeAttributes(CompactionSSTable a, CompactionSSTable b)
    {
        for (EquivClassSplitter splitter : classSplitters)
        {
            if (splitter instanceof TimeEquivClassSplitter)
                continue;
            if (splitter.compare(a, b) != 0)
                return false;
        }
        return true;
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

        TimeEquivClassSplitter(List<TimeBucket> timeBuckets, long nowUs)
        {
            this.timeBuckets = timeBuckets;
            this.nowUs = nowUs;
            if (logger.isTraceEnabled())
            {
                logger.trace("TimeEquivClassSplitter initialized with nowUs = {} (time={}), buckets = {}",
                            nowUs, Instant.ofEpochMilli(nowUs / 1000), timeBuckets);
            }
        }

        private long timeKey(CompactionSSTable sstable)
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
                    return (Long.MAX_VALUE / 2) - wIdx;
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

        @Override
        public int compare(CompactionSSTable a, CompactionSSTable b)
        {
            return Long.compare(timeKey(a), timeKey(b));
        }

        @Override
        public String name(CompactionSSTable sstable)
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
    }
}
