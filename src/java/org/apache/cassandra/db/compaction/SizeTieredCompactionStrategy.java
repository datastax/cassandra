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
package org.apache.cassandra.db.compaction;

import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.compaction.writers.CompactionAwareWriter;
import org.apache.cassandra.db.compaction.writers.SplittingSizeTieredCompactionWriter;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.utils.Pair;

import static com.google.common.collect.Iterables.filter;

public class SizeTieredCompactionStrategy extends AbstractCompactionStrategy
{
    private static final Logger logger = LoggerFactory.getLogger(SizeTieredCompactionStrategy.class);

    /**
     * Compare {@link CompactionPick} instances by hotness first and in case of a tie by sstable size by
     * selecting the largest first (a tie would happen for system tables and new/unread sstables).
     * <p/>
     * Note that in previous version there is a comment saying "break ties by compacting the smallest sstables first"
     * but the code was doing the opposite. I preserved the behavior and fixed the comment.
     */
    private static final Comparator<CompactionPick> comparePicksByHotness = Comparator.comparing(CompactionPick::hotness)
                                                                                      .thenComparing(CompactionPick::avgSizeInBytes);

    protected SizeTieredCompactionStrategyOptions sizeTieredOptions;
    @VisibleForTesting
    protected final Set<SSTableReader> sstables = new HashSet<>();

    public SizeTieredCompactionStrategy(ColumnFamilyStore cfs, Map<String, String> options)
    {
        super(cfs, options);
        this.sizeTieredOptions = new SizeTieredCompactionStrategyOptions(options);
    }

    private synchronized CompactionAggregate getNextBackgroundSSTables(final int gcBefore)
    {
        // make local copies so they can't be changed out from under us mid-method
        int minThreshold = cfs.getMinimumCompactionThreshold();
        int maxThreshold = cfs.getMaximumCompactionThreshold();

        Iterable<SSTableReader> candidates = filterSuspectSSTables(filter(cfs.getUncompactingSSTables(), sstables::contains));

        SizeTieredBuckets sizeTieredBuckets = new SizeTieredBuckets(candidates, sizeTieredOptions, minThreshold, maxThreshold);
        sizeTieredBuckets.aggregate();

        backgroundCompactions.setPending(sizeTieredBuckets.getAggregates());

        CompactionAggregate ret = sizeTieredBuckets.getAggregates().isEmpty() ? null : sizeTieredBuckets.getAggregates().get(0);
        if (ret != null && !ret.isEmpty())
            return ret;

        // if there is no sstable to compact in standard way, try compacting single sstable whose droppable tombstone
        // ratio is greater than threshold.
        List<SSTableReader> sstablesWithTombstones = new ArrayList<>();
        for (SSTableReader sstable : candidates)
        {
            if (worthDroppingTombstones(sstable, gcBefore))
                sstablesWithTombstones.add(sstable);
        }
        if (sstablesWithTombstones.isEmpty())
            return null;

        return CompactionAggregate.createForTombstones(Collections.max(sstablesWithTombstones, SSTableReader.sizeComparator));
    }

    /**
     * This class contains the logic for {@link SizeTieredCompactionStrategy}:
     *
     * - for each candidate sstable it gets the length on disk and stores it into {@link this#pairs}
     * - it sorts the candidates into buckets
     * - it caches the sstable hotness (pre-existing code, I'm not sure it buys us much)
     * - it organizes the buckets into a list of {@link CompactionAggregate}, an aggregate per bucket.
     *   An aggregate will have a list of compaction picks, each pick is a list of sstables below the max threshold,
     *   sorted by hotness.
     * - the aggregates are sorted by comparing the total hotness of the first pick of each aggregate
     * - the aggregate with the hottest first pick will have its first pick submitted for compaction.
     */
    @NotThreadSafe
    final static class SizeTieredBuckets
    {
        private final SizeTieredCompactionStrategyOptions options;
        private final List<Pair<SSTableReader, Long>> pairs;
        private final Map<Long, List<SSTableReader>> buckets;
        private final Map<SSTableReader, Double> hotnessCache;
        private final int minThreshold;
        private final int maxThreshold;

        /**
         * This is the list of compactions order by most interesting first
         */
        private List<CompactionAggregate> aggregates;

        /**
         * @param candidates   list sstables that are not yet compacting
         * @param options      the options for size tiered compaction strategy
         * @param minThreshold minimum number of sstables in a bucket to qualify as interesting
         * @param maxThreshold maximum number of sstables to compact at once (the returned bucket will be trimmed down to this)
         */
        SizeTieredBuckets(Iterable<? extends SSTableReader> candidates,
                          SizeTieredCompactionStrategyOptions options,
                          int minThreshold,
                          int maxThreshold)
        {
            this.options = options;
            this.pairs = createSSTableAndLengthPairs(candidates);
            this.buckets = getBuckets(pairs, options);
            this.hotnessCache = getHotnessCache(buckets.values());
            this.minThreshold = minThreshold;
            this.maxThreshold = maxThreshold;

            this.aggregates = new ArrayList<>(buckets.size());

            if (logger.isTraceEnabled())
                logger.trace("Compaction buckets are {}", buckets);
        }

        /**
         * For each sstable, return a pair containing the sstable and its length on disk.
         */
        private static List<Pair<SSTableReader, Long>> createSSTableAndLengthPairs(Iterable<? extends SSTableReader> sstables)
        {
            List<Pair<SSTableReader, Long>> sstableLengthPairs = new ArrayList<>(Iterables.size(sstables));
            for (SSTableReader sstable : sstables)
                sstableLengthPairs.add(Pair.create(sstable, sstable.onDiskLength()));
            return sstableLengthPairs;
        }

        /**
         * Group sstables of similar on disk size into buckets.
         */
        private static Map<Long, List<SSTableReader>> getBuckets(Collection<Pair<SSTableReader, Long>> sstables, SizeTieredCompactionStrategyOptions options)
        {
            // Sort the list in order to get deterministic results during the grouping below
            List<Pair<SSTableReader, Long>> sortedFiles = new ArrayList<>(sstables);
            Collections.sort(sortedFiles, Comparator.comparing(p -> p.right));

            Map<Long, List<SSTableReader>> buckets = new HashMap<>();

            outer:
            for (Pair<SSTableReader, Long> pair : sortedFiles)
            {
                long size = pair.right;

                // look for a bucket containing similar-sized files:
                // group in the same bucket if it's w/in 50% of the average for this bucket,
                // or this file and the bucket are all considered "small" (less than `minSSTableSize`)
                for (Entry<Long, List<SSTableReader>> entry : buckets.entrySet())
                {
                    List<SSTableReader> bucket = entry.getValue();
                    long oldAverageSize = entry.getKey();
                    if ((size > (oldAverageSize * options.bucketLow) && size < (oldAverageSize * options.bucketHigh))
                        || (size < options.minSSTableSize && oldAverageSize < options.minSSTableSize))
                    {
                        // remove and re-add under new new average size
                        buckets.remove(oldAverageSize);
                        long totalSize = bucket.size() * oldAverageSize;
                        long newAverageSize = (totalSize + size) / (bucket.size() + 1);
                        bucket.add(pair.left);
                        buckets.put(newAverageSize, bucket);
                        continue outer;
                    }
                }

                // no similar bucket found; put it in a new one
                ArrayList<SSTableReader> bucket = new ArrayList<>();
                bucket.add(pair.left);
                buckets.put(size, bucket);
            }
            return buckets;
        }

        /**
         * For each bucket with at least minThreshold sstables:
         * <p>
         * - sort the sstables by hotness
         * - divide the bucket into max threshold sstables and add it to a temporary list of candidates along with the total hotness of the bucket section
         * <p>
         * Then select the candidate with the max hotness and the most interesting bucket and put the remaining candidates in the pending list.
         *
         * @return the parent object {@link SizeTieredBuckets}
         */
        SizeTieredBuckets aggregate()
        {
            if (!aggregates.isEmpty())
                return this; // already called

            List<CompactionAggregate> aggregatesWithoutCompactions = new ArrayList<>(buckets.size());
            List<CompactionAggregate> aggregatesWithCompactions = new ArrayList<>(buckets.size());

            for (Map.Entry<Long, List<SSTableReader>> entry : buckets.entrySet())
            {
                long avgSizeBytes = entry.getKey();
                long minSizeBytes = (long) (avgSizeBytes * options.bucketLow);
                long maxizeBytes = (long) (avgSizeBytes * options.bucketHigh);

                List<SSTableReader> bucket = entry.getValue();
                double hotness = totHotness(bucket, hotnessCache);

                if (bucket.size() < minThreshold)
                {
                    if (logger.isTraceEnabled())
                        logger.trace("Aggregate with {} avg bytes for {} files not considered for compaction: {}", avgSizeBytes, bucket.size(), bucket);

                    aggregatesWithoutCompactions.add(CompactionAggregate.createSizeTiered(bucket,
                                                                                          CompactionPick.EMPTY,
                                                                                          ImmutableList.of(),
                                                                                          hotness,
                                                                                          avgSizeBytes,
                                                                                          minSizeBytes,
                                                                                          maxizeBytes));

                    continue;
                }

                // sort the bucket by hotness
                Collections.sort(bucket, (o1, o2) -> -1 * Double.compare(hotnessCache.get(o1), hotnessCache.get(o2)));

                // now divide the candidates into a list of picks, each pick with at most max threshold sstables
                int i = 0;
                CompactionPick selected = null;
                List<CompactionPick> pending = new ArrayList<>();


                while ((bucket.size() - i) >= minThreshold)
                {
                    List<SSTableReader> sstables = bucket.subList(i, i + Math.min(bucket.size() - i, maxThreshold));
                    if (selected == null)
                        selected = CompactionPick.create(avgSizeBytes, sstables, totHotness(sstables, hotnessCache));
                    else
                        pending.add(CompactionPick.create(avgSizeBytes, sstables, totHotness(sstables, hotnessCache)));

                    i += sstables.size();
                }

                if (logger.isTraceEnabled())
                    logger.trace("Aggregate with {} avg bytes for {} files considered for compaction: {}", avgSizeBytes, bucket.size(), bucket);

                // Finally create the new aggregate with the new pending compactions and those already compacting and not yet completed
                aggregatesWithCompactions.add(CompactionAggregate.createSizeTiered(bucket, selected, pending, hotness, avgSizeBytes, minSizeBytes, maxizeBytes));
            }

            // This sorts the aggregates based on the hotness of their selected pick so that the aggregate with the hottest selected pick
            // be first in the list and get submitted
            if (!aggregatesWithCompactions.isEmpty())
            {
                Collections.sort(aggregatesWithCompactions, Collections.reverseOrder(Comparator.comparing(aggregate -> aggregate.getSelected(), comparePicksByHotness)));

                if (logger.isTraceEnabled())
                    logger.trace("Found compaction for aggregate {}", aggregatesWithCompactions.get(0));
            }
            else
            {
                if (logger.isTraceEnabled())
                    logger.trace("No compactions found");
            }

            // publish the results
            this.aggregates.addAll(aggregatesWithCompactions); // those with compactions first, because the first one will be the one submitted
            this.aggregates.addAll(aggregatesWithoutCompactions); // then add those empty
            return this;
        }

        public List<Pair<SSTableReader, Long>> pairs()
        {
            return pairs;
        }

        public List<List<SSTableReader>> buckets()
        {
            return new ArrayList<>(buckets.values());
        }

        public List<CompactionAggregate> getAggregates()
        {
            return aggregates;
        }

        public List<CompactionPick> getCompactions()
        {
            return aggregates.stream().flatMap(aggr -> aggr.getActive().stream()).collect(Collectors.toList());
        }
    }

    /**
     * @return a cache mapping sstables to their current read hotness.
     */
    @VisibleForTesting
    static Map<SSTableReader, Double> getHotnessCache(Collection<List<SSTableReader>> buckets)
    {
        Map<SSTableReader, Double> ret = new HashMap<>();

        for (List<SSTableReader> sstables: buckets)
        {
            for (SSTableReader sstable : sstables)
                ret.put(sstable, sstable.hotness());
        }

        return ret;
    }

    /**
     * @return the sum of the hotness of all the sstables
     */
    private static double totHotness(Iterable<SSTableReader> sstables, @Nullable final Map<SSTableReader, Double> hotnessCache)
    {
        double hotness = 0.0;
        for (SSTableReader sstable : sstables)
        {
            double h = hotnessCache == null ? 0.0 : hotnessCache.getOrDefault(sstable, 0.0);
            hotness += h == 0.0  ? sstable.hotness() : h;
        }

        return hotness;
    }

    @SuppressWarnings("resource")
    public AbstractCompactionTask getNextBackgroundTask(int gcBefore)
    {
        CompactionPick previous = null;
        while (true)
        {
            CompactionAggregate compaction = getNextBackgroundSSTables(gcBefore);
            if (compaction == null || compaction.isEmpty())
                return null;

            // Already tried acquiring references without success. It means there is a race with
            // the tracker but candidate SSTables were not yet replaced in the compaction strategy manager
            if (compaction.getSelected().equals(previous))
            {
                logger.warn("Could not acquire references for compacting SSTables {} which is not a problem per se," +
                            "unless it happens frequently, in which case it must be reported. Will retry later.",
                            compaction.getSelected());
                return null;
            }

            LifecycleTransaction transaction = cfs.getTracker().tryModify(compaction.getSelected().sstables, OperationType.COMPACTION);
            if (transaction != null)
            {
                backgroundCompactions.setSubmitted(transaction.opId(), compaction);
                return CompactionTask.forCompaction(this, transaction, gcBefore);
            }
            previous = compaction.getSelected();
        }
    }

    @SuppressWarnings("resource")
    public synchronized Collection<AbstractCompactionTask> getMaximalTask(final int gcBefore, boolean splitOutput)
    {
        Iterable<SSTableReader> filteredSSTables = filterSuspectSSTables(sstables);
        if (Iterables.isEmpty(filteredSSTables))
            return null;
        LifecycleTransaction txn = cfs.getTracker().tryModify(filteredSSTables, OperationType.COMPACTION);
        if (txn == null)
            return null;
        if (splitOutput)
            return Arrays.asList(SplittingCompactionTask.forSplitting(this, txn, gcBefore));
        return Arrays.asList(CompactionTask.forCompaction(this, txn, gcBefore));
    }

    @SuppressWarnings("resource")
    public AbstractCompactionTask getUserDefinedTask(Collection<SSTableReader> sstables, final int gcBefore)
    {
        assert !sstables.isEmpty(); // checked for by CM.submitUserDefined

        LifecycleTransaction transaction = cfs.getTracker().tryModify(sstables, OperationType.COMPACTION);
        if (transaction == null)
        {
            logger.trace("Unable to mark {} for compaction; probably a background compaction got to it first.  You can disable background compactions temporarily if this is a problem", sstables);
            return null;
        }

        return CompactionTask.forCompaction(this, transaction, gcBefore).setUserDefined(true);
    }

    public long getMaxSSTableBytes()
    {
        return Long.MAX_VALUE;
    }

    public static Map<String, String> validateOptions(Map<String, String> options) throws ConfigurationException
    {
        Map<String, String> uncheckedOptions = AbstractCompactionStrategy.validateOptions(options);
        uncheckedOptions = SizeTieredCompactionStrategyOptions.validateOptions(options, uncheckedOptions);

        uncheckedOptions.remove(CompactionParams.Option.MIN_THRESHOLD.toString());
        uncheckedOptions.remove(CompactionParams.Option.MAX_THRESHOLD.toString());

        return uncheckedOptions;
    }

    @Override
    public synchronized void addSSTable(SSTableReader added)
    {
        sstables.add(added);
    }

    @Override
    public synchronized void removeSSTable(SSTableReader sstable)
    {
        sstables.remove(sstable);
    }

    @Override
    protected Set<SSTableReader> getSSTables()
    {
        return ImmutableSet.copyOf(sstables);
    }

    public String toString()
    {
        return String.format("SizeTieredCompactionStrategy[%s/%s]",
            cfs.getMinimumCompactionThreshold(),
            cfs.getMaximumCompactionThreshold());
    }

    private static class SplittingCompactionTask extends CompactionTask
    {
        public SplittingCompactionTask(AbstractCompactionStrategy strategy, LifecycleTransaction txn, int gcBefore)
        {
            super(strategy, txn, gcBefore, false);
        }

        static AbstractCompactionTask forSplitting(AbstractCompactionStrategy strategy, LifecycleTransaction txn, int gcBefore)
        {
            return new SplittingCompactionTask(strategy, txn, gcBefore);
        }

        @Override
        public CompactionAwareWriter getCompactionAwareWriter(ColumnFamilyStore cfs,
                                                              Directories directories,
                                                              LifecycleTransaction txn,
                                                              Set<SSTableReader> nonExpiredSSTables)
        {
            return new SplittingSizeTieredCompactionWriter(cfs, directories, txn, nonExpiredSSTables);
        }
    }
}
