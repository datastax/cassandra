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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.utils.Pair;

import static com.google.common.collect.Iterables.filter;

public class TimeWindowCompactionStrategy extends AbstractCompactionStrategy
{
    private static final Logger logger = LoggerFactory.getLogger(TimeWindowCompactionStrategy.class);

    private final TimeWindowCompactionStrategyOptions options;
    private final Set<SSTableReader> sstables = new HashSet<>();
    private long lastExpiredCheck;
    private long highestWindowSeen;

    public TimeWindowCompactionStrategy(ColumnFamilyStore cfs, Map<String, String> options)
    {
        super(cfs, options);
        this.options = new TimeWindowCompactionStrategyOptions(options);
        if (!options.containsKey(AbstractCompactionStrategy.TOMBSTONE_COMPACTION_INTERVAL_OPTION) && !options.containsKey(AbstractCompactionStrategy.TOMBSTONE_THRESHOLD_OPTION))
        {
            disableTombstoneCompactions = true;
            logger.debug("Disabling tombstone compactions for TWCS");
        }
        else
            logger.debug("Enabling tombstone compactions for TWCS");
    }

    @Override
    @SuppressWarnings("resource") // transaction is closed by AbstractCompactionTask::execute
    public AbstractCompactionTask getNextBackgroundTask(int gcBefore)
    {
        CompactionPick previousCandidate = null;
        while (true)
        {
            CompactionAggregate compaction = getNextBackgroundCompactions(gcBefore);
            if (compaction == null || compaction.isEmpty())
                return null;

            // Already tried acquiring references without success. It means there is a race with
            // the tracker but candidate SSTables were not yet replaced in the compaction strategy manager
            if (compaction.getSelected().equals(previousCandidate))
            {
                logger.warn("Could not acquire references for compacting SSTables {} which is not a problem per se," +
                            "unless it happens frequently, in which case it must be reported. Will retry later.",
                            compaction.getSelected());
                return null;
            }

            LifecycleTransaction modifier = cfs.getTracker().tryModify(compaction.getSelected().sstables, OperationType.COMPACTION);
            if (modifier != null)
            {
                backgroundCompactions.setSubmitted(modifier.opId(), compaction);
                return CompactionTask.forTimeWindowCompaction(this, modifier, gcBefore);
            }
            previousCandidate = compaction.getSelected();
        }
    }

    /**
     *
     * @param gcBefore
     * @return
     */
    private synchronized CompactionAggregate getNextBackgroundCompactions(final int gcBefore)
    {
        if (Iterables.isEmpty(cfs.getSSTables(SSTableSet.LIVE)))
            return null;

        Set<SSTableReader> uncompacting = ImmutableSet.copyOf(filter(cfs.getUncompactingSSTables(), sstables::contains));

        // Find fully expired SSTables. Those will be included no matter what.
        Set<SSTableReader> expired = Collections.emptySet();

        if (System.currentTimeMillis() - lastExpiredCheck > options.expiredSSTableCheckFrequency)
        {
            logger.debug("TWCS expired check sufficiently far in the past, checking for fully expired SSTables");
            expired = CompactionController.getFullyExpiredSSTables(cfs, uncompacting, options.ignoreOverlaps ? Collections.emptySet() : cfs.getOverlappingLiveSSTables(uncompacting),
                                                                   gcBefore, options.ignoreOverlaps);
            lastExpiredCheck = System.currentTimeMillis();
        }
        else
        {
            logger.debug("TWCS skipping check for fully expired SSTables");
        }

        Set<SSTableReader> candidates = Sets.newHashSet(filterSuspectSSTables(uncompacting));

        CompactionAggregate compactionCandidate = getNextNonExpiredSSTables(Sets.difference(candidates, expired), gcBefore);
        if (expired.isEmpty())
            return compactionCandidate;

        logger.debug("Including expired sstables: {}", expired);
        if (compactionCandidate == null)
        {
            long timestamp = getWindowBoundsInMillis(options.sstableWindowUnit, options.sstableWindowSize,
                                                     Collections.max(expired, Comparator.comparing(SSTableReader::getMaxTimestamp)).getMaxTimestamp()).left;
            return CompactionAggregate.createTimeTiered(expired, timestamp);
        }

        return compactionCandidate.withExpired(expired);
    }

    private CompactionAggregate getNextNonExpiredSSTables(Iterable<SSTableReader> nonExpiringSSTables, final int gcBefore)
    {
        List<CompactionAggregate> candidates = getCompactionCandidates(nonExpiringSSTables);
        backgroundCompactions.setPending(candidates);

        CompactionAggregate ret = candidates.isEmpty() ? null : candidates.get(0);
        if (ret != null && !ret.isEmpty())
            return ret;

        // if there is no sstable to compact in standard way, try compacting single sstable whose droppable tombstone
        // ratio is greater than threshold.
        List<SSTableReader> sstablesWithTombstones = new ArrayList<>();
        for (SSTableReader sstable : nonExpiringSSTables)
        {
            if (worthDroppingTombstones(sstable, gcBefore))
                sstablesWithTombstones.add(sstable);
        }
        if (sstablesWithTombstones.isEmpty())
            return null;

        SSTableReader sstable = Collections.min(sstablesWithTombstones, SSTableReader.sizeComparator);
        return CompactionAggregate.createForTombstones(sstable);
    }

    private List<CompactionAggregate> getCompactionCandidates(Iterable<SSTableReader> candidateSSTables)
    {
        Pair<HashMultimap<Long, SSTableReader>, Long> buckets = getBuckets(candidateSSTables, options.sstableWindowUnit, options.sstableWindowSize, options.timestampResolution);
        // Update the highest window seen, if necessary
        if(buckets.right > this.highestWindowSeen)
            this.highestWindowSeen = buckets.right;

        return getBucketAggregates(buckets.left,
                                   cfs.getMinimumCompactionThreshold(),
                                   cfs.getMaximumCompactionThreshold(),
                                   options.stcsOptions,
                                   this.highestWindowSeen);
    }

    @Override
    public synchronized void addSSTable(SSTableReader sstable)
    {
        sstables.add(sstable);
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

    /**
     * Find the lowest and highest timestamps in a given timestamp/unit pair
     * Returns milliseconds, caller should adjust accordingly
     */
    public static Pair<Long,Long> getWindowBoundsInMillis(TimeUnit windowTimeUnit, int windowTimeSize, long timestampInMillis)
    {
        long lowerTimestamp;
        long upperTimestamp;
        long timestampInSeconds = TimeUnit.SECONDS.convert(timestampInMillis, TimeUnit.MILLISECONDS);

        switch(windowTimeUnit)
        {
            case MINUTES:
                lowerTimestamp = timestampInSeconds - ((timestampInSeconds) % (60L * windowTimeSize));
                upperTimestamp = (lowerTimestamp + (60L * (windowTimeSize - 1L))) + 59L;
                break;
            case HOURS:
                lowerTimestamp = timestampInSeconds - ((timestampInSeconds) % (3600L * windowTimeSize));
                upperTimestamp = (lowerTimestamp + (3600L * (windowTimeSize - 1L))) + 3599L;
                break;
            case DAYS:
            default:
                lowerTimestamp = timestampInSeconds - ((timestampInSeconds) % (86400L * windowTimeSize));
                upperTimestamp = (lowerTimestamp + (86400L * (windowTimeSize - 1L))) + 86399L;
                break;
        }

        return Pair.create(TimeUnit.MILLISECONDS.convert(lowerTimestamp, TimeUnit.SECONDS),
                           TimeUnit.MILLISECONDS.convert(upperTimestamp, TimeUnit.SECONDS));

    }

    /**
     * Group files with similar max timestamp into buckets.
     * <p/>
     * The max timestamp of each sstable is converted into the timestamp resolution and then the window bounds are
     * calculated by calling {@link #getWindowBoundsInMillis(TimeUnit, int, long)}. The sstable is added to the bucket
     * with the same lower timestamp bound. If the lower timestamp bound is higher than any other seen, then it is recorded
     * as the max timestamp seen that will be returned.
     *
     * @param files the candidate sstables
     * @param sstableWindowUnit the time unit for {@code sstableWindowSize}
     * @param sstableWindowSize the size of the time window by which sstables are grouped
     * @param timestampResolution the time unit for converting the sstable timestamp
     * @return A pair, where the left element is the bucket representation (multi-map of lower bound timestamp to sstables),
     *         and the right is the highest lower bound timestamp seen
     */
    @VisibleForTesting
    static Pair<HashMultimap<Long, SSTableReader>, Long> getBuckets(Iterable<SSTableReader> files, TimeUnit sstableWindowUnit, int sstableWindowSize, TimeUnit timestampResolution)
    {
        HashMultimap<Long, SSTableReader> buckets = HashMultimap.create();

        long maxTimestamp = 0;
        // Create hash map to represent buckets
        // For each sstable, add sstable to the time bucket
        // Where the bucket is the file's max timestamp rounded to the nearest window bucket
        for (SSTableReader f : files)
        {
            assert TimeWindowCompactionStrategyOptions.validTimestampTimeUnits.contains(timestampResolution);
            long tStamp = TimeUnit.MILLISECONDS.convert(f.getMaxTimestamp(), timestampResolution);
            Pair<Long,Long> bounds = getWindowBoundsInMillis(sstableWindowUnit, sstableWindowSize, tStamp);
            buckets.put(bounds.left, f);
            if (bounds.left > maxTimestamp)
                maxTimestamp = bounds.left;
        }

        logger.trace("buckets {}, max timestamp {}", buckets, maxTimestamp);
        return Pair.create(buckets, maxTimestamp);
    }


    /**
     * If the current bucket has at least minThreshold SSTables, choose that one. For any other bucket, at least 2 SSTables is enough.
     * In any case, limit to maxThreshold SSTables.
     *
     * @param buckets list of buckets, sorted from newest to oldest, from which to return the newest bucket within thresholds.
     * @param minThreshold minimum number of sstables in a bucket to qualify.
     * @param maxThreshold maximum number of sstables to compact at once (the returned bucket will be trimmed down to this).
     * @param stcsOptions the options for {@link SizeTieredCompactionStrategy} to be used in the newest bucket
     * @param now the latest timestamp in milliseconds
     *
     * @return a list of compaction aggregates, one per time bucket
     */
    @VisibleForTesting
    static List<CompactionAggregate> getBucketAggregates(HashMultimap<Long, SSTableReader> buckets,
                                                         int minThreshold,
                                                         int maxThreshold,
                                                         SizeTieredCompactionStrategyOptions stcsOptions,
                                                         long now)
    {
        List<CompactionAggregate> ret = new ArrayList<>(buckets.size());
        boolean nextCompactionFound = false; // set to true once the first bucket with a compaction is found

        TreeSet<Long> allKeys = new TreeSet<>(buckets.keySet());

        Iterator<Long> it = allKeys.descendingIterator();
        while(it.hasNext())
        {
            Long key = it.next();
            Set<SSTableReader> bucket = buckets.get(key);
            logger.trace("Key {}, now {}", key, now);

            CompactionPick selected = CompactionPick.EMPTY;
            List<CompactionPick> pending = new ArrayList<>(1);

            if (bucket.size() >= minThreshold && key >= now)
            {
                // If we're in the newest bucket, we'll use STCS to prioritize sstables
                SizeTieredCompactionStrategy.SizeTieredBuckets stcsBuckets = new SizeTieredCompactionStrategy.SizeTieredBuckets(bucket,
                                                                                                                                stcsOptions,
                                                                                                                                minThreshold,
                                                                                                                                maxThreshold);
                stcsBuckets.aggregate();

                for (CompactionAggregate stcsAggregate : stcsBuckets.getAggregates())
                {
                    if (selected.isEmpty())
                    {
                        selected = CompactionPick.create(key, stcsAggregate.getSelected());
                        for (CompactionPick comp : stcsAggregate.getActive())
                        {
                            if (comp != stcsAggregate.getSelected())
                                pending.add(comp);
                        }
                    }
                    else
                    {
                        pending.addAll(stcsAggregate.getActive());
                    }
                }

                if (!selected.isEmpty())
                    logger.debug("Newest window has STCS compaction candidates, {}, data files {} , options {}",
                                 nextCompactionFound ? "eligible but not selected due to prior candidate" : "will be selected for compaction",
                                 stcsBuckets.pairs(),
                                 stcsOptions);
                else
                    logger.debug("No STCS compactions found for first window, data files {}, options {}", stcsBuckets.pairs(), stcsOptions);

                if (!nextCompactionFound && !selected.isEmpty())
                {
                    nextCompactionFound = true;
                    ret.add(0, CompactionAggregate.createTimeTiered(bucket, selected, pending, key)); // the first one will be submitted for compaction
                }
                else
                {
                    ret.add(CompactionAggregate.createTimeTiered(bucket, selected, pending, key));
                }
            }



            else if (bucket.size() >= 2 && key < now)
            {
                List<SSTableReader> sstables = new ArrayList<>(bucket);

                // Sort the largest sstables off the end before splitting by maxThreshold
                Collections.sort(sstables, SSTableReader.sizeComparator);

                int i = 0;
                while ((bucket.size() - i) >= 2)
                {
                    List<SSTableReader> pick = sstables.subList(i, i + Math.min(bucket.size() - i, maxThreshold));
                    if (selected.isEmpty())
                        selected = CompactionPick.create(key, pick);
                    else
                        pending.add(CompactionPick.create(key, pick));

                    i += pick.size();
                }

                if (!nextCompactionFound)
                {
                    logger.debug("bucket size {} >= 2 and not in current bucket, compacting what's here: {}", bucket.size(), bucket);
                    nextCompactionFound = true;
                    ret.add(0, CompactionAggregate.createTimeTiered(bucket, selected, pending, key)); // the first one will be submitted for compaction
                }
                else
                {
                    logger.trace("bucket size {} >= 2 and not in current bucket, eligible but not selected: {}", bucket.size(), bucket);
                    ret.add(CompactionAggregate.createTimeTiered(bucket, selected, pending, key));
                }
            }
            else
            {
                logger.trace("No compaction necessary for bucket size {} , key {}, now {}", bucket.size(), key, now);
                ret.add(CompactionAggregate.createTimeTiered(bucket, selected, pending, key)); // add an empty aggregate anyway so we get a full view
            }
        }
        return ret;
    }

    /**
     * @param bucket set of sstables
     * @param maxThreshold maximum number of sstables in a single compaction task.
     * @return A bucket trimmed to the maxThreshold newest sstables.
     */
    @VisibleForTesting
    static List<SSTableReader> trimToThreshold(Set<SSTableReader> bucket, int maxThreshold)
    {
        List<SSTableReader> ssTableReaders = new ArrayList<>(bucket);

        // Trim the largest sstables off the end to meet the maxThreshold
        Collections.sort(ssTableReaders, SSTableReader.sizeComparator);

        return ImmutableList.copyOf(Iterables.limit(ssTableReaders, maxThreshold));
    }

    @Override
    @SuppressWarnings("resource") // transaction is closed by AbstractCompactionTask::execute
    public synchronized Collection<AbstractCompactionTask> getMaximalTask(int gcBefore, boolean splitOutput)
    {
        Iterable<SSTableReader> filteredSSTables = filterSuspectSSTables(sstables);
        if (Iterables.isEmpty(filteredSSTables))
            return null;
        LifecycleTransaction txn = cfs.getTracker().tryModify(filteredSSTables, OperationType.COMPACTION);
        if (txn == null)
            return null;
        return Collections.singleton(CompactionTask.forTimeWindowCompaction(this, txn, gcBefore));
    }

    @Override
    @SuppressWarnings("resource") // transaction is closed by AbstractCompactionTask::execute
    public synchronized AbstractCompactionTask getUserDefinedTask(Collection<SSTableReader> sstables, int gcBefore)
    {
        assert !sstables.isEmpty(); // checked for by CM.submitUserDefined

        LifecycleTransaction modifier = cfs.getTracker().tryModify(sstables, OperationType.COMPACTION);
        if (modifier == null)
        {
            logger.debug("Unable to mark {} for compaction; probably a background compaction got to it first.  You can disable background compactions temporarily if this is a problem", sstables);
            return null;
        }

        return CompactionTask.forTimeWindowCompaction(this, modifier, gcBefore).setUserDefined(true);
    }

    boolean ignoreOverlaps()
    {
        return options.ignoreOverlaps;
    }

    public long getMaxSSTableBytes()
    {
        return Long.MAX_VALUE;
    }


    public static Map<String, String> validateOptions(Map<String, String> options) throws ConfigurationException
    {
        Map<String, String> uncheckedOptions = AbstractCompactionStrategy.validateOptions(options);
        uncheckedOptions = TimeWindowCompactionStrategyOptions.validateOptions(options, uncheckedOptions);

        uncheckedOptions.remove(CompactionParams.Option.MIN_THRESHOLD.toString());
        uncheckedOptions.remove(CompactionParams.Option.MAX_THRESHOLD.toString());

        return uncheckedOptions;
    }

    public String toString()
    {
        return String.format("TimeWindowCompactionStrategy[%s/%s]",
                cfs.getMinimumCompactionThreshold(),
                cfs.getMaximumCompactionThreshold());
    }
}
