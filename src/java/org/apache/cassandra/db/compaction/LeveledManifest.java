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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.io.sstable.Component;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.service.StorageService;

import static org.apache.cassandra.db.compaction.LeveledGenerations.MAX_LEVEL_COUNT;

public class LeveledManifest
{
    private static final Logger logger = LoggerFactory.getLogger(LeveledManifest.class);

    /**
     * if we have more than MAX_COMPACTING_L0 sstables in L0, we will run a round of STCS with at most
     * cfs.getMaxCompactionThreshold() sstables.
     */
    @VisibleForTesting
    static final int MAX_COMPACTING_L0 = 32;

    /**
     * The maximum number of sstables in L0 for calculating the maximum number of bytes in L0.
     */
    static final int MAX_SSTABLES_L0 = 4;

    /**
     * If we go this many rounds without compacting
     * in the highest level, we start bringing in sstables from
     * that level into lower level compactions
     */
    private static final int NO_COMPACTION_LIMIT = 25;

    private final CompactionRealm realm;

    private final LeveledGenerations generations;

    private final CompactionSSTable[] lastCompactedSSTables;
    private final long maxSSTableSizeInBytes;
    private final SizeTieredCompactionStrategyOptions options;
    private final int [] compactionCounter;
    private final int levelFanoutSize;

    LeveledManifest(CompactionRealm realm, int maxSSTableSizeInMB, int fanoutSize, SizeTieredCompactionStrategyOptions options)
    {
        this.realm = realm;
        this.maxSSTableSizeInBytes = maxSSTableSizeInMB * 1024L * 1024L;
        this.options = options;
        this.levelFanoutSize = fanoutSize;

        lastCompactedSSTables = new CompactionSSTable[MAX_LEVEL_COUNT];
        generations = new LeveledGenerations();
        compactionCounter = new int[MAX_LEVEL_COUNT];
    }

    public static LeveledManifest create(CompactionRealm realm, int maxSSTableSize, int fanoutSize, List<CompactionSSTable> sstables)
    {
        return create(realm, maxSSTableSize, fanoutSize, sstables, new SizeTieredCompactionStrategyOptions());
    }

    public static LeveledManifest create(CompactionRealm realm, int maxSSTableSize, int fanoutSize, Iterable<CompactionSSTable> sstables, SizeTieredCompactionStrategyOptions options)
    {
        LeveledManifest manifest = new LeveledManifest(realm, maxSSTableSize, fanoutSize, options);

        // ensure all SSTables are in the manifest
        manifest.addSSTables(sstables);
        manifest.calculateLastCompactedKeys();
        return manifest;
    }

    /**
     * If we want to start compaction in level n, find the newest (by modification time) file in level n+1
     * and use its last token for last compacted key in level n;
     */
    void calculateLastCompactedKeys()
    {
        for (int i = 0; i < generations.levelCount() - 1; i++)
        {
            Set<CompactionSSTable> level = generations.get(i + 1);
            // this level is empty
            if (level.isEmpty())
                continue;

            CompactionSSTable sstableWithMaxModificationTime = null;
            long maxModificationTime = Long.MIN_VALUE;
            for (CompactionSSTable ssTableReader : level)
            {
                long modificationTime = ssTableReader.getCreationTimeFor(Component.DATA);
                if (modificationTime >= maxModificationTime)
                {
                    sstableWithMaxModificationTime = ssTableReader;
                    maxModificationTime = modificationTime;
                }
            }

            lastCompactedSSTables[i] = sstableWithMaxModificationTime;
        }
    }

    public synchronized void addSSTables(Iterable<? extends CompactionSSTable> readers)
    {
        generations.addAll(readers);
    }

    public synchronized void replace(Collection<CompactionSSTable> removed, Collection<CompactionSSTable> added)
    {
        assert !removed.isEmpty(); // use add() instead of promote when adding new sstables
        if (logger.isTraceEnabled())
        {
            generations.logDistribution();
            logger.trace("Replacing [{}]", toString(removed));
        }

        // the level for the added sstables is the max of the removed ones,
        // plus one if the removed were all on the same level
        int minLevel = generations.remove(removed);

        // it's valid to do a remove w/o an add (e.g. on truncate)
        if (added.isEmpty())
            return;

        if (logger.isTraceEnabled())
            logger.trace("Adding [{}]", toString(added));
        generations.addAll(added);
        lastCompactedSSTables[minLevel] = CompactionSSTable.firstKeyOrdering.max(added);
    }

    /**
     * See {@link AbstractCompactionStrategy#removeDeadSSTables}
     */
    public synchronized void removeDeadSSTables()
    {
        int removed = 0;
        Set<? extends CompactionSSTable> liveSet = realm.getLiveSSTables();

        for (int i = 0; i < generations.levelCount(); i++)
        {
            Iterator<CompactionSSTable> it = generations.get(i).iterator();
            while (it.hasNext())
            {
                CompactionSSTable sstable = it.next();
                if (!liveSet.contains(sstable))
                {
                    it.remove();
                    ++removed;
                }
            }
        }

        if (removed > 0)
            logger.debug("Removed {} dead sstables from the compactions tracked list.", removed);
    }

    private String toString(Collection<? extends CompactionSSTable> sstables)
    {
        StringBuilder builder = new StringBuilder();
        for (CompactionSSTable sstable : sstables)
        {
            builder.append(sstable.getColumnFamilyName())
                   .append('-')
                   .append(sstable.getId())
                   .append("(L")
                   .append(sstable.getSSTableLevel())
                   .append("), ");
        }
        return builder.toString();
    }

    public long maxBytesForLevel(int level, long maxSSTableSizeInBytes)
    {
        return maxBytesForLevel(level, levelFanoutSize, maxSSTableSizeInBytes);
    }

    public static long maxBytesForLevel(int level, int levelFanoutSize, long maxSSTableSizeInBytes)
    {
        if (level == 0)
            return MAX_SSTABLES_L0 * maxSSTableSizeInBytes;
        double bytes = Math.pow(levelFanoutSize, level) * maxSSTableSizeInBytes;
        if (bytes > Long.MAX_VALUE)
            throw new RuntimeException("At most " + Long.MAX_VALUE + " bytes may be in a compaction level; your maxSSTableSize must be absurdly high to compute " + bytes);
        return (long) bytes;
    }

    /**
     * @return highest-priority sstables to compact, and level to compact them to
     * If no compactions are necessary, will return null
     */
    synchronized CompactionAggregate.Leveled getCompactionCandidate()
    {
        // during bootstrap we only do size tiering in L0 to make sure
        // the streamed files can be placed in their original levels
        if (StorageService.instance.isBootstrapMode())
        {
            CompactionPick mostInteresting = getSSTablesForSTCS(generations.get(0));
            if (!mostInteresting.isEmpty())
            {
                logger.info("Bootstrapping - doing STCS in L0");
                return getSTCSAggregate(mostInteresting);
            }
            return null;
        }
        // LevelDB gives each level a score of how much data it contains vs its ideal amount, and
        // compacts the level with the highest score. But this falls apart spectacularly once you
        // get behind.  Consider this set of levels:
        // L0: 988 [ideal: 4]
        // L1: 117 [ideal: 10]
        // L2: 12  [ideal: 100]
        //
        // The problem is that L0 has a much higher score (almost 250) than L1 (11), so what we'll
        // do is compact a batch of cfs.getMaximumCompactionThreshold() sstables with all 117 L1 sstables, and put the
        // result (say, 120 sstables) in L1. Then we'll compact the next batch of cfs.getMaxCompactionThreshold(),
        // and so forth.  So we spend most of our i/o rewriting the L1 data with each batch.
        //
        // If we could just do *all* L0 a single time with L1, that would be ideal.  But we can't
        // since we might run out of memory
        //
        // LevelDB's way around this is to simply block writes if L0 compaction falls behind.
        // We don't have that luxury.
        //
        // So instead, we
        // 1) force compacting higher levels first, which minimizes the i/o needed to compact
        //    optimially which gives us a long term win, and
        // 2) if L0 falls behind, we will size-tiered compact it to reduce read overhead until
        //    we can catch up on the higher levels.
        //
        // This isn't a magic wand -- if you are consistently writing too fast for LCS to keep
        // up, you're still screwed.  But if instead you have intermittent bursts of activity,
        // it can help a lot.

        // Let's check that L0 is far enough behind to warrant STCS.
        // If it is, it will be used before proceeding any of higher level
        CompactionAggregate.Leveled l0Compactions = getSTCSInL0CompactionCandidate();

        for (int i = generations.levelCount() - 1; i > 0; i--)
        {
            Set<CompactionSSTable> sstables = generations.get(i);
            if (sstables.isEmpty())
                continue; // mostly this just avoids polluting the debug log with zero scores
            // we want to calculate score excluding compacting ones
            Set<CompactionSSTable> sstablesInLevel = Sets.newHashSet(sstables);
            Set<CompactionSSTable> remaining = Sets.difference(sstablesInLevel, realm.getCompactingSSTables());
            long remainingBytesForLevel = CompactionSSTable.getTotalDataBytes(remaining);
            long maxBytesForLevel = maxBytesForLevel(i, maxSSTableSizeInBytes);
            double score = (double) remainingBytesForLevel / (double) maxBytesForLevel;
            logger.trace("Compaction score for level {} is {}", i, score);

            if (score > 1.001)
            {
                // the highest level should not ever exceed its maximum size under normal curcumstaces,
                // but if it happens we warn about it
                if (i == generations.levelCount() - 1)
                {
                    logger.warn("L" + i + " (maximum supported level) has " + remainingBytesForLevel + " bytes while "
                            + "its maximum size is supposed to be " + maxBytesForLevel + " bytes");
                    continue;
                }

                // before proceeding with a higher level, let's see if L0 is far enough behind to warrant STCS
                if (l0Compactions != null)
                    return l0Compactions;

                // L0 is fine, proceed with this level
                Collection<CompactionSSTable> candidates = getCandidatesFor(i);
                int pendingCompactions = Math.max(0, getEstimatedPendingTasks(i) - 1);

                if (!candidates.isEmpty())
                {
                    int nextLevel = getNextLevel(candidates);
                    candidates = getOverlappingStarvedSSTables(nextLevel, candidates);
                    if (logger.isTraceEnabled())
                        logger.trace("Compaction candidates for L{} are {}", i, toString(candidates));
                    return CompactionAggregate.createLeveled(sstablesInLevel, candidates, pendingCompactions, maxSSTableSizeInBytes, i, nextLevel, score, levelFanoutSize);
                }
                else
                {
                    logger.trace("No compaction candidates for L{}", i);
                }
            }
        }

        // Higher levels are happy, time for a standard, non-STCS L0 compaction
        Set<CompactionSSTable> sstables = getLevel(0);

        if (sstables.isEmpty())
            return null;
        Collection<CompactionSSTable> candidates = getCandidatesFor(0);
        if (candidates.isEmpty())
        {
            // Since we don't have any other compactions to do, see if there is a STCS compaction to perform in L0; if
            // there is a long running compaction, we want to make sure that we continue to keep the number of SSTables
            // small in L0.
            return l0Compactions;
        }
        double l0Score = (double) CompactionSSTable.getTotalDataBytes(sstables) / (double) maxBytesForLevel(0, maxSSTableSizeInBytes);
        int l0PendingCompactions = Math.max(0, getEstimatedPendingTasks(0) - 1);
        return CompactionAggregate.createLeveled(sstables, candidates, l0PendingCompactions, maxSSTableSizeInBytes, 0, getNextLevel(candidates), l0Score, levelFanoutSize);
    }

    private CompactionAggregate.Leveled getSTCSInL0CompactionCandidate()
    {
        if (!DatabaseDescriptor.getDisableSTCSInL0() && generations.get(0).size() > MAX_COMPACTING_L0)
        {
            CompactionPick mostInteresting = getSSTablesForSTCS(getLevel(0));
            if (!mostInteresting.isEmpty())
            {
                logger.debug("L0 is too far behind, performing size-tiering there first");
                return getSTCSAggregate(mostInteresting);
            }
        }

        return null;
    }

    private CompactionAggregate.Leveled getSTCSAggregate(CompactionPick compaction)
    {
        Set<CompactionSSTable> sstables = getLevel(0);
        double score = (double) CompactionSSTable.getTotalDataBytes(sstables) / (double) maxBytesForLevel(0, maxSSTableSizeInBytes);
        int remainingSSTables = sstables.size() - compaction.sstables().size();
        int pendingTasks = remainingSSTables > realm.getMinimumCompactionThreshold()
                           ? (int) Math.ceil(remainingSSTables / realm.getMaximumCompactionThreshold())
                           : 0;
        return CompactionAggregate.createLeveledForSTCS(sstables, compaction, pendingTasks, score, levelFanoutSize);
    }

    private CompactionPick getSSTablesForSTCS(Collection<CompactionSSTable> sstables)
    {
        Iterable<? extends CompactionSSTable> candidates = realm.getNoncompactingSSTables(sstables);

        SizeTieredCompactionStrategy.SizeTieredBuckets sizeTieredBuckets;
        sizeTieredBuckets = new SizeTieredCompactionStrategy.SizeTieredBuckets(candidates,
                                                                               options,
                                                                               realm.getMinimumCompactionThreshold(),
                                                                               realm.getMaximumCompactionThreshold());
        sizeTieredBuckets.aggregate();

        return CompactionAggregate.getSelected(sizeTieredBuckets.getAggregates());
    }

    /**
     * If we do something that makes many levels contain too little data (cleanup, change sstable size) we will "never"
     * compact the high levels.
     *
     * This method finds if we have gone many compaction rounds without doing any high-level compaction, if so
     * we start bringing in one sstable from the highest level until that level is either empty or is doing compaction.
     *
     * @param targetLevel the level the candidates will be compacted into
     * @param candidates the original sstables to compact
     * @return
     */
    private Collection<CompactionSSTable> getOverlappingStarvedSSTables(int targetLevel, Collection<CompactionSSTable> candidates)
    {
        Set<CompactionSSTable> withStarvedCandidate = new HashSet<>(candidates);

        for (int i = generations.levelCount() - 1; i > 0; i--)
            compactionCounter[i]++;
        compactionCounter[targetLevel] = 0;
        if (logger.isTraceEnabled())
        {
            for (int j = 0; j < compactionCounter.length; j++)
                logger.trace("CompactionCounter: {}: {}", j, compactionCounter[j]);
        }

        for (int i = generations.levelCount() - 1; i > 0; i--)
        {
            if (getLevelSize(i) > 0)
            {
                if (compactionCounter[i] > NO_COMPACTION_LIMIT)
                {
                    // we try to find an sstable that is fully contained within  the boundaries we are compacting;
                    // say we are compacting 3 sstables: 0->30 in L1 and 0->12, 12->33 in L2
                    // this means that we will not create overlap in L2 if we add an sstable
                    // contained within 0 -> 33 to the compaction
                    PartitionPosition max = null;
                    PartitionPosition min = null;
                    for (CompactionSSTable candidate : candidates)
                    {
                        if (min == null || candidate.getFirst().compareTo(min) < 0)
                            min = candidate.getFirst();
                        if (max == null || candidate.getLast().compareTo(max) > 0)
                            max = candidate.getLast();
                    }
                    if (min == null || max == null || min.equals(max)) // single partition sstables - we cannot include a high level sstable.
                        return candidates;
                    Set<? extends CompactionSSTable> compacting = realm.getCompactingSSTables();
                    Range<PartitionPosition> boundaries = new Range<>(min, max);
                    for (CompactionSSTable sstable : generations.get(i))
                    {
                        Range<PartitionPosition> r = new Range<>(sstable.getFirst(), sstable.getLast());
                        if (boundaries.contains(r) && !compacting.contains(sstable))
                        {
                            logger.info("Adding high-level (L{}) {} to candidates", sstable.getSSTableLevel(), sstable);
                            withStarvedCandidate.add(sstable);
                            return withStarvedCandidate;
                        }
                    }
                }
                return candidates;
            }
        }

        return candidates;
    }

    public synchronized int getLevelSize(int i)
    {
        return generations.get(i).size();
    }

    public synchronized int[] getSSTableCountPerLevel()
    {
        int[] counts = new int[getLevelCount()];
        for (int i = 0; i < counts.length; i++)
            counts[i] = getLevel(i).size();
        return counts;
    }

    public synchronized int[] getAllLevelSize()
    {
        return generations.getAllLevelSize();
    }

    @VisibleForTesting
    public synchronized int remove(CompactionSSTable reader)
    {
        int level = reader.getSSTableLevel();
        assert level >= 0 : reader + " not present in manifest: "+level;
        generations.remove(Collections.singleton(reader));
        return level;
    }

    public synchronized Set<CompactionSSTable> getSSTables()
    {
        return generations.allSSTables();
    }

    private static Set<CompactionSSTable> overlapping(Collection<CompactionSSTable> candidates, Iterable<CompactionSSTable> others)
    {
        assert !candidates.isEmpty();
        /*
         * Picking each sstable from others that overlap one of the sstable of candidates is not enough
         * because you could have the following situation:
         *   candidates = [ s1(a, c), s2(m, z) ]
         *   others = [ s3(e, g) ]
         * In that case, s2 overlaps none of s1 or s2, but if we compact s1 with s2, the resulting sstable will
         * overlap s3, so we must return s3.
         *
         * Thus, the correct approach is to pick sstables overlapping anything between the first key in all
         * the candidate sstables, and the last.
         */
        Iterator<CompactionSSTable> iter = candidates.iterator();
        CompactionSSTable sstable = iter.next();
        Token first = sstable.getFirst().getToken();
        Token last = sstable.getLast().getToken();
        while (iter.hasNext())
        {
            sstable = iter.next();
            first = first.compareTo(sstable.getFirst().getToken()) <= 0 ? first : sstable.getFirst().getToken();
            last = last.compareTo(sstable.getLast().getToken()) >= 0 ? last : sstable.getLast().getToken();
        }
        return overlapping(first, last, others);
    }

    static Set<CompactionSSTable> overlappingWithBounds(CompactionSSTable sstable, Map<CompactionSSTable, Bounds<Token>> others)
    {
        return overlappingWithBounds(sstable.getFirst().getToken(), sstable.getLast().getToken(), others);
    }

    /**
     * @return sstables from @param sstables that contain keys between @param start and @param end, inclusive.
     */
    @VisibleForTesting
    static Set<CompactionSSTable> overlapping(Token start, Token end, Iterable<CompactionSSTable> sstables)
    {
        return overlappingWithBounds(start, end, genBounds(sstables));
    }

    private static Set<CompactionSSTable> overlappingWithBounds(Token start, Token end, Map<CompactionSSTable, Bounds<Token>> sstables)
    {
        assert start.compareTo(end) <= 0;
        Set<CompactionSSTable> overlapped = new HashSet<>();
        Bounds<Token> promotedBounds = new Bounds<>(start, end);

        for (Map.Entry<CompactionSSTable, Bounds<Token>> pair : sstables.entrySet())
        {
            if (pair.getValue().intersects(promotedBounds))
                overlapped.add(pair.getKey());
        }
        return overlapped;
    }

    @VisibleForTesting
    static Map<CompactionSSTable, Bounds<Token>> genBounds(Iterable<? extends CompactionSSTable> ssTableReaders)
    {
        Map<CompactionSSTable, Bounds<Token>> boundsMap = new HashMap<>();
        for (CompactionSSTable sstable : ssTableReaders)
        {
            boundsMap.put(sstable, new Bounds<>(sstable.getFirst().getToken(), sstable.getLast().getToken()));
        }
        return boundsMap;
    }

    /**
     * Determine the highest-priority sstables to compact for the given level and add any overlapping sstables
     * from the next level.
     * <p/>
     * @return highest-priority sstables to compact for the given level.
     * If no compactions are possible (because of concurrent compactions or because some sstables are excluded
     * for prior failure), will return an empty list.  Never returns null.
     *
     * @param level the level number
     * @return highest-priority sstables to compact for the given level.
     */
    private Collection<CompactionSSTable> getCandidatesFor(int level)
    {
        assert !generations.get(level).isEmpty();
        logger.trace("Choosing candidates for L{}", level);

        final Set<? extends CompactionSSTable> compacting = realm.getCompactingSSTables();

        if (level == 0)
        {
            Set<CompactionSSTable> compactingL0 = getCompactingL0();

            PartitionPosition lastCompactingKey = null;
            PartitionPosition firstCompactingKey = null;
            for (CompactionSSTable candidate : compactingL0)
            {
                if (firstCompactingKey == null || candidate.getFirst().compareTo(firstCompactingKey) < 0)
                    firstCompactingKey = candidate.getFirst();
                if (lastCompactingKey == null || candidate.getLast().compareTo(lastCompactingKey) > 0)
                    lastCompactingKey = candidate.getLast();
            }

            // L0 is the dumping ground for new sstables which thus may overlap each other.
            //
            // We treat L0 compactions specially:
            // 1a. add sstables to the candidate set until we have at least maxSSTableSizeInMB
            // 1b. prefer choosing older sstables as candidates, to newer ones
            // 1c. any L0 sstables that overlap a candidate, will also become candidates
            // 2. At most max_threshold sstables from L0 will be compacted at once
            // 3. If total candidate size is less than maxSSTableSizeInMB, we won't bother compacting with L1,
            //    and the result of the compaction will stay in L0 instead of being promoted (see promote())
            //
            // Note that we ignore suspect-ness of L1 sstables here, since if an L1 sstable is suspect we're
            // basically screwed, since we expect all or most L0 sstables to overlap with each L1 sstable.
            // So if an L1 sstable is suspect we can't do much besides try anyway and hope for the best.
            Set<CompactionSSTable> candidates = new HashSet<>();
            Map<CompactionSSTable, Bounds<Token>> remaining = genBounds(Iterables.filter(generations.get(0), Predicates.not(CompactionSSTable::isMarkedSuspect)));

            for (CompactionSSTable sstable : ageSortedSSTables(remaining.keySet()))
            {
                if (candidates.contains(sstable))
                    continue;

                Sets.SetView<CompactionSSTable> overlappedL0 = Sets.union(Collections.singleton(sstable), overlappingWithBounds(sstable, remaining));
                if (!Sets.intersection(overlappedL0, compactingL0).isEmpty())
                    continue;

                for (CompactionSSTable newCandidate : overlappedL0)
                {
                    if (firstCompactingKey == null || lastCompactingKey == null || overlapping(firstCompactingKey.getToken(), lastCompactingKey.getToken(), Collections.singleton(newCandidate)).size() == 0)
                        candidates.add(newCandidate);
                    remaining.remove(newCandidate);
                }

                if (candidates.size() > realm.getMaximumCompactionThreshold())
                {
                    // limit to only the cfs.getMaximumCompactionThreshold() oldest candidates
                    candidates = new HashSet<>(ageSortedSSTables(candidates).subList(0, realm.getMaximumCompactionThreshold()));
                    break;
                }
            }

            // leave everything in L0 if we didn't end up with a full sstable's worth of data
            if (CompactionSSTable.getTotalDataBytes(candidates) > maxSSTableSizeInBytes)
            {
                // add sstables from L1 that overlap candidates
                // if the overlapping ones are already busy in a compaction, leave it out.
                // TODO try to find a set of L0 sstables that only overlaps with non-busy L1 sstables
                Set<CompactionSSTable> l1overlapping = overlapping(candidates, generations.get(1));
                if (Sets.intersection(l1overlapping, compacting).size() > 0)
                    return Collections.emptyList();
                if (!overlapping(candidates, compactingL0).isEmpty())
                    return Collections.emptyList();
                candidates = Sets.union(candidates, l1overlapping);
            }
            if (candidates.size() < 2)
                return Collections.emptyList();
            else
                return candidates;
        }

        // look for a non-suspect keyspace to compact with, starting with where we left off last time,
        // and wrapping back to the beginning of the generation if necessary
        Map<CompactionSSTable, Bounds<Token>> sstablesNextLevel = genBounds(generations.get(level + 1));
        Iterator<CompactionSSTable> levelIterator = generations.wrappingIterator(level, lastCompactedSSTables[level]);
        while (levelIterator.hasNext())
        {
            CompactionSSTable sstable = levelIterator.next();
            Set<CompactionSSTable> candidates = Sets.union(Collections.singleton(sstable), overlappingWithBounds(sstable, sstablesNextLevel));

            if (Iterables.any(candidates, CompactionSSTable::isMarkedSuspect))
                continue;
            if (Sets.intersection(candidates, compacting).isEmpty())
                return candidates;
        }

        // all the sstables were suspect or overlapped with something suspect
        return Collections.emptyList();
    }

    private Set<CompactionSSTable> getCompactingL0()
    {
        Set<CompactionSSTable> sstables = new HashSet<>();
        Set<CompactionSSTable> levelSSTables = new HashSet<>(generations.get(0));
        for (CompactionSSTable sstable : realm.getCompactingSSTables())
        {
            if (levelSSTables.contains(sstable))
                sstables.add(sstable);
        }
        return sstables;
    }

    @VisibleForTesting
    List<CompactionSSTable> ageSortedSSTables(Collection<CompactionSSTable> candidates)
    {
        return ImmutableList.sortedCopyOf(CompactionSSTable.maxTimestampAscending, candidates);
    }

    public synchronized Set<CompactionSSTable>[] getSStablesPerLevelSnapshot()
    {
        return generations.snapshot();
    }

    @Override
    public String toString()
    {
        return "Manifest@" + hashCode();
    }

    public synchronized int getLevelCount()
    {
        for (int i = generations.levelCount() - 1; i >= 0; i--)
        {
            if (generations.get(i).size() > 0)
                return i;
        }
        return 0;
    }

    public synchronized List<CompactionAggregate> getEstimatedTasks(CompactionAggregate.Leveled selected)
    {
        List<CompactionAggregate> ret = new ArrayList<>(generations.levelCount());

        for (int i = generations.levelCount() - 1; i >= 0; i--)
        {
            Set<CompactionSSTable> sstables = generations.get(i);

            // do not log high levels that are empty, only log after we've found a non-empty level
            if (sstables.isEmpty() && ret.isEmpty())
                continue;

            if (selected != null && selected.level == i)
            {
                ret.add(selected);
                continue; // pending tasks already calculated by getCompactionCandidate()
            }

            if (i == 0)
            { // for L0 if it is too far behind then pick the STCS choice
                CompactionAggregate l0Compactions = getSTCSInL0CompactionCandidate();
                if (l0Compactions != null)
                {
                    ret.add(l0Compactions);
                    continue;
                }
            }

            int pendingTasks = getEstimatedPendingTasks(i);
            double score = (double) CompactionSSTable.getTotalDataBytes(sstables) / (double) maxBytesForLevel(i, maxSSTableSizeInBytes);
            ret.add(CompactionAggregate.createLeveled(sstables, pendingTasks, maxSSTableSizeInBytes, i, score, levelFanoutSize));
        }

        logger.trace("Estimating {} compactions to do for {}", ret.size(), realm.metadata());
        return ret;
    }

    /**
     * @return the estimated number of LCS compactions for a given level with the given sstables. Because it compacts one sstable at
     *         a time, this number is determined as the number of bytes above the maximum divided the maximum sstable size in bytes.
     *
     *         This is however incorrect for L0. If the STCS threshold has been exceeded, we simply divide by the max threshold,
     *         otherwise we currently use a very pessimistic estimate (no overlapping sstables).
     */
    private int getEstimatedPendingTasks(int level)
    {
        final Set<CompactionSSTable> sstables = getLevel(level);
        if (sstables.isEmpty())
            return 0;

        final Set<? extends CompactionSSTable> compacting = realm.getCompactingSSTables();
        final Set<CompactionSSTable> remaining = Sets.difference(Sets.newHashSet(sstables), compacting);

        if (level == 0 && !DatabaseDescriptor.getDisableSTCSInL0() && remaining.size() > MAX_COMPACTING_L0)
            return remaining.size() / realm.getMaximumCompactionThreshold();

        // If there is 1 byte over TBL - (MBL * 1.001), there is still a task left, so we need to round up.
        return Math.toIntExact((long) Math.ceil((Math.max(0L, CompactionSSTable.getTotalDataBytes(remaining) -
                                                              (maxBytesForLevel(level, maxSSTableSizeInBytes) * 1.001)) / (double) maxSSTableSizeInBytes)));
    }

    int getNextLevel(Collection<CompactionSSTable> sstables)
    {
        int maximumLevel = Integer.MIN_VALUE;
        int minimumLevel = Integer.MAX_VALUE;
        for (CompactionSSTable sstable : sstables)
        {
            maximumLevel = Math.max(sstable.getSSTableLevel(), maximumLevel);
            minimumLevel = Math.min(sstable.getSSTableLevel(), minimumLevel);
        }

        int newLevel;
        if (minimumLevel == 0 && minimumLevel == maximumLevel && CompactionSSTable.getTotalDataBytes(sstables) < maxSSTableSizeInBytes)
        {
            newLevel = 0;
        }
        else
        {
            newLevel = minimumLevel == maximumLevel ? maximumLevel + 1 : maximumLevel;
            assert newLevel > 0;
        }
        return newLevel;
    }

    synchronized Set<CompactionSSTable> getLevel(int level)
    {
        return ImmutableSet.copyOf(generations.get(level));
    }

    synchronized List<CompactionSSTable> getLevelSorted(int level, Comparator<CompactionSSTable> comparator)
    {
        return ImmutableList.sortedCopyOf(comparator, generations.get(level));
    }

    synchronized void newLevel(CompactionSSTable sstable, int oldLevel)
    {
        generations.newLevel(sstable, oldLevel);
        lastCompactedSSTables[oldLevel] = sstable;
    }
}
