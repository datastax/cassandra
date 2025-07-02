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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.ObjIntConsumer;

import org.apache.cassandra.db.DiskBoundaries;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.SortedLocalRanges;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.SortingIterator;

public interface ShardManager
{
    /// Single-partition, and generally sstables with very few partitions, can cover very small sections of the token
    /// space, resulting in very high densities.
    ///
    /// When the number of partitions in an sstable is smaller than this threshold, we will use a per-partition minimum
    /// span, calculated from the total number of partitions in this table.
    long PER_PARTITION_SPAN_THRESHOLD = 100;

    /// Additionally, sstables that have completely fallen outside the local token ranges will end up with a zero
    /// coverage.
    ///
    /// To avoid problems with this we check if coverage is below the minimum, and replace it using the per-partition
    /// calculation.
    double MINIMUM_TOKEN_COVERAGE = Math.scalb(1.0, -48);

    static ShardManager create(DiskBoundaries diskBoundaries, AbstractReplicationStrategy rs, boolean isReplicaAware)
    {
        List<Token> diskPositions = diskBoundaries.getPositions();

        SortedLocalRanges localRanges = diskBoundaries.getLocalRanges();
        IPartitioner partitioner = localRanges.getRealm().getPartitioner();
        // this should only happen in tests that change partitioners, but we don't want UCS to throw
        // where other strategies work even if the situations are unrealistic.
        if (localRanges.getRanges().isEmpty() || !localRanges.getRanges()
                                                             .get(0)
                                                             .range()
                                                             .left
                                                             .getPartitioner()
                                                             .equals(localRanges.getRealm().getPartitioner()))
            localRanges = new SortedLocalRanges(localRanges.getRealm(),
                                                localRanges.getRingVersion(),
                                                null);


        if (diskPositions != null && diskPositions.size() > 1)
            return new ShardManagerDiskAware(localRanges, diskPositions);
        else if (partitioner.splitter().isPresent())
            if (isReplicaAware)
                return new ShardManagerReplicaAware(rs, localRanges.getRealm());
            else
                return new ShardManagerNoDisks(localRanges);
        else
            return new ShardManagerTrivial(partitioner);
    }

    /// The token range fraction spanned by the given range, adjusted for the local range ownership.
    double rangeSpanned(Range<Token> tableRange);

    /// The total fraction of the token space covered by the local ranges.
    double localSpaceCoverage();

    /// The fraction of the token space covered by a shard set, i.e. the space that is split in the requested number of
    /// shards.
    ///
    /// If no disks are defined, this is the same as localSpaceCoverage(). Otherwise, it is the token coverage of a disk.
    double shardSetCoverage();

    /// The minimum token space share per partition that should be assigned to sstables with small numbers of partitions
    /// or which have fallen outside the local token ranges.
    double minimumPerPartitionSpan();

    /// Construct a boundary/shard iterator for the given number of shards.
    ///
    /// If a list of the ranges for each shard is required instead, use [#getShardRanges].
    ShardTracker boundaries(int shardCount);

    static Range<Token> coveringRange(CompactionSSTable sstable)
    {
        return coveringRange(sstable.getFirst(), sstable.getLast());
    }

    static Range<Token> coveringRange(PartitionPosition first, PartitionPosition last)
    {
        // To include the token of last, the range's upper bound must be increased.
        return new Range<>(first.getToken(), last.getToken().nextValidToken());
    }


    /// Return the token space share that the given SSTable spans, excluding any non-locally owned space.
    /// Returns a positive floating-point number between 0 and 1.
    default double rangeSpanned(CompactionSSTable rdr)
    {
        double reported = rdr.tokenSpaceCoverage();

        double span;
        if (reported > 0)   // also false for NaN
            span = reported;
        else
            span = rangeSpanned(rdr.getFirst(), rdr.getLast());

        long partitionCount = rdr.estimatedKeys();
        return adjustSmallSpans(span, partitionCount);
    }

    private double adjustSmallSpans(double span, long partitionCount)
    {
        if (partitionCount >= PER_PARTITION_SPAN_THRESHOLD && span >= MINIMUM_TOKEN_COVERAGE)
            return span;

        // Too small ranges are expected to be the result of either an sstable with a very small number of partitions,
        // or falling outside the local token ranges. In these cases we apply a per-partition minimum calculated from
        // the number of partitions in the table.
        double perPartitionMinimum = Math.min(partitionCount * minimumPerPartitionSpan(), 1.0);
        return span > perPartitionMinimum ? span : perPartitionMinimum;
    }

    default double rangeSpanned(PartitionPosition first, PartitionPosition last)
    {
        return rangeSpanned(ShardManager.coveringRange(first, last));
    }

    /// Return the density of an SSTable, i.e. its size divided by the covered token space share.
    /// This is an improved measure of the compaction age of an SSTable that grows both with STCS-like full-SSTable
    /// compactions (where size grows, share is constant), LCS-like size-threshold splitting (where size is constant
    /// but share shrinks), UCS-like compactions (where size may grow and covered shards i.e. share may decrease)
    /// and can reproduce levelling structure that corresponds to all, including their mixtures.
    default double density(CompactionSSTable rdr)
    {
        return rdr.onDiskLength() / rangeSpanned(rdr);
    }

    default double density(long onDiskLength, PartitionPosition min, PartitionPosition max, long approximatePartitionCount)
    {
        double span = rangeSpanned(min, max);
        return onDiskLength / adjustSmallSpans(span, approximatePartitionCount);
    }


    /// Seggregate the given sstables into the shard ranges that intersect sstables from the collection, and call
    /// the given function on the intersecting sstable set, with access to the shard tracker from which information
    /// about the shard can be recovered.
    ///
    /// If an operationRange is given, this method restricts the collection to the given range and assumes all sstables
    /// cover at least some portion of that range.
    private <R extends CompactionSSTable> void assignSSTablesInShards(Collection<R> sstables,
                                                                      Range<Token> operationRange,
                                                                      int numShardsForDensity,
                                                                      BiConsumer<Collection<R>, ShardTracker> consumer)
    {
        var boundaries = boundaries(numShardsForDensity);
        SortingIterator<R> items = SortingIterator.create(CompactionSSTable.firstKeyComparator, sstables);
        PriorityQueue<R> active = new PriorityQueue<>(CompactionSSTable.lastKeyComparator);
        // Advance inside the range. This will add all sstables that start before the end of the covering shard.
        if (operationRange != null)
            boundaries.advanceTo(operationRange.left.nextValidToken());
        while (items.hasNext() || !active.isEmpty())
        {
            if (active.isEmpty())
            {
                boundaries.advanceTo(items.peek().getFirst().getToken());
                active.add(items.next());
            }
            Token shardEnd = boundaries.shardEnd();
            if (operationRange != null &&
                !operationRange.right.isMinimum() &&
                shardEnd != null &&
                shardEnd.compareTo(operationRange.right) >= 0)
                shardEnd = null;    // Take all remaining sstables.

            while (items.hasNext() && (shardEnd == null || items.peek().getFirst().getToken().compareTo(shardEnd) <= 0))
                active.add(items.next());

            consumer.accept(active, boundaries);

            while (!active.isEmpty() && (shardEnd == null || active.peek().getLast().getToken().compareTo(shardEnd) <= 0))
                active.poll();

            if (!active.isEmpty()) // shardEnd must be non-null (otherwise the line above exhausts all)
                boundaries.advanceTo(shardEnd.nextValidToken());
        }
    }

    /// Seggregate the given sstables into the shard ranges that intersect sstables from the collection, and call
    /// the given function on the combination of each shard index and the intersecting sstable set.
    ///
    /// If an operationRange is given, this method restricts the collection to the given range and assumes all sstables
    /// cover at least some portion of that range.
    default <R extends CompactionSSTable> void assignSSTablesToShardIndexes(Collection<R> sstables,
                                                                            Range<Token> operationRange,
                                                                            int numShardsForDensity,
                                                                            ObjIntConsumer<Collection<R>> consumer)
    {
        assignSSTablesInShards(sstables, operationRange, numShardsForDensity,
                               (rangeSSTables, boundaries) -> consumer.accept(rangeSSTables, boundaries.shardIndex()));
    }

    /// Seggregate the given sstables into the shard ranges that intersect sstables from the collection, and call
    /// the given function on the combination of each shard range and the intersecting sstable set.
    default <T, R extends CompactionSSTable> List<T> splitSSTablesInShards(Collection<R> sstables,
                                                                           int numShardsForDensity,
                                                                           BiFunction<Collection<R>, Range<Token>, T> maker)
    {
        return splitSSTablesInShards(sstables, null, numShardsForDensity, maker);
    }

    /// Seggregate the given sstables into the shard ranges that intersect sstables from the collection, and call
    /// the given function on the combination of each shard range and the intersecting sstable set.
    ///
    /// This version restricts the operation to the given token range, and assumes all sstables cover at least some
    /// portion of that range.
    default <T, R extends CompactionSSTable> List<T> splitSSTablesInShards(Collection<R> sstables,
                                                                           Range<Token> operationRange,
                                                                           int numShardsForDensity,
                                                                           BiFunction<Collection<R>, Range<Token>, T> maker)
    {
        List<T> tasks = new ArrayList<>();
        assignSSTablesInShards(sstables, operationRange, numShardsForDensity, (rangeSSTables, boundaries) -> {
            final T result = maker.apply(rangeSSTables, boundaries.shardSpan());
            if (result != null)
                tasks.add(result);
        });
        return tasks;
    }

    /// Seggregate the given sstables into the shard ranges that intersect sstables from the collection, and call
    /// the given function on the combination of each shard range and the intersecting sstable set.
    ///
    /// This version restricts the operation to the given token range (which may be null) and accepts a parallelism
    /// limit and will group shards together to fit within that limit.
    default <T, R extends CompactionSSTable> List<T> splitSSTablesInShardsLimited(Collection<R> sstables,
                                                                                  Range<Token> operationRange,
                                                                                  int numShardsForDensity,
                                                                                  int coveredShards,
                                                                                  int maxParallelism,
                                                                                  BiFunction<Collection<R>, Range<Token>, T> maker)
    {
        if (coveredShards <= maxParallelism)
            return splitSSTablesInShards(sstables, operationRange, numShardsForDensity, maker);

        var shards = splitSSTablesInShards(sstables,
                                           operationRange,
                                           numShardsForDensity,
                                           (rangeSSTables, range) -> Pair.create(Set.copyOf(rangeSSTables), range));

        return applyMaxParallelism(maxParallelism, maker, shards);
    }

    private static <T, R extends CompactionSSTable> List<T> applyMaxParallelism(int maxParallelism,
                                                                                BiFunction<Collection<R>, Range<Token>, T> maker,
                                                                                List<Pair<Set<R>, Range<Token>>> shards)
    {
        Iterator<Pair<Set<R>, Range<Token>>> iter = shards.iterator();
        List<T> tasks = new ArrayList<>(maxParallelism);
        int shardsRemaining = shards.size();
        int tasksRemaining = maxParallelism;

        if (shardsRemaining > tasksRemaining)
        {
            double totalSpan = shards.stream().map(Pair::right).mapToDouble(r -> r.left.size(r.right)).sum();
            double spanPerTask = totalSpan / maxParallelism;

            Set<R> currentSSTables = new HashSet<>();
            Token rangeStart = null;
            double currentSpan = 0;

            // While we have more shards to process than there are tasks, we need to bunch shards up into tasks.
            while (shardsRemaining > tasksRemaining)
            {
                Pair<Set<R>, Range<Token>> pair = iter.next(); // shardsRemaining counts the shards so iter can't be exhausted at this point
                Token currentStart = pair.right.left;
                Token currentEnd = pair.right.right;
                double span = currentStart.size(currentEnd);

                if (rangeStart == null)
                    rangeStart = currentStart;

                currentSSTables.addAll(pair.left);
                currentSpan += span;

                // If there is only one task remaining, we should not issue it until we are processing the last shard.
                // The latter condition is normally guaranteed, but floating point rounding has a very small chance of making the calculations wrong
                if (currentSpan >= spanPerTask && tasksRemaining > 1)
                {
                    tasks.add(maker.apply(currentSSTables, new Range<>(rangeStart, currentEnd)));
                    --tasksRemaining;
                    currentSSTables = new HashSet<>();
                    rangeStart = null;
                    currentSpan = 0;
                }
                --shardsRemaining;
            }

            // At this point there are as many tasks remaining as there are shards
            // (this includes the case of issuing a task for the last shard when only one task remains).

            // Add any already collected sstables to the next task.
            if (!currentSSTables.isEmpty())
            {
                assert shardsRemaining > 0;
                Pair<Set<R>, Range<Token>> pair = iter.next(); // shardsRemaining counts the shards so iter can't be exhausted at this point
                currentSSTables.addAll(pair.left);
                Token currentEnd = pair.right.right;
                tasks.add(maker.apply(currentSSTables, new Range<>(rangeStart, currentEnd)));
                --tasksRemaining;
                --shardsRemaining;
            }
            assert shardsRemaining == tasksRemaining : shardsRemaining + " != " + tasksRemaining;
        }

        // If we still have tasks and shards to process, produce one task for each shard.
        while (iter.hasNext())
        {
            Pair<Set<R>, Range<Token>> pair = iter.next(); // shardsRemaining counts the shards so iter can't be exhausted at this point
            tasks.add(maker.apply(pair.left, pair.right));
            --tasksRemaining;
            --shardsRemaining;
        }

        assert tasks.size() == Math.min(maxParallelism, shards.size()) : tasks.size() + " != " + maxParallelism;
        assert shardsRemaining == 0 : shardsRemaining + " != 0";
        return tasks;
    }

    /// Return the number of shards that the given range of positions (start- and end-inclusive) spans.
    default int coveredShardCount(PartitionPosition first, PartitionPosition last, int numShardsForDensity)
    {
        var boundaries = boundaries(numShardsForDensity);
        boundaries.advanceTo(first.getToken());
        int firstShard = boundaries.shardIndex();
        boundaries.advanceTo(last.getToken());
        int lastShard = boundaries.shardIndex();
        return lastShard - firstShard + 1;
    }

    /// Get the list of shard ranges for the given shard count. Useful for diagnostics and debugging.
    default List<Range<Token>> getShardRanges(int shardCount)
    {
        var boundaries = boundaries(shardCount);
        var result = new ArrayList<Range<Token>>(shardCount);
        while (true)
        {
            result.add(boundaries.shardSpan());
            if (boundaries.shardEnd() == null)
                break;
            boundaries.advanceTo(boundaries.shardEnd().nextValidToken());
        }
        return result;
    }
}
