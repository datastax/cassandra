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

package org.apache.cassandra.utils;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.stream.IntStream;

public class Overlaps
{
    /// Construct a minimal list of overlap sets, i.e. the sections of the range span when we have overlapping items,
    /// where we ensure:
    /// - non-overlapping items are never put in the same set
    /// - no item is present in non-consecutive sets
    /// - for any point where items overlap, the result includes a set listing all overlapping items
    ///
    /// For example, for inputs A[0, 4), B[2, 8), C[6, 10), D[1, 9) the result would be the sets ABD and BCD. We are not
    /// interested in the spans where A, B, or C are present on their own or in combination with D, only that there
    /// exists a set in the list that is a superset of any such combination, and that the non-overlapping A and C are
    /// never together in a set.
    ///
    /// Note that the full list of overlap sets A, AD, ABD, BD, BCD, CD, C is also an answer that satisfies the three
    /// conditions above, but it contains redundant sets (e.g. AD is already contained in ABD).
    ///
    /// @param items            A list of items to distribute in overlap sets. This is assumed to be a transient list and the method
    ///                         may modify or consume it. It is assumed that the start and end positions of an item are ordered,
    ///                         and the items are non-empty.
    /// @param startsAfter      Predicate determining if its left argument's start if fully after the right argument's end.
    ///                         This will only be used with arguments where left's start is known to be after right's start.
    ///                         It is up to the caller if this is a strict comparison -- strict (>) for end-inclusive spans
    ///                         and non-strict (>=) for end-exclusive.
    /// @param startsComparator Comparator of items' starting positions.
    /// @param endsComparator   Comparator of items' ending positions.
    /// @return List of overlap sets.
    public static <E> List<Set<E>> constructOverlapSets(Collection<E> items,
                                                        BiPredicate<E, E> startsAfter,
                                                        Comparator<E> startsComparator,
                                                        Comparator<E> endsComparator)
    {
        return constructOverlapSets(items, startsAfter, startsComparator, endsComparator,
                                    (sets, active) -> {
                                        sets.add(new HashSet<>(active));
                                        return sets;
                                    },
                                    new ArrayList<>());
    }

    /// This is the same as the method above, but only returns the size of the biggest overlap set
    ///
    /// @param items            A list of items to distribute in overlap sets. This is assumed to be a transient list and the method
    ///                         may modify or consume it. It is assumed that the start and end positions of an item are ordered,
    ///                         and the items are non-empty.
    /// @param startsAfter      Predicate determining if its left argument's start if fully after the right argument's end.
    ///                         This will only be used with arguments where left's start is known to be after right's start.
    ///                         It is up to the caller if this is a strict comparison -- strict (>) for end-inclusive spans
    ///                         and non-strict (>=) for end-exclusive.
    /// @param startsComparator Comparator of items' starting positions.
    /// @param endsComparator   Comparator of items' ending positions.
    /// @return The maximum overlap in the given set of items.
    public static <E> int maxOverlap(Collection<E> items,
                                     BiPredicate<? super E, ? super E> startsAfter,
                                     Comparator<? super E> startsComparator,
                                     Comparator<? super E> endsComparator)
    {
        return constructOverlapSets(items, startsAfter, startsComparator, endsComparator,
                                    (max, active) -> Math.max(max, active.size()), 0);
    }

    /// Construct a minimal list of overlap sets, i.e. the sections of the range span when we have overlapping items,
    /// where we ensure:
    /// - non-overlapping items are never put in the same set
    /// - no item is present in non-consecutive sets
    /// - for any point where items overlap, the result includes a set listing all overlapping items
    /// and process it with the given reducer function. Implements the methods above.
    ///
    /// For example, for inputs A[0, 4), B[2, 8), C[6, 10), D[1, 9) the result would be the sets ABD and BCD. We are not
    /// interested in the spans where A, B, or C are present on their own or in combination with D, only that there
    /// exists a set in the list that is a superset of any such combination, and that the non-overlapping A and C are
    /// never together in a set.
    ///
    /// Note that the full list of overlap sets A, AD, ABD, BD, BCD, CD, C is also an answer that satisfies the three
    /// conditions above, but it contains redundant sets (e.g. AD is already contained in ABD).
    ///
    /// @param items            A list of items to distribute in overlap sets. It is assumed that the start and end
    ///                         positions of an item are ordered, and the items are non-empty.
    /// @param startsAfter      Predicate determining if its left argument's start if fully after the right argument's end.
    ///                         This will only be used with arguments where left's start is known to be after right's start.
    ///                         It is up to the caller if this is a strict comparison -- strict (>) for end-inclusive spans
    ///                         and non-strict (>=) for end-exclusive.
    /// @param startsComparator Comparator of items' starting positions.
    /// @param endsComparator   Comparator of items' ending positions.
    /// @param reducer          Function to apply to each overlap set.
    /// @param initialValue     Initial value for the reducer.
    /// @return The result of processing the overlap sets.
    public static <E, R> R constructOverlapSets(Collection<E> items,
                                                BiPredicate<? super E, ? super E> startsAfter,
                                                Comparator<? super E> startsComparator,
                                                Comparator<? super E> endsComparator,
                                                BiFunction<R, Collection<E>, R> reducer,
                                                R initialValue)
    {
        R overlaps = initialValue;
        if (items.isEmpty())
            return overlaps;

        PriorityQueue<E> active = new PriorityQueue<>(endsComparator);
        SortingIterator<E> itemsSorted = SortingIterator.create(startsComparator, items);
        while (itemsSorted.hasNext())
        {
            E item = itemsSorted.next();
            if (!active.isEmpty() && startsAfter.test(item, active.peek()))
            {
                // New item starts after some active ends. It does not overlap with it, so:
                // -- output the previous active set
                overlaps = reducer.apply(overlaps, active);
                // -- remove all items that also end before the current start
                do
                {
                    active.poll();
                }
                while (!active.isEmpty() && startsAfter.test(item, active.peek()));
            }

            // Add the new item to the active state. We don't care if it starts later than others in the active set,
            // the important point is that it overlaps with all of them.
            active.add(item);
        }

        assert !active.isEmpty();
        overlaps = reducer.apply(overlaps, active);

        return overlaps;
    }

    /// Transform a list to transitively combine adjacent sets that have a common element, resulting in disjoint sets.
    public static <T> List<Set<T>> combineSetsWithCommonElement(List<? extends Set<T>> overlapSets)
    {
        Set<T> group = overlapSets.get(0);
        List<Set<T>> groups = new ArrayList<>();
        for (int i = 1; i < overlapSets.size(); ++i)
        {
            Set<T> current = overlapSets.get(i);
            if (Collections.disjoint(current, group))
            {
                groups.add(group);
                group = current;
            }
            else
            {
                group.addAll(current);
            }
        }
        groups.add(group);
        return groups;
    }

    /// Split a list of items into disjoint non-overlapping sets.
    ///
    /// @param items            A list of items to distribute in overlap sets. It is assumed that the start and end
    ///                         positions of an item are ordered, and the items are non-empty.
    /// @param startsAfter      Predicate determining if its left argument's start if fully after the right argument's end.
    ///                         This will only be used with arguments where left's start is known to be after right's start.
    ///                         It is up to the caller if this is a strict comparison -- strict (>) for end-inclusive spans
    ///                         and non-strict (>=) for end-exclusive.
    /// @param startsComparator Comparator of items' starting positions.
    /// @param endsComparator   Comparator of items' ending positions.
    /// @return list of non-overlapping sets of items
    public static <T> List<Set<T>> splitInNonOverlappingSets(List<T> items,
                                                             BiPredicate<T, T> startsAfter,
                                                             Comparator<T> startsComparator,
                                                             Comparator<T> endsComparator)
    {
        if (items.isEmpty())
            return List.of();

        List<Set<T>> overlapSets = Overlaps.constructOverlapSets(items, startsAfter, startsComparator, endsComparator);
        return combineSetsWithCommonElement(overlapSets);
    }


    /// Overlap inclusion method to use when combining overlap sections into a bucket. For example, with
    /// items A(0, 5), B(2, 9), C(6, 12), D(10, 12) whose overlap sections calculation returns \[AB, BC, CD\],
    ///   - `NONE` means no sections are to be merged. AB, BC and CD will be separate buckets, selections AB, BC and CD
    ///     will be added separately, thus some items will be partially used / single-source compacted, likely
    ///     to be recompacted again with the next selected bucket.
    ///   - `SINGLE` means only overlaps of the sstables in the selected bucket will be added. AB+BC will be one bucket,
    ///     and CD will be another (as BC is already used). A middle ground of sorts, should reduce overcompaction but
    ///     still has some.
    ///   - `TRANSITIVE` means a transitive closure of overlapping sstables will be selected. AB+BC+CD will be in the
    ///     same bucket, selected compactions will apply to all overlapping sstables and no overcompaction will be done,
    ///     at the cost of reduced compaction parallelism and increased length of the operation.
    public enum InclusionMethod
    {
        NONE, SINGLE, TRANSITIVE
    }

    public interface BucketMaker<E, B>
    {
        B makeBucket(List<Set<E>> sets, int startIndexInclusive, int endIndexExclusive);
    }

    /// Assign overlap sections into buckets. Identify sections that have at least threshold-many overlapping
    /// items and apply the overlap inclusion method to combine with any neighbouring sections that contain
    /// selected sstables to make sure we make full use of any sstables selected for compaction (i.e. avoid
    /// recompacting, see [InclusionMethod]).
    ///
    /// For non-transitive inclusion method the order in which we select the buckets matters because an sstables that
    /// spans overlap sets could be chosen for only one of the candidate buckets containing it. To make the most
    /// efficient selection we thus perform it by descending size, starting with the sets with most overlap.
    ///
    /// @param threshold       Threshold for selecting a bucket. Sets below this size will be ignored, unless they need
    ///                        to be grouped with a neighboring set due to overlap.
    /// @param inclusionMethod `NONE` to only form buckets of the overlapping sets, `SINGLE` to include all
    ///                        sets that share an sstable with a selected bucket, or `TRANSITIVE` to include
    ///                        all sets that have an overlap chain to a selected bucket.
    /// @param overlaps        An ordered list of overlap sets as returned by [#constructOverlapSets].
    /// @param bucketer        Method used to create a bucket out of the supplied set indexes.
    /// @param unselectedHandler Action to take on sets that are below the threshold and not included in any bucket.
    public static <E, B> List<B> assignOverlapsIntoBuckets(int threshold,
                                                           InclusionMethod inclusionMethod,
                                                           List<Set<E>> overlaps,
                                                           BucketMaker<E, B> bucketer,
                                                           Consumer<Set<E>> unselectedHandler)
    {
        switch (inclusionMethod)
        {
            case TRANSITIVE:
                return assignOverlapsTransitive(threshold, overlaps, bucketer, unselectedHandler);
            case SINGLE:
            case NONE:
                return assignOverlapsSingleOrNone(threshold, inclusionMethod, overlaps, bucketer, unselectedHandler);
            default:
                throw new UnsupportedOperationException(inclusionMethod + " is not supported");
        }
    }

    private static <E, B> List<B> assignOverlapsSingleOrNone(int threshold,
                                                             InclusionMethod inclusionMethod,
                                                             List<Set<E>> overlaps,
                                                             BucketMaker<E, B> bucketer,
                                                             Consumer<Set<E>> unselectedHandler)
    {
        List<B> buckets = new ArrayList<>();
        int regionCount = overlaps.size();
        SortingIterator<Integer> bySize = new SortingIterator<>((a, b) -> Integer.compare(overlaps.get(b).size(),
                                                                                          overlaps.get(a).size()),
                                                                overlaps.isEmpty() ? new Integer[1] : IntStream.range(0, overlaps.size()).boxed().toArray());

        BitSet used = new BitSet(overlaps.size());
        while (bySize.hasNext())
        {
            final int i = bySize.next();
            if (used.get(i))
                continue;

            Set<E> bucket = overlaps.get(i);
            if (bucket.size() < threshold)
                break;  // no more buckets will be above threshold
            used.set(i);

            Set<E> allOverlapping = bucket;
            int j = i - 1;
            int k = i + 1;
            int startIndex = i;
            int endIndex = i + 1;
            // expand to include neighbors that intersect with current bucket
            if (inclusionMethod == InclusionMethod.SINGLE)
            {
                // expand the bucket to include all overlapping sets
                allOverlapping = new HashSet<>(bucket);
                Set<E> overlapTarget = bucket;
                for (; j >= 0 && !used.get(j); --j)
                {
                    Set<E> next = overlaps.get(j);
                    if (!setsIntersect(next, overlapTarget))
                        break;
                    allOverlapping.addAll(next);
                    used.set(j);
                }
                startIndex = j + 1;
                for (; k < regionCount && !used.get(k); ++k)
                {
                    Set<E> next = overlaps.get(k);
                    if (!setsIntersect(next, overlapTarget))
                        break;
                    allOverlapping.addAll(next);
                    used.set(k);
                }
                endIndex = k;
            }
            // Now mark all overlapping with the extended as used
            Set<E> overlapTarget = allOverlapping;
            for (; j >= 0 && !used.get(j); --j)
            {
                Set<E> next = overlaps.get(j);
                if (!setsIntersect(next, overlapTarget))
                    break;
                used.set(j);
                unselectedHandler.accept(next);
            }
            for (; k < regionCount && !used.get(k); ++k)
            {
                Set<E> next = overlaps.get(k);
                if (!setsIntersect(next, overlapTarget))
                    break;
                used.set(k);
                unselectedHandler.accept(next);
            }
            buckets.add(bucketer.makeBucket(overlaps, startIndex, endIndex));
        }

        for (int i = used.nextClearBit(0); i < regionCount; i = used.nextClearBit(i + 1))
            unselectedHandler.accept(overlaps.get(i));

        return buckets;
    }

    private static <E, B> List<B> assignOverlapsTransitive(int threshold,
                                                           List<Set<E>> overlaps,
                                                           BucketMaker<E, B> bucketer,
                                                           Consumer<Set<E>> unselectedHandler)
    {
        List<B> buckets = new ArrayList<>();
        int regionCount = overlaps.size();
        int lastEnd = 0;
        for (int i = 0; i < regionCount; ++i)
        {
            Set<E> bucket = overlaps.get(i);
            int maxOverlap = bucket.size();
            if (maxOverlap < threshold)
                continue;

            // expand to include neighbors that intersect with expanded buckets
            Set<E> allOverlapping = new HashSet<>(bucket);
            Set<E> overlapTarget = allOverlapping;
            int j;
            for (j = i - 1; j >= lastEnd; --j)
            {
                Set<E> next = overlaps.get(j);
                if (!setsIntersect(next, overlapTarget))
                    break;
                allOverlapping.addAll(next);
            }
            int startIndex = j + 1;
            for (j = i + 1; j < regionCount; ++j)
            {
                Set<E> next = overlaps.get(j);
                if (!setsIntersect(next, overlapTarget))
                    break;
                allOverlapping.addAll(next);
            }
            i = j - 1;
            int endIndex = j;

            buckets.add(bucketer.makeBucket(overlaps, startIndex, endIndex));
            for (int k = lastEnd; k < startIndex; ++k)
                unselectedHandler.accept(overlaps.get(k));
            lastEnd = endIndex;
        }
        for (int k = lastEnd; k < regionCount; ++k)
            unselectedHandler.accept(overlaps.get(k));
        return buckets;
    }

    private static <E> boolean setsIntersect(Set<E> s1, Set<E> s2)
    {
        // Note: optimized for small sets and O(1) lookup.
        for (E s : s1)
            if (s2.contains(s))
                return true;

        return false;
    }

    /// Pull the last elements from the given list, up to the given limit.
    public static <T> List<T> pullLast(List<T> source, int limit)
    {
        List<T> result = new ArrayList<>(limit);
        while (--limit >= 0)
            result.add(source.remove(source.size() - 1));
        return result;
    }

    /// Select up to `limit` sstables from each overlapping set (more than `limit` in total) by taking the last entries
    /// from `allObjectsSorted`. To achieve this, keep selecting the last sstable until the next one we would add would
    /// bring the number selected in some overlap section over `limit`.
    public static <T> Collection<T> pullLastWithOverlapLimit(List<T> allObjectsSorted, List<Set<T>> overlapSets, int limit)
    {
        int setsCount = overlapSets.size();
        int[] selectedInBucket = new int[setsCount];
        int allCount = allObjectsSorted.size();
        for (int selectedCount = 0; selectedCount < allCount; ++selectedCount)
        {
            T candidate = allObjectsSorted.get(allCount - 1 - selectedCount);
            for (int i = 0; i < setsCount; ++i)
            {
                if (overlapSets.get(i).contains(candidate))
                {
                    ++selectedInBucket[i];
                    if (selectedInBucket[i] > limit)
                        return pullLast(allObjectsSorted, selectedCount);
                }
            }
        }
        return allObjectsSorted;
    }
}
