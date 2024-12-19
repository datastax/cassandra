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
package org.apache.cassandra.index.sai.disk.format;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.StorageAttachedIndexGroup;
import org.apache.cassandra.io.sstable.format.SSTableReader;

/**
 * Represents, for a sstable, the "state" (version and generation) of the index components it is using.
 * <p>
 * This class essentially store, for each "group" of index components (so for the per-sstable group, and for each index),
 * a version and generation, identifying a particular build of each group. This is used by {@link IndexComponentDiscovery}
 * to return which concrete components should be loaded for the sstable, but as this class is immutable, it can be
 * used to figure changes to index files between two different times (by capturing the state before, and comparing to
 * the state after); see {@link #indexWasUpdated} as an example.
 * <p>
 * As this state only reference the {@link ComponentsBuildId} of the components, it does not represent whether that
 * build is complete/valid, and as such this class does not guarantee _in general_ that the builds it returns are
 * usable, and whether it does depend on context. But some methods may explicitly return a state with only complete
 * groups (like {@link #of(IndexDescriptor)}).
 */
public class SSTableIndexComponentsState
{
    public static final SSTableIndexComponentsState EMPTY = new SSTableIndexComponentsState(null, Map.of());

    // The state of the per-sstable group of components, if they exist.
    private final @Nullable State perSSTableState;

    // The state for every "group" of per-index components keyed by the index name.
    private final Map<String, State> perIndexStates;

    private SSTableIndexComponentsState(@Nullable State perSSTableState, Map<String, State> perIndexStates)
    {
        Preconditions.checkNotNull(perIndexStates);
        this.perSSTableState = perSSTableState;
        this.perIndexStates = Collections.unmodifiableMap(perIndexStates);
    }

    /**
     * Extracts the current state of a particular SSTable given its descriptor.
     * <p>
     * Please note that this method only include "complete" component groups in the state, and thus represents the
     * "usable" groups. In particular, if the per-sstable group is not complete, the returned state will be empty.
     *
     * @param descriptor the index descriptor of the sstable for which to get the component state.
     * @return the state of the sstable's complete components.
     */
    public static SSTableIndexComponentsState of(IndexDescriptor descriptor)
    {
        var perSSTable = descriptor.perSSTableComponents();
        // If the per-sstable part is not complete, then nothing is complete.
        if (!perSSTable.isComplete())
            return EMPTY;

        Map<String, State> perIndexStates = new HashMap<>();
        for (IndexContext context : descriptor.includedIndexes())
        {
            var perIndex = descriptor.perIndexComponents(context);
            if (perIndex.isComplete())
                perIndexStates.put(context.getIndexName(), State.of(perIndex));
        }
        return new SSTableIndexComponentsState(State.of(perSSTable), perIndexStates);
    }

    /**
     * Extracts the current index components state of a particular SSTable.
     * <p>
     * This method delegates to {@link #of(IndexDescriptor)}, so see that method for additional details.
     *
     * @param sstable the sstable for which to get the component state.
     * @return the state of the sstable's complete index components. If the sstable belongs to a table that is not
     * indexed (or not by SAI), then this will be {@link #EMPTY}.
     *
     * @throws IllegalStateException if the {@link org.apache.cassandra.db.ColumnFamilyStore} of the sstable cannot
     * be found for some reason (it is necessary to retrieve the underlying {@link IndexDescriptor}). This may happen
     * if this is called by an "offline" tool.
     */
    public static SSTableIndexComponentsState of(SSTableReader sstable)
    {
        ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(sstable.metadata().id);
        if (cfs == null)
            throw new IllegalStateException("Cannot find the ColumnFamilyStore for the sstable " + sstable);

        StorageAttachedIndexGroup saiGroup = StorageAttachedIndexGroup.getIndexGroup(cfs);
        // If the table is not indexed (at least by SAI), fine.
        if (saiGroup == null)
            return SSTableIndexComponentsState.EMPTY;

        return SSTableIndexComponentsState.of(saiGroup.descriptorFor(sstable));
    }

    public static Builder builder()
    {
        return new Builder();
    }

    /**
     * Returns a newly created builder initialized with the data of this state.
     */
    public Builder unbuild()
    {
        Builder builder = new Builder();
        builder.addPerSSTable(perSSTableState);
        perIndexStates.forEach(builder::addPerIndex);
        return builder;
    }

    public boolean isEmpty()
    {
        return perSSTableState == null && perIndexStates.isEmpty();
    }

    public @Nullable State perSSTable()
    {
        return perSSTableState;
    }

    public @Nullable State perIndex(String indexName)
    {
        Preconditions.checkNotNull(indexName);
        return perIndexStates.get(indexName);
    }

    public @Nullable ComponentsBuildId perSSTableBuild()
    {
        return perSSTableState == null ? null : perSSTableState.buildId;
    }

    public @Nullable ComponentsBuildId perIndexBuild(String indexName)
    {
        Preconditions.checkNotNull(indexName);
        var state = perIndexStates.get(indexName);
        return state == null ? null : state.buildId;
    }

    /**
     * Returns whether the provided index have been updated since the given state.
     * <p>
     * Having been "updated" for this method means that builds of the components used by the index have changed.
     * Importantly, this is true if _either_ the per-sstable components have changed, or that of the index itself,
     * since every index uses the per-sstable components.
     */
    public boolean indexWasUpdated(SSTableIndexComponentsState stateBefore, String indexName)
    {
        Preconditions.checkNotNull(indexName);
        return !Objects.equals(stateBefore.perSSTableBuild(), this.perSSTableBuild())
                || !Objects.equals(stateBefore.perIndexBuild(indexName), this.perIndexBuild(indexName));
    }

    /**
     * The set of the names of all the indexes for which the state has a build for.
     * <p>
     * This does not include anything regarding the per-sstable components.
     */
    public Set<String> includedIndexes()
    {
        return perIndexStates.keySet();
    }

    /**
     * The total size (in MB) of all the components included in this state.
     */
    public long totalSizeInMB()
    {
        long total = perSSTableState == null ? 0 : perSSTableState.sizeInMB;
        for (State state : perIndexStates.values())
            total += state.sizeInMB;
        return total;
    }

    /**
     * Returns a diff between this state and the provided one which is assumed to be an earlier version.
     *
     * @param before the state to compare this state with.
     * @return the diff between the 2 states.
     */
    public Diff diff(SSTableIndexComponentsState before)
    {
        boolean perSSTableModified = !Objects.equals(before.perSSTableBuild(), this.perSSTableBuild());
        Set<String> modifiedIndexes = this.includedIndexes()
                                          .stream()
                                          .filter(index -> !Objects.equals(before.perIndexBuild(index), this.perIndexBuild(index)))
                                          .collect(Collectors.toSet());
        Set<String> removedIndexes = before.includedIndexes()
                                           .stream()
                                           .filter(index -> !this.perIndexStates.containsKey(index))
                                           .collect(Collectors.toSet());
        return new Diff(before, this, perSSTableModified, modifiedIndexes, removedIndexes);
    }

    /**
     * Applies the provided diff to this state, if applicable.
     * <p>
     * The assumption of this method is that the state it is applied to is for the same sstable that the 2 states that
     * were used to produce the provided diff.
     * <p>
     * The diff will apply successfully if for anything that is modified in the provided diff, the current state is
     * equivalent to the "before" state of the diff. If that is not the case, an {@link UnapplicableDiffException} will
     * be thrown. But for anything that was not modified by the diff, the current state will be kept as is. Note in
     * particular that this means that if {@code this == diff.before}, then the result will be exactly {@code diff.after},
     * but as long as {@code this} has only modifications (compared to {@code diff.before}) that are not in {@code diff},
     * then the diff will still apply correctly.
     * <p>
     * In other word, this method allows to compute the expected result of some index builds represented by the diff
     * to the "current" state as long as said "current" state is the "before" state of the diff plus some eventual
     * concurrent modifications, as long as those concurrent modifications do not conflict with the ones of the diff.
     *
     * @param diff the diff to try to apply to this state.
     * @return the result of applying the diff to this state, if successful.
     *
     * @throws UnapplicableDiffException if the diff cannot be applied to this state.
     */
    public SSTableIndexComponentsState tryApplyDiff(Diff diff)
    {
        if (diff.isEmpty())
            return this;

        Builder builder = builder();
        builder.addPerSSTable(diff.perSSTableUpdated
                              ? diffState(diff.before.perSSTableState, diff.after.perSSTableState, this.perSSTableState, () -> "per-sstable components build")
                              : this.perSSTableState);

        // Adds anything modified to the "modified" version, but making sure the diff "applies", meaning that the
        // "current" state is still the origin of the diff.
        for (String modified : diff.perIndexesUpdated)
        {
            builder.addPerIndex(modified, diffState(diff.before.perIndex(modified), diff.after.perIndex(modified), this.perIndex(modified), () -> "index " + modified + " components build"));
        }
        // Then mirror all the current index that were not modified, but skipping removed ones.
        for (String index : includedIndexes())
        {
            // The `perIndexesUpdated` have already been handled above. And a removed index means the index has been
            // dropped, so even if some concurrent build on the index happened concurrently, the index is still gone.
            if (diff.perIndexesUpdated.contains(index) || diff.perIndexesRemoved.contains(index))
                continue;

            builder.addPerIndex(index, this.perIndex(index));
        }
        return builder.build();
    }

    private static State diffState(State diffBefore, State diffAfter, State current, Supplier<String> what)
    {
        // If current is `null`, but our "before" isn't, that means the index this is a component of has been dropped
        // since the state we use to create the diff (and for the per-sstable components, it was the only index that
        // was dropped). We want to handle a drop that happens concurrently of some build/rebuild of the same index,
        // because it's impossible to completly prevent it anyway, and the result is simply that the index is not there
        // anymore.
        if (current == null && diffBefore != null)
            return null;

        if (!(Objects.equals(diffBefore, current)))
            throw new UnapplicableDiffException("Current " + what.get() + " expected to be " + diffBefore + ", but was " + current);

        return diffAfter;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(perSSTableState, perIndexStates);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (!(obj instanceof SSTableIndexComponentsState))
            return false;

        SSTableIndexComponentsState that = (SSTableIndexComponentsState) obj;
        return Objects.equals(this.perSSTableState, that.perSSTableState)
               && this.perIndexStates.equals(that.perIndexStates);
    }

    @Override
    public String toString()
    {
        Stream<String> perIndex = perIndexStates.entrySet()
                                                .stream()
                                                .map(e -> e.getKey() + ": " + e.getValue());
        Stream<String> all = perSSTableState == null
                             ? perIndex
                             : Stream.concat(Stream.of("<shared>: " + perSSTableState), perIndex);

        return all.collect(Collectors.joining(", ", "{", "}"));
    }

    /**
     * Represents the "state" for one "group" of components (so either the per-sstable one, or one of the per-index ones).
     */
    public static class State
    {
        /** The "build" (version and generaton) the components. */
        public final ComponentsBuildId buildId;

        /** The total size (in MB) of the components (we use MB because this is meant to be indicative, is enough
         * precision in practice and is more human-readable). */
        public final long sizeInMB;

        private State(ComponentsBuildId buildId, long sizeInMB)
        {
            Preconditions.checkNotNull(buildId);
            this.buildId = buildId;
            this.sizeInMB = sizeInMB;
        }

        private static State of(IndexComponents.ForRead components)
        {
            return new State(components.buildId(), toMB(components.liveSizeOnDiskInBytes()));
        }

        private static long toMB(long bytes)
        {
            if (bytes == 0)
                return 0;

            // We avoid returning 0 unless the size is truly zero to avoid making it look like the components do not
            // exist. Mostly a detail in practice but ...
            return Math.max(bytes / 1024 / 1024, 1);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (!(obj instanceof State))
                return false;

            State that = (State) obj;
            return this.buildId.equals(that.buildId) && this.sizeInMB == that.sizeInMB;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(buildId, sizeInMB);
        }

        @Override
        public String toString()
        {
            return String.format("%s (%dMB)", buildId, sizeInMB);
        }
    }

    /**
     * Builder for {@link SSTableIndexComponentsState} instances.
     * <p>
     * This should primarily be used by implementations of {@link IndexComponentDiscovery} and tests, as the rest of
     * the code should generally not build a state manually (and instead use methods like {@link SSTableIndexComponentsState#of}).
     */
    public static class Builder
    {
        private State perSSTableState;
        // We use a linked map to preserve order of insertions. This is not crucial, but the overhead is negligible and
        // in the case of tests, the predictability of entry order make things _a lot_ easier/natural.
        private final Map<String, State> perIndexStates = new LinkedHashMap<>();
        // This make extra sure we don't reuse a builder by accident as it is not safe to do so (we pass the map
        // directly when we build the state). If one wants to reuse a builder, it should `copy` manually first.
        private boolean built;

        public Builder addPerSSTable(Version version, int generation, long sizeInMB)
        {
            return addPerSSTable(ComponentsBuildId.of(version, generation), sizeInMB);
        }

        public Builder addPerSSTable(ComponentsBuildId buildId, long sizeInMB)
        {
            return addPerSSTable(new State(buildId, sizeInMB));
        }

        public Builder addPerSSTable(State state)
        {
            Preconditions.checkState(!built, "Builder has already been used");
            this.perSSTableState = state;
            return this;
        }

        public Builder addPerIndex(String name, Version version, int generation, long sizeInMB)
        {
            return addPerIndex(name, ComponentsBuildId.of(version, generation), sizeInMB);
        }

        public Builder addPerIndex(String name, ComponentsBuildId buildId, long sizeInMB)
        {
            return addPerIndex(name, new State(buildId, sizeInMB));
        }

        public Builder addPerIndex(String name, State state)
        {
            Preconditions.checkState(!built, "Builder has already been used");
            Preconditions.checkNotNull(name);
            if (state != null)
                perIndexStates.put(name, state);
            return this;
        }

        public Builder removePerSSTable()
        {
            Preconditions.checkState(!built, "Builder has already been used");
            perSSTableState = null;
            return this;
        }

        public Builder removePerIndex(String name)
        {
            Preconditions.checkState(!built, "Builder has already been used");
            Preconditions.checkNotNull(name);
            perIndexStates.remove(name);
            return this;
        }

        public Builder copy()
        {
            Builder copy = new Builder();
            copy.perSSTableState = perSSTableState;
            copy.perIndexStates.putAll(perIndexStates);
            return copy;
        }

        public SSTableIndexComponentsState build()
        {
            built = true;
            return new SSTableIndexComponentsState(perSSTableState, perIndexStates);
        }
    }

    /**
     * Represents the difference between two {@link SSTableIndexComponentsState} instances that are assumed snapshots
     * of the same sstable at 2 different times.
     */
    public static class Diff
    {
        /** Older of the 2 states compared in this diff. */
        public final SSTableIndexComponentsState before;
        /** Newer of the 2 states compared in this diff. */
        public final SSTableIndexComponentsState after;
        /** Whether the per-sstable components were updated between the 2 states. */
        public final boolean perSSTableUpdated;
        /** Which per-index components were updated between the 2 states. */
        public final Set<String> perIndexesUpdated;
        /** Which per-index components were removed (where in {@link #before}) but not {@link #after}. */
        public final Set<String> perIndexesRemoved;

        private Diff(SSTableIndexComponentsState before, SSTableIndexComponentsState after, boolean perSSTableUpdated, Set<String> perIndexesUpdated, Set<String> perIndexesRemoved)
        {
            this.before = before;
            this.after = after;
            this.perSSTableUpdated = perSSTableUpdated;
            this.perIndexesUpdated = Collections.unmodifiableSet(perIndexesUpdated);
            this.perIndexesRemoved = Collections.unmodifiableSet(perIndexesRemoved);
        }

        /**
         * Whether this diff is empty, meaning that no changes were detected between the 2 states.
         */
        public boolean isEmpty()
        {
            return !perSSTableUpdated && perIndexesUpdated.isEmpty() && perIndexesRemoved.isEmpty();
        }

        /**
         * Whether the operation that created this diff (meaning, the operation(s) that happened on {@link #before}
         * to create {@link #after}) left some "unused" components, meaning that new components (new version or
         * generation) were created where previous one existed.
         */
        public boolean createsUnusedComponents()
        {
            // Removing any components left them "unused".
            if (!perIndexesRemoved.isEmpty())
                return true;

            return (perSSTableUpdated && before.perSSTableState != null)
                || perIndexesUpdated.stream().anyMatch(index -> before.perIndex(index) != null);
        }

        @Override
        public String toString()
        {
            if (isEmpty())
                return String.format("%s (no diff)", before);

            List<String> updates = new ArrayList<>();
            if (perSSTableUpdated)
            {
                if (after.perSSTableState == null)
                    updates.add("-<shared>");
                else
                    updates.add("+<shared>");
            }
            for (String updated : perIndexesUpdated)
                updates.add('+' + updated);
            for (String removed : perIndexesRemoved)
                updates.add('-' + removed);
            return String.format("%s -> %s (%s)", before, after, String.join(" ", updates));
        }
    }

    public static class UnapplicableDiffException extends RuntimeException
    {
        public UnapplicableDiffException(String message)
        {
            super(message);
        }
    }
}
