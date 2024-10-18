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

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;

import org.apache.cassandra.index.sai.IndexContext;

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

    private final @Nullable ComponentsBuildId perSSTableBuild;

    // Version and generation for every "group" of components that is complete. The "null" key is used for the
    // per-sstable components, otherwise it is the index name.
    private final Map<String, ComponentsBuildId> perIndexBuilds;

    private SSTableIndexComponentsState(@Nullable ComponentsBuildId perSSTableBuild, Map<String, ComponentsBuildId> perIndexBuilds)
    {
        Preconditions.checkNotNull(perIndexBuilds);
        this.perSSTableBuild = perSSTableBuild;
        this.perIndexBuilds = Collections.unmodifiableMap(perIndexBuilds);
    }

    /**
     * Extracts the current state of a particular SSTable given its descriptor.
     * <p>
     * Please note that this method only include "complete" component groups in the state, and thus represents the
     * "usable" groups. In particular, if the per-sstable group is not complete, the returned state will be empty.
     */
    public static SSTableIndexComponentsState of(IndexDescriptor descriptor)
    {
        var perSSTable = descriptor.perSSTableComponents();
        // If the per-sstable part is not complete, then nothing is complete.
        if (!perSSTable.isComplete())
            return EMPTY;

        Map<String, ComponentsBuildId> perIndexBuilds = new HashMap<>();
        for (IndexContext context : descriptor.includedIndexes())
        {
            var perIndex = descriptor.perIndexComponents(context);
            if (perIndex.isComplete())
                perIndexBuilds.put(context.getIndexName(), perIndex.buildId());
        }
        return new SSTableIndexComponentsState(perSSTable.buildId(), perIndexBuilds);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public boolean isEmpty()
    {
        return perSSTableBuild == null && perIndexBuilds.isEmpty();
    }

    public @Nullable ComponentsBuildId perSSTableBuild()
    {
        return perSSTableBuild;
    }

    public @Nullable ComponentsBuildId perIndexBuild(String indexName)
    {
        Preconditions.checkNotNull(indexName); // the `buildOf` method(s) should be used instead
        return perIndexBuilds.get(indexName);
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
        return !Objects.equals( stateBefore.perSSTableBuild(), this.perSSTableBuild())
                || !Objects.equals(stateBefore.perIndexBuild(indexName), this.perIndexBuild(indexName));
    }

    /**
     * The set of the names of all the indexes for which the state has a build for.
     * <p>
     * This does not include anything regarding the per-sstable components.
     */
    public Set<String> includedIndexes()
    {
        return perIndexBuilds.keySet();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(perSSTableBuild, perIndexBuilds);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (!(obj instanceof SSTableIndexComponentsState))
            return false;

        SSTableIndexComponentsState that = (SSTableIndexComponentsState) obj;
        return Objects.equals(this.perSSTableBuild, that.perSSTableBuild)
               && this.perIndexBuilds.equals(that.perIndexBuilds);
    }

    @Override
    public String toString()
    {
        Stream<String> perIndex = perIndexBuilds.entrySet()
                                                .stream()
                                                .map(e -> e.getKey() + ": " + e.getValue());
        Stream<String> all = perSSTableBuild == null
                             ? perIndex
                             : Stream.concat(Stream.of("<shared>: " + perSSTableBuild), perIndex);

        return all.collect(Collectors.joining(", ", "{", "}"));
    }

    /**
     * Builder for {@link SSTableIndexComponentsState} instances.
     * <p>
     * This should primarily be used by implementations of {@link IndexComponentDiscovery} and tests, as the rest of
     * the code should generally not build a state manually (and instead use methods like {@link SSTableIndexComponentsState#of}).
     */
    public static class Builder
    {
        private ComponentsBuildId perSSTableBuild;
        // We use a linked map to preserve order of insertions. This is not crucial, but the overhead is negligible and
        // in the case of tests, the predictability of entry order make things _a lot_ easier/natural.
        private final Map<String, ComponentsBuildId> perIndexBuilds = new LinkedHashMap<>();
        // This make extra sure we don't reuse a builder by accident as it is not safe to do so (we pass the map
        // directly when we build the state). If one wants to reuse a builder, it should `copy` manually first.
        private boolean built;

        public Builder addPerSSTable(Version version, int generation)
        {
            return addPerSSTable(ComponentsBuildId.of(version, generation));
        }

        public Builder addPerSSTable(ComponentsBuildId buildId)
        {
            Preconditions.checkState(!built, "Builder has already been used");
            this.perSSTableBuild = buildId;
            return this;
        }

        public Builder addPerIndex(String name, Version version, int generation)
        {
            return addPerIndex(name, ComponentsBuildId.of(version, generation));
        }

        public Builder addPerIndex(String name, ComponentsBuildId buildId)
        {
            Preconditions.checkState(!built, "Builder has already been used");
            Preconditions.checkNotNull(name);
            perIndexBuilds.put(name, buildId);
            return this;
        }

        public ComponentsBuildId addPerSSTableIfAbsent(ComponentsBuildId buildId)
        {
            if (perSSTableBuild == null)
            {
                addPerSSTable(buildId);
                return null;
            }
            else
            {
                return perSSTableBuild;
            }
        }

        public ComponentsBuildId addPerIndexIfAbsent(String name, ComponentsBuildId buildId)
        {
            Preconditions.checkState(!built, "Builder has already been used");
            Preconditions.checkNotNull(name);
            return perIndexBuilds.computeIfAbsent(name, __ -> buildId);
        }

        public Builder replacePerSSTable(UnaryOperator<ComponentsBuildId> updater)
        {
            Preconditions.checkState(!built, "Builder has already been used");
            this.perSSTableBuild = updater.apply(perSSTableBuild);
            return this;
        }

        public Builder replacePerIndex(String name, UnaryOperator<ComponentsBuildId> updater)
        {
            Preconditions.checkState(!built, "Builder has already been used");
            Preconditions.checkNotNull(name);
            perIndexBuilds.compute(name, (__, prev) -> updater.apply(prev));
            return this;
        }

        public Builder removePerSSTable()
        {
            Preconditions.checkState(!built, "Builder has already been used");
            perSSTableBuild = null;
            return this;
        }

        public Builder removePerIndex(String name)
        {
            Preconditions.checkState(!built, "Builder has already been used");
            Preconditions.checkNotNull(name);
            perIndexBuilds.remove(name);
            return this;
        }

        public Builder copy()
        {
            Builder copy = new Builder();
            copy.perSSTableBuild = perSSTableBuild;
            copy.perIndexBuilds.putAll(perIndexBuilds);
            return copy;
        }

        public SSTableIndexComponentsState build()
        {
            built = true;
            return new SSTableIndexComponentsState(perSSTableBuild, perIndexBuilds);
        }
    }
}
