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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.NoSuchFileException;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.util.collection.IntObjectHashMap;
import io.netty.util.collection.IntObjectMap;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.util.PathUtils;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.NoSpamLogger;

import static org.apache.cassandra.config.CassandraRelevantProperties.CUSTOM_SAI_INDEX_COMPONENTS_DISCOVERY;

/**
 * Handles "discovering" SAI index components files from disk for a given sstable.
 * <p>
 * This is used by {@link IndexDescriptor} and should rarely, if ever, be used directly, but it is exposed publicly to
 * make the logic "pluggable" (typically for tiered-storage that may not store files directly on disk and thus require
 * some specific abstraction).
 */
public abstract class IndexComponentDiscovery
{
    private static final Logger logger = LoggerFactory.getLogger(IndexComponentDiscovery.class);
    private static final NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(logger, 1, TimeUnit.MINUTES);

    static IndexComponentDiscovery instance = !CUSTOM_SAI_INDEX_COMPONENTS_DISCOVERY.isPresent()
                              ? new DefaultIndexComponentDiscovery() {}
                              : FBUtilities.construct(CUSTOM_SAI_INDEX_COMPONENTS_DISCOVERY.getString(), "SAI index components discovery");

    public abstract DiscoveredGroups discoverComponents(Descriptor descriptor);

    /**
     * Represents all the groups of discovered components, that is normall a per-sstable one, and a number of per-index
     * ones.
     */
    public static class DiscoveredGroups
    {
        private final Map<String, DiscoveredComponents> groups;

        private DiscoveredGroups(Map<String, DiscoveredComponents> groups)
        {
            this.groups = groups;
        }

        public static DiscoveredGroups of(Map<String, DiscoveredComponents.Builder> groupBuilders)
        {
            Map<String, DiscoveredComponents> groups = new HashMap<>();
            for (Map.Entry<String, DiscoveredComponents.Builder> entry : groupBuilders.entrySet())
            {
                var builder = entry.getValue();
                if (builder.buildId != null && !builder.types.isEmpty())
                    groups.put(entry.getKey(), builder.build());
            }
            return new DiscoveredGroups(groups);
        }

        public DiscoveredComponents perSSTableGroup()
        {
            return groups.get(null);
        }

        public DiscoveredComponents perIndexGroup(String indexName)
        {
            return groups.get(indexName);
        }

        @Override
        public String toString()
        {
            return groups.entrySet()
                         .stream()
                         .map(e -> (e.getKey() == null ? "<shared>" : e.getKey()) + ": " + e.getValue())
                         .collect(Collectors.joining(", ", "{", "}"));
        }
    }

    public static class DiscoveredComponents
    {
        private final ComponentsBuildId buildId;
        private final Set<IndexComponentType> types;

        private DiscoveredComponents(ComponentsBuildId buildId, Set<IndexComponentType> types)
        {
            this.buildId = buildId;
            this.types = types;
        }

        public ComponentsBuildId buildId()
        {
            return buildId;
        }

        public Set<IndexComponentType> types()
        {
            return types;
        }

        @Override
        public String toString()
        {
            return String.format("%s: %s", buildId, types);
        }

        public static class Builder
        {
            private ComponentsBuildId buildId;
            private final Set<IndexComponentType> types = EnumSet.noneOf(IndexComponentType.class);

            public void setBuildId(ComponentsBuildId buildId)
            {
                Preconditions.checkState(this.buildId == null, "Build ID already set");
                this.buildId = buildId;
            }

            public void add(IndexComponentType type)
            {
                this.types.add(type);
            }

            public void addAll(Collection<IndexComponentType> types)
            {
                this.types.addAll(types);
            }

            public DiscoveredComponents build()
            {
                Preconditions.checkState(buildId != null, "Build ID should have bee set");
                return new DiscoveredComponents(buildId, Collections.unmodifiableSet(types));
            }
        }
    }

    private static class DefaultIndexComponentDiscovery extends IndexComponentDiscovery
    {
        @Override
        public DiscoveredGroups discoverComponents(Descriptor descriptor)
        {
            DiscoveredGroups groups = tryDiscoverComponentsFromTOC(descriptor);
            return groups == null
                   ? discoverComponentsFromDiskFallback(descriptor)
                   : groups;
        }


    }

    protected static IndexComponentType completionMarker(@Nullable String name)
    {
        return name == null ? IndexComponentType.GROUP_COMPLETION_MARKER : IndexComponentType.COLUMN_COMPLETION_MARKER;
    }

    /**
     * Tries reading the TOC file of the provided SSTable to discover its current SAI components.
     *
     * @param descriptor the SSTable to read the TOC file of.
     * @return the discovered components, or `null` if the TOC file is missing or if it is corrupted in some way.
     */
    protected @Nullable DiscoveredGroups tryDiscoverComponentsFromTOC(Descriptor descriptor)
    {
        Set<Component> componentsFromToc = readSAIComponentFromSSTableTOC(descriptor);
        if (componentsFromToc == null)
            return null;

        // We collect all the version/generation for which we have files on disk for the per-sstable parts and every
        // per-index found.
        Map<String, DiscoveredComponents.Builder> discovered = new HashMap<>();
        Set<String> invalid = new HashSet<>();
        for (Component component : componentsFromToc)
        {
            // We try parsing it as an SAI index name, and ignore if it doesn't match.
            var opt = Version.tryParseFileName(component.name);
            if (opt.isEmpty())
                continue;

            var parsed = opt.get();
            String indexName = parsed.indexName;

            if (invalid.contains(indexName))
                continue;

            DiscoveredComponents.Builder forGroup  = discovered.computeIfAbsent(indexName, __ -> new DiscoveredComponents.Builder());
            // Make sure it is the same version and generation that any we've seen so far.
            if (forGroup.buildId == null)
            {
                forGroup.setBuildId(parsed.buildId);
            }
            else if (!forGroup.buildId.equals(parsed.buildId))
            {
                logger.error("Found multiple versions/generations of SAI components in TOC for SSTable {}: cannot load {}",
                             descriptor, indexName == null ? "per-SSTable components" : "per-index components of " + indexName);

                discovered.remove(indexName);
                invalid.add(indexName);
                continue;
            }
            forGroup.types.add(parsed.component);
        }

        // We then do an additional pass to remove any group that is not complete.
        invalid.clear();
        for (Map.Entry<String, DiscoveredComponents.Builder> entry : discovered.entrySet())
        {
            String name = entry.getKey();
            DiscoveredComponents.Builder components = entry.getValue();

            // If it's in the TOC, it should have a completion marker: if we don't, it's either a bug in the code that
            // rewrote the TOC incorrectly, or the marker was lost. In any case, worth logging an error.
            if (!components.types.contains(completionMarker(name)))
            {
                logger.error("Found no completion marker for SAI components in TOC for SSTable {}: cannot load {}",
                             descriptor, name == null ? "per-SSTable components" : "per-index components of " + name);
                invalid.add(name);
            }
        }

        invalid.forEach(discovered::remove);
        return DiscoveredGroups.of(discovered);
    }

    private @Nullable Set<Component> readSAIComponentFromSSTableTOC(Descriptor descriptor)
    {
        try
        {
            // We skip the check for missing components on purpose: we do the existence check here because we want to
            // know when it fails.
            Set<Component> components = SSTable.readTOC(descriptor, false);
            Set<Component> SAIComponents = new HashSet<>();
            for (Component component : components)
            {
                // We only care about SAI components, which are "custom"
                if (component.type != Component.Type.CUSTOM)
                    continue;

                // And all start with "SAI" (the rest can depend on the version, but that part is common to all version)
                if (!component.name.startsWith(Version.SAI_DESCRIPTOR))
                    continue;

                // Lastly, we check that the component file exists. If it doesn't, then we assume something is wrong
                // with the TOC and we fall back to scanning the disk. This is admittedly a bit conservative, but
                // we do have test data in `test/data/legacy-sai/aa` where the TOC is broken: it lists components that
                // simply do not match the accompanying files (the index name differs), and it is unclear if this is
                // just a mistake made while gathering the test data or if some old version used to write broken TOC
                // for some reason (more precisely, it is hard to be entirely sure this isn't the later).
                // Overall, there is no real reason for the TOC to list non-existing files (typically, when we remove
                // an index, the TOC is rewritten to omit the removed component _before_ the files are deleted), so
                // falling back conservatively feels reasonable.
                if (!descriptor.fileFor(component).exists())
                {
                    noSpamLogger.warn("The TOC file for SSTable {} lists SAI component {} but it doesn't exists. Assuming the TOC is corrupted somehow and falling back on disk scanning (which may be slower)", descriptor, component.name);
                    return null;
                }

                SAIComponents.add(component);
            }
            return SAIComponents;
        }
        catch (NoSuchFileException e)
        {
            // This is totally fine when we're building an `IndexDescriptor` for a new sstable that does not exist.
            // But if the sstable exist, then that's less expected as we should have a TOC. But because we want to
            // be somewhat resilient to losing the TOC and that historically the TOC hadn't been relyed on too strongly,
            // we return `null` which trigger the fall-back path to scan disk.
            if (descriptor.fileFor(Component.DATA).exists())
            {
                noSpamLogger.warn("SSTable {} exists (its data component exists) but it has no TOC file. Will use disk scanning to discover SAI components as fallback (which may be slower).", descriptor);
                return null;
            }

            return Collections.emptySet();
        }
        catch (IOException e)
        {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Scan disk to find all the SAI components for the provided descriptor that exists on disk. Then pick
     * the approriate set of those components (the highest version/generation for which there is a completion marker).
     * This should usually only be used ask a fallback because this will scan the whole table directory every time and
     * can be a bit inefficient, especially when some tiered storage is used underneath where scanning a directory may
     * be particularly expensive. And picking the most recent version/generation is usually the right thing to do, but
     * may lack flexibility in some cases.
     *
     * @param descriptor the SSTable for which to discover components for.
     * @return the discovered components. This is never {@code null}, but could well be empty if no SAI components are
     * found.
     */
    protected DiscoveredGroups discoverComponentsFromDiskFallback(Descriptor descriptor)
    {
        // We first collect all the version/generation for which we have files on disk.
        Map<String, Map<Version, IntObjectMap<Set<IndexComponentType>>>> candidates = Maps.newHashMap();

        PathUtils.forEach(descriptor.directory.toPath(), path -> {
            String filename = path.getFileName().toString();
            // First, we skip any file that do not belong to the sstable this is a descriptor for.
            if (!filename.startsWith(descriptor.filenamePart()))
                return;

            // Then we try parsing it as an SAI index file name, and if it matches and is for the requested context,
            // add it to the candidates.
            Version.tryParseFileName(filename)
                   .ifPresent(parsed -> candidates.computeIfAbsent(parsed.indexName, __ -> new HashMap<>())
                                                  .computeIfAbsent(parsed.buildId.version(), __ -> new IntObjectHashMap<>())
                                                  .computeIfAbsent(parsed.buildId.generation(), __ -> EnumSet.noneOf(IndexComponentType.class))
                                                  .add(parsed.component));
        });

        Map<String, DiscoveredComponents.Builder> discovered = new HashMap<>();
        for (var entry : candidates.entrySet())
        {
            String name = entry.getKey();
            var candidatesForContext = entry.getValue();

            // The "active" components are then the most recent generation of the most recent version for which we have
            // a completion marker.
            IndexComponentType completionMarker = completionMarker(name);

            for (Version version : Version.ALL)
            {
                IntObjectMap<Set<IndexComponentType>> versionCandidates = candidatesForContext.get(version);
                if (versionCandidates == null)
                    continue;

                OptionalInt maxGeneration = versionCandidates.entrySet()
                                                             .stream()
                                                             .filter(e -> e.getValue().contains(completionMarker))
                                                             .mapToInt(Map.Entry::getKey)
                                                             .max();

                if (maxGeneration.isPresent())
                {
                    var components = new DiscoveredComponents.Builder();
                    components.setBuildId(ComponentsBuildId.of(version, maxGeneration.getAsInt()));
                    components.addAll(versionCandidates.get(maxGeneration.getAsInt()));
                    discovered.put(name, components);
                    break;
                }
            }
        }
        return DiscoveredGroups.of(discovered);
    }
}
