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

import com.google.common.collect.Maps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.util.collection.IntObjectHashMap;
import io.netty.util.collection.IntObjectMap;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.util.PathUtils;
import org.apache.cassandra.utils.NoSpamLogger;

/**
 * Helper methods/classes used by `IndexDescriptor` to discover, from disk, the index components that a sstable has.
 * <p>
 * Not meant to be exposed outside this package; the public facing API for components is {@link IndexDescriptor}.
 */
class ComponentDiscovery
{
    private static final Logger logger = LoggerFactory.getLogger(ComponentDiscovery.class);
    private static final NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(logger, 1, TimeUnit.MINUTES);

    static class DiscoveredGroups
    {
        Map<String, DiscoveredComponents> groups = new HashMap<>();

        @Override
        public String toString()
        {
            return groups.entrySet().stream().map(e -> e.getKey() + ": " + e.getValue()).collect(Collectors.joining(", ", "{", "}"));
        }
    }

    static class DiscoveredComponents
    {
        Version version;
        int generation;
        final Set<IndexComponentType> types = EnumSet.noneOf(IndexComponentType.class);

        @Override
        public String toString()
        {
            return String.format("version=%s, generation=%d, types=%s", version, generation, types);
        }
    }

    static DiscoveredGroups discoverComponents(Descriptor descriptor)
    {
        DiscoveredGroups groups = tryDiscoverComponentsFromTOC(descriptor);
        return groups == null
               ? discoverComponentsFromDiskFallback(descriptor)
               : groups;
    }

    private static IndexComponentType completionMarker(@Nullable String name)
    {
        return name == null ? IndexComponentType.GROUP_COMPLETION_MARKER : IndexComponentType.COLUMN_COMPLETION_MARKER;
    }

    private static @Nullable DiscoveredGroups tryDiscoverComponentsFromTOC(Descriptor descriptor)
    {
        Set<Component> componentsFromToc = readSAIComponentFromSSTableTOC(descriptor);
        if (componentsFromToc == null)
            return null;

        // We collect all the version/generation for which we have files on disk for the per-sstable parts and every
        // per-index found.
        DiscoveredGroups discovered = new DiscoveredGroups();
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

            DiscoveredComponents forGroup = discovered.groups.computeIfAbsent(indexName, __ -> new DiscoveredComponents());
            // Make sure it is the same version and generation that any we've seen so far.
            if (forGroup.version == null)
            {
                forGroup.version = parsed.version;
                forGroup.generation = parsed.generation;
            }
            else if (!forGroup.version.equals(parsed.version) || forGroup.generation != parsed.generation)
            {
                logger.error("Found multiple versions/generations of SAI components in TOC for SSTable {}: cannot load {}",
                             descriptor, indexName == null ? "per-SSTable components" : "per-index components of " + indexName);

                discovered.groups.remove(indexName);
                invalid.add(indexName);
                continue;
            }
            forGroup.types.add(parsed.component);
        }

        // We then do an additional pass to remove any group that is not complete.
        invalid.clear();
        for (Map.Entry<String, DiscoveredComponents> entry : discovered.groups.entrySet())
        {
            String name = entry.getKey();
            DiscoveredComponents components = entry.getValue();

            // If it's in the TOC, it should have a completion marker: if we don't, it's either a bug in the code that
            // rewrote the TOC incorrectly, or the marker was lost. In any case, worth logging an error.
            if (!components.types.contains(completionMarker(name)))
            {
                logger.error("Found no completion marker for SAI components in TOC for SSTable {}: cannot load {}",
                             descriptor, name == null ? "per-SSTable components" : "per-index components of " + name);
                invalid.add(name);
            }
        }

        invalid.forEach(discovered.groups::remove);
        return discovered;
    }

    // Returns `null` if something fishy happened while reading the components from the TOC and we should fall back
    // to `discoverComponentsFromDiskFallback` for safety.
    private static @Nullable Set<Component> readSAIComponentFromSSTableTOC(Descriptor descriptor)
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

    // San disk to find all the SAI components for the provided descriptor that exists on disk. Then pick
    // the approriate set of those components (the highest version/generation for which there is a completion marker).
    // This is only use a fallback when there is no TOC file for the sstable because this will scan the whole
    // table directory every time and this can be a bit inefficient, especially when some tiered storage is used
    // underneath where scanning a directory may be particularly expensive.
    private static DiscoveredGroups discoverComponentsFromDiskFallback(Descriptor descriptor)
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
                                                  .computeIfAbsent(parsed.version, __ -> new IntObjectHashMap<>())
                                                  .computeIfAbsent(parsed.generation, __ -> EnumSet.noneOf(IndexComponentType.class))
                                                  .add(parsed.component));
        });

        DiscoveredGroups discovered = new DiscoveredGroups();
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
                    var components = new DiscoveredComponents();
                    components.version = version;
                    components.generation = maxGeneration.getAsInt();
                    components.types.addAll(versionCandidates.get(maxGeneration.getAsInt()));
                    discovered.groups.put(name, components);
                    break;
                }
            }
        }
        return discovered;
    }
}
