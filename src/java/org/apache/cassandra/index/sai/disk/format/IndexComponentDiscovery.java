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
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.util.PathUtils;
import org.apache.cassandra.schema.TableMetadata;
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


    // This works around potential (if very unlikely in our case) class-loading issues.
    private static class LazyDiscoveryInitialization
    {
        private static final IndexComponentDiscovery instance = !CUSTOM_SAI_INDEX_COMPONENTS_DISCOVERY.isPresent()
                                                        ? new DefaultIndexComponentDiscovery() {}
                                                        : FBUtilities.construct(CUSTOM_SAI_INDEX_COMPONENTS_DISCOVERY.getString(), "SAI index components discovery");
    }

    public static IndexComponentDiscovery instance()
    {
        return LazyDiscoveryInitialization.instance;
    }

    /**
     * Returns the set of groups of SAI components that should be used for the provided sstable.
     * <p>
     * Note that "discovery" in this method only means finding out the "build ID" (version and generation) that should
     * be used for each group of components (per-sstable and per-index).
     *
     * @param descriptor the descriptor of the sstable for which to discover components.
     * @param metadata the metadata of the table the sstable belongs to.
     * @return the discovered {@link ComponentsBuildId} to use for both per-sstable and each per-index components. The
     * returned build IDs should usually correspond to existing index components on disk but this is not a strong
     * asumption: if some group of components corresponding to the returned build ID has no completion marker or is
     * missing files, the group will not be usuable (and the corresponding index/indexes will not be usable) but this
     * should be handled "gracefully" by callers.
     */
    public abstract SSTableIndexComponentsState discoverComponents(Descriptor descriptor, TableMetadata metadata);

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
    protected @Nullable SSTableIndexComponentsState tryDiscoverComponentsFromTOC(Descriptor descriptor)
    {
        Set<Component> componentsFromToc = readSAIComponentFromSSTableTOC(descriptor);
        if (componentsFromToc == null)
            return null;

        // We collect all the version/generation for which we have files on disk for the per-sstable parts and every
        // per-index found.
        Map<String, StateBuilder> states = new HashMap<>();
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

            var prev = states.computeIfAbsent(indexName, k -> new StateBuilder(parsed.buildId));
            if (!prev.buildId.equals(parsed.buildId))
            {
                logger.error("Found multiple versions/generations of SAI components in TOC for SSTable {}: cannot load {}",
                             descriptor, indexName == null ? "per-SSTable components" : "per-index components of " + indexName);

                states.remove(indexName);
                invalid.add(indexName);
            }

            prev.totalSizeInBytes += descriptor.fileFor(component).length();
        }

        return StateBuilder.convert(states);
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
    protected SSTableIndexComponentsState discoverComponentsFromDiskFallback(Descriptor descriptor)
    {
        // For each "component group" (of each individual index, plus the per-sstable group), the "active" group is the
        // one with the most recent build amongst complete ones. So we scan disk looking for completion markers (since
        // that's what tell us a group is complete), and keep for each group the max build we find.
        Map<String, Map<ComponentsBuildId, StateBuilder>> states = new HashMap<>();
        PathUtils.forEach(descriptor.directory.toPath(), path -> {
            String filename = path.getFileName().toString();
            // First, we skip any file that do not belong to the sstable this is a descriptor for.
            if (!filename.startsWith(descriptor.filenamePart()))
                return;

            Version.tryParseFileName(filename)
                   .ifPresent(parsed -> {
                       var forGroup = states.computeIfAbsent(parsed.indexName, k -> new HashMap<>());
                       var state = forGroup.computeIfAbsent(parsed.buildId, k -> new StateBuilder(parsed.buildId));
                       state.totalSizeInBytes += PathUtils.size(path);
                       if (parsed.component == completionMarker(parsed.indexName))
                           state.isComplete = true;
                   });
        });

        Map<String, StateBuilder> maxStates = new HashMap<>();
        for (var entry : states.entrySet())
        {
            entry.getValue()
                 .values()
                 .stream()
                 .filter(s -> s.isComplete)
                 .max(Comparator.comparing(s -> s.buildId))
                 .ifPresent(max -> maxStates.put(entry.getKey(), max));
        }

        return StateBuilder.convert(maxStates);
    }

    private static class StateBuilder
    {
        private final ComponentsBuildId buildId;
        private long totalSizeInBytes;
        private boolean isComplete;

        StateBuilder(ComponentsBuildId buildId)
        {
            this.buildId = buildId;
        }

        void addTo(SSTableIndexComponentsState.Builder builder, @Nullable String indexName)
        {
            if (indexName == null)
                builder.addPerSSTable(buildId, totalSizeInBytes);
            else
                builder.addPerIndex(indexName, buildId, totalSizeInBytes);
        }

        static SSTableIndexComponentsState convert(Map<String, StateBuilder> states)
        {
            SSTableIndexComponentsState.Builder builder = SSTableIndexComponentsState.builder();
            states.forEach((indexName, state) -> state.addTo(builder, indexName));
            return builder.build();
        }
    }
}
