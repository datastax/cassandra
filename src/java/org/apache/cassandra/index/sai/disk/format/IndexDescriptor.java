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
import java.lang.invoke.MethodHandles;
import java.nio.ByteOrder;
import java.nio.file.NoSuchFileException;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.util.collection.IntObjectHashMap;
import io.netty.util.collection.IntObjectMap;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.db.lifecycle.Tracker;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SSTableContext;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.disk.EmptyIndex;
import org.apache.cassandra.index.sai.disk.PerIndexWriter;
import org.apache.cassandra.index.sai.disk.PerSSTableWriter;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.SearchableIndex;
import org.apache.cassandra.index.sai.disk.io.IndexInput;
import org.apache.cassandra.index.sai.disk.io.IndexOutputWriter;
import org.apache.cassandra.index.sai.disk.oldlucene.EndiannessReverserChecksumIndexInput;
import org.apache.cassandra.index.sai.memory.RowMapping;
import org.apache.cassandra.index.sai.utils.IndexFileUtils;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.storage.StorageProvider;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.io.util.PathUtils;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.lucene.store.BufferedChecksumIndexInput;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.util.IOUtils;

/**
 * The `IndexDescriptor` is an analog of the SSTable {@link Descriptor} and provides version
 * specific information about the on-disk state of {@link StorageAttachedIndex}es.
 * <p>
 * The `IndexDescriptor` is primarily responsible for maintaining a view of the on-disk state
 * of the SAI indexes for a specific {@link org.apache.cassandra.io.sstable.SSTable}. It maintains mappings
 * of the current on-disk components and files. It is responsible for opening files for use by
 * writers and readers.
 * <p>
 * Each sstable has per-index components ({@link IndexComponentType}) associated with it, and also components
 * that are shared by all indexes (notably, the components that make up the PrimaryKeyMap).
 * <p>
 * IndexDescriptor's remaining responsibility is to act as a proxy to the {@link OnDiskFormat}
 * associated with the index {@link Version}.
 */
public class IndexDescriptor
{
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private static final NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(logger, 1, TimeUnit.MINUTES);

    // TODO Because indexes can be added at any time to existing data, the Version of a column index
    // may not match the Version of the base sstable.  OnDiskFormat + IndexFeatureSet + IndexDescriptor
    // was not designed with this in mind, leading to some awkwardness, notably in IFS where some features
    // are per-sstable (`isRowAware`) and some are per-column (`hasVectorIndexChecksum`).

    // per-SSTable fields
    public final Descriptor descriptor;
    public final IPartitioner partitioner;
    public final ClusteringComparator clusteringComparator;
    public final PrimaryKey.Factory primaryKeyFactory;

    // For each context (null is used for per-sstable ones), the concrete set of existing and "active" components (it is
    // possible for multiple version and/or generation of a component to exists "on disk"; the "active" one is the most
    // recent version and generation that is fully built (has a completion marker)).
    private final Map<IndexContext, IndexComponentsImpl> groups = Maps.newHashMap();

    private IndexDescriptor(Descriptor descriptor, IPartitioner partitioner, ClusteringComparator clusteringComparator)
    {
        this.descriptor = descriptor;
        this.partitioner = partitioner;
        this.clusteringComparator = clusteringComparator;

        // Populating the per-sstable components. Not that this needs to happen first to have the proper version below.
        populatePerSSTableComponents();
        assert groups.containsKey(null);

        this.primaryKeyFactory = PrimaryKey.factory(clusteringComparator, getVersion().onDiskFormat().indexFeatureSet());
    }

    private void populatePerSSTableComponents()
    {
        populateComponents( null);
    }

    private void maybePopulateComponents(IndexContext context)
    {
        if (!groups.containsKey(context))
            populateComponents(context);

        assert groups.containsKey(context);
    }

    private void populateComponents(@Nullable IndexContext context)
    {
        if (tryPopulateComponentsFromTOC(context))
            return;

        discoverComponentsFromDiskFallback(context);
    }

    private boolean tryPopulateComponentsFromTOC(@Nullable IndexContext context)
    {
        Set<Component> componentsFromToc = readSAIComponentFromSSTableTOC(descriptor);
        if (componentsFromToc == null)
            return false;

        // We first collect all the version/generation for which we have files on disk.
        String indexName = context == null ? null : context.getIndexName();
        Version version = null;
        int generation = -1;
        Set<IndexComponentType> foundTypes = EnumSet.noneOf(IndexComponentType.class);
        for (Component component : componentsFromToc)
        {
            // We try parsing it as an SAI index name, and ignore if it doesn't match
            // candidates.
            var opt = Version.tryParseFileName(component.name);
            if (opt.isEmpty())
                continue;

            var parsed = opt.get();
            if (!Objects.equals(parsed.indexName, indexName))
                continue;

            // It is a component we're looking for. Make sure it is the same version and generation that any we've
            // seen so far.
            if (version == null)
            {
                version = parsed.version;
                generation = parsed.generation;
            }
            else if (!version.equals(parsed.version) || generation != parsed.generation)
            {
                logger.error("Found multiple versions/generations of SAI components in TOC for SSTable {}: cannot load {}",
                            descriptor, context == null ? "per-SSTable components" : "per-index components of " + indexName);
                // Reset to null so the "default" empty group is used as we exit the loop
                version = null;
                break;
            }

            foundTypes.add(parsed.component);
        }

        IndexComponentType completionMarkerType = context == null
                                                  ? IndexComponentType.GROUP_COMPLETION_MARKER
                                                  : IndexComponentType.COLUMN_COMPLETION_MARKER;
        if (version != null)
        {
            // If it's in the TOC, it should have a completion marker: if we don't, it's either a bug in the code that
            // rewrote the TOC incorrectly, or the marker was lost. In any case, worth logging an error.
            if (!foundTypes.contains(completionMarkerType))
            {
                logger.error("Found no completion marker for SAI components in TOC for SSTable {}: cannot load {}",
                             descriptor, context == null ? "per-SSTable components" : "per-index components of " + indexName);
            }
            else
            {
                IndexComponentsImpl components = new IndexComponentsImpl(context, version, generation);
                foundTypes.forEach(components::addOrGet);
                components.isComplete = true;
                groups.put(context, components);
                return true;
            }
        }

        // If we get here, we haven't found any set of valid components. We register an empty group "marker" for the current
        // version (but invalid generation -1) to avoid re-reading the TOC for the same result and indicate we now know
        // what version we should use for a new build.
        groups.put(context, new IndexComponentsImpl(context, Version.latest(), -1));
        return true;
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
                if (!component.name.startsWith("SAI"))
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
                    noSpamLogger.warn("The TOC file for SSTable {} lists SAI component {} but it doesn't exists. Assuming the TOC is corrupted somehow and falling back on disk scanning (which may be slower)");
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

    // San disk to find all the SAI components that belong to the provided context and exists on disk. Then pick
    // the approriate set of those components (the highest version/generation for which there is a completion marker).
    // This is only use a fallback when there is no TOC file for the sstable because this will scan the whole
    // table directory every time and this can be a bit inefficient, especially when some tiered storage is used
    // underneath where scanning a directory may be particularly expensive.
    private void discoverComponentsFromDiskFallback(@Nullable IndexContext context)
    {
        // We first collect all the version/generation for which we have files on disk.
        String indexName = context == null ? null : context.getIndexName();
        Map<Version, IntObjectMap<Set<IndexComponentType>>> candidates = Maps.newHashMap();

        PathUtils.forEach(descriptor.directory.toPath(), path -> {
            String filename = path.getFileName().toString();
            // First, we skip any file that do not belong to the sstable this is a descriptor for.
            if (!filename.startsWith(descriptor.filenamePart()))
                return;

            // Then we try parsing it as an SAI index file name, and if it matches and is for the requested context,
            // add it to the candidates.
            Version.tryParseFileName(filename)
                   .ifPresent(parsed -> {
                       if (Objects.equals(parsed.indexName, indexName))
                       {
                           candidates.computeIfAbsent(parsed.version, __ -> new IntObjectHashMap<>())
                                     .computeIfAbsent(parsed.generation, __ -> EnumSet.noneOf(IndexComponentType.class))
                                     .add(parsed.component);
                       }
                   });
        });

        // The "active" components are then the most recent generation of the most recent version for which we have
        // a completion marker.
        IndexComponentType completionMarker = context == null
                                          ? IndexComponentType.GROUP_COMPLETION_MARKER
                                          : IndexComponentType.COLUMN_COMPLETION_MARKER;

        for (Version version : Version.ALL)
        {
            IntObjectMap<Set<IndexComponentType>> versionCandidates = candidates.get(version);
            if (versionCandidates == null)
                continue;

            OptionalInt maxGeneration = versionCandidates.entrySet()
                                                         .stream()
                                                         .filter(entry -> entry.getValue().contains(completionMarker))
                                                         .mapToInt(Map.Entry::getKey)
                                                         .max();

            if (maxGeneration.isPresent())
            {
                IndexComponentsImpl components = new IndexComponentsImpl(context, version, maxGeneration.getAsInt());
                versionCandidates.get(maxGeneration.getAsInt()).forEach(components::addOrGet);
                components.isComplete = true;
                groups.put(context, components);
                return;
            }
        }
        // If we get here, we haven't found any set of valid components. We register an empty group "marker" for the
        // current version (but invalid generation -1) to avoid re-scanning the disk for the same result.
        groups.put(context, new IndexComponentsImpl(context, Version.latest(), -1));
    }

    public static IndexDescriptor create(SSTableReader sstable)
    {
        return create(sstable.descriptor, sstable.metadata());
    }

    public static IndexDescriptor create(Descriptor descriptor, TableMetadata metadata)
    {
        return create(descriptor, metadata.partitioner, metadata.comparator);
    }

    // Should not be used directly. Only exists for tests.
    @VisibleForTesting
    public static IndexDescriptor create(Descriptor descriptor, IPartitioner partitioner, ClusteringComparator clusteringComparator)
    {
        return new IndexDescriptor(descriptor, partitioner, clusteringComparator);
    }

    public IndexComponents.ForRead perSSTableComponents()
    {
        return groups.get(null);
    }

    public IndexComponents.ForRead perIndexComponents(IndexContext context)
    {
        maybePopulateComponents(context);
        return groups.get(context);
    }

    public IndexComponents.ForWrite newPerSSTableComponentsForWrite()
    {
        return newComponentsForWrite(null);
    }

    public IndexComponents.ForWrite newPerIndexComponentsForWrite(IndexContext context)
    {
        maybePopulateComponents(context);
        return newComponentsForWrite(context);
    }

    private IndexComponents.ForWrite newComponentsForWrite(@Nullable IndexContext context)
    {
        var currentComponents = groups.get(context);
        // If we're "bumping" the version compared to the existing group, then we can use generation 0. Otherwise, we
        // have to bump the generation, unless we're using immutable components, in which case we always use generation 0.
        // Unless we don't use immutable components, in which case we always use generation 0.
        Version newVersion = Version.latest();
        if (newVersion.useImmutableComponentFiles())
        {
            int candidateGeneration = currentComponents.version().equals(newVersion)
                                      ? currentComponents.generation() + 1
                                      : 0;
            // Usually, we'll just use `candidateGeneration`, but we want to avoid overriding existing file (it's
            // theoretically possible that the next generation was created at some other point, but then corrupted,
            // and so we falled back on the previous generation but some of those file for the next generation still
            // exists). So we check repeatedly increment the generation until we find one for which no files exist.
            return createFirstGenerationAvailableComponents(context, newVersion, candidateGeneration);
        }
        else
        {
            return new IndexComponentsImpl(context, newVersion, 0);
        }
    }

    private IndexComponentsImpl createFirstGenerationAvailableComponents(@Nullable IndexContext context, Version version, int startGeneration)
    {
        int generationToTest = startGeneration;
        while (true)
        {
            IndexComponentsImpl candidate = new IndexComponentsImpl(context, version, generationToTest);
            if (candidate.expectedComponentsForVersion().stream().noneMatch(candidate::componentExistsOnDisk))
                return candidate;

            noSpamLogger.warn(logMessage("Wanted to use generation {} for new build of {} SAI components of {}, but found some existing components on disk for that generation (maybe leftover from an incomplete/corrupted build?); trying next generation"),
                              generationToTest,
                              context == null ? "per-SSTable" : "per-index",
                              descriptor);
            generationToTest++;
        }
    }

    public Version getVersion()
    {
        return perSSTableComponents().version();
    }

    public Version getVersion(IndexContext context)
    {
        return perIndexComponents(context).version();
    }

    public PrimaryKeyMap.Factory newPrimaryKeyMapFactory(SSTableReader sstable) throws IOException
    {
        return getVersion().onDiskFormat().newPrimaryKeyMapFactory(this, sstable);
    }

    public SearchableIndex newSearchableIndex(SSTableContext sstableContext, IndexContext context)
    {
        return isIndexEmpty(context)
               ? new EmptyIndex()
               : getVersion(context).onDiskFormat().newSearchableIndex(sstableContext, context);
    }

    public PerSSTableWriter newPerSSTableWriter() throws IOException
    {
        return perSSTableComponents().version().onDiskFormat().newPerSSTableWriter(this);
    }

    public PerIndexWriter newPerIndexWriter(StorageAttachedIndex index,
                                            LifecycleNewTracker tracker,
                                            RowMapping rowMapping,
                                            long keyCount)
    {
        return Version.latest().onDiskFormat().newPerIndexWriter(index, this, tracker, rowMapping, keyCount);
    }

    /**
     * Returns true if the per-column index components have been built and are valid.
     *
     * @param context The {@link IndexContext} for the index
     * @return true if the per-column index components have been built and are complete
     */
    public boolean isPerIndexBuildComplete(IndexContext context)
    {
        return perSSTableComponents().isComplete() && perIndexComponents(context).isComplete();
    }

    public boolean isSSTableEmpty()
    {
        return perSSTableComponents().isEmpty();
    }

    public boolean isIndexEmpty(IndexContext context)
    {
        return perSSTableComponents().isComplete() && perIndexComponents(context).isEmpty();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(descriptor, getVersion());
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IndexDescriptor other = (IndexDescriptor)o;
        return Objects.equals(descriptor, other.descriptor) &&
               Objects.equals(getVersion(), other.getVersion());
    }

    @Override
    public String toString()
    {
        return descriptor.toString() + "-SAI";
    }

    public String logMessage(String message)
    {
        // Index names are unique only within a keyspace.
        return String.format("[%s.%s.*] %s",
                             descriptor.ksname,
                             descriptor.cfname,
                             message);
    }

    private static void deleteComponentFile(File file)
    {
        logger.debug("Deleting storage attached index component file {}", file);
        try
        {
            IOUtils.deleteFilesIfExist(file.toPath());
        }
        catch (IOException e)
        {
            logger.warn("Unable to delete storage attached index component file {} due to {}.", file, e.getMessage(), e);
        }
    }

    private class IndexComponentsImpl implements IndexComponents.ForWrite
    {
        private final @Nullable IndexContext context;
        private final Version version;
        private final int generation;

        private final Map<IndexComponentType, IndexComponentImpl> components = new EnumMap<>(IndexComponentType.class);

        // Mark groups that are complete (and should not have new components added).
        private volatile boolean isComplete;

        private IndexComponentsImpl(@Nullable IndexContext context, Version version, int generation)
        {
            this.context = context;
            this.version = version;
            this.generation = generation;
        }

        private boolean componentExistsOnDisk(IndexComponentType component)
        {
            return new IndexComponentImpl(component).file().exists();
        }

        @Override
        public Descriptor descriptor()
        {
            return descriptor;
        }

        @Override
        public IndexDescriptor indexDescriptor()
        {
            return IndexDescriptor.this;
        }

        @Nullable
        @Override
        public IndexContext context()
        {
            return context;
        }

        @Override
        public Version version()
        {
            return version;
        }

        @Override
        public int generation()
        {
            return generation;
        }

        @Override
        public boolean has(IndexComponentType component)
        {
            return components.containsKey(component);
        }

        @Override
        public boolean isEmpty()
        {
            return isComplete() && components.size() == 1;
        }

        @Override
        public Collection<IndexComponent.ForRead> all()
        {
            return Collections.unmodifiableCollection(components.values());
        }

        @Override
        public boolean validateComponents(SSTable sstable, Tracker tracker, boolean validateChecksum)
        {
            if (isEmpty())
                return true;

            boolean isValid = true;
            for (IndexComponentType expected : expectedComponentsForVersion())
            {
                var component = components.get(expected);
                if (component == null)
                {
                    logger.warn(logMessage("Missing index component {} from SSTable {}"), expected, descriptor);
                    isValid = false;
                }
                else if (!version().onDiskFormat().validateIndexComponent(component, validateChecksum))
                {
                    logger.warn(logMessage("Invalid/corrupted component {} for SSTable {}"), expected, descriptor);
                    if (CassandraRelevantProperties.DELETE_CORRUPT_SAI_COMPONENTS.getBoolean())
                    {
                        // We delete the corrupted file. Yes, this may break ongoing reads to that component, but
                        // if something is wrong with the file, we're rather fail loudly from that point on than
                        // risking reading and returning corrupted data.
                        deleteComponentFile(component.file());
                        // Note that invalidation will also delete the completion marker
                    }
                    else
                    {
                        logger.debug("Leaving believed-corrupt component {} of SSTable {} in place because {} is false", expected, descriptor, CassandraRelevantProperties.DELETE_CORRUPT_SAI_COMPONENTS.getKey());
                    }

                    isValid = false;
                }
            }
            if (!isValid)
                invalidate(sstable, tracker);
            return isValid;
        }

        @Override
        public void invalidate(SSTable sstable, Tracker tracker)
        {
            // This rewrite the TOC to stop listing the components, which ensures that the `populateComponents` call
            // at the end of this method create an "empty" group.
            sstable.unregisterComponents(allAsCustomComponents(), tracker);

            // We delete the completion marker, to make it clear the group of components shouldn't be used anymore,
            // in particular for the following "populate" call. Note it's comparatively safe to do so in that the
            // marker is never accessed during reads, so we cannot break ongoing operations here.
            var marker = components.remove(completionMarkerComponent());
            if (marker != null)
                deleteComponentFile(marker.file());

            // Keeping legacy behavior if immutable components is disabled.
            if (!version.useImmutableComponentFiles() && CassandraRelevantProperties.DELETE_CORRUPT_SAI_COMPONENTS.getBoolean())
                forceDeleteAllComponents();

            groups.remove(context);
            IndexDescriptor.this.populateComponents(context);
        }

        @Override
        public ForWrite forWrite()
        {
            // The difference between Reader and Writer is just to make code cleaner and make it clear when we read
            // components from when we write/modify them. But this concrete implementatation is both in practice.
            return this;
        }

        @Override
        public IndexComponent.ForRead get(IndexComponentType component)
        {
            IndexComponentImpl info = components.get(component);
            Preconditions.checkNotNull(info, "SSTable %s has no %s component for version %s and generation %s", descriptor, component, version, generation);
            return info;
        }

        @Override
        public long liveSizeOnDiskInBytes()
        {
            return components.values().stream().map(IndexComponentImpl::file).mapToLong(File::length).sum();
        }

        @Override
        public IndexComponent.ForWrite addOrGet(IndexComponentType component)
        {
            Preconditions.checkArgument(!isComplete, "Should not add components for SSTable %s at this point; the completion marker has already been written", descriptor);
            // When a sstable doesn't have any complete group, we use a marker empty one with a generation of -1:
            Preconditions.checkArgument(generation >= 0, "Should not be adding component to empty components");
            return components.computeIfAbsent(component, IndexComponentImpl::new);
        }

        @Override
        public void forceDeleteAllComponents()
        {
            components.values()
                      .stream()
                      .map(IndexComponentImpl::file)
                      .forEach(IndexDescriptor::deleteComponentFile);
            components.clear();
        }

        @Override
        public void markComplete() throws IOException
        {
            addOrGet(completionMarkerComponent()).createEmpty();
            isComplete = true;
            groups.put(context, this);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(descriptor, context, version, generation);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            IndexComponentsImpl that = (IndexComponentsImpl) o;
            return Objects.equals(descriptor, that.descriptor())
                   && Objects.equals(context, that.context)
                   && Objects.equals(version, that.version)
                   && generation == that.generation;
        }

        @Override
        public String toString()
        {
            return String.format("%s components for %s (v: %s, gen: %d): %s",
                                 context == null ? "Per-SSTable" : "Per-Index",
                                 descriptor,
                                 version,
                                 generation,
                                 components.values());
        }

        private class IndexComponentImpl implements IndexComponent.ForRead, IndexComponent.ForWrite
        {
            private final IndexComponentType component;

            private volatile String filenamePart;
            private volatile File file;

            private IndexComponentImpl(IndexComponentType component)
            {
                this.component = component;
            }

            @Override
            public IndexComponentsImpl parent()
            {
                return IndexComponentsImpl.this;
            }

            @Override
            public IndexComponentType componentType()
            {
                return component;
            }

            @Override
            public ByteOrder byteOrder()
            {
                return version.onDiskFormat().byteOrderFor(component, context);
            }

            @Override
            public String fileNamePart()
            {
                // Not thread-safe, but not really the end of the world if called multiple time
                if (filenamePart == null)
                    filenamePart = version.fileNameFormatter().format(component, context, generation);
                return filenamePart;
            }

            @Override
            public Component asCustomComponent()
            {
                return new Component(Component.Type.CUSTOM, fileNamePart());
            }

            @Override
            public File file()
            {
                // Not thread-safe, but not really the end of the world if called multiple time
                if (file == null)
                    file = descriptor.fileFor(asCustomComponent());
                return file;
            }

            @Override
            public FileHandle createFileHandle()
            {
                try (final FileHandle.Builder builder = StorageProvider.instance.fileHandleBuilderFor(this))
                {
                    return builder.order(byteOrder()).complete();
                }
            }

            @Override
            public FileHandle createFlushTimeFileHandle()
            {
                try (final FileHandle.Builder builder = StorageProvider.instance.flushTimeFileHandleBuilderFor(this))
                {
                    return builder.order(byteOrder()).complete();
                }
            }

            @Override
            public IndexInput openInput()
            {
                return IndexFileUtils.instance.openBlockingInput(createFileHandle());
            }

            @Override
            public ChecksumIndexInput openCheckSummedInput()
            {
                var indexInput = openInput();
                return checksumIndexInput(indexInput);
            }

            /**
             * Returns a ChecksumIndexInput that reads the indexInput in the correct endianness for the context.
             * These files were written by the Lucene {@link org.apache.lucene.store.DataOutput}. When written by
             * Lucene 7.5, {@link org.apache.lucene.store.DataOutput} wrote the file using big endian formatting.
             * After the upgrade to Lucene 9, the {@link org.apache.lucene.store.DataOutput} writes in little endian
             * formatting.
             *
             * @param indexInput The index input to read
             * @return A ChecksumIndexInput that reads the indexInput in the correct endianness for the context
             */
            private ChecksumIndexInput checksumIndexInput(IndexInput indexInput)
            {
                if (version == Version.AA)
                    return new EndiannessReverserChecksumIndexInput(indexInput);
                else
                    return new BufferedChecksumIndexInput(indexInput);
            }

            @Override
            public IndexOutputWriter openOutput(boolean append) throws IOException
            {
                File file = file();

                if (logger.isTraceEnabled())
                    logger.trace(this.parent().logMessage("Creating SSTable attached index output for component {} on file {}..."),
                                 component,
                                 file);

                return IndexFileUtils.instance.openOutput(file, byteOrder(), append);
            }

            @Override
            public void createEmpty() throws IOException
            {
                com.google.common.io.Files.touch(file().toJavaIOFile());
            }

            @Override
            public int hashCode()
            {
                return Objects.hash(this.parent(), component);
            }

            @Override
            public boolean equals(Object o)
            {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                IndexComponentImpl that = (IndexComponentImpl) o;
                return Objects.equals(this.parent(), that.parent())
                       && component == that.component;
            }

            @Override
            public String toString()
            {
                return file().toString();
            }
        }
    }
}
