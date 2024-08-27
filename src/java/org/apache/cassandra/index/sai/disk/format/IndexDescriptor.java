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
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.lifecycle.Tracker;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.disk.io.IndexFileUtils;
import org.apache.cassandra.index.sai.disk.io.IndexInput;
import org.apache.cassandra.index.sai.disk.io.IndexOutputWriter;
import org.apache.cassandra.index.sai.disk.oldlucene.EndiannessReverserChecksumIndexInput;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.storage.StorageProvider;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileHandle;
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

    public final Descriptor descriptor;

    // For each context (null is used for per-sstable ones), the concrete set of existing and "active" components (it
    // is possible for multiple version and/or generation of a component to exists "on disk"; the "active" one is the
    // most recent version and generation that is fully built (has a completion marker)).
    private final Map<IndexContext, IndexComponentsImpl> groups = Maps.newHashMap();

    private IndexDescriptor(Descriptor descriptor)
    {
        this.descriptor = descriptor;
    }

    public static IndexDescriptor empty(Descriptor descriptor)
    {
        IndexDescriptor created = new IndexDescriptor(descriptor);
        // Some code assumes that you can always at least call `perSSTableComponents()` and not get `null`, so we
        // set it to an empty group here.
        created.populateOne(null, null);
        return created;
    }

    public static IndexDescriptor load(SSTableReader sstable, Set<IndexContext> indices)
    {
        ComponentDiscovery.DiscoveredGroups discovered = ComponentDiscovery.discoverComponents(sstable.descriptor);
        IndexDescriptor descriptor = new IndexDescriptor(sstable.descriptor);
        descriptor.populate(discovered, indices);
        return descriptor;
    }

    private void populate(ComponentDiscovery.DiscoveredGroups discovered, Collection<IndexContext> indices)
    {
        populateOne(discovered.groups.get(null), null);
        for (var context : indices)
            populateOne(discovered.groups.get(context.getIndexName()), context);
    }

    private void populateOne(@Nullable ComponentDiscovery.DiscoveredComponents discovered, @Nullable IndexContext context)
    {
        IndexComponentsImpl components;
        if (discovered == null)
        {
            // Means there isn't a complete build for this context. We add some empty "group" as a marker.
            components = createEmptyGroup(context);
        }
        else
        {
            components = new IndexComponentsImpl(context, discovered.version, discovered.generation);
            discovered.types.forEach(components::addOrGet);
            components.isComplete = true;
        }
        groups.put(context, components);
    }

    private IndexComponentsImpl createEmptyGroup(@Nullable IndexContext context)
    {
        return new IndexComponentsImpl(context, Version.latest(), -1);
    }

    /**
     * The set of components _expected_ to be written for a newly flushed sstable given the provided set of indices.
     * This includes both per-sstable and per-index components.
     * <p>
     * Please note that the final sstable may not contain all of these components, as some may be empty or not written
     * due to the specific of the flush, but this should be a superset of the components written.
     */
    public static Set<Component> componentsForNewlyFlushedSSTable(Collection<StorageAttachedIndex> indices)
    {
        Version version = Version.latest();
        Set<Component> components = new HashSet<>();
        for (IndexComponentType component : version.onDiskFormat().perSSTableComponentTypes())
            components.add(customComponentFor(version, component, null, 0));

        for (StorageAttachedIndex index : indices)
            addPerIndexComponentsForNewlyFlushedSSTable(components, version, index.getIndexContext());
        return components;
    }

    /**
     * The set of per-index components _expected_ to be written for a newly flushed sstable for the provided index.
     * <p>
     * This is a subset of {@link #componentsForNewlyFlushedSSTable(Collection)} and has the same caveats.
     */
    public static Set<Component> perIndexComponentsForNewlyFlushedSSTable(IndexContext context)
    {
        return addPerIndexComponentsForNewlyFlushedSSTable(new HashSet<>(), Version.latest(), context);
    }

    private static Set<Component> addPerIndexComponentsForNewlyFlushedSSTable(Set<Component> addTo, Version version, IndexContext context)
    {
        for (IndexComponentType component : version.onDiskFormat().perIndexComponentTypes(context))
            addTo.add(customComponentFor(version, component, context, 0));
        return addTo;
    }

    private static Component customComponentFor(Version version, IndexComponentType componentType, @Nullable IndexContext context, int generation)
    {
        return new Component(SSTableFormat.Components.Types.CUSTOM, version.fileNameFormatter().format(componentType, context, generation));
    }

    /**
     * Given the indexes for the sstable this is a descriptor for, reload from disk to check if newer components are
     * available.
     * <p>
     * This method is generally <b>not</b> safe to call concurrently with the other methods that modify the state
     * of {@link IndexDescriptor}, which are {@link #newPerSSTableComponentsForWrite()} and
     * {@link #newPerIndexComponentsForWrite(IndexContext)}. This method is in fact meant for tiered storage use-cases
     * where (post-flush) index building is done on separate dedicated services, and this method allows to reload the
     * result of such external services once it is made available locally.
     *
     * @param indices The set of indices to should part of the reloaded descriptor.
     * @return this descriptor, for chaining purpose.
     */
    public IndexDescriptor reload(Set<IndexContext> indices)
    {
        ComponentDiscovery.DiscoveredGroups discovered = ComponentDiscovery.discoverComponents(descriptor);

        // We want to make sure the descriptor only has data for the provided `indices` on reload, so we remove any
        // index data that is not in the ones provided. This essentially make sure we don't hold up memory for
        // dropped indexes.
        for (IndexContext context : new HashSet<>(groups.keySet()))
        {
            if (context != null && !indices.contains(context))
                groups.remove(context);
        }

        // Then reload data for the provided indices.
        populate(discovered, indices);
        return this;
    }

    public IndexComponents.ForRead perSSTableComponents()
    {
        return groups.get(null);
    }

    public IndexComponents.ForRead perIndexComponents(IndexContext context)
    {
        var perIndex = groups.get(context);
        return perIndex == null ? createEmptyGroup(context) : perIndex;
    }

    public IndexComponents.ForWrite newPerSSTableComponentsForWrite()
    {
        return newComponentsForWrite(null);
    }

    public IndexComponents.ForWrite newPerIndexComponentsForWrite(IndexContext context)
    {
        return newComponentsForWrite(context);
    }

    private IndexComponents.ForWrite newComponentsForWrite(@Nullable IndexContext context)
    {
        var currentComponents = groups.get(context);
        // If we're "bumping" the version compared to the existing group, then we can use generation 0. Otherwise, we
        // have to bump the generation, unless we're using immutable components, in which case we always use generation 0.
        // Unless we don't use immutable components, in which case we always use generation 0.
        Version newVersion = Version.latest();
        if (currentComponents != null && newVersion.useImmutableComponentFiles())
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

    /**
     * Returns true if the per-column index components of the provided sstable have been built and are valid.
     *
     * @param sstable The sstable to check
     * @param context The {@link IndexContext} for the index
     * @return true if the per-column index components have been built and are complete
     */
    public static boolean isIndexBuildCompleteOnDisk(SSTableReader sstable, IndexContext context)
    {
        IndexDescriptor descriptor = IndexDescriptor.load(sstable, Set.of(context));
        return descriptor.perSSTableComponents().isComplete()
               && descriptor.perIndexComponents(context).isComplete();
    }

    public boolean isIndexEmpty(IndexContext context)
    {
        return perSSTableComponents().isComplete() && perIndexComponents(context).isEmpty();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(descriptor, perSSTableComponents().version());
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IndexDescriptor other = (IndexDescriptor)o;
        return Objects.equals(descriptor, other.descriptor) &&
               Objects.equals(perSSTableComponents().version(), other.perSSTableComponents().version());
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
        public boolean validateComponents(SSTable sstable, Tracker tracker, boolean validateChecksum, boolean rethrow)
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
                else
                {
                    try
                    {
                        version().onDiskFormat().validateIndexComponent(component, validateChecksum);
                    }
                    catch (UncheckedIOException e)
                    {
                        logger.warn(logMessage("Invalid/corrupted component {} for SSTable {}"), expected, descriptor);

                        if (rethrow)
                            throw e;

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
                            logger.debug("Leaving believed-corrupt component {} of SSTable {} in place because {} is false",
                                         expected, descriptor, CassandraRelevantProperties.DELETE_CORRUPT_SAI_COMPONENTS.getKey());
                        }

                        isValid = false;
                    }
                }
            }
            if (!isValid)
                invalidate(sstable, tracker);
            return isValid;
        }

        @Override
        public void invalidate(SSTable sstable, Tracker tracker)
        {
            // This rewrite the TOC to stop listing the components, which ensures that if the node is restarted,
            // then discovery will use an empty group for that context (like we add at the end of this method).
            sstable.unregisterComponents(allAsCustomComponents(), tracker);

            // Also delete the completion marker, to make it clear the group of components shouldn't be used anymore.
            // Note it's comparatively safe to do so in that the marker is never accessed during reads, so we cannot
            // break ongoing operations here.
            var marker = components.remove(completionMarkerComponent());
            if (marker != null)
                deleteComponentFile(marker.file());

            // Keeping legacy behavior if immutable components is disabled.
            if (!version.useImmutableComponentFiles() && CassandraRelevantProperties.DELETE_CORRUPT_SAI_COMPONENTS.getBoolean())
                forceDeleteAllComponents();

            // We replace the group by an explicitly empty one.
            groups.put(context, createEmptyGroup(context));
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
            Preconditions.checkNotNull(info, "SSTable %s has no %s component for version %s and generation %s (context: %s)", descriptor, component, version, generation, context);
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
                return new Component(SSTableFormat.Components.Types.CUSTOM, fileNamePart());
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
                var builder = StorageProvider.instance.fileHandleBuilderFor(this);
                var b = builder.order(byteOrder());
                return b.complete();
            }

            @Override
            public FileHandle createIndexBuildTimeFileHandle()
            {
                final FileHandle.Builder builder = StorageProvider.instance.indexBuildTimeFileHandleBuilderFor(this);
                return builder.order(byteOrder()).complete();
            }

            @Override
            public IndexInput openInput()
            {
                return IndexFileUtils.instance().openBlockingInput(createFileHandle());
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
                    return new EndiannessReverserChecksumIndexInput(indexInput, version);
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

                return IndexFileUtils.instance().openOutput(file, byteOrder(), append, version);
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
