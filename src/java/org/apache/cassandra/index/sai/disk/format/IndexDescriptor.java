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
import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SSTableContext;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.disk.EmptyIndex;
import org.apache.cassandra.index.sai.disk.PerIndexWriter;
import org.apache.cassandra.index.sai.disk.PerSSTableWriter;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.SearchableIndex;
import org.apache.cassandra.index.sai.disk.io.IndexOutputWriter;
import org.apache.cassandra.index.sai.memory.RowMapping;
import org.apache.cassandra.index.sai.utils.IndexFileUtils;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.storage.StorageProvider;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.lucene.store.IndexInput;
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
 * Each sstable has per-index components ({@link IndexComponent}) associated with it, and also components
 * that are shared by all indexes (notably, the components that make up the PrimaryKeyMap).
 * <p>
 * IndexDescriptor's remaining responsibility is to act as a proxy to the {@link OnDiskFormat}
 * associated with the index {@link Version}.
 */
public class IndexDescriptor
{
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    // TODO Because indexes can be added at any time to existing data, the Version of a column index
    // may not match the Version of the base sstable.  OnDiskFormat + IndexFeatureSet + IndexDescriptor
    // was not designed with this in mind, leading to some awkwardness, notably in IFS where some features
    // are per-sstable (`isRowAware`) and some are per-column (`hasVectorIndexChecksum`).

    // per-SSTable fields
    public final Descriptor descriptor;
    public final IPartitioner partitioner;
    public final ClusteringComparator clusteringComparator;
    public final PrimaryKey.Factory primaryKeyFactory;

    // turns an index context or name into a unique identifier that can be identity-compared
    private final ComponentGroupId.Provider idProvider = new ComponentGroupId.Provider();
    // group -> version
    private final Map<ComponentGroupId, Version> versions = Maps.newIdentityHashMap();
    // group -> components
    private final Map<ComponentGroupId, Set<IndexComponent>> components = Maps.newIdentityHashMap();
    // component -> file
    private final Map<AttachedIndexComponent, File> fileMap = Maps.newHashMap();

    /**
     * A component together with the group it belongs to.
     */
    private class AttachedIndexComponent
    {
        public final IndexComponent component;
        public final ComponentGroupId id;

        public AttachedIndexComponent(IndexComponent component, IndexContext context)
        {
            this(component, idProvider.get(context));
        }

        public AttachedIndexComponent(IndexComponent component, ComponentGroupId id)
        {
            this.component = component;
            this.id = id;
        }

        @Override
        public int hashCode()
        {
            return Objects.hashCode(component, id);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;
            AttachedIndexComponent other = (AttachedIndexComponent)obj;
            return component == other.component && id == other.id;
        }
    }

    private IndexDescriptor(Version version, Descriptor descriptor, IPartitioner partitioner, ClusteringComparator clusteringComparator)
    {
        this.descriptor = descriptor;
        this.partitioner = partitioner;
        this.clusteringComparator = clusteringComparator;
        this.primaryKeyFactory = PrimaryKey.factory(clusteringComparator, version.onDiskFormat().indexFeatureSet());

        versions.put(ComponentGroupId.SSTABLE, version);
        components.put(ComponentGroupId.SSTABLE, Sets.newHashSet());
    }

    public static IndexDescriptor createNew(Descriptor descriptor, IPartitioner partitioner, ClusteringComparator clusteringComparator)
    {
        return new IndexDescriptor(Version.LATEST, descriptor, partitioner, clusteringComparator);
    }

    public static IndexDescriptor createFrom(SSTableReader sstable)
    {
        // see if we have a completion component on disk, and if so use that version
        for (Version version : Version.ALL)
        {
            if (componentExistsOnDisk(version, sstable.descriptor, IndexComponent.GROUP_COMPLETION_MARKER, null))
                return new IndexDescriptor(version,
                                           sstable.descriptor,
                                           sstable.metadata().partitioner,
                                           sstable.metadata().comparator);
        }
        // we always want a non-null IndexDescriptor, even if it's empty
        return new IndexDescriptor(Version.LATEST,
                                   sstable.descriptor,
                                   sstable.metadata().partitioner,
                                   sstable.metadata().comparator);
    }

    public boolean hasComponent(IndexComponent component)
    {
        registerPerSSTableComponents();
        return components.get(ComponentGroupId.SSTABLE).contains(component);
    }

    public boolean hasComponent(IndexComponent component, IndexContext context)
    {
        registerPerIndexComponents(context);
        var components = this.components.get(idProvider.get(context));
        return components != null && components.contains(component);
    }

    public String componentFileName(IndexComponent component)
    {
        return versions.get(ComponentGroupId.SSTABLE).fileNameFormatter().format(component, null);
    }

    public String componentFileName(IndexComponent component, IndexContext context)
    {
        return getIndexVersion(context).fileNameFormatter().format(component, context);
    }

    public Version getIndexVersion(IndexContext context)
    {
        return versions.computeIfAbsent(idProvider.get(context), __ ->
        {
            for (Version version : Version.ALL)
            {
                if (componentExistsOnDisk(version, descriptor, IndexComponent.GROUP_COMPLETION_MARKER, context))
                    return version;
            }
            // this is called by flush while creating new index files, as well as loading files that already exist
            return Version.LATEST;
        });
    }

    /**
     * Returns true if the given component exists on disk for the given index.
     * If context is null, the component is assumed to be a per-sstable component.
     */
    private static boolean componentExistsOnDisk(Version version, Descriptor descriptor, IndexComponent component, IndexContext context)
    {
        var file = fileFor(descriptor, version, component, context);
        return file.exists();
    }

    public Version getIndexVersion(ComponentGroupId id)
    {
        return versions.get(id);
    }

    public File fileFor(IndexComponent component)
    {
        var ac = new AttachedIndexComponent(component, ComponentGroupId.SSTABLE);
        return fileMap.computeIfAbsent(ac, __ -> createFile(component, null));
    }

    public File fileFor(IndexComponent component, IndexContext context)
    {
        return fileMap.computeIfAbsent(new AttachedIndexComponent(component, context),
                                                     p -> createFile(component, context));
    }

    public Set<Component> getLivePerSSTableComponents()
    {
        registerPerSSTableComponents();
        return components.get(ComponentGroupId.SSTABLE).stream()
                         .map(c -> new Component(Component.Type.CUSTOM, componentFileName(c)))
                         .collect(Collectors.toSet());
    }

    public Set<Component> getLivePerIndexComponents(IndexContext context)
    {
        registerPerIndexComponents(context);
        var components = this.components.get(idProvider.get(context));
        return components == null
               ? Collections.emptySet()
               : components.stream()
                 .map(c -> new Component(Component.Type.CUSTOM, componentFileName(c, context)))
                 .collect(Collectors.toSet());
    }

    public PrimaryKeyMap.Factory newPrimaryKeyMapFactory(SSTableReader sstable) throws IOException
    {
        return versions.get(ComponentGroupId.SSTABLE).onDiskFormat().newPrimaryKeyMapFactory(this, sstable);
    }

    public SearchableIndex newSearchableIndex(SSTableContext sstableContext, IndexContext context)
    {
        return isIndexEmpty(context)
               ? new EmptyIndex()
               : getIndexVersion(context).onDiskFormat().newSearchableIndex(sstableContext, context);
    }

    public PerSSTableWriter newPerSSTableWriter() throws IOException
    {
        return versions.get(ComponentGroupId.SSTABLE).onDiskFormat().newPerSSTableWriter(this);
    }

    public PerIndexWriter newPerIndexWriter(StorageAttachedIndex index,
                                            LifecycleNewTracker tracker,
                                            RowMapping rowMapping)
    {
        return Version.LATEST.onDiskFormat().newPerIndexWriter(index, this, tracker, rowMapping);
    }

    /**
     * @return true if the per-sstable index components have been built and are complete
     */
    public boolean isPerSSTableBuildComplete()
    {
        return hasComponent(IndexComponent.GROUP_COMPLETION_MARKER);
    }

    /**
     * Returns true if the per-column index components have been built and are valid.
     *
     * @param context The {@link IndexContext} for the index
     * @return true if the per-column index components have been built and are complete
     */
    public boolean isPerIndexBuildComplete(IndexContext context)
    {
        return hasComponent(IndexComponent.GROUP_COMPLETION_MARKER) &&
               hasComponent(IndexComponent.COLUMN_COMPLETION_MARKER, context);
    }

    public boolean isSSTableEmpty()
    {
        return isPerSSTableBuildComplete() && numberOfComponents() == 1;
    }

    public boolean isIndexEmpty(IndexContext context)
    {
        return isPerIndexBuildComplete(context) && numberOfComponents(context) == 1;
    }

    public long sizeOnDiskOfPerSSTableComponents()
    {
        return versions.get(ComponentGroupId.SSTABLE).onDiskFormat()
                       .perSSTableComponents()
                       .stream()
                       .map(this::fileFor)
                       .filter(File::exists)
                       .mapToLong(File::length)
                       .sum();
    }

    public long sizeOnDiskOfPerIndexComponents(IndexContext context)
    {
        registerPerIndexComponents(context);
        var components = this.components.get(idProvider.get(context));
        if (components == null)
            return 0;

        return components.stream()
                         .map(c -> new AttachedIndexComponent(c, context))
                         .map(fileMap::get)
                         .filter(java.util.Objects::nonNull)
                         .filter(File::exists)
                         .mapToLong(File::length)
                         .sum();
    }

    @VisibleForTesting
    public long sizeOnDiskOfPerIndexComponent(IndexComponent component, IndexContext context)
    {
        var components = this.components.get(idProvider.get(context));
        if (components == null)
            return 0;

        return components.stream()
                         .filter(c -> c == component)
                         .map(c -> new AttachedIndexComponent(c, context))
                         .map(fileMap::get)
                         .filter(java.util.Objects::nonNull)
                         .filter(File::exists)
                         .mapToLong(File::length)
                         .sum();
    }

    public boolean validatePerIndexComponents(IndexContext context)
    {
        logger.debug("validatePerIndexComponents called for " + context.getIndexName());
        registerPerIndexComponents(context);
        return getIndexVersion(context).onDiskFormat().validatePerIndexComponents(this, context, false);
    }

    public boolean validatePerIndexComponentsChecksum(IndexContext context)
    {
        registerPerIndexComponents(context);
        return getIndexVersion(context).onDiskFormat().validatePerIndexComponents(this, context, true);
    }

    public boolean validatePerSSTableComponents()
    {
        registerPerSSTableComponents();
        return versions.get(ComponentGroupId.SSTABLE).onDiskFormat().validatePerSSTableComponents(this, false);
    }

    public boolean validatePerSSTableComponentsChecksum()
    {
        registerPerSSTableComponents();
        return versions.get(ComponentGroupId.SSTABLE).onDiskFormat().validatePerSSTableComponents(this, true);
    }

    public void deletePerSSTableIndexComponents()
    {
        registerPerSSTableComponents();
        var perSSTableComponents = components.get(ComponentGroupId.SSTABLE);
        perSSTableComponents.stream()
                            .map(c -> fileMap.remove(new AttachedIndexComponent(c, ComponentGroupId.SSTABLE)))
                            .filter(java.util.Objects::nonNull)
                            .forEach(this::deleteComponent);
        perSSTableComponents.clear();
    }

    public void deleteColumnIndex(IndexContext context)
    {
        registerPerIndexComponents(context);
        var components = this.components.get(idProvider.get(context));
        if (components == null)
            return;

        components.stream()
                  .map(c -> new AttachedIndexComponent(c, context))
                  .map(fileMap::remove)
                  .filter(java.util.Objects::nonNull)
                  .forEach(this::deleteComponent);
    }

    public void createComponentOnDisk(IndexComponent component) throws IOException
    {
        com.google.common.io.Files.touch(fileFor(component).toJavaIOFile());
        registerPerSSTableComponent(component);
    }

    public void createComponentOnDisk(IndexComponent component, IndexContext context) throws IOException
    {
        com.google.common.io.Files.touch(fileFor(component, context).toJavaIOFile());
        components.computeIfAbsent(idProvider.get(context), k -> Sets.newHashSet()).add(component);
    }

    public IndexInput openPerSSTableInput(IndexComponent component)
    {
        return IndexFileUtils.instance.openBlockingInput(createPerSSTableFileHandle(component));
    }

    public IndexInput openPerIndexInput(IndexComponent component, IndexContext context)
    {
        return IndexFileUtils.instance.openBlockingInput(createPerIndexFileHandle(component, context));
    }

    public IndexOutputWriter openPerSSTableOutput(IndexComponent component) throws IOException
    {
        return openPerSSTableOutput(component, false);
    }

    public IndexOutputWriter openPerSSTableOutput(IndexComponent component, boolean append) throws IOException
    {
        final File file = fileFor(component);

        if (logger.isTraceEnabled())
            logger.trace(logMessage("Creating SSTable attached index output for component {} on file {}..."),
                         component,
                         file);

        IndexOutputWriter writer = IndexFileUtils.instance.openOutput(file, append);

        registerPerSSTableComponent(component);

        return writer;
    }

    public IndexOutputWriter openPerIndexOutput(IndexComponent component, IndexContext context) throws IOException
    {
        return openPerIndexOutput(component, context, false);
    }

    public IndexOutputWriter openPerIndexOutput(IndexComponent component, IndexContext context, boolean append) throws IOException
    {
        final File file = fileFor(component, context);

        if (logger.isTraceEnabled())
            logger.trace(context.logMessage("Creating sstable attached index output for component {} on file {}..."),
                         component,
                         file);

        IndexOutputWriter writer = IndexFileUtils.instance.openOutput(file, append);

        registerPerSSTableComponent(component);

        return writer;
    }

    public FileHandle createPerSSTableFileHandle(IndexComponent component)
    {
        try (final FileHandle.Builder builder = StorageProvider.instance.fileHandleBuilderFor(this, component))
        {
            return builder.complete();
        }
    }

    public FileHandle createPerIndexFileHandle(IndexComponent component, IndexContext context)
    {
        try (final FileHandle.Builder builder = StorageProvider.instance.fileHandleBuilderFor(this, component, context))
        {
            return builder.complete();
        }
    }

    /**
     * Opens a file handle for the provided index component similarly to {@link #createPerIndexFileHandle(IndexComponent, IndexContext)},
     * but this method shoud be called instead of the aforemented one if the access is done "as part of flushing", that is
     * before the full index that this is a part of has been finalized.
     * <p>
     * The use of this method can allow specific storage providers, typically tiered storage ones, to distinguish accesses
     * that happen "at flush time" from other accesses, as the related file may be in different tier of storage.
     */
    public FileHandle createFlushTimePerIndexFileHandle(IndexComponent indexComponent, IndexContext indexContext)
    {
        try (final FileHandle.Builder builder = StorageProvider.instance.flushTimeFileHandleBuilderFor(this, indexComponent, indexContext))
        {
            return builder.complete();
        }
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(descriptor, versions.get(ComponentGroupId.SSTABLE));
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IndexDescriptor other = (IndexDescriptor)o;
        return Objects.equal(descriptor, other.descriptor) &&
               Objects.equal(versions.get(ComponentGroupId.SSTABLE), other.versions.get(ComponentGroupId.SSTABLE));
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

    private void registerPerSSTableComponents()
    {
        versions.get(ComponentGroupId.SSTABLE).onDiskFormat().perSSTableComponents()
                .stream()
                .filter(c -> !components.get(ComponentGroupId.SSTABLE).contains(c) && fileFor(c).exists())
                .forEach(components.get(ComponentGroupId.SSTABLE)::add);
    }

    private void registerPerIndexComponents(IndexContext context)
    {
        Set<IndexComponent> components = this.components.computeIfAbsent(idProvider.get(context), k -> Sets.newHashSet());
        getIndexVersion(context).onDiskFormat().perIndexComponents(context)
                                     .stream()
                                     .filter(c -> !components.contains(c) && fileFor(c, context).exists())
                                     .forEach(components::add);
    }

    private int numberOfComponents(IndexContext context)
    {
        return components.containsKey(idProvider.get(context))
               ? components.get(idProvider.get(context)).size()
               : 0;
    }

    private int numberOfComponents()
    {
        return components.get(ComponentGroupId.SSTABLE).size();
    }

    private File createFile(IndexComponent component, IndexContext context)
    {
        Component customComponent = new Component(Component.Type.CUSTOM, componentFileName(component, context));
        return descriptor.fileFor(customComponent);
    }

    public static File fileFor(Descriptor descriptor, Version version, IndexComponent component, IndexContext context)
    {
        var componentFileName = version.fileNameFormatter().format(component, context);
        var customComponent = new Component(Component.Type.CUSTOM, componentFileName);
        return descriptor.fileFor(customComponent);
    }

    private void deleteComponent(File file)
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

    private void registerPerSSTableComponent(IndexComponent component)
    {
        components.get(ComponentGroupId.SSTABLE).add(component);
    }

    /**
     * A unique (within this sstable) identifier for a group of SAI components that either
     * belong to a single column index, or are shared across all columns in the sstable.  The identifiers
     * may be compared using object identity.
     * <p>
     * This is only used internally and is NOT persisted.
     * <p>
     * Usage: create a Provider() (per sstable) and then call get() to get an identifier for a column or sstable.
     * The Provider takes care of ensuring instance uniqueness.
     */
    @SuppressWarnings("InstantiationOfUtilityClass")
    static class ComponentGroupId
    {
        public static final ComponentGroupId SSTABLE = new ComponentGroupId();

        private ComponentGroupId() {}

        public static class Provider
        {
            private final Map<String, ComponentGroupId> identifiers = new HashMap<>();

            /**
             *
             * @param indexContext the index whose identifier we want.
             * @return a unique identifier for the given index
             */
            ComponentGroupId get(IndexContext indexContext)
            {
                if (indexContext == null)
                    return SSTABLE;
                return identifiers.computeIfAbsent(indexContext.getIndexName(), __ -> new ComponentGroupId());
            }
        }
    }
}
