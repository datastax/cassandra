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
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
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
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.IOUtils;

/**
 * The {@link IndexDescriptor} is an analog of the SSTable {@link Descriptor} and provides version
 * specific information about the on-disk state of {@link StorageAttachedIndex}es.
 *
 * The {@IndexDescriptor} is primarily responsible for maintaining a view of the on-disk state
 * of the SAI indexes for a specific {@link org.apache.cassandra.io.sstable.SSTable}. It maintains mappings
 * of the current on-disk components and files. It is responsible for opening files for use by
 * writers and readers.
 *
 * It's remaining responsibility is to act as a proxy to the {@link OnDiskFormat} associated with the
 * index {@link Version}.
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
    private final IndexIdentifier.Provider indexIdentifierProvider = new IndexIdentifier.Provider();
    // index -> version
    private final Map<IndexIdentifier, Version> perIndexVersions = Maps.newHashMap();
    // index -> components
    private final Map<IndexIdentifier, Set<IndexComponent>> perIndexComponents = Maps.newHashMap();
    // component -> file
    private final Map<AttachedIndexComponent, File> onDiskPerIndexFileMap = Maps.newHashMap();

    /**
     * A component together with the index it belongs to.
     */
    private class AttachedIndexComponent
    {
        public final IndexComponent component;
        public final IndexIdentifier id;

        public AttachedIndexComponent(IndexComponent component, IndexContext context)
        {
            this(component, indexIdentifierProvider.get(context));
        }

        public AttachedIndexComponent(IndexComponent component, IndexIdentifier id)
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

        perIndexVersions.put(IndexIdentifier.SSTABLE, version);
        perIndexComponents.put(IndexIdentifier.SSTABLE, Sets.newHashSet());
    }

    public static IndexDescriptor createNew(Descriptor descriptor, IPartitioner partitioner, ClusteringComparator clusteringComparator)
    {
        return new IndexDescriptor(Version.LATEST, descriptor, partitioner, clusteringComparator);
    }

    public static IndexDescriptor createFrom(SSTableReader sstable)
    {
        for (Version version : Version.ALL)
        {
            IndexDescriptor indexDescriptor = new IndexDescriptor(version,
                                                                  sstable.descriptor,
                                                                  sstable.metadata().partitioner,
                                                                  sstable.metadata().comparator);
            if (indexDescriptor.hasComponent(IndexComponent.GROUP_COMPLETION_MARKER))
                return indexDescriptor;
        }
        return new IndexDescriptor(Version.LATEST,
                                   sstable.descriptor,
                                   sstable.metadata().partitioner,
                                   sstable.metadata().comparator);
    }

    public boolean hasComponent(IndexComponent indexComponent)
    {
        registerPerSSTableComponents();
        return perIndexComponents.get(IndexIdentifier.SSTABLE).contains(indexComponent);
    }

    public boolean hasComponent(IndexComponent indexComponent, IndexContext indexContext)
    {
        registerPerIndexComponents(indexContext);
        var components = perIndexComponents.get(indexIdentifierProvider.get(indexContext));
        return components != null && components.contains(indexComponent);
    }

    public String componentName(IndexComponent indexComponent)
    {
        return perIndexVersions.get(IndexIdentifier.SSTABLE).fileNameFormatter().format(indexComponent, null);
    }

    public String componentName(IndexComponent indexComponent, IndexContext indexContext)
    {
        return getIndexVersion(indexContext).fileNameFormatter().format(indexComponent, indexContext);
    }

    public Version getIndexVersion(IndexContext indexContext)
    {
        // FIXME per-index versions are not wired in yet
//        return getIndexVersion(indexIdentifierProvider.get(indexContext));
        return getIndexVersion(IndexIdentifier.SSTABLE);
    }

    public Version getIndexVersion(IndexIdentifier id)
    {
        return perIndexVersions.get(id);
    }

    public File fileFor(IndexComponent component)
    {
        var ac = new AttachedIndexComponent(component, IndexIdentifier.SSTABLE);
        return onDiskPerIndexFileMap.computeIfAbsent(ac, __ -> createFile(component, null));
    }

    public File fileFor(IndexComponent component, IndexContext indexContext)
    {
        return onDiskPerIndexFileMap.computeIfAbsent(new AttachedIndexComponent(component, indexContext),
                                                     p -> createFile(component, indexContext));
    }

    public Set<Component> getLivePerSSTableComponents()
    {
        registerPerSSTableComponents();
        return perIndexComponents.get(IndexIdentifier.SSTABLE).stream()
                                 .map(c -> new Component(Component.Type.CUSTOM, componentName(c)))
                                 .collect(Collectors.toSet());
    }

    public Set<Component> getLivePerIndexComponents(IndexContext indexContext)
    {
        registerPerIndexComponents(indexContext);
        var components = perIndexComponents.get(indexIdentifierProvider.get(indexContext));
        return components == null
               ? Collections.emptySet()
               : components.stream()
                 .map(c -> new Component(Component.Type.CUSTOM, componentName(c, indexContext)))
                 .collect(Collectors.toSet());
    }

    public PrimaryKeyMap.Factory newPrimaryKeyMapFactory(SSTableReader sstable) throws IOException
    {
        return perIndexVersions.get(IndexIdentifier.SSTABLE).onDiskFormat().newPrimaryKeyMapFactory(this, sstable);
    }

    public SearchableIndex newSearchableIndex(SSTableContext sstableContext, IndexContext indexContext)
    {
        return isIndexEmpty(indexContext)
               ? new EmptyIndex()
               : getIndexVersion(indexContext).onDiskFormat().newSearchableIndex(sstableContext, indexContext);
    }

    public PerSSTableWriter newPerSSTableWriter() throws IOException
    {
        return perIndexVersions.get(IndexIdentifier.SSTABLE).onDiskFormat().newPerSSTableWriter(this);
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
     * @param indexContext The {@link IndexContext} for the index
     * @return true if the per-column index components have been built and are complete
     */
    public boolean isPerIndexBuildComplete(IndexContext indexContext)
    {
        return hasComponent(IndexComponent.GROUP_COMPLETION_MARKER) &&
               hasComponent(IndexComponent.COLUMN_COMPLETION_MARKER, indexContext);
    }

    public boolean isSSTableEmpty()
    {
        return isPerSSTableBuildComplete() && numberOfComponents() == 1;
    }

    public boolean isIndexEmpty(IndexContext indexContext)
    {
        return isPerIndexBuildComplete(indexContext) && numberOfComponents(indexContext) == 1;
    }

    public long sizeOnDiskOfPerSSTableComponents()
    {
        return perIndexVersions.get(IndexIdentifier.SSTABLE).onDiskFormat()
                                .perSSTableComponents()
                                .stream()
                                .map(this::fileFor)
                                .filter(File::exists)
                                .mapToLong(File::length)
                                .sum();
    }

    public long sizeOnDiskOfPerIndexComponents(IndexContext indexContext)
    {
        registerPerIndexComponents(indexContext);
        var components = perIndexComponents.get(indexIdentifierProvider.get(indexContext));
        if (components == null)
            return 0;

        return components.stream()
                         .map(c -> new AttachedIndexComponent(c, indexContext))
                         .map(onDiskPerIndexFileMap::get)
                         .filter(java.util.Objects::nonNull)
                         .filter(File::exists)
                         .mapToLong(File::length)
                         .sum();
    }

    @VisibleForTesting
    public long sizeOnDiskOfPerIndexComponent(IndexComponent indexComponent, IndexContext indexContext)
    {
        var components = perIndexComponents.get(indexIdentifierProvider.get(indexContext));
        if (components == null)
            return 0;

        return components.stream()
                         .filter(c -> c == indexComponent)
                         .map(c -> new AttachedIndexComponent(c, indexContext))
                         .map(onDiskPerIndexFileMap::get)
                         .filter(java.util.Objects::nonNull)
                         .filter(File::exists)
                         .mapToLong(File::length)
                         .sum();
    }

    public boolean validatePerIndexComponents(IndexContext indexContext)
    {
        logger.debug("validatePerIndexComponents called for " + indexContext.getIndexName());
        registerPerIndexComponents(indexContext);
        return getIndexVersion(indexContext).onDiskFormat().validatePerIndexComponents(this, indexContext, false);
    }

    public boolean validatePerIndexComponentsChecksum(IndexContext indexContext)
    {
        registerPerIndexComponents(indexContext);
        return getIndexVersion(indexContext).onDiskFormat().validatePerIndexComponents(this, indexContext, true);
    }

    public boolean validatePerSSTableComponents()
    {
        registerPerSSTableComponents();
        return perIndexVersions.get(IndexIdentifier.SSTABLE).onDiskFormat().validatePerSSTableComponents(this, false);
    }

    public boolean validatePerSSTableComponentsChecksum()
    {
        registerPerSSTableComponents();
        return perIndexVersions.get(IndexIdentifier.SSTABLE).onDiskFormat().validatePerSSTableComponents(this, true);
    }

    public void deletePerSSTableIndexComponents()
    {
        registerPerSSTableComponents();
        var perSSTableComponents = perIndexComponents.get(IndexIdentifier.SSTABLE);
        perSSTableComponents.stream()
                            .map(c -> onDiskPerIndexFileMap.remove(new AttachedIndexComponent(c, IndexIdentifier.SSTABLE)))
                            .filter(java.util.Objects::nonNull)
                            .forEach(this::deleteComponent);
        perSSTableComponents.clear();
    }

    public void deleteColumnIndex(IndexContext indexContext)
    {
        registerPerIndexComponents(indexContext);
        var components = perIndexComponents.get(indexIdentifierProvider.get(indexContext));
        if (components == null)
            return;

        components.stream()
                  .map(c -> new AttachedIndexComponent(c, indexContext))
                  .map(onDiskPerIndexFileMap::remove)
                  .filter(java.util.Objects::nonNull)
                  .forEach(this::deleteComponent);
    }

    public void createComponentOnDisk(IndexComponent component) throws IOException
    {
        Files.touch(fileFor(component).toJavaIOFile());
        registerPerSSTableComponent(component);
    }

    public void createComponentOnDisk(IndexComponent component, IndexContext indexContext) throws IOException
    {
        Files.touch(fileFor(component, indexContext).toJavaIOFile());
        registerPerIndexComponent(component, indexContext.getIndexName());
    }

    public IndexInput openPerSSTableInput(IndexComponent indexComponent)
    {
        final File file = fileFor(indexComponent);
        if (logger.isTraceEnabled())
            logger.trace(logMessage("Opening blocking index input for file {} ({})"),
                         file,
                         FBUtilities.prettyPrintMemory(file.length()));

        return IndexFileUtils.instance.openBlockingInput(file);
    }

    public IndexInput openPerIndexInput(IndexComponent indexComponent, IndexContext indexContext)
    {
        final File file = fileFor(indexComponent, indexContext);
        if (logger.isTraceEnabled())
            logger.trace(logMessage("Opening blocking index input for file {} ({})"),
                         file,
                         FBUtilities.prettyPrintMemory(file.length()));

        return IndexFileUtils.instance.openBlockingInput(file);
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

    public IndexOutputWriter openPerIndexOutput(IndexComponent indexComponent, IndexContext indexContext) throws IOException
    {
        return openPerIndexOutput(indexComponent, indexContext, false);
    }

    public IndexOutputWriter openPerIndexOutput(IndexComponent component, IndexContext indexContext, boolean append) throws IOException
    {
        final File file = fileFor(component, indexContext);

        if (logger.isTraceEnabled())
            logger.trace(indexContext.logMessage("Creating sstable attached index output for component {} on file {}..."),
                         component,
                         file);

        IndexOutputWriter writer = IndexFileUtils.instance.openOutput(file, append);

        registerPerSSTableComponent(component);

        return writer;
    }

    public FileHandle createPerSSTableFileHandle(IndexComponent indexComponent)
    {
        final File file = fileFor(indexComponent);

        if (logger.isTraceEnabled())
        {
            logger.trace(logMessage("Opening {} file handle for {} ({})"),
                         file, FBUtilities.prettyPrintMemory(file.length()));
        }

        try (final FileHandle.Builder builder = new FileHandle.Builder(file).mmapped(true))
        {
            return builder.complete();
        }
    }

    public FileHandle createPerIndexFileHandle(IndexComponent indexComponent, IndexContext indexContext)
    {
        final File file = fileFor(indexComponent, indexContext);

        if (logger.isTraceEnabled())
        {
            logger.trace(indexContext.logMessage("Opening file handle for {} ({})"),
                         file, FBUtilities.prettyPrintMemory(file.length()));
        }

        try (final FileHandle.Builder builder = new FileHandle.Builder(file).mmapped(true))
        {
            return builder.complete();
        }
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(descriptor, perIndexVersions.get(IndexIdentifier.SSTABLE));
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IndexDescriptor other = (IndexDescriptor)o;
        return Objects.equal(descriptor, other.descriptor) &&
               Objects.equal(perIndexVersions.get(IndexIdentifier.SSTABLE), other.perIndexVersions.get(IndexIdentifier.SSTABLE));
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
        perIndexVersions.get(IndexIdentifier.SSTABLE).onDiskFormat().perSSTableComponents()
                        .stream()
                        .filter(c -> !perIndexComponents.get(IndexIdentifier.SSTABLE).contains(c) && fileFor(c).exists())
                        .forEach(perIndexComponents.get(IndexIdentifier.SSTABLE)::add);
    }

    private void registerPerIndexComponents(IndexContext indexContext)
    {
        Set<IndexComponent> indexComponents = perIndexComponents.computeIfAbsent(indexIdentifierProvider.get(indexContext), k -> Sets.newHashSet());
        getIndexVersion(indexContext).onDiskFormat().perIndexComponents(indexContext)
                                     .stream()
                                     .filter(c -> !indexComponents.contains(c) && fileFor(c, indexContext).exists())
                                     .forEach(indexComponents::add);
    }

    private int numberOfComponents(IndexContext indexContext)
    {
        return perIndexComponents.containsKey(indexIdentifierProvider.get(indexContext))
               ? perIndexComponents.get(indexIdentifierProvider.get(indexContext)).size()
               : 0;
    }

    private int numberOfComponents()
    {
        return perIndexComponents.get(IndexIdentifier.SSTABLE).size();
    }

    private File createFile(IndexComponent component, IndexContext indexContext)
    {
        Component customComponent = new Component(Component.Type.CUSTOM, componentName(component, indexContext));
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

    private void registerPerSSTableComponent(IndexComponent indexComponent)
    {
        perIndexComponents.get(IndexIdentifier.SSTABLE).add(indexComponent);
    }

    private void registerPerIndexComponent(IndexComponent indexComponent, String index)
    {
        perIndexComponents.computeIfAbsent(indexIdentifierProvider.get(index), k -> Sets.newHashSet()).add(indexComponent);
    }
}
