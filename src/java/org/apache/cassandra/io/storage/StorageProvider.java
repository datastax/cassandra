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

package org.apache.cassandra.io.storage;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cache.ChunkCache;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.sstable.format.bti.BtiFormat;
import org.apache.cassandra.io.sstable.metadata.ZeroCopyMetadata;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.PathUtils;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.INativeLibrary;

import static org.apache.cassandra.config.CassandraRelevantProperties.CUSTOM_STORAGE_PROVIDER;

/**
 * The storage provider is used to support directory creation and remote/local conversion for remote storage.
 * The default implementation {@link DefaultProvider} is based on local file system.
 */
public interface StorageProvider
{
    Logger logger = LoggerFactory.getLogger(StorageProvider.class);

    StorageProvider instance = !CUSTOM_STORAGE_PROVIDER.isPresent()
                               ? new DefaultProvider()
                               : FBUtilities.construct(CUSTOM_STORAGE_PROVIDER.getString(), "storage provider");

    enum DirectoryType
    {
        DATA("data_file_directories"),
        LOCAL_SYSTEM_DATA("local_system_data_file_directories"),
        METADATA("metadata_directory"),
        COMMITLOG("commit_log_directory"),
        HINTS("hints_directory"),
        SAVED_CACHES("saved_caches_directory"),
        CDC("cdc_raw_directory"),
        SNAPSHOT("snapshot_directory"),
        NODES("nodes_local_directory"),
        LOG_TRANSACTION("log_transaction_directory"),
        LOGS("logs_directory"),
        TEMP("temp_directory"),
        OTHERS("other_directory");

        final String name;

        final boolean readable;
        final boolean writable;

        DirectoryType(String name)
        {
            this.name = name;
            this.readable = true;
            this.writable = true;
        }
    }

    /**
     * @return local path if given path is remote path, otherwise returns itself
     */
    File getLocalPath(File path);

    /**
     * @return local path if given path is remote path, otherwise returns itself
     */
    Path getLocalPath(Path path);

    /**
     * update the given path with open options for the sstable components
     */
    File withOpenOptions(File ret, Component component);

    /**
     * Create data directories for given table
     *
     * @param ksMetadata   The keyspace metadata, can be null. This is used when schema metadata is
     *                     not available in {@link Schema}, eg. CNDB backup & restore
     * @param keyspaceName the name of the keyspace
     * @param dirs         current local data directories
     * @return data directories that are created
     */
    Directories.DataDirectory[] createDataDirectories(@Nullable KeyspaceMetadata ksMetadata, String keyspaceName, Directories.DataDirectory[] dirs);

    /**
     * Create directory for the given path and type, either locally or remotely if any remote storage parameters are passed in.
     *
     * @param dir  the directory absolute path to create
     * @param type the type of directory to create
     * @return the actual directory path, which can be either local or remote; or null if directory can't be created
     */
    File createDirectory(String dir, DirectoryType type);

    /**
     * Remove the give file from any local cache, for example the OS page cache, or at least it tries to.
     * @param file the file that is no longer required in the file system caches
     */
    void invalidateFileSystemCache(File file);

    /**
     * Remove the give sstable from any local cache, for example the OS page cache, or at least it tries to.
     *
     * @param descriptor the descriptor for the sstable that is no longer required in the file system caches
     * @param tidied whether ReaderTidier has been run, aka. deleting sstable files.
     */
    void invalidateFileSystemCache(Descriptor descriptor, boolean tidied);

    /**
     * Creates a new {@link FileHandle.Builder} for the given sstable component.
     * <p>
     * The returned builder will be configured with the appropriate "access mode" (mmap or not), and the "chunk cache"
     * will have been set if appropriate.
     *
     * @param descriptor descriptor for the sstable whose handler is built.
     * @param component sstable component for which to build the handler.
     * @return a new {@link FileHandle.Builder} for the provided sstable component with access mode and chunk cache
     *   configured as appropriate.
     */
    FileHandle.Builder fileHandleBuilderFor(Descriptor descriptor, Component component);

    /**
     * Creates a new {@link FileHandle.Builder} for the given sstable component.
     * <p>
     * The returned builder will be configured with the appropriate "access mode" (mmap or not), and the "chunk cache"
     * will have been set if appropriate.
     *
     * @param descriptor descriptor for the sstable whose handler is built.
     * @param component sstable component for which to build the handler.
     * @param zeroCopyMetadata zero copy metadata for the sstable
     * @return a new {@link FileHandle.Builder} for the provided sstable component with access mode and chunk cache
     *   configured as appropriate.
     */
    FileHandle.Builder fileHandleBuilderFor(Descriptor descriptor, Component component, ZeroCopyMetadata zeroCopyMetadata);

    /**
     * Creates a new {@link FileHandle.Builder} for the given SAI component.
     * <p>
     * The returned builder will be configured with the appropriate "access mode" (mmap or not), and the "chunk cache"
     * will have been set if appropriate.
     *
     * @param component index component for which to build the handler.
     * @return a new {@link FileHandle.Builder} for the provided SAI component with access mode and chunk cache
     *   configured as appropriate.
     */
    FileHandle.Builder fileHandleBuilderFor(IndexComponent.ForRead component);

    /**
     * Creates a new {@link FileChannel} to read the given file, that is suitable for reading the file "at write time",
     * that is typcally for when we need to access the partially written file to complete checksum.
     *
     * @param file the file to be read
     * @return a new {@link FileChannel} for the provided file
     */
    default FileChannel writeTimeReadFileChannelFor(File file) throws IOException
    {
        return FileChannel.open(file.toPath(), StandardOpenOption.READ);
    }

    /**
     * Creates a new {@link FileHandle.Builder} for the given SAI component and context (for index with per-index files),
     * that is suitable for reading the component "at flush time", that is typcally for when we need to access the
     * component to complete the writing of another related component.
     * <p>
     * Other the fact that this method will be called a different time, it's requirements are the same than for
     * {@link #fileHandleBuilderFor(IndexComponent.ForRead)}.
     *
     * @param component index component for which to build the handler.
     * @return a new {@link FileHandle.Builder} for the provided SAI component with access mode and chunk cache
     *   configured as appropriate.
     */
    FileHandle.Builder flushTimeFileHandleBuilderFor(IndexComponent.ForRead component);

    class DefaultProvider implements StorageProvider
    {
        @Override
        public File getLocalPath(File path)
        {
            return path;
        }

        @Override
        public Path getLocalPath(Path path)
        {
            return path;
        }

        @Override
        public File withOpenOptions(File ret, Component component)
        {
            return ret;
        }

        @Override
        public Directories.DataDirectory[] createDataDirectories(@Nullable KeyspaceMetadata ksMetadata, String keyspaceName, Directories.DataDirectory[] dirs)
        {
            // data directories are already created in DatabadeDescriptor#createAllDirectories
            return dirs;
        }

        @Override
        public File createDirectory(String dir, DirectoryType type)
        {
            File ret = new File(dir);
            PathUtils.createDirectoriesIfNotExists(ret.toPath());
            return ret;
        }

        @Override
        public void invalidateFileSystemCache(File file)
        {
            INativeLibrary.instance.trySkipCache(file, 0, 0);
        }

        @Override
        public void invalidateFileSystemCache(Descriptor desc, boolean tidied)
        {
            for (Component c : desc.discoverComponents())
                StorageProvider.instance.invalidateFileSystemCache(desc.fileFor(c));
        }

        protected Config.DiskAccessMode accessMode(Component component)
        {
            if (component.type == BigFormat.Components.Types.PRIMARY_INDEX
                || component.type == BtiFormat.Components.Types.PARTITION_INDEX
                || component.type == BtiFormat.Components.Types.ROW_INDEX)
                return DatabaseDescriptor.getIndexAccessMode();
            else
                return DatabaseDescriptor.getDiskAccessMode();
        }

        @Override
        @SuppressWarnings("resource")
        public FileHandle.Builder fileHandleBuilderFor(Descriptor descriptor, Component component)
        {
            return new FileHandle.Builder(descriptor.fileFor(component))
                   .mmapped(accessMode(component) == Config.DiskAccessMode.mmap)
                   .withChunkCache(ChunkCache.instance);
        }

        @Override
        @SuppressWarnings("resource")
        public FileHandle.Builder fileHandleBuilderFor(Descriptor descriptor, Component component, ZeroCopyMetadata zeroCopyMetadata)
        {
            return new FileHandle.Builder(descriptor.fileFor(SSTableFormat.Components.DATA))
                   .mmapped(DatabaseDescriptor.getDiskAccessMode() == Config.DiskAccessMode.mmap)
                   .withChunkCache(ChunkCache.instance)
                   .slice(zeroCopyMetadata);
        }

        @Override
        @SuppressWarnings("resource")
        public FileHandle.Builder fileHandleBuilderFor(IndexComponent.ForRead component)
        {
            File file = component.file();
            if (logger.isTraceEnabled())
            {
                logger.trace(component.parent().logMessage("Opening {} file handle for {} ({})"),
                             file, FBUtilities.prettyPrintMemory(file.length()));
            }
            var builder = new FileHandle.Builder(file);
            // Comments on why we don't use adviseRandom for some components where you might expect it:
            //
            // KD_TREE: no adviseRandom because we do a large bulk read on startup, queries later may
            //     benefit from adviseRandom but there's no way to split those apart
            // POSTINGS_LISTS: for common terms with 1000s of rows, adviseRandom seems likely to
            //     make it slower; no way to get cardinality at this point in the code
            //     (and we already have shortcut code for the common 1:1 vector case)
            //     so we leave it alone here
            if (component.componentType() == IndexComponentType.TERMS_DATA
                || component.componentType() == IndexComponentType.VECTOR
                || component.componentType() == IndexComponentType.PRIMARY_KEY_TRIE)
            {
                builder = builder.adviseRandom();
            }
            return builder.mmapped(true);
        }

        @Override
        public FileHandle.Builder flushTimeFileHandleBuilderFor(IndexComponent.ForRead component)
        {
            // By default, no difference between accesses "at flush time" and "at query time", but subclasses may need
            // to differenciate both.
            return fileHandleBuilderFor(component);
        }
    }
}
