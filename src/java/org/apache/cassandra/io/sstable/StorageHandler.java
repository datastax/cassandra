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

package org.apache.cassandra.io.sstable;

import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.Set;

import com.google.common.base.Preconditions;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.lifecycle.Tracker;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Throwables;

import static org.apache.cassandra.config.CassandraRelevantProperties.REMOTE_STORAGE_HANDLER_FACTORY;

/**
 * The handler of the storage of sstables, and possibly other files such as txn logs.
 * <p/>
 * If sstables are stored on the local disk, then this handler is a thin wrapper of {@link Directories.SSTableLister},
 * but for sstables stored remotely, for example on S3, then the handler may need to perform more
 * work, such as selecting only part of the remote sstables available, or adding new ones when offline compaction
 * has run. This behaviour can be implemented in a sub-class created from factory that can be set with {@link #remoteStorageHandlerFactory}.
 * <p/>
 */
public abstract class StorageHandler
{
    private final static String remoteStorageHandlerFactory = REMOTE_STORAGE_HANDLER_FACTORY.getString();

    private static class InstanceHolder
    {
        private static final StorageHandlerFactory FACTORY = maybeInitializeFactory(remoteStorageHandlerFactory);
    }

    public enum ReloadReason
    {
        /** New nodes joined or left */
        TOPOLOGY_CHANGED(true),
        /** Data was truncated */
        TRUNCATION(false),
        /** SSTables might have been added or removed, regardless of a specific reason
         * e.g. it could be compaction or flushing or regions being updated which caused
         * new sstables to arrive */
        SSTABLES_CHANGED(false),
        /** Data was replayed either from the commit log or a batch log */
        DATA_REPLAYED(true),
        /** When repair task started */
        REPAIR(true),
        /** A request over forced by users to reload. */
        USER_REQUESTED(true),
        /** When region status changed */
        REGION_CHANGED(false),
        /** When index is built */
        INDEX_BUILT(false),
        /** New node restarted with existing on disk data */
        REPLACE(true),
        /** Retry in case of failure, i.e. if a timeout occurred **/
        RETRY(false);

        /** When this is true, a reload operation will reload all sstables even those that could
         * have been flushed by other nodes. */
        public final boolean loadFlushedSSTables;

        ReloadReason(boolean loadFlushedSSTables)
        {
            this.loadFlushedSSTables = loadFlushedSSTables;
        }
    }

    protected final TableMetadataRef metadata;
    protected final Directories directories;
    protected final Tracker dataTracker;

    public StorageHandler(TableMetadataRef metadata, Directories directories, Tracker dataTracker)
    {
        Preconditions.checkNotNull(directories, "Directories should not be null");

        this.metadata = metadata;
        this.directories = directories;
        this.dataTracker = dataTracker;
    }

    /**
     * @return true if the node is ready to serve data for this table. This means that the
     *         node is not bootstrapping and that no data may be missing, e.g. if sstables are
     *         being downloaded from remote storage or streamed from other nodes then isReady()
     *         would return false. Generally, user read queries should not succeed if this method
     *         returns false.
     */
    public abstract boolean isReady();

    /**
     * Load the initial sstables into the tracker that was passed in to the constructor.
     *
     * @return the sstables that were loaded
     */
    public abstract Collection<SSTableReader> loadInitialSSTables();

    /**
     * Reload any sstables that may have been created and not yet loaded. This is normally
     * a no-op for the default local storage, but for remote storage implementations it
     * signals that sstables need to be refreshed.
     *
     * @return the sstables that were loaded
     */
    public abstract Collection<SSTableReader> reloadSSTables(ReloadReason reason);

    /**
     * This method determines if the backing storage handler allows auto compaction
     * <p/>
     * @return true if auto compaction should be enabled
     */
    public abstract boolean enableAutoCompaction();

    /**
     * This method will run the operation specified by the {@link Runnable} passed it
     * whilst guaranteeing the guarantees that no sstable will be loaded or unloaded
     * whilst this operation is running, by waiting for in-progress operation to complete.
     * In other words, the storage handler must not change the status of the tracker,
     * or try to load any sstable as long as this operation is executing.
     *
     * @param runnable the operation to execute.
     */
    public abstract void runWithReloadingDisabled(Runnable runnable);

    /**
     * Called when the CFS is unloaded, this needs to perform any cleanup.
     */
    public abstract void unload();

    /**
     * Called during flush when we try to open a {@link SSTableReader} on the written sstable but reading it fails.
     * <p>
     * The default implementation simply propagates the exception that failed the opening, but it can be overriden to
     * try to recover and return a proper reader. This method provides as much information on the written sstable
     * as to allow recovering. In the case of tiered storage, where some tier may be temporarily unresponsive, it
     * can be used to provide a "shim" reader to avoid failing the flush and until sstable can be successfully reloaded.
     *
     * @param reason the {@link SSTableReader.OpenReason} for the opening that failed.
     * @param descriptor the sstable descriptor.
     * @param components the components that have been written.
     * @param compressedSize the size on disk of the file that was writen.
     * @param uncompressedSize the size of the uncompressed data that was written. If the sstable is not compressed, it
     *                         will be the same as {@code compressedSize}.
     * @param stats the metadata/statistics on the written sstable.
     * @param firstKey the first key of the sstable.
     * @param lastKey the last key of the sstable.
     * @param estimatedKeys the number of keys written in the sstable (this is allowed to be an estimation, to mimick
     *                      what {@link SSTableReader#estimatedKeys()} would return, but in practice it will be exact
     *                      since we know how many keys we just wrote).
     * @param throwable the exeption that failed the sstable opening.
     * @return a reader for the sstable, if one can be created despite the initial error. If not, this message should
     * simply rethrow {@code throwable}.
     */
    public SSTableReader onOpeningWrittenSSTableFailure(SSTableReader.OpenReason reason,
                                                        Descriptor descriptor,
                                                        Set<Component> components,
                                                        long compressedSize,
                                                        long uncompressedSize,
                                                        StatsMetadata stats,
                                                        DecoratedKey firstKey,
                                                        DecoratedKey lastKey,
                                                        long estimatedKeys,
                                                        Throwable throwable)
    {
        // By default, just propagate the exception (not much we can do with local storage in particular).
        throw Throwables.unchecked(throwable);
    }

    public static StorageHandler create(TableMetadataRef metadata, Directories directories, Tracker dataTracker)
    {
        return InstanceHolder.FACTORY.create(metadata, directories, dataTracker);
    }

    private static StorageHandlerFactory maybeInitializeFactory(String factory)
    {
        if (factory == null)
            return StorageHandlerFactory.DEFAULT;

        Class<StorageHandlerFactory> factoryClass =  FBUtilities.classForName(factory, "Remote storage handler factory");

        try
        {
            return factoryClass.getConstructor().newInstance();
        }
        catch (NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e)
        {
            throw new ConfigurationException("Unable to find correct constructor for " + factory, e);
        }
    }
}
