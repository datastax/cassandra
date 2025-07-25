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

package org.apache.cassandra.io.sstable.format;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.db.rows.PartitionSerializationException;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.guardrails.Guardrails;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.StorageHandler;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.concurrent.Transactional;

/**
 * This is the API all table writers must implement.
 *
 * TableWriter.create() is the primary way to create a writer for a particular format.
 * The format information is part of the Descriptor.
 */
public abstract class SSTableWriter extends SSTable implements Transactional
{
    private static final Logger logger = LoggerFactory.getLogger(SSTableWriter.class);

    protected final LifecycleNewTracker lifecycleNewTracker;

    protected long repairedAt;
    protected UUID pendingRepair;
    protected boolean isTransient;
    protected long maxDataAge = -1;
    protected final long keyCount;
    protected final MetadataCollector metadataCollector;
    protected final SerializationHeader header;
    protected final Collection<SSTableFlushObserver> observers;
    private boolean isInternalKeyspace;

    protected abstract AbstractTransactional txnProxy();

    protected SSTableWriter(Descriptor descriptor,
                            Set<Component> components,
                            LifecycleNewTracker lifecycleNewTracker,
                            long keyCount,
                            long repairedAt,
                            UUID pendingRepair,
                            boolean isTransient,
                            TableMetadataRef metadata,
                            MetadataCollector metadataCollector,
                            SerializationHeader header,
                            Collection<SSTableFlushObserver> observers)
    {
        super(descriptor, components, metadata, DatabaseDescriptor.getDiskOptimizationStrategy());
        this.lifecycleNewTracker = lifecycleNewTracker;
        this.keyCount = keyCount;
        this.repairedAt = repairedAt;
        this.pendingRepair = pendingRepair;
        this.isTransient = isTransient;
        this.metadataCollector = metadataCollector;
        this.header = header;
        this.observers = observers == null ? Collections.emptySet() : observers;
        isInternalKeyspace = SchemaConstants.isInternalKeyspace(metadata.keyspace);
    }

    private static Set<Component> indexComponents(Collection<Index.Group> indexGroups)
    {
        if (indexGroups == null)
            return Collections.emptySet();

        Set<Component> components = new HashSet<>();
        for (Index.Group group : indexGroups)
        {
            components.addAll(group.componentsForNewSSTable());
        }

        return components;
    }

    public static SSTableWriter create(Descriptor descriptor,
                                       long keyCount,
                                       long repairedAt,
                                       UUID pendingRepair,
                                       boolean isTransient,
                                       TableMetadataRef metadata,
                                       MetadataCollector metadataCollector,
                                       SerializationHeader header,
                                       Collection<Index.Group> indexGroups,
                                       LifecycleNewTracker lifecycleNewTracker)
    {
        Factory writerFactory = descriptor.getFormat().getWriterFactory();
        return writerFactory.open(descriptor,
                                  keyCount,
                                  repairedAt,
                                  pendingRepair,
                                  isTransient,
                                  metadata,
                                  metadataCollector,
                                  header,
                                  observers(descriptor, indexGroups, lifecycleNewTracker, metadata.get(), keyCount),
                                  lifecycleNewTracker,
                                  indexComponents(indexGroups));
    }

    public static SSTableWriter create(Descriptor descriptor,
                                       long keyCount,
                                       long repairedAt,
                                       UUID pendingRepair,
                                       boolean isTransient,
                                       int sstableLevel,
                                       SerializationHeader header,
                                       Collection<Index.Group> indexGroups,
                                       LifecycleNewTracker lifecycleNewTracker)
    {
        TableMetadataRef metadata = Schema.instance.getTableMetadataRef(descriptor);
        return create(metadata, descriptor, keyCount, repairedAt, pendingRepair, isTransient, sstableLevel, header, indexGroups, lifecycleNewTracker);
    }

    public static SSTableWriter create(TableMetadataRef metadata,
                                       Descriptor descriptor,
                                       long keyCount,
                                       long repairedAt,
                                       UUID pendingRepair,
                                       boolean isTransient,
                                       int sstableLevel,
                                       SerializationHeader header,
                                       Collection<Index.Group> indexGroups,
                                       LifecycleNewTracker lifecycleNewTracker)
    {
        MetadataCollector collector = new MetadataCollector(metadata.get().comparator).sstableLevel(sstableLevel);
        return create(descriptor, keyCount, repairedAt, pendingRepair, isTransient, metadata, collector, header, indexGroups, lifecycleNewTracker);
    }

    @VisibleForTesting
    public static SSTableWriter create(Descriptor descriptor,
                                       long keyCount,
                                       long repairedAt,
                                       UUID pendingRepair,
                                       boolean isTransient,
                                       SerializationHeader header,
                                       Collection<Index.Group> indexGroups,
                                       LifecycleNewTracker lifecycleNewTracker)
    {
        return create(descriptor, keyCount, repairedAt, pendingRepair, isTransient, 0, header, indexGroups, lifecycleNewTracker);
    }

    private static Collection<SSTableFlushObserver> observers(Descriptor descriptor,
                                                              Collection<Index.Group> indexGroups,
                                                              LifecycleNewTracker tracker,
                                                              TableMetadata metadata,
                                                              long keyCount)
    {
        if (indexGroups == null)
            return Collections.emptyList();

        List<SSTableFlushObserver> observers = new ArrayList<>(indexGroups.size());
        for (Index.Group group : indexGroups)
        {
            SSTableFlushObserver observer = group.getFlushObserver(descriptor, tracker, metadata, keyCount);
            if (observer != null)
            {
                observer.begin();
                observers.add(observer);
            }
        }

        return ImmutableList.copyOf(observers);
    }

    public abstract void mark();

    /**
     * Appends partition data to this writer.
     *
     * @param partition the partition to write
     * @return the created index entry if something was written, that is if {@code iterator}
     * wasn't empty, {@code null} otherwise.
     *
     * @throws FSWriteError if a write to the dataFile fails
     */
    public RowIndexEntry append(UnfilteredRowIterator partition)
    {
        if (partition.isEmpty())
            return null;

        try
        {
            if (!startPartition(partition.partitionKey(), partition.partitionLevelDeletion()))
                return null;

            if (!partition.staticRow().isEmpty())
                addUnfiltered(partition.staticRow());

            while (partition.hasNext())
                addUnfiltered(partition.next());

            return endPartition();
        }
        catch (BufferOverflowException boe)
        {
            throw new PartitionSerializationException(partition, boe);
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, getFilename());
        }
    }

    /**
     * Start a partition. Will be followed by a sequence of addUnfiltered(), and finished with endPartition().
     * The static row may be given in the first addUnfiltered() (by cursors), or via addStaticRow() called before any
     * other addUnfiltered() (by append(UnfilteredRowIterator) above).
     *
     * @param key
     * @param partitionLevelDeletion
     * @return true if the partition was successfully started, false if there is a problem (e.g. key not in order).
     * @throws IOException
     */
    public abstract boolean startPartition(DecoratedKey key, DeletionTime partitionLevelDeletion) throws IOException;

    public abstract void addUnfiltered(Unfiltered unfiltered) throws IOException;

    public abstract RowIndexEntry endPartition() throws IOException;

    public abstract long getFilePointer();

    public abstract long getOnDiskFilePointer();

    public long getEstimatedOnDiskBytesWritten()
    {
        return getOnDiskFilePointer();
    }

    public abstract void resetAndTruncate();

    public SSTableWriter setRepairedAt(long repairedAt)
    {
        if (repairedAt > 0)
            this.repairedAt = repairedAt;
        return this;
    }

    public SSTableWriter setMaxDataAge(long maxDataAge)
    {
        this.maxDataAge = maxDataAge;
        return this;
    }

    public SSTableWriter setTokenSpaceCoverage(double rangeSpanned)
    {
        metadataCollector.tokenSpaceCoverage(rangeSpanned);
        return this;
    }

    /**
     * Open the resultant SSTableReader after it has been fully written.
     *
     * @param storageHandler the underlying storage handler. This is used in case of a failure opening the
     *                       SSTableReader to call the `StorageHandler#onOpeningWrittenSSTableFailure` callback, which
     *                       in some implementations may attempt to recover from the error. If `null`, the said callback
     *                       will not be called on failure.
     */
    public abstract void openResult(@Nullable StorageHandler storageHandler);

    /**
     * Open the resultant SSTableReader before it has been fully written
     */
    public abstract boolean openEarly(Consumer<SSTableReader> callWhenReady);

    /**
     * Open the resultant SSTableReader once it has been fully written, but before the
     * _set_ of tables that are being written together as one atomic operation are all ready
     */
    public abstract SSTableReader openFinalEarly();

    public SSTableReader finish(boolean openResult, @Nullable StorageHandler storageHandler)
    {
        prepareToCommit();
        if (openResult)
            openResult(storageHandler);
        txnProxy().commit();
        return finished();
    }

    /**
     * Open the resultant SSTableReader once it has been fully written, and all related state
     * is ready to be finalised including other sstables being written involved in the same operation
     */
    public abstract SSTableReader finished();

    // finalise our state on disk, including renaming
    public final void prepareToCommit()
    {
        try
        {
            txnProxy().prepareToCommit();
        }
        finally
        {
            // need to generate all index files before commit, so they will be included in txn log
            observers.forEach(obs -> obs.complete(this));

            // track newly written sstable after index files are written
            lifecycleNewTracker.trackNewWritten(this);
         }
    }

    // notify sstable flush observer about sstable writer switched
    public final void onSSTableWriterSwitched()
    {
        observers.forEach(SSTableFlushObserver::onSSTableWriterSwitched);
    }

    public final Throwable commit(Throwable accumulate)
    {
        return txnProxy().commit(accumulate);
    }

    public final Throwable abort(Throwable accumulate)
    {
        try
        {
            return txnProxy().abort(accumulate);
        }
        finally
        {
            observers.forEach(observer -> observer.abort(accumulate));
        }
    }

    public final void close()
    {
        txnProxy().close();
    }

    public final void abort()
    {
        try
        {
            txnProxy().abort();
        }
        finally
        {
            observers.forEach(observer -> observer.abort(null));
        }
    }

    protected Map<MetadataType, MetadataComponent> finalizeMetadata()
    {
        return metadataCollector.finalizeMetadata(getPartitioner().getClass().getCanonicalName(),
                                                  metadata().params.bloomFilterFpChance,
                                                  repairedAt,
                                                  pendingRepair,
                                                  isTransient,
                                                  header);
    }

    protected StatsMetadata statsMetadata()
    {
        return (StatsMetadata) finalizeMetadata().get(MetadataType.STATS);
    }

    public void releaseMetadataOverhead()
    {
        metadataCollector.release();
    }

    public static void rename(Descriptor tmpdesc, Descriptor newdesc, Set<Component> components)
    {
        for (Component component : Sets.difference(components, Sets.newHashSet(Component.DATA, Component.SUMMARY)))
        {
            tmpdesc.fileFor(component).move(newdesc.fileFor(component));
        }

        // do -Data last because -Data present should mean the sstable was completely renamed before crash
        tmpdesc.fileFor(Component.DATA).move(newdesc.fileFor(Component.DATA));

        // rename it without confirmation because summary can be available for loadNewSSTables but not for closeAndOpenReader
        if (components.contains(Component.SUMMARY))
            tmpdesc.fileFor(Component.SUMMARY).tryMove(newdesc.fileFor(Component.SUMMARY));
    }

    public static void copy(Descriptor tmpdesc, Descriptor newdesc, Set<Component> components)
    {
        for (Component component : Sets.difference(components, Sets.newHashSet(Component.DATA, Component.SUMMARY)))
        {
            FileUtils.copyWithConfirm(tmpdesc.fileFor(component), newdesc.fileFor(component));
        }

        // do -Data last because -Data present should mean the sstable was completely copied before crash
        FileUtils.copyWithConfirm(tmpdesc.fileFor(Component.DATA), newdesc.fileFor(Component.DATA));

        // copy it without confirmation because summary can be available for loadNewSSTables but not for closeAndOpenReader
        if (components.contains(Component.SUMMARY))
            FileUtils.copyWithOutConfirm(tmpdesc.fileFor(Component.SUMMARY), newdesc.fileFor(Component.SUMMARY));
    }

    public static void hardlink(Descriptor tmpdesc, Descriptor newdesc, Set<Component> components)
    {
        for (Component component : Sets.difference(components, Sets.newHashSet(Component.DATA, Component.SUMMARY)))
        {
            FileUtils.createHardLinkWithConfirm(tmpdesc.fileFor(component), newdesc.fileFor(component));
        }

        // do -Data last because -Data present should mean the sstable was completely copied before crash
        FileUtils.createHardLinkWithConfirm(tmpdesc.fileFor(Component.DATA), newdesc.fileFor(Component.DATA));

        // copy it without confirmation because summary can be available for loadNewSSTables but not for closeAndOpenReader
        if (components.contains(Component.SUMMARY))
            FileUtils.createHardLinkWithoutConfirm(tmpdesc.fileFor(Component.SUMMARY), newdesc.fileFor(Component.SUMMARY));
    }

    public interface SSTableSizeParameters
    {
        long partitionCount();
        long partitionKeySize();
        long dataSize();
    }

    public static abstract class Factory
    {
        public abstract long estimateSize(SSTableSizeParameters parameters);

        public abstract SSTableWriter open(Descriptor descriptor,
                                           long keyCount,
                                           long repairedAt,
                                           UUID pendingRepair,
                                           boolean isTransient,
                                           TableMetadataRef metadata,
                                           MetadataCollector metadataCollector,
                                           SerializationHeader header,
                                           Collection<SSTableFlushObserver> observers,
                                           LifecycleNewTracker lifecycleNewTracker,
                                           Set<Component> indexComponents);
    }

    protected void maybeLogLargePartitionWarning(DecoratedKey key, long rowSize)
    {
        if (isInternalKeyspace)
            return;

        if (Guardrails.partitionSize.triggersOn(rowSize, null))
        {
            String keyString = metadata().partitionKeyAsCQLLiteral(key.getKey());
            Guardrails.partitionSize.guard(rowSize, String.format("%s in %s", keyString, metadata), true, null);
        }
    }
}
