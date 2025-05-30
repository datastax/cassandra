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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.DiskBoundaries;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.commitlog.IntervalSet;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.io.sstable.StorageHandler;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.utils.FBUtilities;

public class RangeAwareSSTableWriter implements SSTableMultiWriter
{
    private final List<Token> boundaries;
    private final List<Directories.DataDirectory> directories;
    private final int sstableLevel;
    private final IntervalSet<CommitLogPosition> commitLogIntervals;
    private final long estimatedKeys;
    private final long repairedAt;
    private final UUID pendingRepair;
    private final boolean isTransient;
    private final SSTableFormat.Type format;
    private final SerializationHeader header;
    private final LifecycleNewTracker lifecycleNewTracker;
    private int currentIndex = -1;
    public final ColumnFamilyStore cfs;
    private final List<SSTableMultiWriter> finishedWriters = new ArrayList<>();
    private final List<SSTableReader> finishedReaders = new ArrayList<>();
    private SSTableMultiWriter currentWriter = null;

    public RangeAwareSSTableWriter(ColumnFamilyStore cfs, long estimatedKeys, long repairedAt, UUID pendingRepair, boolean isTransient, SSTableFormat.Type format, IntervalSet<CommitLogPosition> commitLogIntervals, int sstableLevel, long totalSize, LifecycleNewTracker lifecycleNewTracker, SerializationHeader header) throws IOException
    {
        DiskBoundaries db = cfs.getDiskBoundaries();
        directories = db.directories;
        this.sstableLevel = sstableLevel;
        this.cfs = cfs;
        this.estimatedKeys = estimatedKeys / directories.size();
        this.repairedAt = repairedAt;
        this.pendingRepair = pendingRepair;
        this.isTransient = isTransient;
        this.format = format;
        this.lifecycleNewTracker = lifecycleNewTracker;
        this.header = header;
        this.commitLogIntervals = commitLogIntervals;
        boundaries = db.getPositions();
        if (boundaries == null)
        {
            Directories.DataDirectory localDir = cfs.getDirectories().getWriteableLocation(totalSize);
            if (localDir == null)
                throw new IOException(String.format("Insufficient disk space to store %s",
                                                    FBUtilities.prettyPrintMemory(totalSize)));
            Descriptor desc = cfs.newSSTableDescriptor(cfs.getDirectories().getLocationForDisk(localDir), format);
            currentWriter = cfs.createSSTableMultiWriter(desc, estimatedKeys, repairedAt, pendingRepair, isTransient, commitLogIntervals, sstableLevel, header, lifecycleNewTracker);
        }
    }

    private void maybeSwitchWriter(DecoratedKey key)
    {
        if (boundaries == null)
            return;

        boolean switched = false;
        while (currentIndex < 0 || key.getToken().compareTo(boundaries.get(currentIndex)) > 0)
        {
            switched = true;
            currentIndex++;
        }

        if (switched)
        {
            if (currentWriter != null)
                finishedWriters.add(currentWriter);

            Descriptor desc = cfs.newSSTableDescriptor(cfs.getDirectories().getLocationForDisk(directories.get(currentIndex)), format);
            currentWriter = cfs.createSSTableMultiWriter(desc, estimatedKeys, repairedAt, pendingRepair, isTransient, commitLogIntervals, sstableLevel, header, lifecycleNewTracker);
        }
    }

    public void append(UnfilteredRowIterator partition)
    {
        maybeSwitchWriter(partition.partitionKey());
        currentWriter.append(partition);
    }

    @Override
    public Collection<SSTableReader> finish(boolean openResult, StorageHandler storageHandler)
    {
        if (currentWriter != null)
            finishedWriters.add(currentWriter);
        currentWriter = null;
        for (SSTableMultiWriter writer : finishedWriters)
        {
            if (writer.getBytesWritten() > 0)
                finishedReaders.addAll(writer.finish(openResult, storageHandler));
            else
                SSTableMultiWriter.abortOrDie(writer);
        }
        return finishedReaders;
    }

    @Override
    public Collection<SSTableReader> finished()
    {
        return finishedReaders;
    }

    @Override
    public void openResult(StorageHandler storageHandler)
    {
        finishedWriters.forEach(w -> w.openResult(storageHandler));
        currentWriter.openResult(storageHandler);
    }

    public String getFilename()
    {
        return String.join("/", cfs.keyspace.getName(), cfs.getTableName());
    }

    @Override
    public long getBytesWritten()
    {
        long bytesWritten = currentWriter != null ? currentWriter.getBytesWritten() : 0L;
        for (SSTableMultiWriter writer : finishedWriters)
            bytesWritten += writer.getBytesWritten();
        return bytesWritten;
    }

    @Override
    public long getOnDiskBytesWritten()
    {
        long bytesWritten = currentWriter != null ? currentWriter.getOnDiskBytesWritten() : 0L;
        for (SSTableMultiWriter writer : finishedWriters)
            bytesWritten += writer.getOnDiskBytesWritten();
        return bytesWritten;
    }

    @Override
    public int getSegmentCount()
    {
        return finishedWriters.size() + 1;
    }

    @Override
    public TableId getTableId()
    {
        return cfs.metadata.id;
    }

    @Override
    public Throwable commit(Throwable accumulate)
    {
        if (currentWriter != null)
            finishedWriters.add(currentWriter);
        currentWriter = null;
        for (SSTableMultiWriter writer : finishedWriters)
            accumulate = writer.commit(accumulate);
        return accumulate;
    }

    @Override
    public Throwable abort(Throwable accumulate)
    {
        if (currentWriter != null)
            finishedWriters.add(currentWriter);
        currentWriter = null;
        for (SSTableMultiWriter finishedWriter : finishedWriters)
            accumulate = finishedWriter.abort(accumulate);

        return accumulate;
    }

    @Override
    public void prepareToCommit()
    {
        if (currentWriter != null)
            finishedWriters.add(currentWriter);
        currentWriter = null;
        finishedWriters.forEach(SSTableMultiWriter::prepareToCommit);
    }

    @Override
    public void close()
    {
        if (currentWriter != null)
            finishedWriters.add(currentWriter);
        currentWriter = null;
        finishedWriters.forEach(SSTableMultiWriter::close);
    }
}
