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

import java.io.EOFException;
import java.io.IOException;
import java.util.Collection;
import java.util.EnumMap;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.io.sstable.StorageHandler;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.io.util.SequentialWriterOption;
import org.apache.cassandra.net.AsyncStreamingInputPlus;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadataRef;

import static java.lang.String.format;
import static org.apache.cassandra.utils.FBUtilities.prettyPrintMemory;

public class SSTableZeroCopyWriter extends SSTable implements SSTableMultiWriter
{
    private static final Logger logger = LoggerFactory.getLogger(SSTableZeroCopyWriter.class);

    private volatile SSTableReader finalReader;
    private final Map<Component.Type, SequentialWriter> componentWriters;
    private final LifecycleNewTracker lifecycleNewTracker;

    private static final SequentialWriterOption WRITER_OPTION =
        SequentialWriterOption.newBuilder()
                              .trickleFsync(false)
                              .bufferSize(2 << 20)
                              .bufferType(BufferType.OFF_HEAP)
                              .build();

    public SSTableZeroCopyWriter(Descriptor descriptor,
                                 TableMetadataRef metadata,
                                 LifecycleNewTracker lifecycleNewTracker,
                                 final Collection<Component> components)
    {
        super(descriptor, ImmutableSet.copyOf(components), metadata, DatabaseDescriptor.getDiskOptimizationStrategy());
        this.lifecycleNewTracker = lifecycleNewTracker;

        lifecycleNewTracker.trackNew(this);
        this.componentWriters = new EnumMap<>(Component.Type.class);

        if (!descriptor.getFormat().streamingComponents().containsAll(components))
            throw new AssertionError(format("Unsupported streaming component detected %s",
                                            Sets.difference(ImmutableSet.copyOf(components), descriptor.getFormat().streamingComponents())));

        for (Component c : components)
            componentWriters.put(c.type, makeWriter(descriptor, c));
    }

    private static SequentialWriter makeWriter(Descriptor descriptor, Component component)
    {
        return new SequentialWriter(descriptor.fileFor(component), WRITER_OPTION, false);
    }

    private void write(DataInputPlus in, long size, SequentialWriter out) throws FSWriteError
    {
        final int BUFFER_SIZE = 1 << 20;
        long bytesRead = 0;
        byte[] buff = new byte[BUFFER_SIZE];
        try
        {
            while (bytesRead < size)
            {
                int toRead = (int) Math.min(size - bytesRead, BUFFER_SIZE);
                in.readFully(buff, 0, toRead);
                int count = Math.min(toRead, BUFFER_SIZE);
                out.write(buff, 0, count);
                bytesRead += count;
            }
            out.sync(); // finish will also call sync(). Leaving here to get stuff flushed as early as possible
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, out.getFile());
        }
    }

    @Override
    public void append(UnfilteredRowIterator partition)
    {
        throw new UnsupportedOperationException("Operation not supported by BigTableBlockWriter");
    }

    @Override
    public Collection<SSTableReader> finish(boolean openResult, StorageHandler storageHandler)
    {
        for (SequentialWriter writer : componentWriters.values())
            writer.finish();

        SSTable.appendTOC(descriptor, components());

        lifecycleNewTracker.trackNewWritten(this);
        return finished();
    }

    @Override
    public Collection<SSTableReader> finished()
    {
        if (finalReader == null)
            finalReader = descriptor.getFormat().getReaderFactory().open(descriptor, components(), metadata);

        return ImmutableList.of(finalReader);
    }

    @Override
    public void openResult(StorageHandler storageHandler)
    {
    }

    @Override
    public long getBytesWritten()
    {
        // TODO: these two may need fixing.
        return 0;
    }

    @Override
    public long getOnDiskBytesWritten()
    {
        return 0;
    }

    @Override
    public int getSegmentCount()
    {
        return 1;
    }

    @Override
    public TableId getTableId()
    {
        return metadata.id;
    }

    @Override
    public Throwable commit(Throwable accumulate)
    {
        for (SequentialWriter writer : componentWriters.values())
            accumulate = writer.commit(accumulate);
        return accumulate;
    }

    @Override
    public Throwable abort(Throwable accumulate)
    {
        for (SequentialWriter writer : componentWriters.values())
            accumulate = writer.abort(accumulate);
        return accumulate;
    }

    @Override
    public void prepareToCommit()
    {
        for (SequentialWriter writer : componentWriters.values())
            writer.prepareToCommit();

        lifecycleNewTracker.trackNewWritten(this);
    }

    @Override
    public void close()
    {
        for (SequentialWriter writer : componentWriters.values())
            writer.close();
    }

    public void writeComponent(Component.Type type, DataInputPlus in, long size)
    {
        logger.info("Writing component {} to {} length {}", type, componentWriters.get(type).getFile(), prettyPrintMemory(size));

        if (in instanceof AsyncStreamingInputPlus)
            write((AsyncStreamingInputPlus) in, size, componentWriters.get(type));
        else
            write(in, size, componentWriters.get(type));
    }

    private void write(AsyncStreamingInputPlus in, long size, SequentialWriter writer)
    {
        logger.info("Block Writing component to {} length {}", writer.getFile(), prettyPrintMemory(size));

        try
        {
            in.consume(writer::writeDirectlyToChannel, size);
            writer.sync();
        }
        // FIXME: handle ACIP exceptions properly
        catch (EOFException | AsyncStreamingInputPlus.InputTimeoutException e)
        {
            in.close();
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, writer.getFile());
        }
    }
}
