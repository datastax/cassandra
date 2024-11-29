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

package org.apache.cassandra.db.compaction;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import javax.annotation.Nullable;

import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.TableMetadata;

/// Shared compaction progress tracker. This combines the progress tracking of multiple compaction tasks into a single
/// progress tracker, and of reporting completion of all tasks when all of them complete.
///
/// Subtasks may start and add themselves in any order. There may also be periods of time when all started tasks have
/// completed but there are new ones to still initiate. Because of this all parameters returned by this progress may
/// increase over time, including the total sizes and sstable lists.
///
/// To know how many subtasks to expect, this class's [#registerExpectedSubtask] method must be called once per subtask
/// before any of them start.
public class SharedCompactionProgress implements CompactionProgress
{
    private final List<CompactionProgress> sources = new CopyOnWriteArrayList<>();
    private final AtomicInteger toComplete = new AtomicInteger(0);
    private final UUID operationId;
    private final OperationType operationType;
    private final TableOperation.Unit unit;

    public SharedCompactionProgress(UUID operationId, OperationType operationType, TableOperation.Unit unit)
    {
        this.operationId = operationId;
        this.operationType = operationType;
        this.unit = unit;
    }

    public void registerExpectedSubtask()
    {
        toComplete.incrementAndGet();
    }

    public void addSubtask(CompactionProgress progress)
    {
        sources.add(progress);
        assert progress.operationType() == operationType;
        assert progress.unit() == unit;
    }

    /// Mark a subtask as complete. Returns true if the caller is the last subtask to complete.
    /// This must be called once per subtask.
    /// Note that completion is determined by the number of tasks expected to run, not by the set that is currently
    /// registered/running.
    /// @param progress The progress of the subtask that is complete (currently unused)
    public boolean completeSubtask(CompactionProgress progress)
    {
        return toComplete.decrementAndGet() == 0;
    }

    @Nullable
    @Override
    public CompactionStrategy strategy()
    {
        if (sources.isEmpty())
            return null;
        return sources.get(0).strategy();
    }


    @Override
    public Optional<String> keyspace()
    {
        if (sources.isEmpty())
            return Optional.empty();
        return sources.get(0).keyspace();
    }

    @Override
    public Optional<String> table()
    {
        if (sources.isEmpty())
            return Optional.empty();
        return sources.get(0).table();
    }

    @Nullable
    @Override
    public TableMetadata metadata()
    {
        if (sources.isEmpty())
            return null;
        return sources.get(0).metadata();
    }

    @Override
    public OperationType operationType()
    {
        return operationType;
    }

    @Override
    public UUID operationId()
    {
        return operationId;
    }

    @Override
    public TableOperation.Unit unit()
    {
        return unit;
    }

    @Override
    public Set<SSTableReader> inSSTables()
    {
        Set<SSTableReader> set = new HashSet<>();
        for (CompactionProgress source : sources)
            set.addAll(source.inSSTables());

        return set;
    }

    @Override
    public Set<SSTableReader> outSSTables()
    {
        Set<SSTableReader> set = new HashSet<>();
        for (CompactionProgress source : sources)
            set.addAll(source.outSSTables());

        return set;
    }

    @Override
    public Set<SSTableReader> sstables()
    {
        Set<SSTableReader> set = new HashSet<>();
        for (CompactionProgress p : sources)
            set.addAll(p.sstables());

        return set;
    }

    @Override
    public long inputDiskSize()
    {
        long sum = 0L;
        for (CompactionProgress source : sources)
            sum += source.inputDiskSize();

        return sum;
    }

    @Override
    public long inputUncompressedSize()
    {
        long sum = 0L;
        for (CompactionProgress source : sources)
            sum += source.inputUncompressedSize();

        return sum;
    }

    @Override
    public long adjustedInputDiskSize()
    {
        long sum = 0L;
        for (CompactionProgress source : sources)
            sum += source.adjustedInputDiskSize();

        return sum;
    }

    @Override
    public long outputDiskSize()
    {
        long sum = 0L;
        for (CompactionProgress source : sources)
            sum += source.outputDiskSize();

        return sum;
    }

    @Override
    public long uncompressedBytesRead()
    {
        long sum = 0L;
        for (CompactionProgress source : sources)
            sum += source.uncompressedBytesRead();

        return sum;
    }

    @Override
    public long uncompressedBytesRead(int level)
    {
        long sum = 0L;
        for (CompactionProgress source : sources)
            sum += source.uncompressedBytesRead(level);

        return sum;
    }

    @Override
    public long uncompressedBytesWritten()
    {
        long sum = 0L;
        for (CompactionProgress source : sources)
            sum += source.uncompressedBytesWritten();

        return sum;
    }

    @Override
    public long partitionsRead()
    {
        long sum = 0L;
        for (CompactionProgress source : sources)
            sum += source.partitionsRead();

        return sum;
    }

    @Override
    public long rowsRead()
    {
        long sum = 0L;
        for (CompactionProgress source : sources)
            sum += source.rowsRead();

        return sum;
    }

    @Override
    public long completed()
    {
        long sum = 0L;
        for (CompactionProgress source : sources)
            sum += source.completed();

        return sum;
    }

    @Override
    public long total()
    {
        long sum = 0L;
        for (CompactionProgress source : sources)
            sum += source.total();

        return sum;
    }

    @Override
    public long startTimeNanos()
    {
        long min = Long.MAX_VALUE;
        for (CompactionProgress source : sources)
            min = Math.min(min, source.startTimeNanos());

        return min;
    }

    @Override
    public long[] partitionsHistogram()
    {
        return mergeHistograms(CompactionProgress::partitionsHistogram);
    }

    @Override
    public long[] rowsHistogram()
    {
        return mergeHistograms(CompactionProgress::rowsHistogram);
    }

    private long[] mergeHistograms(Function<CompactionProgress, long[]> retriever)
    {
        long[] merged = new long[0];
        for (CompactionProgress source : sources)
        {
            long[] histogram = retriever.apply(source);
            if (histogram.length > merged.length)
                merged = Arrays.copyOf(merged, histogram.length);
            for (int i = 0; i < histogram.length; i++)
                merged[i] += histogram[i];
        }
        return merged;
    }
}
