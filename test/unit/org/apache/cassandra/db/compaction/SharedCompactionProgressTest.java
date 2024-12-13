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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;
import org.mockito.Mockito;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

/// Tests partially written by Copilot.
public class SharedCompactionProgressTest
{
    private SharedCompactionProgress sharedCompactionProgress;
    private UUID operationId;

    int id = 0;

    @Before
    public void setUp()
    {
        operationId = UUID.randomUUID();
        sharedCompactionProgress = new SharedCompactionProgress(operationId, OperationType.COMPACTION, TableOperation.Unit.BYTES);
    }

    private CompactionProgress getMockProgress()
    {
        CompactionProgress mockProgress = Mockito.mock(CompactionProgress.class);
        when(mockProgress.operationId()).thenReturn(UUIDGen.withSequence(operationId, id++));
        when(mockProgress.operationType()).thenReturn(OperationType.COMPACTION);
        when(mockProgress.unit()).thenReturn(TableOperation.Unit.BYTES);
        var input = mockSSTable("i");
        when(mockProgress.inSSTables()).thenReturn(input);
        var output = mockSSTable("o");
        when(mockProgress.outSSTables()).thenReturn(output);
        var sstables = mockSSTable("s");
        when(mockProgress.sstables()).thenReturn(sstables);
        when(mockProgress.inputDiskSize()).thenReturn(100L);
        when(mockProgress.outputDiskSize()).thenReturn(200L);
        when(mockProgress.uncompressedBytesRead()).thenReturn(300L);
        when(mockProgress.uncompressedBytesWritten()).thenReturn(400L);
        when(mockProgress.partitionsRead()).thenReturn(500L);
        when(mockProgress.rowsRead()).thenReturn(600L);
        when(mockProgress.completed()).thenReturn(700L);
        when(mockProgress.total()).thenReturn(800L);
        when(mockProgress.startTimeNanos()).thenReturn(900L);
        when(mockProgress.inputUncompressedSize()).thenReturn(1000L);
        when(mockProgress.adjustedInputDiskSize()).thenReturn(1100L);
        when(mockProgress.partitionsHistogram()).thenReturn(new long[]{1, 2, 3});
        when(mockProgress.rowsHistogram()).thenReturn(new long[]{4, 5, 6});
        return mockProgress;
    }

    private void checkProgress(CompactionProgress progress, int count)
    {
        assertEquals(count, progress.inSSTables().size());
        assertTrue(progress.inSSTables().stream().map(Object::toString).allMatch(s -> s.startsWith("i")));
        assertEquals(count, progress.outSSTables().size());
        assertTrue(progress.outSSTables().stream().map(Object::toString).allMatch(s -> s.startsWith("o")));
        assertEquals(count, progress.sstables().size());
        assertTrue(progress.sstables().stream().map(Object::toString).allMatch(s -> s.startsWith("s")));
        assertEquals(100L * count, progress.inputDiskSize());
        assertEquals(200L * count, progress.outputDiskSize());
        assertEquals(300L * count, progress.uncompressedBytesRead());
        assertEquals(400L * count, progress.uncompressedBytesWritten());
        assertEquals(500L * count, progress.partitionsRead());
        assertEquals(600L * count, progress.rowsRead());
        assertEquals(700L * count, progress.completed());
        assertEquals(800L * count, progress.total());
        assertEquals(900L, progress.startTimeNanos());
        assertEquals(1000L * count, progress.inputUncompressedSize());
        assertEquals(1100L * count, progress.adjustedInputDiskSize());
        assertArrayEquals(new long[]{1 * count, 2 * count, 3 * count}, progress.partitionsHistogram());
        assertArrayEquals(new long[]{4 * count, 5 * count, 6 * count}, progress.rowsHistogram());
    }

    private Set<SSTableReader> mockSSTable(String nameprefix)
    {
        SSTableReader readerMock = Mockito.mock(SSTableReader.class);
        when(readerMock.toString()).thenReturn(nameprefix + ThreadLocalRandom.current().nextInt());
        return ImmutableSet.of(readerMock);
    }

    @Test
    public void testCompleteSubtask()
    {
        CompactionProgress mockProgress = getMockProgress();
        sharedCompactionProgress.registerExpectedSubtask();
        sharedCompactionProgress.addSubtask(mockProgress);
        checkProgress(sharedCompactionProgress, 1);
        boolean isComplete = sharedCompactionProgress.completeSubtask(mockProgress);
        checkProgress(sharedCompactionProgress, 1);
        assertTrue(isComplete);
    }

    @Test
    public void testComplete2Subtasks()
    {
        sharedCompactionProgress.registerExpectedSubtask();
        sharedCompactionProgress.registerExpectedSubtask();
        CompactionProgress mockProgress1 = getMockProgress();
        CompactionProgress mockProgress2 = getMockProgress();
        sharedCompactionProgress.addSubtask(mockProgress1);
        checkProgress(sharedCompactionProgress, 1);
        sharedCompactionProgress.addSubtask(mockProgress2);
        boolean isComplete = sharedCompactionProgress.completeSubtask(mockProgress2);
        assertFalse(isComplete);
        isComplete = sharedCompactionProgress.completeSubtask(mockProgress1);
        assertTrue(isComplete);
        checkProgress(sharedCompactionProgress, 2);
    }

    @Test
    public void testComplete2SubtasksLateStart()
    {
        sharedCompactionProgress.registerExpectedSubtask();
        sharedCompactionProgress.registerExpectedSubtask();
        CompactionProgress mockProgress = getMockProgress();
        sharedCompactionProgress.addSubtask(mockProgress);
        boolean isComplete = sharedCompactionProgress.completeSubtask(mockProgress);
        assertFalse(isComplete);
        checkProgress(sharedCompactionProgress, 1);
        mockProgress = getMockProgress();
        sharedCompactionProgress.addSubtask(mockProgress);
        isComplete = sharedCompactionProgress.completeSubtask(mockProgress);
        assertTrue(isComplete);
        checkProgress(sharedCompactionProgress, 2);
    }

    @Test
    public void testConcurrentAccess() throws InterruptedException, ExecutionException
    {
        int threadCount = 100;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        for (int i = 0; i < threadCount; ++i)
            sharedCompactionProgress.registerExpectedSubtask();

        AtomicInteger completed = new AtomicInteger(0);
        List<Future<?>> futures = new ArrayList<>();

        for (int i = 0; i < threadCount; i++)
        {
            futures.add(executor.submit(() ->
                {
                    CompactionProgress mockProgress = getMockProgress();
                    sharedCompactionProgress.addSubtask(mockProgress);
                    if (ThreadLocalRandom.current().nextBoolean())
                        FBUtilities.sleepQuietly(ThreadLocalRandom.current().nextInt(1));
                    if (sharedCompactionProgress.completeSubtask(mockProgress))
                        completed.incrementAndGet();
                }));
        }

        for (Future<?> future : futures)
            future.get();

        assertEquals(1, completed.get());
        executor.shutdown();
        checkProgress(sharedCompactionProgress, threadCount);
    }
}