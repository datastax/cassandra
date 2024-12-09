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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.utils.FBUtilities;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;

import static org.mockito.Mockito.*;

public class SharedCompactionObserverTest
{
    private SharedCompactionObserver sharedCompactionObserver;
    private CompactionObserver mockObserver;
    private CompactionProgress mockProgress;
    private UUID operationId;


    @Before
    public void setUp()
    {
        mockObserver = Mockito.mock(CompactionObserver.class);
        sharedCompactionObserver = new SharedCompactionObserver(mockObserver);
        operationId = UUID.randomUUID();
        mockProgress = Mockito.mock(CompactionProgress.class);
        when(mockProgress.operationId()).thenReturn(operationId);
    }

    @Test
    public void testOnInProgress()
    {
        sharedCompactionObserver.onInProgress(mockProgress);
        verify(mockObserver, times(1)).onInProgress(mockProgress);
    }

    @Test
    public void testOnCompleted()
    {
        sharedCompactionObserver.registerExpectedSubtask();
        sharedCompactionObserver.onInProgress(mockProgress);
        sharedCompactionObserver.onCompleted(operationId, true);
        verify(mockObserver, times(1)).onCompleted(operationId, true);
    }

    @Test
    public void testOnCompletedFailure()
    {
        sharedCompactionObserver.registerExpectedSubtask();
        sharedCompactionObserver.onInProgress(mockProgress);
        sharedCompactionObserver.onCompleted(operationId, false);
        verify(mockObserver, times(1)).onCompleted(operationId, false);
    }

    @Test
    public void testMultipleSubtasksCompletion()
    {
        sharedCompactionObserver.registerExpectedSubtask();
        sharedCompactionObserver.registerExpectedSubtask();
        sharedCompactionObserver.onInProgress(mockProgress);
        sharedCompactionObserver.onInProgress(mockProgress);

        sharedCompactionObserver.onCompleted(operationId, true);
        verify(mockObserver, never()).onCompleted(any(UUID.class), anyBoolean());

        sharedCompactionObserver.onCompleted(operationId, true);
        verify(mockObserver, times(1)).onCompleted(operationId, true);
    }

    @Test
    public void testMultipleSubtasksInProgressAfterCompletion()
    {
        sharedCompactionObserver.registerExpectedSubtask();
        sharedCompactionObserver.registerExpectedSubtask();
        sharedCompactionObserver.onInProgress(mockProgress);
        sharedCompactionObserver.onCompleted(operationId, true);
        verify(mockObserver, never()).onCompleted(any(UUID.class), anyBoolean());

        sharedCompactionObserver.onInProgress(mockProgress);
        sharedCompactionObserver.onCompleted(operationId, true);
        verify(mockObserver, times(1)).onCompleted(operationId, true);
    }

    @Test
    public void testMultipleSubtasksCompletionWithFailure()
    {
        sharedCompactionObserver.registerExpectedSubtask();
        sharedCompactionObserver.registerExpectedSubtask();
        sharedCompactionObserver.onInProgress(mockProgress);
        sharedCompactionObserver.onInProgress(mockProgress);

        sharedCompactionObserver.onCompleted(operationId, true);
        verify(mockObserver, never()).onCompleted(any(UUID.class), anyBoolean());

        sharedCompactionObserver.onCompleted(operationId, false);
        verify(mockObserver, times(1)).onCompleted(operationId, false);
    }

    @Test
    public void testConcurrentAccess() throws InterruptedException, ExecutionException
    {
        int threadCount = 100;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++)
            sharedCompactionObserver.registerExpectedSubtask();
        List<Future<?>> futures = new ArrayList<>();

        for (int i = 0; i < threadCount; i++)
        {
            futures.add(executor.submit(() ->
                                        {
                                            sharedCompactionObserver.onInProgress(mockProgress);
                                            if (ThreadLocalRandom.current().nextBoolean())
                                                FBUtilities.sleepQuietly(ThreadLocalRandom.current().nextInt(1));
                                            sharedCompactionObserver.onCompleted(operationId, true);
                                            latch.countDown();
                                        }));
        }

        for (Future<?> future : futures)
            future.get();

        verify(mockObserver, times(1)).onInProgress(mockProgress);
        verify(mockObserver, times(1)).onCompleted(operationId, true);
        executor.shutdown();
    }

    @Test
    public void testErrorNoRegister()
    {
        Util.assumeAssertsEnabled();
        Assert.assertThrows(AssertionError.class, () -> sharedCompactionObserver.onCompleted(operationId, true));
    }

    @Test
    public void testErrorNoInProgress()
    {
        Util.assumeAssertsEnabled();
        sharedCompactionObserver.registerExpectedSubtask();
        Assert.assertThrows(AssertionError.class, () -> sharedCompactionObserver.onCompleted(operationId, true));
    }

    @Test
    public void testErrorWrongProgress()
    {
        Util.assumeAssertsEnabled();
        sharedCompactionObserver.registerExpectedSubtask();
        sharedCompactionObserver.registerExpectedSubtask();
        sharedCompactionObserver.onInProgress(mockProgress);
        var mockProgress2 = Mockito.mock(CompactionProgress.class);
        when(mockProgress2.operationId()).thenReturn(UUID.randomUUID());
        Assert.assertThrows(AssertionError.class, () -> sharedCompactionObserver.onInProgress(mockProgress2));
    }
}