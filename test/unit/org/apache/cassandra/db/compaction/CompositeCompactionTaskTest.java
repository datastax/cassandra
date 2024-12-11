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
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CompositeCompactionTaskTest
{
    private CompactionRealm mockRealm;
    private LifecycleTransaction mockTransaction;
    private CompositeCompactionTask compositeCompactionTask;
    private AbstractCompactionTask mockTask1;
    private AbstractCompactionTask mockTask2;

    @Before
    public void setUp() {
        mockRealm = Mockito.mock(CompactionRealm.class);
        mockTransaction = Mockito.mock(LifecycleTransaction.class);
        when(mockTransaction.isOffline()).thenReturn(true);
        when(mockTransaction.opId()).thenReturn(UUID.randomUUID());
        when(mockRealm.tryModify(any(), any(), any())).thenReturn(mockTransaction);

        mockTask1 = Mockito.mock(AbstractCompactionTask.class, Mockito.withSettings().useConstructor(mockRealm, mockTransaction));
        mockTask2 = Mockito.mock(AbstractCompactionTask.class, Mockito.withSettings().useConstructor(mockRealm, mockTransaction));
        compositeCompactionTask = CompositeCompactionTask.combineTasks(mockTask1, mockTask2);
    }

    @Test
    public void testExecute() {
        // Testing executeInternal() instead of execute() because we cannot mock transaction.close()
        compositeCompactionTask.executeInternal();
        verify(mockTask1, times(1)).execute();
        verify(mockTask2, times(1)).execute();
    }

    @Test
    public void testRejected() {
        compositeCompactionTask.rejected(null);
        verify(mockTask1, times(1)).rejected(null);
        verify(mockTask2, times(1)).rejected(null);
    }

    @Test
    public void testSetUserDefined() {
        compositeCompactionTask.setUserDefined(true);
        verify(mockTask1, times(1)).setUserDefined(true);
        verify(mockTask2, times(1)).setUserDefined(true);
    }

    @Test
    public void testSetCompactionType() {
        OperationType compactionType = OperationType.COMPACTION;
        compositeCompactionTask.setCompactionType(compactionType);
        verify(mockTask1, times(1)).setCompactionType(compactionType);
        verify(mockTask2, times(1)).setCompactionType(compactionType);
    }

    @Test
    public void testSetOpObserver() {
        TableOperationObserver opObserver = Mockito.mock(TableOperationObserver.class);
        compositeCompactionTask.setOpObserver(opObserver);
        verify(mockTask1, times(1)).setOpObserver(opObserver);
        verify(mockTask2, times(1)).setOpObserver(opObserver);
    }

    @Test
    public void testAddObserver() {
        CompactionObserver compObserver = Mockito.mock(CompactionObserver.class);
        compositeCompactionTask.addObserver(compObserver);
        verify(mockTask1, times(1)).addObserver(compObserver);
        verify(mockTask2, times(1)).addObserver(compObserver);
    }

    @Test
    public void testExecuteWithException() {
        doThrow(new RuntimeException("Test Exception")).when(mockTask1).execute();
        assertThrows(RuntimeException.class, () -> compositeCompactionTask.executeInternal());
        verify(mockTask1, times(1)).execute();
        verify(mockTask2, times(1)).execute();
    }

    @Test
    public void testApplyParallelismLimit_NoLimit() {
        testApplyParallelismLimit(3, 0);
    }

    @Test
    public void testApplyParallelismLimit_LimitGreaterThanTasks() {
        testApplyParallelismLimit(3, 3);
        testApplyParallelismLimit(5, 6);
    }

    @Test
    public void testApplyParallelismLimit_LimitLessThanTasks()
    {
        testApplyParallelismLimit(5, 2);
        testApplyParallelismLimit(8, 4);
    }

    private void testApplyParallelismLimit(int taskCount, int limit) {
        List<AbstractCompactionTask> tasks = createMockTasks(taskCount);
        List<AbstractCompactionTask> result = CompositeCompactionTask.applyParallelismLimit(tasks, limit);

        assertEquals(limit > 0 ? Math.min(limit, taskCount) : taskCount, result.size());
        assertEquals(taskCount, result.stream().flatMap(t -> t instanceof CompositeCompactionTask ? ((CompositeCompactionTask) t).tasks.stream()
                                                                                                  : Stream.of(t)).count());

        for (AbstractCompactionTask task : result) {
            if (task instanceof CompositeCompactionTask)
                task.executeInternal(); // can't call execute() because it will call transaction.close()
            else
                task.execute();
        }
        for (AbstractCompactionTask task : tasks) {
            verify(task, times(1)).execute();
        }
    }

    private List<AbstractCompactionTask> createMockTasks(int count) {
        List<AbstractCompactionTask> tasks = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            AbstractCompactionTask task = mock(AbstractCompactionTask.class, Mockito.withSettings().useConstructor(mockRealm, mockTransaction));
            tasks.add(task);
        }
        return tasks;
    }
}
