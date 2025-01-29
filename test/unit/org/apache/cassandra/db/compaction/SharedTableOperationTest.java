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

import org.apache.cassandra.utils.NonThrowingCloseable;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class SharedTableOperationTest {

    private SharedTableOperation sharedTableOperation;
    private TableOperationObserver mockObserver;
    private TableOperation mockOperation1;
    private TableOperation mockOperation2;
    private TableOperation mockOperation3;
    private TableOperation.Progress mockProgress;
    private NonThrowingCloseable mockCloseable;

    @Before
    public void setUp() {
        mockProgress = Mockito.mock(SharedTableOperation.Progress.class);
        sharedTableOperation = new SharedTableOperation(mockProgress);
        mockObserver = Mockito.mock(TableOperationObserver.class);
        mockCloseable = Mockito.mock(NonThrowingCloseable.class);
        mockOperation1 = Mockito.mock(TableOperation.class);
        mockOperation2 = Mockito.mock(TableOperation.class);
        mockOperation3 = Mockito.mock(TableOperation.class);

        when(mockObserver.onOperationStart(sharedTableOperation)).thenReturn(mockCloseable);
    }

    @Test
    public void testGetProgress() {
        assertEquals(mockProgress, sharedTableOperation.getProgress());
    }

    @Test
    public void testOneChild() {
        // Register expected subtask
        sharedTableOperation.registerExpectedSubtask();

        // Wrap observer
        TableOperationObserver wrappedObserver = sharedTableOperation.wrapObserver(mockObserver);

        // Start operation
        NonThrowingCloseable closeable = wrappedObserver.onOperationStart(mockOperation1);
        assertNotNull(closeable);

        // Verify observer communication
        verify(mockObserver, times(1)).onOperationStart(sharedTableOperation);

        // Close operation
        closeable.close();
        verify(mockCloseable, times(1)).close();
    }

    @Test
    public void testOneChildStop() {
        // Register expected subtask
        sharedTableOperation.registerExpectedSubtask();

        // Wrap observer
        TableOperationObserver wrappedObserver = sharedTableOperation.wrapObserver(mockObserver);

        // Start operation
        NonThrowingCloseable closeable = wrappedObserver.onOperationStart(mockOperation1);
        assertNotNull(closeable);
        // When stopped, an operation will also close itself
        Mockito.doAnswer(inv -> {
            closeable.close();
            return null;
        }).when(mockOperation1).stop(any());

        // Verify observer communication
        verify(mockObserver, times(1)).onOperationStart(sharedTableOperation);

        sharedTableOperation.stop(TableOperation.StopTrigger.CLEANUP);
        verify(mockOperation1, times(1)).stop(TableOperation.StopTrigger.CLEANUP);

        // Close operation
        verify(mockCloseable, times(1)).close();
    }

    @Test
    public void testThreeChildren() {
        // Register expected subtasks
        sharedTableOperation.registerExpectedSubtask();
        sharedTableOperation.registerExpectedSubtask();
        sharedTableOperation.registerExpectedSubtask();

        // Wrap observer
        TableOperationObserver wrappedObserver = sharedTableOperation.wrapObserver(mockObserver);

        // Start operations
        NonThrowingCloseable closeable1 = wrappedObserver.onOperationStart(mockOperation1);
        NonThrowingCloseable closeable2 = wrappedObserver.onOperationStart(mockOperation2);
        assertNotNull(closeable1);
        assertNotNull(closeable2);
        closeable1.close();

        verify(mockObserver, times(1)).onOperationStart(sharedTableOperation);
        verify(mockCloseable, times(0)).close();

        NonThrowingCloseable closeable3 = wrappedObserver.onOperationStart(mockOperation3);
        assertNotNull(closeable3);

        // Close operations
        closeable2.close();
        closeable3.close();

        verify(mockObserver, times(1)).onOperationStart(sharedTableOperation);
        verify(mockCloseable, times(1)).close();
    }

    @Test
    public void testThreeChildrenStop() {
        // Register expected subtasks
        sharedTableOperation.registerExpectedSubtask();
        sharedTableOperation.registerExpectedSubtask();
        sharedTableOperation.registerExpectedSubtask();

        // Wrap observer
        TableOperationObserver wrappedObserver = sharedTableOperation.wrapObserver(mockObserver);

        // Start first operation
        NonThrowingCloseable closeable1 = wrappedObserver.onOperationStart(mockOperation1);
        assertNotNull(closeable1);
        // When stopped, an operation will also close itself
        Mockito.doAnswer(inv -> {
            closeable1.close();
            return null;
        }).when(mockOperation1).stop(any());

        // Issue stop before starting the next operations
        sharedTableOperation.stop(TableOperation.StopTrigger.CLEANUP);
        verify(mockObserver, times(1)).onOperationStart(sharedTableOperation);
        verify(mockCloseable, times(0)).close();
        verify(mockOperation1, times(1)).stop(TableOperation.StopTrigger.CLEANUP);

        // Start remaining operations
        NonThrowingCloseable closeable2 = wrappedObserver.onOperationStart(mockOperation2);
        assertNotNull(closeable2);
        verify(mockOperation2, times(1)).stop(TableOperation.StopTrigger.CLEANUP);

        NonThrowingCloseable closeable3 = wrappedObserver.onOperationStart(mockOperation3);
        assertNotNull(closeable3);
        verify(mockOperation3, times(1)).stop(TableOperation.StopTrigger.CLEANUP);

        // In response to a stop request, the child process will finish and close itself as it executes
        closeable3.close();
        verify(mockCloseable, times(0)).close();
        closeable2.close(); // simulating a bit of disorder here

        // Verify observer communication
        verify(mockObserver, times(1)).onOperationStart(sharedTableOperation);
        verify(mockCloseable, times(1)).close();
        verify(mockOperation1, times(1)).stop(TableOperation.StopTrigger.CLEANUP);
        verify(mockOperation2, times(1)).stop(TableOperation.StopTrigger.CLEANUP);
        verify(mockOperation3, times(1)).stop(TableOperation.StopTrigger.CLEANUP);
    }
}
