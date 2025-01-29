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

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.cassandra.utils.NonThrowingCloseable;

/// A [TableOperation] tracking the progress and offering stop control of a composite operation.
/// This class is used for [UnifiedCompactionStrategy]'s parallelized compactions together with
/// [SharedCompactionProgress] and [SharedCompactionObserver]. It uses a shared progress to present an integrated view
/// of the composite operation for a [TableOperationObserver] (e.g [org.apache.cassandra.db.compaction.ActiveOperations]).
public class SharedTableOperation extends AbstractTableOperation implements TableOperation, TableOperationObserver
{
    private final Progress sharedProgress;
    private NonThrowingCloseable obsCloseable;
    private final List<TableOperation> components = new CopyOnWriteArrayList<>();
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicInteger toClose = new AtomicInteger(0);
    private final AtomicReference<TableOperationObserver> observer = new AtomicReference<>(null);
    private volatile boolean isGlobal;

    public SharedTableOperation(Progress sharedProgress)
    {
        this.sharedProgress = sharedProgress;
    }

    public void registerExpectedSubtask()
    {
        toClose.incrementAndGet();
    }

    @Override
    public Progress getProgress()
    {
        return sharedProgress;
    }

    @Override
    public void stop(StopTrigger trigger)
    {
        super.stop(trigger);
        // Stop all ongoing subtasks
        for (TableOperation component : components)
            component.stop(trigger);
        // We will also issue a stop immediately after the start of any operation that is still to initiate in
        // [onOperationStart].
    }

    @Override
    public boolean isGlobal()
    {
        return isGlobal;
    }

    public TableOperationObserver wrapObserver(TableOperationObserver observer)
    {
        if (!this.observer.compareAndSet(null, observer))
            assert this.observer.get() == observer : "All components must use the same observer";
        // We will register with the observer when one of the components starts.

        // Note: if the observer is Noop, we still want to wrap to complete the shared operation when all subtasks complete.
        return this;
    }

    @Override
    public NonThrowingCloseable onOperationStart(TableOperation operation)
    {
        if (started.compareAndSet(false, true))
        {
            obsCloseable = observer.get().onOperationStart(this);
            isGlobal = operation.isGlobal();
        }
        // Save the component reference to be able to stop it if needed.
        components.add(operation);

        if (isStopRequested())
            operation.stop(trigger());
        return this::closeOne;
    }

    private void closeOne()
    {
        final int stillToClose = toClose.decrementAndGet();
        if (stillToClose == 0 && obsCloseable != null)
            obsCloseable.close();
        assert stillToClose >= 0 : "Closed more than expected";
    }
}
