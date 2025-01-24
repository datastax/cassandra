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

import org.apache.cassandra.utils.NonThrowingCloseable;

public class SharedTableOperation extends AbstractTableOperation implements TableOperation
{
    private final Progress sharedProgress;
    private NonThrowingCloseable obsCloseable;
    private final List<TableOperation> components = new CopyOnWriteArrayList<>();
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicInteger toClose = new AtomicInteger(0);

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
        for (TableOperation component : components)
            component.stop(trigger);
    }

    @Override
    public boolean isGlobal()
    {
        if (components.isEmpty())
            return false;
        else
            return components.get(0).isGlobal();
    }

    public TableOperationObserver wrapObserver(TableOperationObserver observer)
    {
        // Note: if the observer is Noop, we still want to wrap to complete the shared operation when all subtasks complete
        return operation -> onOperationStart(observer, operation);
    }

    public NonThrowingCloseable onOperationStart(TableOperationObserver observer, TableOperation operation)
    {
        if (started.compareAndSet(false, true))
            obsCloseable = observer.onOperationStart(this);
        components.add(operation);
        if (isStopRequested())
            operation.stop(trigger());
        return this::closeOne;
    }

    private void closeOne()
    {
        if (toClose.decrementAndGet() == 0 && obsCloseable != null)
            obsCloseable.close();
    }
}
