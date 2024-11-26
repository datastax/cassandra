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

import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/// Utility class to share a compaction observer among multiple compaction tasks and only report start and completion
/// once when the first task starts and completion when all tasks complete (successfully or not, where the passed
/// `isSuccess` state is a logical and of the subtasks').
///
/// Because subtasks may start in any order, we need to know the number of tasks in advance. This is done by calling
/// [#registerExpectedSubtask] once per subtask before starting any of them.
///
/// This class assumes that all subtasks use the same progress object and the same transaction id, and will verify that
/// if assertions are enabled.
public class SharedCompactionObserver implements CompactionObserver
{
    private final AtomicInteger toReportOnComplete = new AtomicInteger(0);
    private final AtomicBoolean onCompleteIsSuccess = new AtomicBoolean(true);
    private final AtomicReference<CompactionProgress> inProgressReported = new AtomicReference<>(null);
    private final CompactionObserver observer;

    public SharedCompactionObserver(CompactionObserver observer)
    {
        this.observer = observer;
    }

    public void registerExpectedSubtask()
    {
        toReportOnComplete.incrementAndGet();
        assert inProgressReported.get() == null
            : "Task started before all subtasks registered for operation " + inProgressReported.get().operationId();
    }

    @Override
    public void onInProgress(CompactionProgress progress)
    {
        if (inProgressReported.compareAndSet(null, progress))
            observer.onInProgress(progress);
        else
            assert inProgressReported.get() == progress; // progress object must also be shared
    }

    @Override
    public void onCompleted(UUID id, boolean isSuccess)
    {
        onCompleteIsSuccess.compareAndSet(true, isSuccess); // acts like AND
        final int remainingToComplete = toReportOnComplete.decrementAndGet();
        assert inProgressReported.get() != null : "onCompleted called before onInProgress";
        assert remainingToComplete >= 0 : "onCompleted called without corresponding registerExpectedSubtask";
        // The individual operation ID given here may be different from the shared ID. Pass on the shared one.
        if (remainingToComplete == 0)
            observer.onCompleted(inProgressReported.get().operationId(), onCompleteIsSuccess.get());
    }
}
