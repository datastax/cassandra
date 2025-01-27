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

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.utils.Throwables;

/// A composition of several compaction tasks into one. This object executes the given tasks sequentially and
/// is used to limit the parallelism of some compaction tasks that split into a large number of parallelizable ones
/// but should not be allowed to take all compaction executor threads.
public class CompositeCompactionTask extends AbstractCompactionTask
{
    @VisibleForTesting
    final ArrayList<AbstractCompactionTask> tasks;

    public CompositeCompactionTask(AbstractCompactionTask first)
    {
        super(first.realm, first.realm.tryModify(Collections.emptyList(), OperationType.COMPACTION, first.transaction.opId()));
        tasks = new ArrayList<>();
        addTask(first);
    }

    /// Add a task to the composition.
    public CompositeCompactionTask addTask(AbstractCompactionTask task)
    {
        tasks.add(task);
        return this;
    }

    @Override
    protected void runMayThrow() throws Exception
    {
        // Run all tasks in sequence, regardless if any of them fail.
        Throwable accumulate = null;
        for (AbstractCompactionTask task : tasks)
        {
            accumulate = Throwables.perform(accumulate, () -> task.execute(opObserver));
            // The previous operation may have completed due to a requested stop. We do not stop other tasks in our
            // list if that is the case, because if the tasks are related, the [SharedTableOperation] will have already
            // requested a stop from the other components as well. If we stopped the other tasks here, we may
            // overrespond to a user's request to stop an individual operation.
            // On the other hand, [CompactionManager] sometimes requests a stop of all ongoing operations e.g. to
            // initiate a table drop. Such requests, however, do not affect tasks in the executor queue; as this class
            // is acting similarly to an executor queue, we do not apply such stop requests to the remaining tasks
            // either.
        }
        Throwables.maybeFail(accumulate);
    }

    @Override
    public Throwable rejected(Throwable t)
    {
        for (AbstractCompactionTask task : tasks)
            t = task.rejected(t);
        return super.rejected(t);
    }

    @Override
    public AbstractCompactionTask setUserDefined(boolean isUserDefined)
    {
        for (AbstractCompactionTask task : tasks)
            task.setUserDefined(isUserDefined);
        return super.setUserDefined(isUserDefined);
    }

    @Override
    public AbstractCompactionTask setCompactionType(OperationType compactionType)
    {
        for (AbstractCompactionTask task : tasks)
            task.setCompactionType(compactionType);
        return super.setCompactionType(compactionType);
    }

    @Override
    public void addObserver(CompactionObserver compObserver)
    {
        for (AbstractCompactionTask task : tasks)
            task.addObserver(compObserver);
        super.addObserver(compObserver);
    }

    @Override
    public String toString()
    {
        return "Composite " + tasks;
    }

    /// Limit the parallelism of a list of compaction tasks by combining them into a smaller number of composite tasks.
    /// This method assumes that the caller has preference for the tasks to be executed in order close to the order of
    /// the input list. See [UnifiedCompactionStrategy#getMaximalTasks] for an example of how to use this method.
    public static List<AbstractCompactionTask> applyParallelismLimit(List<AbstractCompactionTask> tasks, int parallelismLimit)
    {
        if (tasks.size() <= parallelismLimit || parallelismLimit <= 0)
            return tasks;

        List<AbstractCompactionTask> result = new ArrayList<>(parallelismLimit);
        int taskIndex = 0;
        for (AbstractCompactionTask task : tasks)
        {
            if (result.size() < parallelismLimit)
                result.add(task);
            else
            {
                result.set(taskIndex, combineTasks(result.get(taskIndex), task));
                if (++taskIndex == parallelismLimit)
                    taskIndex = 0;
            }
        }
        return result;
    }

    /// Make a composite tasks that combines two tasks. If the former is already a composite task, the latter is added
    /// to it. Otherwise, a new composite task is created.
    public static CompositeCompactionTask combineTasks(AbstractCompactionTask task1, AbstractCompactionTask task2)
    {
        CompositeCompactionTask composite;
        if (task1 instanceof CompositeCompactionTask)
            composite = (CompositeCompactionTask) task1;
        else
            composite = new CompositeCompactionTask(task1);
        return composite.addTask(task2);
    }

}
