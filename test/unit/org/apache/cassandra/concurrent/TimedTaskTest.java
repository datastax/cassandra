/*
 * Copyright IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.concurrent;

import java.util.concurrent.Callable;

import org.junit.Test;

import org.apache.cassandra.utils.WithResources;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class TimedTaskTest
{
    private static final class UserTask implements Runnable
    {
        public void run() {}
    }

    private static final class UserDebuggableTask implements DebuggableTask.RunnableDebuggableTask
    {
        public void run() {}
        public long creationTimeNanos() { return 0; }
        public long startTimeNanos() { return 0; }
        public String description() { return "test"; }
    }

    @Test
    public void ageClampsAtZero()
    {
        assertEquals(0L, TimedTask.ageNanos(100L, 50L));
        assertEquals(25L, TimedTask.ageNanos(25L, 50L));
    }

    @Test
    public void unstampedTaskIsNotAged()
    {
        assertEquals(0L, TimedTask.queuedNanos(new FutureTask<>(new UserTask())));
        assertEquals(0L, TimedTask.queuedNanos(new UserTask()));
        assertEquals(0L, TimedTask.queuedNanos(null));
    }

    @Test
    public void stampedTaskIsAged()
    {
        FutureTask<?> task = new FutureTask<>(new UserTask());
        task.markEnqueued(1L);
        assertEquals(1L, task.enqueuedAtNanos());
        assertTrue(TimedTask.queuedNanos(task) > 0);
    }

    @Test
    public void wrappersReportTheUserClass()
    {
        UserTask task = new UserTask();
        Callable<Integer> callable = () -> 42;
        for (TaskFactory factory : new TaskFactory[]{ TaskFactory.standard(), TaskFactory.localAware() })
        {
            assertSame(UserTask.class, WrappedTask.classOf(factory.toExecute(task)));
            assertSame(UserTask.class, WrappedTask.classOf(factory.toExecute(WithResources.none(), task)));
            assertSame(UserTask.class, WrappedTask.classOf(factory.toSubmit(task)));
            assertSame(UserTask.class, WrappedTask.classOf(factory.toSubmit(task, 42)));
            assertSame(UserTask.class, WrappedTask.classOf(factory.toSubmit(ExecutorLocals.propagate(), task)));
            assertSame(callable.getClass(), WrappedTask.classOf(factory.toSubmit(callable)));
        }
        assertSame(UserDebuggableTask.class, WrappedTask.classOf(TaskFactory.localAware().toExecute(new UserDebuggableTask())));
        assertSame(UserDebuggableTask.class, WrappedTask.classOf(new FutureTask<>(FutureTask.callable(new UserDebuggableTask()))));
        assertSame(UserTask.class, WrappedTask.classOf(FutureTask.callable("id", task)));
        assertSame(UserTask.class, WrappedTask.classOf(FutureTask.callable("id", task, 42)));
        assertSame(UserTask.class, WrappedTask.classOf(task));
    }

    @Test
    public void debuggableWrappersStayDebuggable()
    {
        UserDebuggableTask task = new UserDebuggableTask();
        assertTrue(TaskFactory.localAware().toExecute(task) instanceof DebuggableTask);
        assertSame(task, ((FutureTask<?>) TaskFactory.localAware().toSubmit(task)).debuggableTask());
        assertTrue(FutureTask.callable(task) instanceof DebuggableTask.CallableDebuggableTask);
    }

    @Test
    public void runFinishedFutureReportsItsOwnClass()
    {
        FutureTask<?> task = new FutureTask<>(new UserTask());
        task.run();
        assertSame(FutureTask.class, task.taskClass());
    }

    @Test
    public void defaultInterfaceMethodsReportIdle()
    {
        ResizableThreadPool pool = new ResizableThreadPool()
        {
            public int getCorePoolSize() { return 0; }
            public void setCorePoolSize(int n) {}
            public int getMaximumPoolSize() { return 0; }
            public void setMaximumPoolSize(int n) {}
            public int getActiveTaskCount() { return 0; }
            public long getCompletedTaskCount() { return 0; }
            public int getPendingTaskCount() { return 0; }
        };
        assertEquals(0L, pool.oldestTaskQueueTime());
        assertEquals(0L, pool.longestRunningTaskTime());
        assertNull(pool.getLongestRunningTaskClass());
    }
}
