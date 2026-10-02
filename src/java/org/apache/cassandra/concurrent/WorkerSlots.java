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

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import io.netty.util.concurrent.FastThreadLocal;

import static org.apache.cassandra.utils.MonotonicClock.Global.approxTime;

/**
 * The running task of each worker thread of one {@link java.util.concurrent.ThreadPoolExecutor}, stamped from its
 * {@code beforeExecute}/{@code afterExecute} hooks and scanned by its liveness gauges. A worker registers its slot on
 * its first task; slots of exited threads are pruned when a new worker registers and when a gauge is read, never on
 * the per-task path. No locks and no background threads.
 * <p>
 * The slot is held in a static thread-local, so a worker thread must belong to exactly one executor. This rules out
 * work-stealing pools whose threads serve several executors, such as {@link SharedExecutorPool}'s {@link SEPWorker},
 * which must not use WorkerSlots; {@link SEPExecutor} tracks its running tasks through {@link SEPWorker} instead.
 */
final class WorkerSlots
{
    // a worker thread belongs to exactly one executor, so a single static index serves every pool. Held lazily: the
    // FastThreadLocal initialises netty's InternalThreadLocalMap and its logger, which must not happen at executor
    // construction (the in-JVM dtest Instance builds an executor before it sets the node's log identity)
    private static final class Holder
    {
        static final FastThreadLocal<WorkerSlot> SLOT = new FastThreadLocal<>();
    }

    private final List<WorkerSlot> slots = new CopyOnWriteArrayList<>();

    /**
     * One worker thread's running task, written only by that thread. The worker writes the plain taskClassName, then
     * publishes it with the volatile store of startedAtNanos; readers read startedAtNanos first, then taskClassName,
     * and capture both in the same scan, so they see at least the class name of the task whose stamp they read (a read
     * racing a task boundary sees a newer class name). The idle clear is a release-only lazySet. The name is held
     * rather than the Class, so an idle worker pins no class or class loader.
     */
    static final class WorkerSlot
    {
        private static final AtomicLongFieldUpdater<WorkerSlot> startedAtNanosUpdater = AtomicLongFieldUpdater.newUpdater(WorkerSlot.class, "startedAtNanos");

        final Thread thread;
        volatile long startedAtNanos;   // 0 when idle
        String taskClassName;

        WorkerSlot(Thread thread)
        {
            this.thread = thread;
        }
    }

    /** The oldest running task: the stamp that selected it and its class name, both captured in the scan, never re-read. */
    static final class Running
    {
        final long capturedStartNanos;
        final String taskClassName;

        Running(long capturedStartNanos, String taskClassName)
        {
            this.capturedStartNanos = capturedStartNanos;
            this.taskClassName = taskClassName;
        }
    }

    /** Called on the worker thread before each task. */
    void markRunning(Class<?> taskClass)
    {
        WorkerSlot slot = Holder.SLOT.get();
        if (slot == null)
            slot = register(Thread.currentThread());
        slot.taskClassName = taskClass.getName();
        slot.startedAtNanos = approxTime.now();
    }

    // once per worker thread; prunes here too, as executors without metrics never read the gauges
    private WorkerSlot register(Thread thread)
    {
        WorkerSlot slot = new WorkerSlot(thread);
        Holder.SLOT.set(slot);
        slots.removeIf(s -> !s.thread.isAlive());
        slots.add(slot);
        return slot;
    }

    /** Called on the worker thread after each task. */
    static void markIdle()
    {
        WorkerSlot slot = Holder.SLOT.get();
        if (slot != null)
            WorkerSlot.startedAtNanosUpdater.lazySet(slot, 0L);
    }

    /** The oldest task running on a live worker, or null when none is; prunes slots of exited threads. */
    Running oldestRunning()
    {
        long oldestStart = Long.MAX_VALUE;
        String oldestClassName = null;
        boolean sawExited = false;
        for (WorkerSlot s : slots)
        {
            // a thread can die stamped (beforeExecute threw), so liveness is checked whatever the stamp
            if (!s.thread.isAlive())
            {
                sawExited = true;
                continue;
            }
            long start = s.startedAtNanos;
            String taskClassName = s.taskClassName;
            if (start != 0L && start < oldestStart)
            {
                oldestStart = start;
                oldestClassName = taskClassName;
            }
        }
        if (sawExited)
            slots.removeIf(s -> !s.thread.isAlive());
        return oldestStart == Long.MAX_VALUE ? null : new Running(oldestStart, oldestClassName);
    }

    int size()
    {
        return slots.size();
    }
}
