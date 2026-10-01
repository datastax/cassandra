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
 */
final class WorkerSlots
{
    // a worker thread belongs to exactly one executor, so a single static index serves every pool
    private static final FastThreadLocal<WorkerSlot> SLOT = new FastThreadLocal<>();

    private final List<WorkerSlot> slots = new CopyOnWriteArrayList<>();

    /**
     * One worker thread's running task, written only by that thread. The worker writes the plain taskClass, then
     * publishes it with the volatile store of startedAtNanos; readers read startedAtNanos first, then taskClass, and
     * capture both in the same scan, so they see at least the class of the task whose stamp they read (a read racing
     * a task boundary sees a newer class). The idle clear is a release-only lazySet.
     */
    static final class WorkerSlot
    {
        private static final AtomicLongFieldUpdater<WorkerSlot> startedAtNanosUpdater = AtomicLongFieldUpdater.newUpdater(WorkerSlot.class, "startedAtNanos");

        final Thread thread;
        volatile long startedAtNanos;   // 0 when idle
        Class<?> taskClass;

        WorkerSlot(Thread thread)
        {
            this.thread = thread;
        }
    }

    /** The oldest running task: the stamp that selected it and its class, both captured in the scan, never re-read. */
    static final class Running
    {
        final long capturedStartNanos;
        final Class<?> taskClass;

        Running(long capturedStartNanos, Class<?> taskClass)
        {
            this.capturedStartNanos = capturedStartNanos;
            this.taskClass = taskClass;
        }
    }

    /** Called on the worker thread before each task. */
    void markRunning(Class<?> taskClass)
    {
        WorkerSlot slot = SLOT.get();
        if (slot == null)
            slot = register(Thread.currentThread());
        slot.taskClass = taskClass;
        slot.startedAtNanos = approxTime.now();
    }

    // once per worker thread; prunes here too, as executors without metrics never read the gauges
    private WorkerSlot register(Thread thread)
    {
        WorkerSlot slot = new WorkerSlot(thread);
        SLOT.set(slot);
        slots.removeIf(s -> !s.thread.isAlive());
        slots.add(slot);
        return slot;
    }

    /** Called on the worker thread after each task. */
    static void markIdle()
    {
        WorkerSlot slot = SLOT.get();
        if (slot != null)
            WorkerSlot.startedAtNanosUpdater.lazySet(slot, 0L);
    }

    /** The oldest task running on a live worker, or null when none is; prunes slots of exited threads. */
    Running oldestRunning()
    {
        WorkerSlot oldest = null;
        long oldestStart = Long.MAX_VALUE;
        Class<?> oldestClass = null;
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
            Class<?> taskClass = s.taskClass;
            if (start != 0L && start < oldestStart)
            {
                oldestStart = start;
                oldestClass = taskClass;
                oldest = s;
            }
        }
        if (sawExited)
            slots.removeIf(s -> !s.thread.isAlive());
        return oldest == null ? null : new Running(oldestStart, oldestClass);
    }

    int size()
    {
        return slots.size();
    }
}
