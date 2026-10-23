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

package org.apache.cassandra.index.sai.utils;

import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A fan-out harness: a producer dispatches units of work onto an executor under a <b>pluggable
 * admission policy</b>. Thread concurrency is always bounded by the supplied executor; whether the
 * <i>weight</i> (e.g. bytes) of work in flight — queued + running — is also bounded is decided by the
 * concrete subtype, so the policy is swappable via configuration:
 * <ul>
 *   <li>{@link BoundedFanout} — a weighted semaphore admits at most a fixed total weight, parking the
 *       producer (no busy-wait) when the budget is exhausted; designed backpressure.</li>
 *   <li>{@link UnboundedFanout} — no admission control; every unit is dispatched immediately, so both
 *       the count and the memory of in-flight tasks are unbounded (bounded only by the executor).</li>
 * </ul>
 *
 * <p>Neither admission nor the completion barrier busy-waits: bounded admission parks on a semaphore,
 * and {@link #awaitCompletion()} parks on a monitor.
 *
 * <p><b>Contract.</b> {@link #submit} must not be called from one of the executor's own worker threads
 * (bounded admission could park the caller waiting for a permit only a worker can release —
 * self-starvation). {@code submit} and {@code awaitCompletion} are expected on a single producer
 * thread; the dispatched tasks may complete on many threads.
 */
public abstract class InsertFanout
{
    private final Executor executor;

    /** Tasks dispatched by this instance that have not yet completed. */
    private final AtomicLong inFlight = new AtomicLong();
    private final Object completion = new Object();
    private final AtomicReference<Throwable> firstError = new AtomicReference<>();

    protected InsertFanout(Executor executor)
    {
        this.executor = executor;
    }

    /**
     * Admit a unit weighing {@code weight}, blocking (parking) if the policy requires it.
     *
     * @return the amount to hand back to {@link #release} when the unit completes (allows a subtype to
     *         clamp or ignore the requested weight)
     */
    protected abstract int admit(int weight);

    /** Release admission previously granted by {@link #admit}. */
    protected abstract void release(int admitted);

    /**
     * Admit {@code weight} units (per the policy), then run {@code task} on the executor, releasing the
     * admission when it completes. The first task to throw is captured and surfaced via {@link #error()}.
     *
     * @param weight the work's weight in the budget's units (e.g. bytes); values &lt; 1 are treated as 1
     */
    public final void submit(int weight, Runnable task)
    {
        final int admitted = admit(weight);
        inFlight.incrementAndGet();
        boolean handedOff = false;
        try
        {
            executor.execute(() ->
            {
                try
                {
                    task.run();
                }
                catch (Throwable t)
                {
                    firstError.compareAndSet(null, t);
                }
                finally
                {
                    release(admitted);
                    completed();
                }
            });
            handedOff = true;
        }
        finally
        {
            // The executor rejected the task (e.g. shutdown): undo the admission and the in-flight count
            // so we neither leak permits nor block awaitCompletion() forever.
            if (!handedOff)
            {
                release(admitted);
                completed();
            }
        }
    }

    /** The first throwable thrown by a dispatched task, or {@code null} if none has failed. */
    public final Throwable error()
    {
        return firstError.get();
    }

    /**
     * Park (no spin) until every task dispatched by this instance has completed. Does not throw on a
     * task failure — callers inspect {@link #error()}. Must be called from the producer thread (no
     * concurrent {@link #submit}).
     */
    public final void awaitCompletion()
    {
        synchronized (completion)
        {
            while (inFlight.get() > 0)
            {
                try
                {
                    completion.wait();
                }
                catch (InterruptedException e)
                {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted while draining insert fan-out", e);
                }
            }
        }
    }

    private void completed()
    {
        if (inFlight.decrementAndGet() == 0L)
        {
            synchronized (completion)
            {
                completion.notifyAll();
            }
        }
    }
}
