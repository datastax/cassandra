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

package org.apache.cassandra.index.sai.disk.vector;

import java.util.ArrayDeque;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.Consumer;
import java.util.function.IntConsumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import io.github.jbellis.jvector.graph.ParallelExecutor;

/**
 * Cassandra's implementation of jvector's {@link ParallelExecutor} seam: it runs a jvector
 * iteration on an executor Cassandra owns, chunked the way Cassandra chooses.
 *
 * <p>This is deliberately Cassandra's own rather than one of jvector's factories. The factories
 * each pin a policy — {@code forkJoin} requires a {@link java.util.concurrent.ForkJoinPool}
 * specifically, {@code callerRuns} gives up parallelism — and the policy question here (which
 * executor, how wide, how finely to chunk) belongs to the host that sized the pool. Taking a plain
 * {@link ExecutorService} means the shared build pool can stop being a {@code ForkJoinPool}
 * without touching any jvector call site.
 *
 * <h2>The drain guarantee</h2>
 * When a call returns — normally, on failure, or on interrupt — <b>no body is still running</b>.
 * That is not a nicety, and this is the class that has to get it right: the merge reads source
 * vectors out of memory-mapped SAI components, and {@code SegmentBuilder} releases the
 * {@code SSTableIndex} pin holding those mappings once {@code merge()} returns. A body still
 * reading after an early unwind would fault on an unmapped page — a JVM-level SIGSEGV, not
 * something the compaction framework could catch and report. So a failing iteration stops issuing
 * chunks, lets not-yet-started chunks become no-ops, waits out every chunk that did start, and only
 * then rethrows the first failure.
 *
 * <p>An interrupt is handled the same way: recorded, remaining chunks skipped, started chunks
 * waited out, and the thread's interrupt flag restored before unwinding. Never a shortcut out from
 * under running work.
 *
 * <p>Nested use from inside a body runs inline rather than submitting: a chunk that blocked waiting
 * on sub-chunks of the same bounded executor could starve it into deadlock, with every worker
 * waiting on chunks that can never be scheduled.
 *
 * <p>Only the <em>body</em> is distributed. A stream source is materialized on the calling thread
 * and then split by element count exactly as {@link #forEachInt} splits an index range — never
 * into fixed-size batches. A fixed batch (this class once used 32) is tuned for per-node
 * streams and is a trap for coarse ones: jvector's ordinal-assignment pass hands over four
 * quarter-million-node tasks per source, and a 32-element batch folded them onto one thread for
 * 55 minutes on a 16M-node merge. Splitting by count cannot do that; the price is holding the
 * materialized elements, which for the compactor's list-backed streams is nothing new.
 */
public final class BoundedParallelExecutor implements ParallelExecutor
{
    /** Chunks per worker: more than one smooths skewed per-element cost. */
    private static final int CHUNKS_PER_WORKER = 4;

    // A body that iterates again must not wait on the same bounded executor it is running inside.
    private static final ThreadLocal<Boolean> IN_BODY = ThreadLocal.withInitial(() -> Boolean.FALSE);

    private final ExecutorService executor;
    private final int parallelism;

    /**
     * @param executor    runs the chunks; its lifecycle stays with the caller and it is never shut
     *                    down here
     * @param parallelism the executor's intended width, used to pick chunk granularity and to bound
     *                    the in-flight window; values below 1 are treated as 1
     */
    public BoundedParallelExecutor(ExecutorService executor, int parallelism)
    {
        this.executor = executor;
        this.parallelism = Math.max(1, parallelism);
    }

    @Override
    public void forEachInt(int upperBound, IntConsumer body)
    {
        if (upperBound <= 0)
            return;

        if (IN_BODY.get())
        {
            for (int i = 0; i < upperBound; i++)
                body.accept(i);
            return;
        }

        int chunks = Math.min(upperBound, CHUNKS_PER_WORKER * parallelism);
        // The chunk count is fixed and small, so every chunk can be in flight at once; there is no
        // unbounded queue to guard against.
        Drain drain = new Drain(Integer.MAX_VALUE);
        for (int c = 0; c < chunks && drain.healthy(); c++)
        {
            int start = (int) ((long) upperBound * c / chunks);
            int end = (int) ((long) upperBound * (c + 1) / chunks);
            drain.submit(() -> {
                for (int i = start; i < end; i++)
                    body.accept(i);
            });
        }
        drain.finish();
    }

    @Override
    public void forEach(IntStream source, IntConsumer body)
    {
        if (IN_BODY.get())
        {
            source.forEach(body);
            return;
        }
        int[] items = source.toArray();
        forEachInt(items.length, i -> body.accept(items[i]));
    }

    @Override
    public <T> void forEach(Stream<T> source, Consumer<T> body)
    {
        if (IN_BODY.get())
        {
            source.forEach(body);
            return;
        }
        List<T> items = source.collect(Collectors.toList());
        forEachInt(items.size(), i -> body.accept(items.get(i)));
    }

    /**
     * Tracks one iteration's in-flight chunks and guarantees nothing is still running when
     * {@link #finish} returns.
     */
    private final class Drain
    {
        private final int window;
        private final ArrayDeque<Future<?>> inFlight = new ArrayDeque<>();
        // Written by the orchestrating thread, read by workers: a failing iteration stops issuing
        // chunks here and not-yet-started chunks become no-ops.
        private volatile boolean aborted;
        private Throwable failure;
        private boolean interrupted;

        Drain(int window)
        {
            this.window = window;
        }

        boolean healthy()
        {
            return failure == null && !interrupted;
        }

        void fail(Throwable t)
        {
            if (failure == null)
                failure = t;
            aborted = true;
        }

        void submit(Runnable chunk)
        {
            if (!healthy())
                return;

            try
            {
                inFlight.add(executor.submit(() -> {
                    if (aborted)
                        return; // iteration is failing: skip a chunk that has not begun

                    IN_BODY.set(Boolean.TRUE);
                    try
                    {
                        chunk.run();
                    }
                    finally
                    {
                        IN_BODY.remove();
                    }
                }));
            }
            catch (RejectedExecutionException e)
            {
                fail(e);
                return;
            }

            if (inFlight.size() >= window)
                settle(inFlight.poll());
        }

        /** Waits {@code f} out, across interrupts, recording any failure. */
        private void settle(Future<?> f)
        {
            while (true)
            {
                try
                {
                    f.get();
                    return;
                }
                catch (ExecutionException e)
                {
                    fail(e.getCause());
                    return;
                }
                catch (InterruptedException e)
                {
                    interrupted = true; // note it and keep waiting: no unwind under running work
                    aborted = true;     // unstarted chunks need not run
                }
            }
        }

        /** Awaits every outstanding chunk, then rethrows the first failure and restores the interrupt flag. */
        void finish()
        {
            while (!inFlight.isEmpty())
                settle(inFlight.poll());

            if (interrupted)
                Thread.currentThread().interrupt();

            if (failure instanceof RuntimeException)
                throw (RuntimeException) failure;
            if (failure instanceof Error)
                throw (Error) failure;
            if (failure != null)
                throw new RuntimeException(failure);
            if (interrupted)
                throw new RuntimeException(new InterruptedException("interrupted while awaiting a jvector iteration"));
        }
    }
}
