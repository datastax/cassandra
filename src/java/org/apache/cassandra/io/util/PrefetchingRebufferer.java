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

package org.apache.cassandra.io.util;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.CompletableFuture; // checkstyle: permit this import
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Meter;
import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.metrics.DefaultNameFactory;
import org.apache.cassandra.metrics.MetricNameFactory;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.Throwables;

import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

/// A rebufferer that prefetches the next N buffers in sequential order.
///
/// *Not* thread-safe (does not need to be as readers aren't).
public class PrefetchingRebufferer implements Rebufferer
{
    private static final Logger LOGGER = LoggerFactory.getLogger(PrefetchingRebufferer.class);
    private static final NoSpamLogger NO_SPAM_LOGGER = NoSpamLogger.getLogger(LOGGER, 1, TimeUnit.MINUTES);

    private static final int PREFETCHING_SIZE_KB = CassandraRelevantProperties.READ_PREFETCHING_SIZE_KB.getInt();
    private static final double PREFETCHING_WINDOW = CassandraRelevantProperties.READ_PREFETCHING_WINDOW.getDouble();
    private static final int PREFETCHING_THREADS = CassandraRelevantProperties.READ_PREFETCHING_THREADS.getInt(FBUtilities.getAvailableProcessors());

    private static final boolean ENABLED = PREFETCHING_SIZE_KB > 0;

    @VisibleForTesting
    public static final PrefetchingMetrics metrics;

    private static final ExecutorPlus executor;

    static
    {
            if (ENABLED)
            {
                // Technically, we re-validate the window in the ctor for good measure, but in practice, not point in waiting
                // until the first prefetching read happens before erroring if the configuration is incorrect.
                Preconditions.checkArgument(PREFETCHING_WINDOW >= 0 && PREFETCHING_WINDOW <= 1, "Invalid prefetching window value: %s", PREFETCHING_WINDOW);
                Preconditions.checkArgument(PREFETCHING_THREADS > 0, "Invalid prefetching threads: %s", PREFETCHING_THREADS);

                LOGGER.info("Prefetching is enabled for sequential reads (e.g. range queries, compactions); size={}kb and window={}", PREFETCHING_SIZE_KB, PREFETCHING_WINDOW);

                metrics = new PrefetchingMetrics();
                executor = executorFactory().withJmxInternal().pooled("ReadPrefetching", PREFETCHING_THREADS);
            }
            else
            {
                metrics = null;
                executor = null;
            }
    }

    /// Rebufferer factory on top of which prefetching is applied.
    private final RebuffererFactory source;

    /// Number or [Rebufferer] we're already created. We only create up to [#prefetchSize] + 1 rebufferer and then reuse
    /// them, but we instantiate them lazily so this tell use if we have created our max already.
    private int createdRebuffers;

    /// The rebufferer that generated the last [BufferHolder] returned by [#rebuffer]. We cannot reuse that rebufferer
    /// until the next call to [#rebuffer], because only then can we assume the buffer was released. This rebufferer is
    /// why we create [#prefetchSize] + 1 rebufferers.
    private ReusedRebufferer lastReturnedRebufferer;

    /// As mentioned above, we reuse rebufferer (to save allocations) and this queue contains rebufferers that have
    /// been created but are not currently used by a [PrefetchedEntry] in [#queue].
    private final Deque<ReusedRebufferer> unusedRebufferers;

    /// The buffers that are being/have been prefetched (but have not yet be requested by [#rebuffer]).
    private final Deque<PrefetchedEntry> queue;

    /// The number of buffers prefetched when prefetch is triggered ().
    private final int prefetchSize;

    /// The minimum number of buffers that should be prefetched; as soon as we have less than this number of prefetched
    /// buffers, we fetch up to [#prefetchSize].
    private final int windowSize;

    /** We expect the buffer size to be a power of 2, this is the mask for aligning to the buffer size */
    private final int alignmentMask;

    private PrefetchingRebufferer(RebuffererFactory source)
    {
        this(source, PREFETCHING_SIZE_KB * 1024, PREFETCHING_WINDOW);
    }

    @VisibleForTesting
    PrefetchingRebufferer(RebuffererFactory source, int prefetchingSize, double window)
    {
        assert Integer.bitCount(source.chunkSize()) == 1 : String.format("%d must be a power of two", source.chunkSize());
        assert prefetchingSize > 0 : String.format("prefetching size %d must be > 0", prefetchingSize);
        assert window >= 0 && window <= 1 : String.format("prefetching window %f must be in [0, 1]", window);

        this.source = source;
        this.prefetchSize = (int)Math.ceil((double)prefetchingSize / source.chunkSize());
        this.windowSize = (int)Math.ceil(window * prefetchSize);
        this.unusedRebufferers = new ArrayDeque<>(prefetchSize);
        this.queue = new ArrayDeque<>(prefetchSize);
        this.alignmentMask = -source.chunkSize();
    }

    public static Rebufferer withPrefetching(RebuffererFactory factory)
    {
        if (!ENABLED)
            return factory.instantiateRebufferer(false);

        int chunkSize = factory.chunkSize();
        // No `chunkSize` typically means mmap, where prefetching doesn't make sense anyway (the OS already does it)
        if (chunkSize <= 0)
            return factory.instantiateRebufferer(false);

        if (Integer.bitCount(chunkSize) != 1)
        {
            NO_SPAM_LOGGER.debug("Prefecting is enabled for SEQUENTIAL reads on {} but the chunk size {} is not a " +
                                 "power of two (should only true for zero-copied files); will not prefetch.",
                                 factory.channel().filePath(), chunkSize);
            return factory.instantiateRebufferer(false);
        }

        return new PrefetchingRebufferer(factory);
    }

    private int chunkSize()
    {
        return source.chunkSize();
    }

    @Override
    public BufferHolder rebuffer(long position)
    {
        // Only now do we know it is safe to reuse the rebufferer from the previous call to this method.
        if (lastReturnedRebufferer != null)
            unusedRebufferers.add(lastReturnedRebufferer);

        if (position >= fileLength())
        {
            assert position == fileLength() : "Requested seek to " + position + " but file length is " + fileLength();
            return Rebufferer.EMPTY;
        }

        long pageAlignedPos = position & alignmentMask;

        PrefetchedEntry entry = queue.poll();
        boolean isNonSequential = false;

        // Release any prefetched buffers that are before the requested position.
        while (entry != null && entry.position < pageAlignedPos)
        {
            isNonSequential = true;
            unusedRebufferers.add(entry.release());
            entry = queue.poll();
        }

        // If the next entry matches, use it, but also trigger futher prefetch if necessary.
        if (entry != null && entry.position == pageAlignedPos)
        {
            prefetch(pageAlignedPos + chunkSize());

            if (!entry.isReady())
                metrics.notReady.mark();

            BufferHolder holder = entry.get();
            lastReturnedRebufferer = entry.forReuse();
            return holder;
        }

        // We get here in 2 cases:
        // 1. `entry == null`: this is either the first call to this rebufferer, or we've seeked forward since the
        //    last `rebuffer` and all the prefected entries where discared by the loop above.
        // 2. `entry.position > pageAlignedPos` (we have prefected entries, but they are "later" in the file). This
        //    means the code has jumped backward since the last `rebuffer`. We don't want our prefetching queue to grow
        //    unbounded if we get a series of backward seek, so we need to release those prefecth, at least if they
        //    don't fall within the "current" prefetching window. But for now, we simply release all prefetched entries
        //    because:
        //    - if this is used for a genuinely sequential process, like compaction/scrub, then we shouldn't seek
        //      backward at all, and if we do it's exceptional, to handle some problem/retry, and being optimal in those
        //      rare case is not a priority.
        //    - if this is used for actual user reads, then this rebuffer should sit on top of the chunk cache anyway,
        //      and so those prefetched entries will still be in the cache even if we remove them from our own queue, and
        //      we're not really losing anything (arguably the queue of this rebuffer is a tad superfluous when we sit
        //      on top of the chunk cache for that reason, but that queue exists mainly for when that's not the case).
        // In both cases, we ensure the prefecth queue is empty, then prefetch from the current position, and wait on
        // the first entry.
        while (entry != null)
        {
            isNonSequential = true;
            unusedRebufferers.add(entry.release());
            entry = queue.poll();
        }
        prefetch(pageAlignedPos);

        if (isNonSequential)
            metrics.nonSequentialRequest.mark();

        entry = queue.poll();
        assert entry != null; // the call to `prefetch` should ensure this.

        // We call prefetch for the next entry again because we just pulled an entry so we may have to restore our
        // correct number of prefetched. But depending on the window, this will probably not do anything.
        // We do this before waiting on our current entry to trigger prefetch as soon as possible.
        prefetch(pageAlignedPos + chunkSize());

        // Note that we don't increment the `notReady` metric here, because we just triggered the read, so we know
        // it's not going to be ready (unless we're extremely lucky) and if this is due to prior seek, we're already
        // recorded it (and if it's just the first call to `rebuffer`, it's normal, we don't need to track it).
        BufferHolder holder = entry.get();
        lastReturnedRebufferer = entry.forReuse();

        return holder;
    }

     /// Trigger prefetches of the next N buffers unless they are already in the queue.
     ///
     /// This method does not block, it just submits prefetch to the executor and then return immediately.
     ///
     /// @param pageAlignedPosition the position in the file, already aligned to a page
    private void prefetch(long pageAlignedPosition)
    {
        // see caller, prefetch is only called with an empty queue or if the requested position is exactly at the
        // beginning of the queue after purging all older entries
        assert queue.isEmpty() || pageAlignedPosition == queue.peekFirst().position :
            String.format("Unexpected prefetching position %d, first: %s, last: %s", pageAlignedPosition, queue.peekFirst(), queue.peekLast());

        // Only trigger prefetch if we have less than our window already prefetech.
        if (queue.size() > windowSize)
            return;

        long firstPositionToPrefetch = queue.isEmpty() ? pageAlignedPosition : queue.peekLast().position + chunkSize();
        int toPrefetch = prefetchSize - queue.size();

        // We trigger all the prefetch on the executor, and so in parallel (within the constraint of the executor).
        for (int i = 0; i < toPrefetch; i++)
        {
            long prefetchPosition = firstPositionToPrefetch + ((long)i * chunkSize());
            if (prefetchPosition >= source.fileLength())
                break;

            ReusedRebufferer rebufferer = unusedRebufferers.poll();
            // The +1 is because we also have `lastReturnedRebufferer` on top of what is prefetched
            if (rebufferer == null && createdRebuffers < prefetchSize + 1)
            {
                rebufferer = ReusedRebufferer.create(source);
                ++createdRebuffers;
            }
            // We must have gotten a rebufferer: we know the queue is smaller than `prefetchSize`, so either we had
            // created less than `prefetchSize` rebuffererer yet, and we just created one above, or some rebufferer must
            // have been unused.
            assert rebufferer != null;
            PrefetchedEntry newEntry = new PrefetchedEntry(prefetchPosition, rebufferer);
            queue.addLast(newEntry);

            CompletableFuture<?> prevUseFuture = rebufferer.prevUseFuture;
            // If `prevUseFuture` is not null, this effectively means that the rebufferer previous usage was for a
            // prefetched entry we didn't wait on before release (we discarded the entry). But even though we didn't
            // need the prefetched value, the prefetching was triggered, and we need to make certain it is done before
            // we reuse the underlying buffer.
            executor.execute(() -> {
                if (prevUseFuture != null)
                {
                    try
                    {
                        prevUseFuture.join();
                    }
                    catch (Exception e)
                    {
                        // We ignore expections here because it has already been handling (_including_ being logged
                        // within `PrefetchedEntry#release`.
                    }
                }
                newEntry.triggerPrefetch();
            });
        }
    }

    @Override
    public ChannelProxy channel()
    {
        return source.channel();
    }

    @Override
    public long fileLength()
    {
        return source.fileLength();
    }

    @Override
    public double getCrcCheckChance()
    {
        return source.getCrcCheckChance();
    }

    @Override
    public long adjustPosition(long position)
    {
        return position;
    }

    @Override
    public void close()
    {
        assert unusedRebufferers.isEmpty() : "buffers should have been released";
        assert queue.isEmpty() : "Prefetched buffers should have been released";
        source.close();
    }

    @Override
    public void closeReader()
    {
        // First, release any inflight prefetch (moving the rebuffer to `unusedRebufferers` temporarily)
        queue.forEach(entry -> unusedRebufferers.add(entry.release()));
        queue.clear();

        unusedRebufferers.forEach(r -> {
            if (r.prevUseFuture == null)
                r.rebufferer.closeReader();
            else
                r.prevUseFuture.whenComplete((_1, _2) -> r.rebufferer.closeReader());
        });
        unusedRebufferers.clear();
    }

    @Override
    public String toString()
    {
        return String.format("Prefetching rebufferer: (%d/%d) buffers, %d buffer size", prefetchSize, windowSize, chunkSize());
    }

    /// Represents an ongoing or completed prefetching of one "chunk" (buffed)
    private static final class PrefetchedEntry
    {
        private final long position;
        private final ReusedRebufferer rebufferer;
        private final CompletableFuture<BufferHolder> future;
        private boolean reused;

        PrefetchedEntry(long position, ReusedRebufferer rebufferer)
        {
            this.position = position;
            this.rebufferer = rebufferer;
            this.future = new CompletableFuture<>();

            metrics.prefetched.mark();
        }

        /// Called on an [PrefetchingRebufferer#executor] to do the actual (blocking) prefetching.
        /// This is the only method of this class that is not executed on the thread of the [PrefetchingRebufferer]
        /// that creates it (meaning, the thread on which the [PrefetchingRebufferer#rebuffer(long)] method is called).
        void triggerPrefetch()
        {
            try
            {
                future.complete(rebufferer.rebuffer(position));
            }
            catch (Exception e)
            {
                future.completeExceptionally(e);
            }
        }

        /// Must only be called after a successfull [#get] (when the prefetched entry has been consumed and the
        /// buffer returned); returns a [ReusedRebufferer] ready for reuse.
        ReusedRebufferer forReuse()
        {
            assert !reused;
            assert isReady() : "Should not have been called on incomplete prefetch";
            reused = true;
            return rebufferer.prepareForReuse(null);
        }

        /// Called when the entry is discarded, to release the underlying buffer once the prefetch complete (since we
        /// won't use the result of that prefetch, we need to release the buffer ourselves).
        ReusedRebufferer release()
        {
            // We should call one of `forReuse` or this for each entry, but only one, or we risk putting the same
            // rebufferer in `unusedRebufferers` twice, which would be a problem
            assert !reused;
            reused = true;

            return rebufferer.prepareForReuse(future.whenComplete((buffer, error) -> {
                try
                {
                    if (buffer != null)
                    {
                        buffer.release();
                        metrics.unused.mark();
                    }

                    // We shouldn't fail, but we're also explicitly not using the result of that prefetch, so no reason
                    // to do more than warn.
                    if (error != null)
                        LOGGER.warn("Error during prefetching; but this prefetch is discarded", error);
                }
                catch (Throwable t)
                {
                    // Erroring during release "could" be more problematic, so log a genuine error
                    LOGGER.error("Failed to release (discarded) prefetched buffer", t);
                }
            }));
        }

        boolean isReady()
        {
            return future.isDone();
        }

        BufferHolder get()
        {
            try
            {
                return future.join();
            }
            catch (Throwable t)
            {
                // Remove `CompletionException` and any other wrapping as code upstream may not expect them.
                throw Throwables.cleaned(t);
            }
        }

        @Override
        public String toString()
        {
            return String.format("Position: %d, Done: %s", position, future.isDone());
        }
    }

    private static class ReusedRebufferer
    {
        private final @Nullable CompletableFuture<?> prevUseFuture;
        private final Rebufferer rebufferer;

        private ReusedRebufferer(@Nullable CompletableFuture<?> prevUseFuture, Rebufferer rebufferer)
        {
            this.prevUseFuture = prevUseFuture;
            this.rebufferer = rebufferer;
        }

        static ReusedRebufferer create(RebuffererFactory factory)
        {
            return new ReusedRebufferer(null, factory.instantiateRebufferer(false));
        }

        BufferHolder rebuffer(long position)
        {
            return rebufferer.rebuffer(position);
        }

        ReusedRebufferer prepareForReuse(@Nullable CompletableFuture<?> lastUseFuture)
        {
            return new ReusedRebufferer(lastUseFuture, rebufferer);
        }
    }

    @VisibleForTesting
    public static class PrefetchingMetrics
    {
        /// Total number of buffers that were prefetched.
        final Meter prefetched;

        /// Total number of buffers that were prefetched but were not used (either due to a non-sequential access, or
        /// to the rebufferer being closed before the end of the underlying file).
        final Meter unused;

        /// Total number of buffers that were prefetched but were not ready when they were requested and the caller
        /// had to wait. A large number of those means that the configuration of pre-fetching should be adjusted.
        final Meter notReady;

        /// Number of times a [PrefetchingRebufferer#rebuffer] call was not respecting sequential acess (meaning the
        /// position requested was not exactly for the chunk following the previous call)
        final Meter nonSequentialRequest;

        PrefetchingMetrics()
        {
            MetricNameFactory factory = new DefaultNameFactory("ReadPrefetching", "");
            prefetched = Metrics.meter(factory.createMetricName("Prefetched"));
            unused = Metrics.meter(factory.createMetricName("Unused"));
            notReady = Metrics.meter(factory.createMetricName("NotReady"));
            nonSequentialRequest = Metrics.meter(factory.createMetricName("NonSequentialRequest"));
        }

        @VisibleForTesting
        void reset()
        {
            prefetched.mark(-prefetched.getCount());
            unused.mark(-unused.getCount());
            notReady.mark(-notReady.getCount());
            nonSequentialRequest.mark(-nonSequentialRequest.getCount());
        }

        @Override
        public String toString()
        {
            if (prefetched.getCount() == 0)
                return "No prefetching yet";

            return String.format("Prefetched: [%s], Unused: [%s] (%.2f), Not ready: [%s] (%.2f), Non sequential request: [%s]",
                                 prefetched.getCount(),
                                 unused.getCount(), (double) unused.getCount() / prefetched.getCount(),
                                 notReady.getCount(), (double) notReady.getCount() / prefetched.getCount(),
                                 nonSequentialRequest.getCount());
        }
    }
}