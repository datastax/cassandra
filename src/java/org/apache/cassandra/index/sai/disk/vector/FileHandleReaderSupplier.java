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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.jbellis.jvector.disk.RandomAccessReader;
import io.github.jbellis.jvector.disk.ReaderSupplier;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.utils.INativeLibrary;

/**
 * A jvector {@link ReaderSupplier} backed by a Cassandra {@link FileHandle} that actually
 * implements the cache-warming hooks.
 *
 * <h2>Why this exists</h2>
 *
 * {@code ReaderSupplier} declares exactly one abstract method, {@link ReaderSupplier#get()};
 * {@code prefetch(long,long)} and {@code willNeed(long,long)} are {@code default} no-ops. That
 * makes it a functional interface, and SAI was loading source graphs with a method reference:
 *
 * <pre>{@code OnDiskGraphIndex.load(graphHandle::createReader, offset, false)}</pre>
 *
 * which satisfies {@code get()} and silently inherits both no-ops. The consequence was that
 * EVERY prefetch mechanism in jvector was inert on the graphs compaction reads:
 *
 * <ul>
 *   <li>{@code OnDiskGraphIndex.prefetchL0Records} -> {@code supplier.prefetch} -> no-op</li>
 *   <li>{@code OnDiskGraphIndex.willNeedL0Record} -> {@code supplier.willNeed} -> no-op,
 *       which covers both {@code FrontierPrefetchingView} and the cross-source seed hint</li>
 * </ul>
 *
 * Measured 2026-08-23: a source pretouch reported warming 3.9M-16M ordinals "in 0 ms" — constant
 * elapsed across a 4x range of work, i.e. no work — while a 30,985-batch merge sat at ~39 b/min
 * with the device at 4.02 KB mean request size, 99% util and 50% iowait. jvector's own benchmarks
 * never saw this because they go through {@code ReaderSupplierFactory.open()}, which returns
 * {@code MemorySegmentReader$Supplier} and does implement both methods.
 *
 * <h2>What it does</h2>
 *
 * <ul>
 *   <li>{@link #get()} delegates to the handle, unchanged.</li>
 *   <li>{@link #prefetch} streams the range through a private channel, blocking until the pages
 *       are resident. Bytes are read into a small reusable buffer and discarded — the point is
 *       the page-cache side effect, and a small buffer keeps the copy L2-resident.</li>
 *   <li>{@link #willNeed} issues POSIX_FADV_WILLNEED and returns immediately, so a single thread
 *       can put many reads in flight. This is the one that matters for beam search, where the
 *       next read is not known until the current one lands.</li>
 * </ul>
 *
 * Offsets from jvector are ABSOLUTE file offsets — {@code OnDiskGraphIndex} seeks to
 * {@code neighborsOffset + ...} directly — so they need no rebasing even though an SAI graph
 * lives at an offset inside the TERMS_DATA component.
 *
 * <h2>Ownership</h2>
 *
 * The {@link FileHandle} is owned by the caller ({@code PerIndexFiles}); {@link #close()} does
 * NOT close it. It closes only the private channel opened here. Every operation is best-effort:
 * a failure warns and returns, and can never fail a read, a compaction or a query.
 */
public class FileHandleReaderSupplier implements ReaderSupplier
{
    private static final Logger logger = LoggerFactory.getLogger(FileHandleReaderSupplier.class);

    /**
     * Streaming buffer for {@link #prefetch}. Sized to stay L2-resident rather than to amortize
     * syscalls: the bytes are discarded, cold throughput is device-bound at anything >= 32 KB,
     * and the warm case is within noise of larger buffers.
     */
    private static final int PREFETCH_BUFFER_BYTES = 64 * 1024;

    private static final ThreadLocal<ByteBuffer> PREFETCH_BUF =
        ThreadLocal.withInitial(() -> ByteBuffer.allocateDirect(PREFETCH_BUFFER_BYTES));

    private final FileHandle handle;
    private final String path;
    private final long length;

    /**
     * Channel used ONLY for warming, kept apart from the read path so hints never perturb a
     * reader's position and so the fd stays available for fadvise. Null when it could not be
     * opened, which degrades both hooks to no-ops — the pre-existing behaviour.
     */
    private final FileChannel adviceChannel;
    private final int adviceFd;

    public FileHandleReaderSupplier(FileHandle handle)
    {
        this.handle = handle;
        this.path = handle.path();
        this.length = handle.dataLength();

        FileChannel ch = null;
        int fd = -1;
        try
        {
            ch = FileChannel.open(Paths.get(path), StandardOpenOption.READ);
            fd = INativeLibrary.instance.getfd(ch);
        }
        catch (Throwable t)
        {
            logger.warn("Could not open an advice channel for {}; graph prefetch hints disabled", path, t);
            if (ch != null)
            {
                try { ch.close(); } catch (IOException ignored) { }
                ch = null;
            }
            fd = -1;
        }
        this.adviceChannel = ch;
        this.adviceFd = fd;
    }

    @Override
    public RandomAccessReader get() throws IOException
    {
        return handle.createReader();
    }

    /**
     * Synchronously stream {@code [offset, offset+length)} into the page cache. Used by
     * {@code prefetchL0Records} for ranges the caller knows a bulk phase is about to read.
     */
    @Override
    public void prefetch(long offset, long len)
    {
        if (adviceChannel == null || len <= 0)
            return;

        long end = Math.min(offset + len, length);
        long pos = Math.max(0, offset);
        if (pos >= end)
            return;

        ByteBuffer buf = PREFETCH_BUF.get();
        try
        {
            while (pos < end)
            {
                buf.clear().limit((int) Math.min(buf.capacity(), end - pos));
                int n = adviceChannel.read(buf, pos);
                if (n < 0)
                    break;
                pos += n;
            }
        }
        catch (Throwable t)
        {
            // Warming is advisory; a partial warm is still a warm.
            logger.warn("Ranged prefetch of {} [{}, {}) failed; continuing without a warm cache",
                        path, offset, end, t);
        }
    }

    /**
     * Asynchronously hint that {@code [offset, offset+len)} will be read soon. Returns without
     * blocking, so back-to-back hints put several reads in flight from one thread — which is the
     * whole point for best-first search, whose next read is data-dependent.
     */
    public void willNeed(long offset, long len)
    {
        if (adviceFd < 0 || len <= 0)
            return;

        long end = Math.min(offset + len, length);
        long start = Math.max(0, offset);
        if (start >= end)
            return;

        // posix_fadvise takes an int length; a hint is per-record and far below 2 GiB, but clamp
        // rather than overflow into a negative.
        int span = (int) Math.min(end - start, Integer.MAX_VALUE);
        INativeLibrary.instance.tryWillNeed(adviceFd, start, span, path);
    }

    /**
     * Closes ONLY the advice channel. The {@link FileHandle} belongs to the caller and outlives
     * this supplier; closing it here would invalidate readers still in use.
     */
    @Override
    public void close() throws IOException
    {
        if (adviceChannel != null)
            adviceChannel.close();
    }
}
