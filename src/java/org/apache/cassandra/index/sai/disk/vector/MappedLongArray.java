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

package org.apache.cassandra.index.sai.disk.vector;

import java.io.Closeable;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.cassandra.io.util.FileUtils;

/**
 * A growable off-heap array of {@code long}s backed by a memory-mapped file.
 *
 * <p>The backing file grows on demand (doubling strategy) and newly mapped regions are
 * initialized to the sentinel value {@code -1L}. {@link #put(int, long)} automatically
 * ensures capacity and is thread-safe.
 */
class MappedLongArray implements Closeable
{
    private final RandomAccessFile raf;
    private final FileChannel channel;
    private final Arena arena = Arena.ofShared();
    private final AtomicReference<MemorySegment> segment = new AtomicReference<>();
    /** Current capacity in longs. */
    private volatile int capacity;

    /**
     * Creates a new array backed by {@code file}, with the given initial capacity (in longs).
     * The file is created / truncated to exactly {@code initialCapacity * 8} bytes.
     */
    MappedLongArray(java.io.File file, int initialCapacity) throws IOException
    {
        this.raf = new RandomAccessFile(file, "rw");
        this.channel = raf.getChannel();
        remap(0, initialCapacity);
    }

    /** Sets the long at {@code index} to {@code value}, growing the backing file if necessary. */
    void put(int index, long value)
    {
        ensureCapacity(index + 1);
        segment.get().setAtIndex(ValueLayout.JAVA_LONG, index, value);
    }

    /** Returns the long at {@code index}. Index must be within current capacity. */
    long get(int index)
    {
        return segment.get().getAtIndex(ValueLayout.JAVA_LONG, index);
    }

    private void ensureCapacity(int minCapacity)
    {
        if (minCapacity <= capacity)
            return;

        synchronized (this)
        {
            if (minCapacity <= capacity)
                return;

            int newCapacity = Math.max(minCapacity, capacity * 2);
            remap(capacity, newCapacity);
        }
    }

    private void remap(int oldCapacity, int newCapacity)
    {
        try
        {
            assert oldCapacity <= newCapacity;

            long newByteSize = (long) newCapacity * Long.BYTES;
            raf.setLength(newByteSize);

            MemorySegment newSegment = channel.map(FileChannel.MapMode.READ_WRITE, 0, newByteSize, arena);
            // Initialize the newly added region to the sentinel -1L (all bits set to 1).
            newSegment.asSlice((long) oldCapacity * Long.BYTES, (long) (newCapacity - oldCapacity) * Long.BYTES)
                      .fill((byte) -1);

            this.segment.set(newSegment);
            this.capacity = newCapacity;
        }
        catch (IOException e)
        {
            throw new UncheckedIOException("Failed to grow vectorByRowId mmap file to " + newCapacity, e);
        }
    }

    @Override
    public void close() throws IOException
    {
        FileUtils.closeQuietly(raf, channel);
        arena.close();
    }
}
