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
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.cassandra.io.util.FileUtils;

/**
 * Base for growable off-heap arrays of fixed-width primitives backed by a memory-mapped file.
 *
 * <p>The backing file grows on demand (doubling strategy) and every newly mapped region is initialized to a
 * default element chosen at construction, so entries that were never set read as that default. Subclasses add the
 * typed accessors, calling {@link #ensureCapacity} before writes. Growth is thread-safe; earlier mappings stay
 * valid until {@link #close()}, so concurrent readers and writers are never invalidated by a remap.
 *
 * <p>The array does not delete its backing file.
 */
abstract class MappedArray implements Closeable
{
    private final FileChannel channel;
    private final Arena arena = Arena.ofShared();
    private final AtomicReference<MemorySegment> segment = new AtomicReference<>();
    private final int elementBytes;
    /** The default element, as the low {@code elementBytes} bytes of this value. */
    private final long defaultBits;
    /** Current capacity in elements. */
    private volatile int capacity;

    /**
     * Creates a new array backed by {@code file}, with the given initial capacity (in elements, at least 1).
     * The file is created / truncated to exactly {@code initialCapacity * elementBytes} bytes.
     *
     * @param elementBytes width of an element, 4 or 8
     * @param defaultBits  the default element, as the low {@code elementBytes} bytes of this value
     */
    MappedArray(java.io.File file, int initialCapacity, int elementBytes, long defaultBits) throws IOException
    {
        assert elementBytes == Integer.BYTES || elementBytes == Long.BYTES;
        this.elementBytes = elementBytes;
        this.defaultBits = defaultBits;
        this.channel = FileChannel.open(file.toPath(),
                                        StandardOpenOption.READ,
                                        StandardOpenOption.WRITE,
                                        StandardOpenOption.CREATE,
                                        StandardOpenOption.TRUNCATE_EXISTING);
        try
        {
            remap(0, Math.max(1, initialCapacity));
        }
        catch (RuntimeException e)
        {
            FileUtils.closeQuietly(channel);
            arena.close();
            throw e;
        }
    }

    /** The current mapping. Index must be within current capacity. */
    final MemorySegment segment()
    {
        return segment.get();
    }

    /** Ensures that indexes {@code [0, minCapacity)} are addressable, growing the backing file if necessary. */
    final void ensureCapacity(int minCapacity)
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

            long newByteSize = (long) newCapacity * elementBytes;
            // Extend the file to newByteSize by writing a zero byte at the last position.
            // FileChannel.truncate() only shrinks; a positional write is the standard way to grow.
            if (channel.size() < newByteSize)
            {
                ByteBuffer one = ByteBuffer.allocate(1);
                // a positional write may transfer 0 bytes; retry until the byte is actually written
                while (one.hasRemaining())
                    channel.write(one, newByteSize - 1 + one.position());
            }

            MemorySegment newSegment = channel.map(FileChannel.MapMode.READ_WRITE, 0, newByteSize, arena);
            fillWithDefault(newSegment, oldCapacity, newCapacity);

            this.segment.set(newSegment);
            this.capacity = newCapacity;
        }
        catch (IOException e)
        {
            throw new UncheckedIOException("Failed to grow mmap file to " + newCapacity + " elements", e);
        }
    }

    /** Sets elements {@code [from, to)} of {@code mapped} to the default element. */
    private void fillWithDefault(MemorySegment mapped, int from, int to)
    {
        for (long i = from; i < to; i++)
        {
            if (elementBytes == Long.BYTES)
                mapped.setAtIndex(ValueLayout.JAVA_LONG, i, defaultBits);
            else
                mapped.setAtIndex(ValueLayout.JAVA_INT, i, (int) defaultBits);
        }
    }

    @Override
    public void close() throws IOException
    {
        FileUtils.closeQuietly(channel);
        arena.close();
    }

    /**
     * A growable off-heap array of {@code int}s backed by a memory-mapped file; unset entries read as the default
     * element given at construction. {@link #put(int, int)} automatically ensures capacity and is thread-safe.
     */
    static class OfInt extends MappedArray
    {
        OfInt(java.io.File file, int initialCapacity, int defaultValue) throws IOException
        {
            super(file, initialCapacity, Integer.BYTES, defaultValue);
        }

        /** Sets the int at {@code index} to {@code value}, growing the backing file if necessary. */
        void put(int index, int value)
        {
            ensureCapacity(index + 1);
            segment().setAtIndex(ValueLayout.JAVA_INT, index, value);
        }

        /** Returns the int at {@code index}. Index must be within current capacity. */
        int get(int index)
        {
            return segment().getAtIndex(ValueLayout.JAVA_INT, index);
        }
    }

    /**
     * A growable off-heap array of {@code long}s backed by a memory-mapped file; unset entries read as the default
     * element given at construction. {@link #put(int, long)} automatically ensures capacity and is thread-safe.
     */
    static class OfLong extends MappedArray
    {
        OfLong(java.io.File file, int initialCapacity, long defaultValue) throws IOException
        {
            super(file, initialCapacity, Long.BYTES, defaultValue);
        }

        /** Sets the long at {@code index} to {@code value}, growing the backing file if necessary. */
        void put(int index, long value)
        {
            ensureCapacity(index + 1);
            segment().setAtIndex(ValueLayout.JAVA_LONG, index, value);
        }

        /** Returns the long at {@code index}. Index must be within current capacity. */
        long get(int index)
        {
            return segment().getAtIndex(ValueLayout.JAVA_LONG, index);
        }
    }
}
