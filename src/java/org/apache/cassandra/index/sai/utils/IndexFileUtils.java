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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.CRC32;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.jbellis.jvector.disk.BufferedRandomAccessWriter;
import net.nicoulaj.compilecommand.annotations.DontInline;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.index.sai.disk.io.IndexInput;
import org.apache.cassandra.index.sai.disk.io.IndexInputReader;
import org.apache.cassandra.index.sai.disk.io.IndexOutputWriter;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.storage.StorageProvider;
import org.apache.cassandra.io.util.DataPosition;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.io.util.SequentialWriterOption;
import org.apache.lucene.codecs.CodecUtil;

public class IndexFileUtils
{
    protected static final Logger logger = LoggerFactory.getLogger(IndexFileUtils.class);

    @VisibleForTesting
    protected static final SequentialWriterOption defaultWriterOption = SequentialWriterOption.newBuilder()
                                                                                              .trickleFsync(DatabaseDescriptor.getTrickleFsync())
                                                                                              .trickleFsyncByteInterval(DatabaseDescriptor.getTrickleFsyncIntervalInKb() * 1024)
                                                                                              .bufferType(BufferType.OFF_HEAP)
                                                                                              .finishOnClose(true)
                                                                                              .build();

    public static final IndexFileUtils instance = new IndexFileUtils();

    private static final SequentialWriterOption writerOption = defaultWriterOption;

    /**
     * Remembers checksums of files so we don't have to recompute them from the beginning of the file whenever appending
     * to a file. Keeps checksums with respective file lengths and footer checksums so we can detect file changes
     * that don't go through this code, and we can evict stale entries.
     */
    private static final Cache<String, Guard<FileChecksum>> checksumCache = Caffeine.newBuilder()
                                                                                    .maximumSize(4096)
                                                                                    .build();

    @VisibleForTesting
    protected IndexFileUtils()
    {}

    public IndexOutputWriter openOutput(File file, ByteOrder order, boolean append) throws IOException
    {
        assert writerOption.finishOnClose() : "IndexOutputWriter relies on close() to sync with disk.";
        var checksumWriter = new IncrementalChecksumSequentialWriter(file, append);
        return new IndexOutputWriter(checksumWriter, order);
    }

    public BufferedRandomAccessWriter openRandomAccessOutput(File file, boolean append) throws IOException
    {
        assert writerOption.finishOnClose() : "IndexOutputWriter relies on close() to sync with disk.";

        var out = new BufferedRandomAccessWriter(file.toPath());
        if (append)
            out.seek(file.length());

        return out;
    }

    public IndexInput openInput(FileHandle handle)
    {
        return IndexInputReader.create(handle);
    }

    public IndexInput openBlockingInput(FileHandle fileHandle)
    {
        final RandomAccessReader randomReader = fileHandle.createReader();
        return IndexInputReader.create(randomReader, fileHandle::close);
    }

    public interface ChecksumWriter
    {
        long getChecksum();
    }

    static class IncrementalChecksumSequentialWriter extends SequentialWriter implements ChecksumWriter
    {
        /** Protects the checksum so only one Writer can update it */
        private Guard<FileChecksum> checksumGuard;
        /** Current (running) checksum from the beginning of the file till the current position */
        private FileChecksum checksum;
        /** Remembers the checksum after closing this writer */
        private long finalChecksum;

        IncrementalChecksumSequentialWriter(File file, boolean append) throws IOException
        {
            super(file, writerOption);

            while (checksum == null)
            {
                checksumGuard = checksumCache.get(file.path(), s -> new Guard<>(new FileChecksum()));
                checksum = checksumGuard.tryLock();

                if (checksum == null)
                {
                    // If we're here this means some other Writer did not unlock the checksum object,
                    // so we can't use the same checksum safely, as there is a slight probability it
                    // is in active use. This is not necessarily a bug - e.g. it is also possible
                    // the other writer was interrupted and the client code simply forgot to close() it.
                    // Therefore, we'll get a new one, just to be safe.
                    logger.warn("File {} still in use by another instance of {}", file, this.getClass().getSimpleName());
                    checksumCache.invalidate(file.path());
                }
            }

            if (append)
            {
                var fileLength = file.length();
                skipBytes(fileLength);

                // It is possible we didn't get a good checksum.
                // We could have gotten a zero checksum because the cache has a limited size,
                // or the file could have been changed in the meantime by another process, in which case the
                // footer checksum would not match.
                // However, we don't recalculate the checksum always, because it is very costly:
                var footerChecksum = calculateFooterChecksum();
                if (checksum.fileLength != fileLength || checksum.footerChecksum != footerChecksum)
                {
                    logger.warn("Length and checksum ({}, {}) of file {} does not match the length and checksum ({}, {}) in the checksum cache. " +
                                "Recomputing the checksum from the beginning.",
                                fileLength, footerChecksum, file, checksum.fileLength, checksum.footerChecksum);
                    recalculateFileChecksum();
                }
            }
            else
            {
                // We might be overwriting an existing file
                checksum.reset();
            }
        }

        /**
         * Recalculates checksum for the file.
         * <p>
         * Useful when the file is opened for append and checksum will need to account for the existing data.
         * e.g. if the file opened for append is a new file, then checksum start at 0 and goes from there with the writes.
         * If the file opened for append is an existing file, without recalculating the checksum will start at 0
         * and only account for appended data. Checksum validation will compare it to the checksum of the whole file and fail.
         * Hence, for the existing files this method should be called to recalculate the checksum.
         *
         * @throws IOException if file read failed.
         */
        public void recalculateFileChecksum() throws IOException
        {
            checksum.reset();
            if (!file.exists())
                return;

            try(FileChannel ch = StorageProvider.instance.writeTimeReadFileChannelFor(file))
            {
                if (ch.size() == 0)
                    return;

                final ByteBuffer buf = ByteBuffer.allocateDirect(65536);
                int b = ch.read(buf);
                while (b > 0)
                {
                    buf.flip();
                    checksum.update(buf);
                    buf.clear();
                    b = ch.read(buf);
                }
            }

            assert checksum.fileLength == position();
        }

        /**
         * Returns the checksum of the footer of the index file.
         * Those bytes contain the checksum for the whole file, so
         * this checksum can be used to verify the integrity of the whole file.
         * Note that this is not the same as checksum written in the file footer;
         * this is done this way so we don't have to decode the footer here.
         */
        public long calculateFooterChecksum() throws IOException
        {
            CRC32 footerChecksum = new CRC32();
            try (FileChannel ch = StorageProvider.instance.writeTimeReadFileChannelFor(file))
            {
                ch.position(Math.max(0, file.length() - CodecUtil.footerLength()));
                final ByteBuffer buf = ByteBuffer.allocate(CodecUtil.footerLength());
                int b = ch.read(buf);
                while (b > 0)
                {
                    buf.flip();
                    footerChecksum.update(buf);
                    buf.clear();
                    b = ch.read(buf);
                }
            }
            return footerChecksum.getValue();
        }

        @Override
        public void write(ByteBuffer src) throws IOException
        {
            ByteBuffer shallowCopy = src.slice().order(src.order());
            super.write(src);
            checksum.update(shallowCopy);
        }

        @Override
        public void writeBoolean(boolean v) throws IOException
        {
            super.writeBoolean(v);
            checksum.update(v ? 1 : 0);
        }

        @Override
        public void writeByte(int b) throws IOException
        {
            super.writeByte(b);
            checksum.update(b);
        }

        // Do not override write(byte[] b) to avoid double-counting bytes in the checksum.
        // It just calls this method anyway.
        @Override
        public void write(byte[] b, int off, int len) throws IOException
        {
            super.write(b, off, len);
            checksum.update(b, off, len);
        }

        @Override
        public void writeChar(int v) throws IOException
        {
            super.writeChar(v);
            addTochecksum(v, 2);
        }

        @Override
        public void writeInt(int v) throws IOException
        {
            super.writeInt(v);
            addTochecksum(v, 4);
        }

        @Override
        public void writeLong(long v) throws IOException
        {
            super.writeLong(v);
            addTochecksum(v, 8);
        }

        public long getChecksum()
        {
            return checksum != null ? checksum.getValue() : finalChecksum;
        }

        // To avoid double-counting bytes in the checksum.
        // Same as super's but calls super.writeByte
        @DontInline
        @Override
        protected void writeSlow(long bytes, int count) throws IOException
        {
            int origCount = count;
            if (ByteOrder.BIG_ENDIAN == buffer.order())
                while (count > 0) super.writeByte((int) (bytes >>> (8 * --count)));
            else
                while (count > 0) super.writeByte((int) (bytes >>> (8 * (origCount - count--))));
        }

        private void addTochecksum(long bytes, int count)
        {
            int origCount = count;
            if (ByteOrder.BIG_ENDIAN == buffer.order())
                while (count > 0) checksum.update((int) (bytes >>> (8 * --count)));
            else
                while (count > 0) checksum.update((int) (bytes >>> (8 * (origCount - count--))));
        }

        @Override
        public void truncate(long toSize)
        {
            if (toSize == 0)
            {
                checksum.reset();
                super.truncate(toSize);
            }

            // this would invalidate the checksum
            throw new UnsupportedOperationException("truncate to non zero length not supported");
        }

        @Override
        public void close()
        {
            try
            {
                super.close();
            }
            finally
            {
                try
                {
                    // Copy the checksum value to a field in order to make the checksum value available past close().
                    // Release the FileChecksum object so it can be used by another writer.
                    finalChecksum = checksum.getValue();
                    checksum.footerChecksum = calculateFooterChecksum();
                }
                catch (IOException e)
                {
                    // mark the checksum as unusable
                    // even though it stays in the cache, it won't ever match on the file length
                    checksum.fileLength = -1;
                }
                finally
                {
                    checksumGuard.release();
                    checksum = null;
                }
            }
        }
    }

    /**
     * A lightweight helper to guard against concurrent access to an object.
     * Used when we know object should be owned by one owner at a time.
     */
    static class Guard<T>
    {
        final T inner;
        final AtomicBoolean locked = new AtomicBoolean(false);

        public Guard(T inner)
        {
            this.inner = inner;
        }

        /**
         * Locks the object and returns it.
         * If it was already locked, return null.
         * @return protected object
         */
        public T tryLock()
        {
            return locked.compareAndSet(false, true)
                   ? inner
                   : null;
        }

        public void release()
        {
            locked.set(false);
        }
    }

    /**
     * Computes the checksum from the begining of a file.
     * Keeps track of the number of bytes processed.
     * We need the number of bytes so we can invalidate the checksum if the file was appended or truncated.
     */
    static class FileChecksum
    {
        long fileLength = 0;
        long footerChecksum = 0;
        final CRC32 fileChecksum = new CRC32();

        public void reset()
        {
            fileLength = 0;
            fileChecksum.reset();
        }

        public void update(int b)
        {
            fileLength += 1;
            fileChecksum.update(b);
        }

        public void update(byte[] b, int off, int len)
        {
            fileLength += len;
            fileChecksum.update(b, off, len);
        }

        public void update(ByteBuffer b)
        {
            fileLength += b.remaining();
            fileChecksum.update(b);
        }

        public long getValue()
        {
            return fileChecksum.getValue();
        }
    }

}
