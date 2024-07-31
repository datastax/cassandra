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

package org.apache.cassandra.index.sai.disk.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.function.Supplier;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.zip.CRC32;
import java.util.zip.CRC32C;
import java.util.zip.Checksum;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.jbellis.jvector.disk.BufferedRandomAccessWriter;
import net.nicoulaj.compilecommand.annotations.DontInline;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.io.util.SequentialWriterOption;
import org.apache.lucene.store.ChecksumIndexInput;

public class IndexFileUtils
{
    protected static final Logger logger = LoggerFactory.getLogger(IndexFileUtils.class);

    @VisibleForTesting
    public static final SequentialWriterOption DEFAULT_WRITER_OPTION = SequentialWriterOption.newBuilder()
                                                                                             .trickleFsync(DatabaseDescriptor.getTrickleFsync())
                                                                                             .trickleFsyncByteInterval(DatabaseDescriptor.getTrickleFsyncIntervalInKiB() * 1024)
                                                                                             .bufferType(BufferType.OFF_HEAP)
                                                                                             .finishOnClose(true)
                                                                                             .build();

    private static final IndexFileUtils instance = new IndexFileUtils(DEFAULT_WRITER_OPTION);
    private static final Supplier<Checksum> CHECKSUM_FACTORY = CRC32C::new;
    private static final Supplier<Checksum> LEGACY_CHECKSUM_FACTORY = CRC32::new;
    private static IndexFileUtils overrideInstance = null;

    private final SequentialWriterOption writerOption;

    public static synchronized void setOverrideInstance(IndexFileUtils overrideInstance)
    {
        IndexFileUtils.overrideInstance = overrideInstance;
    }

    public static IndexFileUtils instance()
    {
        if (overrideInstance == null)
            return instance;
        else
            return overrideInstance;
    }

    @VisibleForTesting
    protected IndexFileUtils(SequentialWriterOption writerOption)
    {
        this.writerOption = writerOption;
    }

    public IndexOutputWriter openOutput(File file, ByteOrder order, boolean append, Version version) throws IOException
    {
        assert writerOption.finishOnClose() : "IndexOutputWriter relies on close() to sync with disk.";

        var checksumWriter = new IncrementalChecksumSequentialWriter(file, writerOption, version);
        IndexOutputWriter indexOutputWriter = new IndexOutputWriter(checksumWriter, order);
        if (append)
        {
            // Got to recalculate checksum for the file opened for append, otherwise final checksum will be wrong.
            // Checksum verification is not happening here as it sis not guranteed that the file has the checksum/footer.
            checksumWriter.recalculateChecksum();
            indexOutputWriter.skipBytes(file.length());
        }

        return indexOutputWriter;
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

    public static ChecksumIndexInput getBufferedChecksumIndexInput(org.apache.lucene.store.IndexInput indexInput, Version version)
    {
        return new BufferedChecksumIndexInput(indexInput, getChecksumFactory(version).get());
    }

    public static Supplier<Checksum> getChecksumFactory(Version version)
    {
        // TODO Use the version to determine which checksum algorithm to use
        return LEGACY_CHECKSUM_FACTORY;
    }

    public interface ChecksumWriter
    {
        long getChecksum();
    }

    /**
     * The SequentialWriter that calculates checksum of the data written to the file. This writer extends
     * {@link SequentialWriter} to add the checksumming functionality and typically is used together
     * with {@link IndexOutputWriter}. This, in turn, is used in conjunction with {@link BufferedChecksumIndexInput}
     * to verify the checksum of the data read from the file, so they must share the same checksum algorithm.
     */
    static class IncrementalChecksumSequentialWriter extends SequentialWriter implements ChecksumWriter
    {
        private final Checksum checksum;

        IncrementalChecksumSequentialWriter(File file, SequentialWriterOption writerOption, Version version)
        {
            super(file, writerOption);
            this.checksum = getChecksumFactory(version).get();
        }

        /**
         * Recalculates checksum for the file.
         *
         * Usefil when the file is opened for append and checksum will need to account for the existing data.
         * e.g. if the file opened for append is a new file, then checksum start at 0 and goes from there with the writes.
         * If the file opened for append is an existing file, without recalculating the checksum will start at 0
         * and only account for appended data. Checksum validation will compare it to the checksum of the whole file and fail.
         * Hence, for the existing files this method should be called to recalculate the checksum.
         *
         * @throws IOException if file read failed.
         */
        public void recalculateChecksum() throws IOException
        {
            checksum.reset();
            if (!file.exists())
                return;

            try(FileChannel ch = FileChannel.open(file.toPath(), StandardOpenOption.READ))
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
            return checksum.getValue();
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

        @Override
        public void writeMostSignificantBytes(long register, int bytes) throws IOException
        {
            super.writeMostSignificantBytes(register, bytes);
            addMsbToChecksum(register, bytes);
        }

        /**
         * Based on {@link DataOutputPlus#writeMostSignificantBytes(long, int)}
         */
        private void addMsbToChecksum(long register, int bytes)
        {
            long msbValue = 0;
            switch (bytes)
            {
                case 0:
                    break;
                case 1:
                    msbValue = (int) (register >>> 56);
                    break;
                case 2:
                    msbValue = (int) (register >> 48);
                    break;
                case 3:
                    msbValue = (int) (register >> 40);
                    break;
                case 4:
                    msbValue = (int) (register >> 32);
                    break;
                case 5:
                    msbValue = (int) (register >> 24);
                    break;
                case 6:
                    msbValue = (int) (register >> 16);
                    break;
                case 7:
                    msbValue = (int) (register >> 8);
                    break;
                case 8:
                    msbValue = register;
                    break;
                default:
                    throw new IllegalArgumentException();
            }
            addTochecksum(msbValue, bytes);
        }

        private void addTochecksum(long bytes, int count)
        {
            int origCount = count;
            if (ByteOrder.BIG_ENDIAN == buffer.order())
                while (count > 0) checksum.update((int) (bytes >>> (8 * --count)));
            else
                while (count > 0) checksum.update((int) (bytes >>> (8 * (origCount - count--))));
        }
    }
}
