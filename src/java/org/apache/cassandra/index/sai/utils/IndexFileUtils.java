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

import java.io.File;
import java.io.IOException;
import java.util.zip.CRC32;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.index.sai.disk.io.IndexInputReader;
import org.apache.cassandra.index.sai.disk.io.IndexOutputWriter;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.io.util.SequentialWriterOption;
import org.apache.lucene.store.IndexInput;

public class IndexFileUtils
{
    @VisibleForTesting
    protected static final SequentialWriterOption defaultWriterOption = SequentialWriterOption.newBuilder()
                                                                                              .trickleFsync(DatabaseDescriptor.getTrickleFsync())
                                                                                              .trickleFsyncByteInterval(DatabaseDescriptor.getTrickleFsyncIntervalInKb() * 1024)
                                                                                              .bufferType(BufferType.OFF_HEAP)
                                                                                              .finishOnClose(true)
                                                                                              .build();

    public static final IndexFileUtils instance = new IndexFileUtils();

    private static final SequentialWriterOption writerOption = defaultWriterOption;

    @VisibleForTesting
    protected IndexFileUtils()
    {}

    public IndexOutputWriter openOutput(File file)
    {
        assert writerOption.finishOnClose() : "IndexOutputWriter relies on close() to sync with disk.";

        return new IndexOutputWriter(new IncrementalChecksumSequentialWriter(file));
    }

    public IndexInput openInput(FileHandle handle)
    {
        return IndexInputReader.create(handle);
    }

    public IndexInput openBlockingInput(File file)
    {
        try (final FileHandle.Builder builder = new FileHandle.Builder(file.getAbsolutePath()))
        {
            final FileHandle fileHandle = builder.complete();
            final RandomAccessReader randomReader = fileHandle.createReader();

            return IndexInputReader.create(randomReader, fileHandle::close);
        }
    }

    public interface ChecksumWriter
    {
        long getChecksum();
    }

    class IncrementalChecksumSequentialWriter extends SequentialWriter implements ChecksumWriter
    {
        private final CRC32 checksum = new CRC32();

        IncrementalChecksumSequentialWriter(File file)
        {
            super(file, writerOption);
        }

        @Override
        public void writeByte(int b) throws IOException
        {
            super.writeByte(b);
            checksum.update(b);
        }

        @Override
        public void write(byte[] b) throws IOException
        {
            super.write(b);
            checksum.update(b);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException
        {
            super.write(b, off, len);
            checksum.update(b, off, len);
        }

        public long getChecksum()
        {
            return checksum.getValue();
        }
    }
}
