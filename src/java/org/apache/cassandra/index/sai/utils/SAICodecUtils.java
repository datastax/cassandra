/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import java.io.OutputStream;

import io.github.jbellis.jvector.disk.RandomAccessWriter;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.io.compress.CorruptBlockException;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.BufferedChecksumIndexInput;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.OutputStreamDataOutput;

import static org.apache.lucene.codecs.CodecUtil.CODEC_MAGIC;
import static org.apache.lucene.codecs.CodecUtil.FOOTER_MAGIC;
import static org.apache.lucene.codecs.CodecUtil.readBEInt;
import static org.apache.lucene.codecs.CodecUtil.readBELong;
import static org.apache.lucene.codecs.CodecUtil.writeBEInt;
import static org.apache.lucene.codecs.CodecUtil.writeBELong;

public class SAICodecUtils
{
    public static final String FOOTER_POINTER = "footerPointer";

    public static DataOutput toLuceneOutput(java.io.DataOutput out) {
        var os = new OutputStream()
        {
            @Override
            public void write(int b) throws IOException
            {
                out.write(b);
            }
        };
        return new OutputStreamDataOutput(os);
    }

    public static void writeHeader(DataOutput out) throws IOException
    {
        writeBEInt(out, CODEC_MAGIC);
        out.writeString(Version.current().toString());
    }

    public static int headerSize() {
        // Lucene's string-writing code is complex, but this is what it works out to
        // until version length exceeds 127 characters or we add non-ascii characters
        return 7;
    }

    public static void writeFooter(IndexOutput out) throws IOException
    {
        writeBEInt(out, FOOTER_MAGIC);
        writeBEInt(out, 0);
        writeCRC(out);
    }

    // Warning: this method produces an incomplete checksum when using other Lucene tooling because it computes
    // the checksum without including the FOOTER_MAGIC and 0. See https://github.com/riptano/cndb/issues/14501.
    public static void writeFooter(RandomAccessWriter braw, long checksum) throws IOException
    {
        var out = toLuceneOutput(braw);
        writeBEInt(out, FOOTER_MAGIC);
        writeBEInt(out, 0);
        writeBELong(out, checksum);
    }

    public static Version checkHeader(DataInput in) throws IOException
    {
        return checkHeader(in, Version.EARLIEST);
    }

    public static Version checkHeader(DataInput in, Version earliest) throws IOException
    {
        try
        {
            final int actualMagic = readBEInt(in);
            if (actualMagic != CODEC_MAGIC)
            {
                throw new CorruptIndexException("codec header mismatch: actual header=" + actualMagic + " vs expected header=" + CODEC_MAGIC, in);
            }
            final Version actualVersion = Version.parse(in.readString());
            if (!actualVersion.onOrAfter(earliest))
            {
                throw new IOException("Unsupported version: " + actualVersion);
            }
            return actualVersion;
        }
        catch (Throwable th)
        {
            if (th.getCause() instanceof CorruptBlockException)
            {
                throw new CorruptIndexException("corrupted", in, th.getCause());
            }
            else
            {
                throw th;
            }
        }
    }

    public static long checkFooter(ChecksumIndexInput in) throws IOException
    {
        validateFooter(in, false);
        long actualChecksum = in.getChecksum();
        long expectedChecksum = readCRC(in);
        if (expectedChecksum != actualChecksum)
        {
            throw new CorruptIndexException("checksum failed (hardware problem?) : expected=" + Long.toHexString(expectedChecksum) +
                                            " actual=" + Long.toHexString(actualChecksum), in);
        }
        return actualChecksum;
    }

    public static void validate(IndexInput input) throws IOException
    {
        validate(input, Version.EARLIEST);
    }

    public static void validate(IndexInput input, Version earliest) throws IOException
    {
        checkHeader(input, earliest);
        validateFooterAndResetPosition(input);
    }

    public static void validate(IndexInput input, long footerPointer) throws IOException
    {
        checkHeader(input);

        long current = input.getFilePointer();
        input.seek(footerPointer);
        validateFooter(input, true);

        input.seek(current);
    }

    public static void validateFooterAndResetPosition(IndexInput in) throws IOException
    {
        long position = in.getFilePointer();
        long fileLength = in.length();
        long footerLength = CodecUtil.footerLength();
        long footerPosition = fileLength - footerLength;

        if (footerPosition < 0)
        {
            throw new CorruptIndexException("invalid codec footer (file truncated?): file length=" + fileLength + ", footer length=" + footerLength, in);
        }

        in.seek(footerPosition);
        validateFooter(in, false);
        in.seek(position);
    }

    public static void validateChecksum(IndexInput input) throws IOException
    {
        long position = input.getFilePointer();
        long expected = CodecUtil.retrieveChecksum(input);

        input.seek(position);
        long actual = CodecUtil.checksumEntireFile(input);
        if (expected != actual)
            throw new CorruptIndexException("checksum failed (hardware problem?) : expected=" + Long.toHexString(expected) + " actual=" + Long.toHexString(actual), input);
    }

    /**
     * Copied from org.apache.lucene.codecs.CodecUtil.validateFooter(IndexInput)
     */
    public static void validateFooter(IndexInput in, boolean padded) throws IOException
    {
        long remaining = in.length() - in.getFilePointer();
        long expected = CodecUtil.footerLength();

        if (remaining >= 4)
        {
            final int magic = readBEInt(in);

            if (magic != FOOTER_MAGIC)
            {
                String additionalDetails = "";
                if (remaining != expected)
                    additionalDetails = " (and invalid number of bytes: remaining=" + remaining + ", expected=" + expected + ", fp=" + in.getFilePointer() + ')';
                throw new CorruptIndexException("codec footer mismatch (file truncated?): actual footer=" + magic + " vs expected footer=" + FOOTER_MAGIC + additionalDetails, in);
            }
        }

        if (!padded)
        {
            if (remaining < expected)
            {
                throw new CorruptIndexException("misplaced codec footer (file truncated?): remaining=" + remaining + ", expected=" + expected + ", fp=" + in.getFilePointer(), in);
            }
            else if (remaining > expected)
            {
                throw new CorruptIndexException("misplaced codec footer (file extended?): remaining=" + remaining + ", expected=" + expected + ", fp=" + in.getFilePointer(), in);
            }
        }


        final int algorithmID = readBEInt(in);

        if (algorithmID != 0)
        {
            throw new CorruptIndexException("codec footer mismatch: unknown algorithmID: " + algorithmID, in);
        }
    }


    // Copied from Lucene CodecUtil as they are not public

    /**
     * Reads CRC32 value as a 64-bit long from the input.
     *
     * @throws CorruptIndexException if CRC is formatted incorrectly (wrong bits set)
     * @throws IOException if an i/o error occurs
     */
    static long readCRC(IndexInput input) throws IOException
    {
        long value = readBELong(input);
        if ((value & 0xFFFFFFFF00000000L) != 0)
        {
            throw new CorruptIndexException("Illegal CRC-32 checksum: " + value, input);
        }
        return value;
    }

    /**
     * Writes CRC32 value as a 64-bit long to the output.
     *
     * @throws IllegalStateException if CRC is formatted incorrectly (wrong bits set)
     * @throws IOException if an i/o error occurs
     */
    static void writeCRC(IndexOutput output) throws IOException
    {
        long value = output.getChecksum();
        if ((value & 0xFFFFFFFF00000000L) != 0)
        {
            throw new IllegalStateException(
            "Illegal CRC-32 checksum: " + value + " (resource=" + output + ")");
        }
        writeBELong(output, value);
    }

    // Copied from Lucene PackedInts as they are not public

    public static int checkBlockSize(int blockSize, int minBlockSize, int maxBlockSize) {
        if (blockSize >= minBlockSize && blockSize <= maxBlockSize) {
            if ((blockSize & blockSize - 1) != 0) {
                throw new IllegalArgumentException("blockSize must be a power of two, got " + blockSize);
            } else {
                return Integer.numberOfTrailingZeros(blockSize);
            }
        } else {
            throw new IllegalArgumentException("blockSize must be >= " + minBlockSize + " and <= " + maxBlockSize + ", got " + blockSize);
        }
    }

    public static int numBlocks(long size, int blockSize) {
        int numBlocks = (int)(size / (long)blockSize) + (size % (long)blockSize == 0L ? 0 : 1);
        if ((long)numBlocks * (long)blockSize < size) {
            throw new IllegalArgumentException("size is too large for this block size");
        } else {
            return numBlocks;
        }
    }

    // Copied from Lucene BlockPackedReaderIterator as they are not public

    /**
     * Same as DataInput.readVLong but supports negative values
     */
    public static long readVLong(DataInput in) throws IOException
    {
        byte b = in.readByte();
        if (b >= 0) return b;
        long i = b & 0x7FL;
        b = in.readByte();
        i |= (b & 0x7FL) << 7;
        if (b >= 0) return i;
        b = in.readByte();
        i |= (b & 0x7FL) << 14;
        if (b >= 0) return i;
        b = in.readByte();
        i |= (b & 0x7FL) << 21;
        if (b >= 0) return i;
        b = in.readByte();
        i |= (b & 0x7FL) << 28;
        if (b >= 0) return i;
        b = in.readByte();
        i |= (b & 0x7FL) << 35;
        if (b >= 0) return i;
        b = in.readByte();
        i |= (b & 0x7FL) << 42;
        if (b >= 0) return i;
        b = in.readByte();
        i |= (b & 0x7FL) << 49;
        if (b >= 0) return i;
        b = in.readByte();
        i |= (b & 0xFFL) << 56;
        return i;
    }
}
