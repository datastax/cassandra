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

import java.io.BufferedOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOError;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.zip.CRC32;
import javax.annotation.Nonnull;

import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.compress.CompressedSequentialWriter;

public class ChecksumWriter
{
    private final CRC32 incrementalChecksum = new CRC32();
    private final DataOutput incrementalOut;
    private final CRC32 fullChecksum = new CRC32();

    public ChecksumWriter(DataOutput incrementalOut)
    {
        this.incrementalOut = incrementalOut;
    }

    public void writeChunkSize(int length)
    {
        try
        {
            incrementalOut.writeInt(length);
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }

    // checksumIncrementalResult indicates if the checksum we compute for this buffer should itself be
    // included in the full checksum, translating to if the partial checksum is serialized along with the
    // data it checksums (in which case the file checksum as calculated by external tools would mismatch if
    // we did not include it), or independently.

    // CompressedSequentialWriters serialize the partial checksums inline with the compressed data chunks they
    // corroborate, whereas ChecksummedSequentialWriters serialize them to a different file.
    public void appendDirect(ByteBuffer bb, boolean checksumIncrementalResult)
    {
        try
        {
            ByteBuffer toAppend = bb.duplicate();
            toAppend.mark();
            incrementalChecksum.update(toAppend);
            toAppend.reset();

            int incrementalChecksumValue = (int) incrementalChecksum.getValue();
            incrementalOut.writeInt(incrementalChecksumValue);

            fullChecksum.update(toAppend);
            if (checksumIncrementalResult)
            {
                ByteBuffer byteBuffer = ByteBuffer.allocate(4);
                byteBuffer.putInt(incrementalChecksumValue);
                assert byteBuffer.arrayOffset() == 0;
                fullChecksum.update(byteBuffer.array(), 0, byteBuffer.array().length);
            }
            incrementalChecksum.reset();

        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }

    /**
     * Checksum the given buffer and append the partial checksum after its end.
     * Full checksum is updated to reflect the checksum bytes.
     *
     * Assumes the buffer has enough capacity to fit the extra 4 bytes, and leaves the buffer ready
     * for writing (i.e. setting position to 0 and limit to the input limit + 4).
     */
    public void appendToBuf(ByteBuffer bb)
    {
        incrementalChecksum.update(bb);
        bb.limit(bb.capacity());
        int incrementalChecksumValue = (int) incrementalChecksum.getValue();
        bb.putInt(incrementalChecksumValue);
        bb.flip();
        fullChecksum.update(bb);
        incrementalChecksum.reset();
        bb.flip();
    }

    public void writeFullChecksum(@Nonnull File digestFile)
    {
        writeFullChecksum(digestFile, fullChecksum.getValue());
    }

    /**
     * Write given checksum into the digest file. This is used when {@link CompressedSequentialWriter} is reset and truncated,
     * and we need to recompute digest for the whole file.
     */
    public static void writeFullChecksum(@Nonnull File digestFile, long checksum)
    {
        FileOutputStreamPlus fos = null;
        try (DataOutputStream out = new DataOutputStream(new BufferedOutputStream(fos = digestFile.newOutputStream(File.WriteMode.OVERWRITE))))
        {
            out.write(String.valueOf(checksum).getBytes(StandardCharsets.UTF_8));
            out.flush();
            fos.sync();
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, digestFile);
        }
    }
}

