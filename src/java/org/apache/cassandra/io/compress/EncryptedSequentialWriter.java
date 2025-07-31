/*
 * Copyright DataStax, Inc.
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
package org.apache.cassandra.io.compress;

import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.util.zip.CRC32;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.util.ChecksumWriter;
import org.apache.cassandra.io.util.DataPosition;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.io.util.SequentialWriterOption;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.io.util.PageAware;

/**
 * Encryption-only writer. This is not a normal file writer in the sense that it is not meant to just accept a sequence
 * of writes. Instead, data is written to file in portions (chunks) of a given size (typically a disk page) but to write
 * a chunk the writer must be given a smaller slice of data (up to maxBytesInPage()) and then told to pad
 * (padToPageBoundary()) to the next chunk boundary. This triggers encryption and writing of the current chunk, and the
 * file position moves to the start of the next chunk.
 *
 * This essentially reserves bytes at the end of the chunk for storing a CRC code plus any information necessary for
 * decryption. The advantage of the scheme (over the compressed writer) is the fact that decrypted and encrypted file
 * positions do not differ for any written data, and thus we can avoid having to consult a compressed offsets map
 * to find where the encrypted data resides.
 *
 * Note that if asked to write a sequence of bytes that goes beyond the chunk boundary, the writer will accept it, and
 * the reader will jump over the metadata when consuming the same bytes. This functionality is to be used sparingly as
 * it may cause some surprises (e.g. the difference between two positions is not equal to the size of the data); it is
 * currently used to write keys whose length can go over the page size.
 *
 * This writer does not provide precise data length; instead when constructed from a file on disk it will return the
 * position after the last useable write. If any user depends on reading data located at the end of the file, they
 * should make sure that data is positioned at the end of a chunk. See establishEndAddressablePosition below.
 */
public class EncryptedSequentialWriter extends SequentialWriter
{
    private static final Logger logger = LoggerFactory.getLogger(EncryptedSequentialWriter.class);

    public static final int FOOTER_LENGTH = 8; // CRC and encrypted length
    public static final int CHUNK_SIZE = PageAware.PAGE_SIZE;
    // Note: We could just as well permit other chunk sizes for encryption, but as is stands we can't specify them
    // without changing CompressionParams (either its serialization or map entries), which is validated on streaming
    // and could cause incompatibility.
    // Having the size fixed to 4k also prevents fragmentation in the chunk cache, but may be wasteful if the
    // encryptor needs to store a lot of information each frame.


    private final ChecksumWriter crcMetadata;

    private final ICompressor encryptor;

    // used to store encrypted data
    private final ByteBuffer encrypted;

    private final int maxBytesInChunk;

    /** Position of the last synced content. Used to avoid having to find the unencrypted end position of the file. */
    private long lastContent = 0;

    /**
     * @param file File to write
     * @param option Write option (buffer size and type will be set the same as compression params)
     * @param encryptor Encryptor to use as an ICompressor
     */
    public EncryptedSequentialWriter(File file,
                                     SequentialWriterOption option,
                                     ICompressor encryptor)
    {
        super(file, true, SequentialWriterOption.newBuilder()
                            .bufferSize(maxBytesInPage(encryptor))
                            .bufferType(BufferType.preferredForCompression())
                            .finishOnClose(option.finishOnClose())
                            .build(), true);
        assert Integer.bitCount(CHUNK_SIZE) == 1
                : "Chunk size of EncryptedSequentialWriter must be a power of two, was " + CHUNK_SIZE;

        this.encryptor = encryptor;
        this.encrypted = BufferType.preferredForCompression().allocate(CHUNK_SIZE);

        maxBytesInChunk = buffer.capacity();
        crcMetadata = new ChecksumWriter(new DataOutputStream(Channels.newOutputStream(channel)));
    }

    public static int maxBytesInPage(ICompressor encryptor)
    {
        return encryptor.findMaxBytesInChunk(CHUNK_SIZE - FOOTER_LENGTH);
    }

    @Override
    public long getOnDiskFilePointer()
    {
        return lastFlushOffset;
    }

    @Override
    public void flush()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void flushData()
    {
        try
        {
            // compressing data with buffer re-use
            buffer.flip();
            encrypted.clear();
            encryptor.compress(buffer, encrypted);
        }
        catch (IOException e)
        {
            throw new RuntimeException("Compression exception", e); // shouldn't happen
        }

        try
        {
            int compressedLength = encrypted.position();

            assert encrypted.remaining() >= FOOTER_LENGTH;
            // pad _after_ encryption because known 0s in plaintext can make encryption easier to break
            ByteBufferUtil.writeZeroes(encrypted, encrypted.remaining() - FOOTER_LENGTH);

            encrypted.putInt(compressedLength);

            encrypted.flip();
            // add the corresponding checksum
            crcMetadata.appendToBuf(encrypted);

            // write everything out
            channel.write(encrypted);

            assert encrypted.limit() == encrypted.capacity() : "encrypted.limit()=" + encrypted.limit() + " encrypted.capacity()=" + encrypted.capacity() + " compressedLength=" + compressedLength;

            lastFlushOffset += encrypted.capacity();
            lastContent = current();
            assert fchannel.position() == lastFlushOffset : "fchannel.position=" + fchannel.position() + " lastFlushOffset=" + lastFlushOffset;

            if (runPostFlush != null)
                runPostFlush.accept(current());
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, getFile());
        }
    }

    @Override
    protected void resetBuffer()
    {
        // Our position now moves to the position at the start of next page.
        bufferOffset = lastFlushOffset;
        buffer.clear();
    }

    public void updateFileHandle(FileHandle.Builder fhBuilder, long dataLength)
    {
        // Set length to last content position to avoid having to read and decrypt the last chunk to find it.
        fhBuilder.withLengthOverride(lastContent);
    }

    @Override
    public synchronized void resetAndTruncate(DataPosition mark)
    {
        assert mark instanceof BufferedFileWriterMark;

        long previous = current();
        long truncateTarget = ((BufferedFileWriterMark) mark).pointer;

        // If we're resetting to a point within our buffered data, just adjust our buffered position to drop bytes to
        // the right of the desired mark.
        if (previous - truncateTarget <= buffer.position())
        {
            buffer.position(buffer.position() - ((int) (previous - truncateTarget)));
            return;
        }

        // synchronize current buffer with disk - we don't want any data loss
        sync();

        // find the aligned position of the truncation target; we should keep everything in the file before this point
        // and restore the buffer contents written during the next sync.
        long truncateChunk = truncateTarget & -CHUNK_SIZE;

        try
        {
            encrypted.clear();
            encrypted.limit(CHUNK_SIZE);
            fchannel.position(truncateChunk);
            fchannel.read(encrypted);

            CRC32 checksum = new CRC32();
            encrypted.flip();
            encrypted.limit(CHUNK_SIZE - 4);
            checksum.update(encrypted);
            encrypted.limit(CHUNK_SIZE);

            if (encrypted.getInt(CHUNK_SIZE - 4) != (int) checksum.getValue())
                throw new CorruptBlockException(getFile().toString(), truncateChunk, CHUNK_SIZE);

            try
            {
                // Repopulate buffer from encrypted data
                buffer.clear();
                int length = encrypted.getInt(CHUNK_SIZE - FOOTER_LENGTH);
                encrypted.position(0).limit(length);
                encryptor.uncompress(encrypted, buffer);
            }
            catch (IOException e)
            {
                throw new CorruptBlockException(getFile().toString(), truncateChunk, CHUNK_SIZE, e);
            }
        }
        catch (CorruptBlockException e)
        {
            throw new CorruptSSTableException(e, getFile());
        }
        catch (EOFException e)
        {
            throw new CorruptSSTableException(new CorruptBlockException(getFile().toString(), truncateChunk, CHUNK_SIZE), getFile());
        }
        catch (IOException e)
        {
            throw new FSReadError(e, getFile());
        }


        // truncate file to given position
        truncate(truncateChunk);

        bufferOffset = truncateChunk;
        buffer.position((int) truncateTarget & (CHUNK_SIZE - 1));
        lastContent = current();
    }

    // Page management using chunk boundaries

    @Override
    public int maxBytesInPage()
    {
        return maxBytesInChunk;
    }

    @Override
    public void padToPageBoundary()
    {
        if (buffer.position() == 0)
            return;

        doFlush(0);
    }

    @Override
    public int bytesLeftInPage()
    {
        return buffer.remaining();
    }

    @Override
    public long paddedPosition()
    {
        return bufferOffset + (buffer.position() == 0 ? 0 : CHUNK_SIZE);
    }

    public void establishEndAddressablePosition(int bytesNeeded) throws IOException
    {
        // Make sure the data does not span a page boundary (and the encryption data put there).
        if  (bytesLeftInPage() < bytesNeeded)
            padToPageBoundary();

        // Now pad to place the data at the end of the page.
        int padding = bytesLeftInPage() - bytesNeeded;
        assert padding >= 0 : "Requested " + bytesNeeded + " metadata bytes do not fit max page size " + bytesLeftInPage();

        ByteBufferUtil.writeZeroes(buffer, padding);

        assert bytesLeftInPage() == bytesNeeded;

        // The padding above should not affect space used at all, but saves us from having to decode the last page to
        // find the real end position in the file.
    }
}
