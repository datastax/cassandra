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
package org.apache.cassandra.io.sstable.format.trieindex;

import java.io.EOFException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import javax.crypto.spec.DESKeySpec;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.cache.ChunkCache;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.compress.EncryptedSequentialWriter;
import org.apache.cassandra.io.compress.EncryptionConfig;
import org.apache.cassandra.io.compress.Encryptor;
import org.apache.cassandra.io.compress.EncryptorTest;
import org.apache.cassandra.io.sstable.metadata.ZeroCopyMetadata;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.io.util.SequentialWriterOption;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.utils.PageAware;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

@RunWith(Parameterized.class)
public class PartitionIndexEncryptedTest extends PartitionIndexTest
{
    static final CompressionParams compressionParamsNormal;
    static final CompressionParams compressionParamsOutOfPlace;
    static final CompressionParams compressionParamsBlowfish;
    static final CompressionParams compressionParamsDes;

    static
    {
        Map<String, String> opts = new HashMap<>();

        opts.put(EncryptionConfig.KEY_PROVIDER, EncryptorTest.KeyProviderFactoryStub.class.getName());

        opts.put(EncryptionConfig.CIPHER_ALGORITHM, "AES/CBC/PKCS5Padding");
        opts.put(EncryptionConfig.SECRET_KEY_STRENGTH, Integer.toString(128));
        opts.put(CompressionParams.CLASS, Encryptor.class.getName());
        compressionParamsNormal = CompressionParams.fromMap(opts);

        opts.put(EncryptionConfig.CIPHER_ALGORITHM, "AES/ECB/PKCS5Padding");
        opts.put(EncryptionConfig.SECRET_KEY_STRENGTH, Integer.toString(256));
        opts.put(CompressionParams.CLASS, OutOfPlaceEncryptor.class.getName());
        compressionParamsOutOfPlace = CompressionParams.fromMap(opts);

        opts.put(EncryptionConfig.CIPHER_ALGORITHM, "Blowfish/CBC/PKCS5Padding");
        opts.put(EncryptionConfig.SECRET_KEY_STRENGTH, Integer.toString(256));
        opts.put(CompressionParams.CLASS, Encryptor.class.getName());
        compressionParamsBlowfish = CompressionParams.fromMap(opts);

        opts.put(EncryptionConfig.CIPHER_ALGORITHM, "DES/CBC/PKCS5Padding");
        opts.put(EncryptionConfig.SECRET_KEY_STRENGTH, Integer.toString(DESKeySpec.DES_KEY_LEN * 8));
        opts.put(CompressionParams.CLASS, Encryptor.class.getName());
        compressionParamsDes = CompressionParams.fromMap(opts);
    }

    public static class OutOfPlaceEncryptor extends Encryptor
    {
        public static OutOfPlaceEncryptor create(Map<String, String> options)
        {
            EncryptionConfig encryptionConfig = EncryptionConfig.forClass(OutOfPlaceEncryptor.class).fromCompressionOptions(options).build();
            return new OutOfPlaceEncryptor(encryptionConfig);
        }

        OutOfPlaceEncryptor(EncryptionConfig encryptionConfig)
        {
            super(encryptionConfig);
        }

        @Override
        public boolean canDecompressInPlace()
        {
            return false;
        }
    }

    @Parameterized.Parameters()
    public static Collection<Object[]> generateData()
    {
        return Arrays.asList(new Object[][]{
                new Object[] {Config.DiskAccessMode.standard, false, compressionParamsNormal, ByteComparable.Version.LEGACY},
                new Object[] {Config.DiskAccessMode.standard, false, compressionParamsNormal, ByteComparable.Version.OSS41},
                new Object[] {Config.DiskAccessMode.standard, false, compressionParamsNormal, ByteComparable.Version.OSS50},
                // fromFile and out-of-place have independent implementations, one run suffices to test both
                new Object[] {Config.DiskAccessMode.standard, true, compressionParamsOutOfPlace, ByteComparable.Version.LEGACY},
                new Object[] {Config.DiskAccessMode.standard, true, compressionParamsOutOfPlace, ByteComparable.Version.OSS41},
                new Object[] {Config.DiskAccessMode.standard, true, compressionParamsOutOfPlace, ByteComparable.Version.OSS50},
                new Object[] {Config.DiskAccessMode.mmap, false, compressionParamsBlowfish, ByteComparable.Version.LEGACY},
                new Object[] {Config.DiskAccessMode.mmap, false, compressionParamsBlowfish, ByteComparable.Version.OSS41},
                new Object[] {Config.DiskAccessMode.mmap, false, compressionParamsBlowfish, ByteComparable.Version.OSS50},
                new Object[] {Config.DiskAccessMode.mmap, true, compressionParamsDes, ByteComparable.Version.LEGACY},
                new Object[] {Config.DiskAccessMode.mmap, true, compressionParamsDes, ByteComparable.Version.OSS41},
                new Object[] {Config.DiskAccessMode.mmap, true, compressionParamsDes, ByteComparable.Version.OSS50},
        });
    }

    @Parameterized.Parameter(value = 1)
    public static boolean fromFile = false;

    @Parameterized.Parameter(value = 2)
    public static CompressionParams compressionParams;

    @Parameterized.Parameter(value = 3)
    public static ByteComparable.Version version;

    class JumpingEncryptedFile extends EncryptedSequentialWriter
    {
        long[] cutoffs;
        long[] offsets;

        JumpingEncryptedFile(File file, SequentialWriterOption option, long... cutoffsAndOffsets)
        {
            super(file, option, compressionParams.getSstableCompressor().encryptionOnly());
            assert (cutoffsAndOffsets.length & 1) == 0;
            cutoffs = new long[cutoffsAndOffsets.length / 2];
            offsets = new long[cutoffs.length];
            for (int i = 0; i < cutoffs.length; ++i)
            {
                cutoffs[i] = cutoffsAndOffsets[i * 2];
                offsets[i] = cutoffsAndOffsets[i * 2 + 1];
            }
        }

        @Override
        public long position()
        {
            return jumped(super.position(), cutoffs, offsets);
        }
    }

    protected SequentialWriter makeWriter(File file)
    {

        return new EncryptedSequentialWriter(file,
                SequentialWriterOption
                        .newBuilder()
                        .finishOnClose(false)
                        .build(),
                compressionParams.getSstableCompressor().encryptionOnly());
    }

    @Override
    public SequentialWriter makeJumpingWriter(File file, long[] cutoffsAndOffsets)
    {
        return new JumpingEncryptedFile(file, SequentialWriterOption.newBuilder().finishOnClose(true).build(), cutoffsAndOffsets);
    }

    @Override
    protected FileHandle.Builder makeHandle(File file)
    {
        return new FileHandle.Builder(file)
                .bufferSize(PageAware.PAGE_SIZE)
                .mmapped(accessMode == Config.DiskAccessMode.mmap)
                .withChunkCache(ChunkCache.instance)
                .maybeEncrypted(true)
                .withCompressionMetadata(CompressionMetadata.encryptedOnly(compressionParams));
    }

    @Override
    protected PartitionIndex loadPartitionIndex(FileHandle.Builder fhBuilder, SequentialWriter writer) throws IOException
    {
        if (fromFile)
            try (FileHandle.Builder fromFileBuilder = makeHandle(writer.getFile()))
            {
                return PartitionIndex.load(fromFileBuilder, partitioner, false, ZeroCopyMetadata.EMPTY, version);
            }
        else
            return PartitionIndex.load(fhBuilder, partitioner, false, ZeroCopyMetadata.EMPTY, version);
    }

    @Override
    public void testDumpTrieToFile()
    {
        //FIXME: the tested trie dump method seems to be used only in tests, it should be revisited and fixed evnetually
    }

    /**
     * Verifies that seeking, reading and skipping over encryption-only files result in the same positions and read the
     * same data. See DSP-25176.
     */
    @Test
    public void testSkipAcrossHoles() throws IOException
    {
        int pageSize = PageAware.PAGE_SIZE;
        File tempFile = new File(java.io.File.createTempFile(getClass().getName(), ".test"));
        try (SequentialWriter writer = makeWriter(tempFile))
        {
            for (int i = 0; i < pageSize * 8; ++i)
                writer.writeByte((byte) writer.position());
            writer.finish();
        }

        try (FileHandle.Builder fhBuilder = makeHandle(tempFile);
             FileHandle fh = fhBuilder.complete();
             RandomAccessReader rdr = fh.createReader())
        {
            long len = rdr.length();
            for (int readSize : new int[]{ 1, 7, 33, 45, 67, pageSize + 55, pageSize * 2, pageSize * 3 + 34 })
            {
                byte[] buf = new byte[readSize];
                for (int seekPos = pageSize - 33; seekPos < len - 1; ++seekPos)
                {
                    rdr.seek(seekPos);
                    int read = rdr.read(buf, 0, buf.length);
                    long afterRead = rdr.getFilePointer();
                    int nextByte = getNextByte(rdr);
                    int expectedNextByte = (int) afterRead & 0xFF;

                    rdr.seek(seekPos);
                    TrieIndexSSTableReader.skipBytesWithCorrectPosition(rdr, read);
                    long afterSkip = rdr.getFilePointer();
                    int nextByteAfterSkip = getNextByte(rdr);
                    String context = String.format("(seek to %x, read %x bytes (of %x) to pos %x next %x; seek to %x corrected skip %x bytes to pos %x next %x)", seekPos, read, readSize, afterRead, nextByte, seekPos, read, afterSkip, nextByteAfterSkip);

                    Assert.assertEquals("Position" + context, afterRead, afterSkip);
                    Assert.assertEquals("Next byte" + context, nextByte, nextByteAfterSkip);

                    rdr.seek(seekPos);
                    rdr.skipBytes(read);
                    afterSkip = rdr.getFilePointer();
                    nextByteAfterSkip = getNextByte(rdr);
                    context = String.format("(seek to %x, read %x bytes (of %x) to pos %x next %x; seek to %x plain skip %x bytes to pos %x next %x)", seekPos, read, readSize, afterRead, nextByte, seekPos, read, afterSkip, nextByteAfterSkip);

                    if (afterRead != afterSkip)
                        System.out.println("Different position after plain skip " + context); // this is expected and corrected for by TrieIndexSSTableReader.skipBytesWithCorrectPosition
                    Assert.assertEquals("Next byte" + context, nextByte, nextByteAfterSkip);

                    if (nextByte != Integer.MAX_VALUE)
                        Assert.assertEquals("Byte from write pos" + context, expectedNextByte, nextByte);
                    else
                        break; // because length() is imprecise, next seeks may hit beyond the end of the file
                }
            }
        }
    }

    private static int getNextByte(RandomAccessReader rdr) throws IOException
    {
        try
        {
            return rdr.readByte() & 0xFF;
        }
        catch (EOFException e)
        {
            return Integer.MAX_VALUE;
        }
    }
}
