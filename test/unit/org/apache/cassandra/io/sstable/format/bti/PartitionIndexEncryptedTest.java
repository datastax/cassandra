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
package org.apache.cassandra.io.sstable.format.bti;

import java.io.EOFException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import javax.crypto.spec.DESKeySpec;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
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
import org.apache.cassandra.io.compress.OutOfPlaceEncryptor;
import org.apache.cassandra.io.sstable.metadata.ZeroCopyMetadata;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.PageAware;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.io.util.SequentialWriterOption;
import org.apache.cassandra.schema.CompressionParams;
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

    /**
     * This class has 12 parameterisations against the 6 of PartitionIndexTest, and each one is as fast as a plain one
     * now that encrypted index reads go through the chunk cache. With the full count the class takes about 7 minutes
     * here and would take about twice as long on CI, well over the 8-minute class fork timeout (test.timeout). A
     * quarter of the keys keeps every parameterisation and brings the class to about 1.5 minutes here (about 3 on CI).
     */
    @Override
    protected int count()
    {
        return COUNT / 4;
    }

    @Parameterized.Parameters(name="accessMode {0} BC version {1} compressionParams {2} fromFile {3}")
    public static Collection<Object[]> generateData()
    {
        return Arrays.asList(new Object[][]{
                new Object[] {Config.DiskAccessMode.standard, ByteComparable.Version.LEGACY, compressionParamsNormal, false},
                new Object[] {Config.DiskAccessMode.standard, ByteComparable.Version.OSS41, compressionParamsNormal, false},
                new Object[] {Config.DiskAccessMode.standard, ByteComparable.Version.OSS50, compressionParamsNormal, false},
                // fromFile and out-of-place have independent implementations, one run suffices to test both
                new Object[] {Config.DiskAccessMode.standard, ByteComparable.Version.LEGACY, compressionParamsOutOfPlace, true},
                new Object[] {Config.DiskAccessMode.standard, ByteComparable.Version.OSS41, compressionParamsOutOfPlace, true},
                new Object[] {Config.DiskAccessMode.standard, ByteComparable.Version.OSS50, compressionParamsOutOfPlace, true},
                new Object[] {Config.DiskAccessMode.mmap, ByteComparable.Version.LEGACY, compressionParamsBlowfish, false},
                new Object[] {Config.DiskAccessMode.mmap, ByteComparable.Version.OSS41, compressionParamsBlowfish, false},
                new Object[] {Config.DiskAccessMode.mmap, ByteComparable.Version.OSS50, compressionParamsBlowfish, false},
                new Object[] {Config.DiskAccessMode.mmap, ByteComparable.Version.LEGACY, compressionParamsDes, true},
                new Object[] {Config.DiskAccessMode.mmap, ByteComparable.Version.OSS41, compressionParamsDes, true},
                new Object[] {Config.DiskAccessMode.mmap, ByteComparable.Version.OSS50, compressionParamsDes, true},
        });
    }

    // Parameters 0 (accessMode) and 1 (version) are the fields of PartitionIndexTest, which its test methods read.
    // Do not redeclare them here: JUnit matches @Parameter fields by name, so a redeclared field would be injected
    // instead and the base-class field would stay null.

    @Parameterized.Parameter(value = 2)
    public static CompressionParams compressionParams;

    @Parameterized.Parameter(value = 3)
    public static boolean fromFile;

    CompressionMetadata compressionMetadata;

    @Before
    public void setCompressionMetadata()
    {
        compressionMetadata = CompressionMetadata.encryptedOnly(compressionParams);
    }

    @After
    public void releaseCompressionMetadata()
    {
        compressionMetadata.close();
    }

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
                .encryptionOnly()
                .withCompressionMetadata(compressionMetadata);
    }

    @Override
    protected PartitionIndex loadPartitionIndex(FileHandle.Builder fhBuilder, SequentialWriter writer, ZeroCopyMetadata zeroCopyMetadata) throws IOException
    {
        if (fromFile)
        {
            FileHandle.Builder fromFileBuilder = makeHandle(writer.getFile());
            return PartitionIndex.load(fromFileBuilder, partitioner, false, zeroCopyMetadata, version);
        }
        else
            return PartitionIndex.load(fhBuilder, partitioner, false, zeroCopyMetadata, version);
    }

    /**
     * Verifies that seeking, reading and skipping over encryption-only files result in the same positions and read the
     * same data (see DSP-25176), that the end of the file reads as EOF, and that positions past the data are errors.
     */
    @Test
    public void testSkipAcrossHoles() throws IOException
    {
        int pageSize = PageAware.PAGE_SIZE;
        File tempFile = FileUtils.createTempFile(getClass().getName(), ".test");
        try
        {
            long dataEnd;
            try (SequentialWriter writer = makeWriter(tempFile))
            {
                for (int i = 0; i < pageSize * 8; ++i)
                    writer.writeByte((byte) writer.position());
                dataEnd = writer.position();
                writer.finish();
            }

            FileHandle.Builder fhBuilder = makeHandle(tempFile);
            try (FileHandle fh = fhBuilder.complete();
                 RandomAccessReader rdr = fh.createReader())
            {
                long len = rdr.length();
                // the last chunk is partially filled: length() is the end of the data in it
                Assert.assertEquals(dataEnd, len);
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
                        Assert.assertEquals(read, rdr.skipBytes(read));
                        long afterSkip = rdr.getFilePointer();
                        int nextByteAfterSkip = getNextByte(rdr);
                        String context = String.format("(seek to %x, read %x bytes (of %x) to pos %x next %x; seek to %x skip %x bytes to pos %x next %x)", seekPos, read, readSize, afterRead, nextByte, seekPos, read, afterSkip, nextByteAfterSkip);

                        Assert.assertEquals("Position" + context, afterRead, afterSkip);
                        Assert.assertEquals("Next byte" + context, nextByte, nextByteAfterSkip);

                        if (nextByte != Integer.MAX_VALUE)
                        {
                            Assert.assertEquals("Byte from write pos" + context, expectedNextByte, nextByte);
                        }
                        else
                        {
                            // the read reached length(), the end of the data
                            Assert.assertEquals("End of data" + context, dataEnd, afterRead);
                            break;
                        }
                    }
                }

                // seeking to length() is valid and reads as EOF
                rdr.seek(len);
                Assert.assertEquals(len, rdr.getFilePointer());
                Assert.assertTrue(rdr.isEOF());
                Assert.assertEquals(0, rdr.bytesRemaining());
                Assert.assertEquals(Integer.MAX_VALUE, getNextByte(rdr));
                Assert.assertEquals(0, rdr.skipBytes(1));
                Assert.assertEquals(len, rdr.getFilePointer());

                // seeking past length() is an error
                Assert.assertThrows(IllegalArgumentException.class, () -> rdr.seek(len + 1));
            }
        }
        finally
        {
            tempFile.tryDelete();
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
