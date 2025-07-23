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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import javax.crypto.spec.DESKeySpec;
import javax.crypto.spec.DESedeKeySpec;

import com.google.common.io.Files;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.DataPosition;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.io.util.SequentialWriterOption;
import org.apache.cassandra.io.util.SequentialWriterTest;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.utils.memory.BufferPools;

import static org.apache.cassandra.io.compress.EncryptedSequentialWriter.CHUNK_SIZE;
import static org.apache.cassandra.io.compress.EncryptedSequentialWriter.FOOTER_LENGTH;
import static org.apache.commons.io.FileUtils.readFileToByteArray;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class EncryptedSequentialWriterTest extends SequentialWriterTest
{
    ICompressor encryptor;
    CompressionParams compressionParams;

    static
    {
        DatabaseDescriptor.daemonInitialization();
    }

    static final ICompressor AESEncryptor;
    static final CompressionParams AESEncryptorParams;
    static
    {
        Map<String, String> opts = new HashMap<>();

        opts.put(CompressionParams.CLASS, Encryptor.class.getName());

        opts.put(EncryptionConfig.CIPHER_ALGORITHM, "AES/CBC/PKCS5Padding");
        opts.put(EncryptionConfig.SECRET_KEY_STRENGTH, Integer.toString(128));

        opts.put(EncryptionConfig.KEY_PROVIDER, EncryptorTest.KeyProviderFactoryStub.class.getName());

        AESEncryptor = Encryptor.create(opts);
        AESEncryptorParams = CompressionParams.fromMap(opts);
    }

    private void runTests(String cipher, int strength) throws IOException
    {
        assertEquals(0, BufferPools.forChunkCache().usedSizeInBytes());

        Map<String, String> opts = new HashMap<>();

        opts.put(CompressionParams.CLASS, Encryptor.class.getName());

        opts.put(EncryptionConfig.CIPHER_ALGORITHM, cipher);
        opts.put(EncryptionConfig.SECRET_KEY_STRENGTH, Integer.toString(strength));

        opts.put(EncryptionConfig.KEY_PROVIDER, EncryptorTest.KeyProviderFactoryStub.class.getName());

        compressionParams = CompressionParams.fromMap(opts);
        encryptor = compressionParams.getSstableCompressor();

        String testName = cipher + ":" + strength;

        // Test small < 1 chunk data set
        testWrite(createTempFile(testName + "_small", "1"), 25, false);

        // Test to confirm pipeline w/chunk-aligned data writes works
        testWrite(createTempFile(testName + "_chunkAligned", "1"), CHUNK_SIZE, false);

        // Test to confirm pipeline on non-chunk boundaries works
        testWrite(createTempFile(testName + "_large", "1"), CHUNK_SIZE * 3 + 100, false);

        // Test small < 1 chunk data set
        testWrite(createTempFile(testName + "_small", "2"), 25, true);

        // Test to confirm pipeline w/chunk-aligned data writes works
        testWrite(createTempFile(testName + "_chunkAligned", "2"), CHUNK_SIZE, true);

        // Test to confirm pipeline on non-chunk boundaries works
        testWrite(createTempFile(testName + "_large", "2"), CHUNK_SIZE * 3 + 100, true);

        assertEquals(0, BufferPools.forChunkCache().usedSizeInBytes());
    }

    private File createTempFile(String prefix, String suffix) throws IOException
    {
        return new File(java.io.File.createTempFile(prefix, suffix));
    }

    @Test
    public void testAES128() throws IOException
    {
        runTests("AES/CBC/PKCS5Padding", 128);
    }

    @Test
    public void testAES256() throws IOException
    {
        runTests("AES/CBC/PKCS5Padding", 256);
    }

    @Test
    public void testAESwithECB() throws IOException
    {
        runTests("AES/ECB/PKCS5Padding", 128);
    }

    @Test
    public void testDES() throws IOException
    {
        runTests("DES/CBC/PKCS5Padding", DESKeySpec.DES_KEY_LEN * 8);
    }

    @Test
    public void testDESede() throws IOException
    {
        runTests("DESede/CBC/PKCS5Padding", DESedeKeySpec.DES_EDE_KEY_LEN * 8);
    }

    @Test
    public void testBlowfish128() throws IOException
    {
        runTests("Blowfish/CBC/PKCS5Padding", 128);
    }

    @Test
    public void testBlowfish256() throws IOException
    {
        runTests("Blowfish/CBC/PKCS5Padding", 256);
    }

    @Test
    public void testRC2() throws IOException
    {
        runTests("RC2/CBC/PKCS5Padding", 256);
    }

    private void testWrite(File f, int bytesToTest, boolean useMemmap) throws IOException
    {
        final String filename = f.toJavaIOFile().getAbsolutePath();

        byte[] dataPre = new byte[bytesToTest];
        byte[] rawPost = new byte[bytesToTest];
        try (EncryptedSequentialWriter writer = new EncryptedSequentialWriter(f, SequentialWriterOption.DEFAULT, encryptor))
        {
            Random r = new Random(42);

            // Test both write with byte[] and ByteBuffer
            r.nextBytes(dataPre);
            r.nextBytes(rawPost);
            ByteBuffer dataPost = makeBB(bytesToTest);
            dataPost.put(rawPost);
            dataPost.flip();

            writer.write(dataPre);
            DataPosition mark = writer.mark();

            // Pad till end of chunk
            writer.padToPageBoundary();

            if (bytesToTest <= writer.maxBytesInPage())
                assertEquals(CHUNK_SIZE, writer.getLastFlushOffset());
            else
                assertTrue(writer.getLastFlushOffset() % CHUNK_SIZE == 0);

            writer.resetAndTruncate(mark);
            writer.write(dataPost);
            writer.finish();
        }

        assert f.exists();
        FileHandle.Builder builder = new FileHandle.Builder(f)
                .withCompressionMetadata(CompressionMetadata.encryptedOnly(compressionParams))
                .maybeEncrypted(true)
                .mmapped(useMemmap);
        try (FileHandle fh = builder.complete();
             RandomAccessReader reader = fh.createReader())
        {
            // No longer true: assertEquals(dataPre.length + rawPost.length, reader.length());
            byte[] result = new byte[dataPre.length + rawPost.length];

            reader.readFully(result);

            // Encrypted writer does not guarantee assert(reader.isEOF());
            reader.close();

            byte[] fullInput = new byte[bytesToTest * 2];
            System.arraycopy(dataPre, 0, fullInput, 0, dataPre.length);
            System.arraycopy(rawPost, 0, fullInput, bytesToTest, rawPost.length);
            assert Arrays.equals(result, fullInput);
        }
        finally
        {
            if (f.exists())
                f.delete();
            File metadata = new File(f + ".metadata");
            if (metadata.exists())
                metadata.delete();
        }
    }

    private ByteBuffer makeBB(int size)
    {
        return BufferType.preferredForCompression().allocate(size);
    }

    private final List<TestableCSW> writers = new ArrayList<>();

    @After
    public void cleanup()
    {
        for (TestableCSW sw : writers)
            sw.cleanup();
        writers.clear();
    }

    @Test
    @Override
    public void resetAndTruncateTest()
    {
        File tempFile = new File(Files.createTempDir(), "reset.txt");
        final int writeSize = CHUNK_SIZE * 5 / 4;
        byte[] toWrite = new byte[writeSize];
        try (SequentialWriter writer = new EncryptedSequentialWriter(tempFile, SequentialWriterOption.DEFAULT, AESEncryptor))
        {
            // write bytes greather than buffer
            writer.write(toWrite);
            long flushedOffset = writer.getLastFlushOffset();
            long resetPos = writer.position(); // Note: not the same as writeSize as position jumps over the metadata
            // mark thi position
            DataPosition pos = writer.mark();
            // write another
            writer.write(toWrite);
            // another buffer should be flushed
            assertEquals(flushedOffset * 2, writer.getLastFlushOffset());
            assertEquals(resetPos * 2, writer.position());  // Not exactly guaranteed, but should be true for test as
            // 2.5 chunks should have two metadata sections
            // reset writer
            writer.resetAndTruncate(pos);
            // current position and flushed size should be changed
            assertEquals(resetPos, writer.position());
            assertEquals(flushedOffset, writer.getLastFlushOffset());
            // write another byte less than buffer
            writer.write(new byte[]{0});
            assertEquals(resetPos + 1, writer.position());
            // flush off set should not be increase
            assertEquals(flushedOffset, writer.getLastFlushOffset());
            writer.finish();
        }
        catch (IOException e)
        {
            Assert.fail();
        }
    }

    @Test
    public void emptyFileTest() throws IOException
    {
        File tempFile = createTempFile("empty", ".txt");
        try (SequentialWriter writer = new EncryptedSequentialWriter(tempFile, SequentialWriterOption.DEFAULT, AESEncryptor))
        {
            // do not write anything, but finalize to do a sync
            writer.finish();
        }

        FileHandle.Builder builder = new FileHandle.Builder(tempFile)
                .withCompressionMetadata(CompressionMetadata.encryptedOnly(AESEncryptorParams))
                .maybeEncrypted(true);
        try (FileHandle fh = builder.complete();
             RandomAccessReader reader = fh.createReader())
        {
            assertTrue(reader.isEOF());
            assertEquals(0, reader.length());
        }
        assertEquals(0, tempFile.length());
    }

    protected TestableTransaction newTest() throws IOException
    {
        TestableCSW sw = new TestableCSW();
        writers.add(sw);
        return sw;
    }

    private static class TestableCSW extends TestableSW
    {
        private TestableCSW() throws IOException
        {
            this(tempFile("encryptedsequentialwriter"));
        }

        private TestableCSW(File file) throws IOException
        {
            this(file, new EncryptedSequentialWriter(file, SequentialWriterOption.DEFAULT, AESEncryptor));
        }

        private TestableCSW(File file, EncryptedSequentialWriter sw) throws IOException
        {
            super(file, sw, sw.maxBytesInPage());
        }

        private int getInt(byte[] buf, int position)
        {
            return ByteBuffer.wrap(buf).getInt(position);
        }

        protected void assertInProgress() throws Exception
        {
            Assert.assertTrue(file.exists());
            byte[] compressed = readFileToByteArray(file.toJavaIOFile());
            byte[] uncompressed = new byte[partialContents.length];
            AESEncryptor.uncompress(compressed, 0, getInt(compressed, compressed.length - FOOTER_LENGTH), uncompressed, 0);
            Assert.assertTrue(Arrays.equals(partialContents, uncompressed));
        }

        protected void assertPrepared() throws Exception
        {
            Assert.assertTrue(file.exists());
            int offset = CHUNK_SIZE;
            byte[] compressed = readFileToByteArray(file.toJavaIOFile());
            byte[] uncompressed = new byte[fullContents.length];
            AESEncryptor.uncompress(compressed, 0, getInt(compressed, offset - FOOTER_LENGTH), uncompressed, 0);
            AESEncryptor.uncompress(compressed, offset, getInt(compressed,compressed.length - FOOTER_LENGTH), uncompressed, partialContents.length);
            Assert.assertTrue(Arrays.equals(fullContents, uncompressed));
        }

        protected void assertAborted() throws Exception
        {
            super.assertAborted();
        }

        void cleanup()
        {
            file.delete();
        }
    }

}
