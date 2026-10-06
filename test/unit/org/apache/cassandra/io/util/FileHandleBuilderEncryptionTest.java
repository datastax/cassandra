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

package org.apache.cassandra.io.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.compress.EncryptedSequentialWriter;
import org.apache.cassandra.io.compress.EncryptionConfig;
import org.apache.cassandra.io.compress.Encryptor;
import org.apache.cassandra.io.compress.EncryptorTest;
import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.schema.CompressionParams;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Checks that {@link FileHandle.Builder#complete()} rejects the inconsistent combinations of
 * {@link FileHandle.Builder#encryptionOnly()} and the compression metadata, and that encryption-only files are read
 * with any {@link ICompressor} whose {@link ICompressor#encryptionOnly()} is not null, not only {@link Encryptor}.
 */
public class FileHandleBuilderEncryptionTest
{
    private static final int BYTES_TO_WRITE = 10_000;

    private File file;

    @BeforeClass
    public static void setupDD()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @After
    public void tearDown()
    {
        if (file != null)
            file.tryDelete();
    }

    @Before
    public void setUp()
    {
        file = FileUtils.createTempFile("file-handle-builder-encryption", ".db");
        file.deleteOnExit();
    }

    private static CompressionParams encryptionParams(Class<? extends ICompressor> compressorClass)
    {
        Map<String, String> opts = new HashMap<>();
        opts.put(EncryptionConfig.KEY_PROVIDER, EncryptorTest.KeyProviderFactoryStub.class.getName());
        opts.put(EncryptionConfig.CIPHER_ALGORITHM, "AES/CBC/PKCS5Padding");
        opts.put(EncryptionConfig.SECRET_KEY_STRENGTH, Integer.toString(128));
        opts.put(CompressionParams.CLASS, compressorClass.getName());
        return CompressionParams.fromMap(opts);
    }

    private void writeEncryptedFile(CompressionParams params) throws IOException
    {
        try (EncryptedSequentialWriter writer = new EncryptedSequentialWriter(file,
                                                                              SequentialWriterOption.newBuilder().finishOnClose(false).build(),
                                                                              params.getSstableCompressor().encryptionOnly()))
        {
            for (int i = 0; i < BYTES_TO_WRITE; ++i)
                writer.writeByte((byte) i);
            writer.finish();
        }
    }

    private void assertReadable(CompressionMetadata metadata, boolean mmapped) throws IOException
    {
        try (FileHandle fh = new FileHandle.Builder(file).mmapped(mmapped)
                                                         .withCompressionMetadata(metadata)
                                                         .encryptionOnly()
                                                         .complete();
             RandomAccessReader reader = fh.createReader())
        {
            // bytes written at the end of a chunk land at the start of the next one, so read them sequentially
            for (int i = 0; i < BYTES_TO_WRITE; ++i)
                assertThat(reader.readByte()).isEqualTo((byte) i);
        }
    }

    @Test
    public void testEncryptionOnlyWithEncryptionOnlyMetadata() throws IOException
    {
        CompressionParams params = encryptionParams(Encryptor.class);
        writeEncryptedFile(params);
        for (boolean mmapped : new boolean[]{ false, true })
        {
            try (CompressionMetadata metadata = CompressionMetadata.encryptedOnly(params))
            {
                assertReadable(metadata, mmapped);
            }
        }
    }

    /**
     * An encrypting compressor does not need to be an {@link Encryptor}: {@link ICompressor#encryptionOnly()} only
     * promises an {@link ICompressor}.
     */
    @Test
    public void testEncryptionOnlyWithOtherEncryptingCompressor() throws IOException
    {
        CompressionParams params = encryptionParams(DelegatingEncryptor.class);
        assertThat(params.getSstableCompressor().encryptionOnly()).isInstanceOf(DelegatingEncryptor.class);
        writeEncryptedFile(params);
        for (boolean mmapped : new boolean[]{ false, true })
        {
            try (CompressionMetadata metadata = CompressionMetadata.encryptedOnly(params))
            {
                assertReadable(metadata, mmapped);
            }
        }
    }

    @Test
    public void testEncryptionOnlyWithoutMetadata() throws IOException
    {
        writeEncryptedFile(encryptionParams(Encryptor.class));
        FileHandle.Builder builder = new FileHandle.Builder(file).encryptionOnly();
        assertThatThrownBy(builder::complete).isInstanceOf(IllegalStateException.class)
                                             .hasMessageContaining("is marked as encryption-only but its compression metadata has no encrypting compressor");
    }

    @Test
    public void testEncryptionOnlyWithNonEncryptingCompressor() throws IOException
    {
        writeEncryptedFile(encryptionParams(Encryptor.class));
        for (CompressionParams params : new CompressionParams[]{ CompressionParams.lz4(), CompressionParams.noCompression() })
        {
            try (CompressionMetadata metadata = CompressionMetadata.encryptedOnly(params))
            {
                FileHandle.Builder builder = new FileHandle.Builder(file).withCompressionMetadata(metadata).encryptionOnly();
                assertThatThrownBy(builder::complete).describedAs(params.toString())
                                                     .isInstanceOf(IllegalStateException.class)
                                                     .hasMessageContaining("is marked as encryption-only but its compression metadata has no encrypting compressor");
            }
        }
    }

    @Test
    public void testEncryptionOnlyMetadataWithoutEncryptionOnly() throws IOException
    {
        CompressionParams params = encryptionParams(Encryptor.class);
        writeEncryptedFile(params);
        for (boolean mmapped : new boolean[]{ false, true })
        {
            try (CompressionMetadata metadata = CompressionMetadata.encryptedOnly(params))
            {
                FileHandle.Builder builder = new FileHandle.Builder(file).mmapped(mmapped).withCompressionMetadata(metadata);
                assertThatThrownBy(builder::complete).isInstanceOf(IllegalStateException.class)
                                                     .hasMessageContaining("has encryption-only compression metadata but is not marked as encryption-only");
            }
        }
    }

    /**
     * An encrypting {@link ICompressor} that is not an {@link Encryptor}: it delegates to one.
     */
    public static class DelegatingEncryptor implements ICompressor
    {
        private final Encryptor delegate;

        private DelegatingEncryptor(Encryptor delegate)
        {
            this.delegate = delegate;
        }

        public static DelegatingEncryptor create(Map<String, String> options)
        {
            return new DelegatingEncryptor(Encryptor.create(options));
        }

        @Override
        public int initialCompressedBufferLength(int chunkLength)
        {
            return delegate.initialCompressedBufferLength(chunkLength);
        }

        @Override
        public int uncompress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset) throws IOException
        {
            return delegate.uncompress(input, inputOffset, inputLength, output, outputOffset);
        }

        @Override
        public void compress(ByteBuffer input, ByteBuffer output) throws IOException
        {
            delegate.compress(input, output);
        }

        @Override
        public void uncompress(ByteBuffer input, ByteBuffer output) throws IOException
        {
            delegate.uncompress(input, output);
        }

        @Override
        public BufferType preferredBufferType()
        {
            return delegate.preferredBufferType();
        }

        @Override
        public boolean supports(BufferType bufferType)
        {
            return delegate.supports(bufferType);
        }

        @Override
        public Set<String> supportedOptions()
        {
            return delegate.supportedOptions();
        }

        @Override
        public ICompressor encryptionOnly()
        {
            return this;
        }

        @Override
        public boolean canDecompressInPlace()
        {
            return delegate.canDecompressInPlace();
        }
    }
}
