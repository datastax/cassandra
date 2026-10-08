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

package org.apache.cassandra.io.compress;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.crypto.KeyAccessException;
import org.apache.cassandra.io.compress.RotatingKeyProviderFactory.RotatingKeyProvider;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * {@link Encryptor} with an {@link org.apache.cassandra.crypto.IMultiKeyProvider}: every chunk must be encrypted
 * with the key named in its header, whatever key the cipher was initialized with before, and the {@code byte[]}
 * decrypt path must honor a non-zero input offset.
 */
public class MultiKeyEncryptorTest
{
    private static final String[] CIPHERS = { "AES/CBC/PKCS5Padding", "AES/ECB/PKCS5Padding" };
    private static final int CHUNK = 4096;
    private static final AtomicInteger TEST_IDS = new AtomicInteger();

    @BeforeClass
    public static void setUp()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    /**
     * A fresh encryptor initializes a cipher with the key of {@code getSecretKey}; the first chunk must still be
     * encrypted with the key its header names. The chunk goes straight to {@code compress}, without sizing it first.
     */
    @Test
    public void firstChunkAfterConstructionUsesHeaderKey() throws IOException
    {
        for (String cipher : CIPHERS)
        {
            Encryptor encryptor = newEncryptor(cipher);
            byte[] input = randomBytes(CHUNK);
            ByteBuffer encrypted = ByteBuffer.allocate(CHUNK + 64);
            encryptor.compress(ByteBuffer.wrap(input), encrypted);
            encrypted.flip();
            assertArrayEquals(cipher, input, decrypt(encryptor, encrypted, input.length));
        }
    }

    /**
     * {@code initialCompressedBufferLength} (re)initializes the cipher with the key of {@code getSecretKey} before
     * every chunk, as the metadata serializer and the encrypted sequential writer do.
     */
    @Test
    public void chunkAfterOutputLengthUsesHeaderKey() throws IOException
    {
        for (String cipher : CIPHERS)
        {
            Encryptor encryptor = newEncryptor(cipher);
            for (int i = 0; i < 3; i++)
            {
                byte[] input = randomBytes(CHUNK - i * 100);
                int outputLength = encryptor.initialCompressedBufferLength(input.length);
                ByteBuffer encrypted = ByteBuffer.allocate(outputLength);
                encryptor.compress(ByteBuffer.wrap(input), encrypted);
                encrypted.flip();
                assertArrayEquals(cipher + " chunk " + i, input, decrypt(encryptor, encrypted, input.length));
            }
        }
    }

    @Test
    public void everyChunkCarriesItsOwnKey() throws IOException
    {
        RotatingKeyProvider provider = RotatingKeyProviderFactory.provider();
        Encryptor encryptor = newEncryptor(CIPHERS[0]);
        byte[] input = randomBytes(CHUNK);
        int before = provider.headersWritten();

        ByteBuffer first = encrypt(encryptor, input);
        ByteBuffer second = encrypt(encryptor, input);
        assertEquals(2, provider.headersWritten() - before);
        assertNotEquals("each chunk names its own key", first.getInt(0), second.getInt(0));
        assertArrayEquals(input, decrypt(encryptor, first, input.length));
        assertArrayEquals(input, decrypt(encryptor, second, input.length));
    }

    /** The {@code byte[]} decrypt path with the chunk at a non-zero offset, larger than the chunk itself. */
    @Test
    public void arrayDecryptionHonorsInputOffset() throws IOException
    {
        for (String cipher : CIPHERS)
        {
            Encryptor encryptor = newEncryptor(cipher);
            byte[] input = randomBytes(100);
            ByteBuffer encrypted = encrypt(encryptor, input);
            int length = encrypted.remaining();

            for (int offset : new int[]{ 0, 7, length + 13, 1000 })
            {
                byte[] surrounded = randomBytes(offset + length + 50);
                encrypted.duplicate().get(surrounded, offset, length);
                int outputOffset = 5;
                byte[] output = new byte[outputOffset + input.length + 10];
                int decrypted = encryptor.uncompress(surrounded, offset, length, output, outputOffset);
                assertEquals(cipher + " offset " + offset, input.length, decrypted);
                assertArrayEquals(cipher + " offset " + offset, input, Arrays.copyOfRange(output, outputOffset, outputOffset + decrypted));
            }
        }
    }

    /** A chunk naming a key the provider does not have fails, it is never decrypted with another key. */
    @Test
    public void unknownKeyIdFailsClosed() throws IOException
    {
        Encryptor encryptor = newEncryptor(CIPHERS[0]);
        byte[] input = randomBytes(CHUNK);
        ByteBuffer encrypted = encrypt(encryptor, input);
        encrypted.putInt(0, Integer.MIN_VALUE);

        try
        {
            decrypt(encryptor, encrypted, input.length);
            fail("expected a failure");
        }
        catch (IOException e)
        {
            assertTrue(e.getMessage(), e.getCause() instanceof KeyAccessException);
        }
        try
        {
            encryptor.uncompress(encrypted.array(), encrypted.arrayOffset() + encrypted.position(), encrypted.remaining(), new byte[input.length], 0);
            fail("expected a failure");
        }
        catch (IOException e)
        {
            assertTrue(e.getMessage(), e.getCause() instanceof KeyAccessException);
        }
    }

    private static Encryptor newEncryptor(String cipher)
    {
        Map<String, String> options = new HashMap<>();
        options.put(EncryptionConfig.CIPHER_ALGORITHM, cipher);
        options.put(EncryptionConfig.SECRET_KEY_STRENGTH, "128");
        options.put(EncryptionConfig.KEY_PROVIDER, RotatingKeyProviderFactory.class.getName());
        // a distinct option map gives a distinct Encryptor, hence a fresh per-thread cipher state
        options.put(RotatingKeyProviderFactory.TEST_ID, Integer.toString(TEST_IDS.incrementAndGet()));
        return Encryptor.create(options);
    }

    private static ByteBuffer encrypt(Encryptor encryptor, byte[] input) throws IOException
    {
        ByteBuffer encrypted = ByteBuffer.allocate(encryptor.initialCompressedBufferLength(input.length));
        encryptor.compress(ByteBuffer.wrap(input), encrypted);
        encrypted.flip();
        return encrypted;
    }

    private static byte[] decrypt(Encryptor encryptor, ByteBuffer encrypted, int length) throws IOException
    {
        ByteBuffer decrypted = ByteBuffer.allocate(length);
        encryptor.uncompress(encrypted.duplicate(), decrypted);
        decrypted.flip();
        byte[] bytes = new byte[decrypted.remaining()];
        decrypted.get(bytes);
        return bytes;
    }

    private static byte[] randomBytes(int length)
    {
        byte[] bytes = new byte[length];
        new Random(length).nextBytes(bytes);
        return bytes;
    }
}
