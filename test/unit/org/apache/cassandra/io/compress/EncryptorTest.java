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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import javax.crypto.SecretKey;
import javax.crypto.spec.DESKeySpec;
import javax.crypto.spec.DESedeKeySpec;
import javax.crypto.spec.SecretKeySpec;

import com.google.common.collect.Maps;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.crypto.IKeyProvider;
import org.apache.cassandra.crypto.IKeyProviderFactory;
import org.apache.cassandra.crypto.LocalFileSystemKeyProviderFactory;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class EncryptorTest
{
    private static final int BUFFER_SIZE = 64 * 1024;

    @BeforeClass
    public static void generateConfigs()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    public static class KeyProviderFactoryStub implements IKeyProviderFactory
    {

        @Override
        public IKeyProvider getKeyProvider(Map<String, String> options) throws IOException
        {
            return new KeyProviderStub();
        }

        @Override
        public Set<String> supportedOptions()
        {
            return Collections.emptySet();
        }
    }

    static class KeyProviderStub implements IKeyProvider
    {
        @Override
        public SecretKey getSecretKey(String cipherName, int keyStrength)
        {
            return new SecretKeySpec(new byte[keyStrength / 8], cipherName.replaceAll("/.*", ""));
        }
    }

    @Test
    public void testAES128() throws IOException
    {
        testEncryptionAndDecryption("AES/CBC/PKCS5Padding", 128, true);
        testEncryptionAndDecryption("AES/CBC/PKCS5Padding", 128, false);
        testArrayDecryption("AES/CBC/PKCS5Padding", 128);
    }

    @Test
    public void testAES256() throws IOException
    {
        testEncryptionAndDecryption("AES/CBC/PKCS5Padding", 256, true);
        testEncryptionAndDecryption("AES/CBC/PKCS5Padding", 256, false);
        testArrayDecryption("AES/CBC/PKCS5Padding", 256);
    }

    @Test
    public void testAESwithECB() throws IOException
    {
        testEncryptionAndDecryption("AES/ECB/PKCS5Padding", 128, true);
        testEncryptionAndDecryption("AES/ECB/PKCS5Padding", 128, false);
        testArrayDecryption("AES/ECB/PKCS5Padding", 128);
    }

    @Test
    public void testDES() throws IOException
    {
        testEncryptionAndDecryption("DES/CBC/PKCS5Padding", DESKeySpec.DES_KEY_LEN * 8, true);
        testEncryptionAndDecryption("DES/CBC/PKCS5Padding", DESKeySpec.DES_KEY_LEN * 8, false);
        testArrayDecryption("DES/CBC/PKCS5Padding", DESKeySpec.DES_KEY_LEN * 8);
    }

    @Test
    public void testDESede() throws IOException
    {
        testEncryptionAndDecryption("DESede/CBC/PKCS5Padding", DESedeKeySpec.DES_EDE_KEY_LEN * 8, true);
        testEncryptionAndDecryption("DESede/CBC/PKCS5Padding", DESedeKeySpec.DES_EDE_KEY_LEN * 8, false);
        testArrayDecryption("DESede/CBC/PKCS5Padding", DESedeKeySpec.DES_EDE_KEY_LEN * 8);
    }

    @Test
    public void testBlowfish128() throws IOException
    {
        testEncryptionAndDecryption("Blowfish/CBC/PKCS5Padding", 128, true);
        testEncryptionAndDecryption("Blowfish/CBC/PKCS5Padding", 128, false);
        testArrayDecryption("Blowfish/CBC/PKCS5Padding", 128);
    }

    @Test
    public void testBlowfish256() throws IOException
    {
        testEncryptionAndDecryption("Blowfish/CBC/PKCS5Padding", 256, true);
        testEncryptionAndDecryption("Blowfish/CBC/PKCS5Padding", 256, false);
        testArrayDecryption("Blowfish/CBC/PKCS5Padding", 256);
    }

    @Test
    public void testRC2() throws IOException
    {
        testEncryptionAndDecryption("RC2/CBC/PKCS5Padding", 256, true);
        testEncryptionAndDecryption("RC2/CBC/PKCS5Padding", 256, false);
        testArrayDecryption("RC2/CBC/PKCS5Padding", 256);
    }


    private void testEncryptionAndDecryption(String algorithm, int keyStrength, boolean offHeap) throws IOException
    {
        Map<String, String> options = Maps.newHashMap();
        options.put("cipher_algorithm", algorithm);
        options.put("secret_key_strength", "" + keyStrength);
        options.put("secret_key_provider_factory_class", KeyProviderFactoryStub.class.getName());

        Encryptor cc = Encryptor.create(options);

        Random random = new Random();
        ByteBuffer input = allocate(BUFFER_SIZE, offHeap);
        byte[] randomBytes = new byte[BUFFER_SIZE];
        random.nextBytes(randomBytes);
        input.put(randomBytes);
        input.flip();

        assertTrue(cc.initialCompressedBufferLength(input.capacity()) >= input.capacity());

        ByteBuffer output1 = allocate(cc.initialCompressedBufferLength(input.capacity()), offHeap);
        cc.compress(input, output1);
        input.flip();
        output1.flip();

        ByteBuffer output2 = allocate(cc.initialCompressedBufferLength(input.capacity()), offHeap);
        cc.compress(input, output2);
        input.flip();
        output2.flip();

        ByteBuffer decrypted = allocate(input.capacity(), offHeap);

        cc.uncompress(output1, decrypted);
        output1.flip();
        decrypted.flip();

        assertEquals(input.remaining(), decrypted.remaining());
        assertEquals(0, input.compareTo(decrypted));

        if (!algorithm.contains("/ECB/"))
        {
            assertEquals(output1.remaining(), output2.remaining());
            assertNotEquals(0, output1.compareTo(output2));
        }
    }

    private void testArrayDecryption(String algorithm, int keyStrength) throws IOException
    {
        Map<String, String> options = Maps.newHashMap();
        options.put("cipher_algorithm", algorithm);
        options.put("secret_key_strength", "" + keyStrength);
        options.put("secret_key_provider_factory_class", KeyProviderFactoryStub.class.getName());

        Encryptor cc = Encryptor.create(options);

        Random random = new Random();
        ByteBuffer input = allocate(BUFFER_SIZE, false);
        byte[] randomBytes = new byte[BUFFER_SIZE];
        random.nextBytes(randomBytes);
        input.put(randomBytes);
        input.flip();

        assertTrue(cc.initialCompressedBufferLength(input.capacity()) >= input.capacity());

        ByteBuffer output = allocate(cc.initialCompressedBufferLength(input.capacity()), false);
        cc.compress(input, output);
        input.flip();
        output.flip();

        byte[] decrypted = new byte[input.capacity()];

        cc.uncompress(output.array(), 0, output.limit(), decrypted, 0);

        assertArrayEquals(input.array(), decrypted);
    }

    @Test
    public void testWithLocalFileSystemKeyProvider() throws IOException
    {
        Random random = new Random();
        ByteBuffer input = allocate(BUFFER_SIZE, false);
        random.nextBytes(input.array());

        String secretKeyFilePath =
                CassandraRelevantProperties.JAVA_IO_TMPDIR.getString() + File.separator + "dse" + File.separator + "secret_key_file.txt";
        Map<String, String> options = Maps.newHashMap();
        options.put("cipher_algorithm", "AES/CBC/PKCS5Padding");
        options.put("secret_key_strength", "128");
        options.put("secret_key_provider_factory_class", LocalFileSystemKeyProviderFactory.class.getName());
        options.put("secret_key_file", secretKeyFilePath);

        Encryptor cc1 = Encryptor.create(options);
        ByteBuffer output = allocate(cc1.initialCompressedBufferLength(input.capacity()), false);
        cc1.compress(input, output);
        output.flip();

        Encryptor cc2 = Encryptor.create(options);
        ByteBuffer decrypted = allocate(input.capacity(), false);
        cc2.uncompress(output, decrypted);

        assertArrayEquals(input.array(), decrypted.array());
        assertTrue(new File(secretKeyFilePath).delete());
    }

    private ByteBuffer allocate(int size, boolean offHeap)
    {
        return offHeap ? ByteBuffer.allocateDirect(size) : ByteBuffer.allocate(size);
    }
}
