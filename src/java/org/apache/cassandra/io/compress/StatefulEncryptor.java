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

import java.nio.ByteBuffer;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.AlgorithmParameterSpec;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.ShortBufferException;
import javax.crypto.spec.IvParameterSpec;

import org.apache.commons.math3.random.ISAACRandom;

import org.apache.cassandra.crypto.IKeyProvider;
import org.apache.cassandra.crypto.IMultiKeyProvider;
import org.apache.cassandra.crypto.KeyAccessException;
import org.apache.cassandra.crypto.KeyGenerationException;

/**
 * Encrypts blocks of data. Reuses the same Cipher instance and avoids needless initialization.
 * Not thread-safe!
 */
class StatefulEncryptor
{
    private final SecureRandom random;
    private final EncryptionConfig config;
    private final byte[] iv;
    private final Cipher cipher;
    private final ISAACRandom fastRandom;

    private boolean initialized = false;

    StatefulEncryptor(EncryptionConfig config, SecureRandom random) throws NoSuchPaddingException, NoSuchAlgorithmException,
            InvalidAlgorithmParameterException, InvalidKeyException, KeyAccessException, KeyGenerationException
    {
        this.random = random;
        this.config = config;
        this.iv = new byte[config.getIvLength()];
        cipher = Cipher.getInstance(config.getCipherName());
        int[] seed = new int[256];
        for (int i = 0; i < seed.length; i++)
            seed[i] = random.nextInt();
        this.fastRandom = new ISAACRandom(seed);
        maybeInit(config.getKeyProvider().getSecretKey(config.getCipherName(), config.getKeyStrength()));
    }

    private void maybeInit(SecretKey key) throws InvalidAlgorithmParameterException, InvalidKeyException
    {
        if (!initialized)
            init(key);
    }

    private void init(SecretKey key) throws InvalidAlgorithmParameterException, InvalidKeyException
    {
        if (config.isIvEnabled())
            cipher.init(Cipher.ENCRYPT_MODE, key, createIV(), random);
        else
            cipher.init(Cipher.ENCRYPT_MODE, key, random);
        initialized = true;
    }

    private AlgorithmParameterSpec createIV()
    {
        for (int i = 0; i < config.getIvLength(); i += 4)
        {
            int value = fastRandom.nextInt();
            iv[i] = (byte)(value >>> 24);
            iv[i + 1] = (byte)(value >>> 16);
            iv[i + 2] = (byte)(value >>> 8);
            iv[i + 3] = (byte) value;
        }
        return new IvParameterSpec(iv);
    }

    void encrypt(ByteBuffer input, ByteBuffer output)
            throws InvalidAlgorithmParameterException, InvalidKeyException, BadPaddingException, ShortBufferException, IllegalBlockSizeException, KeyAccessException, KeyGenerationException
    {
        SecretKey key;
        IKeyProvider keyProvider = config.getKeyProvider();
        if (keyProvider instanceof IMultiKeyProvider)
        {
            key = ((IMultiKeyProvider) keyProvider).writeHeader(config.getCipherName(), config.getKeyStrength(), output);
        }
        else
        {
            key = keyProvider.getSecretKey(config.getCipherName(), config.getKeyStrength());
        }

        maybeInit(key);
        if (config.isIvEnabled())
        {
            output.put(iv);
        }
        cipher.doFinal(input, output);
        initialized = false;
    }

    int outputLength(int inputSize) throws InvalidAlgorithmParameterException, InvalidKeyException, KeyAccessException, KeyGenerationException
    {
        IKeyProvider keyProvider = config.getKeyProvider();
        maybeInit(keyProvider.getSecretKey(config.getCipherName(), config.getKeyStrength()));
        int headerSize = keyProvider instanceof IMultiKeyProvider ? ((IMultiKeyProvider) keyProvider).headerLength() : 0;
        return config.getIvLength() + cipher.getOutputSize(inputSize) + headerSize;
    }
}
