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
import java.util.Objects;
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
    /** The key the cipher is currently initialized with; only meaningful while {@link #initialized}. */
    private SecretKey initializedKey;

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

    /**
     * Initializes the cipher with the given key, unless it is already initialized with that key. A multi-key provider
     * can return different keys from {@link IKeyProvider#getSecretKey} (used by the constructor and by
     * {@link #outputLength}) and from {@link IMultiKeyProvider#writeHeader} (used by {@link #encrypt}): the chunk must
     * always be encrypted with the key named in its header, so a different key always re-initializes the cipher.
     * Keys are compared with {@link SecretKey#equals}: a provider should return the same instance, or equal keys,
     * for the same key, or every chunk pays one extra cipher initialization.
     */
    private void maybeInit(SecretKey key) throws InvalidAlgorithmParameterException, InvalidKeyException
    {
        Objects.requireNonNull(key, "the key provider returned a null key");
        if (!initialized || !key.equals(initializedKey))
            init(key);
    }

    private void init(SecretKey key) throws InvalidAlgorithmParameterException, InvalidKeyException
    {
        // a failed cipher.init leaves the cipher uninitialized: do not remember the previous key
        initialized = false;
        initializedKey = null;
        if (config.isIvEnabled())
            cipher.init(Cipher.ENCRYPT_MODE, key, createIV(), random);
        else
            cipher.init(Cipher.ENCRYPT_MODE, key, random);
        initializedKey = key;
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

        // the cipher is initialized with the key named in the header (fresh IV), whatever key was used before
        maybeInit(key);
        if (config.isIvEnabled())
        {
            output.put(iv);
        }
        cipher.doFinal(input, output);
        initialized = false;
        initializedKey = null;
    }

    int outputLength(int inputSize) throws InvalidAlgorithmParameterException, InvalidKeyException, KeyAccessException, KeyGenerationException
    {
        IKeyProvider keyProvider = config.getKeyProvider();
        maybeInit(keyProvider.getSecretKey(config.getCipherName(), config.getKeyStrength()));
        int headerSize = keyProvider instanceof IMultiKeyProvider ? ((IMultiKeyProvider) keyProvider).headerLength() : 0;
        return config.getIvLength() + cipher.getOutputSize(inputSize) + headerSize;
    }
}
