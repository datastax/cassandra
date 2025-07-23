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
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.crypto.NoSuchPaddingException;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.crypto.KeyAccessException;
import org.apache.cassandra.crypto.KeyGenerationException;
import org.apache.cassandra.service.StorageService;

/**
 * Encrypts and decrypts data stored in Cassandra database. Plugs into C* custom compression API.
 * Use compression_options of a column family to make this class process data in CF.
 * <p/>
 * The following compression_options properties are available for this class:
 * <ul>
 * <li>cipher_algorithm - name of the encryption algorithm provided in Java Cryptographic Extensions, default AES/CBC/PKCS5Padding</li>
 * <li>secret_key_strength - bit-strength of the key, default 128</li>
 * <li>secret_key_provider_factory_class - name of the factory for </li>
 * </ul>
 */
public class Encryptor implements ICompressor
{
    private static final Logger logger = LoggerFactory.getLogger(Encryptor.class);
    private static final SecureRandom random = new SecureRandom();

    // Cache Encryptor instances by EncryptionConfig.
    private static final LoadingCache<EncryptionConfig, Encryptor> encryptorsByConfig =
            CacheBuilder.newBuilder()
                    .expireAfterAccess(1, TimeUnit.DAYS)
                    .removalListener(notification ->
                    {
                        EncryptionConfig config = (EncryptionConfig) notification.getKey();
                        logger.debug("Removing cached encryptor for {}", config);
                    })
                    .build(new CacheLoader<EncryptionConfig, Encryptor>()
                    {
                        @Override
                        public Encryptor load(EncryptionConfig encryptionConfig)
                        {
                            return new Encryptor(encryptionConfig);
                        }
                    });

    private final EncryptionConfig encryptionConfig;

    // No need to migrate these to InlinedThreadLocal and take a slot from the optimized local access fields. These would probably be
    // used and accessed for a total period of time that's much shorter than their expected lifetime (as determined by the thread
    // lifetime, but more importantly for long-living threads - by the enclosing Encryptor's lifetime), so most of the time they
    // would just sit there, occupying a slot.
    private final ThreadLocal<StatefulEncryptor> encryptor = new ThreadLocal<>();
    private final ThreadLocal<StatefulDecryptor> decryptor = new ThreadLocal<>();

    public static Encryptor create(Map<String, String> options)
    {
        EncryptionConfig encryptionConfig = EncryptionConfig.forClass(Encryptor.class).fromCompressionOptions(options).build();
        return encryptorsByConfig.getUnchecked(encryptionConfig);
    }

    protected Encryptor(EncryptionConfig encryptionConfig)
    {
        this.encryptionConfig = encryptionConfig;
        logger.debug("Creating new encryptor with {}", encryptionConfig);
        try
        {
            // Try to get encryptor/decryptor objects to signal any errors now
            if (StorageService.instance.isInitialized() && !StorageService.instance.isBootstrapMode())
            {
                getEncryptor();
                getDecryptor();
            }

        }
        catch (NoSuchAlgorithmException e)
        {
            throw new RuntimeException("Failed to initialize " + Encryptor.class.getSimpleName() + ". " +
                    "Cipher algorithm not supported: " + encryptionConfig.getCipherName());
        }
        catch (NoSuchPaddingException e)
        {
            throw new RuntimeException("Failed to initialize " + Encryptor.class.getSimpleName() + ". " +
                    "Cipher padding not supported: " + encryptionConfig.getCipherName());
        }
        catch (Exception e)
        {
            throw new RuntimeException("Failed to initialize " + Encryptor.class.getSimpleName() + ": " + e.getClass().getName() + ": " + e.getMessage(), e);
        }
    }

    private StatefulEncryptor getEncryptor()
            throws InvalidAlgorithmParameterException, NoSuchAlgorithmException, InvalidKeyException, NoSuchPaddingException, KeyAccessException, KeyGenerationException
    {
        StatefulEncryptor encryptor = this.encryptor.get();
        if (encryptor == null)
        {
            encryptor = new StatefulEncryptor(encryptionConfig, random);
            this.encryptor.set(encryptor);
        }

        return encryptor;
    }

    private StatefulDecryptor getDecryptor()
            throws NoSuchAlgorithmException, NoSuchPaddingException
    {
        StatefulDecryptor decryptor = this.decryptor.get();
        if (decryptor == null)
        {
            decryptor = new StatefulDecryptor(encryptionConfig, random);
            this.decryptor.set(decryptor);
        }
        return decryptor;
    }

    /** Returns expected size of data after encryption */
    public int initialCompressedBufferLength(int chunkLength)
    {
        try
        {
            return getEncryptor().outputLength(chunkLength);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    /** Encrypts a block of data */
    public void compress(ByteBuffer input, ByteBuffer output) throws IOException
    {
        try
        {
            getEncryptor().encrypt(input, output);
        }
        catch (Exception e)
        {
            throw new IOException("Failed to encrypt data", e);
        }
    }

    /** Decrypts a block of data */
    public int uncompress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset) throws IOException
    {
        try
        {
            return getDecryptor().decrypt(input, inputOffset, inputLength, output, outputOffset);
        }
        catch (Exception e)
        {
            throw new IOException("Failed to decrypt data inputOffset=" + inputOffset + " inputLength=" + inputLength + " outputOffset=" + outputOffset, e);
        }
    }

    public void uncompress(ByteBuffer input, ByteBuffer output) throws IOException
    {
        try
        {
            getDecryptor().decrypt(input, output);
        }
        catch (Exception e)
        {
            throw new IOException("Failed to decrypt data", e);
        }
    }

    @Override
    public BufferType preferredBufferType()
    {
        // encrypt and decrypt operations happen on heap
        return BufferType.ON_HEAP;
    }

    @Override
    public boolean supports(BufferType bufferType)
    {
        return BufferType.ON_HEAP == bufferType;
    }

    public Set<String> supportedOptions()
    {
        return Sets.union(
                Sets.newHashSet(EncryptionConfig.CIPHER_ALGORITHM,
                        EncryptionConfig.SECRET_KEY_STRENGTH,
                        EncryptionConfig.IV_LENGTH,
                        EncryptionConfig.KEY_PROVIDER,
                        EncryptionConfig.SECRET_KEY_PROVIDER_FACTORY_CLASS),
                encryptionConfig.getKeyProviderFactory().supportedOptions());
    }

    public ICompressor encryptionOnly()
    {
        return this;
    }

    public boolean canDecompressInPlace()
    {
        // Encryptor stores metadata first, and then the encrypted data. In any case, the write position in the buffer
        // is earlier, or at most equal, to the read position, which means decryptors (being copy-safe) can work
        // correctly with the same buffer as both input and output.
        return true;
    }
}
