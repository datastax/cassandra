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
import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.config.OptionMap;
import org.apache.cassandra.crypto.IKeyProvider;
import org.apache.cassandra.crypto.IKeyProviderFactory;

public class EncryptionConfig
{
    public static final String CIPHER_ALGORITHM = "cipher_algorithm";
    public static final String SECRET_KEY_STRENGTH = "secret_key_strength";
    public static final String SECRET_KEY_PROVIDER_FACTORY_CLASS = "secret_key_provider_factory_class";
    public static final String KEY_PROVIDER = "key_provider";
    public static final String IV_LENGTH = "iv_length";

    private static final String DEFAULT_CIPHER_TRANSFORMATION = "AES/CBC/PKCS5Padding";
    private static final int DEFAULT_SECRET_KEY_STRENGTH = 128;

    private final IKeyProviderFactory keyProviderFactory;
    private final IKeyProvider keyProvider;
    private final String cipherName;
    private final int keyStrength;
    private final boolean ivEnabled;
    private final int ivLength;
    private final ImmutableMap<String, String> encryptionOptions;

    public static Builder forClass(Class<?> callerClass)
    {
        return new Builder(callerClass);
    }

    private EncryptionConfig(IKeyProviderFactory keyProviderFactory, IKeyProvider keyProvider,
                             String cipherName, int keyStrength, boolean ivEnabled, int ivLength,
                             Map<String, String> encryptionOptions)
    {
        this.keyProviderFactory = keyProviderFactory;
        this.keyProvider = keyProvider;
        this.cipherName = cipherName;
        this.keyStrength = keyStrength;
        this.ivEnabled = ivEnabled;
        this.ivLength = ivLength;
        this.encryptionOptions = ImmutableMap.copyOf(encryptionOptions);
    }

    public IKeyProviderFactory getKeyProviderFactory()
    {
        return keyProviderFactory;
    }

    public IKeyProvider getKeyProvider()
    {
        return keyProvider;
    }

    public int getKeyStrength()
    {
        return keyStrength;
    }

    public String getCipherName()
    {
        return cipherName;
    }

    public boolean isIvEnabled()
    {
        return ivEnabled;
    }

    public int getIvLength()
    {
        return ivLength;
    }

    public Map<String, String> asMap()
    {
        return encryptionOptions;
    }

    public static class Builder
    {
        private final Class<?> callerClass;
        private final Map<String, String> compressionOptions = new HashMap<>();

        private Builder(Class<?> callerClass)
        {
            this.callerClass = callerClass;
        }

        public Builder fromCompressionOptions(Map<String, String> compressionOptions)
        {
            this.compressionOptions.putAll(compressionOptions);
            return this;
        }

        public EncryptionConfig build()
        {
            OptionMap optionMap = new OptionMap(compressionOptions);
            String cipherName = optionMap.get(CIPHER_ALGORITHM, DEFAULT_CIPHER_TRANSFORMATION);
            int keyStrength = optionMap.get(SECRET_KEY_STRENGTH, DEFAULT_SECRET_KEY_STRENGTH);
            int userIvLength = optionMap.get(IV_LENGTH, -1);
            boolean ivEnabled = cipherName.matches(".*/(CBC|CFB|OFB|PCBC)/.*");
            int ivLength = !ivEnabled
                    ? 0
                    : userIvLength > 0
                    ? userIvLength
                    : getIvLength(cipherName.replaceAll("/.*", ""));

            try
            {
                Class<?> keyProviderFactoryClass = getKeyFactory(compressionOptions);
                IKeyProviderFactory keyProviderFactory = (IKeyProviderFactory) keyProviderFactoryClass.newInstance();
                IKeyProvider keyProvider = keyProviderFactory.getKeyProvider(compressionOptions);
                return new EncryptionConfig(keyProviderFactory, keyProvider, cipherName, keyStrength,
                        ivEnabled, ivLength, compressionOptions);
            }
            catch (InstantiationException | IllegalAccessException | ClassNotFoundException | IOException e)
            {
                throw new RuntimeException("Failed to initialize " + callerClass.getSimpleName() + ": " + e.getMessage(), e);
            }
        }

        private Class<?> getKeyFactory(Map<String, String> options) throws ClassNotFoundException
        {
            String className;
            if (options.containsKey(KEY_PROVIDER))
            {
                className = options.get(KEY_PROVIDER);
            }
            else if (options.containsKey(SECRET_KEY_PROVIDER_FACTORY_CLASS))
            {
                // for backwards compatibility
                className = options.get(SECRET_KEY_PROVIDER_FACTORY_CLASS);
            }
            else
            {
                className = "LocalFileSystemKeyProvider";
            }

            if (!className.contains("."))
            {
                className = "org.apache.cassandra.crypto." + className;
            }

            return Class.forName(className);
        }

        private int getIvLength(String algorithm)
        {
            return algorithm.equals("AES") ? 16 : 8;
        }
    }

    @Override
    public String toString()
    {
        return "EncryptionConfig{" +
                "cipher name: " + cipherName + ", " +
                "key strength: " + keyStrength + ", " +
                "options" + encryptionOptions.toString() + "}";
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;
        if (!(obj instanceof EncryptionConfig))
            return false;
        EncryptionConfig other = (EncryptionConfig) obj;
        // All EncryptionConfig options are actually based on the passed compression/encryption options map.
        return encryptionOptions.equals(other.encryptionOptions);
    }

    @Override
    public int hashCode()
    {
        // All EncryptionConfig options are actually based on the passed compression/encryption options map.
        return encryptionOptions.hashCode();
    }
}
