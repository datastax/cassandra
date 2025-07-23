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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import org.apache.cassandra.crypto.IKeyProvider;
import org.apache.cassandra.crypto.IKeyProviderFactory;

import static org.apache.cassandra.io.compress.EncryptionConfig.CIPHER_ALGORITHM;
import static org.apache.cassandra.io.compress.EncryptionConfig.IV_LENGTH;
import static org.apache.cassandra.io.compress.EncryptionConfig.KEY_PROVIDER;
import static org.apache.cassandra.io.compress.EncryptionConfig.SECRET_KEY_STRENGTH;
import static org.junit.Assert.*;

public class EncryptionConfigTest
{
    @Test
    public void testEqualsAndHashCode()
    {
        Map<String, String> baseOptions = new HashMap<>();
        baseOptions.put(KEY_PROVIDER, TestKeyProviderFactory.class.getName());
        baseOptions.put(CIPHER_ALGORITHM, "AES/CBC/NoPadding");
        baseOptions.put(SECRET_KEY_STRENGTH, "256");
        EncryptionConfig baseConfig = configFor(baseOptions);

        assertEquals(baseConfig, configFor(baseOptions));
        assertEquals(baseConfig.hashCode(), configFor(baseOptions).hashCode());

        Map<String, String> noExplicitCipherOptions = new HashMap<>(baseOptions);
        noExplicitCipherOptions.remove(CIPHER_ALGORITHM);
        assertNotEquals(baseConfig, configFor(noExplicitCipherOptions));
        assertNotEquals(baseConfig.hashCode(), configFor(noExplicitCipherOptions).hashCode());

        Map<String, String> noSecretKeyOptions = new HashMap<>(baseOptions);
        noSecretKeyOptions.remove(SECRET_KEY_STRENGTH);
        assertNotEquals(baseConfig, configFor(noSecretKeyOptions));
        assertNotEquals(baseConfig.hashCode(), configFor(noSecretKeyOptions).hashCode());

        Map<String, String> explicitIvLengthOptions = new HashMap<>(baseOptions);
        explicitIvLengthOptions.put(IV_LENGTH, "24");
        assertNotEquals(baseConfig, configFor(explicitIvLengthOptions));
        assertNotEquals(baseConfig.hashCode(), configFor(explicitIvLengthOptions).hashCode());
    }

    private static EncryptionConfig configFor(Map<String, String> options)
    {
        return EncryptionConfig.forClass(Encryptor.class)
                               .fromCompressionOptions(options)
                               .build();
    }

    public static class TestKeyProviderFactory implements IKeyProviderFactory
    {
        @Override
        public IKeyProvider getKeyProvider(Map<String, String> options)
        {
            return null;
        }

        @Override
        public Set<String> supportedOptions()
        {
            return Collections.emptySet();
        }
    }
}
