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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import org.apache.cassandra.crypto.IKeyProvider;
import org.apache.cassandra.crypto.IKeyProviderFactory;
import org.apache.cassandra.crypto.IMultiKeyProvider;
import org.apache.cassandra.crypto.KeyAccessException;

/**
 * A test {@link IMultiKeyProvider} that returns a brand new key from every {@link #writeHeader} call, so that every
 * chunk is encrypted with its own key, and a fixed "stale" key from {@link #getSecretKey} that no header ever names.
 * Any chunk encrypted with the key of {@link #getSecretKey} instead of the key named in its header is therefore
 * undecryptable, which makes a key mix-up between the two calls fail loudly.
 * <p>
 * The header is the 4-byte id of the key. Keys live in a static registry for the lifetime of the JVM and the counters
 * are never reset, so tests must compare counts as deltas.
 */
public class RotatingKeyProviderFactory implements IKeyProviderFactory
{
    /** Accepted as a compression option so that tests can build distinct encryptors from otherwise equal options. */
    public static final String TEST_ID = "rotating_test_id";

    private static final RotatingKeyProvider PROVIDER = new RotatingKeyProvider();

    public static RotatingKeyProvider provider()
    {
        return PROVIDER;
    }

    @Override
    public IKeyProvider getKeyProvider(Map<String, String> options)
    {
        return PROVIDER;
    }

    @Override
    public Set<String> supportedOptions()
    {
        return Collections.singleton(TEST_ID);
    }

    public static class RotatingKeyProvider implements IMultiKeyProvider
    {
        private static final int HEADER_LENGTH = Integer.BYTES;

        private final ConcurrentMap<Integer, SecretKey> keysById = new ConcurrentHashMap<>();
        private final AtomicInteger nextId = new AtomicInteger();
        private final AtomicInteger headersWritten = new AtomicInteger();
        private final Random random = new Random(7);

        /** A key that {@link #writeHeader} never returns. */
        @Override
        public SecretKey getSecretKey(String cipherName, int keyStrength)
        {
            byte[] bytes = new byte[keyStrength / 8];
            Arrays.fill(bytes, (byte) 0x5A);
            return new SecretKeySpec(bytes, algorithm(cipherName));
        }

        @Override
        public SecretKey writeHeader(String cipherName, int keyStrength, ByteBuffer output)
        {
            byte[] bytes = new byte[keyStrength / 8];
            synchronized (random)
            {
                random.nextBytes(bytes);
            }
            SecretKey key = new SecretKeySpec(bytes, algorithm(cipherName));
            int id = nextId.getAndIncrement();
            keysById.put(id, key);
            headersWritten.incrementAndGet();
            output.putInt(id);
            return key;
        }

        @Override
        public SecretKey readHeader(String cipherName, int keyStrength, ByteBuffer input) throws KeyAccessException
        {
            if (input.remaining() < HEADER_LENGTH)
                throw new KeyAccessException("Truncated key header: " + input.remaining() + " bytes");
            int id = input.getInt();
            SecretKey key = keysById.get(id);
            if (key == null)
                throw new KeyAccessException("Unknown key id " + id);
            return key;
        }

        @Override
        public int headerLength()
        {
            return HEADER_LENGTH;
        }

        public int headersWritten()
        {
            return headersWritten.get();
        }

        private static String algorithm(String cipherName)
        {
            return cipherName.replaceAll("/.*", "");
        }
    }
}
