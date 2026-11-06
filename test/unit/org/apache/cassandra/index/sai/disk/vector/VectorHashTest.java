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

package org.apache.cassandra.index.sai.disk.vector;

import java.nio.ByteBuffer;
import java.security.SecureRandom;

import org.junit.Test;

import net.openhft.chronicle.bytes.Bytes;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

public class VectorHashTest
{
    private static final SecureRandom random = new SecureRandom();

    @Test
    public void testOfSameSaltAndValue()
    {
        byte[] salt = new byte[32];
        random.nextBytes(salt);

        ByteBuffer buf = ByteBuffer.allocate(24);
        for (int i = 0; i < 24; i++)
        {
            buf.put((byte) i);
        }
        buf.flip();

        VectorHash hash1 = VectorHash.of(salt, buf);
        VectorHash hash2 = VectorHash.of(salt, buf);

        assertEquals(hash1, hash2);
        assertEquals(hash1.hashCode(), hash2.hashCode());
    }

    @Test
    public void testOfDifferentSaltSameValue()
    {
        byte[] salt1 = new byte[32];
        byte[] salt2 = new byte[32];
        random.nextBytes(salt1);
        random.nextBytes(salt2);

        ByteBuffer buf = ByteBuffer.allocate(24);
        for (int i = 0; i < 24; i++)
        {
            buf.put((byte) i);
        }
        buf.flip();

        VectorHash hash1 = VectorHash.of(salt1, buf);
        VectorHash hash2 = VectorHash.of(salt2, buf);

        assertNotEquals(hash1, hash2);
    }

    @Test
    public void testOfSameSaltDifferentValue()
    {
        byte[] salt = new byte[32];
        random.nextBytes(salt);

        ByteBuffer buf1 = ByteBuffer.allocate(24);
        ByteBuffer buf2 = ByteBuffer.allocate(24);
        for (int i = 0; i < 24; i++)
        {
            buf1.put((byte) i);
            buf2.put((byte) (i + 1));
        }
        buf1.flip();
        buf2.flip();

        VectorHash hash1 = VectorHash.of(salt, buf1);
        VectorHash hash2 = VectorHash.of(salt, buf2);

        assertNotEquals(hash1, hash2);
    }

    @Test
    public void testBufferSafety()
    {
        byte[] salt = new byte[32];
        random.nextBytes(salt);

        ByteBuffer buf = ByteBuffer.allocate(16);
        for (int i = 0; i < 16; i++)
        {
            buf.put((byte) i);
        }
        buf.flip();

        buf.position(4);
        buf.limit(12);

        int originalPosition = buf.position();
        int originalLimit = buf.limit();

        VectorHash hash = VectorHash.of(salt, buf);

        assertNotNull(hash);
        assertEquals(originalPosition, buf.position());
        assertEquals(originalLimit, buf.limit());
    }

    @Test
    public void testDirectAndHeapBuffers()
    {
        byte[] salt = new byte[32];
        random.nextBytes(salt);

        ByteBuffer heapBuf = ByteBuffer.allocate(16);
        ByteBuffer directBuf = ByteBuffer.allocateDirect(16);

        for (int i = 0; i < 16; i++)
        {
            heapBuf.put((byte) i);
            directBuf.put((byte) i);
        }

        heapBuf.flip();
        directBuf.flip();

        VectorHash hashHeap = VectorHash.of(salt, heapBuf);
        VectorHash hashDirect = VectorHash.of(salt, directBuf);

        assertEquals(hashHeap, hashDirect);
    }

    @Test
    public void testMarshallerRoundTrip()
    {
        VectorHash hash = new VectorHash(123456789L, -987654321L, 111111111L, -222222222L);
        VectorHash.Marshaller marshaller = new VectorHash.Marshaller();

        Bytes<?> bytes = Bytes.allocateElasticOnHeap();
        try
        {
            marshaller.write(bytes, hash);
            bytes.readPosition(0);
            VectorHash readHash = marshaller.read(bytes, null);
            assertEquals(hash, readHash);
        }
        finally
        {
            bytes.releaseLast();
        }
    }
}
