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
import java.security.DigestException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.hash.serialization.BytesReader;
import net.openhft.chronicle.hash.serialization.BytesWriter;

/**
 * A compact, 256-bit cryptographic hash representation of a vector value,
 * salted with a per-compaction seed to avoid predictable collisions.
 *
 * @param l0 first 64 bits of the hash
 * @param l1 second 64 bits of the hash
 * @param l2 third 64 bits of the hash
 * @param l3 fourth 64 bits of the hash
 */
public record VectorHash(long l0, long l1, long l2, long l3)
{
    /** Serialized width of a VectorHash in the deduplicator map. */
    public static final int BYTES = 4 * Long.BYTES;

    private static class ThreadLocalState
    {
        final MessageDigest digest;
        final byte[] digestBuffer = new byte[BYTES];

        ThreadLocalState()
        {
            try
            {
                this.digest = MessageDigest.getInstance("SHA-256");
            }
            catch (NoSuchAlgorithmException e)
            {
                throw new RuntimeException("SHA-256 algorithm not available", e);
            }
        }
    }

    private static final ThreadLocal<ThreadLocalState> threadLocalState = ThreadLocal.withInitial(ThreadLocalState::new);

    /**
     * Computes a 256-bit salted SHA-256 VectorHash from the given salt and vector buffer.
     *
     * @param salt the per-compaction salt
     * @param vectorBuf the vector byte buffer
     * @return the resulting VectorHash
     */
    public static VectorHash of(byte[] salt, ByteBuffer vectorBuf)
    {
        ThreadLocalState state = threadLocalState.get();
        MessageDigest digest = state.digest;
        digest.reset();
        digest.update(salt);

        // Safe duplicate to avoid changing the position/limit of the original buffer
        ByteBuffer dup = vectorBuf.duplicate();
        digest.update(dup);

        try
        {
            digest.digest(state.digestBuffer, 0, BYTES);
        }
        catch (DigestException e)
        {
            throw new RuntimeException("Failed to compute SHA-256 digest into pre-allocated buffer", e);
        }

        long l0 = readLong(state.digestBuffer, 0);
        long l1 = readLong(state.digestBuffer, 8);
        long l2 = readLong(state.digestBuffer, 16);
        long l3 = readLong(state.digestBuffer, 24);

        return new VectorHash(l0, l1, l2, l3);
    }

    private static long readLong(byte[] bytes, int offset)
    {
        return ((long) bytes[offset] & 0xFF) << 56
             | ((long) bytes[offset + 1] & 0xFF) << 48
             | ((long) bytes[offset + 2] & 0xFF) << 40
             | ((long) bytes[offset + 3] & 0xFF) << 32
             | ((long) bytes[offset + 4] & 0xFF) << 24
             | ((long) bytes[offset + 5] & 0xFF) << 16
             | ((long) bytes[offset + 6] & 0xFF) << 8
             | ((long) bytes[offset + 7] & 0xFF);
    }

    /** Chronicle key marshaller; every VectorHash serializes to exactly {@link #BYTES} bytes. */
    public static class Marshaller implements BytesReader<VectorHash>, BytesWriter<VectorHash>
    {
        @Override
        public void write(Bytes out, VectorHash hash)
        {
            out.writeLong(hash.l0);
            out.writeLong(hash.l1);
            out.writeLong(hash.l2);
            out.writeLong(hash.l3);
        }

        @Override
        public VectorHash read(Bytes in, VectorHash using)
        {
            // records are immutable, so `using` cannot be reused
            return new VectorHash(in.readLong(), in.readLong(), in.readLong(), in.readLong());
        }
    }
}
