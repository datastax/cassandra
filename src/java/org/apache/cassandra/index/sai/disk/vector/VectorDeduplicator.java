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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.Objects;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;

import net.openhft.chronicle.hash.serialization.SizeMarshaller;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import net.openhft.chronicle.map.ExternalMapQueryContext;
import net.openhft.chronicle.map.MapAbsentEntry;

import org.apache.cassandra.io.util.FileUtils;

/**
 * Maps unique vectors to canonical {@link VectorHandle}s for the lifetime of a single graph merge.
 *
 * <p>A per-instance cryptographically random salt is generated at construction time so that hash
 * values are unpredictable across different compaction runs. The first {@link VectorHandle}
 * presented for a given vector becomes that vector's canonical handle; subsequent calls with the
 * same vector content return the already-recorded handle unchanged.
 *
 * <p>{@link #deduplicate} is safe to call concurrently from multiple threads.
 *
 * <p>The instance owns an off-heap {@link ChronicleMap} backed by a temporary file; call
 * {@link #close()} when the compaction segment is complete to release those resources.
 */
public final class VectorDeduplicator implements AutoCloseable
{
    private final byte[] salt;
    private final ChronicleMap<VectorHash, VectorHandle> map;
    /** Retained solely to delete the temp file on {@link #close()}; ChronicleMap does not delete its own backing file. */
    private final org.apache.cassandra.io.util.File backingFile;

    /**
     * Creates a new deduplicator with a fresh random salt and the given entry estimate.
     *
     * @param estimatedEntries approximate number of distinct vectors expected; used to size the map
     * @throws IOException if the temporary backing file cannot be created
     */
    public VectorDeduplicator(long estimatedEntries) throws IOException
    {
        this.salt = new byte[32];
        new SecureRandom().nextBytes(this.salt);

        long entries = Math.max(estimatedEntries, 1);
        this.backingFile = FileUtils.createTempFile("deduplicator_chronicle_map", ".map");
        this.map = ChronicleMapBuilder.of(VectorHash.class, VectorHandle.class)
                                      .averageKeySize(VectorHash.BYTES)
                                      .keySizeMarshaller(SizeMarshaller.constant(VectorHash.BYTES))
                                      .keyMarshaller(new VectorHash.Marshaller())
                                      .averageValueSize(VectorHandle.BYTES)
                                      .valueSizeMarshaller(SizeMarshaller.constant(VectorHandle.BYTES))
                                      .valueMarshaller(new VectorHandle.Marshaller())
                                      .entries(entries)
                                      .createPersistedTo(backingFile.toJavaIOFile());
    }

    /**
     * Computes the vector's salted hash and returns the canonical {@link VectorHandle}.
     *
     * <p>If the vector is first seen, {@code supplier} is called to produce the canonical handle which
     * is then recorded and returned. If an identical vector was seen previously, the existing canonical
     * handle is returned and {@code supplier} is <em>not</em> called.
     *
     * <p>Safe to call concurrently from multiple threads.
     *
     * @param vectorBuf the vector byte buffer; its position and limit are not modified
     * @param supplier  called at most once, only on a cache miss, to produce the canonical handle;
     *                  may return {@code null} to signal an anomaly, in which case nothing is inserted
     * @return the canonical handle for this vector (first-seen wins), or {@code null} if the supplier
     *         returned {@code null}
     */
    public VectorHandle deduplicate(ByteBuffer vectorBuf, Supplier<VectorHandle> supplier)
    {
        VectorHash hash = VectorHash.of(salt, vectorBuf);
        return computeIfAbsent(hash, supplier);
    }

    /**
     * Atomically returns the existing value for {@code key}, or inserts and returns the value produced
     * by {@code supplier} if the key is absent. The supplier is called at most once, under the write
     * lock, only on a miss. Returns {@code null} if the supplier returns {@code null}, in which case
     * nothing is inserted.
     */
    private VectorHandle computeIfAbsent(VectorHash key, Supplier<VectorHandle> supplier)
    {
        try (ExternalMapQueryContext<VectorHash, VectorHandle, ?> ctx = map.queryContext(key))
        {
            ctx.writeLock().lock();
            try
            {
                MapAbsentEntry<VectorHash, VectorHandle> absent = ctx.absentEntry();
                if (absent == null)
                    return Objects.requireNonNull(ctx.entry(), "absentEntry() was null but entry() is also null")
                                  .value()
                                  .get();
                VectorHandle value = supplier.get();
                if (value == null)
                    return null;
                absent.doInsert(ctx.wrapValueAsData(value));
                return value;
            }
            finally
            {
                ctx.writeLock().unlock();
            }
        }
    }

    /** Returns the temporary backing file owned by this instance; exposed for testing only. */
    @VisibleForTesting
    java.io.File backingFile()
    {
        return backingFile.toJavaIOFile();
    }

    @Override
    public void close() throws IOException
    {
        FileUtils.closeQuietly(map);
        backingFile.delete();
    }
}
