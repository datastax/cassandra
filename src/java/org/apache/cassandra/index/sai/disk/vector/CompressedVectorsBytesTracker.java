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

import io.github.jbellis.jvector.quantization.MutableCompressedVectors;
import io.github.jbellis.jvector.quantization.MutablePQVectors;

/**
 * Tracks the memory used by a {@link MutableCompressedVectors} as vectors are added, without calling its
 * {@code ramBytesUsed()} for every vector. For {@link MutablePQVectors}, {@code ramBytesUsed()} walks every chunk of
 * compressed vectors, so calling it for every vector makes each insert slower as the segment grows.
 * <p>
 * The size of {@link MutablePQVectors} only changes when a chunk of {@link #VECTORS_PER_CHUNK} vectors is added, so the
 * size is measured again only when the last vector lands in a new chunk. If jvector's chunk size ever differs from
 * {@link #VECTORS_PER_CHUNK}, the tracked size is still the real size each time it is measured:
 * <ul>
 * <li>if jvector's chunks are smaller, the tracked size can be lower than the real size by the compressed size of
 * fewer than {@link #VECTORS_PER_CHUNK} vectors, until the next measurement;</li>
 * <li>if they are larger, the size is measured more often than needed.</li>
 * </ul>
 * {@code CompressedVectorsBytesTrackerTest} fails if jvector's chunk size changes.
 * <p>
 * Not thread-safe; the caller must serialize the changes to the vectors and the calls to this class.
 */
final class CompressedVectorsBytesTracker
{
    /** Number of vectors per chunk in jvector's {@link MutablePQVectors} ({@code VECTORS_PER_CHUNK}, which is private). */
    static final int VECTORS_PER_CHUNK = 1024;

    private final MutableCompressedVectors<?> vectors;
    private long bytesUsed;
    private int lastChunk;

    /**
     * Starts tracking the given vectors, measuring their current size.
     */
    CompressedVectorsBytesTracker(MutableCompressedVectors<?> vectors)
    {
        this.vectors = vectors;
        this.bytesUsed = vectors.ramBytesUsed();
        this.lastChunk = lastChunk(vectors.count());
    }

    /**
     * @return the size of the vectors at the last measurement
     */
    long bytesUsed()
    {
        return bytesUsed;
    }

    /**
     * Measures the size of the vectors again if the last vector is in a different chunk than at the last
     * measurement, which is when it can have changed.
     *
     * @return the growth of the size since the last measurement, or 0 if it was not measured again
     */
    long refreshBytesUsedDelta()
    {
        int chunk = lastChunk(vectors.count());
        if (chunk == lastChunk)
            return 0;
        lastChunk = chunk;
        long previousBytesUsed = bytesUsed;
        bytesUsed = vectors.ramBytesUsed();
        return bytesUsed - previousBytesUsed;
    }

    private static int lastChunk(int count)
    {
        return count == 0 ? -1 : (count - 1) / VECTORS_PER_CHUNK;
    }
}
