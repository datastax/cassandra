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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.BeforeClass;
import org.junit.Test;

import io.github.jbellis.jvector.graph.ListRandomAccessVectorValues;
import io.github.jbellis.jvector.quantization.MutablePQVectors;
import io.github.jbellis.jvector.quantization.ProductQuantization;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;

import static org.apache.cassandra.index.sai.disk.vector.CompressedVectorsBytesTracker.VECTORS_PER_CHUNK;
import static org.junit.Assert.assertEquals;

public class CompressedVectorsBytesTrackerTest
{
    private static final VectorTypeSupport vts = VectorizationProvider.getInstance().getVectorTypeSupport();
    private static final int DIMENSION = 16;
    private static final Random random = new Random(42);
    private static final List<VectorFloat<?>> vectors = new ArrayList<>();
    private static ProductQuantization pq;

    @BeforeClass
    public static void setup()
    {
        for (int i = 0; i < 1000; i++)
            vectors.add(randomVector());
        pq = ProductQuantization.compute(new ListRandomAccessVectorValues(vectors, DIMENSION), 4, 256, false);
    }

    /**
     * {@link CompressedVectorsBytesTracker} relies on the size of {@link MutablePQVectors} changing only when the
     * vector count passes a multiple of {@link CompressedVectorsBytesTracker#VECTORS_PER_CHUNK}. This fails if
     * jvector's private {@code MutablePQVectors.VECTORS_PER_CHUNK} no longer matches it.
     */
    @Test
    public void testSizeChangesOnlyAtChunkBoundaries()
    {
        MutablePQVectors pqVectors = new MutablePQVectors(pq);
        long previousBytesUsed = pqVectors.ramBytesUsed();
        int sizeChanges = 0;
        for (int ordinal = 0; ordinal < 5 * VECTORS_PER_CHUNK; ordinal++)
        {
            pqVectors.encodeAndSet(ordinal, vectors.get(ordinal % vectors.size()));
            long bytesUsed = pqVectors.ramBytesUsed();
            boolean firstInChunk = ordinal % VECTORS_PER_CHUNK == 0;
            assertEquals("size change after vector " + ordinal, firstInChunk, bytesUsed != previousBytesUsed);
            if (bytesUsed != previousBytesUsed)
                sizeChanges++;
            previousBytesUsed = bytesUsed;
        }
        assertEquals(5, sizeChanges);
    }

    /**
     * The tracked size matches {@code ramBytesUsed()} after every insert, including when gaps are filled with
     * {@code setZero}, as in {@link CompactionGraph#maybeAddVector}.
     */
    @Test
    public void testTrackedSizeMatchesRealSize()
    {
        MutablePQVectors pqVectors = new MutablePQVectors(pq);
        CompressedVectorsBytesTracker tracker = new CompressedVectorsBytesTracker(pqVectors);
        long initialBytesUsed = pqVectors.ramBytesUsed();
        assertEquals(initialBytesUsed, tracker.bytesUsed());

        long charged = 0;
        int ordinal = -1;
        for (int i = 0; i < 20_000; i++)
        {
            // usually the next ordinal, sometimes a gap that can span several chunks
            ordinal += random.nextInt(50) == 0 ? 2 + random.nextInt(3 * VECTORS_PER_CHUNK) : 1;
            while (pqVectors.count() < ordinal)
                pqVectors.setZero(pqVectors.count());
            pqVectors.encodeAndSet(ordinal, vectors.get(i % vectors.size()));

            charged += tracker.refreshBytesUsedDelta();
            assertEquals("after inserting ordinal " + ordinal, pqVectors.ramBytesUsed() - initialBytesUsed, charged);
            assertEquals(pqVectors.ramBytesUsed(), tracker.bytesUsed());
        }
    }

    /**
     * A tracker created for vectors that already hold vectors (as after the PQ refinement in {@link CompactionGraph})
     * starts from their current size and only charges what is added afterwards.
     */
    @Test
    public void testTrackerForExistingVectors()
    {
        MutablePQVectors pqVectors = new MutablePQVectors(pq);
        int count = 2 * VECTORS_PER_CHUNK + 100;
        for (int ordinal = 0; ordinal < count; ordinal++)
            pqVectors.encodeAndSet(ordinal, vectors.get(ordinal % vectors.size()));

        CompressedVectorsBytesTracker tracker = new CompressedVectorsBytesTracker(pqVectors);
        assertEquals(pqVectors.ramBytesUsed(), tracker.bytesUsed());

        long before = pqVectors.ramBytesUsed();
        long charged = 0;
        for (int ordinal = count; ordinal < 4 * VECTORS_PER_CHUNK; ordinal++)
        {
            pqVectors.encodeAndSet(ordinal, vectors.get(ordinal % vectors.size()));
            charged += tracker.refreshBytesUsedDelta();
        }
        assertEquals(pqVectors.ramBytesUsed() - before, charged);
    }

    private static VectorFloat<?> randomVector()
    {
        float[] values = new float[DIMENSION];
        for (int i = 0; i < DIMENSION; i++)
            values[i] = (float) random.nextGaussian();
        return vts.createFloatVector(values);
    }
}
