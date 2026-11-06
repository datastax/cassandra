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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class VectorDeduplicatorTest
{
    private VectorDeduplicator deduplicator;

    @Before
    public void setUp() throws IOException
    {
        deduplicator = new VectorDeduplicator(100);
    }

    @After
    public void tearDown() throws IOException
    {
        if (deduplicator != null)
            deduplicator.close();
    }

    // -----------------------------------------------------------------------
    // Helper
    // -----------------------------------------------------------------------

    private static ByteBuffer vectorOf(float... values)
    {
        ByteBuffer buf = ByteBuffer.allocate(values.length * Float.BYTES);
        for (float v : values)
            buf.putFloat(v);
        buf.flip();
        return buf;
    }

    private static ByteBuffer randomVector(Random rnd, int dim)
    {
        ByteBuffer buf = ByteBuffer.allocate(dim * Float.BYTES);
        for (int i = 0; i < dim; i++)
            buf.putFloat(rnd.nextFloat());
        buf.flip();
        return buf;
    }

    // -----------------------------------------------------------------------
    // First-seen canonical handle selection
    // -----------------------------------------------------------------------

    @Test
    public void firstCallReturnsCandidateHandle() throws IOException
    {
        ByteBuffer vec = vectorOf(1.0f, 2.0f, 3.0f);
        VectorHandle candidate = new VectorHandle(0, 42);

        VectorHandle canonical = deduplicator.deduplicate(vec, () -> candidate);

        assertEquals(candidate, canonical);
    }

    @Test
    public void secondCallWithSameVectorReturnsCachedHandle() throws IOException
    {
        ByteBuffer vec = vectorOf(1.0f, 2.0f, 3.0f);
        VectorHandle first = new VectorHandle(0, 1);
        VectorHandle second = new VectorHandle(1, 99);

        VectorHandle canonical1 = deduplicator.deduplicate(vec, () -> first);
        VectorHandle canonical2 = deduplicator.deduplicate(vec, () -> second);

        assertEquals(first, canonical1);
        assertEquals("duplicate vector must map to the first-seen handle", first, canonical2);
    }

    @Test
    public void supplierNotCalledOnCacheHit()
    {
        ByteBuffer vec = vectorOf(1.0f, 2.0f, 3.0f);
        VectorHandle first = new VectorHandle(0, 1);
        AtomicInteger supplierCalls = new AtomicInteger();

        deduplicator.deduplicate(vec, () -> { supplierCalls.incrementAndGet(); return first; });
        deduplicator.deduplicate(vec, () -> { supplierCalls.incrementAndGet(); return new VectorHandle(1, 99); });
        deduplicator.deduplicate(vec, () -> { supplierCalls.incrementAndGet(); return new VectorHandle(2, 0); });

        assertEquals("supplier must be called exactly once (on the initial miss)", 1, supplierCalls.get());
    }

    @Test
    public void nullSupplierResultReturnsNullAndInsertsNothing()
    {
        ByteBuffer vec = vectorOf(7.0f, 8.0f);

        VectorHandle result = deduplicator.deduplicate(vec, () -> null);

        assertNull("null from supplier must be returned as-is", result);

        // A subsequent call with a real handle must now insert (vector was never recorded).
        VectorHandle handle = new VectorHandle(0, 5);
        VectorHandle canonical = deduplicator.deduplicate(vec, () -> handle);
        assertEquals("vector must be insertable after a previous null-supplier miss", handle, canonical);
    }

    @Test
    public void distinctVectorsReturnDistinctHandles() throws IOException
    {
        ByteBuffer vec1 = vectorOf(1.0f, 0.0f);
        ByteBuffer vec2 = vectorOf(0.0f, 1.0f);
        VectorHandle handle1 = new VectorHandle(0, 0);
        VectorHandle handle2 = new VectorHandle(0, 1);

        VectorHandle canonical1 = deduplicator.deduplicate(vec1, () -> handle1);
        VectorHandle canonical2 = deduplicator.deduplicate(vec2, () -> handle2);

        assertEquals(handle1, canonical1);
        assertEquals(handle2, canonical2);
    }

    // -----------------------------------------------------------------------
    // Duplicate mapping across multiple sources and nodes
    // -----------------------------------------------------------------------

    @Test
    public void deduplicatesAcrossMultipleSources() throws IOException
    {
        ByteBuffer vec = vectorOf(3.14f, 2.72f, 1.41f);

        // Four handles from three different sources all present the same vector
        VectorHandle src0n5  = new VectorHandle(0, 5);
        VectorHandle src1n10 = new VectorHandle(1, 10);
        VectorHandle src2n0  = new VectorHandle(2, 0);
        VectorHandle src0n7  = new VectorHandle(0, 7);

        VectorHandle c0 = deduplicator.deduplicate(vec, () -> src0n5);
        VectorHandle c1 = deduplicator.deduplicate(vec, () -> src1n10);
        VectorHandle c2 = deduplicator.deduplicate(vec, () -> src2n0);
        VectorHandle c3 = deduplicator.deduplicate(vec, () -> src0n7);

        assertEquals("first call must be canonical", src0n5, c0);
        assertEquals("second call must return first-seen canonical", src0n5, c1);
        assertEquals("third call must return first-seen canonical", src0n5, c2);
        assertEquals("fourth call must return first-seen canonical", src0n5, c3);
    }

    @Test
    public void eachUniqueVectorHasItsOwnCanonical() throws IOException
    {
        // Three distinct vectors, each seen twice with different handles
        ByteBuffer vecA = vectorOf(1.0f);
        ByteBuffer vecB = vectorOf(2.0f);
        ByteBuffer vecC = vectorOf(3.0f);

        VectorHandle hA1 = new VectorHandle(0, 0);
        VectorHandle hA2 = new VectorHandle(1, 0);
        VectorHandle hB1 = new VectorHandle(0, 1);
        VectorHandle hB2 = new VectorHandle(1, 1);
        VectorHandle hC1 = new VectorHandle(0, 2);
        VectorHandle hC2 = new VectorHandle(1, 2);

        assertEquals(hA1, deduplicator.deduplicate(vecA, () -> hA1));
        assertEquals(hB1, deduplicator.deduplicate(vecB, () -> hB1));
        assertEquals(hC1, deduplicator.deduplicate(vecC, () -> hC1));

        // Duplicate presentations must all map to their own canonical
        assertEquals(hA1, deduplicator.deduplicate(vecA, () -> hA2));
        assertEquals(hB1, deduplicator.deduplicate(vecB, () -> hB2));
        assertEquals(hC1, deduplicator.deduplicate(vecC, () -> hC2));
    }

    // -----------------------------------------------------------------------
    // ByteBuffer position/limit safety
    // -----------------------------------------------------------------------

    @Test
    public void deduplicateDoesNotAlterBufferPositionOrLimit()
    {
        ByteBuffer buf = ByteBuffer.allocate(16);
        for (int i = 0; i < 4; i++)
            buf.putFloat(i * 0.1f);
        buf.flip();
        buf.position(4);
        buf.limit(12);

        int originalPosition = buf.position();
        int originalLimit = buf.limit();

        deduplicator.deduplicate(buf, () -> new VectorHandle(0, 0));

        assertEquals(originalPosition, buf.position());
        assertEquals(originalLimit, buf.limit());
    }


    // -----------------------------------------------------------------------
    // Thread-safety / concurrency
    // -----------------------------------------------------------------------

    @Test
    public void concurrentDeduplicationReturnsConsistentCanonical() throws Exception
    {
        int threads = 8;
        int vectorsPerThread = 10000;

        // All threads will present the SAME vector with different handles.
        ByteBuffer sharedVec = vectorOf(42.0f, 43.0f, 44.0f);
        CyclicBarrier startBarrier = new CyclicBarrier(threads);

        List<Future<VectorHandle>> futures = new ArrayList<>(threads * vectorsPerThread);
        try (ExecutorService pool = Executors.newFixedThreadPool(threads))
        {
            for (int t = 0; t < threads; t++)
            {
                int threadIdx = t;
                for (int i = 0; i < vectorsPerThread; i++)
                {
                    int vectorIdx = i;
                    futures.add(pool.submit(() -> {
                        startBarrier.await();
                        // Each thread/vector combination has a unique candidate handle
                        VectorHandle candidate = new VectorHandle(threadIdx, vectorIdx);
                        // Make a thread-local duplicate of the shared buffer
                        ByteBuffer local = sharedVec.duplicate();
                        return deduplicator.deduplicate(local, () -> candidate);
                    }));
                }
            }

            pool.shutdown();
            assertTrue("pool failed to terminate within timeout", pool.awaitTermination(30, TimeUnit.SECONDS));
        }

        // Collect all results — every future must complete without exception
        VectorHandle first = null;
        for (Future<VectorHandle> f : futures)
        {
            VectorHandle result = f.get();
            assertNotNull(result);
            if (first == null)
                first = result;
            else
                assertEquals("all concurrent calls must resolve to the same canonical handle", first, result);
        }
    }

    @Test
    public void concurrentDistinctVectorsEachGetOwnCanonical() throws Exception
    {
        int threads = 4;
        CyclicBarrier startBarrier = new CyclicBarrier(threads);

        // Each thread presents a unique vector twice with different handles; both calls must return
        // the same canonical that was first established for that vector.
        List<Future<VectorHandle[]>> futures = new ArrayList<>(threads);
        try (ExecutorService pool = Executors.newFixedThreadPool(threads))
        {
            for (int t = 0; t < threads; t++)
            {
                int threadIdx = t;
                futures.add(pool.submit(() -> {
                    startBarrier.await();
                    ByteBuffer vec = vectorOf(threadIdx * 100.0f, threadIdx * 100.0f + 1.0f);
                    VectorHandle h1 = new VectorHandle(threadIdx, 0);
                    VectorHandle h2 = new VectorHandle(threadIdx, 1);
                    VectorHandle c1 = deduplicator.deduplicate(vec, () -> h1);
                    VectorHandle c2 = deduplicator.deduplicate(vec, () -> h2);
                    return new VectorHandle[]{ c1, c2 };
                }));
            }

            pool.shutdown();
            assertTrue("pool failed to terminate within timeout", pool.awaitTermination(30, TimeUnit.SECONDS));
        }

        for (Future<VectorHandle[]> f : futures)
        {
            VectorHandle[] pair = f.get();
            assertEquals("both calls for the same unique vector must agree on a canonical", pair[0], pair[1]);
        }
    }

    // -----------------------------------------------------------------------
    // Scale tests: Larger number of vectors
    // -----------------------------------------------------------------------

    @Test
    public void manyUniqueRandomVectors() throws IOException
    {
        int count = 100_000;
        int dim = 32;
        Random rnd = new Random(42);

        try (VectorDeduplicator d = new VectorDeduplicator(count))
        {
            for (int i = 0; i < count; i++)
            {
                ByteBuffer vec = randomVector(rnd, dim);
                VectorHandle candidate = new VectorHandle(i % 4, i);
                VectorHandle canonical = d.deduplicate(vec, () -> candidate);

                assertEquals("each unique vector must return its own candidate handle", candidate, canonical);
            }
        }
    }

    @Test
    public void manyVectorsWithHeavyDuplicates() throws IOException
    {
        int distinctCount = 1_000;
        int totalInserts = 100_000;
        int dim = 32;
        Random rnd = new Random(12345);

        // Pre-generate distinct vectors
        List<ByteBuffer> distinctVectors = new ArrayList<>(distinctCount);
        for (int i = 0; i < distinctCount; i++)
            distinctVectors.add(randomVector(rnd, dim));

        try (VectorDeduplicator d = new VectorDeduplicator(distinctCount))
        {
            Map<Integer, VectorHandle> expectedCanonicals = new HashMap<>();
            Set<VectorHandle> observedCanonicals = new HashSet<>();

            for (int i = 0; i < totalInserts; i++)
            {
                int vecIdx = rnd.nextInt(distinctCount);
                ByteBuffer vec = distinctVectors.get(vecIdx);
                VectorHandle candidate = new VectorHandle(i % 8, i);

                VectorHandle canonical = d.deduplicate(vec, () -> candidate);
                assertNotNull(canonical);

                if (!expectedCanonicals.containsKey(vecIdx))
                {
                    // First time this distinct vector is presented
                    expectedCanonicals.put(vecIdx, candidate);
                    assertEquals("first presentation must establish candidate as canonical", candidate, canonical);
                }
                else
                {
                    // Subsequent presentations must return the first-seen canonical handle
                    assertEquals("duplicate presentation must return the established canonical",
                                 expectedCanonicals.get(vecIdx), canonical);
                }
                observedCanonicals.add(canonical);
            }

            assertEquals("exactly " + distinctCount + " distinct canonical handles must exist across 10,000 inserts",
                         distinctCount, observedCanonicals.size());
        }
    }

    @Test
    public void tenThousandConcurrentDeduplicationsWithDuplicates() throws Exception
    {
        int distinctCount = 1_000;
        int threads = 8;
        int opsPerThread = 1_250; // total 10,000 ops
        int dim = 64;
        Random rnd = new Random(999);

        List<ByteBuffer> distinctVectors = new ArrayList<>(distinctCount);
        for (int i = 0; i < distinctCount; i++)
            distinctVectors.add(randomVector(rnd, dim));

        try (VectorDeduplicator d = new VectorDeduplicator(distinctCount))
        {
            ConcurrentHashMap<Integer, VectorHandle> establishedCanonicals = new ConcurrentHashMap<>();
            CyclicBarrier barrier = new CyclicBarrier(threads);

            List<Future<Void>> futures = new ArrayList<>(threads);
            try (ExecutorService pool = Executors.newFixedThreadPool(threads))
            {
                for (int t = 0; t < threads; t++)
                {
                    int threadIdx = t;
                    futures.add(pool.submit(() -> {
                        barrier.await();
                        Random threadRnd = new Random(threadIdx * 1000L + 7);
                        for (int op = 0; op < opsPerThread; op++)
                        {
                            int vecIdx = threadRnd.nextInt(distinctCount);
                            ByteBuffer vec = distinctVectors.get(vecIdx);
                            VectorHandle candidate = new VectorHandle(threadIdx, op);

                            VectorHandle canonical = d.deduplicate(vec, () -> candidate);
                            assertNotNull(canonical);

                            // The first handle recorded for vecIdx in our map must match whatever other threads observe
                            VectorHandle prior = establishedCanonicals.putIfAbsent(vecIdx, canonical);
                            if (prior != null)
                            {
                                assertEquals("all concurrent threads must observe the same canonical handle for vector " + vecIdx,
                                             prior, canonical);
                            }
                        }
                        return null;
                    }));
                }

                pool.shutdown();
                assertTrue("pool failed to terminate within timeout", pool.awaitTermination(30, TimeUnit.SECONDS));
            }

            for (Future<Void> f : futures)
                f.get();

            assertEquals("all 1,000 distinct vectors must have established a canonical handle",
                         distinctCount, establishedCanonicals.size());
        }
    }

    // -----------------------------------------------------------------------
    // Resource cleanup on close
    // -----------------------------------------------------------------------

    @Test
    public void closeDeletesToBackingFile() throws Exception
    {
        VectorDeduplicator d = new VectorDeduplicator(10);
        java.io.File file = d.backingFile();
        org.junit.Assert.assertTrue("backing file must exist while open", file.exists());

        ByteBuffer vec = vectorOf(0.0f);
        d.deduplicate(vec, () -> new VectorHandle(0, 0));

        d.close();
        org.junit.Assert.assertFalse("backing file must be deleted on close", file.exists());
        deduplicator = null;
    }

    @Test
    public void closingEmptyDeduplicatorDoesNotThrow() throws Exception
    {
        VectorDeduplicator empty = new VectorDeduplicator(10);
        java.io.File file = empty.backingFile();
        org.junit.Assert.assertTrue("backing file must exist while open", file.exists());

        empty.close();
        org.junit.Assert.assertFalse("backing file must be deleted on close", file.exists());
        deduplicator = null;
    }
}
