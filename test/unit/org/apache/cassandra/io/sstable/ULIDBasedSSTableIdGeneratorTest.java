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

package org.apache.cassandra.io.sstable;

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import de.huxhorn.sulky.ulid.ULID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for ULIDBasedSSTableId.Builder.generator() method focusing on the specific
 * retry logic and state transitions introduced in PR#2175.
 *<p>
 * Note: Basic generator functionality (uniqueness, sorting, concurrent access) is already
 * tested in SSTableIdTest.testULIDBasedIdProperties() and generatorFuzzTest().
 *<p>
 * These tests specifically focus on:
 * - The retry loop when nextStrictlyMonotonicValue() returns empty Optional
 * - State transitions from null to initialized
 * - concurrency generation that may trigger retry logic
 */
public class ULIDBasedSSTableIdGeneratorTest
{
    private AtomicReference<ULID.Value> prevRef;

    @Before
    public void setUp() throws Exception
    {
        Field prevRefField = ULIDBasedSSTableId.Builder.class.getDeclaredField("prevRef");
        prevRefField.setAccessible(true);
        prevRef = (AtomicReference<ULID.Value>) prevRefField.get(null);
        prevRef.set(null);
    }

    /**
     * Test initial state: when prevRef is null, generator should call ulid.nextValue()
     * and successfully set the prevRef.
     */
    @Test
    public void testGeneratorInitialState()
    {
        Supplier<ULIDBasedSSTableId> generator = ULIDBasedSSTableId.Builder.instance.generator(Stream.empty());

        ULIDBasedSSTableId id1 = generator.get();
        assertThat(id1).isNotNull();
        assertThat(prevRef.get()).isNotNull();
        assertThat(prevRef.get()).isEqualTo(id1.ulid);
    }

    /**
     * Test monotonic progression: subsequent calls should generate strictly monotonic ULIDs.
     */
    @Test
    public void testGeneratorMonotonicProgression()
    {
        Supplier<ULIDBasedSSTableId> generator = ULIDBasedSSTableId.Builder.instance.generator(Stream.empty());

        ULIDBasedSSTableId id1 = generator.get();
        ULIDBasedSSTableId id2 = generator.get();
        ULIDBasedSSTableId id3 = generator.get();

        assertThat(id1.compareTo(id2)).isLessThan(0);
        assertThat(id2.compareTo(id3)).isLessThan(0);
        assertThat(id1.compareTo(id3)).isLessThan(0);
    }

    @Test
    public void testGeneratorRetryOnEmptyOptional()
    {
        // Use a real ULID just to manufacture realistic ULID.Value instances
        ULID realUlid = new ULID();

        // Simulate an already existing previous value so that the generator
        // takes the nextStrictlyMonotonicValue(prevVal) branch.
        ULID.Value prevVal = realUlid.nextValue();
        prevRef.set(prevVal);

        // This is the value we expect to be used after the retry.
        ULID.Value nextVal = realUlid.nextValue();

        // Mock the ULID that will be injected into the Builder
        ULID mockUlid = Mockito.mock(ULID.class);

        // First call returns Optional.empty() → used to trigger the retry.
        // Second call returns nextVal → should be the value that generator uses.
        Mockito.when(mockUlid.nextStrictlyMonotonicValue(prevVal))
                        .thenReturn(Optional.empty(), Optional.of(nextVal));

        // Create a Builder that uses our mock ULID instead of the static one
        ULIDBasedSSTableId.Builder builder = new ULIDBasedSSTableId.Builder(mockUlid);
        Supplier<ULIDBasedSSTableId> generator = builder.generator(Stream.empty());

        // With the old loop condition (newVal != null && !CAS),
        // this would have exited after the Optional.empty() with newVal == null,
        // leading to a NPE in ULIDBasedSSTableId(newVal).
        ULIDBasedSSTableId id = generator.get();

        // Now we expect a successful retry and a non-null ULID
        assertThat(id).isNotNull();
        assertThat(id.ulid).isEqualTo(nextVal);
        assertThat(prevRef.get()).isEqualTo(nextVal);

        // Ensure we actually hit the retry path (two calls: empty, then present)
        Mockito.verify(mockUlid, Mockito.times(2)).nextStrictlyMonotonicValue(prevVal);
    }

    /**
     * Test the retry loop when nextStrictlyMonotonicValue
     *  by generating many IDs rapidly in a tight loop, which may
     * trigger the retry logic when the timestamp hasn't advanced enough for
     * strictly monotonic values.
     */
    @Test
    public void testGeneratorRetry()
    {
        Supplier<ULIDBasedSSTableId> generator = ULIDBasedSSTableId.Builder.instance.generator(Stream.empty());

        // Generate many IDs in rapid succession
        // This increases the likelihood of hitting the case where nextStrictlyMonotonicValue
        // returns empty because the timestamp hasn't advanced
        Set<ULIDBasedSSTableId> ids = new HashSet<>();
        for (int i = 0; i < 1000; i++)
        {
            ULIDBasedSSTableId id = generator.get();
            assertThat(id).isNotNull();
            assertThat(ids.add(id)).isTrue(); // Ensure all IDs are unique
        }

        assertThat(ids).hasSize(1000);

        // Verify monotonic ordering
        ULIDBasedSSTableId[] sortedIds = ids.toArray(new ULIDBasedSSTableId[0]);
        java.util.Arrays.sort(sortedIds);
        for (int i = 1; i < sortedIds.length; i++)
            assertThat(sortedIds[i - 1].compareTo(sortedIds[i])).isLessThan(0);
    }

    /**
     * Test concurrent access: multiple threads generating IDs simultaneously
     * should produce unique, monotonically increasing IDs without duplicates.
     */
    @Test
    public void testGeneratorConcurrentAccess() throws InterruptedException
    {
        final int NUM_THREADS = 20;
        final int IDS_PER_THREAD = 50;

        ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);
        CyclicBarrier barrier = new CyclicBarrier(NUM_THREADS);
        Set<ULIDBasedSSTableId> allIds = ConcurrentHashMap.newKeySet();
        CountDownLatch latch = new CountDownLatch(NUM_THREADS);

        Supplier<ULIDBasedSSTableId> generator = ULIDBasedSSTableId.Builder.instance.generator(Stream.empty());

        for (int i = 0; i < NUM_THREADS; i++)
        {
            executor.submit(() -> {
                try
                {
                    barrier.await(); // Synchronize start
                    for (int j = 0; j < IDS_PER_THREAD; j++)
                    {
                        ULIDBasedSSTableId id = generator.get();
                        assertThat(id).isNotNull();
                        allIds.add(id);
                    }
                }
                catch (InterruptedException | BrokenBarrierException e)
                {
                    throw new RuntimeException(e);
                }
                finally
                {
                    latch.countDown();
                }
            });
        }

        assertThat(latch.await(30, TimeUnit.SECONDS)).isTrue();
        executor.shutdown();

        // Verify all IDs are unique
        assertThat(allIds).hasSize(NUM_THREADS * IDS_PER_THREAD);

        // Verify monotonic ordering
        ULIDBasedSSTableId[] sortedIds = allIds.toArray(new ULIDBasedSSTableId[0]);
        java.util.Arrays.sort(sortedIds);

        for (int i = 1; i < sortedIds.length; i++)
            assertThat(sortedIds[i - 1].compareTo(sortedIds[i])).isLessThan(0);
    }

}
