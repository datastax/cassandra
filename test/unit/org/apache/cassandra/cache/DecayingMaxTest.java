/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.cache;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class DecayingMaxTest
{
    @Test
    public void testConstructorRejectsFactorOutOfRange()
    {
        assertThrows(IllegalArgumentException.class, () -> new DecayingMax(0.0));
        assertThrows(IllegalArgumentException.class, () -> new DecayingMax(1.0));
        assertThrows(IllegalArgumentException.class, () -> new DecayingMax(-0.5));
        assertThrows(IllegalArgumentException.class, () -> new DecayingMax(1.5));
    }

    @Test
    public void testInitialValueIsZero()
    {
        assertEquals(0L, new DecayingMax(0.5).get());
    }

    @Test
    public void testHigherSampleWins()
    {
        DecayingMax decayingMax = new DecayingMax(0.5);
        decayingMax.update(100);
        assertEquals(100L, decayingMax.get());
        decayingMax.update(200);
        assertEquals(200L, decayingMax.get());
    }

    /**
     * Passing 0 repeatedly is a pure decay: the stored value just keeps getting multiplied by {@code factor}.
     */
    @Test
    public void testZeroSampleDecaysPreviousValue()
    {
        DecayingMax decayingMax = new DecayingMax(0.5);
        decayingMax.update(256);
        assertEquals(256L, decayingMax.get());

        decayingMax.update(0);
        assertEquals(128L, decayingMax.get());

        decayingMax.update(0);
        assertEquals(64L, decayingMax.get());

        decayingMax.update(0);
        assertEquals(32L, decayingMax.get());
    }

    /**
     * A lower sample than the decayed previous value does not pull the watermark down below the decay step;
     * it only "wins" once it exceeds the decayed value.
     */
    @Test
    public void testLowerSampleThanDecayedValueDoesNotWin()
    {
        DecayingMax decayingMax = new DecayingMax(0.5);
        decayingMax.update(256);
        // decayed previous value is 128; sample of 100 loses to it.
        decayingMax.update(100);
        assertEquals(128L, decayingMax.get());
    }

    @Test
    public void testSampleHigherThanDecayedValueWins()
    {
        DecayingMax decayingMax = new DecayingMax(0.5);
        decayingMax.update(256);
        // decayed previous value is 128; sample of 150 wins.
        decayingMax.update(150);
        assertEquals(150L, decayingMax.get());
    }

    @Test
    public void testDifferentFactor()
    {
        DecayingMax decayingMax = new DecayingMax(0.25);
        decayingMax.update(1000);
        decayingMax.update(0);
        assertEquals(250L, decayingMax.get());
        decayingMax.update(0);
        assertEquals(62L, decayingMax.get());
    }

    /**
     * Guards against the lost-update race this class is exposed to in production: {@code ChunkCache} calls
     * {@link DecayingMax#update(long)} concurrently from a grow path (reacting to pool exhaustion, potentially
     * many reader threads single-flighted down to one) and from the periodic shrink-tick scheduler thread.
     * With a plain non-atomic {@code value = Math.max(sample, (long) (value * factor))} read-modify-write, two
     * concurrent updates can race and one sample can be silently dropped. Repeatedly calling {@link
     * DecayingMax#update(long)} with the same ever-increasing sample from many threads must converge on exactly
     * that sample with no lost updates.
     */
    @Test
    public void testConcurrentUpdatesDoNotLoseSamples() throws Exception
    {
        DecayingMax decayingMax = new DecayingMax(0.5);
        int threads = 16;
        int updatesPerThread = 2000;
        AtomicLong maxSampleSeen = new AtomicLong();
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        try
        {
            for (int t = 0; t < threads; t++)
            {
                final int threadIdx = t;
                pool.submit(() -> {
                    try
                    {
                        start.await();
                    }
                    catch (InterruptedException e)
                    {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    for (int i = 0; i < updatesPerThread; i++)
                    {
                        long sample = (long) threadIdx * updatesPerThread + i;
                        maxSampleSeen.updateAndGet(prev -> Math.max(prev, sample));
                        decayingMax.update(sample);
                    }
                });
            }
            start.countDown();
            pool.shutdown();
            assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));
        }
        finally
        {
            pool.shutdownNow();
        }
        // The single largest sample submitted across all threads must never be lost to a concurrent racing
        // update, regardless of interleaving: the final value must be at least that sample (it can only be
        // larger if a still-higher sample happened to land last and hasn't decayed below it).
        assertTrue("largest concurrently-submitted sample must not be lost to a racing update",
                   decayingMax.get() >= maxSampleSeen.get());
    }
}
