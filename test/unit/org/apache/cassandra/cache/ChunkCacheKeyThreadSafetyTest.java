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

package org.apache.cassandra.cache;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Attempts to catch potential bugs in the implementation of ChunkCache.Key.hashCode() that could cause
 * it to return incorrect values when called concurrently on the same or different keys.
 * This test is not exhaustive and is not intended to be, but it should catch some common mistakes such
 * as accidentally sharing data buffers or sharing instances of library objects that are not meant to be shared
 * across threads (e.g. HashStreams).
 */
public class ChunkCacheKeyThreadSafetyTest
{
    @Test
    public void testCallingHashCodeOnDistinctKeysIsThreadSafe() throws InterruptedException, ExecutionException
    {
        final int numThreads = 4;
        final int count = 1_000_000;

        class Task implements Runnable
        {
            final Random rnd = new Random(0);
            final int[] hashes = new int[count];

            @Override
            public void run()
            {
                for (int i = 0; i < count; i++)
                {
                    long readerId = rnd.nextLong();
                    long position = rnd.nextLong();
                    ChunkCache.Key key = new ChunkCache.Key(readerId, position);
                    hashes[i] = key.hashCode();
                }
            }
        }

        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        try
        {
            List<Future<?>> futures = new ArrayList<>(numThreads);
            Task[] tasks = new Task[numThreads];

            for (int i = 0; i < numThreads; i++)
            {
                tasks[i] = new Task();
                futures.add(executor.submit(tasks[i]));
            }

            for (Future<?> future : futures)
                future.get(); // Wait for completion and propagate exceptions

            for (int i = 1; i < numThreads; i++)
                assertArrayEquals(tasks[0].hashes, tasks[i].hashes);
        }
        finally
        {
            executor.shutdown();
        }
    }

    @Test
    public void testCallingHashCodeOnSameKeyIsThreadSafe() throws InterruptedException, ExecutionException
    {
        final int numThreads = 4;
        final int callsPerThread = 100_000_000;
        final ChunkCache.Key key = new ChunkCache.Key(123L, 456L);
        final int expectedHashCode = key.hashCode();

        class Task implements Runnable
        {
            @Override
            public void run()
            {
                for (int i = 0; i < callsPerThread; i++)
                    assertEquals("hashCode is not stable under concurrent access", expectedHashCode, key.hashCode());
            }
        }

        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        try
        {
            List<Future<?>> futures = new ArrayList<>(numThreads);
            for (int i = 0; i < numThreads; i++)
                futures.add(executor.submit(new Task()));
            for (Future<?> future : futures)
                future.get(); // This will re-throw any exception from the worker thread
        }
        finally
        {
            executor.shutdown();
        }
    }
}
