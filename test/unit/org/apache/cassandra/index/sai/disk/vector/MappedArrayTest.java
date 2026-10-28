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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MappedArrayTest
{
    private File intFile;
    private File longFile;

    @Before
    public void setUp() throws IOException
    {
        intFile = File.createTempFile("mapped_int_array", ".map");
        longFile = File.createTempFile("mapped_long_array", ".map");
    }

    @After
    public void tearDown()
    {
        intFile.delete();
        longFile.delete();
    }

    @Test
    public void unsetIntsReadAsDefault() throws IOException
    {
        for (int defaultValue : new int[]{ -1, 0, 0x12345678, Integer.MIN_VALUE })
        {
            try (MappedArray.OfInt array = new MappedArray.OfInt(intFile, 16, defaultValue))
            {
                for (int i = 0; i < 16; i++)
                    assertEquals(defaultValue, array.get(i));
            }
        }
    }

    @Test
    public void unsetLongsReadAsDefault() throws IOException
    {
        for (long defaultValue : new long[]{ -1L, 0L, 0x1234567890ABCDEFL, Long.MIN_VALUE })
        {
            try (MappedArray.OfLong array = new MappedArray.OfLong(longFile, 16, defaultValue))
            {
                for (int i = 0; i < 16; i++)
                    assertEquals(defaultValue, array.get(i));
            }
        }
    }

    @Test
    public void zeroInitialCapacityIsUsable() throws IOException
    {
        try (MappedArray.OfInt array = new MappedArray.OfInt(intFile, 0, -1))
        {
            array.put(0, 5);
            array.put(100, 7);
            assertEquals(5, array.get(0));
            assertEquals(7, array.get(100));
            assertEquals(-1, array.get(50));
        }
    }

    @Test
    public void intArrayGrowsPreservingValuesAndDefault() throws IOException
    {
        try (MappedArray.OfInt array = new MappedArray.OfInt(intFile, 4, 0x12345678))
        {
            array.put(0, 10);
            array.put(3, 13);
            // forces several doublings
            array.put(10_000, 42);

            assertEquals(10, array.get(0));
            assertEquals(13, array.get(3));
            assertEquals(42, array.get(10_000));
            assertEquals("gap stays default after growth", 0x12345678, array.get(4));
            assertEquals("gap stays default after growth", 0x12345678, array.get(9_999));
        }
    }

    @Test
    public void longArrayGrowsPreservingValuesAndDefault() throws IOException
    {
        try (MappedArray.OfLong array = new MappedArray.OfLong(longFile, 4, 0x1234567890ABCDEFL))
        {
            array.put(1, 1L << 40);
            array.put(10_000, Long.MAX_VALUE);

            assertEquals(1L << 40, array.get(1));
            assertEquals(Long.MAX_VALUE, array.get(10_000));
            assertEquals(0x1234567890ABCDEFL, array.get(0));
            assertEquals(0x1234567890ABCDEFL, array.get(9_999));
        }
    }

    @Test
    public void growthExtendsBackingFile() throws IOException
    {
        try (MappedArray.OfInt array = new MappedArray.OfInt(intFile, 4, -1))
        {
            assertEquals(4L * Integer.BYTES, intFile.length());
            array.put(1_000, 1);
            assertTrue(intFile.length() >= 1_001L * Integer.BYTES);
        }
    }

    @Test
    public void concurrentPutsWithGrowthLoseNothing() throws Exception
    {
        int threads = 8;
        int perThread = 20_000;
        try (MappedArray.OfInt array = new MappedArray.OfInt(intFile, 16, -1);
             ExecutorService pool = Executors.newFixedThreadPool(threads))
        {
            List<Future<?>> futures = new ArrayList<>();
            for (int t = 0; t < threads; t++)
            {
                final int offset = t;
                futures.add(pool.submit(() -> {
                    // interleaved indexes so all threads keep triggering growth
                    for (int i = 0; i < perThread; i++)
                    {
                        int index = i * threads + offset;
                        array.put(index, index);
                    }
                }));
            }
            for (Future<?> f : futures)
                f.get(60, TimeUnit.SECONDS);

            for (int index = 0; index < threads * perThread; index++)
                assertEquals(index, array.get(index));
        }
    }
}
