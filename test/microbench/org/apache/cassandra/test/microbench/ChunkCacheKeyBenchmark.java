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

package org.apache.cassandra.test.microbench;

import org.apache.cassandra.cache.ChunkCache;
import org.openjdk.jmh.annotations.*;

import java.util.concurrent.TimeUnit;
import java.util.Random;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 2, time = 1) // seconds
@Measurement(iterations = 5, time = 1) // seconds
@Fork(value = 1)
@State(Scope.Benchmark)
public class ChunkCacheKeyBenchmark
{
    private ChunkCache.Key[] keys;
    private int index;


    private static final int KEY_COUNT = 1 << 20;
    private static final int KEY_COUNT_MASK = KEY_COUNT - 1;

    @Setup(Level.Trial)
    public void setup()
    {
        keys = new ChunkCache.Key[KEY_COUNT];
        Random random = new Random(12345);
        for (int i = 0; i < KEY_COUNT; i++)
        {
            long readerId = random.nextLong();
            long position = random.nextLong();
            keys[i] = new ChunkCache.Key(readerId, position);
        }
        index = 0;
    }


    @Benchmark
    public int hashKey()
    {
        // Cycle through the array to avoid JIT constant folding
        ChunkCache.Key key = keys[index];
        index = (index + 1) & KEY_COUNT_MASK;
        return key.hashCode();
    }
}