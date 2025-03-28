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
package org.apache.cassandra.index.sai.utils;

import java.util.Iterator;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentSkipListMap;

import com.google.common.collect.Iterators;

import org.apache.cassandra.index.sai.memory.MemoryIndex;
import org.apache.cassandra.utils.ObjectSizes;

/**
 * A sorted set of {@link PrimaryKey}s.
 *
 * The primary keys are sorted first by token, then by partition key value, and then by clustering.
 */
public class PrimaryKeys implements Iterable<MemoryIndex.PkWithFrequency>
{
    private static final long EMPTY_SIZE = ObjectSizes.measure(new PrimaryKeys());

    // from https://github.com/gaul/java-collection-overhead
    private static final long MAP_ENTRY_OVERHEAD = 40 + Integer.BYTES;

    private final ConcurrentSkipListMap<PrimaryKey, Integer> keys = new ConcurrentSkipListMap<>();

    /**
     * Adds the specified {@link PrimaryKey} incrementing its frequency.
     *
     * @param key a primary key
     * @return the bytes allocated for the key (0 if it already existed in the set)
     */
    public long addAndIncrementFrequency(PrimaryKey key)
    {
        return keys.compute(key, (k, v) -> v == null ? 1 : v + 1) == 1 ? MAP_ENTRY_OVERHEAD : 0;
    }

    /**
     * Adds the specified {@link PrimaryKey} resetting its frequency to 1.
     *
     * @param key a primary key
     * @return the bytes allocated for the key (0 if it already existed in the set)
     */
    public long addAndResetFrequency(PrimaryKey key)
    {
        Object prev = keys.put(key, 1);
        return prev == null ? MAP_ENTRY_OVERHEAD : 0;
    }

    /**
     * Removes the specified {@link PrimaryKey}.
     *
     * @param key the key to remove
     * @return
     */
    public long remove(PrimaryKey key)
    {
        return keys.remove(key) != null ? -MAP_ENTRY_OVERHEAD : 0;
    }

    public SortedSet<PrimaryKey> keys()
    {
        return keys.keySet();
    }

    public int size()
    {
        return keys.size();
    }

    public boolean isEmpty()
    {
        return keys.isEmpty();
    }

    public static long unsharedHeapSize()
    {
        return EMPTY_SIZE;
    }

    @Override
    public Iterator<MemoryIndex.PkWithFrequency> iterator()
    {
        return Iterators.transform(keys.entrySet().iterator(),
                                   entry -> new MemoryIndex.PkWithFrequency(entry.getKey(), entry.getValue()));
    }
}
