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

import org.apache.cassandra.utils.ObjectSizes;

/**
 * A sorted set of {@link PrimaryKey}s.
 *
 * The primary keys are sorted first by token, then by partition key value, and then by clustering.
 */
public class PrimaryKeys implements Iterable<PrimaryKey>
{
    private static final long EMPTY_SIZE = ObjectSizes.measure(new PrimaryKeys());

    // from https://github.com/gaul/java-collection-overhead
    private static final long MAP_ENTRY_OVERHEAD = 36;

    // We store a mapping from PrimaryKey to an Object to allow for put-first semantics with a subsequent remove
    // that only removes the key if the object reference is different from the one that was put. This is used
    // to simplify updates to collection columns and to analyzed text columns.
    private final ConcurrentSkipListMap<PrimaryKey, Object> keys = new ConcurrentSkipListMap<>();

    /**
     * Adds the specified {@link PrimaryKey}.
     *
     * @param key a primary key
     */
    public long put(PrimaryKey key, Object source)
    {
        // Store the latest reference associated with the key.
        return keys.put(key, source) == null ? MAP_ENTRY_OVERHEAD : 0;
    }

    /**
     * Removes the specified {@link PrimaryKey} only if the reference associated with the key is different from the
     * specified source.
     * @param key the key to remove
     * @param source the reference to compare against the current value associated with the key
     * @return
     */
    public long maybeRemove(PrimaryKey key, Object source)
    {
        // If the reference associated with the key is the one that added it, we want to keep the key.
        var result = keys.compute(key, (k, previousSource) -> previousSource == source ? previousSource : null);
        return result == null ? -MAP_ENTRY_OVERHEAD : 0;
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
    public Iterator<PrimaryKey> iterator()
    {
        return keys.keySet().iterator();
    }
}
