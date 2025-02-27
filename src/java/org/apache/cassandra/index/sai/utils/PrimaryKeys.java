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
import java.util.concurrent.ConcurrentSkipListSet;

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
    private static final long SET_ENTRY_OVERHEAD = 36;

    private final ConcurrentSkipListSet<PrimaryKey> keys = new ConcurrentSkipListSet<>();

    /**
     * Adds the specified {@link PrimaryKey}.
     *
     * @param key a primary key
     * @return the bytes allocated for the key (0 if it already existed in the set)
     */
    public long add(PrimaryKey key)
    {
        return keys.add(key) ? SET_ENTRY_OVERHEAD : 0;
    }

    public void addAll(Iterable<PrimaryKey> keys)
    {
        for (var key : keys)
            add(key);
    }

    public SortedSet<PrimaryKey> keys()
    {
        return keys;
    }

    public int size()
    {
        return keys.size();
    }

    public boolean isEmpty()
    {
        return keys.isEmpty();
    }

    public long unsharedHeapSize()
    {
        return EMPTY_SIZE;
    }

    @Override
    public Iterator<PrimaryKey> iterator()
    {
        return keys.iterator();
    }
}
