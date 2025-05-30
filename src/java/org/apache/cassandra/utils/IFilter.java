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
package org.apache.cassandra.utils;


import org.apache.cassandra.utils.concurrent.SharedCloseable;

public interface IFilter extends SharedCloseable
{
    interface FilterKey
    {
        /** Places the murmur3 hash of the key in the first two elements of the given long array. */
        void filterHash(long[] dest);
        default short filterHashLowerBits()
        {
            long[] dest = new long[2];
            filterHash(dest);
            return (short) dest[1];
        }
    }

    void add(FilterKey key);

    boolean isPresent(FilterKey key);

    void clear();

    long serializedSize();

    void close();

    IFilter sharedCopy();

    /**
     * Returns the amount of memory in bytes used off heap.
     * @return the amount of memory in bytes used off heap
     */
    long offHeapSize();

    /**
     * This is used to avoid creating empty file for filters that do not support serialization
     *
     * @return true if current filter supports serialization to disk
     */
    boolean isSerializable();

    /**
     * @return serializer for current filter; or throw {@link UnsupportedOperationException} if filter is not serializable
     */
    IFilterSerializer getSerializer();
}
