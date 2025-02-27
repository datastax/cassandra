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

package org.apache.cassandra.index.sai.disk.vector;

import io.github.jbellis.jvector.util.RamUsageEstimator;

public class RamEstimation
{
    /**
     * @param externalNodeCount the size() of the ConcurrentHashMap
     * @return an estimate of the number of bytes used
     */
    public static long concurrentHashMapRamUsed(int externalNodeCount) {
        long REF_BYTES = RamUsageEstimator.NUM_BYTES_OBJECT_REF;
        long AH_BYTES = RamUsageEstimator.NUM_BYTES_ARRAY_HEADER;
        long CORES = Runtime.getRuntime().availableProcessors();

        long chmNodeBytes =
        REF_BYTES // node itself in Node[]
        + 3L * REF_BYTES
        + Integer.BYTES; // node internals
        float chmLoadFactor = 0.75f; // this is hardcoded inside ConcurrentHashMap
        // CHM has a striped counter Cell implementation, we expect at most one per core
        long chmCounters = AH_BYTES + CORES * (REF_BYTES + Long.BYTES);

        double nodeCount = externalNodeCount / chmLoadFactor;

        return
        (long) nodeCount * (chmNodeBytes + REF_BYTES)// nodes
        + AH_BYTES // nodes array
        + Long.BYTES
        + 3 * Integer.BYTES
        + 3 * REF_BYTES // extra internal fields
        + chmCounters
        + REF_BYTES; // the Map reference itself
    }

    /**
     * @param elementCount the size() of the DenseIntMap
     * @return an estimate of the number of bytes used by a DenseIntMap
     */
    public static long denseIntMapRamUsed(int elementCount) {
        long REF_BYTES = RamUsageEstimator.NUM_BYTES_OBJECT_REF;
        long AH_BYTES = RamUsageEstimator.NUM_BYTES_ARRAY_HEADER;
        long RWLOCK_BYTES = RamUsageEstimator.NUM_BYTES_OBJECT_HEADER + 3 * REF_BYTES; // Approx. size for ReadWriteLock
        long ATOMIC_INT_BYTES = RamUsageEstimator.NUM_BYTES_OBJECT_HEADER + Integer.BYTES + REF_BYTES; // AtomicInteger overhead

        // Find power of 2 greater than or equal to elementCount
        int capacity = 1;
        while (capacity < elementCount) {
            capacity <<= 1;
        }

        // Calculate size for AtomicReferenceArray with for capacity elements
        long atomicRefArrayBytes = AH_BYTES + capacity * REF_BYTES;

        return RWLOCK_BYTES // Size of the ReadWriteLock object
               + ATOMIC_INT_BYTES // Size of the AtomicInteger
               + atomicRefArrayBytes; // Size of the AtomicReferenceArray structure
    }
}
