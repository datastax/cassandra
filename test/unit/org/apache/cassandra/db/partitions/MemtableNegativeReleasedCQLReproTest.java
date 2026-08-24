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
package org.apache.cassandra.db.partitions;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.memtable.Memtable;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * CQL-driven regression test for CASSANDRA-21469 / HCD-534
 * ("MemtableReclaimMemory AssertionError: Negative released in MemtablePool$SubPool").
 * <p>
 * Root cause: {@link org.apache.cassandra.db.rows.BTreeRow}{@code .mergeRowBTrees} used
 * {@code Reconciler.retain} (instead of {@code removeShadowed}) on the UPDATE side when the existing
 * row's deletion shadowed incoming cells. {@code retain} calls {@code PostReconciliationFunction.delete}
 * which subtracts heap size from {@code owns}, but those incoming cells were never allocated to the
 * memtable (they only exist transiently during the write). Under overwrite/delete churn (e.g. repair
 * re-streaming old cells covered by newer deletions), {@code owns} drifted negative and the next flush
 * hit {@code AssertionError: Negative released}.
 * <p>
 * Test: Writes a row with a high-timestamp row deletion, then writes older-timestamp cells to trigger
 * the {@code deletion == existingDeletion} path in mergeRowBTrees. Checks all shard allocators' {@code
 * owns >= 0} via {@code Memtable.getMemoryUsage()} (NOT via {@code getAllocator().onHeap().owns()},
 * which only reads the top-level allocator that TrieMemtable never uses for writes). Forces TrieMemtable
 * explicitly because SkipListMemtable has a completely different merge path and is unaffected.
 */
public class MemtableNegativeReleasedCQLReproTest extends CQLTester
{
    /**
     * The buggy path fires when an incoming write carries cells with older timestamps than an existing
     * row deletion ({@code deletion == existingDeletion} in mergeRowBTrees). Write a row tombstone at
     * ts=5000, then repeatedly write cells at ts &lt; 5000. On buggy code each write subtracts the
     * incoming cells' heap size from {@code owns} even though that memory was never allocated to the
     * memtable, drifting {@code owns} negative. Flushing then crashes with "Negative released".
     */
    @Test
    public void oldCellsShadowedByRowDeletionKeepOwnsNonNegative() throws Throwable
    {
        // Force TrieMemtable — only it uses TriePartitionUpdater / BTreeRow.mergeRowBTrees.
        // SkipListMemtable does not hit this bug.
        createTable("CREATE TABLE %s (pk int, ck int, v int, PRIMARY KEY (pk, ck)) " +
                    "WITH memtable = 'trie'");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        // Establish a row tombstone at ts=5000.
        execute("DELETE FROM %s USING TIMESTAMP 5000 WHERE pk = 0 AND ck = 0");

        // Write cells at timestamps < 5000. Each write hits the mergeRowBTrees path where
        // deletion == existingDeletion (the existing row's deletion shadows the incoming cells).
        // On buggy code, retain() is called on the update side, subtracting the incoming cell's
        // heap size even though it was never owned by the memtable.
        for (int i = 0; i < 100; i++)
        {
            long ts = 1000 + (i * 30L); // always < 5000, always shadowed
            execute("UPDATE %s USING TIMESTAMP " + ts + " SET v = " + i + " WHERE pk = 0 AND ck = 0");

            // getMemoryUsage sums shard.allocator.onHeap().owns() across all shards, which is the
            // correct total for TrieMemtable. The top-level allocator (getAllocator()) is always 0
            // for TrieMemtable and must NOT be used here.
            long ownsOnHeap = Memtable.getMemoryUsage(cfs.getTracker().getView().getCurrentMemtable()).ownsOnHeap;
            assertThat(ownsOnHeap)
                .as("memtable on-heap owns went NEGATIVE (iteration %d) — the memtable reported " +
                    "releasing more on-heap memory than it allocated (CASSANDRA-21469 / HCD-534)", i)
                .isGreaterThanOrEqualTo(0L);
        }

        // The real crash path: flush runs discard() -> releaseAll() -> SubPool.released(owns).
        // If owns < 0, SubPool.released asserts "Negative released: <value>".
        flush();
    }
}
