/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.utils.memory;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.util.concurrent.ListenableFuture;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.memtable.AbstractAllocatorMemtable;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.memtable.ShardBoundaries;
import org.apache.cassandra.db.memtable.TrieMemtable;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.concurrent.OpOrder;

import static org.junit.Assert.fail;

/**
 * Regression test for the TrieMemtable shard-lock / flush-barrier deadlock.
 *
 * Will print the deadlocked stacks when TrieMemtable blocks on pool allocation while holding a shard
 * writeLock (or the barrier can otherwise complete past lock-queued pre-barrier ops).
 *
 * Cycle reproduced: the barrier waits on pre-barrier op W1; W1 queues on the (single)
 * MemtableShard writeLock; the lock is held by post-barrier op W2, parked in
 * SubAllocator.allocate (isBlocking == false, correctly); memory is freed only by the
 * flush; the flush waits on the barrier. markBlocking() releases only allocator waiters,
 * never lock waiters, so the barrier cannot complete.
 *
 * The flush machinery is replaced by the three calls ColumnFamilyStore.Flush.run makes
 * (issue, markBlocking, await); pool exhaustion is synthetic (claim the whole SubPool
 * limit, restored in a finally so the shared pool is clean for other tests).
 * The setup checkpoints tolerate a fixed build: if W1/W2 complete instead of wedging,
 * the test proceeds to the barrier join and goes green.
 */
public class TrieMemtableShardLockDeadlockTest
{
    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.daemonInitialization();
    }


    /** put() touches none of this beyond construction; single shard, no scheduled flush. */
    private static final OpOrder READ_ORDER = new OpOrder();
    
    private static final Memtable.Owner OWNER = new Memtable.Owner()
    {
        public ListenableFuture<CommitLogPosition> signalFlushRequired(Memtable m, ColumnFamilyStore.FlushReason r) { return null; }
        public Memtable getCurrentMemtable() { return null; }
        public Iterable<Memtable> getIndexMemtables() { return Collections.emptyList(); }
        public ShardBoundaries localRangeSplits(int shardCount) { return ShardBoundaries.NONE; }
        public OpOrder readOrdering() { return READ_ORDER; }
        public int getMemtableFlushPeriodInMs() { return 0; }
    };

    @Test(timeout = 60_000)
    public void writeBarrierMustCompleteDespiteShardLockQueue() throws Exception
    {
        TableMetadata tm = TableMetadata.builder("ks", "t")
                                        .addPartitionKeyColumn("pk", Int32Type.instance)
                                        .addRegularColumn("v", BytesType.instance)
                                        .build();

        Memtable.Factory factory = TrieMemtable.FACTORY;

        Memtable mt = factory.create(new AtomicReference<>(CommitLogPosition.NONE),
                                     TableMetadataRef.forOfflineTools(tm),
                                     OWNER);

        OpOrder order = new OpOrder(); // stands in for Keyspace.writeOrder
        MemtablePool pool = AbstractAllocatorMemtable.MEMORY_POOL;

        OpOrder.Group g1 = order.start();          // W1's op: PRE-barrier

        OpOrder.Barrier barrier = order.newBarrier();
        barrier.issue();
        barrier.markBlocking();                    // exactly what ColumnFamilyStore.Flush.run does

        pool.onHeap.allocated(pool.onHeap.limit);  // synthetic exhaustion: next allocate parks
        pool.offHeap.allocated(pool.offHeap.limit);
        try
        {
            OpOrder.Group g2 = order.start();      // W2's op: POST-barrier
            Thread w2 = run("w2", () -> { mt.put(update(tm), UpdateTransaction.NO_OP, g2); g2.close(); });
            waitUntilBlockedAtOrDone(w2, "MemtableShard", "SubAllocator"); // lock holder, parked in allocate

            Thread w1 = run("w1", () -> { mt.put(update(tm), UpdateTransaction.NO_OP, g1); g1.close(); });
            waitUntilBlockedAtOrDone(w1, "MemtableShard", "ReentrantLock");// queued on the same (only) lock

            // A flush's writeBarrier.await() must complete: markBlocking() has run, and the
            // design guarantees pre-barrier ops can always make progress. On current main
            // it never returns -- W1 is on a lock, invisible to the valve.
            Thread flush = run("flush", barrier::await);
            flush.join(15_000);

            if (flush.isAlive())
                fail("DEADLOCK: writeBarrier.await() did not complete in 15s.\n" + dump(flush, w1, w2));
        }
        finally
        {
            pool.onHeap.released(pool.onHeap.limit);   // restore the shared pool for other tests;
            pool.offHeap.released(pool.offHeap.limit); // also drains the wedged daemon threads
        }
    }

    private static PartitionUpdate update(TableMetadata tm)
    {
        PartitionUpdate.SimpleBuilder b = PartitionUpdate.simpleBuilder(tm, 42);
        b.row().add("v", ByteBuffer.allocate(64));
        return b.build();
    }

    private static Thread run(String name, Runnable r)
    {
        Thread t = new Thread(r, name);
        t.setDaemon(true);
        t.start();
        return t;
    }

    /** Wait until the thread's stack shows all substrings (wedged, the bug) or the thread ends (fixed build). */
    private static void waitUntilBlockedAtOrDone(Thread t, String... frameSubstrings) throws InterruptedException
    {
        for (long deadline = System.nanoTime() + 30_000_000_000L; System.nanoTime() < deadline; Thread.sleep(50))
        {
            if (!t.isAlive())
                return;

            String stack = java.util.Arrays.toString(t.getStackTrace());
            boolean all = true;

            for (String s : frameSubstrings)
                all &= stack.contains(s);

            if (all)
                return;
        }
        throw new AssertionError(t.getName() + " neither finished nor reached " + String.join("+", frameSubstrings));
    }

    /** ~jstack of the deadlock participants for the failure message. */
    private static String dump(Thread... threads)
    {
        StringBuilder sb = new StringBuilder();
        for (Thread t : threads)
        {
            sb.append('"').append(t.getName()).append("\" ").append(t.getState()).append('\n');
            StackTraceElement[] stack = t.getStackTrace();

            for (int i = 0; i < Math.min(stack.length, 12); i++)
                sb.append("    at ").append(stack[i]).append('\n');
        }
        return sb.toString();
    }
}
