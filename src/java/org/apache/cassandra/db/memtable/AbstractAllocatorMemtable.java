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

package org.apache.cassandra.db.memtable;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.WrappedRunnable;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.HeapPool;
import org.apache.cassandra.utils.memory.MemtableAllocator;
import org.apache.cassandra.utils.memory.MemtableCleaner;
import org.apache.cassandra.utils.memory.MemtablePool;
import org.apache.cassandra.utils.memory.NativePool;
import org.apache.cassandra.utils.memory.SlabPool;

public abstract class AbstractAllocatorMemtable extends AbstractMemtable
{
    private static final Logger logger = LoggerFactory.getLogger(AbstractAllocatorMemtable.class);

    public static final MemtablePool MEMORY_POOL = AbstractAllocatorMemtable.createMemtableAllocatorPool();

    protected final Owner owner;
    protected final MemtableAllocator allocator;

    // the precise upper bound of CommitLogPosition owned by this memtable
    private volatile AtomicReference<CommitLogPosition> commitLogUpperBound;
    // the precise lower bound of CommitLogPosition owned by this memtable; equal to its predecessor's commitLogUpperBound
    private AtomicReference<CommitLogPosition> commitLogLowerBound;

    // The approximate lower bound by this memtable; must be <= commitLogLowerBound once our predecessor
    // has been finalised, and this is enforced in the ColumnFamilyStore.setCommitLogUpperBound
    private final CommitLogPosition approximateCommitLogLowerBound = CommitLog.instance.getCurrentPosition();

    // Record the comparator of the CFS at the creation of the memtable. This
    // is only used when a user update the CF comparator, to know if the
    // memtable was created with the new or old comparator.
    public final ClusteringComparator initialComparator;

    private static MemtablePool createMemtableAllocatorPool()
    {
        long heapLimit = DatabaseDescriptor.getMemtableHeapSpaceInMb() << 20;
        long offHeapLimit = DatabaseDescriptor.getMemtableOffheapSpaceInMb() << 20;
        float memtableCleanupThreshold = DatabaseDescriptor.getMemtableCleanupThreshold();
        MemtableCleaner cleaner = ColumnFamilyStore::flushLargestMemtable;
        switch (DatabaseDescriptor.getMemtableAllocationType())
        {
        case unslabbed_heap_buffers:
            logger.debug("Memtables allocating with on-heap buffers");
            return new HeapPool(heapLimit, memtableCleanupThreshold, cleaner);
        case heap_buffers:
            logger.debug("Memtables allocating with on-heap slabs");
            return new SlabPool(heapLimit, 0, memtableCleanupThreshold, cleaner);
        case offheap_buffers:
            logger.debug("Memtables allocating with off-heap buffers");
            return new SlabPool(heapLimit, offHeapLimit, memtableCleanupThreshold, cleaner);
        case offheap_objects:
            logger.debug("Memtables allocating with off-heap objects");
            return new NativePool(heapLimit, offHeapLimit, memtableCleanupThreshold, cleaner);
        default:
            throw new AssertionError();
        }
    }

    // only to be used by init(), to setup the very first memtable for the cfs
    public AbstractAllocatorMemtable(AtomicReference<CommitLogPosition> commitLogLowerBound, TableMetadataRef metadataRef, Owner owner)
    {
        super(metadataRef);
        this.commitLogLowerBound = commitLogLowerBound;
        this.allocator = MEMORY_POOL.newAllocator();
        this.initialComparator = metadata.get().comparator;
        this.owner = owner;
        scheduleFlush();
    }

    public CommitLogPosition getApproximateCommitLogLowerBound()
    {
        return approximateCommitLogLowerBound;
    }

    public MemtableAllocator getAllocator()
    {
        return allocator;
    }

    public boolean shouldSwitch(ColumnFamilyStore.FlushReason reason)
    {
        switch (reason)
        {
        case SCHEMA_CHANGE:
            return initialComparator != metadata().comparator // If the CF comparator has changed, because our partitions reference the old one
                   || metadata().params.memtable.factory != factory(); // If a different type of memtable is requested
        default:
            return true;
        }
    }

    public void metadataUpdated()
    {
        scheduleFlush();
    }

    public void performSnapshot(String snapshotName)
    {
        // unless shouldSwitch(SNAPSHOT) returns false, this cannot be called.
        throw new AssertionError();
    }

    protected abstract Factory factory();

    public void switchOut(OpOrder.Barrier writeBarrier, AtomicReference<CommitLogPosition> commitLogUpperBound)
    {
        super.switchOut(writeBarrier, commitLogUpperBound);
        this.commitLogUpperBound = commitLogUpperBound;
        allocator.setDiscarding();
    }

    public void discard()
    {
        allocator.setDiscarded();
    }

    // decide if this memtable should take the write, or if it should go to the next memtable
    public boolean accepts(OpOrder.Group opGroup, CommitLogPosition commitLogPosition)
    {
        // if the barrier hasn't been set yet, then this memtable is still taking ALL writes
        OpOrder.Barrier barrier = this.writeBarrier;
        if (barrier == null)
            return true;
        // if the barrier has been set, but is in the past, we are definitely destined for a future memtable
        if (!barrier.isAfter(opGroup))
            return false;
        // if we aren't durable we are directed only by the barrier
        if (commitLogPosition == null)
            return true;
        while (true)
        {
            // otherwise we check if we are in the past/future wrt the CL boundary;
            // if the boundary hasn't been finalised yet, we simply update it to the max of
            // its current value and ours; if it HAS been finalised, we simply accept its judgement
            // this permits us to coordinate a safe boundary, as the boundary choice is made
            // atomically wrt our max() maintenance, so an operation cannot sneak into the past
            CommitLogPosition currentLast = commitLogUpperBound.get();
            if (currentLast instanceof LastCommitLogPosition)
                return currentLast.compareTo(commitLogPosition) >= 0;
            if (currentLast != null && currentLast.compareTo(commitLogPosition) >= 0)
                return true;
            if (commitLogUpperBound.compareAndSet(currentLast, commitLogPosition))
                return true;
        }
    }

    public CommitLogPosition getCommitLogLowerBound()
    {
        return commitLogLowerBound.get();
    }

    public CommitLogPosition getCommitLogUpperBound()
    {
        return commitLogUpperBound.get();
    }

    public boolean mayContainDataBefore(CommitLogPosition position)
    {
        return approximateCommitLogLowerBound.compareTo(position) < 0;
    }

    public String toString()
    {
        return String.format("Memtable-%s@%s(%s serialized bytes, %s ops, %.0f%%/%.0f%% of on/off-heap limit)",
                             metadata.get().name,
                             hashCode(),
                             FBUtilities.prettyPrintMemory(liveDataSize.get()),
                             currentOperations,
                             100 * allocator.onHeap().ownershipRatio(),
                             100 * allocator.offHeap().ownershipRatio());
    }

    /**
     * For testing only. Give this memtable too big a size to make it always fail flushing.
     */
    @VisibleForTesting
    public void makeUnflushable()
    {
        liveDataSize.addAndGet(1024L * 1024 * 1024 * 1024 * 1024);
    }

    public MemoryUsage getMemoryUsage()
    {
        MemoryUsage stats = new MemoryUsage(MEMORY_POOL);
        addMemoryUsageTo(stats);
        return stats;
    }

    public void addMemoryUsageTo(MemoryUsage stats)
    {
        stats.ownershipRatioOnHeap += getAllocator().onHeap().ownershipRatio();
        stats.ownershipRatioOffHeap += getAllocator().offHeap().ownershipRatio();
        stats.ownsOnHeap += getAllocator().onHeap().owns();
        stats.ownsOffHeap += getAllocator().offHeap().owns();
    }

    public void allocateExtraOnHeap(long additionalSpace, OpOrder.Group opGroup)
    {
        getAllocator().onHeap().allocate(additionalSpace, opGroup);
    }

    public void allocateExtraOffHeap(long additionalSpace, OpOrder.Group opGroup)
    {
        getAllocator().offHeap().allocate(additionalSpace, opGroup);
    }

    void scheduleFlush()
    {
        int period = metadata().params.memtableFlushPeriodInMs;
        if (period > 0)
        {
            logger.trace("scheduling flush in {} ms", period);
            WrappedRunnable runnable = new WrappedRunnable()
            {
                protected void runMayThrow()
                {
                    if (isClean())
                    {
                        // if we're still clean, instead of swapping just reschedule a flush for later
                        scheduleFlush();
                    }
                    else
                    {
                        // we'll be rescheduled by the constructor of the Memtable.
                        owner.signalFlushRequired(AbstractAllocatorMemtable.this,
                                                  ColumnFamilyStore.FlushReason.MEMTABLE_PERIOD_EXPIRED);
                    }
                }
            };
            ScheduledExecutors.scheduledTasks.schedule(runnable, period, TimeUnit.MILLISECONDS);
        }
    }
}
