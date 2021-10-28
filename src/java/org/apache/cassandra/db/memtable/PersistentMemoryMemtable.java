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

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.intel.pmem.llpl.TransactionalMemoryBlock;
import com.intel.pmem.llpl.util.AutoCloseableIterator;
import com.intel.pmem.llpl.util.LongART;
import com.intel.pmem.llpl.util.ConcurrentLongART;
import com.intel.pmem.llpl.Transaction;
import com.intel.pmem.llpl.TransactionalHeap;
import com.intel.pmem.llpl.util.LongLinkedList;
import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.EmptyIterators;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.memtable.pmem.PmemRowMap;
import org.apache.cassandra.db.memtable.pmem.PmemTableInfo;
import org.apache.cassandra.db.memtable.pmem.PmemUnfilteredPartitionIterator;
import org.apache.cassandra.db.memtable.pmem.PmemPartition;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.IncludingExcludingBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;
import org.apache.cassandra.utils.concurrent.OpOrder;

public class PersistentMemoryMemtable
extends AbstractMemtable
//extends SkipListMemtable        // to test framework
{
    private static final Logger logger = LoggerFactory.getLogger(PersistentMemoryMemtable.class);
    public static final TransactionalHeap heap;
    private static final Map<TableId, ConcurrentLongART> tablesMap;
    private static final LongLinkedList tablesLinkedList;
    private static final int CORES = FBUtilities.getAvailableProcessors();
    private final Owner owner;
    public static final ByteComparable.Version BYTE_COMPARABLE_VERSION = ByteComparable.Version.OSS41;
    private ConcurrentLongART memtableCart;

    private static final Map<TableId, PmemTableInfo> tablesMetadataMap = new ConcurrentHashMap<>();
    private static int version;
    static
    {
        tablesMap = new ConcurrentHashMap<>();
        String path = System.getProperty("pmem_path");
        if (path == null)
        {
            logger.error("Failed to open pool. System property \"pmem_path\" in \"conf/jvm.options\" is unset!");
            System.exit(1);
        }
        long size = 0;
        try
        {
            size = Long.parseLong(System.getProperty("pool_size"));
        }
        catch (NumberFormatException e)
        {
            logger.error("Failed to open pool. System property \"pool_size\" in \"conf/jvm.options\" is invalid!");
            System.exit(1);
        }
        heap = TransactionalHeap.exists(path) ?
               TransactionalHeap.openHeap(path) :
               TransactionalHeap.createHeap(path, size);
        long rootHandle = heap.getRoot();
        if(rootHandle == 0)
        {
            //Holds CART address for each table
            tablesLinkedList = new LongLinkedList(heap);
            heap.setRoot(tablesLinkedList.handle());
        }
        else
        {
            tablesLinkedList = LongLinkedList.fromHandle(heap,rootHandle);
            if(tablesMap.size() == 0)
            {
                reloadTablesMap(tablesLinkedList);
            }
        }
    }

    private static void reloadTablesMap(LongLinkedList tablesLinkedList )
    {
        Iterator<Long> tablesIterator = tablesLinkedList.iterator();
        while(tablesIterator.hasNext())
        {
            PmemTableInfo pmemTableInfo = PmemTableInfo.fromHandle(heap, tablesIterator.next());
            ConcurrentLongART tableCart = ConcurrentLongART.fromHandle(heap,pmemTableInfo.getCartAddress());
            tablesMap.putIfAbsent(pmemTableInfo.getTableID(), tableCart );

            tablesMetadataMap.putIfAbsent(pmemTableInfo.getTableID(), pmemTableInfo);
            TableMetadataRef metadataRef = Schema.instance.getTableMetadataRef(pmemTableInfo.getTableID());
            if (metadataRef != null)
            reloadTablesMetadataMap(metadataRef.get(), pmemTableInfo, heap);
        }
        logger.debug("Reloaded tables\n");
    }

    //TODO: This seems to be needed by defaultMemoryFactory, check
    PersistentMemoryMemtable(AtomicReference<CommitLogPosition> commitLogLowerBound, TableMetadataRef metadataRef, Owner owner)
    {
        this(metadataRef,owner);
    }

    public PersistentMemoryMemtable(TableMetadataRef metadaRef, Owner owner)
    {
        super(metadaRef);
        this.owner = owner;
        if ((memtableCart = tablesMap.get(metadaRef.id)) == null)
        {
            this.version = 1;
            this.memtableCart = new ConcurrentLongART(heap, CORES);
            tablesMap.put(metadaRef.id, memtableCart);

            SerializationHeader sHeader = new SerializationHeader(false,
                                                                  metadaRef.get(),
                                                                  metadaRef.get().regularAndStaticColumns(),
                                                                  EncodingStats.NO_STATS);
            Map<Integer, SerializationHeader> tableMetadataMap = new ConcurrentHashMap<>();
            tableMetadataMap.putIfAbsent(version, sHeader);

            PmemTableInfo pmemTableInfo = new PmemTableInfo(heap, memtableCart.handle(), metadaRef, tableMetadataMap);
            tablesLinkedList.addFirst(pmemTableInfo.handle());
            tablesMetadataMap.putIfAbsent(metadaRef.id, pmemTableInfo);
        }
        else
        {
            PmemTableInfo previousTableinfo = tablesMetadataMap.get(metadaRef.id);
            if (tablesMetadataMap.get(metadaRef.id).getSerializationHeaderMap().size() == 0)
            {
                reloadTablesMetadataMap(metadaRef.get(), previousTableinfo, heap);
            }

            int currentVersion = tablesMetadataMap.get(metadaRef.id).getSerializationHeaderMap().size();
            int previousColumCount = previousTableinfo.getSerializationHeaderMap().get(currentVersion).columns().size();
            if (metadaRef.get().regularAndStaticColumns().size() != previousColumCount)
            {
                int nextVersion = currentVersion + 1;
                SerializationHeader alteredHeader = new SerializationHeader(false,
                                                                            metadaRef.get(),
                                                                            metadaRef.get().regularAndStaticColumns(),
                                                                            EncodingStats.NO_STATS);
                tablesMetadataMap.get(metadaRef.id).getSerializationHeaderMap().putIfAbsent(nextVersion, alteredHeader);

                TransactionalMemoryBlock tableMetadatablock = heap.memoryBlockFromHandle(previousTableinfo.metadataBlockHandle());
                tableMetadatablock.free();
                previousTableinfo.serializeSerializationHeader(tablesMetadataMap.get(metadaRef.id).getSerializationHeaderMap(), heap);
            }
        }
    }

// This function assumes it is being called from within an outer transaction
    private long merge(Object update, long partitionHandle)
    {
        PmemPartition pMemPartition = new PmemPartition(heap, ((PartitionUpdate)update).partitionKey(), metadata(),null);
        try
        {
            if (partitionHandle == 0)
            {
                pMemPartition.initialize((PartitionUpdate) update, heap);
            }
            else
            {
                pMemPartition.load(heap, partitionHandle);
                pMemPartition.update((PartitionUpdate) update, heap);
            }
            return (pMemPartition.getAddress());
        }
        catch (IOException e)
        {
              throw new RuntimeException(e.getMessage()
                                         + " Failed to update partition ", e);
        }
    }

    public long put(PartitionUpdate update, UpdateTransaction indexer, OpOrder.Group opGroup)
    {
        long colUpdateTimeDelta = Long.MAX_VALUE;

        Transaction tx = Transaction.create(heap);
        tx.run(()-> {
            ByteSource partitionByteSource = update.partitionKey().asComparableBytes(ByteComparable.Version.OSS41);
            byte[] partitionKeyBytes= ByteSourceInverse.readBytes(partitionByteSource);
            //ConcurrentAdaptiveRadixTree cart = tablesMap.get(update.metadata().id);
            memtableCart.put(partitionKeyBytes, update, this::merge);
        });
        long[] pair = new long[]{ update.dataSize(), colUpdateTimeDelta };
        return pair[1];
    }

    public MemtableUnfilteredPartitionIterator makePartitionIterator(ColumnFilter columnFilter, DataRange dataRange)
    {
        AbstractBounds<PartitionPosition> keyRange = dataRange.keyRange();
        boolean startIsMin = keyRange.left.isMinimum();
        boolean stopIsMin = keyRange.right.isMinimum();
        boolean isBound = keyRange instanceof Bounds;
        boolean includeStart = isBound || keyRange instanceof IncludingExcludingBounds;
        boolean includeStop = isBound || keyRange instanceof Range;
        int minLocalDeletionTime = Integer.MAX_VALUE;

        AutoCloseableIterator<LongART.Entry> entryIterator;
        //ConcurrentAdaptiveRadixTree cart = tablesMap.get(metadata.id);

        if (startIsMin)
        {
            if(stopIsMin)
            {
                entryIterator = memtableCart.getEntryIterator();
            }
            else
            {
                ByteSource partitionByteSource = keyRange.right.asComparableBytes(ByteComparable.Version.OSS41);
                byte[] partitionKeyBytesRight = ByteSourceInverse.readBytes(partitionByteSource);
                entryIterator =  memtableCart.getHeadEntryIterator(partitionKeyBytesRight, includeStop);
            }
        }
        else
        {
            if(stopIsMin)
            {
                ByteSource partitionByteSource = keyRange.left.asComparableBytes(ByteComparable.Version.OSS41);
                byte[] partitionKeyBytesLeft = ByteSourceInverse.readBytes(partitionByteSource);
                entryIterator = memtableCart.getTailEntryIterator(partitionKeyBytesLeft,includeStart);
            }
            else
            {
                ByteSource partitionByteSource = keyRange.left.asComparableBytes(ByteComparable.Version.OSS41);
                byte[] partitionKeyBytesLeft = ByteSourceInverse.readBytes(partitionByteSource);
                partitionByteSource = keyRange.right.asComparableBytes(ByteComparable.Version.OSS41);
                byte[] partitionKeyBytesRight = ByteSourceInverse.readBytes(partitionByteSource);
                entryIterator = memtableCart.getEntryIterator(partitionKeyBytesLeft, includeStart, partitionKeyBytesRight, includeStop);
            }
        }
        return new PmemUnfilteredPartitionIterator(heap, metadata(), entryIterator, minLocalDeletionTime, columnFilter, dataRange);
    }

    public Partition getPartition(DecoratedKey key)
    {
        PmemPartition pMemPartition = null;
        ByteSource partitionByteSource = key.asComparableBytes(BYTE_COMPARABLE_VERSION);
        byte[] partitionKeyBytes= ByteSourceInverse.readBytes(partitionByteSource);
        // ConcurrentAdaptiveRadixTree cart = tablesMap.get(metadata().id);
        if(memtableCart.size() != 0)
        {
            AutoCloseableIterator<LongART.Entry> cartIterator = memtableCart.getEntryIterator(partitionKeyBytes, true, partitionKeyBytes, true);
            long rowHandle;
            if (cartIterator.hasNext())
            {
                rowHandle = cartIterator.next().getValue();
                pMemPartition = new PmemPartition(heap, key, metadata(), cartIterator);
                try
                {
                    pMemPartition.load(heap, rowHandle);
                }
                catch (IllegalArgumentException e)
                {
                    throw new RuntimeException(e.getMessage()
                                               + " Failed to reload partition ", e);
                }
            }
        }
        return pMemPartition;
    }

    public long partitionCount()
    {
      //  ConcurrentAdaptiveRadixTree cart = tablesMap.get(metadata().id);
        if (memtableCart == null)
            return 0;
        return memtableCart.size();
    }

    public FlushCollection<?> getFlushSet(PartitionPosition from, PartitionPosition to)
    {
        return new AbstractFlushCollection<PmemPartition>()
        {
            @Override
            public long partitionKeySize()
            {
                return 0;
            }

            @Override
            public Iterator<PmemPartition> iterator()
            {
                return Collections.emptyIterator();
            }

            @Override
            public Memtable memtable()
            {
                return PersistentMemoryMemtable.this;
            }

            @Override
            public PartitionPosition from()
            {
                return from;
            }

            @Override
            public PartitionPosition to()
            {
                return to;
            }

            @Override
            public long partitionCount()
            {
                return 0;
            }
        };
    }

    public boolean shouldSwitch(ColumnFamilyStore.FlushReason reason)
    {
        // We want to avoid all flushing.
        switch (reason)
        {
        case STARTUP: // Called after reading and replaying the commit log.
        case SHUTDOWN: // Called to flush data before shutdown.
            return false;
        case INTERNALLY_FORCED: // Called to ensure ordering and persistence of system table events.
            return false;
        case MEMTABLE_PERIOD_EXPIRED: // The specified memtable expiration time elapsed.
        case INDEX_TABLE_FLUSH: // Flush requested on index table because main table is flushing.
        case STREAMS_RECEIVED: // Flush to save streamed data that was written to memtable.
            return false;   // do not do anything

        case INDEX_BUILD_COMPLETED:
        case INDEX_REMOVED:
            // Both of these are needed as safepoints for index management. Nothing to do.
            return false;

        case VIEW_BUILD_STARTED:
        case INDEX_BUILD_STARTED:
            // TODO: Figure out secondary indexes and views.
            return false;

            case SCHEMA_CHANGE: //TODO: check this scenario
            if (!(metadata().params.memtable.factory instanceof Factory))
                return true;    // User has switched to a different memtable class. Flush and release all held data.
            // Otherwise, assuming we can handle the change, don't switch.
            // TODO: Handle
            return false;

        case STREAMING: // Called to flush data so it can be streamed. TODO: How dow we stream?
        case REPAIR: // Called to flush data for repair. TODO: How do we repair?
            // ColumnFamilyStore will create sstables of the affected ranges which will not be consulted on reads and
            // will be deleted after streaming.
            return false;

        case SNAPSHOT:
            // We don't flush for this. Returning false will trigger a performSnapshot call.
            return false;

        case DROP: // Called when a table is dropped. This memtable is no longer necessary.
            deleteTable(true);
            return false;
        case TRUNCATE: // The data is being deleted, but the table remains.
            // Returning true asks the ColumnFamilyStore to replace this memtable object without flushing.
            // This will call setDiscarded() below to delete all held data.
            deleteTable(false);
            return false;

        case MEMTABLE_LIMIT: // The memtable size limit is reached, and this table was selected for flushing.
                             // Also passed if we call owner.signalLimitReached()
        case COMMITLOG_DIRTY: // Commitlog thinks it needs to keep data from this table.
            // Neither of the above should happen as we specify writesAreDurable and don't use an allocator/cleaner.
            throw new AssertionError();

        case USER_FORCED:
        case UNIT_TESTS:
            return false;
        default:
            throw new AssertionError();
        }
    }
    /**
     * Called when the table's metadata is updated. The memtable's metadata reference now points to the new version.
     * This method is called only when, we configure memtable property at table level
     */
    public void metadataUpdated()
    {
        //Multiple threads are not supposed to be able to alter at the same time
        PmemTableInfo previousTableinfo = tablesMetadataMap.get(metadata().id);
        int currentVersion = tablesMetadataMap.get(metadata.id).getSerializationHeaderMap().size();

        int previousColumCount = previousTableinfo.getSerializationHeaderMap().get(currentVersion).columns().size();
        if (metadata.get().regularAndStaticColumns().size() != previousColumCount)
        {
            int nextVersion = currentVersion + 1;
            SerializationHeader alteredHeader = new SerializationHeader(false,
                                                                        metadata.get(),
                                                                        metadata.get().regularAndStaticColumns(),
                                                                        EncodingStats.NO_STATS);
            tablesMetadataMap.get(metadata.id).getSerializationHeaderMap().putIfAbsent(nextVersion, alteredHeader);
            Transaction tx = Transaction.create(heap);
            tx.run(() -> {
                TransactionalMemoryBlock tableMetadatablock = heap.memoryBlockFromHandle(previousTableinfo.metadataBlockHandle());
                tableMetadatablock.free();
                previousTableinfo.serializeSerializationHeader(tablesMetadataMap.get(metadata.id).getSerializationHeaderMap(), heap);
            });
        }
    }

    public void performSnapshot(String snapshotName)
    {
        // TODO: implement. Figure out how to restore snapshot (with external tools).
    }

    public void switchOut(OpOrder.Barrier writeBarrier, AtomicReference<CommitLogPosition> commitLogUpperBound)
    {
        //super.switchOut(writeBarrier, commitLogUpperBound);
        // This can prepare the memtable data for deletion; it will still be used while the flush is proceeding.
        // A setDiscarded call will follow.
    }

    private void deleteTable(boolean dropTable)
    {
       // ConcurrentAdaptiveRadixTree cart = tablesMap.get(metadata().id);
        memtableCart.clear((Long artHandle)->{
            PmemPartition pmemPartition = new PmemPartition(heap, metadata());
            pmemPartition.load(heap, artHandle);
            PmemRowMap rm = PmemRowMap.loadFromAddress(heap,pmemPartition.getRowMapAddress(), metadata(), pmemPartition.partitionLevelDeletion());
            //PmemRowMap rm = PmemRowMap.loadFromAddress(pmemPartition.getRowMapAddress());
            rm.delete();
        });
        if(dropTable)
        {
            memtableCart.free();
            Iterator<Long> tablesIterator = tablesLinkedList.iterator();
            int i = 0;
            while (tablesIterator.hasNext())
            {
                PmemTableInfo pmemTableInfo = PmemTableInfo.fromHandle(heap, tablesIterator.next());
                if (pmemTableInfo.getTableID().equals(metadata.id))
                {
                    tablesLinkedList.remove(i);
                    break;
                }
                i++;
            }
            tablesMap.remove(metadata.id);
        }
    }

    public void discard()
    {
        // TODO: Implement. This should delete all memtable data from pmem.
        //super.discard();

    }

    @Override
    public boolean accepts(OpOrder.Group opGroup, CommitLogPosition commitLogPosition)
    {
        return true;
    }

    public CommitLogPosition getApproximateCommitLogLowerBound()
    {
        // We don't maintain commit log positions
        return CommitLogPosition.NONE;
    }

    public CommitLogPosition getCommitLogLowerBound()
    {
        // We don't maintain commit log positions
        return CommitLogPosition.NONE;
    }

    public CommitLogPosition getCommitLogUpperBound()
    {
        // We don't maintain commit log positions
        return CommitLogPosition.NONE;
    }

    public boolean isClean()
    {
        return true;
       //return partitionCount() == 0;
    }

    public boolean mayContainDataBefore(CommitLogPosition position)
    {
        // We don't track commit log positions, so if we are dirty, we may.
        return !isClean();
    }

    public void addMemoryUsageTo(MemoryUsage stats)
    {
        // our memory usage is not counted
    }

    @Override
    public void markExtraOnHeapUsed(long additionalSpace, OpOrder.Group opGroup)
    {
        // we don't track this
    }

    @Override
    public void markExtraOffHeapUsed(long additionalSpace, OpOrder.Group opGroup)
    {
        // we don't track this
    }

    public static Factory factory(Map<String, String> furtherOptions)
    {
        Boolean skipOption = Boolean.parseBoolean(furtherOptions.remove("skipCommitLog"));
        return skipOption ? commitLogSkippingFactory : commitLogWritingFactory;
    }

    private static final Factory commitLogSkippingFactory = new Factory(true);
    private static final Factory commitLogWritingFactory = new Factory(false);

    public static class Factory implements Memtable.Factory
    {
        private final boolean skipCommitLog;

        public Factory(boolean skipCommitLog)
        {
            this.skipCommitLog = skipCommitLog;
        }

        public Memtable create(AtomicReference<CommitLogPosition> commitLogLowerBound,
                               TableMetadataRef metadaRef,
                               Owner owner)
        {
            return new PersistentMemoryMemtable(metadaRef, owner);
        }

        public boolean writesShouldSkipCommitLog()
        {
            return skipCommitLog;
        }

        public boolean writesAreDurable()
        {
            return true;
        }

        public boolean streamToMemtable()
        {
            return true;
        }

        public boolean streamFromMemtable()
        {
            return true;
        }
    }

    public static void reloadTablesMetadataMap(TableMetadata metadata, PmemTableInfo pmemTableInfo, TransactionalHeap heap)
    {
            Map<Integer, SerializationHeader> tableMetadataMap = pmemTableInfo.deserializeHeader(metadata, heap);
            tablesMetadataMap.get(pmemTableInfo.getTableID()).getSerializationHeaderMap().putAll(tableMetadataMap);
    }

    public static Map<TableId, PmemTableInfo> getTablesMetadataMap()
    {
        return tablesMetadataMap;
    }
}
