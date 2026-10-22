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

package org.apache.cassandra.io.sstable.format;

import java.io.IOError;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.compaction.AbstractTableOperation;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.CompactionRealm;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.compaction.TableOperation;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.partitions.ImmutableBTreePartition;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.rows.AbstractCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellData;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.db.rows.WrappingUnfilteredRowIterator;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.IScrubber;
import org.apache.cassandra.io.sstable.SSTableIdentityIterator;
import org.apache.cassandra.io.sstable.SSTableRewriter;
import org.apache.cassandra.io.sstable.format.SSTableFormat.Components;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.io.util.ReadPattern;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.OutputHandler;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.concurrent.Refs;
import org.apache.cassandra.utils.memory.HeapCloner;

import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;

@NotThreadSafe
public abstract class SortedTableScrubber<R extends SSTableReaderWithFilter> implements IScrubber
{
    private final static Logger logger = LoggerFactory.getLogger(SortedTableScrubber.class);

    protected final CompactionRealm realm;
    protected final LifecycleTransaction transaction;
    protected final File destination;
    protected final IScrubber.Options options;
    protected final R sstable;
    protected final OutputHandler outputHandler;
    protected final boolean isCommutative;
    protected final long expectedBloomFilterSize;
    protected final ReadWriteLock fileAccessLock = new ReentrantReadWriteLock();
    protected final RandomAccessReader dataFile;
    protected final ScrubInfo scrubInfo;

    protected final NegativeLocalDeletionInfoMetrics negativeLocalDeletionInfoMetrics = new NegativeLocalDeletionInfoMetrics();

    private static final Comparator<Partition> partitionComparator = Comparator.comparing(Partition::partitionKey);
    protected final SortedSet<Partition> outOfOrder = new TreeSet<>(partitionComparator);


    protected int goodPartitions;
    protected int badPartitions;
    protected int emptyPartitions;


    protected SortedTableScrubber(CompactionRealm realm,
                                  LifecycleTransaction transaction,
                                  OutputHandler outputHandler,
                                  Options options)
    {
        this.sstable = (R) transaction.onlyOne();
        Preconditions.checkNotNull(sstable.metadata());
        assert sstable.metadata().keyspace.equals(realm.getKeyspaceName());
        if (!sstable.descriptor.cfname.equals(realm.metadata().name))
        {
            logger.warn("Descriptor points to a different table {} than metadata {}", sstable.descriptor.cfname, realm.metadata().name);
        }
        try
        {
            sstable.metadata().validateCompatibility(realm.metadata());
        }
        catch (ConfigurationException ex)
        {
            logger.warn("Descriptor points to a different table {} than metadata {}", sstable.descriptor.cfname, realm.metadata().name);
        }

        this.realm = realm;
        this.transaction = transaction;
        this.outputHandler = outputHandler;
        this.options = options;
        this.destination = realm.getDirectories().getLocationForDisk(realm.getDiskBoundaries().getCorrectDiskForSSTable(sstable));
        this.isCommutative = realm.metadata().isCounter();

        List<SSTableReader> toScrub = Collections.singletonList(sstable);

        long approximateKeyCount;
        try
        {
            approximateKeyCount = SSTableReader.getApproximateKeyCount(toScrub);
        }
        catch (RuntimeException ex)
        {
            approximateKeyCount = 0;
        }
        this.expectedBloomFilterSize = Math.max(realm.metadata().params.minIndexInterval, approximateKeyCount);

        // loop through each partition, deserializing to check for damage.
        // We'll also loop through the index at the same time, using the position from the index to recover if the
        // partition header (key or data size) is corrupt. (This means our position in the index file will be one
        // partition "ahead" of the data file.)
        boolean isOffline = options.overrideTxnIsOffline ? false : transaction.isOffline();
        this.dataFile = isOffline
                        ? sstable.openDataReader(ReadPattern.SEQUENTIAL)
                        : sstable.openDataReader(CompactionManager.instance.getRateLimiter(), ReadPattern.SEQUENTIAL);

        this.scrubInfo = new ScrubInfo(dataFile, sstable, fileAccessLock.readLock());

        if (options.reinsertOverflowedTTLRows)
            outputHandler.output("Starting scrub with reinsert overflowed TTL option");
    }

    public static void deleteOrphanedComponents(Descriptor descriptor, Set<Component> components)
    {
        File dataFile = descriptor.fileFor(Components.DATA);
        if (components.contains(Components.DATA) && dataFile.length() > 0)
            // everything appears to be in order... moving on.
            return;

        // missing the DATA file! all components are orphaned
        logger.warn("Removing orphans for {}: {}", descriptor, components);
        for (Component component : components)
        {
            File file = descriptor.fileFor(component);
            if (file.exists())
                descriptor.fileFor(component).delete();
        }
    }

    @Override
    public List<SSTableReader>  scrub()
    {
        List<SSTableReader> finished = new ArrayList<>();
        outputHandler.output("Scrubbing %s (%s)", sstable, FBUtilities.prettyPrintMemory(dataFile.length()));
        // Do not use early opening with scrub as it is unsafe: out-of-order partitions, and the out-of-order rows of
        // in-order partitions, are collected in memory and written to a separate sstable only at the end
        // (writeOutOfOrderPartitions). Early opening moves the start of the source sstable (moveStarts) past the
        // last key K written so far; a partition with a key before K that goes to that separate sstable, whether or
        // not scrub has reached it yet, is then readable from neither until the operation completes.
        try (SSTableRewriter writer = SSTableRewriter.constructWithoutEarlyOpening(transaction, false, sstable.maxDataAge);
             Refs<SSTableReader> refs = Refs.ref(Collections.singleton(sstable)))
        {
            StatsMetadata metadata = sstable.getSSTableMetadata();
            writer.switchWriter(CompactionManager.createWriter(realm, destination, expectedBloomFilterSize, metadata.repairedAt, metadata.pendingRepair, metadata.isTransient, sstable, transaction));

            scrubInternal(writer);

            if (!outOfOrder.isEmpty())
                finished.add(writeOutOfOrderPartitions(metadata));

            // finish obsoletes the old sstable
            transaction.obsoleteOriginals();
            finished.addAll(writer.setRepairedAt(badPartitions > 0 ? ActiveRepairService.UNREPAIRED_SSTABLE : sstable.getSSTableMetadata().repairedAt).finish());
        }
        catch (IOException ex)
        {
            throw new RuntimeException(ex);
        }
        finally
        {
            boolean isOffline = options.overrideTxnIsOffline ? false : transaction.isOffline();
            if (isOffline)
                finished.forEach(sstable -> sstable.selfRef().release());
        }

        outputSummary(finished);

        return finished; // already released
    }

    protected abstract void scrubInternal(SSTableRewriter writer) throws IOException;

    private void outputSummary(List<SSTableReader> finished)
    {
        if (!finished.isEmpty())
        {
            outputHandler.output("Scrub of %s complete: %d partitions in new sstable and %d empty (tombstoned) partitions dropped", sstable, goodPartitions, emptyPartitions);
            if (negativeLocalDeletionInfoMetrics.fixedRows > 0)
                outputHandler.output("Fixed %d rows with overflowed local deletion time.", negativeLocalDeletionInfoMetrics.fixedRows);
            if (badPartitions > 0)
                outputHandler.warn("Unable to recover %d partitions that were skipped.  You can attempt manual recovery from the pre-scrub snapshot.  You can also run nodetool repair to transfer the data from a healthy replica, if any", badPartitions);
        }
        else
        {
            if (badPartitions > 0)
                outputHandler.warn("No valid partitions found while scrubbing %s; it is marked for deletion now. If you want to attempt manual recovery, you can find a copy in the pre-scrub snapshot", sstable);
            else if (negativeLocalDeletionInfoMetrics.droppedRows > 0)
                outputHandler.output("Scrub of %s complete; all %d partitions were empty (tombstoned, or only holding rows with overflowed local expiration time)", sstable, emptyPartitions);
            else
                outputHandler.output("Scrub of %s complete; looks like all %d partitions were tombstoned", sstable, emptyPartitions);
        }

        if (negativeLocalDeletionInfoMetrics.droppedRows > 0)
            outputHandler.warn("Dropped %d rows with overflowed local expiration time (CASSANDRA-14092); restore the original sstables (e.g. from the pre-scrub snapshot, if one was taken) and re-run scrub with --reinsert-overflowed-ttl to keep them", negativeLocalDeletionInfoMetrics.droppedRows);
    }

    private SSTableReader writeOutOfOrderPartitions(StatsMetadata metadata)
    {
        // out of order partitions/rows, but no bad partition found - we can keep our repairedAt time
        long repairedAt = badPartitions > 0 ? ActiveRepairService.UNREPAIRED_SSTABLE : sstable.getSSTableMetadata().repairedAt;
        SSTableReader newInOrderSstable;
        try (SSTableWriter inOrderWriter = CompactionManager.createWriter(realm, destination, expectedBloomFilterSize, repairedAt, metadata.pendingRepair, metadata.isTransient, sstable, transaction))
        {
            for (Partition partition : outOfOrder)
                inOrderWriter.append(partition.unfilteredIterator());
            inOrderWriter.setRepairedAt(-1);
            inOrderWriter.setMaxDataAge(sstable.maxDataAge);
            newInOrderSstable = inOrderWriter.finish(true, null);
        }
        transaction.update(newInOrderSstable, false);
        outputHandler.warn("%d out of order partition (or partitions without of order rows) found while scrubbing %s; " +
                           "Those have been written (in order) to a new sstable (%s)", outOfOrder.size(), sstable, newInOrderSstable);
        return newInOrderSstable;
    }

    protected abstract UnfilteredRowIterator withValidation(UnfilteredRowIterator iter, File file);

    @Override
    @VisibleForTesting
    public ScrubResult scrubWithResult()
    {
        List<SSTableReader> scrubbed = scrub();
        return new ScrubResult(goodPartitions, badPartitions, emptyPartitions, scrubbed);
    }

    @Override
    public TableOperation getScrubInfo()
    {
        return scrubInfo;
    }

    protected String keyString(DecoratedKey key)
    {
        return keyString(realm.metadata(), key);
    }

    private static String keyString(TableMetadata metadata, DecoratedKey key)
    {
        if (key == null)
            return "(unknown)";

        try
        {
            return metadata.partitionKeyType.getString(key.getKey());
        }
        catch (Exception e)
        {
            return String.format("(corrupted; hex value: %s)", ByteBufferUtil.bytesToHex(key.getKey()));
        }
    }

    protected boolean tryAppend(DecoratedKey prevKey, DecoratedKey key, SSTableRewriter writer)
    {
        // OrderCheckerIterator will check, at iteration time, that the rows are in the proper order. If it detects
        // that one row is out of order, it will stop returning them. The remaining rows will be sorted and added
        // to the outOfOrder set that will be later written to a new SSTable.
        RowMergingSSTableIterator rowMergingIterator = getRowMergingIterator(key);
        try (OrderCheckerIterator sstableIterator = new OrderCheckerIterator(getIterator(rowMergingIterator), realm.metadata().comparator);
             UnfilteredRowIterator iterator = withValidation(sstableIterator, dataFile.getFile()))
        {
            if (prevKey != null && prevKey.compareTo(key) > 0)
            {
                saveOutOfOrderPartition(prevKey, key, iterator);
                negativeLocalDeletionInfoMetrics.droppedRows += rowMergingIterator.droppedRows();
                return false;
            }

            if (writer.tryAppend(iterator) == null)
                emptyPartitions++;
            else
                goodPartitions++;
            // only count the rows of the partitions that were written, as a partition can fail and be retried
            negativeLocalDeletionInfoMetrics.droppedRows += rowMergingIterator.droppedRows();

            if (sstableIterator.hasRowsOutOfOrder())
            {
                outputHandler.warn("Out of order rows found in partition: %s", keyString(key));
                outOfOrder.add(sstableIterator.getRowsOutOfOrder());
            }
        }

        return true;
    }

    private RowMergingSSTableIterator getRowMergingIterator(DecoratedKey key)
    {
        return new RowMergingSSTableIterator(SSTableIdentityIterator.create(sstable, dataFile, key),
                                             outputHandler,
                                             sstable.descriptor.version,
                                             options.reinsertOverflowedTTLRows);
    }

    /**
     * Only wrap with {@link FixNegativeLocalDeletionTimeIterator} if {@link IScrubber.Options#reinsertOverflowedTTLRows} option
     * is specified
     */
    private UnfilteredRowIterator getIterator(RowMergingSSTableIterator rowMergingIterator)
    {
        if (options.reinsertOverflowedTTLRows)
            return new FixNegativeLocalDeletionTimeIterator(rowMergingIterator, outputHandler, negativeLocalDeletionInfoMetrics);
        else
            return rowMergingIterator;
    }

    private void saveOutOfOrderPartition(DecoratedKey prevKey, DecoratedKey key, UnfilteredRowIterator iterator)
    {
        // TODO bitch if the row is too large?  if it is there's not much we can do ...
        outputHandler.warn("Out of order partition detected (%s found after %s)", keyString(key), keyString(prevKey));
        outOfOrder.add(ImmutableBTreePartition.create(iterator));
    }

    protected static void throwIfFatal(Throwable th)
    {
        if (th instanceof Error && !(th instanceof AssertionError || th instanceof IOError))
            throw (Error) th;
    }

    protected void throwIfCannotContinue(DecoratedKey key, Throwable th)
    {
        if (isCommutative && !options.skipCorrupted)
        {
            outputHandler.warn("An error occurred while scrubbing the partition with key '%s'.  Skipping corrupt " +
                               "data in counter tables will result in undercounts for the affected " +
                               "counters (see CASSANDRA-2759 for more details), so by default the scrub will " +
                               "stop at this point.  If you would like to skip the row anyway and continue " +
                               "scrubbing, re-run the scrub with the --skip-corrupted option.",
                               keyString(key));
            throw new IOError(th);
        }
    }


    public static class ScrubInfo extends AbstractTableOperation
    {
        private final RandomAccessReader dataFile;
        private final SSTableReader sstable;
        private final TimeUUID scrubCompactionId;
        private final Lock fileReadLock;

        public ScrubInfo(RandomAccessReader dataFile, SSTableReader sstable, Lock fileReadLock)
        {
            this.dataFile = dataFile;
            this.sstable = sstable;
            this.fileReadLock = fileReadLock;
            scrubCompactionId = nextTimeUUID();
        }

        public OperationProgress getProgress()
        {
            fileReadLock.lock();
            try
            {
                return new OperationProgress(sstable.metadata(), 
                                             OperationType.SCRUB,
                                             dataFile.getFilePointer(),
                                             dataFile.length(),
                                             scrubCompactionId,
                                             ImmutableSet.of(sstable),
                                             File.getPath(sstable.getFilename()).getParent().toString());
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
            finally
            {
                fileReadLock.unlock();
            }
        }

        public boolean isGlobal()
        {
            return false;
        }
    }

    /**
     * In some case like CASSANDRA-12127 the cells might have been stored in the wrong order. This decorator check the
     * cells order and collect the out-of-order cells to correct the problem.
     */
    private static final class OrderCheckerIterator extends AbstractIterator<Unfiltered> implements WrappingUnfilteredRowIterator
    {
        private final UnfilteredRowIterator iterator;
        private final ClusteringComparator comparator;

        private Unfiltered previous;

        /**
         * The partition containing the rows which are out of order.
         */
        private Partition rowsOutOfOrder;

        public OrderCheckerIterator(UnfilteredRowIterator iterator, ClusteringComparator comparator)
        {
            this.iterator = iterator;
            this.comparator = comparator;
        }

        @Override
        public UnfilteredRowIterator wrapped()
        {
            return iterator;
        }

        public boolean hasRowsOutOfOrder()
        {
            return rowsOutOfOrder != null;
        }

        public Partition getRowsOutOfOrder()
        {
            return rowsOutOfOrder;
        }

        @Override
        protected Unfiltered computeNext()
        {
            if (!iterator.hasNext())
                return endOfData();

            Unfiltered next = iterator.next();

            // If we detect that some rows are out of order we will store and sort the remaining ones to insert them
            // in a separate SSTable.
            if (previous != null && comparator.compare(next, previous) < 0)
            {
                rowsOutOfOrder = ImmutableBTreePartition.create(UnfilteredRowIterators.concat(next, iterator), false);
                return endOfData();
            }
            previous = next;
            return next;
        }
    }

    /**
     * During 2.x migration, under some circumstances rows might have gotten duplicated.
     * Merging iterator merges rows with same clustering.
     * <p>
     * For more details, refer to CASSANDRA-12144.
     * <p>
     * Non-row unfiltereds (range tombstone markers) are returned unchanged. Rows with an overflowed local expiration
     * time are rebuilt or dropped (see {@link #computeFinalRow(Row)}); a dropped row is skipped, so this iterator never
     * returns {@code null}.
     */
    @VisibleForTesting
    static class RowMergingSSTableIterator extends AbstractIterator<Unfiltered> implements WrappingUnfilteredRowIterator
    {
        private Unfiltered nextToOffer = null;
        private final OutputHandler output;
        private final UnfilteredRowIterator wrapped;
        private final Version sstableVersion;
        private final boolean reinsertOverflowedTTLRows;
        private final boolean sstableSupportsExtendedDeletionTime;
        private int droppedRows = 0;

        RowMergingSSTableIterator(UnfilteredRowIterator source, OutputHandler output, Version sstableVersion, boolean reinsertOverflowedTTLRows)
        {
            this.wrapped = source;
            this.output = output;
            this.sstableVersion = sstableVersion;
            this.reinsertOverflowedTTLRows = reinsertOverflowedTTLRows;
            this.sstableSupportsExtendedDeletionTime = MessagingService.Version.supportsExtendedDeletionTime(sstableVersion.correspondingMessagingVersion());
        }

        @Override
        public UnfilteredRowIterator wrapped()
        {
            return wrapped;
        }

        @Override
        public void close()
        {
            wrapped.close();
        }

        @Override
        protected Unfiltered computeNext()
        {
            while (nextToOffer != null || wrapped.hasNext())
            {
                Unfiltered next = nextToOffer != null ? nextToOffer : wrapped.next();
                nextToOffer = null;

                if (!next.isRow())
                    return next;

                Row row = computeFinalRow(mergeDuplicates((Row) next));
                if (row != null)
                    return row;
            }
            return endOfData();
        }

        /**
         * Merges into {@code row} the rows that follow it with the same clustering; the first unfiltered that is not
         * such a duplicate is kept in {@link #nextToOffer}.
         */
        private Row mergeDuplicates(Row row)
        {
            boolean logged = false;
            while (wrapped.hasNext())
            {
                Unfiltered peek = wrapped.next();
                if (!peek.isRow() || !row.clustering().equals(peek.clustering()))
                {
                    nextToOffer = peek; // Offer peek in next call
                    return row;
                }

                // Duplicate row, merge it.
                row = Rows.merge(row, (Row) peek);

                if (!logged)
                {
                    output.warn("Duplicate row detected in %s.%s: %s %s", metadata().keyspace, metadata().name, keyString(metadata(), partitionKey()), row.clustering().toString(metadata()));
                    logged = true;
                }
            }
            return row;
        }

         private Row computeFinalRow(Row next)
         {
             // If the row has overflowed let rows skip them unless we need to keep them for the overflow policy
             if (hasOverflowedLocalExpirationTimeRow(next) && !reinsertOverflowedTTLRows)
             {
                 output.debug("Dropping row with overflowed local expiration time in %s.%s: %s %s",
                              metadata().keyspace, metadata().name, keyString(metadata(), partitionKey()), next.clustering().toString(metadata()));
                 droppedRows++;
                 return null;
             }
             else if (reinsertOverflowedTTLRows)
                 return rebuildTimestamptsForOverflowedRows(next);
             else
                 return next;
         }

         /*
          * When building ldt on deser it won't overflow now being a long as it used to. 
          * This causes row resurrection for old sstable formats!
          * To prevent it we preserve the overflow to be backwards compatible and to feed into the overflow policy
          */
         private Row rebuildTimestamptsForOverflowedRows(Row row)
         {
             if (sstableVersion.hasUIntDeletionTime())
                 return row;

             return row.transformAndFilter(this::rebuildTimestampsForOverflowedLivenessInfo,
                                           this::rebuildTimestampsForOverflowedCells)
                       .clone(HeapCloner.instance);
         }

        private LivenessInfo rebuildTimestampsForOverflowedLivenessInfo(LivenessInfo livenessInfo)
        {
            return (livenessInfo.isExpiring() && isOverflowed(livenessInfo.localExpirationTime()))
                   ? livenessInfo.withUpdatedTimestampAndLocalDeletionTime(livenessInfo.timestamp(), livenessInfo.localExpirationTime(), false)
                   : livenessInfo;
        }

        private <C extends CellData<?, C>> C rebuildTimestampsForOverflowedCells(C cell)
         {
             return cell.isExpiring() && isOverflowed(cell.localDeletionTime())
                    ? cell.withUpdatedTimestampAndLocalDeletionTime(cell.timestamp(), cell.localDeletionTime())
                    : cell;
         }


         private boolean hasOverflowedLocalExpirationTimeRow(Row next)
         {
             if (sstableVersion.hasUIntDeletionTime())
                 return false;

             if (next.primaryKeyLivenessInfo().isExpiring() && isOverflowed(next.primaryKeyLivenessInfo().localExpirationTime()))
             {
                 return true;
             }

             for (ColumnData cd : next)
             {
                 if (cd.column().isSimple())
                 {
                     Cell<?> cell = (Cell<?>)cd;
                     if (cell.isExpiring() && isOverflowed(cell.localDeletionTime()))
                         return true;
                 }
                 else
                 {
                     ComplexColumnData complexData = (ComplexColumnData)cd;
                     for (Cell<?> cell : complexData)
                     {
                         if (cell.isExpiring() && isOverflowed(cell.localDeletionTime()))
                             return true;
                     }
                 }
             }

             return false;
         }

        /**
         * Whether a local expiration time read from an sstable without unsigned int deletion times has overflowed
         * (CASSANDRA-14092), see {@link Cell#decodeLocalDeletionTime}. This only depends on the sstable version:
         * <ul>
         * <li>{@link CellData#INVALID_DELETION_TIME} has always overflowed;</li>
         * <li>sstables written with a messaging version without extended deletion times (BIG versions before
         * {@code oa}) were always written with the 2038 cap, so a value above
         * {@link CellData#MAX_DELETION_TIME_2038_LEGACY_CAP} has overflowed;</li>
         * <li>sstables written with a messaging version with extended deletion times (legacy BTI versions such as
         * {@code bb} and {@code cc}) can legitimately hold an expiration time after 2038, so any other value is
         * valid. A pre-CASSANDRA-14092 overflow read back from such an sstable is kept, with the expiration time
         * that was intended when the row was written.</li>
         * </ul>
         */
        private boolean isOverflowed(long localExpirationTime)
        {
            return localExpirationTime == CellData.INVALID_DELETION_TIME
                   || (!sstableSupportsExtendedDeletionTime && localExpirationTime > CellData.MAX_DELETION_TIME_2038_LEGACY_CAP);
        }

        int droppedRows()
        {
            return droppedRows;
        }
     }

    /**
     * This iterator converts negative {@link AbstractCell#localDeletionTime()} into {@link AbstractCell#MAX_DELETION_TIME}
     * <p>
     * This is to recover entries with overflowed localExpirationTime due to CASSANDRA-14092
     */
    private static final class FixNegativeLocalDeletionTimeIterator extends AbstractIterator<Unfiltered> implements WrappingUnfilteredRowIterator
    {
        /**
         * The decorated iterator.
         */
        private final UnfilteredRowIterator iterator;

        private final OutputHandler outputHandler;
        private final NegativeLocalDeletionInfoMetrics negativeLocalExpirationTimeMetrics;

        public FixNegativeLocalDeletionTimeIterator(UnfilteredRowIterator iterator, OutputHandler outputHandler,
                                                    NegativeLocalDeletionInfoMetrics negativeLocalDeletionInfoMetrics)
        {
            this.iterator = iterator;
            this.outputHandler = outputHandler;
            this.negativeLocalExpirationTimeMetrics = negativeLocalDeletionInfoMetrics;
        }

        private static <C extends CellData<?, C>> C fixCellExpirationTime(C cell)
        {
            return cell.isExpiring() && cell.localDeletionTime() == CellData.INVALID_DELETION_TIME
                   ? cell.withUpdatedTimestampAndLocalDeletionTime(cell.timestamp() + 1, CellData.MAX_DELETION_TIME_2038_LEGACY_CAP)
                   : cell;
        }

        private static LivenessInfo fixLivenessInfoExpirationTime(LivenessInfo livenessInfo)
        {
            return (livenessInfo.isExpiring() && livenessInfo.localExpirationTime() == CellData.INVALID_DELETION_TIME)
                   ? livenessInfo.withUpdatedTimestampAndLocalDeletionTime(livenessInfo.timestamp() + 1, CellData.MAX_DELETION_TIME_2038_LEGACY_CAP)
                   : livenessInfo;
        }

        @Override
        public UnfilteredRowIterator wrapped()
        {
            return iterator;
        }

        @Override
        protected Unfiltered computeNext()
        {
            if (!iterator.hasNext())
                return endOfData();

            Unfiltered next = iterator.next();
            if (!next.isRow())
                return next;

            if (hasNegativeLocalExpirationTime((Row) next))
            {
                outputHandler.debug("Found row with negative local expiration time: %s", next.toString(metadata(), false));
                negativeLocalExpirationTimeMetrics.fixedRows++;
                return fixNegativeLocalExpirationTime((Row) next);
            }

            return next;
        }

        private boolean hasNegativeLocalExpirationTime(Row next)
        {
            Row row = next;
            if (row.primaryKeyLivenessInfo().isExpiring() && row.primaryKeyLivenessInfo().localExpirationTime() == Cell.INVALID_DELETION_TIME)
            {
                return true;
            }

            for (ColumnData cd : row)
            {
                if (cd.column().isSimple())
                {
                    Cell<?> cell = (Cell<?>) cd;
                    if (cell.isExpiring() && cell.localDeletionTime() == Cell.INVALID_DELETION_TIME)
                        return true;
                }
                else
                {
                    ComplexColumnData complexData = (ComplexColumnData) cd;
                    for (Cell<?> cell : complexData)
                    {
                        if (cell.isExpiring() && cell.localDeletionTime() == Cell.INVALID_DELETION_TIME)
                            return true;
                    }
                }
            }

            return false;
        }

        private Unfiltered fixNegativeLocalExpirationTime(Row row)
        {
            return row.transformAndFilter(FixNegativeLocalDeletionTimeIterator::fixLivenessInfoExpirationTime,
                                          FixNegativeLocalDeletionTimeIterator::fixCellExpirationTime)
                      .clone(HeapCloner.instance);
        }
    }

    @VisibleForTesting
    static class NegativeLocalDeletionInfoMetrics
    {
        public volatile int fixedRows = 0;
        public volatile int droppedRows = 0;
    }
}
