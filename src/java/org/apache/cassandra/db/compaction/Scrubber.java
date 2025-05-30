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
package org.apache.cassandra.db.compaction;

import java.io.Closeable;
import java.io.IOError;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.partitions.ImmutableBTreePartition;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.rows.AbstractCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.db.rows.WrappingUnfilteredRowIterator;
import org.apache.cassandra.io.sstable.SSTableIdentityIterator;
import org.apache.cassandra.io.sstable.SSTableRewriter;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.format.ScrubPartitionIterator;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.io.util.ReadPattern;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.OutputHandler;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.cassandra.utils.concurrent.Refs;
import org.apache.cassandra.utils.memory.HeapCloner;

public class Scrubber implements Closeable
{
    private final CompactionRealm realm;
    private final SSTableReader sstable;
    private final LifecycleTransaction transaction;
    private final boolean isOffline;
    private final File destination;
    private final boolean skipCorrupted;
    private final boolean reinsertOverflowedTTLRows;

    private final boolean isCommutative;
    private final boolean isIndex;
    private final boolean checkData;
    private final long expectedBloomFilterSize;

    private final ReadWriteLock fileAccessLock;
    private final RandomAccessReader dataFile;
    private ScrubPartitionIterator indexIterator;
    private final ScrubInfo scrubInfo;

    private int goodPartitions;
    private int badPartitions;
    private int emptyPartitions;

    private NegativeLocalDeletionInfoMetrics negativeLocalDeletionInfoMetrics = new NegativeLocalDeletionInfoMetrics();

    private final OutputHandler outputHandler;

    private static final Comparator<Partition> partitionComparator = Comparator.comparing(Partition::partitionKey);
    private final SortedSet<Partition> outOfOrder = new TreeSet<>(partitionComparator);

    public Scrubber(CompactionRealm realm, LifecycleTransaction transaction, boolean skipCorrupted, boolean checkData)
    {
        this(realm, transaction, skipCorrupted, checkData, false);
    }

    public Scrubber(CompactionRealm realm, LifecycleTransaction transaction, boolean skipCorrupted, boolean checkData,
                    boolean reinsertOverflowedTTLRows)
    {
        this(realm, transaction, transaction.isOffline(), skipCorrupted, new OutputHandler.LogOutput(), checkData, reinsertOverflowedTTLRows);
    }

    @SuppressWarnings("resource")
    public Scrubber(CompactionRealm realm,
                    LifecycleTransaction transaction,
                    boolean isOffline,
                    boolean skipCorrupted,
                    OutputHandler outputHandler,
                    boolean checkData,
                    boolean reinsertOverflowedTTLRows)
    {
        this.realm = realm;
        this.transaction = transaction;
        this.isOffline = isOffline;
        this.sstable = transaction.onlyOne();
        this.outputHandler = outputHandler;
        this.skipCorrupted = skipCorrupted;
        this.reinsertOverflowedTTLRows = reinsertOverflowedTTLRows;

        List<SSTableReader> toScrub = Collections.singletonList(sstable);

        this.destination = realm.getDirectories().getLocationForDisk(realm.getDiskBoundaries().getCorrectDiskForSSTable(sstable));
        this.isCommutative = realm.metadata().isCounter();

        boolean hasIndexFile = sstable.hasIndex();
        this.isIndex = realm.metadata().isIndex();
        if (!hasIndexFile)
        {
            // if there's any corruption in the -Data.db then partitions can't be skipped over. but it's worth a shot.
            outputHandler.warn("Missing index component");
        }
        this.checkData = checkData && !this.isIndex; //LocalByPartitionerType does not support validation
        this.expectedBloomFilterSize = Math.max(
        realm.metadata().params.minIndexInterval,
            hasIndexFile ? SSTableReader.getApproximateKeyCount(toScrub) : 0);

        this.fileAccessLock = new ReentrantReadWriteLock();
        // loop through each partition, deserializing to check for damage.
        // We'll also loop through the index at the same time, using the position from the index to recover if the
        // partition header (key or data size) is corrupt. (This means our position in the index file will be one
        // partition "ahead" of the data file.)
        this.dataFile = isOffline
                        ? sstable.openDataReader(ReadPattern.SEQUENTIAL)
                        : sstable.openDataReader(CompactionManager.instance.getRateLimiter(), ReadPattern.SEQUENTIAL);

        try
        {
            this.indexIterator = hasIndexFile
                                 ? openIndexIterator()
                                 : null;
        }
        catch (RuntimeException ex)
        {
            outputHandler.warn("Detected corruption in the index file - cannot open index iterator", ex);
        }

        this.scrubInfo = new ScrubInfo(dataFile, sstable, fileAccessLock.readLock());

        if (reinsertOverflowedTTLRows)
            outputHandler.output("Starting scrub with reinsert overflowed TTL option");
    }

    private ScrubPartitionIterator openIndexIterator()
    {
        try
        {
            return sstable.scrubPartitionsIterator();
        }
        catch (IOException e)
        {
            outputHandler.warn("Index is unreadable.");
        }
        return null;
    }

    private UnfilteredRowIterator withValidation(UnfilteredRowIterator iter, File file)
    {
        return checkData ? UnfilteredRowIterators.withValidation(iter, file) : iter;
    }

    private String keyString(DecoratedKey key)
    {
        if (key == null)
            return "(unknown)";

        try
        {
            return realm.metadata().partitionKeyType.getString(key.getKey());
        }
        catch (Exception e)
        {
            return String.format("(corrupted; hex value: %s)", ByteBufferUtil.bytesToHex(key.getKey()));
        }
    }

    public List<SSTableReader>  scrub()
    {
        List<SSTableReader> finished = new ArrayList<>();
        outputHandler.output(String.format("Scrubbing %s (%s)", sstable, FBUtilities.prettyPrintMemory(dataFile.length())));
        try (SSTableRewriter writer = SSTableRewriter.construct(realm, transaction, false, sstable.maxDataAge);
             Refs<SSTableReader> refs = Refs.ref(Collections.singleton(sstable)))
        {
            assert !indexAvailable() || indexIterator.dataPosition() == sstable.getDataFileSliceDescriptor().dataStart : indexIterator.dataPosition();

            StatsMetadata metadata = sstable.getSSTableMetadata();
            writer.switchWriter(CompactionManager.createWriter(realm, destination, expectedBloomFilterSize, metadata.repairedAt, metadata.pendingRepair, metadata.isTransient, sstable, transaction));

            DecoratedKey prevKey = null;

            while (!dataFile.isEOF())
            {
                scrubInfo.throwIfStopRequested();

                // position in a data file where the partition starts
                long dataStart = dataFile.getFilePointer();
                outputHandler.debug("Reading row at " + dataStart);

                DecoratedKey key = null;
                Throwable keyReadError = null;
                try
                {
                    ByteBuffer raw = ByteBufferUtil.readWithShortLength(dataFile);
                    if (!realm.metadataRef().getLocal().isIndex())
                        realm.metadataRef().getLocal().partitionKeyType.validate(raw);
                    key = sstable.decorateKey(raw);
                }
                catch (Throwable th)
                {
                    keyReadError = th;
                    throwIfFatal(th);
                    // check for null key below
                }

                // position of the partition in a data file, it points to the beginning of the partition key
                long dataStartFromIndex = -1;
                // size of the partition (including partition key)
                long dataSizeFromIndex = -1;
                ByteBuffer currentIndexKey = null;
                if (indexAvailable())
                {
                    currentIndexKey = indexIterator.key();
                    dataStartFromIndex = indexIterator.dataPosition();
                    if (!indexIterator.isExhausted())
                    {
                        try
                        {
                            indexIterator.advance();
                            if (!indexIterator.isExhausted())
                                dataSizeFromIndex = indexIterator.dataPosition() - dataStartFromIndex;
                        }
                        catch (Throwable th)
                        {
                            throwIfFatal(th);
                            outputHandler.warn(String.format(
                                "Failed to advance to the next index position. Index is corrupted. " +
                                "Continuing without the index. " +
                                "Last position read is %d.", indexIterator.dataPosition()), th);
                            indexIterator.close();
                            indexIterator = null;
                            currentIndexKey = null;
                            dataStartFromIndex = -1;
                            dataSizeFromIndex = -1;
                        }
                    }
                }

                String keyName = key == null ? "(unreadable key)" : keyString(key);
                outputHandler.debug(String.format("partition %s is %s", keyName, FBUtilities.prettyPrintMemory(dataSizeFromIndex)));

                try
                {
                    if (key == null)
                        throw new IOError(new IOException("Unable to read partition key from data file", keyReadError));

                    if (currentIndexKey != null && !key.getKey().equals(currentIndexKey))
                    {
                        throw new IOError(new IOException(String.format("Key from data file (%s) does not match key from index file (%s)",
                                //ByteBufferUtil.bytesToHex(key.getKey()), ByteBufferUtil.bytesToHex(currentIndexKey))));
                                "_too big_", ByteBufferUtil.bytesToHex(currentIndexKey))));
                    }

                    if (indexIterator != null && dataSizeFromIndex > dataFile.length())
                        throw new IOError(new IOException("Impossible partition size (greater than file length): " + dataSizeFromIndex));

                    if (indexIterator != null && dataStart != dataStartFromIndex)
                        outputHandler.warn(String.format("Data file partition position %d differs from index file row position %d", dataStart, dataStartFromIndex));

                    if (tryAppend(prevKey, key, writer))
                        prevKey = key;
                }
                catch (Throwable th)
                {
                    throwIfFatal(th);
                    outputHandler.warn(String.format("Error reading partition %s (stacktrace follows):", keyName), th);

                    if (currentIndexKey != null
                        && (key == null || !key.getKey().equals(currentIndexKey) || dataStart != dataStartFromIndex))
                    {

// position where the row should start in a data file (right after the partition key)
                        long rowStartFromIndex = dataStartFromIndex + TypeSizes.SHORT_SIZE + currentIndexKey.remaining();
                        outputHandler.output(String.format("Retrying from partition index; data is %s bytes starting at %s",
                                                           dataSizeFromIndex, rowStartFromIndex));
                        key = sstable.decorateKey(currentIndexKey);
                        try
                        {
                            if (!realm.metadataRef().getLocal().isIndex())
                                realm.metadataRef().getLocal().partitionKeyType.validate(key.getKey());
                            dataFile.seek(rowStartFromIndex);

                            if (tryAppend(prevKey, key, writer))
                                prevKey = key;
                        }
                        catch (Throwable th2)
                        {
                            throwIfFatal(th2);
                            throwIfCannotContinue(key, th2);

                            outputHandler.warn("Retry failed too. Skipping to next partition (retry's stacktrace follows)", th2);
                            badPartitions++;
                            if (!seekToNextPartition())
                                break;
                        }
                    }
                    else
                    {
                        throwIfCannotContinue(key, th);

                        badPartitions++;
                        if (indexIterator != null)
                        {
                            outputHandler.warn("Partition starting at position " + dataStart + " is unreadable; skipping to next");
                            if (!seekToNextPartition())
                                break;
                        }
                        else
                        {
                            outputHandler.warn(String.format(
                                "Unrecoverable error while scrubbing %s." +
                                "Scrubbing cannot continue. The sstable will be marked for deletion. " +
                                "You can attempt manual recovery from the pre-scrub snapshot. " +
                                "You can also run nodetool repair to transfer the data from a healthy replica, if any.",
                            sstable));
                            // There's no way to resync and continue. Give up.
                            break;
                        }
                    }
                }
            }

            if (!outOfOrder.isEmpty())
            {
                // out of order partitions/rows, but no bad partition found - we can keep our repairedAt time
                long repairedAt = badPartitions > 0 ? ActiveRepairService.UNREPAIRED_SSTABLE : sstable.getSSTableMetadata().repairedAt;
                SSTableReader newInOrderSstable;
                try (SSTableWriter inOrderWriter = CompactionManager.createWriter(realm, destination, expectedBloomFilterSize, repairedAt, metadata.pendingRepair, metadata.isTransient, sstable, transaction))
                {
                    for (Partition partition : outOfOrder)
                        inOrderWriter.append(partition.unfilteredIterator());
                    inOrderWriter.setMaxDataAge(sstable.maxDataAge);
                    newInOrderSstable = inOrderWriter.finish(true, null);
                }
                transaction.update(newInOrderSstable, false);
                finished.add(newInOrderSstable);
                outputHandler.warn(String.format("%d out of order partition (or partitions with out of order rows) found while scrubbing %s; " +
                                                 "Those have been written (in order) to a new sstable (%s)", outOfOrder.size(), sstable, newInOrderSstable));
            }

            // finish obsoletes the old sstable
            transaction.obsoleteOriginals();
            finished.addAll(writer.setRepairedAt(badPartitions > 0 ? ActiveRepairService.UNREPAIRED_SSTABLE : sstable.getSSTableMetadata().repairedAt).finish());
        }
        finally
        {
            if (isOffline)
                finished.forEach(sstable -> sstable.selfRef().release());
        }

        if (!finished.isEmpty())
        {
            outputHandler.output("Scrub of " + sstable + " complete: " + goodPartitions + " partitions in new sstable and " + emptyPartitions + " empty (tombstoned) partitions dropped");
            if (negativeLocalDeletionInfoMetrics.fixedRows > 0)
                outputHandler.output("Fixed " + negativeLocalDeletionInfoMetrics.fixedRows + " rows with overflowed local deletion time.");
            if (badPartitions > 0)
                outputHandler.warn("Unable to recover " + badPartitions + " partitions that were skipped.  You can attempt manual recovery from the pre-scrub snapshot.  You can also run nodetool repair to transfer the data from a healthy replica, if any");
        }
        else
        {
            if (badPartitions > 0)
                outputHandler.warn("No valid partitions found while scrubbing " + sstable + "; it is marked for deletion now. If you want to attempt manual recovery, you can find a copy in the pre-scrub snapshot");
            else
                outputHandler.output("Scrub of " + sstable + " complete; looks like all " + emptyPartitions + " partitions were tombstoned");
        }

        return finished; // already released
    }

    @SuppressWarnings("resource")
    private boolean tryAppend(DecoratedKey prevKey, DecoratedKey key, SSTableRewriter writer)
    {
        // OrderCheckerIterator will check, at iteration time, that the rows are in the proper order. If it detects
        // that one row is out of order, it will stop returning them. The remaining rows will be sorted and added
        // to the outOfOrder set that will be later written to a new SSTable.
        OrderCheckerIterator sstableIterator = new OrderCheckerIterator(getIterator(key),
                                                                        realm.metadata().comparator);

        try (UnfilteredRowIterator iterator = withValidation(sstableIterator, dataFile.getFile()))
        {
            if (prevKey != null && prevKey.compareTo(key) > 0)
            {
                saveOutOfOrderPartition(prevKey, key, iterator);
                return false;
            }

            if (!writer.tryAppend(iterator))
                emptyPartitions++;
            else
                goodPartitions++;
        }

        if (sstableIterator.hasRowsOutOfOrder())
        {
            outputHandler.warn(String.format("Out of order rows found in partition: %s", keyString(key)));
            outOfOrder.add(sstableIterator.getRowsOutOfOrder());
        }

        return true;
    }

    /**
     * Only wrap with {@link FixNegativeLocalDeletionTimeIterator} if {@link #reinsertOverflowedTTLRows} option
     * is specified
     */
    @SuppressWarnings("resource")
    private UnfilteredRowIterator getIterator(DecoratedKey key)
    {
        RowMergingSSTableIterator rowMergingIterator = new RowMergingSSTableIterator(SSTableIdentityIterator.create(sstable, dataFile, key));
        return reinsertOverflowedTTLRows ? new FixNegativeLocalDeletionTimeIterator(rowMergingIterator,
                                                                                    outputHandler,
                                                                                    negativeLocalDeletionInfoMetrics) : rowMergingIterator;
    }

    private boolean indexAvailable()
    {
        return indexIterator != null && !indexIterator.isExhausted();
    }

    private boolean seekToNextPartition()
    {
        while (indexAvailable())
        {
            long nextRowPositionFromIndex = indexIterator.dataPosition();

            try
            {
                dataFile.seek(nextRowPositionFromIndex);
                return true;
            }
            catch (Throwable th)
            {
                throwIfFatal(th);
                outputHandler.warn(String.format("Failed to seek to next row position %d", nextRowPositionFromIndex), th);
                badPartitions++;
            }

            try
            {
                indexIterator.advance();
            }
            catch (Throwable th)
            {
                outputHandler.warn("Failed to go to the next entry in index", th);
                throw Throwables.cleaned(th);
            }
        }

        return false;
    }

    private void saveOutOfOrderPartition(DecoratedKey prevKey, DecoratedKey key, UnfilteredRowIterator iterator)
    {
        // TODO bitch if the row is too large?  if it is there's not much we can do ...
        outputHandler.warn(String.format("Out of order partition detected (%s found after %s)",
                                         keyString(key), keyString(prevKey)));
        outOfOrder.add(ImmutableBTreePartition.create(iterator));
    }

    private void throwIfFatal(Throwable th)
    {
        if (th instanceof Error && !(th instanceof AssertionError || th instanceof IOError))
            throw (Error) th;
    }

    private void throwIfCannotContinue(DecoratedKey key, Throwable th)
    {
        if (isIndex)
        {
            outputHandler.warn(String.format("An error occurred while scrubbing the partition with key '%s' for an index table. " +
                                             "Scrubbing will abort for this table and the index will be rebuilt.", keyString(key)));
            throw new IOError(th);
        }

        if (isCommutative && !skipCorrupted)
        {
            outputHandler.warn(String.format("An error occurred while scrubbing the partition with key '%s'.  Skipping corrupt " +
                                             "data in counter tables will result in undercounts for the affected " +
                                             "counters (see CASSANDRA-2759 for more details), so by default the scrub will " +
                                             "stop at this point.  If you would like to skip the row anyway and continue " +
                                             "scrubbing, re-run the scrub with the --skip-corrupted option.",
                                             keyString(key)));
            throw new IOError(th);
        }
    }

    public void close()
    {
        fileAccessLock.writeLock().lock();
        try
        {
            FileUtils.closeQuietly(dataFile);
        FileUtils.closeQuietly(indexIterator);
        }
        finally
        {
            fileAccessLock.writeLock().unlock();
        }
    }

    public TableOperation getScrubInfo()
    {
        return scrubInfo;
    }

    private static class ScrubInfo extends AbstractTableOperation
    {
        private final RandomAccessReader dataFile;
        private final SSTableReader sstable;
        private final UUID scrubCompactionId;
        private final Lock fileReadLock;

        public ScrubInfo(RandomAccessReader dataFile, SSTableReader sstable, Lock fileReadLock)
        {
            this.dataFile = dataFile;
            this.sstable = sstable;
            this.fileReadLock = fileReadLock;
            scrubCompactionId = UUIDGen.getTimeUUID();
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
                                             ImmutableSet.of(sstable));
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

    @VisibleForTesting
    public ScrubResult scrubWithResult()
    {
        List<SSTableReader> scrubbed = scrub();
        return new ScrubResult(this, scrubbed);
    }

    public static final class ScrubResult
    {
        public final int goodPartitions;
        public final int badPartitions;
        public final int emptyPartitions;
        public final List<SSTableReader> scrubbed;

        public ScrubResult(Scrubber scrubber, List<SSTableReader> scrubbed)
        {
            this.goodPartitions = scrubber.goodPartitions;
            this.badPartitions = scrubber.badPartitions;
            this.emptyPartitions = scrubber.emptyPartitions;
            this.scrubbed = scrubbed;
        }
    }

    public class NegativeLocalDeletionInfoMetrics
    {
        public volatile int fixedRows = 0;
    }

    /**
     * During 2.x migration, under some circumstances rows might have gotten duplicated.
     * Merging iterator merges rows with same clustering.
     *
     * For more details, refer to CASSANDRA-12144.
     */
    private static class RowMergingSSTableIterator extends WrappingUnfilteredRowIterator
    {
        Unfiltered nextToOffer = null;

        RowMergingSSTableIterator(UnfilteredRowIterator source)
        {
            super(source);
        }

        @Override
        public boolean hasNext()
        {
            return nextToOffer != null || wrapped.hasNext();
        }

        @Override
        public Unfiltered next()
        {
            Unfiltered next = nextToOffer != null ? nextToOffer : wrapped.next();

            if (next.isRow())
            {
                while (wrapped.hasNext())
                {
                    Unfiltered peek = wrapped.next();
                    if (!peek.isRow() || !next.clustering().equals(peek.clustering()))
                    {
                        nextToOffer = peek; // Offer peek in next call
                        return next;
                    }

                    // Duplicate row, merge it.
                    next = Rows.merge((Row) next, (Row) peek);
                }
            }

            nextToOffer = null;
            return next;
        }
    }

    /**
     * In some case like CASSANDRA-12127 the cells might have been stored in the wrong order. This decorator check the
     * cells order and collect the out of order cells to correct the problem.
     */
    private static final class OrderCheckerIterator extends AbstractIterator<Unfiltered> implements UnfilteredRowIterator
    {
        /**
         * The decorated iterator.
         */
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

        public TableMetadata metadata()
        {
            return iterator.metadata();
        }

        public boolean isReverseOrder()
        {
            return iterator.isReverseOrder();
        }

        public RegularAndStaticColumns columns()
        {
            return iterator.columns();
        }

        public DecoratedKey partitionKey()
        {
            return iterator.partitionKey();
        }

        public Row staticRow()
        {
            return iterator.staticRow();
        }

        @Override
        public boolean isEmpty()
        {
            return iterator.isEmpty();
        }

        public void close()
        {
            iterator.close();
        }

        public DeletionTime partitionLevelDeletion()
        {
            return iterator.partitionLevelDeletion();
        }

        public EncodingStats stats()
        {
            return iterator.stats();
        }

        public boolean hasRowsOutOfOrder()
        {
            return rowsOutOfOrder != null;
        }

        public Partition getRowsOutOfOrder()
        {
            return rowsOutOfOrder;
        }

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
     * This iterator converts negative {@link AbstractCell#localDeletionTime()} into {@link AbstractCell#MAX_DELETION_TIME}
     *
     * This is to recover entries with overflowed localExpirationTime due to CASSANDRA-14092
     */
    private static final class FixNegativeLocalDeletionTimeIterator extends AbstractIterator<Unfiltered> implements UnfilteredRowIterator
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

        public TableMetadata metadata()
        {
            return iterator.metadata();
        }

        public boolean isReverseOrder()
        {
            return iterator.isReverseOrder();
        }

        public RegularAndStaticColumns columns()
        {
            return iterator.columns();
        }

        public DecoratedKey partitionKey()
        {
            return iterator.partitionKey();
        }

        public Row staticRow()
        {
            return iterator.staticRow();
        }

        @Override
        public boolean isEmpty()
        {
            return iterator.isEmpty();
        }

        public void close()
        {
            iterator.close();
        }

        public DeletionTime partitionLevelDeletion()
        {
            return iterator.partitionLevelDeletion();
        }

        public EncodingStats stats()
        {
            return iterator.stats();
        }

        protected Unfiltered computeNext()
        {
            if (!iterator.hasNext())
                return endOfData();

            Unfiltered next = iterator.next();
            if (!next.isRow())
                return next;

            if (hasNegativeLocalExpirationTime((Row) next))
            {
                outputHandler.debug(String.format("Found row with negative local expiration time: %s", next.toString(metadata(), false)));
                negativeLocalExpirationTimeMetrics.fixedRows++;
                return fixNegativeLocalExpirationTime((Row) next);
            }

            return next;
        }

        private boolean hasNegativeLocalExpirationTime(Row next)
        {
            Row row = next;
            if (row.primaryKeyLivenessInfo().isExpiring() && row.primaryKeyLivenessInfo().localExpirationTime() < 0)
            {
                return true;
            }

            for (ColumnData cd : row)
            {
                if (cd.column().isSimple())
                {
                    Cell<?> cell = (Cell<?>)cd;
                    if (cell.isExpiring() && cell.localDeletionTime() < 0)
                        return true;
                }
                else
                {
                    ComplexColumnData complexData = (ComplexColumnData)cd;
                    for (Cell<?> cell : complexData)
                    {
                        if (cell.isExpiring() && cell.localDeletionTime() < 0)
                            return true;
                    }
                }
            }

            return false;
        }

        private Unfiltered fixNegativeLocalExpirationTime(Row row)
        {
            LivenessInfo livenessInfo = row.primaryKeyLivenessInfo();
            if (livenessInfo.isExpiring() && livenessInfo.localExpirationTime() < 0)
                livenessInfo = livenessInfo.withUpdatedTimestampAndLocalDeletionTime(livenessInfo.timestamp() + 1, AbstractCell.MAX_DELETION_TIME);

            return row.transformAndFilter(livenessInfo, row.deletion(), cd -> {
                if (cd.column().isSimple())
                {
                    Cell cell = (Cell)cd;
                    return cell.isExpiring() && cell.localDeletionTime() < 0
                           ? cell.withUpdatedTimestampAndLocalDeletionTime(cell.timestamp() + 1, AbstractCell.MAX_DELETION_TIME)
                           : cell;
                }
                else
                {
                    ComplexColumnData complexData = (ComplexColumnData)cd;
                    return complexData.transformAndFilter(cell -> cell.isExpiring() && cell.localDeletionTime() < 0
                                                                  ? cell.withUpdatedTimestampAndLocalDeletionTime(cell.timestamp() + 1, AbstractCell.MAX_DELETION_TIME)
                                                                  : cell);
                }
            }).clone(HeapCloner.instance);
        }
    }
}
