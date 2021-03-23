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

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;

import com.google.common.util.concurrent.RateLimiter;

import org.apache.cassandra.cache.InstrumentingCache;
import org.apache.cassandra.cache.KeyCacheKey;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.format.SSTableReader.PartitionPositionBounds;
import org.apache.cassandra.io.sstable.format.SSTableReader.UniqueIdentifier;
import org.apache.cassandra.io.sstable.format.big.BigTableRowIndexEntry;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.util.ChannelProxy;
import org.apache.cassandra.io.util.CheckedFunction;
import org.apache.cassandra.io.util.DiskOptimizationStrategy;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.metrics.RestorableMeter;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.EstimatedHistogram;
import org.apache.cassandra.utils.IFilter;
import org.apache.cassandra.utils.concurrent.Ref;
import org.apache.cassandra.utils.concurrent.SelfRefCounted;

public abstract class AbstractSSTableReader extends SSTable implements SelfRefCounted<AbstractSSTableReader>
{
    public final UniqueIdentifier instanceId = new UniqueIdentifier();
    public final SSTableReader.OpenReason openReason;

    public final SerializationHeader header;

    protected AbstractSSTableReader(Descriptor descriptor,
                                    Set<Component> components,
                                    TableMetadataRef metadata,
                                    DiskOptimizationStrategy optimizationStrategy,
                                    SSTableReader.OpenReason openReason,
                                    SerializationHeader header)
    {
        super(descriptor, components, metadata, optimizationStrategy);
        this.openReason = openReason;
        this.header = header;
    }

    public abstract void setupOnline();

    public abstract AbstractSSTableReader cloneWithRestoredStart(DecoratedKey first);

    public abstract AbstractSSTableReader cloneWithNewStart(DecoratedKey newStart, Runnable runOnClose);

    public abstract void addTo(Ref.IdentityCollection collection);

    public abstract void runOnClose(Runnable runOnClose);

    public abstract <R> R runWithLock(CheckedFunction<Descriptor, R, IOException> task) throws IOException;

    public abstract void createLinks(String snapshotDirectoryPath);

    // ---- statistical information retrieval methods

    public abstract Iterable<DecoratedKey> getKeySamples(Range<Token> range);

    /**
     * @return An estimate of the number of keys for given ranges in this SSTable
     */
    public abstract long estimatedKeysForRanges(Collection<Range<Token>> ranges);

    // ---- search methods

    public abstract boolean checkEntryExists(PartitionPosition key, SSTableReader.Operator op, boolean updateCacheAndStats);

    /**
     * Retrieves the position while updating the key cache and the stats.
     *
     * @param key The key to apply as the rhs to the given Operator. A 'fake' key is allowed to
     *            allow key selection by token bounds but only if op != * EQ
     * @param op  The Operator defining matching keys: the nearest key to the target matching the operator wins.
     */
    public abstract RowIndexEntry<?> getPosition(PartitionPosition key, SSTableReader.Operator op);

    /**
     * Determine the minimal set of sections that can be extracted from this SSTable to cover the given ranges.
     *
     * @return A sorted list of (offset,end) pairs that cover the given ranges in the datafile for this SSTable.
     */
    public abstract List<PartitionPositionBounds> getPositionsForRanges(Collection<Range<Token>> ranges);

    /**
     * Finds and returns the first key beyond a given token in this SSTable or null if no such key exists.
     */
    public abstract DecoratedKey firstKeyBeyond(PartitionPosition maxKeyBound);

    public abstract DecoratedKey keyAt(long offset) throws IOException;

    public abstract DecoratedKey keyAt(RandomAccessReader indexFileReader, long indexPosition) throws IOException;

    public abstract boolean intersects(Collection<Range<Token>> ranges);

    // ---- read methods

    public abstract UnfilteredRowIterator iterator(DecoratedKey partitionKey, Slices slices, ColumnFilter columns, boolean reversed, SSTableReadsListener listener);

    public abstract PartitionIndexIterator allKeysIterator() throws IOException;

    public abstract UnfilteredRowIterator simpleIterator(Supplier<FileDataInput> dfile, DecoratedKey key, boolean tombstoneOnly);

    /**
     * Direct I/O SSTableScanner over a defined range of tokens.
     *
     * @param range the range of keys to cover
     * @return A Scanner for seeking over the rows of the SSTable.
     */
    public ISSTableScanner getScanner(Range<Token> range)
    {
        if (range == null)
            return getScanner();
        return getScanner(Collections.singletonList(range));
    }

    /**
     * Direct I/O SSTableScanner over the entirety of the sstable..
     *
     * @return A Scanner over the full content of the SSTable.
     */
    public abstract ISSTableScanner getScanner();

    /**
     * Direct I/O SSTableScanner over a defined collection of ranges of tokens.
     *
     * @param ranges the range of keys to cover
     * @return A Scanner for seeking over the rows of the SSTable.
     */
    public abstract ISSTableScanner getScanner(Collection<Range<Token>> ranges);

    /**
     * Direct I/O SSTableScanner over an iterator of bounds.
     *
     * @param rangeIterator the keys to cover
     * @return A Scanner for seeking over the rows of the SSTable.
     */
    public abstract ISSTableScanner getScanner(Iterator<AbstractBounds<PartitionPosition>> rangeIterator);

    /**
     * @param columnFilter  the columns to return.
     * @param dataRange     filter to use when reading the columns
     * @param readsListener a listener used to handle internal read events
     * @return A Scanner for seeking over the rows of the SSTable
     */
    public abstract ISSTableScanner getScanner(ColumnFilter columnFilter, DataRange dataRange, SSTableReadsListener readsListener);

    // ---- Data read methods

    /**
     * Creates a reader for the data file handle
     */
    public abstract RandomAccessReader openDataReader();

    /**
     * Creates a reader for the data file handle with a given rate limiter
     *
     * @param rateLimiter rate limiter, must not be null
     */
    public abstract RandomAccessReader openDataReader(RateLimiter rateLimiter);

    /**
     * Creates a reader for the data file handle and seeks to the provided position. Equivalent of calling
     * {@link #openDataReader()} and then {@link RandomAccessReader#seek(long)}.
     * TODO remove this method, it is redundant
     */
    public abstract FileDataInput getFileDataInput(long position);

    /**
     * Returns a channel proxy for the data file handle
     */
    public abstract ChannelProxy getDataChannel();

    // --- The methods below are mostly trivial accessors

    /**
     * Returns associated instance of {@link StatsMetadata}. The methods below are simple accessors to the public final
     * fields of that instance and there probably do not make much sense to keep them in the future.
     * <p>
     * TODO remove the methods which are trivial delegates to {@link StatsMetadata} methods and public final fields
     * in order to simplify the implementation and reduce the amount of redundant code
     */
    public abstract StatsMetadata getSSTableMetadata();

    public abstract long getMaxDataAge();

    public abstract long getMinTimestamp();

    public abstract long getMaxTimestamp();

    public abstract int getMinLocalDeletionTime();

    public abstract int getMinTTL();

    public abstract int getSSTableLevel();

    public abstract double getCompressionRatio();

    public abstract long getTotalRows();

    public abstract boolean isTransient();

    public abstract EstimatedHistogram getEstimatedCellPerPartitionCount();

    public abstract EstimatedHistogram getEstimatedPartitionSize();

    public abstract double getEstimatedDroppableTombstoneRatio(int gcBefore);

    public abstract double getDroppableTombstonesBefore(int i);

    public abstract EncodingStats stats();

    public abstract UUID getPendingRepair();

    public abstract long getRepairedAt();
    // ---- end of StatsMetadata redundant delegate methods ----

    public abstract boolean isRepaired();

    public abstract boolean isPendingRepair();

    public abstract boolean newSince(long truncatedAt);

    public abstract long estimatedKeys();

    public abstract long getCreationTimeFor(Component component);

    public abstract void markSuspect();

    public abstract boolean isMarkedSuspect();

    public abstract void setReplaced();

    public abstract boolean isReplaced();

    public abstract void markObsolete(Runnable tidier);

    public abstract boolean isMarkedCompacted();

    public abstract boolean mayHaveTombstones();

    public abstract void incrementReadCount();

    public abstract long onDiskLength();

    public abstract long uncompressedLength();

    public abstract void setCrcCheckChance(double crcCheckChance);

    /**
     * Returns the amount of memory in bytes used off heap by the compression meta-data
     */
    public abstract long getCompressionMetadataOffHeapSize();

    /**
     * Returns the compression metadata for this sstable.
     *
     * @throws IllegalStateException if the sstable is not compressed
     */
    public abstract CompressionMetadata getCompressionMetadata();

    public abstract RestorableMeter getReadMeter();

    public abstract void mutateRepairedAndReload(long repairedAt, UUID pendingRepair, boolean isTransient) throws IOException;

    public abstract void mutateLevelAndReload(int level) throws IOException;

    // ---- Bloom filter information accessors ----
    // TODO those methods are trivial delegates which run getter methods on IFilter or BloomFilterTracker
    //   for simplicity we should rather return IFilter and BloomFilterTracker
    public abstract IFilter getBloomFilter();

    public abstract long getBloomFilterOffHeapSize();

    public abstract long getBloomFilterSerializedSize();

    public abstract long getRecentBloomFilterFalsePositiveCount();

    public abstract long getRecentBloomFilterTruePositiveCount();

    public abstract long getBloomFilterFalsePositiveCount();

    public abstract long getBloomFilterTruePositiveCount();

    // ---- key cache methods - TODO remove them when possible
    public abstract InstrumentingCache<KeyCacheKey, BigTableRowIndexEntry> getKeyCache();

    public abstract boolean isKeyCacheEnabled();

    public abstract long getKeyCacheRequest();

    public abstract long getKeyCacheHit();

    public abstract KeyCacheKey getCacheKey(DecoratedKey key);

    public abstract void cacheKey(DecoratedKey key, BigTableRowIndexEntry value);

    public abstract BigTableRowIndexEntry getCachedPosition(DecoratedKey key, boolean b);

    public abstract double getCrcCheckChance();

    /**
     * Retrieves the partition-level deletion time at the given position of the data file, as specified by
     * {@link SSTableFlushObserver#partitionLevelDeletion(DeletionTime, long)}.
     *
     * @param position the start position of the partion-level deletion time in the data file
     * @return the partion-level deletion time at the specified position
     */
    public abstract DeletionTime partitionLevelDeletionAt(long position) throws IOException;

    /**
     * Retrieves the static row at the given position of the data file, as specified by
     * {@link SSTableFlushObserver#staticRow(Row, long)}.
     *
     * @param position the start position of the static row in the data file
     * @param columnFilter the columns to fetch, {@code null} to select all the columns
     * @return the static row at the specified position
     */
    public abstract Row staticRowAt(long position, ColumnFilter columnFilter) throws IOException;

    /**
     * Retrieves the clustering prefix of the unfiltered at the given position of the data file, as specified by
     * {@link SSTableFlushObserver#nextUnfilteredCluster(Unfiltered, long)}.
     *
     * @param position the start position of the unfiltered in the data file
     * @return the clustering prefix of the unfiltered at the specified position
     */
    public abstract ClusteringPrefix<?> clusteringAt(long position) throws IOException;

    /**
     * Retrieves the unfiltered at the given position of the data file, as specified by
     * {@link SSTableFlushObserver#nextUnfilteredCluster(Unfiltered, long)}.
     *
     * @param position the start position of the unfiltered in the data file
     * @param columnFilter the columns to fetch, {@code null} to select all the columns
     * @return the unfiltered at the specified position
     */
    public abstract Unfiltered unfilteredAt(long position, ColumnFilter columnFilter) throws IOException;

    public abstract void reloadSSTableMetadata() throws IOException;

    public abstract void unmarkSuspect();

    public abstract void overrideReadMeter(RestorableMeter restorableMeter);

    protected abstract int getMaxLocalDeletionTime();

    public abstract AbstractSSTableReader cloneAndReplace(IFilter filter);

    public interface Factory
    {
        PartitionIndexIterator indexIterator(Descriptor descriptor, TableMetadata metadata);

        // TODO in the implementation of those methods we will refer the current static methods which are implemented in AbstractdBigTableReader
        // TODO make those static openXXX methods private

        AbstractSSTableReader openForBatch(Descriptor desc, Set<Component> components, TableMetadataRef metadata);

        AbstractSSTableReader open(Descriptor desc);

        AbstractSSTableReader open(Descriptor desc, TableMetadataRef metadata);

        AbstractSSTableReader open(Descriptor desc, Set<Component> components, TableMetadataRef metadata);

        AbstractSSTableReader open(Descriptor desc, Set<Component> components, TableMetadataRef metadata, boolean validate, boolean isOffline);

        AbstractSSTableReader openNoValidation(Descriptor desc, TableMetadataRef tableMetadataRef);

        AbstractSSTableReader openNoValidation(Descriptor desc, Set<Component> components, ColumnFamilyStore cfs);

        AbstractSSTableReader moveAndOpenSSTable(ColumnFamilyStore cfs, Descriptor oldDescriptor, Descriptor newDescriptor, Set<Component> components, boolean copyData);


    }
}
