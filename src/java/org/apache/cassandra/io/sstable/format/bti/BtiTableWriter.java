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
package org.apache.cassandra.io.sstable.format.bti;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cache.ChunkCache;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.sstable.AbstractRowIndexEntry;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.format.DataComponent;
import org.apache.cassandra.io.sstable.format.IndexComponent;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReader.OpenReason;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SortedTableWriter;
import org.apache.cassandra.io.sstable.format.bti.BtiFormat.Components;
import org.apache.cassandra.io.sstable.metadata.CompactionMetadata;
import org.apache.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.util.DataPosition;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.MmappedRegionsCache;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.IFilter;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.compress.EncryptedSequentialWriter;
import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.schema.TableMetadata;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static org.apache.cassandra.io.util.FileHandle.Builder.NO_LENGTH_OVERRIDE;

/**
 * Writes SSTables in BTI format (see {@link BtiFormat}), which can be read by {@link BtiTableReader}.
 */
@VisibleForTesting
public class BtiTableWriter extends SortedTableWriter<BtiFormatPartitionWriter, BtiTableWriter.IndexWriter>
{
    private static final Logger logger = LoggerFactory.getLogger(BtiTableWriter.class);

    public BtiTableWriter(Builder builder, LifecycleNewTracker lifecycleNewTracker, SSTable.Owner owner)
    {
        super(builder, lifecycleNewTracker, owner);
    }

    @VisibleForTesting
    IndexWriter indexWriter()
    {
        return indexWriter;
    }

    @Override
    protected TrieIndexEntry createRowIndexEntry(DecoratedKey key, DeletionTime partitionLevelDeletion, long finishResult) throws IOException
    {
        TrieIndexEntry entry = TrieIndexEntry.create(partitionWriter.getInitialPosition(),
                                                     finishResult,
                                                     partitionLevelDeletion,
                                                     partitionWriter.getRowIndexBlockCount());
        indexWriter.append(key, entry);
        return entry;
    }

    /**
     * Opens a reader over what has been written so far.
     *
     * @param partitionIndex the partition index of the reader, not null. This method takes ownership of it as soon as
     *                       it is called: it is handed to the returned reader, or closed if the open fails. Callers
     *                       must therefore not do anything that can throw between obtaining the index and calling
     *                       this method.
     */
    @SuppressWarnings({ "resource", "RedundantSuppression" }) // dataFile is closed along with the reader
    private BtiTableReader openInternal(OpenReason openReason, long lengthOverride, PartitionIndex partitionIndex)
    {
        IFilter filter = null;
        FileHandle dataFile = null;
        FileHandle rowIndexFile = null;

        try
        {
            BtiTableReader.Builder builder = unbuildTo(new BtiTableReader.Builder(descriptor), true).setMaxDataAge(maxDataAge)
                                                                                                    .setSerializationHeader(header)
                                                                                                    .setOpenReason(openReason);
            Map<MetadataType, MetadataComponent> finalMetadata = finalizeMetadata();
            builder.setStatsMetadata((StatsMetadata) finalMetadata.get(MetadataType.STATS));
            builder.setCompactionMetadata(Optional.ofNullable((CompactionMetadata)finalMetadata.get(MetadataType.COMPACTION)));

            rowIndexFile = indexWriter.rowIndexFHBuilder.complete();
            dataFile = openDataFile(lengthOverride, builder.getStatsMetadata());
            filter = indexWriter.getFilterCopy();

            return builder.setPartitionIndex(partitionIndex)
                          .setFirst(partitionIndex.firstKey())
                          .setLast(partitionIndex.lastKey())
                          .setRowIndexFile(rowIndexFile)
                          .setDataFile(dataFile)
                          .setFilter(filter)
                          .build(owner().orElse(null), true, true);
        }
        catch (RuntimeException | Error ex)
        {
            Throwables.closeNonNullAndAddSuppressed(ex, filter, dataFile, rowIndexFile, partitionIndex);
            // last, as it rethrows some errors (e.g. OutOfMemoryError)
            JVMStabilityInspector.inspectThrowable(ex);
            throw ex;
        }
    }

    @Override
    public void openEarly(Consumer<SSTableReader> callWhenReady)
    {
        // Because the partition index writer is one partition behind, we want the file to stop at the start of the
        // last partition that was written.
        long dataLength = partitionWriter.getInitialPosition();
        indexWriter.buildPartial(dataLength, partitionIndex ->
        {
            // openInternal takes ownership of the partial partition index and closes it if the open fails; the only
            // statement before it hands the index over is a plain setter of the file handle builder's length.
            indexWriter.rowIndexWriter.updateFileHandle(indexWriter.rowIndexFHBuilder);
            BtiTableReader reader;
            try
            {
                reader = openInternal(OpenReason.EARLY, dataLength, partitionIndex);
            }
            finally
            {
                // also when the open failed: the index handles it completed may have cached partial chunks
                indexWriter.invalidateChunkCacheIfTruncated();
            }
            callWhenReady.accept(reader);
        });
    }

    @Override
    public SSTableReader openFinalEarly()
    {
        // we must ensure the data is completely flushed to disk
        indexWriter.complete(); // This will be called by completedPartitionIndex() below too, but we want it done now to
        // ensure outstanding openEarly actions are not triggered.
        dataWriter.sync();
        // Note: Nothing must be written to any of the files after this point, as the chunk cache could pick up and
        // retain a partially-written page.

        return openFinal(OpenReason.EARLY);
    }

    @Override
    protected SSTableReader openFinal(OpenReason openReason)
    {

        if (maxDataAge < 0)
            maxDataAge = Clock.Global.currentTimeMillis();

        // If completedPartitionIndex() throws, there is no index to release; once it returns, openInternal owns it.
        PartitionIndex partitionIndex;
        try
        {
            partitionIndex = indexWriter.completedPartitionIndex();
        }
        catch (RuntimeException | Error e)
        {
            JVMStabilityInspector.inspectThrowable(e);
            throw e;
        }
        return openInternal(openReason, NO_LENGTH_OVERRIDE, partitionIndex);
    }

    @Override
    public void openResult(@javax.annotation.Nullable org.apache.cassandra.io.sstable.StorageHandler storageHandler)
    {
        txnProxy.openResult(storageHandler);
    }

    /**
     * Encapsulates writing the index and filter for an SSTable. The state of this object is not valid until it has been closed.
     */
    protected static class IndexWriter extends SortedTableWriter.AbstractIndexWriter
    {
        final SequentialWriter rowIndexWriter;
        private final FileHandle.Builder rowIndexFHBuilder;
        private final SequentialWriter partitionIndexWriter;
        private final FileHandle.Builder partitionIndexFHBuilder;
        private final PartitionIndexBuilder partitionIndex;
        boolean partitionIndexCompleted = false;
        private DataPosition riMark;
        private DataPosition piMark;

        @Nullable
        private final ChunkCache chunkCache;
        // set once resetAndTruncate has truncated the index files, see invalidateChunkCacheIfTruncated
        private boolean truncated;

        @Nullable
        private final TableMetrics tableMetrics;

        // Encryption-only metadata handed to the index FileHandle builders when encryption is enabled;
        // the builders keep this same instance until FileHandle.Builder.complete() takes its own shared
        // copy, so it must stay open for the writer's whole lifetime and be released on cleanup.
        // Null when encryption is not used.
        @Nullable
        final CompressionMetadata indexEncryptionMetadata;

        IndexWriter(Builder b, SequentialWriter dataWriter)
        {
            super(b);
            chunkCache = b.getChunkCache();

            // The indexes are encrypted with the encryptor of the parameters the data file is written with: those
            // are the ones stored in the compression info file, which readers take the index encryptor from. They
            // may differ from the table's schema parameters, as the data writer's are chosen by the pluggable
            // CompressionParams.Selector (forFlush/forCompaction).
            TableMetadata metadata = b.getTableMetadataRef().getLocal();
            CompressionParams params = dataParams(dataWriter, b.getComponents().contains(SSTableFormat.Components.COMPRESSION_INFO));
            ICompressor compressor = params.getSstableCompressor();
            ICompressor encryptor = compressor != null && b.descriptor.version.indicesAreEncrypted() ? compressor.encryptionOnly()
                                                                                                     : null;
            // Build into locals so that a failure partway through construction can release whatever was
            // already created: SortedTableWriter's constructor only sees a null indexWriter in that case
            // and cannot close any of it (including the bloom filter created by the super constructor).
            CompressionMetadata encryptionMetadata = null;
            SequentialWriter riWriter = null;
            SequentialWriter piWriter = null;
            PartitionIndexBuilder pIndexBuilder = null;
            try
            {
                if (encryptor != null)
                {
                    // Create encrypted writers and configure FileHandle builders for encryption. The index
                    // encryption helper always configures encryption here: its condition (indices encrypted and a
                    // compressor with an encryption-only part) is the one that made encryptor non-null.
                    encryptionMetadata = CompressionMetadata.encryptedOnly(params);
                    riWriter = new EncryptedSequentialWriter(descriptor.fileFor(Components.ROW_INDEX),
                                                             b.getIOOptions().writerOptions,
                                                             encryptor);
                    rowIndexFHBuilder = BtiTableReaderLoadingBuilder.withIndexEncryption(IndexComponent.fileBuilder(Components.ROW_INDEX, b, b.operationType)
                                                                                                       .withMmappedRegionsCache(b.getMmappedRegionsCache()),
                                                                                         descriptor,
                                                                                         encryptionMetadata);

                    piWriter = new EncryptedSequentialWriter(descriptor.fileFor(Components.PARTITION_INDEX),
                                                             b.getIOOptions().writerOptions,
                                                             encryptor);
                    partitionIndexFHBuilder = BtiTableReaderLoadingBuilder.withIndexEncryption(IndexComponent.fileBuilder(Components.PARTITION_INDEX, b, b.operationType)
                                                                                                             .withMmappedRegionsCache(b.getMmappedRegionsCache()),
                                                                                               descriptor,
                                                                                               encryptionMetadata);
                }
                else
                {
                    // Create regular writers
                    riWriter = new SequentialWriter(descriptor.fileFor(Components.ROW_INDEX), b.getIOOptions().writerOptions);
                    rowIndexFHBuilder = IndexComponent.fileBuilder(Components.ROW_INDEX, b, b.operationType)
                                                      .withMmappedRegionsCache(b.getMmappedRegionsCache());
                    piWriter = new SequentialWriter(descriptor.fileFor(Components.PARTITION_INDEX), b.getIOOptions().writerOptions);
                    partitionIndexFHBuilder = IndexComponent.fileBuilder(Components.PARTITION_INDEX, b, b.operationType)
                                                            .withMmappedRegionsCache(b.getMmappedRegionsCache());
                }
                pIndexBuilder = new PartitionIndexBuilder(piWriter, partitionIndexFHBuilder, descriptor.version.getByteComparableVersion());

                // register listeners to be alerted when the data files are flushed
                piWriter.setPostFlushListener(pIndexBuilder::markPartitionIndexSynced);
                riWriter.setPostFlushListener(pIndexBuilder::markRowIndexSynced);
                dataWriter.setPostFlushListener(pIndexBuilder::markDataSynced);
            }
            catch (RuntimeException | Error ex)
            {
                Throwables.closeNonNullAndAddSuppressed(ex, pIndexBuilder, piWriter, riWriter, encryptionMetadata, bf);
                throw ex;
            }
            indexEncryptionMetadata = encryptionMetadata;
            rowIndexWriter = riWriter;
            partitionIndexWriter = piWriter;
            partitionIndex = pIndexBuilder;

            // Everything from here on must not throw and must not open further resources: a failure past
            // the catch above would leak all of the above, since this instance never reaches the caller
            // and doPostCleanup never runs. In particular the bloom filter memory accounting must stay
            // last -- doPostCleanup's matching decrement does not run on the construction-failure path,
            // so the increment may only happen once construction can no longer fail.

            // The per-table bloom filter memory is tracked when:
            // 1. Periodic early open: Opens incomplete sstables when size threshold is hit during writing.
            //    The BF memory usage is tracked via Tracker.
            // 2. Completion early open: Opens completed sstables when compaction results in multiple sstables.
            //    The BF memory usage is tracked via Tracker.
            // 3. A new sstable is first created here if early-open is not enabled.
            tableMetrics = DatabaseDescriptor.getSSTablePreemptiveOpenIntervalInMiB() <= 0 ? ColumnFamilyStore.metricsForIfPresent(metadata.id) : null;
            if (tableMetrics != null && bf != null)
                tableMetrics.inFlightBloomFilterOffHeapMemoryUsed.getAndAdd(bf.offHeapSize());
        }

        public long append(DecoratedKey key, AbstractRowIndexEntry indexEntry) throws IOException
        {
            bf.add(key);
            long position;
            if (indexEntry.isIndexed())
            {
                long indexStart = rowIndexWriter.position();
                try
                {
                    ByteBufferUtil.writeWithShortLength(key.getKey(), rowIndexWriter);
                    ((TrieIndexEntry) indexEntry).serialize(rowIndexWriter, rowIndexWriter.position(), descriptor.version);
                }
                catch (IOException e)
                {
                    throw new FSWriteError(e, rowIndexWriter.getFile());
                }

                if (logger.isTraceEnabled())
                    logger.trace("wrote index entry: {} at {}", indexEntry, indexStart);
                position = indexStart;
            }
            else
            {
                // Write data position directly in trie.
                position = ~indexEntry.position;
            }
            partitionIndex.addEntry(key, position);
            return position;
        }

        public boolean buildPartial(long dataPosition, Consumer<PartitionIndex> callWhenReady)
        {
            return partitionIndex.buildPartial(callWhenReady, rowIndexWriter.position(), dataPosition);
        }

        public void mark()
        {
            riMark = rowIndexWriter.mark();
            piMark = partitionIndexWriter.mark();
        }

        public void resetAndTruncate()
        {
            // we can't un-set the bloom filter addition, but extra keys in there are harmless.
            // we can't reset dbuilder either, but that is the last thing called in after append, so
            // we assume that if that worked then we won't be trying to reset.
            // A reset within the writer's buffer changes nothing on disk. Otherwise the writer truncates the file
            // (which moves its last flush offset back) and rewrites the chunk containing the mark.
            long riFlushed = rowIndexWriter.getLastFlushOffset();
            long piFlushed = partitionIndexWriter.getLastFlushOffset();
            rowIndexWriter.resetAndTruncate(riMark);
            partitionIndexWriter.resetAndTruncate(piMark);
            if (rowIndexWriter.getLastFlushOffset() == riFlushed && partitionIndexWriter.getLastFlushOffset() == piFlushed)
                return;

            // A reader opened early may have cached the chunk containing the mark, and the cache would serve that
            // stale content to the handles opened later (as the final reader's) for the same file. Give the files a
            // fresh id in the chunk cache so that handles opened from now on do not see what was cached before.
            // Handles already open keep the old id, but they only read the index entries of partitions written
            // before the mark, which the truncation does not change.
            truncated = true;
            invalidateChunkCache();
        }

        /**
         * After a truncation, a plain (unencrypted) index writer flushes at positions that are no longer aligned to
         * the chunk size, so a reader opened early at such a boundary may cache a partial last chunk that later
         * handles for the same file would be served. Called after each early open, this makes the handles opened
         * later use a fresh chunk cache id. Without truncations flushes are aligned and cached chunks are shared.
         */
        void invalidateChunkCacheIfTruncated()
        {
            if (truncated)
                invalidateChunkCache();
        }

        /**
         * Opens a handle on what was flushed of the row index so far, like an early open does.
         */
        @VisibleForTesting
        FileHandle openRowIndexHandle()
        {
            rowIndexWriter.updateFileHandle(rowIndexFHBuilder);
            return rowIndexFHBuilder.complete();
        }

        private void invalidateChunkCache()
        {
            // This assumes that the index file handles use the builder's chunk cache, as the default StorageProvider
            // does. A provider substituting another cache in primaryIndexWriteTimeFileHandleBuilderFor must
            // invalidate that one itself (ChunkCache.invalidateFile only affects the instance it is called on).
            if (chunkCache == null)
                return;
            chunkCache.invalidateFile(descriptor.fileFor(Components.ROW_INDEX));
            chunkCache.invalidateFile(descriptor.fileFor(Components.PARTITION_INDEX));
        }

        protected void doPrepare()
        {
            flushBf();

            complete();

            // release channels and file buffers
            rowIndexWriter.prepareToCommit();
            partitionIndexWriter.prepareToCommit();
        }

        void complete() throws FSWriteError
        {
            if (partitionIndexCompleted)
                return;

            try
            {
                rowIndexWriter.sync();
                rowIndexWriter.updateFileHandle(rowIndexFHBuilder);

                partitionIndex.complete();
                partitionIndexCompleted = true;
            }
            catch (IOException e)
            {
                throw new FSWriteError(e, partitionIndexWriter.getFile());
            }
        }

        PartitionIndex completedPartitionIndex()
        {
            complete();
            try
            {
                return PartitionIndex.load(partitionIndexFHBuilder, metadata.getLocal().partitioner, false, descriptor.version.getByteComparableVersion());
            }
            catch (IOException e)
            {
                throw new FSReadError(e, partitionIndexWriter.getFile());
            }
        }

        protected Throwable doCommit(Throwable accumulate)
        {
            return rowIndexWriter.commit(accumulate);
        }

        protected Throwable doAbort(Throwable accumulate)
        {
            return rowIndexWriter.abort(accumulate);
        }

        @Override
        protected Throwable doPostCleanup(Throwable accumulate)
        {
            if (tableMetrics != null && bf != null)
                tableMetrics.inFlightBloomFilterOffHeapMemoryUsed.getAndAdd(-bf.offHeapSize());
            return Throwables.close(accumulate, bf, partitionIndex, rowIndexWriter, partitionIndexWriter, indexEncryptionMetadata);
        }
    }

    public static class Builder extends SortedTableWriter.Builder<BtiFormatPartitionWriter, IndexWriter, BtiTableWriter, Builder>
    {
        private MmappedRegionsCache mmappedRegionsCache;
        private OperationType operationType;

        private boolean dataWriterOpened;
        private boolean partitionWriterOpened;
        private boolean indexWriterOpened;

        public Builder(Descriptor descriptor)
        {
            super(descriptor);
        }

        @Override
        public Builder addDefaultComponents(Collection<Index.Group> indexGroups)
        {
            super.addDefaultComponents(indexGroups);

            addComponents(ImmutableSet.of(Components.PARTITION_INDEX, Components.ROW_INDEX));

            return this;
        }

        // The following getters for the resources opened by buildInternal method can be only used during the lifetime of
        // that method - that is, during the construction of the sstable.

        @Override
        public MmappedRegionsCache getMmappedRegionsCache()
        {
            return ensuringInBuildInternalContext(mmappedRegionsCache);
        }

        @Override
        protected OperationType getOperationType()
        {
            return ensuringInBuildInternalContext(operationType);
        }

        @Override
        protected SequentialWriter openDataWriter()
        {
            checkState(!dataWriterOpened, "Data writer has been already opened.");

            return DataComponent.buildWriter(descriptor,
                                             getTableMetadataRef().getLocal(),
                                             getIOOptions().writerOptions,
                                             getMetadataCollector(),
                                             ensuringInBuildInternalContext(operationType));
        }

        @Override
        protected IndexWriter openIndexWriter(SequentialWriter dataWriter)
        {
            checkNotNull(dataWriter);
            checkState(!indexWriterOpened, "Index writer has been already opened.");

            IndexWriter indexWriter = new IndexWriter(this, dataWriter);
            indexWriterOpened = true;
            return indexWriter;
        }

        @Override
        protected BtiFormatPartitionWriter openPartitionWriter(SequentialWriter dataWriter, IndexWriter indexWriter)
        {
            checkNotNull(dataWriter);
            checkNotNull(indexWriter);
            checkState(!partitionWriterOpened, "Partition writer has been already opened.");

            BtiFormatPartitionWriter partitionWriter = new BtiFormatPartitionWriter(getSerializationHeader(),
                                                                                    getTableMetadataRef().getLocal().comparator,
                                                                                    dataWriter,
                                                                                    indexWriter.rowIndexWriter,
                                                                                    descriptor.version);
            partitionWriterOpened = true;
            return partitionWriter;
        }

        private <T> T ensuringInBuildInternalContext(T value)
        {
            checkState(value != null, "The requested resource has not been initialized yet.");
            return value;
        }

        @Override
        protected BtiTableWriter buildInternal(LifecycleNewTracker lifecycleNewTracker, Owner owner)
        {
            try
            {
                this.mmappedRegionsCache = new MmappedRegionsCache();
                this.operationType = lifecycleNewTracker.opType();

                return new BtiTableWriter(this, lifecycleNewTracker, owner);
            }
            catch (RuntimeException | Error ex)
            {
                Throwables.closeNonNullAndAddSuppressed(ex, mmappedRegionsCache);
                throw ex;
            }
            finally
            {
                mmappedRegionsCache = null;
                partitionWriterOpened = false;
                indexWriterOpened = false;
                dataWriterOpened = false;
            }
        }
    }
}
