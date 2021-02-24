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
package org.apache.cassandra.io.sstable.format.trieindex;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cache.ChunkCache;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.rows.RangeTombstoneBoundMarker;
import org.apache.cassandra.db.rows.RangeTombstoneBoundaryMarker;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.compress.CompressedSequentialWriter;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.format.RowIndexEntry;
import org.apache.cassandra.io.sstable.format.SSTableFlushObserver;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.format.big.IndexInfo;
import org.apache.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.util.BufferedDataOutputStreamPlus;
import org.apache.cassandra.io.util.ChecksummedSequentialWriter;
import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.io.util.DataPosition;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.io.util.SequentialWriterOption;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableParams;
import org.apache.cassandra.utils.BloomFilter;
import org.apache.cassandra.utils.BloomFilterSerializer;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.FilterFactory;
import org.apache.cassandra.utils.IFilter;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.SyncUtil;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.concurrent.Transactional;

@VisibleForTesting
public class TrieIndexSSTableWriter extends SSTableWriter
{
    private static final Logger logger = LoggerFactory.getLogger(TrieIndexSSTableWriter.class);

    private final PartitionWriter partitionWriter;
    private final IndexWriter iwriter;
    private final FileHandle.Builder dbuilder;
    protected final SequentialWriter dataFile;
    private DataPosition dataMark;
    private long lastEarlyOpenLength = 0;
    private final Optional<ChunkCache> chunkCache = Optional.ofNullable(ChunkCache.instance);

    private DecoratedKey currentKey;
    private DeletionTime currentPartitionLevelDeletion;
    private long currentStartPosition;

    private static final SequentialWriterOption WRITER_OPTION = SequentialWriterOption.newBuilder()
                                                                                      .trickleFsync(DatabaseDescriptor.getTrickleFsync())
                                                                                      .trickleFsyncByteInterval(DatabaseDescriptor.getTrickleFsyncIntervalInKb() * 1024)
                                                                                      .bufferType(BufferType.OFF_HEAP)
                                                                                      .build();

    public TrieIndexSSTableWriter(Descriptor descriptor,
                                  SSTableWriterCreationHelper helper,
                                  Collection<SSTableFlushObserver> observers,
                                  Set<Component> indexComponents,
                                  SSTableTracker sstableTracker)
    {
        super(descriptor, components(helper.table(), indexComponents), helper, observers, sstableTracker);

        if (compression)
        {
            CompressionParams compressionParams = helper.table().params.get(TableParams.COMPRESSION);
            dataFile = new CompressedSequentialWriter(getFile(),
                                                      descriptor.filenameFor(Component.COMPRESSION_INFO),
                                                      descriptor.filenameFor(Component.DIGEST),
                                                      WRITER_OPTION,
                                                      compressionParams,
                                                      metadataCollector);
        }
        else
        {
            dataFile = new ChecksummedSequentialWriter(getFile(),
                                                       descriptor.filenameFor(Component.CRC),
                                                       descriptor.filenameFor(Component.DIGEST),
                                                       WRITER_OPTION);
        }
        dbuilder = new FileHandle.Builder(descriptor.filenameFor(Component.DATA)).compressed(compression)
                                                                                 .mmapped(metadata.get().diskAccessMode == Config.AccessMode.mmap);
        chunkCache.ifPresent(dbuilder::withChunkCache);
        iwriter = new IndexWriter(helper.table());

        partitionWriter = new PartitionWriter(this.header, metadata().comparator, dataFile, iwriter.rowIndexFile, descriptor.version, this.observers);
    }

    private static Set<Component> components(TableMetadata metadata, Set<Component> indexComponents)
    {
        Set<Component> components = SetsFactory.setFromArray(Component.DATA,
                                                             Component.PARTITION_INDEX,
                                                             Component.ROW_INDEX,
                                                             Component.STATS,
                                                             Component.TOC,
                                                             Component.DIGEST);

        if (metadata.params.getDouble(TableParams.BLOOM_FILTER_FP_CHANCE) < 1.0)
            components.add(Component.FILTER);

        if (metadata.params.get(TableParams.COMPRESSION).isEnabled())
        {
            components.add(Component.COMPRESSION_INFO);
        }
        else
        {
            // it would feel safer to actually add this component later in maybeWriteDigest(),
            // but the components are unmodifiable after construction
            components.add(Component.CRC);
        }

        components.addAll(indexComponents);

        return components;
    }

    public void mark()
    {
        dataMark = dataFile.mark();
        iwriter.mark();
    }

    public void resetAndTruncate()
    {
        dataFile.resetAndTruncate(dataMark);
        iwriter.resetAndTruncate();
    }

    /**
     * Perform sanity checks on @param decoratedKey and @return the position in the data file before any data is written
     */
    protected void checkKeyOrder(DecoratedKey decoratedKey)
    {
        assert decoratedKey != null : "Keys must not be null"; // empty keys ARE allowed b/c of indexed row values
        if (currentKey != null && currentKey.compareTo(decoratedKey) >= 0)
            throw new RuntimeException("Last written key " + currentKey + " >= current key " + decoratedKey + " writing into " + getFile());
    }

    public boolean startPartition(DecoratedKey key, DeletionTime partitionLevelDeletion) throws IOException
    {
        if (key.getKeyLength() > FBUtilities.MAX_UNSIGNED_SHORT)
        {
            logger.error("Key size {} exceeds maximum of {}, skipping row", key.getKeyLength(), FBUtilities.MAX_UNSIGNED_SHORT);
            return false;
        }

        checkKeyOrder(key);
        currentKey = key;
        currentPartitionLevelDeletion = partitionLevelDeletion;
        currentStartPosition = dataFile.position();
        if (!observers.isEmpty())
            observers.forEach(o -> o.startPartition(key, currentStartPosition));

        // Reuse the writer for each row
        partitionWriter.reset();

        partitionWriter.writePartitionHeader(key, partitionLevelDeletion);

        metadataCollector.updatePartitionDeletion(partitionLevelDeletion);
        return true;
    }

    public void addUnfiltered(Unfiltered unfiltered) throws IOException
    {
        SSTableWriter.guardCollectionSize(metadata(), currentKey, unfiltered);

        if (unfiltered.isRow())
        {
            Row row = (Row) unfiltered;
            metadataCollector.updateMinMaxClustering(row.clustering());
            Rows.collectStats(row, metadataCollector);
        }
        else
        {
            RangeTombstoneMarker marker = (RangeTombstoneMarker) unfiltered;
            metadataCollector.updateMinMaxClustering(marker.clustering());
            if (marker.isBoundary())
            {
                RangeTombstoneBoundaryMarker bm = (RangeTombstoneBoundaryMarker) marker;
                metadataCollector.update(bm.endDeletionTime());
                metadataCollector.update(bm.startDeletionTime());
            }
            else
            {
                metadataCollector.update(((RangeTombstoneBoundMarker) marker).deletionTime());
            }
        }

        partitionWriter.addUnfiltered(unfiltered);
    }

    public RowIndexEntry endPartition() throws IOException
    {
        metadataCollector.addCellPerPartitionCount();

        long trieRoot = partitionWriter.finish();
        RowIndexEntry entry = TrieIndexEntry.create(currentStartPosition, trieRoot,
                                                    currentPartitionLevelDeletion,
                                                    partitionWriter.rowIndexCount);

        long endPosition = dataFile.position();
        long partitionSize = endPosition - currentStartPosition;
        maybeLogLargePartitionWarning(currentKey, partitionSize);
        metadataCollector.addPartitionSizeInBytes(partitionSize);
        metadataCollector.addKeyHash(currentKey.hash2_64());
        last = currentKey;
        if (first == null)
            first = currentKey;

        if (logger.isTraceEnabled())
            logger.trace("wrote {} at {}", currentKey, entry.position);
        iwriter.append(currentKey, entry);
        return entry;
    }

    private void maybeLogLargePartitionWarning(DecoratedKey key, long rowSize)
    {
        if (SchemaConstants.isInternalKeyspace(metadata().keyspace))
            return;

        if (Guardrails.partitionSize.triggersOn(rowSize, null))
        {
            String keyString = metadata().partitionKeyAsCQLLiteral(key.getTempKey());
            Guardrails.partitionSize.guard(rowSize, String.format("%s in %s", keyString, metadata), true, null);

            for (SSTableWriteWarning listener : warningListeners)
            {
                try
                {
                    listener.largePartitionWarning(metadata.keyspace, metadata.name, key, rowSize);
                }
                catch (Throwable t)
                {
                    JVMStabilityInspector.inspectThrowable(t);
                    logger.warn("Problem notifying listeners about wide row threshold", t);
                }
            }
        }
    }

    @SuppressWarnings("resource")
    public boolean openEarly(Consumer<SSTableReader> callWhenReady)
    {
        long dataLength = dataFile.position();

        dataFile.requestSyncOnNextFlush();
        iwriter.rowIndexFile.requestSyncOnNextFlush();
        iwriter.partitionIndexFile.requestSyncOnNextFlush();

        return iwriter.buildPartial(dataLength, partitionIndex ->
        {
            // useful for debugging problems with the trie index
            //partitionIndex.dumpTrie(descriptor.filenameFor(Component.PARTITION_INDEX) + ".txt");

            StatsMetadata stats = statsMetadata();
            iwriter.rowIndexFile.updateFileHandle(iwriter.rowIndexFHBuilder);
            FileHandle ifile = iwriter.rowIndexFHBuilder.complete();
            // With trie indices it is no longer necessary to limit the file size; just make sure indices and data
            // get updated length / compression metadata.
            dataFile.updateFileHandle(dbuilder);
            int dataBufferSize = optimizationStrategy.bufferSize(stats.estimatedPartitionSize.percentile(DatabaseDescriptor.getDiskOptimizationEstimatePercentile()));
            FileHandle dfile = dbuilder.bufferSize(dataBufferSize).complete();
            invalidateCacheAtBoundary(dfile);
            SSTableReader sstable = TrieIndexSSTableReader.internalOpen(descriptor,
                                                               components, metadata,
                                                               ifile, dfile, partitionIndex, iwriter.bf.sharedCopy(),
                                                               maxDataAge, stats, SSTableReader.OpenReason.EARLY, header);

            sstable.first = getMinimalKey(partitionIndex.firstKey());
            sstable.last = getMinimalKey(partitionIndex.lastKey());
            callWhenReady.accept(sstable);
        });
    }

    void invalidateCacheAtBoundary(FileHandle dfile)
    {
        if (lastEarlyOpenLength != 0 && dfile.dataLength() > lastEarlyOpenLength)
        {
            dfile.invalidateIfCached(lastEarlyOpenLength);
        }

        lastEarlyOpenLength = dfile.dataLength();
    }

    public SSTableReader openFinalEarly()
    {
        // we must ensure the data is completely flushed to disk
        iwriter.complete(); // This will be called by completedPartitionIndex() below too, but we want it done now to
                            // ensure outstanding openEarly actions are not triggered.
        dataFile.sync();
        iwriter.rowIndexFile.sync();
        // Note: Nothing must be written to any of the files after this point, as the chunk cache could pick up and
        // retain a partially-written page (see DB-2446).

        return openFinal(SSTableReader.OpenReason.EARLY);
    }

    @SuppressWarnings("resource")
    private SSTableReader openFinal(SSTableReader.OpenReason openReason)
    {
        if (maxDataAge < 0)
            maxDataAge = System.currentTimeMillis();

        StatsMetadata stats = statsMetadata();
        // finalize in-memory state for the reader
        PartitionIndex partitionIndex = iwriter.completedPartitionIndex();
        iwriter.rowIndexFile.updateFileHandle(iwriter.rowIndexFHBuilder);
        FileHandle rowIndexFile = iwriter.rowIndexFHBuilder.complete();
        dataFile.updateFileHandle(dbuilder);
        int dataBufferSize = optimizationStrategy.bufferSize(stats.estimatedPartitionSize.percentile(DatabaseDescriptor.getDiskOptimizationEstimatePercentile()));
        FileHandle dfile = dbuilder.bufferSize(dataBufferSize).complete();
        invalidateCacheAtBoundary(dfile);
        SSTableReader sstable = TrieIndexSSTableReader.internalOpen(descriptor,
                                                            components,
                                                            this.metadata,
                                                            rowIndexFile,
                                                            dfile,
                                                            partitionIndex,
                                                            iwriter.bf.sharedCopy(),
                                                            maxDataAge,
                                                            stats,
                                                            openReason,
                                                            header);
        sstable.first = getMinimalKey(first);
        sstable.last = getMinimalKey(last);
        return sstable;
    }

    protected SSTableWriter.TransactionalProxy txnProxy()
    {
        return new TransactionalProxy();
    }

    class TransactionalProxy extends SSTableWriter.TransactionalProxy
    {
        // finalise our state on disk, including renaming
        protected void doPrepare()
        {
            iwriter.prepareToCommit();

            // write sstable statistics
            dataFile.prepareToCommit();
            writeMetadata(descriptor, finalizeMetadata());

            // save the table of components
            SSTable.appendTOC(descriptor, components);

            if (openResult)
                finalReader = openFinal(SSTableReader.OpenReason.NORMAL);
        }

        protected Throwable doCommit(Throwable accumulate)
        {
            accumulate = writerTidier.commit(accumulate);
            accumulate = dataFile.commit(accumulate);
            accumulate = iwriter.commit(accumulate);
            return accumulate;
        }

        @Override
        protected Throwable doPostCleanup(Throwable accumulate)
        {
            partitionWriter.close();
            accumulate = dbuilder.close(accumulate);
            return accumulate;
        }

        protected Throwable doAbort(Throwable accumulate)
        {
            accumulate = writerTidier.abort(accumulate);
            accumulate = iwriter.abort(accumulate);
            accumulate = dataFile.abort(accumulate);
            return accumulate;
        }
    }

    private void writeMetadata(Descriptor desc, Map<MetadataType, MetadataComponent> components)
    {
        File file = desc.filenameFor(Component.STATS);
        try (SequentialWriter out = new SequentialWriter(file, WRITER_OPTION))
        {
            desc.getMetadataSerializer().serialize(components, out, desc);
            out.finish();
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, file);
        }
    }

    public long getFilePointer()
    {
        return dataFile.position();
    }

    public long getOnDiskFilePointer()
    {
        return dataFile.getOnDiskFilePointer();
    }

    public long getEstimatedOnDiskBytesWritten()
    {
        return dataFile.getEstimatedOnDiskBytesWritten();
    }

    /**
     * Encapsulates writing the index and filter for an SSTable. The state of this object is not valid until it has been closed.
     */
    class IndexWriter extends AbstractTransactional implements Transactional
    {
        private final SequentialWriter rowIndexFile;
        public final FileHandle.Builder rowIndexFHBuilder;
        private final SequentialWriter partitionIndexFile;
        public final FileHandle.Builder partitionIndexFHBuilder;
        public final PartitionIndexBuilder partitionIndex;
        public final IFilter bf;
        boolean partitionIndexCompleted = false;
        private DataPosition riMark, piMark;

        IndexWriter(TableMetadata table)
        {
            CompressionParams params = table.params.get(TableParams.COMPRESSION);
            ICompressor encryptor = compression ? params.getSstableCompressor().encryptionOnly() : null;
            if (encryptor != null)
            {
                CompressionMetadata compressionMetadata = CompressionMetadata.encryptedOnly(params);
                rowIndexFile = new EncryptedSequentialWriter(descriptor.filenameFor(Component.ROW_INDEX), WRITER_OPTION, encryptor);
                rowIndexFHBuilder = SSTableReader.indexFileHandleBuilder(descriptor, table, Component.ROW_INDEX, true);
                rowIndexFHBuilder.withCompressionMetadata(compressionMetadata);
                partitionIndexFile = new EncryptedSequentialWriter(descriptor.filenameFor(Component.PARTITION_INDEX), WRITER_OPTION, encryptor);
                partitionIndexFHBuilder = SSTableReader.indexFileHandleBuilder(descriptor, table, Component.PARTITION_INDEX, true);
                partitionIndexFHBuilder.withCompressionMetadata(compressionMetadata);
            }
            else
            {
                rowIndexFile = new SequentialWriter(descriptor.filenameFor(Component.ROW_INDEX), WRITER_OPTION);
                rowIndexFHBuilder = SSTableReader.indexFileHandleBuilder(descriptor, table, Component.ROW_INDEX, false);
                partitionIndexFile = new SequentialWriter(descriptor.filenameFor(Component.PARTITION_INDEX), WRITER_OPTION);
                partitionIndexFHBuilder = SSTableReader.indexFileHandleBuilder(descriptor, table, Component.PARTITION_INDEX, false);
            }
            partitionIndex = new PartitionIndexBuilder(partitionIndexFile, partitionIndexFHBuilder);
            bf = FilterFactory.getFilter(keyCount, table.params.getDouble(TableParams.BLOOM_FILTER_FP_CHANCE));
            // register listeners to be alerted when the data files are flushed
            partitionIndexFile.setFileSyncListener(() -> partitionIndex.markPartitionIndexSynced(partitionIndexFile.getLastFlushOffset()));
            rowIndexFile.setFileSyncListener(() -> partitionIndex.markRowIndexSynced(rowIndexFile.getLastFlushOffset()));
            dataFile.setFileSyncListener(() -> partitionIndex.markDataSynced(dataFile.getLastFlushOffset()));
        }

        public long append(DecoratedKey key, RowIndexEntry<IndexInfo> indexEntry) throws IOException
        {
            bf.add(key);
            long position;
            if (indexEntry.isIndexed())
            {
                long indexStart = rowIndexFile.position();
                try
                {
                    ByteBufferUtil.writeWithShortLength(key.getTempKey(), rowIndexFile);
                    indexEntry.serialize(rowIndexFile, rowIndexFile.position());
                }
                catch (IOException e)
                {
                    throw new FSWriteError(e, rowIndexFile.getPath());
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
            return partitionIndex.buildPartial(callWhenReady, rowIndexFile.position(), dataPosition);
        }

        /**
         * Closes the index and bloomfilter, making the public state of this writer valid for consumption.
         */
        void flushBf()
        {
            if (components.contains(Component.FILTER))
            {
                File path = descriptor.filenameFor(Component.FILTER);
                try (SeekableByteChannel fos = Files.newByteChannel(path.toPath(), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
                     DataOutputStreamPlus stream = new BufferedDataOutputStreamPlus(fos))
                {
                    // bloom filter
                    BloomFilterSerializer.serialize((BloomFilter) bf, stream, descriptor.version.hasOldBfFormat());
                    stream.flush();
                    SyncUtil.sync((FileChannel) fos);
                }
                catch (IOException e)
                {
                    throw new FSWriteError(e, path);
                }
            }
        }

        public void mark()
        {
            riMark = rowIndexFile.mark();
            piMark = partitionIndexFile.mark();
        }

        public void resetAndTruncate()
        {
            // we can't un-set the bloom filter addition, but extra keys in there are harmless.
            // we can't reset dbuilder either, but that is the last thing called in afterappend so
            // we assume that if that worked then we won't be trying to reset.
            rowIndexFile.resetAndTruncate(riMark);
            partitionIndexFile.resetAndTruncate(piMark);
        }

        protected void doPrepare()
        {
            flushBf();

            // truncate index file
            rowIndexFile.prepareToCommit();
            rowIndexFile.updateFileHandle(rowIndexFHBuilder);

            complete();
        }

        void complete() throws FSWriteError
        {
            if (partitionIndexCompleted)
                return;

            try
            {
                partitionIndex.complete();
                partitionIndexCompleted = true;
            }
            catch (IOException e)
            {
                throw new FSWriteError(e, partitionIndexFile.getPath());
            }
        }

        PartitionIndex completedPartitionIndex()
        {
            complete();
            try
            {
                return PartitionIndex.load(partitionIndexFHBuilder, getPartitioner(), ZeroCopyMetadata.EMPTY, false);
            }
            catch (IOException e)
            {
                throw new FSReadError(e, partitionIndexFile.getPath());
            }
        }

        protected Throwable doCommit(Throwable accumulate)
        {
            return rowIndexFile.commit(accumulate);
        }

        protected Throwable doAbort(Throwable accumulate)
        {
            return rowIndexFile.abort(accumulate);
        }

        @Override
        protected Throwable doPostCleanup(Throwable accumulate)
        {
            return Throwables.close(accumulate, UnmodifiableArrayList.of(bf, partitionIndex, rowIndexFile, rowIndexFHBuilder, partitionIndexFile, partitionIndexFHBuilder));
        }
    }
}
