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
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.compress.CompressionMetadataReaderType;
import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.KeyReader;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.format.CompressionInfoComponent;
import org.apache.cassandra.io.sstable.format.FilterComponent;
import org.apache.cassandra.io.sstable.format.SortedTableReaderLoadingBuilder;
import org.apache.cassandra.io.sstable.format.StatsComponent;
import org.apache.cassandra.io.sstable.format.TOCComponent;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.sstable.format.bti.BtiFormat.Components;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.sstable.metadata.ValidationMetadata;
import org.apache.cassandra.io.sstable.metadata.ZeroCopyMetadata;
import org.apache.cassandra.io.storage.StorageProvider;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.utils.FilterFactory;
import org.apache.cassandra.utils.IFilter;
import org.apache.cassandra.utils.Throwables;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class BtiTableReaderLoadingBuilder extends SortedTableReaderLoadingBuilder<BtiTableReader, BtiTableReader.Builder>
{
    private final static Logger logger = LoggerFactory.getLogger(BtiTableReaderLoadingBuilder.class);

    private FileHandle.Builder partitionIndexFileBuilder;
    private FileHandle.Builder rowIndexFileBuilder;

    public BtiTableReaderLoadingBuilder(SSTable.Builder<?, ?> builder)
    {
        super(builder);
    }

    @Override
    public KeyReader buildKeyReader(TableMetrics tableMetrics) throws IOException
    {
        StatsComponent statsComponent = StatsComponent.load(descriptor, MetadataType.STATS, MetadataType.HEADER, MetadataType.VALIDATION);
        return createKeyReader(statsComponent.statsMetadata());
    }

    private KeyReader createKeyReader(StatsMetadata statsMetadata) throws IOException
    {
        checkNotNull(statsMetadata);

        try (CompressionMetadata compressionMetadata = CompressionInfoComponent.maybeLoad(descriptor, components, statsMetadata.zeroCopyMetadata);
             CompressionMetadata indexEncryptionMetadata = indexEncryptionMetadata(compressionMetadata);
             PartitionIndex index = PartitionIndex.load(partitionIndexFileBuilder(indexEncryptionMetadata), tableMetadataRef.getLocal().partitioner, false, descriptor.version.getByteComparableVersion());
             FileHandle dFile = dataFileBuilder(statsMetadata).withCompressionMetadata(compressionMetadata)
                                                              .withCrcCheckChance(() -> tableMetadataRef.getLocal().params.crcCheckChance)
                                                              .complete();
             FileHandle riFile = rowIndexFileBuilder(indexEncryptionMetadata).complete())
        {
            return PartitionIterator.create(index,
                                            tableMetadataRef.getLocal().partitioner,
                                            riFile,
                                            dFile,
                                            descriptor.version);
        }
    }

    @Override
    protected void openComponents(BtiTableReader.Builder builder, SSTable.Owner owner, boolean validate, boolean online) throws IOException
    {
        try
        {
            StatsComponent statsComponent = StatsComponent.load(descriptor, MetadataType.STATS, MetadataType.VALIDATION, MetadataType.HEADER, MetadataType.COMPACTION);
            builder.setSerializationHeader(statsComponent.serializationHeader(descriptor, builder.getTableMetadataRef().getLocal(), !online));
            checkArgument(!online || builder.getSerializationHeader() != null);

            builder.setStatsMetadata(statsComponent.statsMetadata());
            builder.setCompactionMetadata(Optional.ofNullable(statsComponent.compactionMetadata()));
            ValidationMetadata validationMetadata = statsComponent.validationMetadata();
            validatePartitioner(builder.getTableMetadataRef().getLocal(), validationMetadata);

            boolean filterNeeded = online;
            if (filterNeeded)
                builder.setFilter(loadFilter(validationMetadata));
            boolean rebuildFilter = filterNeeded && builder.getFilter() == null;

            if (builder.getComponents().contains(Components.PARTITION_INDEX) && builder.getComponents().contains(Components.ROW_INDEX) && rebuildFilter)
            {
                IFilter filter = buildBloomFilter(statsComponent.statsMetadata());
                builder.setFilter(filter);
                FilterComponent.save(filter, descriptor, false);
                if (validationMetadata.bloomFilterFPChance != tableMetadataRef.getLocal().params.bloomFilterFpChance)
                {
                    StatsComponent.load(descriptor, MetadataType.values())
                                  .with(validationMetadata.withBloomFilterFPChance(tableMetadataRef.getLocal().params.bloomFilterFpChance))
                                  .save(descriptor);
                }
                if (descriptor.fileFor(Components.FILTER).exists())
                    TOCComponent.maybeAdd(descriptor, BigFormat.Components.FILTER);
            }


            if (builder.getFilter() == null)
                builder.setFilter(FilterFactory.AlwaysPresent);

            if (descriptor.version.hasKeyRange() && builder.getStatsMetadata() != null)
            {
                IPartitioner partitioner = tableMetadataRef.getLocal().partitioner;
                builder.setFirst(partitioner.decorateKey(builder.getStatsMetadata().firstKey));
                builder.setLast(partitioner.decorateKey(builder.getStatsMetadata().lastKey));
            }

            try (CompressionMetadata compressionMetadata = CompressionInfoComponent.maybeLoad(descriptor, components, statsComponent.statsMetadata().zeroCopyMetadata);
                 CompressionMetadata indexEncryptionMetadata = indexEncryptionMetadata(compressionMetadata))
            {
                builder.setDataFile(dataFileBuilder(builder.getStatsMetadata())
                                    .withCompressionMetadata(compressionMetadata)
                                    .withCrcCheckChance(() -> tableMetadataRef.getLocal().params.crcCheckChance)
                                    .complete());

                if (builder.getComponents().contains(Components.ROW_INDEX))
                    builder.setRowIndexFile(rowIndexFileBuilder(indexEncryptionMetadata).complete());

                if (builder.getComponents().contains(Components.PARTITION_INDEX))
                {
                    builder.setPartitionIndex(openPartitionIndex(indexEncryptionMetadata, !builder.getFilter().isInformative(), statsComponent.statsMetadata().zeroCopyMetadata));
                    if (builder.getFirst() == null || builder.getLast() == null)
                    {
                        builder.setFirst(builder.getPartitionIndex().firstKey());
                        builder.setLast(builder.getPartitionIndex().lastKey());
                    }
                }
            }
        }
        catch (IOException | RuntimeException | Error ex)
        {
            // in case of failure, close only those components which have been opened in this try-catch block
            Throwables.closeNonNullAndAddSuppressed(ex, builder.getPartitionIndex(), builder.getRowIndexFile(), builder.getDataFile(), builder.getFilter());
            throw ex;
        }
    }

    private IFilter buildBloomFilter(StatsMetadata statsMetadata) throws IOException
    {
        IFilter bf = null;

        try (KeyReader keyReader = createKeyReader(statsMetadata))
        {
            bf = FilterFactory.getFilter(statsMetadata.totalRows, tableMetadataRef.getLocal().params.bloomFilterFpChance);

            while (!keyReader.isExhausted())
            {
                DecoratedKey key = tableMetadataRef.getLocal().partitioner.decorateKey(keyReader.key());
                bf.add(key);

                keyReader.advance();
            }
        }
        catch (IOException | RuntimeException | Error ex)
        {
            Throwables.closeNonNullAndAddSuppressed(ex, bf);
            throw ex;
        }

        return bf;
    }

    /**
     * The index files only need the encryptor of the data file's compression parameters: hand them an
     * {@link CompressionMetadata#encryptedOnly(CompressionParams) encryption-only} copy of the parameters rather
     * than the data file's metadata, so that the index handles do not pin (and expose through
     * {@link FileHandle#compressionMetadata()}) the data file's chunk offsets. The result carries no off-heap memory;
     * the caller closes it once the index handles are complete (they take their own shared copy).
     *
     * @return the encryption-only metadata, or {@code null} if the sstable is not compressed or its indexes are not
     * encrypted
     */
    @Nullable
    private CompressionMetadata indexEncryptionMetadata(@Nullable CompressionMetadata compressionMetadata)
    {
        if (compressionMetadata == null || !descriptor.version.indicesAreEncrypted())
            return null;
        ICompressor compressor = compressionMetadata.parameters.getSstableCompressor();
        if (compressor == null || compressor.encryptionOnly() == null)
            return null;
        return CompressionMetadata.encryptedOnly(compressionMetadata.parameters);
    }

    private PartitionIndex openPartitionIndex(@Nullable CompressionMetadata indexEncryptionMetadata, boolean preload, ZeroCopyMetadata zeroCopyMetadata) throws IOException
    {
        try (FileHandle indexFile = partitionIndexFileBuilder(indexEncryptionMetadata).complete())
        {
            return PartitionIndex.load(indexFile, tableMetadataRef.getLocal().partitioner, preload, zeroCopyMetadata, descriptor.version.getByteComparableVersion());
        }
        catch (IOException ex)
        {
            logger.debug("Partition index file is corrupted: " + descriptor.fileFor(Components.PARTITION_INDEX), ex);
            throw ex;
        }
    }

    private FileHandle.Builder rowIndexFileBuilder(@Nullable CompressionMetadata indexEncryptionMetadata)
    {
        assert rowIndexFileBuilder == null || rowIndexFileBuilder.file.equals(descriptor.fileFor(Components.ROW_INDEX));

        if (rowIndexFileBuilder == null)
            rowIndexFileBuilder = StorageProvider.instance.fileHandleBuilderFor(descriptor, Components.ROW_INDEX);

        rowIndexFileBuilder.withChunkCache(chunkCache);
        rowIndexFileBuilder.mmapped(ioOptions.indexDiskAccessMode);
        // The builder is reused: do not keep the (closed) metadata of a previous call. Its encryptionOnly flag
        // cannot be cleared, but it never needs to be: whether the indexes are encrypted is fixed for the
        // descriptor and components this loader is built for.
        rowIndexFileBuilder.withCompressionMetadata(null);
        return withIndexEncryption(rowIndexFileBuilder, descriptor, indexEncryptionMetadata);
    }

    private FileHandle.Builder partitionIndexFileBuilder(@Nullable CompressionMetadata indexEncryptionMetadata)
    {
        assert partitionIndexFileBuilder == null || partitionIndexFileBuilder.file.equals(descriptor.fileFor(Components.PARTITION_INDEX));

        if (partitionIndexFileBuilder == null)
            partitionIndexFileBuilder = StorageProvider.instance.fileHandleBuilderFor(descriptor, Components.PARTITION_INDEX);

        partitionIndexFileBuilder.withChunkCache(chunkCache);
        partitionIndexFileBuilder.mmapped(ioOptions.indexDiskAccessMode);
        // The builder is reused: do not keep the (closed) metadata of a previous call. Its encryptionOnly flag
        // cannot be cleared, but it never needs to be: whether the indexes are encrypted is fixed for the
        // descriptor and components this loader is built for.
        partitionIndexFileBuilder.withCompressionMetadata(null);
        return withIndexEncryption(partitionIndexFileBuilder, descriptor, indexEncryptionMetadata);
    }

    /**
     * Configures a builder of one of the index files of the given sstable ({@link Components#PARTITION_INDEX} or
     * {@link Components#ROW_INDEX}) to decrypt the file, if the sstable's indexes are encrypted. Every opener of an
     * existing sstable's index files must go through this method: an encrypted index read without it is read as
     * plaintext garbage. {@link BtiTableWriter.IndexWriter} uses it too, for the builders of the early-open index
     * handles, passing encryption-only metadata built from the compression parameters of the data writer (the ones
     * stored in the compression info file).
     * <p>
     * When the sstable is not being fully opened (e.g. by offline tools), the compression metadata can be obtained
     * with {@link #maybeLoadIndexEncryptionMetadata(Descriptor)}.
     *
     * @param builder             the builder of the index file handle
     * @param descriptor          the sstable the index file belongs to
     * @param compressionMetadata the compression metadata of the sstable (only its parameters are used, so an
     *                            {@link CompressionMetadata#encryptedOnly(CompressionParams) encryption-only} instance
     *                            is enough), or {@code null} if the sstable is not compressed; the caller keeps
     *                            ownership of it and must close it after the builder has completed
     * @return the given builder
     */
    public static FileHandle.Builder withIndexEncryption(FileHandle.Builder builder, Descriptor descriptor, @Nullable CompressionMetadata compressionMetadata)
    {
        if (compressionMetadata != null && descriptor.version.indicesAreEncrypted())
        {
            ICompressor compressor = compressionMetadata.parameters.getSstableCompressor();
            if (compressor != null && compressor.encryptionOnly() != null)
                builder.withCompressionMetadata(compressionMetadata).encryptionOnly();
        }
        return builder;
    }

    /**
     * Reads the encryption parameters of the indexes of the given sstable, for use with
     * {@link #withIndexEncryption(FileHandle.Builder, Descriptor, CompressionMetadata)} when the sstable is not
     * being fully opened (e.g. by offline tools). Only the header of the compression info file is read: no chunk
     * offsets are loaded.
     * <p>
     * An sstable without a compression info file is treated as not encrypted, unless its TOC lists one: then the
     * file is missing and a {@link CorruptSSTableException} is thrown rather than reading encrypted indexes as
     * plaintext. This check is best-effort: if the sstable has no TOC, the check is skipped and the indexes are
     * treated as not encrypted.
     *
     * @return an {@link CompressionMetadata#encryptedOnly(CompressionParams) encryption-only} compression metadata
     * that the caller must close, or {@code null} if the indexes of the sstable are not encrypted
     * @throws CorruptSSTableException if the TOC lists a compression info file that does not exist
     */
    @Nullable
    public static CompressionMetadata maybeLoadIndexEncryptionMetadata(Descriptor descriptor)
    {
        if (!descriptor.version.indicesAreEncrypted())
            return null;

        CompressionParams params = CompressionInfoComponent.readCompressionParamsIfExists(descriptor, CompressionMetadataReaderType.READ_TIME);
        if (params == null)
        {
            // CompressionInfo.db is known to be absent here, so there is no need to probe the other components:
            // fail if the TOC says it should be there
            CompressionInfoComponent.verifyCompressionInfoExistenceIfApplicable(descriptor, Set.of());
            return null;
        }

        ICompressor compressor = params.getSstableCompressor();
        if (compressor == null || compressor.encryptionOnly() == null)
            return null;

        return CompressionMetadata.encryptedOnly(params);
    }
}
