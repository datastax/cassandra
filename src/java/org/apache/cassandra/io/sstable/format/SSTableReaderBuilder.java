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

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.Downsampling;
import org.apache.cassandra.io.sstable.IndexSummary;
import org.apache.cassandra.io.sstable.IndexSummaryBuilder;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.format.big.BigTableReader;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.sstable.metadata.ValidationMetadata;
import org.apache.cassandra.io.storage.StorageProvider;
import org.apache.cassandra.io.sstable.metadata.ZeroCopyMetadata;
import org.apache.cassandra.io.util.DiskOptimizationStrategy;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileInputStreamPlus;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.BloomFilter;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.FilterFactory;
import org.apache.cassandra.utils.IFilter;
import org.apache.cassandra.utils.JVMStabilityInspector;

public abstract class SSTableReaderBuilder
{
    private static final Logger logger = LoggerFactory.getLogger(SSTableReaderBuilder.class);

    protected final SSTableReader.Factory readerFactory;
    protected final Descriptor descriptor;
    protected final TableMetadataRef metadataRef;
    protected final TableMetadata metadata;
    protected final long maxDataAge;
    protected final Set<Component> components;
    protected final StatsMetadata statsMetadata;
    protected final SSTableReader.OpenReason openReason;
    protected final SerializationHeader header;

    protected IndexSummary summary;
    protected DecoratedKey first;
    protected DecoratedKey last;
    protected IFilter bf;
    protected FileHandle ifile;
    protected FileHandle dfile;

    public SSTableReaderBuilder(Descriptor descriptor,
                                TableMetadataRef metadataRef,
                                long maxDataAge,
                                Set<Component> components,
                                StatsMetadata statsMetadata,
                                SSTableReader.OpenReason openReason,
                                SerializationHeader header)
    {
        this.descriptor = descriptor;
        this.metadataRef = metadataRef;
        this.metadata = metadataRef.get();
        this.maxDataAge = maxDataAge;
        this.components = components;
        this.statsMetadata = statsMetadata;
        this.openReason = openReason;
        this.header = header;
        this.readerFactory = descriptor.getFormat().getReaderFactory();
    }

    public abstract SSTableReader build();

    public static FileHandle.Builder defaultIndexHandleBuilder(Descriptor descriptor, Component component, boolean dataFileCompressed)
    {
        return StorageProvider.instance.fileHandleBuilderFor(descriptor, component).maybeEncrypted(dataFileCompressed && descriptor.version.indicesAreEncrypted());
    }

    public static FileHandle.Builder primaryIndexWriteTimeBuilder(Descriptor descriptor, Component component, OperationType operationType, boolean dataFileCompressed)
    {
        return StorageProvider.instance.primaryIndexWriteTimeFileHandleBuilderFor(descriptor, component, operationType).maybeEncrypted(dataFileCompressed && descriptor.version.indicesAreEncrypted());
    }

    @SuppressWarnings("resource")
    public static FileHandle.Builder defaultDataHandleBuilder(Descriptor descriptor, ZeroCopyMetadata zeroCopyMetadata)
    {
        return StorageProvider.instance.fileHandleBuilderFor(descriptor, Component.DATA, zeroCopyMetadata);
    }

    /**
     * Load index summary, first key and last key from Summary.db file if it exists.
     *
     * if loaded index summary has different index interval from current value stored in schema,
     * then Summary.db file will be deleted and need to be rebuilt.
     */
    void loadSummary()
    {
        File summariesFile = descriptor.fileFor(Component.SUMMARY);
        if (!summariesFile.exists())
        {
            if (logger.isDebugEnabled())
                logger.debug("SSTable Summary File {} does not exist", summariesFile.absolutePath());
            return;
        }

        DataInputStream iStream = null;
        try
        {
            iStream = new DataInputStream(Files.newInputStream(summariesFile.toPath()));
            summary = IndexSummary.serializer.deserialize(iStream,
                                                          metadata.partitioner,
                                                          metadata.params.minIndexInterval,
                                                          metadata.params.maxIndexInterval);
            first = metadata.partitioner.decorateKey(ByteBufferUtil.readWithLength(iStream));
            last = metadata.partitioner.decorateKey(ByteBufferUtil.readWithLength(iStream));
        }
        catch (IOException e)
        {
            if (summary != null)
                summary.close();
            logger.trace("Cannot deserialize SSTable Summary File {}: {}", summariesFile.path(), e.getMessage());
            // corrupted; delete it and fall back to creating a new summary
            FileUtils.closeQuietly(iStream);
            // delete it and fall back to creating a new summary
            FileUtils.deleteWithConfirm(summariesFile);
        }
        finally
        {
            FileUtils.closeQuietly(iStream);
        }
    }

    /**
     * Build index summary, first key, last key if {@code summaryLoaded} is false and recreate bloom filter if
     * {@code recreteBloomFilter} is true by reading through Index.db file.
     *
     * @param recreateBloomFilter true if recreate bloom filter
     * @param summaryLoaded true if index summary, first key and last key are already loaded and not need to build again
     */
    void buildSummaryAndBloomFilter(boolean recreateBloomFilter,
                                    boolean summaryLoaded,
                                    Set<Component> components,
                                    StatsMetadata statsMetadata) throws IOException
    {
        if (!components.contains(Component.PRIMARY_INDEX))
            return;

        if (!recreateBloomFilter && summaryLoaded)
            return;

        if (logger.isDebugEnabled())
            logger.debug("Attempting to build summary for {}", descriptor);

        try (PartitionIndexIterator indexIterator = readerFactory.indexIterator(descriptor, metadata)) {
            long histogramCount = statsMetadata.estimatedPartitionSize.count();
            long estimatedKeys = histogramCount > 0 && !statsMetadata.estimatedPartitionSize.isOverflowed()
                                 ? histogramCount
                                 : SSTable.estimateRowsFromIndex(indexIterator); // statistics is supposed to be optional
            if (recreateBloomFilter)
            {
                logger.debug("Recreating bloom filter for {} with fpChance={}", descriptor, metadata.params.bloomFilterFpChance);
                bf = FilterFactory.getFilter(estimatedKeys, metadata.params.bloomFilterFpChance);
            }

            // we read the positions in a BRAF so we don't have to worry about an entry spanning a mmap boundary.
            try (IndexSummaryBuilder summaryBuilder = summaryLoaded ? null : new IndexSummaryBuilder(estimatedKeys, metadata.params.minIndexInterval, Downsampling.BASE_SAMPLING_LEVEL))
            {
                while (!indexIterator.isExhausted())
                {
                    DecoratedKey decoratedKey = metadata.partitioner.decorateKey(indexIterator.key());

                    if (!summaryLoaded)
                    {
                        if (first == null)
                            first = decoratedKey;
                        last = decoratedKey;

                        summaryBuilder.maybeAddEntry(decoratedKey, indexIterator.keyPosition());
                    }

                    if (recreateBloomFilter)
                        bf.add(decoratedKey);

                    indexIterator.advance();
                }

                if (!summaryLoaded)
                    summary = summaryBuilder.build(metadata.partitioner);
            }
        }

        if (!summaryLoaded)
        {
            first = SSTable.getMinimalKey(first);
            last = SSTable.getMinimalKey(last);
        }
    }

    public static IFilter loadBloomFilter(TableMetadata metadata, File file, boolean oldFormat)
    {
        if (BloomFilter.lazyLoading() && !SchemaConstants.isLocalSystemKeyspace(metadata.keyspace))
        {
            logger.debug("postponing bloom filter deserialization for {}", file);
            return FilterFactory.AlwaysPresentForLazyLoading;
        }

        return doLoadBloomFilter(file, oldFormat);
    }

    public static IFilter doLoadBloomFilter(File file, boolean oldFormat)
    {
        if (file.exists())
        {
            logger.debug("Loading bloom filter from {}", file);
            IFilter filter = null;
            try (FileInputStreamPlus stream = file.newInputStream())
            {
                filter = BloomFilter.getDeserializer().deserialize(stream, oldFormat);
                return filter;
            }
            catch (Throwable t)
            {
                JVMStabilityInspector.inspectThrowable(t);
                logger.error("Failed to deserialize Bloom filter: {}", t.getMessage());
                if (filter != null)
                    filter.close();
            }
        }
        else
        {
            logger.error("Bloom filter {} not found", file);
        }
        return null;
    }

    public static class ForWriter extends SSTableReaderBuilder
    {
        public ForWriter(Descriptor descriptor,
                         TableMetadataRef metadataRef,
                         long maxDataAge,
                         Set<Component> components,
                         StatsMetadata statsMetadata,
                         SSTableReader.OpenReason openReason,
                         SerializationHeader header)
        {
            super(descriptor, metadataRef, maxDataAge, components, statsMetadata, openReason, header);
        }

        public SSTableReaderBuilder.ForWriter dfile(FileHandle dfile)
        {
            this.dfile = dfile;
            return this;
        }

        public SSTableReaderBuilder.ForWriter ifile(FileHandle ifile)
        {
            this.ifile = ifile;
            return this;
        }

        public SSTableReaderBuilder.ForWriter bf(IFilter bf)
        {
            this.bf = bf;
            return this;
        }

        public SSTableReaderBuilder.ForWriter summary(IndexSummary summary)
        {
            this.summary = summary;
            return this;
        }

        @Override
        public SSTableReader build()
        {
            SSTableReader reader = new BigTableReader(this);

            reader.setup(true);
            return reader;
        }
    }

    public static class ForBatch extends SSTableReaderBuilder
    {
        public ForBatch(Descriptor descriptor,
                        TableMetadataRef metadataRef,
                        Set<Component> components,
                        StatsMetadata statsMetadata,
                        SerializationHeader header)
        {
            super(descriptor, metadataRef, System.currentTimeMillis(), components, statsMetadata, SSTableReader.OpenReason.NORMAL, header);
        }

        @Override
        public SSTableReader build()
        {
            assert dfile == null && ifile == null && summary == null && bf == null;
            File dataFile = descriptor.fileFor(Component.DATA);
            logger.info("Opening {} ({})", descriptor, FBUtilities.prettyPrintMemory(dataFile.length()));

            initSummary(dataFile, components, statsMetadata);

            boolean compression = components.contains(Component.COMPRESSION_INFO);
            try (FileHandle.Builder ibuilder = defaultIndexHandleBuilder(descriptor, Component.PRIMARY_INDEX, compression);
                 FileHandle.Builder dbuilder = defaultDataHandleBuilder(descriptor, statsMetadata.zeroCopyMetadata).compressed(compression))
            {
                long indexFileLength = descriptor.fileFor(Component.PRIMARY_INDEX).length();
                DiskOptimizationStrategy optimizationStrategy = DatabaseDescriptor.getDiskOptimizationStrategy();
                int dataBufferSize = optimizationStrategy.bufferSize(statsMetadata.estimatedPartitionSize.percentile(DatabaseDescriptor.getDiskOptimizationEstimatePercentile()));
                int indexBufferSize = optimizationStrategy.bufferSize(indexFileLength / summary.size());
                ifile = ibuilder.bufferSize(indexBufferSize).complete();
                dfile = dbuilder.bufferSize(dataBufferSize).complete();
                bf = FilterFactory.AlwaysPresent;

                SSTableReader sstable = new BigTableReader(this);

                sstable.first = first;
                sstable.last = last;

                sstable.setup(false);
                return sstable;
            }
        }

        void initSummary(File dataFilePath, Set<Component> components, StatsMetadata statsMetadata)
        {
            loadSummary();
            if (summary == null)
            {
                try
                {
                    buildSummaryAndBloomFilter(false, false, components, statsMetadata);
                }
                catch (IOException e)
                {
                    throw new CorruptSSTableException(e, dataFilePath);
                }
            }
        }
    }

    public static class ForRead extends SSTableReaderBuilder
    {
        private volatile ValidationMetadata validationMetadata;
        private final boolean isOffline;

        public ForRead(Descriptor descriptor,
                       TableMetadataRef metadataRef,
                       ValidationMetadata validationMetadata,
                       boolean isOffline,
                       Set<Component> components,
                       StatsMetadata statsMetadata,
                       SerializationHeader header)
        {
            super(descriptor, metadataRef, System.currentTimeMillis(), components, statsMetadata, SSTableReader.OpenReason.NORMAL, header);
            this.validationMetadata = validationMetadata;
            this.isOffline = isOffline;
        }

        @Override
        public SSTableReader build()
        {
            assert dfile == null && ifile == null && summary == null && bf == null;
            File dataFilePath = descriptor.fileFor(Component.DATA);
            long fileLength = dataFilePath.length();
            logger.info("Opening {} ({})", descriptor, FBUtilities.prettyPrintMemory(fileLength));

            try
            {
                // load index and filter
                long start = System.nanoTime();
                load(validationMetadata, isOffline, components, DatabaseDescriptor.getDiskOptimizationStrategy(), statsMetadata);
                logger.trace("INDEX LOAD TIME for {}: {} ms.", descriptor, TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
            }
            catch (IOException t)
            {
                throw new CorruptSSTableException(t, dataFilePath);
            }

            SSTableReader sstable = new BigTableReader(this);

            sstable.first = first;
            sstable.last = last;

            sstable.setup(!isOffline); // Don't track hotness if we're offline.
            return sstable;
        }

        /**
         * @param validation Metadata for SSTable being loaded
         * @param isOffline Whether the SSTable is being loaded by an offline tool (sstabledump, scrub, etc)
         */
        private void load(ValidationMetadata validation,
                          boolean isOffline,
                          Set<Component> components,
                          DiskOptimizationStrategy optimizationStrategy,
                          StatsMetadata statsMetadata) throws IOException
        {
            double currentFPChance = validation != null ? validation.bloomFilterFPChance : Double.NaN;
            double desiredFPChance = metadata.params.bloomFilterFpChance;

            if (SSTableReader.shouldLoadBloomFilter(descriptor, components, currentFPChance, desiredFPChance))
                bf = loadBloomFilter(metadata, descriptor.fileFor(Component.FILTER), descriptor.version.hasOldBfFormat());

            boolean recreateBloomFilter = bf == null && SSTableReader.mayRecreateBloomFilter(descriptor, components, currentFPChance, isOffline, desiredFPChance);
            load(recreateBloomFilter, !isOffline, optimizationStrategy, statsMetadata, components);

            // if the filter was neither loaded nor created, or we encountered some problems, we fallback to pass-through filter
            if (bf == null)
            {
                bf = FilterFactory.AlwaysPresent;
                logger.warn("Could not recreate or deserialize existing bloom filter, continuing with a pass-through " +
                            "bloom filter but this will significantly impact reads performance");
            }
        }

        /**
         * Loads ifile, dfile and indexSummary, and optionally recreates and persists the bloom filter.
         * @param recreateBloomFilter Recreate the bloomfilter.
         * @param saveSummaryIfCreated for bulk loading purposes, if the summary was absent and needed to be built, you can
         *                             avoid persisting it to disk by setting this to false
         */
        void load(boolean recreateBloomFilter,
                  boolean saveSummaryIfCreated,
                  DiskOptimizationStrategy optimizationStrategy,
                  StatsMetadata statsMetadata,
                  Set<Component> components) throws IOException
        {
            boolean compression = components.contains(Component.COMPRESSION_INFO);
            try (FileHandle.Builder ibuilder = defaultIndexHandleBuilder(descriptor, Component.PRIMARY_INDEX, compression);
                 FileHandle.Builder dbuilder = defaultDataHandleBuilder(descriptor, statsMetadata.zeroCopyMetadata).compressed(compression))
            {
                loadSummary();
                boolean buildSummary = summary == null || recreateBloomFilter;
                if (buildSummary)
                    buildSummaryAndBloomFilter(recreateBloomFilter, summary != null, components, statsMetadata);

                int dataBufferSize = optimizationStrategy.bufferSize(statsMetadata.estimatedPartitionSize.percentile(DatabaseDescriptor.getDiskOptimizationEstimatePercentile()));

                if (components.contains(Component.PRIMARY_INDEX))
                {
                    long indexFileLength = descriptor.fileFor(Component.PRIMARY_INDEX).length();
                    int indexBufferSize = optimizationStrategy.bufferSize(indexFileLength / summary.size());
                    ifile = ibuilder.bufferSize(indexBufferSize).complete();
                }

                dfile = dbuilder.bufferSize(dataBufferSize).complete();

                if (buildSummary)
                {
                    if (saveSummaryIfCreated)
                        SSTableReader.saveSummary(descriptor, first, last, summary);
                    if (recreateBloomFilter)
                    {
                        SSTableReader.saveBloomFilter(descriptor, bf);
                        ValidationMetadata updatedValidationMetadata = new ValidationMetadata(validationMetadata.partitioner, metadata.params.bloomFilterFpChance);
                        descriptor.getMetadataSerializer().updateSSTableMetadata(descriptor, ImmutableMap.of(MetadataType.VALIDATION, updatedValidationMetadata));
                        validationMetadata = updatedValidationMetadata;
                    }
                }
            }
            catch (Throwable t)
            { // Because the tidier has not been set-up yet in SSTableReader.open(), we must release the files in case of error
                if (ifile != null)
                {
                    ifile.close();
                }

                if (dfile != null)
                {
                    dfile.close();
                }

                if (bf != null)
                {
                    bf.close();
                    bf = null;
                }

                if (summary != null)
                {
                    summary.close();
                }

                throw t;
            }
        }
    }
}
