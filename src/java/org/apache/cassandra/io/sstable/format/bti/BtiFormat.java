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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.GaugeProvider;
import org.apache.cassandra.io.sstable.IScrubber;
import org.apache.cassandra.io.sstable.MetricsProviders;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.filter.BloomFilterMetrics;
import org.apache.cassandra.io.sstable.format.AbstractSSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReaderLoadingBuilder;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.format.SortedTableScrubber;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.OutputHandler;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

/**
 * Bigtable format with trie indices. See BTIFormat.md for the format documentation.
 */
public class BtiFormat extends AbstractSSTableFormat<BtiTableReader, BtiTableWriter>
{
    private final static Logger logger = LoggerFactory.getLogger(BtiFormat.class);

    public static final String NAME = "bti";

    private final Version latestVersion = new BtiVersion(this, BtiVersion.current_version);
    static final BtiTableReaderFactory readerFactory = new BtiTableReaderFactory();
    static final BtiTableWriterFactory writerFactory = new BtiTableWriterFactory();

    public static class Components extends SSTableFormat.Components
    {
        public static class Types extends AbstractSSTableFormat.Components.Types
        {
            public static final Component.Type PARTITION_INDEX = Component.Type.createSingleton("PARTITION_INDEX", "Partitions.db", true, BtiFormat.class);
            public static final Component.Type ROW_INDEX = Component.Type.createSingleton("ROW_INDEX", "Rows.db", true, BtiFormat.class);
        }

        public final static Component PARTITION_INDEX = Types.PARTITION_INDEX.getSingleton();

        public final static Component ROW_INDEX = Types.ROW_INDEX.getSingleton();

        private final static Set<Component> PRIMARY_COMPONENTS = ImmutableSet.of(DATA,
                                                                                 PARTITION_INDEX);

        private final static Set<Component> MUTABLE_COMPONENTS = ImmutableSet.of(STATS);

        private static final Set<Component> UPLOAD_COMPONENTS = ImmutableSet.of(DATA,
                                                                                PARTITION_INDEX,
                                                                                ROW_INDEX,
                                                                                COMPRESSION_INFO,
                                                                                STATS);

        private static final Set<Component> BATCH_COMPONENTS = ImmutableSet.of(DATA,
                                                                               PARTITION_INDEX,
                                                                               ROW_INDEX,
                                                                               COMPRESSION_INFO,
                                                                               FILTER,
                                                                               STATS);

        private final static Set<Component> ALL_COMPONENTS = ImmutableSet.of(DATA,
                                                                             PARTITION_INDEX,
                                                                             ROW_INDEX,
                                                                             STATS,
                                                                             COMPRESSION_INFO,
                                                                             FILTER,
                                                                             DIGEST,
                                                                             CRC,
                                                                             TOC);

        private final static Set<Component> GENERATED_ON_LOAD_COMPONENTS = ImmutableSet.of(FILTER);
    }


    public BtiFormat(Map<String, String> options)
    {
        super(NAME, options);
    }

    public static boolean is(SSTableFormat<?, ?> format)
    {
        return format.name().equals(NAME);
    }

    public static BtiFormat getInstance()
    {
        return (BtiFormat) Objects.requireNonNull(DatabaseDescriptor.getSSTableFormats().get(NAME), "Unknown SSTable format: " + NAME);
    }

    public static boolean isSelected()
    {
        return is(DatabaseDescriptor.getSelectedSSTableFormat());
    }

    @Override
    public Version getLatestVersion()
    {
        return latestVersion;
    }

    @Override
    public Version getVersion(String version)
    {
        return new BtiVersion(this, version);
    }

    @Override
    public BtiTableWriterFactory getWriterFactory()
    {
        return writerFactory;
    }

    @Override
    public BtiTableReaderFactory getReaderFactory()
    {
        return readerFactory;
    }

    @Override
    public Set<Component> primaryComponents()
    {
        return Components.PRIMARY_COMPONENTS;
    }

    @Override
    public Set<Component> batchComponents()
    {
        return Components.BATCH_COMPONENTS;
    }

    @Override
    public Set<Component> uploadComponents()
    {
        return Components.UPLOAD_COMPONENTS;
    }

    @Override
    public Set<Component> mutableComponents()
    {
        return Components.MUTABLE_COMPONENTS;
    }

    @Override
    public Set<Component> allComponents()
    {
        return Components.ALL_COMPONENTS;
    }

    @Override
    public Set<Component> generatedOnLoadComponents()
    {
        return Components.GENERATED_ON_LOAD_COMPONENTS;
    }

    @Override
    public SSTableFormat.KeyCacheValueSerializer<BtiTableReader, TrieIndexEntry> getKeyCacheValueSerializer()
    {
        throw new AssertionError("BTI sstables do not use key cache");
    }

    @Override
    public IScrubber getScrubber(ColumnFamilyStore cfs, LifecycleTransaction transaction, OutputHandler outputHandler, IScrubber.Options options)
    {
        Preconditions.checkArgument(cfs.metadata().equals(transaction.onlyOne().metadata()), "SSTable metadata does not match current definition");
        return new BtiTableScrubber(cfs, transaction, outputHandler, options);
    }

    @Override
    public MetricsProviders getFormatSpecificMetricsProviders()
    {
        return BtiTableSpecificMetricsProviders.instance;
    }

    @Override
    public void deleteOrphanedComponents(Descriptor descriptor, Set<Component> components)
    {
        SortedTableScrubber.deleteOrphanedComponents(descriptor, components);
    }

    private void delete(Descriptor desc, List<Component> components)
    {
        logger.debug("Deleting sstable: {}", desc);

        if (components.remove(SSTableFormat.Components.DATA))
            components.add(0, SSTableFormat.Components.DATA); // DATA component should be first

        for (Component component : components)
        {
            logger.trace("Deleting component {} of {}", component, desc);
            desc.fileFor(component).deleteIfExists();
        }
    }

    @Override
    public void delete(Descriptor desc)
    {
        try
        {
            delete(desc, Lists.newArrayList(Sets.intersection(allComponents(), desc.discoverComponents())));
        }
        catch (Throwable t)
        {
            JVMStabilityInspector.inspectThrowable(t);
        }
    }

    static class BtiTableReaderFactory implements SSTableReaderFactory<BtiTableReader, BtiTableReader.Builder>
    {
        @Override
        public SSTableReader.Builder<BtiTableReader, BtiTableReader.Builder> builder(Descriptor descriptor)
        {
            return new BtiTableReader.Builder(descriptor);
        }

        @Override
        public SSTableReaderLoadingBuilder<BtiTableReader, BtiTableReader.Builder> loadingBuilder(Descriptor descriptor, TableMetadataRef tableMetadataRef, Set<Component> components)
        {
            return new BtiTableReaderLoadingBuilder(new SSTable.Builder<>(descriptor).setTableMetadataRef(tableMetadataRef)
                                                                                     .setComponents(components));
        }

        @Override
        public Pair<DecoratedKey, DecoratedKey> readKeyRange(Descriptor descriptor, IPartitioner partitioner) throws IOException
        {
            return PartitionIndex.readFirstAndLastKey(descriptor.fileFor(Components.PARTITION_INDEX), partitioner, descriptor.version.getByteComparableVersion());
        }

        @Override
        public Class<BtiTableReader> getReaderClass()
        {
            return BtiTableReader.class;
        }
    }

    static class BtiTableWriterFactory implements SSTableWriterFactory<BtiTableWriter, BtiTableWriter.Builder>
    {
        @Override
        public BtiTableWriter.Builder builder(Descriptor descriptor)
        {
            return new BtiTableWriter.Builder(descriptor);
        }

        @Override
        public long estimateSize(SSTableWriter.SSTableSizeParameters parameters)
        {
            return (long) ((parameters.partitionCount() * 8 // index entries
                            + parameters.partitionKeysSize() // keys in data file
                            + parameters.dataSize()) // data
                           * 1.2); // bloom filter and row index overhead
        }
    }

    static class BtiVersion extends Version
    {
        private static final Logger logger = LoggerFactory.getLogger(BtiVersion.class);

        public static final String current_version = CassandraRelevantProperties.TRIE_INDEX_FORMAT_VERSION.getString();

        static
        {
            logger.info("Trie index format current version: {}", current_version);
        }

        public static final String earliest_supported_version = "aa";

        // aa (DSE 6.0): trie index format
        // ab (DSE pre-6.8): ILLEGAL - handled as 'b' (predates 'ba'). Pre-GA "LABS" releases of DSE 6.8 used this
        //                   sstable version.
        // ac (DSE 6.0.11, 6.7.6): corrected sstable min/max clustering (DB-3691/CASSANDRA-14861)
        // ad (DSE 6.0.14, 6.7.11): added hostId of the node from which the sstable originated (DB-4629)
        // b  (DSE early 6.8 "LABS") has some of 6.8 features but not all
        // ba (DSE 6.8): encrypted indices and metadata
        //               new BloomFilter serialization format
        //               add incremental NodeSync information to metadata
        //               improved min/max clustering representation
        //               presence marker for partition level deletions
        // bb (DSE 6.8.5): added hostId of the node from which the sstable originated (DB-4629)
        // ca (DSE-DB aka Stargazer based on OSS 4.0): bb fields without maxColumnValueLengths + all OSS fields
        // cb (OSS 5.0): token space coverage
        // cc : added explicitly frozen tuples in header, non-frozen UDT columns dropping support

        // versions aa-cz are not supported in OSS
        // da (5.0): initial version of the BIT format
        // NOTE: when adding a new version, please add that to LegacySSTableTest, too.

        private final boolean isLatestVersion;

        /**
         * DB-2648/CASSANDRA-9067: DSE 6.8/OSS 4.0 bloom filter representation changed (bitset data is no longer stored
         * as BIG_ENDIAN longs, which avoids some redundant bit twiddling).
         */
        private final boolean hasOldBfFormat;
        private final boolean hasAccurateLegacyMinMax;
        private final boolean hasOriginatingHostId;
        private final boolean hasMaxColumnValueLengths;
        private final boolean hasImprovedMinMax;
        private final boolean hasLegacyMinMax;
        private final boolean hasZeroCopyMetadata;
        private final boolean hasIncrementalNodeSyncMetadata;
        private final boolean hasIsTransient;
        private final boolean hasTokenSpaceCoverage;
        private final boolean hasMisplacedPartitionLevelDeletionsPresenceMarker;


        private final int correspondingMessagingVersion;
        private final ByteComparable.Version byteComparableVersion;
        private final boolean hasPartitionLevelDeletionsPresenceMarker;
        private final boolean hasKeyRange;
        private final boolean hasUIntDeletionTime;
        private final boolean hasExplicitlyFrozenTuples;

        BtiVersion(BtiFormat format, String version)
        {
            super(format, version);

            boolean dOrLater = version.compareTo("d") >= 0;
            boolean cOrLater = dOrLater || version.startsWith("c");
            boolean bOrLater = cOrLater || version.startsWith("b");
            boolean aOrLater = bOrLater || version.startsWith("a");

            isLatestVersion = version.compareTo(current_version) == 0;
            correspondingMessagingVersion = MessagingService.VERSION_50;
            byteComparableVersion = version.compareTo("ca") >= 0 ? ByteComparable.Version.OSS50 : ByteComparable.Version.LEGACY;
            hasOldBfFormat = aOrLater && !bOrLater;
            hasImprovedMinMax = bOrLater;
            hasLegacyMinMax = aOrLater && !bOrLater;
            hasAccurateLegacyMinMax = !bOrLater && version.compareTo("ac") >= 0;
            hasOriginatingHostId = bOrLater && version.compareTo("bb") >= 0 || !bOrLater && version.compareTo("ad") >= 0;
            hasIsTransient = cOrLater;
            hasTokenSpaceCoverage = version.compareTo("cb") >= 0;
            hasMisplacedPartitionLevelDeletionsPresenceMarker = bOrLater && !dOrLater;
            hasPartitionLevelDeletionsPresenceMarker = dOrLater;
            hasKeyRange = dOrLater;
            hasUIntDeletionTime = dOrLater;

            hasMaxColumnValueLengths = bOrLater && !cOrLater; // DSE only field
            hasZeroCopyMetadata = bOrLater && !cOrLater; // DSE only field
            hasIncrementalNodeSyncMetadata = bOrLater && !cOrLater; // DSE only field

            hasExplicitlyFrozenTuples = version.compareTo("cc") < 0 || version.compareTo("da") >= 0; // we don't know if what DA is going to be eventually, but it is almost certain it will not include explicitly frozen tuples
        }

        @Override
        public boolean isLatestVersion()
        {
            return isLatestVersion;
        }

        @Override
        public int correspondingMessagingVersion()
        {
            return correspondingMessagingVersion;
        }

        @Override
        public boolean hasCommitLogLowerBound()
        {
            return true;
        }

        @Override
        public boolean hasCommitLogIntervals()
        {
            return true;
        }

        @Override
        public boolean hasMaxCompressedLength()
        {
            return true;
        }

        @Override
        public boolean hasPendingRepair()
        {
            return true;
        }

        // this field is not present in DSE
        @Override
        public boolean hasIsTransient()
        {
            return hasIsTransient;
        }

        @Override
        public ByteComparable.Version getByteComparableVersion()
        {
            return byteComparableVersion;
        }

        @Override
        public boolean hasMetadataChecksum()
        {
            return true;
        }

        @Override
        public boolean hasOldBfFormat()
        {
            return hasOldBfFormat;
        }

        @Override
        public boolean hasAccurateMinMax()
        {
            return hasAccurateLegacyMinMax;
        }

        public boolean hasLegacyMinMax()
        {
            return hasLegacyMinMax;
        }

        @Override
        public boolean hasOriginatingHostId()
        {
            return hasOriginatingHostId;
        }

        @Override
        public boolean hasImprovedMinMax() {
            return hasImprovedMinMax;
        }

        @Override
        public boolean hasTokenSpaceCoverage()
        {
            return hasTokenSpaceCoverage;
        }

        @Override
        public boolean hasPartitionLevelDeletionsPresenceMarker()
        {
            return hasPartitionLevelDeletionsPresenceMarker;
        }

        @Override
        public boolean hasMisplacedPartitionLevelDeletionsPresenceMarker()
        {
            return hasMisplacedPartitionLevelDeletionsPresenceMarker;
        }

        @Override
        public boolean hasKeyRange()
        {
            return hasKeyRange;
        }

        @Override
        public boolean isCompatible()
        {
            return version.compareTo(earliest_supported_version) >= 0 && version.charAt(0) <= current_version.charAt(0);
        }

        @Override
        public boolean isCompatibleForStreaming()
        {
            return isCompatible() && version.charAt(0) == current_version.charAt(0);
        }

        @Override
        public boolean hasUIntDeletionTime()
        {
            return hasUIntDeletionTime;
        }

        @Override
        public boolean hasZeroCopyMetadata()
        {
            return hasZeroCopyMetadata;
        }

        @Override
        public boolean hasIncrementalNodeSyncMetadata()
        {
            return hasIncrementalNodeSyncMetadata;
        }

        @Override
        public boolean hasMaxColumnValueLengths()
        {
            return hasMaxColumnValueLengths;
        }

        @Override
        public boolean hasImplicitlyFrozenTuples()
        {
            return hasExplicitlyFrozenTuples;
        }
    }

    private static class BtiTableSpecificMetricsProviders implements MetricsProviders
    {
        private final static BtiTableSpecificMetricsProviders instance = new BtiTableSpecificMetricsProviders();

        private final Iterable<GaugeProvider<?>> gaugeProviders = BloomFilterMetrics.instance.getGaugeProviders();

        @Override
        public Iterable<GaugeProvider<?>> getGaugeProviders()
        {
            return gaugeProviders;
        }
    }

    @SuppressWarnings("unused")
    public static class BtiFormatFactory implements Factory
    {
        @Override
        public String name()
        {
            return NAME;
        }

        @Override
        public SSTableFormat<?, ?> getInstance(Map<String, String> options)
        {
            return new BtiFormat(options);
        }
    }
}
