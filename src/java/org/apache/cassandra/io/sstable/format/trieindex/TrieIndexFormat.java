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
import java.util.Collection;
import java.util.Set;
import java.util.regex.Pattern;

import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.PartitionIndexIterator;
import org.apache.cassandra.io.sstable.format.SSTableFlushObserver;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.Pair;

/**
 * Bigtable format with trie indices
 */
public class TrieIndexFormat implements SSTableFormat
{
    public static final TrieIndexFormat instance = new TrieIndexFormat();
    public static final Version latestVersion = new TrieIndexVersion(TrieIndexVersion.current_version);
    static final ReaderFactory readerFactory = new ReaderFactory();
    static final WriterFactory writerFactory = new WriterFactory();

    private static final Pattern VALIDATION = Pattern.compile("[a-z]+");

    private TrieIndexFormat()
    {

    }

    @Override
    public Type getType()
    {
        return Type.TRIE_INDEX;
    }

    @Override
    public Version getLatestVersion()
    {
        return latestVersion;
    }

    @Override
    public Version getVersion(String version)
    {
        return new TrieIndexVersion(version);
    }

    @Override
    public boolean validateVersion(String ver)
    {
        return ver != null && VALIDATION.matcher(ver).matches();
    }

    @Override
    public SSTableWriter.Factory getWriterFactory()
    {
        return writerFactory;
    }

    @Override
    public SSTableReader.Factory getReaderFactory()
    {
        return readerFactory;
    }

    static class WriterFactory extends SSTableWriter.Factory
    {
        @Override
        public SSTableWriter open(Descriptor descriptor,
                                  SSTableWriterCreationHelper helper,
                                  Collection<SSTableFlushObserver> observers,
                                  Set<Component> indexComponents,
                                  SSTableTracker sstableTracker)
        {
            return new TrieIndexSSTableWriter(descriptor, helper, observers, indexComponents, sstableTracker);
        }
    }

    // Data, primary index and row index (which may be 0-length) are required.
    // For the 3.0+ sstable format, the (misnomed) stats component hold the serialization header which we need to deserialize the sstable content
    static Set<Component> REQUIRED_COMPONENTS = ImmutableSet.of(Component.DATA, Component.PARTITION_INDEX, Component.ROW_INDEX, Component.STATS);

    static class ReaderFactory extends SSTableReader.Factory
    {
        @Override
        public TrieIndexSSTableReader open(Descriptor descriptor, Set<Component> components, TableMetadataRef metadata, Long maxDataAge, StatsMetadata sstableMetadata, SSTableReader.OpenReason openReason, SerializationHeader header)
        {
            return new TrieIndexSSTableReader(descriptor, components, metadata, maxDataAge, sstableMetadata, openReason, header);
        }

        @Override
        public PartitionIndexIterator keyIterator(Descriptor desc, TableMetadata metadata)
        {
            IPartitioner partitioner = metadata.partitioner;
            boolean compressedData = desc.filenameFor(Component.COMPRESSION_INFO).exists();
            try
            {
                StatsMetadata stats = (StatsMetadata) desc.getMetadataSerializer().deserialize(desc, MetadataType.STATS);

                try (FileHandle.Builder piBuilder = SSTableReader.indexFileHandleBuilder(desc, metadata, Component.PARTITION_INDEX, compressedData);
                     FileHandle.Builder riBuilder = SSTableReader.indexFileHandleBuilder(desc, metadata, Component.ROW_INDEX, compressedData);
                     FileHandle.Builder dBuilder = SSTableReader.dataFileHandleBuilder(desc, metadata, stats.zeroCopyMetadata, compressedData);
                     PartitionIndex index = PartitionIndex.load(piBuilder, partitioner, stats.zeroCopyMetadata, false);
                     FileHandle dFile = dBuilder.complete();
                     FileHandle riFile = riBuilder.complete())
                {
                    return new PartitionIterator(index.sharedCopy(),
                                                 partitioner,
                                                 riFile.sharedCopy(),
                                                 dFile.sharedCopy(),
                                                 Rebufferer.ReaderConstraint.NONE)
                            .closeHandles();
                }
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }

        @Override
        public Pair<DecoratedKey, DecoratedKey> getKeyRange(Descriptor descriptor, IPartitioner partitioner) throws IOException
        {
            File indexFile = descriptor.filenameFor(Component.PARTITION_INDEX);
            if (!indexFile.exists())
                return null;
            boolean compressedData = descriptor.filenameFor(Component.COMPRESSION_INFO).exists();

            StatsMetadata stats = (StatsMetadata) descriptor.getMetadataSerializer().deserialize(descriptor, MetadataType.STATS);


            try (FileHandle.Builder fhBuilder = SSTableReader.indexFileHandleBuilder(descriptor,
                                                                                     TableMetadata.minimal(descriptor.ksname, descriptor.cfname),
                                                                                     Component.PARTITION_INDEX, compressedData);
                 PartitionIndex pIndex = PartitionIndex.load(fhBuilder, partitioner, stats.zeroCopyMetadata,false))
            {
                return Pair.create(pIndex.firstKey(), pIndex.lastKey());
            }
        }

        @Override
        public Set<Component> requiredComponents()
        {
            return REQUIRED_COMPONENTS;
        }
    }

    // versions are denoted as [major][minor].  Minor versions must be forward-compatible:
    // new fields are allowed in e.g. the metadata component, but fields can't be removed
    // or have their size changed.
    //
    static class TrieIndexVersion extends Version
    {
        public static final String current_version = "bb";
        public static final String earliest_supported_version = "aa";
        public static final EncodingVersion latestVersion = EncodingVersion.last();

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
        // NOTE: when adding a new version, please add that to LegacySSTableTest, too.

        private final boolean isLatestVersion;
        /**
         * DB-2648/CASSANDRA-9067: DSE 6.8/OSS 4.0 bloom filter representation changed (bitset data is no longer stored
         * as BIG_ENDIAN longs, which avoids some redundant bit twiddling).
         */
        private final boolean hasOldBfFormat;
        private final boolean hasAccurateLegacyMinMax;
        private final boolean hasOriginatingHostId;

        TrieIndexVersion(String version)
        {
            super(instance, version = mapAb(version));

            isLatestVersion = version.compareTo(current_version) == 0;
            hasOldBfFormat = version.compareTo("b") < 0;
            hasAccurateLegacyMinMax = version.compareTo("ac") >= 0;
            hasOriginatingHostId = (version.compareTo("ad") >= 0 && version.compareTo("ba") < 0) || (version.compareTo("bb") >= 0);
        }

        private static String mapAb(String version)
        {
            return "ab".equals(version) ? "b" : version;
        }

        @Override
        public boolean isLatestVersion()
        {
            return isLatestVersion;
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

        public boolean hasMaxCompressedLength()
        {
            return true;
        }

        public boolean hasPendingRepair()
        {
            return true;
        }

        public boolean hasMetadataChecksum()
        {
            return true;
        }

        @Override
        public boolean indicesAreEncrypted()
        {
            return version.compareTo("b") >= 0;
        }

        @Override
        public boolean metadataAreEncrypted()
        {
            return version.compareTo("ba") >= 0;
        }

        @Override
        public boolean supportsZeroCopy()
        {
            return version.compareTo("b") >= 0;
        }

        @Override
        public boolean hasIncrementalNodeSyncMetadata()
        {
            return version.compareTo("b") >= 0;
        }

        @Override
        public boolean hasAccurateLegacyMinMax()
        {
            return hasAccurateLegacyMinMax;
        }

        @Override
        public boolean hasPartitionLevelDeletionsPresenceMarker()
        {
            return version.compareTo("ba") >= 0;
        }

        @Override
        public boolean hasImprovedMinMax()
        {
            // Note that this was not in early 6.8 "LABS" releases
            return version.compareTo("ba") >= 0;
        }

        @Override
        public boolean hasMaxColumnValueLengths()
        {
            return version.compareTo("ba") >= 0;
        }

        @Override
        public boolean hasOriginatingHostId()
        {
            return hasOriginatingHostId;
        }

        @Override
        public EncodingVersion encodingVersion()
        {
            return latestVersion;
        }

        @Override
        public boolean isCompatible()
        {
            return version.compareTo(earliest_supported_version) >= 0 && version.charAt(0) <= current_version.charAt(0);
        }

        @Override
        public boolean hasOldBfFormat()
        {
            return hasOldBfFormat;
        }
    }
}
