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
package org.apache.cassandra.io.sstable.metadata;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.function.UnaryOperator;
import java.util.zip.CRC32;

import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableFormat.Components;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.utils.FBUtilities.updateChecksumInt;

/**
 * Metadata serializer for SSTables {@code version >= 'na'}.
 *
 * <pre>
 * File format := | number of components (4 bytes) | crc | toc | crc | component1 | c1 crc | component2 | c2 crc | ... |
 * toc         := | component type (4 bytes) | position of component |
 * </pre>
 *
 * IMetadataComponent.Type's ordinal() defines the order of serialization.
 */
public class MetadataSerializer implements IMetadataSerializer
{
    private static final Logger logger = LoggerFactory.getLogger(MetadataSerializer.class);

    private static final int CHECKSUM_LENGTH = 4; // CRC32

    public void serialize(Map<MetadataType, MetadataComponent> components, DataOutputPlus out, Descriptor descriptor) throws IOException
    {
        Version version = descriptor.version;
        boolean checksum = version.hasMetadataChecksum();
        CRC32 crc = new CRC32();
        final int componentsCount = components.size();
        
        // sort components by type
        MetadataComponent[] sortedComponents = components.values().toArray(new MetadataComponent[componentsCount]);
        Arrays.sort(sortedComponents);

        // write number of component
        out.writeInt(componentsCount);
        updateChecksumInt(crc, componentsCount);
        maybeWriteChecksum(crc, out, version);

        ICompressor encryptor = getEncryptor(descriptor, true);
        ByteBuffer[] componentsSerializations = new ByteBuffer[componentsCount];

        // serialize and possibly encrypt components
        for (int i = 0; i < componentsCount; ++i)
        {
            MetadataComponent metadataComponent = sortedComponents[i];
            MetadataType componentType = metadataComponent.getType();
            int size = componentType.serializer.serializedSize(version, metadataComponent);

            try (DataOutputBuffer dob = new DataOutputBuffer(size))
            {
                componentType.serializer.serialize(version, metadataComponent, dob);
                if (encryptor != null)
                {
                    ByteBuffer encrypted = ByteBuffer.allocate(encryptor.initialCompressedBufferLength(size));
                    encryptor.compress(dob.buffer(), encrypted);
                    encrypted.flip();
                    componentsSerializations[i] = encrypted;
                }
                else
                {
                    componentsSerializations[i] = dob.buffer();
                }
            }
        }

        // build and write toc
        int lastPosition = 4 + (8 * componentsCount) + (checksum ? 2 * CHECKSUM_LENGTH : 0);
        for (int i = 0; i < componentsCount; ++i)
        {
            MetadataComponent component = sortedComponents[i];
            MetadataType type = component.getType();
            // serialize type
            out.writeInt(type.ordinal());
            updateChecksumInt(crc, type.ordinal());
            // serialize position
            out.writeInt(lastPosition);
            updateChecksumInt(crc, lastPosition);
            int size = componentsSerializations[i].remaining();
            lastPosition += size + (checksum ? CHECKSUM_LENGTH : 0);
        }
        maybeWriteChecksum(crc, out, version);

        // copy components to output
        for (int i = 0; i < componentsCount; ++i)
        {
            ByteBuffer bytes = componentsSerializations[i];
            out.write(bytes);
            crc.reset();
            crc.update(bytes);
            maybeWriteChecksum(crc, out, version);
        }
    }

    private static void maybeWriteChecksum(CRC32 crc, DataOutputPlus out, Version version) throws IOException
    {
        if (version.hasMetadataChecksum())
            out.writeInt((int) crc.getValue());
    }

    public Map<MetadataType, MetadataComponent> deserialize(Descriptor descriptor, EnumSet<MetadataType> types) throws IOException
    {
        Map<MetadataType, MetadataComponent> components;
        logger.trace("Load metadata for {}", descriptor);
        File statsFile = descriptor.fileFor(Components.STATS);
        if (!statsFile.exists())
        {
            logger.trace("No sstable stats for {}", descriptor);
            components = new EnumMap<>(MetadataType.class);
            components.put(MetadataType.STATS, MetadataCollector.defaultStatsMetadata());
        }
        else
        {
            try (RandomAccessReader r = RandomAccessReader.open(statsFile))
            {
                components = deserialize(descriptor, r, types);
            }
        }
        return components;
    }

    public MetadataComponent deserialize(Descriptor descriptor, MetadataType type) throws IOException
    {
        return deserialize(descriptor, EnumSet.of(type)).get(type);
    }

    public Map<MetadataType, MetadataComponent> deserialize(Descriptor descriptor,
                                                            FileDataInput in,
                                                            EnumSet<MetadataType> selectedTypes)
    throws IOException
    {
        boolean isChecksummed = descriptor.version.hasMetadataChecksum();
        CRC32 crc = new CRC32();

        /*
         * Read TOC
         */

        int length = (int) in.bytesRemaining();

        int count = in.readInt();
        updateChecksumInt(crc, count);
        maybeValidateChecksum(crc, in, descriptor);

        int[] ordinals = new int[count];
        int[]  offsets = new int[count];
        int[]  lengths = new int[count];

        for (int i = 0; i < count; i++)
        {
            ordinals[i] = in.readInt();
            updateChecksumInt(crc, ordinals[i]);

            offsets[i] = in.readInt();
            updateChecksumInt(crc, offsets[i]);
        }
        maybeValidateChecksum(crc, in, descriptor);

        lengths[count - 1] = length - offsets[count - 1];
        for (int i = 0; i < count - 1; i++)
            lengths[i] = offsets[i + 1] - offsets[i];

        /*
         * Read components
         */

        MetadataType[] allMetadataTypes = MetadataType.values();

        Map<MetadataType, MetadataComponent> components = new EnumMap<>(MetadataType.class);
        ICompressor encryptor = getEncryptor(descriptor, false);

        for (int i = 0; i < count; i++)
        {
            MetadataType type = allMetadataTypes[ordinals[i]];

            if (!selectedTypes.contains(type))
            {
                in.skipBytes(lengths[i]);
                continue;
            }

            byte[] buffer = new byte[isChecksummed ? lengths[i] - CHECKSUM_LENGTH : lengths[i]];
            int bufLen = buffer.length;
            in.readFully(buffer);

            crc.reset(); crc.update(buffer);
            maybeValidateChecksum(crc, in, descriptor);
            
            if (encryptor != null)
            {
                // Because we only use the encryption component, we are guaranteed that the serialization will fit
                // within the buffer we already have, and that we can decrypt in place
                // (see org.apache.cassandra.io.compress.Encryptor.canDecompressInPlace).
                assert encryptor.canDecompressInPlace();
                bufLen = encryptor.uncompress(buffer, 0, buffer.length, buffer, 0);
            }
            
            try (DataInputBuffer dataInputBuffer = new DataInputBuffer(buffer, 0, bufLen))
            {
                components.put(type, type.serializer.deserialize(descriptor.version, dataInputBuffer));
            }
        }

        return components;
    }

    private static void maybeValidateChecksum(CRC32 crc, FileDataInput in, Descriptor descriptor) throws IOException
    {
        if (!descriptor.version.hasMetadataChecksum())
            return;

        int actualChecksum = (int) crc.getValue();
        int expectedChecksum = in.readInt();

        if (actualChecksum != expectedChecksum)
        {
            File file = descriptor.fileFor(Components.STATS);
            throw new CorruptSSTableException(new IOException("Checksums do not match for " + file), file);
        }
    }

    @Override
    public void mutate(Descriptor descriptor, String description, UnaryOperator<StatsMetadata> transform) throws IOException
    {
        if (logger.isTraceEnabled() )
            logger.trace("Mutating {} to {}", descriptor.fileFor(Components.STATS), description);

        mutate(descriptor, transform);
    }

    @Override
    public void mutateLevel(Descriptor descriptor, int newLevel) throws IOException
    {
        if (logger.isTraceEnabled())
            logger.trace("Mutating {} to level {}", descriptor.fileFor(Components.STATS), newLevel);

        mutate(descriptor, stats -> stats.mutateLevel(newLevel));
    }

    @Override
    public void mutateRepairMetadata(Descriptor descriptor, long newRepairedAt, TimeUUID newPendingRepair, boolean isTransient) throws IOException
    {
        if (logger.isTraceEnabled())
            logger.trace("Mutating {} to repairedAt time {} and pendingRepair {}",
                         descriptor.fileFor(Components.STATS), newRepairedAt, newPendingRepair);

        mutate(descriptor, stats -> stats.mutateRepairedMetadata(newRepairedAt, newPendingRepair, isTransient));
    }

    private void mutate(Descriptor descriptor, UnaryOperator<StatsMetadata> transform) throws IOException
    {
        Map<MetadataType, MetadataComponent> currentComponents = deserialize(descriptor, EnumSet.allOf(MetadataType.class));
        StatsMetadata stats = (StatsMetadata) currentComponents.remove(MetadataType.STATS);

        currentComponents.put(MetadataType.STATS, transform.apply(stats));
        rewriteSSTableMetadata(descriptor, currentComponents);
    }

    public void rewriteSSTableMetadata(Descriptor descriptor, Map<MetadataType, MetadataComponent> currentComponents) throws IOException
    {
        File file = descriptor.tmpFileFor(Components.STATS);
        try (DataOutputStreamPlus out = file.newOutputStream(File.WriteMode.OVERWRITE))
        {
            serialize(currentComponents, out, descriptor);
            out.flush();
        }
        catch (IOException e)
        {
            Throwables.throwIfInstanceOf(e, FileNotFoundException.class);
            throw new FSWriteError(e, file);
        }
        file.move(descriptor.fileFor(Components.STATS));
    }

    public void updateSSTableMetadata(Descriptor descriptor, Map<MetadataType, MetadataComponent> updatedComponents) throws IOException
    {
        Map<MetadataType, MetadataComponent> currentComponents = deserialize(descriptor, EnumSet.allOf(MetadataType.class));
        currentComponents.putAll(updatedComponents);
        rewriteSSTableMetadata(descriptor, currentComponents);
    }

    /**
     * Read the compression info file pointed by the given descriptor and create the corresponding encryptor.
     *
     * Returns null if no encryption applies (version doesn't support it, compression is not applied, or the applicable
     * compression does not include encryption).
     */
    // Package-private for testing
    static CompressionParams testCompressionParams = null;
    
    private ICompressor getEncryptor(Descriptor desc, boolean writeTime)
    {
        if (!desc.version.metadataIsEncrypted())
            return null;
        
        // For testing, use the provided compression params
        if (testCompressionParams != null)
        {
            ICompressor compressor = testCompressionParams.getSstableCompressor();
            if (compressor != null)
                return compressor.encryptionOnly();
            return null;
        }
        
        File compressionFile = desc.fileFor(Components.COMPRESSION_INFO);

        try
        {
            // Read the compression metadata from file
            // We pass a small compressedLength as we only need the parameters, not the actual chunk offsets
            CompressionMetadata cm = CompressionMetadata.open(compressionFile, 1024, false);
            // Note: we use only the encryption component, without any compression. The reason for doing this is to
            // avoid having to allocate (and save the size of) an additional buffer to hold the larger uncompressed
            // serialization on reads.
            ICompressor compressor = cm.parameters.getSstableCompressor();
            if (compressor != null)
                return compressor.encryptionOnly();
            return null;
        }
        catch (Throwable t)
        {
            // If we can't read the compression metadata, assume no encryption.
            // During flush, the compression file may not be accessible yet in some implementations
            // causing FSReadError. Catch Throwable to handle both Exception and Error.
            logger.debug("Could not read compression metadata for {}: {}", desc, t.getMessage());
            return null;
        }
    }
}
