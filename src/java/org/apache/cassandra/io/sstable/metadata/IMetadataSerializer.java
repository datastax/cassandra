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

import java.io.IOException;
import java.util.EnumSet;
import java.util.Map;
import java.util.function.UnaryOperator;

import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.utils.TimeUUID;

/**
 * Interface for SSTable metadata serializer
 * <p>
 * SSTable writers save the metadata through {@link #rewriteSSTableMetadata(Descriptor, Map, CompressionParams)}
 * (see {@link org.apache.cassandra.io.sstable.format.StatsComponent#save(Descriptor, CompressionParams)}), not
 * through {@link #rewriteSSTableMetadata(Descriptor, Map)} or {@link #serialize(Map, DataOutputPlus, Descriptor)}:
 * implementations overriding or wrapping a serializer must override or forward the overloads taking the compression
 * parameters too, otherwise the metadata of sstables being written falls back to reading the encryptor from the
 * compression info file.
 */
public interface IMetadataSerializer
{
    /**
     * Serialize given metadata components. This should be called after all other components have been written.
     * In particular, if the sstable version encrypts its metadata, the method reads the COMPRESSION_INFO component
     * (through the {@link org.apache.cassandra.io.compress.CompressionMetadataReaderType#WRITE_TIME write-time}
     * channel) to retrieve the encryptor that must be used for the metadata to avoid leaking sensitive data in e.g.
     * min/max clusterings. Callers that know the compression parameters of the sstable's data file should use
     * {@link #serialize(Map, DataOutputPlus, Descriptor, CompressionParams)} instead.
     *
     * @param components Metadata components to serialize
     * @param out
     * @param descriptor
     * @throws IOException
     * @throws org.apache.cassandra.io.sstable.CorruptSSTableException if the metadata may have to be encrypted, the
     *                                                                 TOC lists a compression info file and it does
     *                                                                 not exist
     */
    void serialize(Map<MetadataType, MetadataComponent> components, DataOutputPlus out, Descriptor descriptor) throws IOException;

    /**
     * Serialize given metadata components, encrypting them - if the sstable version encrypts its metadata - with the
     * encryptor of the given compression parameters, which must be those the sstable's data file is (being) written
     * with (i.e. those stored in its COMPRESSION_INFO component). Unlike
     * {@link #serialize(Map, DataOutputPlus, Descriptor)}, the COMPRESSION_INFO component is not read, so it does
     * not need to have been written yet.
     * <p>
     * The default implementation ignores the parameters and delegates to
     * {@link #serialize(Map, DataOutputPlus, Descriptor)}, for the benefit of implementations predating this method.
     *
     * @param components        Metadata components to serialize
     * @param out
     * @param descriptor
     * @param compressionParams the compression parameters of the data file, not null:
     *                          {@link CompressionParams#noCompression()} if it is not compressed
     * @throws IOException
     */
    default void serialize(Map<MetadataType, MetadataComponent> components, DataOutputPlus out, Descriptor descriptor, CompressionParams compressionParams) throws IOException
    {
        serialize(components, out, descriptor);
    }

    /**
     * Deserialize specified metadata components from given descriptor.
     *
     * @param descriptor SSTable descriptor
     * @return Deserialized metadata components, in deserialized order.
     * @throws IOException
     */
    Map<MetadataType, MetadataComponent> deserialize(Descriptor descriptor, EnumSet<MetadataType> types) throws IOException;

    /**
     * Deserialized only metadata component specified from given descriptor.
     *
     * @param descriptor SSTable descriptor
     * @param type Metadata component type to deserialize
     * @return Deserialized metadata component. Can be null if specified type does not exist.
     * @throws IOException
     */
    MetadataComponent deserialize(Descriptor descriptor, MetadataType type) throws IOException;

    /**
     * Mutate SSTable Metadata
     *
     * NOTE: mutating stats metadata of a live sstable will race with entire-sstable-streaming, please use
     * {@link SSTableReader#mutateLevelAndReload} instead on live sstable.
     *
     * @param descriptor SSTable descriptor
     * @param description on changed attributions
     * @param transform function to mutate sstable metadata
     * @throws IOException
     */
    public void mutate(Descriptor descriptor, String description, UnaryOperator<StatsMetadata> transform) throws IOException;

    /**
     * Mutate SSTable level
     *
     * NOTE: mutating stats metadata of a live sstable will race with entire-sstable-streaming, please use
     * {@link SSTableReader#mutateLevelAndReload} instead on live sstable.
     *
     * @param descriptor SSTable descriptor
     * @param newLevel new SSTable level
     * @throws IOException
     */
    void mutateLevel(Descriptor descriptor, int newLevel) throws IOException;

    /**
     * Mutate the repairedAt time, pendingRepair ID, and transient status.
     *
     * NOTE: mutating stats metadata of a live sstable will race with entire-sstable-streaming, please use
     * {@link SSTableReader#mutateLevelAndReload} instead on live sstable.
     */
    public void mutateRepairMetadata(Descriptor descriptor, long newRepairedAt, TimeUUID newPendingRepair, boolean isTransient) throws IOException;

    /**
     * Replace the sstable metadata file ({@code -Statistics.db}) with the given components.
     * If the sstable version encrypts its metadata, the encryptor is read from the sstable's COMPRESSION_INFO
     * component (see {@link #serialize(Map, DataOutputPlus, Descriptor)}), through the
     * {@link org.apache.cassandra.io.compress.CompressionMetadataReaderType#READ_TIME read-time} channel if the
     * sstable is complete (it has a TOC), through the write-time one otherwise. Writers should use
     * {@link #rewriteSSTableMetadata(Descriptor, Map, CompressionParams)}.
     */
    void rewriteSSTableMetadata(Descriptor descriptor, Map<MetadataType, MetadataComponent> currentComponents) throws IOException;

    /**
     * Replace the sstable metadata file ({@code -Statistics.db}) with the given components, encrypting them with the
     * given compression parameters, not null (see {@link #serialize(Map, DataOutputPlus, Descriptor, CompressionParams)}).
     * This is what sstable writers use, as they know the parameters their data file is written with.
     * <p>
     * The default implementation ignores the parameters and delegates to
     * {@link #rewriteSSTableMetadata(Descriptor, Map)}, for the benefit of implementations predating this method.
     */
    default void rewriteSSTableMetadata(Descriptor descriptor, Map<MetadataType, MetadataComponent> currentComponents, CompressionParams compressionParams) throws IOException
    {
        rewriteSSTableMetadata(descriptor, currentComponents);
    }

    /**
     * Updates the sstable metadata components (works similarly to {@link #rewriteSSTableMetadata(Descriptor, Map)} but
     * only updates the provided components rather than replacing the whole metadata map).
     */
    void updateSSTableMetadata(Descriptor descriptor, Map<MetadataType, MetadataComponent> updatedComponents) throws IOException;

}
