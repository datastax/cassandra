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
import java.util.Arrays;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.exceptions.UnknownColumnException;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableFormat.Components;
import org.apache.cassandra.io.sstable.metadata.CompactionMetadata;
import org.apache.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.sstable.metadata.ValidationMetadata;
import org.apache.cassandra.schema.TableMetadata;

public class StatsComponent
{
    public final Descriptor descriptor;
    public final Map<MetadataType, MetadataComponent> metadata;

    public StatsComponent(Descriptor descriptor, Map<MetadataType, MetadataComponent> metadata)
    {
        this.descriptor = descriptor;
        this.metadata = ImmutableMap.copyOf(metadata);
    }

    public static StatsComponent load(Descriptor descriptor) throws IOException
    {
        return load(descriptor, MetadataType.values());
    }

    public static StatsComponent load(Descriptor descriptor, MetadataType... types) throws IOException
    {
        Map<MetadataType, MetadataComponent> metadata;
        try
        {
            metadata = descriptor.getMetadataSerializer().deserialize(descriptor, EnumSet.copyOf(Arrays.asList(types)));
        }
        catch (IOException e)
        {
            throw new CorruptSSTableException(e, descriptor.fileFor(Components.STATS));
        }

        return new StatsComponent(descriptor, metadata);
    }

    public SerializationHeader.Component serializationHeader()
    {
        return (SerializationHeader.Component) metadata.get(MetadataType.HEADER);
    }

    public SerializationHeader serializationHeader(Descriptor descriptor, TableMetadata metadata)
    {
        SerializationHeader.Component header = serializationHeader();
        if (header != null)
        {
            try
            {
                return header.toHeader(descriptor, metadata);
            }
            catch (UnknownColumnException ex)
            {
                throw new IllegalArgumentException(ex);
            }
        }

        return null;
    }

    public CompactionMetadata compactionMetadata()
    {
        return (CompactionMetadata) metadata.get(MetadataType.COMPACTION);
    }

    public ValidationMetadata validationMetadata()
    {
        return (ValidationMetadata) metadata.get(MetadataType.VALIDATION);
    }

    public StatsMetadata statsMetadata()
    {
        return (StatsMetadata) metadata.get(MetadataType.STATS);
    }

    public StatsComponent with(ValidationMetadata validationMetadata)
    {
        Map<MetadataType, MetadataComponent> newMetadata = new EnumMap<>(metadata);
        newMetadata.put(MetadataType.VALIDATION, validationMetadata);
        return new StatsComponent(descriptor, newMetadata);
    }

    public void save(Descriptor desc)
    {
        try
        {
            desc.getMetadataSerializer().rewriteSSTableMetadata(desc, metadata);
        }
        catch (IOException e)
        {
            throw new FSWriteError(e);
        }
    }
}
