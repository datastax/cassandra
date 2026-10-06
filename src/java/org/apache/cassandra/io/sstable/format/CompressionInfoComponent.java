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
import java.nio.file.NoSuchFileException;
import java.util.Set;
import javax.annotation.Nullable;

import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.compress.CompressionMetadataReaderType;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableFormat.Components;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.SliceDescriptor;
import org.apache.cassandra.schema.CompressionParams;

public class CompressionInfoComponent
{
    public static CompressionMetadata maybeLoad(Descriptor descriptor, Set<Component> components, SliceDescriptor sliceDescriptor)
    {
        if (components.contains(Components.COMPRESSION_INFO))
            return load(descriptor, sliceDescriptor);

        return null;
    }

    public static CompressionMetadata loadIfExists(Descriptor descriptor, SliceDescriptor sliceDescriptor)
    {
        if (descriptor.fileFor(Components.COMPRESSION_INFO).exists())
            return load(descriptor, sliceDescriptor);

        return null;
    }

    /**
     * Reads only the header of the compression info file of the given sstable and returns its compression
     * parameters, without loading any chunk offsets (see
     * {@link CompressionMetadata#readCompressionParams(File, boolean, CompressionMetadataReaderType)}).
     *
     * @param descriptor the sstable
     * @param readerType {@link CompressionMetadataReaderType#WRITE_TIME} when the file is read while the sstable is
     *                   still being written (it may not have been uploaded to remote storage yet), which callers on
     *                   the write path must pass; {@link CompressionMetadataReaderType#READ_TIME} otherwise
     * @return the compression parameters, or {@code null} if the sstable has no compression info file
     */
    @Nullable
    public static CompressionParams readCompressionParamsIfExists(Descriptor descriptor, CompressionMetadataReaderType readerType)
    {
        File compressionFile = descriptor.fileFor(Components.COMPRESSION_INFO);
        if (!compressionFile.exists())
            return null;

        // hasMaxCompressedSize must match how the file was written - the version flag - otherwise everything the
        // header stores after the parameters is read at the wrong offset.
        return CompressionMetadata.readCompressionParams(compressionFile,
                                                         descriptor.version.hasMaxCompressedLength(),
                                                         readerType);
    }

    public static CompressionMetadata load(Descriptor descriptor)
    {
        return load(descriptor, SliceDescriptor.NONE);
    }

    public static CompressionMetadata load(Descriptor descriptor, SliceDescriptor sliceDescriptor)
    {
        return CompressionMetadata.open(descriptor.fileFor(Components.COMPRESSION_INFO),
                                        descriptor.fileFor(Components.DATA).length(),
                                        descriptor.version.hasMaxCompressedLength(),
                                        sliceDescriptor);
    }

    /**
     * Best-effort checking to verify the expected compression info component exists, according to the TOC file.
     * The verification depends on the existence of TOC file. If absent, the verification is skipped.
     *
     * @param descriptor
     * @param actualComponents actual components listed from the file system.
     * @throws CorruptSSTableException if TOC expects compression info but not found from disk.
     * @throws FSReadError             if unable to read from TOC file.
     */
    public static void verifyCompressionInfoExistenceIfApplicable(Descriptor descriptor, Set<Component> actualComponents) throws CorruptSSTableException, FSReadError
    {
        File tocFile = descriptor.fileFor(Components.TOC);
        if (tocFile.exists())
        {
            try
            {
                Set<Component> expectedComponents = TOCComponent.loadTOC(descriptor, false);
                if (expectedComponents.contains(Components.COMPRESSION_INFO) && !actualComponents.contains(Components.COMPRESSION_INFO))
                {
                    File compressionInfoFile = descriptor.fileFor(Components.COMPRESSION_INFO);
                    throw new CorruptSSTableException(new NoSuchFileException(compressionInfoFile.absolutePath()), compressionInfoFile);
                }
            }
            catch (IOException e)
            {
                throw new FSReadError(e, tocFile);
            }
        }
    }
}
