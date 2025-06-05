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

import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.config.CassandraRelevantProperties.ENCRYPTOR_EXTRACTOR_CLASS;

interface EncryptorExtractor
{
    EncryptorExtractor INSTANCE = ENCRYPTOR_EXTRACTOR_CLASS.isPresent()
                                       ? FBUtilities.construct(ENCRYPTOR_EXTRACTOR_CLASS.getString(), "Encryptor extractor instance")
                                       : new DefaultEncryptorExtractor();

    ICompressor getEncryptor(Descriptor desc);

    class DefaultEncryptorExtractor implements EncryptorExtractor
    {
        /**
         * Read the compression info file pointed by the given descriptor and create the corresponding encryptor.
         * Returns null if no encryption applies (version doesn't support it, compression is not applied, or the applicable
         * compression does not include encryption).
         */
        @Override
        public ICompressor getEncryptor(Descriptor desc)
        {
            if (!desc.version.metadataAreEncrypted())
                return null;
            File compressionFile = desc.fileFor(Component.COMPRESSION_INFO);
            if (!compressionFile.exists())
                return null;

            try (CompressionMetadata cm = CompressionMetadata.read(compressionFile, true))
            {
                // Note: we use only the encryption component, without any compression. The reason for doing this is to
                // avoid having to allocate (and save the size of) an additional buffer to hold the larger uncompressed
                // serialization on reads.
                return cm.parameters.getSstableCompressor().encryptionOnly();
            }
        }
    }
}
