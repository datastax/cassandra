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
package org.apache.cassandra.index.sai.disk;

import org.apache.cassandra.index.sai.disk.io.IndexComponents;
import org.apache.cassandra.index.sai.disk.v1.MetadataWriter;
import org.apache.cassandra.index.sai.disk.v1.NumericValuesWriter;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.SAICodecUtils;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.lucene.store.IndexOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Writes all SSTable-attached index token and offset structures.
 */
public interface SSTableComponentsWriter
{
    Logger logger = LoggerFactory.getLogger(SSTableComponentsWriter.class);

    SSTableComponentsWriter NONE = (key) -> {};

    void nextRow(PrimaryKey key) throws IOException;

    default void complete() throws IOException
    {}

    default void abort(Throwable accumulator)
    {}

    class OnDiskSSTableComponentsWriter implements SSTableComponentsWriter
    {
        private final Descriptor descriptor;
        private final IndexComponents indexComponents;

        private final MetadataWriter metadataWriter;
        private final IndexOutput primaryKeys;
        private final NumericValuesWriter primaryKeyOffsets;

        public OnDiskSSTableComponentsWriter(Descriptor descriptor, CompressionParams compressionParams) throws IOException
        {
            this.descriptor = descriptor;
            this.indexComponents = IndexComponents.perSSTable(descriptor, compressionParams);
            this.primaryKeys = indexComponents.createOutput(IndexComponents.PRIMARY_KEYS);
            SAICodecUtils.writeHeader(this.primaryKeys);

            this.metadataWriter = new MetadataWriter(indexComponents.createOutput(IndexComponents.GROUP_META));
            this.primaryKeyOffsets = new NumericValuesWriter(
                    IndexComponents.PRIMARY_KEY_OFFSETS,
                    indexComponents.createOutput(IndexComponents.PRIMARY_KEY_OFFSETS),
                    metadataWriter,
                    true);
        }

        public void nextRow(PrimaryKey key) throws IOException
        {
            long offset = primaryKeys.getFilePointer();
            byte[] rawKey = key.asBytes();
            primaryKeys.writeBytes(rawKey, rawKey.length);
            primaryKeyOffsets.add(offset);
        }

        public void complete() throws IOException
        {
            SAICodecUtils.writeFooter(primaryKeys);
            FileUtils.close(primaryKeys, primaryKeyOffsets, metadataWriter);
            indexComponents.createGroupCompletionMarker();
        }

        public void abort(Throwable accumulator)
        {
            logger.debug(indexComponents.logMessage("Aborting token/offset writer for {}..."), descriptor);
            IndexComponents.deletePerSSTableIndexComponents(descriptor);
        }

    }
}
