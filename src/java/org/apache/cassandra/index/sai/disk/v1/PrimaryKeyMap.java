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
package org.apache.cassandra.index.sai.disk.v1;

import java.io.Closeable;
import java.io.IOException;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.index.sai.disk.io.IndexComponents;
import org.apache.cassandra.index.sai.utils.LongArray;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.Throwables;

public interface PrimaryKeyMap extends Closeable
{
    PrimaryKey primaryKeyFromRowId(long sstableRowId) throws IOException;

    default PrimaryKeyMap copyOf()
    {
        return IDENTITY;
    }

    @VisibleForTesting
    default long size()
    {
        return 0;
    }

    default void close() throws IOException
    {}

    PrimaryKeyMap IDENTITY = sstableRowId -> PrimaryKey.factory().createKey(new Murmur3Partitioner.LongToken(sstableRowId), sstableRowId);

    class DefaultPrimaryKeyMap implements PrimaryKeyMap
    {
        private final long size;
        private final FileHandle primaryKeysFile;
        private final FileHandle primaryKeyOffsetsFile;
        private final RandomAccessReader primaryKeys;
        private final LongArray primaryKeyOffsets;

        private final PrimaryKey.PrimaryKeyFactory keyFactory;

        public DefaultPrimaryKeyMap(IndexComponents indexComponents, TableMetadata tableMetadata) throws IOException
        {
            MetadataSource metadataSource = MetadataSource.loadGroupMetadata(indexComponents);
            NumericValuesMeta meta = new NumericValuesMeta(metadataSource.get(IndexComponents.PRIMARY_KEY_OFFSETS.name));
            this.size = meta.valueCount;

            this.primaryKeyOffsetsFile = indexComponents.createFileHandle(IndexComponents.PRIMARY_KEY_OFFSETS);
            this.primaryKeyOffsets = new MonotonicBlockPackedReader(primaryKeyOffsetsFile, indexComponents, meta).open();

            this.primaryKeysFile = indexComponents.createFileHandle(IndexComponents.PRIMARY_KEYS);
            this.primaryKeys = primaryKeysFile.createReader();

            this.keyFactory = PrimaryKey.factory(tableMetadata);
        }

        private DefaultPrimaryKeyMap(DefaultPrimaryKeyMap orig)
        {
            this.primaryKeysFile = orig.primaryKeysFile.sharedCopy();
            this.primaryKeyOffsetsFile = orig.primaryKeyOffsetsFile.sharedCopy();

            this.primaryKeys = primaryKeysFile.createReader();
            this.primaryKeyOffsets = orig.primaryKeyOffsets;

            this.keyFactory = orig.keyFactory.copyOf();
            this.size = orig.size();
        }


        @Override
        public PrimaryKeyMap copyOf()
        {
            try
            {
                return new DefaultPrimaryKeyMap(this);
            }
            catch (Throwable e)
            {
                throw Throwables.unchecked(e);
            }
        }

        @Override
        public long size()
        {
            return size;
        }

        @Override
        public PrimaryKey primaryKeyFromRowId(long sstableRowId) throws IOException
        {
            long startOffset = primaryKeyOffsets.get(sstableRowId);
            long endOffset = (sstableRowId + 1) < size
                    ? primaryKeyOffsets.get(sstableRowId + 1)
                    : primaryKeys.length();

            if (endOffset - startOffset <= 0)
                throw new IOException(
                        "Primary key length <= 0 for row " + sstableRowId + " of " + primaryKeyOffsetsFile.path());
            if (endOffset - startOffset > Integer.MAX_VALUE)
                throw new IOException(
                        "Primary key length too large for row " + sstableRowId + " of " + primaryKeyOffsetsFile.path());

            int length = (int) (endOffset - startOffset);
            byte[] serializedKeyBuf = new byte[length];

            primaryKeys.seek(startOffset);
            primaryKeys.readFully(serializedKeyBuf);
            return keyFactory.createKey(serializedKeyBuf, sstableRowId);
        }

        @Override
        public void close() throws IOException
        {
            FileUtils.close(primaryKeyOffsets, primaryKeyOffsetsFile, primaryKeys, primaryKeysFile);
        }
    }
}
