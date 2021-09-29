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
package org.apache.cassandra.index.sai.disk.v2;

import java.io.IOException;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.apache.cassandra.index.sai.SSTableQueryContext;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.LongArray;
import org.apache.cassandra.index.sai.disk.v1.MetadataSource;
import org.apache.cassandra.index.sai.disk.v1.MonotonicBlockPackedReader;
import org.apache.cassandra.index.sai.disk.v1.NumericValuesMeta;
import org.apache.cassandra.index.sai.disk.v1.TrieTermsDictionaryReader;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

public class V2PrimaryKeyMap implements PrimaryKeyMap
{
    public static class V2PrimaryKeyMapFactory implements PrimaryKeyMap.Factory
    {
        private final FileHandle primaryKeysFile;
        private final FileHandle primaryKeyOffsetsFile;
        private final FileHandle primaryKeyMapFile;
        private final long primaryKeyMapRoot;
        private final LongArray.Factory primaryKeyOffsetsFactory;
        private final MetadataSource metadata;
        private final PrimaryKey.PrimaryKeyFactory keyFactory;
        private final long size;

        public V2PrimaryKeyMapFactory(IndexDescriptor indexDescriptor) throws IOException
        {
            String offsetsComponentName = indexDescriptor.version.fileNameFormatter().format(IndexComponent.PRIMARY_KEY_OFFSETS, null);
            this.metadata = MetadataSource.load(indexDescriptor.openPerSSTableInput(IndexComponent.GROUP_META));
            NumericValuesMeta primaryKeyOffsetsMeta = new NumericValuesMeta(metadata.get(offsetsComponentName));
            this.size = primaryKeyOffsetsMeta.valueCount;

            this.primaryKeyOffsetsFile = indexDescriptor.createPerSSTableFileHandle(IndexComponent.PRIMARY_KEY_OFFSETS);
            this.primaryKeyOffsetsFactory = new MonotonicBlockPackedReader(primaryKeyOffsetsFile, primaryKeyOffsetsMeta);

            this.primaryKeysFile = indexDescriptor.createPerSSTableFileHandle(IndexComponent.PRIMARY_KEYS);

            String primaryKeyMapComponentName = indexDescriptor.version.fileNameFormatter().format(IndexComponent.PRIMARY_KEY_MAP, null);

            this.primaryKeyMapRoot = metadata.get(primaryKeyMapComponentName).readLong();

            this.primaryKeyMapFile = indexDescriptor.createPerSSTableFileHandle(IndexComponent.PRIMARY_KEY_MAP);

            this.keyFactory = indexDescriptor.primaryKeyFactory;
        }

        @Override
        public PrimaryKeyMap newPerSSTablePrimaryKeyMap(SSTableQueryContext context)
        {
            return new V2PrimaryKeyMap(new LongArray.DeferredLongArray(() -> primaryKeyOffsetsFactory.open()),
                                       primaryKeysFile.createReader(),
                                       new TrieTermsDictionaryReader(primaryKeyMapFile.instantiateRebufferer(), primaryKeyMapRoot),
                                       keyFactory,
                                       size);
        }

        @Override
        public void close() throws IOException
        {
            FileUtils.closeQuietly(primaryKeyOffsetsFile, primaryKeysFile);
        }
    }

    private final LongArray primaryKeyOffsets;
    private final RandomAccessReader primaryKeys;
    private final TrieTermsDictionaryReader primaryKeyToRowIdMap;
    private final PrimaryKey.PrimaryKeyFactory keyFactory;
    private final long size;

    private V2PrimaryKeyMap(LongArray primaryKeyOffsets,
                            RandomAccessReader primaryKeys,
                            TrieTermsDictionaryReader primaryKeyToRowIdMap,
                            PrimaryKey.PrimaryKeyFactory keyFactory,
                            long size)
    {
        this.primaryKeyOffsets = primaryKeyOffsets;
        this.primaryKeys = primaryKeys;
        this.primaryKeyToRowIdMap = primaryKeyToRowIdMap;
        this.keyFactory = keyFactory;
        this.size = size;
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
        primaryKeys.seek(startOffset);
        return keyFactory.createKey(primaryKeys, sstableRowId);
    }

    @Override
    public long rowIdFromPrimaryKey(PrimaryKey key) throws IOException
    {
        return primaryKeyToRowIdMap.exactMatch(v -> key.asComparableBytes(v));
    }

    @Override
    public void close() throws IOException
    {
        FileUtils.close(primaryKeyOffsets, primaryKeys);
    }
}
