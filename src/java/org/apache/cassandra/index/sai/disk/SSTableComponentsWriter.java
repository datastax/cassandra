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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.Files;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.tries.MemtableTrie;
import org.apache.cassandra.db.tries.Trie;
import org.apache.cassandra.index.sai.SSTableIndex;
import org.apache.cassandra.index.sai.disk.io.IndexComponents;
import org.apache.cassandra.index.sai.disk.v1.BKDReader;
import org.apache.cassandra.index.sai.disk.v1.MetadataWriter;
import org.apache.cassandra.index.sai.disk.v1.NumericIndexWriter;
import org.apache.cassandra.index.sai.disk.v1.NumericValuesWriter;
import org.apache.cassandra.index.sai.disk.v1.PartitionKeysMeta;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.IOUtils;

/**
 * Writes all SSTable-attached index token and offset structures.
 */
public interface SSTableComponentsWriter
{
    Logger logger = LoggerFactory.getLogger(SSTableComponentsWriter.class);

    int MAX_RECURSIVE_KEY_LENGTH = 128;

    SSTableComponentsWriter NONE = (key) -> {};

    void nextRow(PrimaryKey key) throws MemtableTrie.SpaceExhaustedException, IOException;

    default void complete() throws IOException
    {}

    default void abort(Throwable accumulator)
    {}

    class OnDiskSSTableComponentsWriter implements SSTableComponentsWriter
    {
        private final Descriptor descriptor;
        private final IndexComponents indexComponents;
        private final List<PartitionKeysMeta> partitionKeysMetas = new ArrayList<>();

        private MemtableTrie<Long> rowMapping;
        private long maxSSTableRowId;
        private long rowCount;
        private int maxKeyLength = Integer.MIN_VALUE;

        public OnDiskSSTableComponentsWriter(Descriptor descriptor, CompressionParams compressionParams) throws IOException
        {
            this.descriptor = descriptor;
            indexComponents = IndexComponents.perSSTable(descriptor, compressionParams);
            this.rowMapping = new MemtableTrie<>(BufferType.OFF_HEAP);
        }

        public void nextRow(PrimaryKey key) throws MemtableTrie.SpaceExhaustedException, IOException
        {
            if (key.size() <= MAX_RECURSIVE_KEY_LENGTH)
                rowMapping.putRecursive(v -> key.asComparableBytes(v), key.sstableRowId(), (existing, neww) -> neww);
            else
                rowMapping.apply(Trie.singleton(v -> key.asComparableBytes(v), key.sstableRowId()), (existing, neww) -> neww);
            maxSSTableRowId = key.sstableRowId();
            maxKeyLength = Math.max(maxKeyLength, key.asBytes().length);
            rowCount++;
            // If the trie is full then we need to flush it and start a new one
            if (rowMapping.reachedAllocatedSizeThreshold())
                flush();
        }

        public void complete() throws IOException
        {
            if (rowCount > 0)
                flush();
            maxKeyLength = partitionKeysMetas.stream().map(meta -> meta.bytesPerDim).max(Comparator.naturalOrder()).orElse(0);
            rowCount = partitionKeysMetas.stream().mapToLong(meta -> meta.pointCount).sum();
            List<BKDReader> readers = new ArrayList<>(partitionKeysMetas.size());
            List<BKDReader.IteratorState> iteratorStates = new ArrayList<>(partitionKeysMetas.size());
            try (NumericIndexWriter writer = new NumericIndexWriter(indexComponents, maxKeyLength, maxSSTableRowId, rowCount, false);
                 IndexOutput bkdOutput = indexComponents.createOutput(IndexComponents.PRIMARY_KEYS);
                 MetadataWriter metadataWriter = new MetadataWriter(indexComponents.createOutput(IndexComponents.GROUP_META));
                 IndexOutput metaOutput = metadataWriter.builder(IndexComponents.PRIMARY_KEYS.name))
            {
                if (rowCount > 0)
                {
                    try (FileHandle segmentedPrimaryKeys = indexComponents.createFileHandle(indexComponents.PRIMARY_KEYS, true))
                    {
                        for (PartitionKeysMeta partitionKeysMeta : partitionKeysMetas)
                        {
                            iteratorStates.add(createIteratorState(partitionKeysMeta, segmentedPrimaryKeys.sharedCopy(), readers));
                        }
                    }
                }
                writer.writeIndex(bkdOutput, new MergeOneDimPointValues(iteratorStates, maxKeyLength)).write(metaOutput);
            }
            finally
            {
                indexComponents.deleteTemporaryComponent(indexComponents.PRIMARY_KEYS);
                FileUtils.closeQuietly(iteratorStates);
                FileUtils.closeQuietly(readers);
            }
            indexComponents.createGroupCompletionMarker();
        }

        public void abort(Throwable accumulator)
        {
            logger.debug(indexComponents.logMessage("Aborting token/offset writer for {}..."), descriptor);
            IndexComponents.deletePerSSTableIndexComponents(descriptor);
        }

        private BKDReader.IteratorState createIteratorState(PartitionKeysMeta partitionKeysMeta,
                                                            FileHandle segmentedPrimaryKeys,
                                                            List<BKDReader> readers) throws IOException
        {
            BKDReader reader = new BKDReader(indexComponents, segmentedPrimaryKeys, partitionKeysMeta.bkdPosition);
            readers.add(reader);
            return reader.iteratorState();
        }

        private void flush() throws IOException
        {
            // If no rows were added then the maxKeyLength will be negative
            maxKeyLength = maxKeyLength < 0 ? 0 : maxKeyLength;
            try (NumericIndexWriter writer = new NumericIndexWriter(indexComponents, maxKeyLength, maxSSTableRowId, rowCount, true);
                 IndexOutput bkdOutput = indexComponents.createOutput(IndexComponents.PRIMARY_KEYS, true, true))
            {
                partitionKeysMetas.add(writer.writeIndex(bkdOutput, new OneDimPointValues(rowMapping, maxKeyLength)));
            }
            maxKeyLength = Integer.MIN_VALUE;
            rowMapping = new MemtableTrie<>(BufferType.OFF_HEAP);
            rowCount = 0;
        }
    }

    class OneDimPointValues extends MutableOneDimPointValues
    {
        private final MemtableTrie<Long> trie;
        private final byte[] buffer;

        OneDimPointValues(MemtableTrie<Long> trie, int maximumSize)
        {
            this.trie = trie;
            this.buffer = new byte[maximumSize];
        }

        @Override
        public void intersect(IntersectVisitor visitor) throws IOException
        {
            try
            {
                Iterator<Map.Entry<ByteComparable, Long>> iterator = trie.entrySet().iterator();
                while (iterator.hasNext())
                {
                    Map.Entry<ByteComparable, Long> entry = iterator.next();
                    ByteBufferUtil.toBytes(entry.getKey().asComparableBytes(ByteComparable.Version.OSS41), buffer);
                    long rowId = entry.getValue();
                    visitor.visit(rowId, buffer);
                }
            }
            catch (Exception e)
            {
                throw new IOException(e);
            }
        }
    }
}
