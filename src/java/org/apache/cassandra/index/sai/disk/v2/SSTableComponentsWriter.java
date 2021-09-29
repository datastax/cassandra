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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.tries.MemtableTrie;
import org.apache.cassandra.db.tries.Trie;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.sai.disk.PerSSTableWriter;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.io.IndexOutputWriter;
import org.apache.cassandra.index.sai.disk.v1.MetadataWriter;
import org.apache.cassandra.index.sai.disk.v1.NumericValuesWriter;
import org.apache.cassandra.index.sai.disk.v1.TrieTermsDictionaryReader;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.SAICodecUtils;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.tries.IncrementalDeepTrieWriterPageAware;
import org.apache.cassandra.io.tries.IncrementalTrieWriter;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.lucene.store.IndexOutput;

/**
 * Writes all SSTable-attached index token and offset structures.
 */
public class SSTableComponentsWriter implements PerSSTableWriter
{
    Logger logger = LoggerFactory.getLogger(SSTableComponentsWriter.class);
    int MAX_RECURSIVE_KEY_LENGTH = 128;

    private final IndexDescriptor indexDescriptor;
    private final MetadataWriter metadataWriter;
    private final IndexOutputWriter primaryKeys;
    private final NumericValuesWriter primaryKeyOffsets;
    private final List<Long> roots;

    private MemtableTrie<Long> rowMapping;


    public SSTableComponentsWriter(IndexDescriptor indexDescriptor) throws IOException
    {
        this.indexDescriptor = indexDescriptor;
        this.primaryKeys = indexDescriptor.openPerSSTableOutput(IndexComponent.PRIMARY_KEYS);
        SAICodecUtils.writeHeader(this.primaryKeys);
        this.metadataWriter = new MetadataWriter(indexDescriptor.openPerSSTableOutput(IndexComponent.GROUP_META));
        this.primaryKeyOffsets = new NumericValuesWriter(indexDescriptor.version.fileNameFormatter().format(IndexComponent.PRIMARY_KEY_OFFSETS, null),
                                                         indexDescriptor.openPerSSTableOutput(IndexComponent.PRIMARY_KEY_OFFSETS),
                                                         metadataWriter,
                                                         true);
        this.rowMapping = new MemtableTrie<>(BufferType.OFF_HEAP);
        this.roots = new ArrayList<>();
    }

    @Override
    public void startPartition(PrimaryKey primaryKey, long position) throws IOException
    {
        addKeyToMapping(primaryKey);
    }

    @Override
    public void nextRow(PrimaryKey key) throws IOException
    {
        long offset = primaryKeys.getFilePointer();
        PrimaryKey.serializer.serialize(primaryKeys.asSequentialWriter(), 0, key);
        primaryKeyOffsets.add(offset);
        addKeyToMapping(key);
    }

    public void complete() throws IOException
    {
        flush();
        compactPrimaryKeyMap();
        SAICodecUtils.writeFooter(primaryKeys);
        FileUtils.close(primaryKeys, primaryKeyOffsets, metadataWriter);
        indexDescriptor.createComponentOnDisk(IndexComponent.GROUP_COMPLETION_MARKER);
    }

    public void abort(Throwable accumulator)
    {
        logger.debug(indexDescriptor.logMessage("Aborting token/offset writer for {}..."), indexDescriptor.descriptor);
        indexDescriptor.deletePerSSTableIndexComponents();
    }

    private void addKeyToMapping(PrimaryKey key) throws IOException
    {
        try
        {
//            if (key.size() <= MAX_RECURSIVE_KEY_LENGTH)
//                rowMapping.putRecursive(v -> key.asComparableBytes(v), key.sstableRowId(), (existing, neww) -> neww);
//            else
                rowMapping.apply(Trie.singleton(v -> key.asComparableBytes(v), key.sstableRowId()), (existing, neww) -> neww);
            // If the trie is full then we need to flush it and start a new one
            if (rowMapping.reachedAllocatedSizeThreshold())
                flush();
        }
        catch (MemtableTrie.SpaceExhaustedException e)
        {
            throw new IOException(e);
        }
    }

    private void flush() throws IOException
    {
        try (IndexOutputWriter writer = indexDescriptor.openPerSSTableOutput(IndexComponent.PRIMARY_KEY_MAP, true, true);
             IncrementalTrieWriter<Long> trieWriter = new IncrementalDeepTrieWriterPageAware<>(TrieTermsDictionaryReader.trieSerializer,
                                                                                               writer.asSequentialWriter()))
        {

            Iterator<Map.Entry<ByteComparable, Long>> iterator = rowMapping.entryIterator();

            while (iterator.hasNext())
            {
                Map.Entry<ByteComparable, Long> entry = iterator.next();
                trieWriter.add(entry.getKey(), entry.getValue());
            }

            roots.add(trieWriter.complete());
        }

        rowMapping = new MemtableTrie<>(BufferType.OFF_HEAP);
    }

    private void compactPrimaryKeyMap() throws IOException
    {
        try (FileHandle mapFileHandle = indexDescriptor.createPerSSTableFileHandle(IndexComponent.PRIMARY_KEY_MAP, true);
             IndexOutputWriter output = indexDescriptor.openPerSSTableOutput(IndexComponent.PRIMARY_KEY_MAP, true, false);
             IncrementalTrieWriter<Long> writer = new IncrementalDeepTrieWriterPageAware<>(TrieTermsDictionaryReader.trieSerializer, output.asSequentialWriter()))
        {
            SAICodecUtils.writeHeader(output);

            for (long root : roots)
            {
                Iterator<Pair<ByteComparable, Long>> iterator = new TrieTermsDictionaryReader(mapFileHandle.instantiateRebufferer(), root).iterator();
                while (iterator.hasNext())
                {
                    Pair<ByteComparable, Long> entry = iterator.next();
                    writer.add(entry.left, entry.right);
                }
            }

            try (IndexOutput metadata = metadataWriter.builder(indexDescriptor.version.fileNameFormatter().format(IndexComponent.PRIMARY_KEY_MAP, null)))
            {
                metadata.writeLong(writer.complete());
            }
            SAICodecUtils.writeFooter(output);
        }
    }
}
