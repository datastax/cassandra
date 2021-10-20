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
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.tries.MemtableTrie;
import org.apache.cassandra.db.tries.Trie;
import org.apache.cassandra.index.sai.disk.PerSSTableWriter;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v2.blockindex.BlockIndexFileProvider;
import org.apache.cassandra.index.sai.disk.v2.blockindex.BlockIndexMeta;
import org.apache.cassandra.index.sai.disk.v2.blockindex.BlockIndexReader;
import org.apache.cassandra.index.sai.disk.v2.blockindex.BlockIndexWriter;
import org.apache.cassandra.index.sai.disk.v2.blockindex.BytesUtil;
import org.apache.cassandra.index.sai.disk.v2.blockindex.MergeIndexIterators;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.lucene.store.IndexOutput;

/**
 * Writes all SSTable-attached index token and offset structures.
 */
public class SSTableComponentsWriter implements PerSSTableWriter
{
    Logger logger = LoggerFactory.getLogger(SSTableComponentsWriter.class);

    private final IndexDescriptor indexDescriptor;
    private MemtableTrie<Long> rowMapping;
    private final List<BlockIndexMeta> metadatas;

    public SSTableComponentsWriter(IndexDescriptor indexDescriptor) throws IOException
    {
        this.indexDescriptor = indexDescriptor;
        this.rowMapping = new MemtableTrie<>(BufferType.OFF_HEAP);
        this.metadatas = new ArrayList<>();
    }

    @Override
    public void startPartition(PrimaryKey primaryKey, long position) throws IOException
    {
    }

    @Override
    public void nextRow(PrimaryKey key) throws IOException
    {
        addKeyToMapping(key);
    }

    public void complete() throws IOException
    {
        Stopwatch stopwatch = Stopwatch.createStarted();
        if (rowMapping.valuesCount() > 0)
        {
            flush(!metadatas.isEmpty());
            logger.debug(indexDescriptor.logMessage("Final flush for sstable {}. Elapsed time {}ms"),
                         indexDescriptor.descriptor,
                         stopwatch.elapsed(TimeUnit.MILLISECONDS));
        }
        compactSegments();
        logger.debug(indexDescriptor.logMessage("Compacted segments for sstable {}. Elapsed time {}ms"),
                     indexDescriptor.descriptor,
                     stopwatch.elapsed(TimeUnit.MILLISECONDS));
        indexDescriptor.createComponentOnDisk(IndexComponent.GROUP_COMPLETION_MARKER);
    }

    public void abort(Throwable accumulator)
    {
        logger.debug(indexDescriptor.logMessage("Aborting token/offset writer for {}..."), indexDescriptor.descriptor);
        indexDescriptor.deletePerSSTableIndexComponents();
        indexDescriptor.deletePerSSTableTemporaryComponents();
    }

    private void addKeyToMapping(PrimaryKey key) throws IOException
    {
        try
        {
            if (rowMapping.reachedAllocatedSizeThreshold())
                flush(true);
            rowMapping.apply(Trie.singleton(v -> key.asComparableBytes(v), key.sstableRowId()), (existing, neww) -> neww);
            // If the trie is full then we need to flush it and start a new one
        }
        catch (MemtableTrie.SpaceExhaustedException e)
        {
            throw new IOException(e);
        }
    }

    private void flush(boolean temporary) throws IOException
    {
        try (BlockIndexFileProvider fileProvider = new PerSSTableFileProvider(indexDescriptor))
        {
            BlockIndexWriter writer = new BlockIndexWriter(fileProvider, temporary);
            Iterator<Map.Entry<ByteComparable, Long>> iterator = rowMapping.entryIterator();

            while (iterator.hasNext())
            {
                Map.Entry<ByteComparable, Long> entry = iterator.next();
                writer.add(entry.getKey(), entry.getValue());
            }
            BlockIndexMeta metadata = writer.finish();
            rowMapping.discardBuffers();
            if (temporary)
            {
                metadatas.add(metadata);
                rowMapping = new MemtableTrie<>(BufferType.OFF_HEAP);
            }
            else
            {
                try (final IndexOutput out = fileProvider.openMetadataOutput())
                {
                    metadata.write(out);
                }
            }
        }
    }

    private void compactSegments() throws IOException
    {
        if (metadatas.isEmpty())
            return;

        Stopwatch stopwatch = Stopwatch.createStarted();
        List<BlockIndexReader.IndexIterator> iterators = new ArrayList<>(metadatas.size());
        List<BlockIndexReader> readers = new ArrayList<>(metadatas.size());
        try (BlockIndexFileProvider fileProvider = new PerSSTableFileProvider(indexDescriptor))
        {
            for (BlockIndexMeta metadata : metadatas)
            {
                BlockIndexReader reader = new BlockIndexReader(fileProvider, true, metadata);
                readers.add(reader);
                iterators.add(reader.iterator());
            }
            logger.debug(indexDescriptor.logMessage("Created iterators for {} segments for sstable {}. Elapsed time {} ms"),
                         metadatas.size(),
                         indexDescriptor.descriptor,
                         stopwatch.elapsed(TimeUnit.MILLISECONDS));
            try (MergeIndexIterators mergeIndexIterators = new MergeIndexIterators(iterators))
            {
                BlockIndexWriter writer = new BlockIndexWriter(fileProvider, false);

                long terms = 0;
                while (true)
                {
                    BlockIndexReader.IndexState state = mergeIndexIterators.next();
                    if (state == null)
                    {
                        break;
                    }
                    writer.add(BytesUtil.fixedLength(state.term), state.rowid);
                    terms++;
                }
                logger.debug(indexDescriptor.logMessage("Completed adding {} terms to writer for sstable {}. Elapsed time {} ms"),
                             terms,
                             indexDescriptor.descriptor,
                             stopwatch.elapsed(TimeUnit.MILLISECONDS));

                BlockIndexMeta meta = writer.finish();

                logger.debug(indexDescriptor.logMessage("Completed writing per-sstable index files for sstable {}. Elapsed time {} ms"),
                             indexDescriptor.descriptor,
                             stopwatch.elapsed(TimeUnit.MILLISECONDS));

                try (final IndexOutput out = fileProvider.openMetadataOutput())
                {
                    meta.write(out);
                }
            }
            finally
            {
                FileUtils.closeQuietly(readers);
            }
        }
        finally
        {
            indexDescriptor.deletePerSSTableTemporaryComponents();
        }
    }
}
