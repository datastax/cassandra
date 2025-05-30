/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.index.sasi;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.UUID;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.SecondaryIndexBuilder;
import org.apache.cassandra.index.sasi.conf.ColumnIndex;
import org.apache.cassandra.index.sasi.disk.PerSSTableIndexWriter;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.sstable.SSTableIdentityIterator;
import org.apache.cassandra.io.sstable.SSTableWatcher;
import org.apache.cassandra.io.sstable.format.PartitionIndexIterator;
import org.apache.cassandra.io.sstable.format.RowIndexEntry;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.io.util.ReadPattern;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.UUIDGen;

class SASIIndexBuilder extends SecondaryIndexBuilder
{
    private final ColumnFamilyStore cfs;
    private final UUID compactionId = UUIDGen.getTimeUUID();

    private final Set<Index> builtIndexes;
    private final SortedMap<SSTableReader, Map<ColumnMetadata, ColumnIndex>> sstables;

    private long bytesProcessed = 0;
    private final long totalSizeInBytes;

    public SASIIndexBuilder(ColumnFamilyStore cfs, SortedMap<SSTableReader, Map<ColumnMetadata, ColumnIndex>> sstables, Set<Index> indexes)
    {
        long totalIndexBytes = 0;
        for (SSTableReader sstable : sstables.keySet())
            totalIndexBytes += sstable.uncompressedLength();

        this.cfs = cfs;
        this.sstables = sstables;
        this.builtIndexes = indexes;
        this.totalSizeInBytes = totalIndexBytes;
    }

    public void build()
    {
        AbstractType<?> keyValidator = cfs.metadata().partitionKeyType;
        for (Map.Entry<SSTableReader, Map<ColumnMetadata, ColumnIndex>> e : sstables.entrySet())
        {
            SSTableReader sstable = e.getKey();
            Map<ColumnMetadata, ColumnIndex> indexes = e.getValue();

            SSTableWatcher.instance.onIndexBuild(sstable, builtIndexes);
            try (RandomAccessReader dataFile = sstable.openDataReader(ReadPattern.SEQUENTIAL))
            {
                PerSSTableIndexWriter indexWriter = SASIIndex.newWriter(keyValidator, sstable.descriptor, indexes, OperationType.COMPACTION);

                long previousKeyPosition = 0;
                try (PartitionIndexIterator keys = sstable.allKeysIterator())
                {
                    while (!keys.isExhausted())
                    {
                        throwIfStopRequested();

                        final DecoratedKey key = sstable.decorateKey(keys.key());
                        final long keyPosition = keys.keyPosition();

                        indexWriter.startPartition(key, keyPosition);

                        RowIndexEntry indexEntry = sstable.getPosition(key, SSTableReader.Operator.EQ);
                        dataFile.seek(indexEntry.position);
                        ByteBufferUtil.readWithShortLength(dataFile); // key

                        try (SSTableIdentityIterator partition = SSTableIdentityIterator.create(sstable, dataFile, key))
                        {
                            // if the row has statics attached, it has to be indexed separately
                            if (cfs.metadata().hasStaticColumns())
                                indexWriter.nextUnfilteredCluster(partition.staticRow());

                            while (partition.hasNext())
                                indexWriter.nextUnfilteredCluster(partition.next());
                        }

                        keys.advance();
                        long dataPosition = keys.isExhausted() ? sstable.uncompressedLength() : keys.dataPosition();
                        bytesProcessed += dataPosition - previousKeyPosition;
                        previousKeyPosition = dataPosition;
                    }

                    completeSSTable(indexWriter, sstable, indexes.values());
                }
                catch (IOException ex)
                {
                    throw new FSReadError(ex, sstable.getFilename());
                }
            }
        }
    }

    @Override
    public OperationProgress getProgress()
    {
        return new OperationProgress(cfs.metadata(),
                                     OperationType.INDEX_BUILD,
                                     bytesProcessed,
                                     totalSizeInBytes,
                                     compactionId,
                                     sstables.keySet());
    }

    private void completeSSTable(PerSSTableIndexWriter indexWriter, SSTableReader sstable, Collection<ColumnIndex> indexes)
    {
        indexWriter.complete(sstable);

        for (ColumnIndex index : indexes)
        {
            File tmpIndex = sstable.descriptor.fileFor(index.getComponent());
            if (!tmpIndex.exists()) // no data was inserted into the index for given sstable
                continue;

            index.update(Collections.<SSTableReader>emptyList(), Collections.singletonList(sstable));
        }
    }
}
