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

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.sai.SSTableQueryContext;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.block.BlockPackedReader;
import org.apache.cassandra.index.sai.disk.v1.block.MonotonicBlockPackedReader;
import org.apache.cassandra.index.sai.disk.v1.block.NumericValuesMeta;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.io.sstable.SSTableUniqueIdentifier;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.utils.Throwables;

public class V1PrimaryKeyMap implements PrimaryKeyMap
{
    public static class V1PrimaryKeyMapFactory implements Factory
    {
        private final LongArray.Factory tokenReaderFactory;
        private final LongArray.Factory offsetReaderFactory;
        private final MetadataSource metadata;
        private final long size;
        private final KeyFetcher keyFetcher;
        private final IPartitioner partitioner;
        private final PrimaryKey.PrimaryKeyFactory primaryKeyFactory;
        private final SSTableUniqueIdentifier generation;

        private FileHandle token = null;
        private FileHandle offset = null;

        public V1PrimaryKeyMapFactory(IndexDescriptor indexDescriptor, SSTableReader sstable)
        {
            String offsetsComponentName = indexDescriptor.version.fileNameFormatter().format(IndexComponent.OFFSETS_VALUES, null);
            String tokensComponentName = indexDescriptor.version.fileNameFormatter().format(IndexComponent.TOKEN_VALUES, null);
            try
            {
                this.metadata = MetadataSource.loadGroupMetadata(indexDescriptor);
                NumericValuesMeta offsetsMeta = new NumericValuesMeta(this.metadata.get(offsetsComponentName));
                NumericValuesMeta tokensMeta = new NumericValuesMeta(this.metadata.get(tokensComponentName));
                this.size = offsetsMeta.valueCount;

                token = indexDescriptor.createPerSSTableFileHandle(IndexComponent.TOKEN_VALUES);
                offset = indexDescriptor.createPerSSTableFileHandle(IndexComponent.OFFSETS_VALUES);

                this.tokenReaderFactory = new BlockPackedReader(token, tokensMeta);
                this.offsetReaderFactory = new MonotonicBlockPackedReader(offset, offsetsMeta);
                this.partitioner = indexDescriptor.partitioner;
                this.keyFetcher = new KeyFetcher(sstable);
                this.primaryKeyFactory = indexDescriptor.primaryKeyFactory;
                this.generation = indexDescriptor.descriptor.generation;
            }
            catch (Throwable t)
            {
                throw Throwables.unchecked(Throwables.close(t, token, offset));
            }
        }

        @Override
        public PrimaryKeyMap newPerSSTablePrimaryKeyMap(SSTableQueryContext context)
        {
            final LongArray rowIdToToken = new LongArray.DeferredLongArray(() -> tokenReaderFactory.openTokenReader(0, context));
            final LongArray rowIdToOffset = new LongArray.DeferredLongArray(() -> offsetReaderFactory.open());

            return new V1PrimaryKeyMap(rowIdToToken, rowIdToOffset, partitioner, keyFetcher, primaryKeyFactory, size, generation);
        }

        @Override
        public void close() throws IOException
        {
            FileUtils.closeQuietly(token, offset);
        }
    }

    private final LongArray rowIdToToken;
    private final LongArray rowIdToOffset;
    private final IPartitioner partitioner;
    private final KeyFetcher keyFetcher;
    private final RandomAccessReader reader;
    private final PrimaryKey.PrimaryKeyFactory primaryKeyFactory;
    private final long size;
    private final SSTableUniqueIdentifier generation;
    private final ByteBuffer tokenBuffer = ByteBuffer.allocate(Long.BYTES);

    private V1PrimaryKeyMap(LongArray rowIdToToken,
                            LongArray rowIdToOffset,
                            IPartitioner partitioner,
                            KeyFetcher keyFetcher,
                            PrimaryKey.PrimaryKeyFactory primaryKeyFactory,
                            long size,
                            SSTableUniqueIdentifier generation)
    {
        this.rowIdToToken = rowIdToToken;
        this.rowIdToOffset = rowIdToOffset;
        this.partitioner = partitioner;
        this.keyFetcher = keyFetcher;
        this.reader = keyFetcher.createReader();
        this.primaryKeyFactory = primaryKeyFactory;
        this.size = size;
        this.generation = generation;
    }

    @Override
    public SSTableUniqueIdentifier generation()
    {
        return generation;
    }

    @Override
    public PrimaryKey primaryKeyFromRowId(long sstableRowId) throws IOException
    {
        tokenBuffer.putLong(rowIdToToken.get(sstableRowId));
        tokenBuffer.rewind();
        return primaryKeyFactory.createKey(partitioner.getTokenFactory().fromByteArray(tokenBuffer))
                                .withSSTableRowId(sstableRowId)
                                .withPrimaryKeySupplier(() -> primaryKeyFactory.createKey(keyFetcher.apply(reader,
                                                                                                           rowIdToOffset.get(sstableRowId)))
                                                                               .withSSTableRowId(sstableRowId));
    }

    @Override
    public long rowIdFromPrimaryKey(PrimaryKey key) throws IOException
    {
        return rowIdToToken.findTokenRowID(key.token().getLongValue());
    }

    @Override
    public long size()
    {
        return size;
    }

    @Override
    public void close() throws IOException
    {
        FileUtils.closeQuietly(rowIdToToken, rowIdToOffset, reader);
    }
}
