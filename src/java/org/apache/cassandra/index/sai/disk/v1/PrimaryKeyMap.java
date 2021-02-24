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

import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.index.sai.disk.io.IndexComponents;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.Throwables;

public interface PrimaryKeyMap extends Closeable
{
    PrimaryKey primaryKeyFromRowId(long sstableRowId);

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
        private final FileHandle primaryKeys;
        private final BKDReader reader;
        private final BKDReader.IteratorState iterator;
        private final PrimaryKey.PrimaryKeyFactory keyFactory;
        private final long size;

        public DefaultPrimaryKeyMap(IndexComponents indexComponents, TableMetadata tableMetadata) throws IOException
        {
            MetadataSource metadataSource = MetadataSource.loadGroupMetadata(indexComponents);

            PartitionKeysMeta partitionKeysMeta = new PartitionKeysMeta(metadataSource.get(indexComponents.PRIMARY_KEYS.name));

            this.size = partitionKeysMeta.pointCount;

            if (partitionKeysMeta.pointCount > 0)
            {
                this.primaryKeys = indexComponents.createFileHandle(IndexComponents.PRIMARY_KEYS);
                this.reader = new BKDReader(indexComponents, primaryKeys, partitionKeysMeta.bkdPosition);
                this.iterator = reader.iteratorState();
            }
            else
            {
                this.primaryKeys = null;
                this.reader = null;
                this.iterator = null;
            }

            this.keyFactory = PrimaryKey.factory(tableMetadata);
        }

        private DefaultPrimaryKeyMap(DefaultPrimaryKeyMap orig) throws IOException
        {
            this.reader = orig.reader;
            this.primaryKeys = orig.primaryKeys;
            this.iterator = reader.iteratorState();
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
        public PrimaryKey primaryKeyFromRowId(long sstableRowId)
        {
            if (iterator != null)
            {
                iterator.seekTo(sstableRowId);
                PrimaryKey key = keyFactory.createKey(iterator.asByteComparable(), sstableRowId);
                return key;
            }
            return null;
        }

        @Override
        public void close() throws IOException
        {
            FileUtils.closeQuietly(reader, iterator);
            Throwables.maybeFail(Throwables.close(null, primaryKeys));
        }
    }
}
