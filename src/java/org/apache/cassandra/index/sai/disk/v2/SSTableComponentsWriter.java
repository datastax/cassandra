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

import com.google.common.base.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.index.sai.disk.PerSSTableWriter;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.index.sai.disk.format.IndexComponents;
import org.apache.cassandra.index.sai.disk.v1.MetadataWriter;
import org.apache.cassandra.index.sai.disk.v1.bitpack.NumericValuesWriter;
import org.apache.cassandra.index.sai.disk.v2.keystore.KeyStoreWriter;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.lucene.util.IOUtils;

public class SSTableComponentsWriter implements PerSSTableWriter
{
    protected static final Logger logger = LoggerFactory.getLogger(SSTableComponentsWriter.class);

    private final IndexComponents.ForWrite perSSTableComponents;
    private final MetadataWriter metadataWriter;
    private final NumericValuesWriter tokenWriter;
    private final NumericValuesWriter partitionSizeWriter;
    private final KeyStoreWriter partitionKeysWriter;
    private final KeyStoreWriter clusteringKeysWriter;

    private long partitionId = -1;

    public SSTableComponentsWriter(IndexComponents.ForWrite perSSTableComponents) throws IOException
    {
        this.perSSTableComponents = perSSTableComponents;
        this.metadataWriter = new MetadataWriter(perSSTableComponents);
        this.tokenWriter = new NumericValuesWriter(perSSTableComponents.addOrGet(IndexComponentType.TOKEN_VALUES),
                                                   metadataWriter, false);

        this.partitionSizeWriter = new NumericValuesWriter(perSSTableComponents.addOrGet(IndexComponentType.PARTITION_SIZES), metadataWriter, true);
        NumericValuesWriter partitionKeyBlockOffsetWriter = new NumericValuesWriter(perSSTableComponents.addOrGet(IndexComponentType.PARTITION_KEY_BLOCK_OFFSETS), metadataWriter, true);
        this.partitionKeysWriter = new KeyStoreWriter(perSSTableComponents.addOrGet(IndexComponentType.PARTITION_KEY_BLOCKS),
                                                      metadataWriter,
                                                      partitionKeyBlockOffsetWriter,
                                                      CassandraRelevantProperties.SAI_SORTED_TERMS_PARTITION_BLOCK_SHIFT.getInt(),
                                                      false);
        if (perSSTableComponents.hasClustering())
        {
            NumericValuesWriter clusteringKeyBlockOffsetWriter = new NumericValuesWriter(perSSTableComponents.addOrGet(IndexComponentType.CLUSTERING_KEY_BLOCK_OFFSETS), metadataWriter, true);
            this.clusteringKeysWriter = new KeyStoreWriter(perSSTableComponents.addOrGet(IndexComponentType.CLUSTERING_KEY_BLOCKS),
                                                           metadataWriter,
                                                           clusteringKeyBlockOffsetWriter,
                                                           CassandraRelevantProperties.SAI_SORTED_TERMS_CLUSTERING_BLOCK_SHIFT.getInt(),
                                                           true);
        }
        else
        {
            this.clusteringKeysWriter = null;
        }
    }

    @Override
    public void startPartition(DecoratedKey partitionKey) throws IOException
    {
        partitionId++;
        partitionKeysWriter.add(v -> ByteSource.of(partitionKey.getKey(), v));
        if (perSSTableComponents.hasClustering())
            clusteringKeysWriter.startPartition();
    }

    @Override
    public void nextRow(PrimaryKey primaryKey) throws IOException
    {
        assert partitionId >= 0;

        tokenWriter.add(primaryKey.token().getLongValue());
        partitionSizeWriter.add(partitionId);
        if (perSSTableComponents.hasClustering())
            clusteringKeysWriter.add(perSSTableComponents.comparator().asByteComparable(primaryKey.clustering()));
    }

    @Override
    public void complete(Stopwatch stopwatch) throws IOException
    {
        IOUtils.close(tokenWriter, partitionSizeWriter, partitionKeysWriter, clusteringKeysWriter, metadataWriter);
        perSSTableComponents.markComplete();
    }

    @Override
    public void abort(Throwable accumulator)
    {
        logger.debug(perSSTableComponents.logMessage("Aborting per-SSTable index component writer for {}..."), perSSTableComponents.descriptor());
        perSSTableComponents.forceDeleteAllComponents();
    }
}
