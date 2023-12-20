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
package org.apache.cassandra.index.sai.disk.v1dse;

import java.io.IOException;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.index.sai.disk.PerSSTableIndexWriter;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1dse.bitpack.NumericValuesWriter;
import org.apache.cassandra.index.sai.disk.v1dse.io.IndexFileUtils;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.lucene.util.IOUtils;

/**
 * Writes all SSTable-attached index token and offset structures.
 */
public class SSTableComponentsWriter implements PerSSTableIndexWriter
{
    protected static final Logger logger = LoggerFactory.getLogger(SSTableComponentsWriter.class);

    private final IndexDescriptor indexDescriptor;
    private final NumericValuesWriter tokenWriter;
    private final NumericValuesWriter offsetWriter;
    private final MetadataWriter metadataWriter;

    private long currentKeyPartitionOffset;

    public SSTableComponentsWriter(IndexDescriptor indexDescriptor) throws IOException
    {
        this.indexDescriptor = indexDescriptor;

        this.metadataWriter = new MetadataWriter(IndexFileUtils.instance.openPerSSTableOutput(IndexComponent.GROUP_META, indexDescriptor));

        this.tokenWriter = new NumericValuesWriter(indexDescriptor.componentName(IndexComponent.TOKEN_VALUES),
                                                   IndexFileUtils.instance.openPerSSTableOutput(IndexComponent.TOKEN_VALUES, indexDescriptor),
                                                   metadataWriter, false);
        this.offsetWriter = new NumericValuesWriter(indexDescriptor.componentName(IndexComponent.OFFSETS_VALUES),
                                                    IndexFileUtils.instance.openPerSSTableOutput(IndexComponent.OFFSETS_VALUES, indexDescriptor),
                                                    metadataWriter, true);
    }

    @Override
    public void startPartition(DecoratedKey key, long keyPosition)
    {
        currentKeyPartitionOffset = keyPosition;
    }

    @Override
    public void nextRow(PrimaryKey primaryKey) throws IOException
    {
        recordCurrentTokenOffset(primaryKey.token().getLongValue(), currentKeyPartitionOffset);
    }

    @Override
    public void complete() throws IOException
    {
        IOUtils.close(tokenWriter, offsetWriter, metadataWriter);
        indexDescriptor.createComponentOnDisk(IndexComponent.GROUP_COMPLETION_MARKER);
    }

    @Override
    public void abort()
    {
        logger.debug(indexDescriptor.logMessage("Aborting token/offset writer for {}..."), indexDescriptor.sstableDescriptor);
        indexDescriptor.deletePerSSTableIndexComponents();
    }

    @VisibleForTesting
    public void recordCurrentTokenOffset(long tokenValue, long keyOffset) throws IOException
    {
        tokenWriter.add(tokenValue);
        offsetWriter.add(keyOffset);
    }
}
