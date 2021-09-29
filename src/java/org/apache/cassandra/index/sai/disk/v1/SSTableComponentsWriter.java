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

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.sai.disk.PerSSTableWriter;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.lucene.util.IOUtils;

public class SSTableComponentsWriter implements PerSSTableWriter
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

        this.metadataWriter = new MetadataWriter(indexDescriptor.openPerSSTableOutput(IndexComponent.GROUP_META));

        this.tokenWriter = new NumericValuesWriter(indexDescriptor.version.fileNameFormatter().format(IndexComponent.TOKEN_VALUES, null),
                                                   indexDescriptor.openPerSSTableOutput(IndexComponent.TOKEN_VALUES),
                                                   metadataWriter, false);
        this.offsetWriter = new NumericValuesWriter(indexDescriptor.version.fileNameFormatter().format(IndexComponent.OFFSETS_VALUES, null),
                                                    indexDescriptor.openPerSSTableOutput(IndexComponent.OFFSETS_VALUES),
                                                    metadataWriter, true);
    }

    @Override
    public void startPartition(PrimaryKey primaryKey, long position)
    {
        currentKeyPartitionOffset = position;
    }

    @Override
    public void nextRow(PrimaryKey key) throws IOException
    {
        recordCurrentTokenOffset((long)key.partitionKey().getToken().getTokenValue(), currentKeyPartitionOffset);
    }

    @VisibleForTesting
    public void recordCurrentTokenOffset(long tokenValue, long keyOffset) throws IOException
    {
        tokenWriter.add(tokenValue);
        offsetWriter.add(keyOffset);
    }

    public void complete() throws IOException
    {
        IOUtils.close(tokenWriter, offsetWriter, metadataWriter);
        indexDescriptor.createComponentOnDisk(IndexComponent.GROUP_COMPLETION_MARKER);
    }

    public void abort(Throwable accumulator)
    {
        logger.debug(indexDescriptor.logMessage("Aborting token/offset writer for {}..."), indexDescriptor.descriptor);
        indexDescriptor.deletePerSSTableIndexComponents();
    }
}
