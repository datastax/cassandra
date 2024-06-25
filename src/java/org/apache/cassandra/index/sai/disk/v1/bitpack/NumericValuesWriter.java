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
package org.apache.cassandra.index.sai.disk.v1.bitpack;

import java.io.Closeable;
import java.io.IOException;

import org.apache.cassandra.index.sai.disk.io.IndexOutput;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.v1.MetadataWriter;
import org.apache.cassandra.index.sai.utils.SAICodecUtils;

import static org.apache.cassandra.config.CassandraRelevantProperties.SAI_NUMERIC_VALUES_BLOCK_SIZE;
import static org.apache.cassandra.config.CassandraRelevantProperties.SAI_NUMERIC_VALUES_MONOTONIC_BLOCK_SIZE;


public class NumericValuesWriter implements Closeable
{
    public static final int MONOTONIC_BLOCK_SIZE = SAI_NUMERIC_VALUES_MONOTONIC_BLOCK_SIZE.getInt();
    public static final int BLOCK_SIZE = SAI_NUMERIC_VALUES_BLOCK_SIZE.getInt();

    private final IndexComponent.ForWrite components;
    private final IndexOutput output;
    private final AbstractBlockPackedWriter writer;
    private final MetadataWriter metadataWriter;
    private final int blockSize;
    private long count = 0;

    public NumericValuesWriter(IndexComponent.ForWrite components,
                               MetadataWriter metadataWriter,
                               boolean monotonic) throws IOException
    {
        this(components, metadataWriter, monotonic, monotonic ? MONOTONIC_BLOCK_SIZE : BLOCK_SIZE);
    }

    public NumericValuesWriter(IndexComponent.ForWrite components,
                               MetadataWriter metadataWriter,
                               boolean monotonic,
                               int blockSize) throws IOException
    {
        this.components = components;
        this.output = components.openOutput();
        SAICodecUtils.writeHeader(output);

        this.writer = monotonic ? new MonotonicBlockPackedWriter(output, blockSize)
                                : new BlockPackedWriter(output, blockSize);
        this.metadataWriter = metadataWriter;
        this.blockSize = blockSize;

    }

    @Override
    public void close() throws IOException
    {
        try (IndexOutput o = metadataWriter.builder(components.fileNamePart()))
        {
            final long fp = writer.finish();
            SAICodecUtils.writeFooter(output);

            NumericValuesMeta meta = new NumericValuesMeta(count, blockSize, fp);
            meta.write(o);
        }
        finally
        {
            output.close();
        }
    }

    public void add(long value) throws IOException
    {
        writer.add(value);
        count++;
    }
}
