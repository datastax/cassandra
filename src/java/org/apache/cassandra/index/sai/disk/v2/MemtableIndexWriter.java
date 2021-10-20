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
import java.lang.invoke.MethodHandles;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.hppc.LongArrayList;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.disk.MemtableTermsIterator;
import org.apache.cassandra.index.sai.disk.PerIndexWriter;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v2.blockindex.BlockIndexFileProvider;
import org.apache.cassandra.index.sai.disk.v2.blockindex.BlockIndexMeta;
import org.apache.cassandra.index.sai.disk.v2.blockindex.BlockIndexWriter;
import org.apache.cassandra.index.sai.memory.MemtableIndex;
import org.apache.cassandra.index.sai.memory.RowMapping;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.lucene.store.IndexOutput;

public class MemtableIndexWriter implements PerIndexWriter
{
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    protected final IndexDescriptor indexDescriptor;
    protected final IndexContext indexContext;

    private final MemtableIndex memtable;
    private final RowMapping rowMapping;

    public MemtableIndexWriter(MemtableIndex memtable,
                               IndexDescriptor indexDescriptor,
                               IndexContext indexContext,
                               RowMapping rowMapping)
    {
        this.indexDescriptor = indexDescriptor;
        this.indexContext = indexContext;
        this.memtable = memtable;
        this.rowMapping = rowMapping;
    }

    @Override
    public IndexContext indexContext()
    {
        return indexContext;
    }

    @Override
    public void addRow(PrimaryKey rowKey, Row row) throws IOException
    {
        // Memtable indexes are flushed directly to disk with the aid of a mapping between primary
        // keys and row IDs in the flushing SSTable. This writer, therefore, does nothing in
        // response to the flushing of individual rows.
    }

    @Override
    public void abort(Throwable cause)
    {
        logger.warn(indexContext.logMessage("Aborting index memtable flush for {}..."), indexDescriptor.descriptor, cause);
        indexDescriptor.deleteColumnIndex(indexContext);
    }

    @Override
    public void flush() throws IOException
    {
        long start = System.nanoTime();

        try
        {
            if (!rowMapping.hasRows() || (memtable == null) || memtable.isEmpty())
            {
                logger.debug(indexContext.logMessage("No indexed rows to flush from SSTable {}."), indexDescriptor.descriptor);
                // Write a completion marker even though we haven't written anything to the index
                // so we won't try to build the index again for the SSTable
                indexDescriptor.createComponentOnDisk(IndexComponent.COLUMN_COMPLETION_MARKER, indexContext);
                return;
            }

            final Iterator<Pair<ByteComparable, LongArrayList>> iterator = rowMapping.merge(memtable);

            try (MemtableTermsIterator terms = new MemtableTermsIterator(memtable.getMinTerm(), memtable.getMaxTerm(), iterator))
            {
                long cellCount = flush(terms);

                indexDescriptor.createComponentOnDisk(IndexComponent.COLUMN_COMPLETION_MARKER, indexContext);

                indexContext.getIndexMetrics().memtableIndexFlushCount.inc();

                long durationMillis = Math.max(1, TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));

                if (logger.isTraceEnabled())
                {
                    logger.trace(indexContext.logMessage("Flushed {} Memtable index cells for {} in {} ms."), cellCount, indexDescriptor.descriptor, durationMillis);
                }

                logger.debug(indexContext.logMessage("Flushed {} Memtable index cells for {} in {} ms."), cellCount, indexDescriptor.descriptor, durationMillis);

                indexContext.getIndexMetrics().memtableFlushCellsPerSecond.update((long) (cellCount * 1000.0 / durationMillis));
            }
        }
        catch (Throwable t)
        {
            logger.error(indexContext.logMessage("Error while flushing index {}"), t.getMessage(), t);
            indexContext.getIndexMetrics().memtableIndexFlushErrors.inc();

            throw t;
        }
    }

    private long flush(MemtableTermsIterator terms) throws IOException
    {
        long numRows;

        try (BlockIndexFileProvider fileProvider = new PerIndexFileProvider(indexDescriptor, indexContext);
             BlockIndexWriter writer = new BlockIndexWriter(fileProvider, false))
        {
            numRows = writer.addAll(terms, 0L);
            // If no rows were written we need to delete any created column index components
            // so that the index is correctly identified as being empty (only having a completion marker)
            if (numRows == 0)
            {
                indexDescriptor.deleteColumnIndex(indexContext);
                return 0;
            }
            BlockIndexMeta metadata = writer.finish();

            try (final IndexOutput out = fileProvider.openMetadataOutput())
            {
                metadata.write(out);
            }
        }
        logger.debug(indexContext.logMessage("Wrote {} indexed rows to disk for SSTable {}."), numRows, indexDescriptor.descriptor);
        return numRows;
    }
}
