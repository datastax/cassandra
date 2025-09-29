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
import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.agrona.collections.Int2IntHashMap;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.disk.MemtableTermsIterator;
import org.apache.cassandra.index.sai.disk.PerIndexWriter;
import org.apache.cassandra.index.sai.disk.format.IndexComponents;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.disk.v1.kdtree.ImmutableOneDimPointValues;
import org.apache.cassandra.index.sai.disk.v1.kdtree.NumericIndexWriter;
import org.apache.cassandra.index.sai.disk.v1.trie.InvertedIndexWriter;
import org.apache.cassandra.index.sai.disk.vector.VectorMemtableIndex;
import org.apache.cassandra.index.sai.memory.MemtableIndex;
import org.apache.cassandra.index.sai.memory.RowMapping;
import org.apache.cassandra.index.sai.memory.TrieMemoryIndex;
import org.apache.cassandra.index.sai.memory.TrieMemtableIndex;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Column index writer that flushes indexed data directly from the corresponding Memtable index, without buffering index
 * data in memory.
 */
public class MemtableIndexWriter implements PerIndexWriter
{
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final IndexComponents.ForWrite perIndexComponents;
    private final MemtableIndex memtableIndex;
    private final PrimaryKey.Factory pkFactory;
    private final RowMapping rowMapping;

    public MemtableIndexWriter(MemtableIndex memtableIndex,
                               IndexComponents.ForWrite perIndexComponents,
                               PrimaryKey.Factory pkFactory,
                               RowMapping rowMapping)
    {
        assert rowMapping != null && rowMapping != RowMapping.DUMMY : "Row mapping must exist during FLUSH.";

        this.perIndexComponents = perIndexComponents;
        this.memtableIndex = memtableIndex;
        this.pkFactory = pkFactory;
        this.rowMapping = rowMapping;
    }

    @Override
    public IndexContext indexContext()
    {
        return perIndexComponents.context();
    }

    @Override
    public IndexComponents.ForWrite writtenComponents()
    {
        return perIndexComponents;
    }

    @Override
    public void addRow(PrimaryKey key, Row row, long sstableRowId)
    {
        // Memtable indexes are flushed directly to disk with the aid of a mapping between primary
        // keys and row IDs in the flushing SSTable. This writer, therefore, does nothing in
        // response to the flushing of individual rows.
    }

    @Override
    public void abort(Throwable cause)
    {
        logger.warn(perIndexComponents.logMessage("Aborting index memtable flush for {}..."), perIndexComponents.descriptor(), cause);
        perIndexComponents.forceDeleteAllComponents();
    }

    @Override
    public void complete(Stopwatch stopwatch) throws IOException
    {
        long start = stopwatch.elapsed(TimeUnit.MILLISECONDS);

        try
        {
            if (!rowMapping.hasRows() || (memtableIndex == null) || memtableIndex.isEmpty())
            {
                logger.debug(perIndexComponents.logMessage("No indexed rows to flush from SSTable {}."), perIndexComponents.descriptor());
                // Write a completion marker even though we haven't written anything to the index
                // so we won't try to build the index again for the SSTable
                perIndexComponents.markComplete();
                return;
            }

            final DecoratedKey minKey = rowMapping.minKey.partitionKey();
            final DecoratedKey maxKey = rowMapping.maxKey.partitionKey();

            if (indexContext().isVector())
            {
                flushVectorIndex(minKey, maxKey, start, stopwatch);
            }
            else
            {
                var iterator = rowMapping.merge(memtableIndex);
                try (MemtableTermsIterator terms = new MemtableTermsIterator(memtableIndex.getMinTerm(), memtableIndex.getMaxTerm(), iterator))
                {
                    long cellCount = flush(minKey, maxKey, indexContext().getValidator(), terms, rowMapping.maxSegmentRowId);

                    completeIndexFlush(cellCount, start, stopwatch);
                }
            }
        }
        catch (Throwable t)
        {
            logger.error(perIndexComponents.logMessage("Error while flushing index {}"), t.getMessage(), t);
            if (indexContext().getIndexMetrics() != null)
                indexContext().getIndexMetrics().memtableIndexFlushErrors.inc();

            throw t;
        }
    }

    @Override
    public void onSSTableWriterSwitched(Stopwatch stopwatch) throws IOException
    {
        // no-op for memtable index where all terms are already inside memory index, we can't get rid of memory index
        // until full flush are completed
    }

    private long flush(DecoratedKey minKey, DecoratedKey maxKey, AbstractType<?> termComparator, MemtableTermsIterator terms, int maxSegmentRowId) throws IOException
    {
        long numPostings;
        long numRows;
        long totalTermCount;
        SegmentMetadataBuilder metadataBuilder = new SegmentMetadataBuilder(0, perIndexComponents);
        SegmentMetadata.ComponentMetadataMap indexMetas;
        if (TypeUtil.isLiteral(termComparator))
        {
            try (InvertedIndexWriter writer = new InvertedIndexWriter(perIndexComponents, writeFrequencies()))
            {
                // Convert PrimaryKey->length map to rowId->length using RowMapping
                var docLengths = new Int2IntHashMap(Integer.MIN_VALUE);
                Arrays.stream(((TrieMemtableIndex) memtableIndex).getRangeIndexes())
                      .map(TrieMemoryIndex.class::cast)
                      .forEach(trieMemoryIndex ->
                          trieMemoryIndex.getDocLengths().forEach((pk, length) -> {
                              int rowId = rowMapping.get(pk);
                              if (rowId >= 0)
                                  docLengths.put(rowId, (int) length);
                          })
                      );

                indexMetas = writer.writeAll(metadataBuilder.intercept(terms), docLengths);
                numPostings = writer.getPostingsCount();
                totalTermCount = docLengths.values().stream().mapToInt(i -> i).sum();
                numRows = docLengths.size();
            }
        }
        else
        {
            try (NumericIndexWriter writer = new NumericIndexWriter(perIndexComponents,
                                                                    TypeUtil.fixedSizeOf(termComparator),
                                                                    maxSegmentRowId,
                                                                    // The number of postings is unknown. Also, there are stale entries in IndexMemtable.
                                                                    Integer.MAX_VALUE,
                                                                    indexContext().getIndexWriterConfig()))
            {
                ImmutableOneDimPointValues values = ImmutableOneDimPointValues.fromTermEnum(terms, termComparator);
                indexMetas = writer.writeAll(metadataBuilder.intercept(values));
                numPostings = writer.getPointCount();
                numRows = numPostings;
                totalTermCount = numPostings;
            }
        }

        // If no rows were written we need to delete any created column index components
        // so that the index is correctly identified as being empty (only having a completion marker)
        if (numPostings == 0)
        {
            perIndexComponents.forceDeleteAllComponents();
            return 0;
        }

        metadataBuilder.setNumRows(numRows);
        metadataBuilder.setTotalTermCount(totalTermCount);
        metadataBuilder.setKeyRange(pkFactory.createPartitionKeyOnly(minKey), pkFactory.createPartitionKeyOnly(maxKey));
        metadataBuilder.setRowIdRange(terms.getMinSSTableRowId(), terms.getMaxSSTableRowId());
        metadataBuilder.setTermRange(terms.getMinTerm(), terms.getMaxTerm());
        metadataBuilder.setComponentsMetadata(indexMetas);
        SegmentMetadata metadata = metadataBuilder.build();

        try (MetadataWriter writer = new MetadataWriter(perIndexComponents))
        {
            SegmentMetadata.write(writer, Collections.singletonList(metadata));
        }

        return numPostings;
    }

    private boolean writeFrequencies()
    {
        return indexContext().isAnalyzed() && Version.current().onOrAfter(Version.BM25_EARLIEST);
    }

    private void flushVectorIndex(DecoratedKey minKey, DecoratedKey maxKey, long startTime, Stopwatch stopwatch) throws IOException
    {
        var vectorIndex = (VectorMemtableIndex) memtableIndex;

        if (!vectorIndex.preFlush(rowMapping::get))
        {
            logger.debug(perIndexComponents.logMessage("Whole graph is deleted. Skipping index flush for {}."), perIndexComponents.descriptor());
            perIndexComponents.markComplete();
            return;
        }

        SegmentMetadata.ComponentMetadataMap metadataMap = vectorIndex.writeData(perIndexComponents);

        SegmentMetadata metadata = new SegmentMetadata(0,
                                                       rowMapping.size(), // TODO this isn't the right size metric.
                                                       0,
                                                       rowMapping.maxSegmentRowId,
                                                       pkFactory.createPartitionKeyOnly(minKey),
                                                       pkFactory.createPartitionKeyOnly(maxKey),
                                                       ByteBufferUtil.bytes(0), // VSTODO by pass min max terms for vectors
                                                       ByteBufferUtil.bytes(0), // VSTODO by pass min max terms for vectors
                                                       null,
                                                       metadataMap,
                                                       rowMapping.size());

        try (MetadataWriter writer = new MetadataWriter(perIndexComponents))
        {
            SegmentMetadata.write(writer, Collections.singletonList(metadata));
        }

        completeIndexFlush(rowMapping.size(), startTime, stopwatch);
    }

    private void completeIndexFlush(long cellCount, long startTime, Stopwatch stopwatch) throws IOException
    {
        perIndexComponents.markComplete();

        if (indexContext().getIndexMetrics() != null)
            indexContext().getIndexMetrics().memtableIndexFlushCount.inc();

        long elapsedTime = stopwatch.elapsed(TimeUnit.MILLISECONDS);

        logger.debug(perIndexComponents.logMessage("Completed flushing {} memtable index cells to SSTable {}. Duration: {} ms. Total elapsed: {} ms"),
                     cellCount,
                     perIndexComponents.descriptor(),
                     elapsedTime - startTime,
                     elapsedTime);

        if (indexContext().getIndexMetrics() != null)
            indexContext().getIndexMetrics().memtableFlushCellsPerSecond.update((long) (cellCount * 1000.0 / Math.max(1, elapsedTime - startTime)));
    }
}
