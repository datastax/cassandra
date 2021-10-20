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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.base.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.analyzer.AbstractAnalyzer;
import org.apache.cassandra.index.sai.disk.PerIndexWriter;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v2.blockindex.BlockIndexFileProvider;
import org.apache.cassandra.index.sai.disk.v2.blockindex.BlockIndexMeta;
import org.apache.cassandra.index.sai.disk.v2.blockindex.BlockIndexReader;
import org.apache.cassandra.index.sai.disk.v2.blockindex.BlockIndexWriter;
import org.apache.cassandra.index.sai.disk.v2.blockindex.BytesUtil;
import org.apache.cassandra.index.sai.disk.v2.blockindex.MergeIndexIterators;
import org.apache.cassandra.index.sai.utils.NamedMemoryLimiter;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.lucene.store.IndexOutput;

/**
 * Column index writer that accumulates (on-heap) indexed data from a compacted SSTable as it's being flushed to disk.
 */
@NotThreadSafe
public class V2SSTableIndexWriter implements PerIndexWriter
{
    private static final Logger logger = LoggerFactory.getLogger(V2SSTableIndexWriter.class);
    private static final NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(logger, 1, TimeUnit.MINUTES);

    public static final int MAX_STRING_TERM_SIZE = Integer.getInteger("cassandra.sai.max_string_term_size_kb", 1) * 1024;
    public static final int MAX_FROZEN_TERM_SIZE = Integer.getInteger("cassandra.sai.max_frozen_term_size_kb", 5) * 1024;
    public static final String TERM_OVERSIZE_MESSAGE = "Can't add term of column {} to index for key: {}, term size {} " +
                                                       "max allowed size {}, use analyzed = true (if not yet set) for that column.";

    protected final IndexDescriptor indexDescriptor;
    protected final IndexContext indexContext;
    protected final List<BlockIndexMeta> metadatas = new ArrayList<>();
    protected final List<Long> offsets = new ArrayList<>();

    private final int nowInSec = FBUtilities.nowInSeconds();
    private final AbstractAnalyzer analyzer;
    private final NamedMemoryLimiter limiter;
    private final int maxTermSize;
    private final BooleanSupplier isIndexValid;

    private boolean aborted = false;

    // segment writer
    private V2SegmentBuilder currentBuilder;

    public V2SSTableIndexWriter(IndexDescriptor indexDescriptor,
                                IndexContext indexContext,
                                NamedMemoryLimiter limiter,
                                BooleanSupplier isIndexValid)
    {
        this.indexContext = indexContext;
        this.indexDescriptor = indexDescriptor;
        this.analyzer = indexContext.getAnalyzerFactory().create();
        this.limiter = limiter;
        this.isIndexValid = isIndexValid;
        this.maxTermSize = indexContext.isFrozen() ? MAX_FROZEN_TERM_SIZE : MAX_STRING_TERM_SIZE;
    }

    @Override
    public IndexContext indexContext()
    {
        return indexContext;
    }

    @Override
    public void addRow(PrimaryKey key, Row row) throws IOException
    {
        if (maybeAbort())
            return;

        if (indexContext.isNonFrozenCollection())
        {
            Iterator<ByteBuffer> valueIterator = indexContext.getValuesOf(row, nowInSec);
            if (valueIterator != null)
            {
                while (valueIterator.hasNext())
                {
                    ByteBuffer value = valueIterator.next();
                    addTerm(TypeUtil.instance.encode(value.duplicate(), indexContext.getValidator(), false), key, indexContext.getValidator());
                }
            }
        }
        else
        {
            ByteBuffer value = indexContext.getValueOf(key.partitionKey(), row, nowInSec);
            if (value != null)
                addTerm(TypeUtil.instance.encode(value.duplicate(), indexContext.getValidator(), false), key, indexContext.getValidator());
        }
    }

    @Override
    public void flush() throws IOException
    {
        if (maybeAbort())
        {
            logger.warn(indexContext.logMessage("Indexing operation was aborted"));
            return;
        }

        Stopwatch stopwatch = Stopwatch.createStarted();

        boolean emptySegment = currentBuilder == null || currentBuilder.isEmpty();
        logger.debug(indexContext.logMessage("Completing index flush with {}buffered data..."), emptySegment ? "no " : "");

        try
        {
            // parts are present but there is something still in memory, let's flush that inline
            if (!emptySegment)
            {
                flushSegment(!metadatas.isEmpty());

                logger.debug(indexContext.logMessage("Final flush on sstable {} resulting in {} segments. Elapsed time {}ms"),
                             indexDescriptor.descriptor,
                             metadatas.size(),
                             stopwatch.elapsed(TimeUnit.MILLISECONDS));
            }

            // Even an empty segment may carry some fixed memory, so remove it:
            if (currentBuilder != null)
            {
                currentBuilder.release("Flushing final segment");
                currentBuilder = null;
            }

            compactSegments();

            logger.debug(indexContext.logMessage("Segment compaction finished on sstable {}. Elapsed time {}ms"),
                         indexDescriptor.descriptor,
                         stopwatch.elapsed(TimeUnit.MILLISECONDS));

            indexDescriptor.createComponentOnDisk(IndexComponent.COLUMN_COMPLETION_MARKER, indexContext);
        }
        finally
        {
            if (indexContext.getIndexMetrics() != null)
            {
                indexContext.getIndexMetrics().segmentsPerCompaction.update(metadatas.size());
                metadatas.clear();
                indexContext.getIndexMetrics().compactionCount.inc();
            }
        }
    }

    @Override
    public void abort(Throwable cause)
    {
        aborted = true;

        logger.warn(indexContext.logMessage("Aborting SSTable index flush for {}..."), indexDescriptor.descriptor, cause);

        // It's possible for the current builder to be unassigned after we flush a final segment.
        if (currentBuilder != null)
        {
            // If an exception is thrown out of any writer operation prior to successful segment
            // flush, we will end up here, and we need to free up builder memory tracked by the limiter:
            currentBuilder.release("Aborting index writer");
        }

        indexDescriptor.deleteColumnIndex(indexContext);
    }

    /**
     * abort current write if index is dropped
     *
     * @return true if current write is aborted.
     */
    private boolean maybeAbort()
    {
        if (aborted)
            return true;

        if (isIndexValid.getAsBoolean())
            return false;

        abort(new RuntimeException(String.format("index %s is dropped", indexContext.getIndexName())));
        return true;
    }

    private void addTerm(ByteBuffer term, PrimaryKey key, AbstractType<?> type) throws IOException
    {
        if (term.remaining() >= maxTermSize)
        {
            noSpamLogger.warn(indexContext.logMessage(TERM_OVERSIZE_MESSAGE),
                              indexContext.getColumnName(),
                              indexContext.keyValidator().getString(key.partitionKey().getKey()),
                              FBUtilities.prettyPrintMemory(term.remaining()),
                              FBUtilities.prettyPrintMemory(maxTermSize));
            return;
        }

        if (currentBuilder == null)
        {
            currentBuilder = newSegmentBuilder();
        }
        else if (currentBuilder.shouldFlush(key.sstableRowId()))
        {
            flushSegment(true);
            currentBuilder = newSegmentBuilder();
        }

        if (term.remaining() == 0) return;

        if (!TypeUtil.instance.isLiteral(type))
        {
            // TODO: fix int cast by encoding the row id delta
            limiter.increment(currentBuilder.add(term, key.sstableRowId()));
        }
        else
        {
            analyzer.reset(term);
            try
            {
                while (analyzer.hasNext())
                {
                    ByteBuffer tokenTerm = analyzer.next();
                    // TODO: fix cast
                    limiter.increment(currentBuilder.add(tokenTerm, key.sstableRowId()));
                }
            }
            finally
            {
                analyzer.end();
            }
        }
    }

    private long flushSegment(boolean temporary) throws IOException
    {
        long start = System.nanoTime();

        try
        {
            BlockIndexMeta segmentMetadata = currentBuilder.flush(indexDescriptor, indexContext, temporary);
            long segmentRowIdOffset = currentBuilder.segmentRowIdOffset;

            long flushMillis = Math.max(1, TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));

            long numRows = -1;
            if (segmentMetadata != null)
            {
                if (temporary)
                {
                    metadatas.add(segmentMetadata);
                    offsets.add(segmentRowIdOffset);
                }

                //TODO Need to look at some of these metrics
                numRows = segmentMetadata.numRows;
                double rowCount = segmentMetadata.numRows;
                if (indexContext.getIndexMetrics() != null)
                    indexContext.getIndexMetrics().compactionSegmentCellsPerSecond.update((long)(rowCount / flushMillis * 1000.0));

//                double segmentBytes = segmentMetadata.componentMetadatas.indexSize();
//                if (indexContext.getIndexMetrics() != null)
//                    indexContext.getIndexMetrics().compactionSegmentBytesPerSecond.update((long)(segmentBytes / flushMillis * 1000.0));

//                logger.debug(indexContext.logMessage("Flushed segment with {} cells for a total of {} in {} ms."),
//                             (long) rowCount, FBUtilities.prettyPrintMemory((long) segmentBytes), flushMillis);
            }

            // Builder memory is released against the limiter at the conclusion of a successful
            // flush. Note that any failure that occurs before this (even in term addition) will
            // actuate this column writer's abort logic from the parent SSTable-level writer, and
            // that abort logic will release the current builder's memory against the limiter.
            currentBuilder.release("Flushing index segment");
            currentBuilder = null;
            return numRows;
        }
        catch (Throwable t)
        {
            logger.error(indexContext.logMessage("Failed to build index for SSTable {}."), indexDescriptor.descriptor, t);
            indexDescriptor.deleteColumnIndex(indexContext);

            indexContext.getIndexMetrics().segmentFlushErrors.inc();

            throw t;
        }
    }


    private void compactSegments() throws IOException
    {
        if (metadatas.isEmpty())
            return;

        Stopwatch stopwatch = Stopwatch.createStarted();

        List<BlockIndexReader> readers = new ArrayList<>();
        List<BlockIndexReader.IndexIterator> iterators = new ArrayList<>();
        try (BlockIndexFileProvider fileProvider = new PerIndexFileProvider(indexDescriptor, indexContext))
        {
            Iterator<BlockIndexMeta> metadataIterator = metadatas.iterator();
            Iterator<Long> offsetIterator = offsets.iterator();
            while (metadataIterator.hasNext())
            {
                BlockIndexMeta metadata = metadataIterator.next();
                long offset = offsetIterator.next();
                BlockIndexReader reader = new BlockIndexReader(fileProvider, true, metadata);
                readers.add(reader);
                iterators.add(reader.iterator(offset));
            }

            logger.debug(indexContext.logMessage("Created iterators for {} segments for sstable {}. Elapsed time {} ms"),
                         metadatas.size(),
                         indexDescriptor.descriptor,
                         stopwatch.elapsed(TimeUnit.MILLISECONDS));

            try (MergeIndexIterators mergeIndexIterators = new MergeIndexIterators(iterators))
            {

                BlockIndexWriter writer = new BlockIndexWriter(fileProvider, false);

                // TODO: write row id -> point id map
                // TODO: write point id -> row id map?
                long terms = 0;
                while (true)
                {
                    BlockIndexReader.IndexState state = mergeIndexIterators.next();
                    if (state == null)
                    {
                        break;
                    }
                    //TODO Probably ought to be using type here
                    writer.add(BytesUtil.fixedLength(state.term), state.rowid);
                    terms++;
                }
                logger.debug(indexContext.logMessage("Completed adding {} terms to writer for sstable {}. Elapsed time {} ms"),
                             terms,
                             indexDescriptor.descriptor,
                             stopwatch.elapsed(TimeUnit.MILLISECONDS));

                BlockIndexMeta metadata = writer.finish();

                logger.debug(indexContext.logMessage("Completed writing index files for sstable {}. Elapsed time {} ms"),
                             indexDescriptor.descriptor,
                             stopwatch.elapsed(TimeUnit.MILLISECONDS));

                try (final IndexOutput out = fileProvider.openMetadataOutput())
                {
                    metadata.write(out);
                }
            }
            finally
            {
                FileUtils.closeQuietly(readers);
            }
        }
        finally
        {
            indexDescriptor.deletePerIndexTemporaryComponents(indexContext);
        }
    }

    private V2SegmentBuilder newSegmentBuilder()
    {
          return new V2SegmentBuilder(indexDescriptor, indexContext, limiter);
    }
}
