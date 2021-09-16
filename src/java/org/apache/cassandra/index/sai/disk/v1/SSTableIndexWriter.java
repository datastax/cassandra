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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import javax.annotation.concurrent.NotThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.analyzer.AbstractAnalyzer;
import org.apache.cassandra.index.sai.disk.PerIndexWriter;
import org.apache.cassandra.index.sai.disk.PerIndexFiles;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.utils.NamedMemoryLimiter;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.NoSpamLogger;

/**
 * Column index writer that accumulates (on-heap) indexed data from a compacted SSTable as it's being flushed to disk.
 */
@NotThreadSafe
public class SSTableIndexWriter implements PerIndexWriter
{
    private static final Logger logger = LoggerFactory.getLogger(SSTableIndexWriter.class);
    private static final NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(logger, 1, TimeUnit.MINUTES);

    public static final int MAX_STRING_TERM_SIZE = Integer.getInteger("cassandra.sai.max_string_term_size_kb", 1) * 1024;
    public static final int MAX_FROZEN_TERM_SIZE = Integer.getInteger("cassandra.sai.max_frozen_term_size_kb", 5) * 1024;
    public static final String TERM_OVERSIZE_MESSAGE =
            "Can't add term of column {} to index for key: {}, term size {} " +
                    "max allowed size {}, use analyzed = true (if not yet set) for that column.";

    protected final IndexDescriptor indexDescriptor;
    protected final IndexContext indexContext;
    protected final List<SegmentMetadata> segments = new ArrayList<>();

    private final int nowInSec = FBUtilities.nowInSeconds();
    private final AbstractAnalyzer analyzer;
    private final NamedMemoryLimiter limiter;
    private final int maxTermSize;
    private final BooleanSupplier isIndexValid;

    private boolean aborted = false;

    // segment writer
    private SegmentBuilder currentBuilder;
    private long maxSSTableRowId;

    public SSTableIndexWriter(IndexDescriptor indexDescriptor, IndexContext indexContext, NamedMemoryLimiter limiter,
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
                    addTerm(TypeUtil.encode(value.duplicate(), indexContext.getValidator()), key, indexContext.getValidator());
                }
            }
        }
        else
        {
            ByteBuffer value = indexContext.getValueOf(key.partitionKey(), row, nowInSec);
            if (value != null)
                addTerm(TypeUtil.encode(value.duplicate(), indexContext.getValidator()), key, indexContext.getValidator());
        }
        maxSSTableRowId = key.sstableRowId();
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
        else if (shouldFlush(key.sstableRowId()))
        {
            flushSegment();
            currentBuilder = newSegmentBuilder();
        }

        if (term.remaining() == 0) return;

        if (!TypeUtil.isLiteral(type))
        {
            limiter.increment(currentBuilder.add(term, key));
        }
        else
        {
            analyzer.reset(term);
            try
            {
                while (analyzer.hasNext())
                {
                    ByteBuffer tokenTerm = analyzer.next();
                    limiter.increment(currentBuilder.add(tokenTerm, key));
                }
            }
            finally
            {
                analyzer.end();
            }
        }
    }

    private boolean shouldFlush(long sstableRowId)
    {
        // If we've hit the minimum flush size and we've breached the global limit, flush a new segment:
        boolean reachMemoryLimit = limiter.usageExceedsLimit() && currentBuilder.hasReachedMinimumFlushSize();

        if (reachMemoryLimit)
        {
            logger.debug(indexContext.logMessage("Global limit of {} and minimum flush size of {} exceeded. " +
                                                 "Current builder usage is {} for {} cells. Global Usage is {}. Flushing..."),
                         FBUtilities.prettyPrintMemory(limiter.limitBytes()),
                         FBUtilities.prettyPrintMemory(currentBuilder.getMinimumFlushBytes()),
                         FBUtilities.prettyPrintMemory(currentBuilder.totalBytesAllocated()),
                         currentBuilder.getRowCount(),
                         FBUtilities.prettyPrintMemory(limiter.currentBytesUsed()));
        }

        return reachMemoryLimit || currentBuilder.exceedsSegmentLimit(sstableRowId);
    }

    private void flushSegment() throws IOException
    {
        long start = System.nanoTime();

        try
        {
            long bytesAllocated = currentBuilder.totalBytesAllocated();

            SegmentMetadata segmentMetadata = currentBuilder.flush(indexDescriptor, indexContext);

            long flushMillis = Math.max(1, TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));

            if (segmentMetadata != null)
            {
                segments.add(segmentMetadata);

                //TODO Need to look at some of these metrics
                double rowCount = segmentMetadata.numRows;
                if (indexContext.getIndexMetrics() != null)
                    indexContext.getIndexMetrics().compactionSegmentCellsPerSecond.update((long)(rowCount / flushMillis * 1000.0));

                double segmentBytes = segmentMetadata.componentMetadatas.indexSize();
                if (indexContext.getIndexMetrics() != null)
                    indexContext.getIndexMetrics().compactionSegmentBytesPerSecond.update((long)(segmentBytes / flushMillis * 1000.0));

                logger.debug(indexContext.logMessage("Flushed segment with {} cells for a total of {} in {} ms."),
                             (long) rowCount, FBUtilities.prettyPrintMemory((long) segmentBytes), flushMillis);
            }

            // Builder memory is released against the limiter at the conclusion of a successful
            // flush. Note that any failure that occurs before this (even in term addition) will
            // actuate this column writer's abort logic from the parent SSTable-level writer, and
            // that abort logic will release the current builder's memory against the limiter.
            long globalBytesUsed = currentBuilder.release(indexContext);
            currentBuilder = null;
            logger.debug(indexContext.logMessage("Flushing index segment for SSTable {} released {}. Global segment memory usage now at {}."),
                         indexDescriptor.descriptor, FBUtilities.prettyPrintMemory(bytesAllocated), FBUtilities.prettyPrintMemory(globalBytesUsed));

        }
        catch (Throwable t)
        {
            logger.error(indexContext.logMessage("Failed to build index for SSTable {}."), indexDescriptor.descriptor, t);
            indexDescriptor.deleteColumnIndex(indexContext);

            indexContext.getIndexMetrics().segmentFlushErrors.inc();

            throw t;
        }
    }

    @Override
    public void flush() throws IOException
    {
        if (maybeAbort())
            return;

        boolean emptySegment = currentBuilder == null || currentBuilder.isEmpty();
        logger.debug(indexContext.logMessage("Completing index flush with {}buffered data..."), emptySegment ? "no " : "");

        try
        {
            // parts are present but there is something still in memory, let's flush that inline
            if (!emptySegment)
            {
                flushSegment();
            }

            // Even an empty segment may carry some fixed memory, so remove it:
            if (currentBuilder != null)
            {
                long bytesAllocated = currentBuilder.totalBytesAllocated();
                long globalBytesUsed = currentBuilder.release(indexContext);
                logger.debug(indexContext.logMessage("Flushing final segment for SSTable {} released {}. Global segment memory usage now at {}."),
                             indexDescriptor.descriptor, FBUtilities.prettyPrintMemory(bytesAllocated), FBUtilities.prettyPrintMemory(globalBytesUsed));
            }

            compactSegments();

            writeSegmentsMetadata();
            indexDescriptor.createComponentOnDisk(IndexComponent.COLUMN_COMPLETION_MARKER, indexContext.getIndexName());
        }
        finally
        {
            if (indexContext.getIndexMetrics() != null)
            {
                indexContext.getIndexMetrics().segmentsPerCompaction.update(segments.size());
                segments.clear();
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
            long allocated = currentBuilder.totalBytesAllocated();
            long globalBytesUsed = currentBuilder.release(indexContext);
            logger.debug(indexContext.logMessage("Aborting index writer for SSTable {} released {}. Global segment memory usage now at {}."),
                         indexDescriptor.descriptor, FBUtilities.prettyPrintMemory(allocated), FBUtilities.prettyPrintMemory(globalBytesUsed));
        }

        indexDescriptor.deleteColumnIndex(indexContext);
    }

    protected void writeSegmentsMetadata() throws IOException
    {
        if (segments.isEmpty())
            return;

        assert segments.size() == 1 : "A post-compacted index should only contain a single segment";

        try
        {
            indexDescriptor.newIndexMetadataSerializer().serialize(new V1IndexOnDiskMetadata(segments), indexDescriptor, indexContext);
        }
        catch (IOException e)
        {
            abort(e);
            throw e;
        }
    }

    private void compactSegments() throws IOException
    {
        if (segments.isEmpty())
            return;

        PrimaryKey minKey = segments.get(0).minKey;
        PrimaryKey maxKey = segments.get(segments.size() - 1).maxKey;

        try (SegmentMerger segmentMerger = SegmentMerger.newSegmentMerger(indexContext.isLiteral());
             PerIndexFiles perIndexFiles = indexDescriptor.perIndexFiles(indexContext, true))
        {
            for (final SegmentMetadata segment : segments)
            {
                segmentMerger.addSegment(indexContext, segment, perIndexFiles);
            }
            segments.clear();
            segments.add(segmentMerger.merge(indexDescriptor, indexContext, minKey, maxKey, maxSSTableRowId));
        }
        finally
        {
            indexDescriptor.deleteTemporaryComponents(indexContext);
        }
    }

    private SegmentBuilder newSegmentBuilder()
    {
        SegmentBuilder builder = indexContext.isLiteral()
                                 ? new SegmentBuilder.RAMStringSegmentBuilder(indexContext.getValidator(), limiter)
                                 : new SegmentBuilder.KDTreeSegmentBuilder(indexContext.getValidator(), limiter, indexContext.getIndexWriterConfig());

        long globalBytesUsed = limiter.increment(builder.totalBytesAllocated());
        logger.debug(indexContext.logMessage("Created new segment builder while flushing SSTable {}. Global segment memory usage now at {}."),
                     indexDescriptor.descriptor, FBUtilities.prettyPrintMemory(globalBytesUsed));

        return builder;
    }
}
