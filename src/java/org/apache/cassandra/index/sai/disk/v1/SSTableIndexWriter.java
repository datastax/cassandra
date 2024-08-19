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
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.jbellis.jvector.pq.ProductQuantization;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SSTableIndex;
import org.apache.cassandra.index.sai.analyzer.AbstractAnalyzer;
import org.apache.cassandra.index.sai.disk.PerIndexWriter;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.index.sai.disk.format.IndexComponents;
import org.apache.cassandra.index.sai.disk.v2.V2VectorIndexSearcher;
import org.apache.cassandra.index.sai.disk.v3.V3OnDiskFormat;
import org.apache.cassandra.index.sai.disk.v5.V5VectorPostingsWriter;
import org.apache.cassandra.index.sai.disk.vector.CassandraDiskAnn;
import org.apache.cassandra.index.sai.disk.vector.CassandraOnHeapGraph;
import org.apache.cassandra.index.sai.disk.vector.VectorCompression.CompressionType;
import org.apache.cassandra.index.sai.utils.NamedMemoryLimiter;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Throwables;

import static org.apache.cassandra.utils.Clock.Global.nanoTime;

/**
 * Column index writer that accumulates (on-heap) indexed data from a compacted SSTable as it's being flushed to disk.
 */
@NotThreadSafe
public class SSTableIndexWriter implements PerIndexWriter
{
    private static final Logger logger = LoggerFactory.getLogger(SSTableIndexWriter.class);

    private final IndexComponents.ForWrite perIndexComponents;
    private final IndexContext indexContext;
    private final long nowInSec = FBUtilities.nowInSeconds();
    private final AbstractAnalyzer analyzer;
    private final NamedMemoryLimiter limiter;
    private final BooleanSupplier isIndexValid;
    private final long keyCount;

    private boolean aborted = false;

    // segment writer
    private SegmentBuilder currentBuilder;
    private final List<SegmentMetadata> segments = new ArrayList<>();

    public SSTableIndexWriter(IndexComponents.ForWrite perIndexComponents, NamedMemoryLimiter limiter, BooleanSupplier isIndexValid, long keyCount)
    {
        this.perIndexComponents = perIndexComponents;
        this.indexContext = perIndexComponents.context();
        Preconditions.checkNotNull(indexContext, "Provided components %s are the per-sstable ones, expected per-index ones", perIndexComponents);
        this.analyzer = indexContext.getAnalyzerFactory().create();
        this.limiter = limiter;
        this.isIndexValid = isIndexValid;
        this.keyCount = keyCount;
    }

    @Override
    public IndexContext indexContext()
    {
        return indexContext;
    }

    @Override
    public IndexComponents.ForWrite writtenComponents()
    {
        return perIndexComponents;
    }

    @Override
    public void addRow(PrimaryKey key, Row row, long sstableRowId) throws IOException
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
                    addTerm(TypeUtil.asIndexBytes(value.duplicate(), indexContext.getValidator()), key, sstableRowId, indexContext.getValidator());
                }
            }
        }
        else
        {
            ByteBuffer value = indexContext.getValueOf(key.partitionKey(), row, nowInSec);
            if (value != null)
                addTerm(TypeUtil.asIndexBytes(value.duplicate(), indexContext.getValidator()), key, sstableRowId, indexContext.getValidator());
        }
    }

    @Override
    public void complete(Stopwatch stopwatch) throws IOException
    {
        if (maybeAbort())
            return;

        long start = stopwatch.elapsed(TimeUnit.MILLISECONDS);
        long elapsed;

        boolean emptySegment = currentBuilder == null || currentBuilder.isEmpty();
        logger.debug("Completing index flush with {}buffered data...", emptySegment ? "no " : "");

        try
        {
            // parts are present but there is something still in memory, let's flush that inline
            if (!emptySegment)
            {
                flushSegment();
                elapsed = stopwatch.elapsed(TimeUnit.MILLISECONDS);
                logger.debug("Completed flush of final segment for SSTable {}. Duration: {} ms. Total elapsed: {} ms",
                             perIndexComponents.descriptor(),
                             elapsed - start,
                             elapsed);
            }

            // Even an empty segment may carry some fixed memory, so remove it:
            if (currentBuilder != null)
            {
                long bytesAllocated = currentBuilder.totalBytesAllocated();
                long globalBytesUsed = currentBuilder.release(indexContext);
                logger.debug("Flushing final segment for SSTable {} released {}. Global segment memory usage now at {}",
                             perIndexComponents.descriptor(), FBUtilities.prettyPrintMemory(bytesAllocated), FBUtilities.prettyPrintMemory(globalBytesUsed));
            }

            writeSegmentsMetadata();
            perIndexComponents.markComplete();
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
        if (aborted)
            return;

        aborted = true;

        logger.warn("Aborting SSTable index flush for {}...", perIndexComponents.descriptor(), cause);

        // It's possible for the current builder to be unassigned after we flush a final segment.
        if (currentBuilder != null)
        {
            // If an exception is thrown out of any writer operation prior to successful segment
            // flush, we will end up here, and we need to free up builder memory tracked by the limiter:
            long allocated = currentBuilder.totalBytesAllocated();
            long globalBytesUsed = currentBuilder.release(indexContext);
            logger.debug("Aborting index writer for SSTable {} released {}. Global segment memory usage now at {}",
                         perIndexComponents.descriptor(), FBUtilities.prettyPrintMemory(allocated), FBUtilities.prettyPrintMemory(globalBytesUsed));
        }

        if (CassandraRelevantProperties.DELETE_CORRUPT_SAI_COMPONENTS.getBoolean())
            perIndexComponents.forceDeleteAllComponents();
        else
            logger.debug("Skipping delete of index components after failure on index build of {}.{}", perIndexComponents.indexDescriptor(), indexContext);
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

    private void addTerm(ByteBuffer term, PrimaryKey key, long sstableRowId, AbstractType<?> type) throws IOException
    {
        if (!indexContext.validateMaxTermSize(key.partitionKey(), term))
            return;

        if (currentBuilder == null)
        {
            currentBuilder = newSegmentBuilder(sstableRowId);
        }
        else if (shouldFlush(sstableRowId))
        {
            flushSegment();
            currentBuilder = newSegmentBuilder(sstableRowId);
        }

        if (term.remaining() == 0)
            return;

        long allocated = currentBuilder.addAll(term, type, key, sstableRowId, analyzer, indexContext);
        limiter.increment(allocated);
    }

    private boolean shouldFlush(long sstableRowId)
    {
        // If we've hit the minimum flush size and we've breached the global limit, flush a new segment:
        boolean reachMemoryLimit = limiter.usageExceedsLimit() && currentBuilder.hasReachedMinimumFlushSize();

        if (currentBuilder.requiresFlush() || reachMemoryLimit)
        {
            logger.debug("Global limit of {} and minimum flush size of {} exceeded. " +
                         "Current builder usage is {} for {} rows. Global Usage is {}. Flushing...",
                         FBUtilities.prettyPrintMemory(limiter.limitBytes()),
                         FBUtilities.prettyPrintMemory(currentBuilder.getMinimumFlushBytes()),
                         FBUtilities.prettyPrintMemory(currentBuilder.totalBytesAllocated()),
                         currentBuilder.getRowCount(),
                         FBUtilities.prettyPrintMemory(limiter.currentBytesUsed()));
        }

        return reachMemoryLimit || currentBuilder.exceedsSegmentLimit(sstableRowId) || currentBuilder.requiresFlush();
    }

    private void flushSegment() throws IOException
    {
        currentBuilder.awaitAsyncAdditions();
        if (currentBuilder.supportsAsyncAdd()
            && currentBuilder.totalBytesAllocatedConcurrent.sum() > 1.1 * currentBuilder.totalBytesAllocated())
        {
            logger.warn("Concurrent memory usage is higher than estimated: {} vs {}",
                        currentBuilder.totalBytesAllocatedConcurrent.sum(), currentBuilder.totalBytesAllocated());
        }

        // throw exceptions that occurred during async addInternal()
        var ae = currentBuilder.getAsyncThrowable();
        if (ae != null)
            Throwables.throwAsUncheckedException(ae);

        long start = nanoTime();
        try
        {
            long bytesAllocated = currentBuilder.totalBytesAllocated();
            SegmentMetadata segmentMetadata = currentBuilder.flush();
            long flushMillis = Math.max(1, TimeUnit.NANOSECONDS.toMillis(nanoTime() - start));

            if (segmentMetadata != null)
            {
                segments.add(segmentMetadata);

                double rowCount = segmentMetadata.numRows;
                if (indexContext.getIndexMetrics() != null)
                    indexContext.getIndexMetrics().compactionSegmentCellsPerSecond.update((long)(rowCount / flushMillis * 1000.0));

                double segmentBytes = segmentMetadata.componentMetadatas.indexSize();
                if (indexContext.getIndexMetrics() != null)
                    indexContext.getIndexMetrics().compactionSegmentBytesPerSecond.update((long)(segmentBytes / flushMillis * 1000.0));

                logger.debug("Flushed segment with {} cells for a total of {} in {} ms",
                             (long) rowCount, FBUtilities.prettyPrintMemory((long) segmentBytes), flushMillis);
            }

            // Builder memory is released against the limiter at the conclusion of a successful
            // flush. Note that any failure that occurs before this (even in term addition) will
            // actuate this column writer's abort logic from the parent SSTable-level writer, and
            // that abort logic will release the current builder's memory against the limiter.
            long globalBytesUsed = currentBuilder.release(indexContext);
            currentBuilder = null;
            logger.debug("Flushing index segment for SSTable {} released {}. Global segment memory usage now at {}",
                         perIndexComponents.descriptor(), FBUtilities.prettyPrintMemory(bytesAllocated), FBUtilities.prettyPrintMemory(globalBytesUsed));

        }
        catch (Throwable t)
        {
            logger.error("Failed to build index for SSTable {}", perIndexComponents.descriptor(), t);
            perIndexComponents.forceDeleteAllComponents();

            indexContext.getIndexMetrics().segmentFlushErrors.inc();

            throw t;
        }
    }

    private void writeSegmentsMetadata() throws IOException
    {
        if (segments.isEmpty())
            return;

        try (MetadataWriter writer = new MetadataWriter(perIndexComponents))
        {
            SegmentMetadata.write(writer, segments);
        }
        catch (IOException e)
        {
            abort(e);
            throw e;
        }
    }

    private SegmentBuilder newSegmentBuilder(long rowIdOffset) throws IOException
    {
        SegmentBuilder builder;

        if (indexContext.isVector())
        {
            // if we have a PQ instance available, we can use it to build a CompactionGraph;
            // otherwise, build on heap (which will create PQ for next time, if we have enough vectors)
            var pqi = CassandraOnHeapGraph.getPqIfPresent(indexContext, vc -> vc.type == CompressionType.PRODUCT_QUANTIZATION);
            if (pqi == null && segments.size() > 0)
                pqi = maybeReadPqFromLastSegment();

            if (pqi == null || pqi.unitVectors.isEmpty() || !V3OnDiskFormat.ENABLE_LTM_CONSTRUCTION)
            {
                builder = new SegmentBuilder.VectorOnHeapSegmentBuilder(perIndexComponents, rowIdOffset, keyCount, limiter);
            }
            else
            {
                var allRowsHaveVectors = allRowsHaveVectorsInWrittenSegments(indexContext);
                builder = new SegmentBuilder.VectorOffHeapSegmentBuilder(perIndexComponents, rowIdOffset, keyCount, pqi.pq, pqi.unitVectors.get(), allRowsHaveVectors, limiter);
            }
        }
        else if (indexContext.isLiteral())
        {
            builder = new SegmentBuilder.RAMStringSegmentBuilder(perIndexComponents, rowIdOffset, limiter);
        }
        else
        {
            builder = new SegmentBuilder.KDTreeSegmentBuilder(perIndexComponents, rowIdOffset, limiter, indexContext.getIndexWriterConfig());
        }

        long globalBytesUsed = limiter.increment(builder.totalBytesAllocated());
        logger.debug("Created new segment builder while flushing SSTable {}. Global segment memory usage now at {}",
                     perIndexComponents.descriptor(),
                     FBUtilities.prettyPrintMemory(globalBytesUsed));

        return builder;
    }

    private static boolean allRowsHaveVectorsInWrittenSegments(IndexContext indexContext)
    {
        int segmentsChecked = 0;
        for (SSTableIndex index : indexContext.getView().getIndexes())
        {
            for (Segment segment : index.getSegments())
            {
                segmentsChecked++;
                var searcher = (V2VectorIndexSearcher) segment.getIndexSearcher();
                var structure = searcher.getPostingsStructure();
                if (structure == V5VectorPostingsWriter.Structure.ZERO_OR_ONE_TO_MANY)
                    return false;
            }
        }
        return segmentsChecked != 0;
    }

    private CassandraOnHeapGraph.PqInfo maybeReadPqFromLastSegment() throws IOException
    {
        // No PQ instance available in completed indexes, so check if we just wrote one
        var pqComponent = perIndexComponents.get(IndexComponentType.PQ);
        assert pqComponent != null; // we always have a PQ component even if it's not actually PQ compression
        try (var fh = pqComponent.createFileHandle();
             var reader = fh.createReader())
        {
            var sm = segments.get(segments.size() - 1);
            long offset = sm.componentMetadatas.get(IndexComponentType.PQ).offset;
            // close parallel to code in CassandraDiskANN constructor, but different enough
            // (we only want the PQ codebook) that it's difficult to extract into a common method
            reader.seek(offset);
            boolean unitVectors;
            if (reader.readInt() == CassandraDiskAnn.PQ_MAGIC)
            {
                reader.readInt(); // skip over version
                unitVectors = reader.readBoolean();
            }
            else
            {
                unitVectors = true;
                reader.seek(offset);
            }
            var compressionType = CompressionType.values()[reader.readByte()];
            if (compressionType == CompressionType.PRODUCT_QUANTIZATION)
            {
                var pq = ProductQuantization.load(reader);
                return new CassandraOnHeapGraph.PqInfo(pq, Optional.of(unitVectors));
            }
        }
        return null;
    }
}
