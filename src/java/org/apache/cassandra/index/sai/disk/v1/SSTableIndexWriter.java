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
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.jbellis.jvector.graph.disk.feature.FeatureId;
import io.github.jbellis.jvector.quantization.ProductQuantization;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SSTableIndex;
import org.apache.cassandra.index.sai.disk.PerIndexWriter;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.index.sai.disk.format.IndexComponents;
import org.apache.cassandra.index.sai.disk.v1.Segment;
import org.apache.cassandra.index.sai.disk.v2.V2VectorIndexSearcher;
import org.apache.cassandra.index.sai.disk.v3.V3OnDiskFormat;
import org.apache.cassandra.index.sai.disk.v5.V5OnDiskFormat;
import org.apache.cassandra.index.sai.disk.v5.V5VectorIndexSearcher;
import org.apache.cassandra.index.sai.disk.v5.V5VectorPostingsWriter;
import org.apache.cassandra.index.sai.disk.vector.CassandraDiskAnn;
import org.apache.cassandra.index.sai.disk.vector.CassandraOnHeapGraph;
import org.apache.cassandra.index.sai.disk.vector.CompactionGraphMerger;
import org.apache.cassandra.index.sai.disk.vector.JVectorVersionUtil;
import org.apache.cassandra.index.sai.disk.vector.VectorCompression.CompressionType;
import org.apache.cassandra.index.sai.disk.vector.VectorIndexIntegrity;
import org.apache.cassandra.index.sai.disk.vector.VectorSourceTagRing;
import org.apache.cassandra.index.sai.metrics.IndexMetrics;
import org.apache.cassandra.index.sai.metrics.VectorCompactionMetrics;
import org.apache.cassandra.index.sai.utils.NamedMemoryLimiter;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.storage.StorageProvider;
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
    private final IndexMetrics indexMetrics;
    private final long nowInSec = FBUtilities.nowInSeconds();
    private final NamedMemoryLimiter limiter;
    private final BooleanSupplier isIndexDropped;
    private final BooleanSupplier isIndexUnloaded;
    private final long keyCount;
    @Nullable
    private final Set<SSTableReader> inputSSTables;
    /** Non-null only for a vector-index compaction build: the per-thread row-source tag ring. */
    @Nullable
    private final VectorSourceTagRing sourceTagRing;

    private boolean aborted = false;

    // segment writer
    private SegmentBuilder currentBuilder;
    private final List<SegmentMetadata> segments = new ArrayList<>();

    public SSTableIndexWriter(IndexComponents.ForWrite perIndexComponents, NamedMemoryLimiter limiter,
                              BooleanSupplier isIndexDropped, BooleanSupplier isIndexUnloaded, long keyCount)
    {
        this(perIndexComponents, limiter, isIndexDropped, isIndexUnloaded, keyCount, null);
    }

    public SSTableIndexWriter(IndexComponents.ForWrite perIndexComponents, NamedMemoryLimiter limiter,
                              BooleanSupplier isIndexDropped, BooleanSupplier isIndexUnloaded, long keyCount,
                              @Nullable Set<SSTableReader> inputSSTables)
    {
        this.perIndexComponents = perIndexComponents;
        this.indexContext = perIndexComponents.context();
        Preconditions.checkNotNull(indexContext, "Provided components %s are the per-sstable ones, expected per-index ones", perIndexComponents);
        this.indexMetrics = indexContext.getIndexMetrics().orElse(null);
        this.limiter = limiter;
        this.isIndexDropped = isIndexDropped;
        this.isIndexUnloaded = isIndexUnloaded;
        this.keyCount = keyCount;
        this.inputSSTables = inputSSTables;

        // Vector compaction: register the row-source tag ring BEFORE any row flows through
        // the merge listener. Registration is per-compaction-thread and idempotent across
        // output-writer switches; the compaction iterator's close() drops it.
        if (indexContext.isVector() && inputSSTables != null && !inputSSTables.isEmpty()
            && CompactionGraphMerger.ENABLED)
        {
            this.sourceTagRing = VectorSourceTagRing.acquireForThread(indexContext.getDefinition());
        }
        else
        {
            this.sourceTagRing = null;
        }
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

        // This is to avoid duplicates (and also reduce space taken by indexes on static columns).
        // An index on a static column indexes static rows only.
        // An index on a non-static column indexes regular rows only.
        if (indexContext.getDefinition().isStatic() != row.isStatic())
            return;

        boolean addedRow = false;
        if (indexContext.isNonFrozenCollection())
        {
            Iterator<ByteBuffer> valueIterator = indexContext.getValuesOf(row, nowInSec);
            if (valueIterator != null)
            {
                while (valueIterator.hasNext())
                {
                    ByteBuffer value = valueIterator.next();
                    addedRow = addTerm(TypeUtil.asIndexBytes(value.duplicate(), indexContext.getValidator()), key, sstableRowId, indexContext.getValidator());
                }
            }
        }
        else
        {
            ByteBuffer value = indexContext.getValueOf(key.partitionKey(), row, nowInSec);
            if (value != null)
            {
                addedRow = addTerm(TypeUtil.asIndexBytes(value.duplicate(), indexContext.getValidator()), key, sstableRowId, indexContext.getValidator());
            }
        }
        if (addedRow)
            currentBuilder.incRowCount();

    }

    @Override
    public void onSSTableWriterSwitched(Stopwatch stopwatch) throws IOException
    {
        if (maybeAbort())
            return;

        boolean emptySegment = currentBuilder == null || currentBuilder.isEmpty();
        logger.debug("Flushing index {} with {}buffered data on sstable writer switched...", indexContext.getIndexName(), emptySegment ? "no " : "");
        if (!emptySegment)
            flushSegment();
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
            indexContext.getIndexMetrics().ifPresent(m -> {
                m.segmentsPerCompaction.update(segments.size());
                segments.clear();
                m.compactionCount.inc();
            });
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

        boolean dropped = isIndexDropped.getAsBoolean();
        boolean unloaded = isIndexUnloaded.getAsBoolean();
        if (!dropped && !unloaded)
            return false;

        String message = String.format("index %s is %s", indexContext.getIndexName(), dropped ? "dropped" : "unloaded");
        RuntimeException runtimeException = new RuntimeException(message);

        // abort index build for remove on disk index file
        abort(runtimeException);

        // if index is dropped, we can continue compaction task or index build without current index
        if (dropped)
            return true;

        // if index is unloaded after unassigning tenant, fail the compaction task or index build to avoid incomplete index files
        throw runtimeException;
    }

    private boolean addTerm(ByteBuffer term, PrimaryKey key, long sstableRowId, AbstractType<?> type) throws IOException
    {
        if (!indexContext.validateMaxTermSize(key.partitionKey(), term))
            return false;

        if (currentBuilder == null)
        {
            currentBuilder = newSegmentBuilder(sstableRowId);
        }
        else if (shouldFlush(sstableRowId))
        {
            flushSegment();
            currentBuilder = newSegmentBuilder(sstableRowId);
        }

        if (term.remaining() == 0 && TypeUtil.skipsEmptyValue(indexContext.getValidator()))
            return false;

        long allocated = currentBuilder.analyzeAndAdd(term, type, key, sstableRowId, indexMetrics);
        limiter.increment(allocated);
        return true;
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
                double segmentBytes = segmentMetadata.componentMetadatas.indexSize();

                indexContext.getIndexMetrics().ifPresent(m -> {
                    m.compactionSegmentCellsPerSecond.update((long)(rowCount / flushMillis * 1000.0));
                    m.compactionSegmentBytesPerSecond.update((long)(segmentBytes / flushMillis * 1000.0));
                });

                logger.debug("Flushed segment with {} cells for a total of {} in {} ms for index {} with starting row id {} for sstable {}",
                             (long) rowCount, FBUtilities.prettyPrintMemory((long) segmentBytes), flushMillis, indexContext.getIndexName(),
                             segmentMetadata.minSSTableRowId, perIndexComponents.descriptor());
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

            indexContext.getIndexMetrics().ifPresent(m -> m.segmentFlushErrors.inc());

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
            // Decide between the bounded jvector merge path (OnDiskGraphIndexCompactor) and the
            // legacy rebuild path (VectorOffHeap/OnHeapSegmentBuilder). The merge applies only to
            // the first output segment of a multi-sstable compaction whose sources all carry
            // INLINE_VECTORS and whose output uses the V5 vector-postings format.
            final int inputSSTableCount = inputSSTables == null ? 0 : inputSSTables.size();
            MergeSourceScan scan = null;
            String rebuildReason; // null => merge path chosen

            if (!CompactionGraphMerger.ENABLED)
                rebuildReason = "merge disabled by killswitch (" + CassandraRelevantProperties.SAI_VECTOR_GRAPH_COMPACTION_MERGE_ENABLED.getKey() + "=false)";
            else if (!segments.isEmpty())
                rebuildReason = "not the first output segment (merge applies only to the first segment of a build)";
            else if (inputSSTables == null || inputSSTables.isEmpty())
                rebuildReason = "no input sstables to merge (not a multi-sstable compaction)";
            else
            {
                try (io.github.jbellis.jvector.util.work.ProgressTracker.PhaseScope ignored =
                             VectorCompactionMetrics.INSTANCE.start(
                                     VectorCompactionMetrics.Phase.SOURCE_SCAN,
                                     indexContext))
                {
                    scan = collectMergeSources();
                }
                if (scan.candidateVectorSegments == 0)
                    rebuildReason = "no vector index segments among the " + scan.inputSSTableCount + " input sstable(s)";
                else if (scan.inlineVectorSegments < scan.candidateVectorSegments)
                    rebuildReason = (scan.candidateVectorSegments - scan.inlineVectorSegments) + " of " + scan.candidateVectorSegments
                                    + " source segments lack INLINE_VECTORS (first: " + scan.firstMissingInlineSource
                                    + "; likely NVQ — OnDiskGraphIndexCompactor requires INLINE_VECTORS on every source)";
                else if (scan.uncoveredInputSSTables > 0)
                    rebuildReason = scan.uncoveredInputSSTables + " input sstable(s) have no vector index segment " +
                                    "(rows there may carry vectors absent from every source graph)";
                else if (scan.sources.size() < 2)
                    rebuildReason = "only " + scan.sources.size() + " mergeable source segment(s); at least 2 required";
                else if (!V5OnDiskFormat.writeV5VectorPostings(indexContext.version()))
                    rebuildReason = "output on-disk format " + indexContext.version() + " does not use V5 vector postings (required for merge)";
                else
                    rebuildReason = null; // all merge conditions satisfied
            }

            if (rebuildReason == null)
            {
                builder = new SegmentBuilder.VectorMergeSegmentBuilder(perIndexComponents, rowIdOffset, keyCount, scan.sources, limiter);
                logBuildDecision("MERGE", "streaming merge of " + scan.sources.size() + " on-disk graph segments",
                                 "LOW (streams on-disk graphs on the compaction thread)", inputSSTableCount, scan);
            }
            else
            {
                // if we have a PQ instance available, we can use it to build a CompactionGraph;
                // otherwise, build on heap (which will create PQ for next time, if we have enough vectors)
                var pqi = CassandraOnHeapGraph.getPqIfPresent(indexContext, vc -> vc.type == CompressionType.PRODUCT_QUANTIZATION);
                // If no PQ instance available in indexes of completed sstables, check if we just wrote one in the previous segment
                if (pqi == null && !segments.isEmpty())
                    pqi = maybeReadPqFromLastSegment();

                if (pqi != null && V3OnDiskFormat.ENABLE_LTM_CONSTRUCTION)
                {
                    var allRowsHaveVectors = allRowsHaveVectorsInWrittenSegments(indexContext);
                    builder = new SegmentBuilder.VectorOffHeapSegmentBuilder(perIndexComponents, rowIdOffset, keyCount, pqi.pq, pqi.unitVectors, allRowsHaveVectors, limiter);
                    logBuildDecision("OFF_HEAP_REBUILD", rebuildReason,
                                     "MODERATE (vectors off-heap; graph built with all-core jvector pools)", inputSSTableCount, scan);
                }
                else
                {
                    // building on heap is the only way to get a PQ from nothing (CompactionGraph only knows how to fine-tune an existing one)
                    builder = new SegmentBuilder.VectorOnHeapSegmentBuilder(perIndexComponents, rowIdOffset, keyCount, limiter);
                    logBuildDecision("ON_HEAP_REBUILD", rebuildReason,
                                     "HIGH (full graph materialized on heap)", inputSSTableCount, scan);
                }
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
        logger.debug("Created new segment builder while flushing SSTable {}. Global segment memory usage now at {} with {} active segment builders",
                     perIndexComponents.descriptor(),
                     FBUtilities.prettyPrintMemory(globalBytesUsed),
                     SegmentBuilder.ACTIVE_BUILDER_COUNT.get() - 1);

        return builder;
    }

    /**
     * Scans the input sstables' vector index segments for jvector merge eligibility.
     */
    private MergeSourceScan collectMergeSources()
    {
        var sources = new ArrayList<CompactionGraphMerger.SourceSegment>();
        int candidateVectorSegments = 0;
        int inlineVectorSegments = 0;
        int nonVectorSearchersSkipped = 0;
        String firstMissingInlineSource = null;
        int inputSSTableCount = inputSSTables == null ? 0 : inputSSTables.size();
        var uncoveredInputs = inputSSTables == null
                              ? java.util.Collections.<SSTableReader>emptySet()
                              : new java.util.HashSet<>(inputSSTables);

        if (inputSSTables != null && !inputSSTables.isEmpty())
        {
            for (SSTableIndex ssTableIndex : indexContext.getView().getIndexes())
            {
                if (!inputSSTables.contains(ssTableIndex.getSSTable()))
                    continue;
                uncoveredInputs.remove(ssTableIndex.getSSTable());

                long inputRows = ssTableIndex.getSSTable().getTotalRows();
                if ((ssTableIndex.isEmpty() || ssTableIndex.getSegments().isEmpty()) && inputRows > 0)
                    throw VectorIndexIntegrity.abort(String.format(
                            "compaction input %s has %d row(s) but an EMPTY vector index (%s); a merge over it "
                            + "would silently drop those vectors",
                            ssTableIndex.getSSTable().descriptor, inputRows,
                            ssTableIndex.isEmpty() ? "EmptyIndex" : "no segments"));

                for (Segment segment : ssTableIndex.getSegments())
                {
                    var searcher = segment.getIndexSearcher();
                    if (!(searcher instanceof V2VectorIndexSearcher))
                    {
                        nonVectorSearchersSkipped++;
                        continue;
                    }
                    candidateVectorSegments++;
                    var diskAnn = ((V2VectorIndexSearcher) searcher).graph;
                    if (diskAnn.getOnDiskGraph().getIdUpperBound() == 0)
                        throw VectorIndexIntegrity.abort(String.format(
                                "compaction input %s@rowOffset=%d has a vector index segment whose graph is EMPTY",
                                ssTableIndex.getSSTable().descriptor, segment.metadata.segmentRowIdOffset));
                    if (!diskAnn.getOnDiskGraph().getFeatureSet().contains(FeatureId.INLINE_VECTORS))
                    {
                        if (firstMissingInlineSource == null)
                            firstMissingInlineSource = ssTableIndex.getSSTable().descriptor
                                                       + "@rowOffset=" + segment.metadata.segmentRowIdOffset;
                        continue;
                    }
                    inlineVectorSegments++;
                    sources.add(new CompactionGraphMerger.SourceSegment(diskAnn, segment.metadata.segmentRowIdOffset, ssTableIndex));
                }
            }
        }
        return new MergeSourceScan(sources, inputSSTableCount, candidateVectorSegments,
                                   inlineVectorSegments, nonVectorSearchersSkipped, firstMissingInlineSource,
                                   uncoveredInputs.size());
    }

    /**
     * Emits a single INFO line explaining which vector-index build path was chosen for this segment.
     */
    private void logBuildDecision(String path, String reason, String heapProfile, int inputSSTableCount, @Nullable MergeSourceScan scan)
    {
        logger.info("Vector SAI build decision [{}.{}.{}] sstable={} path={} rows~={} inputs={}: {}. " +
                    "sources_used={} candidates={} inline_vectors={} non_vector_skipped={}, " +
                    "output_version={} v5_postings={} heap={}, killswitch={} enable_fused={} enable_nvq={} jvector_version={}",
                    indexContext.getKeyspace(), indexContext.getTable(), indexContext.getIndexName(),
                    perIndexComponents.descriptor(), path, keyCount, inputSSTableCount, reason,
                    scan == null ? 0 : scan.sources.size(),
                    scan == null ? 0 : scan.candidateVectorSegments,
                    scan == null ? 0 : scan.inlineVectorSegments,
                    scan == null ? 0 : scan.nonVectorSearchersSkipped,
                    indexContext.version(), V5OnDiskFormat.writeV5VectorPostings(indexContext.version()), heapProfile,
                    CompactionGraphMerger.ENABLED, JVectorVersionUtil.ENABLE_FUSED, JVectorVersionUtil.ENABLE_NVQ,
                    indexContext.version().onDiskFormat().jvectorFileFormatVersion());
    }

    /** Result of scanning a compaction's input sstables for jvector merge eligibility. */
    private static final class MergeSourceScan
    {
        /** Candidate source segments carrying INLINE_VECTORS (usable for the merge). */
        final List<CompactionGraphMerger.SourceSegment> sources;
        /** Number of input sstables scanned. */
        final int inputSSTableCount;
        /** Vector index segments (V2VectorIndexSearcher) found across the inputs. */
        final int candidateVectorSegments;
        /** Of the candidates, how many carried INLINE_VECTORS. */
        final int inlineVectorSegments;
        /** Searchers skipped because they were not vector searchers. */
        final int nonVectorSearchersSkipped;
        /** Human-readable id of the first candidate lacking INLINE_VECTORS, or null. */
        @Nullable
        final String firstMissingInlineSource;
        /**
         * Input sstables with NO vector index segment in the view. Any > 0 disqualifies the merge.
         */
        final int uncoveredInputSSTables;

        MergeSourceScan(List<CompactionGraphMerger.SourceSegment> sources, int inputSSTableCount,
                        int candidateVectorSegments, int inlineVectorSegments, int nonVectorSearchersSkipped,
                        @Nullable String firstMissingInlineSource, int uncoveredInputSSTables)
        {
            this.sources = sources;
            this.inputSSTableCount = inputSSTableCount;
            this.candidateVectorSegments = candidateVectorSegments;
            this.inlineVectorSegments = inlineVectorSegments;
            this.nonVectorSearchersSkipped = nonVectorSearchersSkipped;
            this.firstMissingInlineSource = firstMissingInlineSource;
            this.uncoveredInputSSTables = uncoveredInputSSTables;
        }
    }

    private static boolean allRowsHaveVectorsInWrittenSegments(IndexContext indexContext)
    {
        for (SSTableIndex index : indexContext.getView().getIndexes())
        {
            for (Segment segment : index.getSegments())
            {
                if (segment.getIndexSearcher() instanceof  V2VectorIndexSearcher)
                    return true; // V2 doesn't know, so we err on the side of being optimistic.  See comments in CompactionGraph
                var searcher = (V5VectorIndexSearcher) segment.getIndexSearcher();
                var structure = searcher.getPostingsStructure();
                if (structure == V5VectorPostingsWriter.Structure.ZERO_OR_ONE_TO_MANY)
                    return false;
            }
        }
        return true;
    }

    private CassandraOnHeapGraph.PqInfo maybeReadPqFromLastSegment() throws IOException
    {
        var pqComponent = perIndexComponents.get(IndexComponentType.PQ);
        assert pqComponent != null; // we always have a PQ component even if it's not actually PQ compression

        var fhBuilder = StorageProvider.instance.indexBuildTimeFileHandleBuilderFor(pqComponent);
        try (var fh = fhBuilder.complete();
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
                return new CassandraOnHeapGraph.PqInfo(pq, unitVectors, sm.numRows);
            }
        }
        return null;
    }
}
