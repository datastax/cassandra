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
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.jbellis.jvector.quantization.VectorCompressor;
import io.github.jbellis.jvector.util.work.ProgressTracker;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.analyzer.AbstractAnalyzer;
import org.apache.cassandra.index.sai.analyzer.ByteLimitedMaterializer;
import org.apache.cassandra.index.sai.analyzer.NoOpAnalyzer;
import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.RAMStringIndexer;
import org.apache.cassandra.index.sai.disk.TermsIterator;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.index.sai.disk.format.IndexComponents;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.disk.v1.kdtree.BKDTreeRamBuffer;
import org.apache.cassandra.index.sai.disk.v1.kdtree.MutableOneDimPointValues;
import org.apache.cassandra.index.sai.disk.v1.kdtree.NumericIndexWriter;
import org.apache.cassandra.index.sai.disk.v1.trie.InvertedIndexWriter;
import org.apache.cassandra.index.sai.disk.vector.CassandraOnHeapGraph;
import org.apache.cassandra.index.sai.disk.vector.CompactionGraph;
import org.apache.cassandra.index.sai.disk.vector.JVectorVersionUtil;
import org.apache.cassandra.index.sai.utils.InsertFanout;
import org.apache.cassandra.index.sai.metrics.IndexMetrics;
import org.apache.cassandra.index.sai.metrics.VectorCompactionMetrics;
import org.apache.cassandra.index.sai.utils.NamedMemoryLimiter;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.metrics.QuickSlidingWindowReservoir;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;
import org.apache.lucene.util.BytesRef;

import static org.apache.cassandra.config.CassandraRelevantProperties.SAI_TEST_LAST_VALID_SEGMENTS;

/**
 * Creates an on-heap index data structure to be flushed to an SSTable index.
 * <p>
 * Not threadsafe, but does potentially make concurrent calls to addInternal by
 * delegating them to an asynchronous executor.  This will be done when supportsAsyncAdd is true.
 * Callers should check getAsyncThrowable when they are done adding rows to see if there was an error.
 */
@NotThreadSafe
public abstract class SegmentBuilder
{
    private static final Logger logger = LoggerFactory.getLogger(SegmentBuilder.class);

    // Served as safe net in case memory limit is not triggered or when merger merges small segments..
    public static final long LAST_VALID_SEGMENT_ROW_ID = ((long)Integer.MAX_VALUE / 2) - 1L;
    private static long testLastValidSegmentRowId = SAI_TEST_LAST_VALID_SEGMENTS.getLong();

    /** The number of column indexes being built globally. (Starts at one to avoid divide by zero.) */
    public static final AtomicLong ACTIVE_BUILDER_COUNT = new AtomicLong(1);

    /** Minimum flush size, dynamically updated as segment builds are started and completed/aborted. */
    private static volatile long minimumFlushBytes;

    protected final IndexComponents.ForWrite components;

    final AbstractType<?> termComparator;
    final AbstractAnalyzer analyzer;

    // track memory usage for this segment so we can flush when it gets too big
    private final NamedMemoryLimiter limiter;
    long totalBytesAllocated;
    // when we're adding terms asynchronously, totalBytesAllocated will be an approximation and this tracks the exact size
    final LongAdder totalBytesAllocatedConcurrent = new LongAdder();

    private final long lastValidSegmentRowID;

    private boolean flushed = false;
    private boolean active = true;

    // Node-wide concurrent-build admission permit for vector builds; null for non-vector builders and when
    // the bound is disabled. Acquired in the vector subclass constructor, released exactly once in release().
    private JVectorVersionUtil.BuildPermit buildPermit;

    // segment metadata
    private long minSSTableRowId = -1;
    private long maxSSTableRowId = -1;
    private long segmentRowIdOffset = 0;
    int rowCount = 0;
    long totalTermCount = 0;
    int maxSegmentRowId = -1;
    // in token order
    private PrimaryKey minKey;
    private PrimaryKey maxKey;
    // in termComparator order
    protected ByteBuffer minTerm;
    protected ByteBuffer maxTerm;

    protected final QuickSlidingWindowReservoir termSizeReservoir = new QuickSlidingWindowReservoir(100);
    /** Weighted, bounded (or unbounded) async insert fan-out; set by the vector builders, null for synchronous builders. */
    protected InsertFanout insertFanout;


    public boolean requiresFlush()
    {
        return false;
    }

    public static class KDTreeSegmentBuilder extends SegmentBuilder
    {
        protected final byte[] buffer;
        private final BKDTreeRamBuffer kdTreeRamBuffer;
        private final IndexWriterConfig indexWriterConfig;

        KDTreeSegmentBuilder(IndexComponents.ForWrite components, long rowIdOffset, NamedMemoryLimiter limiter, IndexWriterConfig indexWriterConfig)
        {
            super(components, rowIdOffset, limiter);

            int typeSize = TypeUtil.fixedSizeOf(termComparator);
            this.kdTreeRamBuffer = new BKDTreeRamBuffer(1, typeSize);
            this.buffer = new byte[typeSize];
            this.indexWriterConfig = indexWriterConfig;
            totalBytesAllocated = kdTreeRamBuffer.ramBytesUsed();
            totalBytesAllocatedConcurrent.add(totalBytesAllocated);}

        public boolean isEmpty()
        {
            return kdTreeRamBuffer.numRows() == 0;
        }

        @Override
        protected long addInternal(List<ByteBuffer> terms, int segmentRowId)
        {
            assert terms.size() == 1;
            TypeUtil.toComparableBytes(terms.get(0), termComparator, buffer);
            return kdTreeRamBuffer.addPackedValue(segmentRowId, new BytesRef(buffer));
        }

        @Override
        protected void flushInternal(SegmentMetadataBuilder metadataBuilder) throws IOException
        {
            try (NumericIndexWriter writer = new NumericIndexWriter(components,
                                                                    TypeUtil.fixedSizeOf(termComparator),
                                                                    maxSegmentRowId,
                                                                    kdTreeRamBuffer.numPoints(),
                                                                    indexWriterConfig))
            {

                MutableOneDimPointValues values = kdTreeRamBuffer.asPointValues();
                var metadataMap = writer.writeAll(metadataBuilder.intercept(values));
                metadataBuilder.setComponentsMetadata(metadataMap);
            }
        }

        @Override
        public boolean requiresFlush()
        {
            return kdTreeRamBuffer.requiresFlush();
        }
    }

    public static class RAMStringSegmentBuilder extends SegmentBuilder
    {
        final RAMStringIndexer ramIndexer;
        private final ByteComparable.Version byteComparableVersion;

        RAMStringSegmentBuilder(IndexComponents.ForWrite components, long rowIdOffset, NamedMemoryLimiter limiter)
        {
            super(components, rowIdOffset, limiter);
            this.byteComparableVersion = components.byteComparableVersionFor(IndexComponentType.TERMS_DATA);
            ramIndexer = new RAMStringIndexer(writeFrequencies());
            totalBytesAllocated = ramIndexer.estimatedBytesUsed();
            totalBytesAllocatedConcurrent.add(totalBytesAllocated);
        }

        private boolean writeFrequencies()
        {
            return !(analyzer instanceof NoOpAnalyzer) && components.version().onOrAfter(Version.BM25_EARLIEST);
        }

        public boolean isEmpty()
        {
            return ramIndexer.isEmpty();
        }

        @Override
        protected long addInternal(List<ByteBuffer> terms, int segmentRowId)
        {
            var bytesRefs = terms.stream()
                                 .map(term -> components.onDiskFormat().encodeForTrie(term, termComparator))
                                 .map(encodedTerm -> ByteSourceInverse.readBytes(encodedTerm.asComparableBytes(byteComparableVersion)))
                                 .map(BytesRef::new)
                                 .collect(Collectors.toList());
            // ramIndexer is responsible for merging duplicate (term, row) pairs
            return ramIndexer.addAll(bytesRefs, segmentRowId);
        }

        @Override
        protected void flushInternal(SegmentMetadataBuilder metadataBuilder) throws IOException
        {
            try (InvertedIndexWriter writer = new InvertedIndexWriter(components, writeFrequencies()))
            {
                TermsIterator termsWithPostings = ramIndexer.getTermsWithPostings(minTerm, maxTerm, byteComparableVersion);
                var docLengths = ramIndexer.getDocLengths();
                var metadataMap = writer.writeAll(metadataBuilder.intercept(termsWithPostings), docLengths);
                metadataBuilder.setComponentsMetadata(metadataMap);
            }
        }

        @Override
        public boolean requiresFlush()
        {
            return ramIndexer.requiresFlush();
        }
    }

    public static class VectorOffHeapSegmentBuilder extends SegmentBuilder
    {
        private final CompactionGraph graphIndex;

        public VectorOffHeapSegmentBuilder(IndexComponents.ForWrite components,
                                           long rowIdOffset,
                                           long keyCount,
                                           VectorCompressor<?> compressor,
                                           boolean unitVectors,
                                           boolean allRowsHaveVectors,
                                           NamedMemoryLimiter limiter)
        {
            super(components, rowIdOffset, limiter);
            try
            {
                graphIndex = new CompactionGraph(components, compressor, unitVectors, keyCount, allRowsHaveVectors);
            }
            catch (IOException e)
            {
                throw new UncheckedIOException(e);
            }
            totalBytesAllocated = graphIndex.ramBytesUsed();
            totalBytesAllocatedConcurrent.add(totalBytesAllocated);
            insertFanout = JVectorVersionUtil.newInsertFanout();
            // The graph's PQ training takes a write lock that every in-flight addGraphNode task (on the
            // shared build pool) must not be holding out; drain the fan-out first or the pool deadlocks.
            // See CompactionGraph.quiesceInserts.
            graphIndex.onBeforeTraining(insertFanout::awaitCompletion);
            acquireBuildPermit();
        }

        @Override
        public boolean isEmpty()
        {
            return graphIndex.isEmpty();
        }

        @Override
        protected long addInternal(List<ByteBuffer> terms, int segmentRowId)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        protected long addInternalAsync(List<ByteBuffer> terms, int segmentRowId, PrimaryKey key)
        {
            assert terms.size() == 1;

            // CompactionGraph splits adding a node into two parts:
            // (1) maybeAddVector, which must be done serially because it writes to disk incrementally
            // (2) addGraphNode, which may be done asynchronously
            int weight = terms.get(0).remaining();
            CompactionGraph.InsertionResult result;
            try
            {
                result = graphIndex.maybeAddVector(terms.get(0), segmentRowId);
            }
            catch (IOException e)
            {
                throw new UncheckedIOException(e);
            }
            if (result.vector == null)
                return result.bytesUsed;

            // Dispatch the graph insertion via the fan-out harness (bounded or unbounded admission).
            insertFanout.submit(weight, () -> {
                long bytesAdded = result.bytesUsed + graphIndex.addGraphNode(result);
                totalBytesAllocatedConcurrent.add(bytesAdded);
                termSizeReservoir.update(bytesAdded);
            });
            Throwable async = insertFanout.error();
            if (async != null)
                throw new RuntimeException("Error adding term asynchronously", async);
            return termSizeReservoir.size() == 0 ? weight : (long) termSizeReservoir.getMean();
        }

        @Override
        protected void flushInternal(SegmentMetadataBuilder metadataBuilder) throws IOException
        {
            if (graphIndex.isEmpty())
                return;
            var componentsMetadata = graphIndex.flush();
            logger.info("VectorOffHeapSegmentBuilder: legacy off-heap graph build+flush {} rows for {}",
                        getRowCount(), components.descriptor());
            metadataBuilder.setComponentsMetadata(componentsMetadata);
        }

        @Override
        public boolean supportsAsyncAdd()
        {
            return true;
        }

        @Override
        public boolean requiresFlush()
        {
            return graphIndex.requiresFlush();
        }

        @Override
        long release(IndexContext indexContext)
        {
            try
            {
                graphIndex.close();
            }
            catch (IOException e)
            {
                throw new UncheckedIOException(e);
            }
            return super.release(indexContext);
        }
    }

    /**
     * Segment builder for the jvector on-disk graph merge path. Instead of rebuilding the graph
     * from individual vectors, it records each incoming vector and its output row ID into a
     * ChronicleMap during the row-by-row pass, then delegates the actual graph construction to
     * {@link org.apache.cassandra.index.sai.disk.vector.CompactionGraphMerger} at flush time.
     *
     * <p>This path is active when all input SSTables already have graph indexes and at least
     * two source segments are available. The merger writes one output segment per compaction.
     */
    public static class VectorMergeSegmentBuilder extends SegmentBuilder
    {
        private final List<org.apache.cassandra.index.sai.disk.vector.CompactionGraphMerger.SourceSegment> sourceSegments;
        private final org.apache.cassandra.index.sai.disk.vector.CompactionGraphMerger merger;
        /**
         * Output postings keyed by the GLOBAL ORDINAL HANDLE of the source graph node that owns the
         * row's winning vector cell.
         */
        private final net.openhft.chronicle.map.ChronicleMap<Long,
                org.apache.cassandra.index.sai.disk.vector.VectorPostings.CompactionVectorPostings> postingsMap;
        private final org.apache.cassandra.io.util.File postingsMapFile;
        // Atomic so the running max is correct when per-row ingest is dispatched across the shared build pool.
        private final java.util.concurrent.atomic.AtomicInteger maxSegmentRowIdAtomic = new java.util.concurrent.atomic.AtomicInteger(-1);
        // Non-null only when ingest_parallel is on.
        private final InsertFanout mergeInsertFanout;

        /** Per-source-segment global ordinal base. */
        private final long[] ordinalBase;
        /** Per-source-segment id bound, sizing the surviving bitsets. */
        private final int[] idUpperBound;
        /**
         * Per-source-segment surviving-ordinal bitsets, built incrementally during ingest.
         */
        private final java.util.BitSet[] surviving;
        /** Source sstable id → indexes into {@code sourceSegments}, ascending by rowid offset. */
        private final java.util.Map<org.apache.cassandra.io.sstable.SSTableId<?>, int[]> sourceGroups;
        /** Row-source tag ring registered by SSTableIndexWriter; null on the direct-construction test path. */
        private final org.apache.cassandra.index.sai.disk.vector.VectorSourceTagRing tagRing;
        private final org.apache.cassandra.schema.ColumnMetadata column;

        /** Long task spanning the row-attribution and ChronicleMap population window. */
        private ProgressTracker.PhaseScope ingestPhase = ProgressTracker.PhaseScope.NOOP;

        // Thread-confined resolution resources (all resolution runs on the compaction thread), closed at release().
        private final java.util.Map<org.apache.cassandra.io.sstable.SSTableId<?>, org.apache.cassandra.index.sai.disk.PrimaryKeyMap> pkMaps = new java.util.HashMap<>();
        private final java.util.Map<Integer, org.apache.cassandra.index.sai.disk.vector.OrdinalsView> ordinalViews = new java.util.HashMap<>();

        /** Rows whose winning source ordinal could not be resolved. Any > 0 fails the flush loudly. */
        private long anomalies;
        private String firstAnomaly;
        /** Rows that carried an indexable vector, i.e. reached resolution. */
        private final java.util.concurrent.atomic.AtomicLong rowsWithVector = new java.util.concurrent.atomic.AtomicLong();
        /** Rows skipped before resolution because the vector cell was null or empty. */
        private final java.util.concurrent.atomic.AtomicLong rowsWithoutVector = new java.util.concurrent.atomic.AtomicLong();

        @SuppressWarnings("unchecked")
        public VectorMergeSegmentBuilder(
                IndexComponents.ForWrite components,
                long rowIdOffset,
                long estimatedRows,
                List<org.apache.cassandra.index.sai.disk.vector.CompactionGraphMerger.SourceSegment> sourceSegments,
                NamedMemoryLimiter limiter) throws java.io.IOException
        {
            super(components, rowIdOffset, limiter);
            this.sourceSegments = sourceSegments;
            this.column = components.context().getDefinition();

            this.merger = new org.apache.cassandra.index.sai.disk.vector.CompactionGraphMerger(
                    sourceSegments,
                    components.context().getIndexWriterConfig().getSimilarityFunction(),
                    components);

            // Global ordinal bases + surviving bitsets, and the sstable → segments routing table.
            this.ordinalBase = new long[sourceSegments.size()];
            this.idUpperBound = new int[sourceSegments.size()];
            this.surviving = new java.util.BitSet[sourceSegments.size()];
            var groups = new java.util.HashMap<org.apache.cassandra.io.sstable.SSTableId<?>, java.util.List<Integer>>();
            long base = 0;
            for (int i = 0; i < sourceSegments.size(); i++)
            {
                var src = sourceSegments.get(i);
                ordinalBase[i] = base;
                idUpperBound[i] = src.graph().getIdUpperBound();
                base += idUpperBound[i];
                surviving[i] = new java.util.BitSet(idUpperBound[i]);
                if (src.sstableIndex() != null)
                    groups.computeIfAbsent(src.sstableIndex().getSSTable().descriptor.id, k -> new java.util.ArrayList<>()).add(i);
            }
            this.sourceGroups = new java.util.HashMap<>();
            for (var e : groups.entrySet())
            {
                int[] idxs = e.getValue().stream()
                        .sorted(java.util.Comparator.comparingLong(i -> sourceSegments.get((int) i).segmentRowIdOffset()))
                        .mapToInt(Integer::intValue).toArray();
                sourceGroups.put(e.getKey(), idxs);
            }

            // Re-acquire the tag ring (idempotent, same thread).
            this.tagRing = org.apache.cassandra.db.compaction.CompactionRowSourceTagging.current()
                           instanceof org.apache.cassandra.index.sai.disk.vector.VectorSourceTagRing ring ? ring : null;

            postingsMapFile = components.tmpFileFor("merge_postings_chronicle_map");
            int entries = (int) Math.min(Math.max(1000L, (long) (1.1 * estimatedRows)), (long) Integer.MAX_VALUE - 100_000);
            try (ProgressTracker.PhaseScope ignored = VectorCompactionMetrics.INSTANCE.start(
                    VectorCompactionMetrics.Phase.POSTINGS_MAP_SETUP, components.context()))
            {
                postingsMap = net.openhft.chronicle.map.ChronicleMapBuilder
                              .of(Long.class,
                                  (Class<org.apache.cassandra.index.sai.disk.vector.VectorPostings.CompactionVectorPostings>) (Class<?>) org.apache.cassandra.index.sai.disk.vector.VectorPostings.CompactionVectorPostings.class)
                              .averageValueSize(org.apache.cassandra.index.sai.disk.vector.VectorPostings.emptyBytesUsed()
                                               + io.github.jbellis.jvector.util.RamUsageEstimator.NUM_BYTES_OBJECT_REF
                                               + 2 * Integer.BYTES)
                              .valueMarshaller(new org.apache.cassandra.index.sai.disk.vector.VectorPostings.Marshaller())
                              .entries(entries)
                              .createPersistedTo(postingsMapFile.toJavaIOFile());
            }

            totalBytesAllocated = 0;
            totalBytesAllocatedConcurrent.add(0);
            this.mergeInsertFanout = JVectorVersionUtil.isIngestParallel() ? JVectorVersionUtil.newInsertFanout() : null;
            acquireBuildPermit();
            ingestPhase = VectorCompactionMetrics.INSTANCE.start(VectorCompactionMetrics.Phase.ROW_INGEST,
                                                                 components.context());
        }

        @Override
        public boolean isEmpty()
        {
            // NOT postingsMap.isEmpty(): under parallel ingest the postings land asynchronously;
            // the per-add counter is incremented synchronously and cannot race.
            return rowsWithVector.get() == 0;
        }

        @Override
        protected long addInternal(List<ByteBuffer> terms, int segmentRowId)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        protected long addInternalAsync(List<ByteBuffer> terms, int segmentRowId, PrimaryKey key)
        {
            assert terms.size() == 1;
            var vectorBuf = terms.get(0);
            if (vectorBuf == null || vectorBuf.remaining() == 0)
            {
                rowsWithoutVector.incrementAndGet();
                return 0;
            }
            rowsWithVector.incrementAndGet();

            maxSegmentRowIdAtomic.accumulateAndGet(segmentRowId, Math::max);

            // Resolution runs SYNCHRONOUSLY on the compaction thread.
            long handle = resolveHandle(key);
            if (handle < 0)
                return 0; // anomaly recorded; flush will fail loudly

            if (mergeInsertFanout != null)
            {
                mergeInsertFanout.submit(Long.BYTES + Integer.BYTES, () -> putPostings(handle, segmentRowId));
                Throwable async = mergeInsertFanout.error();
                if (async != null)
                    throw new RuntimeException("Error populating merge postings asynchronously", async);
            }
            else
            {
                putPostings(handle, segmentRowId);
            }
            return 0;
        }

        /**
         * Join this output row to the source graph node whose cell won its merge.
         */
        private long resolveHandle(PrimaryKey key)
        {
            if (tagRing == null)
                return recordAnomaly("no row-source tag ring registered (non-compaction path?)", key);
            org.apache.cassandra.io.sstable.SSTableId<?> winner =
                    tagRing.consume(key.partitionKey(), key.clustering(), column);
            if (winner == null)
                return recordAnomaly("no source tag for row (ring wrap or untagged merge)", key);
            int[] group = sourceGroups.get(winner);
            if (group == null)
                return recordAnomaly("winning sstable " + winner + " has no merge source segments", key);

            long rowId;
            try
            {
                var pkMap = pkMaps.get(winner);
                if (pkMap == null)
                {
                    int segIdx = group[0];
                    pkMap = sourceSegments.get(segIdx).sstableIndex().getSSTableContext()
                            .primaryKeyMapFactory.newPerSSTablePrimaryKeyMap();
                    pkMaps.put(winner, pkMap);
                }
                rowId = pkMap.exactRowIdOrInvertedCeiling(key);
            }
            catch (Exception e)
            {
                throw new RuntimeException("Failed to resolve source rowid for merge", e);
            }
            if (rowId < 0)
                return recordAnomaly("primary key not found in winning sstable " + winner, key);

            // Route to the segment whose rowid range contains the row.
            int segIdx = -1;
            for (int i = group.length - 1; i >= 0; i--)
            {
                if (sourceSegments.get(group[i]).segmentRowIdOffset() <= rowId)
                {
                    segIdx = group[i];
                    break;
                }
            }
            if (segIdx < 0)
                return recordAnomaly("source rowid " + rowId + " below all segment offsets of " + winner, key);

            int ordinal;
            try
            {
                var view = ordinalViews.get(segIdx);
                if (view == null)
                {
                    view = sourceSegments.get(segIdx).diskAnn().getOrdinalsView();
                    ordinalViews.put(segIdx, view);
                }
                ordinal = view.getOrdinalForRowId(
                        Math.toIntExact(rowId - sourceSegments.get(segIdx).segmentRowIdOffset()));
            }
            catch (Exception e)
            {
                throw new RuntimeException("Failed to resolve source ordinal for merge", e);
            }
            if (ordinal < 0 || ordinal >= idUpperBound[segIdx])
                return recordAnomaly("no ordinal for source rowid " + rowId + " in segment " + segIdx + " of " + winner, key);

            surviving[segIdx].set(ordinal);
            return ordinalBase[segIdx] + ordinal;
        }

        private long recordAnomaly(String what, PrimaryKey key)
        {
            anomalies++;
            if (firstAnomaly == null)
                firstAnomaly = what + " [key=" + key + ']';
            return -1;
        }

        /** Test hook: ingest a pre-resolved (global handle, output rowid) pair, bypassing tag/PK resolution. */
        @com.google.common.annotations.VisibleForTesting
        public void addResolved(int sourceSegmentIdx, int sourceOrdinal, int segmentRowId)
        {
            surviving[sourceSegmentIdx].set(sourceOrdinal);
            maxSegmentRowIdAtomic.accumulateAndGet(segmentRowId, Math::max);
            putPostings(ordinalBase[sourceSegmentIdx] + sourceOrdinal, segmentRowId);
        }

        private void putPostings(long handle, int segmentRowId)
        {
            try (var ctx = postingsMap.queryContext(handle))
            {
                //noinspection LockAcquiredButNotSafelyReleased
                ctx.writeLock().lock();
                var absent = ctx.absentEntry();
                if (absent != null)
                {
                    var cvp = new org.apache.cassandra.index.sai.disk.vector.VectorPostings.CompactionVectorPostings(0, segmentRowId);
                    absent.doInsert(ctx.wrapValueAsData(cvp));
                }
                else
                {
                    var entry = ctx.entry();
                    assert entry != null;
                    var cvp = entry.value().get();
                    cvp.add(segmentRowId);
                    entry.doReplaceValue(ctx.wrapValueAsData(cvp));
                }
            }
        }

        @Override
        protected void flushInternal(SegmentMetadataBuilder metadataBuilder) throws java.io.IOException
        {
            finishIngestPhase();

            // Drain any parallel per-row ingest so the postings map is complete before it is read/merged.
            if (mergeInsertFanout != null)
            {
                try (ProgressTracker.PhaseScope ignored = VectorCompactionMetrics.INSTANCE.start(
                        VectorCompactionMetrics.Phase.INGEST_DRAIN, components.context()))
                {
                    mergeInsertFanout.awaitCompletion();
                    Throwable async = mergeInsertFanout.error();
                    if (async != null)
                        throw new RuntimeException("Error populating merge postings asynchronously", async);
                }
            }

            if (anomalies > 0)
                throw new IllegalStateException(String.format(
                        "Vector graph merge: %d row(s) could not be joined to a source graph ordinal (first: %s). " +
                        "Set -D%s=false to fall back to the graph-rebuild path.",
                        anomalies, firstAnomaly,
                        org.apache.cassandra.config.CassandraRelevantProperties.SAI_VECTOR_GRAPH_COMPACTION_MERGE_ENABLED.getKey()));

            if (postingsMap.isEmpty())
            {
                long withVector = rowsWithVector.get();
                if (withVector > 0)
                    throw org.apache.cassandra.index.sai.disk.vector.VectorIndexIntegrity.abort(String.format(
                            "vector graph merge for %s would produce an EMPTY index: %d row(s) resolved to a "
                            + "source graph ordinal but no postings were recorded (%d row(s) had no vector)",
                            components.descriptor(), withVector, rowsWithoutVector.get()));

                logger.info("{} vector graph merge indexed no rows ({} row(s) carried no vector); " +
                            "writing an empty index for this segment",
                            components.logMessage(""), rowsWithoutVector.get());
                return;
            }

            long total = Math.max(1L, postingsMap.size());
            org.apache.cassandra.index.sai.disk.vector.VectorMergeOperation op =
                    new org.apache.cassandra.index.sai.disk.vector.VectorMergeOperation(
                            components.context().columnFamilyStore().metadata(),
                            org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID(),
                            java.util.Collections.emptyList(),
                            total);

            long memoryEstimate = total * JVectorVersionUtil.getMergeBytesPerOrdinal();
            org.apache.cassandra.index.sai.utils.NamedMemoryLimiter memLimiter =
                    org.apache.cassandra.index.sai.disk.v1.V1OnDiskFormat.SEGMENT_BUILD_MEMORY_LIMITER;
            memLimiter.increment(memoryEstimate);
            logger.debug("Vector graph merge measurement: charged {} bytes to SAI segment-build limiter ({} of {} bytes used)",
                         memoryEstimate, memLimiter.currentBytesUsed(), memLimiter.limitBytes());
            if (memLimiter.usageExceedsLimit())
                logger.warn("Vector graph merge estimated {} bytes pushes SAI segment-build memory to {} over the {} byte limit; proceeding",
                            memoryEstimate, memLimiter.currentBytesUsed(), memLimiter.limitBytes());

            // Pin every source SAI index for the merge's full lifetime.
            List<org.apache.cassandra.index.sai.SSTableIndex> pinnedSources = new ArrayList<>();
            try
            {
                for (var src : sourceSegments)
                {
                    org.apache.cassandra.index.sai.SSTableIndex idx = src.sstableIndex();
                    if (idx == null)
                        continue; // test path manages source lifetime directly
                    if (!idx.reference())
                        throw new IllegalStateException("Source SAI index is being released (DROP/teardown in progress); abandoning graph merge");
                    pinnedSources.add(idx);
                }

                try (org.apache.cassandra.utils.NonThrowingCloseable c =
                             org.apache.cassandra.db.compaction.CompactionManager.instance.active.onOperationStart(op);
                     ProgressTracker.PhaseScope ignored = VectorCompactionMetrics.INSTANCE.start(
                             VectorCompactionMetrics.Phase.TOTAL_MERGE, components.context()))
                {
                    var componentsMetadata = merger.merge(postingsMap, maxSegmentRowIdAtomic.get(),
                            new org.apache.cassandra.index.sai.disk.vector.CompactionProgressLimiter(op, components.context()),
                            ordinalBase, surviving);
                    op.setCompleted(total);
                    metadataBuilder.setComponentsMetadata(componentsMetadata);
                }
            }
            finally
            {
                for (org.apache.cassandra.index.sai.SSTableIndex idx : pinnedSources)
                {
                    try { idx.release(); }
                    catch (Throwable t) { logger.warn("Error releasing pinned source SAI index after graph merge", t); }
                }
                memLimiter.decrement(memoryEstimate);
                logger.debug("Vector graph merge measurement: released {} bytes from SAI segment-build limiter ({} bytes used)",
                             memoryEstimate, memLimiter.currentBytesUsed());
            }
        }

        @Override
        public boolean supportsAsyncAdd()
        {
            return true;
        }

        @Override
        public boolean requiresFlush()
        {
            return false;
        }

        @Override
        long release(IndexContext indexContext)
        {
            finishIngestPhase();
            try (ProgressTracker.PhaseScope ignored = VectorCompactionMetrics.INSTANCE.start(
                    VectorCompactionMetrics.Phase.CLEANUP, components.context()))
            {
                for (var pkMap : pkMaps.values())
                {
                    try { pkMap.close(); }
                    catch (Exception e) { logger.warn("Error closing source primary-key map after graph merge", e); }
                }
                for (var view : ordinalViews.values())
                {
                    try { view.close(); }
                    catch (Exception e) { logger.warn("Error closing source ordinals view after graph merge", e); }
                }
                try
                {
                    postingsMap.close();
                    org.apache.cassandra.io.util.FileUtils.deleteWithConfirm(postingsMapFile);
                }
                catch (Exception e)
                {
                    throw new java.io.UncheckedIOException(new java.io.IOException("Error closing merge postings map", e));
                }
            }
            return super.release(indexContext);
        }

        private void finishIngestPhase()
        {
            ProgressTracker.PhaseScope phase = ingestPhase;
            ingestPhase = ProgressTracker.PhaseScope.NOOP;
            phase.close();
        }
    }

    public static class VectorOnHeapSegmentBuilder extends SegmentBuilder
    {
        private final CassandraOnHeapGraph<Integer> graphIndex;

        public VectorOnHeapSegmentBuilder(IndexComponents.ForWrite components, long rowIdOffset, long keyCount, NamedMemoryLimiter limiter)
        {
            super(components, rowIdOffset, limiter);
            graphIndex = new CassandraOnHeapGraph<>(components.context(), false, null);
            totalBytesAllocated = graphIndex.ramBytesUsed();
            totalBytesAllocatedConcurrent.add(totalBytesAllocated);
            insertFanout = JVectorVersionUtil.newInsertFanout();
            acquireBuildPermit();
        }

        @Override
        public boolean isEmpty()
        {
            return graphIndex.isEmpty();
        }

        @Override
        protected long addInternal(List<ByteBuffer> terms, int segmentRowId)
        {
            assert terms.size() == 1;
            return graphIndex.add(terms.get(0), segmentRowId);
        }

        @Override
        protected long addInternalAsync(List<ByteBuffer> terms, int segmentRowId, PrimaryKey key)
        {
            int weight = terms.get(0).remaining();
            insertFanout.submit(weight, () -> {
                long bytesAdded = addInternal(terms, segmentRowId);
                totalBytesAllocatedConcurrent.add(bytesAdded);
                termSizeReservoir.update(bytesAdded);
            });
            Throwable async = insertFanout.error();
            if (async != null)
                throw new RuntimeException("Error adding term asynchronously", async);
            return termSizeReservoir.size() == 0 ? weight : (long) termSizeReservoir.getMean();
        }

        @Override
        protected void flushInternal(SegmentMetadataBuilder metadataBuilder) throws IOException
        {
            var shouldFlush = graphIndex.preFlush(p -> p);
            // there are no deletes to worry about when building the index during compaction,
            // and SegmentBuilder::flush checks for the empty index case before calling flushInternal
            assert shouldFlush;
            var componentsMetadata = graphIndex.flush(components);
            logger.info("VectorOnHeapSegmentBuilder: legacy on-heap graph build+flush {} rows for {}",
                        getRowCount(), components.descriptor());
            metadataBuilder.setComponentsMetadata(componentsMetadata);
        }

        @Override
        public boolean supportsAsyncAdd()
        {
            return true;
        }
    }

    private SegmentBuilder(IndexComponents.ForWrite components, long rowIdOffset, NamedMemoryLimiter limiter)
    {
        IndexContext context = Objects.requireNonNull(components.context(), "IndexContext must be set on segment builder");
        this.components = components;
        this.termComparator = context.getValidator();
        this.analyzer = context.getAnalyzerFactory().create();
        this.limiter = limiter;
        this.segmentRowIdOffset = rowIdOffset;
        this.lastValidSegmentRowID = testLastValidSegmentRowId >= 0 ? testLastValidSegmentRowId : LAST_VALID_SEGMENT_ROW_ID;

        minimumFlushBytes = limiter.limitBytes() / ACTIVE_BUILDER_COUNT.getAndIncrement();
    }

    /**
     * Acquire the node-wide concurrent-build admission permit for this (vector) build, parking until one is
     * free. Called at the end of a vector subclass constructor; the permit is released exactly once in
     * {@link #release(IndexContext)}. A no-op when the bound is disabled ({@code concurrent_builds <= 0}).
     */
    protected void acquireBuildPermit()
    {
        this.buildPermit = JVectorVersionUtil.acquireBuildPermit();
    }

    public SegmentMetadata flush() throws IOException
    {
        assert !flushed;
        flushed = true;

        if (getRowCount() == 0)
        {
            logger.warn(components.logMessage("No rows to index during flush of SSTable {}."), components.descriptor());
            return null;
        }

        SegmentMetadataBuilder metadataBuilder = new SegmentMetadataBuilder(segmentRowIdOffset, components);
        metadataBuilder.setKeyRange(minKey, maxKey);
        metadataBuilder.setRowIdRange(minSSTableRowId, maxSSTableRowId);
        metadataBuilder.setTermRange(minTerm, maxTerm);
        metadataBuilder.setNumRows(getRowCount());
        metadataBuilder.setTotalTermCount(totalTermCount);

        flushInternal(metadataBuilder);
        return metadataBuilder.build();
    }

    public long analyzeAndAdd(ByteBuffer rawTerm,
                              AbstractType<?> type,
                              PrimaryKey key,
                              long sstableRowId,
                              @Nullable IndexMetrics indexMetrics)
    {
        long totalSize = 0;
        if (TypeUtil.isLiteral(type))
        {
            var terms = ByteLimitedMaterializer.materializeTokens(analyzer, rawTerm, components.context(), key);
            totalSize += add(terms, key, sstableRowId);
            totalTermCount += terms.size();
            if (indexMetrics != null)
                indexMetrics.compactionTermsProcessedCount.inc(terms.size());
        }
        else
        {
            totalSize += add(List.of(rawTerm), key, sstableRowId);
            totalTermCount++;
            if (indexMetrics != null)
                indexMetrics.compactionTermsProcessedCount.inc();
        }
        return totalSize;
    }

    private long add(List<ByteBuffer> terms, PrimaryKey key, long sstableRowId)
    {
        if (terms.isEmpty())
            return 0;

        Preconditions.checkState(!flushed, "Cannot add to flushed segment");
        Preconditions.checkArgument(sstableRowId >= maxSSTableRowId,
                                    "rowId must be greater than or equal to the last rowId added: %s < %s", sstableRowId, maxSSTableRowId);
        Preconditions.checkArgument(maxKey == null || key.compareTo(maxKey) >= 0,
                                    "Key must be greater than or equal to the last key added: %s < %s", key, maxKey);

        minSSTableRowId = minSSTableRowId < 0 ? sstableRowId : minSSTableRowId;
        maxSSTableRowId = sstableRowId;

        minKey = minKey == null ? key : minKey;
        maxKey = key;

        // Update term boundaries for all terms in this row
        for (ByteBuffer term : terms)
        {
            assert term != null : "term must not be null";
            minTerm = TypeUtil.min(term, minTerm, termComparator, components.version());
            maxTerm = TypeUtil.max(term, maxTerm, termComparator, components.version());
        }

        assert minTerm != null : "minTerm should not be null at this point";
        assert maxTerm != null : "maxTerm should not be null at this point";

        // segmentRowIdOffset should encode sstableRowId into Integer
        int segmentRowId = Math.toIntExact(sstableRowId - segmentRowIdOffset);

        if (segmentRowId == PostingList.END_OF_STREAM)
            throw new IllegalArgumentException("Illegal segment row id: END_OF_STREAM found");

        maxSegmentRowId = Math.max(maxSegmentRowId, segmentRowId);

        long bytesAllocated;
        if (supportsAsyncAdd())
        {
            // only vector indexing is done async and there can only be one term
            assert terms.size() == 1;
            bytesAllocated = addInternalAsync(terms, segmentRowId, key);
        }
        else
        {
            bytesAllocated = addInternal(terms, segmentRowId);
        }

        totalBytesAllocated += bytesAllocated;
        return bytesAllocated;
    }

    /**
     * Async add. {@code key} is the row's primary key — the vector merge builder joins it,
     * via the compaction row-source tags, to the source graph ordinal; other builders ignore it.
     */
    protected long addInternalAsync(List<ByteBuffer> terms, int segmentRowId, PrimaryKey key)
    {
        throw new UnsupportedOperationException();
    }

    public boolean supportsAsyncAdd() {
        return false;
    }

    public Throwable getAsyncThrowable()
    {
        return insertFanout == null ? null : insertFanout.error();
    }

    public void awaitAsyncAdditions()
    {
        // addTerm is only called by the compaction thread, serially, so no new inserts are dispatched
        // while we drain. Parks (no spin) until every dispatched insert has completed.
        if (insertFanout != null)
            insertFanout.awaitCompletion();
    }

    long totalBytesAllocated()
    {
        return totalBytesAllocated;
    }

    boolean hasReachedMinimumFlushSize()
    {
        return totalBytesAllocated >= minimumFlushBytes;
    }

    long getMinimumFlushBytes()
    {
        return minimumFlushBytes;
    }

    /**
     * This method does three things:
     *
     * 1.) It decrements active builder count and updates the global minimum flush size to reflect that.
     * 2.) It releases the builder's memory against its limiter.
     * 3.) It defensively marks the builder inactive to make sure nothing bad happens if we try to close it twice.
     *
     * @param indexContext
     *
     * @return the number of bytes currently used by the memory limiter
     */
    long release(IndexContext indexContext)
    {
        if (active)
        {
            minimumFlushBytes = limiter.limitBytes() / ACTIVE_BUILDER_COUNT.decrementAndGet();
            long used = limiter.decrement(totalBytesAllocated);
            active = false;
            if (buildPermit != null)
            {
                buildPermit.close();
                buildPermit = null;
            }
            return used;
        }

        logger.warn(indexContext.logMessage("Attempted to release storage attached index segment builder memory after builder marked inactive."));
        return limiter.currentBytesUsed();
    }

    public abstract boolean isEmpty();

    protected abstract long addInternal(List<ByteBuffer> terms, int segmentRowId);

    protected abstract void flushInternal(SegmentMetadataBuilder metadataBuilder) throws IOException;

    int getRowCount()
    {
        return rowCount;
    }

    void incRowCount()
    {
        rowCount++;
    }

    /**
     * @return true if next SSTable row ID exceeds max segment row ID
     */
    boolean exceedsSegmentLimit(long ssTableRowId)
    {
        if (getRowCount() == 0)
            return false;

        // To handle the case where there are many non-indexable rows. eg. rowId-1 and rowId-3B are indexable,
        // the rest are non-indexable. We should flush them as 2 separate segments, because rowId-3B is going
        // to cause error in on-disk index structure with 2B limitation.
        return ssTableRowId - segmentRowIdOffset > lastValidSegmentRowID;
    }

    @VisibleForTesting
    public static long updateLastValidSegmentRowId(long lastValidSegmentRowID)
    {
        long current = testLastValidSegmentRowId;
        testLastValidSegmentRowId = lastValidSegmentRowID;
        return current;
    }
}
