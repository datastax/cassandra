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
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.jbellis.jvector.quantization.VectorCompressor;
import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;
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
import org.apache.cassandra.index.sai.utils.NamedMemoryLimiter;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;
import org.apache.lucene.util.BytesRef;

import static org.apache.cassandra.utils.FBUtilities.busyWaitWhile;

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

    /** for parallelism within a single compaction */
    public static final ExecutorService compactionExecutor = new DebuggableThreadPoolExecutor(Runtime.getRuntime().availableProcessors(),
                                                                                              1,
                                                                                              TimeUnit.MINUTES,
                                                                                              new ArrayBlockingQueue<>(10 * Runtime.getRuntime().availableProcessors()),
                                                                                              new NamedThreadFactory("SegmentBuilder", Thread.MIN_PRIORITY));

    // Served as safe net in case memory limit is not triggered or when merger merges small segments..
    public static final long LAST_VALID_SEGMENT_ROW_ID = ((long)Integer.MAX_VALUE / 2) - 1L;
    private static long testLastValidSegmentRowId = Long.parseLong(System.getProperty("cassandra.sai.test_last_valid_segments", "-1"));

    /** The number of column indexes being built globally. (Starts at one to avoid divide by zero.) */
    public static final AtomicLong ACTIVE_BUILDER_COUNT = new AtomicLong(1);

    /** Minimum flush size, dynamically updated as segment builds are started and completed/aborted. */
    private static volatile long minimumFlushBytes;

    protected final IndexComponents.ForWrite components;

    final AbstractType<?> termComparator;
    final AbstractAnalyzer analyzer;

    // track memory usage for this segment so we can flush when it gets too big
    protected final NamedMemoryLimiter limiter;
    long totalBytesAllocated;

    private final long lastValidSegmentRowID;

    private boolean flushed = false;
    private boolean active = true;

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

    protected final AtomicInteger updatesInFlight = new AtomicInteger(0);
    protected AtomicReference<Throwable> asyncThrowable = new AtomicReference<>();


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
        }

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
        }

        private boolean writeFrequencies()
        {
            return !(analyzer instanceof NoOpAnalyzer) && Version.current().onOrAfter(Version.BM25_EARLIEST);
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
        protected final AtomicLong maxBytesAddedObserved = new AtomicLong(0);
        protected final LongAdder reconciliationBytes = new LongAdder();

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
        protected void addInternalAsync(List<ByteBuffer> terms, int segmentRowId)
        {
            assert terms.size() == 1;

            // CompactionGraph splits adding a node into two parts:
            // (1) maybeAddVector, which must be done serially because it writes to disk incrementally
            // (2) addGraphNode, which may be done asynchronously
            CompactionGraph.InsertionResult result;
            try
            {
                result = graphIndex.maybeAddVector(terms.get(0), segmentRowId);
                observeAllocatedBytes(result.bytesUsed);
            }
            catch (IOException e)
            {
                throw new UncheckedIOException(e);
            }
            if (result.vector == null)
                return;

            updatesInFlight.incrementAndGet();

            // Increment by double max bytes before dispatching the task to avoid a subsequent insertion from
            // exceeding the limit. Also add back any bytes that were reconciled previous calls that have since
            // completed. It is expected that the reconciliation process typically results in a net reduction
            // because we overestimate the number of bytes required per insertion, and as such, we allow for a lazy
            // reconciliation process.
            long estimatedBytes = maxBytesAddedObserved.get() * 2;
            long reconciledBytes = reconciliationBytes.sumThenReset();
            observeAllocatedBytes(estimatedBytes + reconciledBytes);

            compactionExecutor.submit(() -> {
                try
                {
                    long bytesAdded = graphIndex.addGraphNode(result);
                    maxBytesAddedObserved.accumulateAndGet(bytesAdded, Math::max);
                    // Store the difference between the estimated and actual bytes added for correction on the
                    // next call to addInternalAsync.
                    reconciliationBytes.add(bytesAdded - estimatedBytes);
                }
                catch (Throwable th)
                {
                    asyncThrowable.compareAndExchange(null, th);
                }
                finally
                {
                    updatesInFlight.decrementAndGet();
                }
            });

            if (asyncThrowable.get() != null) {
                throw new RuntimeException("Error adding term asynchronously", asyncThrowable.get());
            }
        }

        @Override
        protected void flushInternal(SegmentMetadataBuilder metadataBuilder) throws IOException
        {
            if (graphIndex.isEmpty())
                return;
            var componentsMetadata = graphIndex.flush();
            metadataBuilder.setComponentsMetadata(componentsMetadata);
        }

        @Override
        public boolean supportsAsyncAdd()
        {
            return true;
        }

        @Override
        public void reconcileAsyncByteAllocations()
        {
            observeAllocatedBytes(reconciliationBytes.sumThenReset());
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

    public static class VectorOnHeapSegmentBuilder extends SegmentBuilder
    {
        private final CassandraOnHeapGraph<Integer> graphIndex;
        protected final AtomicLong maxBytesAddedObserved = new AtomicLong(0);
        protected final LongAdder reconciliationBytes = new LongAdder();

        public VectorOnHeapSegmentBuilder(IndexComponents.ForWrite components, long rowIdOffset, long keyCount, NamedMemoryLimiter limiter)
        {
            super(components, rowIdOffset, limiter);
            graphIndex = new CassandraOnHeapGraph<>(components.context(), false, null);
            totalBytesAllocated = graphIndex.ramBytesUsed();
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
        protected void addInternalAsync(List<ByteBuffer> terms, int segmentRowId)
        {
            updatesInFlight.incrementAndGet();
            // See VectorOffHeapSegmentBuilder for comments on this logic
            long estimatedBytes = maxBytesAddedObserved.get() * 2;
            long reconciledBytes = reconciliationBytes.sumThenReset();
            observeAllocatedBytes(estimatedBytes + reconciledBytes);
            compactionExecutor.submit(() -> {
                try
                {
                    long bytesAdded = addInternal(terms, segmentRowId);
                    maxBytesAddedObserved.accumulateAndGet(bytesAdded, Math::max);
                    // Store the difference between the estimated and actual bytes added for correction on the
                    // next call to addInternalAsync.
                    reconciliationBytes.add(bytesAdded - estimatedBytes);
                }
                catch (Throwable th)
                {
                    asyncThrowable.compareAndExchange(null, th);
                }
                finally
                {
                    updatesInFlight.decrementAndGet();
                }
            });
            if (asyncThrowable.get() != null) {
                throw new RuntimeException("Error adding term asynchronously", asyncThrowable.get());
            }
        }

        @Override
        protected void flushInternal(SegmentMetadataBuilder metadataBuilder) throws IOException
        {
            var shouldFlush = graphIndex.preFlush(p -> p);
            // there are no deletes to worry about when building the index during compaction,
            // and SegmentBuilder::flush checks for the empty index case before calling flushInternal
            assert shouldFlush;
            var componentsMetadata = graphIndex.flush(components);
            metadataBuilder.setComponentsMetadata(componentsMetadata);
        }

        @Override
        public boolean supportsAsyncAdd()
        {
            return true;
        }

        @Override
        public void reconcileAsyncByteAllocations()
        {
            observeAllocatedBytes(reconciliationBytes.sumThenReset());
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

    protected void observeAllocatedBytes(long bytes)
    {
        if (bytes != 0)
        {
            totalBytesAllocated += bytes;
            limiter.increment(bytes);
        }
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

    public void analyzeAndAdd(ByteBuffer rawTerm, AbstractType<?> type, PrimaryKey key, long sstableRowId)
    {
        if (TypeUtil.isLiteral(type))
        {
            var terms = ByteLimitedMaterializer.materializeTokens(analyzer, rawTerm, components.context(), key);
            add(terms, key, sstableRowId);
            totalTermCount += terms.size();
        }
        else
        {
            add(List.of(rawTerm), key, sstableRowId);
            totalTermCount++;
        }
    }

    private void add(List<ByteBuffer> terms, PrimaryKey key, long sstableRowId)
    {
        if (terms.isEmpty())
            return;

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
            minTerm = TypeUtil.min(term, minTerm, termComparator, Version.current());
            maxTerm = TypeUtil.max(term, maxTerm, termComparator, Version.current());
        }

        assert minTerm != null : "minTerm should not be null at this point";
        assert maxTerm != null : "maxTerm should not be null at this point";

        // segmentRowIdOffset should encode sstableRowId into Integer
        int segmentRowId = Math.toIntExact(sstableRowId - segmentRowIdOffset);

        if (segmentRowId == PostingList.END_OF_STREAM)
            throw new IllegalArgumentException("Illegal segment row id: END_OF_STREAM found");

        maxSegmentRowId = Math.max(maxSegmentRowId, segmentRowId);

        if (supportsAsyncAdd())
        {
            // only vector indexing is done async and there can only be one term
            assert terms.size() == 1;
            addInternalAsync(terms, segmentRowId);
        }
        else
        {
            long bytesAllocated = addInternal(terms, segmentRowId);
            observeAllocatedBytes(bytesAllocated);
        }
    }

    protected void addInternalAsync(List<ByteBuffer> terms, int segmentRowId)
    {
        throw new UnsupportedOperationException();
    }

    public boolean supportsAsyncAdd() {
        return false;
    }

    protected void reconcileAsyncByteAllocations()
    {
    }

    public Throwable getAsyncThrowable()
    {
        return asyncThrowable.get();
    }

    public void awaitAsyncAdditions()
    {
        // addTerm is only called by the compaction thread, serially, so we don't need to worry about new
        // terms being added while we're waiting -- updatesInFlight can only decrease
        busyWaitWhile(() -> updatesInFlight.get() > 0, 60_000);
        reconcileAsyncByteAllocations();
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
