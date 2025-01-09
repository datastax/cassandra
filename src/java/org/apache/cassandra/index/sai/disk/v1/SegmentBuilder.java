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
import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.jbellis.jvector.quantization.VectorCompressor;
import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.analyzer.AbstractAnalyzer;
import org.apache.cassandra.index.sai.analyzer.ByteLimitedMaterializer;
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
import org.apache.cassandra.metrics.QuickSlidingWindowReservoir;
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
    private final NamedMemoryLimiter limiter;
    long totalBytesAllocated;
    // when we're adding terms asynchronously, totalBytesAllocated will be an approximation and this tracks the exact size
    final LongAdder totalBytesAllocatedConcurrent = new LongAdder();

    private final long lastValidSegmentRowID;

    private boolean flushed = false;
    private boolean active = true;

    // segment metadata
    private long minSSTableRowId = -1;
    private long maxSSTableRowId = -1;
    private long segmentRowIdOffset = 0;
    int rowCount = 0;
    int maxSegmentRowId = -1;
    // in token order
    private PrimaryKey minKey;
    private PrimaryKey maxKey;
    // in termComparator order
    protected ByteBuffer minTerm;
    protected ByteBuffer maxTerm;

    protected final AtomicInteger updatesInFlight = new AtomicInteger(0);
    protected final QuickSlidingWindowReservoir termSizeReservoir = new QuickSlidingWindowReservoir(100);
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
            totalBytesAllocatedConcurrent.add(totalBytesAllocated);}

        public boolean isEmpty()
        {
            return kdTreeRamBuffer.numRows() == 0;
        }

        protected long addInternal(ByteBuffer term, int segmentRowId)
        {
            TypeUtil.toComparableBytes(term, termComparator, buffer);
            return kdTreeRamBuffer.addPackedValue(segmentRowId, new BytesRef(buffer));
        }

        @Override
        protected void flushInternal(SegmentMetadataBuilder metadataBuilder) throws IOException
        {
            try (NumericIndexWriter writer = new NumericIndexWriter(components,
                                                                    TypeUtil.fixedSizeOf(termComparator),
                                                                    maxSegmentRowId,
                                                                    rowCount,
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
            ramIndexer = new RAMStringIndexer();
            totalBytesAllocated = ramIndexer.estimatedBytesUsed();
            totalBytesAllocatedConcurrent.add(totalBytesAllocated);

        }

        public boolean isEmpty()
        {
            return ramIndexer.isEmpty();
        }

        protected long addInternal(ByteBuffer term, int segmentRowId)
        {
            var encodedTerm = components.onDiskFormat().encodeForTrie(term, termComparator);
            var bytes = ByteSourceInverse.readBytes(encodedTerm.asComparableBytes(byteComparableVersion));
            var bytesRef = new BytesRef(bytes);
            return ramIndexer.add(bytesRef, segmentRowId);
        }

        @Override
        protected void flushInternal(SegmentMetadataBuilder metadataBuilder) throws IOException
        {
            try (InvertedIndexWriter writer = new InvertedIndexWriter(components))
            {
                TermsIterator termsWithPostings = ramIndexer.getTermsWithPostings(minTerm, maxTerm, byteComparableVersion);
                var metadataMap = writer.writeAll(metadataBuilder.intercept(termsWithPostings));
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
        }

        @Override
        public boolean isEmpty()
        {
            return graphIndex.isEmpty();
        }

        @Override
        protected long addInternal(ByteBuffer term, int segmentRowId)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        protected long addInternalAsync(ByteBuffer term, int segmentRowId)
        {
            // CompactionGraph splits adding a node into two parts:
            // (1) maybeAddVector, which must be done serially because it writes to disk incrementally
            // (2) addGraphNode, which may be done asynchronously
            CompactionGraph.InsertionResult result;
            try
            {
                result = graphIndex.maybeAddVector(term, segmentRowId);
            }
            catch (IOException e)
            {
                throw new UncheckedIOException(e);
            }
            if (result.vector == null)
                return result.bytesUsed;

            updatesInFlight.incrementAndGet();
            compactionExecutor.submit(() -> {
                try
                {
                    long bytesAdded = result.bytesUsed + graphIndex.addGraphNode(result);
                    totalBytesAllocatedConcurrent.add(bytesAdded);
                    termSizeReservoir.update(bytesAdded);
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
            // bytes allocated will be approximated immediately as the average of recently added terms,
            // rather than waiting until the async update completes to get the exact value.  The latter could
            // result in a dangerously large discrepancy between the amount of memory actually consumed
            // and the amount the limiter knows about if the queue depth grows.
            busyWaitWhile(() -> termSizeReservoir.size() == 0 && asyncThrowable.get() == null);
            if (asyncThrowable.get() != null) {
                throw new RuntimeException("Error adding term asynchronously", asyncThrowable.get());
            }
            return (long) termSizeReservoir.getMean();
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

        public VectorOnHeapSegmentBuilder(IndexComponents.ForWrite components, long rowIdOffset, long keyCount, NamedMemoryLimiter limiter)
        {
            super(components, rowIdOffset, limiter);
            graphIndex = new CassandraOnHeapGraph<>(components.context(), false, null);
            totalBytesAllocated = graphIndex.ramBytesUsed();
            totalBytesAllocatedConcurrent.add(totalBytesAllocated);
        }

        @Override
        public boolean isEmpty()
        {
            return graphIndex.isEmpty();
        }

        @Override
        protected long addInternal(ByteBuffer term, int segmentRowId)
        {
            return graphIndex.add(term, segmentRowId);
        }

        @Override
        protected long addInternalAsync(ByteBuffer term, int segmentRowId)
        {
            updatesInFlight.incrementAndGet();
            compactionExecutor.submit(() -> {
                try
                {
                    long bytesAdded = addInternal(term, segmentRowId);
                    totalBytesAllocatedConcurrent.add(bytesAdded);
                    termSizeReservoir.update(bytesAdded);
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
            // bytes allocated will be approximated immediately as the average of recently added terms,
            // rather than waiting until the async update completes to get the exact value.  The latter could
            // result in a dangerously large discrepancy between the amount of memory actually consumed
            // and the amount the limiter knows about if the queue depth grows.
            busyWaitWhile(() -> termSizeReservoir.size() == 0 && asyncThrowable.get() == null);
            if (asyncThrowable.get() != null) {
                throw new RuntimeException("Error adding term asynchronously", asyncThrowable.get());
            }
            return (long) termSizeReservoir.getMean();
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

        flushInternal(metadataBuilder);
        return metadataBuilder.build();
    }

    public long addAll(ByteBuffer term, AbstractType<?> type, PrimaryKey key, long sstableRowId)
    {
        long totalSize = 0;
        if (TypeUtil.isLiteral(type))
        {
            List<ByteBuffer> tokens = ByteLimitedMaterializer.materializeTokens(analyzer, term, components.context(), key);
            for (ByteBuffer tokenTerm : tokens)
                totalSize += add(tokenTerm, key, sstableRowId);
        }
        else
        {
            totalSize += add(term, key, sstableRowId);
        }
        return totalSize;
    }

    private long add(ByteBuffer term, PrimaryKey key, long sstableRowId)
    {
        assert !flushed : "Cannot add to flushed segment.";
        assert sstableRowId >= maxSSTableRowId;
        minSSTableRowId = minSSTableRowId < 0 ? sstableRowId : minSSTableRowId;
        maxSSTableRowId = sstableRowId;

        assert maxKey == null || maxKey.compareTo(key) <= 0;
        minKey = minKey == null ? key : minKey;
        maxKey = key;

        // Note that the min and max terms are not encoded.
        minTerm = TypeUtil.min(term, minTerm, termComparator, Version.latest());
        maxTerm = TypeUtil.max(term, maxTerm, termComparator, Version.latest());

        rowCount++;

        // segmentRowIdOffset should encode sstableRowId into Integer
        int segmentRowId = Math.toIntExact(sstableRowId - segmentRowIdOffset);

        if (segmentRowId == PostingList.END_OF_STREAM)
            throw new IllegalArgumentException("Illegal segment row id: END_OF_STREAM found");

        maxSegmentRowId = Math.max(maxSegmentRowId, segmentRowId);

        long bytesAllocated = supportsAsyncAdd()
                              ? addInternalAsync(term, segmentRowId)
                              : addInternal(term, segmentRowId);
        totalBytesAllocated += bytesAllocated;

        return bytesAllocated;
    }

    protected long addInternalAsync(ByteBuffer term, int segmentRowId)
    {
        throw new UnsupportedOperationException();
    }

    public boolean supportsAsyncAdd() {
        return false;
    }

    public Throwable getAsyncThrowable()
    {
        return asyncThrowable.get();
    }

    public void awaitAsyncAdditions()
    {
        // addTerm is only called by the compaction thread, serially, so we don't need to worry about new
        // terms being added while we're waiting -- updatesInFlight can only decrease
        busyWaitWhile(() -> updatesInFlight.get() > 0);
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

    protected abstract long addInternal(ByteBuffer term, int segmentRowId);

    protected abstract void flushInternal(SegmentMetadataBuilder metadataBuilder) throws IOException;

    int getRowCount()
    {
        return rowCount;
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
    public static void updateLastValidSegmentRowId(long lastValidSegmentRowID)
    {
        testLastValidSegmentRowId = lastValidSegmentRowID;
    }
}
