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

package org.apache.cassandra.index.sai.disk.vector;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.IntUnaryOperator;
import java.util.function.ToIntFunction;

import com.google.common.annotations.VisibleForTesting;
import org.cliffc.high_scale_lib.NonBlockingHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.jbellis.jvector.graph.GraphIndexBuilder;
import io.github.jbellis.jvector.graph.GraphSearcher;
import io.github.jbellis.jvector.graph.RandomAccessVectorValues;
import io.github.jbellis.jvector.graph.SearchResult;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndexWriter;
import io.github.jbellis.jvector.graph.disk.OrdinalMapper;
import io.github.jbellis.jvector.graph.disk.feature.Feature;
import io.github.jbellis.jvector.graph.disk.feature.FeatureId;
import io.github.jbellis.jvector.graph.disk.feature.InlineVectors;
import io.github.jbellis.jvector.graph.disk.feature.NVQ;
import io.github.jbellis.jvector.graph.similarity.DefaultSearchScoreProvider;
import io.github.jbellis.jvector.quantization.BinaryQuantization;
import io.github.jbellis.jvector.quantization.CompressedVectors;
import io.github.jbellis.jvector.quantization.NVQuantization;
import io.github.jbellis.jvector.quantization.ProductQuantization;
import io.github.jbellis.jvector.quantization.VectorCompressor;
import io.github.jbellis.jvector.util.Accountable;
import io.github.jbellis.jvector.util.Bits;
import io.github.jbellis.jvector.util.DenseIntMap;
import io.github.jbellis.jvector.util.RamUsageEstimator;
import io.github.jbellis.jvector.vector.ArrayVectorFloat;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.VectorUtil;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import org.agrona.collections.IntHashSet;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.compaction.CompactionSSTable;
import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.SSTableIndex;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.index.sai.disk.format.IndexComponents;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.disk.v1.Segment;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.disk.v2.V2VectorIndexSearcher;
import org.apache.cassandra.index.sai.disk.v2.V2VectorPostingsWriter;
import org.apache.cassandra.index.sai.disk.v5.V5OnDiskFormat;
import org.apache.cassandra.index.sai.disk.v5.V5VectorPostingsWriter;
import org.apache.cassandra.index.sai.disk.v5.V5VectorPostingsWriter.Structure;
import org.apache.cassandra.index.sai.disk.vector.VectorCompression.CompressionType;
import org.apache.cassandra.index.sai.metrics.ColumnQueryMetrics;
import org.apache.cassandra.index.sai.utils.PrimaryKeyWithSortKey;
import org.apache.cassandra.index.sai.utils.SAICodecUtils;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.lucene.util.StringHelper;

import static org.apache.cassandra.index.sai.disk.vector.NVQUtil.NUM_SUB_VECTORS;

public class CassandraOnHeapGraph<T> implements Accountable
{
    // Cassandra's PQ features, independent of JVector's
    public enum PQVersion {
        V0, // initial version
        V1, // includes unit vector calculation
    }

    /** minimum number of rows to perform PQ codebook generation */
    public static final int MIN_PQ_ROWS = 1024;

    private static final Logger logger = LoggerFactory.getLogger(CassandraOnHeapGraph.class);
    private static final VectorTypeSupport vts = VectorizationProvider.getInstance().getVectorTypeSupport();

    // We use the metable reference for easier tracing.
    private final String source;
    private final ColumnQueryMetrics.VectorIndexMetrics columnQueryMetrics;
    private final ConcurrentVectorValues vectorValues;
    private final GraphIndexBuilder builder;
    private final VectorType.VectorSerializer serializer;
    private final VectorSimilarityFunction similarityFunction;
    private final ConcurrentMap<VectorFloat<?>, VectorPostings<T>> postingsMap;
    private final DenseIntMap<VectorPostings<T>> postingsByOrdinal;
    private final NonBlockingHashMap<T, VectorFloat<?>> vectorsByKey;
    private final AtomicInteger nextOrdinal = new AtomicInteger();
    private final VectorSourceModel sourceModel;
    private final InvalidVectorBehavior invalidVectorBehavior;
    private final IntHashSet deletedOrdinals;
    private volatile boolean hasDeletions;
    private volatile boolean allVectorsAreUnitLength;

    // we don't need to explicitly close these since only on-heap resources are involved
    private final ThreadLocal<GraphSearcherAccessManager> searchers;

    private final boolean writeNvq;

    /**
     * @param forSearching if true, vectorsByKey will be initialized and populated with vectors as they are added
     */
    public CassandraOnHeapGraph(IndexContext context, boolean forSearching, Memtable memtable)
    {
        this.source = memtable == null
                      ? "null"
                      : memtable.getClass().getSimpleName() + '@' + Integer.toHexString(memtable.hashCode());
        this.columnQueryMetrics = (ColumnQueryMetrics.VectorIndexMetrics) context.getColumnQueryMetrics();
        var indexConfig = context.getIndexWriterConfig();
        var termComparator = context.getValidator();
        serializer = (VectorType.VectorSerializer) termComparator.getSerializer();
        var dimension = ((VectorType<?>) termComparator).dimension;
        vectorValues = new ConcurrentVectorValues(dimension);
        similarityFunction = indexConfig.getSimilarityFunction();
        sourceModel = indexConfig.getSourceModel();
        // We need to be able to inexpensively distinguish different vectors, with a slower path
        // that identifies vectors that are equal but not the same reference.  A comparison-
        // based Map (which only needs to look at vector elements until a difference is found)
        // is thus a better option than hash-based (which has to look at all elements to compute the hash).
        postingsMap = new ConcurrentSkipListMap<>((a, b) -> {
            return Arrays.compare(((ArrayVectorFloat) a).get(), ((ArrayVectorFloat) b).get());
        });
        postingsByOrdinal = new DenseIntMap<>(1024);
        deletedOrdinals = new IntHashSet();
        vectorsByKey = forSearching ? new NonBlockingHashMap<>() : null;
        invalidVectorBehavior = forSearching ? InvalidVectorBehavior.FAIL : InvalidVectorBehavior.IGNORE;

        // Assume true until we observe otherwise.
        allVectorsAreUnitLength = true;

        // NVQ is only written during compaction to save on compute costs
        writeNvq = NVQUtil.shouldWriteNVQ(dimension, context.version()) && !forSearching;

        int jvectorVersion = context.version().onDiskFormat().jvectorFileFormatVersion();
        // This is only a warning since it's not a fatal error to write without hierarchy
        if (indexConfig.isHierarchyEnabled() && jvectorVersion < 4)
            logger.warn("Hierarchical graphs configured but node configured with V3OnDiskFormat.JVECTOR_VERSION {}. " +
                        "Skipping setting for {}", jvectorVersion, indexConfig.getIndexName());

        builder = new GraphIndexBuilder(vectorValues,
                                        similarityFunction,
                                        indexConfig.getAnnMaxDegree(),
                                        indexConfig.getConstructionBeamWidth(),
                                        indexConfig.getNeighborhoodOverflow(1.0f), // no overflow means add will be a bit slower but flush will be faster
                                        indexConfig.getAlpha(dimension > 3 ? 1.2f : 2.0f),
                                        indexConfig.isHierarchyEnabled() && jvectorVersion >= 4);
        searchers = ThreadLocal.withInitial(() -> new GraphSearcherAccessManager(new GraphSearcher(builder.getGraph())));
    }

    public int size()
    {
        return vectorValues.size();
    }

    public boolean isEmpty()
    {
        return postingsMap.values().stream().allMatch(VectorPostings::isEmpty);
    }

    /**
     * @return the ordinal of the vector in the graph, or -1 if the vector is not in the graph
     */
    public int getOrdinal(VectorFloat<?> vector)
    {
        VectorPostings<T> postings = postingsMap.get(vector);
        // There is a small race from when the postings list is created to when it is assigned an ordinal,
        // so we do not assert that the ordinal is set here
        return postings == null ? -1 : postings.getOrdinal(false);
    }

    /**
     * @return the incremental bytes used by adding the given vector to the index
     */
    public long add(ByteBuffer term, T key)
    {
        assert term != null && term.remaining() != 0;

        var vector = vts.createFloatVector(serializer.deserializeFloatArray(term));
        // Validate the vector.  Almost always, this is called at insert time (which sets invalid behavior to FAIL,
        // resulting in the insert being aborted if the vector is invalid), or while writing out an sstable
        // from flush or compaction (which sets invalid behavior to IGNORE, since we can't just rip existing data out of
        // the table).
        //
        // However, it's also possible for this to be called during commitlog replay if the node previously crashed
        // AFTER processing CREATE INDEX, but BEFORE flushing active memtables.  Commitlog replay will then follow
        // the normal insert code path, (which would set behavior to FAIL) so we special-case it here; see VECTOR-269.
        var behavior = invalidVectorBehavior;
        if (!StorageService.instance.isInitialized())
            behavior = InvalidVectorBehavior.IGNORE; // we're replaying the commitlog so force IGNORE
        if (behavior == InvalidVectorBehavior.IGNORE)
        {
            try
            {
                VectorValidation.validateIndexable(vector, similarityFunction);
            }
            catch (InvalidRequestException e)
            {
                if (StorageService.instance.isInitialized())
                    logger.trace("Ignoring invalid vector during index build against existing data: {}", (Object) e);
                else
                    logger.trace("Ignoring invalid vector during commitlog replay: {}", (Object) e);
                return 0;
            }
        }
        else
        {
            assert behavior == InvalidVectorBehavior.FAIL;
            VectorValidation.validateIndexable(vector, similarityFunction);
        }

        var bytesUsed = 0L;

        // Store a cached reference to the vector for brute force computations later. There is a small race
        // condition here: if inserts for the same PrimaryKey add different vectors, vectorsByKey might
        // become out of sync with the graph.
        if (vectorsByKey != null)
        {
            vectorsByKey.put(key, vector);
            // The size of the entries themselves are counted below, so just count the two extra references
            bytesUsed += RamUsageEstimator.NUM_BYTES_OBJECT_REF * 2L;
        }

        VectorPostings<T> postings = postingsMap.get(vector);
        // if the vector is already in the graph, all that happens is that the postings list is updated
        // otherwise, we add the vector in this order:
        // 1. to the postingsMap
        // 2. to the vectorValues
        // 3. to the graph
        // This way, concurrent searches of the graph won't see the vector until it's visible
        // in the other structures as well.
        if (postings == null)
        {
            postings = new VectorPostings<>(key);
            // since we are using ConcurrentSkipListMap, it is NOT correct to use computeIfAbsent here
            if (postingsMap.putIfAbsent(vector, postings) == null)
            {
                // we won the race to add the new entry; assign it an ordinal and add to the other structures
                var ordinal = nextOrdinal.getAndIncrement();
                postings.setOrdinal(ordinal);
                bytesUsed += RamEstimation.concurrentHashMapRamUsed(1); // the new posting Map entry
                bytesUsed += vectorValues.add(ordinal, vector);
                bytesUsed += postings.ramBytesUsed();
                var success = postingsByOrdinal.compareAndPut(ordinal, null, postings);
                assert success : "postingsByOrdinal already contains an entry for ordinal " + ordinal;
                bytesUsed += builder.addGraphNode(ordinal, vector);

                // If necessary, check if the vector is unit length.
                if (!sourceModel.hasKnownUnitLengthVectors() && allVectorsAreUnitLength)
                    if (!(Math.abs(VectorUtil.dotProduct(vector, vector) - 1.0f) < 0.01))
                        allVectorsAreUnitLength = false;

                return bytesUsed;
            }
            else
            {
                postings = postingsMap.get(vector);
            }
        }
        // postings list already exists, just add the new key (if it's not already in the list)
        if (postings.add(key))
        {
            bytesUsed += postings.bytesPerPosting();
        }

        return bytesUsed;
    }

    public Collection<T> keysFromOrdinal(int node)
    {
        return postingsByOrdinal.get(node).getPostings();
    }

    public VectorFloat<?> vectorForKey(T key)
    {
        if (vectorsByKey == null)
            throw new IllegalStateException("vectorsByKey is not initialized");
        return vectorsByKey.get(key);
    }

    public void remove(ByteBuffer term, T key)
    {
        assert term != null && term.remaining() != 0;

        var rawVector = serializer.deserializeFloatArray(term);
        VectorFloat<?> v = vts.createFloatVector(rawVector);
        var postings = postingsMap.get(v);
        if (postings == null)
        {
            // it's possible for this to be called against a different memtable than the one
            // the value was originally added to, in which case we do not expect to find
            // the key among the postings for this vector
            return;
        }

        hasDeletions = true;
        postings.remove(key);
        if (vectorsByKey != null)
            // On updates to a row, we call add then remove, so we must pass the key's value to ensure we only remove
            // the deleted vector from vectorsByKey
            vectorsByKey.remove(key, v);
    }

    /**
     * @return an itererator over {@link PrimaryKeyWithSortKey} in the graph's {@link SearchResult} order
     */
    public CloseableIterator<SearchResult.NodeScore> search(QueryContext context, VectorFloat<?> queryVector, int limit, int rerankK, float threshold, boolean usePruning, Bits toAccept)
    {
        VectorValidation.validateIndexable(queryVector, similarityFunction);

        // search() errors out when an empty graph is passed to it
        if (vectorValues.size() == 0)
            return CloseableIterator.emptyIterator();
        // This configuration indicates rerankless search, but that is only applicable to disk search, so we set
        // rerankK to limit and otherwise ignore the setting.
        if (rerankK <= 0)
            rerankK = limit;

        Bits bits = hasDeletions ? BitsUtil.bitsIgnoringDeleted(toAccept, postingsByOrdinal) : toAccept;
        var graphAccessManager = searchers.get();
        var searcher = graphAccessManager.get();
        searcher.usePruning(usePruning);
        searcher.setView(builder.getGraph().getView());
        try
        {
            var ssf = DefaultSearchScoreProvider.exact(queryVector, similarityFunction, vectorValues);
            long start = System.nanoTime();
            var result = searcher.search(ssf, limit, rerankK, threshold, 0.0f, bits);
            long elapsed = System.nanoTime() - start;
            Tracing.trace("ANN search for {}/{} (usePruning: {}) visited {} nodes, reranked {} to return {} results from {}",
                          limit, rerankK, usePruning, result.getVisitedCount(), result.getRerankedCount(), result.getNodes().length, source);
            columnQueryMetrics.onSearchResult(result, elapsed, false);
            context.addAnnGraphSearchLatency(elapsed);
            if (threshold > 0)
            {
                // Threshold based searches do not support resuming the search.
                graphAccessManager.release();
                return CloseableIterator.wrap(Arrays.stream(result.getNodes()).iterator());
            }
            return new AutoResumingNodeScoreIterator(searcher, graphAccessManager, result, context, columnQueryMetrics, visited -> {}, limit, rerankK, true, source);
        }
        catch (Throwable t)
        {
            // If we don't release it, we'll never be able to aquire it, so catch and rethrow Throwable.
            graphAccessManager.forceRelease();
            throw t;
        }
    }

    /**
     * Prepare for flushing by doing a bunch of housekeeping:
     * 1. Compute row ids for each vector in the postings map
     * 2. Remove any vectors that are no longer in use and populate `deletedOrdinals`, including for range deletions
     * 3. Return true if the caller should proceed to invoke flush, or false if everything was deleted
     * <p>
     * This is split out from flush per se because of (3); we don't want to flush empty
     * index segments, but until we do (1) and (2) we don't know if the segment is empty.
     */
    public boolean preFlush(ToIntFunction<T> postingTransformer)
    {
        var it = postingsMap.entrySet().iterator();
        while (it.hasNext()) {
            var entry = it.next();
            var vp = entry.getValue();
            vp.computeRowIds(postingTransformer);
            if (vp.isEmpty() || vp.shouldAppendDeletedOrdinal())
                deletedOrdinals.add(vp.getOrdinal());
        }
        return deletedOrdinals.size() < builder.getGraph().size();
    }

    public SegmentMetadata.ComponentMetadataMap flush(IndexComponents.ForWrite perIndexComponents) throws IOException
    {
        int nInProgress = builder.insertsInProgress();
        assert nInProgress == 0 : String.format("Attempting to write graph while %d inserts are in progress", nInProgress);
        assert nextOrdinal.get() == builder.getGraph().size() : String.format("nextOrdinal %d != graph size %d -- ordinals should be sequential",
                                                                              nextOrdinal.get(), builder.getGraph().size());
        assert vectorValues.size() == builder.getGraph().size() : String.format("vector count %d != graph size %d",
                                                                                vectorValues.size(), builder.getGraph().size());
        logger.debug("Writing graph with {} rows and {} distinct vectors", postingsMap.values().stream().mapToInt(VectorPostings::size).sum(), vectorValues.size());

        // compute the remapping of old ordinals to new (to fill in holes from deletion and/or to create a
        // closer correspondance to rowids, simplifying postings lookups later)
        V5VectorPostingsWriter.RemappedPostings remappedPostings;
        if (V5OnDiskFormat.writeV5VectorPostings(perIndexComponents.version()))
        {
            // remove postings corresponding to marked-deleted vectors
            var it = postingsMap.entrySet().iterator();
            while (it.hasNext()) {
                var entry = it.next();
                var vp = entry.getValue();
                if (deletedOrdinals.contains(vp.getOrdinal()))
                    it.remove();
            }

            assert postingsMap.keySet().size() + deletedOrdinals.size() == vectorValues.size()
            : String.format("postings map entry count %d + deleted count %d != vector count %d",
                            postingsMap.keySet().size(), deletedOrdinals.size(), vectorValues.size());
            // remove deleted ordinals from the graph.  this is not done at remove() time, because the same vector
            // could be added back again, "undeleting" the ordinal, and the concurrency gets tricky
            deletedOrdinals.stream().parallel().forEach(builder::markNodeDeleted);
            deletedOrdinals.clear();
            builder.cleanup();
            remappedPostings = V5VectorPostingsWriter.remapForMemtable(postingsMap, perIndexComponents.version());
        }
        else
        {
            assert postingsMap.keySet().size() == vectorValues.size() : String.format("postings map entry count %d != vector count %d",
                                                                                      postingsMap.keySet().size(), vectorValues.size());
            builder.cleanup();
            remappedPostings = V2VectorPostingsWriter.remapForMemtable(postingsMap, !deletedOrdinals.isEmpty());
        }

        OrdinalMapper ordinalMapper = remappedPostings.ordinalMapper;

        // Write the NVQ feature, optimize when https://github.com/datastax/jvector/pull/549 is merged
        NVQuantization nvq = writeNvq ? NVQuantization.compute(vectorValues, NUM_SUB_VECTORS) : null;

        IndexComponent.ForWrite termsDataComponent = perIndexComponents.addOrGet(IndexComponentType.TERMS_DATA);
        var indexFile = termsDataComponent.file();
        long termsOffset = SAICodecUtils.headerSize();
        if (indexFile.exists())
            termsOffset += indexFile.length();
        try (var pqOutput = perIndexComponents.addOrGet(IndexComponentType.PQ).openOutput(true);
             var postingsOutput = perIndexComponents.addOrGet(IndexComponentType.POSTING_LISTS).openOutput(true);
             var indexWriter = new OnDiskGraphIndexWriter.Builder(builder.getGraph(), indexFile.toPath())
                               .withStartOffset(termsOffset)
                               .withVersion(perIndexComponents.version().onDiskFormat().jvectorFileFormatVersion())
                               .withMapper(ordinalMapper)
                               .with(nvq != null ? new NVQ(nvq) : new InlineVectors(vectorValues.dimension()))
                               .build())
        {
            SAICodecUtils.writeHeader(pqOutput);
            SAICodecUtils.writeHeader(postingsOutput);
            indexWriter.getOutput().seek(indexFile.length()); // position at the end of the previous segment before writing our own header
            SAICodecUtils.writeHeader(SAICodecUtils.toLuceneOutput(indexWriter.getOutput()), perIndexComponents.version());
            assert indexWriter.getOutput().position() == termsOffset : "termsOffset " + termsOffset + " != " + indexWriter.getOutput().position();

            // compute and write PQ
            long pqOffset = pqOutput.getFilePointer();
            long pqPosition = writePQ(pqOutput.asSequentialWriter(), remappedPostings, perIndexComponents.context());
            long pqLength = pqPosition - pqOffset;

            // write postings
            long postingsOffset = postingsOutput.getFilePointer();
            long postingsPosition;
            if (V5OnDiskFormat.writeV5VectorPostings(perIndexComponents.version()))
            {
                assert deletedOrdinals.isEmpty(); // V5 format does not support recording deleted ordinals
                postingsPosition = new V5VectorPostingsWriter<T>(remappedPostings)
                                   .writePostings(postingsOutput.asSequentialWriter(), vectorValues, postingsMap);
            }
            else
            {
                IntUnaryOperator newToOldMapper = remappedPostings.ordinalMapper::newToOld;
                postingsPosition = new V2VectorPostingsWriter<T>(remappedPostings.structure == Structure.ONE_TO_ONE, builder.getGraph().size(), newToOldMapper)
                                   .writePostings(postingsOutput.asSequentialWriter(), vectorValues, postingsMap, deletedOrdinals);
            }
            long postingsLength = postingsPosition - postingsOffset;

            // write the graph
            var start = System.nanoTime();
            var supplier = nvq != null
                            ? Feature.singleStateFactory(FeatureId.NVQ_VECTORS, nodeId -> new NVQ.State(nvq.encode(vectorValues.getVector(nodeId))))
                            : Feature.singleStateFactory(FeatureId.INLINE_VECTORS, nodeId -> new InlineVectors.State(vectorValues.getVector(nodeId)));
            indexWriter.write(supplier);
            SAICodecUtils.writeFooter(indexWriter.getOutput(), indexWriter.checksum());
            logger.info("Writing graph took {}ms", (System.nanoTime() - start) / 1_000_000);
            long termsLength = indexWriter.getOutput().position() - termsOffset;

            // write remaining footers/checksums
            SAICodecUtils.writeFooter(pqOutput);
            SAICodecUtils.writeFooter(postingsOutput);

            // add components to the metadata map
            return createMetadataMap(termsOffset, termsLength, postingsOffset, postingsLength, pqOffset, pqLength);
        }
    }

    static SegmentMetadata.ComponentMetadataMap createMetadataMap(long termsOffset, long termsLength, long postingsOffset, long postingsLength, long pqOffset, long pqLength)
    {
        SegmentMetadata.ComponentMetadataMap metadataMap = new SegmentMetadata.ComponentMetadataMap();
        metadataMap.put(IndexComponentType.TERMS_DATA, -1, termsOffset, termsLength, Map.of());
        metadataMap.put(IndexComponentType.POSTING_LISTS, -1, postingsOffset, postingsLength, Map.of());
        Map<String, String> vectorConfigs = Map.of("SEGMENT_ID", ByteBufferUtil.bytesToHex(ByteBuffer.wrap(StringHelper.randomId())));
        metadataMap.put(IndexComponentType.PQ, -1, pqOffset, pqLength, vectorConfigs);
        return metadataMap;
    }

    /**
     * Return the best previous CompressedVectors for this column that matches the `matcher` predicate.
     * "Best" means the most recent one that hits the row count target of {@link ProductQuantization#MAX_PQ_TRAINING_SET_SIZE},
     * or the one with the most rows if none are larger than that.
     */
    public static PqInfo getPqIfPresent(IndexContext indexContext, Function<VectorCompression, Boolean> matcher)
    {
        // Retrieve the first compressed vectors for a segment with at least MAX_PQ_TRAINING_SET_SIZE rows
        // or the one with the most rows if none reach that size
        var view = indexContext.getReferencedView(TimeUnit.SECONDS.toNanos(5));
        if (view == null)
        {
            logger.warn("Unable to get view of already built indexes for {}", indexContext);
            return null;
        }

        try
        {
            var indexes = new ArrayList<>(view.getIndexes());
            indexes.sort(Comparator.comparing(SSTableIndex::getSSTable, CompactionSSTable.maxTimestampDescending));

            PqInfo cvi = null;
            long maxRows = 0;
            for (SSTableIndex index : indexes)
            {
                for (Segment segment : index.getSegments())
                {
                    if (segment.metadata.numRows < maxRows)
                        continue;

                    var searcher = (V2VectorIndexSearcher) segment.getIndexSearcher();
                    var cv = searcher.getCompression();
                    if (matcher.apply(cv))
                    {
                        // We can exit now because we won't find a better candidate
                        var candidate = new PqInfo(searcher.getPQ(), searcher.containsUnitVectors(), segment.metadata.numRows);
                        if (segment.metadata.numRows >= ProductQuantization.MAX_PQ_TRAINING_SET_SIZE)
                            return candidate;

                        cvi = candidate;
                        maxRows = segment.metadata.numRows;
                    }
                }
            }
            return cvi;
        }
        finally
        {
            view.release();
        }
    }

    private long writePQ(SequentialWriter writer, V5VectorPostingsWriter.RemappedPostings remapped, IndexContext indexContext) throws IOException
    {
        var preferredCompression = sourceModel.compressionProvider.apply(vectorValues.dimension());

        // Build encoder and compress vectors
        VectorCompressor<?> compressor; // will be null if we can't compress
        CompressedVectors cv = null;
        // limit the PQ computation and encoding to one index at a time -- goal during flush is to
        // evict from memory ASAP so better to do the PQ build (in parallel) one at a time
        synchronized (CassandraOnHeapGraph.class)
        {
            // build encoder (expensive for PQ, cheaper for BQ)
            if (preferredCompression.type == CompressionType.PRODUCT_QUANTIZATION)
            {
                var pqi = getPqIfPresent(indexContext, preferredCompression::equals);
                compressor = computeOrRefineFrom(pqi, preferredCompression);
            }
            else
            {
                assert preferredCompression.type == CompressionType.BINARY_QUANTIZATION : preferredCompression.type;
                compressor = BinaryQuantization.compute(vectorValues);
            }
            assert !vectorValues.isValueShared();
            // encode (compress) the vectors to save
            if (compressor != null)
                cv = compressor.encodeAll(new RemappedVectorValues(remapped, remapped.maxNewOrdinal, vectorValues));
        }

        var actualType = compressor == null ? CompressionType.NONE : preferredCompression.type;
        writePqHeader(writer, allVectorsAreUnitLength, actualType, indexContext.version());
        if (actualType == CompressionType.NONE)
            return writer.position();

        // save (outside the synchronized block, this is io-bound not CPU)
        cv.write(writer, indexContext.version().onDiskFormat().jvectorFileFormatVersion());
        return writer.position();
    }

    static void writePqHeader(DataOutput writer, boolean unitVectors, CompressionType type, Version version)
    throws IOException
    {
        if (version.onDiskFormat().jvectorFileFormatVersion() >= 3)
        {
            // version and optional fields
            writer.writeInt(CassandraDiskAnn.PQ_MAGIC);
            writer.writeInt(PQVersion.V1.ordinal());
            writer.writeBoolean(unitVectors);
        }

        // write the compression type
        writer.writeByte(type.ordinal());
    }

    ProductQuantization computeOrRefineFrom(PqInfo existingInfo, VectorCompression preferredCompression)
    {
        if (existingInfo == null)
        {
            // no previous PQ, compute a new one if we have enough rows to do it
            if (vectorValues.size() < MIN_PQ_ROWS)
                return null;
            else
                return ProductQuantization.compute(vectorValues, preferredCompression.getCompressedSize(), 256, false);
        }

        // use the existing one unmodified if we either don't have enough rows to fine-tune, or
        // the existing one was built with a large enough set
        var existingPQ = existingInfo.pq;
        if (vectorValues.size() < MIN_PQ_ROWS || existingInfo.rowCount >= ProductQuantization.MAX_PQ_TRAINING_SET_SIZE)
            return existingPQ;

        // refine the existing one
        return existingPQ.refine(vectorValues);
    }

    public long ramBytesUsed()
    {
        return postingsBytesUsed() + vectorValues.ramBytesUsed() + builder.getGraph().ramBytesUsed();
    }

    private long postingsBytesUsed()
    {
        return RamEstimation.denseIntMapRamUsed(postingsByOrdinal.size())
               + 3 * RamEstimation.concurrentHashMapRamUsed(postingsMap.size()) // CSLM is much less efficient than CHM
               + postingsMap.values().stream().mapToLong(VectorPostings::ramBytesUsed).sum();
    }

    public enum InvalidVectorBehavior
    {
        IGNORE,
        FAIL
    }

    public static class PqInfo
    {
        public final ProductQuantization pq;
        /** an empty Optional indicates that the index was written with an older version that did not record this information */
        public final boolean unitVectors;
        public final long rowCount;

        public PqInfo(ProductQuantization pq, boolean unitVectors, long rowCount)
        {
            this.pq = pq;
            this.unitVectors = unitVectors;
            this.rowCount = rowCount;
        }
    }

    /** ensures that the graph is connected -- normally not necessary but it can help tests reason about the state */
    @VisibleForTesting
    public void cleanup()
    {
        builder.cleanup();
    }

    /**
     * A simple wrapper that remaps the ordinals in the vector values to the new ordinals
     */
    private static class RemappedVectorValues implements RandomAccessVectorValues
    {
        final V5VectorPostingsWriter.RemappedPostings remapped;
        final int maxNewOrdinal;
        final RandomAccessVectorValues vectorValues;

        RemappedVectorValues(V5VectorPostingsWriter.RemappedPostings remapped, int maxNewOrdinal, RandomAccessVectorValues vectorValues)
        {
            this.remapped = remapped;
            this.maxNewOrdinal = maxNewOrdinal;
            this.vectorValues = vectorValues;
        }

        @Override
        public int size()
        {
            return maxNewOrdinal + 1;
        }

        @Override
        public int dimension()
        {
            return vectorValues.dimension();
        }

        @Override
        public VectorFloat<?> getVector(int i)
        {
            var oldOrdinal = remapped.ordinalMapper.newToOld(i);
            return oldOrdinal == OrdinalMapper.OMITTED ? null : vectorValues.getVector(oldOrdinal);
        }

        @Override
        public boolean isValueShared()
        {
            return vectorValues.isValueShared();
        }

        @Override
        public RandomAccessVectorValues copy()
        {
            return new RemappedVectorValues(remapped, maxNewOrdinal, vectorValues.copy());
        }
    }
}
