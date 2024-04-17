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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.IntUnaryOperator;
import java.util.stream.IntStream;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import org.cliffc.high_scale_lib.NonBlockingHashMap;
import org.cliffc.high_scale_lib.NonBlockingHashMapLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.jbellis.jvector.graph.GraphIndexBuilder;
import io.github.jbellis.jvector.graph.GraphSearcher;
import io.github.jbellis.jvector.graph.SearchResult;
import io.github.jbellis.jvector.graph.disk.Feature;
import io.github.jbellis.jvector.graph.disk.FeatureId;
import io.github.jbellis.jvector.graph.disk.InlineVectors;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndexWriter;
import io.github.jbellis.jvector.graph.similarity.SearchScoreProvider;
import io.github.jbellis.jvector.pq.BQVectors;
import io.github.jbellis.jvector.pq.BinaryQuantization;
import io.github.jbellis.jvector.pq.CompressedVectors;
import io.github.jbellis.jvector.pq.PQVectors;
import io.github.jbellis.jvector.pq.ProductQuantization;
import io.github.jbellis.jvector.pq.VectorCompressor;
import io.github.jbellis.jvector.util.Bits;
import io.github.jbellis.jvector.util.RamUsageEstimator;
import io.github.jbellis.jvector.vector.ArrayVectorFloat;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.VectorUtil;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.ByteSequence;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import org.apache.cassandra.db.compaction.CompactionSSTable;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.SSTableIndex;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.IndexWriterConfig;
import org.apache.cassandra.index.sai.disk.v1.Segment;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.disk.io.IndexFileUtils;
import org.apache.cassandra.index.sai.disk.v2.V2VectorIndexSearcher;
import org.apache.cassandra.index.sai.disk.v3.CassandraDiskAnn;
import org.apache.cassandra.index.sai.utils.SAICodecUtils;
import org.apache.cassandra.index.sai.utils.ScoredPrimaryKey;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.lucene.util.StringHelper;

import static org.apache.cassandra.utils.Clock.Global.nanoTime;

public class CassandraOnHeapGraph<T>
{
    private static final Logger logger = LoggerFactory.getLogger(CassandraOnHeapGraph.class);
    private static final VectorTypeSupport vts = VectorizationProvider.getInstance().getVectorTypeSupport();

    private final ConcurrentVectorValues vectorValues;
    private final GraphIndexBuilder builder;
    private final VectorType.VectorSerializer serializer;
    private final VectorSimilarityFunction similarityFunction;
    private final ConcurrentMap<VectorFloat<?>, VectorPostings<T>> postingsMap;
    private final NonBlockingHashMapLong<VectorPostings<T>> postingsByOrdinal;
    private final NonBlockingHashMap<T, VectorFloat<?>> vectorsByKey;
    private final AtomicInteger nextOrdinal = new AtomicInteger();
    private final VectorSourceModel sourceModel;
    private volatile boolean hasDeletions;

    // we don't need to explicitly close these since only on-heap resources are involved
    private final ThreadLocal<GraphSearcher> searchers;

    /**
     * @param termComparator the vector type -- passed as AbstractType for caller's convenience
     * @param indexConfig
     * @param forSearching if true, vectorsByKey will be initialized and populated with vectors as they are added
     */
    public CassandraOnHeapGraph(AbstractType<?> termComparator, IndexWriterConfig indexConfig, boolean forSearching)
    {
        serializer = (VectorType.VectorSerializer)termComparator.getSerializer();
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
        postingsByOrdinal = new NonBlockingHashMapLong<>();
        vectorsByKey = forSearching ? new NonBlockingHashMap<>() : null;

        builder = new GraphIndexBuilder(vectorValues,
                                        similarityFunction,
                                        indexConfig.getMaximumNodeConnections(),
                                        indexConfig.getConstructionBeamWidth(),
                                        1.2f,
                                        dimension > 3 ? 1.2f : 2.0f);
        searchers = ThreadLocal.withInitial(() -> new GraphSearcher(builder.getGraph()));
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
     * @return the incremental bytes ysed by adding the given vector to the index
     */
    public long add(ByteBuffer term, T key, InvalidVectorBehavior behavior)
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
            postings = new VectorPostings<T>(key);
            // since we are using ConcurrentSkipListMap, it is NOT correct to use computeIfAbsent here
            if (postingsMap.putIfAbsent(vector, postings) == null)
            {
                // we won the race to add the new entry; assign it an ordinal and add to the other structures
                var ordinal = nextOrdinal.getAndIncrement();
                postings.setOrdinal(ordinal);
                bytesUsed += RamEstimation.concurrentHashMapRamUsed(1); // the new posting Map entry
                bytesUsed += vectorValues.add(ordinal, vector);
                bytesUsed += VectorPostings.emptyBytesUsed() + VectorPostings.bytesPerPosting();
                postingsByOrdinal.put(ordinal, postings);
                bytesUsed += builder.addGraphNode(ordinal, vector);
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
            bytesUsed += VectorPostings.bytesPerPosting();
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
     * @return an itererator over {@link ScoredPrimaryKey} in the graph's {@link SearchResult} order
     */
    public CloseableIterator<SearchResult.NodeScore> search(QueryContext context, VectorFloat<?> queryVector, int limit, float threshold, Bits toAccept)
    {
        VectorValidation.validateIndexable(queryVector, similarityFunction);

        // search() errors out when an empty graph is passed to it
        if (vectorValues.size() == 0)
            return CloseableIterator.emptyIterator();

        Bits bits = hasDeletions ? BitsUtil.bitsIgnoringDeleted(toAccept, postingsByOrdinal) : toAccept;
        var searcher = searchers.get();
        var ssf = SearchScoreProvider.exact(queryVector, similarityFunction, vectorValues);
        var topK = sourceModel.topKFor(limit, null);
        var result = searcher.search(ssf, topK, threshold, bits);
        Tracing.trace("ANN search visited {} in-memory nodes to return {} results", result.getVisitedCount(), result.getNodes().length);
        context.addAnnNodesVisited(result.getVisitedCount());
        // Threshold based searches do not support resuming the search.
        return threshold > 0 ? CloseableIterator.wrap(Arrays.stream(result.getNodes()).iterator())
                             : new AutoResumingNodeScoreIterator(searcher, result, context::addAnnNodesVisited, topK, true);
    }

    public Set<Integer> computeDeletedOrdinals(Function<T, Integer> postingTransformer)
    {
        var deletedOrdinals = new HashSet<Integer>();
        postingsMap.values().stream().filter(VectorPostings::isEmpty).forEach(vectorPostings -> deletedOrdinals.add(vectorPostings.getOrdinal()));
        // remove ordinals that don't have corresponding row ids due to partition/range deletion
        for (VectorPostings<T> vectorPostings : postingsMap.values())
        {
            vectorPostings.computeRowIds(postingTransformer);
            if (vectorPostings.shouldAppendDeletedOrdinal())
                deletedOrdinals.add(vectorPostings.getOrdinal());
        }
        return deletedOrdinals;
    }

    public SegmentMetadata.ComponentMetadataMap writeData(IndexDescriptor descriptor, IndexContext indexContext, Set<Integer> deletedOrdinals) throws IOException
    {
        int nInProgress = builder.insertsInProgress();
        assert nInProgress == 0 : String.format("Attempting to write graph while %d inserts are in progress", nInProgress);
        assert nextOrdinal.get() == builder.getGraph().size() : String.format("nextOrdinal %d != graph size %d -- ordinals should be sequential",
                                                                              nextOrdinal.get(), builder.getGraph().size());
        assert vectorValues.size() == builder.getGraph().size() : String.format("vector count %d != graph size %d",
                                                                                vectorValues.size(), builder.getGraph().size());
        assert postingsMap.keySet().size() == vectorValues.size() : String.format("postings map entry count %d != vector count %d",
                                                                                  postingsMap.keySet().size(), vectorValues.size());
        logger.debug("Writing graph with {} rows and {} distinct vectors", postingsMap.values().stream().mapToInt(VectorPostings::size).sum(), vectorValues.size());

        // map of existing ordinal to rowId (aka new ordinal if remapping is possible)
        // null if remapping is not possible
        final BiMap <Integer, Integer> ordinalMap = deletedOrdinals.isEmpty() ? buildOrdinalMap() : null;
        boolean canFastFindRows = ordinalMap != null;
        IntUnaryOperator reverseOrdinalMapper = canFastFindRows
                                                ? x -> ordinalMap.inverse().getOrDefault(x, x)
                                                : x -> x;
        var finalOrdinalMap = ordinalMap == null ? OnDiskGraphIndexWriter.sequentialRenumbering(builder.getGraph()) : ordinalMap;

        var indexFile = descriptor.fileFor(IndexComponent.TERMS_DATA, indexContext);
        long termsOffset = SAICodecUtils.headerSize();
        if (indexFile.exists())
            termsOffset += indexFile.length();
        try (var pqOutput = IndexFileUtils.instance.openOutput(descriptor.fileFor(IndexComponent.PQ, indexContext), true, descriptor.getVersion(indexContext));
             var postingsOutput = IndexFileUtils.instance.openOutput(descriptor.fileFor(IndexComponent.POSTING_LISTS, indexContext), true, descriptor.getVersion(indexContext));
             var indexWriter = new OnDiskGraphIndexWriter.Builder(builder.getGraph(), indexFile.toPath())
                               .withMap(finalOrdinalMap)
                               .with(new InlineVectors(vectorValues.dimension()))
                               .withStartOffset(termsOffset)
                               .build())
        {
            SAICodecUtils.writeHeader(pqOutput);
            SAICodecUtils.writeHeader(postingsOutput);
            indexWriter.getOutput().seek(indexFile.length());
            SAICodecUtils.writeHeader(SAICodecUtils.toLuceneOutput(indexWriter.getOutput()));
            assert indexWriter.getOutput().position() == termsOffset : "termsOffset " + termsOffset + " != " + indexWriter.getOutput().position();

            // compute and write PQ
            long pqOffset = pqOutput.getFilePointer();
            long pqPosition = writePQ(pqOutput.asSequentialWriter(), reverseOrdinalMapper, indexContext);
            long pqLength = pqPosition - pqOffset;

            // write postings
            long postingsOffset = postingsOutput.getFilePointer();
            long postingsPosition = new VectorPostingsWriter<T>(canFastFindRows, reverseOrdinalMapper)
                                    .writePostings(postingsOutput.asSequentialWriter(),
                                                   vectorValues, postingsMap, deletedOrdinals);
            long postingsLength = postingsPosition - postingsOffset;

            // complete (internal clean up) and write the graph
            builder.cleanup();

            var start = nanoTime();
            Map<FeatureId, IntFunction<Feature.State>> suppliers = Collections.singletonMap(FeatureId.INLINE_VECTORS, nodeId -> new InlineVectors.State(vectorValues.getVector(nodeId)));
            indexWriter.write(new EnumMap<>(suppliers));
            SAICodecUtils.writeFooter(indexWriter.getOutput(), indexWriter.checksum());
            logger.info("Writing graph took {}ms", (nanoTime() - start) / 1_000_000);
            long termsLength = indexWriter.getOutput().position() - termsOffset;

            // write remaining footers/checksums
            SAICodecUtils.writeFooter(pqOutput);
            SAICodecUtils.writeFooter(postingsOutput);

            // add components to the metadata map
            SegmentMetadata.ComponentMetadataMap metadataMap = new SegmentMetadata.ComponentMetadataMap();
            metadataMap.put(IndexComponent.TERMS_DATA, -1, termsOffset, termsLength, Map.of());
            metadataMap.put(IndexComponent.POSTING_LISTS, -1, postingsOffset, postingsLength, Map.of());
            Map<String, String> vectorConfigs = Map.of("SEGMENT_ID", ByteBufferUtil.bytesToHex(ByteBuffer.wrap(StringHelper.randomId())));
            metadataMap.put(IndexComponent.PQ, -1, pqOffset, pqLength, vectorConfigs);
            return metadataMap;
        }
    }

    private static CompressedVectors getCompressedVectorsIfPresent(IndexContext indexContext, VectorCompression preferredCompression)
    {
        // Retrieve the first compressed vectors for a segment with more than MAX_PQ_TRAINING_SET_SIZE rows
        // or the one with the most rows if none are larger than MAX_PQ_TRAINING_SET_SIZE
        var indexes = new ArrayList<>(indexContext.getView().getIndexes());
        indexes.sort(Comparator.comparing(SSTableIndex::getSSTable, CompactionSSTable.maxTimestampDescending));

        CompressedVectors compressedVectors = null;
        long maxRows = 0;
        for (SSTableIndex index : indexes)
        {
            for (Segment segment : index.getSegments())
            {
                if (segment.metadata.numRows < maxRows)
                    continue;

                var searcher = segment.getIndexSearcher();
                assert searcher instanceof V2VectorIndexSearcher;
                var cv = ((V2VectorIndexSearcher) searcher).getCompressedVectors();
                if (preferredCompression.matches(cv))
                {
                    // We can exit now because we won't find a better candidate
                    if (segment.metadata.numRows > ProductQuantization.MAX_PQ_TRAINING_SET_SIZE)
                        return cv;

                    compressedVectors = cv;
                    maxRows = segment.metadata.numRows;
                }
            }
        }
        return compressedVectors;
    }

    private BiMap <Integer, Integer> buildOrdinalMap()
    {
        BiMap <Integer, Integer> ordinalMap = HashBiMap.create();
        int minRow = Integer.MAX_VALUE;
        int maxRow = Integer.MIN_VALUE;
        for (VectorPostings<T> vectorPostings : postingsMap.values())
        {
            if (vectorPostings.getRowIds().size() != 1)
            {
                return null;
            }
            int rowId = vectorPostings.getRowIds().getInt(0);
            int ordinal = vectorPostings.getOrdinal();
            minRow = Math.min(minRow, rowId);
            maxRow = Math.max(maxRow, rowId);
            if (ordinalMap.containsKey(ordinal))
            {
                return null;
            } else {
                ordinalMap.put(ordinal, rowId);
            }
        }

        if (minRow != 0 || maxRow != postingsMap.values().size() - 1)
        {
            return null;
        }
        return ordinalMap;
    }

    private long writePQ(SequentialWriter writer, IntUnaryOperator reverseOrdinalMapper, IndexContext indexContext) throws IOException
    {
        var preferredCompression = sourceModel.compressionProvider.apply(vectorValues.dimension());

        // Build encoder and compress vectors
        VectorCompressor<?> compressor; // will be null if we can't compress
        Object encoded = null; // byte[][], or long[][]
        boolean containsUnitVectors;
        // limit the PQ computation and encoding to one index at a time -- goal during flush is to
        // evict from memory ASAP so better to do the PQ build (in parallel) one at a time
        synchronized (CassandraOnHeapGraph.class)
        {
            // build encoder (expensive for PQ, cheaper for BQ)
            if (preferredCompression.type == VectorCompression.CompressionType.PRODUCT_QUANTIZATION)
            {
                var previousCV = getCompressedVectorsIfPresent(indexContext, preferredCompression);
                compressor = computeOrRefineFrom(previousCV, preferredCompression);
            }
            else
            {
                assert preferredCompression.type == VectorCompression.CompressionType.BINARY_QUANTIZATION : preferredCompression.type;
                compressor = BinaryQuantization.compute(vectorValues);
            }
            assert !vectorValues.isValueShared();
            // encode (compress) the vectors to save
            if (compressor != null)
                encoded = compressVectors(reverseOrdinalMapper, compressor);

            containsUnitVectors = IntStream.range(0, vectorValues.size())
                                           .parallel()
                                           .mapToObj(vectorValues::getVector)
                                           .allMatch(v -> Math.abs(VectorUtil.dotProduct(v, v) - 1.0f) < 0.01);
        }

        // version and optional fields
        writer.writeInt(CassandraDiskAnn.PQ_MAGIC);
        writer.writeInt(1); // version
        writer.writeBoolean(containsUnitVectors);

        // write the compression type
        var actualType = compressor == null ? VectorCompression.CompressionType.NONE : preferredCompression.type;
        writer.writeByte(actualType.ordinal());
        if (actualType == VectorCompression.CompressionType.NONE)
            return writer.position();

        // save (outside the synchronized block, this is io-bound not CPU)
        CompressedVectors cv;
        if (compressor instanceof BinaryQuantization)
            cv = new BQVectors((BinaryQuantization) compressor, (long[][]) encoded);
        else
            cv = new PQVectors((ProductQuantization) compressor, (ByteSequence<?>[]) encoded);
        cv.write(writer);
        return writer.position();
    }

    private VectorCompressor<?> computeOrRefineFrom(CompressedVectors previousCV, VectorCompression preferredCompression)
    {
        // refining an existing codebook is much faster than starting from scratch
        VectorCompressor<?> compressor;
        if (previousCV == null)
        {
            if (vectorValues.size() < 1024)
                compressor = null;
            else
                compressor = ProductQuantization.compute(vectorValues, preferredCompression.compressToBytes, 256, false);
        }
        else
        {
            if (vectorValues.size() < 1024)
                compressor = previousCV.getCompressor();
            else
                compressor = ((ProductQuantization) previousCV.getCompressor()).refine(vectorValues);
        }
        return compressor;
    }

    private Object compressVectors(IntUnaryOperator reverseOrdinalMapper, VectorCompressor<?> compressor)
    {
        if (compressor instanceof ProductQuantization)
            return IntStream.range(0, vectorValues.size()).parallel()
                       .mapToObj(i -> {
                           var v = vectorValues.getVector(reverseOrdinalMapper.applyAsInt(i));
                           return ((ProductQuantization) compressor).encode(v);
                       })
                       .toArray(ByteSequence<?>[]::new);
        else if (compressor instanceof BinaryQuantization)
            return IntStream.range(0, vectorValues.size()).parallel()
                            .mapToObj(i -> {
                                var v = vectorValues.getVector(reverseOrdinalMapper.applyAsInt(i));
                                return ((BinaryQuantization) compressor).encode(v);
                            })
                            .toArray(long[][]::new);
        throw new UnsupportedOperationException("Unrecognized compressor " + compressor.getClass());
    }

    public long ramBytesUsed()
    {
        return postingsBytesUsed() + vectorValues.ramBytesUsed() + builder.getGraph().ramBytesUsed();
    }

    private long postingsBytesUsed()
    {
        return RamEstimation.concurrentHashMapRamUsed(postingsByOrdinal.size()) // NBHM is close to CHM
               + 3 * RamEstimation.concurrentHashMapRamUsed(postingsMap.size()) // CSLM is much less efficient than CHM
               + postingsMap.values().stream().mapToLong(VectorPostings::ramBytesUsed).sum();
    }

    private long exactRamBytesUsed()
    {
        return ObjectSizes.measureDeep(this);
    }

    public enum InvalidVectorBehavior
    {
        IGNORE,
        FAIL
    }
}
