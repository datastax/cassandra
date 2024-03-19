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
import java.util.Comparator;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.IntUnaryOperator;
import java.util.stream.IntStream;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import org.cliffc.high_scale_lib.NonBlockingHashMap;
import org.cliffc.high_scale_lib.NonBlockingHashMapLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.jbellis.jvector.disk.OnDiskGraphIndex;
import io.github.jbellis.jvector.graph.GraphIndex;
import io.github.jbellis.jvector.graph.GraphIndexBuilder;
import io.github.jbellis.jvector.graph.GraphSearcher;
import io.github.jbellis.jvector.graph.NodeSimilarity;
import io.github.jbellis.jvector.graph.RandomAccessVectorValues;
import io.github.jbellis.jvector.graph.SearchResult;
import io.github.jbellis.jvector.pq.BQVectors;
import io.github.jbellis.jvector.pq.BinaryQuantization;
import io.github.jbellis.jvector.pq.CompressedVectors;
import io.github.jbellis.jvector.pq.PQVectors;
import io.github.jbellis.jvector.pq.ProductQuantization;
import io.github.jbellis.jvector.pq.VectorCompressor;
import io.github.jbellis.jvector.util.Bits;
import io.github.jbellis.jvector.util.RamUsageEstimator;
import io.github.jbellis.jvector.vector.VectorEncoding;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.VectorUtil;
import org.apache.cassandra.config.CassandraRelevantProperties;
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
import org.apache.cassandra.index.sai.disk.v2.V2VectorIndexSearcher;
import org.apache.cassandra.index.sai.disk.v3.CassandraDiskAnn;
import org.apache.cassandra.index.sai.utils.IndexFileUtils;
import org.apache.cassandra.index.sai.utils.SAICodecUtils;
import org.apache.cassandra.index.sai.utils.ScoredPrimaryKey;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.lucene.util.StringHelper;

public class CassandraOnHeapGraph<T>
{
    private static final Logger logger = LoggerFactory.getLogger(CassandraOnHeapGraph.class);

    private final ConcurrentVectorValues vectorValues;
    private final GraphIndexBuilder<float[]> builder;
    private final VectorType.VectorSerializer serializer;
    private final VectorSimilarityFunction similarityFunction;
    private final ConcurrentMap<float[], VectorPostings<T>> postingsMap;
    private final NonBlockingHashMapLong<VectorPostings<T>> postingsByOrdinal;
    private final NonBlockingHashMap<T, float[]> vectorsByKey;
    private final AtomicInteger nextOrdinal = new AtomicInteger();
    private final VectorSourceModel sourceModel;
    private volatile boolean hasDeletions;

    /**
     * @param termComparator the vector type -- passed as AbstractType for caller's convenience
     * @param indexConfig
     * @param forSearching if true, vectorsByKey will be initialized and populated with vectors as they are added
     */
    public CassandraOnHeapGraph(AbstractType<?> termComparator, IndexWriterConfig indexConfig, boolean forSearching)
    {
        serializer = (VectorType.VectorSerializer)termComparator.getSerializer();
        vectorValues = new ConcurrentVectorValues(((VectorType<?>) termComparator).dimension);
        similarityFunction = indexConfig.getSimilarityFunction();
        sourceModel = indexConfig.getSourceModel();
        // We need to be able to inexpensively distinguish different vectors, with a slower path
        // that identifies vectors that are equal but not the same reference.  A comparison-
        // based Map (which only needs to look at vector elements until a difference is found)
        // is thus a better option than hash-based (which has to look at all elements to compute the hash).
        postingsMap = new ConcurrentSkipListMap<>(Arrays::compare);
        postingsByOrdinal = new NonBlockingHashMapLong<>();
        vectorsByKey = forSearching ? new NonBlockingHashMap<>() : null;

        builder = new GraphIndexBuilder<>(vectorValues,
                                          VectorEncoding.FLOAT32,
                                          similarityFunction,
                                          indexConfig.getMaximumNodeConnections(),
                                          indexConfig.getConstructionBeamWidth(),
                                          1.2f,
                                          1.2f);
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

        var vector = serializer.deserializeFloatArray(term);
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
                validateIndexable(vector, similarityFunction);
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
            validateIndexable(vector, similarityFunction);
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
                bytesUsed += builder.addGraphNode(ordinal, vectorValues);
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

    // copied out of a Lucene PR -- hopefully committed soon
    public static final float MAX_FLOAT32_COMPONENT = 1E17f;
    public static float[] checkInBounds(float[] v) {
        for (int i = 0; i < v.length; i++) {
            if (!Float.isFinite(v[i])) {
                throw new IllegalArgumentException("non-finite value at vector[" + i + "]=" + v[i]);
            }

            if (Math.abs(v[i]) > MAX_FLOAT32_COMPONENT) {
                throw new IllegalArgumentException("Out-of-bounds value at vector[" + i + "]=" + v[i]);
            }
        }
        return v;
    }

    public static void validateIndexable(float[] vector, VectorSimilarityFunction similarityFunction)
    {
        try
        {
            checkInBounds(vector);
        }
        catch (IllegalArgumentException e)
        {
            throw new InvalidRequestException(e.getMessage());
        }

        if (similarityFunction == VectorSimilarityFunction.COSINE)
        {
            for (int i = 0; i < vector.length; i++)
            {
                if (vector[i] != 0)
                    return;
            }
            throw new InvalidRequestException("Zero vectors cannot be indexed or queried with cosine similarity");
        }
    }

    public Collection<T> keysFromOrdinal(int node)
    {
        return postingsByOrdinal.get(node).getPostings();
    }

    public float[] vectorForKey(T key)
    {
        if (vectorsByKey == null)
            throw new IllegalStateException("vectorsByKey is not initialized");
        return vectorsByKey.get(key);
    }

    public void remove(ByteBuffer term, T key)
    {
        assert term != null && term.remaining() != 0;

        var vector = serializer.deserializeFloatArray(term);
        var postings = postingsMap.get(vector);
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
            vectorsByKey.remove(key, vector);
    }

    /**
     * @return an itererator over {@link ScoredPrimaryKey} in the graph's {@link SearchResult} order
     */
    public CloseableIterator<SearchResult.NodeScore> search(QueryContext context, float[] queryVector, int limit, float threshold, Bits toAccept)
    {
        validateIndexable(queryVector, similarityFunction);

        // search() errors out when an empty graph is passed to it
        if (vectorValues.size() == 0)
            return CloseableIterator.emptyIterator();

        Bits bits = hasDeletions ? BitsUtil.bitsIgnoringDeleted(toAccept, postingsByOrdinal) : toAccept;
        // VSTODO re-use searcher objects
        GraphIndex<float[]> graph = builder.getGraph();
        var searcher = new GraphSearcher.Builder<>(graph.getView()).withConcurrentUpdates().build();
        NodeSimilarity.ExactScoreFunction scoreFunction = node2 -> {
            return similarityFunction.compare(queryVector, ((RandomAccessVectorValues<float[]>) vectorValues).vectorValue(node2));
        };
        var topK = sourceModel.topKFor(limit, null);
        var result = searcher.search(scoreFunction, null, topK, threshold, bits);
        Tracing.trace("ANN search visited {} in-memory nodes to return {} results", result.getVisitedCount(), result.getNodes().length);
        context.addAnnNodesVisited(result.getVisitedCount());
        // Threshold based searches do not support resuming the search.
        return threshold > 0 ? CloseableIterator.wrap(Arrays.stream(result.getNodes()).iterator())
                             : new AutoResumingNodeScoreIterator(searcher, result, context::addAnnNodesVisited, topK, true, null);
    }

    public SegmentMetadata.ComponentMetadataMap writeData(IndexDescriptor indexDescriptor, IndexContext indexContext, Function<T, Integer> postingTransformer) throws IOException
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

        try (var pqOutput = IndexFileUtils.instance.openOutput(indexDescriptor.fileFor(IndexComponent.PQ, indexContext), true);
             var postingsOutput = IndexFileUtils.instance.openOutput(indexDescriptor.fileFor(IndexComponent.POSTING_LISTS, indexContext), true);
             var indexOutput = IndexFileUtils.instance.openOutput(indexDescriptor.fileFor(IndexComponent.TERMS_DATA, indexContext), true))
        {
            SAICodecUtils.writeHeader(pqOutput);
            SAICodecUtils.writeHeader(postingsOutput);
            SAICodecUtils.writeHeader(indexOutput);

            var deletedOrdinals = new HashSet<Integer>();
            postingsMap.values().stream().filter(VectorPostings::isEmpty).forEach(vectorPostings -> deletedOrdinals.add(vectorPostings.getOrdinal()));
            // remove ordinals that don't have corresponding row ids due to partition/range deletion
            for (VectorPostings<T> vectorPostings : postingsMap.values())
            {
                vectorPostings.computeRowIds(postingTransformer);
                if (vectorPostings.shouldAppendDeletedOrdinal())
                    deletedOrdinals.add(vectorPostings.getOrdinal());
            }

            // map of existing ordinal to rowId (aka new ordinal if remapping is possible)
            // null if remapping is not possible
            final BiMap <Integer, Integer> ordinalMap = deletedOrdinals.isEmpty() ? buildOrdinalMap() : null;

            boolean canFastFindRows = ordinalMap != null && CassandraRelevantProperties.VSEARCH_11_9_UPGRADES.getBoolean();
            IntUnaryOperator ordinalMapper = canFastFindRows
                                                ? x -> ordinalMap.getOrDefault(x, x)
                                                : x -> x;
            IntUnaryOperator reverseOrdinalMapper = canFastFindRows
                                                        ? x -> ordinalMap.inverse().getOrDefault(x, x)
                                                        : x -> x;

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
            long termsOffset = indexOutput.getFilePointer();

            OnDiskGraphIndex.write(new RemappingOnDiskGraphIndex<>(builder.getGraph(), ordinalMapper, reverseOrdinalMapper),
                                   new RemappingRamAwareVectorValues(vectorValues, reverseOrdinalMapper),
                                   indexOutput.asSequentialWriter());
            long termsLength = indexOutput.getFilePointer() - termsOffset;

            // write footers/checksums
            SAICodecUtils.writeFooter(pqOutput);
            SAICodecUtils.writeFooter(postingsOutput);
            SAICodecUtils.writeFooter(indexOutput);

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
                                           .mapToObj(vectorValues::vectorValue)
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
            cv = new PQVectors((ProductQuantization) compressor, (byte[][]) encoded);
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
                compressor = ProductQuantization.compute(vectorValues, preferredCompression.compressToBytes, false);
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
                       .mapToObj(i -> ((ProductQuantization) compressor).encode(vectorValues.vectorValue(reverseOrdinalMapper.applyAsInt(i))))
                       .toArray(byte[][]::new);
        else if (compressor instanceof BinaryQuantization)
            return IntStream.range(0, vectorValues.size()).parallel()
                            .mapToObj(i -> ((BinaryQuantization) compressor).encode(vectorValues.vectorValue(reverseOrdinalMapper.applyAsInt(i))))
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
