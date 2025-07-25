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

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.IntStream;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.jbellis.jvector.graph.GraphIndexBuilder;
import io.github.jbellis.jvector.graph.ListRandomAccessVectorValues;
import io.github.jbellis.jvector.graph.disk.feature.Feature;
import io.github.jbellis.jvector.graph.disk.feature.FeatureId;
import io.github.jbellis.jvector.graph.disk.feature.InlineVectors;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndex;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndexWriter;
import io.github.jbellis.jvector.graph.disk.OrdinalMapper;
import io.github.jbellis.jvector.graph.similarity.BuildScoreProvider;
import io.github.jbellis.jvector.quantization.BQVectors;
import io.github.jbellis.jvector.quantization.BinaryQuantization;
import io.github.jbellis.jvector.quantization.MutableBQVectors;
import io.github.jbellis.jvector.quantization.MutableCompressedVectors;
import io.github.jbellis.jvector.quantization.MutablePQVectors;
import io.github.jbellis.jvector.quantization.PQVectors;
import io.github.jbellis.jvector.quantization.ProductQuantization;
import io.github.jbellis.jvector.quantization.VectorCompressor;
import io.github.jbellis.jvector.util.Accountable;
import io.github.jbellis.jvector.util.RamUsageEstimator;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.hash.serialization.BytesReader;
import net.openhft.chronicle.hash.serialization.BytesWriter;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import org.agrona.collections.Int2ObjectHashMap;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.index.sai.disk.format.IndexComponents;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.disk.v2.V2VectorPostingsWriter;
import org.apache.cassandra.index.sai.disk.v5.V5OnDiskFormat;
import org.apache.cassandra.index.sai.disk.v5.V5VectorPostingsWriter;
import org.apache.cassandra.index.sai.disk.v5.V5VectorPostingsWriter.Structure;
import org.apache.cassandra.index.sai.disk.vector.VectorPostings.CompactionVectorPostings;
import org.apache.cassandra.index.sai.utils.LowPriorityThreadFactory;
import org.apache.cassandra.index.sai.utils.SAICodecUtils;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.service.StorageService;

import static java.lang.Math.max;
import static java.lang.Math.min;

public class CompactionGraph implements Closeable, Accountable
{
    private static final Logger logger = LoggerFactory.getLogger(CompactionGraph.class);
    private static final VectorTypeSupport vts = VectorizationProvider.getInstance().getVectorTypeSupport();

    private static final ForkJoinPool compactionFjp = new ForkJoinPool(Runtime.getRuntime().availableProcessors(),
                                                                       new LowPriorityThreadFactory(),
                                                                       null,
                                                                       false);
    // see comments to JVector PhysicalCoreExecutor -- HT tends to cause contention for the SIMD units
    private static final ForkJoinPool compactionSimdPool = new ForkJoinPool(Runtime.getRuntime().availableProcessors() / 2,
                                                                            new LowPriorityThreadFactory(),
                                                                            null,
                                                                            false);

    @VisibleForTesting
    public static int PQ_TRAINING_SIZE = ProductQuantization.MAX_PQ_TRAINING_SET_SIZE;

    private final VectorType.VectorSerializer serializer;
    private final VectorSimilarityFunction similarityFunction;
    private final ChronicleMap<VectorFloat<?>, CompactionVectorPostings> postingsMap;
    private final IndexComponents.ForWrite perIndexComponents;
    private final IndexContext context;
    private final boolean unitVectors;
    private final int postingsEntriesAllocated;
    private final File postingsFile;
    private final File termsFile;
    private final int dimension;
    private Structure postingsStructure;
    private OnDiskGraphIndexWriter writer;
    private final long termsOffset;
    private int lastRowId = -1;
    // if `useSyntheticOrdinals` is true then we use `nextOrdinal` to avoid holes, otherwise use rowId as source of ordinals
    private final boolean useSyntheticOrdinals;
    private int nextOrdinal = 0;

    // protects the fine-tuning changes (done in maybeAddVector) from addGraphNode threads
    // (and creates happens-before events so we don't need to mark the other fields volatile)
    private final ReadWriteLock trainingLock = new ReentrantReadWriteLock();
    private boolean pqFinetuned = false;
    // not final; will be updated to different objects after fine-tuning
    private VectorCompressor<?> compressor;
    private MutableCompressedVectors compressedVectors;
    private GraphIndexBuilder builder;

    public CompactionGraph(IndexComponents.ForWrite perIndexComponents, VectorCompressor<?> compressor, boolean unitVectors, long keyCount, boolean allRowsHaveVectors) throws IOException
    {
        this.perIndexComponents = perIndexComponents;
        this.context = perIndexComponents.context();
        this.unitVectors = unitVectors;
        var indexConfig = context.getIndexWriterConfig();
        var termComparator = context.getValidator();
        dimension = ((VectorType<?>) termComparator).dimension;

        // We need to tell Chronicle Map (CM) how many entries to expect.  it's critical not to undercount,
        // or CM will crash.  However, we don't want to just pass in a max entries count of 2B, since it eagerly
        // allocated segments for that many entries, which takes about 25s.
        //
        // If our estimate turns out to be too small, it's not the end of the world, we'll flush this segment
        // and start another to avoid crashing CM.  But we'd rather not do this because the whole goal of
        // CompactionGraph is to write one segment only.
        var dd = perIndexComponents.descriptor();
        var rowsPerKey = max(1, Keyspace.open(dd.ksname).getColumnFamilyStore(dd.cfname).getMeanRowsPerPartition());
        long estimatedRows = (long) (1.1 * keyCount * rowsPerKey); // 10% fudge factor
        int maxRowsInGraph = Integer.MAX_VALUE - 100_000; // leave room for a few more async additions until we flush
        postingsEntriesAllocated = max(1000, (int) min(estimatedRows, maxRowsInGraph));

        serializer = (VectorType.VectorSerializer) termComparator.getSerializer();
        similarityFunction = indexConfig.getSimilarityFunction();
        postingsStructure = Structure.ONE_TO_ONE; // until proven otherwise
        this.compressor = compressor;
        // `allRowsHaveVectors` only tells us about data for which we have already built indexes; if we
        // are adding previously unindexed data then we could still encounter rows with null vectors,
        // so this is just a best guess.  If the guess is wrong then the penalty is that we end up
        // with "holes" in the ordinal sequence (and pq and data files) which we would prefer to avoid
        // (hence the effort to predict `allRowsHaveVectors`) but will not cause correctness issues,
        // and the next compaction will fill in the holes.
        this.useSyntheticOrdinals = !V5OnDiskFormat.writeV5VectorPostings() || !allRowsHaveVectors;

        // the extension here is important to signal to CFS.scrubDataDirectories that it should be removed if present at restart
        Component tmpComponent = new Component(Component.Type.CUSTOM, "chronicle" + Descriptor.TMP_EXT);
        postingsFile = dd.fileFor(tmpComponent);
        postingsMap = ChronicleMapBuilder.of((Class<VectorFloat<?>>) (Class) VectorFloat.class, (Class<CompactionVectorPostings>) (Class) CompactionVectorPostings.class)
                                         .averageKeySize(dimension * Float.BYTES)
                                         .averageValueSize(VectorPostings.emptyBytesUsed() + RamUsageEstimator.NUM_BYTES_OBJECT_REF + 2 * Integer.BYTES)
                                         .keyMarshaller(new VectorFloatMarshaller())
                                         .valueMarshaller(new VectorPostings.Marshaller())
                                         .entries(postingsEntriesAllocated)
                                         .createPersistedTo(postingsFile.toJavaIOFile());

        // VSTODO add LVQ
        BuildScoreProvider bsp;
        if (compressor instanceof ProductQuantization)
        {
            compressedVectors = new MutablePQVectors((ProductQuantization) compressor);
            bsp = BuildScoreProvider.pqBuildScoreProvider(similarityFunction, (PQVectors) compressedVectors);
        }
        else if (compressor instanceof BinaryQuantization)
        {
            var bq = new BinaryQuantization(dimension);
            compressedVectors = new MutableBQVectors(bq);
            bsp = BuildScoreProvider.bqBuildScoreProvider((BQVectors) compressedVectors);
        }
        else
        {
            throw new IllegalArgumentException("Unsupported compressor: " + compressor);
        }
        int jvectorVersion = Version.current().onDiskFormat().jvectorFileFormatVersion();
        if (indexConfig.isHierarchyEnabled() && jvectorVersion < 4)
            logger.warn("Hierarchical graphs configured but node configured with V3OnDiskFormat.JVECTOR_VERSION {}. " +
                        "Skipping setting for {}", jvectorVersion, indexConfig.getIndexName());

        builder = new GraphIndexBuilder(bsp,
                                        dimension,
                                        indexConfig.getAnnMaxDegree(),
                                        indexConfig.getConstructionBeamWidth(),
                                        indexConfig.getNeighborhoodOverflow(1.2f),
                                        indexConfig.getAlpha(dimension > 3 ? 1.2f : 1.4f),
                                        indexConfig.isHierarchyEnabled() && jvectorVersion >= 4,
                                        true, // We always refine during compaction
                                        compactionSimdPool,
                                        compactionFjp);

        termsFile = perIndexComponents.addOrGet(IndexComponentType.TERMS_DATA).file();
        termsOffset = (termsFile.exists() ? termsFile.length() : 0)
                      + SAICodecUtils.headerSize();
        // placeholder writer, will be replaced at flush time when we finalize the index contents
        writer = createTermsWriter(new OrdinalMapper.IdentityMapper(maxRowsInGraph));
        writer.getOutput().seek(termsFile.length()); // position at the end of the previous segment before writing our own header
        SAICodecUtils.writeHeader(SAICodecUtils.toLuceneOutput(writer.getOutput()));
    }

    private OnDiskGraphIndexWriter createTermsWriter(OrdinalMapper ordinalMapper) throws IOException
    {
        return new OnDiskGraphIndexWriter.Builder(builder.getGraph(), termsFile.toPath())
               .withStartOffset(termsOffset)
               .with(new InlineVectors(dimension))
               .withVersion(Version.current().onDiskFormat().jvectorFileFormatVersion())
               .withMapper(ordinalMapper)
               .build();
    }

    @Override
    public void close() throws IOException
    {
        // this gets called in `finally` blocks, so use closeQuietly to avoid generating additional exceptions
        FileUtils.closeQuietly(writer);
        FileUtils.closeQuietly(postingsMap);
        Files.delete(postingsFile.toJavaIOFile().toPath());
    }

    public int size()
    {
        return builder.getGraph().size();
    }

    public boolean isEmpty()
    {
        return postingsMap.values().stream().allMatch(VectorPostings::isEmpty);
    }

    /**
     * @return the result of adding the given (vector) term; see {@link InsertionResult}
     */
    public InsertionResult maybeAddVector(ByteBuffer term, int segmentRowId) throws IOException
    {
        assert term != null && term.remaining() != 0;

        var vector = vts.createFloatVector(serializer.deserializeFloatArray(term));
        // Validate the vector.  Since we are compacting, invalid vectors are ignored instead of failing the operation.
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
            return new InsertionResult(0);
        }

        // if we don't see sequential rowids, it means the skipped row(s) have null vectors
        if (segmentRowId != lastRowId + 1)
            postingsStructure = Structure.ZERO_OR_ONE_TO_MANY;
        lastRowId = segmentRowId;

        var bytesUsed = 0L;
        var postings = postingsMap.get(vector);
        if (postings == null)
        {
            // add a new entry
            // this all runs on the same compaction thread, so we don't need to worry about concurrency
            int ordinal = useSyntheticOrdinals ? nextOrdinal++ : segmentRowId;
            postings = new CompactionVectorPostings(ordinal, segmentRowId);
            postingsMap.put(vector, postings);

            // fine-tune the PQ if we've collected enough vectors
            if (compressor instanceof ProductQuantization && !pqFinetuned && postingsMap.size() >= PQ_TRAINING_SIZE)
            {
                // walk the on-disk Postings once to build (1) a dense list of vectors with no missing entries or zeros
                // and (2) a map of vectors keyed by ordinal
                var trainingVectors = new ArrayList<VectorFloat<?>>(postingsMap.size());
                var vectorsByOrdinal = new Int2ObjectHashMap<VectorFloat<?>>();
                postingsMap.forEach((v, p) -> {
                    var vectorClone = v.copy();
                    trainingVectors.add(vectorClone);
                    vectorsByOrdinal.put(p.getOrdinal(), vectorClone);
                });

                // lock the addGraphNode threads out so they don't try to use old pq codepoints against the new codebook
                trainingLock.writeLock().lock();
                try
                {
                    // Fine tune the pq codebook
                    compressor = ((ProductQuantization) compressor).refine(new ListRandomAccessVectorValues(trainingVectors, dimension));
                    trainingVectors.clear(); // don't need these anymore so let GC reclaim if it wants to

                    // re-encode the vectors added so far
                    int encodedVectorCount = compressedVectors.count();
                    compressedVectors = new MutablePQVectors((ProductQuantization) compressor);
                    compactionFjp.submit(() -> {
                        IntStream.range(0, encodedVectorCount)
                                 .parallel()
                                 .forEach(i -> {
                                     var v = vectorsByOrdinal.get(i);
                                     if (v == null)
                                         compressedVectors.setZero(i);
                                     else
                                         compressedVectors.encodeAndSet(i, v);
                                 });
                    }).join();

                    // Keep the existing edges but recompute their scores
                    builder = GraphIndexBuilder.rescore(builder, BuildScoreProvider.pqBuildScoreProvider(similarityFunction, (PQVectors) compressedVectors));
                }
                finally
                {
                    trainingLock.writeLock().unlock();
                }
                pqFinetuned = true;
            }

            writer.writeInline(ordinal, Feature.singleState(FeatureId.INLINE_VECTORS, new InlineVectors.State(vector)));
            // Fill in any holes in the pqVectors (setZero has the side effect of increasing the count)
            while (compressedVectors.count() < ordinal)
                compressedVectors.setZero(compressedVectors.count());
            compressedVectors.encodeAndSet(ordinal, vector);

            bytesUsed += postings.ramBytesUsed();
            return new InsertionResult(bytesUsed, ordinal, vector);
        }

        // postings list already exists, just add the new key
        if (postingsStructure == Structure.ONE_TO_ONE)
            postingsStructure = Structure.ONE_TO_MANY;
        var newPosting = postings.add(segmentRowId);
        assert newPosting;
        bytesUsed += postings.bytesPerPosting();
        postingsMap.put(vector, postings); // re-serialize to disk

        return new InsertionResult(bytesUsed);
    }

    public long addGraphNode(InsertionResult result)
    {
        trainingLock.readLock().lock();
        try
        {
            return builder.addGraphNode(result.ordinal, result.vector);
        }
        finally
        {
            trainingLock.readLock().unlock();
        }
    }

    public SegmentMetadata.ComponentMetadataMap flush() throws IOException
    {
        // header is required to write the postings, but we need to recreate the writer after that with an accurate OrdinalMapper
        writer.writeHeader();
        writer.close();

        int nInProgress = builder.insertsInProgress();
        assert nInProgress == 0 : String.format("Attempting to write graph while %d inserts are in progress", nInProgress);
        assert !useSyntheticOrdinals || nextOrdinal == builder.getGraph().size() : String.format("nextOrdinal %d != graph size %d -- ordinals should be sequential",
                                                                                                 nextOrdinal, builder.getGraph().size());
        assert compressedVectors.count() == builder.getGraph().getIdUpperBound() : String.format("Largest vector id %d != largest graph id %d",
                                                                                                 compressedVectors.count(), builder.getGraph().getIdUpperBound());
        assert postingsMap.keySet().size() == builder.getGraph().size() : String.format("postings map entry count %d != vector count %d",
                                                                                        postingsMap.keySet().size(), builder.getGraph().size());
        if (logger.isDebugEnabled())
        {
            logger.debug("Writing graph with {} rows and {} distinct vectors",
                         postingsMap.values().stream().mapToInt(VectorPostings::size).sum(), builder.getGraph().size());
            logger.debug("Estimated size is {} + {}", compressedVectors.ramBytesUsed(), builder.getGraph().ramBytesUsed());
        }

        try (var postingsOutput = perIndexComponents.addOrGet(IndexComponentType.POSTING_LISTS).openOutput(true);
             var pqOutput = perIndexComponents.addOrGet(IndexComponentType.PQ).openOutput(true))
        {
            SAICodecUtils.writeHeader(postingsOutput);
            SAICodecUtils.writeHeader(pqOutput);

            // write PQ (time to do this is negligible, don't bother doing it async)
            long pqOffset = pqOutput.getFilePointer();
            CassandraOnHeapGraph.writePqHeader(pqOutput.asSequentialWriter(), unitVectors, VectorCompression.CompressionType.PRODUCT_QUANTIZATION);
            compressedVectors.write(pqOutput.asSequentialWriter(), Version.current().onDiskFormat().jvectorFileFormatVersion());
            long pqLength = pqOutput.getFilePointer() - pqOffset;

            // write postings asynchronously while we run cleanup()
            var ordinalMapper = new AtomicReference<OrdinalMapper>();
            long postingsOffset = postingsOutput.getFilePointer();
            var es = Executors.newSingleThreadExecutor(new NamedThreadFactory("CompactionGraphPostingsWriter"));
            long postingsLength;
            try (var indexHandle = perIndexComponents.get(IndexComponentType.TERMS_DATA).createIndexBuildTimeFileHandle();
                 var index = OnDiskGraphIndex.load(indexHandle::createReader, termsOffset))
            {
                var postingsFuture = es.submit(() -> {
                    // V2 doesn't support ONE_TO_MANY so force it to ZERO_OR_ONE_TO_MANY if necessary;
                    // similarly, if we've been using synthetic ordinals then we can't map to ONE_TO_MANY
                    // (ending up at ONE_TO_MANY when the source sstables were not is unusual, but possible,
                    // if a row with null vector in sstable A gets updated with a vector in sstable B)
                    if (postingsStructure == Structure.ONE_TO_MANY
                        && (!V5OnDiskFormat.writeV5VectorPostings() || useSyntheticOrdinals))
                    {
                        postingsStructure = Structure.ZERO_OR_ONE_TO_MANY;
                    }
                    var rp = V5VectorPostingsWriter.describeForCompaction(postingsStructure,
                                                                          builder.getGraph().size(),
                                                                          postingsMap);
                    ordinalMapper.set(rp.ordinalMapper);
                    try (var view = index.getView())
                    {
                        if (V5OnDiskFormat.writeV5VectorPostings())
                        {
                            return new V5VectorPostingsWriter<Integer>(rp).writePostings(postingsOutput.asSequentialWriter(), view, postingsMap);
                        }
                        else
                        {
                            assert postingsStructure == Structure.ONE_TO_ONE || postingsStructure == Structure.ZERO_OR_ONE_TO_MANY;
                            return new V2VectorPostingsWriter<Integer>(postingsStructure == Structure.ONE_TO_ONE, builder.getGraph().size(), rp.ordinalMapper::newToOld)
                                   .writePostings(postingsOutput.asSequentialWriter(), view, postingsMap, Set.of());
                        }
                    }
                });

                // complete internal graph clean up
                builder.cleanup();

                // wait for postings to finish writing and clean up related resources
                long postingsEnd = postingsFuture.get();
                postingsLength = postingsEnd - postingsOffset;
                es.shutdown();
            }

            // Recreate the writer with the final ordinalMapper
            writer = createTermsWriter(ordinalMapper.get());

            // write the graph edge lists and optionally fused adc features
            var start = System.nanoTime();
            // Required becuase jvector 3 wrote the fused adc map here. We no longer write jvector 3, but we still
            // write out the empty map.
            writer.write(Map.of());
            SAICodecUtils.writeFooter(writer.getOutput(), writer.checksum());
            logger.info("Writing graph took {}ms", (System.nanoTime() - start) / 1_000_000);
            long termsLength = writer.getOutput().position() - termsOffset;

            // write remaining footers/checksums
            SAICodecUtils.writeFooter(pqOutput);
            SAICodecUtils.writeFooter(postingsOutput);

            // add components to the metadata map
            return CassandraOnHeapGraph.createMetadataMap(termsOffset, termsLength, postingsOffset, postingsLength, pqOffset, pqLength);
        }
        catch (ExecutionException | InterruptedException e)
        {
            throw new RuntimeException(e);
        }
    }

    public long ramBytesUsed()
    {
        return compressedVectors.ramBytesUsed() + builder.getGraph().ramBytesUsed();
    }

    public boolean requiresFlush()
    {
        return builder.getGraph().size() >= postingsEntriesAllocated;
    }

    private static class VectorFloatMarshaller implements BytesReader<VectorFloat<?>>, BytesWriter<VectorFloat<?>> {
        @Override
        public void write(Bytes out, VectorFloat<?> vector) {
            out.writeInt(vector.length());
            for (int i = 0; i < vector.length(); i++) {
                out.writeFloat(vector.get(i));
            }
        }

        @Override
        public VectorFloat<?> read(Bytes in, VectorFloat<?> using) {
            int length = in.readInt();
            if (using == null) {
                float[] data = new float[length];
                for (int i = 0; i < length; i++) {
                    data[i] = in.readFloat();
                }
                return vts.createFloatVector(data);
            }

            for (int i = 0; i < length; i++) {
                using.set(i, in.readFloat());
            }
            return using;
        }
    }

    /**
     * AddResult is a container for the result of maybeAddVector.  If this call resulted in a new
     * vector being added to the graph, then `ordinal` and `vector` fields will be populated, otherwise
     * they will be null.
     * <p>
     * bytesUsed is always populated and always non-negative (it will be smaller, but not zero,
     * when adding a vector that already exists in the graph to a new row).
     */
    public static class InsertionResult
    {
        public final long bytesUsed;
        public final Integer ordinal;
        public final VectorFloat<?> vector;

        public InsertionResult(long bytesUsed, Integer ordinal, VectorFloat<?> vector)
        {
            this.bytesUsed = bytesUsed;
            this.ordinal = ordinal;
            this.vector = vector;
        }

        public InsertionResult(long bytesUsed)
        {
            this(bytesUsed, null, null);
        }
    }
}
