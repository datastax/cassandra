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
import java.util.EnumMap;
import java.util.Set;
import java.util.concurrent.ForkJoinPool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.jbellis.jvector.graph.GraphIndexBuilder;
import io.github.jbellis.jvector.graph.disk.Feature;
import io.github.jbellis.jvector.graph.disk.FeatureId;
import io.github.jbellis.jvector.graph.disk.InlineVectorValues;
import io.github.jbellis.jvector.graph.disk.InlineVectors;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndexWriter;
import io.github.jbellis.jvector.graph.similarity.BuildScoreProvider;
import io.github.jbellis.jvector.pq.PQVectors;
import io.github.jbellis.jvector.pq.ProductQuantization;
import io.github.jbellis.jvector.util.Accountable;
import io.github.jbellis.jvector.util.PhysicalCoreExecutor;
import io.github.jbellis.jvector.util.RamUsageEstimator;
import io.github.jbellis.jvector.vector.ArrayByteSequence;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.ByteSequence;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.hash.serialization.BytesReader;
import net.openhft.chronicle.hash.serialization.BytesWriter;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.disk.vector.VectorPostings.CompactionVectorPostings;
import org.apache.cassandra.index.sai.utils.IndexFileUtils;
import org.apache.cassandra.index.sai.utils.SAICodecUtils;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ObjectSizes;

import static org.apache.cassandra.index.sai.disk.v3.V3OnDiskFormat.JVECTOR_2_VERSION;

public class CompactionGraph implements Closeable, Accountable
{
    private static final Logger logger = LoggerFactory.getLogger(CompactionGraph.class);
    private static final VectorTypeSupport vts = VectorizationProvider.getInstance().getVectorTypeSupport();

    private final GraphIndexBuilder builder;
    private final VectorType.VectorSerializer serializer;
    private final VectorSimilarityFunction similarityFunction;
    private final ChronicleMap<VectorFloat<?>, CompactionVectorPostings> postingsMap;
    private final InlineVectorValues inlineVectors;
    private final PQVectors pqVectors;
    private final ArrayList<ByteSequence<?>> pqVectorsList;
    private final IndexDescriptor descriptor;
    private final IndexContext context;
    private final boolean unitVectors;
    private final int entriesAllocated;
    private final File postingsFile;
    private boolean postingsOneToOne;
    private int nextOrdinal = 0;
    private final ProductQuantization compressor;
    private final OnDiskGraphIndexWriter writer;
    private final long termsOffset;

    public CompactionGraph(IndexDescriptor descriptor, IndexContext context, ProductQuantization compressor, boolean unitVectors, long keyCount) throws IOException
    {
        this.descriptor = descriptor;
        this.context = context;
        this.unitVectors = unitVectors;
        var indexConfig = context.getIndexWriterConfig();
        var termComparator = context.getValidator();
        int dimension = ((VectorType<?>) termComparator).dimension;

        // We need to tell Chronicle Map (CM) how many entries to expect.  it's critical not to undercount,
        // or CM will crash.  However, we don't want to just pass in a max entries count of 2B, since it eagerly
        // allocated segments for that many entries, which takes about 25s.
        //
        // If our estimate turns out to be too small, it's not the end of the world, we'll flush this segment
        // and start another to avoid crashing CM.  But we'd rather not do this because the whole goal of
        // CompactionGraph is to write one segment only.
        var dd = descriptor.descriptor;
        var rowsPerKey = Keyspace.open(dd.ksname).getColumnFamilyStore(dd.cfname).getMeanRowsPerPartition();
        long estimatedRows = (long) (1.1 * keyCount * rowsPerKey); // 10% fudge factor
        int maxRowsInGraph = Integer.MAX_VALUE - 100_000; // leave room for a few more async additions until we flush
        entriesAllocated = estimatedRows > maxRowsInGraph ? maxRowsInGraph : (int) estimatedRows;

        serializer = (VectorType.VectorSerializer) termComparator.getSerializer();
        similarityFunction = indexConfig.getSimilarityFunction();
        postingsOneToOne = true;
        this.compressor = compressor;

        // the extension here is important to signal to CFS.scrubDataDirectories that it should be removed if present at restart
        Component tmpComponent = new Component(Component.Type.CUSTOM, "chronicle" + Descriptor.TMP_EXT);
        postingsFile = descriptor.descriptor.fileFor(tmpComponent);
        postingsMap = ChronicleMapBuilder.of((Class<VectorFloat<?>>) (Class) VectorFloat.class, (Class<CompactionVectorPostings>) (Class) CompactionVectorPostings.class)
                                         .averageKeySize(dimension * Float.BYTES)
                                         .averageValueSize(VectorPostings.emptyBytesUsed() + RamUsageEstimator.NUM_BYTES_OBJECT_REF + 2 * Integer.BYTES)
                                         .keyMarshaller(new VectorFloatMarshaller())
                                         .valueMarshaller(new VectorPostings.Marshaller())
                                         .entries(entriesAllocated)
                                         .createPersistedTo(postingsFile.toJavaIOFile());

        builder = new GraphIndexBuilder(null,
                                        dimension,
                                        indexConfig.getMaximumNodeConnections(),
                                        indexConfig.getConstructionBeamWidth(),
                                        1.2f,
                                        dimension > 3 ? 1.2f : 1.4f,
                                        PhysicalCoreExecutor.pool(), ForkJoinPool.commonPool());

        var indexFile = descriptor.fileFor(IndexComponent.TERMS_DATA, context);
        termsOffset = (indexFile.exists() ? indexFile.length() : 0)
                      + SAICodecUtils.headerSize();
        writer = new OnDiskGraphIndexWriter.Builder(builder.getGraph(), indexFile.toPath())
                 .withVersion(JVECTOR_2_VERSION) // VSTODO old version until we add LVQ
                 .withStartOffset(termsOffset)
                 .with(new InlineVectors(dimension))
                 .withMapper(new OnDiskGraphIndexWriter.IdentityMapper())
                 .build();
        writer.getOutput().seek(indexFile.length()); // position at the end of the previous segment before writing our own header
        SAICodecUtils.writeHeader(SAICodecUtils.toLuceneOutput(writer.getOutput()));
        inlineVectors = new InlineVectorValues(dimension, writer);
        pqVectorsList = new ArrayList<>(entriesAllocated);
        pqVectors = new PQVectors(compressor, pqVectorsList);
        // VSTODO add LVQ
        builder.setBuildScoreProvider(BuildScoreProvider.pqBuildScoreProvider(similarityFunction, inlineVectors, pqVectors));
    }

    @Override
    public void close() throws IOException
    {
        // this gets called in finally{} blocks, so use closeQuietly to avoid generating additional exceptions
        FileUtils.closeQuietly(inlineVectors);
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
    public InsertionResult maybeAddVector(ByteBuffer term, Integer key) throws IOException
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

        var bytesUsed = 0L;
        var postings = postingsMap.get(vector);
        if (postings == null)
        {
            // add a new entry
            // this all runs on the same compaction thread, so we don't need to worry about concurrency
            int ordinal = nextOrdinal++;
            postings = new CompactionVectorPostings(ordinal, key);
            postingsMap.put(vector, postings);
            writer.writeInline(ordinal, Feature.singleState(FeatureId.INLINE_VECTORS, new InlineVectors.State(vector)));
            var encoded = (ArrayByteSequence) compressor.encode(vector);
            pqVectorsList.add(encoded);

            bytesUsed += RamEstimation.concurrentHashMapRamUsed(1); // the new posting Map entry
            bytesUsed += encoded.get().length;
            bytesUsed += VectorPostings.emptyBytesUsed() + VectorPostings.bytesPerPosting();
            return new InsertionResult(bytesUsed, ordinal, vector);
        }

        // postings list already exists, just add the new key (if it's not already in the list)
        postingsOneToOne = false;
        if (postings.add(key))
            bytesUsed += VectorPostings.bytesPerPosting();
        return new InsertionResult(bytesUsed);
    }

    public long addGraphNode(InsertionResult result)
    {
        return builder.addGraphNode(result.ordinal, result.vector);
    }

    public SegmentMetadata.ComponentMetadataMap flush(IndexDescriptor __, Set<Integer> deletedOrdinals) throws IOException
    {
        assert deletedOrdinals.isEmpty(); // this is only to provide a consistent api with CassandraOnHeapGraph

        int nInProgress = builder.insertsInProgress();
        assert nInProgress == 0 : String.format("Attempting to write graph while %d inserts are in progress", nInProgress);
        assert nextOrdinal == builder.getGraph().size() : String.format("nextOrdinal %d != graph size %d -- ordinals should be sequential",
                                                                        nextOrdinal, builder.getGraph().size());
        assert pqVectors.count() == builder.getGraph().size() : String.format("vector count %d != graph size %d",
                                                                              pqVectors.count(), builder.getGraph().size());
        assert postingsMap.keySet().size() == builder.getGraph().size() : String.format("postings map entry count %d != vector count %d",
                                                                                        postingsMap.keySet().size(), builder.getGraph().size());
        logger.debug("Writing graph with {} rows and {} distinct vectors",
                     postingsMap.values().stream().mapToInt(VectorPostings::size).sum(), builder.getGraph().size());

        try (var postingsOutput = IndexFileUtils.instance.openOutput(descriptor.fileFor(IndexComponent.POSTING_LISTS, context), true);
             var pqOutput = IndexFileUtils.instance.openOutput(descriptor.fileFor(IndexComponent.PQ, context), true))
        {
            SAICodecUtils.writeHeader(postingsOutput);
            SAICodecUtils.writeHeader(pqOutput);

            // write PQ
            long pqOffset = pqOutput.getFilePointer();
            CassandraOnHeapGraph.writePqHeader(pqOutput.asSequentialWriter(), unitVectors, VectorCompression.CompressionType.PRODUCT_QUANTIZATION);
            pqVectors.write(pqOutput.asSequentialWriter(), JVECTOR_2_VERSION); // VSTODO old version until we add LVQ
            long pqLength = pqOutput.getFilePointer() - pqOffset;

            // write postings
            long postingsOffset = postingsOutput.getFilePointer();
            long postingsPosition = new VectorPostingsWriter<Integer>(postingsOneToOne, i -> i)
                                            .writePostings(postingsOutput.asSequentialWriter(), inlineVectors, postingsMap, Set.of());
            long postingsLength = postingsPosition - postingsOffset;

            // complete (internal clean up) and write the graph
            builder.cleanup();

            var start = System.nanoTime();
            writer.write(new EnumMap<>(FeatureId.class));
            SAICodecUtils.writeFooter(writer.getOutput(), writer.checksum());
            logger.info("Writing graph took {}ms", (System.nanoTime() - start) / 1_000_000);
            long termsLength = writer.getOutput().position() - termsOffset;

            // write remaining footers/checksums
            SAICodecUtils.writeFooter(pqOutput);
            SAICodecUtils.writeFooter(postingsOutput);

            // add components to the metadata map
            return CassandraOnHeapGraph.createMetadataMap(termsOffset, termsLength, postingsOffset, postingsLength, pqOffset, pqLength);
        }
    }

    public long ramBytesUsed()
    {
        return pqVectors.ramBytesUsed() + builder.getGraph().ramBytesUsed();
    }

    private long exactRamBytesUsed()
    {
        return ObjectSizes.measureDeep(this);
    }

    public boolean requiresFlush()
    {
        return nextOrdinal >= entriesAllocated;
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
