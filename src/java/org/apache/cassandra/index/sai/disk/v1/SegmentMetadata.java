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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SSTableContext;
import org.apache.cassandra.index.sai.disk.ModernResettableByteBuffersIndexOutput;
import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.PrimaryKeyWithSource;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.disk.io.IndexInput;
import org.apache.cassandra.index.sai.disk.io.IndexOutput;
import org.apache.cassandra.index.sai.disk.v6.TermsDistribution;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

/**
 * Multiple {@link SegmentMetadata} are stored in {@link IndexComponentType#META} file, each corresponds to an on-disk
 * index segment.
 */
public class SegmentMetadata implements Comparable<SegmentMetadata>
{
    private static final String NAME = "SegmentMetadata";

    public final Version version;

    /**
     * Used to retrieve sstableRowId which equals to offset plus segmentRowId.
     */
    public final long segmentRowIdOffset;

    /**
     * Min and max sstable rowId in current segment.
     *
     * For index generated by compaction, minSSTableRowId is the same as segmentRowIdOffset.
     * But for flush, segmentRowIdOffset is taken from previous segment's maxSSTableRowId.
     */
    public final long minSSTableRowId;
    public final long maxSSTableRowId;

    /**
     * number of indexed rows (aka. pair of term and segmentRowId) in current segment
     */
    public final long numRows;
    /**
     * Represents the total count of terms in a segment.
     * It used to caclulate the average document length for BM25.
     */
    public final long totalTermCount;
    /**
     * A constant representing an invalid total term count when it cannot be read
     * from disk, since the SAI format version doesn't support serializing it.
     */
    public static final long INVALID_TOTAL_TERM_COUNT = -1;

    /**
     * Ordered by their token position in current segment
     */
    public final PrimaryKey minKey;
    public final PrimaryKey maxKey;

    /**
     * Minimum and maximum indexed column value ordered by its {@link org.apache.cassandra.db.marshal.AbstractType}.
     */
    public final ByteBuffer minTerm;
    public final ByteBuffer maxTerm;


    /**
     * Statistical distribution of term values, useful for estimating selectivity of queries against this segment.
     */
    public final TermsDistribution termsDistribution;

    /**
     * Root, offset, length for each index structure in the segment.
     *
     * Note: postings block offsets are stored in terms dictionary, no need to worry about its root.
     */
    public final ComponentMetadataMap componentMetadatas;

    SegmentMetadata(long segmentRowIdOffset,
                    long numRows,
                    long minSSTableRowId,
                    long maxSSTableRowId,
                    PrimaryKey minKey,
                    PrimaryKey maxKey,
                    ByteBuffer minTerm,
                    ByteBuffer maxTerm,
                    TermsDistribution termsDistribution,
                    ComponentMetadataMap componentMetadatas,
                    long totalTermCount)
    {
        // numRows can exceed Integer.MAX_VALUE because it is the count of unique term and segmentRowId pairs.
        Objects.requireNonNull(minKey);
        Objects.requireNonNull(maxKey);
        Objects.requireNonNull(minTerm);
        Objects.requireNonNull(maxTerm);

        this.version = Version.current();
        this.segmentRowIdOffset = segmentRowIdOffset;
        this.minSSTableRowId = minSSTableRowId;
        this.maxSSTableRowId = maxSSTableRowId;
        this.numRows = numRows;
        this.totalTermCount = totalTermCount;
        this.minKey = minKey;
        this.maxKey = maxKey;
        this.minTerm = minTerm;
        this.maxTerm = maxTerm;
        this.termsDistribution = termsDistribution;
        this.componentMetadatas = componentMetadatas;
    }

    private static final Logger logger = LoggerFactory.getLogger(SegmentMetadata.class);

    @SuppressWarnings("resource")
    private SegmentMetadata(IndexInput input, IndexContext context, Version version, SSTableContext sstableContext, boolean loadFullResolutionBounds) throws IOException
    {
        if (!loadFullResolutionBounds)
            logger.warn("Loading segment metadata without full primary key boundary resolution. Some ORDER BY queries" +
                        " may not work correctly.");

        AbstractType<?> termsType = context.getValidator();

        this.version = version;
        this.segmentRowIdOffset = input.readLong();
        this.numRows = input.readLong();
        this.minSSTableRowId = input.readLong();
        this.maxSSTableRowId = input.readLong();

        if (loadFullResolutionBounds)
        {
            // Skip the min/max partition keys since we want the fully resolved PrimaryKey for better semantics.
            // Also, these values are not always correct for flushed sstables, but the min/max row ids are, which
            // provides further justification for skipping them.
            skipBytes(input);
            skipBytes(input);

            // Get the fully qualified PrimaryKey min and max objects to ensure that we skip several edge cases related
            // to possibly confusing equality semantics. The main issue is how we handl PrimaryKey objects that
            // are not fully qualified when doing a binary search on a collection of PrimaryKeyWithSource objects.
            // By materializing the fully qualified PrimaryKey objects, we get the right binary search result.
            final PrimaryKey min, max;
            try (var pkm = sstableContext.primaryKeyMapFactory().newPerSSTablePrimaryKeyMap())
            {
                // We need to load eagerly to allow us to close the partition key map.
                min = pkm.primaryKeyFromRowId(minSSTableRowId).loadDeferred();
                max = pkm.primaryKeyFromRowId(maxSSTableRowId).loadDeferred();
            }

            this.minKey = new PrimaryKeyWithSource(min, sstableContext.sstable.getId(), minSSTableRowId, min, max);
            this.maxKey = new PrimaryKeyWithSource(max, sstableContext.sstable.getId(), maxSSTableRowId, min, max);
        }
        else
        {
            assert sstableContext == null;
            // Only valid in some very specific tests.
            PrimaryKey.Factory primaryKeyFactory = context.keyFactory();
            this.minKey = primaryKeyFactory.createPartitionKeyOnly(DatabaseDescriptor.getPartitioner().decorateKey(readBytes(input)));
            this.maxKey = primaryKeyFactory.createPartitionKeyOnly(DatabaseDescriptor.getPartitioner().decorateKey(readBytes(input)));
        }

        this.minTerm = readBytes(input);
        this.maxTerm = readBytes(input);
        TermsDistribution td = null;
        if (version.onOrAfter(Version.EB))
        {
            int len = input.readInt();
            long fp = input.getFilePointer();
            if (len > 0)
            {
                td = TermsDistribution.read(input, termsType);
                input.seek(fp + len);
            }
        }
        this.termsDistribution = td;
        this.componentMetadatas = new SegmentMetadata.ComponentMetadataMap(input);

        if (version.onOrAfter(Version.ED))
            this.totalTermCount = input.readLong();
        else
            this.totalTermCount = INVALID_TOTAL_TERM_COUNT;
    }

    @SuppressWarnings("resource")
    public static List<SegmentMetadata> load(MetadataSource source, IndexContext context, SSTableContext sstableContext) throws IOException
    {
        return load(source, context, sstableContext, true);
    }

    /**
     * This is only visible for testing because the SegmentFlushTest creates fake boundary scenarios that break
     * normal assumptions about the min/max row ids mapping to specific positions in the per-sstable index components.
     * Only set loadFullResolutionBounds to false in tests when you are sure that is the only possible solution.
     */
    @VisibleForTesting
    @SuppressWarnings("resource")
    public static List<SegmentMetadata> loadForTesting(MetadataSource source, IndexContext context) throws IOException
    {
        return load(source, context, null, false);
    }

    /**
     * Only set loadFullResolutionBounds to false in tests when you are sure that is exactly what you want.
     */
    private static List<SegmentMetadata> load(MetadataSource source, IndexContext context, SSTableContext sstableContext, boolean loadFullResolutionBounds) throws IOException
    {

        IndexInput input = source.get(NAME);

        int segmentCount = input.readVInt();

        List<SegmentMetadata> segmentMetadata = new ArrayList<>(segmentCount);

        for (int i = 0; i < segmentCount; i++)
        {
            segmentMetadata.add(new SegmentMetadata(input, context, source.getVersion(), sstableContext, loadFullResolutionBounds));
        }

        return segmentMetadata;
    }

    /**
     * Writes disk metadata for the given segment list.
     */
    @SuppressWarnings("resource")
    public static void write(MetadataWriter writer, List<SegmentMetadata> segments) throws IOException
    {
        try (IndexOutput output = writer.builder(NAME))
        {
            output.writeVInt(segments.size());

            for (SegmentMetadata metadata : segments)
            {
                output.writeLong(metadata.segmentRowIdOffset);
                output.writeLong(metadata.numRows);
                output.writeLong(metadata.minSSTableRowId);
                output.writeLong(metadata.maxSSTableRowId);

                Stream.of(metadata.minKey.partitionKey().getKey(),
                          metadata.maxKey.partitionKey().getKey(),
                          metadata.minTerm, metadata.maxTerm).forEach(bb -> writeBytes(bb, output));

                if (writer.version().onOrAfter(Version.EB))
                {
                    if (metadata.termsDistribution != null)
                    {
                        var tmp = new ModernResettableByteBuffersIndexOutput(1024, "");
                        metadata.termsDistribution.write(tmp);
                        output.writeInt(tmp.intSize());
                        tmp.copyTo(output);
                    }
                    else
                    {
                        // some indexes, e.g. vector may have no terms distribution
                        output.writeInt(0);
                    }
                }

                metadata.componentMetadatas.write(output);

                if (writer.version().onOrAfter(Version.ED))
                {
                    assert metadata.totalTermCount >= 0 : "totalTermCount cannot be unknown on this or later version";
                    output.writeLong(metadata.totalTermCount);
                }
            }
        }
    }

    @Override
    public int compareTo(SegmentMetadata other)
    {
        return Long.compare(this.segmentRowIdOffset, other.segmentRowIdOffset);
    }

    @Override
    public String toString()
    {
        return "SegmentMetadata{" +
               "segmentRowIdOffset=" + segmentRowIdOffset +
               ", minSSTableRowId=" + minSSTableRowId +
               ", maxSSTableRowId=" + maxSSTableRowId +
               ", numRows=" + numRows +
               ", componentMetadatas=" + componentMetadatas +
               '}';
    }

    public long estimateNumRowsMatching(Expression predicate)
    {
        if (termsDistribution == null)
            throw new IllegalStateException("Terms distribution not available for " + this);


        switch (predicate.getOp())
        {
            case MATCH:
            case EQ:
            case CONTAINS_KEY:
            case CONTAINS_VALUE:
            {
                var value = asByteComparable(predicate.lower.value.encoded, predicate.validator);
                return termsDistribution.estimateNumRowsMatchingExact(value);
            }
            case NOT_EQ:
            case NOT_CONTAINS_KEY:
            case NOT_CONTAINS_VALUE:
            {
                if (TypeUtil.supportsRounding(predicate.validator))
                    return numRows;
                else
                {
                    var value = asByteComparable(predicate.lower.value.encoded, predicate.validator);
                    return numRows - termsDistribution.estimateNumRowsMatchingExact(value);
                }
            }
            case RANGE:
            {
                var lower = predicate.lower != null ? asByteComparable(predicate.lower.value.encoded, predicate.validator) : null;
                var upper = predicate.upper != null ? asByteComparable(predicate.upper.value.encoded, predicate.validator) : null;
                boolean lowerInclusive = predicate.lower != null && predicate.lower.inclusive;
                boolean upperInclusive = predicate.upper != null && predicate.upper.inclusive;
                return termsDistribution.estimateNumRowsInRange(lower, lowerInclusive, upper, upperInclusive);
            }
            default:
                throw new IllegalArgumentException("Unsupported expression: " + predicate);
        }
    }

    private ByteComparable asByteComparable(ByteBuffer value, AbstractType<?> type)
    {
        if (TypeUtil.isLiteral(type))
            return version.onDiskFormat().encodeForTrie(value, type);

        byte[] buffer = new byte[TypeUtil.fixedSizeOf(type)];
        TypeUtil.toComparableBytes(value, type, buffer);
        return ByteComparable.preencoded(termsDistribution.byteComparableVersion, buffer);
    }

    private static ByteBuffer readBytes(IndexInput input) throws IOException
    {
        int len = input.readVInt();
        byte[] bytes = new byte[len];
        input.readBytes(bytes, 0, len);
        return ByteBuffer.wrap(bytes);
    }

    private static void skipBytes(IndexInput input) throws IOException
    {
        int len = input.readVInt();
        input.skipBytes(len);
    }

    static void writeBytes(ByteBuffer buf, IndexOutput out)
    {
        try
        {
            byte[] bytes = ByteBufferUtil.getArray(buf);
            out.writeVInt(bytes.length);
            out.writeBytes(bytes, 0, bytes.length);
        }
        catch (IOException ioe)
        {
            throw new RuntimeException(ioe);
        }
    }

    long getIndexRoot(IndexComponentType indexComponentType)
    {
        return componentMetadatas.get(indexComponentType).root;
    }

    public int toSegmentRowId(long sstableRowId)
    {
        int segmentRowId = Math.toIntExact(sstableRowId - segmentRowIdOffset);

        if (segmentRowId == PostingList.END_OF_STREAM)
            throw new IllegalArgumentException("Illegal segment row id: END_OF_STREAM found");

        return segmentRowId;
    }

    public static class ComponentMetadataMap
    {
        private final Map<IndexComponentType, ComponentMetadata> metas = new HashMap<>();

        ComponentMetadataMap(IndexInput input) throws IOException
        {
            int size = input.readInt();

            for (int i = 0; i < size; i++)
            {
                metas.put(IndexComponentType.valueOf(input.readString()), new ComponentMetadata(input));
            }
        }

        public ComponentMetadataMap()
        {
        }

        public void put(IndexComponentType indexComponentType, long root, long offset, long length)
        {
            metas.put(indexComponentType, new ComponentMetadata(root, offset, length));
        }

        public void put(IndexComponentType indexComponentType, long root, long offset, long length, Map<String, String> additionalMap)
        {
            metas.put(indexComponentType, new ComponentMetadata(root, offset, length, additionalMap));
        }

        private void write(IndexOutput output) throws IOException
        {
            output.writeInt(metas.size());

            for (Map.Entry<IndexComponentType, ComponentMetadata> entry : metas.entrySet())
            {
                output.writeString(entry.getKey().name());
                entry.getValue().write(output);
            }
        }

        public ComponentMetadata get(IndexComponentType indexComponentType)
        {
            if (!metas.containsKey(indexComponentType))
                throw new IllegalArgumentException(indexComponentType + " ComponentMetadata not found");

            return metas.get(indexComponentType);
        }

        public ComponentMetadata getOptional(IndexComponentType indexComponentType)
        {
            return metas.get(indexComponentType);
        }

        public Map<String, Map<String, String>> asMap()
        {
            Map<String, Map<String, String>> metaAttributes = new HashMap<>();

            for (Map.Entry<IndexComponentType, ComponentMetadata> entry : metas.entrySet())
            {
                String name = entry.getKey().name();
                ComponentMetadata metadata = entry.getValue();

                Map<String, String> componentAttributes = metadata.asMap();

                assert !metaAttributes.containsKey(name) : "Found duplicate index type: " + name;
                metaAttributes.put(name, componentAttributes);
            }

            return metaAttributes;
        }

        @Override
        public String toString()
        {
            return "ComponentMetadataMap{" +
                   "metas=" + metas +
                   '}';
        }

        public double indexSize()
        {
            return metas.values().stream().mapToLong(meta -> meta.length).sum();
        }
    }

    public static class ComponentMetadata
    {
        public static final String ROOT = "Root";
        public static final String OFFSET = "Offset";
        public static final String LENGTH = "Length";

        public final long root;
        public final long offset;
        public final long length;
        public final Map<String,String> attributes;

        public ComponentMetadata(long root, long offset, long length)
        {
            this.root = root;
            this.offset = offset;
            this.length = length;
            this.attributes = Collections.emptyMap();
        }

        ComponentMetadata(long root, long offset, long length, Map<String, String> attributes)
        {
            this.root = root;
            this.offset = offset;
            this.length = length;
            this.attributes = attributes;
        }

        ComponentMetadata(IndexInput input) throws IOException
        {
            this.root = input.readLong();
            this.offset = input.readLong();
            this.length = input.readLong();
            int size = input.readInt();

            attributes = new HashMap<>(size);
            for (int x=0; x < size; x++)
            {
                String key = input.readString();
                String value = input.readString();

                attributes.put(key, value);
            }
        }

        public void write(IndexOutput output) throws IOException
        {
            output.writeLong(root);
            output.writeLong(offset);
            output.writeLong(length);

            output.writeInt(attributes.size());
            for (Map.Entry<String,String> entry : attributes.entrySet())
            {
                output.writeString(entry.getKey());
                output.writeString(entry.getValue());
            }
        }

        @Override
        public String toString()
        {
            return String.format("ComponentMetadata{root=%d, offset=%d, length=%d, attributes=%s}", root, offset, length, attributes.toString());
        }

        public Map<String, String> asMap()
        {
            return ImmutableMap.<String, String>builder().putAll(attributes).put(OFFSET, Long.toString(offset)).put(LENGTH, Long.toString(length)).put(ROOT, Long.toString(root)).build();
        }
    }
}
