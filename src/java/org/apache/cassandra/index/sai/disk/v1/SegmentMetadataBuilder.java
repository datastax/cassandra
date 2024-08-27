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

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;


import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.TermsIterator;
import org.apache.cassandra.index.sai.disk.format.IndexComponents;
import org.apache.cassandra.index.sai.disk.format.OnDiskFormat;
import org.apache.cassandra.index.sai.disk.v1.kdtree.MutableOneDimPointValues;
import org.apache.cassandra.index.sai.disk.v6.TermsDistribution;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.lucene.util.BytesRef;

/**
 * {@link SegmentMetadata} contains a lot of information, so it got its own Builder.
 * The builder is not only responsible for setting the fields, but also intercepts
 * the index building process and records the {@link TermsDistribution}.
 */
@NotThreadSafe
public class SegmentMetadataBuilder
{
    private static final String HISTOGRAM_SIZE_OPTION = "statistics.histogram_size";
    private static final String MFT_COUNT_OPTION = "statistics.most_frequent_terms_count";

    private final long segmentRowIdOffset;

    private final AbstractType<?> validator;
    private final OnDiskFormat format;
    private final List<Closeable> interceptors = new ArrayList<>();

    private boolean built = false;

    private PrimaryKey minKey;
    private PrimaryKey maxKey;

    private long minRowId = -1;
    private long maxRowId = -1;

    private ByteBuffer minTerm;
    private ByteBuffer maxTerm;

    private long numRows;

    private final TermsDistribution.Builder termsDistributionBuilder;

    SegmentMetadata.ComponentMetadataMap metadataMap;

    public SegmentMetadataBuilder(long segmentRowIdOffset, IndexComponents.ForWrite components)
    {
        IndexContext context = Objects.requireNonNull(components.context());
        this.segmentRowIdOffset = segmentRowIdOffset;
        this.format = components.version().onDiskFormat();
        this.validator = Objects.requireNonNull(components.context(), "Context must be set").getValidator();

        int histogramSize = context.getIntOption(HISTOGRAM_SIZE_OPTION, 128);
        int mostFrequentTermsCount = context.getIntOption(MFT_COUNT_OPTION, 128);
        this.termsDistributionBuilder = new TermsDistribution.Builder(context.getValidator(), histogramSize, mostFrequentTermsCount);
    }

    public void setKeyRange(@Nonnull PrimaryKey minKey, @Nonnull PrimaryKey maxKey)
    {
        assert minKey.compareTo(maxKey) <= 0: "minKey (" + minKey + ") must not be greater than (" + maxKey + ')';
        this.minKey = minKey;
        this.maxKey = maxKey;
    }

    public void setRowIdRange(long minRowId, long maxRowId)
    {
        assert minRowId <= maxRowId: "minRowId (" + minRowId + ") must not be greater than (" + maxRowId + ')';
        this.minRowId = minRowId;
        this.maxRowId = maxRowId;
    }

    /**
     * Sets the term range of the data indexed by this segment.
     * We need this method because we cannot automatically record min and max term. We need exact
     * values but the values from the index are trucated to 20 bytes for some types like e.g. BigDecimals.
     * <p>
     * This method requires raw serializations of the term types, not bytecomparable encodings.
     */
    public void setTermRange(@Nonnull ByteBuffer minTerm, @Nonnull ByteBuffer maxTerm)
    {
        this.minTerm = minTerm;
        this.maxTerm = maxTerm;
    }

    public void setComponentsMetadata(SegmentMetadata.ComponentMetadataMap metadataMap)
    {
        this.metadataMap = metadataMap;
    }

    /**
     * Should be called whenever a point is added to the index.
     * Points must be added in the index term order.
     * @param term the term value
     * @param rowCount the number of rows with this term value in the segment
     */
    void add(ByteComparable term, int rowCount)
    {
        if (built)
            throw new IllegalStateException("Segment metadata already built, no more additions allowed");

        numRows += rowCount;
        termsDistributionBuilder.add(format.decodeFromTrie(term, validator), rowCount);
    }

    public @Nonnull SegmentMetadata build()
    {
        if (minRowId == -1 || maxRowId == -1)
            throw new IllegalStateException("Segment row id range not set");
        if (minKey == null || maxKey == null)
            throw new IllegalStateException("Segment key range not set");
        if (minTerm == null || maxTerm == null)
            throw new IllegalStateException("Term range not set");

        FileUtils.closeQuietly(interceptors);
        built = true; // must be flipped after closing the interceptors, because they may push some data to us when closing

        return new SegmentMetadata(segmentRowIdOffset,
                                   numRows,
                                   minRowId,
                                   maxRowId,
                                   minKey,
                                   maxKey,
                                   minTerm,
                                   maxTerm,
                                   termsDistributionBuilder.build(),
                                   metadataMap);
    }

    /**
     * Wraps a {@link TermsIterator} in such a way that while it is iterated it adds items to this builder.
     * Used at index building time to build the {@link TermsDistribution}.
     * @return a wrapped iterator which also implements {@link TermsIterator}.
     */
    public TermsIterator intercept(TermsIterator iterator)
    {
        TermsIteratorInterceptor interceptor = new TermsIteratorInterceptor(iterator, this);
        interceptors.add(interceptor);
        return interceptor;
    }

    /**
     * Wraps a {@link MutableOneDimPointValues} in such a way that while it is iterated it adds items to this builder.
     * Used at index building time to build the {@link TermsDistribution}.
     * @return a wrapped iterator which also implements {@link MutableOneDimPointValues}.
     */
    public MutableOneDimPointValues intercept(MutableOneDimPointValues values)
    {
        MutableOneDimPointValuesInterceptor interceptor = new MutableOneDimPointValuesInterceptor(values, this);
        interceptors.add(interceptor);
        return interceptor;
    }


    private static class TermsIteratorInterceptor implements TermsIterator
    {
        final TermsIterator iterator;
        final SegmentMetadataBuilder builder;

        PostingList postings;
        IOException exception;

        public TermsIteratorInterceptor(TermsIterator iterator, SegmentMetadataBuilder builder)
        {
            this.iterator = iterator;
            this.builder = builder;
        }

        @Override
        public PostingList postings() throws IOException
        {
            maybeThrow();
            return postings;
        }

        @Override
        public ByteBuffer getMinTerm()
        {
            return iterator.getMinTerm();
        }

        @Override
        public ByteBuffer getMaxTerm()
        {
            return iterator.getMaxTerm();
        }

        @Override
        public void close() throws IOException
        {
            iterator.close();
            maybeThrow();
        }

        @Override
        public boolean hasNext()
        {
            return iterator.hasNext();
        }

        @Override
        public ByteComparable next()
        {
            ByteComparable term = iterator.next();
            try
            {
                postings = iterator.postings();
            }
            catch (IOException e)
            {
                exception = e;
            }
            builder.add(term, postings.size());
            return term;
        }

        private void maybeThrow() throws IOException
        {
            if (exception != null)
            {
                IOException e = exception;
                exception = null;
                throw e;
            }
        }
    }

    private static class MutableOneDimPointValuesInterceptor extends MutableOneDimPointValues implements Closeable
    {
        final MutableOneDimPointValues values;
        final SegmentMetadataBuilder builder;

        byte[] lastTerm;
        int count = 0;

        public MutableOneDimPointValuesInterceptor(MutableOneDimPointValues values, SegmentMetadataBuilder builder)
        {
            this.values = values;
            this.builder = builder;
        }

        @Override
        public int getDocCount()
        {
            return values.getDocCount();
        }

        @Override
        public long size()
        {
            return values.size();
        }

        @Override
        public void getValue(int i, BytesRef packedValue)
        {
            values.getValue(i, packedValue);
        }

        @Override
        public byte getByteAt(int i, int k)
        {
            return values.getByteAt(i, k);
        }

        @Override
        public int getDocID(int i)
        {
            return values.getDocID(i);
        }

        @Override
        public void swap(int i, int j)
        {
            values.swap(i, j);
        }

        @Override
        public byte[] getMinPackedValue()
        {
            return values.getMinPackedValue();
        }

        @Override
        public byte[] getMaxPackedValue()
        {
            return values.getMaxPackedValue();
        }

        @Override
        public int getNumDimensions()
        {
            return values.getNumDimensions();
        }

        @Override
        public int getBytesPerDimension()
        {
            return values.getBytesPerDimension();
        }

        @Override
        public int getNumIndexDimensions() throws IOException
        {
            return values.getNumIndexDimensions();
        }

        @Override
        public PointTree getPointTree() throws IOException
        {
            return values.getPointTree();
        }

        @Override
        public void intersect(IntersectVisitor visitor) throws IOException
        {
            values.intersect((docId, term) -> {
                if (!Arrays.equals(term, lastTerm))
                {
                    if (lastTerm != null)
                        builder.add(encodeIfNeeded(lastTerm), count);


                    count = 0;
                    lastTerm = Arrays.copyOf(term, term.length);
                }
                count++;
                visitor.visit(docId, term);
            });
        }

        @Override
        public void close() throws IOException
        {
            if (lastTerm != null)
            {
                builder.add(encodeIfNeeded(lastTerm), count);
            }
        }

        /**
         * Converts the term values coming from the index into a proper byte comparable values.
         * This is needed because values of some types are stored in the index using their
         * raw serializations, without encoding; but some others use byte comparable serialization.
         */
        private ByteComparable encodeIfNeeded(byte[] term)
        {
            AbstractType<?> type = builder.validator;
            if (type instanceof InetAddressType || type instanceof IntegerType || type instanceof DecimalType)
                return (v) -> type.asComparableBytes(ByteBuffer.wrap(term), v);

            return (v) -> ByteSource.fixedLength(term);
        }

    }

}


