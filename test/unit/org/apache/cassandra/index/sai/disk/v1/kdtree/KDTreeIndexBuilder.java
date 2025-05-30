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
package org.apache.cassandra.index.sai.disk.v1.kdtree;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import org.junit.Assert;

import com.carrotsearch.hppc.IntArrayList;
import org.apache.cassandra.index.sai.memory.RowMapping;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.ShortType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.SSTableContext;
import org.apache.cassandra.index.sai.disk.MemtableTermsIterator;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.TermsIterator;
import org.apache.cassandra.index.sai.disk.format.IndexComponents;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.disk.v1.IndexSearcher;
import org.apache.cassandra.index.sai.disk.v1.IndexWriterConfig;
import org.apache.cassandra.index.sai.disk.v1.KDTreeIndexSearcher;
import org.apache.cassandra.index.sai.disk.v1.PartitionAwarePrimaryKeyFactory;
import org.apache.cassandra.index.sai.disk.v1.PerIndexFiles;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadataBuilder;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.io.sstable.SSTableId;
import org.apache.cassandra.io.sstable.SequenceBasedSSTableId;
import org.apache.cassandra.utils.AbstractGuavaIterator;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class KDTreeIndexBuilder
{
    public static final PrimaryKeyMap TEST_PRIMARY_KEY_MAP = new PrimaryKeyMap()
    {
        private final PrimaryKey.Factory primaryKeyFactory = new PartitionAwarePrimaryKeyFactory();

        @Override
        public SSTableId<?> getSSTableId()
        {
            return new SequenceBasedSSTableId(0);
        }

        @Override
        public PrimaryKey primaryKeyFromRowId(long sstableRowId)
        {
            return primaryKeyFactory.createTokenOnly(new Murmur3Partitioner.LongToken(sstableRowId));
        }

        @Override
        public long exactRowIdOrInvertedCeiling(PrimaryKey key)
        {
            return key.token().getLongValue();
        }

        @Override
        public long ceiling(PrimaryKey key)
        {
            return key.token().getLongValue();
        }

        @Override
        public long floor(PrimaryKey key)
        {
            return key.token().getLongValue();
        }

        @Override
        public long count()
        {
            return Long.MAX_VALUE;
        }
    };
    public static final PrimaryKeyMap.Factory TEST_PRIMARY_KEY_MAP_FACTORY = () -> TEST_PRIMARY_KEY_MAP;


    private static final BigDecimal ONE_TENTH = BigDecimal.valueOf(1, 1);

    private final IndexDescriptor indexDescriptor;
    private final AbstractType<?> type;
    private final AbstractGuavaIterator<Pair<ByteComparable.Preencoded, IntArrayList>> terms;
    private final int size;
    private final int minSegmentRowId;
    private final int maxSegmentRowId;

    public KDTreeIndexBuilder(IndexDescriptor indexDescriptor,
                              AbstractType<?> type,
                              AbstractGuavaIterator<Pair<ByteComparable.Preencoded, IntArrayList>> terms,
                              int size,
                              int minSegmentRowId,
                              int maxSegmentRowId)
    {
        this.indexDescriptor = indexDescriptor;
        this.type = type;
        this.terms = terms;
        this.size = size;
        this.minSegmentRowId = minSegmentRowId;
        this.maxSegmentRowId = maxSegmentRowId;
    }

    KDTreeIndexSearcher flushAndOpen() throws IOException
    {
        // Wrap postings with RowIdWithFrequency using default frequency of 1
        final TermsIterator termEnum = new MemtableTermsIterator(null, null, new AbstractGuavaIterator<>()
        {
            @Override
            protected Pair<ByteComparable.Preencoded, List<RowMapping.RowIdWithFrequency>> computeNext()
            {
                if (!terms.hasNext())
                    return endOfData();

                Pair<ByteComparable.Preencoded, IntArrayList> pair = terms.next();
                List<RowMapping.RowIdWithFrequency> postings = new ArrayList<>(pair.right.size());
                for (int i = 0; i < pair.right.size(); i++)
                    postings.add(new RowMapping.RowIdWithFrequency(pair.right.get(i), 1));
                return Pair.create(pair.left, postings);
            }
        });
        final ImmutableOneDimPointValues pointValues = ImmutableOneDimPointValues.fromTermEnum(termEnum, type);

        final SegmentMetadata metadata;

        IndexContext indexContext = SAITester.createIndexContext("test", Int32Type.instance);
        IndexComponents.ForWrite components = indexDescriptor.newPerIndexComponentsForWrite(indexContext);
        try (NumericIndexWriter writer = new NumericIndexWriter(components,
                                                                TypeUtil.fixedSizeOf(type),
                                                                maxSegmentRowId,
                                                                size,
                                                                IndexWriterConfig.defaultConfig("test")))
        {
            SegmentMetadataBuilder metadataBuilder = new SegmentMetadataBuilder(0, components);
            final SegmentMetadata.ComponentMetadataMap indexMetas = writer.writeAll(metadataBuilder.intercept(pointValues));
            metadataBuilder.setComponentsMetadata(indexMetas);
            metadataBuilder.setRowIdRange(minSegmentRowId, maxSegmentRowId);
            metadataBuilder.setTermRange(UTF8Type.instance.fromString("c"),
                                         UTF8Type.instance.fromString("d"));
            metadataBuilder.setKeyRange(SAITester.TEST_FACTORY.createTokenOnly(Murmur3Partitioner.instance.decorateKey(UTF8Type.instance.fromString("a")).getToken()),
                                        SAITester.TEST_FACTORY.createTokenOnly(Murmur3Partitioner.instance.decorateKey(UTF8Type.instance.fromString("b")).getToken()));
            metadataBuilder.setNumRows(size);
            metadata = metadataBuilder.build();
        }

        try (PerIndexFiles indexFiles = new PerIndexFiles(components))
        {
            SSTableContext sstableContext = mock(SSTableContext.class);
            when(sstableContext.primaryKeyMapFactory()).thenReturn(KDTreeIndexBuilder.TEST_PRIMARY_KEY_MAP_FACTORY);
            when(sstableContext.usedPerSSTableComponents()).thenReturn(indexDescriptor.perSSTableComponents());

            IndexSearcher searcher = Version.current().onDiskFormat().newIndexSearcher(sstableContext, indexContext, indexFiles, metadata);
            assertThat(searcher).isInstanceOf(KDTreeIndexSearcher.class);
            return (KDTreeIndexSearcher) searcher;
        }
    }

    /**
     * Returns a k-d tree index where:
     * 1. term values have 32b
     * 2. term value is equal to {@code startTermInclusive} + row id;
     * 3. tokens and offsets are equal to row id;
     */
    public static IndexSearcher buildInt32Searcher(IndexDescriptor indexDescriptor, int startTermInclusive, int endTermExclusive)
    throws IOException
    {
        final int size = endTermExclusive - startTermInclusive;
        Assert.assertTrue(size > 0);
        KDTreeIndexBuilder indexBuilder = new KDTreeIndexBuilder(indexDescriptor,
                                                                 Int32Type.instance,
                                                                 singleOrd(int32Range(startTermInclusive, endTermExclusive), Int32Type.instance, startTermInclusive, size),
                                                                 size,
                                                                 startTermInclusive,
                                                                 endTermExclusive);
        return indexBuilder.flushAndOpen();
    }

    public static IndexSearcher buildDecimalSearcher(IndexDescriptor indexDescriptor, BigDecimal startTermInclusive, BigDecimal endTermExclusive)
    throws IOException
    {
        BigDecimal bigDifference = endTermExclusive.subtract(startTermInclusive);
        int size = bigDifference.intValueExact() * 10;
        Assert.assertTrue(size > 0);
        KDTreeIndexBuilder indexBuilder = new KDTreeIndexBuilder(indexDescriptor,
                                                                 DecimalType.instance,
                                                                 singleOrd(decimalRange(startTermInclusive, endTermExclusive), DecimalType.instance, startTermInclusive.intValueExact() * 10, size),
                                                                 size,
                                                                 startTermInclusive.intValueExact() * 10,
                                                                 endTermExclusive.intValueExact() * 10);
        return indexBuilder.flushAndOpen();
    }

    public static IndexSearcher buildBigIntegerSearcher(IndexDescriptor indexDescriptor, BigInteger startTermInclusive, BigInteger endTermExclusive)
    throws IOException
    {
        BigInteger bigDifference = endTermExclusive.subtract(startTermInclusive);
        int size = bigDifference.intValueExact();
        Assert.assertTrue(size > 0);
        KDTreeIndexBuilder indexBuilder = new KDTreeIndexBuilder(indexDescriptor,
                                                                 IntegerType.instance,
                                                                 singleOrd(bigIntegerRange(startTermInclusive, endTermExclusive), IntegerType.instance, startTermInclusive.intValueExact(), size),
                                                                 size,
                                                                 startTermInclusive.intValueExact(),
                                                                 endTermExclusive.intValueExact());
        return indexBuilder.flushAndOpen();
    }

    /**
     * Returns a k-d tree index where:
     * 1. term values have 64b
     * 2. term value is equal to {@code startTermInclusive} + row id;
     * 3. tokens and offsets are equal to row id;
     */
    public static IndexSearcher buildLongSearcher(IndexDescriptor indexDescriptor, long startTermInclusive, long endTermExclusive)
    throws IOException
    {
        final long size = endTermExclusive - startTermInclusive;
        Assert.assertTrue(size > 0);
        KDTreeIndexBuilder indexBuilder = new KDTreeIndexBuilder(indexDescriptor,
                                                                 LongType.instance,
                                                                 singleOrd(longRange(startTermInclusive, endTermExclusive), LongType.instance, Math.toIntExact(startTermInclusive), Math.toIntExact(size)),
                                                                 Math.toIntExact(size),
                                                                 Math.toIntExact(startTermInclusive),
                                                                 Math.toIntExact(endTermExclusive));
        return indexBuilder.flushAndOpen();
    }

    /**
     * Returns a k-d tree index where:
     * 1. term values have 16b
     * 2. term value is equal to {@code startTermInclusive} + row id;
     * 3. tokens and offsets are equal to row id;
     */
    public static IndexSearcher buildShortSearcher(IndexDescriptor indexDescriptor, short startTermInclusive, short endTermExclusive)
    throws IOException
    {
        final int size = endTermExclusive - startTermInclusive;
        Assert.assertTrue(size > 0);
        KDTreeIndexBuilder indexBuilder = new KDTreeIndexBuilder(indexDescriptor,
                                                                 ShortType.instance,
                                                                 singleOrd(shortRange(startTermInclusive, endTermExclusive), ShortType.instance, startTermInclusive, size),
                                                                 size,
                                                                 startTermInclusive,
                                                                 endTermExclusive);
        return indexBuilder.flushAndOpen();
    }

    /**
     * Returns inverted index where each posting list contains exactly one element equal to the terms ordinal number +
     * given offset.
     */
    public static AbstractGuavaIterator<Pair<ByteComparable.Preencoded, IntArrayList>> singleOrd(Iterator<ByteBuffer> terms, AbstractType<?> type, int segmentRowIdOffset, int size)
    {
        return new AbstractGuavaIterator<>()
        {
            private long currentTerm = 0;
            private int currentSegmentRowId = segmentRowIdOffset;

            @Override
            protected Pair<ByteComparable.Preencoded, IntArrayList> computeNext()
            {
                if (currentTerm++ >= size)
                {
                    return endOfData();
                }

                IntArrayList postings = new IntArrayList();
                postings.add(currentSegmentRowId++);
                assertTrue(terms.hasNext());

                final ByteSource encoded = TypeUtil.asComparableBytes(terms.next(), type, TypeUtil.BYTE_COMPARABLE_VERSION);
                return Pair.create(ByteComparable.preencoded(TypeUtil.BYTE_COMPARABLE_VERSION, ByteSourceInverse.readBytes(encoded)), postings);
            }
        };
    }

    /**
     * Returns sequential ordered encoded ints from {@code startInclusive} (inclusive) to {@code endExclusive}
     * (exclusive) by an incremental step of {@code 1}.
     */
    public static Iterator<ByteBuffer> int32Range(int startInclusive, int endExclusive)
    {
        return IntStream.range(startInclusive, endExclusive)
                        .mapToObj(Int32Type.instance::decompose)
                        .collect(Collectors.toList())
                        .iterator();
    }

    /**
     * Returns sequential ordered encoded longs from {@code startInclusive} (inclusive) to {@code endExclusive}
     * (exclusive) by an incremental step of {@code 1}.
     */
    public static Iterator<ByteBuffer> longRange(long startInclusive, long endExclusive)
    {
        return LongStream.range(startInclusive, endExclusive)
                         .mapToObj(LongType.instance::decompose)
                         .collect(Collectors.toList())
                         .iterator();
    }

    public static Iterator<ByteBuffer> decimalRange(final BigDecimal startInclusive, final BigDecimal endExclusive)
    {
        int n = endExclusive.subtract(startInclusive).intValueExact() * 10;
        final Supplier<BigDecimal> generator = new Supplier<>()
        {
            BigDecimal current = startInclusive;

            @Override
            public BigDecimal get()
            {
                BigDecimal result = current;
                current = current.add(ONE_TENTH);
                return result;
            }
        };
        return Stream.generate(generator)
                     .limit(n)
                     .map(bd -> TypeUtil.encode(DecimalType.instance.decompose(bd), DecimalType.instance))
                     .collect(Collectors.toList())
                     .iterator();
    }

    public static Iterator<ByteBuffer> bigIntegerRange(final BigInteger startInclusive, final BigInteger endExclusive)
    {
        int n = endExclusive.subtract(startInclusive).intValueExact();
        final Supplier<BigInteger> generator = new Supplier<>()
        {
            BigInteger current = startInclusive;

            @Override
            public BigInteger get()
            {
                BigInteger result = current;
                current = current.add(BigInteger.ONE);
                return result;
            }
        };
        return Stream.generate(generator)
                     .limit(n)
                     .map(bd -> TypeUtil.encode(IntegerType.instance.decompose(bd), IntegerType.instance))
                     .collect(Collectors.toList())
                     .iterator();
    }


    /**
     * Returns sequential ordered encoded shorts from {@code startInclusive} (inclusive) to {@code endExclusive}
     * (exclusive) by an incremental step of {@code 1}.
     */
    public static Iterator<ByteBuffer> shortRange(short startInclusive, short endExclusive)
    {
        return IntStream.range(startInclusive, endExclusive)
                        .mapToObj(i -> ShortType.instance.decompose((short) i))
                        .collect(Collectors.toList())
                        .iterator();
    }
}
