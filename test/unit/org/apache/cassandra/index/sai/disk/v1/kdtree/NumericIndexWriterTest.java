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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.MemtableTermsIterator;
import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.TermsIterator;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.index.sai.disk.format.IndexComponents;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.IndexWriterConfig;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.memory.RowMapping;
import org.apache.cassandra.index.sai.metrics.QueryEventListener;
import org.apache.cassandra.index.sai.metrics.QueryEventListeners;
import org.apache.cassandra.index.sai.utils.SaiRandomizedTest;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.utils.AbstractGuavaIterator;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.NumericUtils;

public class NumericIndexWriterTest extends SaiRandomizedTest
{
    private IndexDescriptor indexDescriptor;
    private String index;
    private IndexContext indexContext;

    @Before
    public void setup() throws Throwable
    {
        indexDescriptor = newIndexDescriptor();
        index = newIndex();
        indexContext = SAITester.createIndexContext(index, Int32Type.instance);
    }

    @Test
    public void shouldFlushFromRamBuffer() throws Exception
    {
        doShouldFlushFromRamBuffer();
    }

    private void doShouldFlushFromRamBuffer() throws Exception
    {
        final BKDTreeRamBuffer ramBuffer = new BKDTreeRamBuffer(1, Integer.BYTES);
        final int numRows = 120;
        int currentValue = numRows;
        for (int i = 0; i < numRows; ++i)
        {
            byte[] scratch = new byte[Integer.BYTES];
            NumericUtils.intToSortableBytes(currentValue--, scratch, 0);
            ramBuffer.addPackedValue(i, new BytesRef(scratch));
        }

        final MutableOneDimPointValues pointValues = ramBuffer.asPointValues();

        SegmentMetadata.ComponentMetadataMap indexMetas;

        IndexComponents.ForWrite components = indexDescriptor.newPerIndexComponentsForWrite(indexContext);
        try (NumericIndexWriter writer = new NumericIndexWriter(components,
                                                                Integer.BYTES,
                                                                pointValues.getDocCount(),
                                                                pointValues.size(),
                                                                IndexWriterConfig.defaultConfig("test")))
        {
            indexMetas = writer.writeAll(pointValues);
        }

        final FileHandle kdtreeHandle = components.get(IndexComponentType.KD_TREE).createFileHandle();
        final FileHandle kdtreePostingsHandle = components.get(IndexComponentType.KD_TREE_POSTING_LISTS).createFileHandle();

        try (BKDReader reader = new BKDReader(indexContext,
                                              kdtreeHandle,
                                              indexMetas.get(IndexComponentType.KD_TREE).root,
                                              kdtreePostingsHandle,
                                              indexMetas.get(IndexComponentType.KD_TREE_POSTING_LISTS).root
        ))
        {
            final Counter visited = Counter.newCounter();
            try (final PostingList ignored = reader.intersect(new BKDReader.IntersectVisitor()
            {
                @Override
                public boolean visit(byte[] packedValue)
                {
                    // we should read point values in reverse order after sorting
                    assertEquals(1 + visited.get(), NumericUtils.sortableBytesToInt(packedValue, 0));
                    visited.addAndGet(1);
                    return true;
                }

                @Override
                public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue)
                {
                    return PointValues.Relation.CELL_CROSSES_QUERY;
                }
            }, (QueryEventListener.BKDIndexEventListener)QueryEventListeners.NO_OP_BKD_LISTENER, new QueryContext()))
            {
                assertEquals(numRows, visited.get());
            }
        }
    }

    @Test
    public void shouldFlushFromMemtable() throws Exception
    {
        final int maxSegmentRowId = 100;
        final TermsIterator termEnum = buildTermEnum(0, maxSegmentRowId);
        final ImmutableOneDimPointValues pointValues = ImmutableOneDimPointValues
                                                       .fromTermEnum(termEnum, Int32Type.instance);

        SegmentMetadata.ComponentMetadataMap indexMetas;
        IndexComponents.ForWrite components = indexDescriptor.newPerIndexComponentsForWrite(indexContext);
        try (NumericIndexWriter writer = new NumericIndexWriter(components,
                                                                TypeUtil.fixedSizeOf(Int32Type.instance),
                                                                maxSegmentRowId, maxSegmentRowId,
                                                                IndexWriterConfig.defaultConfig("test")))
        {
            indexMetas = writer.writeAll(pointValues);
        }

        final FileHandle kdtreeHandle = components.get(IndexComponentType.KD_TREE).createFileHandle();
        final FileHandle kdtreePostingsHandle = components.get(IndexComponentType.KD_TREE_POSTING_LISTS).createFileHandle();

        try (BKDReader reader = new BKDReader(indexContext,
                                              kdtreeHandle,
                                              indexMetas.get(IndexComponentType.KD_TREE).root,
                                              kdtreePostingsHandle,
                                              indexMetas.get(IndexComponentType.KD_TREE_POSTING_LISTS).root
        ))
        {
            final Counter visited = Counter.newCounter();
            try (final PostingList ignored = reader.intersect(new BKDReader.IntersectVisitor()
            {
                @Override
                public boolean visit(byte[] packedValue)
                {
                    final ByteComparable actualTerm = ByteComparable.preencoded(TypeUtil.BYTE_COMPARABLE_VERSION, packedValue);
                    final ByteComparable expectedTerm = ByteComparable.of(Math.toIntExact(visited.get()));
                    assertEquals("Point value mismatch after visiting " + visited.get() + " entries.", 0,
                                 ByteComparable.compare(actualTerm, expectedTerm, TypeUtil.BYTE_COMPARABLE_VERSION));

                    visited.addAndGet(1);
                    return true;
                }

                @Override
                public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue)
                {
                    return PointValues.Relation.CELL_CROSSES_QUERY;
                }
            }, (QueryEventListener.BKDIndexEventListener)QueryEventListeners.NO_OP_BKD_LISTENER, new QueryContext()))
            {
                assertEquals(maxSegmentRowId, visited.get());
            }
        }
    }

    private TermsIterator buildTermEnum(int startTermInclusive, int endTermExclusive)
    {
        final ByteBuffer minTerm = Int32Type.instance.decompose(startTermInclusive);
        final ByteBuffer maxTerm = Int32Type.instance.decompose(endTermExclusive);

        final AbstractGuavaIterator<Pair<ByteComparable.Preencoded, List<RowMapping.RowIdWithFrequency>>> iterator = new AbstractGuavaIterator<>()
        {
            private int currentTerm = startTermInclusive;
            private int currentRowId = 0;

            @Override
            protected Pair<ByteComparable.Preencoded, List<RowMapping.RowIdWithFrequency>> computeNext()
            {
                if (currentTerm >= endTermExclusive)
                {
                    return endOfData();
                }
                final ByteBuffer term = Int32Type.instance.decompose(currentTerm++);
                final List<RowMapping.RowIdWithFrequency> postings = new ArrayList<>();
                postings.add(new RowMapping.RowIdWithFrequency(currentRowId++, 1));
                final ByteSource encoded = Int32Type.instance.asComparableBytes(term, TypeUtil.BYTE_COMPARABLE_VERSION);
                byte[] bytes = new byte[4];
                encoded.nextBytes(bytes);
                return Pair.create(ByteComparable.preencoded(TypeUtil.BYTE_COMPARABLE_VERSION, bytes), postings);
            }
        };

        return new MemtableTermsIterator(minTerm, maxTerm, iterator);
    }
}
