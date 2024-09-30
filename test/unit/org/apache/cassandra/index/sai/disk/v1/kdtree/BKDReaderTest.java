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

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.index.sai.disk.format.IndexComponents;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.IndexWriterConfig;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.metrics.QueryEventListener;
import org.apache.cassandra.index.sai.utils.SaiRandomizedTest;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;

import static org.apache.cassandra.index.sai.metrics.QueryEventListeners.NO_OP_BKD_LISTENER;
import static org.apache.lucene.index.PointValues.Relation.CELL_CROSSES_QUERY;
import static org.apache.lucene.index.PointValues.Relation.CELL_INSIDE_QUERY;
import static org.apache.lucene.index.PointValues.Relation.CELL_OUTSIDE_QUERY;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

public class BKDReaderTest extends SaiRandomizedTest
{
    private final BKDReader.IntersectVisitor NONE_MATCH = new BKDReader.IntersectVisitor()
    {
        @Override
        public boolean visit(byte[] packedValue)
        {
            return false;
        }

        @Override
        public Relation compare(byte[] minPackedValue, byte[] maxPackedValue)
        {
            return CELL_OUTSIDE_QUERY;
        }
    };

    private final BKDReader.IntersectVisitor ALL_MATCH = new BKDReader.IntersectVisitor()
    {
        @Override
        public boolean visit(byte[] packedValue)
        {
            return true;
        }

        @Override
        public Relation compare(byte[] minPackedValue, byte[] maxPackedValue)
        {
            return CELL_INSIDE_QUERY;
        }
    };

    private final BKDReader.IntersectVisitor ALL_MATCH_WITH_FILTERING = new BKDReader.IntersectVisitor()
    {
        @Override
        public boolean visit(byte[] packedValue)
        {
            return true;
        }

        @Override
        public Relation compare(byte[] minPackedValue, byte[] maxPackedValue)
        {
            return CELL_CROSSES_QUERY;
        }
    };

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
    public void testInts1D() throws IOException
    {
        doTestInts1D();
    }

    private void doTestInts1D() throws IOException
    {
        final int numRows = between(100, 400);
        final BKDTreeRamBuffer buffer = new BKDTreeRamBuffer(1, Integer.BYTES);

        byte[] scratch = new byte[4];
        for (int docID = 0; docID < numRows; docID++)
        {
            NumericUtils.intToSortableBytes(docID, scratch, 0);
            buffer.addPackedValue(docID, new BytesRef(scratch));
        }

        final BKDReader reader = finishAndOpenReaderOneDim(2, buffer);

        try (BKDReader.IteratorState iterator = reader.iteratorState())
        {
            while (iterator.hasNext())
            {
                int value = NumericUtils.sortableBytesToInt(iterator.scratch, 0);
                System.out.println("term=" + value);
                iterator.next();
            }
        }

        try (PostingList intersection = reader.intersect(NONE_MATCH, (QueryEventListener.BKDIndexEventListener)NO_OP_BKD_LISTENER, new QueryContext()))
        {
            assertEquals(PostingList.EMPTY, intersection);
        }

        try (PostingList collectAllIntersection = reader.intersect(ALL_MATCH, (QueryEventListener.BKDIndexEventListener)NO_OP_BKD_LISTENER, new QueryContext());
             PostingList filteringIntersection = reader.intersect(ALL_MATCH_WITH_FILTERING, (QueryEventListener.BKDIndexEventListener)NO_OP_BKD_LISTENER, new QueryContext()))
        {
            assertEquals(numRows, collectAllIntersection.size());
            assertEquals(numRows, filteringIntersection.size());

            for (int docID = 0; docID < numRows; docID++)
            {
                assertEquals(docID, collectAllIntersection.nextPosting());
                assertEquals(docID, filteringIntersection.nextPosting());
            }

            assertEquals(PostingList.END_OF_STREAM, collectAllIntersection.nextPosting());
            assertEquals(PostingList.END_OF_STREAM, filteringIntersection.nextPosting());
        }

        // Simple 1D range query:
        final int queryMin = 42;
        final int queryMax = 87;

        final PostingList intersection = reader.intersect(buildQuery(queryMin, queryMax), (QueryEventListener.BKDIndexEventListener)NO_OP_BKD_LISTENER, new QueryContext());

        long expectedRowID = queryMin;
        for (long id = intersection.nextPosting(); id != PostingList.END_OF_STREAM; id = intersection.nextPosting())
        {
            assertEquals(expectedRowID++, id);
        }
        assertEquals(queryMax - queryMin + 1, intersection.size());

        intersection.close();
        reader.close();
    }

    @Test
    public void testForwardIteration() throws IOException
    {
        final int numRows = between(100, 400);
        final BKDTreeRamBuffer buffer = new BKDTreeRamBuffer(1, Integer.BYTES);

        byte[] scratch = new byte[4];
        for (int docID = 0; docID < numRows; docID++)
        {
            NumericUtils.intToSortableBytes(docID, scratch, 0);
            buffer.addPackedValue(docID, new BytesRef(scratch));
        }

        try (BKDReader reader = finishAndOpenReaderOneDim(2, buffer);
             BKDReader.IteratorState iterator = reader.iteratorState(BKDReader.Direction.FORWARD, null))
        {
            int docId = 0;
            while (iterator.hasNext())
            {
                int value = NumericUtils.sortableBytesToInt(iterator.scratch, 0);
                assertEquals(docId++, value);
                iterator.next();
            }
            assertEquals(numRows, docId);
        }
    }

    @Test
    public void testForwardIterationWithQuery() throws IOException
    {
        final int numRows = between(100, 400);
        final int queryMin = between(2, 20);
        final int queryMax = between(numRows - 20, numRows - 2);
        final BKDTreeRamBuffer buffer = new BKDTreeRamBuffer(1, Integer.BYTES);

        byte[] scratch = new byte[4];
        for (int docID = 0; docID < numRows; docID++)
        {
            NumericUtils.intToSortableBytes(docID, scratch, 0);
            buffer.addPackedValue(docID, new BytesRef(scratch));
        }

        try (BKDReader reader = finishAndOpenReaderOneDim(2, buffer);
             BKDReader.IteratorState iterator = reader.iteratorState(BKDReader.Direction.FORWARD, buildQuery(queryMin, queryMax)))
        {
            int docId = Math.max(0, queryMin);
            while (iterator.hasNext())
            {
                int value = NumericUtils.sortableBytesToInt(iterator.scratch, 0);
                assertEquals(docId++, value);
                iterator.next();
            }
            assertEquals(Math.min(queryMax + 1, numRows), docId);
        }
    }


    @Test
    public void testBackwardIteration() throws IOException
    {
        final int numRows = between(100, 400);
        final BKDTreeRamBuffer buffer = new BKDTreeRamBuffer(1, Integer.BYTES);

        byte[] scratch = new byte[4];
        for (int docID = 0; docID < numRows; docID++)
        {
            NumericUtils.intToSortableBytes(docID, scratch, 0);
            buffer.addPackedValue(docID, new BytesRef(scratch));
        }

        try (BKDReader reader = finishAndOpenReaderOneDim(2, buffer);
             BKDReader.IteratorState iterator = reader.iteratorState(BKDReader.Direction.BACKWARD, null))
        {
            int docId = numRows;
            while (iterator.hasNext())
            {
                int value = NumericUtils.sortableBytesToInt(iterator.scratch, 0);
                assertEquals(--docId, value);
                iterator.next();
            }
            assertEquals(0, docId);
        }

    }

    @Test
    public void testBackwardIterationWithQuery() throws IOException
    {
        final int numRows = between(100, 400);
        final int queryMin = between(2, 20);
        final int queryMax = between(numRows - 20, numRows - 2);
        final BKDTreeRamBuffer buffer = new BKDTreeRamBuffer(1, Integer.BYTES);

        byte[] scratch = new byte[4];
        for (int docID = 0; docID < numRows; docID++)
        {
            NumericUtils.intToSortableBytes(docID, scratch, 0);
            buffer.addPackedValue(docID, new BytesRef(scratch));
        }

        try (BKDReader reader = finishAndOpenReaderOneDim(2, buffer);
             BKDReader.IteratorState iterator = reader.iteratorState(BKDReader.Direction.BACKWARD, buildQuery(queryMin, queryMax)))
        {
            int docId = Math.min(numRows, queryMax);
            while (iterator.hasNext())
            {
                int value = NumericUtils.sortableBytesToInt(iterator.scratch, 0);
                assertEquals(docId--, value);
                iterator.next();
            }
            assertEquals(Math.max(queryMin - 1, 0), docId);
        }
    }

    @Test
    public void testAdvance() throws IOException
    {
        doTestAdvance(false);
    }

    @Test
    public void testAdvanceCrypto() throws IOException
    {
        doTestAdvance(true);
    }

    private void doTestAdvance(boolean crypto) throws IOException
    {
        final int numRows = between(1000, 2000);
        final BKDTreeRamBuffer buffer = new BKDTreeRamBuffer(1, Integer.BYTES);

        byte[] scratch = new byte[4];
        for (int docID = 0; docID < numRows; docID++)
        {
            NumericUtils.intToSortableBytes(docID, scratch, 0);
            buffer.addPackedValue(docID, new BytesRef(scratch));
        }

        final BKDReader reader = finishAndOpenReaderOneDim(2, buffer);

        PostingList intersection = reader.intersect(NONE_MATCH, (QueryEventListener.BKDIndexEventListener)NO_OP_BKD_LISTENER, new QueryContext());
        assertEquals(PostingList.EMPTY, intersection);

        intersection = reader.intersect(ALL_MATCH, (QueryEventListener.BKDIndexEventListener)NO_OP_BKD_LISTENER, new QueryContext());
        assertEquals(numRows, intersection.size());
        assertEquals(100, intersection.advance(100));
        assertEquals(200, intersection.advance(200));
        assertEquals(300, intersection.advance(300));
        assertEquals(400, intersection.advance(400));
        assertEquals(401, intersection.advance(401));
        long expectedRowID = 402;
        for (long id = intersection.nextPosting(); expectedRowID < 500; id = intersection.nextPosting())
        {
            assertEquals(expectedRowID++, id);
        }
        assertEquals(PostingList.END_OF_STREAM, intersection.advance(numRows + 1));

        intersection.close();
    }

    @Test
    public void testResourcesReleaseWhenQueryDoesntMatchAnything() throws Exception
    {
        final BKDTreeRamBuffer buffer = new BKDTreeRamBuffer(1, Integer.BYTES);
        byte[] scratch = new byte[4];
        for (int docID = 0; docID < 1000; docID++)
        {
            NumericUtils.intToSortableBytes(docID, scratch, 0);
            buffer.addPackedValue(docID, new BytesRef(scratch));
        }
        // add a gap between 1000 and 1100
        for (int docID = 1000; docID < 2000; docID++)
        {
            NumericUtils.intToSortableBytes(docID + 100, scratch, 0);
            buffer.addPackedValue(docID, new BytesRef(scratch));
        }

        final BKDReader reader = finishAndOpenReaderOneDim(50, buffer);

        final PostingList intersection = reader.intersect(buildQuery(1017, 1096), (QueryEventListener.BKDIndexEventListener)NO_OP_BKD_LISTENER, new QueryContext());
        assertEquals(PostingList.EMPTY, intersection);
    }

    private BKDReader.IntersectVisitor buildQuery(int queryMin, int queryMax)
    {
        return new BKDReader.IntersectVisitor()
        {
            @Override
            public boolean visit(byte[] packedValue)
            {
                int x = NumericUtils.sortableBytesToInt(packedValue, 0);
                boolean bb = x >= queryMin && x <= queryMax;
                return bb;
            }

            @Override
            public Relation compare(byte[] minPackedValue, byte[] maxPackedValue)
            {
                int min = NumericUtils.sortableBytesToInt(minPackedValue, 0);
                int max = NumericUtils.sortableBytesToInt(maxPackedValue, 0);
                assert max >= min;

                if (max < queryMin || min > queryMax)
                {
                    return Relation.CELL_OUTSIDE_QUERY;
                }
                else if (min >= queryMin && max <= queryMax)
                {
                    return CELL_INSIDE_QUERY;
                }
                else
                {
                    return CELL_CROSSES_QUERY;
                }
            }
        };
    }

    private BKDReader finishAndOpenReaderOneDim(int maxPointsPerLeaf, BKDTreeRamBuffer buffer) throws IOException
    {
        IndexComponents.ForWrite components = indexDescriptor.newPerIndexComponentsForWrite(indexContext);
        final NumericIndexWriter writer = new NumericIndexWriter(components,
                                                                 maxPointsPerLeaf,
                                                                 Integer.BYTES,
                                                                 Math.toIntExact(buffer.numRows()),
                                                                 buffer.numRows(),
                                                                 new IndexWriterConfig("test", 2, 8));

        final SegmentMetadata.ComponentMetadataMap metadata = writer.writeAll(buffer.asPointValues());
        final long bkdPosition = metadata.get(IndexComponentType.KD_TREE).root;
        assertThat(bkdPosition, is(greaterThan(0L)));
        final long postingsPosition = metadata.get(IndexComponentType.KD_TREE_POSTING_LISTS).root;
        assertThat(postingsPosition, is(greaterThan(0L)));

        FileHandle kdtreeHandle = components.get(IndexComponentType.KD_TREE).createFileHandle();
        FileHandle kdtreePostingsHandle = components.get(IndexComponentType.KD_TREE_POSTING_LISTS).createFileHandle();
        return new BKDReader(indexContext,
                             kdtreeHandle,
                             bkdPosition,
                             kdtreePostingsHandle,
                             postingsPosition);
    }

    private BKDReader finishAndOpenReaderOneDim(int maxPointsPerLeaf, MutableOneDimPointValues values, int numRows) throws IOException
    {
        IndexComponents.ForWrite components = indexDescriptor.newPerIndexComponentsForWrite(indexContext);
        final NumericIndexWriter writer = new NumericIndexWriter(components,
                                                                 maxPointsPerLeaf,
                                                                 Integer.BYTES,
                                                                 Math.toIntExact(numRows),
                                                                 numRows,
                                                                 new IndexWriterConfig("test", 2, 8));

        final SegmentMetadata.ComponentMetadataMap metadata = writer.writeAll(values);
        final long bkdPosition = metadata.get(IndexComponentType.KD_TREE).root;
        assertThat(bkdPosition, is(greaterThan(0L)));
        final long postingsPosition = metadata.get(IndexComponentType.KD_TREE_POSTING_LISTS).root;
        assertThat(postingsPosition, is(greaterThan(0L)));

        FileHandle kdtreeHandle = components.get(IndexComponentType.KD_TREE).createFileHandle();
        FileHandle kdtreePostingsHandle = components.get(IndexComponentType.KD_TREE_POSTING_LISTS).createFileHandle();
        return new BKDReader(indexContext,
                             kdtreeHandle,
                             bkdPosition,
                             kdtreePostingsHandle,
                             postingsPosition);
    }
}
