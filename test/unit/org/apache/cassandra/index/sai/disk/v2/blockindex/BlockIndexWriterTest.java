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

package org.apache.cassandra.index.sai.disk.v2.blockindex;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.TreeRangeSet;
import org.junit.Test;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.randomizedtesting.annotations.Seed;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.SSTableQueryContext;
import org.apache.cassandra.index.sai.disk.MergePostingList;
import org.apache.cassandra.index.sai.disk.v1.IndexWriterConfig;
import org.apache.cassandra.index.sai.disk.MemtableTermsIterator;
import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.TermsIterator;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.BKDReader;
import org.apache.cassandra.index.sai.disk.v1.BKDTreeRamBuffer;
import org.apache.cassandra.index.sai.disk.v1.NumericIndexWriter;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.disk.v1.V1OnDiskFormat;
import org.apache.cassandra.index.sai.disk.v2.PerIndexFileProvider;
import org.apache.cassandra.index.sai.disk.v2.PerSSTableFileProvider;
import org.apache.cassandra.index.sai.disk.v2.V2OnDiskFormat;
import org.apache.cassandra.index.sai.disk.v2.V2SSTableIndexWriter;
import org.apache.cassandra.index.sai.disk.v2.V2SegmentBuilder;
import org.apache.cassandra.index.sai.metrics.QueryEventListener;
import org.apache.cassandra.index.sai.utils.NamedMemoryLimiter;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.SaiRandomizedTest;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.NumericUtils;

import static org.apache.cassandra.Util.dk;
import static org.apache.cassandra.index.sai.SAITester.createIndexContext;
import static org.apache.cassandra.index.sai.disk.v2.blockindex.BlockIndexReader.IndexState;
import static org.apache.cassandra.index.sai.metrics.QueryEventListeners.NO_OP_BKD_LISTENER;
import static org.apache.lucene.index.PointValues.Relation.CELL_CROSSES_QUERY;
import static org.apache.lucene.index.PointValues.Relation.CELL_INSIDE_QUERY;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

//@Seed("E0AF4F15BFD93AAB:547C2E55AC3ABF60")
public class BlockIndexWriterTest extends SaiRandomizedTest
{

    @Test
    public void testSeekToSameValues() throws Exception
    {
        List<Pair<ByteComparable, LongArrayList>> list = new ArrayList();

        LongArrayList rowids = new LongArrayList();
        for (int x = 0; x < 20; x++)
        {
            rowids.add(x);
        }

        list.add(add("aaa", rowids.toArray())); // 0

        IndexDescriptor indexDescriptor = newIndexDescriptor();

        IndexContext indexContext = createIndexContext("index_test1", UTF8Type.instance);

        try (BlockIndexFileProvider fileProvider = new PerIndexFileProvider(indexDescriptor, indexContext);
             BlockIndexReader blockIndexReader = createPerIndexReader(fileProvider, list);
             BlockIndexReader.BlockIndexReaderContext context = blockIndexReader.initContext())
        {
            BlockIndexReader.IndexIterator iterator = blockIndexReader.iterator();
            int i = 0;
            while (true)
            {
                IndexState state = iterator.next();
                if (state == null)
                {
                    break;
                }

                System.out.println("term "+(i++)+"="+state.term.utf8ToString()+" rowid="+state.rowid);
            }
        }
    }

    @Test
    public void testRanges() throws Exception
    {
        TreeRangeSet<Integer> set = TreeRangeSet.create();
        set.add(Range.closed(0, 10));
        set.add(Range.open(50, 60));

        boolean contains = set.contains(10);
//        System.out.println("contains="+contains);

        boolean encloses = set.encloses(Range.open(0, 10));
//        System.out.println("encloses="+encloses);

        boolean encloses2 = set.encloses(Range.open(4, 15));

//        System.out.println("encloses2="+encloses2);
    }

    public static class ArrayIndexIterator implements BlockIndexReader.IndexIterator
    {
        final IndexState[] states;
        int idx = 0;

        public ArrayIndexIterator(IndexState[] states)
        {
            this.states = states;
        }

        @Override
        public IndexState next() throws IOException
        {
            if (idx >= states.length)
            {
                return null;
            }
            return states[idx++];
        }

        @Override
        public IndexState current()
        {
            return states[idx - 1];
        }

        @Override
        public void close() throws IOException
        {

        }
    }

    @Test
    public void testMergeIterator() throws Exception
    {
        List<BlockIndexReader.IndexIterator> iterators = new ArrayList<>();
        iterators.add(new ArrayIndexIterator(new IndexState[] {
        new IndexState(new BytesRef("aaa"), 10),
        new IndexState(new BytesRef("aaa"), 20),
        new IndexState(new BytesRef("ccc"), 100),
        new IndexState(new BytesRef("ddd"), 110),
        new IndexState(new BytesRef("nnn"), 2000)}));

        iterators.add(new ArrayIndexIterator(new IndexState[] {
        new IndexState(new BytesRef("bbb"), 4),
        new IndexState(new BytesRef("bbb"), 8),
        new IndexState(new BytesRef("ggg"), 1000),
        new IndexState(new BytesRef("ggg"), 1001)}));

        MergeIndexIterators blockReaders = new MergeIndexIterators(iterators);
        while (true)
        {
            IndexState state = blockReaders.next();
            if (state == null) break;

//            System.out.println("merge state="+state);
        }
    }

    private BlockIndexReader createPerIndexReader(BlockIndexFileProvider fileProvider, List<Pair<ByteComparable, LongArrayList>> list) throws Exception
    {
        try (BlockIndexWriter blockIndexWriter = new BlockIndexWriter(fileProvider, false))
        {

            TermsIterator terms = new MemtableTermsIterator(null,
                                                            null,
                                                            list.iterator());
            while (terms.hasNext())
            {
                ByteComparable term = terms.next();
                PostingList postings = terms.postings();
                while (true)
                {
                    long rowID = postings.nextPosting();
                    if (rowID == PostingList.END_OF_STREAM)
                    {
                        break;
                    }
                    blockIndexWriter.add(term, rowID);
                }
            }

            BlockIndexMeta meta = blockIndexWriter.finish();

            return new BlockIndexReader(fileProvider, false, meta);
        }
    }

    private BlockIndexReader createPerSSTableReader(BlockIndexFileProvider fileProvider, List<Pair<ByteComparable, Long>> list) throws Exception
    {
        try (BlockIndexWriter blockIndexWriter = new BlockIndexWriter(fileProvider, false))
        {

            Iterator<Pair<ByteComparable, Long>> terms = list.iterator();

            while (terms.hasNext())
            {
                Pair<ByteComparable, Long> entry = terms.next();
                blockIndexWriter.add(entry.left, entry.right);
            }

            BlockIndexMeta meta = blockIndexWriter.finish();

            return new BlockIndexReader(fileProvider, false, meta);
        }
    }

    private Row createRow(ColumnMetadata column, ByteBuffer value)
    {
        Row.Builder builder1 = BTreeRow.sortedBuilder();
        builder1.newRow(Clustering.EMPTY);
        builder1.addCell(BufferCell.live(column, 0, value));
        return builder1.build();
    }

    @Test
    public void testPerIndexWriter() throws Exception
    {
        NamedMemoryLimiter memoryLimiter = new NamedMemoryLimiter(1, "SSTable-attached Index Segment Builder");

        IndexDescriptor indexDescriptor = newIndexDescriptor();
        String indexName = "column";

        IndexContext indexContext = createIndexContext(indexName, UTF8Type.instance);

        V2SSTableIndexWriter writer = new V2SSTableIndexWriter(indexDescriptor,
                                                               indexContext,
                                                               memoryLimiter,
                                                               () -> true);

        ColumnMetadata column = ColumnMetadata.regularColumn("sai", "internal", "column", UTF8Type.instance);

        ByteBuffer term1 = UTF8Type.instance.decompose("a");
        Row row1 = createRow(column, term1);
        DecoratedKey key1 = dk("0");

        writer.addRow(PrimaryKey.factory().createKey(key1, Clustering.EMPTY, 0), row1);

        ByteBuffer term2 = UTF8Type.instance.decompose("b");
        Row row2 = createRow(column, term2);
        DecoratedKey key2 = dk("1");

        writer.addRow(PrimaryKey.factory().createKey(key2, Clustering.EMPTY, 1), row2);

        writer.flush();

        try (BlockIndexFileProvider fileProvider = new PerIndexFileProvider(indexDescriptor, indexContext);
             IndexInput input = fileProvider.openMetadataInput())
        {
            BlockIndexMeta blockIndexMeta = new BlockIndexMeta(input);
            try (BlockIndexReader reader = new BlockIndexReader(fileProvider, false, blockIndexMeta);
                 BlockIndexReader.IndexIterator iterator = reader.iterator())
            {
                IndexState state = iterator.next();
                assertNotNull(state);
                assertEquals("a", stringFromTerm(state.term));
                assertEquals(0, state.rowid);
                state = iterator.next();
                assertNotNull(state);
                assertEquals("b", stringFromTerm(state.term));
                assertEquals(1, state.rowid);
                assertNull(iterator.next());
            }
        }
    }

    @Test
    public void testMerge() throws Exception
    {
        IndexDescriptor indexDescriptor1 = newIndexDescriptor();
        IndexContext indexContext1 = createIndexContext("index1", UTF8Type.instance);

        V2SegmentBuilder builder = new V2SegmentBuilder(indexDescriptor1, indexContext1, V1OnDiskFormat.SEGMENT_BUILD_MEMORY_LIMITER);
        builder.add(ByteBuffer.wrap("aaa".getBytes(StandardCharsets.UTF_8)), 100);
        builder.add(ByteBuffer.wrap("aaa".getBytes(StandardCharsets.UTF_8)), 101);
        builder.add(ByteBuffer.wrap("ccc".getBytes(StandardCharsets.UTF_8)), 1000);
        builder.add(ByteBuffer.wrap("ccc".getBytes(StandardCharsets.UTF_8)), 1001);

        BlockIndexMeta meta1 = builder.flush(indexDescriptor1, indexContext1, true);
        builder.release("testMerge 1");

        IndexDescriptor indexDescriptor2 = newIndexDescriptor();
        IndexContext indexContext2 = createIndexContext("index2", UTF8Type.instance);

        V2SegmentBuilder builder2 = new V2SegmentBuilder(indexDescriptor2, indexContext2, V1OnDiskFormat.SEGMENT_BUILD_MEMORY_LIMITER);
        builder2.add(ByteBuffer.wrap("bbb".getBytes(StandardCharsets.UTF_8)), 200);
        builder2.add(ByteBuffer.wrap("bbb".getBytes(StandardCharsets.UTF_8)), 201);
        builder2.add(ByteBuffer.wrap("ddd".getBytes(StandardCharsets.UTF_8)), 2000);
        builder2.add(ByteBuffer.wrap("ddd".getBytes(StandardCharsets.UTF_8)), 2001);

        BlockIndexMeta meta2 = builder2.flush(indexDescriptor2, indexContext2, true);
        builder2.release("testMerge 2");

        try (BlockIndexFileProvider fileProvider1 = new PerIndexFileProvider(indexDescriptor1, indexContext1);
             BlockIndexFileProvider fileProvider2 = new PerIndexFileProvider(indexDescriptor2, indexContext2);
             BlockIndexReader reader1 = new BlockIndexReader(fileProvider1, true, meta1);
             BlockIndexReader reader2 = new BlockIndexReader(fileProvider2, true, meta2))
        {
            BlockIndexReader.IndexIterator iterator = reader1.iterator();
            BlockIndexReader.IndexIterator iterator2 = reader2.iterator();

            try (MergeIndexIterators merged = new MergeIndexIterators(Lists.newArrayList(iterator, iterator2)))
            {
                while (true)
                {
                    IndexState state = merged.next();
                    if (state == null) break;
//            System.out.println("  merged results term="+state.term.utf8ToString()+" rowid="+state.rowid);

                }
            }
        }
    }

    @Test
    public void testPerIndexSeekTo() throws Exception
    {
        List<Pair<ByteComparable, LongArrayList>> list = new ArrayList();
        list.add(add("aaa", new long[]{ 100 })); // 0
        list.add(add("aaabbb", new long[]{ 0, 1 })); // 0
        list.add(add("aaabbb", new long[]{ 2, 3, 4 })); // 1

        list.add(add("cccc", new long[]{ 15 })); // 2
        list.add(add("gggaaaddd", new long[]{ 200, 201, 203 })); // 3
        list.add(add("gggzzzz", new long[]{ 500, 501, 502, 503, 504, 505 })); // 4, 5
        list.add(add("zzzzz", new long[]{ 700, 780, 782, 790, 794, 799 })); //

        IndexDescriptor indexDescriptor = newIndexDescriptor();

        IndexContext indexContext = createIndexContext("index_test1", UTF8Type.instance);

        try (BlockIndexFileProvider fileProvider = new PerIndexFileProvider(indexDescriptor, indexContext);
             BlockIndexReader blockIndexReader = createPerIndexReader(fileProvider, list);
             BlockIndexReader.BlockIndexReaderContext context = blockIndexReader.initContext())
        {

            long pointId = blockIndexReader.seekTo(new BytesRef("cccc"), context);
            assertEquals(6L, pointId);

            long pointId2 = blockIndexReader.seekTo(new BytesRef("gggzzzz"), context);
            assertEquals(10L, pointId2);
        }
    }

    @Test
    public void perSSTableSeekTo() throws Exception
    {
        List<Pair<ByteComparable, Long>> list = new ArrayList();
        list.add(Pair.create(v -> UTF8Type.instance.asComparableBytes(UTF8Type.instance.decompose("a"), v), 0L));
        list.add(Pair.create(v -> UTF8Type.instance.asComparableBytes(UTF8Type.instance.decompose("b"), v), 1L));
        list.add(Pair.create(v -> UTF8Type.instance.asComparableBytes(UTF8Type.instance.decompose("c"), v), 2L));
        list.add(Pair.create(v -> UTF8Type.instance.asComparableBytes(UTF8Type.instance.decompose("d"), v), 3L));
        list.add(Pair.create(v -> UTF8Type.instance.asComparableBytes(UTF8Type.instance.decompose("e"), v), 4L));
        list.add(Pair.create(v -> UTF8Type.instance.asComparableBytes(UTF8Type.instance.decompose("f"), v), 5L));
        list.add(Pair.create(v -> UTF8Type.instance.asComparableBytes(UTF8Type.instance.decompose("g"), v), 6L));
        list.add(Pair.create(v -> UTF8Type.instance.asComparableBytes(UTF8Type.instance.decompose("h"), v), 7L));

        IndexDescriptor indexDescriptor = newIndexDescriptor();

        try (BlockIndexFileProvider fileProvider = new PerSSTableFileProvider(indexDescriptor);
             BlockIndexReader reader = createPerSSTableReader(fileProvider, list);
             BlockIndexReader.BlockIndexReaderContext context = reader.initContext())
        {
            assertEquals(0L, reader.seekTo(new BytesRef("a"), context));
            assertEquals(1L, reader.seekTo(new BytesRef("b"), context));
            assertEquals(2L, reader.seekTo(new BytesRef("c"), context));
            assertEquals(3L, reader.seekTo(new BytesRef("d"), context));
            assertEquals(4L, reader.seekTo(new BytesRef("e"), context));
            assertEquals(5L, reader.seekTo(new BytesRef("f"), context));
            assertEquals(6L, reader.seekTo(new BytesRef("g"), context));
            assertEquals(7L, reader.seekTo(new BytesRef("h"), context));
            assertEquals(6L, reader.seekTo(new BytesRef("g"), context));
            assertEquals(5L, reader.seekTo(new BytesRef("f"), context));
            assertEquals(4L, reader.seekTo(new BytesRef("e"), context));
            assertEquals(3L, reader.seekTo(new BytesRef("d"), context));
            assertEquals(2L, reader.seekTo(new BytesRef("c"), context));
            assertEquals(1L, reader.seekTo(new BytesRef("b"), context));
            assertEquals(0L, reader.seekTo(new BytesRef("a"), context));

            assertEquals("a", stringFromTerm(reader.seekTo(0, context, true)));
            assertEquals("b", stringFromTerm(reader.seekTo(1, context, true)));
            assertEquals("c", stringFromTerm(reader.seekTo(2, context, true)));
            assertEquals("d", stringFromTerm(reader.seekTo(3, context, true)));
            assertEquals("e", stringFromTerm(reader.seekTo(4, context, true)));
            assertEquals("f", stringFromTerm(reader.seekTo(5, context, true)));
            assertEquals("g", stringFromTerm(reader.seekTo(6, context, true)));
            assertEquals("h", stringFromTerm(reader.seekTo(7, context, true)));
            assertEquals("h", stringFromTerm(reader.seekTo(7, context, true)));
            assertEquals("g", stringFromTerm(reader.seekTo(6, context, true)));
            assertEquals("f", stringFromTerm(reader.seekTo(5, context, true)));
            assertEquals("e", stringFromTerm(reader.seekTo(4, context, true)));
            assertEquals("d", stringFromTerm(reader.seekTo(3, context, true)));
            assertEquals("c", stringFromTerm(reader.seekTo(2, context, true)));
            assertEquals("b", stringFromTerm(reader.seekTo(1, context, true)));
            assertEquals("a", stringFromTerm(reader.seekTo(0, context, true)));
        }
    }

    @Test
    public void perSSTableSeekEdge() throws Throwable
    {
        List<Pair<ByteComparable, Long>> list = new ArrayList();
        list.add(Pair.create(v -> UTF8Type.instance.asComparableBytes(UTF8Type.instance.decompose("c"), v), 2L));
        list.add(Pair.create(v -> UTF8Type.instance.asComparableBytes(UTF8Type.instance.decompose("d"), v), 3L));
        list.add(Pair.create(v -> UTF8Type.instance.asComparableBytes(UTF8Type.instance.decompose("e"), v), 4L));
        list.add(Pair.create(v -> UTF8Type.instance.asComparableBytes(UTF8Type.instance.decompose("f"), v), 5L));

        BytesRefBuilder builder = new BytesRefBuilder();
        IndexDescriptor indexDescriptor = newIndexDescriptor();

        try (BlockIndexFileProvider fileProvider = new PerSSTableFileProvider(indexDescriptor);
             BlockIndexReader reader = createPerSSTableReader(fileProvider, list);
             BlockIndexReader.BlockIndexReaderContext context = reader.initContext())
        {
            BytesUtil.gatherBytes(v -> UTF8Type.instance.asComparableBytes(UTF8Type.instance.decompose("a"), v), builder);
            assertEquals(2L, reader.seekTo(builder.toBytesRef(), context));
        }
    }

    @Test
    public void randomPerSSTableSeekTo() throws Throwable
    {
        int numberOfIterations = randomIntBetween(10, 100);
        for (int iteration = 0; iteration < numberOfIterations; iteration++)
        {
            doRandomPerTableSeekTo(iteration);
        }
    }

    private void doRandomPerTableSeekTo(int iteration) throws Throwable
    {
        int numberOfStrings = randomIntBetween(100, 2000);

        List<String> strings = new ArrayList<>();
        Set<String> duplicates = new HashSet<>();
        Map<Long, String> rowIdToStringMap = new HashMap<>();
        List<Pair<ByteComparable, Long>> data = new ArrayList();

        for (long index = 0; index < numberOfStrings; index++)
        {
            String string = randomSimpleString(2, 20);
            while (duplicates.contains(string))
                string = randomSimpleString(2, 20);
            duplicates.add(string);
            strings.add(string);
        }

        strings.sort(String::compareTo);

        for (long index = 0; index < numberOfStrings; index++)
        {
            String string = strings.get((int)index);
            rowIdToStringMap.put(index, string);
            data.add(Pair.create(v -> UTF8Type.instance.asComparableBytes(UTF8Type.instance.decompose(string), v), index));
        }

        IndexDescriptor indexDescriptor = newIndexDescriptor();
        try (BlockIndexFileProvider fileProvider = new PerSSTableFileProvider(indexDescriptor);
             BlockIndexReader reader = createPerSSTableReader(fileProvider, data);
             BlockIndexReader.BlockIndexReaderContext context = reader.initContext())
        {
            for (int index = 0; index < randomIntBetween(500, 1500); index++)
            {
//                System.out.println("index = " + index);
//                if (iteration == 7 && index == 392)
//                    System.out.println();
                long rowId = nextLong(0, numberOfStrings);

                if (randomBoolean())
                {
                    long pointId = reader.seekTo(new BytesRef(rowIdToStringMap.get(rowId)), context);

                    assertEquals(rowId, pointId);
                }
                else
                {
                    assertEquals(rowIdToStringMap.get(rowId), stringFromTerm(reader.seekTo(rowId, context, true)));
                }
            }
        }
    }

    @Test
    public void testTraversalPostings() throws Throwable
    {
        Map<String, LongArrayList> expected = new TreeMap<>();
        expected.put("a", toLongArrayList(0));
        expected.put("b", toLongArrayList(1, 2));
        expected.put("c", toLongArrayList(3, 4, 5));
        expected.put("d", toLongArrayList(6, 7, 8, 9));
        expected.put("e", toLongArrayList(10, 11, 12, 13, 14));
        expected.put("f", toLongArrayList(15, 16, 17, 18, 19, 20));

        List<Pair<ByteComparable, LongArrayList>> termsList = new ArrayList();
        for (String term : expected.keySet())
        {
            LongArrayList postings = expected.get(term);
            termsList.add(Pair.create(v -> UTF8Type.instance.asComparableBytes(UTF8Type.instance.decompose(term), v), postings));
        }

        IndexDescriptor indexDescriptor = newIndexDescriptor();
        IndexContext indexContext = createIndexContext("index_test1", UTF8Type.instance);

        try (BlockIndexFileProvider fileProvider = new PerIndexFileProvider(indexDescriptor, indexContext);
             BlockIndexReader blockIndexReader = createPerIndexReader(fileProvider, termsList))
        {
            for (String term : expected.keySet())
            {
                ByteComparable searchValue = v -> UTF8Type.instance.asComparableBytes(UTF8Type.instance.decompose(term), v);
                LongArrayList expectedPostings = expected.get(term);

                List<PostingList.PeekablePostingList> postingLists = blockIndexReader.traverse(searchValue, searchValue);

                try (PostingList postingList = MergePostingList.merge(postingLists))
                {
                    int count = 0;

                    while (true)
                    {
                        long rowId = postingList.nextPosting();

                        if (rowId == PostingList.END_OF_STREAM)
                            break;

                        assertTrue(count < expectedPostings.size());
                        assertEquals(expectedPostings.get(count), rowId);
                        count++;
                    }
                    assertEquals("Traverse failed for term [" + term + "]. Should have got " + expectedPostings.size() + " rows but only got " + count,
                                 expectedPostings.size(),
                                 count);
                }
            }
        }
    }

    @Test
    public void testMultiPostingsWriter() throws Throwable
    {
        ByteComparable term = v -> UTF8Type.instance.asComparableBytes(UTF8Type.instance.decompose("a"), v);

        IndexDescriptor indexDescriptor = newIndexDescriptor();
        IndexContext indexContext = createIndexContext("index_test1", UTF8Type.instance);

        try (BlockIndexFileProvider fileProvider = new PerIndexFileProvider(indexDescriptor, indexContext);
             BlockIndexWriter blockIndexWriter = new BlockIndexWriter(fileProvider, false))
        {
            for (int rowId = 0; rowId < 100000; rowId++)
                blockIndexWriter.add(term, rowId);

            BlockIndexMeta meta = blockIndexWriter.finish();
        }
    }

    private LongArrayList toLongArrayList(long... rowIds)
    {
        LongArrayList list = new LongArrayList();
        list.add(rowIds);
        return list;
    }

    @Test
    public void testSameTerms() throws Exception
    {
        List<Pair<ByteComparable, LongArrayList>> list = new ArrayList();
        list.add(add("aaa", new long[]{ 100 })); // 0
        list.add(add("aaabbb", new long[]{ 0, 1 })); // 0
        list.add(add("aaabbb", new long[]{ 2, 3, 4 })); // 1

        list.add(add("cccc", new long[]{ 15 })); // 2
        list.add(add("gggaaaddd", new long[]{ 200, 201, 203 })); // 3
        list.add(add("gggzzzz", new long[]{ 500, 501, 502, 503, 504, 505 })); // 4, 5
        list.add(add("zzzzz", new long[]{ 700, 780, 782, 790, 794, 799 })); //

        IndexDescriptor indexDescriptor = newIndexDescriptor();
        IndexContext indexContext = createIndexContext("index_test1", UTF8Type.instance);

        try (BlockIndexFileProvider fileProvider = new PerIndexFileProvider(indexDescriptor, indexContext);
             BlockIndexReader blockIndexReader = createPerIndexReader(fileProvider, list);
             BlockIndexReader.BlockIndexReaderContext context = blockIndexReader.initContext())
        {
            long pointId = blockIndexReader.seekTo(new BytesRef("cccc"), context);
        }
    }

    @Test
    public void randomTest() throws Exception
    {
        for (int x = 0; x < 20; x++)
        {
            doRandomTest();
        }
    }

    public void doRandomTest() throws Exception
    {
        List<Pair<ByteComparable, LongArrayList>> list = new ArrayList();
        int numValues = nextInt(500, 1000);
        final BKDTreeRamBuffer buffer = new BKDTreeRamBuffer(1, Integer.BYTES);

        int maxRowID = -1;
        int totalRows = 0;
        for (int x = 0; x < numValues; x++)
        {
            byte[] scratch = new byte[4];
            NumericUtils.intToSortableBytes(x, scratch, 0);

            int numRows = nextInt(1, 20);
            int rowID = nextInt(100);
            LongArrayList postings = new LongArrayList();
            for (int i = 0; i < numRows; i++)
            {
                maxRowID = Math.max(maxRowID, rowID);
                postings.add(rowID);
                totalRows++;
                buffer.addPackedValue(rowID, new BytesRef(scratch));
                rowID += nextInt(1, 100);
            }
//            System.out.println("term=" + x + " postings=" + postings);
            list.add(Pair.create(ByteComparable.fixedLength(scratch), postings));
        }

        int startIdx = nextInt(0, numValues - 2);
        ByteComparable start = list.get(startIdx).left;

        int queryMin = NumericUtils.sortableBytesToInt(ByteSourceInverse.readBytes(start.asComparableBytes(ByteComparable.Version.OSS41)), 0);

        int endIdx = nextInt(startIdx + 1, numValues - 1);
        ByteComparable end = list.get(endIdx).left;

        int queryMax = NumericUtils.sortableBytesToInt(ByteSourceInverse.readBytes(end.asComparableBytes(ByteComparable.Version.OSS41)), 0);

        String indexName = "index_yay";
        IndexContext indexContext = createIndexContext(indexName, UTF8Type.instance);

//        System.out.println("queryMin=" + queryMin + " queryMax=" + queryMax + " totalRows=" + totalRows + " maxRowID=" + maxRowID);

        IndexDescriptor indexDescriptor = newIndexDescriptor();

        final BKDReader bkdReader = finishAndOpenReaderOneDim(2,
                                                              buffer,
                                                              indexDescriptor,
                                                              maxRowID + 1,
                                                              totalRows,
                                                              indexName);

        final PostingList kdtreePostings = bkdReader.intersect(buildQuery(queryMin, queryMax), (QueryEventListener.BKDIndexEventListener) NO_OP_BKD_LISTENER, new SSTableQueryContext(new QueryContext()));

        IntArrayList kdtreePostingList = collect(kdtreePostings);
        kdtreePostings.close();
        bkdReader.close();

        IndexDescriptor indexDescriptor1 = newIndexDescriptor();
        BlockIndexFileProvider fileProvider = new PerIndexFileProvider(indexDescriptor1, indexContext);
        BlockIndexWriter prefixBytesWriter = new BlockIndexWriter(fileProvider, false);

        TermsIterator terms = new MemtableTermsIterator(null,
                                                        null,
                                                        list.iterator());

        int pointCount = 0;
        while (terms.hasNext())
        {
            ByteComparable term = terms.next();
            PostingList postings = terms.postings();
            while (true)
            {
                long rowID = postings.nextPosting();
                if (rowID == PostingList.END_OF_STREAM) break;
                prefixBytesWriter.add(term, rowID);
                pointCount++;
            }
        }

        BlockIndexMeta meta = prefixBytesWriter.finish();

        BlockIndexReader reader = new BlockIndexReader(fileProvider, false, meta);

        PostingList postings = BlockIndexReader.toOnePostingList(reader.traverse(start, end));
        IntArrayList results2 = collect(postings);

        postings.close();
        fileProvider.close();
        reader.close();

        assertEquals(kdtreePostingList, results2);
    }

    //TODO Rig a generic method for this in TypeUtil or the like to
    // compose a type from a BytesRef
    private String stringFromTerm(BytesRef term)
    {
        ByteSource byteSource = ByteSource.fixedLength(term.bytes, 0, term.length);
        ByteBuffer byteBuffer = UTF8Type.instance.fromComparableBytes(ByteSource.peekable(byteSource), ByteComparable.Version.OSS41);
        return UTF8Type.instance.compose(byteBuffer);
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
            public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue)
            {
                int min = NumericUtils.sortableBytesToInt(minPackedValue, 0);
                int max = NumericUtils.sortableBytesToInt(maxPackedValue, 0);
                assert max >= min;

                if (max < queryMin || min > queryMax)
                {
                    return PointValues.Relation.CELL_OUTSIDE_QUERY;
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

    private IntArrayList collect(PostingList postings) throws IOException
    {
        IntArrayList list = new IntArrayList();
        if (postings == null) return list;
        while (true)
        {
            long rowid = postings.nextPosting();
            if (rowid == PostingList.END_OF_STREAM) break;
            if (list.size() > 0)
            {
                if (list.get(list.size() - 1) == rowid)
                {
                    continue;
                }
            }
            list.add((int) rowid);
        }
        return list;
    }


    private BKDReader finishAndOpenReaderOneDim(int maxPointsPerLeaf,
                                                BKDTreeRamBuffer buffer,
                                                IndexDescriptor indexDescriptor,
                                                int maxRowID,
                                                int totalRows,
                                                String index
                                                ) throws IOException
    {
        IndexContext indexContext = createIndexContext(index, Int32Type.instance);

        final NumericIndexWriter writer = new NumericIndexWriter(indexDescriptor,
                                                                 indexContext,
                                                                 Integer.BYTES,
                                                                 maxRowID,
                                                                 totalRows,
                                                                 new IndexWriterConfig("test", 2, 8),
                                                                 false);

        final SegmentMetadata.ComponentMetadataMap metadata = writer.writeAll(buffer.asPointValues());
        final long bkdPosition = metadata.get(IndexComponent.KD_TREE).root;
        assertThat(bkdPosition, is(greaterThan(0L)));
        final long postingsPosition = metadata.get(IndexComponent.KD_TREE_POSTING_LISTS).root;
        assertThat(postingsPosition, is(greaterThan(0L)));

        FileHandle kdtreeHandle = indexDescriptor.createPerIndexFileHandle(IndexComponent.KD_TREE, indexContext);
        FileHandle kdtreePostingsHandle = indexDescriptor.createPerIndexFileHandle(IndexComponent.KD_TREE_POSTING_LISTS, indexContext);
        return new BKDReader(indexContext,
                             kdtreeHandle,
                             bkdPosition,
                             kdtreePostingsHandle,
                             postingsPosition);
    }

    public static Pair<ByteComparable, LongArrayList> add(String term, long[] array)
    {
        LongArrayList list = new LongArrayList();
        list.add(array, 0, array.length);
        return Pair.create(v -> UTF8Type.instance.asComparableBytes(UTF8Type.instance.decompose(term), v), list);
    }
}
