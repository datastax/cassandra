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
package org.apache.cassandra.index.sai.disk.v1.postings;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.format.IndexComponents;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.io.IndexInput;
import org.apache.cassandra.index.sai.metrics.QueryEventListener;
import org.apache.cassandra.index.sai.postings.IntArrayPostingList;
import org.apache.cassandra.index.sai.utils.SAICodecUtils;
import org.apache.cassandra.index.sai.utils.SaiRandomizedTest;

public class PostingsTest extends SaiRandomizedTest
{
    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    private IndexDescriptor indexDescriptor;
    private String index;
    private IndexContext indexContext;

    @Before
    public void setup() throws Throwable
    {
        indexDescriptor = newIndexDescriptor();
        index = newIndex();
        indexContext = SAITester.createIndexContext(index, UTF8Type.instance);

    }

    @Test
    public void testSingleBlockPostingList() throws Exception
    {
        final int blockSize = 1 << between(3, 8);
        final IntArrayPostingList expectedPostingList = new IntArrayPostingList(new int[]{ 10, 20, 30, 40, 50, 60 });

        long postingPointer;
        IndexComponents.ForWrite components = indexDescriptor.newPerIndexComponentsForWrite(indexContext);
        try (PostingsWriter writer = new PostingsWriter(components, blockSize))
        {
            postingPointer = writer.write(expectedPostingList);
            writer.complete();
        }

        IndexInput input = components.get(IndexComponentType.POSTING_LISTS).openInput();
        SAICodecUtils.validate(input);
        input.seek(postingPointer);

        final PostingsReader.BlocksSummary summary = assertBlockSummary(blockSize, expectedPostingList, input);
        assertEquals(1, summary.offsets.length());

        CountingPostingListEventListener listener = new CountingPostingListEventListener();
        PostingsReader reader = new PostingsReader(input, postingPointer, listener);

        expectedPostingList.reset();
        assertEquals(expectedPostingList.getOrdinal(), reader.getOrdinal());
        assertEquals(expectedPostingList.size(), reader.size());

        long actualRowID;
        while ((actualRowID = reader.nextPosting()) != PostingList.END_OF_STREAM)
        {
            assertEquals(expectedPostingList.nextPosting(), actualRowID);
            assertEquals(expectedPostingList.getOrdinal(), reader.getOrdinal());
        }
        assertEquals(PostingList.END_OF_STREAM, expectedPostingList.nextPosting());
        assertEquals(0, listener.advances);
        reader.close();
        assertEquals(reader.size(), listener.decodes);

        input = components.get(IndexComponentType.POSTING_LISTS).openInput();
        listener = new CountingPostingListEventListener();
        reader = new PostingsReader(input, postingPointer, listener);

        assertEquals(50, reader.advance(45));
        assertEquals(60, reader.advance(60));
        assertEquals(PostingList.END_OF_STREAM, reader.nextPosting());
        assertEquals(2, listener.advances);
        reader.close();

        assertEquals(reader.size(), listener.decodes); // nothing more was decoded
    }

    @Test
    public void testMultiBlockPostingList() throws Exception
    {
        final int numPostingLists = 1 << between(1, 5);
        final int blockSize = 1 << between(5, 10);
        final int numPostings = between(1 << 11, 1 << 15);
        final IntArrayPostingList[] expected = new IntArrayPostingList[numPostingLists];
        final long[] postingPointers = new long[numPostingLists];

        IndexComponents.ForWrite components = indexDescriptor.newPerIndexComponentsForWrite(indexContext);
        try (PostingsWriter writer = new PostingsWriter(components, blockSize))
        {
            for (int i = 0; i < numPostingLists; ++i)
            {
                final int[] postings = randomPostings(numPostings);
                final IntArrayPostingList postingList = new IntArrayPostingList(postings);
                expected[i] = postingList;
                postingPointers[i] = writer.write(postingList);
            }
            writer.complete();
        }

        IndexComponent.ForRead postingLists = components.get(IndexComponentType.POSTING_LISTS);
        try (IndexInput input = postingLists.openInput())
        {
            SAICodecUtils.validate(input);
        }

        for (int i = 0; i < numPostingLists; ++i)
        {
            IndexInput input = postingLists.openInput();
            input.seek(postingPointers[i]);
            final IntArrayPostingList expectedPostingList = expected[i];
            final PostingsReader.BlocksSummary summary = assertBlockSummary(blockSize, expectedPostingList, input);
            assertTrue(summary.offsets.length() > 1);

            final CountingPostingListEventListener listener = new CountingPostingListEventListener();
            try (PostingsReader reader = new PostingsReader(input, postingPointers[i], listener))
            {
                expectedPostingList.reset();
                assertEquals(expectedPostingList.getOrdinal(), reader.getOrdinal());
                assertEquals(expectedPostingList.size(), reader.size());

                assertPostingListEquals(expectedPostingList, reader);
                assertEquals(0, listener.advances);
            }

            // test skipping to the last block
            input = postingLists.openInput();
            try (PostingsReader reader = new PostingsReader(input, postingPointers[i], listener))
            {
                int tokenToAdvance = -1;
                expectedPostingList.reset();
                for (int p = 0; p < numPostings - 7; ++p)
                {
                    tokenToAdvance = expectedPostingList.nextPosting();
                }

                expectedPostingList.reset();
                assertEquals(expectedPostingList.advance(tokenToAdvance),
                             reader.advance(tokenToAdvance));

                assertPostingListEquals(expectedPostingList, reader);
                assertEquals(1, listener.advances);
            }
        }
    }

    @Test
    public void testAdvance() throws Exception
    {
        final int blockSize = 4; // 4 postings per FoR block
        final int maxSegmentRowID = 30;
        final int[] postings = IntStream.range(0, maxSegmentRowID).toArray(); // 30 postings = 7 FoR blocks + 1 VLong block
        final IntArrayPostingList expected = new IntArrayPostingList(postings);

        long fp;
        IndexComponents.ForWrite components = indexDescriptor.newPerIndexComponentsForWrite(indexContext);
        try (PostingsWriter writer = new PostingsWriter(components, blockSize))
        {
            fp = writer.write(expected);
            writer.complete();
        }

        IndexComponent.ForRead postingLists = components.get(IndexComponentType.POSTING_LISTS);
        try (IndexInput input = postingLists.openInput())
        {
            SAICodecUtils.validate(input);
            input.seek(fp);

            final PostingsReader.BlocksSummary summary = assertBlockSummary(blockSize, expected, input);
            assertEquals((int) Math.ceil((double) maxSegmentRowID / blockSize), summary.offsets.length());

            for (int i = 0; i < summary.maxValues.length(); i++)
            {
                assertEquals(Math.min(maxSegmentRowID - 1, (i + 1) * blockSize - 1), summary.maxValues.get(i));
            }
        }

        // exact advance
        testAdvance(postingLists, fp, expected, new int[]{ 3, 7, 11, 15, 19 });
        // non-exact advance
        testAdvance(postingLists, fp, expected, new int[]{ 2, 6, 12, 17, 25 });

        // exact advance
        testAdvance(postingLists, fp, expected, new int[]{ 3, 5, 7, 12 });
        // non-exact advance
        testAdvance(postingLists, fp, expected, new int[]{ 2, 7, 9, 11 });
    }

    @Test
    public void testAdvanceOnRandomizedData() throws IOException
    {
        final int blockSize = 4;
        final int numPostings = nextInt(64, 64_000);
        final int[] postings = randomPostings(numPostings);

        final IntArrayPostingList expected = new IntArrayPostingList(postings);

        long fp;
        IndexComponents.ForWrite components = indexDescriptor.newPerIndexComponentsForWrite(indexContext);
        try (PostingsWriter writer = new PostingsWriter(components, blockSize))
        {
            fp = writer.write(expected);
            writer.complete();
        }

        IndexComponent.ForRead postingLists = components.get(IndexComponentType.POSTING_LISTS);
        try (IndexInput input = postingLists.openInput())
        {
            SAICodecUtils.validate(input);
            input.seek(fp);

            final PostingsReader.BlocksSummary summary = assertBlockSummary(blockSize, expected, input);
            assertEquals((int) Math.ceil((double) numPostings / blockSize), summary.offsets.length());

            for (int i = 0; i < summary.maxValues.length(); i++)
            {
                assertEquals(postings[Math.min(numPostings - 1, (i + 1) * blockSize - 1)], summary.maxValues.get(i));
            }
        }

        testAdvance(postingLists, fp, expected, postings);
    }

    @Test
    public void testNullPostingList() throws IOException
    {
        IndexComponents.ForWrite components = indexDescriptor.newPerIndexComponentsForWrite(indexContext);
        try (PostingsWriter writer = new PostingsWriter(components))
        {
            expectedException.expect(IllegalArgumentException.class);
            writer.write(null);
            writer.complete();
        }
    }

    @Test
    public void testEmptyPostingList() throws IOException
    {
        IndexComponents.ForWrite components = indexDescriptor.newPerIndexComponentsForWrite(indexContext);
        try (PostingsWriter writer = new PostingsWriter(components))
        {
            expectedException.expect(IllegalArgumentException.class);
            writer.write(new IntArrayPostingList(new int[0]));
        }
    }

    @Test
    public void testNonAscendingPostingList() throws IOException
    {
        IndexComponents.ForWrite components = indexDescriptor.newPerIndexComponentsForWrite(indexContext);
        try (PostingsWriter writer = new PostingsWriter(components))
        {
            expectedException.expect(IllegalArgumentException.class);
            writer.write(new IntArrayPostingList(new int[]{ 1, 0 }));
        }
    }

    private void testAdvance(IndexComponent.ForRead postingLists, long fp, IntArrayPostingList expected, int[] targetIDs) throws IOException
    {
        expected.reset();
        final CountingPostingListEventListener listener = new CountingPostingListEventListener();
        PostingsReader reader = openReader(postingLists, fp, listener);
        for (int i = 0; i < 2; ++i)
        {
            assertEquals(expected.nextPosting(), reader.nextPosting());
            assertEquals(expected.getOrdinal(), reader.getOrdinal());
        }

        for (int target : targetIDs)
        {
            final long actualRowId = reader.advance(target);
            final long expectedRowId = expected.advance(target);

            assertEquals(expectedRowId, actualRowId);

            assertEquals(expected.getOrdinal(), reader.getOrdinal());
        }

        // check if iterator is correctly positioned
        assertPostingListEquals(expected, reader);
        // check if reader emitted all events
        assertEquals(targetIDs.length, listener.advances);

        reader.close();
    }

    private PostingsReader openReader(IndexComponent.ForRead postingLists, long fp, QueryEventListener.PostingListEventListener listener) throws IOException
    {
        IndexInput input = postingLists.openInput();
        input.seek(fp);
        return new PostingsReader(input, fp, listener);
    }

    private PostingsReader.BlocksSummary assertBlockSummary(int blockSize, PostingList expected, IndexInput input) throws IOException
    {
        final PostingsReader.BlocksSummary summary = new PostingsReader.BlocksSummary(input, input.getFilePointer());
        assertEquals(blockSize, summary.blockEntries);
        assertEquals(expected.size(), summary.numPostings);
        assertTrue(summary.offsets.length() > 0);
        assertEquals(summary.offsets.length(), summary.maxValues.length());
        return summary;
    }

    private int[] randomPostings(int numPostings)
    {
        final AtomicInteger rowId = new AtomicInteger();
        // postings with duplicates
        return IntStream.generate(() -> rowId.getAndAdd(randomIntBetween(0, 4)))
                        .limit(numPostings)
                        .toArray();
    }

    static class CountingPostingListEventListener implements QueryEventListener.PostingListEventListener
    {
        int advances;
        int decodes;

        @Override
        public void onAdvance()
        {
            advances++;
        }

        @Override
        public void postingDecoded(long postingsDecoded)
        {
            this.decodes += postingsDecoded;
        }
    }
}
