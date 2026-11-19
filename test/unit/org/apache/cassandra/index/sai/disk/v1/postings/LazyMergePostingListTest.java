/*
 * Copyright IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.index.sai.disk.v1.postings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;

import org.junit.Test;

import org.apache.cassandra.index.sai.disk.PostingList;
import org.agrona.collections.LongHashSet;
import org.apache.cassandra.index.sai.postings.IntArrayPostingList;
import org.apache.cassandra.utils.CloseableIterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

public class LazyMergePostingListTest
{
    @Test
    public void duplicateHeadsBeyondCapacity() throws IOException
    {
        // More lists start at row 5 than the capacity: the dropped ones must still contribute their later row ids.
        assertMerges(new int[][]{ { 5, 10 }, { 5, 11 }, { 5, 12 }, { 5, 13 } }, 1, 2);
        assertMerges(new int[][]{ { 5, 10 }, { 5, 11 }, { 5, 12 }, { 5, 13 } }, 2, 2);
    }

    @Test
    public void randomizedNextPosting() throws IOException
    {
        Random random = new Random(1);
        for (int iteration = 0; iteration < 2000; iteration++)
        {
            int[][] lists = randomLists(random);
            int initCapacity = 1 + random.nextInt(4);
            int maxCapacity = initCapacity + random.nextInt(4);
            assertMerges(lists, initCapacity, maxCapacity);
        }
    }

    @Test
    public void randomizedAdvance() throws IOException
    {
        Random random = new Random(2);
        for (int iteration = 0; iteration < 2000; iteration++)
        {
            int[][] lists = randomLists(random);
            int initCapacity = 1 + random.nextInt(4);
            int maxCapacity = initCapacity + random.nextInt(4);

            TreeSet<Integer> expected = union(lists);
            InstrumentedSource source = new InstrumentedSource(lists);
            try (LazyMergePostingList merged = new LazyMergePostingList(source, initCapacity, maxCapacity))
            {
                int target = 0;
                while (true)
                {
                    target += random.nextInt(8);
                    Integer expectedNext = expected.ceiling(target);
                    int actual = merged.advance(target);
                    assertEquals(expectedNext == null ? PostingList.END_OF_STREAM : expectedNext, actual);
                    if (actual == PostingList.END_OF_STREAM)
                        break;
                    expected = new TreeSet<>(expected.tailSet(actual, false));
                    target = actual + 1;
                }
            }
            source.assertAllClosed();
        }
    }

    @Test
    public void emptyPostingLists() throws IOException
    {
        // One empty list, one non-empty list
        int[][] lists = { {}, { 10, 20 } };

        InstrumentedSource source = new InstrumentedSource(lists);
        try (LazyMergePostingList merged = new LazyMergePostingList(source, 2, 4))
        {
            // The iterator from refill(0) during construction must already be closed
            source.assertAllIteratorsClosed();
            assertEquals(10, merged.nextPosting());
            assertEquals(20, merged.nextPosting());
            assertEquals(PostingList.END_OF_STREAM, merged.nextPosting());
        }

        source.assertAllClosed();
        // The empty list should be opened and closed immediately during initialization scan
        assertTrue(source.openedLists.get(0).isClosed);
        assertTrue(source.openedLists.get(1).isClosed);
    }

    @Test
    public void emptySource() throws IOException
    {
        int[][] lists = {};
        InstrumentedSource source = new InstrumentedSource(lists);
        try (LazyMergePostingList merged = new LazyMergePostingList(source, 2, 4))
        {
            source.assertAllIteratorsClosed();
            assertEquals(PostingList.END_OF_STREAM, merged.nextPosting());
            assertEquals(0, merged.size());
        }
        source.assertAllClosed();
    }

    @Test
    public void testDuplicates() throws IOException
    {
        // Duplicate row IDs across lists, and within a single list
        int[][] lists = {
            { 5, 5, 10, 15, 15 },
            { 5, 10, 10, 20 }
        };

        InstrumentedSource source = new InstrumentedSource(lists);
        try (LazyMergePostingList merged = new LazyMergePostingList(source, 2, 4))
        {
            source.assertAllIteratorsClosed();
            assertEquals(5, merged.nextPosting());
            assertEquals(10, merged.nextPosting());
            assertEquals(15, merged.nextPosting());
            assertEquals(20, merged.nextPosting());
            assertEquals(PostingList.END_OF_STREAM, merged.nextPosting());
        }

        source.assertAllClosed();
    }

    @Test
    public void testOverCapacity() throws IOException
    {
        int[][] lists = { { 10 }, { 20 }, { 30 } };
        InstrumentedSource source = new InstrumentedSource(lists);
        try (LazyMergePostingList merged = new LazyMergePostingList(source, 2, 4))
        {
            assertEquals(1, source.walksCount);
            assertEquals(1, source.openedIterators.size());
            assertTrue(source.openedIterators.get(0).isClosed); // refill(0) iterator is closed
            assertEquals(3, source.openedLists.size()); // List 0, 1, 2 opened initially
            assertTrue(source.openedLists.get(2).isClosed); // List 2 displaced and closed
            assertFalse(source.openedLists.get(0).isClosed); // List 0 kept
            assertFalse(source.openedLists.get(1).isClosed); // List 1 kept

            assertEquals(10, merged.nextPosting());
            assertEquals(20, merged.nextPosting());

            assertEquals(1, source.walksCount); // No new walk yet

            // Now read next posting, which will trigger refill(30)
            assertEquals(30, merged.nextPosting());
            assertEquals(2, source.walksCount); // Walk count incremented to 2
            assertEquals(2, source.openedIterators.size());
            assertTrue(source.openedIterators.get(1).isClosed); // refill(30) iterator is closed immediately
            assertEquals(4, source.openedLists.size()); // List 2 opened again
            // Lists 0 and 1 are exhausted, but the resized queue has room for them, so it keeps them
            // and the walk does not open them again.
            assertFalse(source.openedLists.get(0).isClosed);
            assertFalse(source.openedLists.get(1).isClosed);
            assertFalse(source.openedLists.get(3).isClosed); // Still open in queue (head is END_OF_STREAM)

            assertEquals(PostingList.END_OF_STREAM, merged.nextPosting());
        }
        source.assertAllClosed();
    }

    @Test
    public void testOverMaxCapacity() throws IOException
    {
        int[][] lists = { { 10 }, { 20 }, { 30 }, { 40 } };
        InstrumentedSource source = new InstrumentedSource(lists);
        try (LazyMergePostingList merged = new LazyMergePostingList(source, 2, 2))
        {
            source.assertAllIteratorsClosed();
            assertEquals(10, merged.nextPosting());
            assertEquals(20, merged.nextPosting());

            // Next posting triggers refill(30), capacity remains 2
            assertEquals(30, merged.nextPosting());
            assertEquals(2, source.walksCount);
            source.assertAllIteratorsClosed();

            assertEquals(40, merged.nextPosting());
            assertEquals(PostingList.END_OF_STREAM, merged.nextPosting());
        }
        source.assertAllClosed();
    }

    @Test
    public void testRefillOnAdvance() throws IOException
    {
        int[][] lists = { { 10, 50 }, { 20, 60 }, { 30, 70 } };
        InstrumentedSource source = new InstrumentedSource(lists);
        try (LazyMergePostingList merged = new LazyMergePostingList(source, 2, 4))
        {
            assertEquals(1, source.walksCount);
            assertEquals(1, source.openedIterators.size());
            assertTrue(source.openedIterators.get(0).isClosed);
            // List 0, 1, 2 opened initially
            assertEquals(3, source.openedLists.size());

            // Advance to 15 (below bound 30): should not trigger refill
            assertEquals(20, merged.advance(15));
            assertEquals(1, source.walksCount);

            // Advance to 35 (above/equal bound 30): triggers refill(35)
            assertEquals(50, merged.advance(35));
            assertEquals(2, source.walksCount);
            assertEquals(2, source.openedIterators.size());
            assertTrue(source.openedIterators.get(1).isClosed);

            // Check that List 2 was opened again (total 4 opened lists)
            assertEquals(4, source.openedLists.size());

            // Read the rest: 60, then 70, then END_OF_STREAM
            assertEquals(60, merged.nextPosting());
            assertEquals(70, merged.nextPosting());
            assertEquals(PostingList.END_OF_STREAM, merged.nextPosting());
        }
        source.assertAllClosed();
    }

    @Test
    public void testMultipleRefills() throws IOException
    {
        int[][] lists = {
            { 10, 40, 70 },
            { 20, 50, 80 },
            { 30, 60, 90 }
        };
        InstrumentedSource source = new InstrumentedSource(lists);
        try (LazyMergePostingList merged = new LazyMergePostingList(source, 1, 4))
        {
            assertEquals(1, source.walksCount);
            assertEquals(1, source.openedIterators.size());
            assertTrue(source.openedIterators.get(0).isClosed);
            assertEquals(10, merged.nextPosting());

            // Next posting triggers refill(20)
            assertEquals(20, merged.nextPosting());
            assertEquals(2, source.walksCount);
            assertEquals(2, source.openedIterators.size());
            assertTrue(source.openedIterators.get(1).isClosed);

            // No refill for 30
            assertEquals(30, merged.nextPosting());
            assertEquals(2, source.walksCount);

            // Next posting triggers refill(40)
            assertEquals(40, merged.nextPosting());
            assertEquals(3, source.walksCount);
            assertEquals(3, source.openedIterators.size());
            assertTrue(source.openedIterators.get(2).isClosed);

            assertEquals(50, merged.nextPosting());
            assertEquals(60, merged.nextPosting());
            assertEquals(70, merged.nextPosting());
            assertEquals(80, merged.nextPosting());
            assertEquals(90, merged.nextPosting());
            assertEquals(PostingList.END_OF_STREAM, merged.nextPosting());
        }
        source.assertAllClosed();
    }

    @Test
    public void testFailureOnOpen()
    {
        int[][] lists = { { 10 }, { 20 }, { 30 } };
        InstrumentedSource source = new InstrumentedSource(lists);
        // Fail when opening list index 1
        source.failOnIds.add(1L);

        try (var ignored = new LazyMergePostingList(source, 3, 4))
        {
            org.junit.Assert.fail("Expected IOException");
        }
        catch (IOException e)
        {
            assertEquals("Simulated open failure for 1", e.getMessage());
        }

        // Verify that everything already opened is properly closed on failure, and source is closed
        assertEquals(1, source.openedLists.size());
        assertTrue(source.openedLists.get(0).isClosed);
        source.assertAllClosed();
    }

    @Test
    public void testFailureOnAdvance() throws IOException
    {
        int[][] lists = { { 10, 20 } };
        InstrumentedSource source = new InstrumentedSource(lists);
        try (LazyMergePostingList merged = new LazyMergePostingList(source, 2, 4))
        {
            // Now make the list fail on advance
            source.openedLists.get(0).failOnAdvance = true;

            try
            {
                merged.advance(15);
                org.junit.Assert.fail("Expected IOException");
            }
            catch (IOException e)
            {
                assertEquals("Simulated advance failure", e.getMessage());
            }
        }
        // Verify that the list and source are properly closed on merged.close()
        source.assertAllClosed();
    }

    @Test
    public void testFailureOnNextPosting() throws IOException
    {
        int[][] lists = { { 10, 20 } };
        InstrumentedSource source = new InstrumentedSource(lists);
        try (LazyMergePostingList merged = new LazyMergePostingList(source, 2, 4))
        {
            // Now make the list fail on nextPosting
            source.openedLists.get(0).failOnNextPosting = true;

            try
            {
                merged.nextPosting();
                org.junit.Assert.fail("Expected IOException");
            }
            catch (IOException e)
            {
                assertEquals("Simulated nextPosting failure", e.getMessage());
            }
        }
        // Verify that the list and source are properly closed on merged.close()
        source.assertAllClosed();
    }

    @Test
    public void testFailureDuringRefill() throws IOException
    {
        int[][] lists = { { 10 }, { 20 }, { 30 } };
        InstrumentedSource source = new InstrumentedSource(lists);
        try (LazyMergePostingList merged = new LazyMergePostingList(source, 2, 4))
        {
            assertEquals(10, merged.nextPosting());
            assertEquals(20, merged.nextPosting());

            // Next posting triggers refill(30). Let's make it fail on open
            source.failOnOpen = true;

            try
            {
                merged.nextPosting();
                org.junit.Assert.fail("Expected IOException");
            }
            catch (IOException e)
            {
                assertEquals("Simulated open failure", e.getMessage());
            }

            // Verify that everything opened so far is closed immediately on refill failure, and source is closed
            source.assertAllClosed();
        }
    }

    @Test
    public void testFailureOnInitialNextPosting()
    {
        int[][] lists = { { 10 }, { 20 } };
        InstrumentedSource source = new InstrumentedSource(lists)
        {
            @Override
            public PostingList open(long postingListId) throws IOException
            {
                InstrumentedPostingList list = (InstrumentedPostingList) super.open(postingListId);
                if (postingListId == 1L)
                    list.failOnFirstNextPosting = true;
                return list;
            }
        };

        try (var ignored = new LazyMergePostingList(source, 2, 4))
        {
            org.junit.Assert.fail("Expected IOException");
        }
        catch (IOException e)
        {
            assertEquals("Simulated initial nextPosting failure", e.getMessage());
        }

        // Verify that everything already opened (List 0) is closed, and source is closed
        assertEquals(2, source.openedLists.size());
        assertTrue(source.openedLists.get(0).isClosed);
        assertTrue(source.openedLists.get(1).isClosed); // Also List 1 should be closed because its nextPosting failed and Candidate constructor caught it and closed it quietly
        source.assertAllClosed();
    }

    @Test
    public void testFailureOnWalk()
    {
        int[][] lists = { { 10 }, { 20 } };
        InstrumentedSource source = new InstrumentedSource(lists);
        source.failOnWalk = true;

        try (var ignored = new LazyMergePostingList(source, 2, 4))
        {
            org.junit.Assert.fail("Expected RuntimeException");
        }
        catch (Exception e)
        {
            assertEquals("Simulated walk failure", e.getMessage());
        }

        assertTrue(source.isClosed);
    }

    @Test
    public void testFailureOnIteratorHasNext()
    {
        int[][] lists = { { 10 }, { 20 } };
        InstrumentedSource source = new InstrumentedSource(lists);
        source.failOnIteratorHasNext = true;

        try (var ignored = new LazyMergePostingList(source, 2, 4))
        {
            org.junit.Assert.fail("Expected RuntimeException");
        }
        catch (Exception e)
        {
            assertEquals("Simulated hasNext failure", e.getMessage());
        }

        source.assertAllClosed();
    }

    @Test
    public void testFailureOnIteratorNext()
    {
        int[][] lists = { { 10 }, { 20 } };
        InstrumentedSource source = new InstrumentedSource(lists);
        source.failOnIteratorNext = true;

        try (var ignored = new LazyMergePostingList(source, 2, 4))
        {
            org.junit.Assert.fail("Expected RuntimeException");
        }
        catch (Exception e)
        {
            assertEquals("Simulated next failure", e.getMessage());
        }

        source.assertAllClosed();
    }

    private static class InstrumentedPostingList implements PostingList
    {
        private final PostingList delegate;
        boolean isClosed;
        int nextPostingCalls;
        int advanceCalls;
        boolean failOnNextPosting;
        boolean failOnAdvance;
        boolean failOnFirstNextPosting;

        InstrumentedPostingList(PostingList delegate)
        {
            this.delegate = delegate;
        }

        @Override
        public int nextPosting() throws IOException
        {
            if (failOnNextPosting)
                throw new IOException("Simulated nextPosting failure");
            if (failOnFirstNextPosting && nextPostingCalls == 0)
                throw new IOException("Simulated initial nextPosting failure");
            nextPostingCalls++;
            return delegate.nextPosting();
        }

        @Override
        public int size()
        {
            return delegate.size();
        }

        @Override
        public int advance(int targetRowID) throws IOException
        {
            if (failOnAdvance)
                throw new IOException("Simulated advance failure");
            advanceCalls++;
            return delegate.advance(targetRowID);
        }

        @Override
        public void close() throws IOException
        {
            isClosed = true;
            delegate.close();
        }
    }

    private static class InstrumentedCloseableIterator implements CloseableIterator<Long>
    {
        private final java.util.Iterator<Long> delegate;
        boolean isClosed;
        boolean failOnNext;
        boolean failOnHasNext;

        InstrumentedCloseableIterator(java.util.Iterator<Long> delegate)
        {
            this.delegate = delegate;
        }

        @Override
        public boolean hasNext()
        {
            if (failOnHasNext)
                throw new RuntimeException("Simulated hasNext failure");
            return delegate.hasNext();
        }

        @Override
        public Long next()
        {
            if (failOnNext)
                throw new RuntimeException("Simulated next failure");
            return delegate.next();
        }

        @Override
        public void remove()
        {
            delegate.remove();
        }

        @Override
        public void close()
        {
            isClosed = true;
        }
    }

    private static class InstrumentedSource implements LazyMergePostingList.Source
    {
        private final int[][] lists;
        final List<InstrumentedPostingList> openedLists = new ArrayList<>();
        final List<InstrumentedCloseableIterator> openedIterators = new ArrayList<>();
        boolean isClosed;
        int walksCount;
        boolean failOnOpen;
        final LongHashSet failOnIds = new LongHashSet();
        boolean failOnWalk;
        boolean failOnIteratorNext;
        boolean failOnIteratorHasNext;

        InstrumentedSource(int[][] lists)
        {
            this.lists = lists;
        }

        @Override
        public CloseableIterator<Long> postingListIds()
        {
            if (failOnWalk)
                throw new RuntimeException("Simulated walk failure");
            walksCount++;
            List<Long> ids = new ArrayList<>();
            for (int i = 0; i < lists.length; i++)
                ids.add((long) i);
            InstrumentedCloseableIterator iterator = new InstrumentedCloseableIterator(ids.iterator());
            iterator.failOnNext = failOnIteratorNext;
            iterator.failOnHasNext = failOnIteratorHasNext;
            openedIterators.add(iterator);
            return iterator;
        }

        @Override
        public PostingList open(long postingListId) throws IOException
        {
            if (failOnOpen || failOnIds.contains(postingListId))
                throw new IOException("Simulated open failure" + (failOnIds.contains(postingListId) ? " for " + postingListId : ""));
            InstrumentedPostingList list = new InstrumentedPostingList(
                new IntArrayPostingList(lists[(int) postingListId])
            );
            openedLists.add(list);
            return list;
        }

        @Override
        public void close()
        {
            isClosed = true;
        }

        void assertAllClosed()
        {
            assertTrue("Source should be closed", isClosed);
            for (InstrumentedPostingList list : openedLists)
                assertTrue("Posting list should be closed", list.isClosed);
            assertFalse("At least one iterator should have been opened", openedIterators.isEmpty());
            for (InstrumentedCloseableIterator iter : openedIterators)
                assertTrue("Iterator should be closed", iter.isClosed);
        }

        void assertAllIteratorsClosed()
        {
            assertFalse("At least one iterator should have been opened", openedIterators.isEmpty());
            for (InstrumentedCloseableIterator iter : openedIterators)
                assertTrue("Iterator should be closed", iter.isClosed);
        }
    }

    private static int[][] randomLists(Random random)
    {
        int[][] lists = new int[random.nextInt(12)][];
        for (int i = 0; i < lists.length; i++)
        {
            TreeSet<Integer> ids = new TreeSet<>();
            int count = 1 + random.nextInt(6);
            for (int j = 0; j < count; j++)
                ids.add(random.nextInt(25));
            lists[i] = ids.stream().mapToInt(Integer::intValue).toArray();
        }
        return lists;
    }

    private static TreeSet<Integer> union(int[][] lists)
    {
        TreeSet<Integer> expected = new TreeSet<>();
        for (int[] list : lists)
            for (int rowId : list)
                expected.add(rowId);
        return expected;
    }

    private static void assertMerges(int[][] lists, int initCapacity, int maxCapacity) throws IOException
    {
        List<Integer> actual = new ArrayList<>();
        InstrumentedSource source = new InstrumentedSource(lists);
        try (LazyMergePostingList merged = new LazyMergePostingList(source, initCapacity, maxCapacity))
        {
            int rowId;
            while ((rowId = merged.nextPosting()) != PostingList.END_OF_STREAM)
                actual.add(rowId);
        }
        assertEquals(new ArrayList<>(union(lists)), actual);
        source.assertAllClosed();
    }
}
