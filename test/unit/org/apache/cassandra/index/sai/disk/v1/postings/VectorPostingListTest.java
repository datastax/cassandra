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

import java.util.Arrays;
import java.util.Iterator;

import org.junit.Test;

import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.vector.ScoredRowId;
import org.apache.cassandra.utils.CloseableIterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class VectorPostingListTest
{
    @Test
    public void ensureEmptySourceBehavesCorrectly() throws Throwable
    {
        var source = new TestIterator(CloseableIterator.emptyIterator());

        try (var postingList = new VectorPostingList(source))
        {
            // Even an empty source should be closed
            assertTrue(source.isClosed);
            assertEquals(PostingList.END_OF_STREAM, postingList.nextPosting());
        }
    }

    @Test
    public void ensureIteratorIsConsumedClosedAndReordered() throws Throwable
    {
        var source = new TestIterator(Arrays.stream(new ScoredRowId[] {
                new ScoredRowId(3, 3),
                new ScoredRowId(2, 2),
                new ScoredRowId(1, 1),
                new ScoredRowId(4, 4),
        }).iterator());

        try (var postingList = new VectorPostingList(source))
        {
            // The posting list is eagerly consumed, so it should be closed before
            // we close postingList
            assertTrue(source.isClosed);
            assertEquals(4, postingList.size());

            // Verify ordering
            assertEquals(1, postingList.nextPosting());
            assertEquals(2, postingList.nextPosting());
            assertEquals(3, postingList.nextPosting());
            assertEquals(4, postingList.nextPosting());
            assertEquals(PostingList.END_OF_STREAM, postingList.nextPosting());
        }
    }

    @Test
    public void ensureAdvanceWorksCorrectly() throws Throwable
    {
        var source = new TestIterator(Arrays.stream(new ScoredRowId[] {
        new ScoredRowId(3, 3),
        new ScoredRowId(1, 1),
        new ScoredRowId(2, 2),
        }).iterator());

        try (var postingList = new VectorPostingList(source))
        {
            assertEquals(3, postingList.advance(3));
            assertEquals(PostingList.END_OF_STREAM, postingList.advance(4));
        }
    }

    /**
     * A basic iterator that tracks whether it has been closed for better testing.
     */
    private static class TestIterator implements CloseableIterator<ScoredRowId>
    {
        private final Iterator<ScoredRowId> iterator;
        boolean isClosed = false;
        TestIterator(Iterator<ScoredRowId> iter)
        {
            iterator = iter;
        }

        @Override
        public boolean hasNext()
        {
            return iterator.hasNext();
        }

        @Override
        public ScoredRowId next()
        {
            return iterator.next();
        }

        @Override
        public void close()
        {
            isClosed = true;
        }
    }
}
