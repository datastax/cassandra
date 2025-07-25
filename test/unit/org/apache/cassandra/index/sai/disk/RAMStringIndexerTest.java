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
package org.apache.cassandra.index.sai.disk;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.cassandra.index.sai.utils.SaiRandomizedTest;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;
import org.apache.lucene.util.BytesRef;

import static org.apache.cassandra.utils.ByteBufferUtil.string;

public class RAMStringIndexerTest extends SaiRandomizedTest
{
    @Test
    public void test() throws Exception
    {
        RAMStringIndexer indexer = new RAMStringIndexer(false);

        indexer.addAll(List.of(new BytesRef("0")), 100);
        indexer.addAll(List.of(new BytesRef("2")), 102);
        indexer.addAll(List.of(new BytesRef("0")), 200);
        indexer.addAll(List.of(new BytesRef("2")), 202);
        indexer.addAll(List.of(new BytesRef("2")), 302);

        List<List<Long>> matches = new ArrayList<>();
        matches.add(Arrays.asList(100L, 200L));
        matches.add(Arrays.asList(102L, 202L, 302L));

        try (TermsIterator terms = indexer.getTermsWithPostings(ByteBufferUtil.bytes("0"), ByteBufferUtil.bytes("2"), TypeUtil.BYTE_COMPARABLE_VERSION))
        {
            int ord = 0;
            while (terms.hasNext())
            {
                terms.next();
                try (PostingList postings = terms.postings())
                {
                    List<Long> results = new ArrayList<>();
                    long segmentRowId;
                    while ((segmentRowId = postings.nextPosting()) != PostingList.END_OF_STREAM)
                    {
                        results.add(segmentRowId);
                    }
                    assertEquals(matches.get(ord++), results);
                }
            }
            // The min and max are configured, not calculated.
            assertArrayEquals("0".getBytes(), terms.getMinTerm().array());
            assertArrayEquals("2".getBytes(), terms.getMaxTerm().array());
        }
    }

    @Test
    public void testLargeNumberOfDocs()
    {
        int maxDocsSize = 1000;
        RAMStringIndexer indexer = new RAMStringIndexer(false, maxDocsSize);

        int startingRowId = 0;
        int i = 0;
        while (i++ < maxDocsSize)
        {
            int rowId = startingRowId + i;
            indexer.addAll(List.of(new BytesRef("0")), rowId);

            if (i < maxDocsSize)
                assertFalse(indexer.requiresFlush());
        }

        assertTrue(indexer.requiresFlush());
    }

    @Test
    public void testWithFrequencies() throws Exception
    {
        RAMStringIndexer indexer = new RAMStringIndexer(true);

        // Add same term twice in same row to increment frequency
        indexer.addAll(List.of(new BytesRef("A"), new BytesRef("A")), 100);
        indexer.addAll(List.of(new BytesRef("B")), 102);
        indexer.addAll(List.of(new BytesRef("A"), new BytesRef("A"), new BytesRef("A")), 200);
        indexer.addAll(List.of(new BytesRef("B"), new BytesRef("B")), 202);
        indexer.addAll(List.of(new BytesRef("B")), 302);

        // Expected results: rowID -> frequency
        List<Map<Long, Integer>> matches = Arrays.asList(Map.of(100L, 2, 200L, 3),
                                                         Map.of(102L, 1, 202L, 2, 302L, 1));

        try (TermsIterator terms = indexer.getTermsWithPostings(ByteBufferUtil.bytes("A"), ByteBufferUtil.bytes("B"), TypeUtil.BYTE_COMPARABLE_VERSION))
        {
            int ord = 0;
            while (terms.hasNext())
            {
                terms.next();
                try (PostingList postings = terms.postings())
                {
                    Map<Long, Integer> results = new HashMap<>();
                    long segmentRowId;
                    while ((segmentRowId = postings.nextPosting()) != PostingList.END_OF_STREAM)
                    {
                        results.put(segmentRowId, postings.frequency());
                    }
                    assertEquals(matches.get(ord++), results);
                }
            }
            assertArrayEquals("A".getBytes(), terms.getMinTerm().array());
            assertArrayEquals("B".getBytes(), terms.getMaxTerm().array());
        }
    }

    @Test
    public void testLargeSegment() throws IOException
    {
        final RAMStringIndexer indexer = new RAMStringIndexer(false);
        final int numTerms = between(1 << 10, 1 << 13);
        final int numPostings = between(1 << 5, 1 << 10);

        for (int id = 0; id < numTerms; ++id)
        {
            final BytesRef term = new BytesRef(String.format("%04d", id));
            for (int posting = 0; posting < numPostings; ++posting)
            {
                indexer.addAll(List.of(term), posting);
            }
        }

        final TermsIterator terms = indexer.getTermsWithPostings(ByteBufferUtil.EMPTY_BYTE_BUFFER, ByteBufferUtil.EMPTY_BYTE_BUFFER, TypeUtil.BYTE_COMPARABLE_VERSION);

        ByteComparable term;
        long termOrd = 0L;
        while (terms.hasNext())
        {
            term = terms.next();
            final ByteBuffer decoded = ByteBuffer.wrap(ByteSourceInverse.readBytes(term.asComparableBytes(TypeUtil.BYTE_COMPARABLE_VERSION)));
            assertEquals(String.format("%04d", termOrd), string(decoded));

            try (PostingList postingList = terms.postings())
            {
                assertEquals(numPostings, postingList.size());
                for (int i = 0; i < numPostings; ++i)
                {
                    assertEquals(i, postingList.nextPosting());
                }
                assertEquals(PostingList.END_OF_STREAM, postingList.nextPosting());
            }
            termOrd++;
        }

        assertEquals(numTerms, termOrd);
    }

    @Test
    public void testRequiresFlush()
    {
        int maxBlockBytePoolSize = RAMStringIndexer.MAX_BLOCK_BYTE_POOL_SIZE;
        try
        {
            RAMStringIndexer.MAX_BLOCK_BYTE_POOL_SIZE = 1024 * 1024 * 100;
            // primary behavior we're testing is that exceptions aren't thrown due to overflowing backing structures
            RAMStringIndexer indexer = new RAMStringIndexer(false);

            Assert.assertFalse(indexer.requiresFlush());
            for (int i = 0; i < Integer.MAX_VALUE; i++)
            {
                if (indexer.requiresFlush())
                    break;
                indexer.addAll(List.of(new BytesRef(String.format("%5000d", i))), i);
            }
            // If we don't require a flush before MAX_VALUE, the implementation of RAMStringIndexer has sufficiently
            // changed to warrant changes to the test.
            Assert.assertTrue(indexer.requiresFlush());
        }
        finally
        {
            RAMStringIndexer.MAX_BLOCK_BYTE_POOL_SIZE = maxBlockBytePoolSize;
        }
    }
}
