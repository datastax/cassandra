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

import org.junit.Test;

import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.disk.IndexWriterConfig;
import org.apache.cassandra.index.sai.disk.MutableOneDimPointValues;
import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.SegmentMetadata;
import org.apache.cassandra.index.sai.disk.io.IndexComponents;
import org.apache.cassandra.index.sai.utils.NdiRandomizedTest;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;

import static org.apache.cassandra.index.sai.metrics.QueryEventListeners.NO_OP_BKD_LISTENER;

public class MultiLeafPostingsTest extends NdiRandomizedTest
{
    @Test
    public void testMultiBlocks() throws Exception
    {
        final BKDTreeRamBuffer ramBuffer = new BKDTreeRamBuffer(1, Integer.BYTES);

        // add 3 3 leaf blocks with the same value
        for (int i = 0; i < 30; ++i)
        {
            byte[] scratch = new byte[Integer.BYTES];
            NumericUtils.intToSortableBytes(500, scratch, 0);
            ramBuffer.addPackedValue(i, new BytesRef(scratch));
        }
        for (int i = 30; i < 60; ++i)
        {
            byte[] scratch = new byte[Integer.BYTES];
            NumericUtils.intToSortableBytes(800, scratch, 0);
            ramBuffer.addPackedValue(i, new BytesRef(scratch));
        }
        for (int i = 60; i < 90; ++i)
        {
            byte[] scratch = new byte[Integer.BYTES];
            NumericUtils.intToSortableBytes(1000, scratch, 0);
            ramBuffer.addPackedValue(i, new BytesRef(scratch));
        }

        final MutableOneDimPointValues pointValues = ramBuffer.asPointValues();

        final IndexComponents indexComponents = newIndexComponents();
        int docCount = pointValues.getDocCount();

        SegmentMetadata.ComponentMetadataMap indexMetas;

        try (NumericIndexWriter writer = new NumericIndexWriter(indexComponents,
                                                                10,
                                                                Integer.BYTES,
                                                                docCount,
                                                                docCount,
                                                                IndexWriterConfig.defaultConfig("test"),
                                                                false))
        {
            indexMetas = writer.writeAll(pointValues);
        }

        final FileHandle kdtree = indexComponents.createFileHandle(indexComponents.kdTree);
        final FileHandle kdtreePostings = indexComponents.createFileHandle(indexComponents.kdTreePostingLists);

        try (BKDReader reader = new BKDReader(indexComponents,
                                              kdtree,
                                              indexMetas.get(indexComponents.kdTree.ndiType).root,
                                              kdtreePostings,
                                              indexMetas.get(indexComponents.kdTreePostingLists.ndiType).root
        ))
        {
            PostingList postings = reader.intersect(BKDReaderTest.buildQuery(100, 600), NO_OP_BKD_LISTENER, new QueryContext());

            assertFalse(postings instanceof MergePostingList);
            assertEquals(30, postings.size());

            postings.close();

            postings = reader.intersect(BKDReaderTest.buildQuery(700, 900), NO_OP_BKD_LISTENER, new QueryContext());

            assertFalse(postings instanceof MergePostingList);
            assertEquals(30, postings.size());

            postings.close();
        }
    }

    @Test
    public void testOneMultiBlock() throws Exception
    {
        final BKDTreeRamBuffer ramBuffer = new BKDTreeRamBuffer(1, Integer.BYTES);
        final int numRows = 30;
        for (int i = 0; i < numRows; ++i)
        {
            byte[] scratch = new byte[Integer.BYTES];
            NumericUtils.intToSortableBytes(500, scratch, 0);
            ramBuffer.addPackedValue(i, new BytesRef(scratch));
        }

        final MutableOneDimPointValues pointValues = ramBuffer.asPointValues();

        final IndexComponents indexComponents = newIndexComponents();
        int docCount = pointValues.getDocCount();

        SegmentMetadata.ComponentMetadataMap indexMetas;

        try (NumericIndexWriter writer = new NumericIndexWriter(indexComponents,
                                                                10,
                                                                Integer.BYTES,
                                                                docCount,
                                                                docCount,
                                                                IndexWriterConfig.defaultConfig("test"),
                                                                false))
        {
            indexMetas = writer.writeAll(pointValues);
        }

        final FileHandle kdtree = indexComponents.createFileHandle(indexComponents.kdTree);
        final FileHandle kdtreePostings = indexComponents.createFileHandle(indexComponents.kdTreePostingLists);

        try (BKDReader reader = new BKDReader(indexComponents,
                                              kdtree,
                                              indexMetas.get(indexComponents.kdTree.ndiType).root,
                                              kdtreePostings,
                                              indexMetas.get(indexComponents.kdTreePostingLists.ndiType).root
        ))
        {
            PostingList postings = reader.intersect(BKDReaderTest.buildQuery(100, 600), NO_OP_BKD_LISTENER, new QueryContext());

            assertFalse(postings instanceof MergePostingList);
            assertEquals(30, postings.size());

            postings.close();
        }
    }
}
