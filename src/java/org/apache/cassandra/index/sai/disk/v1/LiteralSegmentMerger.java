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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.disk.TermsIterator;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.trie.InvertedIndexWriter;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.SAICodecUtils;
import org.apache.cassandra.io.util.FileUtils;

public class LiteralSegmentMerger implements SegmentMerger
{
    final List<TermsReader> readers = new ArrayList<>();
    final List<TermsIterator> segmentTermsIterators = new ArrayList<>();

    @Override
    public void addSegment(IndexContext indexContext, SegmentMetadata segment, PerIndexFiles indexFiles) throws IOException
    {
        segmentTermsIterators.add(createTermsIterator(indexContext, segment, indexFiles));
    }

    @Override
    public boolean isEmpty()
    {
        return segmentTermsIterators.isEmpty();
    }

    @Override
    public SegmentMetadata merge(IndexDescriptor indexDescriptor,
                                 IndexContext indexContext,
                                 PrimaryKey minKey,
                                 PrimaryKey maxKey,
                                 long maxSSTableRowId) throws IOException
    {
        try (final TermsIteratorMerger merger = new TermsIteratorMerger(segmentTermsIterators.toArray(new TermsIterator[0]), indexContext.getValidator()))
        {

            SegmentMetadata.ComponentMetadataMap indexMetas;
            long numRows;

            try (InvertedIndexWriter indexWriter = new InvertedIndexWriter(indexDescriptor, indexContext, false))
            {
                indexMetas = indexWriter.writeAll(merger);
                numRows = indexWriter.getPostingsCount();
            }
            return new SegmentMetadata(0,
                                       numRows,
                                       merger.minSSTableRowId,
                                       merger.maxSSTableRowId,
                                       minKey,
                                       maxKey,
                                       merger.getMinTerm(),
                                       merger.getMaxTerm(),
                                       indexMetas);
        }
    }

    @Override
    public void close() throws IOException
    {
        FileUtils.closeQuietly(readers);
    }

    @SuppressWarnings({"resource", "RedundantSuppression"})
    private TermsIterator createTermsIterator(IndexContext indexContext, SegmentMetadata segment, PerIndexFiles indexFiles) throws IOException
    {
        final long root = segment.getIndexRoot(IndexComponent.TERMS_DATA);
        assert root >= 0;

        final Map<String, String> map = segment.componentMetadatas.get(IndexComponent.TERMS_DATA).attributes;
        final String footerPointerString = map.get(SAICodecUtils.FOOTER_POINTER);
        final long footerPointer = footerPointerString == null ? -1 : Long.parseLong(footerPointerString);

        final TermsReader termsReader = new TermsReader(indexContext,
                                                        indexFiles.termsData().sharedCopy(),
                                                        indexFiles.postingLists().sharedCopy(),
                                                        root,
                                                        footerPointer);
        readers.add(termsReader);
        return termsReader.allTerms(segment.segmentRowIdOffset);
    }
}
