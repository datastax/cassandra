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
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Test;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import org.agrona.collections.Int2IntHashMap;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.SAIUtil;
import org.apache.cassandra.index.sai.disk.MemtableTermsIterator;
import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.TermsIterator;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.index.sai.disk.format.IndexComponents;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.disk.v1.trie.InvertedIndexWriter;
import org.apache.cassandra.index.sai.memory.RowMapping;
import org.apache.cassandra.index.sai.metrics.QueryEventListener;
import org.apache.cassandra.index.sai.utils.SAICodecUtils;
import org.apache.cassandra.index.sai.utils.SaiRandomizedTest;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;

import static org.apache.cassandra.index.sai.disk.v1.InvertedIndexBuilder.buildStringTermsEnum;
import static org.apache.cassandra.index.sai.metrics.QueryEventListeners.NO_OP_TRIE_LISTENER;

public class TermsReaderTest extends SaiRandomizedTest
{
    public static final ByteComparable.Version VERSION = TypeUtil.BYTE_COMPARABLE_VERSION;

    private final Version version;

    @ParametersFactory()
    public static Collection<Object[]> data()
    {
        // Required because it configures SEGMENT_BUILD_MEMORY_LIMIT, which is needed for Version.AA
        if (DatabaseDescriptor.getRawConfig() == null)
            DatabaseDescriptor.setConfig(DatabaseDescriptor.loadConfig());
        return Version.ALL.stream().map(v -> new Object[]{v}).collect(Collectors.toList());
    }

    @Before
    public void setCurrentSAIVersion()
    {
        SAIUtil.setCurrentVersion(version);
    }

    public TermsReaderTest(Version version)
    {
        this.version = version;
    }

    @Test
    public void testTermQueriesAgainstShortPostingLists() throws IOException
    {
        testTermQueries(version, randomIntBetween(5, 10), randomIntBetween(5, 10));
    }

    @Test
    public void testTermQueriesAgainstLongPostingLists() throws  IOException
    {
        testTermQueries(version, 513, 1025);
    }

    @Test
    public void testTermsIteration() throws IOException
    {
        doTestTermsIteration(version);
    }

    private void doTestTermsIteration(Version version) throws IOException
    {
        final int terms = 70, postings = 2;
        final IndexDescriptor indexDescriptor = newIndexDescriptor();
        final String index = newIndex();
        final IndexContext indexContext = SAITester.createIndexContext(index, UTF8Type.instance);
        final List<InvertedIndexBuilder.TermsEnum> termsEnum = buildTermsEnum(version, terms, postings);

        SegmentMetadata.ComponentMetadataMap indexMetas;
        IndexComponents.ForWrite components = indexDescriptor.newPerIndexComponentsForWrite(indexContext);
        try (InvertedIndexWriter writer = new InvertedIndexWriter(components))
        {
            var iter = termsEnum.stream()
                    .map(InvertedIndexBuilder::toTermWithFrequency)
                    .iterator();
            Int2IntHashMap docLengths = createMockDocLengths(termsEnum);
            indexMetas = writer.writeAll(new MemtableTermsIterator(null, null, iter), docLengths);
        }

        FileHandle termsData = components.get(IndexComponentType.TERMS_DATA).createFileHandle(null);
        FileHandle postingLists = components.get(IndexComponentType.POSTING_LISTS).createFileHandle(null);

        long termsFooterPointer = Long.parseLong(indexMetas.get(IndexComponentType.TERMS_DATA).attributes.get(SAICodecUtils.FOOTER_POINTER));

        try (TermsReader reader = new TermsReader(indexContext,
                                                  termsData,
                                                  components.byteComparableVersionFor(IndexComponentType.TERMS_DATA),
                                                  postingLists,
                                                  indexMetas.get(IndexComponentType.TERMS_DATA).root,
                                                  termsFooterPointer,
                                                  version))
        {
            try (TermsIterator actualTermsEnum = reader.allTerms())
            {
                int i = 0;
                for (ByteComparable term = actualTermsEnum.next(); term != null; term = actualTermsEnum.next())
                {
                    final ByteComparable expected = termsEnum.get(i++).byteComparableBytes;
                    assertEquals(0, ByteComparable.compare(expected, term, VERSION));
                }
            }
        }
    }

    private void testTermQueries(Version version, int numTerms, int numPostings) throws IOException
    {
        final IndexDescriptor indexDescriptor = newIndexDescriptor();
        final String index = newIndex();
        final IndexContext indexContext = SAITester.createIndexContext(index, UTF8Type.instance);
        final List<InvertedIndexBuilder.TermsEnum> termsEnum = buildTermsEnum(version, numTerms, numPostings);

        SegmentMetadata.ComponentMetadataMap indexMetas;
        IndexComponents.ForWrite components = indexDescriptor.newPerIndexComponentsForWrite(indexContext);
        try (InvertedIndexWriter writer = new InvertedIndexWriter(components))
        {
            var iter = termsEnum.stream()
                    .map(InvertedIndexBuilder::toTermWithFrequency)
                    .iterator();
            Int2IntHashMap docLengths = createMockDocLengths(termsEnum);
            indexMetas = writer.writeAll(new MemtableTermsIterator(null, null, iter), docLengths);
        }

        FileHandle termsData = components.get(IndexComponentType.TERMS_DATA).createFileHandle(null);
        FileHandle postingLists = components.get(IndexComponentType.POSTING_LISTS).createFileHandle(null);

        long termsFooterPointer = Long.parseLong(indexMetas.get(IndexComponentType.TERMS_DATA).attributes.get(SAICodecUtils.FOOTER_POINTER));

        try (TermsReader reader = new TermsReader(indexContext,
                                                  termsData,
                                                  components.byteComparableVersionFor(IndexComponentType.TERMS_DATA),
                                                  postingLists,
                                                  indexMetas.get(IndexComponentType.TERMS_DATA).root,
                                                  termsFooterPointer,
                                                  version))
        {
            var iter = termsEnum.stream()
                    .map(InvertedIndexBuilder::toTermWithFrequency)
                    .collect(Collectors.toList());
            for (Pair<ByteComparable.Preencoded, List<RowMapping.RowIdWithFrequency>> pair : iter)
            {
                final byte[] bytes = ByteSourceInverse.readBytes(pair.left.asComparableBytes(VERSION));
                try (PostingList actualPostingList = reader.exactMatch(ByteComparable.preencoded(VERSION, bytes),
                                                                       (QueryEventListener.TrieIndexEventListener)NO_OP_TRIE_LISTENER,
                                                                       new QueryContext()))
                {
                    final List<RowMapping.RowIdWithFrequency> expectedPostingList = pair.right;

                    assertNotNull(actualPostingList);
                    assertEquals(expectedPostingList.size(), actualPostingList.size());

                    for (int i = 0; i < expectedPostingList.size(); ++i)
                    {
                        final long expectedRowID = expectedPostingList.get(i).rowId;
                        long result = actualPostingList.nextPosting();
                        assertEquals(String.format("row %d mismatch of %d in enum %d", i, expectedPostingList.size(), termsEnum.indexOf(pair)), expectedRowID, result);
                    }

                    long lastResult = actualPostingList.nextPosting();
                    assertEquals(PostingList.END_OF_STREAM, lastResult);
                }

                // test skipping
                try (PostingList actualPostingList = reader.exactMatch(ByteComparable.preencoded(VERSION, bytes),
                                                                       (QueryEventListener.TrieIndexEventListener)NO_OP_TRIE_LISTENER,
                                                                       new QueryContext()))
                {
                    final List<RowMapping.RowIdWithFrequency> expectedPostingList = pair.right;
                    // test skipping to the last block
                    final int idxToSkip = numPostings - 2;
                    // tokens are equal to their corresponding row IDs
                    final int tokenToSkip = expectedPostingList.get(idxToSkip).rowId;

                    long advanceResult = actualPostingList.advance(tokenToSkip);
                    assertEquals(tokenToSkip, advanceResult);

                    for (int i = idxToSkip + 1; i < expectedPostingList.size(); ++i)
                    {
                        final long expectedRowID = expectedPostingList.get(i).rowId;
                        long result = actualPostingList.nextPosting();
                        assertEquals(expectedRowID, result);
                    }

                    long lastResult = actualPostingList.nextPosting();
                    assertEquals(PostingList.END_OF_STREAM, lastResult);
                }
            }
        }
    }

    private Int2IntHashMap createMockDocLengths(List<InvertedIndexBuilder.TermsEnum> termsEnum)
    {
        Int2IntHashMap docLengths = new Int2IntHashMap(Integer.MIN_VALUE);
        for (InvertedIndexBuilder.TermsEnum term : termsEnum)
        {
            for (var cursor : term.postings)
                docLengths.put(cursor.value, 1);
        }
        return docLengths;
    }

    private List<InvertedIndexBuilder.TermsEnum> buildTermsEnum(Version version, int terms, int postings)
    {
        // We use terms * postings * 2 as the upper bound on row ids used in the postings list to allow for a somewhat
        // sparse mapping but not one that is too sparse.
        return buildStringTermsEnum(version, terms, postings, () -> randomSimpleString(4, 10), () -> nextInt(0, terms * postings * 2));
    }
}
