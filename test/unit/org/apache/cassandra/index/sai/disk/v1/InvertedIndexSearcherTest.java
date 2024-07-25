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
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.BeforeClass;
import org.junit.Test;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.SSTableContext;
import org.apache.cassandra.index.sai.disk.MemtableTermsIterator;
import org.apache.cassandra.index.sai.disk.format.IndexComponents;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.disk.v1.kdtree.KDTreeIndexBuilder;
import org.apache.cassandra.index.sai.disk.v1.trie.InvertedIndexWriter;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.RangeIterator;
import org.apache.cassandra.index.sai.utils.SaiRandomizedTest;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class InvertedIndexSearcherTest extends SaiRandomizedTest
{
    public static final int LIMIT = Integer.MAX_VALUE;

    // Use a shared index context to prevent creating too many metrics unnecessarily
    private final IndexContext indexContext = SAITester.createIndexContext("meh", UTF8Type.instance);

    @ParametersFactory()
    public static Collection<Object[]> data()
    {
        // Required because it configures SEGMENT_BUILD_MEMORY_LIMIT, which is needed for Version.AA
        if (DatabaseDescriptor.getRawConfig() == null)
            DatabaseDescriptor.setConfig(DatabaseDescriptor.loadConfig());
        return Version.ALL.stream().map(v -> new Object[]{v}).collect(Collectors.toList());
    }

    private final Version version;

    public InvertedIndexSearcherTest(Version version)
    {
        this.version = version;
    }

    @BeforeClass
    public static void setupCQLTester()
    {
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
        StorageService.instance.setPartitionerUnsafe(Murmur3Partitioner.instance);
    }

    @Test
    public void testEqQueriesAgainstStringIndex() throws Exception
    {
        doTestEqQueriesAgainstStringIndex(version);
    }

    private void doTestEqQueriesAgainstStringIndex(Version version) throws Exception
    {
        final int numTerms = randomIntBetween(64, 512), numPostings = randomIntBetween(256, 1024);
        final List<InvertedIndexBuilder.TermsEnum> termsEnum = buildTermsEnum(version, numTerms, numPostings);

        try (IndexSearcher searcher = buildIndexAndOpenSearcher(numTerms, numPostings, termsEnum))
        {
            for (int t = 0; t < numTerms; ++t)
            {
                try (RangeIterator results = searcher.search(new Expression(indexContext)
                        .add(Operator.EQ, termsEnum.get(t).originalTermBytes), null, new QueryContext(), false, LIMIT))
                {
                    assertTrue(results.hasNext());

                    for (int p = 0; p < numPostings; ++p)
                    {
                        final int expectedToken = termsEnum.get(t).postings.get(p);
                        assertTrue(results.hasNext());
                        final long actualToken = results.next().token().getLongValue();
                        assertEquals(expectedToken, actualToken);
                    }
                    assertFalse(results.hasNext());
                }

                try (RangeIterator results = searcher.search(new Expression(indexContext)
                        .add(Operator.EQ, termsEnum.get(t).originalTermBytes), null, new QueryContext(), false, LIMIT))
                {
                    assertTrue(results.hasNext());

                    // test skipping to the last block
                    final int idxToSkip = numPostings - 7;
                    // tokens are equal to their corresponding row IDs
                    final long tokenToSkip = termsEnum.get(t).postings.get(idxToSkip);
                    results.skipTo(SAITester.TEST_FACTORY.createTokenOnly(new Murmur3Partitioner.LongToken(tokenToSkip)));

                    for (int p = idxToSkip; p < numPostings; ++p)
                    {
                        final long expectedToken = termsEnum.get(t).postings.get(p);
                        final long actualToken = results.next().token().getLongValue();
                        assertEquals(expectedToken, actualToken);
                    }
                }
            }

            // try searching for terms that weren't indexed
            final String tooLongTerm = randomSimpleString(10, 12);
            RangeIterator results = searcher.search(new Expression(indexContext)
                                                    .add(Operator.EQ, UTF8Type.instance.decompose(tooLongTerm)), null, new QueryContext(), false, LIMIT);
            assertFalse(results.hasNext());

            final String tooShortTerm = randomSimpleString(1, 2);
            results = searcher.search(new Expression(indexContext)
                                      .add(Operator.EQ, UTF8Type.instance.decompose(tooShortTerm)), null, new QueryContext(), false, LIMIT);
            assertFalse(results.hasNext());
        }
    }

    @Test
    public void testUnsupportedOperator() throws Exception
    {
        final int numTerms = randomIntBetween(5, 15), numPostings = randomIntBetween(5, 20);
        final List<InvertedIndexBuilder.TermsEnum> termsEnum = buildTermsEnum(version, numTerms, numPostings);

        try (IndexSearcher searcher = buildIndexAndOpenSearcher(numTerms, numPostings, termsEnum))
        {
            searcher.search(new Expression(indexContext)
                            .add(Operator.NEQ, UTF8Type.instance.decompose("a")), null, new QueryContext(), false, LIMIT);

            fail("Expect IllegalArgumentException thrown, but didn't");
        }
        catch (IllegalArgumentException e)
        {
            // expected
        }
    }

    private IndexSearcher buildIndexAndOpenSearcher(int terms, int postings, List<InvertedIndexBuilder.TermsEnum> termsEnum) throws IOException
    {
        final int size = terms * postings;
        final IndexDescriptor indexDescriptor = newIndexDescriptor();
        final String index = newIndex();
        final IndexContext indexContext = SAITester.createIndexContext(index, UTF8Type.instance);

        SegmentMetadata.ComponentMetadataMap indexMetas;
        IndexComponents.ForWrite components = indexDescriptor.newPerIndexComponentsForWrite(indexContext);
        try (InvertedIndexWriter writer = new InvertedIndexWriter(components))
        {
            var iter = termsEnum.stream().map(InvertedIndexBuilder.TermsEnum::toPair).iterator();
            indexMetas = writer.writeAll(new MemtableTermsIterator(null, null, iter));
        }

        final SegmentMetadata segmentMetadata = new SegmentMetadata(0,
                                                                    size,
                                                                    0,
                                                                    Long.MAX_VALUE,
                                                                    SAITester.TEST_FACTORY.createTokenOnly(DatabaseDescriptor.getPartitioner().getMinimumToken()),
                                                                    SAITester.TEST_FACTORY.createTokenOnly(DatabaseDescriptor.getPartitioner().getMaximumToken()),
                                                                    termsEnum.get(0).originalTermBytes,
                                                                    termsEnum.get(terms - 1).originalTermBytes,
                                                                    indexMetas);

        try (PerIndexFiles indexFiles = new PerIndexFiles(components))
        {
            SSTableContext sstableContext = mock(SSTableContext.class);
            when(sstableContext.primaryKeyMapFactory()).thenReturn(KDTreeIndexBuilder.TEST_PRIMARY_KEY_MAP_FACTORY);
            when(sstableContext.usedPerSSTableComponents()).thenReturn(indexDescriptor.perSSTableComponents());
            final IndexSearcher searcher = version.onDiskFormat().newIndexSearcher(sstableContext,
                                                                                   indexContext,
                                                                                   indexFiles,
                                                                                   segmentMetadata);
            assertThat(searcher, is(instanceOf(InvertedIndexSearcher.class)));
            return searcher;
        }
    }

    private List<InvertedIndexBuilder.TermsEnum> buildTermsEnum(Version version, int terms, int postings)
    {
        return InvertedIndexBuilder.buildStringTermsEnum(version, terms, postings, () -> randomSimpleString(3, 5), () -> nextInt(0, Integer.MAX_VALUE));
    }

    private ByteBuffer wrap(ByteComparable bc)
    {
        return ByteBuffer.wrap(ByteSourceInverse.readBytes(bc.asComparableBytes(ByteComparable.Version.OSS50)));
    }
}
