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
import java.lang.invoke.MethodHandles;
import java.util.Map;

import com.google.common.base.MoreObjects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.SSTableContext;
import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.TermsIterator;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.iterators.KeyRangeIterator;
import org.apache.cassandra.index.sai.metrics.MulticastQueryEventListeners;
import org.apache.cassandra.index.sai.metrics.QueryEventListener;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.plan.Orderer;
import org.apache.cassandra.index.sai.utils.PrimaryKeyWithSortKey;
import org.apache.cassandra.index.sai.utils.RowIdWithByteComparable;
import org.apache.cassandra.index.sai.utils.SAICodecUtils;
import org.apache.cassandra.index.sai.utils.SegmentOrdering;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

/**
 * Executes {@link Expression}s against the trie-based terms dictionary for an individual index segment.
 */
public class InvertedIndexSearcher extends IndexSearcher implements SegmentOrdering
{
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final TermsReader reader;
    private final QueryEventListener.TrieIndexEventListener perColumnEventListener;
    private final Version version;
    private final boolean filterRangeResults;

    protected InvertedIndexSearcher(SSTableContext sstableContext,
                                    PerIndexFiles perIndexFiles,
                                    SegmentMetadata segmentMetadata,
                                    IndexContext indexContext,
                                    Version version,
                                    boolean filterRangeResults) throws IOException
    {
        super(sstableContext.primaryKeyMapFactory(), perIndexFiles, segmentMetadata, indexContext);

        long root = metadata.getIndexRoot(IndexComponentType.TERMS_DATA);
        assert root >= 0;

        this.version = version;
        this.filterRangeResults = filterRangeResults;
        perColumnEventListener = (QueryEventListener.TrieIndexEventListener)indexContext.getColumnQueryMetrics();

        Map<String,String> map = metadata.componentMetadatas.get(IndexComponentType.TERMS_DATA).attributes;
        String footerPointerString = map.get(SAICodecUtils.FOOTER_POINTER);
        long footerPointer = footerPointerString == null ? -1 : Long.parseLong(footerPointerString);

        var perIndexComponents = perIndexFiles.usedPerIndexComponents();
        reader = new TermsReader(indexContext,
                                 indexFiles.termsData(),
                                 perIndexComponents.byteComparableVersionFor(IndexComponentType.TERMS_DATA),
                                 indexFiles.postingLists(),
                                 root,
                                 footerPointer,
                                 version);
    }

    @Override
    public long indexFileCacheSize()
    {
        // trie has no pre-allocated memory.
        // TODO: Is this still the case now the trie isn't using the chunk cache?
        return 0;
    }

    @SuppressWarnings("resource")
    public KeyRangeIterator search(Expression exp, AbstractBounds<PartitionPosition> keyRange, QueryContext context, boolean defer, int limit) throws IOException
    {
        PostingList postingList = searchPosting(exp, context);
        return toPrimaryKeyIterator(postingList, context);
    }

    private PostingList searchPosting(Expression exp, QueryContext context)
    {
        if (logger.isTraceEnabled())
            logger.trace(indexContext.logMessage("Searching on expression '{}'..."), exp);

        // We use the version to encode the search boundaries for the trie to ensure we use version appropriate bounds.
        if (exp.getOp().isEquality() || exp.getOp() == Expression.Op.MATCH)
        {
            // Value is encoded in non-byte-comparable-version-specific fixed-length format.
            final ByteComparable term = version.onDiskFormat().encodeForTrie(exp.lower.value.encoded, indexContext.getValidator());
            QueryEventListener.TrieIndexEventListener listener = MulticastQueryEventListeners.of(context, perColumnEventListener);
            return reader.exactMatch(term, listener, context);
        }
        else if (exp.getOp() == Expression.Op.RANGE)
        {
            QueryEventListener.TrieIndexEventListener listener = MulticastQueryEventListeners.of(context, perColumnEventListener);
            var lower = exp.getEncodedLowerBoundByteComparable(version);
            var upper = exp.getEncodedUpperBoundByteComparable(version);
            return reader.rangeMatch(filterRangeResults ? exp : null, lower, upper, listener, context);
        }
        throw new IllegalArgumentException(indexContext.logMessage("Unsupported expression: " + exp));
    }

    @Override
    public CloseableIterator<PrimaryKeyWithSortKey> orderBy(Orderer orderer, Expression slice, AbstractBounds<PartitionPosition> keyRange, QueryContext queryContext, int limit) throws IOException
    {
        var iter = new RowIdWithTermsIterator(reader.allTerms(orderer.isAscending()));
        return toMetaSortedIterator(iter, queryContext);
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                          .add("indexContext", indexContext)
                          .toString();
    }

    @Override
    public void close()
    {
        reader.close();
    }

    /**
     * An iterator that iterates over a source
     */
    private static class RowIdWithTermsIterator extends AbstractIterator<RowIdWithByteComparable>
    {
        private final TermsIterator source;
        private PostingList currentPostingList = PostingList.EMPTY;
        private ByteComparable currentTerm = null;

        RowIdWithTermsIterator(TermsIterator source)
        {
            this.source = source;
        }

        @Override
        protected RowIdWithByteComparable computeNext()
        {
            try
            {
                while (true)
                {
                    long nextPosting = currentPostingList.nextPosting();
                    if (nextPosting != PostingList.END_OF_STREAM)
                        return new RowIdWithByteComparable(Math.toIntExact(nextPosting), currentTerm);

                    if (!source.hasNext())
                        return endOfData();

                    currentTerm = source.next();
                    FileUtils.closeQuietly(currentPostingList);
                    currentPostingList = source.postings();
                }
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void close()
        {
            FileUtils.closeQuietly(source, currentPostingList);
        }
    }
}
