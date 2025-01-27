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
import java.io.UncheckedIOException;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.base.MoreObjects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.SSTableContext;
import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.TermsIterator;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.disk.v1.postings.IntersectingPostingList;
import org.apache.cassandra.index.sai.iterators.KeyRangeIterator;
import org.apache.cassandra.index.sai.metrics.MulticastQueryEventListeners;
import org.apache.cassandra.index.sai.metrics.QueryEventListener;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.plan.Orderer;
import org.apache.cassandra.index.sai.utils.BM25Utils;
import org.apache.cassandra.index.sai.utils.BM25Utils.DocTF;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.PrimaryKeyWithSortKey;
import org.apache.cassandra.index.sai.utils.RowIdWithByteComparable;
import org.apache.cassandra.index.sai.utils.SAICodecUtils;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReadsListener;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static org.apache.cassandra.index.sai.disk.PostingList.END_OF_STREAM;

/**
 * Executes {@link Expression}s against the trie-based terms dictionary for an individual index segment.
 */
public class InvertedIndexSearcher extends IndexSearcher
{
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final TermsReader reader;
    private final QueryEventListener.TrieIndexEventListener perColumnEventListener;
    private final Version version;
    private final boolean filterRangeResults;
    private final SSTableReader sstable;
    private final DocLengthsReader docLengthsReader;

    protected InvertedIndexSearcher(SSTableContext sstableContext,
                                    PerIndexFiles perIndexFiles,
                                    SegmentMetadata segmentMetadata,
                                    IndexContext indexContext,
                                    Version version,
                                    boolean filterRangeResults) throws IOException
    {
        super(sstableContext.primaryKeyMapFactory(), perIndexFiles, segmentMetadata, indexContext);
        this.sstable = sstableContext.sstable;

        long root = metadata.getIndexRoot(IndexComponentType.TERMS_DATA);
        assert root >= 0;

        this.version = version;
        this.filterRangeResults = filterRangeResults;
        perColumnEventListener = (QueryEventListener.TrieIndexEventListener)indexContext.getColumnQueryMetrics();
        var docLenghtsMeta = segmentMetadata.componentMetadatas.get(IndexComponentType.DOC_LENGTHS);
        this.docLengthsReader = docLenghtsMeta == null ? null : new DocLengthsReader(indexFiles.docLengths(), docLenghtsMeta);

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
    public KeyRangeIterator search(Expression exp, AbstractBounds<PartitionPosition> keyRange, QueryContext context, boolean defer) throws IOException
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

    private Cell<?> readColumn(SSTableReader sstable, PrimaryKey primaryKey)
    {
        var dk = primaryKey.partitionKey();
        var slices = Slices.with(indexContext.comparator(), Slice.make(primaryKey.clustering()));
        try (var rowIterator = sstable.iterator(dk, slices, columnFilter, false, SSTableReadsListener.NOOP_LISTENER))
        {
            var unfiltered = rowIterator.next();
            assert unfiltered.isRow() : unfiltered;
            Row row = (Row) unfiltered;
            return row.getCell(indexContext.getDefinition());
        }
    }

    @Override
    public CloseableIterator<PrimaryKeyWithSortKey> orderBy(Orderer orderer, Expression slice, AbstractBounds<PartitionPosition> keyRange, QueryContext queryContext, int limit) throws IOException
    {
        if (!orderer.isBM25())
        {
            var iter = new RowIdWithTermsIterator(reader.allTerms(orderer.isAscending()));
            return toMetaSortedIterator(iter, queryContext);
        }
        if (docLengthsReader == null)
            throw new InvalidRequestException(indexContext.getIndexName() + " does not support BM25 scoring until it is rebuilt");

        // find documents that match each term
        var queryTerms = orderer.getQueryTerms();
        var postingLists = queryTerms.stream()
                                     .collect(Collectors.toMap(Function.identity(), term ->
        {
            var encodedTerm = version.onDiskFormat().encodeForTrie(term, indexContext.getValidator());
            var listener = MulticastQueryEventListeners.of(queryContext, perColumnEventListener);
            var postings = reader.exactMatch(encodedTerm, listener, queryContext);
            return postings == null ? PostingList.EMPTY : postings;
        }));
        // extract the match count for each
        var documentFrequencies = postingLists.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> (long) e.getValue().size()));

        var pkm = primaryKeyMapFactory.newPerSSTablePrimaryKeyMap();
        var merged = IntersectingPostingList.intersect(postingLists);
        
        // Wrap the iterator with resource management
        var it = new AbstractIterator<DocTF>() { // Anonymous class extends AbstractIterator
            private boolean closed;
            
            @Override
            protected DocTF computeNext()
            {
                try
                {
                    int rowId = merged.nextPosting();
                    if (rowId == PostingList.END_OF_STREAM)
                        return endOfData();
                    int docLength = docLengthsReader.get(rowId);
                    return new DocTF(pkm.primaryKeyFromRowId(rowId), docLength, merged.frequencies());
                }
                catch (IOException e)
                {
                    throw new UncheckedIOException(e);
                }
            }

            @Override
            public void close()
            {
                if (closed) return;
                closed = true;
                FileUtils.closeQuietly(pkm, merged);
            }
        };
        return bm25Internal(it, queryTerms, documentFrequencies);
    }

    private CloseableIterator<PrimaryKeyWithSortKey> bm25Internal(CloseableIterator<DocTF> keyIterator,
                                                                  List<ByteBuffer> queryTerms,
                                                                  Map<ByteBuffer, Long> documentFrequencies)
    {
        var totalRows = sstable.getTotalRows();
        // since doc frequencies can be an estimate from the index histogram, which does not have bounded error,
        // cap frequencies to total rows so that the IDF term doesn't turn negative
        var cappedFrequencies = documentFrequencies.entrySet().stream()
                                                    .collect(Collectors.toMap(Map.Entry::getKey, e -> Math.min(e.getValue(), totalRows)));
        var docStats = new BM25Utils.DocStats(cappedFrequencies, totalRows);
        return BM25Utils.computeScores(keyIterator,
                                       queryTerms,
                                       docStats,
                                       indexContext,
                                       sstable.descriptor.id);
    }

    @Override
    public CloseableIterator<PrimaryKeyWithSortKey> orderResultsBy(SSTableReader reader, QueryContext queryContext, List<PrimaryKey> keys, Orderer orderer, int limit) throws IOException
    {
        if (!orderer.isBM25())
            return super.orderResultsBy(reader, queryContext, keys, orderer, limit);
        if (docLengthsReader == null)
            throw new InvalidRequestException(indexContext.getIndexName() + " does not support BM25 scoring until it is rebuilt");

        var queryTerms = orderer.getQueryTerms();
        // compute documentFrequencies from either histogram or an index search
        var documentFrequencies = new HashMap<ByteBuffer, Long>();
        boolean hasHistograms = metadata.version.onDiskFormat().indexFeatureSet().hasTermsHistogram();
        for (ByteBuffer term : queryTerms)
        {
            long matches;
            if (hasHistograms)
            {
                matches = metadata.estimateNumRowsMatching(new Expression(indexContext).add(Operator.ANALYZER_MATCHES, term));
            }
            else
            {
                // Without histograms, need to do an actual index scan
                var encodedTerm = version.onDiskFormat().encodeForTrie(term, indexContext.getValidator());
                var listener = MulticastQueryEventListeners.of(queryContext, perColumnEventListener);
                var postingList = this.reader.exactMatch(encodedTerm, listener, queryContext);
                matches = postingList.size();
                FileUtils.closeQuietly(postingList);
            }
            documentFrequencies.put(term, matches);
        }
        var analyzer = indexContext.getAnalyzerFactory().create();
        var it = keys.stream().map(pk -> DocTF.createFromDocument(pk, readColumn(sstable, pk), analyzer, queryTerms)).iterator();
        return bm25Internal(CloseableIterator.wrap(it), queryTerms, documentFrequencies);
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
        FileUtils.closeQuietly(reader, docLengthsReader);
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
                    if (nextPosting != END_OF_STREAM)
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
