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

import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.tries.TriePattern;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.TermsIterator;
import org.apache.cassandra.index.sai.disk.io.IndexFileUtils;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.disk.io.IndexInput;
import org.apache.cassandra.index.sai.disk.v1.postings.MergePostingList;
import org.apache.cassandra.index.sai.disk.v1.postings.PostingsReader;
import org.apache.cassandra.index.sai.disk.v1.postings.ScanningPostingsReader;
import org.apache.cassandra.index.sai.disk.v1.trie.PatternTermsDictionaryReader;
import org.apache.cassandra.index.sai.disk.v1.trie.ReverseTrieTermsDictionaryReader;
import org.apache.cassandra.index.sai.disk.v1.trie.TermsDictionaryIterator;
import org.apache.cassandra.index.sai.disk.v1.trie.TrieTermsDictionaryReader;
import org.apache.cassandra.index.sai.metrics.QueryEventListener;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.AbortedOperationException;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.ReadPattern;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;

import static org.apache.cassandra.index.sai.utils.SAICodecUtils.validate;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

/**
 * Synchronous reader of terms dictionary and postings lists to produce a {@link PostingList} with matching row ids.
 *
 * {@link #exactMatch(ByteComparable, QueryEventListener.TrieIndexEventListener, QueryContext)} does:
 * <ul>
 * <li>{@link TermQuery#lookupTermDictionary(ByteComparable)}: does term dictionary lookup to find the posting list file
 * position</li>
 * <li>{@link TermQuery#getPostingReader(long)}: reads posting list block summary and initializes posting read which
 * reads the first block of the posting list into memory</li>
 * </ul>
 */
public class TermsReader implements Closeable
{
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final IndexContext indexContext;
    private final FileHandle termDictionaryFile;
    private final FileHandle postingsFile;
    private final long termDictionaryRoot;
    private final Version version;
    private final ByteComparable.Version termDictionaryFileEncodingVersion;

    public TermsReader(IndexContext indexContext,
                       FileHandle termsData,
                       ByteComparable.Version termsDataEncodingVersion,
                       FileHandle postingLists,
                       long root,
                       long termsFooterPointer,
                       Version version) throws IOException
    {
        this.indexContext = indexContext;
        this.version = version;
        termDictionaryFile = termsData;
        postingsFile = postingLists;
        termDictionaryRoot = root;
        this.termDictionaryFileEncodingVersion = termsDataEncodingVersion;

        try (final IndexInput indexInput = IndexFileUtils.instance().openInput(termDictionaryFile))
        {
            // if the pointer is -1 then this is a previous version of the index
            // use the old way to validate the footer
            // the footer pointer is used due to encrypted indexes padding extra bytes
            if (termsFooterPointer == -1)
            {
                validate(indexInput);
            }
            else
            {
                validate(indexInput, termsFooterPointer);
            }
        }

        try (final IndexInput indexInput = IndexFileUtils.instance().openInput(postingsFile))
        {
            validate(indexInput);
        }
    }

    @Override
    public void close()
    {
        try
        {
            termDictionaryFile.close();
        }
        finally
        {
            postingsFile.close();
        }
    }

    public TermsIterator allTerms()
    {
        return allTerms(true);
    }

    public TermsIterator allTerms(boolean ascending)
    {
        // blocking, since we use it only for segment merging for now
        return ascending ? new TermsScanner(version, this.indexContext.getValidator())
                         : new ReverseTermsScanner();
    }

    /**
     * Iterates, in the given order, the terms of an ordering scan restricted by the given pattern expression
     * ({@code LIKE} on the ordering column): the terms matching the pattern, which are found by walking the terms trie
     * with the pattern (see {@link PatternTermsDictionaryReader}). Only valid for non-composite literal indexes, whose
     * terms are stored as their raw bytes.
     */
    public TermsIterator patternTerms(Expression filter, boolean ascending)
    {
        assert filter.getOp() == Expression.Op.PATTERN : "Not a pattern expression: " + filter;
        return new PatternTermsScanner(filter.getTriePattern(), ascending);
    }

    public PostingList exactMatch(ByteComparable term, QueryEventListener.TrieIndexEventListener perQueryEventListener, QueryContext context)
    {
        perQueryEventListener.onSegmentHit();
        return new TermQuery(term, perQueryEventListener, context).execute();
    }

    /**
     * Range query that uses the lower and upper bounds to retrieve the search results within the range. When
     * the expression is not null, it post-filters results using the expression.
     */
    public PostingList rangeMatch(Expression exp, ByteComparable lower, ByteComparable upper, QueryEventListener.TrieIndexEventListener perQueryEventListener, QueryContext context)
    {
        perQueryEventListener.onSegmentHit();
        Predicate<byte[]> termFilter = exp == null ? null : term -> exp.isSatisfiedBy(ByteBuffer.wrap(term));
        return new RangeQuery(termFilter, lower, upper, perQueryEventListener, context).execute();
    }

    /**
     * Returns the union of the posting lists of all the terms that match the given pattern (the LIKE operators, see
     * {@link org.apache.cassandra.cql3.LikePatterns#forPatternOperator}).
     * <p>
     * The terms trie is walked with the pattern, which skips the branches that cannot contain a match (see
     * {@link PatternTermsDictionaryReader}). This is only correct for indexes over non-composite literal types: those
     * store the raw, unterminated term bytes in the trie at every index version (see
     * {@code OnDiskFormat#encodeForTrie}), so the pattern can be matched directly against the stored bytes. Callers are
     * responsible for that guard.
     *
     * @param pattern the pattern to match
     * @param perQueryEventListener the event listener
     * @param context the query context
     * @return the union of the posting lists of all matching terms, possibly {@link PostingList#EMPTY}
     */
    public PostingList patternMatch(TriePattern pattern,
                                    QueryEventListener.TrieIndexEventListener perQueryEventListener,
                                    QueryContext context)
    {
        perQueryEventListener.onSegmentHit();
        return new PatternQuery(pattern, perQueryEventListener, context).execute();
    }

    @VisibleForTesting
    public class TermQuery
    {
        private final IndexInput postingsInput;
        private final IndexInput postingsSummaryInput;
        private final QueryEventListener.TrieIndexEventListener listener;
        private final long lookupStartTime;
        private final QueryContext context;

        private ByteComparable term;

        TermQuery(ByteComparable term, QueryEventListener.TrieIndexEventListener listener, QueryContext context)
        {
            this.listener = listener;
            // If the second open throws, the constructor exits before the caller could clean up: close the first
            // input on the way out instead of leaking it.
            postingsInput = IndexFileUtils.instance().openInput(postingsFile);
            try
            {
                postingsSummaryInput = IndexFileUtils.instance().openInput(postingsFile);
            }
            catch (Throwable t)
            {
                FileUtils.closeQuietly(postingsInput);
                throw t;
            }
            this.term = term;
            lookupStartTime = nanoTime();
            this.context = context;
        }

        public PostingList execute()
        {
            try
            {
                long postingOffset = lookupTermDictionary(term);
                if (postingOffset == PostingList.OFFSET_NOT_FOUND)
                {
                    FileUtils.closeQuietly(postingsInput);
                    FileUtils.closeQuietly(postingsSummaryInput);
                    return null;
                }

                context.checkpoint();

                // when posting is found, resources will be closed when posting reader is closed.
                return getPostingReader(postingOffset);
            }
            catch (Throwable e)
            {
                //TODO Is there an equivalent of AOE in OS?
                if (!(e instanceof AbortedOperationException))
                    logger.error(indexContext.logMessage("Failed to execute term query"), e);

                closeOnException();
                throw Throwables.cleaned(e);
            }
        }

        private void closeOnException()
        {
            FileUtils.closeQuietly(postingsInput);
            FileUtils.closeQuietly(postingsSummaryInput);
        }

        public long lookupTermDictionary(ByteComparable term)
        {
            try (TrieTermsDictionaryReader reader = new TrieTermsDictionaryReader(termDictionaryFile.instantiateRebufferer(null, ReadPattern.SEQUENTIAL), termDictionaryRoot, termDictionaryFileEncodingVersion))
            {
                final long offset = reader.exactMatch(term);

                listener.onTraversalComplete(nanoTime() - lookupStartTime, TimeUnit.NANOSECONDS);

                if (offset == TrieTermsDictionaryReader.NOT_FOUND)
                    return PostingList.OFFSET_NOT_FOUND;

                return offset;
            }
        }

        public PostingsReader getPostingReader(long offset) throws IOException
        {
            PostingsReader.BlocksSummary header = new PostingsReader.BlocksSummary(postingsSummaryInput, offset);

            return new PostingsReader(postingsInput, header, readFrequencies(), listener.postingListEventListener());
        }
    }

    public class RangeQuery
    {
        private final QueryEventListener.TrieIndexEventListener listener;
        private final long lookupStartTime;
        private final QueryContext context;

        @Nullable
        private final Predicate<byte[]> termFilter;
        private final ByteComparable lower;
        private final ByteComparable upper;

        // When the term filter is not null, only the terms it accepts are read
        RangeQuery(@Nullable Predicate<byte[]> termFilter, ByteComparable lower, ByteComparable upper, QueryEventListener.TrieIndexEventListener listener, QueryContext context)
        {
            this.listener = listener;
            this.termFilter = termFilter;
            lookupStartTime = Clock.Global.nanoTime();
            this.context = context;
            this.lower = lower;
            this.upper = upper;
        }

        /**
         * Opens the reader of the terms of the query.
         */
        protected TermsDictionaryIterator openReader()
        {
            // Note: we always pass true for include start because we use the ByteComparable terminator above
            // to selectively determine when we have a match on the first/last term. This is probably part of the API
            // that could change, but it's been there for a bit, so we'll leave it for now.
            return new TrieTermsDictionaryReader(termDictionaryFile.instantiateRebufferer(null, ReadPattern.SEQUENTIAL),
                                                 termDictionaryRoot,
                                                 lower,
                                                 upper,
                                                 true,
                                                 termFilter != null,
                                                 termDictionaryFileEncodingVersion);
        }

        public PostingList execute()
        {
            try (TermsDictionaryIterator reader = openReader())
            {
                if (!reader.hasNext())
                    return PostingList.EMPTY;

                context.checkpoint();
                PostingList postings = readAndMergePostings(reader);

                listener.onTraversalComplete(Clock.Global.nanoTime() - lookupStartTime, TimeUnit.NANOSECONDS);

                return postings;
            }
            catch (Throwable e)
            {
                if (!(e instanceof AbortedOperationException))
                    logger.error(indexContext.logMessage("Failed to execute term query"), e);

                throw Throwables.cleaned(e);
            }
        }

        /**
         * Reads the posting lists of the terms of the query accepted by the term filter, if any, and merges them
         * into a single posting list. It assumes that the posting list for each term is sorted.
         *
         * @return the posting lists for the terms matching the query.
         */
        private PostingList readAndMergePostings(TermsDictionaryIterator reader) throws IOException
        {
            assert reader.hasNext();
            ArrayList<PostingList> postingLists = new ArrayList<>();

            // index inputs will be closed with the onClose method of the returned merged posting list;
            // if the second open throws, the first must be closed here instead of leaking
            IndexInput postingsInput = IndexFileUtils.instance().openInput(postingsFile);
            IndexInput postingsSummaryInput;
            try
            {
                postingsSummaryInput = IndexFileUtils.instance().openInput(postingsFile);
            }
            catch (Throwable t)
            {
                FileUtils.closeQuietly(postingsInput);
                throw t;
            }

            try
            {
                do
                {
                    long postingsOffset;
                    if (termFilter == null)
                    {
                        postingsOffset = reader.nextAsLong();
                    }
                    else
                    {
                        Pair<ByteComparable, Long> entry = reader.next();
                        if (!termFilter.test(ByteSourceInverse.readBytes(entry.left.asComparableBytes(termDictionaryFileEncodingVersion))))
                            continue;
                        postingsOffset = entry.right;
                    }

                    var currentReader = currentReader(postingsInput, postingsSummaryInput, postingsOffset);

                    if (!currentReader.isEmpty())
                        postingLists.add(currentReader);
                    else
                        FileUtils.close(currentReader);
                } while (reader.hasNext());
            }
            catch (Throwable e)
            {
                // a mid-scan failure exits before the merged posting list (whose onClose owns the resources)
                // could be built: release the collected postings and both inputs
                FileUtils.closeQuietly(postingLists);
                FileUtils.closeQuietly(postingsInput);
                FileUtils.closeQuietly(postingsSummaryInput);
                throw e;
            }

            return MergePostingList.merge(postingLists)
                                   .onClose(() -> FileUtils.close(postingsInput, postingsSummaryInput));
        }

        private PostingsReader currentReader(IndexInput postingsInput,
                                             IndexInput postingsSummaryInput,
                                             long postingsOffset) throws IOException
        {
            var blocksSummary = new PostingsReader.BlocksSummary(postingsSummaryInput,
                                                                 postingsOffset,
                                                                 PostingsReader.InputCloser.NOOP);
            return new PostingsReader(postingsInput,
                                      blocksSummary,
                                      readFrequencies(),
                                      listener.postingListEventListener(),
                                      PostingsReader.InputCloser.NOOP);
        }
    }

    /**
     * Reads the posting lists of the terms that match a pattern, see {@link #patternMatch}.
     */
    private class PatternQuery extends RangeQuery
    {
        private final TriePattern pattern;

        PatternQuery(TriePattern pattern, QueryEventListener.TrieIndexEventListener listener, QueryContext context)
        {
            super(null, null, null, listener, context);
            this.pattern = pattern;
        }

        @Override
        protected TermsDictionaryIterator openReader()
        {
            return new PatternTermsDictionaryReader(termDictionaryFile.instantiateRebufferer(null, ReadPattern.SEQUENTIAL),
                                                    termDictionaryRoot,
                                                    pattern,
                                                    true,
                                                    false,
                                                    termDictionaryFileEncodingVersion);
        }
    }

    private boolean readFrequencies()
    {
        return indexContext.isAnalyzed() && version.onOrAfter(Version.BM25_EARLIEST);
    }

    /**
     * Iterates the terms of the terms dictionary read by the given reader, in its order, with their postings.
     */
    private abstract class AbstractTermsScanner<T extends Iterator<Pair<ByteComparable, Long>> & AutoCloseable> implements TermsIterator
    {
        protected final T terms;
        private final IndexInput postingsInput;
        private final IndexInput postingsSummaryInput;
        private Pair<ByteComparable, Long> entry;

        AbstractTermsScanner(T terms)
        {
            this.terms = terms;
            // If an open throws, the constructor exits before the caller could clean up: close what was already
            // opened on the way out instead of leaking it.
            IndexInput postings = null;
            try
            {
                postings = IndexFileUtils.instance().openInput(postingsFile);
                this.postingsSummaryInput = IndexFileUtils.instance().openInput(postingsFile);
            }
            catch (Throwable t)
            {
                FileUtils.closeQuietly(postings);
                FileUtils.closeQuietly(terms);
                throw t;
            }
            this.postingsInput = postings;
        }

        @Override
        @SuppressWarnings("resource")
        public PostingList postings() throws IOException
        {
            assert entry != null;
            var blockSummary = new PostingsReader.BlocksSummary(postingsSummaryInput, entry.right, PostingsReader.InputCloser.NOOP);
            return new ScanningPostingsReader(postingsInput, blockSummary, readFrequencies());
        }

        @Override
        public void close()
        {
            FileUtils.closeQuietly(terms);
            FileUtils.closeQuietly(postingsInput);
            FileUtils.closeQuietly(postingsSummaryInput);
        }

        @Override
        public ByteBuffer getMinTerm()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public ByteBuffer getMaxTerm()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public ByteComparable next()
        {
            if (!hasNext())
                return null;
            entry = terms.next();
            return entry.left;
        }

        @Override
        public boolean hasNext()
        {
            return terms.hasNext();
        }
    }

    /**
     * Iterates all the terms in ascending order.
     */
    private class TermsScanner extends AbstractTermsScanner<TrieTermsDictionaryReader>
    {
        private final ByteBuffer minTerm, maxTerm;

        private TermsScanner(Version version, AbstractType<?> type)
        {
            super(new TrieTermsDictionaryReader(termDictionaryFile.instantiateRebufferer(null, ReadPattern.SEQUENTIAL), termDictionaryRoot, termDictionaryFileEncodingVersion));
            // We decode based on the logic used to encode the min and max terms in the trie.
            if (version.onOrAfter(Version.DB) && TypeUtil.isComposite(type))
            {
                this.minTerm = indexContext.getValidator().fromComparableBytes(ByteSource.peekable(terms.getMinTerm().asComparableBytes(termDictionaryFileEncodingVersion)), termDictionaryFileEncodingVersion);
                this.maxTerm = indexContext.getValidator().fromComparableBytes(ByteSource.peekable(terms.getMaxTerm().asComparableBytes(termDictionaryFileEncodingVersion)), termDictionaryFileEncodingVersion);
            }
            else
            {
                this.minTerm = ByteBuffer.wrap(ByteSourceInverse.readBytes(terms.getMinTerm().asComparableBytes(termDictionaryFileEncodingVersion)));
                this.maxTerm = ByteBuffer.wrap(ByteSourceInverse.readBytes(terms.getMaxTerm().asComparableBytes(termDictionaryFileEncodingVersion)));
            }
        }

        @Override
        public ByteBuffer getMinTerm()
        {
            return minTerm;
        }

        @Override
        public ByteBuffer getMaxTerm()
        {
            return maxTerm;
        }
    }

    /**
     * Iterates all the terms in descending order.
     */
    private class ReverseTermsScanner extends AbstractTermsScanner<ReverseTrieTermsDictionaryReader>
    {
        private ReverseTermsScanner()
        {
            super(new ReverseTrieTermsDictionaryReader(termDictionaryFile.instantiateRebufferer(null, ReadPattern.SEQUENTIAL), termDictionaryRoot));
        }
    }

    /**
     * Iterates the terms that match a pattern, in ascending or descending order (see {@link #patternTerms}).
     */
    private class PatternTermsScanner extends AbstractTermsScanner<PatternTermsDictionaryReader>
    {
        private PatternTermsScanner(TriePattern pattern, boolean ascending)
        {
            super(new PatternTermsDictionaryReader(termDictionaryFile.instantiateRebufferer(null, ReadPattern.SEQUENTIAL),
                                                   termDictionaryRoot,
                                                   pattern,
                                                   ascending,
                                                   true,
                                                   termDictionaryFileEncodingVersion));
        }
    }
}
