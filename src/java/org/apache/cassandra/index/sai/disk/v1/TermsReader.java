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
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.LikePatterns;
import org.apache.cassandra.db.marshal.AbstractType;
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
import org.apache.cassandra.index.sai.disk.v1.trie.ReverseTrieTermsDictionaryReader;
import org.apache.cassandra.index.sai.disk.v1.trie.TrieTermsDictionaryReader;
import org.apache.cassandra.index.sai.metrics.QueryEventListener;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.AbortedOperationException;
import org.apache.cassandra.index.sai.utils.AutomatonQueries;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.ReadPattern;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;
import org.apache.lucene.util.automaton.CompiledAutomaton;

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
     * Iterates, in the given order, the terms of an ordering scan restricted by the given prefix or automaton pattern
     * expression ({@code LIKE} on the ordering column): the terms starting with the prefix, or the terms accepted by
     * the automaton, which all start with its common prefix. Only the range of terms starting with that prefix is
     * read. Only valid for non-composite literal indexes, whose terms are stored as their raw bytes.
     */
    public TermsIterator patternTerms(Expression filter, boolean ascending)
    {
        byte[] prefix;
        CompiledAutomaton automaton;
        if (filter.getOp() == Expression.Op.PREFIX)
        {
            prefix = ByteBufferUtil.getArray(filter.lower.value.encoded);
            automaton = null;
        }
        else
        {
            assert filter.getOp() == Expression.Op.AUTOMATON : "Not a pattern expression: " + filter;
            prefix = filter.getAutomatonCommonPrefix();
            automaton = filter.getAutomaton();
        }
        return ascending ? new TermsScanner(prefix, automaton) : new ReverseTermsScanner(prefix, automaton);
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
        return new RangeQuery(termFilter, lower, upper, false, perQueryEventListener, context).execute();
    }

    /**
     * Range query for scans whose only possible over-match is a term exactly equal to the upper bound: the end
     * bound of the trie scan is always inclusive, so instead of collecting and post-filtering every matched term
     * (see {@link #rangeMatch}), the postings of the single term equal to {@code upper}, if that term exists, are
     * excluded from the merge. Used by prefix scans over {@code [enc(prefix), nextOf(enc(prefix)))} of
     * non-composite literal indexes, whose terms are stored order-preserving at every index version.
     */
    public PostingList rangeMatchExcludingUpperBoundTerm(ByteComparable lower, ByteComparable upper, QueryEventListener.TrieIndexEventListener perQueryEventListener, QueryContext context)
    {
        perQueryEventListener.onSegmentHit();
        return new RangeQuery(null, lower, upper, true, perQueryEventListener, context).execute();
    }

    /**
     * Intersects the terms dictionary with a byte-level automaton (the non-prefix LIKE matching engine, see
     * {@link AutomatonQueries}) and returns the union of the posting lists of all accepted terms.
     * <p>
     * This is only correct for indexes over non-composite literal types: those store the raw, unterminated term
     * bytes in the trie at every index version (see {@code OnDiskFormat#encodeForTrie}), so the automaton can be run
     * directly over the stored bytes. Callers are responsible for that guard.
     * <p>
     * The terms accepted by the automaton all start with its common prefix, so the intersection is a range query
     * over {@code [commonPrefix, nextOf(commonPrefix))} keeping the terms the automaton accepts. The scan visits
     * every term of that range: past a {@code %} wildcard, every term starting with the literal prefix of the
     * pattern may match. Only patterns with a {@code _} wildcard could skip terms by seeking.
     *
     * @param automaton the compiled byte-level automaton to intersect with the dictionary
     * @param perQueryEventListener the event listener
     * @param context the query context
     * @return the union of the posting lists of all matching terms, possibly {@link PostingList#EMPTY}
     */
    @VisibleForTesting
    public PostingList intersect(CompiledAutomaton automaton,
                                 QueryEventListener.TrieIndexEventListener perQueryEventListener,
                                 QueryContext context)
    {
        return intersect(automaton, AutomatonQueries.commonPrefixBytes(automaton), perQueryEventListener, context);
    }

    /**
     * Same as {@link #intersect(CompiledAutomaton, QueryEventListener.TrieIndexEventListener, QueryContext)}, with
     * the automaton's common byte prefix ({@link AutomatonQueries#commonPrefixBytes}) computed by the caller, so
     * that it isn't recomputed for every segment a query intersects (see {@code Expression#getAutomatonCommonPrefix()}).
     */
    public PostingList intersect(CompiledAutomaton automaton,
                                 byte[] commonPrefix,
                                 QueryEventListener.TrieIndexEventListener perQueryEventListener,
                                 QueryContext context)
    {
        perQueryEventListener.onSegmentHit();
        ByteComparable lower = commonPrefix.length == 0
                               ? null
                               : ByteComparable.preencoded(termDictionaryFileEncodingVersion, commonPrefix);
        // the end bound of the scan is inclusive, but the automaton rejects nextOf(commonPrefix)
        byte[] upperBytes = AutomatonQueries.prefixUpperBound(commonPrefix);
        ByteComparable upper = upperBytes == null
                               ? null
                               : ByteComparable.preencoded(termDictionaryFileEncodingVersion, upperBytes);
        return new RangeQuery(term -> LikePatterns.accepts(automaton, term), lower, upper, false, perQueryEventListener, context).execute();
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
        private final boolean excludeExactUpperTerm;

        // When the term filter is not null, only the terms it accepts are read
        RangeQuery(@Nullable Predicate<byte[]> termFilter, ByteComparable lower, ByteComparable upper, boolean excludeExactUpperTerm, QueryEventListener.TrieIndexEventListener listener, QueryContext context)
        {
            assert termFilter == null || !excludeExactUpperTerm : "per-term filtering already excludes the upper bound term";
            this.listener = listener;
            this.termFilter = termFilter;
            lookupStartTime = Clock.Global.nanoTime();
            this.context = context;
            this.lower = lower;
            this.upper = upper;
            this.excludeExactUpperTerm = excludeExactUpperTerm;
        }

        public PostingList execute()
        {
            // Note: we always pass true for include start because we use the ByteComparable terminator above
            // to selectively determine when we have a match on the first/last term. This is probably part of the API
            // that could change, but it's been there for a bit, so we'll leave it for now.
            try (TrieTermsDictionaryReader reader = new TrieTermsDictionaryReader(termDictionaryFile.instantiateRebufferer(null, ReadPattern.SEQUENTIAL),
                                                                                  termDictionaryRoot,
                                                                                  lower,
                                                                                  upper,
                                                                                  true,
                                                                                  termFilter != null,
                                                                                  termDictionaryFileEncodingVersion))
            {
                if (!reader.hasNext())
                    return PostingList.EMPTY;

                // The end bound of the trie scan is inclusive. When the caller asked for an end-exclusive scan,
                // look up the postings offset of the term exactly equal to the upper bound (a single extra trie
                // descent) so its postings can be skipped by readAndMergePostings without collecting and
                // comparing the bytes of every matched term. Deferred until the scan is known non-empty, so
                // empty segments (the common case for selective prefixes over many segments) skip the descent.
                long excludedPostingsOffset = excludeExactUpperTerm && upper != null
                                              ? lookupExactTermOffset(upper)
                                              : TrieTermsDictionaryReader.NOT_FOUND;

                context.checkpoint();
                PostingList postings = readAndMergePostings(reader, excludedPostingsOffset);

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
         * Looks up the postings offset (the trie payload) of the term exactly equal to the given key, or
         * {@link TrieTermsDictionaryReader#NOT_FOUND} if the term is not present in this segment.
         */
        private long lookupExactTermOffset(ByteComparable term)
        {
            try (TrieTermsDictionaryReader reader = new TrieTermsDictionaryReader(termDictionaryFile.instantiateRebufferer(null, ReadPattern.SEQUENTIAL),
                                                                                  termDictionaryRoot,
                                                                                  termDictionaryFileEncodingVersion))
            {
                return reader.exactMatch(term);
            }
        }

        /**
         * Reads the posting lists of the terms of the range accepted by the term filter, if any, and merges them
         * into a single posting list. It assumes that the posting list for each term is sorted.
         *
         * @param excludedPostingsOffset postings offset of the single term to exclude from the merge (the term
         *        exactly matching an end-exclusive upper bound), or {@link TrieTermsDictionaryReader#NOT_FOUND}
         * @return the posting lists for the terms matching the query.
         */
        private PostingList readAndMergePostings(TrieTermsDictionaryReader reader, long excludedPostingsOffset) throws IOException
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
                        if (excludedPostingsOffset != TrieTermsDictionaryReader.NOT_FOUND && excludedPostingsOffset == postingsOffset)
                            continue;
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

    private boolean readFrequencies()
    {
        return indexContext.isAnalyzed() && version.onOrAfter(Version.BM25_EARLIEST);
    }

    /**
     * Iterates the terms of the terms dictionary read by the given reader, in its order, with their postings, keeping
     * only those accepted by the filter, if any.
     */
    private abstract class AbstractTermsScanner implements TermsIterator
    {
        private final Iterator<Pair<ByteComparable, Long>> terms;
        private final AutoCloseable termsCloser;
        @Nullable
        private final Predicate<byte[]> filter;
        private final IndexInput postingsInput;
        private final IndexInput postingsSummaryInput;
        private Pair<ByteComparable, Long> entry;
        private Pair<ByteComparable, Long> pending;

        <T extends Iterator<Pair<ByteComparable, Long>> & AutoCloseable> AbstractTermsScanner(T terms, @Nullable Predicate<byte[]> filter)
        {
            this.terms = terms;
            this.termsCloser = terms;
            this.filter = filter;
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
            FileUtils.closeQuietly(termsCloser);
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
            entry = pending;
            pending = null;
            return entry.left;
        }

        @Override
        public boolean hasNext()
        {
            if (pending != null)
                return true;

            while (terms.hasNext())
            {
                Pair<ByteComparable, Long> candidate = terms.next();
                if (filter == null || filter.test(ByteSourceInverse.readBytes(candidate.left.asComparableBytes(termDictionaryFileEncodingVersion))))
                {
                    pending = candidate;
                    return true;
                }
            }
            return false;
        }
    }

    /**
     * Iterates the terms in ascending order: all of them, or only those of an ordering scan restricted by a pattern
     * (see {@link #patternTerms}).
     */
    private class TermsScanner extends AbstractTermsScanner
    {
        private final ByteBuffer minTerm, maxTerm;

        private TermsScanner(Version version, AbstractType<?> type)
        {
            this(new TrieTermsDictionaryReader(termDictionaryFile.instantiateRebufferer(null, ReadPattern.SEQUENTIAL), termDictionaryRoot, termDictionaryFileEncodingVersion), version, type);
        }

        private TermsScanner(TrieTermsDictionaryReader termsDictionaryReader, Version version, AbstractType<?> type)
        {
            super(termsDictionaryReader, null);
            // We decode based on the logic used to encode the min and max terms in the trie.
            if (version.onOrAfter(Version.DB) && TypeUtil.isComposite(type))
            {
                this.minTerm = indexContext.getValidator().fromComparableBytes(ByteSource.peekable(termsDictionaryReader.getMinTerm().asComparableBytes(termDictionaryFileEncodingVersion)), termDictionaryFileEncodingVersion);
                this.maxTerm = indexContext.getValidator().fromComparableBytes(ByteSource.peekable(termsDictionaryReader.getMaxTerm().asComparableBytes(termDictionaryFileEncodingVersion)), termDictionaryFileEncodingVersion);
            }
            else
            {
                this.minTerm = ByteBuffer.wrap(ByteSourceInverse.readBytes(termsDictionaryReader.getMinTerm().asComparableBytes(termDictionaryFileEncodingVersion)));
                this.maxTerm = ByteBuffer.wrap(ByteSourceInverse.readBytes(termsDictionaryReader.getMaxTerm().asComparableBytes(termDictionaryFileEncodingVersion)));
            }
        }

        private TermsScanner(byte[] prefix, @Nullable CompiledAutomaton automaton)
        {
            super(new TrieTermsDictionaryReader(termDictionaryFile.instantiateRebufferer(null, ReadPattern.SEQUENTIAL),
                                                termDictionaryRoot,
                                                bound(prefix),
                                                prefixUpperBound(prefix),
                                                true,
                                                true,
                                                termDictionaryFileEncodingVersion),
                  patternFilter(prefix, automaton));
            // reading them would move the reader, and only the full scans of segment merging need them
            this.minTerm = null;
            this.maxTerm = null;
        }

        @Override
        public ByteBuffer getMinTerm()
        {
            if (minTerm == null)
                throw new UnsupportedOperationException();
            return minTerm;
        }

        @Override
        public ByteBuffer getMaxTerm()
        {
            if (maxTerm == null)
                throw new UnsupportedOperationException();
            return maxTerm;
        }
    }

    /**
     * Iterates the terms in descending order: all of them, or only those of an ordering scan restricted by a pattern
     * (see {@link #patternTerms}).
     */
    private class ReverseTermsScanner extends AbstractTermsScanner
    {
        private ReverseTermsScanner()
        {
            super(new ReverseTrieTermsDictionaryReader(termDictionaryFile.instantiateRebufferer(null, ReadPattern.SEQUENTIAL), termDictionaryRoot), null);
        }

        private ReverseTermsScanner(byte[] prefix, @Nullable CompiledAutomaton automaton)
        {
            super(new ReverseTrieTermsDictionaryReader(termDictionaryFile.instantiateRebufferer(null, ReadPattern.SEQUENTIAL),
                                                       termDictionaryRoot,
                                                       bound(prefix),
                                                       prefixUpperBound(prefix),
                                                       termDictionaryFileEncodingVersion),
                  patternFilter(prefix, automaton));
        }
    }

    /**
     * The upper bound of the terms starting with the prefix, {@code nextOf(prefix)}: the readers include it, so
     * {@link #patternFilter} skips it.
     */
    @Nullable
    private ByteComparable prefixUpperBound(byte[] prefix)
    {
        return bound(AutomatonQueries.prefixUpperBound(prefix));
    }

    @Nullable
    private ByteComparable bound(@Nullable byte[] bytes)
    {
        return bytes == null || bytes.length == 0 ? null : ByteComparable.preencoded(termDictionaryFileEncodingVersion, bytes);
    }

    /**
     * Accepts the terms starting with the prefix, which the automaton, if any, accepts.
     */
    private static Predicate<byte[]> patternFilter(byte[] prefix, @Nullable CompiledAutomaton automaton)
    {
        return term -> startsWith(term, prefix) && (automaton == null || LikePatterns.accepts(automaton, term));
    }

    private static boolean startsWith(byte[] term, byte[] prefix)
    {
        return term.length >= prefix.length && Arrays.equals(term, 0, prefix.length, prefix, 0, prefix.length);
    }
}
