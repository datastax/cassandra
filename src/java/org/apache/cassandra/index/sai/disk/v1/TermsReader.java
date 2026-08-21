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
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.apache.lucene.util.automaton.CompiledAutomaton;
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
import org.apache.cassandra.index.sai.utils.AutomatonSeeker;
import org.apache.cassandra.index.sai.utils.AutomatonTermsExceededException;
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
        return new RangeQuery(exp, lower, upper, false, perQueryEventListener, context).execute();
    }

    /**
     * Range query for scans whose only possible over-match is a term exactly equal to the upper bound: the end
     * bound of the trie scan is always inclusive, so instead of collecting and post-filtering every matched term
     * (see {@link #rangeMatch}), the postings of the single term equal to {@code upper}, if that term exists, are
     * excluded from the merge. Used by prefix scans over {@code [enc(prefix), nextOf(enc(prefix)))} on
     * order-preserving formats.
     */
    public PostingList rangeMatchExcludingUpperBoundTerm(ByteComparable lower, ByteComparable upper, QueryEventListener.TrieIndexEventListener perQueryEventListener, QueryContext context)
    {
        perQueryEventListener.onSegmentHit();
        return new RangeQuery(null, lower, upper, true, perQueryEventListener, context).execute();
    }

    /**
     * Intersects the terms dictionary with a Lucene byte-level automaton (the non-prefix LIKE matching engine,
     * see {@link AutomatonQueries}) and returns the union of the posting lists of all accepted terms.
     * <p>
     * This is only correct for indexes over non-composite literal types: those store the raw, unterminated term
     * bytes in the trie at every index version (see {@code OnDiskFormat#encodeForTrie}), so the dictionary iterates
     * terms in exactly the unsigned-byte order the automaton bounds assume, and the automaton can be run directly
     * over the stored bytes. Callers are responsible for that guard.
     * <p>
     * The intersection restricts the dictionary range to
     * {@code [commonPrefix(automaton), nextOf(commonPrefix(automaton)))} and, for {@code NORMAL} automatons,
     * walks it guided by the automaton (Lucene's {@code AutomatonTermsEnum} strategy, see
     * {@link AutomatonSeeker}): each visited term is tested against the automaton's {@link ByteRunAutomaton},
     * and on a rejection the walk seeks the dictionary to the next byte string that could possibly match instead
     * of visiting every term in between. Match-all ({@code ALL}) automatons scan the range sequentially,
     * accepting every term.
     *
     * @param automaton the compiled byte-level automaton to intersect with the dictionary
     * @param maxVisitedTerms cap on the number of dictionary terms this scan may visit before failing with
     *        {@link AutomatonTermsExceededException}; a fresh per-call budget — the query path shares a single
     *        per-query budget instead, see {@link #intersect(CompiledAutomaton, AutomatonQueries.ExpansionsBudget,
     *        QueryEventListener.TrieIndexEventListener, QueryContext)}
     * @param perQueryEventListener the event listener
     * @param context the query context
     * @return the union of the posting lists of all matching terms, possibly {@link PostingList#EMPTY}
     * @throws AutomatonTermsExceededException if the scan visits more than {@code maxVisitedTerms} terms
     */
    public PostingList intersect(CompiledAutomaton automaton,
                                 int maxVisitedTerms,
                                 QueryEventListener.TrieIndexEventListener perQueryEventListener,
                                 QueryContext context)
    {
        return intersect(automaton, new AutomatonQueries.ExpansionsBudget(maxVisitedTerms), perQueryEventListener, context);
    }

    /**
     * Same as {@link #intersect(CompiledAutomaton, int, QueryEventListener.TrieIndexEventListener, QueryContext)},
     * consuming the given visited-terms budget. The query path passes the query's shared budget
     * ({@code QueryContext#automatonExpansionsBudget()}), making the expansions cap a PER-QUERY bound across all
     * segments (and memtable shards) rather than a per-segment one.
     *
     * @throws AutomatonTermsExceededException if the budget is exhausted, carrying the budget's total limit
     */
    public PostingList intersect(CompiledAutomaton automaton,
                                 AutomatonQueries.ExpansionsBudget budget,
                                 QueryEventListener.TrieIndexEventListener perQueryEventListener,
                                 QueryContext context)
    {
        switch (automaton.type)
        {
            case NONE:
                return PostingList.EMPTY;
            case SINGLE:
            {
                perQueryEventListener.onSegmentHit();
                byte[] termBytes = new byte[automaton.term.length];
                System.arraycopy(automaton.term.bytes, automaton.term.offset, termBytes, 0, automaton.term.length);
                ByteComparable term = ByteComparable.preencoded(termDictionaryFileEncodingVersion, termBytes);
                PostingList postings = new TermQuery(term, perQueryEventListener, context).execute();
                return postings == null ? PostingList.EMPTY : postings;
            }
            case ALL:
            case NORMAL:
                perQueryEventListener.onSegmentHit();
                return new AutomatonIntersection(automaton, budget, perQueryEventListener, context).execute();
            default:
                throw new AssertionError("Unknown automaton type: " + automaton.type);
        }
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

        private final Expression exp;
        private final ByteComparable lower;
        private final ByteComparable upper;
        private final boolean excludeExactUpperTerm;

        // When the exp is not null, we need to post filter the results
        RangeQuery(Expression exp, ByteComparable lower, ByteComparable upper, boolean excludeExactUpperTerm, QueryEventListener.TrieIndexEventListener listener, QueryContext context)
        {
            assert exp == null || !excludeExactUpperTerm : "per-term post-filtering already excludes the upper bound term";
            this.listener = listener;
            this.exp = exp;
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
                                                                                  exp != null,
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
                PostingList postings = exp == null
                                       ? readAndMergePostings(reader, excludedPostingsOffset)
                                       : readFilterAndMergePosting(reader);

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
         * Reads the posting lists for the matching terms and merges them into a single posting list.
         * It assumes that the posting list for each term is sorted.
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
                    long postingsOffset = reader.nextAsLong();
                    if (excludedPostingsOffset != TrieTermsDictionaryReader.NOT_FOUND && excludedPostingsOffset == postingsOffset)
                        continue;

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

        /**
         * Reads the posting lists for the matching terms, apply the expression to filter results, and merge them into
         * a single posting list. It assumes that the posting list for each term is sorted.
         *
         * @return the posting lists for the terms matching the query.
         */
        private PostingList readFilterAndMergePosting(TrieTermsDictionaryReader reader) throws IOException
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
                    Pair<ByteComparable, Long> nextTriePair = reader.next();
                    ByteSource mapEntry = nextTriePair.left.asComparableBytes(termDictionaryFileEncodingVersion);
                    long postingsOffset = nextTriePair.right;
                    byte[] nextBytes = ByteSourceInverse.readBytes(mapEntry);

                    if (exp.isSatisfiedBy(ByteBuffer.wrap(nextBytes)))
                    {
                        var currentReader = currentReader(postingsInput, postingsSummaryInput, postingsOffset);

                        if (!currentReader.isEmpty())
                            postingLists.add(currentReader);
                        else
                            FileUtils.close(currentReader);
                    }
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
     * Automaton intersection over the terms dictionary: restricts the dictionary to the range implied by the
     * automaton's common byte prefix and walks it guided by the automaton — accepted terms are collected, and on
     * a rejected term the walk seeks the dictionary ({@link TrieTermsDictionaryReader#skipTo}) to the next
     * possibly-matching byte string computed by the {@link AutomatonSeeker} instead of visiting every term
     * (match-all automatons keep the plain bounded scan, every term is accepted). The seek targets are computed
     * in the same raw byte space the trie is sorted by: this intersection is only used for non-composite literal
     * indexes, whose tries store the raw unterminated term bytes (see {@link TermsReader#intersect}), so the
     * automaton's byte order and the trie order coincide. The posting lists of the accepted terms are merged
     * into a single posting list.
     */
    private class AutomatonIntersection
    {
        private final QueryEventListener.TrieIndexEventListener listener;
        private final long lookupStartTime;
        private final QueryContext context;

        private final CompiledAutomaton automaton;
        private final AutomatonQueries.ExpansionsBudget budget;

        AutomatonIntersection(CompiledAutomaton automaton,
                              AutomatonQueries.ExpansionsBudget budget,
                              QueryEventListener.TrieIndexEventListener listener,
                              QueryContext context)
        {
            this.listener = listener;
            this.automaton = automaton;
            this.budget = budget;
            this.lookupStartTime = Clock.Global.nanoTime();
            this.context = context;
        }

        public PostingList execute()
        {
            // A match-all automaton has no run automaton; every term in the dictionary is accepted.
            ByteRunAutomaton runAutomaton = automaton.type == CompiledAutomaton.AUTOMATON_TYPE.ALL
                                            ? null
                                            : automaton.runAutomaton;

            // NORMAL automatons drive a seek-skipping walk; a match-all (ALL) automaton accepts every term of
            // the scanned range, so there is nothing to skip and no seeker is needed.
            AutomatonSeeker seeker = automaton.type == CompiledAutomaton.AUTOMATON_TYPE.NORMAL
                                     ? new AutomatonSeeker(automaton)
                                     : null;

            // The initial seek target: the least byte string that does not put the automaton into a reject
            // state. It always shares the automaton's common byte prefix with every acceptable string (or is a
            // prefix of it), so the common-prefix range below already contains it; it only tightens the start
            // of the walk (see the skipTo below).
            byte[] initialTarget = null;
            if (seeker != null)
            {
                initialTarget = seeker.nextSeekTerm(null);
                if (initialTarget == null)
                    return PostingList.EMPTY;
            }

            // The scan is restricted to [commonPrefix, nextOf(commonPrefix)). The end bound of the trie scan is
            // inclusive, so a term exactly equal to nextOf(commonPrefix) is explicitly skipped by the scan below
            // (it does not start with the common prefix, so it can never be accepted by the automaton).
            byte[] commonPrefix = AutomatonQueries.commonPrefixBytes(automaton);
            ByteComparable lower = commonPrefix.length == 0
                                   ? null
                                   : ByteComparable.preencoded(termDictionaryFileEncodingVersion, commonPrefix);
            byte[] upperBytes = AutomatonQueries.prefixUpperBound(commonPrefix);
            ByteComparable upper = upperBytes == null
                                   ? null
                                   : ByteComparable.preencoded(termDictionaryFileEncodingVersion, upperBytes);

            try (TrieTermsDictionaryReader reader = new TrieTermsDictionaryReader(termDictionaryFile.instantiateRebufferer(null, ReadPattern.SEQUENTIAL),
                                                                                  termDictionaryRoot,
                                                                                  lower,
                                                                                  upper,
                                                                                  true,
                                                                                  true,
                                                                                  termDictionaryFileEncodingVersion))
            {
                // Tighten the start of the walk to the initial seek target when it is past the common prefix
                // (a strictly increasing key, satisfying skipTo's statefulness contract).
                if (initialTarget != null && Arrays.compareUnsigned(initialTarget, commonPrefix) > 0)
                    reader.skipTo(ByteComparable.preencoded(termDictionaryFileEncodingVersion, initialTarget));

                if (!reader.hasNext())
                    return PostingList.EMPTY;

                context.checkpoint();
                PostingList postings = scanFilterAndMergePostings(reader, runAutomaton, seeker, upperBytes);

                listener.onTraversalComplete(Clock.Global.nanoTime() - lookupStartTime, TimeUnit.NANOSECONDS);

                return postings;
            }
            catch (Throwable e)
            {
                if (!(e instanceof AbortedOperationException) && !(e instanceof AutomatonTermsExceededException))
                    logger.error(indexContext.logMessage("Failed to execute automaton intersection"), e);

                throw Throwables.cleaned(e);
            }
        }

        /**
         * Walks the bounded dictionary range, keeps the terms accepted by the automaton and merges their posting
         * lists into a single (sorted, deduplicated) posting list. With a seeker (NORMAL automatons) the walk is
         * seek-skipping: whenever the seeker classifies a visited term as {@code *_AND_SEEK}, the dictionary is
         * repositioned at the seeker's next possibly-matching string instead of advancing term by term. The
         * visited-terms budget counts the terms actually visited (post-skip).
         */
        private PostingList scanFilterAndMergePostings(TrieTermsDictionaryReader reader,
                                                       ByteRunAutomaton runAutomaton,
                                                       @Nullable AutomatonSeeker seeker,
                                                       byte[] upperBytes) throws IOException
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
                    Pair<ByteComparable, Long> entry = reader.next();
                    byte[] termBytes = ByteSourceInverse.readBytes(entry.left.asComparableBytes(termDictionaryFileEncodingVersion));

                    // The end bound of the trie scan is inclusive; the term equal to the exclusive upper bound of
                    // the prefix range must not be visited (it is necessarily the last term of the scan).
                    if (upperBytes != null && Arrays.equals(termBytes, upperBytes))
                        break;

                    budget.consumeVisitedTerm();

                    boolean accepted;
                    boolean seek = false;
                    if (seeker != null)
                    {
                        switch (seeker.accept(termBytes))
                        {
                            case YES:
                                accepted = true;
                                break;
                            case YES_AND_SEEK:
                                accepted = true;
                                seek = true;
                                break;
                            case NO_AND_SEEK:
                                accepted = false;
                                seek = true;
                                break;
                            case NO:
                            default:
                                accepted = false;
                                break;
                        }
                    }
                    else
                    {
                        accepted = runAutomaton == null || runAutomaton.run(termBytes, 0, termBytes.length);
                    }

                    if (accepted)
                    {
                        var summary = new PostingsReader.BlocksSummary(postingsSummaryInput, entry.right, PostingsReader.InputCloser.NOOP);
                        var currentReader = new PostingsReader(postingsInput, summary, readFrequencies(), listener.postingListEventListener(), PostingsReader.InputCloser.NOOP);

                        if (!currentReader.isEmpty())
                            postingLists.add(currentReader);
                        else
                            FileUtils.close(currentReader);
                    }

                    if (seek)
                    {
                        // Seek targets are strictly increasing (the seeker computes the next string after the
                        // visited term), satisfying skipTo's statefulness contract; the reader's end bound keeps
                        // applying, so a target beyond the range simply exhausts the walk.
                        byte[] target = seeker.nextSeekTerm(termBytes);
                        if (target == null)
                            break; // no string after this term can possibly match
                        reader.skipTo(ByteComparable.preencoded(termDictionaryFileEncodingVersion, target));
                    }
                } while (reader.hasNext());
            }
            catch (Throwable e)
            {
                FileUtils.closeQuietly(postingLists);
                FileUtils.closeQuietly(postingsInput);
                FileUtils.closeQuietly(postingsSummaryInput);
                throw e;
            }

            return MergePostingList.merge(postingLists)
                                   .onClose(() -> FileUtils.close(postingsInput, postingsSummaryInput));
        }
    }

    private boolean readFrequencies()
    {
        return indexContext.isAnalyzed() && version.onOrAfter(Version.BM25_EARLIEST);
    }

    private class TermsScanner implements TermsIterator
    {
        private final TrieTermsDictionaryReader termsDictionaryReader;
        private final ByteBuffer minTerm, maxTerm;
        private Pair<ByteComparable, Long> entry;
        private final IndexInput postingsInput;
        private final IndexInput postingsSummaryInput;

        private TermsScanner(Version version, AbstractType<?> type)
        {
            this.termsDictionaryReader = new TrieTermsDictionaryReader(termDictionaryFile.instantiateRebufferer(null, ReadPattern.SEQUENTIAL), termDictionaryRoot, termDictionaryFileEncodingVersion);
            this.postingsInput = IndexFileUtils.instance().openInput(postingsFile);
            this.postingsSummaryInput = IndexFileUtils.instance().openInput(postingsFile);
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
            termsDictionaryReader.close();
            FileUtils.closeQuietly(postingsInput);
            FileUtils.closeQuietly(postingsSummaryInput);
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

        @Override
        public ByteComparable next()
        {
            if (termsDictionaryReader.hasNext())
            {
                entry = termsDictionaryReader.next();
                return entry.left;
            }
            return null;
        }

        @Override
        public boolean hasNext()
        {
            return termsDictionaryReader.hasNext();
        }
    }

    private class ReverseTermsScanner implements TermsIterator
    {
        private final ReverseTrieTermsDictionaryReader iterator;
        private Pair<ByteComparable, Long> entry;
        private final IndexInput postingsInput;
        private final IndexInput postingsSummaryInput;

        private ReverseTermsScanner()
        {
            this.iterator = new ReverseTrieTermsDictionaryReader(termDictionaryFile.instantiateRebufferer(null, ReadPattern.SEQUENTIAL), termDictionaryRoot);
            this.postingsInput = IndexFileUtils.instance().openInput(postingsFile);
            this.postingsSummaryInput = IndexFileUtils.instance().openInput(postingsFile);
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
            iterator.close();
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
            if (iterator.hasNext())
            {
                entry = iterator.next();
                return entry.left;
            }
            return null;
        }

        @Override
        public boolean hasNext()
        {
            return iterator.hasNext();
        }
    }
}
