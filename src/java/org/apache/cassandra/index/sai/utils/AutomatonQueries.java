/*
 * Copyright IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.index.sai.utils;

import java.util.Arrays;
import java.util.Objects;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.cql3.LikePatterns;
import org.apache.cassandra.utils.Pair;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.apache.lucene.util.automaton.Operations;

/**
 * Helpers for intersecting the Lucene {@link CompiledAutomaton}s of the CQL {@code LIKE} patterns (compiled by
 * {@link LikePatterns}) with the SAI terms dictionaries (see {@code TermsReader#intersect} and
 * {@code TrieMemoryIndex#automatonMatch}).
 * <p>
 * The automata are UTF-8 byte automata (see {@link LikePatterns}). Non-composite literal SAI terms are stored in
 * the trie dictionaries as their raw, unterminated UTF-8 bytes at every index version (see
 * {@code OnDiskFormat#encodeForTrie}), so the automata can be run directly over the stored term bytes, and the
 * terms are iterated by the dictionaries in exactly the unsigned-byte order the automaton bounds assume.
 * <p>
 * The CQL surface riding this engine consists of the non-prefix {@code LIKE} variants
 * ({@link org.apache.cassandra.cql3.Operator#LIKE_SUFFIX}, {@link org.apache.cassandra.cql3.Operator#LIKE_CONTAINS},
 * {@link org.apache.cassandra.cql3.Operator#LIKE_MATCHES}); see {@link LikePatterns#forPatternOperator}.
 * ({@code LIKE '<term>%'} keeps its dedicated bounded range scan.)
 */
public final class AutomatonQueries
{
    private static final Logger logger = LoggerFactory.getLogger(AutomatonQueries.class);

    /**
     * Default cap on the number of dictionary terms an automaton intersection may visit before failing with
     * {@link AutomatonTermsExceededException}. Queries read the configurable cap via
     * {@link #maxAutomatonExpansions()} instead.
     */
    public static final int DEFAULT_MAX_VISITED_TERMS = 8192;

    /** Client-facing message when an automaton query visits more dictionary terms than the configured cap. */
    public static final String EXPANSIONS_EXCEEDED_MESSAGE =
        "The %s restriction on column %s visited too many indexed terms (more than %d in a single read command on this replica). " +
        "Use a more selective pattern, or raise the " +
        CassandraRelevantProperties.SAI_MAX_AUTOMATON_EXPANSIONS.getKey() + " system property.";

    private AutomatonQueries()
    {
    }

    /**
     * The last value of the {@link CassandraRelevantProperties#SAI_MAX_AUTOMATON_EXPANSIONS} property and the cap it
     * was validated into, so that every distinct value is validated (and an invalid one reported) only once, while
     * changes of the property still apply to the following queries.
     */
    private static volatile Pair<String, Integer> parsedMaxAutomatonExpansions;

    /**
     * The configured cap on the number of dictionary/trie terms the automaton intersections of a query context may
     * visit, in total, across all sstable index segments and memtable index shards they touch (see {@code
     * QueryContext#automatonExpansionsBudget} for the exact scope), read at query time from the
     * {@link CassandraRelevantProperties#SAI_MAX_AUTOMATON_EXPANSIONS} system property (default
     * {@value #DEFAULT_MAX_VISITED_TERMS}). A value that is not a positive integer is reported once and replaced by
     * the default.
     */
    public static int maxAutomatonExpansions()
    {
        String raw = CassandraRelevantProperties.SAI_MAX_AUTOMATON_EXPANSIONS.getString();
        Pair<String, Integer> parsed = parsedMaxAutomatonExpansions;
        if (parsed == null || !Objects.equals(parsed.left, raw))
            parsedMaxAutomatonExpansions = parsed = Pair.create(raw, parseMaxAutomatonExpansions(raw));
        return parsed.right;
    }

    @VisibleForTesting
    static int parseMaxAutomatonExpansions(String raw)
    {
        try
        {
            int value = Integer.parseInt(raw.trim());
            if (value > 0)
                return value;
        }
        catch (NumberFormatException | NullPointerException e)
        {
            // reported below
        }
        logger.warn("Invalid value '{}' for the {} system property: it must be a positive integer. Using the default {}.",
                    raw, CassandraRelevantProperties.SAI_MAX_AUTOMATON_EXPANSIONS.getKey(), DEFAULT_MAX_VISITED_TERMS);
        return DEFAULT_MAX_VISITED_TERMS;
    }

    /**
     * A countdown of the number of dictionary/trie terms automaton intersections may visit before failing with
     * {@link AutomatonTermsExceededException}. The query path shares a single budget per query context (created
     * lazily by {@code QueryContext#automatonExpansionsBudget()} from {@link #maxAutomatonExpansions()}; one per
     * read command executed on a replica, see there for the exact scope), consumed by every sstable segment
     * intersection ({@code TermsReader#intersect}) and memtable shard scan ({@code TrieMemoryIndex#automatonMatch})
     * it performs, so the cap does not grow with the segment count. Engine-level entry points may instead pass a
     * fresh per-call budget.
     * <p>
     * Not thread-safe: like {@code QueryContext}, a budget is only ever consumed by the single thread running the
     * query it belongs to.
     */
    public static final class ExpansionsBudget
    {
        /** How many visited terms are consumed between two calls to the checkpoint callback. */
        private static final int CHECKPOINT_INTERVAL = 64;

        private final int limit;
        private int remaining;
        @Nullable
        private final Runnable checkpoint;

        public ExpansionsBudget(int limit)
        {
            this(limit, null);
        }

        /**
         * @param limit the total number of visited terms allowed
         * @param checkpoint called every {@value #CHECKPOINT_INTERVAL} consumed terms, so that long intersections can
         *                   be aborted (e.g. {@code QueryContext#checkpoint()} on query timeout); may be {@code null}
         */
        public ExpansionsBudget(int limit, @Nullable Runnable checkpoint)
        {
            this.limit = limit;
            this.remaining = limit;
            this.checkpoint = checkpoint;
        }

        /** The total number of visited terms this budget allowed when it was created. */
        public int limit()
        {
            return limit;
        }

        /**
         * The number of visited terms consumed from this budget so far. With the automaton-guided seek-skipping
         * intersection this counts the terms actually visited <em>after</em>
         * skipping, which tests use to pin the optimization (materially fewer visited terms than the scanned
         * dictionary range).
         */
        public int visitedTerms()
        {
            return limit - Math.max(remaining, 0);
        }

        /**
         * Consumes one visited term from the budget.
         *
         * @throws AutomatonTermsExceededException carrying the budget's total limit, when the budget is exhausted
         */
        public void consumeVisitedTerm()
        {
            if (remaining-- <= 0)
                throw new AutomatonTermsExceededException(limit);
            if (checkpoint != null && (limit - remaining) % CHECKPOINT_INTERVAL == 0)
                checkpoint.run();
        }
    }

    /**
     * Returns the byte prefix shared by all terms accepted by the given compiled automaton, to be used as the
     * inclusive lower bound of a dictionary scan. Returns an empty array when there is no common prefix (or when
     * the automaton matches everything).
     */
    public static byte[] commonPrefixBytes(CompiledAutomaton automaton)
    {
        switch (automaton.type)
        {
            case NONE:
            case ALL:
                return new byte[0];
            case SINGLE:
                return bytesRefToArray(automaton.term);
            case NORMAL:
                // automaton.automaton is the determinized UTF-8 byte automaton for NORMAL compiled automata.
                return bytesRefToArray(Operations.getCommonPrefixBytesRef(automaton.automaton));
            default:
                throw new AssertionError("Unknown automaton type: " + automaton.type);
        }
    }

    /**
     * Returns the smallest byte sequence that sorts after every byte sequence starting with {@code prefix}, i.e.
     * the exclusive upper bound of the scan {@code [prefix, nextOf(prefix))}, or {@code null} if no such bound
     * exists (the prefix is empty or consists solely of {@code 0xFF} bytes). Used for both prefix and automaton
     * common-prefix scans (see {@code Expression#getPrefixUpperBoundByteComparable}), it is only meaningful for
     * tries storing raw unterminated term bytes.
     */
    public static byte[] prefixUpperBound(byte[] prefix)
    {
        int last = prefix.length - 1;
        while (last >= 0 && prefix[last] == (byte) 0xFF)
            last--;
        if (last < 0)
            return null;
        byte[] upperBytes = Arrays.copyOf(prefix, last + 1);
        upperBytes[last]++;
        return upperBytes;
    }

    private static byte[] bytesRefToArray(BytesRef ref)
    {
        return Arrays.copyOfRange(ref.bytes, ref.offset, ref.offset + ref.length);
    }
}
