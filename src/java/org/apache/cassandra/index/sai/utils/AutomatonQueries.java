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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.TooComplexToDeterminizeException;

/**
 * Factory methods building Lucene {@link CompiledAutomaton}s to be intersected with the SAI terms dictionaries
 * (see {@code TermsReader#intersect} and {@code TrieMemoryIndex#automatonMatch}).
 * <p>
 * All automata produced here are defined over Unicode code points and are then compiled into UTF-8 byte automata
 * (via {@code CompiledAutomaton(automaton, null, true, workLimit, false /* not binary *&#47;)}), matching how Lucene
 * itself matches its UTF-8 encoded terms. Non-composite literal SAI terms are stored in the trie dictionaries as
 * their raw, unterminated UTF-8 bytes at every index version (see {@code OnDiskFormat#encodeForTrie}), so the
 * resulting {@link org.apache.lucene.util.automaton.ByteRunAutomaton} can be run directly over the stored term
 * bytes, and the terms are iterated by the dictionaries in exactly the unsigned-byte order the automaton bounds
 * assume.
 * <p>
 * The CQL surface riding this engine consists of the non-prefix {@code LIKE} variants
 * ({@link Operator#LIKE_SUFFIX}, {@link Operator#LIKE_CONTAINS}, {@link Operator#LIKE_MATCHES});
 * see {@link #forPatternOperator}. ({@code LIKE '<term>%'} keeps its dedicated bounded range scan.)
 */
public final class AutomatonQueries
{
    /**
     * Default cap on the number of dictionary terms an automaton intersection may visit before failing with
     * {@link AutomatonTermsExceededException}. Queries read the configurable cap via
     * {@link #maxAutomatonExpansions()} instead.
     */
    public static final int DEFAULT_MAX_VISITED_TERMS = 8192;

    /** Client-facing message when an automaton query visits more dictionary terms than the configured cap. */
    public static final String EXPANSIONS_EXCEEDED_MESSAGE =
        "The %s restriction on column %s visited too many indexed terms (more than %d across the whole query). " +
        "Use a more selective pattern, or raise the " +
        CassandraRelevantProperties.SAI_MAX_AUTOMATON_EXPANSIONS.getKey() + " system property.";

    /** Client-facing message when a pattern is invalid or too complex to determinize. */
    public static final String INVALID_PATTERN_MESSAGE = "Invalid pattern for %s restriction on column %s: %s";

    /**
     * A small bounded compile cache for pattern automatons, keyed by (operator, pattern bytes). Post-filtering
     * ({@link Operator#isSatisfiedBy}) evaluates the same pattern against every candidate row, so caching the
     * compiled automaton makes replica filtering protection and index post-filtering O(1) per row instead of
     * recompiling per row. The cache is a bounded lock-free Caffeine cache: lookups run per candidate row from
     * concurrent queries, so they must not contend on a shared monitor (compilation is idempotent, so a lost
     * insertion race only costs a recompile).
     * <p>
     * Thread-safety of the cached values: the {@link CompiledAutomaton} instances are shared across queries and
     * threads. All operations used on them here are read-only and safe to share ({@code runAutomaton.run},
     * {@code term} comparisons, common-prefix extraction), but {@code CompiledAutomaton#floor()} must NEVER be
     * called on a shared instance: it mutates a private {@code Transition} scratch field and is not thread-safe.
     * The seek-driven dictionary intersection ({@link AutomatonSeeker}) honours
     * this: it walks the automaton through its own per-query {@code Transition} scratch and never calls
     * {@code floor()}.
     */
    // Weight-bounded rather than entry-bounded: a near-determinize-limit pattern
    // can retain several MB of RunAutomaton transition tables, so 128 arbitrary entries could otherwise pin
    // hundreds of MB of untracked static heap. The weigher approximates retained bytes from the automaton
    // state count; typical patterns weigh a few KB, so the budget still caches thousands of them.
    private static final long COMPILE_CACHE_MAX_WEIGHT_BYTES = 32 << 20;

    /**
     * Failed compiles (syntactically invalid or too complex to determinize) are cached too, keyed like
     * {@link #COMPILE_CACHE}: without this, a bad pattern that reaches the per-row post-filter path (e.g. LIKE
     * served by a custom index implementation, which bypasses the eager SAI coordinator compile) would re-run
     * determinization up to the work limit for every candidate row. Entries are small (the error message), so a
     * plain size bound suffices.
     */
    private static final Cache<CacheKey, String> FAILED_COMPILE_CACHE =
        Caffeine.newBuilder().maximumSize(1024).build();
    private static final Cache<CacheKey, CompiledAutomaton> COMPILE_CACHE =
        Caffeine.newBuilder()
                .maximumWeight(COMPILE_CACHE_MAX_WEIGHT_BYTES)
                .<CacheKey, CompiledAutomaton>weigher((key, automaton) -> {
                    // ~ >=160 bytes per state across the automaton + run-automaton transition tables, floored
                    // so ALL/NONE/SINGLE simplifications (no automaton) still weigh something
                    int states = automaton.automaton == null ? 0 : automaton.automaton.getNumStates();
                    return Math.max(256, states * 160);
                })
                .build();

    private AutomatonQueries()
    {
    }

    /**
     * The configured cap on the number of dictionary/trie terms an automaton query may visit, in total, across
     * all sstable index segments and memtable index shards it touches (see {@code
     * QueryContext#automatonExpansionsBudget}), read at query time from the
     * {@link CassandraRelevantProperties#SAI_MAX_AUTOMATON_EXPANSIONS} system property (default
     * {@value #DEFAULT_MAX_VISITED_TERMS}).
     */
    public static int maxAutomatonExpansions()
    {
        return CassandraRelevantProperties.SAI_MAX_AUTOMATON_EXPANSIONS.getInt();
    }

    /**
     * A countdown of the number of dictionary/trie terms automaton intersections may visit before failing with
     * {@link AutomatonTermsExceededException}. The query path shares a single budget per query (created lazily by
     * {@code QueryContext#automatonExpansionsBudget()} from {@link #maxAutomatonExpansions()}), consumed by every
     * sstable segment intersection ({@code TermsReader#intersect}) and memtable shard scan
     * ({@code TrieMemoryIndex#automatonMatch}) the query performs, so the guardrail does not scale with segment
     * count or depend on compaction state. Engine-level entry points may instead pass a fresh per-call budget.
     * <p>
     * Not thread-safe: like {@code QueryContext}, a budget is only ever consumed by the single thread running the
     * query it belongs to.
     */
    public static final class ExpansionsBudget
    {
        private final int limit;
        private int remaining;

        public ExpansionsBudget(int limit)
        {
            this.limit = limit;
            this.remaining = limit;
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
        }
    }

    /**
     * Builds a byte-level automaton for a CQL {@code LIKE} pattern.
     * <p>
     * CQL LIKE semantics (see {@code SingleColumnRestriction.LikeRestriction#makeSpecific}): {@code %} is the only
     * wildcard and matches any (possibly empty) character sequence. There is no escape mechanism and {@code _} is a
     * literal character, not a single-character wildcard. This method accepts the full pattern, so it serves
     * {@code x%} (prefix), {@code %x} (suffix), {@code %x%} (contains) and generic {@code a%b} patterns alike.
     *
     * @param pattern the LIKE pattern, with {@code %} wildcards
     * @return the compiled automaton
     */
    public static CompiledAutomaton fromLikePattern(String pattern)
    {
        List<Automaton> parts = new ArrayList<>();
        int segmentStart = 0;
        boolean lastWasWildcard = false;
        for (int i = 0; i < pattern.length(); i++)
        {
            if (pattern.charAt(i) == '%')
            {
                if (i > segmentStart)
                {
                    parts.add(Automata.makeString(pattern.substring(segmentStart, i)));
                    lastWasWildcard = false;
                }
                // Collapse runs of consecutive wildcards into a single any-string.
                if (!lastWasWildcard)
                {
                    parts.add(Automata.makeAnyString());
                    lastWasWildcard = true;
                }
                segmentStart = i + 1;
            }
        }
        if (segmentStart < pattern.length() || parts.isEmpty())
            parts.add(Automata.makeString(pattern.substring(segmentStart)));
        return compile(parts.size() == 1 ? parts.get(0) : Operations.concatenate(parts));
    }

    /**
     * Compiles a Unicode code point automaton into its UTF-8 byte form, determinizing it within
     * {@link Operations#DEFAULT_DETERMINIZE_WORK_LIMIT}.
     *
     * @param automaton an automaton over Unicode code points
     * @return the compiled byte-level automaton
     * @throws org.apache.lucene.util.automaton.TooComplexToDeterminizeException if determinization is too costly
     */
    public static CompiledAutomaton compile(Automaton automaton)
    {
        return new CompiledAutomaton(automaton, null, true, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT, false);
    }

    /**
     * Returns whether the automaton accepts the given term bytes, handling all
     * {@link CompiledAutomaton.AUTOMATON_TYPE}s (the {@code ALL}/{@code NONE}/{@code SINGLE} simplifications have
     * no {@link org.apache.lucene.util.automaton.ByteRunAutomaton}).
     */
    public static boolean accepts(CompiledAutomaton automaton, byte[] termBytes)
    {
        return accepts(automaton, termBytes, 0, termBytes.length);
    }

    /**
     * Zero-copy variant for the per-row post-filter paths (including replica filtering protection): cell
     * values are almost always slices of a larger row buffer, so materializing a fresh {@code byte[]} per
     * candidate row is pure garbage; array-backed buffers are run in place, only direct buffers are copied.
     */
    public static boolean accepts(CompiledAutomaton automaton, java.nio.ByteBuffer term)
    {
        if (term.hasArray())
            return accepts(automaton, term.array(), term.arrayOffset() + term.position(), term.remaining());
        return accepts(automaton, org.apache.cassandra.utils.ByteBufferUtil.getArray(term));
    }

    private static boolean accepts(CompiledAutomaton automaton, byte[] termBytes, int offset, int length)
    {
        switch (automaton.type)
        {
            case NONE:
                return false;
            case ALL:
                return true;
            case SINGLE:
                return automaton.term.bytesEquals(new BytesRef(termBytes, offset, length));
            case NORMAL:
                return automaton.runAutomaton.run(termBytes, offset, length);
            default:
                throw new AssertionError("Unknown automaton type: " + automaton.type);
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
     * exists (the prefix is empty or consists solely of {@code 0xFF} bytes). This mirrors
     * {@code Expression#getPrefixUpperBoundByteComparable} and is only meaningful for tries storing raw
     * unterminated term bytes.
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

    /**
     * Builds a byte-level automaton accepting all terms that end with the given literal suffix
     * ({@code LIKE '%<term>'}; embedded {@code %} characters in the trimmed value are literal, consistently with
     * how {@link Operator#LIKE_PREFIX} and {@link Operator#LIKE_CONTAINS} treat their trimmed value).
     */
    public static CompiledAutomaton suffixAutomaton(String suffix)
    {
        return compile(Operations.concatenate(Automata.makeAnyString(), Automata.makeString(suffix)));
    }

    /**
     * Builds a byte-level automaton accepting all terms that contain the given literal substring
     * ({@code LIKE '%<term>%'}; embedded {@code %} characters in the trimmed value are literal).
     */
    public static CompiledAutomaton containsAutomaton(String substring)
    {
        return compile(Operations.concatenate(List.of(Automata.makeAnyString(),
                                                      Automata.makeString(substring),
                                                      Automata.makeAnyString())));
    }

    /**
     * Builds (or fetches from a small LRU cache) the {@link CompiledAutomaton} for a pattern-matching operator and
     * its restriction value, translating the CQL-level semantics of each operator:
     * <ul>
     *   <li>{@link Operator#LIKE_SUFFIX} / {@link Operator#LIKE_CONTAINS}: the value is the pattern with the
     *       leading/trailing {@code %} already trimmed by {@code LikeRestriction#makeSpecific}; it is matched
     *       literally (consistent with the literal treatment of the trimmed {@link Operator#LIKE_PREFIX} value);</li>
     *   <li>{@link Operator#LIKE_MATCHES}: the value is the whole pattern and every {@code %} is an any-string
     *       wildcard (see {@link #fromLikePattern}).</li>
     * </ul>
     *
     * @param operator the pattern operator
     * @param value the restriction value, as stored in the {@code RowFilter} expression
     * @param column the restricted column name, only used in error messages
     * @return the compiled automaton
     * @throws InvalidRequestException if the pattern is syntactically invalid or too complex to determinize
     */
    public static CompiledAutomaton forPatternOperator(Operator operator, ByteBuffer value, Object column)
    {
        // Zero-copy, lock-free probe: this runs per candidate row from the Operator#isSatisfiedBy post-filters,
        // so it must neither copy the pattern buffer nor contend on a shared monitor. ByteBuffer#equals/hashCode
        // compare content, so the caller's buffer can key the lookup directly.
        CacheKey probe = new CacheKey(operator, value);
        CompiledAutomaton cached = COMPILE_CACHE.getIfPresent(probe);
        if (cached != null)
            return cached;
        String cachedFailure = FAILED_COMPILE_CACHE.getIfPresent(probe);
        if (cachedFailure != null)
            throw new InvalidRequestException(String.format(INVALID_PATTERN_MESSAGE, operator, column, cachedFailure));

        CompiledAutomaton automaton;
        try
        {
            automaton = buildForPatternOperator(operator, value);
        }
        catch (TooComplexToDeterminizeException | IllegalArgumentException e)
        {
            FAILED_COMPILE_CACHE.put(new CacheKey(operator, ByteBufferUtil.clone(value)), String.valueOf(e.getMessage()));
            throw new InvalidRequestException(String.format(INVALID_PATTERN_MESSAGE, operator, column, e.getMessage()));
        }

        // Store under a defensive copy of the pattern bytes: the static cache must not retain (and potentially
        // pin) a buffer owned by the caller. A concurrent compile of the same pattern just replaces the entry
        // with an equivalent value.
        COMPILE_CACHE.put(new CacheKey(operator, ByteBufferUtil.clone(value)), automaton);
        return automaton;
    }

    private static CompiledAutomaton buildForPatternOperator(Operator operator, ByteBuffer value)
    {
        switch (operator)
        {
            case LIKE_SUFFIX:
                return suffixAutomaton(utf8(value));
            case LIKE_CONTAINS:
                return containsAutomaton(utf8(value));
            case LIKE_MATCHES:
                return fromLikePattern(utf8(value));
            default:
                throw new AssertionError("Not a pattern operator: " + operator);
        }
    }

    private static String utf8(ByteBuffer value)
    {
        return StandardCharsets.UTF_8.decode(value.duplicate()).toString();
    }

    /**
     * Compile-cache key: (operator, pattern buffer), compared and hashed by buffer content.
     * {@link ByteBuffer#equals}/{@link ByteBuffer#hashCode} are content-based over the remaining bytes, so lookup
     * keys can wrap the caller's buffer without copying it; only keys actually stored in the cache hold a
     * defensive copy (see {@link #forPatternOperator}).
     */
    private static final class CacheKey
    {
        private final Operator operator;
        private final ByteBuffer value;

        CacheKey(Operator operator, ByteBuffer value)
        {
            this.operator = operator;
            this.value = value;
        }

        @Override
        public boolean equals(Object o)
        {
            if (!(o instanceof CacheKey))
                return false;
            CacheKey other = (CacheKey) o;
            return operator == other.operator && value.equals(other.value);
        }

        @Override
        public int hashCode()
        {
            return 31 * operator.hashCode() + value.hashCode();
        }
    }
}
