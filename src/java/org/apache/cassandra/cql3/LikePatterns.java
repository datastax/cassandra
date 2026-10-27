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

package org.apache.cassandra.cql3;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import org.apache.cassandra.exceptions.InvalidPatternException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.TooComplexToDeterminizeException;

/**
 * The CQL {@code LIKE} pattern engine: compiles the patterns of the {@code LIKE} operators into Lucene
 * {@link CompiledAutomaton}s and runs them over values.
 * <p>
 * CQL LIKE semantics (see {@code SingleColumnRestriction.LikeRestriction#makeSpecific}): {@code %} is the only
 * wildcard and matches any (possibly empty) character sequence, wherever it appears in the pattern. There is no
 * escape mechanism and {@code _} is a literal character.
 * <p>
 * All automata produced here are defined over Unicode code points and are then compiled into UTF-8 byte automata
 * (via {@code CompiledAutomaton(automaton, null, true, workLimit, false /* not binary *&#47;)}), matching how Lucene
 * itself matches its UTF-8 encoded terms, so the resulting {@link org.apache.lucene.util.automaton.ByteRunAutomaton}
 * can be run directly over the raw bytes of {@code text}/{@code ascii} values. The same compiled automaton serves the
 * post-filtering of the pattern operators ({@link Operator#isSatisfiedBy}, including replica filtering protection) and
 * the index implementations intersecting it with their term dictionaries (SAI, see
 * {@code org.apache.cassandra.index.sai.utils.AutomatonQueries}), keeping both in exact agreement.
 * <p>
 * {@link #forPatternOperator} is the entry point, and the only cached one; the builders it uses are public for tests.
 */
public final class LikePatterns
{
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
     * The seek-driven dictionary intersection of SAI ({@code AutomatonSeeker}) honours this: it walks the automaton
     * through its own per-query {@code Transition} scratch and never calls {@code floor()}.
     */
    // Weight-bounded rather than entry-bounded: a near-determinize-limit pattern
    // can retain several MB of RunAutomaton transition tables, so 128 arbitrary entries could otherwise pin
    // hundreds of MB of untracked static heap. The weigher approximates retained bytes from the automaton
    // state count; typical patterns weigh a few KB, so the budget still caches thousands of them.
    private static final long COMPILE_CACHE_MAX_WEIGHT_BYTES = 32 << 20;

    private static final Cache<CacheKey, CompiledAutomaton> COMPILE_CACHE =
        Caffeine.newBuilder()
                .maximumWeight(COMPILE_CACHE_MAX_WEIGHT_BYTES)
                .<CacheKey, CompiledAutomaton>weigher((key, automaton) -> {
                    // ~ >=160 bytes per state across the automaton + run-automaton transition tables, floored
                    // so ALL/NONE/SINGLE simplifications (no automaton) still weigh something, plus the key's
                    // copy of the pattern
                    int states = automaton.automaton == null ? 0 : automaton.automaton.getNumStates();
                    return Math.max(256, states * 160) + key.value.remaining();
                })
                .build();

    private LikePatterns()
    {
    }

    /**
     * Builds a byte-level automaton for a CQL {@code LIKE} pattern. This method accepts the full pattern, so it
     * serves {@code x%} (prefix), {@code %x} (suffix), {@code %x%} (contains) and generic patterns such as
     * {@code a%b}, {@code a%b%}, {@code %a%b} or {@code %a%b%} alike. Not cached.
     *
     * @param pattern the LIKE pattern, with {@code %} wildcards
     * @return the compiled automaton
     */
    @VisibleForTesting
    public static CompiledAutomaton patternAutomaton(String pattern)
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
        // a pattern without wildcards accepts a single string; with wildcards, infinitely many
        return compile(parts.size() == 1 ? parts.get(0) : Operations.concatenate(parts), !lastWasWildcard && parts.size() == 1);
    }

    /**
     * Compiles a Unicode code point automaton into its UTF-8 byte form, determinizing it within
     * {@link Operations#DEFAULT_DETERMINIZE_WORK_LIMIT}. Not cached.
     *
     * @param automaton an automaton over Unicode code points
     * @return the compiled byte-level automaton
     * @throws org.apache.lucene.util.automaton.TooComplexToDeterminizeException if determinization is too costly
     */
    @VisibleForTesting
    public static CompiledAutomaton compile(Automaton automaton)
    {
        return new CompiledAutomaton(automaton, null, true, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT, false);
    }

    /**
     * As {@link #compile(Automaton)}, for an automaton whose finiteness is known: Lucene would otherwise compute
     * it by a recursive walk of the automaton that gives up (with an {@code IllegalArgumentException}) past 1000
     * states, i.e. on a pattern whose first literal is longer than that.
     *
     * @param finite whether the automaton accepts a finite language
     */
    private static CompiledAutomaton compile(Automaton automaton, boolean finite)
    {
        return new CompiledAutomaton(automaton, finite, true, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT, false);
    }

    /**
     * Returns whether the automaton accepts the given value bytes, handling all
     * {@link CompiledAutomaton.AUTOMATON_TYPE}s (the {@code ALL}/{@code NONE}/{@code SINGLE} simplifications have
     * no {@link org.apache.lucene.util.automaton.ByteRunAutomaton}).
     */
    public static boolean accepts(CompiledAutomaton automaton, byte[] valueBytes)
    {
        return accepts(automaton, valueBytes, 0, valueBytes.length);
    }

    /**
     * Zero-copy variant for the per-row post-filter paths (including replica filtering protection): cell
     * values are almost always slices of a larger row buffer, so materializing a fresh {@code byte[]} per
     * candidate row is pure garbage; array-backed buffers are run in place, only direct buffers are copied.
     */
    public static boolean accepts(CompiledAutomaton automaton, ByteBuffer value)
    {
        if (value.hasArray())
            return accepts(automaton, value.array(), value.arrayOffset() + value.position(), value.remaining());
        return accepts(automaton, ByteBufferUtil.getArray(value));
    }

    private static boolean accepts(CompiledAutomaton automaton, byte[] valueBytes, int offset, int length)
    {
        switch (automaton.type)
        {
            case NONE:
                return false;
            case ALL:
                return true;
            case SINGLE:
                return automaton.term.bytesEquals(new BytesRef(valueBytes, offset, length));
            case NORMAL:
                return automaton.runAutomaton.run(valueBytes, offset, length);
            default:
                throw new AssertionError("Unknown automaton type: " + automaton.type);
        }
    }

    /**
     * Builds a byte-level automaton accepting all values that end with the given literal suffix
     * ({@code LIKE '%<term>'}). The CQL layer never produces a suffix containing {@code %} (a pattern with an inner
     * wildcard is a {@link Operator#LIKE_MATCHES} pattern, see {@code LikeRestriction#makeSpecific}); any
     * {@code %} in the value is matched literally, like by the {@link Operator#LIKE_SUFFIX} post-filter. Not cached.
     */
    @VisibleForTesting
    public static CompiledAutomaton suffixAutomaton(String suffix)
    {
        return compile(Operations.concatenate(Automata.makeAnyString(), Automata.makeString(suffix)), false);
    }

    /**
     * Builds a byte-level automaton accepting all values that contain the given literal substring
     * ({@code LIKE '%<term>%'}). As for {@link #suffixAutomaton}, any {@code %} in the value is matched literally.
     * Not cached.
     */
    @VisibleForTesting
    public static CompiledAutomaton containsAutomaton(String substring)
    {
        return compile(Operations.concatenate(List.of(Automata.makeAnyString(),
                                                      Automata.makeString(substring),
                                                      Automata.makeAnyString())), false);
    }

    /**
     * The entry point of the pattern operators: returns the {@link CompiledAutomaton} for a pattern-matching
     * operator and its restriction value, from the compile cache when it holds it, translating the CQL-level
     * semantics of each operator:
     * <ul>
     *   <li>{@link Operator#LIKE_SUFFIX} / {@link Operator#LIKE_CONTAINS}: the value is the literal left once
     *       {@code LikeRestriction#makeSpecific} trimmed the leading/trailing {@code %} wildcards of a pattern
     *       without inner wildcards; it is matched literally;</li>
     *   <li>{@link Operator#LIKE_MATCHES}: the value is either a literal (exact match) or the whole pattern of a
     *       LIKE with an inner wildcard (including any leading/trailing ones), and every {@code %} is an any-string
     *       wildcard (see {@link #patternAutomaton}).</li>
     * </ul>
     *
     * @param operator the pattern operator
     * @param value the restriction value, as stored in the {@code RowFilter} expression
     * @param column the restricted column name, only used in error messages; {@code null} if unknown
     * @return the compiled automaton
     * @throws InvalidPatternException if the pattern is syntactically invalid or too complex to determinize
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

        CompiledAutomaton automaton;
        try
        {
            automaton = buildForPatternOperator(operator, value);
        }
        catch (TooComplexToDeterminizeException | IllegalArgumentException e)
        {
            // not cached: the query fails on the first evaluation
            throw new InvalidPatternException(operator, column, String.valueOf(e.getMessage()));
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
                return patternAutomaton(utf8(value));
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
