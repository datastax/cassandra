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

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.function.UnaryOperator;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import org.apache.cassandra.db.tries.TriePattern;
import org.apache.cassandra.exceptions.InvalidPatternException;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * The CQL {@code LIKE} patterns: translates the values of the {@code LIKE} operators into {@link TriePattern}s, which
 * match values one at a time ({@link TriePattern#matches}), and select the matching keys of tries of values while they
 * are walked (see {@link org.apache.cassandra.db.tries.Trie#patternMatched}).
 * <p>
 * CQL LIKE semantics (see {@code SingleColumnRestriction.LikeRestriction#makeSpecific}): {@code %} matches any
 * (possibly empty) character sequence and {@code _} any single character, wherever they appear in the pattern. The
 * patterns of {@link Operator#LIKE_MATCHES} are in an internal form, where {@link #ESCAPE} escapes the literal
 * occurrences of the wildcards and its own: the coordinator translates the {@code ESCAPE} clause of the query, if
 * any, into it. The values of the other operators are literals.
 * <p>
 * Patterns are matched against the raw bytes of {@code text}/{@code ascii} values, a {@code _} taking the whole UTF-8
 * encoding of a character. Matching does not backtrack and its cost does not depend on the shape of the pattern, so
 * any pattern can be served.
 * <p>
 * {@link #forPatternOperator} is the entry point; it caches the patterns.
 */
public final class LikePatterns
{
    /**
     * A bounded cache of the patterns, keyed by (operator, value). Post-filtering ({@link Operator#isSatisfiedBy})
     * evaluates the same pattern against every candidate row, and a pattern builds the tables of its matcher on first
     * use, so caching saves rebuilding them for every row. The cache is a lock-free Caffeine cache: lookups run per
     * candidate row from concurrent queries, so they must not contend on a shared monitor (building a pattern is
     * idempotent, so a lost insertion race only costs a rebuild). The cached patterns are immutable and can be shared.
     */
    // Weight-bounded rather than entry-bounded, as the patterns can be long: the weigher approximates the retained
    // bytes, mostly the tables of the matcher (256 entries of a word per 64 positions).
    private static final long PATTERN_CACHE_MAX_WEIGHT_BYTES = 32 << 20;

    private static final Cache<CacheKey, TriePattern> PATTERN_CACHE =
        Caffeine.newBuilder()
                .maximumWeight(PATTERN_CACHE_MAX_WEIGHT_BYTES)
                .<CacheKey, TriePattern>weigher((key, pattern) -> 256 * 8 * (key.value.remaining() / 64 + 1)
                                                                + 16 * key.value.remaining())
                .build();

    /**
     * The escape character of the internal form of {@link Operator#LIKE_MATCHES} patterns: followed by {@code %},
     * {@code _} or itself, it stands for that character literally. It is never followed by anything else.
     */
    public static final char ESCAPE = '\\';

    private LikePatterns()
    {
    }

    /**
     * Returns whether a {@link Operator#LIKE_MATCHES} pattern, in internal form, contains a wildcard.
     */
    public static boolean hasWildcard(ByteBuffer pattern)
    {
        // '%', '_' and the escape are single bytes in UTF-8 (and ASCII), never part of a multibyte character
        for (int i = pattern.position(); i < pattern.limit(); i++)
        {
            byte b = pattern.get(i);
            if (b == ESCAPE)
                i++;
            else if (b == '%' || b == '_')
                return true;
        }
        return false;
    }

    /**
     * Returns the pattern, in internal form, that matches the given literal: its {@code %}, {@code _} and
     * {@link #ESCAPE} characters escaped.
     */
    public static String escape(String literal)
    {
        StringBuilder pattern = new StringBuilder(literal.length());
        for (int i = 0; i < literal.length(); i++)
        {
            char c = literal.charAt(i);
            if (c == '%' || c == '_' || c == ESCAPE)
                pattern.append(ESCAPE);
            pattern.append(c);
        }
        return pattern.toString();
    }

    /**
     * Applies a normalization (e.g. the lowercasing or folding of a non-tokenizing index analyzer) to the literal
     * runs of a {@link Operator#LIKE_MATCHES} pattern in internal form, and returns the normalized pattern. The
     * wildcards and escapes are kept out of the normalization, which could otherwise turn literal characters into
     * them (ASCII folding turns {@code ＿} into {@code _}, for example).
     *
     * @param pattern the pattern, in internal form
     * @param normalize the normalization of a literal; it may return {@code null} for an empty result
     * @return the normalized pattern, in internal form
     */
    public static ByteBuffer normalizeLiterals(ByteBuffer pattern, UnaryOperator<ByteBuffer> normalize)
    {
        ByteBuffer literal = ByteBuffer.allocate(pattern.remaining());
        ByteArrayOutputStream normalized = new ByteArrayOutputStream(pattern.remaining());
        for (int i = pattern.position(); i < pattern.limit(); i++)
        {
            byte b = pattern.get(i);
            if (b == ESCAPE && i + 1 < pattern.limit())
            {
                literal.put(pattern.get(++i));
            }
            else if (b == '%' || b == '_')
            {
                appendNormalized(literal, normalize, normalized);
                normalized.write(b);
            }
            else
            {
                literal.put(b);
            }
        }
        appendNormalized(literal, normalize, normalized);
        return ByteBuffer.wrap(normalized.toByteArray());
    }

    /**
     * Appends the escaped normalization of the literal accumulated in the given buffer, and clears it.
     */
    private static void appendNormalized(ByteBuffer literal, UnaryOperator<ByteBuffer> normalize, ByteArrayOutputStream out)
    {
        if (literal.position() == 0)
            return;
        literal.flip();
        ByteBuffer normalized = normalize.apply(literal.duplicate());
        literal.clear();
        if (normalized == null)
            return;
        for (int i = normalized.position(); i < normalized.limit(); i++)
        {
            byte b = normalized.get(i);
            if (b == '%' || b == '_' || b == ESCAPE)
                out.write(ESCAPE);
            out.write(b);
        }
    }

    /**
     * Returns the literal of a wildcard-free {@link Operator#LIKE_MATCHES} pattern in internal form, i.e. the
     * pattern without its escapes.
     */
    public static ByteBuffer unescape(ByteBuffer pattern)
    {
        ByteBuffer literal = ByteBuffer.allocate(pattern.remaining());
        for (int i = pattern.position(); i < pattern.limit(); i++)
        {
            byte b = pattern.get(i);
            if (b == ESCAPE && i + 1 < pattern.limit())
                b = pattern.get(++i);
            literal.put(b);
        }
        literal.flip();
        return literal;
    }

    /**
     * Returns the {@link TriePattern} for a pattern operator and its restriction value, from the cache when it holds
     * it:
     * <ul>
     *   <li>{@link Operator#LIKE_PREFIX} / {@link Operator#LIKE_SUFFIX} / {@link Operator#LIKE_CONTAINS}: the value is
     *       the literal left once {@code LikeRestriction#makeSpecific} trimmed the leading/trailing {@code %}
     *       wildcards of a pattern without inner wildcards; it is matched literally;</li>
     *   <li>{@link Operator#LIKE_MATCHES}: the value is a pattern in internal form (see {@link #ESCAPE}), either
     *       wildcard-free (exact match) or the whole pattern of a LIKE with an inner wildcard (including any
     *       leading/trailing ones), see {@link #triePattern}.</li>
     * </ul>
     *
     * @param operator the pattern operator
     * @param value the restriction value, as stored in the {@code RowFilter} expression
     * @param column the restricted column name, only used in error messages; {@code null} if unknown
     * @return the pattern, which is shared and must only be used through its public methods
     * @throws InvalidPatternException if a {@link Operator#LIKE_MATCHES} pattern is not in valid internal form
     */
    public static TriePattern forPatternOperator(Operator operator, ByteBuffer value, @Nullable Object column)
    {
        // Zero-copy probe: this runs per candidate row from the Operator#isSatisfiedBy post-filters, so it must not
        // copy the pattern buffer. ByteBuffer#equals/hashCode compare content, so the caller's buffer can key the
        // lookup directly.
        CacheKey probe = new CacheKey(operator, value);
        TriePattern cached = PATTERN_CACHE.getIfPresent(probe);
        if (cached != null)
            return cached;

        TriePattern pattern;
        try
        {
            pattern = build(operator, value);
        }
        catch (IllegalArgumentException e)
        {
            // not cached: the query fails on the first evaluation
            throw new InvalidPatternException(operator, column, String.valueOf(e.getMessage()));
        }

        // Store under a defensive copy of the pattern bytes: the static cache must not retain (and potentially pin) a
        // buffer owned by the caller. A concurrent build of the same pattern just replaces the entry with an equal one.
        PATTERN_CACHE.put(new CacheKey(operator, ByteBufferUtil.clone(value)), pattern);
        return pattern;
    }

    /**
     * Returns whether the given value matches the pattern of the given pattern operator and restriction value (see
     * {@link #forPatternOperator}).
     *
     * @throws InvalidPatternException if a {@link Operator#LIKE_MATCHES} pattern is not in valid internal form
     */
    public static boolean matches(Operator operator, ByteBuffer pattern, ByteBuffer value)
    {
        return forPatternOperator(operator, pattern, null).matches(value);
    }

    private static TriePattern build(Operator operator, ByteBuffer value)
    {
        switch (operator)
        {
            case LIKE_PREFIX:
                return TriePattern.builder().literal(value).anyString().build();
            case LIKE_SUFFIX:
                return TriePattern.builder().anyString().literal(value).build();
            case LIKE_CONTAINS:
                return TriePattern.builder().anyString().literal(value).anyString().build();
            case LIKE_MATCHES:
                return triePattern(value);
            default:
                throw new AssertionError("Not a pattern operator: " + operator);
        }
    }

    /**
     * Builds the {@link TriePattern} of a CQL {@code LIKE} pattern in internal form (see {@link #ESCAPE}): {@code %}
     * matches any string, and {@code _} any single Unicode character, whatever the length of its UTF-8 encoding. Not
     * cached.
     *
     * @param pattern the LIKE pattern, with {@code %} and {@code _} wildcards and {@link #ESCAPE} escapes
     * @throws IllegalArgumentException if the pattern ends with a lone escape, escapes another character, or is not
     *         valid UTF-8
     */
    @VisibleForTesting
    public static TriePattern triePattern(ByteBuffer pattern)
    {
        TriePattern.Builder builder = TriePattern.builder();
        // '%', '_' and the escape are single bytes in UTF-8 (and ASCII), never part of a multibyte character
        for (int i = pattern.position(); i < pattern.limit(); i++)
        {
            byte b = pattern.get(i);
            if (b == ESCAPE)
            {
                if (++i == pattern.limit())
                    throw new IllegalArgumentException("LIKE pattern ends with an escape: " + utf8(pattern));
                byte escaped = pattern.get(i);
                if (escaped != '%' && escaped != '_' && escaped != ESCAPE)
                    throw new IllegalArgumentException("LIKE pattern escapes a character other than '%', '_' or '\\': " + utf8(pattern));
                builder.literal(escaped);
            }
            else if (b == '%')
                builder.anyString();
            else if (b == '_')
                builder.anyChar();
            else
                builder.literal(b);
        }
        return builder.build();
    }

    private static String utf8(ByteBuffer value)
    {
        return StandardCharsets.UTF_8.decode(value.duplicate()).toString();
    }

    /**
     * Cache key: (operator, pattern buffer), compared and hashed by buffer content. {@link ByteBuffer#equals} and
     * {@link ByteBuffer#hashCode} are content-based over the remaining bytes, so lookup keys can wrap the caller's buffer
     * without copying it; only keys actually stored in the cache hold a defensive copy (see {@link #forPatternOperator}).
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
