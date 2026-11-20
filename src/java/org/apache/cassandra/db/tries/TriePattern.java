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

package org.apache.cassandra.db.tries;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.apache.cassandra.utils.Hex;

/// A pattern that selects the keys of a trie, see [Trie#patternMatched(TriePattern)].
///
/// A pattern is a sequence of elements, each of which matches
///   - a literal character,
///   - any single character ([Builder#anyChar], `?` in [#glob] syntax and `_` in SQL `LIKE` syntax), or
///   - any sequence of characters, including the empty one ([Builder#anyString], `*` in [#glob] syntax and `%` in SQL
///     `LIKE` syntax).
///
/// A key is selected if all of it matches the pattern. Patterns and keys are UTF-8 encoded: the builder rejects literals
/// that are not valid UTF-8, and the keys must be valid UTF-8 (which is not checked; matching other keys gives
/// unspecified results).
///
/// Internally, the pattern is a sequence of positions, each accepting a class of bytes, some of which (the loops) can
/// be repeated any number of times, including zero. A character is matched as one byte that is not a continuation
/// byte, followed by a loop of continuation bytes (`0x80`-`0xBF`). The pattern is normalized so that no two loops are
/// adjacent, which the matchers rely on. See `PatternMatching.md` for details.
public final class TriePattern
{
    // Element codes. Values 0-255 are literal bytes.
    /// Any sequence of bytes (a loop accepting all bytes).
    static final int ANY_STRING = 0x100;
    /// The first byte of a UTF-8 character, i.e. any byte that is not a continuation byte.
    static final int CHAR_START = 0x101;
    /// Any number of UTF-8 continuation bytes (a loop accepting `0x80`-`0xBF`).
    static final int CHAR_CONTINUATION = 0x102;

    /// The elements of the normalized pattern.
    final int[] elements;

    /// The matcher used by [#matches], created on first use. It is never used to walk a trie, so its state history is
    /// not used and it can be shared by concurrent calls.
    private volatile PatternMatcher valueMatcher;

    private TriePattern(int[] elements)
    {
        this.elements = elements;
    }

    /// Creates a pattern from glob syntax, where `*` matches any sequence of characters and `?` matches any single
    /// character. All other characters are matched literally. There is no way to escape `*` or `?`.
    public static TriePattern glob(String pattern)
    {
        Builder builder = builder();
        // '*' and '?' are ASCII, they can never be part of a multi-byte UTF-8 character.
        for (byte b : pattern.getBytes(StandardCharsets.UTF_8))
        {
            if (b == '*')
                builder.anyString();
            else if (b == '?')
                builder.anyChar();
            else
                builder.literal(b);
        }
        return builder.build();
    }

    public static Builder builder()
    {
        return new Builder();
    }

    /// The number of positions in the pattern.
    int length()
    {
        return elements.length;
    }

    /// Whether the given value, which must be valid UTF-8, matches the pattern. Thread-safe.
    public boolean matches(ByteBuffer value)
    {
        PatternMatcher matcher = valueMatcher;
        if (matcher == null)
            valueMatcher = matcher = PatternMatcher.create(this);
        return matcher.matches(value);
    }

    /// Returns the literal bytes at the start of the pattern, which every matching key starts with.
    public byte[] literalPrefix()
    {
        int length = 0;
        while (length < elements.length && isLiteral(elements[length]))
            ++length;
        byte[] prefix = new byte[length];
        for (int i = 0; i < length; ++i)
            prefix[i] = (byte) elements[i];
        return prefix;
    }

    static boolean isLiteral(int element)
    {
        return element < 0x100;
    }

    /// Whether the element accepts the given byte.
    static boolean accepts(int element, int b)
    {
        switch (element)
        {
            case ANY_STRING:
                return true;
            case CHAR_START:
                return !isContinuation(b);
            case CHAR_CONTINUATION:
                return isContinuation(b);
            default:
                return element == b;
        }
    }

    private static boolean isContinuation(int b)
    {
        return (b & 0xC0) == 0x80;
    }

    @Override
    public boolean equals(Object o)
    {
        return o instanceof TriePattern && Arrays.equals(elements, ((TriePattern) o).elements);
    }

    @Override
    public int hashCode()
    {
        return Arrays.hashCode(elements);
    }

    /// Returns the pattern in glob syntax, with literal `*`, `?`, `\` and control characters shown as `\xNN`.
    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < elements.length; )
        {
            switch (elements[i])
            {
                case ANY_STRING:
                    sb.append('*');
                    ++i;
                    break;
                case CHAR_START:
                    sb.append('?');
                    ++i;
                    break;
                case CHAR_CONTINUATION:
                    ++i;
                    break;
                default:
                    // a run of literals, which is valid UTF-8
                    int start = i;
                    while (i < elements.length && isLiteral(elements[i]))
                        ++i;
                    byte[] bytes = new byte[i - start];
                    for (int j = start; j < i; ++j)
                        bytes[j - start] = (byte) elements[j];
                    new String(bytes, StandardCharsets.UTF_8).codePoints().forEach(c -> {
                        if (c == '*' || c == '?' || c == '\\' || c < 0x20 || c == 0x7F)
                            sb.append(String.format("\\x%02x", c));
                        else
                            sb.appendCodePoint(c);
                    });
            }
        }
        return sb.toString();
    }

    /// Builds a [TriePattern] from a sequence of literals and wildcards.
    public static class Builder
    {
        /// Token for [#anyChar], expanded into [#CHAR_START] and [#CHAR_CONTINUATION] by [#build].
        private static final int ANY_CHAR = 0x110;

        private int[] tokens = new int[16];
        private int size = 0;

        private Builder()
        {
        }

        private Builder add(int token)
        {
            if (size == tokens.length)
                tokens = Arrays.copyOf(tokens, size * 2);
            tokens[size++] = token;
            return this;
        }

        /// Adds a literal byte. The literal bytes between the wildcards must form valid UTF-8, see [#build].
        public Builder literal(byte b)
        {
            return add(b & 0xFF);
        }

        /// Adds the given bytes as literals.
        public Builder literal(byte[] bytes)
        {
            for (byte b : bytes)
                literal(b);
            return this;
        }

        /// Adds the remaining bytes of the given buffer as literals. The buffer's position is not changed.
        public Builder literal(ByteBuffer bytes)
        {
            for (int i = bytes.position(); i < bytes.limit(); ++i)
                literal(bytes.get(i));
            return this;
        }

        /// Adds the UTF-8 encoding of the given string as literals.
        public Builder literal(String s)
        {
            return literal(s.getBytes(StandardCharsets.UTF_8));
        }

        /// Adds a wildcard matching any single character.
        public Builder anyChar()
        {
            return add(ANY_CHAR);
        }

        /// Adds a wildcard matching any sequence of characters, including the empty one.
        public Builder anyString()
        {
            return add(ANY_STRING);
        }

        /// Builds the pattern.
        ///
        /// @throws IllegalArgumentException if the literals between two wildcards, or before the first or after the
        ///         last one, are not valid UTF-8. The wildcards are then always on character boundaries.
        public TriePattern build()
        {
            checkLiteralsAreUtf8();

            // Normalize: collapse runs of any-string wildcards, and move any-character wildcards before them, i.e. `*?`
            // becomes `?*`. As both are on character boundaries, both orders match one or more characters.
            int[] normalized = new int[size];
            int count = 0;
            boolean anyStringPending = false;
            for (int i = 0; i < size; ++i)
            {
                int token = tokens[i];
                if (token == ANY_STRING)
                    anyStringPending = true;
                else if (token == ANY_CHAR)
                    normalized[count++] = token;
                else
                {
                    if (anyStringPending)
                        normalized[count++] = ANY_STRING;
                    anyStringPending = false;
                    normalized[count++] = token;
                }
            }
            if (anyStringPending)
                normalized[count++] = ANY_STRING;

            // Expand characters into a start byte and a loop of continuation bytes. The loop takes all the
            // continuation bytes of the character, as the next element (the start of a literal character, another
            // character or the end) cannot take any. A character followed by an any-string has no loop: the
            // any-string takes the continuation bytes, and two loops must not be adjacent. An any-string is only
            // followed by a literal or the end, so it is never followed by a loop either.
            int[] elements = new int[count * 2];
            int length = 0;
            for (int i = 0; i < count; ++i)
            {
                int token = normalized[i];
                if (token != ANY_CHAR)
                    elements[length++] = token;
                else
                {
                    elements[length++] = CHAR_START;
                    if (i + 1 == count || normalized[i + 1] != ANY_STRING)
                        elements[length++] = CHAR_CONTINUATION;
                }
            }
            return new TriePattern(Arrays.copyOf(elements, length));
        }

        private void checkLiteralsAreUtf8()
        {
            CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder();
            int start = 0;
            for (int i = 0; i <= size; ++i)
            {
                if (i < size && isLiteral(tokens[i]))
                    continue;

                if (i > start)
                {
                    byte[] literal = new byte[i - start];
                    for (int j = start; j < i; ++j)
                        literal[j - start] = (byte) tokens[j];
                    try
                    {
                        decoder.decode(ByteBuffer.wrap(literal));
                    }
                    catch (CharacterCodingException e)
                    {
                        throw new IllegalArgumentException("Pattern literal is not valid UTF-8: 0x" + Hex.bytesToHex(literal));
                    }
                }
                start = i + 1;
            }
        }
    }
}
