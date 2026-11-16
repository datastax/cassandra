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
import java.util.Arrays;

import org.agrona.DirectBuffer;

/// [PatternMatcher] for patterns of any length, whose states are stored in `numLongs` words: bit `i % 64` of word
/// `i / 64` is set if position `i` is active. Shifts carry the top bits of each word into the next one.
///
/// A trailing any-string wildcard is used as the match position, which keeps patterns of exactly 64 positions that end
/// in one within a single word.
final class PatternMatcherLong extends PatternMatcher
{
    final int numLongs;
    // Positions that are not a literal byte
    final long[] isWildcard;
    // Positions that can be repeated (any-string wildcards and UTF-8 continuation loops), which stay active after
    // accepting a byte
    final long[] isLoop;
    // Positions that are any-string wildcards (* in glob, % in SQL like syntax)
    final long[] isStar;
    // For each byte class c (b >>> 6) and word i, at 2 * (c * numLongs + i) and the index after it the positions that
    // advance by one and by two positions after accepting a byte of that class, before shifting
    final long[] advances;
    // The positions that accept each byte, the words for byte b starting at b * numLongs
    final long[] patternMatch;

    // The match position is bit matchBit of the last word.
    final long matchBit;
    final boolean hasTrailingStar;

    // The state at depth 0
    final long[] rootState;
    // The state at each depth of the current path, the words for depth d starting at d * numLongs
    long[] states;

    PatternMatcherLong(TriePattern pattern)
    {
        super(pattern.elements);
        int length = elements.length;
        hasTrailingStar = length > 0 && elements[length - 1] == TriePattern.ANY_STRING;
        if (hasTrailingStar)
            --length;
        int numLongs = this.numLongs = length / 64 + 1;
        long[] baseMatch = new long[numLongs];
        long[] isWildStart = new long[numLongs];
        long[] isContLoop = new long[numLongs];
        isStar = new long[numLongs];
        patternMatch = new long[256 * numLongs];
        for (int i = 0; i < elements.length; ++i)
        {
            int element = elements[i];
            int word = i >>> 6;
            long bit = 1L << (i & 63);
            switch (element)
            {
                case TriePattern.CHAR_START:
                    isWildStart[word] |= bit;
                    break;
                case TriePattern.CHAR_CONTINUATION:
                    isContLoop[word] |= bit;
                    break;
                case TriePattern.ANY_STRING:
                    isStar[word] |= bit;
                    break;
                default:
                    baseMatch[word] |= bit;
                    break;
            }
            for (int b = 0; b < 256; ++b)
                if (TriePattern.accepts(element, b))
                    patternMatch[b * numLongs + word] |= bit;
        }

        isWildcard = new long[numLongs];
        isLoop = new long[numLongs];
        advances = new long[8 * numLongs];
        for (int i = 0; i < numLongs; ++i)
        {
            isWildcard[i] = isWildStart[i] | isContLoop[i] | isStar[i];
            // Only the positions that accept the byte survive the patternMatch mask, so a loop stays active only if
            // it accepts the byte, and the same isLoop works for all byte classes.
            isLoop[i] = isStar[i] | isContLoop[i];
            // The positions followed by a star, i.e. isStar >> 1 across words
            long beforeStar = isStar[i] >>> 1 | (i + 1 < numLongs ? isStar[i + 1] << 63 : 0);
            long isWildThenStar = isWildStart[i] & beforeStar;
            long baseThenStar = baseMatch[i] & beforeStar;
            // ASCII (two classes): a character start completes the character, skipping its continuation loop
            setAdvances(0, i, baseMatch[i] | isWildThenStar | isStar[i], baseThenStar | isWildStart[i]);
            setAdvances(1, i, baseMatch[i] | isWildThenStar | isStar[i], baseThenStar | isWildStart[i]);
            // continuation byte: a continuation loop can also be left
            setAdvances(2, i, baseMatch[i] | isStar[i] | isContLoop[i], baseThenStar);
            // multi-byte lead: a character start must continue into its continuation loop
            setAdvances(3, i, baseMatch[i] | isWildStart[i] | isStar[i], baseThenStar);
        }

        matchBit = 1L << (length % 64);
        rootState = new long[numLongs];
        // Entering a loop position also enters the next one, as the loop can be skipped.
        rootState[0] = 1 | (isLoop[0] & 1) << 1;
        states = new long[32 * numLongs];
        System.arraycopy(rootState, 0, states, 0, numLongs);
    }

    private void setAdvances(int byteClass, int word, long advanceOne, long advanceTwo)
    {
        int index = 2 * (byteClass * numLongs + word);
        advances[index] = advanceOne;
        advances[index + 1] = advanceTwo;
    }

    private PatternMatcherLong(PatternMatcherLong source, long[] rootState)
    {
        super(source.elements);
        numLongs = source.numLongs;
        isWildcard = source.isWildcard;
        isLoop = source.isLoop;
        isStar = source.isStar;
        advances = source.advances;
        patternMatch = source.patternMatch;
        matchBit = source.matchBit;
        hasTrailingStar = source.hasTrailingStar;
        this.rootState = rootState;
        states = new long[32 * numLongs];
        System.arraycopy(rootState, 0, states, 0, numLongs);
    }

    @Override
    public int descend(int depth, int transition)
    {
        // The positions that accept the byte (wildcards included, all encoded in patternMatch) stay if they are loops,
        // and advance by one or two positions as given by the advances of the byte's class.
        int numLongs = this.numLongs;
        int next = depth * numLongs;
        if (next + numLongs > states.length)
            states = Arrays.copyOf(states, states.length * 2);
        long[] states = this.states;
        long[] patternMatch = this.patternMatch;
        long[] advances = this.advances;
        int current = next - numLongs;
        int byteMatch = transition * numLongs;
        int a = (transition >>> 6) * 2 * numLongs;
        long carry = 0;
        long sum = 0;
        long stars = 0;
        long wildcards = 0;
        for (int i = 0; i < numLongs; ++i, a += 2)
        {
            long match = states[current + i] & patternMatch[byteMatch + i];
            long advanceOne = match & advances[a];
            long advanceTwo = match & advances[a + 1];
            long state = states[next + i] = match & isLoop[i] | advanceOne << 1 | advanceTwo << 2 | carry;
            carry = advanceOne >>> 63 | advanceTwo >>> 62;
            sum |= state;
            stars |= state & isStar[i];
            wildcards |= state & isWildcard[i];
        }
        if (sum == 0)
            return EMPTY;
        // Compute the flags while the words are at hand.
        return matchFlags(states, next) | (stars != 0 ? HAS_ANY_STRING : 0) | (wildcards != 0 ? HAS_WILDCARD : 0);
    }

    /// Computes the state after reading the given byte from the state at `from` in `source`, writes it at `to` in
    /// `target`, and returns the union of its words, which is 0 if the state is empty.
    private long step(long[] source, int from, long[] target, int to, int transition)
    {
        int numLongs = this.numLongs;
        long[] patternMatch = this.patternMatch;
        long[] advances = this.advances;
        long[] isLoop = this.isLoop;
        int byteMatch = transition * numLongs;
        int a = (transition >>> 6) * 2 * numLongs;
        long carry = 0;
        long sum = 0;
        for (int i = 0; i < numLongs; ++i, a += 2)
        {
            long match = source[from + i] & patternMatch[byteMatch + i];
            long advanceOne = match & advances[a];
            long advanceTwo = match & advances[a + 1];
            sum |= target[to + i] = match & isLoop[i] | advanceOne << 1 | advanceTwo << 2 | carry;
            carry = advanceOne >>> 63 | advanceTwo >>> 62;
        }
        return sum;
    }

    /// The match flags of the state whose words start at the given offset of the given array.
    private int matchFlags(long[] state, int offset)
    {
        if ((state[offset + numLongs - 1] & matchBit) == 0)
            return 0;
        return hasTrailingStar ? MATCH | MATCHES_ALL_EXTENSIONS : MATCH;
    }

    /// The flags of the state whose words start at the given offset of the given array.
    private int computeFlags(long[] state, int offset)
    {
        return matchFlags(state, offset)
               | (intersects(state, offset, isStar, 0) ? HAS_ANY_STRING : 0)
               | (intersects(state, offset, isWildcard, 0) ? HAS_WILDCARD : 0);
    }

    @Override
    public void descend(int depth, DirectBuffer bytes, int pos, int count)
    {
        // The flags of these states are not needed, flags() computes them if asked.
        int numLongs = this.numLongs;
        int end = (depth + count) * numLongs;
        if (end > states.length)
            states = Arrays.copyOf(states, Math.max(states.length * 2, end));
        long[] states = this.states;
        for (int next = depth * numLongs; next < end; next += numLongs)
            step(states, next - numLongs, states, next, bytes.getByte(pos++) & 0xFF);
    }

    @Override
    public int flags(int depth)
    {
        return computeFlags(states, depth * numLongs);
    }

    /// Whether the `numLongs` words of `a` starting at `aOffset` and of `b` starting at `bOffset` share a bit.
    private boolean intersects(long[] a, int aOffset, long[] b, int bOffset)
    {
        for (int i = 0; i < numLongs; ++i)
            if ((a[aOffset + i] & b[bOffset + i]) != 0)
                return true;
        return false;
    }

    @Override
    public int singleLiteral(int depth)
    {
        int offset = depth * numLongs;
        int position = -1;
        for (int i = 0; i < numLongs; ++i)
        {
            long bits = states[offset + i];
            if (bits == 0)
                continue;
            if (position >= 0 || (bits & (bits - 1)) != 0 || (bits & isWildcard[i]) != 0)
                return -1;
            position = i * 64 + Long.numberOfTrailingZeros(bits);
        }
        return position >= 0 && position < elements.length ? elements[position] : -1;
    }

    @Override
    public int nextAcceptedByte(int depth, int afterByte, Direction direction)
    {
        int offset = depth * numLongs;
        int nextByte = direction.select(256, -1);
        if (!intersects(states, offset, isWildcard, 0))
        {
            // Only literal positions are active (usually just one): the next candidate is the closest of their bytes.
            for (int i = 0; i < numLongs; ++i)
            {
                for (long bits = states[offset + i]; bits != 0; bits &= bits - 1)
                {
                    int position = i * 64 + Long.numberOfTrailingZeros(bits);
                    if (position >= elements.length)
                        continue; // match position, nothing can follow it
                    int b = elements[position];
                    if (direction.lt(afterByte, b) && direction.lt(b, nextByte))
                        nextByte = b;
                }
            }
            return nextByte;
        }

        // A byte class is active. Look for the next byte any of the active positions accepts.
        nextByte = afterByte;
        do
        {
            nextByte += direction.increase;
        }
        while (direction.inLoop(nextByte, 0, 255) && !intersects(patternMatch, nextByte * numLongs, states, offset));
        return nextByte;
    }

    @Override
    public PatternMatcher tail(int depth)
    {
        return new PatternMatcherLong(this, Arrays.copyOfRange(states, depth * numLongs, (depth + 1) * numLongs));
    }

    @Override
    public boolean matches(ByteBuffer value)
    {
        long[] state = rootState.clone();
        long[] next = new long[numLongs];
        for (int pos = value.position(); pos < value.limit(); ++pos)
        {
            if (hasTrailingStar && (state[numLongs - 1] & matchBit) != 0)
                return true; // a trailing star matches anything that follows
            if (step(state, 0, next, 0, value.get(pos) & 0xFF) == 0)
                return false;
            long[] swap = state;
            state = next;
            next = swap;
        }
        return (state[numLongs - 1] & matchBit) != 0;
    }
}
