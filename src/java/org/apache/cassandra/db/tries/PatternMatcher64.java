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

/// [PatternMatcher] for patterns of up to 63 positions, whose states are stored in a single `long`: bit `i` is set
/// if position `i` is active, and bit `length` is the match position (the end of the pattern).
final class PatternMatcher64 extends PatternMatcher
{
    // The positions that stay active after accepting a byte: the loops
    final long isLoop;
    // For each byte class c (b >>> 6), at 2c and 2c + 1 the positions reached by advancing one and two positions
    // after accepting a byte of that class, already shifted into place
    final long[] shifts;
    // The positions that accept each byte
    final long[] patternMatch;

    // Positions that are not a literal byte
    final long isWildcard;

    // Positions that are any-string wildcards (* in glob, % in SQL like syntax)
    final long isStar;

    // The bit that marks a match: the end of the pattern, or a trailing any-string wildcard, which matches everything
    // from the point it is reached
    final long matchBit;

    // The state at depth 0
    final long rootState;
    // The state at each depth of the current path
    long[] states;

    PatternMatcher64(TriePattern pattern)
    {
        super(pattern.elements);
        assert elements.length < 64;
        long baseMatch = 0;
        long isWildStart = 0;
        long isContLoop = 0;
        long isStar = 0;
        long[] patternMatch = new long[256];
        long bit = 1;
        for (int element : elements)
        {
            if (TriePattern.isLiteral(element))
            {
                patternMatch[element] |= bit;
                baseMatch |= bit;
            }
            else
            {
                switch (element)
                {
                    case TriePattern.CHAR_START:
                        isWildStart |= bit;
                        break;
                    case TriePattern.CHAR_CONTINUATION:
                        isContLoop |= bit;
                        break;
                    case TriePattern.ANY_STRING:
                        isStar |= bit;
                        break;
                }
            }
            bit <<= 1;
        }
        this.isWildcard = isWildStart | isContLoop | isStar;
        this.isStar = isStar;
        // Only the positions that accept the byte survive the patternMatch mask, so a loop stays active only if it
        // accepts the byte, and the same isLoop works for all byte classes.
        this.isLoop = isStar | isContLoop;
        long isWildThenStar = isWildStart & isStar >> 1;
        long baseThenStar = baseMatch & isStar >> 1;
        long[] shift1 = {
            // ASCII (two classes): a character start completes the character, skipping its continuation loop
            baseMatch | isWildThenStar | isStar,
            baseMatch | isWildThenStar | isStar,
            // continuation byte: only a star or a continuation loop can accept it
            baseMatch | isStar | isContLoop,
            // multi-byte lead: a character start must continue into its continuation loop
            baseMatch | isWildStart | isStar };
        long[] shift2 = { baseThenStar | isWildStart, baseThenStar | isWildStart, baseThenStar, baseThenStar };
        long[] shifts = new long[8];
        for (int c = 0; c < 4; ++c)
        {
            shifts[2 * c] = shift1[c] << 1;
            shifts[2 * c + 1] = shift2[c] << 2;
        }
        this.shifts = shifts;

        int i = 0;
        for (; i < 128; ++i)
            patternMatch[i] |= isStar | isWildStart;
        for (; i < 192; ++i)
            patternMatch[i] |= isStar | isContLoop;
        for (; i < 256; ++i)
            patternMatch[i] |= isStar | isWildStart;

        this.patternMatch = patternMatch;
        long matchBit = 1L << elements.length;
        if ((isStar & matchBit >>> 1) != 0)
            matchBit >>>= 1; // we can end early on trailing stars
        this.matchBit = matchBit;
        states = new long[32];
        // Entering a loop position also enters the next one, as the loop can be skipped.
        rootState = 1 | (isStar & 1) << 1;
        states[0] = rootState;
    }

    private PatternMatcher64(PatternMatcher64 source, long rootState)
    {
        super(source.elements);
        isWildcard = source.isWildcard;
        isStar = source.isStar;
        isLoop = source.isLoop;
        shifts = source.shifts;
        patternMatch = source.patternMatch;
        matchBit = source.matchBit;
        this.rootState = rootState;
        states = new long[32];
        states[0] = rootState;
    }

    private long next(long state, int transition)
    {
        int i = (transition >>> 5) & 6; // 2 * (transition >>> 6)
        long match = state & patternMatch[transition];
        return match & isLoop | match << 1 & shifts[i] | match << 2 & shifts[i + 1];
    }

    @Override
    public int descend(int depth, int transition)
    {
        long next = next(states[depth - 1], transition);
        if (depth >= states.length)
            states = Arrays.copyOf(states, states.length * 2);
        states[depth] = next;
        return next != 0 ? flags(next) : EMPTY;
    }

    @Override
    public void descend(int depth, DirectBuffer bytes, int pos, int count)
    {
        int end = depth + count;
        if (end > states.length)
            states = Arrays.copyOf(states, Math.max(states.length * 2, end));
        long[] states = this.states;
        long state = states[depth - 1];
        for (int i = depth; i < end; ++i)
            states[i] = state = next(state, bytes.getByte(pos++) & 0xFF);
    }

    @Override
    public int flags(int depth)
    {
        return flags(states[depth]);
    }

    private int flags(long state)
    {
        int flags = 0;
        if ((state & matchBit) != 0)
            flags |= MATCH;
        if ((state & isStar) != 0)
        {
            flags |= HAS_ANY_STRING;
            if ((state & isStar & matchBit) != 0)
                flags |= MATCHES_ALL_EXTENSIONS;
        }
        if ((state & isWildcard) != 0)
            flags |= HAS_WILDCARD;
        return flags;
    }

    @Override
    public int singleLiteral(int depth)
    {
        long state = states[depth];
        if ((state & (state - 1)) != 0 || (state & isWildcard) != 0)
            return -1;
        int position = Long.numberOfTrailingZeros(state);
        return position < elements.length ? elements[position] : -1;
    }

    @Override
    public int nextAcceptedByte(int depth, int afterByte, Direction direction)
    {
        long state = states[depth];
        int nextByte = direction.select(256, -1);
        if ((state & isWildcard) == 0)
        {
            // Only literal positions are active (usually just one): the next candidate is the closest of their bytes.
            for (long bits = state; bits != 0; bits &= bits - 1)
            {
                int position = Long.numberOfTrailingZeros(bits);
                if (position >= elements.length)
                    continue; // match position, nothing can follow it
                int b = elements[position];
                if (direction.lt(afterByte, b) && direction.lt(b, nextByte))
                    nextByte = b;
            }
            return nextByte;
        }

        // A byte class is active. Look for the next byte any of the active positions accepts.
        nextByte = afterByte;
        do
        {
            nextByte += direction.increase;
        }
        while (direction.inLoop(nextByte, 0, 255) && (patternMatch[nextByte] & state) == 0);
        return nextByte;
    }

    @Override
    public PatternMatcher tail(int depth)
    {
        return new PatternMatcher64(this, states[depth]);
    }

    @Override
    public boolean matches(ByteBuffer value)
    {
        long state = rootState;
        for (int i = value.position(); i < value.limit(); ++i)
        {
            if ((state & isStar & matchBit) != 0)
                return true; // a trailing star matches anything that follows
            state = next(state, value.get(i) & 0xFF);
            if (state == 0)
                return false;
        }
        return (state & matchBit) != 0;
    }
}
