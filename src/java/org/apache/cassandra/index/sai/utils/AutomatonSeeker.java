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

package org.apache.cassandra.index.sai.utils;

import java.util.Arrays;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.apache.lucene.util.automaton.Transition;

/**
 * Drives an automaton-guided (seek-skipping) walk of an ordered terms dictionary: given the byte string of a
 * visited dictionary term, decides whether it is accepted and, on a rejection outside of a "linear" (sequential)
 * stretch, computes the next string in unsigned byte order that does not put the DFA into a reject state, so the
 * caller can <em>seek</em> the dictionary there ({@code TrieTermsDictionaryReader#skipTo}) instead of visiting
 * every term in between.
 * <p>
 * This is a port of the {@code nextSeekTerm}/{@code nextString} machinery of Lucene 9.8's
 * {@code org.apache.lucene.index.AutomatonTermsEnum} (which is welded to Lucene's {@code TermsEnum} protocol and
 * cannot be reused directly), decoupled from the terms enumeration: the caller owns the dictionary cursor and
 * feeds visited terms to {@link #accept}, seeking to {@link #nextSeekTerm} whenever a {@code *_AND_SEEK} result
 * is returned. As in Lucene, the walk does not attempt to skip to the next <em>fully accepted</em> string (not
 * possible for infinite languages); it skips runs of terms that would put the DFA into a reject state, and
 * detects loops ("linear" mode) to fall back to plain sequential scanning of stretches where seeking cannot help.
 * <p>
 * The seeker only supports {@link CompiledAutomaton.AUTOMATON_TYPE#NORMAL} automatons and, like the intersection
 * it serves, is only correct when the dictionary stores (and is ordered by) the same raw bytes the
 * {@link ByteRunAutomaton} runs over — i.e. SAI's non-composite literal trie dictionaries, which store the raw
 * unterminated term bytes at every index version (see {@code OnDiskFormat#encodeForTrie}).
 * <p>
 * Note on shared {@link CompiledAutomaton} instances (see the compile cache in {@link AutomatonQueries}): this
 * class only performs read-only operations on the compiled automaton ({@code runAutomaton.step/isAccept/run},
 * {@code automaton.initTransition/getNextTransition} into a private {@link Transition} scratch); it never calls
 * the non-thread-safe {@code CompiledAutomaton#floor()}. All mutable walk state is per-instance, and a seeker
 * instance itself is single-query, single-thread state.
 */
@NotThreadSafe
public final class AutomatonSeeker
{
    /** The verdict on a visited term, mirroring Lucene's {@code FilteredTermsEnum.AcceptStatus}. */
    public enum Acceptance
    {
        /** Term accepted; keep reading the dictionary sequentially. */
        YES,
        /** Term accepted; seek to {@link #nextSeekTerm} for the next candidate. */
        YES_AND_SEEK,
        /** Term rejected; keep reading the dictionary sequentially (inside a linear stretch). */
        NO,
        /** Term rejected; seek to {@link #nextSeekTerm} for the next candidate. */
        NO_AND_SEEK
    }

    // a tableized array-based form of the DFA
    private final ByteRunAutomaton runAutomaton;
    // common suffix of the automaton, used as a cheap pre-filter
    private final BytesRef commonSuffixRef;
    // true if the automaton accepts a finite language
    private final boolean finite;
    // the byte-level DFA, with sorted transitions per state and no transitions to dead states
    private final Automaton automaton;
    // Visited-state tracking: each short records the generation the state was last visited in, so the array
    // never needs clearing between walks (only used for infinite languages, where loops exist)
    private final short[] visited;
    private short curGen;
    // the string being built for the next seek position
    private final BytesRefBuilder seekBytesRef = new BytesRefBuilder();
    // true while enumerating an infinite (looping) portion of the DFA, where the caller should read
    // sequentially up to linearUpperBound instead of seeking
    private boolean linear;
    private final BytesRef linearUpperBound = new BytesRef();
    private final Transition transition = new Transition();
    private final IntsRefBuilder savedStates = new IntsRefBuilder();

    // scratch used to wrap visited term bytes without reallocating
    private final BytesRef termScratch = new BytesRef();

    public AutomatonSeeker(CompiledAutomaton compiled)
    {
        if (compiled.type != CompiledAutomaton.AUTOMATON_TYPE.NORMAL)
            throw new IllegalArgumentException("AutomatonSeeker requires a NORMAL compiled automaton, got " + compiled.type);
        this.finite = compiled.finite;
        this.runAutomaton = compiled.runAutomaton;
        assert this.runAutomaton != null;
        this.commonSuffixRef = compiled.commonSuffixRef;
        this.automaton = compiled.automaton;
        // No need to track visited states for a finite language without loops.
        this.visited = finite ? null : new short[runAutomaton.getSize()];
    }

    /**
     * Returns the verdict on a visited dictionary term: whether it is accepted by the automaton, and whether the
     * caller should keep scanning sequentially or seek to {@link #nextSeekTerm}.
     */
    public Acceptance accept(byte[] termBytes)
    {
        termScratch.bytes = termBytes;
        termScratch.offset = 0;
        termScratch.length = termBytes.length;
        return accept(termScratch);
    }

    private Acceptance accept(BytesRef term)
    {
        if (commonSuffixRef == null || StringHelper.endsWith(term, commonSuffixRef))
        {
            if (runAutomaton.run(term.bytes, term.offset, term.length))
                return linear ? Acceptance.YES : Acceptance.YES_AND_SEEK;
            else
                return (linear && term.compareTo(linearUpperBound) < 0) ? Acceptance.NO : Acceptance.NO_AND_SEEK;
        }
        else
        {
            return (linear && term.compareTo(linearUpperBound) < 0) ? Acceptance.NO : Acceptance.NO_AND_SEEK;
        }
    }

    /**
     * Computes the seek target following the given term: the least byte string greater than {@code term}
     * ({@code null} for the initial positioning, where the least viable string overall — possibly the empty
     * string — is returned) that does not put the DFA into a reject state. The dictionary term the caller seeks
     * to (the least dictionary term at or after the target) is then fed back to {@link #accept}.
     *
     * @return the next seek target, or {@code null} when no string after {@code term} can possibly match (the
     *         walk is exhausted)
     */
    public @Nullable byte[] nextSeekTerm(@Nullable byte[] term)
    {
        if (term == null)
        {
            seekBytesRef.clear();
            // the empty term is a valid seek start when the DFA accepts it
            if (runAutomaton.isAccept(0))
                return copyOfSeekBytes();
        }
        else
        {
            seekBytesRef.copyBytes(term, 0, term.length);
        }

        // seek to the next possible string
        return nextString() ? copyOfSeekBytes() : null;
    }

    private byte[] copyOfSeekBytes()
    {
        return ArrayUtil.copyOfSubArray(seekBytesRef.bytes(), 0, seekBytesRef.length());
    }

    private void setVisited(int state)
    {
        if (!finite)
            visited[state] = curGen;
    }

    private boolean isVisited(int state)
    {
        return !finite && visited[state] == curGen;
    }

    /**
     * Sets the walk to operate in linear fashion: a looping transition was found at the given position, so an
     * upper bound is set and terms below it are scanned sequentially (like a term range query for that portion of
     * the term space).
     */
    private void setLinear(int position)
    {
        assert !linear;

        int state = 0;
        int maxInterval = 0xff;
        for (int i = 0; i < position; i++)
        {
            state = runAutomaton.step(state, seekBytesRef.byteAt(i) & 0xff);
            assert state >= 0 : "state=" + state;
        }
        final int numTransitions = automaton.getNumTransitions(state);
        automaton.initTransition(state, transition);
        for (int i = 0; i < numTransitions; i++)
        {
            automaton.getNextTransition(transition);
            if (transition.min <= (seekBytesRef.byteAt(position) & 0xff)
                && (seekBytesRef.byteAt(position) & 0xff) <= transition.max)
            {
                maxInterval = transition.max;
                break;
            }
        }
        // 0xff terms don't get the optimization... not worth the trouble.
        if (maxInterval != 0xff)
            maxInterval++;
        int length = position + 1; /* position + maxTransition */
        if (linearUpperBound.bytes.length < length)
            linearUpperBound.bytes = new byte[ArrayUtil.oversize(length, Byte.BYTES)];
        System.arraycopy(seekBytesRef.bytes(), 0, linearUpperBound.bytes, 0, position);
        linearUpperBound.bytes[position] = (byte) maxInterval;
        linearUpperBound.length = length;

        linear = true;
    }

    /**
     * Increments {@link #seekBytesRef} to the next string in binary order after it that will not put the machine
     * into a reject state. If such a string does not exist, returns false.
     * <p>
     * The correctness of this method depends upon the automaton being deterministic, and having no transitions to
     * dead states (guaranteed by {@link CompiledAutomaton}).
     *
     * @return true if more possible solutions exist for the DFA
     */
    private boolean nextString()
    {
        int state;
        int pos = 0;
        savedStates.grow(seekBytesRef.length() + 1);
        savedStates.setIntAt(0, 0);

        while (true)
        {
            if (!finite && ++curGen == 0)
            {
                // Clear the visited states every time curGen wraps (so very infrequently to not impact
                // average perf).
                Arrays.fill(visited, (short) -1);
            }
            linear = false;
            // walk the automaton until a character is rejected.
            for (state = savedStates.intAt(pos); pos < seekBytesRef.length(); pos++)
            {
                setVisited(state);
                int nextState = runAutomaton.step(state, seekBytesRef.byteAt(pos) & 0xff);
                if (nextState == -1)
                    break;
                savedStates.setIntAt(pos + 1, nextState);
                // we found a loop, record it for faster enumeration
                if (!linear && isVisited(nextState))
                    setLinear(pos);
                state = nextState;
            }

            // take the useful portion, and the last non-reject state, and attempt to
            // append characters that will match.
            if (nextString(state, pos))
            {
                return true;
            }
            else
            {
                /* no more solutions exist from this useful portion, backtrack */
                if ((pos = backtrack(pos)) < 0)
                    return false; /* no more solutions at all */

                final int newState = runAutomaton.step(savedStates.intAt(pos), seekBytesRef.byteAt(pos) & 0xff);
                if (newState >= 0 && runAutomaton.isAccept(newState))
                    return true; /* String is good to go as-is */

                /* else advance further */
                // paranoia (from Lucene): if we backtrack thru an infinite DFA, the loop detection is important;
                // restart from scratch for all infinite DFAs
                if (!finite)
                    pos = 0;
            }
        }
    }

    /**
     * Returns the next string in lexicographic order that will not put the machine into a reject state, walking
     * the DFA from the given position of {@link #seekBytesRef}, starting at the given state, along the minimal
     * path in lexicographic order as long as possible.
     * <p>
     * If this method returns false, there might still be more solutions; it is necessary to backtrack to find out.
     *
     * @param state current non-reject state
     * @param position useful portion of the string
     * @return true if more possible solutions exist for the DFA from this position
     */
    private boolean nextString(int state, int position)
    {
        /*
         * the next lexicographic character must be greater than the existing
         * character, if it exists.
         */
        int c = 0;
        if (position < seekBytesRef.length())
        {
            c = seekBytesRef.byteAt(position) & 0xff;
            // if the next byte is 0xff and is not part of the useful portion,
            // then by definition it puts us in a reject state, and therefore this
            // path is dead. there cannot be any higher transitions. backtrack.
            if (c++ == 0xff)
                return false;
        }

        seekBytesRef.setLength(position);
        setVisited(state);

        final int numTransitions = automaton.getNumTransitions(state);
        automaton.initTransition(state, transition);
        // find the minimal path (lexicographic order) that is >= c

        for (int i = 0; i < numTransitions; i++)
        {
            automaton.getNextTransition(transition);
            if (transition.max >= c)
            {
                int nextChar = Math.max(c, transition.min);
                // append either the next sequential char, or the minimum transition
                seekBytesRef.grow(seekBytesRef.length() + 1);
                seekBytesRef.append((byte) nextChar);
                state = transition.dest;
                /*
                 * as long as is possible, continue down the minimal path in
                 * lexicographic order. if a loop or accept state is encountered, stop.
                 */
                while (!isVisited(state) && !runAutomaton.isAccept(state))
                {
                    setVisited(state);
                    /*
                     * Note: we work with a DFA with no transitions to dead states.
                     * so the below is ok, if it is not an accept state,
                     * then there MUST be at least one transition.
                     */
                    automaton.initTransition(state, transition);
                    automaton.getNextTransition(transition);
                    state = transition.dest;

                    // append the minimum transition
                    seekBytesRef.grow(seekBytesRef.length() + 1);
                    seekBytesRef.append((byte) transition.min);

                    // we found a loop, record it for faster enumeration
                    if (!linear && isVisited(state))
                        setLinear(seekBytesRef.length() - 1);
                }
                return true;
            }
        }
        return false;
    }

    /**
     * Attempts to backtrack through the string after encountering a dead end at the given position.
     *
     * @return {@code position >= 0} if more possible solutions exist for the DFA, negative if the string is
     *         exhausted
     */
    private int backtrack(int position)
    {
        while (position-- > 0)
        {
            int nextChar = seekBytesRef.byteAt(position) & 0xff;
            // if a character is 0xff it's a dead-end too,
            // because there is no higher character in binary sort order.
            if (nextChar++ != 0xff)
            {
                seekBytesRef.setByteAt(position, (byte) nextChar);
                seekBytesRef.setLength(position + 1);
                return position;
            }
        }
        return -1; /* all solutions exhausted */
    }
}
