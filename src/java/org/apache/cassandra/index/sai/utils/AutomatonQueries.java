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

import org.apache.cassandra.cql3.LikePatterns;
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
 * ({@code LIKE '<term>%'} keeps its dedicated bounded range scan.) The intersections scan the terms starting with
 * the common prefix of the automaton ({@link #commonPrefixBytes}), filtered by the automaton.
 */
public final class AutomatonQueries
{
    private AutomatonQueries()
    {
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
