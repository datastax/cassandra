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

import org.agrona.DirectBuffer;

/// Matches the keys of a trie against a [TriePattern] while the trie is walked, one byte at a time.
///
/// The state of the match is the set of pattern positions that the bytes of the current path can reach. The matcher
/// keeps the state for every depth of the current path, so that a walk can continue from any earlier depth, as it does
/// when it moves on to the next branch. The state at depth 0 (the root) is set on construction; the state at any
/// other depth is computed by [#descend] from the state at the previous depth.
///
/// There are two implementations, which differ in how they store the set of positions: [PatternMatcher64] uses a
/// single `long` and serves patterns of up to 63 positions, and [PatternMatcherLong] uses an array of `long`s. See
/// `PatternMatching.md` for a description of the algorithm.
public abstract class PatternMatcher
{
    // Flags describing a state, returned by flags()

    /// The state contains the end of the pattern: the path matches.
    public static final int MATCH = 1;
    /// The pattern ends in an any-string wildcard, which the path has reached: the path and all its extensions match,
    /// and a walk does not need to track the state below this point.
    public static final int MATCHES_ALL_EXTENSIONS = 2;
    /// The state contains an any-string wildcard: no branch can be ruled out without being walked.
    public static final int HAS_ANY_STRING = 4;
    /// The state contains a position that is not a literal byte.
    public static final int HAS_WILDCARD = 8;
    /// Returned by [#descend] instead of flags when the state is empty.
    public static final int EMPTY = -1;

    /// The elements of the pattern, see [TriePattern].
    final int[] elements;

    PatternMatcher(int[] elements)
    {
        this.elements = elements;
    }

    /// Creates a matcher for the given pattern, choosing the most efficient implementation for its length.
    public static PatternMatcher create(TriePattern pattern)
    {
        return pattern.length() < 64 ? new PatternMatcher64(pattern) : new PatternMatcherLong(pattern);
    }

    /// Sets the state at the given depth to the state reached by following the given byte from the state at the
    /// previous depth.
    ///
    /// @return the flags of the new state (see [#flags]), or [#EMPTY] if it is empty, i.e. no key that starts with the
    ///         current path can match.
    public abstract int descend(int depth, int transition);

    /// Follows the given bytes from the state at `depth - 1`, setting the states at `depth` to `depth + count - 1`.
    public abstract void descend(int depth, DirectBuffer bytes, int pos, int count);

    /// Returns the flags ([#MATCH], [#MATCHES_ALL_EXTENSIONS], [#HAS_ANY_STRING], [#HAS_WILDCARD]) that describe the
    /// state at the given depth.
    public abstract int flags(int depth);

    /// If the state at the given depth is a single literal position, returns its byte: the only byte that can follow
    /// the path. Otherwise returns -1.
    public abstract int singleLiteral(int depth);

    /// Returns the first byte after the given one, in the given direction, that the state at the given depth can
    /// accept, or 256 (forward) / -1 (reverse) if there is none. Used to skip branches that cannot match.
    public abstract int nextAcceptedByte(int depth, int afterByte, Direction direction);

    /// Returns a new matcher for the same pattern, whose state at depth 0 is this matcher's state at the given depth,
    /// to match the keys of a branch whose path leads to that depth.
    public abstract PatternMatcher tail(int depth);

    /// Whether the given value matches the pattern, starting from this matcher's root state. This neither uses nor
    /// changes the state history, so it can be called concurrently on a matcher that is not used to walk a trie.
    public abstract boolean matches(ByteBuffer value);
}
