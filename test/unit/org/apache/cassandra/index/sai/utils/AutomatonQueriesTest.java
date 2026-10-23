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

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.junit.Test;

import org.apache.cassandra.cql3.LikePatterns;
import org.apache.lucene.util.automaton.Automata;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * The SAI-side helpers of the automaton intersections; the pattern engine itself is tested by
 * {@code org.apache.cassandra.cql3.LikePatternsTest}.
 */
public class AutomatonQueriesTest
{
    @Test
    public void commonPrefixBytes()
    {
        assertArrayEquals(utf8("abc"), AutomatonQueries.commonPrefixBytes(LikePatterns.fromLikePattern("abc%")));
        assertArrayEquals(utf8("abc"), AutomatonQueries.commonPrefixBytes(LikePatterns.fromLikePattern("abc%def")));
        assertArrayEquals(utf8("abc"), AutomatonQueries.commonPrefixBytes(LikePatterns.fromLikePattern("abc"))); // SINGLE
        assertArrayEquals(new byte[0], AutomatonQueries.commonPrefixBytes(LikePatterns.fromLikePattern("%abc")));
        assertArrayEquals(new byte[0], AutomatonQueries.commonPrefixBytes(LikePatterns.fromLikePattern("%"))); // ALL
        assertArrayEquals(new byte[0], AutomatonQueries.commonPrefixBytes(LikePatterns.compile(Automata.makeEmpty()))); // NONE
        // multi-byte characters contribute their full UTF-8 encoding to the prefix
        assertArrayEquals(utf8("café"), AutomatonQueries.commonPrefixBytes(LikePatterns.fromLikePattern("café%")));
    }

    @Test
    public void prefixUpperBound()
    {
        assertArrayEquals(new byte[]{ 0x62 }, AutomatonQueries.prefixUpperBound(new byte[]{ 0x61 }));
        assertArrayEquals(new byte[]{ 0x61, 0x63 }, AutomatonQueries.prefixUpperBound(new byte[]{ 0x61, 0x62 }));
        // trailing 0xFF bytes cannot be incremented and are dropped
        assertArrayEquals(new byte[]{ 0x62 }, AutomatonQueries.prefixUpperBound(new byte[]{ 0x61, (byte) 0xFF }));
        assertArrayEquals(new byte[]{ 0x62 }, AutomatonQueries.prefixUpperBound(new byte[]{ 0x61, (byte) 0xFF, (byte) 0xFF }));
        // no upper bound exists for an empty or all-0xFF prefix
        assertNull(AutomatonQueries.prefixUpperBound(new byte[0]));
        assertNull(AutomatonQueries.prefixUpperBound(new byte[]{ (byte) 0xFF, (byte) 0xFF }));

        // sanity: the bound is exclusive and sorts after every extension of the prefix
        byte[] prefix = utf8("ab");
        byte[] upper = AutomatonQueries.prefixUpperBound(prefix);
        assertTrue(Arrays.compareUnsigned(prefix, upper) < 0);
        assertTrue(Arrays.compareUnsigned(utf8("ab￿￿"), upper) < 0);
        assertTrue(Arrays.compareUnsigned(utf8("ac"), upper) >= 0);
    }

    private static byte[] utf8(String s)
    {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
