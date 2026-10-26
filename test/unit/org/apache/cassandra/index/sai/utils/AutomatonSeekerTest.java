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

import org.junit.Test;

import org.apache.cassandra.cql3.LikePatterns;
import org.apache.cassandra.index.sai.utils.AutomatonSeeker.Acceptance;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.CompiledAutomaton;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * The seek-skipping walk itself is covered against brute force by {@code AutomatonIntersectionTest} and
 * {@code TrieMemoryIndexAutomatonTest}; this pins how the seeker serves each {@link CompiledAutomaton.AUTOMATON_TYPE}.
 */
public class AutomatonSeekerTest
{
    @Test
    public void matchAllAcceptsEveryTermWithoutSeeking()
    {
        CompiledAutomaton all = LikePatterns.fromLikePattern("%");
        assertEquals(CompiledAutomaton.AUTOMATON_TYPE.ALL, all.type);
        AutomatonSeeker seeker = new AutomatonSeeker(all);
        assertArrayEquals(new byte[0], seeker.nextSeekTerm(null));
        assertEquals(Acceptance.YES, seeker.accept(utf8("")));
        assertEquals(Acceptance.YES, seeker.accept(utf8("anything")));
    }

    @Test
    public void emptyLanguageEndsTheWalkAtOnce()
    {
        CompiledAutomaton none = LikePatterns.compile(Automata.makeEmpty());
        assertEquals(CompiledAutomaton.AUTOMATON_TYPE.NONE, none.type);
        AutomatonSeeker seeker = new AutomatonSeeker(none);
        assertNull(seeker.nextSeekTerm(null));
        assertEquals(Acceptance.NO_AND_SEEK, seeker.accept(utf8("a")));
        assertNull(seeker.nextSeekTerm(utf8("a")));
    }

    @Test
    public void singleTermSeeksStraightToItsTerm()
    {
        CompiledAutomaton single = LikePatterns.fromLikePattern("beta");
        assertEquals(CompiledAutomaton.AUTOMATON_TYPE.SINGLE, single.type);
        AutomatonSeeker seeker = new AutomatonSeeker(single);
        assertArrayEquals(utf8("beta"), seeker.nextSeekTerm(null));
        assertEquals(Acceptance.NO_AND_SEEK, seeker.accept(utf8("alpha")));
        assertArrayEquals(utf8("beta"), seeker.nextSeekTerm(utf8("alpha")));
        assertEquals(Acceptance.YES_AND_SEEK, seeker.accept(utf8("beta")));
        assertNull(seeker.nextSeekTerm(utf8("beta")));
        assertEquals(Acceptance.NO_AND_SEEK, seeker.accept(utf8("gamma")));
        assertNull(seeker.nextSeekTerm(utf8("gamma")));
    }

    @Test
    public void normalAutomatonSeeksToTheNextViableString()
    {
        CompiledAutomaton pattern = LikePatterns.fromLikePattern("ab%z");
        assertEquals(CompiledAutomaton.AUTOMATON_TYPE.NORMAL, pattern.type);
        AutomatonSeeker seeker = new AutomatonSeeker(pattern);
        // the least viable string starts with the common prefix
        assertTrue(new String(seeker.nextSeekTerm(null), StandardCharsets.UTF_8).startsWith("ab"));
        assertRejected(seeker.accept(utf8("aa")));
        assertTrue(new String(seeker.nextSeekTerm(utf8("aa")), StandardCharsets.UTF_8).startsWith("ab"));
        assertAccepted(seeker.accept(utf8("abz")));
        assertAccepted(seeker.accept(utf8("abxyz")));
        assertRejected(seeker.accept(utf8("abzy")));
        assertRejected(seeker.accept(utf8("b")));
        assertNull(seeker.nextSeekTerm(utf8("b"))); // nothing after "b" can start with "ab"
    }

    private static void assertAccepted(Acceptance acceptance)
    {
        assertTrue(acceptance.toString(), acceptance == Acceptance.YES || acceptance == Acceptance.YES_AND_SEEK);
    }

    private static void assertRejected(Acceptance acceptance)
    {
        assertTrue(acceptance.toString(), acceptance == Acceptance.NO || acceptance == Acceptance.NO_AND_SEEK);
    }

    private static byte[] utf8(String s)
    {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
