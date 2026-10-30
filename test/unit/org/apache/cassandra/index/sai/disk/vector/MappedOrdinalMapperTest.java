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

package org.apache.cassandra.index.sai.disk.vector;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.junit.Test;

import io.github.jbellis.jvector.graph.disk.OrdinalMapper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MappedOrdinalMapperTest
{
    @Test
    public void freshMapperHasNoMappings() throws IOException
    {
        try (MappedOrdinalMapper mapper = new MappedOrdinalMapper(10, 5))
        {
            assertEquals(-1, mapper.maxOrdinal());
            // every valid old ordinal is unassigned → throws
            for (int i = 0; i < 10; i++)
                assertOldToNewThrows(mapper, i);
            // every valid new ordinal has no owner yet → OMITTED
            for (int i = 0; i < 5; i++)
                assertEquals(OrdinalMapper.OMITTED, mapper.newToOld(i));
        }
    }

    @Test
    public void assignedOrdinalsMapInBothDirections() throws IOException
    {
        try (MappedOrdinalMapper mapper = new MappedOrdinalMapper(10, 5))
        {
            mapper.assign(7, 0);
            mapper.assign(2, 3);
            mapper.assign(9, 1);

            assertEquals(0, mapper.oldToNew(7));
            assertEquals(3, mapper.oldToNew(2));
            assertEquals(1, mapper.oldToNew(9));
            assertEquals(7, mapper.newToOld(0));
            assertEquals(2, mapper.newToOld(3));
            assertEquals(9, mapper.newToOld(1));

            // unassigned old ordinal → throws
            assertOldToNewThrows(mapper, 0);
            // new ordinal with no owner → OMITTED
            assertEquals("unassigned new ordinal is a hole", OrdinalMapper.OMITTED, mapper.newToOld(2));
            assertEquals(3, mapper.maxOrdinal());
        }
    }

    @Test
    public void ordinalZeroIsDistinguishedFromUnmapped() throws IOException
    {
        try (MappedOrdinalMapper mapper = new MappedOrdinalMapper(4, 4))
        {
            mapper.assign(0, 0);
            assertEquals(0, mapper.oldToNew(0));
            assertEquals(0, mapper.newToOld(0));
            // ordinal 1 was never assigned → throws
            assertOldToNewThrows(mapper, 1);
            assertEquals(OrdinalMapper.OMITTED, mapper.newToOld(1));
        }
    }

    @Test
    public void lookupsOutOfRangeThrow() throws IOException
    {
        try (MappedOrdinalMapper mapper = new MappedOrdinalMapper(10, 5))
        {
            mapper.assign(3, 2);
            assertOldToNewThrows(mapper, -1);
            assertOldToNewThrows(mapper, 10);
            assertOldToNewThrows(mapper, Integer.MAX_VALUE);
            assertNewToOldThrows(mapper, -1);
            assertNewToOldThrows(mapper, 5);
            assertNewToOldThrows(mapper, Integer.MAX_VALUE);
        }
    }

    @Test
    public void assignOutOfRangeIsRejected() throws IOException
    {
        try (MappedOrdinalMapper mapper = new MappedOrdinalMapper(10, 5))
        {
            assertAssignRejected(mapper, -1, 0);
            assertAssignRejected(mapper, 10, 0);
            assertAssignRejected(mapper, 0, -1);
            assertAssignRejected(mapper, 0, 5);
            assertEquals("a rejected assign must leave no trace", -1, mapper.maxOrdinal());
            assertOldToNewThrows(mapper, 0);
            assertEquals(OrdinalMapper.OMITTED, mapper.newToOld(0));
        }
    }

    @Test
    public void behavesLikeMapMapperOnRandomPermutation() throws IOException
    {
        int idUpperBound = 50_000;
        int survivors = 20_000;
        Random random = new Random(42);

        // pick distinct old ordinals and give them new ordinals [0, survivors) in random order
        int[] olds = random.ints(0, idUpperBound).distinct().limit(survivors).toArray();
        Map<Integer, Integer> oldToNew = new HashMap<>();
        for (int newOrdinal = 0; newOrdinal < survivors; newOrdinal++)
            oldToNew.put(olds[newOrdinal], newOrdinal);
        OrdinalMapper expected = new OrdinalMapper.MapMapper(oldToNew);

        try (MappedOrdinalMapper actual = new MappedOrdinalMapper(idUpperBound, survivors))
        {
            // assign in a different (old-ordinal) order than new-ordinal order
            oldToNew.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .forEach(e -> actual.assign(e.getKey(), e.getValue()));

            assertEquals(expected.maxOrdinal(), actual.maxOrdinal());
            for (int old = 0; old < idUpperBound; old++)
            {
                if (oldToNew.containsKey(old))
                    assertEquals("oldToNew(" + old + ")", oldToNew.get(old).intValue(), actual.oldToNew(old));
                else
                    assertOldToNewThrows(actual, old);
            }
            for (int n = 0; n < survivors; n++)
                assertEquals("newToOld(" + n + ")", expected.newToOld(n), actual.newToOld(n));
        }
    }

    @Test
    public void closeDeletesBackingFiles() throws IOException
    {
        MappedOrdinalMapper mapper = new MappedOrdinalMapper(10, 5);
        mapper.assign(1, 1);
        File[] files = mapper.backingFiles();
        assertEquals(2, files.length);
        for (File f : files)
            assertTrue(f + " should exist while open", f.exists());

        mapper.close();

        for (File f : files)
            assertFalse(f + " should be deleted on close", f.exists());
    }

    private static void assertOldToNewThrows(MappedOrdinalMapper mapper, int oldOrdinal)
    {
        try
        {
            mapper.oldToNew(oldOrdinal);
            fail("expected oldToNew(" + oldOrdinal + ") to throw");
        }
        catch (IllegalArgumentException expected)
        {
            // expected
        }
    }

    private static void assertNewToOldThrows(MappedOrdinalMapper mapper, int newOrdinal)
    {
        try
        {
            mapper.newToOld(newOrdinal);
            fail("expected newToOld(" + newOrdinal + ") to throw");
        }
        catch (IllegalArgumentException expected)
        {
            // expected
        }
    }

    private static void assertAssignRejected(MappedOrdinalMapper mapper, int oldOrdinal, int newOrdinal)
    {
        try
        {
            mapper.assign(oldOrdinal, newOrdinal);
            fail("expected assign(" + oldOrdinal + ", " + newOrdinal + ") to be rejected");
        }
        catch (IllegalArgumentException expected)
        {
            // expected
        }
    }
}
