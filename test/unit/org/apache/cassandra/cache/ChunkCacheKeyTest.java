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

package org.apache.cassandra.cache;

import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for ChunkCache.Key class
 */
public class ChunkCacheKeyTest
{
    /**
     * Helper method to create a Key instance using reflection since the constructor is package-private
     */
    private ChunkCache.Key createKey(long readerId, long position) throws Exception
    {
        return new ChunkCache.Key(readerId, position);
    }

    @Test
    public void testKeyConstruction() throws Exception
    {
        ChunkCache.Key key = createKey(123L, 456L);
        assertEquals(123L, key.readerId);
        assertEquals(456L, key.position);
    }

    @Test
    public void testKeyConstructionWithZeroValues() throws Exception
    {
        ChunkCache.Key key = createKey(0L, 0L);
        assertEquals(0L, key.readerId);
        assertEquals(0L, key.position);
    }

    @Test
    public void testKeyConstructionWithNegativeValues() throws Exception
    {
        ChunkCache.Key key = createKey(-1L, -1L);
        assertEquals(-1L, key.readerId);
        assertEquals(-1L, key.position);
    }

    @Test
    public void testKeyConstructionWithMaxValues() throws Exception
    {
        ChunkCache.Key key = createKey(Long.MAX_VALUE, Long.MAX_VALUE);
        assertEquals(Long.MAX_VALUE, key.readerId);
        assertEquals(Long.MAX_VALUE, key.position);
    }

    @Test
    public void testKeyConstructionWithMinValues() throws Exception
    {
        ChunkCache.Key key = createKey(Long.MIN_VALUE, Long.MIN_VALUE);
        assertEquals(Long.MIN_VALUE, key.readerId);
        assertEquals(Long.MIN_VALUE, key.position);
    }

    @Test
    public void testEqualsReflexive() throws Exception
    {
        ChunkCache.Key key = createKey(100L, 200L);
        assertTrue(key.equals(key));
    }

    @Test
    public void testEqualsSymmetric() throws Exception
    {
        ChunkCache.Key key1 = createKey(100L, 200L);
        ChunkCache.Key key2 = createKey(100L, 200L);
        assertTrue(key1.equals(key2));
        assertTrue(key2.equals(key1));
    }

    @Test
    public void testEqualsTransitive() throws Exception
    {
        ChunkCache.Key key1 = createKey(100L, 200L);
        ChunkCache.Key key2 = createKey(100L, 200L);
        ChunkCache.Key key3 = createKey(100L, 200L);
        assertTrue(key1.equals(key2));
        assertTrue(key2.equals(key3));
        assertTrue(key1.equals(key3));
    }

    @Test
    public void testEqualsConsistent() throws Exception
    {
        ChunkCache.Key key1 = createKey(100L, 200L);
        ChunkCache.Key key2 = createKey(100L, 200L);
        // Multiple invocations should return the same result
        for (int i = 0; i < 10; i++)
        {
            assertTrue(key1.equals(key2));
        }
    }

    @Test
    public void testEqualsWithNull() throws Exception
    {
        ChunkCache.Key key = createKey(100L, 200L);
        assertFalse(key.equals(null));
    }

    @Test
    public void testEqualsWithDifferentReaderId() throws Exception
    {
        ChunkCache.Key key1 = createKey(100L, 200L);
        ChunkCache.Key key2 = createKey(101L, 200L);
        assertFalse(key1.equals(key2));
        assertFalse(key2.equals(key1));
    }

    @Test
    public void testEqualsWithDifferentPosition() throws Exception
    {
        ChunkCache.Key key1 = createKey(100L, 200L);
        ChunkCache.Key key2 = createKey(100L, 201L);
        assertFalse(key1.equals(key2));
        assertFalse(key2.equals(key1));
    }

    @Test
    public void testEqualsWithBothDifferent() throws Exception
    {
        ChunkCache.Key key1 = createKey(100L, 200L);
        ChunkCache.Key key2 = createKey(101L, 201L);
        assertFalse(key1.equals(key2));
        assertFalse(key2.equals(key1));
    }

    @Test
    public void testHashCodeConsistency() throws Exception
    {
        ChunkCache.Key key = createKey(100L, 200L);
        int hash1 = key.hashCode();
        int hash2 = key.hashCode();
        assertEquals(hash1, hash2);
    }

    @Test
    public void testHashCodeEqualityContract() throws Exception
    {
        ChunkCache.Key key1 = createKey(100L, 200L);
        ChunkCache.Key key2 = createKey(100L, 200L);
        // If two objects are equal, their hash codes must be equal
        assertTrue(key1.equals(key2));
        assertEquals(key1.hashCode(), key2.hashCode());
    }

    @Test
    public void testHashCodeDifferentKeys() throws Exception
    {
        ChunkCache.Key key1 = createKey(100L, 200L);
        ChunkCache.Key key2 = createKey(101L, 200L);
        ChunkCache.Key key3 = createKey(100L, 201L);

        // Different keys should ideally have different hash codes (though not guaranteed)
        // We just verify they're computed without error
        int hash1 = key1.hashCode();
        int hash2 = key2.hashCode();
        int hash3 = key3.hashCode();

        // At least one should be different
        assertTrue(hash1 != hash2 || hash1 != hash3);
    }

    @Test
    public void testHashCodeDistribution() throws Exception
    {
        // Test that hash codes are reasonably distributed
        Set<Integer> hashes = new HashSet<>();
        for (long i = 0; i < 1000; i++)
        {
            ChunkCache.Key key = createKey(i, i * 2);
            hashes.add(key.hashCode());
        }

        // We should have a good distribution - at least 90% unique hashes
        assertTrue("Hash distribution is poor: " + hashes.size() + " unique hashes out of 1000",
                   hashes.size() > 900);
    }

    @Test
    public void testHashCodeWithZeroValues() throws Exception
    {
        ChunkCache.Key key = createKey(0L, 0L);
        // Should not throw and should produce a valid hash
        int hash = key.hashCode();
        // Verify consistency
        assertEquals(hash, key.hashCode());
    }

    @Test
    public void testHashCodeWithMaxValues() throws Exception
    {
        ChunkCache.Key key = createKey(Long.MAX_VALUE, Long.MAX_VALUE);
        int hash = key.hashCode();
        assertEquals(hash, key.hashCode());
    }

    @Test
    public void testHashCodeWithMinValues() throws Exception
    {
        ChunkCache.Key key = createKey(Long.MIN_VALUE, Long.MIN_VALUE);
        int hash = key.hashCode();
        assertEquals(hash, key.hashCode());
    }

    @Test
    public void testCompareToReflexive() throws Exception
    {
        ChunkCache.Key key = createKey(100L, 200L);
        assertEquals(0, key.compareTo(key));
    }

    @Test
    public void testCompareToSymmetric() throws Exception
    {
        ChunkCache.Key key1 = createKey(100L, 200L);
        ChunkCache.Key key2 = createKey(100L, 200L);
        assertEquals(0, key1.compareTo(key2));
        assertEquals(0, key2.compareTo(key1));
    }

    @Test
    public void testCompareToTransitive() throws Exception
    {
        ChunkCache.Key key1 = createKey(100L, 200L);
        ChunkCache.Key key2 = createKey(200L, 300L);
        ChunkCache.Key key3 = createKey(300L, 400L);

        assertTrue(key1.compareTo(key2) < 0);
        assertTrue(key2.compareTo(key3) < 0);
        assertTrue(key1.compareTo(key3) < 0);
    }

    @Test
    public void testCompareToConsistentWithEquals() throws Exception
    {
        ChunkCache.Key key1 = createKey(100L, 200L);
        ChunkCache.Key key2 = createKey(100L, 200L);

        assertTrue(key1.equals(key2));
        assertEquals(0, key1.compareTo(key2));
    }

    @Test
    public void testCompareToByReaderId() throws Exception
    {
        ChunkCache.Key key1 = createKey(100L, 200L);
        ChunkCache.Key key2 = createKey(200L, 200L);

        assertTrue(key1.compareTo(key2) < 0);
        assertTrue(key2.compareTo(key1) > 0);
    }

    @Test
    public void testCompareToByPosition() throws Exception
    {
        // When readerId is the same, position determines order
        ChunkCache.Key key1 = createKey(100L, 200L);
        ChunkCache.Key key2 = createKey(100L, 300L);

        assertTrue(key1.compareTo(key2) < 0);
        assertTrue(key2.compareTo(key1) > 0);
    }

    @Test
    public void testCompareToReaderIdTakesPrecedence() throws Exception
    {
        // readerId comparison takes precedence over position
        ChunkCache.Key key1 = createKey(100L, 500L);
        ChunkCache.Key key2 = createKey(200L, 100L);

        // key1 < key2 because readerId 100 < 200, even though position 500 > 100
        assertTrue(key1.compareTo(key2) < 0);
        assertTrue(key2.compareTo(key1) > 0);
    }

    @Test
    public void testCompareToWithZeroValues() throws Exception
    {
        ChunkCache.Key key1 = createKey(0L, 0L);
        ChunkCache.Key key2 = createKey(0L, 0L);
        assertEquals(0, key1.compareTo(key2));
    }

    @Test
    public void testCompareToWithNegativeValues() throws Exception
    {
        ChunkCache.Key key1 = createKey(-100L, -200L);
        ChunkCache.Key key2 = createKey(-50L, -100L);

        assertTrue(key1.compareTo(key2) < 0);
        assertTrue(key2.compareTo(key1) > 0);
    }

    @Test
    public void testCompareToWithMaxValues() throws Exception
    {
        ChunkCache.Key key1 = createKey(Long.MAX_VALUE, Long.MAX_VALUE);
        ChunkCache.Key key2 = createKey(Long.MAX_VALUE - 1, Long.MAX_VALUE);

        assertTrue(key1.compareTo(key2) > 0);
        assertTrue(key2.compareTo(key1) < 0);
    }

    @Test
    public void testCompareToWithMinValues() throws Exception
    {
        ChunkCache.Key key1 = createKey(Long.MIN_VALUE, Long.MIN_VALUE);
        ChunkCache.Key key2 = createKey(Long.MIN_VALUE + 1, Long.MIN_VALUE);

        assertTrue(key1.compareTo(key2) < 0);
        assertTrue(key2.compareTo(key1) > 0);
    }

    @Test
    public void testCompareToOrdering() throws Exception
    {
        ChunkCache.Key[] keys = new ChunkCache.Key[]{
        createKey(300L, 100L),
        createKey(100L, 300L),
        createKey(200L, 200L),
        createKey(100L, 100L),
        createKey(100L, 200L)
        };

        Arrays.sort(keys);

        // Expected order after sorting:
        // (100, 100), (100, 200), (100, 300), (200, 200), (300, 100)
        assertEquals(100L, keys[0].readerId);
        assertEquals(100L, keys[0].position);

        assertEquals(100L, keys[1].readerId);
        assertEquals(200L, keys[1].position);

        assertEquals(100L, keys[2].readerId);
        assertEquals(300L, keys[2].position);

        assertEquals(200L, keys[3].readerId);
        assertEquals(200L, keys[3].position);

        assertEquals(300L, keys[4].readerId);
        assertEquals(100L, keys[4].position);
    }

    @Test
    public void testKeyInHashMap() throws Exception
    {
        // Test that Key works correctly as a HashMap key
        HashMap<ChunkCache.Key, String> map = new HashMap<>();

        ChunkCache.Key key1 = createKey(100L, 200L);
        ChunkCache.Key key2 = createKey(100L, 200L);
        ChunkCache.Key key3 = createKey(101L, 200L);

        map.put(key1, "value1");

        // key2 should retrieve the same value as key1 since they're equal
        assertEquals("value1", map.get(key2));

        // key3 should not retrieve anything
        assertEquals(null, map.get(key3));

        // Adding with key2 should replace the value
        map.put(key2, "value2");
        assertEquals(1, map.size());
        assertEquals("value2", map.get(key1));
    }

    @Test
    public void testKeyInTreeSet() throws Exception
    {
        // Test that Key works correctly in a TreeSet (uses compareTo)
        TreeSet<ChunkCache.Key> set = new TreeSet<>();

        ChunkCache.Key key1 = createKey(100L, 200L);
        ChunkCache.Key key2 = createKey(100L, 200L);
        ChunkCache.Key key3 = createKey(200L, 100L);

        set.add(key1);
        set.add(key2);
        set.add(key3);

        // Should only have 2 elements (key1 and key2 are equal)
        assertEquals(2, set.size());
        assertTrue(set.contains(key1));
        assertTrue(set.contains(key2));
        assertTrue(set.contains(key3));
    }

    @Test
    public void testEqualsDifferentTypes() throws Exception
    {
        ChunkCache.Key key = createKey(100L, 200L);
        Object other = new Object();
        assertFalse(key.equals(other));
    }

    @Test
    public void testEqualsWithString() throws Exception
    {
        ChunkCache.Key key = createKey(100L, 200L);
        assertFalse(key.equals("not a key"));
    }
}
