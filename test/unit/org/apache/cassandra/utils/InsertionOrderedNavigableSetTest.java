/*
 * Copyright DataStax, Inc.
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

package org.apache.cassandra.utils;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.SortedSet;

import org.junit.Test;
import static org.junit.Assert.*;

public class InsertionOrderedNavigableSetTest
{
    private final Comparator<Integer> naturalOrder = Integer::compareTo;

    @Test
    public void testEmptySet()
    {
        InsertionOrderedNavigableSet<Integer> set = new InsertionOrderedNavigableSet<>(naturalOrder);
        
        assertTrue(set.isEmpty());
        assertEquals(0, set.size());
        assertFalse(set.contains(1));
        assertEquals(naturalOrder, set.comparator());
        assertNull(set.pollFirst());
        assertNull(set.pollLast());
    }

    @Test
    public void testAddInOrder()
    {
        InsertionOrderedNavigableSet<Integer> set = new InsertionOrderedNavigableSet<>(naturalOrder);
        
        assertTrue(set.add(1));
        assertTrue(set.add(2));
        assertTrue(set.add(3));
        
        assertEquals(3, set.size());
        assertFalse(set.isEmpty());
        assertEquals(Integer.valueOf(1), set.first());
        assertEquals(Integer.valueOf(3), set.last());
    }

    @Test(expected = IllegalStateException.class)
    public void testAddOutOfOrder()
    {
        InsertionOrderedNavigableSet<Integer> set = new InsertionOrderedNavigableSet<>(naturalOrder);
        set.add(2);
        set.add(1); // Should throw IllegalStateException
    }

    @Test(expected = IllegalStateException.class)
    public void testAddEqual()
    {
        InsertionOrderedNavigableSet<Integer> set = new InsertionOrderedNavigableSet<>(naturalOrder);
        set.add(1);
        set.add(1); // Should throw IllegalStateException
    }

    @Test
    public void testPollFirst()
    {
        InsertionOrderedNavigableSet<Integer> set = new InsertionOrderedNavigableSet<>(naturalOrder);
        set.add(1);
        set.add(2);
        set.add(3);
        
        assertEquals(Integer.valueOf(1), set.pollFirst());
        assertEquals(2, set.size());
        assertEquals(Integer.valueOf(2), set.first());
    }

    @Test
    public void testPollLast()
    {
        InsertionOrderedNavigableSet<Integer> set = new InsertionOrderedNavigableSet<>(naturalOrder);
        set.add(1);
        set.add(2);
        set.add(3);
        
        assertEquals(Integer.valueOf(3), set.pollLast());
        assertEquals(2, set.size());
        assertEquals(Integer.valueOf(2), set.last());
    }

    @Test(expected = NoSuchElementException.class)
    public void testFirstOnEmpty()
    {
        InsertionOrderedNavigableSet<Integer> set = new InsertionOrderedNavigableSet<>(naturalOrder);
        set.first();
    }

    @Test(expected = NoSuchElementException.class)
    public void testLastOnEmpty()
    {
        InsertionOrderedNavigableSet<Integer> set = new InsertionOrderedNavigableSet<>(naturalOrder);
        set.last();
    }

    @Test
    public void testContains()
    {
        InsertionOrderedNavigableSet<Integer> set = new InsertionOrderedNavigableSet<>(naturalOrder);
        set.add(1);
        set.add(3);
        set.add(5);
        
        assertTrue(set.contains(1));
        assertTrue(set.contains(3));
        assertTrue(set.contains(5));
        assertFalse(set.contains(2));
        assertFalse(set.contains(4));
    }

    @Test
    public void testRemove()
    {
        InsertionOrderedNavigableSet<Integer> set = new InsertionOrderedNavigableSet<>(naturalOrder);
        set.add(1);
        set.add(2);
        set.add(3);
        
        assertTrue(set.remove(2));
        assertEquals(2, set.size());
        assertFalse(set.contains(2));
        assertFalse(set.remove(4));
    }

    @Test
    public void testIterator()
    {
        InsertionOrderedNavigableSet<Integer> set = new InsertionOrderedNavigableSet<>(naturalOrder);
        set.add(1);
        set.add(2);
        set.add(3);
        
        Iterator<Integer> iter = set.iterator();
        assertTrue(iter.hasNext());
        assertEquals(Integer.valueOf(1), iter.next());
        assertEquals(Integer.valueOf(2), iter.next());
        assertEquals(Integer.valueOf(3), iter.next());
        assertFalse(iter.hasNext());
    }

    @Test
    public void testToArray()
    {
        InsertionOrderedNavigableSet<Integer> set = new InsertionOrderedNavigableSet<>(naturalOrder);
        set.add(1);
        set.add(2);
        set.add(3);
        
        Object[] array = set.toArray();
        assertArrayEquals(new Object[]{1, 2, 3}, array);
        
        Integer[] typedArray = set.toArray(new Integer[0]);
        assertArrayEquals(new Integer[]{1, 2, 3}, typedArray);
    }

    @Test
    public void testContainsAll()
    {
        InsertionOrderedNavigableSet<Integer> set = new InsertionOrderedNavigableSet<>(naturalOrder);
        set.add(1);
        set.add(2);
        set.add(3);
        
        assertTrue(set.containsAll(Arrays.asList(1, 2)));
        assertTrue(set.containsAll(Arrays.asList(1, 2, 3)));
        assertFalse(set.containsAll(Arrays.asList(1, 2, 4)));
    }

    @Test
    public void testAddAll()
    {
        InsertionOrderedNavigableSet<Integer> set = new InsertionOrderedNavigableSet<>(naturalOrder);
        
        assertTrue(set.addAll(Arrays.asList(1, 2, 3)));
        assertEquals(3, set.size());
        assertFalse(set.addAll(Collections.emptyList()));
    }

    @Test(expected = IllegalStateException.class)
    public void testAddAllOutOfOrder()
    {
        InsertionOrderedNavigableSet<Integer> set = new InsertionOrderedNavigableSet<>(naturalOrder);
        set.add(2);
        set.addAll(Arrays.asList(1, 3)); // Should fail on 1
    }

    @Test
    public void testRetainAll()
    {
        InsertionOrderedNavigableSet<Integer> set = new InsertionOrderedNavigableSet<>(naturalOrder);
        set.add(1);
        set.add(2);
        set.add(3);
        set.add(4);
        
        assertTrue(set.retainAll(Arrays.asList(2, 3)));
        assertEquals(2, set.size());
        assertTrue(set.contains(2));
        assertTrue(set.contains(3));
        assertFalse(set.contains(1));
        assertFalse(set.contains(4));
    }

    @Test
    public void testRemoveAll()
    {
        InsertionOrderedNavigableSet<Integer> set = new InsertionOrderedNavigableSet<>(naturalOrder);
        set.add(1);
        set.add(2);
        set.add(3);
        set.add(4);
        
        assertTrue(set.removeAll(Arrays.asList(2, 3)));
        assertEquals(2, set.size());
        assertTrue(set.contains(1));
        assertTrue(set.contains(4));
        assertFalse(set.contains(2));
        assertFalse(set.contains(3));
    }

    @Test
    public void testClear()
    {
        InsertionOrderedNavigableSet<Integer> set = new InsertionOrderedNavigableSet<>(naturalOrder);
        set.add(1);
        set.add(2);
        set.add(3);
        
        set.clear();
        assertTrue(set.isEmpty());
        assertEquals(0, set.size());
    }

    @Test
    public void testHeadSet()
    {
        InsertionOrderedNavigableSet<Integer> set = new InsertionOrderedNavigableSet<>(naturalOrder);
        set.add(1);
        set.add(2);
        set.add(3);
        set.add(4);
        set.add(5);
        
        NavigableSet<Integer> head = set.headSet(3, false);
        assertEquals(2, head.size());
        assertTrue(head.contains(1));
        assertTrue(head.contains(2));
        assertFalse(head.contains(3));
        
        NavigableSet<Integer> headInclusive = set.headSet(3, true);
        assertEquals(3, headInclusive.size());
        assertTrue(headInclusive.contains(3));
        
        SortedSet<Integer> headDefault = set.headSet(3);
        assertEquals(2, headDefault.size());
        assertFalse(headDefault.contains(3));
    }

    @Test
    public void testTailSet()
    {
        InsertionOrderedNavigableSet<Integer> set = new InsertionOrderedNavigableSet<>(naturalOrder);
        set.add(1);
        set.add(2);
        set.add(3);
        set.add(4);
        set.add(5);
        
        NavigableSet<Integer> tail = set.tailSet(3, false);
        assertEquals(2, tail.size());
        assertTrue(tail.contains(4));
        assertTrue(tail.contains(5));
        assertFalse(tail.contains(3));
        
        NavigableSet<Integer> tailInclusive = set.tailSet(3, true);
        assertEquals(3, tailInclusive.size());
        assertTrue(tailInclusive.contains(3));
        
        SortedSet<Integer> tailDefault = set.tailSet(3);
        assertEquals(3, tailDefault.size());
        assertTrue(tailDefault.contains(3));
    }

    @Test
    public void testHeadSetEmpty()
    {
        InsertionOrderedNavigableSet<Integer> set = new InsertionOrderedNavigableSet<>(naturalOrder);
        NavigableSet<Integer> head = set.headSet(3, true);
        assertTrue(head.isEmpty());
    }

    @Test
    public void testTailSetEmpty()
    {
        InsertionOrderedNavigableSet<Integer> set = new InsertionOrderedNavigableSet<>(naturalOrder);
        NavigableSet<Integer> tail = set.tailSet(3, true);
        assertTrue(tail.isEmpty());
    }

    @Test(expected = NullPointerException.class)
    public void testHeadSetNull()
    {
        InsertionOrderedNavigableSet<Integer> set = new InsertionOrderedNavigableSet<>(naturalOrder);
        set.headSet(null, true);
    }

    @Test(expected = NullPointerException.class)
    public void testTailSetNull()
    {
        InsertionOrderedNavigableSet<Integer> set = new InsertionOrderedNavigableSet<>(naturalOrder);
        set.tailSet(null, true);
    }

    @Test
    public void testEqualsAndHashCode()
    {
        InsertionOrderedNavigableSet<Integer> set1 = new InsertionOrderedNavigableSet<>(naturalOrder);
        InsertionOrderedNavigableSet<Integer> set2 = new InsertionOrderedNavigableSet<>(naturalOrder);
        
        assertEquals(set1, set2);
        assertEquals(set1.hashCode(), set2.hashCode());
        
        set1.add(1);
        set1.add(2);
        
        set2.add(1);
        set2.add(2);
        
        assertEquals(set1, set2);
        assertEquals(set1.hashCode(), set2.hashCode());
        
        set2.add(3);
        assertNotEquals(set1, set2);
    }

    @Test
    public void testEqualsWithDifferentComparator()
    {
        InsertionOrderedNavigableSet<Integer> set1 = new InsertionOrderedNavigableSet<>(naturalOrder);
        InsertionOrderedNavigableSet<Integer> set2 = new InsertionOrderedNavigableSet<>(Integer::compareTo);
        
        // Even though comparators are functionally equivalent, they're different objects
        assertNotEquals(set1, set2);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testLower()
    {
        InsertionOrderedNavigableSet<Integer> set = new InsertionOrderedNavigableSet<>(naturalOrder);
        set.lower(1);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testFloor()
    {
        InsertionOrderedNavigableSet<Integer> set = new InsertionOrderedNavigableSet<>(naturalOrder);
        set.floor(1);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testCeiling()
    {
        InsertionOrderedNavigableSet<Integer> set = new InsertionOrderedNavigableSet<>(naturalOrder);
        set.ceiling(1);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testHigher()
    {
        InsertionOrderedNavigableSet<Integer> set = new InsertionOrderedNavigableSet<>(naturalOrder);
        set.higher(1);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testDescendingSet()
    {
        InsertionOrderedNavigableSet<Integer> set = new InsertionOrderedNavigableSet<>(naturalOrder);
        set.descendingSet();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testDescendingIterator()
    {
        InsertionOrderedNavigableSet<Integer> set = new InsertionOrderedNavigableSet<>(naturalOrder);
        set.descendingIterator();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSubSetWithBooleans()
    {
        InsertionOrderedNavigableSet<Integer> set = new InsertionOrderedNavigableSet<>(naturalOrder);
        set.subSet(1, true, 3, true);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSubSet()
    {
        InsertionOrderedNavigableSet<Integer> set = new InsertionOrderedNavigableSet<>(naturalOrder);
        set.subSet(1, 3);
    }

    @Test
    public void testCustomComparator()
    {
        Comparator<String> lengthComparator = Comparator.comparing(String::length);
        InsertionOrderedNavigableSet<String> set = new InsertionOrderedNavigableSet<>(lengthComparator);
        
        set.add("a");
        set.add("bb");
        set.add("ccc");
        
        assertEquals("a", set.first());
        assertEquals("ccc", set.last());
        assertEquals(lengthComparator, set.comparator());
    }
}