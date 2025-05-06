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

package org.apache.cassandra.index.sai.view;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SSTableIndex;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.utils.Interval;
import org.apache.cassandra.utils.IntervalTree;

public class View implements Iterable<SSTableIndex>
{
    private final Map<Descriptor, SSTableIndex> view;
    private final AtomicInteger references = new AtomicInteger(1);
    private volatile boolean indexWasDropped;

    private final TermTree termTree;
    private final AbstractType<?> keyValidator;
    private final IntervalTree<Key, SSTableIndex, Interval<Key, SSTableIndex>> keyIntervalTree;

    /**
     * Construct a threadsafe view.
     * @param context the index context
     * @param indexes the indexes. Note that the referencing logic for these indexes is handled
     *                outside of this constructor and all indexes are assumed to have been referenced already.
     *                The view will release the indexes when it is finally released.
     */
    public View(IndexContext context, Collection<SSTableIndex> indexes)
    {
        this.view = new HashMap<>();
        this.keyValidator = context.keyValidator();

        AbstractType<?> validator = context.getValidator();

        TermTree.Builder termTreeBuilder = new RangeTermTree.Builder(validator);

        List<Interval<Key, SSTableIndex>> keyIntervals = new ArrayList<>();
        for (SSTableIndex sstableIndex : indexes)
        {
            this.view.put(sstableIndex.getSSTable().descriptor, sstableIndex);
            if (!sstableIndex.getIndexContext().isVector())
                termTreeBuilder.add(sstableIndex);

            keyIntervals.add(Interval.create(new Key(sstableIndex.minKey()),
                                             new Key(sstableIndex.maxKey()),
                                             sstableIndex));
        }

        this.termTree = termTreeBuilder.build();
        this.keyIntervalTree = IntervalTree.build(keyIntervals);
    }

    /**
     * Search for a list of {@link SSTableIndex}es that contain values within
     * the value range requested in the {@link Expression}. Expressions associated with ORDER BY are not
     * expected, and will throw an exception.
     */
    public Set<SSTableIndex> match(Expression expression)
    {
        if (expression.getOp() == Expression.Op.ORDER_BY)
            throw new IllegalArgumentException("ORDER BY expression is not supported");
        if (expression.getOp() == Expression.Op.BOUNDED_ANN || expression.getOp().isNonEquality())
            return new HashSet<>(getIndexes());
        return termTree.search(expression);
    }

    public List<SSTableIndex> match(DecoratedKey minKey, DecoratedKey maxKey)
    {
        return keyIntervalTree.search(Interval.create(new Key(minKey), new Key(maxKey), null));
    }

    public Iterator<SSTableIndex> iterator()
    {
        return view.values().iterator();
    }

    public Collection<SSTableIndex> getIndexes()
    {
        return view.values();
    }

    public boolean reference()
    {
        while (true)
        {
            int n = references.get();
            if (n <= 0)
                return false;
            if (references.compareAndSet(n, n + 1))
            {
                return true;
            }
        }
    }

    public void release()
    {
        int n = references.decrementAndGet();
        if (n == 0)
            if (indexWasDropped)
                view.values().forEach(SSTableIndex::markIndexDropped);
            else
                view.values().forEach(SSTableIndex::release);
    }

    public void markIndexWasDropped()
    {
        // This ordering allows us to guarantee that in flight queries will not be interrupted in problematic ways.
        indexWasDropped = true;
        release();
    }

    public int size()
    {
        return view.size();
    }

    /**
     * Tells if an index for the given sstable exists.
     * It's equivalent to {@code getSSTableIndex(descriptor) != null }.
     * @param descriptor identifies the sstable
     */
    public boolean containsSSTableIndex(Descriptor descriptor)
    {
        return view.containsKey(descriptor);
    }

    /**
     * Tells if the view is aware of the given sstable.
     * @param descriptor identifies the sstable
     */
    public boolean isAwareOfSSTable(Descriptor descriptor)
    {
        return view.containsKey(descriptor);
    }

    /**
     * Get the SSTableIndex for the given sstable descriptor
     * @param descriptor identifies the sstable
     * @return the SSTableIndex or null if not found
     */
    public SSTableIndex getSSTableIndex(Descriptor descriptor)
    {
        return view.get(descriptor);
    }

    /**
     * This is required since IntervalTree doesn't support custom Comparator
     * implementations and relied on items to be comparable which "raw" keys are not.
     */
    private static class Key implements Comparable<Key>
    {
        private final DecoratedKey key;

        public Key(DecoratedKey key)
        {
            this.key = key;
        }

        public int compareTo(Key o)
        {
            if (key == null && o.key == null)
                return 0;
            if (key == null)
                return -1;
            if (o.key == null)
                return 1;
            return key.compareTo(o.key);
        }
    }

    @Override
    public String toString()
    {
        return String.format("View{view=%s, keyValidator=%s, keyIntervalTree=%s}", view, keyValidator, keyIntervalTree);
    }
}
