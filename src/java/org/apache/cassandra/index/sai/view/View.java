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

import javax.annotation.Nullable;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SSTableIndex;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.Interval;
import org.apache.cassandra.utils.IntervalTree;

public class View implements Iterable<SSTableIndex>
{
    private final Set<Descriptor> sstables;
    private final Map<Descriptor, SSTableIndex> view;

    private final TermTree termTree;
    private final AbstractType<?> keyValidator;
    private final IntervalTree<Key, SSTableIndex, Interval<Key, SSTableIndex>> keyIntervalTree;

    public View(IndexContext context, Collection<Descriptor> sstables, Collection<SSTableIndex> indexes)
    {
        this.view = new HashMap<>();
        this.sstables = new HashSet<>(sstables);
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

    public Collection<Descriptor> getSSTables()
    {
        return sstables;
    }

    public Collection<SSTableIndex> getIndexes()
    {
        return view.values();
    }

    public int size()
    {
        return view.size();
    }

    public @Nullable SSTableIndex getSSTableIndex(Descriptor descriptor)
    {
        return view.get(descriptor);
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
     * Returns true if this view has been based on the Cassandra view containing given sstable.
     * In other words, it tells if SAI was given a chance to load the index for the given sstable.
     * It does not determine if the index exists and was actually loaded.
     * To check the existence of the index, use {@link #containsSSTableIndex(Descriptor)}.
     * <p>
     * This method allows to distinguish a situation when the sstable has no index, the index is
     * invalid, or was not loaded for whatever reason,
     * from a situation where the view hasn't been updated yet to reflect the newly added sstable.
     */
    public boolean isAwareOfSSTable(Descriptor descriptor)
    {
        return sstables.contains(descriptor);
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
