/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.cassandra.index;

import java.util.Collections;
import java.util.Set;
import java.util.function.Predicate;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.WriteContext;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.transactions.IndexTransaction;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableFlushObserver;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.TableMetadata;

/**
 * An {@link Index.Group} containing a single {@link Index}, to which it just delegates the calls.
 */
public class SingletonIndexGroup implements Index.Group
{
    private volatile Index delegate;
    private final Set<Index> indexes = Sets.newConcurrentHashSet();

    protected SingletonIndexGroup()
    {
    }

    @Override
    public Set<Index> getIndexes()
    {
        return indexes;
    }

    public Index getIndex()
    {
        return delegate;
    }

    @Override
    public void addIndex(Index index)
    {
        Preconditions.checkState(delegate == null);
        // This class does not work for SAI because the `componentsForNewBuid` method would be incorrect (so more
        // generally, it does not work for indexes that use dedicated sstable components, which is only SAI). See
        // comments on `componentsForNewBuid` for more details.
        Preconditions.checkState(!(index instanceof StorageAttachedIndex), "This shoudl not be used with SAI");
        delegate = index;
        indexes.add(index);
    }

    @Override
    public void removeIndex(Index index)
    {
        Preconditions.checkState(containsIndex(index));
        delegate = null;
        indexes.clear();
    }

    @Override
    public boolean containsIndex(Index index)
    {
        return index.equals(delegate);
    }

    @Override
    public Index.Indexer indexerFor(Predicate<Index> indexSelector,
                                    DecoratedKey key,
                                    RegularAndStaticColumns columns,
                                    int nowInSec,
                                    WriteContext ctx,
                                    IndexTransaction.Type transactionType,
                                    Memtable memtable)
    {
        Preconditions.checkNotNull(delegate);
        return indexSelector.test(delegate) ? delegate.indexerFor(key, columns, nowInSec, ctx, transactionType, memtable) : null;
    }

    @Override
    public Index.QueryPlan queryPlanFor(RowFilter rowFilter)
    {
        Preconditions.checkNotNull(delegate);

        // Indexes using a singleton group don't support disjunctions,
        // so we only consider the top-level AND expressions for index selection.
        for (RowFilter.Expression e : rowFilter.withoutDisjunctions().expressions())
        {
            if (delegate.supportsExpression(e.column(), e.operator()))
                return new SingletonIndexQueryPlan(delegate, delegate.getPostIndexQueryFilter(rowFilter));
        }

        return null;
    }

    @Override
    public SSTableFlushObserver getFlushObserver(Descriptor descriptor, LifecycleNewTracker tracker, TableMetadata tableMetadata, long keyCount)
    {
        Preconditions.checkNotNull(delegate);
        return delegate.getFlushObserver(descriptor, tracker);
    }

    @Override
    public Set<Component> componentsForNewSSTable()
    {
        // This class is only used for indexes that don't use per-sstable components, aka not-SAI (note that SASI uses
        // some "file" per sstable, but it is not a `Component` in practice). We could add an equivalent
        // `componentsForNewSSTable` method in `Index`, so that we can call `delegate.componentsForNewBuild` here, but
        // this would kind of weird for SAI because of the per-sstable components: should they be returned by such
        // method on `Index` or not? Tldr, for SAI, it's cleaner to deal with components created at the group level,
        // which is what `StorageAttachedIndexGroup.componentsForNewBuild` does, and it's simpler to always use
        // `StorageAttachedIndexGroup` for SAI, which is the case. So at that point, adding an
        // `Index.componentsForNewSSTable` method would just be dead code, so let's avoid it.
        return Collections.emptySet();
    }

    @Override
    public Set<Component> activeComponents(SSTableReader sstable)
    {
        // Same rermarks as for `componentsForNewBuid`.
        return Collections.emptySet();
    }
}
