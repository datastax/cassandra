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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SSTableContext;
import org.apache.cassandra.index.sai.SSTableIndex;
import org.apache.cassandra.index.sai.StorageAttachedIndexGroup;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.Pair;

/**
 * Maintain a atomic view for read requests, so that requests can read all data during concurrent compactions.
 *
 * All per-column {@link SSTableIndex} updates should be proxied by {@link StorageAttachedIndexGroup} to make
 * sure per-sstable {@link SSTableContext} are in-sync.
 */
public class IndexViewManager
{
    private static final Logger logger = LoggerFactory.getLogger(IndexViewManager.class);
    
    private final IndexContext context;
    private final AtomicReference<View> viewRef = new AtomicReference<>();

    public IndexViewManager(IndexContext context)
    {
        this(context, Collections.emptySet(), Collections.emptySet());
    }

    @VisibleForTesting
    IndexViewManager(IndexContext context, Collection<Descriptor> sstables, Collection<SSTableIndex> indices)
    {
        this.context = context;
        this.viewRef.set(new View(context, sstables, indices));
        // View references the indices, so we release them here.
        indices.forEach(SSTableIndex::release);
    }

    public View getView()
    {
        return viewRef.get();
    }

    /**
     * Replaces old SSTables with new by creating new immutable view.
     *
     * @param oldSSTables A set of SSTables to remove.
     * @param newSSTables A set of SSTables added in Cassandra.
     * @param newSSTableContexts A set of SSTableContexts to add to tracker.
     * @param validate if true, per-column index files' header and footer will be validated.
     *
     * @return A set of SSTables which have attached to them invalid index components.
     */
    public Set<SSTableContext> update(Collection<SSTableReader> oldSSTables,
                                      Collection<SSTableReader> newSSTables,
                                      Collection<SSTableContext> newSSTableContexts,
                                      boolean validate)
    {
        // Valid indexes on the left and invalid SSTable contexts on the right...
        // The valid indexes are referenced as a part of object initialization.
        Pair<Set<SSTableIndex>, Set<SSTableContext>> indexes = context.getBuiltIndexes(newSSTableContexts, validate);

        View currentView, newView = null;
        Map<Descriptor, SSTableIndex> newViewIndexes = new HashMap<>();
        Collection<SSTableReader> toRemove = new HashSet<>(oldSSTables);

        do
        {
            currentView = viewRef.get();
            if (!currentView.reference())
                continue;
            if (newView != null)
                newView.release();
            newViewIndexes.clear();

            Set<Descriptor> sstables = new HashSet<>(currentView.getSSTables());
            for (SSTableReader sstable : oldSSTables)
                sstables.remove(sstable.descriptor);
            for (SSTableReader sstable : newSSTables)
                sstables.add(sstable.descriptor);

            for (SSTableIndex sstableIndex : currentView)
            {
                // When aborting early open transaction, toRemove may have the same sstable files as newSSTableContexts,
                // but different SSTableReader java objects with different start positions. So we need to release them
                // from existing view.  see DSP-19677
                SSTableReader sstable = sstableIndex.getSSTable();
                if (!toRemove.contains(sstable))
                    addOrUpdateSSTableIndex(sstableIndex, newViewIndexes);
            }

            for (SSTableIndex sstableIndex : indexes.left)
                addOrUpdateSSTableIndex(sstableIndex, newViewIndexes);

            newView = new View(context, sstables, newViewIndexes.values());
            currentView.release();
        } while (newView == null || !viewRef.compareAndSet(currentView, newView));

        // newView referenced these for a second time in this method, so we call release once here.
        indexes.left.forEach(SSTableIndex::release);
        currentView.release();

        if (logger.isTraceEnabled())
            logger.trace(context.logMessage("There are now {} active SSTable indexes."), viewRef.get().getIndexes().size());

        return indexes.right;
    }

    private static void addOrUpdateSSTableIndex(SSTableIndex ssTableIndex, Map<Descriptor, SSTableIndex> addTo)
    {
        var descriptor = ssTableIndex.getSSTable().descriptor;
        SSTableIndex previous = addTo.get(descriptor);
        if (previous != null)
        {
            // If the new index use the same files that the exiting one (and the previous one is still complete, meaning
            // that the files weren't corrupted), then keep the old one (no point in changing for the same thing).
            if (previous.usedPerIndexComponents().isComplete() && ssTableIndex.usedPerIndexComponents().buildId().equals(previous.usedPerIndexComponents().buildId()))
                return;
        }
        addTo.put(descriptor, ssTableIndex);
    }

    public void prepareSSTablesForRebuild(Collection<SSTableReader> sstablesToRebuild)
    {
        Set<SSTableReader> toRemove = new HashSet<>(sstablesToRebuild);
        View oldView, newView = null;
        Set<SSTableIndex> indexesToRemove;
        do
        {
            oldView = viewRef.get();
            if (!oldView.reference())
                continue;
            if (newView != null)
                newView.release();

            indexesToRemove = oldView.getIndexes()
                                     .stream()
                                     .filter(index -> toRemove.contains(index.getSSTable()))
                                     .collect(Collectors.toSet());
            var newIndexes = new HashSet<>(oldView.getIndexes());
            newIndexes.removeAll(indexesToRemove);
            // TODO why are we storing more sstables than indexes here? Are we non-queryable until we get the
            // count back up?
            newView = new View(context, oldView.getSSTables(), newIndexes);
            oldView.release();
        }
        while (newView == null || !viewRef.compareAndSet(oldView, newView));
        oldView.release();
    }

    /**
     * Called when index is dropped. Mark all {@link SSTableIndex} as released and per-column index files
     * will be removed when in-flight queries completed and {@code obsolete} is true.
     *
     * @param indexWasDropped true if the index is invalidated because it was dropped; false if the index is simply
     *                        being unloaded.
     */
    public void invalidate(boolean indexWasDropped)
    {
        View oldView, newView = null;
        do
        {
            // We skip referencing becuase we do not use its indexes.
            oldView = viewRef.get();
            if (newView != null)
                newView.release();
            // TODO are we non queryable at this point?
            newView = new View(context, oldView.getSSTables(), Collections.emptySet());
        } while (!viewRef.compareAndSet(oldView, newView));

        if (indexWasDropped)
            oldView.markIndexWasDropped();
        else
            oldView.release();
    }
}
