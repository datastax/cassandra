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

package org.apache.cassandra.io.sstable;

import java.util.Set;

import org.apache.cassandra.index.sai.disk.format.IndexComponents;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.config.CassandraRelevantProperties.CUSTOM_SSTABLE_WATCHER;

/**
 * Watcher used when opening sstables to discover extra components, eg. archive component
 */
public interface SSTableWatcher
{
    SSTableWatcher instance = !CUSTOM_SSTABLE_WATCHER.isPresent()
                               ? new SSTableWatcher() {}
                               : FBUtilities.construct(CUSTOM_SSTABLE_WATCHER.getString(), "sstable watcher");

    /**
     * Discover extra components before reading TOC file
     *
     * @param descriptor sstable descriptor for current sstable
     */
    default void discoverComponents(Descriptor descriptor)
    {
    }

    /**
     * Discover extra components before opening sstable
     *
     * @param descriptor sstable descriptor for current sstable
     * @param existing existing sstable components
     * @return all discovered sstable components
     */
    default Set<Component> discoverComponents(Descriptor descriptor, Set<Component> existing)
    {
        return existing;
    }

    /**
     * Called before executing index build on existing sstable
     */
    default void onIndexBuild(SSTableReader sstable)
    {
    }

    /**
     * Called when an index is dropped on index components affected by that drop.
     * <p>
     * By default, this method simply deletes the components locally, but it can overriden if different/additional
     * behavior is needed.
     *
     * @param components index components that are no longer in used due to an index drop. Note that this can
     *                   be either per-index components (for the components of the exact index being dropped),
     *                   or per-sstable components if the index dropped was the only index for the table and the
     *                   per-sstable components are no longer needed. More precisely, if the last index of a table
     *                   is dropped, then this method will usually be called twice per sstable, once for the index
     *                   components, and once for the per-sstable components.
     */
    default void onIndexDropped(IndexComponents.ForWrite components)
    {
        components.forceDeleteAllComponents();
    }
}
