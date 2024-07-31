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

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileOutputStreamPlus;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.config.CassandraRelevantProperties.CUSTOM_SSTABLE_WATCHER;
import static org.apache.cassandra.io.util.File.WriteMode.APPEND;

/**
 * Watcher used when opening sstables to discover extra components, eg. archive component
 */
public interface SSTableWatcher
{
    SSTableWatcher instance = !CUSTOM_SSTABLE_WATCHER.isPresent()
                               ? new SSTableWatcher() {}
                               : FBUtilities.construct(CUSTOM_SSTABLE_WATCHER.getString(), "sstable watcher");

    static final Logger logger = LoggerFactory.getLogger(SSTableWatcher.class);

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
     * Appends new component names to the TOC component.
     */
    default void appendTOC(Descriptor descriptor, Collection<Component> components)
    {
        File tocFile = descriptor.fileFor(Component.TOC);
        writeTOC(tocFile, components, APPEND);
    }

    /**
     * Write TOC file with given components and write mode
     */
    default void writeTOC(File tocFile, Collection<Component> components, File.WriteMode writeMode)
    {
        FileOutputStreamPlus fos = null;
        try (PrintWriter w = new PrintWriter((fos = tocFile.newOutputStream(writeMode))))
        {
            for (Component component : components)
                w.println(component.name);
            w.flush();
            fos.sync();
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, tocFile);
        }
    }

    /**
     * Rewrite TOC components with given components. The default implementation deletes the existing file and append new components
     */
    default void rewriteTOC(Descriptor descriptor, Collection<Component> components)
    {
        File tocFile = descriptor.fileFor(Component.TOC);
        if (!tocFile.tryDelete())
            logger.error("Failed to delete TOC component for " + descriptor);
        appendTOC(descriptor, components);
    }
}
