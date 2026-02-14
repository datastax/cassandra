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

package org.apache.cassandra.index.sai.disk.v1;

import java.io.Closeable;
import java.io.UncheckedIOException;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Map;

import org.slf4j.Logger;

import org.apache.cassandra.index.sai.disk.format.IndexComponents;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;

public class PerIndexFiles implements Closeable
{
    private static final Logger logger = org.slf4j.LoggerFactory.getLogger(PerIndexFiles.class);

    private final Map<IndexComponentType, FileHandle> files = new EnumMap<>(IndexComponentType.class);
    private final IndexComponents.ForRead perIndexComponents;

    public PerIndexFiles(IndexComponents.ForRead perIndexComponents)
    {
        this.perIndexComponents = perIndexComponents;

        var toOpen = new HashSet<>(perIndexComponents.expectedComponentsForVersion());
        toOpen.remove(IndexComponentType.META);
        toOpen.remove(IndexComponentType.COLUMN_COMPLETION_MARKER);

        var componentsPresent = new HashSet<IndexComponentType>();
        for (IndexComponentType component : toOpen)
        {
            try
            {
                files.put(component, perIndexComponents.get(component).createFileHandle());
                componentsPresent.add(component);
            }
            catch (UncheckedIOException e)
            {
                // leave logging until we're done
            }
        }

        logger.info("Components present for {} are {}", perIndexComponents.indexDescriptor(), componentsPresent);
    }

    public IndexComponents.ForRead usedPerIndexComponents()
    {
        return perIndexComponents;
    }

    /** It is the caller's responsibility to close the returned file handle. */
    public FileHandle termsData()
    {
        return getFile(IndexComponentType.TERMS_DATA).sharedCopy();
    }

    /** It is the caller's responsibility to close the returned file handle. */
    public FileHandle postingLists()
    {
        return getFile(IndexComponentType.POSTING_LISTS).sharedCopy();
    }

    /** It is the caller's responsibility to close the returned file handle. */
    public FileHandle kdtree()
    {
        return getFile(IndexComponentType.KD_TREE).sharedCopy();
    }

    /** It is the caller's responsibility to close the returned file handle. */
    public FileHandle kdtreePostingLists()
    {
        return getFile(IndexComponentType.KD_TREE_POSTING_LISTS).sharedCopy();
    }

    /** It is the caller's responsibility to close the returned file handle. */
    public FileHandle vectors()
    {
        return getFile(IndexComponentType.VECTOR).sharedCopy();
    }

    /** It is the caller's responsibility to close the returned file handle. */
    public FileHandle pq()
    {
        return getFile(IndexComponentType.PQ).sharedCopy();
    }

    public FileHandle getFile(IndexComponentType indexComponentType)
    {
        FileHandle file = files.get(indexComponentType);
        if (file == null)
            throw new IllegalArgumentException(String.format(perIndexComponents.logMessage("Component %s not found for SSTable %s"),
                                                             indexComponentType,
                                                             perIndexComponents.descriptor()));

        return file;
    }

    @Override
    public void close()
    {
        FileUtils.closeQuietly(files.values());
    }
}
