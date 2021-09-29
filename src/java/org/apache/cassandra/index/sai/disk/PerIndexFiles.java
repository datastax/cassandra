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

package org.apache.cassandra.index.sai.disk;

import java.io.Closeable;
import java.util.Map;

import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;

/**
 * This class is a container for the per-index files used during a query.
 */
public abstract class PerIndexFiles implements Closeable
{
    protected final IndexDescriptor indexDescriptor;
    protected final IndexContext indexContext;
    protected final Map<IndexComponent, FileHandle> files;
    protected final boolean temporary;

    public PerIndexFiles(IndexDescriptor indexDescriptor, IndexContext indexContext, boolean temporary)
    {
        this.indexDescriptor = indexDescriptor;
        this.indexContext = indexContext;
        files = populate(indexDescriptor, indexContext, temporary);
        this.temporary = temporary;
    }

    public FileHandle get(IndexComponent indexComponent)
    {
        FileHandle file = files.get(indexComponent);
        if (file == null)
            throw new IllegalArgumentException(String.format(indexContext.logMessage("Component for %s not found for SSTable %s"),
                                                             indexComponent.representation,
                                                             indexDescriptor.descriptor));

        return file;
    }

    protected abstract Map<IndexComponent, FileHandle> populate(IndexDescriptor indexDescriptor, IndexContext indexContext, boolean temporary);

    @Override
    public void close()
    {
        FileUtils.closeQuietly(files.values());
    }
}
