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

package org.apache.cassandra.index.sai.disk.v2;

import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.disk.PerIndexFiles;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.utils.IndexFileUtils;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.lucene.store.IndexInput;

public class V2PerIndexFiles extends PerIndexFiles
{
    public V2PerIndexFiles(IndexDescriptor indexDescriptor, IndexContext indexContext)
    {
        this(indexDescriptor, indexContext, false);
    }

    public V2PerIndexFiles(IndexDescriptor indexDescriptor, IndexContext indexContext, boolean temporary)
    {
        super(indexDescriptor, indexContext, temporary);
    }

    @Override
    protected Map<IndexComponent, FileHandle> populate(IndexDescriptor indexDescriptor, IndexContext columnContext, boolean temporary)
    {
        return new HashMap<>();
    }

    public IndexInput openInput(IndexComponent indexComponent)
    {
        FileHandle fileHandle = get(indexComponent);
        return IndexFileUtils.instance.openInput(fileHandle.sharedCopy());
    }

    @Override
    public FileHandle get(IndexComponent indexComponent)
    {
        return files.computeIfAbsent(indexComponent,
                                                (indexComponent1 ->
                                                 indexDescriptor.createPerIndexFileHandle(indexComponent,
                                                                                          indexContext,
                                                                                          temporary)));
    }

    @Override
    public void close()
    {
        FileUtils.closeQuietly(files.values());
    }
}
