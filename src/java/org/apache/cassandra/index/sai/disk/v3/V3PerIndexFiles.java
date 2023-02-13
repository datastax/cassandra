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

package org.apache.cassandra.index.sai.disk.v3;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.PerIndexFiles;
import org.apache.cassandra.io.util.FileHandle;

@NotThreadSafe
public class V3PerIndexFiles extends PerIndexFiles
{
    public V3PerIndexFiles(IndexDescriptor indexDescriptor, IndexContext indexContext, boolean temporary)
    {
        super(indexDescriptor, indexContext, temporary);
    }

    public FileHandle getFileAndCache(IndexComponent indexComponent)
    {
        return files.computeIfAbsent(indexComponent, comp -> indexDescriptor.createPerIndexFileHandle(indexComponent, indexContext, temporary));
    }

    @Override
    public FileHandle getFile(IndexComponent indexComponent)
    {
        return indexDescriptor.createPerIndexFileHandle(indexComponent, indexContext, temporary);
    }
}
