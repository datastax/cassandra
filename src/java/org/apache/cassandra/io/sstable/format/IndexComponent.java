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

package org.apache.cassandra.io.sstable.format;

import org.apache.cassandra.cache.ChunkCache;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.IOOptions;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.storage.StorageProvider;
import org.apache.cassandra.io.util.FileHandle;

public class IndexComponent
{
    public static FileHandle.Builder fileBuilder(Descriptor descriptor, Component component, IOOptions ioOptions, ChunkCache chunkCache)
    {
        return StorageProvider.instance.primaryIndexWriteTimeFileHandleBuilderFor(descriptor, component, ioOptions.indexDiskAccessMode, chunkCache, OperationType.UNKNOWN);
    }

    public static FileHandle.Builder fileBuilder(Component component, SSTable ssTable)
    {
        return fileBuilder(ssTable.descriptor, component, ssTable.ioOptions, ssTable.chunkCache);
    }

    public static FileHandle.Builder fileBuilder(Component component, SSTable.Builder<?, ?> builder, OperationType operationType)
    {
        return StorageProvider.instance.primaryIndexWriteTimeFileHandleBuilderFor(builder.descriptor, component, builder.getIOOptions().indexDiskAccessMode, builder.getChunkCache(), operationType);
    }
}