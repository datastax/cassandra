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

import java.io.IOException;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.io.IndexOutputWriter;
import org.apache.cassandra.index.sai.disk.v2.blockindex.BlockIndexFileProvider;
import org.apache.cassandra.index.sai.disk.v2.blockindex.FileValidator;
import org.apache.cassandra.index.sai.utils.IndexFileUtils;
import org.apache.cassandra.index.sai.utils.SharedIndexInput;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.lucene.store.IndexInput;

import static org.apache.cassandra.index.sai.disk.format.IndexComponent.COMPRESSED_TERMS_DATA;
import static org.apache.cassandra.index.sai.disk.format.IndexComponent.GROUP_META;
import static org.apache.cassandra.index.sai.disk.format.IndexComponent.KD_TREE_POSTING_LISTS;
import static org.apache.cassandra.index.sai.disk.format.IndexComponent.ORDER_MAP;
import static org.apache.cassandra.index.sai.disk.format.IndexComponent.POSTING_LISTS;
import static org.apache.cassandra.index.sai.disk.format.IndexComponent.ROW_ID_POINT_ID_MAP;
import static org.apache.cassandra.index.sai.disk.format.IndexComponent.TERMS_DATA;
import static org.apache.cassandra.index.sai.disk.format.IndexComponent.TERMS_INDEX;

public class PerSSTableFileProvider implements BlockIndexFileProvider
{
    private static final Set<IndexComponent> components = EnumSet.of(TERMS_DATA, TERMS_INDEX, POSTING_LISTS, KD_TREE_POSTING_LISTS);

    private final IndexDescriptor indexDescriptor;

    protected final Map<IndexComponent, FileHandle> files = new EnumMap<>(IndexComponent.class);
    protected final Map<IndexComponent, FileHandle> tempFiles = new EnumMap<>(IndexComponent.class);

    public PerSSTableFileProvider(IndexDescriptor indexDescriptor)
    {
        this.indexDescriptor = indexDescriptor;
    }

    @Override
    public IndexOutputWriter openValuesOutput(boolean temporary) throws IOException
    {
        return indexDescriptor.openPerSSTableOutput(TERMS_DATA, true, temporary);
    }

    @Override
    public IndexOutputWriter openIndexOutput(boolean temporary) throws IOException
    {
        return indexDescriptor.openPerSSTableOutput(TERMS_INDEX, true, temporary);
    }

    @Override
    public IndexOutputWriter openLeafPostingsOutput(boolean temporary) throws IOException
    {
        return indexDescriptor.openPerSSTableOutput(POSTING_LISTS, true, temporary);
    }

    @Override
    public IndexOutputWriter openOrderMapOutput(boolean temporary) throws IOException
    {
        return indexDescriptor.openPerSSTableOutput(ORDER_MAP, true, temporary);
    }

    @Override
    public IndexOutputWriter openCompressedValuesOutput(boolean temporary) throws IOException
    {
        return indexDescriptor.openPerSSTableOutput(COMPRESSED_TERMS_DATA, true, temporary);
    }

    @Override
    public IndexOutputWriter openMultiPostingsOutput(boolean temporary) throws IOException
    {
        return indexDescriptor.openPerSSTableOutput(KD_TREE_POSTING_LISTS, true, temporary);
    }

    @Override
    public IndexOutputWriter openPointIdMapOutput(boolean temporary) throws IOException
    {
        return indexDescriptor.openPerSSTableOutput(ROW_ID_POINT_ID_MAP, true, temporary);
    }

    @Override
    public SharedIndexInput openPointIdMapInput(boolean temporary) throws IOException
    {
        return new SharedIndexInput(openInput(ROW_ID_POINT_ID_MAP, temporary));
    }

    @Override
    public IndexOutputWriter openMetadataOutput() throws IOException
    {
        return indexDescriptor.openPerSSTableOutput(GROUP_META);
    }

    @Override
    public SharedIndexInput openValuesInput(boolean temporary)
    {
        return new SharedIndexInput(openInput(TERMS_DATA, temporary));
    }

    @Override
    public SharedIndexInput openOrderMapInput(boolean temporary)
    {
        return new SharedIndexInput(openInput(ORDER_MAP, temporary));
    }

    @Override
    public SharedIndexInput openLeafPostingsInput(boolean temporary)
    {
        return new SharedIndexInput(openInput(POSTING_LISTS, temporary));
    }

    @Override
    public SharedIndexInput openIndexInput(boolean temporary)
    {
        return new SharedIndexInput(openInput(TERMS_INDEX, temporary));
    }

    @Override
    public SharedIndexInput openMultiPostingsInput(boolean temporary)
    {
        return new SharedIndexInput(openInput(KD_TREE_POSTING_LISTS, temporary));
    }

    @Override
    public SharedIndexInput openCompressedValuesInput(boolean temporary)
    {
        return new SharedIndexInput(openInput(COMPRESSED_TERMS_DATA, temporary));
    }

    @Override
    public SharedIndexInput openMetadataInput()
    {
        return new SharedIndexInput(openInput(GROUP_META, false));
    }

    @Override
    public FileHandle getIndexFileHandle(boolean temporary)
    {
        return getFileHandle(TERMS_INDEX, temporary);
    }

    @Override
    public HashMap<IndexComponent, FileValidator.FileInfo> fileInfoMap() throws IOException
    {
        final HashMap<IndexComponent, FileValidator.FileInfo> map = new HashMap<>();

        for (IndexComponent indexComponent : components)
        {
            try (IndexInput input = openInput(indexComponent, false))
            {
                map.put(indexComponent, FileValidator.generate(input));
            }
        }

        return map;
    }

    @Override
    public void validate(Map<IndexComponent, FileValidator.FileInfo> fileInfoMap) throws IOException
    {
//        for (Map.Entry<IndexComponent,FileValidator.FileInfo> entry : fileInfoMap.entrySet())
//        {
//            FileValidator.FileInfo fileInfo = FileValidator.generate(openInput(entry.getKey(), false));
//            if (!fileInfo.equals(entry.getValue()))
//                throw new IOException("CRC check on component "+entry.getKey()+" failed.");
//        }
    }

    @Override
    public void close()
    {
        FileUtils.closeQuietly(tempFiles.values());
        FileUtils.closeQuietly(files.values());
    }

    private IndexInput openInput(IndexComponent indexComponent, boolean temporary)
    {
        return IndexFileUtils.instance.openInput(getFileHandle(indexComponent, temporary));
    }

    private FileHandle getFileHandle(IndexComponent indexComponent, boolean temporary)
    {
        return temporary ? tempFiles.computeIfAbsent(indexComponent,
                                                     c -> indexDescriptor.createPerSSTableFileHandle(c, true))
                         : files.computeIfAbsent(indexComponent,
                                                 c -> indexDescriptor.createPerSSTableFileHandle(c, false));
    }
}
