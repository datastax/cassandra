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

package org.apache.cassandra.index.sai.disk.v7;

import java.util.EnumSet;
import java.util.Set;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.index.sai.disk.v3.V3OnDiskFormat;
import org.apache.cassandra.index.sai.disk.v6.V6OnDiskFormat;
import org.apache.cassandra.index.sai.utils.TypeUtil;

public class V7OnDiskFormat extends V6OnDiskFormat
{
    public static final V7OnDiskFormat instance = new V7OnDiskFormat();

    private static final Set<IndexComponentType> PER_SSTABLE_COMPONENTS = EnumSet.of(IndexComponentType.GROUP_COMPLETION_MARKER,
                                                                                     IndexComponentType.GROUP_META,
                                                                                     IndexComponentType.TOKEN_VALUES,
                                                                                     IndexComponentType.PRIMARY_KEY_TRIE,
                                                                                     IndexComponentType.PRIMARY_KEY_TRIE_COMPRESSION_INFO,
                                                                                     IndexComponentType.PRIMARY_KEY_BLOCKS,
                                                                                     IndexComponentType.PRIMARY_KEY_BLOCKS_COMPRESSION_INFO,
                                                                                     IndexComponentType.PRIMARY_KEY_BLOCK_OFFSETS);


    public static final Set<IndexComponentType> NUMERIC_COMPONENTS = EnumSet.of(IndexComponentType.COLUMN_COMPLETION_MARKER,
                                                                                IndexComponentType.META,
                                                                                IndexComponentType.KD_TREE,
                                                                                IndexComponentType.KD_TREE_COMPRESSION_INFO,
                                                                                IndexComponentType.KD_TREE_POSTING_LISTS,
                                                                                IndexComponentType.KD_TREE_POSTING_LISTS_COMPRESSION_INFO);


    private static final Set<IndexComponentType> LITERAL_COMPONENTS = EnumSet.of(IndexComponentType.COLUMN_COMPLETION_MARKER,
                                                                                 IndexComponentType.META,
                                                                                 IndexComponentType.TERMS_DATA,
                                                                                 IndexComponentType.TERMS_DATA_COMPRESSION_INFO,
                                                                                 IndexComponentType.POSTING_LISTS,
                                                                                 IndexComponentType.POSTING_LISTS_COMPRESSION_INFO,
                                                                                 IndexComponentType.DOC_LENGTHS);

    private static final Set<IndexComponentType> COMPRESSION_INFO_COMPONENTS = EnumSet.of(IndexComponentType.POSTING_LISTS_COMPRESSION_INFO,
                                                                                          IndexComponentType.TERMS_DATA_COMPRESSION_INFO,
                                                                                          IndexComponentType.KD_TREE_COMPRESSION_INFO,
                                                                                          IndexComponentType.KD_TREE_POSTING_LISTS_COMPRESSION_INFO,
                                                                                          IndexComponentType.PRIMARY_KEY_TRIE_COMPRESSION_INFO,
                                                                                          IndexComponentType.PRIMARY_KEY_BLOCKS_COMPRESSION_INFO);


    @Override
    public Set<IndexComponentType> perIndexComponentTypes(AbstractType<?> validator)
    {
        if (TypeUtil.isLiteral(validator))
            return LITERAL_COMPONENTS;
        return super.perIndexComponentTypes(validator);
    }


    @Override
    public Set<IndexComponentType> perSSTableComponentTypes()
    {
        return PER_SSTABLE_COMPONENTS;
    }

    /**
     * Returns the components storing compression info about compression of other index components.
     * Compression information components are not present on disk if the index component they describe is not compressed.
     */
    @Override
    public Set<IndexComponentType> compressionInfoComponentTypes()
    {
        return COMPRESSION_INFO_COMPONENTS;
    }
}
