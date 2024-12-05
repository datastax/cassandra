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

package org.apache.cassandra.index.sai.disk.format;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * This is a definitive list of all the on-disk components for all versions
 */
public enum IndexComponentType
{
    /**
     * Stores per-index metadata.
     *
     * V1
     */
    META("Meta", false),
    /**
     * KDTree written by {@code BKDWriter} indexes mappings of term to one ore more segment row IDs
     * (segment row ID = SSTable row ID - segment row ID offset).
     *
     * V1
     */
    KD_TREE("KDTree", true),
    KD_TREE_POSTING_LISTS("KDTreePostingLists", true),

    /**
     * Vector index components
     */
    VECTOR("Vector", false),
    PQ("PQ", false),

    /**
     * Term dictionary written by {@code TrieTermsDictionaryWriter} stores mappings of term and
     * file pointer to posting block on posting file.
     *
     * V1
     */
    TERMS_DATA("TermsData", true),
    /**
     * Stores postings written by {@code PostingsWriter}
     *
     * V1
     */
    POSTING_LISTS("PostingLists", true),
    /**
     * If present indicates that the column index build completed successfully
     *
     * V1
     */
    COLUMN_COMPLETION_MARKER("ColumnComplete", false),

    // per-sstable components
    /**
     * Partition key token value for rows including row tombstone and static row. (access key is rowId)
     *
     * V1 V2
     */
    TOKEN_VALUES("TokenValues", false),
    /**
     * Partition key offset in sstable data file for rows including row tombstone and static row. (access key is
     * rowId)
     *
     * V1
     */
    OFFSETS_VALUES("OffsetsValues", false),
    /**
     * An on-disk trie containing the primary keys used for looking up the rowId from a partition key
     *
     * V2
     */
    PRIMARY_KEY_TRIE("PrimaryKeyTrie", true),
    /**
     * Prefix-compressed blocks of primary keys used for rowId to partition key lookups
     *
     * V2
     */
    PRIMARY_KEY_BLOCKS("PrimaryKeyBlocks", true),
    /**
     * Encoded sequence of offsets to primary key blocks
     *
     * V2
     */
    PRIMARY_KEY_BLOCK_OFFSETS("PrimaryKeyBlockOffsets", false),
    /**
     * Stores per-sstable metadata.
     *
     * V1
     */
    GROUP_META("GroupMeta", false),
    /**
     * If present indicates that the per-sstable index build completed successfully
     *
     * V1 V2
     */
    GROUP_COMPLETION_MARKER("GroupComplete", false),

    /**
     * Stores document length information for BM25 scoring
     */
    DOC_LENGTHS("DocLengths", false);

    public final String representation;
    public final boolean compressed;

    IndexComponentType(String representation, boolean compressed)
    {
        this.representation = representation;
        this.compressed = compressed;
    }

    static final Map<String, IndexComponentType> byRepresentation = new HashMap<>();
    static
    {
        for (IndexComponentType component : values())
            byRepresentation.put(component.representation, component);
    }

    public static @Nullable IndexComponentType fromRepresentation(String representation)
    {
        return byRepresentation.get(representation);
    }
}
