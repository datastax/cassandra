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
    META("Meta"),
    /**
     * KDTree written by {@code BKDWriter} indexes mappings of term to one ore more segment row IDs
     * (segment row ID = SSTable row ID - segment row ID offset).
     *
     * V1
     */
    KD_TREE("KDTree"),
    KD_TREE_POSTING_LISTS("KDTreePostingLists"),

    /**
     * Vector index components
     */
    VECTOR("Vector"),
    PQ("PQ"),

    /**
     * Term dictionary written by {@code TrieTermsDictionaryWriter} stores mappings of term and
     * file pointer to posting block on posting file.
     *
     * V1
     */
    TERMS_DATA("TermsData"),
    /**
     * Stores postings written by {@code PostingsWriter}
     *
     * V1
     */
    POSTING_LISTS("PostingLists"),
    /**
     * If present indicates that the column index build completed successfully
     *
     * V1
     */
    COLUMN_COMPLETION_MARKER("ColumnComplete"),

    // per-sstable components
    /**
     * Partition key token value for rows including row tombstone and static row. (access key is rowId)
     *
     * V1 V2
     */
    TOKEN_VALUES("TokenValues"),
    /**
     * Partition key offset in sstable data file for rows including row tombstone and static row. (access key is
     * rowId)
     *
     * V1
     */
    OFFSETS_VALUES("OffsetsValues"),

    /**
     * An on-disk block-packed index containing the starting and ending rowIds for each partition.
     */
    PARTITION_SIZES("PartitionSizes"),

    /**
     * Prefix-compressed blocks of partition keys used for rowId to partition key lookups
     * <p>
     * V2
     */
    PARTITION_KEY_BLOCKS("PartitionKeyBlocks"),

    /**
     * Encoded sequence of offsets to partition key blocks
     * <p>
     * V2
     */
    PARTITION_KEY_BLOCK_OFFSETS("PartitionKeyBlockOffsets"),

    /**
     * Prefix-compressed blocks of clustering keys used for rowId to clustering key lookups
     */
    CLUSTERING_KEY_BLOCKS("ClusteringKeyBlocks"),

    /**
     * Encoded sequence of offsets to clustering key blocks
     */
    CLUSTERING_KEY_BLOCK_OFFSETS("ClusteringKeyBlockOffsets"),

    /**
     * Stores per-sstable metadata.
     *
     * V1
     */
    GROUP_META("GroupMeta"),
    /**
     * If present indicates that the per-sstable index build completed successfully
     *
     * V1 V2
     */
    GROUP_COMPLETION_MARKER("GroupComplete"),
    
    /**
     * Stores document length information for BM25 scoring
     */
    DOC_LENGTHS("DocLengths");

    public final String representation;

    IndexComponentType(String representation)
    {
        this.representation = representation;
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
