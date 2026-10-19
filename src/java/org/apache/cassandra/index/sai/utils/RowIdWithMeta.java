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

package org.apache.cassandra.index.sai.utils;

import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.io.sstable.SSTableId;

/**
 * Represents a row id with additional metadata. The metadata is not a type parameter to prevent unnecessary boxing.
 */
public abstract class RowIdWithMeta
{
    private final int segmentRowId;

    protected RowIdWithMeta(int segmentRowId)
    {
        this.segmentRowId = segmentRowId;
    }

    public final int getSegmentRowId()
    {
        return segmentRowId;
    }

    public PrimaryKeyWithSortKey buildPrimaryKeyWithSortKey(IndexContext indexContext,
                                                            SSTableId<?> sstableId,
                                                            PrimaryKeyMap primaryKeyMap,
                                                            long segmentRowIdOffset)
    {
        var pk = primaryKeyMap.primaryKeyFromRowId(segmentRowIdOffset + segmentRowId);
        return wrapPrimaryKey(indexContext, sstableId, pk);
    }

    /**
     * Wrap the provided primary key with the stored metadata.
     * @param indexContext the index context
     * @param sstableId the sstable id
     * @param primaryKey the primary key
     * @return the wrapped primary key with its associated metadata
     */
    protected abstract PrimaryKeyWithSortKey wrapPrimaryKey(IndexContext indexContext, SSTableId<?> sstableId, PrimaryKey primaryKey);
}
