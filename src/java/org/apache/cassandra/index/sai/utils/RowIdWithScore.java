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
import org.apache.cassandra.io.sstable.SSTableId;

/**
 * Represents a row id with a score.
 */
public class RowIdWithScore extends RowIdWithMeta implements Comparable<RowIdWithScore>
{
    private final float score;

    public RowIdWithScore(int segmentRowId, float score)
    {
        super(segmentRowId);
        this.score = score;
    }

    @Override
    public int compareTo(RowIdWithScore o)
    {
        return Float.compare(score, o.score);
    }

    @Override
    protected PrimaryKeyWithSortKey wrapPrimaryKey(IndexContext indexContext, SSTableId<?> sstableId, PrimaryKey primaryKey)
    {
        return new PrimaryKeyWithScore(indexContext, sstableId, primaryKey, score);
    }
}
