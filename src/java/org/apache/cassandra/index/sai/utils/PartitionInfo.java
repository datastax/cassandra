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

import javax.annotation.Nullable;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.rows.BaseRowIterator;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;

public class PartitionInfo
{
    public final DecoratedKey key;
    public final Row staticRow;
    public final RegularAndStaticColumns columns;

    // present if it's unfiltered partition iterator
    @Nullable
    public final DeletionTime partitionDeletion;

    // present if it's unfiltered partition iterator
    @Nullable
    public final EncodingStats encodingStats;

    private PartitionInfo(DecoratedKey key,
                          Row staticRow,
                          RegularAndStaticColumns columns,
                          @Nullable DeletionTime partitionDeletion,
                          @Nullable EncodingStats encodingStats)
    {
        this.key = key;
        this.staticRow = staticRow;
        this.columns = columns;
        this.partitionDeletion = partitionDeletion;
        this.encodingStats = encodingStats;
    }

    public static <U extends Unfiltered, R extends BaseRowIterator<U>> PartitionInfo create(R baseRowIterator)
    {
        // only unfiltered row iterators have a partition deletion time and encoding stats
        DeletionTime partitionDeletion = null;
        EncodingStats encodingStats = null;
        if (baseRowIterator instanceof UnfilteredRowIterator)
        {
            UnfilteredRowIterator unfilteredRowIterator = (UnfilteredRowIterator) baseRowIterator;
            partitionDeletion = unfilteredRowIterator.partitionLevelDeletion();
            encodingStats = unfilteredRowIterator.stats();
        }

        return new PartitionInfo(baseRowIterator.partitionKey(),
                                 baseRowIterator.staticRow(),
                                 baseRowIterator.columns(),
                                 partitionDeletion,
                                 encodingStats);
    }
}
