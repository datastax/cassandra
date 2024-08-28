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
package org.apache.cassandra.db;

import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;

/**
 * An interface that allows to capture what local data has been read
 * <p>
 * This is used by CNDB remote file cache warmup strategy to track access pattern
 */
public interface ReadObserver
{
    ReadObserver NO_OP = new ReadObserver() {};

    /**
     * Observes the provided unmerged partitions.
     * <p>
     * This method allows tracking of the partitions from individual sstable or memtable
     */
    default UnfilteredPartitionIterator observeUnmergedPartitions(UnfilteredPartitionIterator partitions)
    {
        return partitions;
    }

    /**
     * Observes the provided unmerged partition.
     * <p>
     * This method allows tracking of the partition from individual sstable or memtable
     */
    default UnfilteredRowIterator observeUnmergedPartition(UnfilteredRowIterator partition)
    {
        return partition;
    }

    /**
     * Observes the provided merged partitions.
     * <p>
     * This method allows tracking of partitions after they have been merged from multiple sstables and memtable.
     */
    default UnfilteredPartitionIterator observeMergedPartitions(UnfilteredPartitionIterator partitions)
    {
        return partitions;
    }

    /**
     * Called on read request completion
     * <p>
     * This method is used to signify that a read request has completed, allowing any necessary
     * final tracking or cleanup.
     */
    default void onComplete() {}
}
