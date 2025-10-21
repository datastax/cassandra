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

package org.apache.cassandra.index.sai.disk.vector;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.memory.MemtableIndex;

public abstract class AbstractMemtableIndex implements MemtableIndex
{
    protected final IndexContext indexContext;
    protected final Memtable memtable;
    protected final Version version;

    private final int flushThresholdMaxRows;

    public AbstractMemtableIndex(IndexContext indexContext, Memtable memtable)
    {
        this.indexContext = indexContext;
        this.memtable = memtable;
        this.flushThresholdMaxRows = indexContext.isVector() ? CassandraRelevantProperties.SAI_VECTOR_FLUSH_THRESHOLD_MAX_ROWS.getInt()
                                                             : CassandraRelevantProperties.SAI_NON_VECTOR_FLUSH_THRESHOLD_MAX_ROWS.getInt();
        this.version = indexContext.version();
    }

    /**
     * Called when index is updated
     */
    protected void onIndexUpdated()
    {
        if (flushThresholdMaxRows > 0 && getRowCount() >= flushThresholdMaxRows)
            memtable.signalFlushRequired(ColumnFamilyStore.FlushReason.INDEX_MEMTABLE_LIMIT, true);
    }
}
