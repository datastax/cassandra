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

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.memory.MemtableIndex;
import org.apache.cassandra.utils.WrappedRunnable;

public abstract class AbstractMemtableIndex implements MemtableIndex
{
    private static final Logger logger = LoggerFactory.getLogger(AbstractMemtableIndex.class);

    protected final IndexContext indexContext;
    protected final Memtable memtable;

    public AbstractMemtableIndex(IndexContext indexContext, Memtable memtable)
    {
        this.indexContext = indexContext;
        this.memtable = memtable;
    }

    protected abstract int size();

    /**
     * Called when index is updated
     */
    protected void onIndexUpdated()
    {
        if (indexContext.getIndexWriterConfig().hasFlushThreshold()
            && size() >= indexContext.getIndexWriterConfig().getFlushThresholdMaxRows())
            memtable.signalFlushRequired(ColumnFamilyStore.FlushReason.INDEX_MEMTABLE_LIMIT);
    }

    protected void maybeScheduleFlush()
    {
        if (indexContext.getIndexWriterConfig().hasFlushPeriod())
        {
            int periodInSeconds = indexContext.getIndexWriterConfig().getFlushPeriodInSeconds();
            logger.trace("scheduling memtable index flush in {} seconds for index {}", periodInSeconds, indexContext.getIndexName());
            scheduleFlush(periodInSeconds);
        }
    }

    private void scheduleFlush(int periodInSeconds)
    {
        WrappedRunnable runnable = new WrappedRunnable()
        {
            protected void runMayThrow()
            {
                // if it's clean, reschedule
                if (isEmpty())
                {
                    scheduleFlush(periodInSeconds);
                }
                // signal flush
                else
                {
                    memtable.signalFlushRequired(ColumnFamilyStore.FlushReason.INDEX_MEMTABLE_PERIOD_EXPIRED);
                }
            }
        };
        ScheduledExecutors.scheduledTasks.schedule(runnable, periodInSeconds, TimeUnit.SECONDS);
    }
}
