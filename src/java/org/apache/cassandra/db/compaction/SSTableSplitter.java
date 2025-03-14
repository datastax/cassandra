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
package org.apache.cassandra.db.compaction;

import java.util.*;
import java.util.function.LongPredicate;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.compaction.writers.CompactionAwareWriter;
import org.apache.cassandra.db.compaction.writers.MaxSSTableSizeWriter;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;

public class SSTableSplitter 
{
    private final AbstractCompactionTask task;

    public SSTableSplitter(CompactionRealm realm, LifecycleTransaction transaction, int sstableSizeInMB)
    {
        this.task = new SplittingCompactionTask(realm, transaction, sstableSizeInMB);
    }

    public void split()
    {
        task.execute();
    }

    private static class SplittingCompactionTask extends CompactionTask
    {
        private final int sstableSizeInMB;

        public SplittingCompactionTask(CompactionRealm realm, LifecycleTransaction transaction, int sstableSizeInMB)
        {
            super(realm, transaction, CompactionManager.NO_GC, false, null);
            this.sstableSizeInMB = sstableSizeInMB;

            if (sstableSizeInMB <= 0)
                throw new IllegalArgumentException("Invalid target size for SSTables, must be > 0 (got: " + sstableSizeInMB + ")");
        }

        @Override
        protected CompactionController getCompactionController(Set<SSTableReader> toCompact)
        {
            return new SplitController(realm);
        }

        @Override
        public CompactionAwareWriter getCompactionAwareWriter(CompactionRealm realm,
                                                              Directories directories,
                                                              Set<SSTableReader> nonExpiredSSTables)
        {
            return new MaxSSTableSizeWriter(realm, directories, transaction, nonExpiredSSTables, sstableSizeInMB * 1024L * 1024L, 0, false);
        }

        @Override
        protected boolean partialCompactionsAcceptable()
        {
            return true;
        }
    }

    public static class SplitController extends CompactionController
    {
        public SplitController(CompactionRealm cfs)
        {
            super(cfs, CompactionManager.NO_GC);
        }

        @Override
        public LongPredicate getPurgeEvaluator(DecoratedKey key)
        {
            return time -> false;
        }
    }
}
