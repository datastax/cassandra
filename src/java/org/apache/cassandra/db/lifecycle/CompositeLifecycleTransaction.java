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

package org.apache.cassandra.db.lifecycle;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.io.sstable.format.SSTableReader;

public class CompositeLifecycleTransaction extends LifecycleTransaction
{
    private final AtomicInteger partsToCommitOrAbort;
    private volatile boolean obsoleteOriginalsRequested;
    private volatile boolean wasAborted;

    public CompositeLifecycleTransaction(Tracker tracker, OperationType operationType, Iterable<? extends SSTableReader> readers, UUID uuid, int partCount)
    {
        super(tracker, operationType, readers, uuid);
        partsToCommitOrAbort = new AtomicInteger(partCount);
        wasAborted = false;
    }


    public void requestObsoleteOriginals()
    {
        obsoleteOriginalsRequested = true;
    }

    public void commitPart()
    {
        partCommittedOrAborted();
    }

    private void partCommittedOrAborted()
    {
        if (partsToCommitOrAbort.decrementAndGet() == 0)
        {
            if (wasAborted)
                abort();
            else
            {
                checkpoint();
                if (obsoleteOriginalsRequested)
                    obsoleteOriginals();
                finish();
            }
        }
    }

    public void abortPart()
    {
        wasAborted = true;
        partCommittedOrAborted();
    }

    boolean wasAborted()
    {
        return wasAborted;
    }
}
