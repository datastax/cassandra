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

import java.util.UUID;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.lifecycle.ILifecycleTransaction;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.service.ActiveRepairService;

/**
 * Lightweight operation to finalize a repair session by promoting/demoting sstables.
 * Used for normal repair completion
 */
public class RepairFinalizationOperation
{
    private static final Logger logger = LoggerFactory.getLogger(RepairFinalizationOperation.class);

    private final CompactionRealm realm;
    private final ILifecycleTransaction transaction;
    private final UUID sessionID;
    private final long repairedAt;
    private final boolean isTransient;

    public RepairFinalizationOperation(CompactionRealm realm,
                                       ILifecycleTransaction transaction,
                                       UUID sessionID,
                                       long repairedAt,
                                       boolean isTransient)
    {
        this.realm = realm;
        this.transaction = transaction;
        this.sessionID = sessionID;
        this.repairedAt = repairedAt;
        this.isTransient = isTransient;
    }

    /**
     * Execute the repair finalization synchronously.
     * No compaction task tracking - this is a simple metadata operation.
     *
     * @throws Exception if the operation fails
     */
    public void execute() throws Exception
    {
        boolean completed = false;
        boolean obsoleteSSTables = isTransient && repairedAt > 0;

        try
        {
            if (obsoleteSSTables)
            {
                logger.info("Obsoleting transient repaired sstables for {}", sessionID);
                Preconditions.checkState(Iterables.all(transaction.originals(), SSTableReader::isTransient));
                transaction.obsoleteOriginals();
            }
            else
            {
                logger.info("Moving {} from pending to repaired with repaired at = {} for session id = {}", transaction.originals(), repairedAt, sessionID);
                realm.mutateRepairedWithLock(transaction.originals(),
                                             repairedAt,
                                             ActiveRepairService.NO_PENDING_REPAIR,
                                             false);
            }
            completed = true;
        }
        finally
        {
            if (obsoleteSSTables)
            {
                transaction.prepareToCommit();
                transaction.commit();
            }
            else
            {
                // we abort here because mutating metadata isn't guarded by LifecycleTransaction, so this won't roll
                // anything back. Also, we don't want to obsolete the originals. We're only using it to prevent other
                // compactions from marking these sstables compacting, and unmarking them when we're done
                transaction.abort();
            }

            if (completed)
            {
                realm.repairSessionCompleted(sessionID);
            }
        }
    }

    public UUID getSessionID()
    {
        return sessionID;
    }

    public ILifecycleTransaction getTransaction()
    {
        return transaction;
    }
}