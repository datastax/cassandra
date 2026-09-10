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

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.lifecycle.ILifecycleTransaction;

/**
 * Repair finishing task supporting background execution and cancellation.
 *
 * This class wraps {@link RepairFinalizationOperation} to provide full compaction task
 * lifecycle management (CREATED → STARTED → ACTIVE → COMPLETE states) with proper
 * scheduledTasks tracking and removal.
 *
 * For normal repair completion, use {@link RepairFinalizationOperation} directly to
 * avoid unnecessary compaction task overhead.
 */
public class RepairFinishedCompactionTask extends AbstractCompactionTask
{
    private static final Logger logger = LoggerFactory.getLogger(RepairFinishedCompactionTask.class);

    private final RepairFinalizationOperation operation;

    public RepairFinishedCompactionTask(CompactionRealm realm,
                                        ILifecycleTransaction transaction,
                                        UUID sessionID,
                                        long repairedAt,
                                        boolean isTransient)
    {
        super(realm, transaction);
        this.operation = new RepairFinalizationOperation(realm, transaction, sessionID, repairedAt, isTransient);
    }

    @VisibleForTesting
    UUID getSessionID()
    {
        return operation.getSessionID();
    }

    @Override
    protected void runMayThrow() throws Exception
    {
        // Delegate to the lightweight operation
        operation.execute();
    }

    @Override
    public long getSpaceOverhead()
    {
        return 0;  // This is just metadata modification, no overhead.
    }
}