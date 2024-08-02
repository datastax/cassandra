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

package org.apache.cassandra.db.commitlog;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.nodes.LocalInfo;
import org.apache.cassandra.nodes.Nodes;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.config.CassandraRelevantProperties.COMMIT_LOG_REPLAYER;

public class CommitLogReplayerFactory
{
    private static final Logger logger = LoggerFactory.getLogger(CommitLogReplayerFactory.class);
    private static final String commitLogReplayer = COMMIT_LOG_REPLAYER.getString();
    public static CommitLogReplayer create(CommitLog commitLog, UUID localHostId)
    {
        // make sure the truncation records are available, even though the CL has not been replayed
        Map<UUID, LocalInfo.TruncationRecord> truncationRecords = Nodes.local().get().getTruncationRecords();

        // compute per-CF and global replay intervals
        Map<TableId, IntervalSet<CommitLogPosition>> cfPersisted = new HashMap<>();
        CommitLogReplayer.ReplayFilter replayFilter = CommitLogReplayer.ReplayFilter.create();

        for (ColumnFamilyStore cfs : ColumnFamilyStore.all())
        {
            LocalInfo.TruncationRecord truncationRecord = truncationRecords.get(cfs.metadata.id.asUUID());
            // but, if we've truncated the cf in question, then we need to need to start replay after the truncation
            CommitLogPosition truncatedAt = truncationRecord == null ? null : truncationRecord.position;
            if (truncatedAt != null)
            {
                // Point in time restore is taken to mean that the tables need to be replayed even if they were
                // deleted at a later point in time. Any truncation record after that point must thus be cleared prior
                // to replay (CASSANDRA-9195).
                long restoreTime = commitLog.archiver.restorePointInTime;
                long truncatedTime = truncationRecord.truncatedAt;
                if (truncatedTime > restoreTime)
                {
                    if (replayFilter.includes(cfs.metadata))
                    {
                        logger.info("Restore point in time is before latest truncation of table {}.{}. Clearing truncation record.",
                                    cfs.metadata.keyspace,
                                    cfs.metadata.name);
                        Nodes.local().removeTruncationRecord(cfs.metadata.id);
                        truncatedAt = null;
                    }
                }
            }

            IntervalSet<CommitLogPosition> filter;
            if (!cfs.memtableWritesAreDurable())
            {
                filter = CommitLogReplayer.persistedIntervals(cfs.getLiveSSTables(), truncatedAt, localHostId);
            }
            else
            {
                // everything is persisted and restored by the memtable itself
                filter = new IntervalSet<>(CommitLogPosition.NONE, CommitLog.instance.getCurrentPosition());
            }
            cfPersisted.put(cfs.metadata.id, filter);
        }
        CommitLogPosition globalPosition = CommitLogReplayer.firstNotCovered(cfPersisted.values());
        logger.debug("Global replay position is {} from columnfamilies {}", globalPosition, FBUtilities.toString(cfPersisted));

        if (commitLogReplayer == null)
            return new CommitLogReplayer(commitLog, globalPosition, cfPersisted, replayFilter);

        Class<CommitLogReplayer> factoryClass = FBUtilities.classForName(commitLogReplayer, "Custom commit log replayer");

        try
        {
            return factoryClass.getConstructor(CommitLog.class, CommitLogPosition.class, Map.class, CommitLogReplayer.ReplayFilter.class)
                        .newInstance(commitLog, globalPosition, cfPersisted, replayFilter);
        }
        catch (NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e)
        {
            throw new ConfigurationException("Unable to find correct constructor for " + commitLogReplayer, e);
        }
    }
}
