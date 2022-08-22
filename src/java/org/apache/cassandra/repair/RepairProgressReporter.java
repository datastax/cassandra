/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.repair;

import java.util.Collection;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.repair.messages.RepairOption;
import org.apache.cassandra.schema.SystemDistributedKeyspace;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.FBUtilities;

public interface RepairProgressReporter
{
    RepairProgressReporter instance = CassandraRelevantProperties.REPAIR_PROGRESS_REPORTER.isPresent()
                                      ? FBUtilities.construct(CassandraRelevantProperties.REPAIR_PROGRESS_REPORTER.getString(),
                                                              "Repair Progress Reporter")
                                      : new DefaultRepairProgressReporter();

    void onParentRepairStarted(TimeUUID parentSession, String keyspaceName, String[] cfnames, RepairOption options);

    void onParentRepairSucceeded(TimeUUID parentSession, Collection<Range<Token>> successfulRanges);

    void onParentRepairFailed(TimeUUID parentSession, Throwable t);

    void onRepairsStarted(TimeUUID id, TimeUUID parentRepairSession, String keyspaceName, String[] cfnames, CommonRange commonRange);

    void onRepairsFailed(TimeUUID id, String keyspaceName, String[] cfnames, Throwable t);

    void onRepairFailed(TimeUUID id, String keyspaceName, String cfname, Throwable t);

    void onRepairSucceeded(TimeUUID id, String keyspaceName, String cfname);

    class DefaultRepairProgressReporter implements RepairProgressReporter
    {
        @Override
        public void onParentRepairStarted(TimeUUID parentSession, String keyspaceName, String[] cfnames, RepairOption options)
        {
            SystemDistributedKeyspace.startParentRepair(parentSession, keyspaceName, cfnames, options);
        }

        @Override
        public void onParentRepairSucceeded(TimeUUID parentSession, Collection<Range<Token>> successfulRanges)
        {
            SystemDistributedKeyspace.successfulParentRepair(parentSession, successfulRanges);
        }

        @Override
        public void onParentRepairFailed(TimeUUID parentSession, Throwable t)
        {
            SystemDistributedKeyspace.failParentRepair(parentSession, t);
        }

        @Override
        public void onRepairsStarted(TimeUUID id, TimeUUID parentRepairSession, String keyspaceName, String[] cfnames, CommonRange commonRange)
        {
            SystemDistributedKeyspace.startRepairs(id, parentRepairSession, keyspaceName, cfnames, commonRange);
        }

        @Override
        public void onRepairsFailed(TimeUUID id, String keyspaceName, String[] cfnames, Throwable t)
        {
            SystemDistributedKeyspace.failRepairs(id, keyspaceName, cfnames, t);
        }

        @Override
        public void onRepairFailed(TimeUUID id, String keyspaceName, String cfname, Throwable t)
        {
            SystemDistributedKeyspace.failedRepairJob(id, keyspaceName, cfname, t);
        }

        @Override
        public void onRepairSucceeded(TimeUUID id, String keyspaceName, String cfname)
        {
            SystemDistributedKeyspace.successfulRepairJob(id, keyspaceName, cfname);
        }
    }
}
