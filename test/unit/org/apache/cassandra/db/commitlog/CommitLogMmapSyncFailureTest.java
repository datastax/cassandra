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

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.config.Config;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;

/**
 * <p>Tests the commitlog to make sure we can replay it - explicitly for the case where we use memory-mapped segments
 * and we do not explicitly flush the data to disk (e.g. because the process died before we could do that).
 *
 * <p>Note: This test is only relevant to commit logs written in memory-mapped mode. Direct, compressed or encrypted
 * commit logs do not write any section content to disk before a sync request is sent, and in their case the
 * section marker is written first.
 */
@RunWith(BMUnitRunner.class)
public class CommitLogMmapSyncFailureTest extends CommitLogSyncFailureTest
{
    @Before
    public void setUp()
    {
        setUp(Config.DiskAccessMode.mmap);
    }

    /*
    @BMRule(name = "force all calls to sync() to not flush to disk",
    targetClass = "CommitLogSegment",
    targetMethod = "sync(boolean)",
    action = "$flush = false")
    */

    @Test
    @BMRule(name = "force all calls to sync() to not flush to disk",
            targetClass = "CommitLogSegment",
            targetMethod = "sync(boolean)",
            targetLocation = "ENTRY",
            action = "return")
    public void replayCommitLogAfterSyncFailure() throws IOException
    {
        // No sync wanted; byteman should have prevented any automatically triggered syncs
        replayCommitLogAfterSyncFailure(false);
    }
}

