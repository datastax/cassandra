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
 * <p>Tests the commitlog to make sure we can replay it - explicitly for the case where we use uncompressed segments
 * and some section ends abruptly, with the next expected marker ending up past EOF (e.g. because the process died
 * while writing).
 *
 * <p>Note: This test is only relevant to commit logs written in uncompressed (direct) mode. Compressed or encrypted
 * commit logs that are hit by such a problem might not be recoverable if section integrity is affected, while for
 * mmapped commit logs see {@link CommitLogMmapSyncFailureTest}.
 */
@RunWith(BMUnitRunner.class)
public class CommitLogUncompressedSyncFailureTest extends CommitLogSyncFailureTest
{
    @Before
    public void setUp()
    {
        setUp(Config.DiskAccessMode.standard);
    }

    @Test
    @BMRule(name = "put last section marker past EOF",
            targetClass = "CommitLogSegmentReader",
            targetMethod = "readSyncMarker",
            targetLocation = "AFTER WRITE $end",
            condition = "$end == $fileLength",
            action = "$end = Integer.MAX_VALUE")
    public void replayCommitLogAfterSyncFailure() throws IOException
    {
        // Sync wanted; byteman should then ensure that the last commit log section "looks" cut off
        replayCommitLogAfterSyncFailure(true);
    }
}
