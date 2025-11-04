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

import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * After recovery of commit logs is performed, this class is responsible for handling the commit log files that were
 * replayed.
 */
public class CommitLogSegmentHandler
{
    private static final Logger logger = LoggerFactory.getLogger(CommitLogSegmentHandler.class);

    public void handleReplayedSegment(final File file, boolean hasInvalidMutations, boolean hasFailedMutations)
    {
        if (!hasFailedMutations && !hasInvalidMutations)
        {
            // (don't decrease managed size, since this was never a "live" segment)
            logger.trace("(Unopened) segment {} is no longer needed and will be deleted now", file);
            FileUtils.deleteWithConfirm(file);
        }
        else
        {
            logger.debug("File {} should not be deleted as it contains invalid or failed mutations", file.name());
        }
    }
}
