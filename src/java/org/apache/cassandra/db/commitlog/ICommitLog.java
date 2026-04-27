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
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.exceptions.CDCWriteException;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.metrics.CommitLogMetrics;
import org.apache.cassandra.schema.TableId;

public interface ICommitLog
{
    ICommitLog start();

    boolean isStarted();

    CommitLogPosition add(Mutation mutation) throws CDCWriteException;

    CommitLogArchiver archiver();

    CommitLogPosition getCurrentPosition();

    List<String> getActiveSegmentNames();

    CommitLogMetrics metrics();

    void sync(boolean flush) throws IOException;

    boolean shouldRejectMutations();

    Map<Keyspace, Integer> recoverSegmentsOnDisk(ColumnFamilyStore.FlushReason flushReason) throws IOException;

    void discardCompletedSegments(final TableId id, final CommitLogPosition lowerBound, final CommitLogPosition upperBound);

    void forceRecycleAllSegments(Collection<TableId> droppedTables);

    void shutdownBlocking() throws InterruptedException;

    void forceRecycleAllSegments();

    /**
     * FOR TESTING PURPOSES
     */
    void stopUnsafe(boolean deleteSegments);

    /**
     * FOR TESTING PURPOSES
     */
    Map<Keyspace, Integer>  resetUnsafe(boolean deleteSegments) throws IOException;

    /**
     * FOR TESTING PURPOSES
     */
    Map<Keyspace, Integer>  restartUnsafe() throws IOException;

    Map<Keyspace, Integer> recoverFiles(ColumnFamilyStore.FlushReason flushReason, File... clogs) throws IOException;

    void recoverPath(String path, boolean tolerateTruncation) throws IOException;

    void recover(String path) throws IOException;

    AbstractCommitLogSegmentManager getSegmentManager();
}
