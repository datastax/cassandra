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

import java.io.File;
import java.io.IOException;

import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.io.util.RandomAccessReader;

public interface ICommitLogReadHandler
{
    enum CommitLogReadErrorReason
    {
        RECOVERABLE_DESCRIPTOR_ERROR,
        UNRECOVERABLE_DESCRIPTOR_ERROR,
        MUTATION_ERROR,
        UNKNOWN_ERROR,
        EOF
    }

    class CommitLogReadException extends IOException
    {
        public final CommitLogReadErrorReason reason;
        public final boolean permissable;

        CommitLogReadException(String message, CommitLogReadErrorReason reason, boolean permissable)
        {
            super(message);
            this.reason = reason;
            this.permissable = permissable;
        }
    }

    /**
     * Allow consumers to prepare a file before replay. Immediate use-case: seeking on replayer on desc that's globalPosition
     *
     * @param desc
     * @param reader
     */
    void prepReader(CommitLogDescriptor desc, RandomAccessReader reader);

    /**
     *  Rather than opening CommitLogSegmentReaders for all files, this allows handlers to specify files that should be skipped
     *
     *  @param file File object for candidate
     *  @param desc CommitLogDescriptor for candidate
     *  @return boolean representing whether we should skip or not
     */
    boolean logAndCheckIfShouldSkip(File file, CommitLogDescriptor desc);

    /**
     * Another skipping approach - based on how far along we are in the segment rather than the file as a whole.
     *
     * @param position  position of sync segment candidate
     */
    boolean shouldSkipSegment(long id, int position);

    /**
     * Handle an error during segment read, signaling whether or not you want the reader to continue based on the error.
     *
     * @param exception
     * @return boolean indicating whether to continue reading or not
     * @throws CommitLogReadException if things are unrecoverable
     */
    boolean shouldStopOnError(CommitLogReadException exception) throws IOException;

    /**
     * Process a deserialized mutation
     *
     * @param m             deserialized mutation
     * @param size          serialized size of the mutation
     * @param entryLocation filePointer offset inside the CommitLogSegment for the record
     * @param desc          CommitLogDescriptor for mutation being processed
     */
    void handleMutation(Mutation m, int size, long entryLocation, CommitLogDescriptor desc);
}
