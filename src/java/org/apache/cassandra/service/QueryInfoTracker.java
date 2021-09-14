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

package org.apache.cassandra.service;

import java.util.Collection;
import java.util.List;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.schema.TableMetadata;

/**
 * A tracker objects that can be registered against {@link StorageProxy} to be called back with information on executed
 * queries.
 *
 * <p>The goal of this interface is to provide to implementations enough information for it to accurately estimate how
 * much "work" a query has performed. So while for write this mostly just mean passing the generated mutations, for
 * reads this mean passing the unfiltered result of the query.
 *
 * <p>The methods of this tracker are called from {@link StorageProxy} and are thus "coordinator level". As such, all
 * user writes or reads will trigger the call of one of these methods, as will internal distributed system table
 * queries, but internal local system table queries will not.
 *
 * <p>For writes, the {@link #onWrite} method is only called for the "user write", but if that write trigger either
 * secondary index or materialized views updates, those additional update do not trigger additional calls.
 *
 * <p>The methods of this tracker are called on hot path, so none of them should be blocking, and they should be as
 * lightweight as possible.
 */
public interface QueryInfoTracker
{
    /** A tracker that does nothing. */
    QueryInfoTracker NOOP = new QueryInfoTracker() {
        @Override
        public WriteTracker onWrite(ClientState state,
                                    boolean isLogged,
                                    Collection<? extends IMutation> mutations,
                                    ConsistencyLevel consistencyLevel)
        {
            return WriteTracker.NOOP;
        }

        @Override
        public ReadTracker onRead(ClientState state,
                                  TableMetadata table,
                                  List<SinglePartitionReadCommand> commands,
                                  ConsistencyLevel consistencyLevel)
        {
            return ReadTracker.NOOP;
        }

        @Override
        public ReadTracker onRangeRead(ClientState state,
                                       TableMetadata table,
                                       PartitionRangeReadCommand command,
                                       ConsistencyLevel consistencyLevel)
        {
            return ReadTracker.NOOP;
        }
    };

    /**
     * Called before every (non-LWT) write coordinated on the local node.
     *
     * @param state the state of the client that performed the write
     * @param isLogged whether this is a logged batch write.
     * @param mutations the mutations written by the write.
     * @param consistencyLevel the consistency level of the write.
     * @return a tracker that should be notified when either the read error out or completes successfully.
     */
    WriteTracker onWrite(ClientState state,
                         boolean isLogged,
                         Collection<? extends IMutation> mutations,
                         ConsistencyLevel consistencyLevel);

    /**
     * Called before every non-range read coordinated on the local node.
     *
     * @param state the state of the client that performed the read
     * @param table the metadata for the table read.
     * @param commands the commands for the read performed.
     * @param consistencyLevel the consistency level of the read.
     * @return a tracker from which a {@link ReadReconciliationObserver} should be otained for the read, and that should
     * be notified when either the read error out or completes successfully.
     */
    ReadTracker onRead(ClientState state,
                       TableMetadata table,
                       List<SinglePartitionReadCommand> commands,
                       ConsistencyLevel consistencyLevel);

    /**
     * Called before every range read coordinated on the local node.
     *
     * @param state the state of the client that performed the range read
     * @param table the metadata for the table read.
     * @param command the command for the read performed.
     * @param consistencyLevel the consistency level of the read.
     * @return a tracker from which a {@link ReadReconciliationObserver} should be otained for the read, and that should
     * be notified when either the read error out or completes successfully.
     */
    ReadTracker onRangeRead(ClientState state,
                            TableMetadata table,
                            PartitionRangeReadCommand command,
                            ConsistencyLevel consistencyLevel);

    /**
     * A tracker for a specific query.
     *
     * <p>For the tracked query, exactly one of its method should be called.
     */
    interface Tracker
    {
        /** Called when the tracked query completes successfully. */
        void onDone();

        /** Called when the tracked query completes with an error. */
        void onError(Throwable exception);
    }

    /**
     * Tracker for a write query.
     */
    interface WriteTracker extends Tracker
    {
        WriteTracker NOOP = new WriteTracker() {
            @Override
            public void onDone()
            {
            }

            @Override
            public void onError(Throwable exception)
            {
            }
        };
    }

    /**
     * Tracker for a read query.
     */
    interface ReadTracker extends Tracker
    {
        ReadTracker NOOP = new ReadTracker() {
            @Override
            public void onDone()
            {
            }

            @Override
            public void onError(Throwable exception)
            {
            }
        };
    }
}
