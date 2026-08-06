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

package org.apache.cassandra.sensors;

import org.apache.cassandra.net.Message;

/**
 * The type of the measurement a {@link Sensor} refers to.
 */
public enum Type
{
    /**
     * Bytes transferred over the internode network for a given request/response cycle, counting both the inbound
     * request payload and the outbound response payload. Tracked on every verb handler that sends or receives an
     * internode message (reads, mutations, counter mutations, Paxos prepare/propose/commit).
     * Aggregated as a {@code sum} in the global registry.
     *
     * @see org.apache.cassandra.db.ReadCommandVerbHandler
     * @see org.apache.cassandra.db.MutationVerbHandler
     * @see org.apache.cassandra.db.CounterMutationVerbHandler
     */
    INTERNODE_BYTES(true),

    /**
     * Bytes read from storage (memtable or SSTable) while executing a read command on a replica, as measured by
     * {@link org.apache.cassandra.db.ReadCommand#executeLocally}. Also incremented for Paxos prepare and propose
     * operations which involve a read of the current partition state.
     * Aggregated as a {@code sum} in the global registry, giving a running total of bytes read per table.
     *
     * @see org.apache.cassandra.db.ReadCommandVerbHandler
     * @see org.apache.cassandra.net.ResponseVerbHandler
     */
    READ_BYTES(true),

    /**
     * Bytes written to the primary table memtable while applying a mutation on a replica, as measured by the
     * serialized data size of each {@link org.apache.cassandra.db.partitions.PartitionUpdate}.
     * Also incremented for Paxos prepare, propose, and commit operations.
     * Aggregated as a {@code sum} in the global registry, giving a running total of bytes written per table.
     *
     * @see org.apache.cassandra.db.ColumnFamilyStore
     * @see org.apache.cassandra.net.ResponseVerbHandler
     */
    WRITE_BYTES(true),

    /**
     * Bytes written to a secondary index (both {@link org.apache.cassandra.index.internal.CassandraIndex legacy
     * secondary indexes} and {@link org.apache.cassandra.index.sai SAI indexes}) while applying a mutation on a
     * replica. Tracked separately from {@link #WRITE_BYTES} so index write amplification can be observed independently.
     * Aggregated as a {@code sum} in the global registry, giving a running total of index bytes written per table.
     *
     * @see org.apache.cassandra.index.internal.CassandraIndex
     * @see org.apache.cassandra.index.sai.memory.TrieMemtableIndex
     */
    INDEX_WRITE_BYTES(true),

    /**
     * Read command wall-clock execution time expressed as a discrete latency tier (1–5, see {@link ReadLatencyTier}).
     * <p>
     * Measured at two points in the request path and combined via {@code max}:
     * <ol>
     *   <li><b>Replica</b> — wall-clock time of {@link org.apache.cassandra.db.ReadCommand#executeLocally} inside
     *       {@link org.apache.cassandra.db.ReadCommandVerbHandler}, propagated to the coordinator via the internode
     *       response. The coordinator takes the {@code max} across all responding replicas.</li>
     *   <li><b>Coordinator</b> — wall-clock time of result fetching, merging, ordering and filtering inside
     *       {@link org.apache.cassandra.cql3.statements.SelectStatement}, folded into the same sensor via
     *       {@code max}.</li>
     * </ol>
     * The CQL client therefore receives the worst-case tier across the entire request path without needing to
     * distinguish where the time was spent, keeping the signal opaque and suitable for use as a single request
     * cost signal by rate limiters.
     * <p>
     * Not synced to the global registry: a per-request max tier has no meaningful global aggregate.
     *
     * @see ReadLatencyTier
     * @see org.apache.cassandra.db.ReadCommandVerbHandler
     * @see org.apache.cassandra.net.ResponseVerbHandler
     * @see org.apache.cassandra.cql3.statements.SelectStatement
     */
    READ_LATENCY_TIER(false);

    /**
     * Whether this sensor type should be propagated to the global {@link SensorsRegistry} via
     * {@link ActiveRequestSensors#syncAllSensors()}. Types that represent per-request signals whose
     * global aggregate is meaningless (e.g. a max-tier value) should set this to {@code false}.
     */
    public final boolean shouldSyncToRegistry;

    Type(boolean shouldSyncToRegistry)
    {
        this.shouldSyncToRegistry = shouldSyncToRegistry;
    }
}