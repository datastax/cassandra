/*
 * Copyright IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.nodes;

import java.net.InetAddress;
import java.util.UUID;
import javax.annotation.Nullable;

/**
 * What DataStax Enterprise records for a node in its {@code nodes/local} file (the node itself) or for a peer in its
 * {@code nodes/peers} file, as far as the DSE 6 preflight inspection uses it: the identity, the releases and the
 * workloads. Every field but {@link #workloads} is null when DSE did not write it. Read by {@link DseNodesFiles}; the
 * node itself never imports these fields beyond what {@link CC4NodesFileReader} reads.
 */
public final class DseNodeRecord
{
    /** {@code peer}: the address of the peer; null for {@code nodes/local}. */
    @Nullable
    public final InetAddress peer;
    /** {@code broadcast_address} of {@code nodes/local}; null for a peer. */
    @Nullable
    public final InetAddress broadcastAddress;
    /** {@code listen_address} of {@code nodes/local}; null for a peer. */
    @Nullable
    public final InetAddress listenAddress;
    @Nullable
    public final UUID hostId;
    @Nullable
    public final String clusterName;
    @Nullable
    public final String dataCenter;
    @Nullable
    public final String rack;
    /** {@code release_version}, as DSE wrote it ({@code 4.0.0.6865} for DSE 6.8.65). */
    @Nullable
    public final String releaseVersion;
    /** {@code dse_version} ({@code 6.8.65}). */
    @Nullable
    public final String dseVersion;
    @Nullable
    public final UUID schemaVersion;
    /** How many {@code tokens} DSE recorded; null when it recorded none. */
    @Nullable
    public final Integer tokenCount;
    /** {@code workloads}, {@code graph} and {@code workload}. */
    public final DseNodeWorkloads workloads;

    public DseNodeRecord(@Nullable InetAddress peer,
                         @Nullable InetAddress broadcastAddress,
                         @Nullable InetAddress listenAddress,
                         @Nullable UUID hostId,
                         @Nullable String clusterName,
                         @Nullable String dataCenter,
                         @Nullable String rack,
                         @Nullable String releaseVersion,
                         @Nullable String dseVersion,
                         @Nullable UUID schemaVersion,
                         @Nullable Integer tokenCount,
                         DseNodeWorkloads workloads)
    {
        this.peer = peer;
        this.broadcastAddress = broadcastAddress;
        this.listenAddress = listenAddress;
        this.hostId = hostId;
        this.clusterName = clusterName;
        this.dataCenter = dataCenter;
        this.rack = rack;
        this.releaseVersion = releaseVersion;
        this.dseVersion = dseVersion;
        this.schemaVersion = schemaVersion;
        this.tokenCount = tokenCount;
        this.workloads = workloads;
    }
}
