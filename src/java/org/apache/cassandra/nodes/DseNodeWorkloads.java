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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;

/**
 * The workload fields DataStax Enterprise writes for a node in its {@code nodes/local} file, or for a peer in its
 * {@code nodes/peers} file: {@code workloads}, the set of the DSE workloads the node runs ({@code Cassandra},
 * {@code Search}, {@code Analytics}, {@code Graph}, {@code Unknown}), {@code graph}, true when DSE Graph is enabled,
 * and {@code workload}, the legacy single string ({@code Cassandra}, {@code SearchAnalytics}, ...). This software does
 * not import them: a node in {@code internode_compatibility_mode: DSE_6} only checks them ({@code Dse6NonCoreTraces}).
 */
public final class DseNodeWorkloads
{
    /** The only DSE workload this software supports. */
    public static final String CASSANDRA = "Cassandra";

    /** The address of the peer; null for {@code nodes/local}, or for a peer entry without an address. */
    @Nullable
    public final InetAddress peer;
    /** {@code workloads}, empty when absent. */
    public final List<String> workloads;
    /** {@code graph}, null when absent. */
    @Nullable
    public final Boolean graph;
    /** {@code workload}, null when absent. */
    @Nullable
    public final String workload;

    public DseNodeWorkloads(@Nullable InetAddress peer, @Nullable List<String> workloads, @Nullable Boolean graph, @Nullable String workload)
    {
        this.peer = peer;
        this.workloads = workloads == null ? Collections.emptyList() : Collections.unmodifiableList(new ArrayList<>(workloads));
        this.graph = graph;
        this.workload = workload;
    }
}
