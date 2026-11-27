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

package org.apache.cassandra.gms;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.nodes.DseNodeWorkloads;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.JsonUtils;

/**
 * The gossip of the DSE 5.1 compatibility mode ({@code internode_compatibility_mode: DSE_5_1}). DSE 5.1 has the
 * application states of Apache Cassandra 3.11, ordinals 0 to 24 ({@code X1} to {@code X10} at 15 to 24, which it never
 * publishes nor reads), and this software's ordinals 0 to 14 mean the same there; so gossip with a DSE 5.1 node keeps
 * this software's ordinals, and its states at 15 to 20 land in DSE 5.1's unused padding states. A state at an ordinal
 * above {@link #DSE51_MAX_ORDINAL} would make DSE 5.1 fail on the whole gossip message and close the connection, so
 * none is sent to it ({@code EndpointStateSerializer}). DSE 5.1 keeps its own state, a JSON object, in the state at
 * ordinal 9, this software's {@code X_11_PADDING}, as DSE 6 does.
 * <p>
 * DSE 5.1.51 fires its state listeners for every state a gossip message carries, unchanged or not (it does not have
 * CASSANDRA-15097): a peer sending all its states every round would make each DSE 5.1 node handle its {@code STATUS}
 * ("state jump to NORMAL"), update its topology for its {@code DC} and {@code RACK} and rewrite its row of
 * {@code system.peers} every round. A DSE 5.1 node is therefore sent only the states above the version its digest asks
 * for ({@code DseGossipStates.sendsAllOwnStates}), as DSE 5.1 nodes gossip among themselves. For drivers connected to
 * DSE 5.1 nodes to learn this node, its states carry {@code STATUS} (ordinal 0), {@code RPC_ADDRESS} (8, never the
 * any-address: the broadcast native address) and {@code RPC_READY} (14), which this software publishes anyway.
 */
public final class Dse51GossipStates
{
    /** The highest application-state ordinal DSE 5.1 knows, that of its last padding state {@code X10}. */
    public static final int DSE51_MAX_ORDINAL = 24;

    /**
     * The names of DSE's workloads, in the order DSE 5.1 concatenates them into the {@code workloads} entry of its
     * state (reverse alphabetical: {@code SearchGraphCassandra} for a node running Search, Graph and Cassandra).
     */
    static final List<String> WORKLOAD_NAMES = List.of("Search", "Graph", "Cassandra", "Analytics");

    private Dse51GossipStates()
    {
    }

    /**
     * The JSON this node gossips to DSE 5.1 nodes as its DSE state, at the ordinal of {@code X_11_PADDING}: the keys
     * DSE 5.1.51 writes, in its order, as it writes them (the DSE 5.1.51 captures), with {@code dseVersion}
     * ({@code DseCompat#gossipedDseVersion}), the {@code Cassandra} workload in both {@code workloads} and
     * {@code workload}, {@code active} the string {@code "true"}, no graph, and two differences: {@code server_id} is
     * null, where a DSE 5.1.51 node sends its server id (DSE reads that field only when it is not null), and there is
     * no {@code health}, which a DSE 5.1.51 node reads as 0 when it is missing. DSE 6's {@code cluster_name} and
     * {@code partitioner_name} are left out, since DSE 5.1 never reads them. With {@code workloads} and {@code graph}
     * present, DSE 5.1 logs nothing at INFO or above about this state.
     */
    static String publishedState(String dseVersion)
    {
        Map<String, Object> state = new LinkedHashMap<>();
        state.put("dse_version", dseVersion);
        state.put("workloads", DseNodeWorkloads.CASSANDRA);
        state.put("workload", DseNodeWorkloads.CASSANDRA);
        state.put("active", "true");
        state.put("server_id", null);
        state.put("graph", false);
        return JsonUtils.writeAsJsonString(state);
    }

    /**
     * The workloads a DSE 5.1 node gossips in its DSE state, the state at the ordinal of {@code X_11_PADDING}, in the
     * form of the workloads DSE 5.1 stored in its system tables
     * ({@code Dse51SystemTables.StoredColumns#nodeWorkloads}), for the checks of the non-core DSE workloads, which read
     * both ({@code Dse6NonCoreTraces}, once gossip has settled at the start, then periodically), or null when
     * {@code state} is null or has no such state. As DSE 5.1 reads it:
     * {@code workloads} is the names concatenated ({@link #splitWorkloads}), {@code graph} a boolean or its text, and a
     * value that is not a JSON object is the old form of the state (a node started with
     * {@code -Denable-old-dse-state=true}), a bare workload name taken as both {@code workload} and {@code workloads}.
     */
    @Nullable
    public static DseNodeWorkloads gossipedWorkloads(InetAddressAndPort endpoint, @Nullable EndpointState state)
    {
        VersionedValue dseState = state == null ? null : state.getApplicationState(ApplicationState.X_11_PADDING);
        if (dseState == null)
            return null;
        Map<String, Object> entries;
        try
        {
            entries = JsonUtils.fromJsonMap(dseState.value);
        }
        catch (MarshalException e)
        {
            entries = null;
        }
        if (entries == null)
            return new DseNodeWorkloads(endpoint.getAddress(), splitWorkloads(dseState.value), null, dseState.value);
        Object workloads = entries.get("workloads");
        Object workload = entries.get("workload");
        Object graph = entries.get("graph");
        return new DseNodeWorkloads(endpoint.getAddress(),
                                    workloads == null ? Collections.emptyList() : splitWorkloads(workloads.toString()),
                                    graph == null ? null : Boolean.valueOf(graph.toString()),
                                    workload == null ? null : workload.toString());
    }

    /**
     * The workload names of the {@code workloads} entry of a DSE 5.1 node's state, sorted as the {@code workloads} sets
     * of its system tables are: the names DSE 5.1 concatenates ({@link #WORKLOAD_NAMES}), so
     * {@code SearchGraphCassandra} gives {@code [Cassandra, Graph, Search]}. A value that is not made of those names
     * only is returned as one name, as it is, which no check takes for the {@code Cassandra} workload; an empty value
     * gives no name.
     */
    public static List<String> splitWorkloads(String concatenated)
    {
        List<String> names = new ArrayList<>();
        int at = 0;
        while (at < concatenated.length())
        {
            String found = null;
            for (String name : WORKLOAD_NAMES)
            {
                if (concatenated.startsWith(name, at))
                {
                    found = name;
                    break;
                }
            }
            if (found == null)
                return List.of(concatenated);
            if (!names.contains(found))
                names.add(found);
            at += found.length();
        }
        Collections.sort(names);
        return names;
    }
}
