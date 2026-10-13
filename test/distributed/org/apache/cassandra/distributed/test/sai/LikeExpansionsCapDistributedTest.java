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

package org.apache.cassandra.distributed.test.sai;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.exceptions.RequestFailureReason;

import static org.apache.cassandra.config.CassandraRelevantProperties.SAI_MAX_AUTOMATON_EXPANSIONS;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A SAI LIKE pattern exceeding the automaton expansions cap on a remote replica fails the read with the
 * {@link RequestFailureReason#SAI_AUTOMATON_EXPANSIONS_EXCEEDED} reason, sent over the wire to the coordinator and
 * reported to the client, while the replica logs it as a warning rather than as a fatal error of its read stage.
 */
public class LikeExpansionsCapDistributedTest extends TestBaseImpl
{
    private static final String WARNING = "visited too many indexed terms";

    @Test
    public void testRemoteExpansionsCapFailure() throws Exception
    {
        // the property is read at query time by both nodes, which share the JVM system properties
        try (WithProperties properties = new WithProperties().set(SAI_MAX_AUTOMATON_EXPANSIONS, "16");
             Cluster cluster = init(Cluster.build(2).withConfig(config -> config.with(GOSSIP).with(NETWORK)).start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.t (pk int PRIMARY KEY, v text) WITH read_repair = 'NONE'"));
            cluster.schemaChange(withKeyspace("CREATE CUSTOM INDEX ON %s.t(v) USING 'StorageAttachedIndex'"));
            SAIUtil.waitForIndexQueryableOnAllNodes(cluster, KEYSPACE);

            // a single row on both replicas, and many more terms on the second replica only, so that only the remote
            // read of a coordinator on the first node exceeds the cap
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.t (pk, v) VALUES (0, 'zzza')"), ConsistencyLevel.ALL);
            IInvokableInstance replica = cluster.get(2);
            for (int pk = 1; pk <= 64; pk++)
                replica.executeInternal(withKeyspace("INSERT INTO %s.t (pk, v) VALUES (?, ?)"), pk, String.format("term%02da", pk));

            // a pattern bounded by its common prefix stays under the cap on both replicas
            assertRows(cluster.coordinator(1).execute(withKeyspace("SELECT pk FROM %s.t WHERE v LIKE 'zz%%a'"), ConsistencyLevel.ALL),
                       row(0));

            long mark = replica.logs().mark();
            String address = replica.config().broadcastAddress().getAddress().getHostAddress();
            assertThatThrownBy(() -> cluster.coordinator(1).execute(withKeyspace("SELECT pk FROM %s.t WHERE v LIKE '%%a'"), ConsistencyLevel.ALL))
                .hasMessageContaining(RequestFailureReason.SAI_AUTOMATON_EXPANSIONS_EXCEEDED.name() + " from /" + address)
                .hasMessageContaining("use a more selective pattern");

            assertThat(replica.logs().grep(mark, WARNING).getResult()).isNotEmpty();
            assertThat(replica.logs().grep(mark, "Fatal exception").getResult()).isEmpty();
            assertThat(replica.logs().grep(mark, "AutomatonTermsExceededException").getResult()).isEmpty();
        }
    }
}
