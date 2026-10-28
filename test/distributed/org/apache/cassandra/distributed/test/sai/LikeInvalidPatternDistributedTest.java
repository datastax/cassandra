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

import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.index.StubIndex;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A LIKE pattern too complex to be compiled into an automaton is rejected by the coordinator with an invalid request
 * naming the column, whatever index serves the restriction (here a custom, non-SAI one, whose rows the replicas
 * filter with the pattern): it never reaches the replicas.
 */
public class LikeInvalidPatternDistributedTest extends TestBaseImpl
{
    private static final String ERROR = "Invalid pattern for LIKE '<term>' restriction on column v";

    @Test
    public void testRemoteInvalidPatternFailure() throws Exception
    {
        try (Cluster cluster = init(Cluster.build(2).withConfig(config -> config.with(GOSSIP).with(NETWORK)).start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.t (pk int PRIMARY KEY, v text) WITH read_repair = 'NONE'"));
            cluster.schemaChange(withKeyspace("CREATE CUSTOM INDEX ON %s.t(v) USING '" + LikeStubIndex.class.getName() + "'"));

            // rows on the second replica only, so that only the remote read of a coordinator on the first node
            // filters rows with the pattern
            IInvokableInstance replica = cluster.get(2);
            replica.executeInternal(withKeyspace("INSERT INTO %s.t (pk, v) VALUES (1, 'abc')"));
            replica.executeInternal(withKeyspace("INSERT INTO %s.t (pk, v) VALUES (2, 'xyz')"));

            // a valid pattern is filtered on the replicas with the LIKE semantics
            assertRows(cluster.coordinator(1).execute(withKeyspace("SELECT pk FROM %s.t WHERE v LIKE 'a%%c'"), ConsistencyLevel.ALL),
                       row(1));

            // a generic pattern too complex to determinize
            StringBuilder pattern = new StringBuilder();
            for (int i = 0; i < 1000; i++)
                pattern.append("a%");
            pattern.append('a');

            long mark = replica.logs().mark();
            assertThatThrownBy(() -> cluster.coordinator(1).execute(withKeyspace("SELECT pk FROM %s.t WHERE v LIKE ?"), ConsistencyLevel.ALL, pattern.toString()))
                .hasMessageStartingWith(ERROR);

            assertThat(replica.logs().grep(mark, "Invalid pattern").getResult()).isEmpty();
        }
    }

    /**
     * A custom index accepting every LIKE restriction, serving all the rows of the table and leaving the
     * restrictions to the filtering of its results (see {@link StubIndex}).
     */
    public static class LikeStubIndex extends StubIndex
    {
        public LikeStubIndex(ColumnFamilyStore baseCfs, IndexMetadata metadata)
        {
            super(baseCfs, metadata);
        }

        @Override
        public boolean supportsExpression(ColumnMetadata column, Operator operator)
        {
            return operator == Operator.LIKE || operator.isLike() || super.supportsExpression(column, operator);
        }
    }
}
