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

package org.apache.cassandra.distributed.test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.PageSize;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.FBUtilities;

/**
 * End-to-end integration coverage for {@link SelectStatement#executeWithReadQuery}: a real two-node cluster
 * (RF=2), reads coordinated through {@link org.apache.cassandra.service.StorageProxy}, and the continuation
 * {@link PagingState} round-tripped through its wire format between pages. Asserts that an externally-ordered
 * multi-partition read is returned in the pager's command order — not token order — across page boundaries,
 * with every row delivered exactly once.
 */
public class SelectStatementExecuteWithPagerDistributedTest extends TestBaseImpl
{
    @Test
    public void pagesExternallyOrderedReadAcrossNodes() throws Exception
    {
        try (Cluster cluster = init(builder().withNodes(2).start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int PRIMARY KEY, v text)"));

            final int rows = 12;
            for (int pk = 0; pk < rows; pk++)
                cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.tbl (pk, v) VALUES (?, ?)"),
                                               ConsistencyLevel.ALL, pk, "v" + pk);

            final String ks = KEYSPACE;

            // All assertions run inside the instance; a thrown AssertionError propagates as a test failure.
            cluster.get(1).runOnInstance(() -> {
                SelectStatement select = (SelectStatement) org.apache.cassandra.cql3.QueryProcessor.parseStatement(
                    "SELECT pk, v FROM " + ks + ".tbl WHERE pk IN (0,1,2,3,4,5,6,7,8,9,10,11)",
                    ClientState.forInternalCalls());

                long nowInSec = FBUtilities.nowInSeconds();
                SinglePartitionReadCommand.Group base =
                    (SinglePartitionReadCommand.Group) select.getQuery(probe(null), nowInSec);

                // Reverse the (token-ordered) command list, so a token-ordered result would fail this test.
                List<SinglePartitionReadCommand> reversed = new ArrayList<>(base.queries);
                Collections.reverse(reversed);
                SinglePartitionReadCommand.Group group =
                    SinglePartitionReadCommand.Group.create(reversed, base.limits());

                List<Integer> expected = new ArrayList<>();
                for (SinglePartitionReadCommand c : reversed)
                    expected.add(Int32Type.instance.compose(c.partitionKey().getKey()));

                List<Integer> got = new ArrayList<>();
                PagingState state = null;
                int guard = 0;
                do
                {
                    QueryOptions opts = probe(state);
                    ResultMessage.Rows msg = select.executeWithReadQuery(QueryState.forInternalCalls(), opts, group,
                                                                         Dispatcher.RequestTime.forImmediateExecution());
                    if (msg.result.rows.size() > 5)
                        throw new AssertionError("page exceeded requested size: " + msg.result.rows.size());
                    for (List<ByteBuffer> row : msg.result.rows)
                        got.add(Int32Type.instance.compose(row.get(0)));

                    PagingState next = msg.result.metadata.getPagingState();
                    state = next == null ? null
                                         : PagingState.deserialize(next.serialize(ProtocolVersion.CURRENT), ProtocolVersion.CURRENT);
                    if (++guard > 100)
                        throw new AssertionError("paging did not terminate");
                }
                while (state != null);

                if (!expected.equals(got))
                    throw new AssertionError("expected pager order " + expected + " but got " + got);
            });
        }
    }

    /**
     * A page-size-5 read at CL ALL (RF=2), resuming from {@code state} (null on the first page). CL ALL forces
     * every partition to be read from both replicas, so the coordinator on node1 genuinely fans out to node2.
     */
    private static QueryOptions probe(PagingState state)
    {
        return QueryOptions.create(org.apache.cassandra.db.ConsistencyLevel.ALL,
                                   Collections.emptyList(),
                                   false,
                                   PageSize.inRows(5),
                                   state,
                                   null,
                                   ProtocolVersion.CURRENT,
                                   null);
    }
}
