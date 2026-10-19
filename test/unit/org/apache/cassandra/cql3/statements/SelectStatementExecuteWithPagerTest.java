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

package org.apache.cassandra.cql3.statements;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.PageSize;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link SelectStatement#executeWithReadQuery}: running a {@code SELECT} against a
 * caller-supplied, externally-ordered {@link org.apache.cassandra.db.ReadQuery} and paging the result. The
 * recurring theme is that the query's command order — <em>not</em> token order — is what the result follows,
 * across page boundaries (including boundaries that fall inside a partition), while selection/limit/aggregation
 * behave exactly as the normal read path.
 */
public class SelectStatementExecuteWithPagerTest extends CQLTester
{
    private static final Dispatcher.RequestTime NOW = Dispatcher.RequestTime.forImmediateExecution();

    @BeforeClass
    public static void setUp()
    {
        // executeWithReadQuery reads through the distributed path (StorageProxy), so the embedded node must be a
        // live NORMAL replica. CQLTester's default in-process execute() uses the internal read path, which does
        // not require this.
        requireNetworkWithoutDriver();
    }

    private QueryOptions options(PageSize pageSize, PagingState pagingState)
    {
        return QueryOptions.create(ConsistencyLevel.ONE,
                                   Collections.emptyList(),
                                   false,
                                   pageSize,
                                   pagingState,
                                   null,
                                   ProtocolVersion.CURRENT,
                                   null);
    }

    private SelectStatement parseSelect(String cql)
    {
        return (SelectStatement) parseStatement(cql);
    }

    private static int pkOf(SinglePartitionReadCommand cmd)
    {
        return Int32Type.instance.compose(cmd.partitionKey().getKey());
    }

    /** Rebuild a {@code Group} whose commands are in the exact partition order of {@code pkOrder}. */
    private SinglePartitionReadCommand.Group reorderedGroup(SelectStatement select, long nowInSec, int... pkOrder)
    {
        SinglePartitionReadCommand.Group base =
            (SinglePartitionReadCommand.Group) select.getQuery(options(PageSize.NONE, null), nowInSec);

        Map<Integer, SinglePartitionReadCommand> byPk = new HashMap<>();
        for (SinglePartitionReadCommand cmd : base.queries)
            byPk.put(pkOf(cmd), cmd);

        List<SinglePartitionReadCommand> ordered = new ArrayList<>(pkOrder.length);
        for (int pk : pkOrder)
        {
            SinglePartitionReadCommand cmd = byPk.get(pk);
            assertNotNull("no read command for pk=" + pk, cmd);
            ordered.add(cmd);
        }
        return SinglePartitionReadCommand.Group.create(ordered, base.limits());
    }

    /** Round-trip a paging state through its wire format, as the native protocol would between pages. */
    private PagingState roundTrip(PagingState state)
    {
        if (state == null)
            return null;
        return PagingState.deserialize(state.serialize(ProtocolVersion.CURRENT), ProtocolVersion.CURRENT);
    }

    private ResultMessage.Rows onePage(SelectStatement select, SinglePartitionReadCommand.Group group, QueryOptions opts)
    {
        return select.executeWithReadQuery(QueryState.forInternalCalls(), opts, group, NOW);
    }

    /**
     * Page {@code group} fully, collecting every row (decoded by {@code decoder}) in the order returned and
     * asserting each page holds at most {@code pageRows}. The continuation state is round-tripped through the
     * wire format between pages and the group is rebuilt implicitly (same instance) each time.
     */
    private <T> List<T> pageAll(SelectStatement select, SinglePartitionReadCommand.Group group, int pageRows,
                                java.util.function.Function<List<ByteBuffer>, T> decoder)
    {
        List<T> got = new ArrayList<>();
        PagingState state = null;
        int guard = 0;
        do
        {
            ResultMessage.Rows msg = onePage(select, group, options(PageSize.inRows(pageRows), state));
            assertTrue("page exceeded requested size", msg.result.rows.size() <= pageRows);
            for (List<ByteBuffer> row : msg.result.rows)
                got.add(decoder.apply(row));
            state = roundTrip(msg.result.metadata.getPagingState());
            assertTrue("paging did not terminate", ++guard < 1000);
        }
        while (state != null);
        return got;
    }

    private static int intCol(List<ByteBuffer> row, int i)
    {
        return Int32Type.instance.compose(row.get(i));
    }

    @Test
    public void pagerOrderIsPreservedAcrossPages() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text)");
        for (int pk = 0; pk < 10; pk++)
            execute("INSERT INTO %s (pk, v) VALUES (?, ?)", pk, "v" + pk);

        SelectStatement select = parseSelect("SELECT pk, v FROM %s WHERE pk IN (0,1,2,3,4,5,6,7,8,9)");
        long nowInSec = FBUtilities.nowInSeconds();

        // A deliberately non-token order, so a token-ordered result would fail this test.
        int[] order = { 7, 2, 9, 0, 5, 1, 8, 3, 6, 4 };
        SinglePartitionReadCommand.Group group = reorderedGroup(select, nowInSec, order);

        List<Integer> got = pageAll(select, group, 3, row -> intCol(row, 0));

        List<Integer> expected = new ArrayList<>();
        for (int pk : order) expected.add(pk);
        assertEquals("rows must follow the query's command order across pages", expected, got);
    }

    @Test
    public void singlePageWhenPageSizeExceedsResult() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text)");
        for (int pk = 0; pk < 5; pk++)
            execute("INSERT INTO %s (pk, v) VALUES (?, ?)", pk, "v" + pk);

        SelectStatement select = parseSelect("SELECT pk, v FROM %s WHERE pk IN (0,1,2,3,4)");
        long nowInSec = FBUtilities.nowInSeconds();
        SinglePartitionReadCommand.Group group = reorderedGroup(select, nowInSec, 4, 3, 2, 1, 0);

        // One big page: everything comes back at once and there is no continuation.
        ResultMessage.Rows msg = onePage(select, group, options(PageSize.inRows(1000), null));
        List<Integer> got = new ArrayList<>();
        for (List<ByteBuffer> row : msg.result.rows) got.add(intCol(row, 0));

        assertEquals(List.of(4, 3, 2, 1, 0), got);
        assertNull("no continuation expected for a complete single page", msg.result.metadata.getPagingState());
    }

    @Test
    public void userLimitIsApplied() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text)");
        for (int pk = 0; pk < 10; pk++)
            execute("INSERT INTO %s (pk, v) VALUES (?, ?)", pk, "v" + pk);

        SelectStatement select = parseSelect("SELECT pk, v FROM %s WHERE pk IN (0,1,2,3,4,5,6,7,8,9) LIMIT 4");
        long nowInSec = FBUtilities.nowInSeconds();
        int[] order = { 7, 2, 9, 0, 5, 1, 8, 3, 6, 4 };
        SinglePartitionReadCommand.Group group = reorderedGroup(select, nowInSec, order);

        List<Integer> got = pageAll(select, group, 3, row -> intCol(row, 0));

        // Exactly LIMIT rows, and they are the first LIMIT of the query's order.
        assertEquals("LIMIT must cap the total across pages", List.of(7, 2, 9, 0), got);
    }

    @Test
    public void offsetSkipsLeadingRowsInQueryOrder() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text)");
        for (int pk = 0; pk < 10; pk++)
            execute("INSERT INTO %s (pk, v) VALUES (?, ?)", pk, "v" + pk);

        // OFFSET is applied at result build time in the query's order: skip 2, take 4.
        SelectStatement select = parseSelect("SELECT pk, v FROM %s WHERE pk IN (0,1,2,3,4,5,6,7,8,9) LIMIT 4 OFFSET 2");
        long nowInSec = FBUtilities.nowInSeconds();
        int[] order = { 7, 2, 9, 0, 5, 1, 8, 3, 6, 4 };
        SinglePartitionReadCommand.Group group = reorderedGroup(select, nowInSec, order);

        // OFFSET disables user-facing paging (readAll), so the answer arrives in one response.
        ResultMessage.Rows msg = onePage(select, group, options(PageSize.inRows(1000), null));
        List<Integer> got = new ArrayList<>();
        for (List<ByteBuffer> row : msg.result.rows) got.add(intCol(row, 0));

        assertEquals(List.of(9, 0, 5, 1), got);
    }

    @Test
    public void perPartitionLimitIsApplied() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v text, PRIMARY KEY (pk, ck))");
        for (int pk = 0; pk < 2; pk++)
            for (int ck = 0; ck < 3; ck++)
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, ck, "v" + pk + "_" + ck);

        SelectStatement select = parseSelect("SELECT pk, ck FROM %s WHERE pk IN (0,1) PER PARTITION LIMIT 2");
        long nowInSec = FBUtilities.nowInSeconds();
        SinglePartitionReadCommand.Group group = reorderedGroup(select, nowInSec, 1, 0);

        List<int[]> got = pageAllPairs(select, group, 10);
        assertPairs(new int[][]{ {1, 0}, {1, 1}, {0, 0}, {0, 1} }, got);
    }

    @Test
    public void clusteringRowsFollowPartitionOrderThenClusteringOrder() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v text, PRIMARY KEY (pk, ck))");
        for (int pk = 0; pk < 3; pk++)
            for (int ck = 0; ck < 2; ck++)
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, ck, "v" + pk + "_" + ck);

        SelectStatement select = parseSelect("SELECT pk, ck, v FROM %s WHERE pk IN (0,1,2)");
        long nowInSec = FBUtilities.nowInSeconds();
        SinglePartitionReadCommand.Group group = reorderedGroup(select, nowInSec, 2, 0, 1);

        // page size 2 == exactly one (two-row) partition per page here
        List<int[]> got = pageAllPairs(select, group, 2);
        assertPairs(new int[][]{ {2, 0}, {2, 1}, {0, 0}, {0, 1}, {1, 0}, {1, 1} }, got);
    }

    @Test
    public void pageBoundaryFallingInsideAPartitionResumesMidPartition() throws Throwable
    {
        // Partitions are 3 rows wide but the page is 2 rows, so every partition is split across a page boundary
        // and the round-tripped paging state must resume in the middle of a partition.
        createTable("CREATE TABLE %s (pk int, ck int, v text, PRIMARY KEY (pk, ck))");
        for (int pk = 0; pk < 2; pk++)
            for (int ck = 0; ck < 3; ck++)
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, ck, "v" + pk + "_" + ck);

        SelectStatement select = parseSelect("SELECT pk, ck, v FROM %s WHERE pk IN (0,1)");
        long nowInSec = FBUtilities.nowInSeconds();
        SinglePartitionReadCommand.Group group = reorderedGroup(select, nowInSec, 1, 0);

        List<int[]> got = pageAllPairs(select, group, 2);
        assertPairs(new int[][]{ {1, 0}, {1, 1}, {1, 2}, {0, 0}, {0, 1}, {0, 2} }, got);
    }

    @Test
    public void aggregationCountIsComputed() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text)");
        for (int pk = 0; pk < 6; pk++)
            execute("INSERT INTO %s (pk, v) VALUES (?, ?)", pk, "v" + pk);

        SelectStatement select = parseSelect("SELECT count(*) FROM %s WHERE pk IN (0,1,2,3,4,5)");
        long nowInSec = FBUtilities.nowInSeconds();
        SinglePartitionReadCommand.Group group =
            (SinglePartitionReadCommand.Group) select.getQuery(options(PageSize.NONE, null), nowInSec);

        ResultMessage.Rows msg = onePage(select, group, options(PageSize.inRows(2), null));

        assertEquals(1, msg.result.rows.size());
        assertEquals(6L, (long) LongType.instance.compose(msg.result.rows.get(0).get(0)));
    }

    @Test
    public void functionProjectionRoutesThroughSelection() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text)");
        for (int pk = 0; pk < 4; pk++)
            execute("INSERT INTO %s (pk, v) VALUES (?, ?)", pk, "v" + pk);

        // writetime(v) is synthesised by the Selection layer; here it must just work.
        SelectStatement select = parseSelect("SELECT pk, writetime(v) FROM %s WHERE pk IN (0,1,2,3)");
        long nowInSec = FBUtilities.nowInSeconds();
        SinglePartitionReadCommand.Group group = reorderedGroup(select, nowInSec, 3, 1, 2, 0);

        ResultMessage.Rows msg = onePage(select, group, options(PageSize.inRows(10), null));

        assertEquals(4, msg.result.rows.size());
        List<Integer> pks = new ArrayList<>();
        for (List<ByteBuffer> row : msg.result.rows)
        {
            pks.add(intCol(row, 0));
            assertNotNull("writetime column must be present", row.get(1));
        }
        assertEquals(List.of(3, 1, 2, 0), pks);
    }

    @Test
    public void emptyQueryYieldsNoRowsAndNoContinuation() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text)");
        execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 1, "v1");

        // Keys that do not exist.
        SelectStatement select = parseSelect("SELECT pk, v FROM %s WHERE pk IN (100,101,102)");
        long nowInSec = FBUtilities.nowInSeconds();
        SinglePartitionReadCommand.Group group = reorderedGroup(select, nowInSec, 100, 101, 102);

        ResultMessage.Rows msg = onePage(select, group, options(PageSize.inRows(2), null));

        assertTrue(msg.result.rows.isEmpty());
        assertNull(msg.result.metadata.getPagingState());
    }

    @Test
    public void nullReadQueryIsRejected() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text)");
        SelectStatement select = parseSelect("SELECT pk, v FROM %s WHERE pk = 1");
        QueryOptions opts = options(PageSize.inRows(2), null);
        assertThrows(NullPointerException.class,
                     () -> select.executeWithReadQuery(QueryState.forInternalCalls(), opts, null, NOW));
    }

    // --- (pk, ck) pair helpers -------------------------------------------------

    private List<int[]> pageAllPairs(SelectStatement select, SinglePartitionReadCommand.Group group, int pageRows)
    {
        return pageAll(select, group, pageRows, row -> new int[]{ intCol(row, 0), intCol(row, 1) });
    }

    private static void assertPairs(int[][] expected, List<int[]> got)
    {
        assertEquals("row count", expected.length, got.size());
        for (int i = 0; i < expected.length; i++)
        {
            assertEquals("pk at " + i, expected[i][0], got.get(i)[0]);
            assertEquals("ck at " + i, expected[i][1], got.get(i)[1]);
        }
    }
}
