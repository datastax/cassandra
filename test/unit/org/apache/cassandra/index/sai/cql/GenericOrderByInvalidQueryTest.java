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

package org.apache.cassandra.index.sai.cql;

import java.util.Collections;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class GenericOrderByInvalidQueryTest extends SAITester
{
    @BeforeClass
    public static void setupClass()
    {
        requireNetwork();
    }

    @Test
    public void cannotOrderVarintColumn()
    {
        createTable("CREATE TABLE %s (pk int primary key, val varint)");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
        executeOrderByAndAssertInvalidRequestException("varint");
    }

    @Test
    public void cannotOrderDecimalColumn()
    {
        createTable("CREATE TABLE %s (pk int primary key, val decimal)");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
        executeOrderByAndAssertInvalidRequestException("decimal");
    }

    private void executeOrderByAndAssertInvalidRequestException(String cqlType)
    {
        assertThatThrownBy(() -> execute("SELECT * FROM %s ORDER BY val ASC LIMIT 1"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessage("SAI based ordering on column val of type " + cqlType + " is not supported");
    }

    @Test
    public void cannotOrderTextColumnWithoutIndex()
    {
        createTable("CREATE TABLE %s (pk int, val text, PRIMARY KEY(pk))");

        assertInvalidMessage(String.format(StatementRestrictions.NON_CLUSTER_ORDERING_REQUIRES_INDEX_MESSAGE, "val"),
                             "SELECT * FROM %s ORDER BY val ASC LIMIT 1");
        // Also confirm filtering does not make it work.
        assertInvalidMessage(String.format(StatementRestrictions.NON_CLUSTER_ORDERING_REQUIRES_INDEX_MESSAGE, "val"),
                             "SELECT * FROM %s ORDER BY val LIMIT 5 ALLOW FILTERING");
    }

    @Test
    public void testTextOrderingIsNotAllowedWithClusteringOrdering()
    {
        createTable("CREATE TABLE %s (pk int, ck int, val text, PRIMARY KEY(pk, ck))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");

        assertInvalidMessage("Cannot combine clustering column ordering with non-clustering column ordering",
                             "SELECT * FROM %s ORDER BY val, ck ASC LIMIT 2");
    }

    @Test
    public void textOrderingMustHaveLimit()
    {
        createTable("CREATE TABLE %s (pk int primary key, val text)");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");

        assertInvalidMessage("SAI based ORDER BY clause requires a LIMIT that is not greater than 1000. LIMIT was NO LIMIT",
                             "SELECT * FROM %s ORDER BY val");

    }

    @Test
    public void testInvalidColumnName()
    {
        String table = createTable(KEYSPACE, "CREATE TABLE %s (k int, c int, v int, primary key (k, c))");
        assertInvalidMessage(String.format("Undefined column name bad_col in table %s", KEYSPACE + "." + table),
                             "SELECT k from %s ORDER BY bad_col LIMIT 1");
    }

    @Test
    public void disallowClusteringColumnPredicateWithoutSupportingIndex()
    {
        createTable("CREATE TABLE %s (pk int, num int, v text, PRIMARY KEY(pk, num))");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        execute("INSERT INTO %s (pk, num, v) VALUES (3, 1, 'a')");
        execute("INSERT INTO %s (pk, num, v) VALUES (3, 4, 'b')");
        flush();

        // If we didn't have the query planner fail this query, we would get incorrect results for both queries
        // because the clustering columns are not yet available to restrict the ORDER BY result set.
        assertThatThrownBy(() -> execute("SELECT num FROM %s WHERE pk=3 AND num > 3 ORDER BY v LIMIT 1"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessage(StatementRestrictions.NON_CLUSTER_ORDERING_REQUIRES_ALL_RESTRICTED_NON_PARTITION_KEY_COLUMNS_INDEXED_MESSAGE);

        assertThatThrownBy(() -> execute("SELECT num FROM %s WHERE pk=3 AND num = 4 ORDER BY v LIMIT 1"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessage(StatementRestrictions.NON_CLUSTER_ORDERING_REQUIRES_ALL_RESTRICTED_NON_PARTITION_KEY_COLUMNS_INDEXED_MESSAGE);

        // Cover the alternative code path
        createIndex("CREATE CUSTOM INDEX ON %s(num) USING 'StorageAttachedIndex'");
        assertRows(execute("SELECT num FROM %s WHERE pk=3 AND num > 3 ORDER BY v LIMIT 1"), row(4));
    }

    @Test
    public void canOnlyExecuteWithCorrectConsistencyLevel()
    {
        createTable("CREATE TABLE %s (k int primary key, c int, v text)");
        createIndex("CREATE CUSTOM INDEX ON %s(c) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (k, c, v) VALUES (1, 1, 'a')");
        execute("INSERT INTO %s (k, c, v) VALUES (2, 2, 'b')");
        execute("INSERT INTO %s (k, c, v) VALUES (3, 3, 'c')");

        executeConsistencyLevelQueries("c");
        executeConsistencyLevelQueries("v");
    }

    private void executeConsistencyLevelQueries(String column)
    {
        var query = String.format("SELECT * FROM %%s ORDER BY %s LIMIT 3", column);
        ClientWarn.instance.captureWarnings();
        execute(query);
        ResultSet result = execute(query, ConsistencyLevel.ONE);
        assertEquals(3, result.size());
        assertNull(ClientWarn.instance.getWarnings());

        result = execute(query, ConsistencyLevel.LOCAL_ONE);
        assertEquals(3, result.size());
        assertNull(ClientWarn.instance.getWarnings());

        result = execute(query, ConsistencyLevel.QUORUM);
        assertEquals(3, result.size());
        assertEquals(1, ClientWarn.instance.getWarnings().size());
        assertEquals(String.format(SelectStatement.TOPK_CONSISTENCY_LEVEL_WARNING, ConsistencyLevel.QUORUM, ConsistencyLevel.ONE),
                     ClientWarn.instance.getWarnings().get(0));

        ClientWarn.instance.captureWarnings();
        result = execute(query, ConsistencyLevel.LOCAL_QUORUM);
        assertEquals(3, result.size());
        assertEquals(1, ClientWarn.instance.getWarnings().size());
        assertEquals(String.format(SelectStatement.TOPK_CONSISTENCY_LEVEL_WARNING, ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.LOCAL_ONE),
                     ClientWarn.instance.getWarnings().get(0));

        assertThatThrownBy(() -> execute(query, ConsistencyLevel.SERIAL))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessage(String.format(SelectStatement.TOPK_CONSISTENCY_LEVEL_ERROR, ConsistencyLevel.SERIAL));

        assertThatThrownBy(() -> execute(query, ConsistencyLevel.LOCAL_SERIAL))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessage(String.format(SelectStatement.TOPK_CONSISTENCY_LEVEL_ERROR, ConsistencyLevel.LOCAL_SERIAL));
    }

    protected ResultSet execute(String query, ConsistencyLevel consistencyLevel)
    {
        ClientState state = ClientState.forInternalCalls();
        QueryState queryState = new QueryState(state);

        CQLStatement statement = QueryProcessor.parseStatement(formatQuery(query), queryState.getClientState());
        statement.validate(queryState);

        QueryOptions options = QueryOptions.forInternalCalls(Collections.emptyList());
        options.updateConsistency(consistencyLevel);

        return ((ResultMessage.Rows)statement.execute(queryState, options, System.nanoTime())).result;
    }
}
