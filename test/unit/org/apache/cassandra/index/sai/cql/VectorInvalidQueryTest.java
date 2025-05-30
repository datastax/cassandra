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

public class VectorInvalidQueryTest extends SAITester
{
    @BeforeClass
    public static void setupClass()
    {
        requireNetwork();
    }

    @Test
    public void cannotCreateEmptyVectorColumn()
    {
        assertThatThrownBy(() -> execute(String.format("CREATE TABLE %s.%s (pk int, str_val text, val vector<float, 0>, PRIMARY KEY(pk))",
                                                       KEYSPACE, createTableName())))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessage("vectors may only have positive dimensions; given 0");
    }

    @Test
    public void cannotIndex1DWithCosine()
    {
        createTable("CREATE TABLE %s (pk int, v vector<float, 1>, PRIMARY KEY(pk))");
        assertThatThrownBy(() -> createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function' : 'cosine'}"))
                .isInstanceOf(InvalidRequestException.class)
                .hasMessage("Cosine similarity is not supported for single-dimension vectors");
    }

    @Test
    public void cannotQueryEmptyVectorColumn() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, str_val text, val vector<float, 3>, PRIMARY KEY(pk))");
        assertThatThrownBy(() -> execute("SELECT similarity_cosine((vector<float, 0>) [], []) FROM %s"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessage("vectors may only have positive dimensions; given 0");
    }

    @Test
    public void cannotInsertWrongNumberOfDimensions()
    {
        createTable("CREATE TABLE %s (pk int, str_val text, val vector<float, 3>, PRIMARY KEY(pk))");

        assertThatThrownBy(() -> execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0])"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessage("Invalid vector literal for val of type vector<float, 3>; expected 3 elements, but given 2");
    }

    @Test
    public void cannotQueryWrongNumberOfDimensions()
    {
        createTable("CREATE TABLE %s (pk int, str_val text, val vector<float, 3>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (1, 'B', [2.0, 3.0, 4.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (2, 'C', [3.0, 4.0, 5.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (3, 'D', [4.0, 5.0, 6.0])");

        assertThatThrownBy(() -> execute("SELECT * FROM %s ORDER BY val ann of [2.5, 3.5] LIMIT 5"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessage("Invalid vector literal for val of type vector<float, 3>; expected 3 elements, but given 2");
    }

    @Test
    public void testMultiVectorOrderingsNotAllowed()
    {
        createTable("CREATE TABLE %s (pk int, str_val text, val1 vector<float, 3>, val2 vector<float, 3>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(str_val) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(val1) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(val2) USING 'StorageAttachedIndex'");

        assertInvalidMessage("Cannot specify more than one ordering column when using SAI indexes",
                             "SELECT * FROM %s ORDER BY val1 ann of [2.5, 3.5, 4.5], val2 ann of [2.1, 3.2, 4.0] LIMIT 2");
    }

    @Test
    public void testDescendingVectorOrderingIsNotAllowed()
    {
        createTable("CREATE TABLE %s (pk int, val vector<float, 3>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");

        assertInvalidMessage("Descending ANN ordering is not supported",
                             "SELECT * FROM %s ORDER BY val ann of [2.5, 3.5, 4.5] DESC LIMIT 2");
    }

    @Test
    public void testVectorOrderingIsNotAllowedWithClusteringOrdering()
    {
        createTable("CREATE TABLE %s (pk int, ck int, val vector<float, 3>, PRIMARY KEY(pk, ck))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");

        assertInvalidMessage("Cannot combine clustering column ordering with non-clustering column ordering",
                             "SELECT * FROM %s ORDER BY val ann of [2.5, 3.5, 4.5], ck ASC LIMIT 2");
    }

    @Test
    public void testVectorOrderingIsNotAllowedWithoutIndex()
    {
        createTable("CREATE TABLE %s (pk int, str_val text, val vector<float, 3>, PRIMARY KEY(pk))");

        assertInvalidMessage(String.format(StatementRestrictions.NON_CLUSTER_ORDERING_REQUIRES_INDEX_MESSAGE, "val"),
                             "SELECT * FROM %s ORDER BY val ann of [2.5, 3.5, 4.5] LIMIT 5");

        assertInvalidMessage(String.format(StatementRestrictions.NON_CLUSTER_ORDERING_REQUIRES_INDEX_MESSAGE, "val"),
                             "SELECT * FROM %s ORDER BY val ann of [2.5, 3.5, 4.5] LIMIT 5 ALLOW FILTERING");
    }

    @Test
    public void testVectorEqualityIsNotAllowed()
    {
        createTable("CREATE TABLE %s (pk int, str_val text, val vector<float, 3>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");

        assertInvalidMessage(StatementRestrictions.VECTOR_INDEXES_UNSUPPORTED_OP_MESSAGE,
                             "SELECT * FROM %s WHERE val = [2.5, 3.5, 4.5] LIMIT 1");

        assertInvalidMessage(StatementRestrictions.VECTOR_INDEXES_UNSUPPORTED_OP_MESSAGE,
                             "SELECT * FROM %s WHERE val = [2.5, 3.5, 4.5]");
    }

    @Test
    public void annOrderingMustHaveLimit()
    {
        createTable("CREATE TABLE %s (pk int, ck int, val vector<float, 3>, PRIMARY KEY(pk, ck))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");

        assertInvalidMessage("SAI based ORDER BY clause requires a LIMIT that is not greater than 1000. LIMIT was NO LIMIT",
                             "SELECT * FROM %s ORDER BY val ann of [2.5, 3.5, 4.5]");

    }

    @Test
    public void testInvalidColumnNameWithAnn()
    {
        String table = createTable(KEYSPACE, "CREATE TABLE %s (k int, c int, v int, primary key (k, c))");
        assertInvalidMessage(String.format("Undefined column name bad_col in table %s", KEYSPACE + "." + table),
                             "SELECT k from %s ORDER BY bad_col ANN OF [1.0] LIMIT 1");
    }

    @Test
    public void disallowZeroVectorsWithCosineSimilarity()
    {
        createTable(KEYSPACE, "CREATE TABLE %s (pk int primary key, value vector<float, 2>)");
        createIndex("CREATE CUSTOM INDEX ON %s(value) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function' : 'cosine'}");

        assertThatThrownBy(() -> execute("INSERT INTO %s (pk, value) VALUES (0, [0.0, 0.0])")).isInstanceOf(InvalidRequestException.class);
        assertThatThrownBy(() -> execute("INSERT INTO %s (pk, value) VALUES (0, ?)", vector(0, 0))).isInstanceOf(InvalidRequestException.class);
        assertThatThrownBy(() -> execute("INSERT INTO %s (pk, value) VALUES (0, ?)", vector(1E-6f, 1E-6f))).isInstanceOf(InvalidRequestException.class);
        assertThatThrownBy(() -> execute("INSERT INTO %s (pk, value) VALUES (0, ?)", vector(1, Float.NaN))).isInstanceOf(InvalidRequestException.class);
        assertThatThrownBy(() -> execute("INSERT INTO %s (pk, value) VALUES (0, ?)", vector(1, Float.POSITIVE_INFINITY))).isInstanceOf(InvalidRequestException.class);
        assertThatThrownBy(() -> execute("INSERT INTO %s (pk, value) VALUES (0, ?)", vector(Float.NEGATIVE_INFINITY, 1))).isInstanceOf(InvalidRequestException.class);
        assertThatThrownBy(() -> execute("SELECT * FROM %s ORDER BY value ann of [0.0, 0.0] LIMIT 2")).isInstanceOf(InvalidRequestException.class);
        assertThatThrownBy(() -> execute("SELECT * FROM %s ORDER BY value ann of ? LIMIT 2", vector(0, 0))).isInstanceOf(InvalidRequestException.class);
        assertThatThrownBy(() -> execute("SELECT * FROM %s ORDER BY value ann of ? LIMIT 2", vector(1E-6f, 1E-6f))).isInstanceOf(InvalidRequestException.class);
        assertThatThrownBy(() -> execute("SELECT * FROM %s ORDER BY value ann of ? LIMIT 2", vector(1, Float.NaN))).isInstanceOf(InvalidRequestException.class);
        assertThatThrownBy(() -> execute("SELECT * FROM %s ORDER BY value ann of ? LIMIT 2", vector(1, Float.POSITIVE_INFINITY))).isInstanceOf(InvalidRequestException.class);
        assertThatThrownBy(() -> execute("SELECT * FROM %s ORDER BY value ann of ? LIMIT 2", vector(Float.NEGATIVE_INFINITY, 1))).isInstanceOf(InvalidRequestException.class);
    }

    @Test
    public void disallowZeroVectorsWithDefaultSimilarity()
    {
        createTable(KEYSPACE, "CREATE TABLE %s (pk int primary key, value vector<float, 2>)");
        createIndex("CREATE CUSTOM INDEX ON %s(value) USING 'StorageAttachedIndex'");

        assertThatThrownBy(() -> execute("INSERT INTO %s (pk, value) VALUES (0, [0.0, 0.0])")).isInstanceOf(InvalidRequestException.class);
        assertThatThrownBy(() -> execute("INSERT INTO %s (pk, value) VALUES (0, ?)", vector(0, 0))).isInstanceOf(InvalidRequestException.class);
        assertThatThrownBy(() -> execute("INSERT INTO %s (pk, value) VALUES (0, ?)", vector(1, Float.NaN))).isInstanceOf(InvalidRequestException.class);
        assertThatThrownBy(() -> execute("INSERT INTO %s (pk, value) VALUES (0, ?)", vector(1, Float.POSITIVE_INFINITY))).isInstanceOf(InvalidRequestException.class);
        assertThatThrownBy(() -> execute("INSERT INTO %s (pk, value) VALUES (0, ?)", vector(Float.NEGATIVE_INFINITY, 1))).isInstanceOf(InvalidRequestException.class);
        assertThatThrownBy(() -> execute("SELECT * FROM %s ORDER BY value ann of [0.0, 0.0] LIMIT 2")).isInstanceOf(InvalidRequestException.class);
        assertThatThrownBy(() -> execute("SELECT * FROM %s ORDER BY value ann of ? LIMIT 2", vector(0, 0))).isInstanceOf(InvalidRequestException.class);
        assertThatThrownBy(() -> execute("SELECT * FROM %s ORDER BY value ann of ? LIMIT 2", vector(1, Float.NaN))).isInstanceOf(InvalidRequestException.class);
        assertThatThrownBy(() -> execute("SELECT * FROM %s ORDER BY value ann of ? LIMIT 2", vector(1, Float.POSITIVE_INFINITY))).isInstanceOf(InvalidRequestException.class);
        assertThatThrownBy(() -> execute("SELECT * FROM %s ORDER BY value ann of ? LIMIT 2", vector(Float.NEGATIVE_INFINITY, 1))).isInstanceOf(InvalidRequestException.class);
    }

    @Test
    public void disallowClusteringColumnPredicateWithoutSupportingIndex()
    {
        createTable("CREATE TABLE %s (pk int, num int, v vector<float, 2>, PRIMARY KEY(pk, num))");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        execute("INSERT INTO %s (pk, num, v) VALUES (3, 1, [1,1])");
        execute("INSERT INTO %s (pk, num, v) VALUES (3, 4, [1,4])");
        flush();

        // If we didn't have the query planner fail this query, we would get incorrect results for both queries
        // because the clustering columns are not yet available to restrict the ANN result set.
        assertThatThrownBy(() -> execute("SELECT num FROM %s WHERE pk=3 AND num > 3 ORDER BY v ANN OF [1,1] LIMIT 1"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessage(StatementRestrictions.NON_CLUSTER_ORDERING_REQUIRES_ALL_RESTRICTED_NON_PARTITION_KEY_COLUMNS_INDEXED_MESSAGE);

        assertThatThrownBy(() -> execute("SELECT num FROM %s WHERE pk=3 AND num = 4 ORDER BY v ANN OF [1,1] LIMIT 1"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessage(StatementRestrictions.NON_CLUSTER_ORDERING_REQUIRES_ALL_RESTRICTED_NON_PARTITION_KEY_COLUMNS_INDEXED_MESSAGE);

        // Cover the alternative code path
        createIndex("CREATE CUSTOM INDEX ON %s(num) USING 'StorageAttachedIndex'");
        assertRows(execute("SELECT num FROM %s WHERE pk=3 AND num > 3 ORDER BY v ANN OF [1,1] LIMIT 1"), row(4));
    }

    @Test
    public void cannotHaveAggregationOnANNQuery()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v vector<float, 1>, c int)");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function' : 'euclidean'}");

        execute("INSERT INTO %s (k, v, c) VALUES (1, [4], 1)");
        execute("INSERT INTO %s (k, v, c) VALUES (2, [3], 10)");
        execute("INSERT INTO %s (k, v, c) VALUES (3, [2], 100)");
        execute("INSERT INTO %s (k, v, c) VALUES (4, [1], 1000)");

        assertThatThrownBy(() -> execute("SELECT sum(c) FROM %s ORDER BY v ANN OF [0] LIMIT 4"))
                .isInstanceOf(InvalidRequestException.class)
                .hasMessage(SelectStatement.TOPK_AGGREGATION_ERROR);

        assertThatThrownBy(() -> execute("SELECT sum(c) FROM %s WHERE k = 1 ORDER BY v ANN OF [0] LIMIT 4"))
                .isInstanceOf(InvalidRequestException.class)
                .hasMessage(SelectStatement.TOPK_AGGREGATION_ERROR);

        assertThatThrownBy(() -> execute("SELECT * FROM %s GROUP BY k ORDER BY v ANN OF [0] LIMIT 4"))
                .isInstanceOf(InvalidRequestException.class)
                .hasMessage(SelectStatement.TOPK_AGGREGATION_ERROR);

        assertThatThrownBy(() -> execute("SELECT count(*) FROM %s ORDER BY v ANN OF [0] LIMIT 4"))
                .isInstanceOf(InvalidRequestException.class)
                .hasMessage(SelectStatement.TOPK_AGGREGATION_ERROR);
    }

    @Test
    public void canOnlyExecuteWithCorrectConsistencyLevel()
    {
        createTable("CREATE TABLE %s (k int, c int, v vector<float, 1>, PRIMARY KEY(k, c))");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function' : 'euclidean'}");

        execute("INSERT INTO %s (k, c, v) VALUES (1, 1, [1])");
        execute("INSERT INTO %s (k, c, v) VALUES (1, 2, [2])");
        execute("INSERT INTO %s (k, c, v) VALUES (1, 3, [3])");

        ClientWarn.instance.captureWarnings();
        execute("SELECT * FROM %s ORDER BY v ANN OF [2] LIMIT 3");
        ResultSet result = execute("SELECT * FROM %s ORDER BY v ANN OF [2] LIMIT 3", ConsistencyLevel.ONE);
        assertEquals(3, result.size());
        assertNull(ClientWarn.instance.getWarnings());

        result = execute("SELECT * FROM %s ORDER BY v ANN OF [2] LIMIT 3", ConsistencyLevel.LOCAL_ONE);
        assertEquals(3, result.size());
        assertNull(ClientWarn.instance.getWarnings());

        result = execute("SELECT * FROM %s ORDER BY v ANN OF [2] LIMIT 3", ConsistencyLevel.QUORUM);
        assertEquals(3, result.size());
        assertEquals(1, ClientWarn.instance.getWarnings().size());
        assertEquals(String.format(SelectStatement.TOPK_CONSISTENCY_LEVEL_WARNING, ConsistencyLevel.QUORUM, ConsistencyLevel.ONE),
                     ClientWarn.instance.getWarnings().get(0));

        ClientWarn.instance.captureWarnings();
        result = execute("SELECT * FROM %s ORDER BY v ANN OF [2] LIMIT 3", ConsistencyLevel.LOCAL_QUORUM);
        assertEquals(3, result.size());
        assertEquals(1, ClientWarn.instance.getWarnings().size());
        assertEquals(String.format(SelectStatement.TOPK_CONSISTENCY_LEVEL_WARNING, ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.LOCAL_ONE),
                     ClientWarn.instance.getWarnings().get(0));

        assertThatThrownBy(() -> execute("SELECT * FROM %s ORDER BY v ANN OF [2] LIMIT 3", ConsistencyLevel.SERIAL))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessage(String.format(SelectStatement.TOPK_CONSISTENCY_LEVEL_ERROR, ConsistencyLevel.SERIAL));

        assertThatThrownBy(() -> execute("SELECT * FROM %s ORDER BY v ANN OF [2] LIMIT 3", ConsistencyLevel.LOCAL_SERIAL))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessage(String.format(SelectStatement.TOPK_CONSISTENCY_LEVEL_ERROR, ConsistencyLevel.LOCAL_SERIAL));
    }

    protected ResultSet execute(String query, ConsistencyLevel consistencyLevel)
    {
        return execute(query, consistencyLevel, -1);
    }

    protected ResultSet execute(String query, int pageSize)
    {
        return execute(query, ConsistencyLevel.ONE, pageSize);
    }

    protected ResultSet execute(String query, ConsistencyLevel consistencyLevel, int pageSize)
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
