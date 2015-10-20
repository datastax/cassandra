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
package org.apache.cassandra.cql3;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.exceptions.CassandraException;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.service.ClientState;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;

import com.google.common.base.Objects;

import static org.apache.cassandra.cql3.QueryProcessor.process;
import static org.apache.cassandra.cql3.QueryProcessor.processInternal;
import static org.junit.Assert.assertEquals;

public class SelectionOrderingTest
{
    private static final Logger logger = LoggerFactory.getLogger(SelectWithTokenFunctionTest.class);
    static ClientState clientState;
    static String keyspace = "select_with_ordering_test";

    @BeforeClass
    public static void setUpClass() throws Throwable
    {
        SchemaLoader.loadSchema();
        executeSchemaChange("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}");
        executeSchemaChange("CREATE TABLE IF NOT EXISTS %s.single_clustering (a int, b int, c int, PRIMARY KEY (a, b))");
        executeSchemaChange("CREATE TABLE IF NOT EXISTS %s.single_clustering_desc (a int, b int, c int, PRIMARY KEY (a, b)) WITH CLUSTERING ORDER BY (b DESC)");
        executeSchemaChange("CREATE TABLE IF NOT EXISTS %s.multiple_clustering (a int, b int, c int, d int, PRIMARY KEY (a, b, c))");
        clientState = ClientState.forInternalCalls();
    }

    @AfterClass
    public static void stopGossiper()
    {
        Gossiper.instance.stop();
    }

    private static void executeSchemaChange(String query) throws Throwable
    {
        try
        {
            process(String.format(query, keyspace), ConsistencyLevel.ONE);
        }
        catch (RuntimeException exc)
        {
            throw exc.getCause();
        }
    }

    private static UntypedResultSet execute(String query) throws Throwable
    {
        try
        {
            return processInternal(String.format(query, keyspace));
        }
        catch (RuntimeException exc)
        {
            if (exc.getCause() != null)
                throw exc.getCause();
            throw exc;
        }
    }

    @Test
    public void testNormalSelectionOrderSingleClustering() throws Throwable
    {
        for (String descOption : new String[]{"", "_desc"})
        {
            execute("INSERT INTO %s.single_clustering" + descOption + " (a, b, c) VALUES (0, 0, 0)");
            execute("INSERT INTO %s.single_clustering" + descOption + " (a, b, c) VALUES (0, 1, 1)");
            execute("INSERT INTO %s.single_clustering" + descOption + " (a, b, c) VALUES (0, 2, 2)");

            try
            {
                UntypedResultSet results = execute("SELECT * FROM %s.single_clustering" + descOption + " WHERE a=0 ORDER BY b ASC");
                assertEquals(3, results.size());
                Iterator<UntypedResultSet.Row> rows = results.iterator();
                for (int i = 0; i < 3; i++)
                    assertEquals(i, rows.next().getInt("b"));

                results = execute("SELECT * FROM %s.single_clustering" + descOption + " WHERE a=0 ORDER BY b DESC");
                assertEquals(3, results.size());
                rows = results.iterator();
                for (int i = 2; i >= 0; i--)
                    assertEquals(i, rows.next().getInt("b"));

                // order by the only column in the selection
                results = execute("SELECT b FROM %s.single_clustering" + descOption + " WHERE a=0 ORDER BY b ASC");
                assertEquals(3, results.size());
                rows = results.iterator();
                for (int i = 0; i < 3; i++)
                    assertEquals(i, rows.next().getInt("b"));

                results = execute("SELECT b FROM %s.single_clustering" + descOption + " WHERE a=0 ORDER BY b DESC");
                assertEquals(3, results.size());
                rows = results.iterator();
                for (int i = 2; i >= 0; i--)
                    assertEquals(i, rows.next().getInt("b"));

                // order by a column not in the selection
                results = execute("SELECT c FROM %s.single_clustering" + descOption + " WHERE a=0 ORDER BY b ASC");
                assertEquals(3, results.size());
                rows = results.iterator();
                for (int i = 0; i < 3; i++)
                    assertEquals(i, rows.next().getInt("c"));

                results = execute("SELECT c FROM %s.single_clustering" + descOption + " WHERE a=0 ORDER BY b DESC");
                assertEquals(3, results.size());
                rows = results.iterator();
                for (int i = 2; i >= 0; i--)
                    assertEquals(i, rows.next().getInt("c"));
            }
            finally
            {
                execute("DELETE FROM %s.single_clustering" + descOption + " WHERE a = 0");
            }
        }
    }

    @Test
    public void testFunctionSelectionOrderSingleClustering() throws Throwable
    {
        for (String descOption : new String[]{"", "_desc"})
        {
            execute("INSERT INTO %s.single_clustering" + descOption + " (a, b, c) VALUES (0, 0, 0)");
            execute("INSERT INTO %s.single_clustering" + descOption + " (a, b, c) VALUES (0, 1, 1)");
            execute("INSERT INTO %s.single_clustering" + descOption + " (a, b, c) VALUES (0, 2, 2)");

            try
            {
                // order by a column in the selection (wrapped in a function)
                UntypedResultSet results = execute("SELECT blobAsInt(intAsBlob(b)) as col FROM %s.single_clustering" + descOption + " WHERE a=0 ORDER BY b ASC");
                assertEquals(3, results.size());
                Iterator<UntypedResultSet.Row> rows = results.iterator();
                for (int i = 0; i < 3; i++)
                    assertEquals(i, rows.next().getInt("col"));

                results = execute("SELECT blobAsInt(intAsBlob(b)) as col FROM %s.single_clustering" + descOption + " WHERE a=0 ORDER BY b DESC");
                assertEquals(3, results.size());
                rows = results.iterator();
                for (int i = 2; i >= 0; i--)
                    assertEquals(i, rows.next().getInt("col"));

                // order by a column in the selection, plus the column wrapped in a function
                results = execute("SELECT b, blobAsInt(intAsBlob(b)) as col FROM %s.single_clustering" + descOption + " WHERE a=0 ORDER BY b ASC");
                assertEquals(3, results.size());
                rows = results.iterator();
                for (int i = 0; i < 3; i++)
                {
                    UntypedResultSet.Row row = rows.next();
                    assertEquals(i, row.getInt("b"));
                    assertEquals(i, row.getInt("col"));
                }

                results = execute("SELECT b, blobAsInt(intAsBlob(b)) as col FROM %s.single_clustering" + descOption + " WHERE a=0 ORDER BY b DESC");
                assertEquals(3, results.size());
                rows = results.iterator();
                for (int i = 2; i >= 0; i--)
                {
                    UntypedResultSet.Row row = rows.next();
                    assertEquals(i, row.getInt("b"));
                    assertEquals(i, row.getInt("col"));
                }

                // order by a column not in the selection (wrapped in a function)
                results = execute("SELECT blobAsInt(intAsBlob(c)) as col FROM %s.single_clustering" + descOption + " WHERE a=0 ORDER BY b ASC");
                assertEquals(3, results.size());
                rows = results.iterator();
                for (int i = 0; i < 3; i++)
                    assertEquals(i, rows.next().getInt("col"));

                results = execute("SELECT blobAsInt(intAsBlob(c)) as col FROM %s.single_clustering" + descOption + " WHERE a=0 ORDER BY b DESC");
                assertEquals(3, results.size());
                rows = results.iterator();
                for (int i = 2; i >= 0; i--)
                    assertEquals(i, rows.next().getInt("col"));
            }
            finally
            {
                execute("DELETE FROM %s.single_clustering" + descOption + " WHERE a = 0");
            }
        }
    }

    @Test
    public void testNormalSelectionOrderMultipleClustering() throws Throwable
    {
        execute("INSERT INTO %s.multiple_clustering (a, b, c, d) VALUES (0, 0, 0, 0)");
        execute("INSERT INTO %s.multiple_clustering (a, b, c, d) VALUES (0, 0, 1, 1)");
        execute("INSERT INTO %s.multiple_clustering (a, b, c, d) VALUES (0, 0, 2, 2)");
        execute("INSERT INTO %s.multiple_clustering (a, b, c, d) VALUES (0, 1, 0, 3)");
        execute("INSERT INTO %s.multiple_clustering (a, b, c, d) VALUES (0, 1, 1, 4)");
        execute("INSERT INTO %s.multiple_clustering (a, b, c, d) VALUES (0, 1, 2, 5)");
        try
        {
            UntypedResultSet results = execute("SELECT * FROM %s.multiple_clustering WHERE a=0 ORDER BY b ASC");
            assertEquals(6, results.size());
            Iterator<UntypedResultSet.Row> rows = results.iterator();
            assertEquals(0, rows.next().getInt("d"));
            assertEquals(1, rows.next().getInt("d"));
            assertEquals(2, rows.next().getInt("d"));
            assertEquals(3, rows.next().getInt("d"));
            assertEquals(4, rows.next().getInt("d"));
            assertEquals(5, rows.next().getInt("d"));

            results = execute("SELECT * FROM %s.multiple_clustering WHERE a=0 ORDER BY b DESC");
            assertEquals(6, results.size());
            rows = results.iterator();
            assertEquals(5, rows.next().getInt("d"));
            assertEquals(4, rows.next().getInt("d"));
            assertEquals(3, rows.next().getInt("d"));
            assertEquals(2, rows.next().getInt("d"));
            assertEquals(1, rows.next().getInt("d"));
            assertEquals(0, rows.next().getInt("d"));

            results = execute("SELECT * FROM %s.multiple_clustering WHERE a=0 ORDER BY b DESC, c DESC");
            assertEquals(6, results.size());
            rows = results.iterator();
            assertEquals(5, rows.next().getInt("d"));
            assertEquals(4, rows.next().getInt("d"));
            assertEquals(3, rows.next().getInt("d"));
            assertEquals(2, rows.next().getInt("d"));
            assertEquals(1, rows.next().getInt("d"));
            assertEquals(0, rows.next().getInt("d"));

            // select and order by b
            results = execute("SELECT b FROM %s.multiple_clustering WHERE a=0 ORDER BY b ASC");
            assertEquals(6, results.size());
            rows = results.iterator();
            assertEquals(0, rows.next().getInt("b"));
            assertEquals(0, rows.next().getInt("b"));
            assertEquals(0, rows.next().getInt("b"));
            assertEquals(1, rows.next().getInt("b"));
            assertEquals(1, rows.next().getInt("b"));
            assertEquals(1, rows.next().getInt("b"));

            results = execute("SELECT b FROM %s.multiple_clustering WHERE a=0 ORDER BY b DESC");
            assertEquals(6, results.size());
            rows = results.iterator();
            assertEquals(1, rows.next().getInt("b"));
            assertEquals(1, rows.next().getInt("b"));
            assertEquals(1, rows.next().getInt("b"));
            assertEquals(0, rows.next().getInt("b"));
            assertEquals(0, rows.next().getInt("b"));
            assertEquals(0, rows.next().getInt("b"));

            // select c, order by b
            results = execute("SELECT c FROM %s.multiple_clustering WHERE a=0 ORDER BY b ASC");
            rows = results.iterator();
            assertEquals(0, rows.next().getInt("c"));
            assertEquals(1, rows.next().getInt("c"));
            assertEquals(2, rows.next().getInt("c"));
            assertEquals(0, rows.next().getInt("c"));
            assertEquals(1, rows.next().getInt("c"));
            assertEquals(2, rows.next().getInt("c"));

            results = execute("SELECT c FROM %s.multiple_clustering WHERE a=0 ORDER BY b DESC");
            assertEquals(6, results.size());
            rows = results.iterator();
            assertEquals(2, rows.next().getInt("c"));
            assertEquals(1, rows.next().getInt("c"));
            assertEquals(0, rows.next().getInt("c"));
            assertEquals(2, rows.next().getInt("c"));
            assertEquals(1, rows.next().getInt("c"));
            assertEquals(0, rows.next().getInt("c"));

            // select c, order by b, c
            results = execute("SELECT c FROM %s.multiple_clustering WHERE a=0 ORDER BY b ASC, c ASC");
            rows = results.iterator();
            assertEquals(0, rows.next().getInt("c"));
            assertEquals(1, rows.next().getInt("c"));
            assertEquals(2, rows.next().getInt("c"));
            assertEquals(0, rows.next().getInt("c"));
            assertEquals(1, rows.next().getInt("c"));
            assertEquals(2, rows.next().getInt("c"));

            results = execute("SELECT c FROM %s.multiple_clustering WHERE a=0 ORDER BY b DESC, c DESC");
            assertEquals(6, results.size());
            rows = results.iterator();
            assertEquals(2, rows.next().getInt("c"));
            assertEquals(1, rows.next().getInt("c"));
            assertEquals(0, rows.next().getInt("c"));
            assertEquals(2, rows.next().getInt("c"));
            assertEquals(1, rows.next().getInt("c"));
            assertEquals(0, rows.next().getInt("c"));

            // select d, order by b, c
            results = execute("SELECT d FROM %s.multiple_clustering WHERE a=0 ORDER BY b ASC, c ASC");
            rows = results.iterator();
            assertEquals(0, rows.next().getInt("d"));
            assertEquals(1, rows.next().getInt("d"));
            assertEquals(2, rows.next().getInt("d"));
            assertEquals(3, rows.next().getInt("d"));
            assertEquals(4, rows.next().getInt("d"));
            assertEquals(5, rows.next().getInt("d"));

            results = execute("SELECT d FROM %s.multiple_clustering WHERE a=0 ORDER BY b DESC, c DESC");
            assertEquals(6, results.size());
            rows = results.iterator();
            assertEquals(5, rows.next().getInt("d"));
            assertEquals(4, rows.next().getInt("d"));
            assertEquals(3, rows.next().getInt("d"));
            assertEquals(2, rows.next().getInt("d"));
            assertEquals(1, rows.next().getInt("d"));
            assertEquals(0, rows.next().getInt("d"));
        }
        finally
        {
            execute("DELETE FROM %s.multiple_clustering WHERE a = 0");
        }
    }

    @Test
    public void testFunctionSelectionOrderMultipleClustering() throws Throwable
    {
        execute("INSERT INTO %s.multiple_clustering (a, b, c, d) VALUES (0, 0, 0, 0)");
        execute("INSERT INTO %s.multiple_clustering (a, b, c, d) VALUES (0, 0, 1, 1)");
        execute("INSERT INTO %s.multiple_clustering (a, b, c, d) VALUES (0, 0, 2, 2)");
        execute("INSERT INTO %s.multiple_clustering (a, b, c, d) VALUES (0, 1, 0, 3)");
        execute("INSERT INTO %s.multiple_clustering (a, b, c, d) VALUES (0, 1, 1, 4)");
        execute("INSERT INTO %s.multiple_clustering (a, b, c, d) VALUES (0, 1, 2, 5)");
        try
        {
            // select function of b, order by b
            UntypedResultSet results = execute("SELECT blobAsInt(intAsBlob(b)) as col FROM %s.multiple_clustering WHERE a=0 ORDER BY b ASC");
            assertEquals(6, results.size());
            Iterator<UntypedResultSet.Row> rows = results.iterator();
            assertEquals(0, rows.next().getInt("col"));
            assertEquals(0, rows.next().getInt("col"));
            assertEquals(0, rows.next().getInt("col"));
            assertEquals(1, rows.next().getInt("col"));
            assertEquals(1, rows.next().getInt("col"));
            assertEquals(1, rows.next().getInt("col"));

            results = execute("SELECT blobAsInt(intAsBlob(b)) as col FROM %s.multiple_clustering WHERE a=0 ORDER BY b DESC");
            assertEquals(6, results.size());
            rows = results.iterator();
            assertEquals(1, rows.next().getInt("col"));
            assertEquals(1, rows.next().getInt("col"));
            assertEquals(1, rows.next().getInt("col"));
            assertEquals(0, rows.next().getInt("col"));
            assertEquals(0, rows.next().getInt("col"));
            assertEquals(0, rows.next().getInt("col"));

            // select b and function of b, order by b
            results = execute("SELECT b, blobAsInt(intAsBlob(b)) as col FROM %s.multiple_clustering WHERE a=0 ORDER BY b ASC");
            assertEquals(6, results.size());
            rows = results.iterator();
            assertEquals(0, rows.next().getInt("col"));
            assertEquals(0, rows.next().getInt("col"));
            assertEquals(0, rows.next().getInt("col"));
            assertEquals(1, rows.next().getInt("col"));
            assertEquals(1, rows.next().getInt("col"));
            assertEquals(1, rows.next().getInt("col"));

            results = execute("SELECT b, blobAsInt(intAsBlob(b)) as col FROM %s.multiple_clustering WHERE a=0 ORDER BY b DESC");
            assertEquals(6, results.size());
            rows = results.iterator();
            assertEquals(1, rows.next().getInt("col"));
            assertEquals(1, rows.next().getInt("col"));
            assertEquals(1, rows.next().getInt("col"));
            assertEquals(0, rows.next().getInt("col"));
            assertEquals(0, rows.next().getInt("col"));
            assertEquals(0, rows.next().getInt("col"));

            // select c, order by b
            results = execute("SELECT blobAsInt(intAsBlob(c)) as col FROM %s.multiple_clustering WHERE a=0 ORDER BY b ASC");
            rows = results.iterator();
            assertEquals(0, rows.next().getInt("col"));
            assertEquals(1, rows.next().getInt("col"));
            assertEquals(2, rows.next().getInt("col"));
            assertEquals(0, rows.next().getInt("col"));
            assertEquals(1, rows.next().getInt("col"));
            assertEquals(2, rows.next().getInt("col"));

            results = execute("SELECT blobAsInt(intAsBlob(c)) as col FROM %s.multiple_clustering WHERE a=0 ORDER BY b DESC");
            assertEquals(6, results.size());
            rows = results.iterator();
            assertEquals(2, rows.next().getInt("col"));
            assertEquals(1, rows.next().getInt("col"));
            assertEquals(0, rows.next().getInt("col"));
            assertEquals(2, rows.next().getInt("col"));
            assertEquals(1, rows.next().getInt("col"));
            assertEquals(0, rows.next().getInt("col"));

            // select c, order by b, c
            results = execute("SELECT blobAsInt(intAsBlob(c)) as col FROM %s.multiple_clustering WHERE a=0 ORDER BY b ASC, c ASC");
            rows = results.iterator();
            assertEquals(0, rows.next().getInt("col"));
            assertEquals(1, rows.next().getInt("col"));
            assertEquals(2, rows.next().getInt("col"));
            assertEquals(0, rows.next().getInt("col"));
            assertEquals(1, rows.next().getInt("col"));
            assertEquals(2, rows.next().getInt("col"));

            results = execute("SELECT blobAsInt(intAsBlob(c)) as col FROM %s.multiple_clustering WHERE a=0 ORDER BY b DESC, c DESC");
            assertEquals(6, results.size());
            rows = results.iterator();
            assertEquals(2, rows.next().getInt("col"));
            assertEquals(1, rows.next().getInt("col"));
            assertEquals(0, rows.next().getInt("col"));
            assertEquals(2, rows.next().getInt("col"));
            assertEquals(1, rows.next().getInt("col"));
            assertEquals(0, rows.next().getInt("col"));

            // select d, order by b, c
            results = execute("SELECT blobAsInt(intAsBlob(d)) as col FROM %s.multiple_clustering WHERE a=0 ORDER BY b ASC, c ASC");
            rows = results.iterator();
            assertEquals(0, rows.next().getInt("col"));
            assertEquals(1, rows.next().getInt("col"));
            assertEquals(2, rows.next().getInt("col"));
            assertEquals(3, rows.next().getInt("col"));
            assertEquals(4, rows.next().getInt("col"));
            assertEquals(5, rows.next().getInt("col"));

            results = execute("SELECT blobAsInt(intAsBlob(d)) as col FROM %s.multiple_clustering WHERE a=0 ORDER BY b DESC, c DESC");
            assertEquals(6, results.size());
            rows = results.iterator();
            assertEquals(5, rows.next().getInt("col"));
            assertEquals(4, rows.next().getInt("col"));
            assertEquals(3, rows.next().getInt("col"));
            assertEquals(2, rows.next().getInt("col"));
            assertEquals(1, rows.next().getInt("col"));
            assertEquals(0, rows.next().getInt("col"));
        }
        finally
        {
            execute("DELETE FROM %s.multiple_clustering WHERE a = 0");
        }
    }

    /**
     * Check that order-by works with IN (#4327, #10363)
     * migrated from cql_tests.py:TestCQL.order_by_with_in_test()
     */
    @Test
    public void testOrderByForInClause() throws Throwable
    {
        executeSchemaChange("CREATE TABLE %s.order_by_for_in_t1 (my_id varchar, col1 int, value varchar, PRIMARY KEY (my_id, col1))");

        execute("INSERT INTO %s.order_by_for_in_t1 (my_id, col1, value) VALUES ( 'key1', 1, 'a')");
        execute("INSERT INTO %s.order_by_for_in_t1 (my_id, col1, value) VALUES ( 'key2', 3, 'c')");
        execute("INSERT INTO %s.order_by_for_in_t1 (my_id, col1, value) VALUES ( 'key3', 2, 'b')");
        execute("INSERT INTO %s.order_by_for_in_t1 (my_id, col1, value) VALUES ( 'key4', 4, 'd')");

        assertRows(execute("SELECT col1 FROM %s.order_by_for_in_t1 WHERE my_id in('key1', 'key2', 'key3') ORDER BY col1"),
                   row(1), row(2), row(3));

        assertRows(execute("SELECT col1, my_id FROM %s.order_by_for_in_t1 WHERE my_id in('key1', 'key2', 'key3') ORDER BY col1"),
                   row(1, "key1"), row(2, "key3"), row(3, "key2"));

        assertRows(execute("SELECT my_id, col1 FROM %s.order_by_for_in_t1 WHERE my_id in('key1', 'key2', 'key3') ORDER BY col1"),
                   row("key1", 1), row("key3", 2), row("key2", 3));

        executeSchemaChange("CREATE TABLE %s.order_by_for_in_t2 (pk1 int, pk2 int, c int, v text, PRIMARY KEY ((pk1, pk2), c) )");
        execute("INSERT INTO %s.order_by_for_in_t2 (pk1, pk2, c, v) VALUES (1, 1, 2, 'A')");
        execute("INSERT INTO %s.order_by_for_in_t2 (pk1, pk2, c, v) VALUES (1, 2, 1, 'B')");
        execute("INSERT INTO %s.order_by_for_in_t2 (pk1, pk2, c, v) VALUES (1, 3, 3, 'C')");
        execute("INSERT INTO %s.order_by_for_in_t2 (pk1, pk2, c, v) VALUES (1, 1, 4, 'D')");

        assertRows(execute("SELECT v, ttl(v), c FROM %s.order_by_for_in_t2 where pk1 = 1 AND pk2 IN (1, 2) ORDER BY c; "),
                   row("B", null, 1),
                   row("A", null, 2),
                   row("D", null, 4));

        assertRows(execute("SELECT v, ttl(v), c as name_1 FROM %s.order_by_for_in_t2 where pk1 = 1 AND pk2 IN (1, 2) ORDER BY c; "),
                   row("B", null, 1),
                   row("A", null, 2),
                   row("D", null, 4));

        assertInvalidMessage("ORDER BY can only be performed on columns in the select clause (got c)",
                             "SELECT v as c FROM %s.order_by_for_in_t2 where pk1 = 1 AND pk2 IN (1, 2) ORDER BY c; ");

        executeSchemaChange("CREATE TABLE %s.order_by_for_in_t3 (pk1 int, pk2 int, c1 int, c2 int, v text, PRIMARY KEY ((pk1, pk2), c1, c2) )");
        execute("INSERT INTO %s.order_by_for_in_t3 (pk1, pk2, c1, c2, v) VALUES (1, 1, 4, 4, 'A')");
        execute("INSERT INTO %s.order_by_for_in_t3 (pk1, pk2, c1, c2, v) VALUES (1, 2, 1, 2, 'B')");
        execute("INSERT INTO %s.order_by_for_in_t3 (pk1, pk2, c1, c2, v) VALUES (1, 3, 3, 3, 'C')");
        execute("INSERT INTO %s.order_by_for_in_t3 (pk1, pk2, c1, c2, v) VALUES (1, 1, 4, 1, 'D')");

        assertRows(execute("SELECT v, ttl(v), c1, c2 FROM %s.order_by_for_in_t3 where pk1 = 1 AND pk2 IN (1, 2) ORDER BY c1, c2; "),
                   row("B", null, 1, 2),
                   row("D", null, 4, 1),
                   row("A", null, 4, 4));

        assertRows(execute("SELECT v, ttl(v), c1 as name_1, c2 as name_2 FROM %s.order_by_for_in_t3 where pk1 = 1 AND pk2 IN (1, 2) ORDER BY c1, c2; "),
                   row("B", null, 1, 2),
                   row("D", null, 4, 1),
                   row("A", null, 4, 4));

        assertInvalidMessage("ORDER BY can only be performed on columns in the select clause (got c1)",
                             "SELECT v FROM %s.order_by_for_in_t3 where pk1 = 1 AND pk2 IN (1, 2) ORDER BY c1, c2;");

        assertInvalidMessage("ORDER BY can only be performed on columns in the select clause (got c1)",
                             "SELECT v as c2 FROM %s.order_by_for_in_t3 where pk1 = 1 AND pk2 IN (1, 2) ORDER BY c1, c2;");
    }

    private Object[] row(Object... expected)
    {
        return expected;
    }

    private void assertRows(UntypedResultSet result, Object[]... rows)
    {
        if (result == null)
        {
            if (rows.length > 0)
                Assert.fail(String.format("No rows returned by query but %d expected", rows.length));
            return;
        }

        List<ColumnSpecification> meta = result.metadata();
        Iterator<UntypedResultSet.Row> iter = result.iterator();
        int i = 0;
        while (iter.hasNext() && i < rows.length)
        {
            Object[] expected = rows[i];
            UntypedResultSet.Row actual = iter.next();

            Assert.assertEquals(String.format("Invalid number of (expected) values provided for row %d", i), expected.length, meta.size());

            for (int j = 0; j < meta.size(); j++)
            {
                ColumnSpecification column = meta.get(j);
                ByteBuffer expectedByteValue = makeByteBuffer(expected[j], column.type);
                ByteBuffer actualValue = actual.getBytes(column.name.toString());

                if (!Objects.equal(expectedByteValue, actualValue))
                {
                    Object actualValueDecoded = column.type.getSerializer().deserialize(actualValue);
                    if (!actualValueDecoded.equals(expected[j]))
                        Assert.fail(String.format("Invalid value for row %d column %d (%s of type %s), expected <%s> but got <%s>",
                                                  i,
                                                  j,
                                                  column.name,
                                                  column.type.asCQL3Type(),
                                                  formatValue(expectedByteValue, column.type),
                                                  formatValue(actualValue, column.type)));
                }
            }
            i++;
        }

        if (iter.hasNext())
        {
            while (iter.hasNext())
            {
                iter.next();
                i++;
            }
            Assert.fail(String.format("Got less rows than expected. Expected %d but got %d.", rows.length, i));
        }

        Assert.assertTrue(String.format("Got %s rows than expected. Expected %d but got %d", rows.length>i ? "less" : "more", rows.length, i), i == rows.length);
    }

    private static ByteBuffer makeByteBuffer(Object value, AbstractType type)
    {
        if (value == null)
            return null;

        if (value instanceof ByteBuffer)
            return (ByteBuffer)value;

        return type.decompose(value);
    }

    private static String formatValue(ByteBuffer bb, AbstractType<?> type)
    {
        if (bb == null)
            return "null";

        if (type instanceof CollectionType)
        {
            // CollectionType override getString() to use hexToBytes. We can't change that
            // without breaking SSTable2json, but the serializer for collection have the
            // right getString so using it directly instead.
            TypeSerializer ser = type.getSerializer();
            return ser.toString(ser.deserialize(bb));
        }

        return type.getString(bb);
    }

    protected void assertInvalidMessage(String errorMessage, String query) throws Throwable
    {
        try
        {
            execute(query);
            Assert.fail("Query should be invalid but no error was thrown. Query is: " + query);
        }
        catch (CassandraException e)
        {
            if (errorMessage != null)
            {
                assertMessageContains(errorMessage, e);
            }
        }
    }

    private static void assertMessageContains(String text, Exception e)
    {
        Assert.assertTrue("Expected error message to contain '" + text + "', but got '" + e.getMessage() + "'",
                          e.getMessage().contains(text));
    }
}
