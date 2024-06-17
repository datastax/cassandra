/*
 * Copyright DataStax, Inc.
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
package org.apache.cassandra.cql3.validation.operations;

import java.util.Arrays;

import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ResultSet;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.SyntaxException;

public class SelectOffsetTest extends CQLTester
{
    public static final Object[][] EMPTY_ROWS = new Object[0][];

    private static final Logger logger = LoggerFactory.getLogger(SelectOffsetTest.class);

    @BeforeClass
    public static void beforeClass()
    {
        requireNetwork();
    }

    @Test
    public void testParseAndValidate() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v int, PRIMARY KEY (k, c))");

        // with LIMIT
        execute("SELECT * FROM %s LIMIT 4 OFFSET 0");
        execute("SELECT * FROM %s LIMIT 4 OFFSET 1");
        assertRejectsNegativeOffset("SELECT * FROM %s LIMIT 4 OFFSET -1");
        assertRejectsOffsetWithoutLimit("SELECT * FROM %s OFFSET 1");

        // with PER PARTITION LIMIT
        execute("SELECT * FROM %s PER PARTITION LIMIT 2 LIMIT 10 OFFSET 0");
        execute("SELECT * FROM %s PER PARTITION LIMIT 2 LIMIT 10 OFFSET 1");
        assertRejectsNegativeOffset("SELECT * FROM %s PER PARTITION LIMIT 2 LIMIT 10 OFFSET -1");
        assertRejectsOffsetWithoutLimit("SELECT * FROM %s PER PARTITION LIMIT 2 OFFSET 1");

        // with ALLOW FILTERING
        execute("SELECT * FROM %s WHERE v=0 LIMIT 10 OFFSET 0 ALLOW FILTERING");
        execute("SELECT * FROM %s WHERE v=0 LIMIT 10 OFFSET 1 ALLOW FILTERING");
        assertRejectsNegativeOffset("SELECT * FROM %s WHERE v=0 LIMIT 10 OFFSET -1 ALLOW FILTERING");
        assertRejectsOffsetWithoutLimit("SELECT * FROM %s WHERE v=0 OFFSET 1 ALLOW FILTERING");
    }

    private void assertRejectsNegativeOffset(String query) throws Throwable
    {
        assertInvalidThrowMessage("Offset must be positive",
                                  InvalidRequestException.class,
                                  query);
    }

    private void assertRejectsOffsetWithoutLimit(String query) throws Throwable
    {
        assertInvalidThrowMessage("[OFFSET]",
                                  SyntaxException.class,
                                  query);
    }

    @Test
    public void testSkinnyTable() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");

        // test with empty table
        testLimitAndOffset("SELECT * FROM %s");
        testLimitAndOffset("SELECT * FROM %s WHERE k=2");
        testLimitAndOffset("SELECT * FROM %s WHERE v=30");
        testLimitAndOffset("SELECT k, v, sum(v) FROM %s", row(null, null, 0));

        // write some data
        execute("INSERT INTO %s (k, v) VALUES (1, 10)");
        execute("INSERT INTO %s (k, v) VALUES (2, 20)");
        execute("INSERT INTO %s (k, v) VALUES (3, 30)");
        execute("INSERT INTO %s (k, v) VALUES (4, 40)");

        testLimitAndOffset("SELECT * FROM %s", row(1, 10), row(2, 20), row(4, 40), row(3, 30));
        testLimitAndOffset("SELECT * FROM %s WHERE k=2", row(2, 20));
        testLimitAndOffset("SELECT * FROM %s WHERE k<2", row(1, 10));
        testLimitAndOffset("SELECT * FROM %s WHERE k>2", row(4, 40), row(3, 30));
        testLimitAndOffset("SELECT * FROM %s WHERE v=30", row(3, 30));
        testLimitAndOffset("SELECT * FROM %s WHERE v<30", row(1, 10), row(2, 20));
        testLimitAndOffset("SELECT * FROM %s WHERE v>30", row(4, 40));
        testLimitAndOffset("SELECT k, v, sum(v) FROM %s", row(1, 10, 100));
    }

    @Test
    public void testWideTable() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c1 int, c2 int, v int, PRIMARY KEY (k, c1, c2))");

        // test with empty table
        testLimitAndOffset("SELECT * FROM %s");
        testLimitAndOffset("SELECT * FROM %s PER PARTITION LIMIT 3");
        testLimitAndOffset("SELECT * FROM %s GROUP BY k, c1");
        testLimitAndOffset("SELECT k, c1, c2, sum(v) FROM %s GROUP BY k, c1");
        testLimitAndOffset("SELECT k, c1, c2, sum(v) FROM %s", row(null, null, null, 0));

        // write some data
        execute("INSERT INTO %s (k, c1, c2, v) VALUES (0, 0, 0, 0)");
        execute("INSERT INTO %s (k, c1, c2, v) VALUES (0, 0, 1, 1)");
        execute("INSERT INTO %s (k, c1, c2, v) VALUES (0, 0, 2, 2)");
        execute("INSERT INTO %s (k, c1, c2, v) VALUES (0, 1, 0, 3)");
        execute("INSERT INTO %s (k, c1, c2, v) VALUES (0, 1, 1, 4)");
        execute("INSERT INTO %s (k, c1, c2, v) VALUES (0, 1, 2, 5)");
        execute("INSERT INTO %s (k, c1, c2, v) VALUES (1, 0, 0, 6)");
        execute("INSERT INTO %s (k, c1, c2, v) VALUES (1, 0, 1, 7)");
        execute("INSERT INTO %s (k, c1, c2, v) VALUES (1, 0, 2, 8)");
        execute("INSERT INTO %s (k, c1, c2, v) VALUES (1, 1, 0, 9)");
        execute("INSERT INTO %s (k, c1, c2, v) VALUES (1, 1, 1, 10)");
        execute("INSERT INTO %s (k, c1, c2, v) VALUES (1, 1, 2, 11)");

        testLimitAndOffset("SELECT * FROM %s",
                           row(1, 0, 0, 6),
                           row(1, 0, 1, 7),
                           row(1, 0, 2, 8),
                           row(1, 1, 0, 9),
                           row(1, 1, 1, 10),
                           row(1, 1, 2, 11),
                           row(0, 0, 0, 0),
                           row(0, 0, 1, 1),
                           row(0, 0, 2, 2),
                           row(0, 1, 0, 3),
                           row(0, 1, 1, 4),
                           row(0, 1, 2, 5));

        // With filtering restrictions
        testLimitAndOffset("SELECT * FROM %s WHERE k=0",
                           row(0, 0, 0, 0),
                           row(0, 0, 1, 1),
                           row(0, 0, 2, 2),
                           row(0, 1, 0, 3),
                           row(0, 1, 1, 4),
                           row(0, 1, 2, 5));
        testLimitAndOffset("SELECT * FROM %s WHERE k=0 AND c1=1",
                           row(0, 1, 0, 3),
                           row(0, 1, 1, 4),
                           row(0, 1, 2, 5));
        testLimitAndOffset("SELECT * FROM %s WHERE v>2 AND v<8",
                           row(1, 0, 0, 6),
                           row(1, 0, 1, 7),
                           row(0, 1, 0, 3),
                           row(0, 1, 1, 4),
                           row(0, 1, 2, 5));
        testLimitAndOffset("SELECT * FROM %s WHERE v<=2 OR v>=8",
                           row(1, 0, 2, 8),
                           row(1, 1, 0, 9),
                           row(1, 1, 1, 10),
                           row(1, 1, 2, 11),
                           row(0, 0, 0, 0),
                           row(0, 0, 1, 1),
                           row(0, 0, 2, 2));

        // With PER PARTITION LIMIT
        testLimitAndOffset("SELECT * FROM %s PER PARTITION LIMIT 3",
                           row(1, 0, 0, 6),
                           row(1, 0, 1, 7),
                           row(1, 0, 2, 8),
                           row(0, 0, 0, 0),
                           row(0, 0, 1, 1),
                           row(0, 0, 2, 2));
        testLimitAndOffset("SELECT * FROM %s PER PARTITION LIMIT 1",
                           row(1, 0, 0, 6),
                           row(0, 0, 0, 0));

        // With aggregation
        testLimitAndOffset("SELECT k, c1, c2, sum(v) FROM %s", row(1, 0, 0, 66));
        testLimitAndOffset("SELECT count(*) FROM %s", row(12L));

        // With GROUP BY
        testLimitAndOffset("SELECT * FROM %s GROUP BY k, c1",
                           row(1, 0, 0, 6),
                           row(1, 1, 0, 9),
                           row(0, 0, 0, 0),
                           row(0, 1, 0, 3));
        testLimitAndOffset("SELECT k, c1, c2, sum(v) FROM %s GROUP BY k, c1",
                           row(1, 0, 0, 21),
                           row(1, 1, 0, 30),
                           row(0, 0, 0, 3),
                           row(0, 1, 0, 12));

        // With ORDER BY
        testLimitAndOffset("SELECT * FROM %s WHERE k = 0 ORDER BY c1 DESC",
                           row(0, 1, 2, 5),
                           row(0, 1, 1, 4),
                           row(0, 1, 0, 3),
                           row(0, 0, 2, 2),
                           row(0, 0, 1, 1),
                           row(0, 0, 0, 0));
        testLimitAndOffset("SELECT * FROM %s WHERE k = 0 ORDER BY c1 DESC PER PARTITION LIMIT 4",
                           row(0, 1, 2, 5),
                           row(0, 1, 1, 4),
                           row(0, 1, 0, 3),
                           row(0, 0, 2, 2));
        testLimitAndOffset("SELECT * FROM %s WHERE k = 0 ORDER BY c1 DESC PER PARTITION LIMIT 1",
                           row(0, 1, 2, 5));
    }

    @Test
    public void testWideTableWithStatic() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v int, s int static, PRIMARY KEY (k, c))");

        // test with empty table
        testLimitAndOffset("SELECT * FROM %s");
        testLimitAndOffset("SELECT * FROM %s PER PARTITION LIMIT 1");

        // write some data
        execute("INSERT INTO %s (k, s) VALUES (0, 1)");
        execute("INSERT INTO %s (k, c, v) VALUES (0, 0, 0)");
        execute("INSERT INTO %s (k, c, v) VALUES (0, 1, 1)");
        execute("INSERT INTO %s (k, c, v) VALUES (0, 2, 0)");
        execute("INSERT INTO %s (k, s) VALUES (1, 0)");
        execute("INSERT INTO %s (k, c, v) VALUES (1, 0, 1)");
        execute("INSERT INTO %s (k, c, v) VALUES (1, 1, 0)");
        execute("INSERT INTO %s (k, c, v) VALUES (1, 2, 1)");

        testLimitAndOffset("SELECT * FROM %s",
                           row(1, 0, 0, 1),
                           row(1, 1, 0, 0),
                           row(1, 2, 0, 1),
                           row(0, 0, 1, 0),
                           row(0, 1, 1, 1),
                           row(0, 2, 1, 0));
        testLimitAndOffset("SELECT k, s FROM %s",
                           row(1, 0),
                           row(1, 0),
                           row(1, 0),
                           row(0, 1),
                           row(0, 1),
                           row(0, 1));
        testLimitAndOffset("SELECT s FROM %s",
                           row(0),
                           row(0),
                           row(0),
                           row(1),
                           row(1),
                           row(1));

        testLimitAndOffset("SELECT * FROM %s PER PARTITION LIMIT 2",
                           row(1, 0, 0, 1),
                           row(1, 1, 0, 0),
                           row(0, 0, 1, 0),
                           row(0, 1, 1, 1));
        testLimitAndOffset("SELECT * FROM %s PER PARTITION LIMIT 1",
                           row(1, 0, 0, 1),
                           row(0, 0, 1, 0));
    }

    @Test
    public void testANN()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v vector<float, 1>)");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function' : 'euclidean'}");
        execute("INSERT INTO %s (k, v) VALUES (1, [1])");
        execute("INSERT INTO %s (k, v) VALUES (2, [4])");
        execute("INSERT INTO %s (k, v) VALUES (3, [2])");
        execute("INSERT INTO %s (k, v) VALUES (4, [3])");

        // limit over number of rows
        assertRows(execute("SELECT * FROM %s ORDER BY v ANN OF [0] LIMIT 10 OFFSET 0"),
                   row(1, vector(1f)),
                   row(3, vector(2f)),
                   row(4, vector(3f)),
                   row(2, vector(4f)));
        assertRows(execute("SELECT * FROM %s ORDER BY v ANN OF [0] LIMIT 10 OFFSET 1"),
                   row(3, vector(2f)),
                   row(4, vector(3f)),
                   row(2, vector(4f)));
        assertRows(execute("SELECT * FROM %s ORDER BY v ANN OF [0] LIMIT 10 OFFSET 2"),
                   row(4, vector(3f)),
                   row(2, vector(4f)));
        assertRows(execute("SELECT * FROM %s ORDER BY v ANN OF [0] LIMIT 10 OFFSET 3"),
                   row(2, vector(4f)));
        assertRows(execute("SELECT * FROM %s ORDER BY v ANN OF [0] LIMIT 10 OFFSET 4"));
        assertRows(execute("SELECT * FROM %s ORDER BY v ANN OF [0] LIMIT 10 OFFSET 5"));

        // limit below number of rows
        assertRows(execute("SELECT * FROM %s ORDER BY v ANN OF [0] LIMIT 2 OFFSET 0"),
                   row(1, vector(1f)),
                   row(3, vector(2f)));
        assertRows(execute("SELECT * FROM %s ORDER BY v ANN OF [0] LIMIT 2 OFFSET 1"),
                   row(3, vector(2f)),
                   row(4, vector(3f)));
        assertRows(execute("SELECT * FROM %s ORDER BY v ANN OF [0] LIMIT 2 OFFSET 2"),
                   row(4, vector(3f)),
                   row(2, vector(4f)));
        assertRows(execute("SELECT * FROM %s ORDER BY v ANN OF [0] LIMIT 2 OFFSET 3"),
                   row(2, vector(4f)));
        assertRows(execute("SELECT * FROM %s ORDER BY v ANN OF [0] LIMIT 2 OFFSET 4"));
        assertRows(execute("SELECT * FROM %s ORDER BY v ANN OF [0] LIMIT 2 OFFSET 5"));
    }

    @SafeVarargs
    protected static <T> Vector<T> vector(T... values)
    {
        return new Vector<>(values);
    }

    private void testLimitAndOffset(String select, Object[]... rows) throws Throwable
    {
        for (int limit = 1; limit <= rows.length + 1; limit++)
        {
            for (int offset = 0; offset <= rows.length + 1; offset++)
            {
                testLimitAndOffset(select, offset, limit, rows);
            }
            testLimitAndOffset(select, null, limit, rows);
        }
    }

    private void testLimitAndOffset(String query, Integer offset, int limit, Object[]... rows) throws Throwable
    {
        // append the specified limit and offset to the unrestricted query
        StringBuilder sb = new StringBuilder(query);
        sb.append(" LIMIT ").append(limit);
        if (offset != null)
            sb.append(" OFFSET ").append(offset);
        sb.append(" ALLOW FILTERING");
        query = sb.toString();

        // trim the unrestricted query results according to the specified limit and offset
        rows = trimRows(offset, limit, rows);

        // test without paging
        assertRows(execute(query), rows);

        // test with paging
        int numRows = rows.length;
        for (int pageSize : ImmutableSet.of(Integer.MAX_VALUE, numRows + 1, numRows, numRows - 1, 1))
        {
            logger.debug("Executing test query with page size {}: {}", pageSize, query);
            ResultSet rs = executeNetWithPaging(query, pageSize);

            // key-based paging should be disabled when limit/offset paging is used
            if (offset != null && offset > 0)
            {
                Assert.assertTrue(rs.isFullyFetched());
                Assert.assertNull(rs.getExecutionInfo().getPagingState());
            }

            assertRowsNet(rs, rows);
        }
    }

    private static Object[][] trimRows(Integer offset, Integer limit, Object[]... rows)
    {
        offset = offset == null ? 0 : offset;
        limit = limit == null ? rows.length : limit;

        if (offset >= rows.length)
            return EMPTY_ROWS;

        return Arrays.copyOfRange(rows, offset, Math.min(offset + limit, rows.length));
    }
}
