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

import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.utils.FBUtilities;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

public class ComplexQueryTest extends SAITester
{
    @Test
    public void partialUpdateTest() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, c1 text, c2 text, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(c1) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(c2) USING 'StorageAttachedIndex'");
        waitForTableIndexesQueryable();

        execute("INSERT INTO %s (pk, c1, c2) VALUES (?, ?, ?)", 1, "a", "a");
        flush();
        execute("UPDATE %s SET c1 = ? WHERE pk = ?", "b", 1);
        flush();
        execute("UPDATE %s SET c2 = ? WHERE pk = ?", "c", 1);
        flush();

        UntypedResultSet resultSet = execute("SELECT pk FROM %s WHERE c1 = 'b' AND c2='c'");
        assertRows(resultSet, row(1));
    }

    @Test
    public void splitRowsWithBooleanLogic() throws Throwable
    {
        createTable(KEYSPACE, "CREATE TABLE %s (pk int primary key, str_val text, val text)");
        createIndex("CREATE CUSTOM INDEX ON %s(str_val) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
        waitForTableIndexesQueryable();
        disableCompaction(KEYSPACE);

        // flush a sstable with 2 partial rows
        execute("INSERT INTO %s (pk, str_val) VALUES (3, 'A')");
        execute("INSERT INTO %s (pk, val) VALUES (1, 'A')");
        flush();

        // flush another sstable with 2 more partial rows, where PK 3 is now a complete row
        execute("INSERT INTO %s (pk, val) VALUES (3, 'A')");
        execute("INSERT INTO %s (pk, str_val) VALUES (2, 'A')");
        flush();

        // pk 3 should match
        var result = execute("SELECT pk FROM %s WHERE str_val = 'A' AND val = 'A'");
        assertRows(result, row(3));
    }

    @Test
    public void basicOrTest() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, a int, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(a) USING 'StorageAttachedIndex'");
        waitForTableIndexesQueryable();

        execute("INSERT INTO %s (pk, a) VALUES (?, ?)", 1, 1);
        execute("INSERT INTO %s (pk, a) VALUES (?, ?)", 2, 2);
        execute("INSERT INTO %s (pk, a) VALUES (?, ?)", 3, 3);

        UntypedResultSet resultSet = execute("SELECT pk FROM %s WHERE a = 1 or a = 3");

        assertRowsIgnoringOrder(resultSet, row(1), row(3) );
    }

    @Test
    public void basicInTest() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, a int, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(a) USING 'StorageAttachedIndex'");
        waitForTableIndexesQueryable();

        execute("INSERT INTO %s (pk, a) VALUES (?, ?)", 1, 1);
        execute("INSERT INTO %s (pk, a) VALUES (?, ?)", 2, 2);
        execute("INSERT INTO %s (pk, a) VALUES (?, ?)", 3, 3);
        execute("INSERT INTO %s (pk, a) VALUES (?, ?)", 4, 4);
        execute("INSERT INTO %s (pk, a) VALUES (?, ?)", 5, 5);

        UntypedResultSet resultSet = execute("SELECT pk FROM %s WHERE a in (1, 3, 5)");

        assertRowsIgnoringOrder(resultSet, row(1), row(3), row(5));
    }

    @Test
    public void complexQueryTest() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, a int, b int, c int, d int, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(a) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(b) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(c) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(d) USING 'StorageAttachedIndex'");
        waitForTableIndexesQueryable();

        execute("INSERT INTO %s (pk, a, b, c, d) VALUES (?, ?, ?, ?, ?)", 1, 1, 1, 1, 1);
        execute("INSERT INTO %s (pk, a, b, c, d) VALUES (?, ?, ?, ?, ?)", 2, 2, 1, 1, 1);
        execute("INSERT INTO %s (pk, a, b, c, d) VALUES (?, ?, ?, ?, ?)", 3, 3, 2, 1, 1);
        execute("INSERT INTO %s (pk, a, b, c, d) VALUES (?, ?, ?, ?, ?)", 4, 4, 2, 2, 1);
        execute("INSERT INTO %s (pk, a, b, c, d) VALUES (?, ?, ?, ?, ?)", 5, 5, 3, 2, 1);
        execute("INSERT INTO %s (pk, a, b, c, d) VALUES (?, ?, ?, ?, ?)", 6, 6, 3, 2, 2);
        execute("INSERT INTO %s (pk, a, b, c, d) VALUES (?, ?, ?, ?, ?)", 7, 7, 4, 3, 2);
        execute("INSERT INTO %s (pk, a, b, c, d) VALUES (?, ?, ?, ?, ?)", 8, 8, 4, 3, 3);



        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT pk FROM %s WHERE (a = 1 AND c = 1) OR (b IN (3, 4) AND d = 2)"), row(1), row(7), row(6));
            // Shows that IN with an empty list produces no rows
            assertRows(execute("SELECT pk FROM %s WHERE (a = 1 AND c = 1) OR (b IN () AND d = 2)"), row(1));
            assertRows(execute("SELECT pk FROM %s WHERE b IN () AND d = 2"));
            assertRows(execute("SELECT pk FROM %s WHERE b NOT IN () AND d = 2"), row(7), row(6));
        });

    }

    @Test
    public void disjunctionWithClusteringKey() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, a int, PRIMARY KEY(pk, ck))");

        execute("INSERT INTO %s (pk, ck, a) VALUES (?, ?, ?)", 1, 1, 1);
        execute("INSERT INTO %s (pk, ck, a) VALUES (?, ?, ?)", 2, 2, 2);

        assertThatThrownBy(() -> execute("SELECT pk FROM %s WHERE a = 1 or ck = 2"))
                .isInstanceOf(InvalidRequestException.class)
                .hasMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE);

        UntypedResultSet resultSet = execute("SELECT pk FROM %s WHERE a = 1 or ck = 2 ALLOW FILTERING");

        assertRowsIgnoringOrder(resultSet, row(1), row(2));
    }

    @Test
    public void disjunctionWithIndexOnClusteringKey() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, a int, PRIMARY KEY(pk, ck))");
        createIndex("CREATE CUSTOM INDEX ON %s(ck) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(a) USING 'StorageAttachedIndex'");
        waitForTableIndexesQueryable();

        execute("INSERT INTO %s (pk, ck, a) VALUES (?, ?, ?)", 1, 1, 1);
        execute("INSERT INTO %s (pk, ck, a) VALUES (?, ?, ?)", 2, 2, 2);

        UntypedResultSet resultSet = execute("SELECT pk FROM %s WHERE a = 1 or ck = 2");

        assertRowsIgnoringOrder(resultSet, row(1), row(2));
    }

    @Test
    public void complexQueryWithMultipleClusterings() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck0 int, ck1 int, a int, b int, c int, d int, e int, PRIMARY KEY(pk, ck0, ck1))");
        createIndex("CREATE CUSTOM INDEX ON %s(ck0) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(ck1) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(a) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(b) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(c) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(d) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(e) USING 'StorageAttachedIndex'");
        waitForTableIndexesQueryable();

        execute("INSERT INTO %s (pk, ck0, ck1, a, b, c, d, e) VALUES (?, ?, ?, ?, ?, ? ,?, ?)", 1, 1, 1, 1, 1, 1, 1, 1);
        execute("INSERT INTO %s (pk, ck0, ck1, a, b, c, d, e) VALUES (?, ?, ?, ?, ?, ? ,?, ?)", 2, 2, 2, 2, 2, 2, 2, 2);
        execute("INSERT INTO %s (pk, ck0, ck1, a, b, c, d, e) VALUES (?, ?, ?, ?, ?, ? ,?, ?)", 3, 3, 3, 3, 3, 3, 3, 3);
        execute("INSERT INTO %s (pk, ck0, ck1, a, b, c, d, e) VALUES (?, ?, ?, ?, ?, ? ,?, ?)", 4, 4, 4, 4, 4, 4, 4, 4);
        execute("INSERT INTO %s (pk, ck0, ck1, a, b, c, d, e) VALUES (?, ?, ?, ?, ?, ? ,?, ?)", 5, 5, 5, 5, 5, 5, 5, 5);

        UntypedResultSet resultSet = execute("SELECT pk FROM %s WHERE b = 6 AND d = 6 OR (a = 6 OR (c = 3 OR ck0 = 5))");

        assertRowsIgnoringOrder(resultSet, row(3), row(5));

        resultSet = execute("SELECT pk FROM %s WHERE ck0 = 1 AND (b = 6 AND c = 6 OR (d = 6 OR e = 6))");

        assertEquals(0 , resultSet.size());

        resultSet = execute("SELECT pk FROM %s WHERE b = 4 OR a = 3 OR c = 5");

        assertRowsIgnoringOrder(resultSet, row(3), row(4), row(5));
    }

    @Test
    public void complexQueryWithPartitionKeyRestriction() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, a int, b int, PRIMARY KEY(pk, ck))");

        execute("INSERT INTO %s (pk, ck, a, b) VALUES (?, ?, ?, ?)", 1, 1, 1, 5);
        execute("INSERT INTO %s (pk, ck, a, b) VALUES (?, ?, ?, ?)", 1, 2, 2, 6);
        execute("INSERT INTO %s (pk, ck, a, b) VALUES (?, ?, ?, ?)", 2, 1, 3, 7);
        execute("INSERT INTO %s (pk, ck, a, b) VALUES (?, ?, ?, ?)", 2, 2, 4, 8);


        assertThatThrownBy(() -> execute("SELECT pk, ck FROM %s WHERE pk = 1 AND (a = 2 OR b = 7)"))
                .isInstanceOf(InvalidRequestException.class)
                .hasMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE);

        UntypedResultSet resultSet = execute("SELECT pk, ck FROM %s WHERE pk = 1 AND (a = 2 OR b = 7) ALLOW FILTERING");

        assertRowsIgnoringOrder(resultSet, row(1, 2));

        assertThatThrownBy(() -> execute("SELECT pk, ck FROM %s WHERE pk = 1 OR a = 2 OR b = 7 ALLOW FILTERING"))
                .isInstanceOf(InvalidRequestException.class)
                .hasMessage(String.format(StatementRestrictions.PARTITION_KEY_RESTRICTION_MUST_BE_TOP_LEVEL, "pk"));

        // Here pk = 1 is directly under AND operation, so a simple isDisjunction check on it would not be enough
        // to reject it ;)
        assertThatThrownBy(() -> execute("SELECT pk, ck FROM %s WHERE a = 2 OR (pk = 1 AND b = 7) ALLOW FILTERING"))
                .isInstanceOf(InvalidRequestException.class)
                .hasMessage(String.format(StatementRestrictions.PARTITION_KEY_RESTRICTION_MUST_BE_TOP_LEVEL, "pk"));
    }

    @Test
    public void complexQueryWithPartitionKeyRestrictionAndIndexes() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, a int, b int, PRIMARY KEY(pk, ck))");
        createIndex("CREATE CUSTOM INDEX ON %s(a) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(b) USING 'StorageAttachedIndex'");
        waitForTableIndexesQueryable();

        execute("INSERT INTO %s (pk, ck, a, b) VALUES (?, ?, ?, ?)", 1, 1, 1, 5);
        execute("INSERT INTO %s (pk, ck, a, b) VALUES (?, ?, ?, ?)", 1, 2, 2, 6);
        execute("INSERT INTO %s (pk, ck, a, b) VALUES (?, ?, ?, ?)", 2, 1, 3, 7);
        execute("INSERT INTO %s (pk, ck, a, b) VALUES (?, ?, ?, ?)", 2, 2, 4, 8);

        UntypedResultSet resultSet = execute("SELECT pk, ck FROM %s WHERE pk = 1 AND (a = 2 OR b = 7)");

        assertRowsIgnoringOrder(resultSet, row(1, 2));

        assertThatThrownBy(() -> execute("SELECT pk, ck FROM %s WHERE pk = 1 OR a = 2 OR b = 7 ALLOW FILTERING"))
                .isInstanceOf(InvalidRequestException.class)
                .hasMessage(String.format(StatementRestrictions.PARTITION_KEY_RESTRICTION_MUST_BE_TOP_LEVEL, "pk"));
    }

    @Test
    public void indexNotSupportingDisjunctionTest() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, a int, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(a) USING 'org.apache.cassandra.index.sasi.SASIIndex'");
        waitForTableIndexesQueryable();

        execute("INSERT INTO %s (pk, a) VALUES (?, ?)", 1, 1);
        execute("INSERT INTO %s (pk, a) VALUES (?, ?)", 2, 2);

        assertThatThrownBy(() -> execute("SELECT pk FROM %s WHERE a = 1 or a = 2")).isInstanceOf(InvalidRequestException.class)
                                                                                   .hasMessage(StatementRestrictions.INDEX_DOES_NOT_SUPPORT_DISJUNCTION);

        assertThatThrownBy(() -> execute("SELECT pk FROM %s WHERE a = 1 or a = 2 ALLOW FILTERING")).isInstanceOf(InvalidRequestException.class)
                                                                                                   .hasMessage(StatementRestrictions.INDEX_DOES_NOT_SUPPORT_DISJUNCTION);
    }

    @Test
    public void complexQueryWithMultipleNEQ() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, a int, b int, PRIMARY KEY(pk, ck))");
        createIndex("CREATE CUSTOM INDEX ON %s(a) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(b) USING 'StorageAttachedIndex'");
        waitForTableIndexesQueryable();

        execute("INSERT INTO %s (pk, ck, a, b) VALUES (?, ?, ?, ?)", 1, 1, 1, 5);
        execute("INSERT INTO %s (pk, ck, a, b) VALUES (?, ?, ?, ?)", 1, 2, 2, 6);
        execute("INSERT INTO %s (pk, ck, a, b) VALUES (?, ?, ?, ?)", 1, 3, 3, 7);
        execute("INSERT INTO %s (pk, ck, a, b) VALUES (?, ?, ?, ?)", 1, 4, 4, 8);
        execute("INSERT INTO %s (pk, ck, a, b) VALUES (?, ?, ?, ?)", 1, 5, null, null);

        assertRowsIgnoringOrder(execute("SELECT ck FROM %s WHERE pk = 1 AND a != 2 AND b != 7"), row(1), row(4));
        assertRowsIgnoringOrder(execute("SELECT ck FROM %s WHERE pk = 1 AND a != 2 AND a != 3"), row(1), row(4));
        assertRowsIgnoringOrder(execute("SELECT ck FROM %s WHERE pk = 1 AND a NOT IN (2, 3)"), row(1), row(4));
        assertRowsIgnoringOrder(execute("SELECT ck FROM %s WHERE pk = 1 AND a NOT IN (2, 3) AND b NOT IN (7, 8)"), row(1));
    }

    @Test
    public void complexQueryWithIndexedAndNotIndexed()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, x int, y int, z int)");

        execute("INSERT INTO %s(k, x, y, z) values (0, 0, 0, 0)");
        execute("INSERT INTO %s(k, x, y, z) values (1, 0, 1, 0)");
        execute("INSERT INTO %s(k, x, y, z) values (2, 0, 2, 1)");
        execute("INSERT INTO %s(k, x, y, z) values (3, 0, 3, 1)");

        new IndexCombinationsTester(this).withIndexOn("x", "y", "z").test(indexedColumns -> {

            assertQueryUsesIndex("SELECT k FROM %s WHERE x=0 ALLOW FILTERING",
                                 indexedColumns,
                                 c -> contains(c, "x"),
                                 row(0), row(1), row(2), row(3));
            assertQueryUsesIndex("SELECT k FROM %s WHERE x=1 ALLOW FILTERING",
                                 indexedColumns,
                                 c -> contains(c, "x"));

            assertQueryUsesIndex("SELECT k FROM %s WHERE y IN (1, 2) ALLOW FILTERING",
                                 indexedColumns,
                                 c -> contains(c, "y"),
                                 row(1), row(2));
            assertQueryUsesIndex("SELECT k FROM %s WHERE y IN (4, 6) ALLOW FILTERING",
                                 indexedColumns,
                                 c -> contains(c, "y"));

            assertQueryUsesIndex("SELECT k FROM %s WHERE x=0 AND y IN (1, 2) ALLOW FILTERING",
                                 indexedColumns,
                                 c -> contains(c, "x") || contains(c, "y"),
                                 row(1), row(2));
            assertQueryUsesIndex("SELECT k FROM %s WHERE x=1 AND y IN (1, 2) ALLOW FILTERING",
                                 indexedColumns,
                                 c -> contains(c, "x") || contains(c, "y"));

            assertQueryUsesIndex("SELECT k FROM %s WHERE x=0 AND (y=1 OR y=2) ALLOW FILTERING",
                                 indexedColumns,
                                 c -> contains(c, "x") || contains(c, "y"),
                                 row(1), row(2));
            assertQueryUsesIndex("SELECT k FROM %s WHERE x=1 AND (y=1 OR y=2) ALLOW FILTERING",
                                 indexedColumns,
                                 c -> contains(c, "x") || contains(c, "y"));

            assertQueryUsesIndex("SELECT k FROM %s WHERE x=0 OR y=0 ALLOW FILTERING",
                                 indexedColumns,
                                 c -> contains(c, "x", "y"),
                                 row(0), row(1), row(2), row(3));
            assertQueryUsesIndex("SELECT k FROM %s WHERE x=1 OR y=0 ALLOW FILTERING",
                                 indexedColumns,
                                 c -> contains(c, "x", "y"),
                                 row(0));

            assertQueryUsesIndex("SELECT k FROM %s WHERE x=0 OR (y=0 AND z=0) ALLOW FILTERING",
                                 indexedColumns,
                                 c -> contains(c, "x", "y") || contains(c, "x", "z"),
                                 row(0), row(1), row(2), row(3));
            assertQueryUsesIndex("SELECT k FROM %s WHERE x=1 OR (y=0 AND z=0) ALLOW FILTERING",
                                 indexedColumns,
                                 c -> contains(c, "x", "y") || contains(c, "x", "z"),
                                 row(0));

            assertQueryUsesIndex("SELECT k FROM %s WHERE x=0 OR y=0 OR z=0 ALLOW FILTERING",
                                 indexedColumns,
                                 c -> contains(c, "x", "y", "z"),
                                 row(0), row(1), row(2), row(3));
            assertQueryUsesIndex("SELECT k FROM %s WHERE x=1 OR y=0 OR z=0 ALLOW FILTERING",
                                 indexedColumns,
                                 c -> contains(c, "x", "y", "z"),
                                 row(0), row(1));
        });
    }

    private void assertQueryUsesIndex(String query,
                                      Set<String> indexedColumns,
                                      Predicate<Set<String>> shouldUseIndexes,
                                      Object[]... expectedRows)
    {
        // verify query result
        assertRowsIgnoringOrder(execute(query), expectedRows);

        // verify whether indexes are used or skipped
        String formattedQuery = formatQuery(query);
        SelectStatement select = (SelectStatement) QueryProcessor.parseStatement(formattedQuery, ClientState.forInternalCalls());
        ReadCommand cmd = (ReadCommand) select.getQuery(QueryOptions.DEFAULT, FBUtilities.nowInSeconds());
        Index.QueryPlan plan = cmd.indexQueryPlan();
        assertEquals(shouldUseIndexes.test(indexedColumns), plan != null);

        // if we are using indexes, verify that we are using the expected ones
        if (plan != null)
        {
            Set<String> selectedColumns = plan.getIndexes()
                                              .stream()
                                              .map(i -> (StorageAttachedIndex) i)
                                              .map(i -> i.getIndexContext().getColumnName())
                                              .collect(Collectors.toSet());
            Assert.assertTrue(shouldUseIndexes.test(selectedColumns));
        }
    }

    private static boolean contains(Set<String> set, String... elements)
    {
        return set.containsAll(Set.of(elements));
    }
}
