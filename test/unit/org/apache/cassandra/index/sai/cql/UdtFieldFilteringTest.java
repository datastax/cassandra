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
package org.apache.cassandra.index.sai.cql;

import java.util.Arrays;
import java.util.Collection;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.index.sai.SAITester;

/**
 * The restrictions on the fields of UDT and tuple columns in the queries that an SAI index on another column serves:
 * the index selects the rows and the restrictions on the fields filter them.
 * <p>
 * Every query is run on two tables with the same rows: one without indexes, where the whole query is filtered, and
 * one with an SAI index on the column {@code v}. They must give the same rows.
 * <p>
 * The tests are run on frozen and on non-frozen UDT columns.
 */
@RunWith(Parameterized.class)
public class UdtFieldFilteringTest extends SAITester
{
    @Parameterized.Parameter
    public boolean frozen;

    @Parameterized.Parameters(name = "frozen={0}")
    public static Collection<Object[]> data()
    {
        return Arrays.asList(new Object[]{ true }, new Object[]{ false });
    }

    private String geo;
    private String address;

    /** The twin table, without indexes, of the current (indexed) table. */
    private String plain;

    @Before
    public void createTypes()
    {
        geo = createType("CREATE TYPE %s (lat double, lon double)");
        address = createType("CREATE TYPE %s (street text, city text, zip int, loc frozen<" + geo + ">)");
    }

    private String udt(String type)
    {
        return frozen ? "frozen<" + type + '>' : type;
    }

    /**
     * Creates the twin tables: first the one without indexes, then the current one, with an index on {@code v}.
     */
    private void createTables(String schema)
    {
        plain = createTable(schema);
        createTable(schema);
        createIndex("CREATE INDEX ON %s(v) USING 'sai'");
    }

    /**
     * Executes a write on both tables. The query must not contain a '%' other than the table placeholder.
     */
    private void write(String query, Object... values)
    {
        execute(query, values);
        execute(query.replace("%s", KEYSPACE + '.' + plain), values);
    }

    /**
     * Runs the checks with the data in the memtables, then after a flush, then after a compaction.
     */
    private void inAllStates(CheckedFunction check) throws Throwable
    {
        beforeAndAfterFlush(() -> {
            flush(KEYSPACE, plain);
            check.apply();
        });
        compact(KEYSPACE, currentTable(), plain);
        check.apply();
    }

    /**
     * Checks that the query returns the rows with the given ids on both tables, with {@code ALLOW FILTERING}.
     */
    private void assertIds(String where, Object... ids)
    {
        assertIdsBound(where, new Object[0], ids);
    }

    private void assertIdsBound(String where, Object[] values, Object... ids)
    {
        Object[][] rows = new Object[ids.length][];
        for (int i = 0; i < ids.length; i++)
            rows[i] = row(ids[i]);
        assertRowsIgnoringOrder(execute("SELECT id FROM " + KEYSPACE + '.' + plain + " WHERE " + where + " ALLOW FILTERING", values), rows);
        assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE " + where + " ALLOW FILTERING", values), rows);
    }

    private void insertAddresses()
    {
        write("INSERT INTO %s (id, v, addr, t) VALUES (1, 1, {street: 'a', city: 'Rome', zip: 200, loc: {lat: 1.0, lon: 0.5}}, ('foo', 1))");
        write("INSERT INTO %s (id, v, addr, t) VALUES (2, 2, {street: 'b', city: 'Paris', zip: 100}, ('bar', 2))");
        write("INSERT INTO %s (id, v, addr, t) VALUES (3, 3, {street: 'bc', city: 'Rome', zip: 300, loc: {lat: 2.5, lon: 1.5}}, ('baz', 3))");
        write("INSERT INTO %s (id, v, addr) VALUES (4, 4, {city: 'Milan'})");
        write("INSERT INTO %s (id, v) VALUES (5, 5)");
        write("INSERT INTO %s (id, v, addr) VALUES (6, 6, {street: '', city: 'Turin', zip: 0})");
        write("INSERT INTO %s (id, addr) VALUES (7, {city: 'Rome', zip: 400})");
    }

    @Test
    public void testFieldsFilteredAfterIndexSearch() throws Throwable
    {
        createTables("CREATE TABLE %s (id int PRIMARY KEY, v int, addr " + udt(address) + ", t frozen<tuple<text, int>>)");
        insertAddresses();

        inAllStates(() -> {
            // the index on v selects the rows, the restrictions on the fields filter them
            assertIds("v > 0 AND addr.city = 'Rome'", 1, 3);
            assertIds("v >= 2 AND addr.city = 'Rome'", 3);
            assertIds("v = 2 AND addr.city = 'Rome'");
            assertIds("v > 0 AND addr.city = 'rome'");
            assertIds("v > 0 AND addr.street = ''", 6);
            assertIds("v > 0 AND addr.zip >= 100 AND addr.zip < 300", 1, 2);
            assertIds("v > 0 AND addr.zip > 100 AND addr.city = 'Rome'", 1, 3);
            assertIds("v > 0 AND addr.loc.lat > 2.0", 3);
            assertIds("v > 0 AND t.field1 = 'bar'", 2);
            assertIds("v > 0 AND t.field2 >= 2", 2, 3);

            // an unset field, or a null column, satisfies no restriction, != and NOT IN included
            assertIds("v > 0 AND addr.city != 'Rome'", 2, 4, 6);
            assertIds("v > 0 AND addr.zip != 200", 2, 3, 6);
            assertIds("v > 0 AND addr.city IN ('Rome', 'Milan')", 1, 3, 4);
            assertIds("v > 0 AND addr.city NOT IN ('Rome', 'Milan')", 2, 6);
            assertIds("v < 3 AND addr.zip NOT IN (100)", 1);

            // OR: all the rows are read
            assertIds("v = 2 OR addr.city = 'Rome'", 1, 2, 3, 7);
            assertIds("v = 2 OR addr.zip IN (0, 400)", 2, 6, 7);
            assertIds("v = 4 AND (addr.city = 'Rome' OR addr.city = 'Milan')", 4);
            assertIds("(v = 1 AND addr.city = 'Rome') OR (v = 2 AND addr.zip = 100) OR addr.zip = 400", 1, 2, 7);
            assertIds("v > 0 AND (addr.city != 'Rome' OR addr.zip = 300)", 2, 3, 4, 6);

            // bind markers
            assertIdsBound("v > ? AND addr.city = ? AND addr.zip > ?", new Object[]{ 0, "Rome", 200 }, 3);
            assertIdsBound("v > ? AND addr.city IN ?", new Object[]{ 0, Arrays.asList("Paris", "Turin") }, 2, 6);

            // only restrictions on fields: the index isn't used
            assertIds("addr.city = 'Rome'", 1, 3, 7);
            assertIds("addr.city = 'Rome' AND addr.zip > 200", 3, 7);
        });

        // the restrictions on the fields need ALLOW FILTERING, also next to a restriction served by the index
        assertInvalidMessage(String.format(StatementRestrictions.FIELD_REQUIRES_ALLOW_FILTERING_MESSAGE, "addr.city"),
                             "SELECT id FROM %s WHERE v > 0 AND addr.city = 'Rome'");
    }

    @Test
    public void testUpdatesAndDeletions() throws Throwable
    {
        createTables("CREATE TABLE %s (id int PRIMARY KEY, v int, addr " + udt(address) + ", t frozen<tuple<text, int>>)");
        insertAddresses();
        flush(KEYSPACE, currentTable(), plain);

        // the new values are in other sstables, or in the memtable
        write("UPDATE %s SET addr = {city: 'Paris', zip: 500} WHERE id = 1");
        if (!frozen)
        {
            write("UPDATE %s SET addr.city = 'Rome' WHERE id = 2");
            write("UPDATE %s SET addr.zip = null WHERE id = 3");
        }
        write("DELETE addr FROM %s WHERE id = 6");
        write("DELETE FROM %s WHERE id = 4");

        inAllStates(() -> {
            if (frozen)
            {
                assertIds("v > 0 AND addr.city = 'Rome'", 3);
                assertIds("v > 0 AND addr.zip > 0", 1, 2, 3);
            }
            else
            {
                assertIds("v > 0 AND addr.city = 'Rome'", 2, 3);
                assertIds("v > 0 AND addr.zip > 0", 1, 2);
            }
            assertIds("v > 0 AND addr.city = 'Paris'", frozen ? new Object[]{ 1, 2 } : new Object[]{ 1 });
            assertIds("v > 0 AND addr.city = 'Turin'");
            assertIds("v > 0 AND addr.city = 'Milan'");
            assertIds("v > 0 AND addr.zip = 500", 1);
        });
    }

    @Test
    public void testClusteringAndStaticColumns() throws Throwable
    {
        createTables("CREATE TABLE %s (k int, c int, id int, v int, s " + udt(address) + " static, addr " + udt(address) + ", PRIMARY KEY (k, c))");
        write("INSERT INTO %s (k, c, id, v, s, addr) VALUES (1, 1, 1, 1, {city: 'Rome', zip: 1}, {city: 'Milan', zip: 10})");
        write("INSERT INTO %s (k, c, id, v, addr) VALUES (1, 2, 2, 2, {city: 'Turin', zip: 20})");
        write("INSERT INTO %s (k, c, id, v, s, addr) VALUES (2, 1, 3, 3, {city: 'Paris', zip: 2}, {city: 'Milan', zip: 30})");
        write("INSERT INTO %s (k, c, id, v) VALUES (3, 1, 4, 4)");

        inAllStates(() -> {
            assertIds("v > 0 AND addr.city = 'Milan'", 1, 3);
            assertIds("v > 1 AND addr.city = 'Milan'", 3);
            assertIds("v > 0 AND addr.zip >= 20", 2, 3);
            // a static field is that of all the rows of the partition
            assertIds("v > 0 AND s.city = 'Rome'", 1, 2);
            assertIds("v > 1 AND s.city = 'Rome'", 2);
            assertIds("v > 0 AND s.zip != 1", 3);
            assertIds("v > 0 AND s.city = 'Rome' AND addr.city = 'Turin'", 2);
            assertIds("v = 3 OR addr.city = 'Turin'", 2, 3);
            assertIds("k = 1 AND v > 0 AND addr.zip < 20", 1);
        });
    }

    /**
     * {@code IS NOT NULL} on a field selects the rows where the field has a value: an empty string is a value.
     */
    @Test
    public void testIsNotNull() throws Throwable
    {
        createTables("CREATE TABLE %s (id int PRIMARY KEY, v int, addr " + udt(address) + ", t frozen<tuple<text, int>>)");
        insertAddresses();

        inAllStates(() -> {
            assertIds("addr.city IS NOT NULL", 1, 2, 3, 4, 6, 7);
            assertIds("addr.street IS NOT NULL", 1, 2, 3, 6);
            assertIds("addr.zip IS NOT NULL", 1, 2, 3, 6, 7);
            assertIds("addr.loc.lat IS NOT NULL", 1, 3);
            assertIds("t.field1 IS NOT NULL", 1, 2, 3);
            assertIds("v > 0 AND addr.street IS NOT NULL", 1, 2, 3, 6);
            assertIds("v > 2 AND addr.zip IS NOT NULL AND addr.city = 'Rome'", 3);
            assertIds("v = 5 OR addr.loc.lon IS NOT NULL", 1, 3, 5);
            assertIds("v > 0 AND addr.zip IS NOT NULL AND addr.zip < 200", 2, 6);
        });

        assertInvalidMessage(String.format(StatementRestrictions.FIELD_REQUIRES_ALLOW_FILTERING_MESSAGE, "addr.city"),
                             "SELECT id FROM %s WHERE v > 0 AND addr.city IS NOT NULL");
        assertInvalidSyntaxMessage("IS NULL restrictions are not supported",
                                   "SELECT id FROM %s WHERE addr.city IS NULL ALLOW FILTERING");
    }
}
