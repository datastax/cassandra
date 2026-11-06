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

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;

import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.restrictions.SingleColumnRestriction;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.analyzer.AnalyzerEqOperatorSupport;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * CQL {@code WHERE} restrictions on the fields of UDT and tuple columns, served by the storage-attached indexes on
 * these fields or by filtering (HCD-653, phase P2: single-valued paths, not through collections).
 * <p>
 * Every query on an indexed field is also run with {@code ALLOW FILTERING} on a twin table without any index, which
 * must give the same rows: the results of the index path and those of the filtering path are the same.
 * <p>
 * The tests are run on frozen and on non-frozen UDT columns.
 */
@RunWith(Parameterized.class)
public class UdtFieldQueryTest extends SAITester
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
    private String person;

    /** The twin table, without indexes, of the current (indexed) table. */
    private String plain;

    @BeforeClass
    public static void setUpNetwork()
    {
        // for the paging and the client warnings
        requireNetwork();
    }

    @Before
    public void createTypes()
    {
        geo = createType("CREATE TYPE %s (lat double, lon double)");
        address = createType("CREATE TYPE %s (street text, city text, zip int, loc frozen<" + geo + ">, tags frozen<set<text>>)");
        person = createType("CREATE TYPE %s (name text, age int, home frozen<" + address + ">)");
    }

    /**
     * @return the CQL type of a UDT column, frozen or not depending on the test parameter
     */
    private String udt(String type)
    {
        return frozen ? "frozen<" + type + '>' : type;
    }

    /**
     * Creates the twin tables: first the one without indexes, then the current one, which gets the indexes.
     */
    private void createTables(String schema)
    {
        plain = createTable(schema);
        createTable(schema);
    }

    private void index(String target)
    {
        index(target, "");
    }

    /**
     * Creates an index on the current table, named after the table and the target: the test framework can't derive
     * the default names of the indexes on fields.
     */
    private void index(String target, String options)
    {
        String name = currentTable() + '_' + target.replaceAll("[^A-Za-z0-9]", "_") + "_idx";
        createIndex("CREATE INDEX " + name + " ON %s(" + target + ") USING 'sai'" + options);
    }

    /**
     * Executes a write on both tables. The query must not contain a '%' other than the table placeholder.
     */
    private void write(String query, Object... values)
    {
        execute(query, values);
        execute(query.replace("%s", KEYSPACE + '.' + plain), values);
    }

    private void flushBoth()
    {
        flush(KEYSPACE, currentTable(), plain);
    }

    private void compactBoth()
    {
        compact(KEYSPACE, currentTable(), plain);
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
        compactBoth();
        check.apply();
    }

    /**
     * Checks that a query on the indexed table returns the rows with the given ids, without ALLOW FILTERING (it's
     * served by the indexes), and that the same query with ALLOW FILTERING on the table without indexes returns the
     * same rows.
     */
    private void assertIds(String where, Object... ids)
    {
        assertIdsBound(where, new Object[0], ids);
    }

    private void assertIdsBound(String where, Object[] values, Object... ids)
    {
        assertIndexedIdsBound(where, values, ids);
        assertFilteredIds(where, values, ids);
    }

    /**
     * Checks the query on the indexed table only, for the queries that only an index can serve (LIKE, analyzers).
     */
    private void assertIndexedIds(String where, Object... ids)
    {
        assertIndexedIdsBound(where, new Object[0], ids);
    }

    private void assertIndexedIdsBound(String where, Object[] values, Object... ids)
    {
        assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE " + where, values), rows(ids));
    }

    private void assertFilteredIds(String where, Object[] values, Object... ids)
    {
        String query = "SELECT id FROM " + KEYSPACE + '.' + plain + " WHERE " + where;
        if (!where.contains("ALLOW FILTERING"))
            query += " ALLOW FILTERING";
        assertRowsIgnoringOrder(execute(query, values), rows(ids));
    }

    private static Object[][] rows(Object... ids)
    {
        Object[][] rows = new Object[ids.length][];
        for (int i = 0; i < ids.length; i++)
            rows[i] = row(ids[i]);
        return rows;
    }

    /**
     * The address rows of the DSE reference run (u_frozen), plus rows with null and missing fields.
     */
    private void insertAddresses()
    {
        write("INSERT INTO %s (id, addr) VALUES (1, {street: 'a', city: 'Rome', zip: 200, loc: {lat: 1.0, lon: 0.5}, tags: {'x', 'y'}})");
        write("INSERT INTO %s (id, addr) VALUES (2, {street: 'b', city: 'Paris', zip: 100, tags: {'y'}})");
        write("INSERT INTO %s (id, addr) VALUES (3, {street: 'bc', city: 'Rome', zip: 300, loc: {lat: 2.5, lon: 1.5}})");
        write("INSERT INTO %s (id, addr) VALUES (4, {city: 'Milan'})");
        write("INSERT INTO %s (id) VALUES (5)");
        write("INSERT INTO %s (id, addr) VALUES (6, {street: '', city: 'Turin', zip: 0})");
    }

    @Test
    public void testEquality() throws Throwable
    {
        createTables("CREATE TABLE %s (id int PRIMARY KEY, addr " + udt(address) + ')');
        index("addr.city");
        index("addr.zip");
        index("addr.street");
        index("addr.loc.lat");
        insertAddresses();

        inAllStates(() -> {
            assertIds("addr.city = 'Rome'", 1, 3);
            assertIds("addr.city = 'Paris'", 2);
            // exact and case-sensitive, as DSE's StrField
            assertIds("addr.city = 'rome'");
            assertIds("addr.city = 'Nowhere'");
            assertIds("addr.zip = 100", 2);
            assertIds("addr.zip = 0", 6);
            // an empty string is a value, as for text columns
            assertIds("addr.street = ''", 6);
            // a nested field
            assertIds("addr.loc.lat = 1.0", 1);
            assertIds("addr.loc.lat = 2.5", 3);

            // IN
            assertIds("addr.city IN ('Rome', 'Milan')", 1, 3, 4);
            assertIds("addr.city IN ('Paris')", 2);
            assertIds("addr.zip IN (100, 300, 400)", 2, 3);

            // NEQ and NOT IN: the rows where the field is unset (or the column is null) don't match; as for text
            // columns, the indexes on text fields don't serve them
            assertIds("addr.city != 'Rome' ALLOW FILTERING", 2, 4, 6);
            assertIds("addr.zip != 200", 2, 3, 6);
            assertIds("addr.city NOT IN ('Rome', 'Milan') ALLOW FILTERING", 2, 6);
            assertIds("addr.zip NOT IN (100, 200)", 3, 6);
            assertIds("addr.loc.lat != 1.0", 3);

            // several fields of the same column, and other operators, are independent restrictions
            assertIds("addr.city = 'Rome' AND addr.zip = 300", 3);
            assertIds("addr.city = 'Rome' AND addr.zip = 100");
            assertIds("addr.city = 'Rome' AND addr.zip > 250", 3);
            assertIds("addr.city = 'Rome' AND addr.loc.lat < 2.0", 1);
            assertIds("addr.city != 'Rome' AND addr.zip != 0 ALLOW FILTERING", 2);
            // two ranges on different fields of the same column are not combined into one
            assertIds("addr.city >= 'P' AND addr.zip <= 200", 1, 2, 6);
            assertIds("addr.city <= 'P' AND addr.zip >= 0");

            // disjunctions
            assertIds("addr.city = 'Paris' OR addr.zip = 300", 2, 3);
            assertIds("addr.city = 'Paris' OR addr.city = 'Milan'", 2, 4);
            assertIds("(addr.city = 'Rome' AND addr.zip = 200) OR addr.loc.lat = 2.5", 1, 3);
            assertIds("id = 1 AND addr.city = 'Rome'", 1);
        });

        assertInvalidMessage(String.format(StatementRestrictions.HAS_UNSUPPORTED_INDEX_RESTRICTION_MESSAGE_SINGLE, "addr.city"),
                             "SELECT id FROM %s WHERE addr.city != 'Rome'");
    }

    @Test
    public void testRanges() throws Throwable
    {
        createTables("CREATE TABLE %s (id int PRIMARY KEY, addr " + udt(address) + ')');
        index("addr.city");
        index("addr.zip");
        index("addr.loc.lat");
        insertAddresses();

        inAllStates(() -> {
            // numeric ranges (DSE direc.zip:[0 TO 5], t.field2:[2 TO *], graph {23 TO *})
            assertIds("addr.zip > 150", 1, 3);
            assertIds("addr.zip >= 100", 1, 2, 3);
            assertIds("addr.zip >= 100 AND addr.zip < 300", 1, 2);
            assertIds("addr.zip > 100 AND addr.zip <= 300", 1, 3);
            assertIds("addr.zip < 100", 6);
            assertIds("addr.loc.lat >= 1.0", 1, 3);
            assertIds("addr.loc.lat > 1.0 AND addr.loc.lat < 3.0", 3);
            // unordered bounds select nothing
            assertIds("addr.zip > 300 AND addr.zip < 100");
            assertIds("addr.zip >= 200 AND addr.zip <= 100");

            // text ranges (DSE addr.city:[P TO S]), in byte order as for text columns (HCD-642)
            assertIds("addr.city >= 'P' AND addr.city <= 'S'", 1, 2, 3);
            assertIds("addr.city > 'Paris'", 1, 3, 6);
            assertIds("addr.city >= 'Paris' AND addr.city <= 'Paris'", 2);
            assertIds("addr.city < 'P'", 4);
            assertIds("addr.city > 'S' AND addr.city < 'A'");
            // a range and NEQ on the same field
            assertIds("addr.city >= 'P' AND addr.city != 'Rome'", 2, 6);
            assertIds("addr.zip >= 0 AND addr.zip NOT IN (0, 300)", 1, 2);
        });
    }

    @Test
    public void testLike() throws Throwable
    {
        createTables("CREATE TABLE %s (id int PRIMARY KEY, addr " + udt(address) + ')');
        index("addr.street");
        insertAddresses();

        inAllStates(() -> {
            // the patterns are bound: the test queries are formatted with String.format
            // DSE {!tuple}addr.street:b*, graph *x and /.*x.*/
            assertIndexedIdsBound("addr.street LIKE ?", new Object[]{ "b%" }, 2, 3);
            assertIndexedIdsBound("addr.street LIKE ?", new Object[]{ "%c" }, 3);
            assertIndexedIdsBound("addr.street LIKE ?", new Object[]{ "%b%" }, 2, 3);
            assertIndexedIdsBound("addr.street LIKE ?", new Object[]{ "_c" }, 3);
            assertIndexedIdsBound("addr.street LIKE ?", new Object[]{ "a" }, 1);
            assertIndexedIdsBound("addr.street LIKE ?", new Object[]{ "z%" });
        });

        // LIKE needs an index on the field
        assertInvalidMessage("LIKE restriction is only supported on properly indexed columns",
                             "SELECT id FROM %s WHERE addr.city LIKE 'R%%' ALLOW FILTERING");
    }

    @Test
    public void testAnalyzedField() throws Throwable
    {
        createTables("CREATE TABLE %s (id int PRIMARY KEY, addr " + udt(address) + ')');
        // DSE lowerCase: an analyzer on a text field
        index("addr.city", " WITH OPTIONS = {'case_sensitive': 'false'}");
        insertAddresses();

        inAllStates(() -> {
            assertIndexedIds("addr.city = 'rome'", 1, 3);
            assertIndexedIds("addr.city = 'ROME'", 1, 3);
            assertIndexedIds("addr.city : 'rome'", 1, 3);
            assertIndexedIds("addr.city = 'rome' OR addr.city = 'PARIS'", 1, 2, 3);
            assertIndexedIdsBound("addr.city LIKE ?", new Object[]{ "ro%" }, 1, 3);
            // an index with an analyzer doesn't serve the ranges, as on a column: with ALLOW FILTERING, the raw
            // values are compared, as without any index
            for (String range : new String[]{ "addr.city > 'p'", "addr.city >= 'p'", "addr.city < 'p'", "addr.city <= 'p'", "addr.city >= 'p' AND addr.city < 's'" })
                assertInvalidMessage("has an index but does not support the operators specified in the query",
                                     "SELECT id FROM %s WHERE " + range);
            assertIds("addr.city >= 'p' AND addr.city < 's' ALLOW FILTERING");
            assertIds("addr.city >= 'P' AND addr.city < 'S' ALLOW FILTERING", 1, 2, 3);
        });
    }

    /**
     * The {@code =} on a field with an analyzed index behaves as on a column, following the
     * {@code equals_behaviour_when_analyzed} option of the index on the field and the index hints: MATCH (the
     * default) makes it a {@code :} with a client warning naming the field and the index, UNSUPPORTED leaves it to
     * the filtering of the raw values, and an analyzed and a raw index on the same field make it ambiguous unless the
     * hints select one of them.
     */
    @Test
    public void testFieldEqualityWithAnalyzers() throws Throwable
    {
        // MATCH, the default
        createTables("CREATE TABLE %s (id int PRIMARY KEY, addr " + udt(address) + ')');
        index("addr.city", " WITH OPTIONS = {'case_sensitive': 'false'}");
        insertAddresses();
        String matchWarning = String.format(AnalyzerEqOperatorSupport.EQ_RESTRICTION_ON_ANALYZED_WARNING, "addr.city", currentTable() + "_addr_city_idx");
        // on the indexed table only: the twin table without index compares the raw values, by design
        inAllStates(() -> {
            assertThat(rowsAndWarnings("addr.city = 'ROME'", 1, 3)).contains(matchWarning);
            assertThat(rowsAndWarnings("addr.city : 'ROME'", 1, 3)).doesNotContain(matchWarning);
            assertThat(rowsAndWarnings("addr.zip = 200 ALLOW FILTERING", 1)).doesNotContain(matchWarning);
        });

        // UNSUPPORTED: the index doesn't serve the =, the filtering compares the raw values
        createTables("CREATE TABLE %s (id int PRIMARY KEY, addr " + udt(address) + ')');
        index("addr.city", " WITH OPTIONS = {'case_sensitive': 'false', 'equals_behaviour_when_analyzed': 'unsupported'}");
        insertAddresses();
        inAllStates(() -> {
            assertInvalidMessage(String.format(StatementRestrictions.HAS_UNSUPPORTED_INDEX_RESTRICTION_MESSAGE_SINGLE, "addr.city"),
                                 "SELECT id FROM %s WHERE addr.city = 'Rome'");
            assertIds("addr.city = 'ROME' ALLOW FILTERING");
            assertIds("addr.city = 'Rome' ALLOW FILTERING", 1, 3);
            assertIndexedIds("addr.city : 'ROME'", 1, 3);
        });

        // AMBIGUOUS: an analyzed and a raw index on the field, without hints
        createTables("CREATE TABLE %s (id int PRIMARY KEY, addr " + udt(address) + ')');
        String analyzed = currentTable() + "_analyzed_idx";
        String raw = currentTable() + "_raw_idx";
        createIndex("CREATE INDEX " + analyzed + " ON %s(addr.city) USING 'sai' WITH OPTIONS = {'case_sensitive': 'false'}");
        createIndex("CREATE INDEX " + raw + " ON %s(addr.city) USING 'sai'");
        insertAddresses();
        String ambiguous = String.format(AnalyzerEqOperatorSupport.EQ_AMBIGUOUS_ERROR, "addr.city", analyzed, raw, analyzed);
        assertInvalidMessage(ambiguous, "SELECT id FROM %s WHERE addr.city = 'Rome'");
        assertInvalidMessage(ambiguous, "SELECT id FROM %s WHERE addr.city = 'Rome' ALLOW FILTERING");
        inAllStates(() -> {
            // the hints select the index, and with it the meaning of the =
            assertIndexedIds("addr.city = 'ROME' WITH included_indexes = {" + raw + '}');
            assertIndexedIds("addr.city = 'Rome' WITH included_indexes = {" + raw + '}', 1, 3);
            assertIndexedIds("addr.city = 'Rome' WITH excluded_indexes = {" + analyzed + '}', 1, 3);
            assertIndexedIds("addr.city = 'ROME' WITH included_indexes = {" + analyzed + '}', 1, 3);
            assertIndexedIds("addr.city = 'ROME' WITH excluded_indexes = {" + raw + '}', 1, 3);
            // the other operators aren't ambiguous: the raw index serves the ranges, the analyzed one ':'
            assertIndexedIds("addr.city >= 'P' AND addr.city < 'S'", 1, 2, 3);
            assertIndexedIds("addr.city : 'ROME'", 1, 3);
        });
        String analyzedWarning = String.format(AnalyzerEqOperatorSupport.EQ_RESTRICTION_ON_ANALYZED_WARNING, "addr.city", analyzed);
        assertThat(rowsAndWarnings("addr.city = 'ROME' WITH included_indexes = {" + analyzed + '}', 1, 3)).contains(analyzedWarning);
        assertThat(rowsAndWarnings("addr.city = 'Rome' WITH included_indexes = {" + raw + '}', 1, 3)).doesNotContain(analyzedWarning);
    }

    /**
     * Executes the query on the indexed table through the driver, checks its rows and returns its client warnings.
     */
    private List<String> rowsAndWarnings(String where, Object... ids)
    {
        com.datastax.driver.core.ResultSet result = executeNet("SELECT id FROM %s WHERE " + where);
        assertThat(result.all().stream().map(row -> row.getInt("id")).collect(Collectors.toList()))
            .as(where).containsExactlyInAnyOrder(Arrays.stream(ids).map(id -> (Integer) id).toArray(Integer[]::new));
        List<String> warnings = result.getExecutionInfo().getWarnings();
        return warnings == null ? List.of() : warnings;
    }

    /**
     * A range on an inet field whose bounds aren't in the byte order of the inet type (an IPv4 lower bound and an
     * IPv6 upper bound) isn't removed as unsatisfiable: SAI compares IPv4 addresses as IPv4-mapped IPv6 ones, as for
     * an inet column (UnorderedRangeBoundsTest#testInet), with the index and with ALLOW FILTERING.
     */
    @Test
    public void testInetFieldRange() throws Throwable
    {
        String net = createType("CREATE TYPE %s (ip inet)");
        createTables("CREATE TABLE %s (id int PRIMARY KEY, a " + udt(net) + ", ip inet)");
        index("a.ip");
        index("ip");
        write("INSERT INTO %s (id, a, ip) VALUES (1, {ip: '240.0.0.1'}, '240.0.0.1')");
        write("INSERT INTO %s (id, a, ip) VALUES (2, {ip: '7ea::1'}, '7ea::1')");

        // the coordinator filters inet values by their bytes (CNDB-12978), so execute the queries internally
        disableCoordinatorExecution();
        try
        {
            inAllStates(() -> {
                String column = "ip > '239.0.0.1' AND ip <= '7ea::1'";
                String field = "a.ip > '239.0.0.1' AND a.ip <= '7ea::1'";
                // served by the indexes, also with ALLOW FILTERING: SAI compares IPv4 addresses as IPv4-mapped IPv6 ones
                for (String where : new String[]{ column, field, column + " ALLOW FILTERING", field + " ALLOW FILTERING" })
                    assertIndexedIds(where, 1, 2);
                // without index, the filter compares the raw bytes of the inet values: no row, on the column as on
                // the field
                assertFilteredIds(column, new Object[0]);
                assertFilteredIds(field, new Object[0]);
            });
        }
        finally
        {
            enableCoordinatorExecution();
        }
    }

    /**
     * The values excluded by != and NOT IN on a text field are compared as they are, also with a case-insensitive
     * index on the field: the results of != and NOT IN don't depend on the analyzers of the indexes. The index serves
     * neither the != nor the ranges on the field, so they need ALLOW FILTERING, alone and after a search on another
     * index, and they give the same rows as the twin table without indexes.
     */
    @Test
    public void testFieldExclusionsCompareRawValues() throws Throwable
    {
        createTables("CREATE TABLE %s (id int PRIMARY KEY, addr " + udt(address) + ", name text, v int)");
        index("addr.city", " WITH OPTIONS = {'case_sensitive': 'false'}");
        index("v");
        String[] cities = { "Rome", "Paris", "Turin", "Milan", "ROME" };
        for (int i = 0; i < cities.length; i++)
            write("INSERT INTO %s (id, addr, name, v) VALUES (?, {city: '" + cities[i] + "', street: '" + cities[i] + "'}, ?, ?)", i + 1, cities[i], i + 1);
        write("INSERT INTO %s (id, addr, v) VALUES (6, {zip: 1}, 6)");

        inAllStates(() -> {
            // the index doesn't serve them
            assertInvalidMessage("has an index but does not support the operators specified in the query",
                                 "SELECT id FROM %s WHERE addr.city != 'ROME'");
            assertInvalidMessage("has an index but does not support the operators specified in the query",
                                 "SELECT id FROM %s WHERE addr.city NOT IN ('rome', 'TURIN')");
            // alone, with ALLOW FILTERING: the raw values are compared (a null field matches no !=)
            assertIds("addr.city != 'ROME' ALLOW FILTERING", 1, 2, 3, 4);
            assertIds("addr.city NOT IN ('rome', 'TURIN') ALLOW FILTERING", 1, 2, 3, 4, 5);
            assertIds("addr.city NOT IN ('Rome', 'Turin') ALLOW FILTERING", 2, 4, 5);
            // with a range, which the index doesn't serve either
            assertIds("addr.city >= 'P' AND addr.city != 'Rome' ALLOW FILTERING", 2, 3, 5);
            assertIds("addr.city >= 'p' AND addr.city != 'ROME' ALLOW FILTERING");
            // after a search on another index
            assertIds("v > 1 AND addr.city != 'rome' ALLOW FILTERING", 2, 3, 4, 5);
            assertIds("v < 6 AND addr.city NOT IN ('PARIS', 'milan') ALLOW FILTERING", 1, 2, 3, 4, 5);
            assertIds("v < 6 AND addr.city NOT IN ('Paris', 'Milan') ALLOW FILTERING", 1, 3, 5);
            // after the analyzed equality the index serves
            assertIndexedIds("addr.city : 'rome' AND v > 1 AND addr.street != 'ROME' ALLOW FILTERING");
            assertIndexedIds("addr.city : 'rome' AND addr.street != 'Rome' ALLOW FILTERING", 5);
            // with bound values
            assertIdsBound("addr.city NOT IN ? ALLOW FILTERING", new Object[]{ List.of("rome", "Turin") }, 1, 2, 4, 5);
            assertIdsBound("addr.city != ? ALLOW FILTERING", new Object[]{ "ROME" }, 1, 2, 3, 4);
            // as another field of the same column without index, and a plain column with the same values
            assertIds("addr.street != 'ROME' ALLOW FILTERING", 1, 2, 3, 4);
            assertIds("name != 'ROME' ALLOW FILTERING", 1, 2, 3, 4);

            // with LIMIT and paging
            for (String where : new String[]{ "addr.city != 'ROME' ALLOW FILTERING", "v > 1 AND addr.city NOT IN ('rome', 'TURIN') ALLOW FILTERING" })
            {
                List<Integer> expected = ids(execute("SELECT id FROM %s WHERE " + where));
                for (int pageSize : new int[]{ 1, 2 })
                {
                    List<Integer> paged = executeNetWithPaging("SELECT id FROM %s WHERE " + where, pageSize).all()
                                                                                                         .stream()
                                                                                                         .map(row -> row.getInt("id"))
                                                                                                         .collect(Collectors.toList());
                    assertThat(paged).as(where + " with page size " + pageSize).containsExactlyInAnyOrderElementsOf(expected);
                    List<Integer> limited = executeNetWithPaging("SELECT id FROM %s WHERE " + where.replace(" ALLOW FILTERING", "") + " LIMIT 1 ALLOW FILTERING", pageSize).all()
                                                                                                                       .stream()
                                                                                                                       .map(row -> row.getInt("id"))
                                                                                                                       .collect(Collectors.toList());
                    assertThat(limited).hasSize(1);
                    assertThat(expected).containsAll(limited);
                }
            }
        });
    }

    /**
     * Two fields of one column with normalizing indexes, restricted by != and NOT IN in the same query: each field
     * compares its raw values, whatever its index (the expressions are grouped and merged per field). The expected
     * rows are computed from the raw values, and the twin table without indexes gives the same rows.
     */
    @Test
    public void testTwoAnalyzedFieldsExclusions() throws Throwable
    {
        createTables("CREATE TABLE %s (id int PRIMARY KEY, addr " + udt(address) + ')');
        index("addr.city", " WITH OPTIONS = {'case_sensitive': 'false'}");
        index("addr.street", " WITH OPTIONS = {'case_sensitive': 'false'}");
        String[][] rows = { { "Rome", "X" }, { "Paris", "x" }, { "Turin", "Y" }, { "ROME", "y" }, { "Milan", "X" }, { "paris", "Z" }, { "Turin", null }, { null, "x" } };
        for (int i = 0; i < rows.length; i++)
        {
            String city = rows[i][0] == null ? "null" : '\'' + rows[i][0] + '\'';
            String street = rows[i][1] == null ? "null" : '\'' + rows[i][1] + '\'';
            write("INSERT INTO %s (id, addr) VALUES (" + (i + 1) + ", {city: " + city + ", street: " + street + "})");
        }

        // the restrictions, and their evaluation on the raw values (null fields match no !=)
        Object[][] queries = {
            { "addr.city >= 'P' AND addr.city != 'ROME' AND addr.street != 'X' ALLOW FILTERING",
              (BiPredicate<String, String>) (c, s) -> c != null && s != null && c.compareTo("P") >= 0 && !c.equals("ROME") && !s.equals("X") },
            { "addr.city >= 'P' AND addr.city NOT IN ('rome', 'PARIS') AND addr.street NOT IN ('x', 'Z') ALLOW FILTERING",
              (BiPredicate<String, String>) (c, s) -> c != null && s != null && c.compareTo("P") >= 0 && !c.equals("rome") && !c.equals("PARIS") && !s.equals("x") && !s.equals("Z") },
            { "addr.city NOT IN ('rome') AND addr.street NOT IN ('X', 'z') ALLOW FILTERING",
              (BiPredicate<String, String>) (c, s) -> c != null && s != null && !c.equals("rome") && !s.equals("X") && !s.equals("z") },
            { "addr.street >= 'x' AND addr.street != 'Y' AND addr.city != 'turin' ALLOW FILTERING",
              (BiPredicate<String, String>) (c, s) -> c != null && s != null && s.compareTo("x") >= 0 && !s.equals("Y") && !c.equals("turin") },
        };

        inAllStates(() -> {
            for (Object[] query : queries)
            {
                @SuppressWarnings("unchecked")
                BiPredicate<String, String> predicate = (BiPredicate<String, String>) query[1];
                List<Integer> expected = new ArrayList<>();
                for (UntypedResultSet.Row row : execute("SELECT id, addr FROM " + KEYSPACE + '.' + plain))
                {
                    int id = row.getInt("id");
                    String city = rows[id - 1][0];
                    String street = rows[id - 1][1];
                    if (predicate.test(city, street))
                        expected.add(id);
                }
                assertThat(expected).as((String) query[0]).isNotEmpty();
                assertThat(ids(execute("SELECT id FROM %s WHERE " + query[0]))).as((String) query[0])
                                                                               .containsExactlyInAnyOrderElementsOf(expected);
                assertThat(ids(execute("SELECT id FROM " + KEYSPACE + '.' + plain + " WHERE " + query[0]))).as((String) query[0])
                                                                                                         .containsExactlyInAnyOrderElementsOf(expected);
            }
        });
    }

    @Test
    public void testNestedFields() throws Throwable
    {
        createTables("CREATE TABLE %s (id int PRIMARY KEY, p " + udt(person) + ')');
        index("p.name");
        index("p.age");
        index("p.home.city");
        index("p.home.loc.lat");

        // DSE u_nonfrozen and u_nested: 1 = {Ann, 30, home Rome}, 2 = {Bob, 40}
        write("INSERT INTO %s (id, p) VALUES (1, {name: 'Ann', age: 30, home: {street: 'a', city: 'Rome', zip: 200, loc: {lat: 1.0, lon: 0.5}}})");
        write("INSERT INTO %s (id, p) VALUES (2, {name: 'Bob', age: 40})");
        write("INSERT INTO %s (id, p) VALUES (3, {name: 'Cid', age: 41, home: {city: 'Milan', loc: {lat: 3.0}}})");

        inAllStates(() -> {
            assertIds("p.home.city = 'Rome'", 1);
            assertIds("p.home.city IN ('Rome', 'Milan')", 1, 3);
            assertIds("p.home.loc.lat = 1.0", 1);
            assertIds("p.home.loc.lat > 0.0", 1, 3);
            assertIds("p.age >= 40", 2, 3);
            assertIds("p.age = 41 AND p.home.city = 'Milan'", 3);
            assertIds("p.name = 'Bob' OR p.home.loc.lat >= 3.0", 2, 3);
            // an unindexed field of the same column, filtered
            assertIds("p.age = 30 AND p.home.zip = 200 ALLOW FILTERING", 1);
            assertIds("p.home.city != 'Rome' ALLOW FILTERING", 3);
        });
    }

    @Test
    public void testTupleComponents() throws Throwable
    {
        Assume.assumeTrue("tuples are always frozen", frozen);

        createTables("CREATE TABLE %s (id int PRIMARY KEY, t tuple<text, int, double>, n tuple<int, tuple<text, int>>)");
        index("t.field1");
        index("t.field2");
        index("t.field3");
        index("n.field2.field1");

        // DSE t_tuple: 1 = (foo, 1, 1.5), 2 = (bar, 2, 2.5)
        write("INSERT INTO %s (id, t, n) VALUES (1, ('foo', 1, 1.5), (1, ('a', 1)))");
        write("INSERT INTO %s (id, t, n) VALUES (2, ('bar', 2, 2.5), (2, ('b', 2)))");
        write("INSERT INTO %s (id, t) VALUES (3, ('baz', null, 3.5))");

        inAllStates(() -> {
            assertIds("t.field1 = 'foo'", 1);
            assertIds("t.field2 >= 2", 2);
            assertIds("t.field2 IN (1, 2)", 1, 2);
            assertIds("t.field3 < 3.0", 1, 2);
            assertIds("t.field1 > 'bar'", 1, 3);
            assertIds("t.field2 != 1", 2);
            assertIds("n.field2.field1 = 'b'", 2);
            assertIds("n.field1 = 1 ALLOW FILTERING", 1);
            assertIndexedIdsBound("t.field1 LIKE ?", new Object[]{ "ba%" }, 2, 3);
        });

        assertInvalidMessage("Undefined field field4", "SELECT id FROM %s WHERE t.field4 = 1");
        assertInvalidMessage("Undefined field field0", "SELECT id FROM %s WHERE t.field0 = 1");
    }

    @Test
    public void testStaleTermsAcrossSSTables() throws Throwable
    {
        createTables("CREATE TABLE %s (id int PRIMARY KEY, addr " + udt(address) + ')');
        index("addr.city");
        index("addr.zip");

        write("INSERT INTO %s (id, addr) VALUES (1, {city: 'Rome', zip: 1})");
        write("INSERT INTO %s (id, addr) VALUES (2, {city: 'Paris', zip: 2})");
        flushBoth();
        // the whole value is overwritten: the old terms in the first SSTable must be filtered out
        write("INSERT INTO %s (id, addr) VALUES (1, {city: 'Milan', zip: 3})");
        // the value is deleted; on a non-frozen UDT, by deleting its fields (the deletion of a whole non-frozen
        // column, flushed in a different SSTable than its cells, isn't filtered by SAI, see the P2 notes in implementation-notes.md)
        write(frozen ? "DELETE addr FROM %s WHERE id = 2" : "DELETE addr.city, addr.zip FROM %s WHERE id = 2");

        CheckedFunction check = () -> {
            assertIds("addr.city = 'Rome'");
            assertIds("addr.city = 'Milan'", 1);
            assertIds("addr.zip = 1");
            assertIds("addr.zip < 3");
            assertIds("addr.zip >= 3", 1);
            assertIds("addr.city = 'Paris'");
            assertIds("addr.city != 'Rome' ALLOW FILTERING", 1);
        };
        check.apply();
        flushBoth();
        check.apply();
        compactBoth();
        check.apply();
    }

    @Test
    public void testNonFrozenFieldUpdates() throws Throwable
    {
        Assume.assumeFalse("single-field updates are only possible on non-frozen UDTs", frozen);

        createTables("CREATE TABLE %s (id int PRIMARY KEY, p " + person + ')');
        index("p.age");
        index("p.name");
        index("p.home.city");

        // DSE u_nonfrozen: 2 = {Bob, 40} then UPDATE SET p.age = 41
        write("INSERT INTO %s (id, p) VALUES (1, {name: 'Ann', age: 30, home: {city: 'Rome'}})");
        write("INSERT INTO %s (id, p) VALUES (2, {name: 'Bob', age: 40})");
        // in the memtable
        write("UPDATE %s SET p.age = 41 WHERE id = 2");
        assertIds("p.age = 41", 2);
        assertIds("p.age = 40");
        assertIds("p.name = 'Bob'", 2);

        // across SSTables
        flushBoth();
        write("UPDATE %s SET p.age = 42 WHERE id = 2");
        write("UPDATE %s SET p.name = 'Anna' WHERE id = 1");
        write("DELETE p.home FROM %s WHERE id = 1");
        write("UPDATE %s SET p.home = {city: 'Milan'} WHERE id = 3");

        CheckedFunction check = () -> {
            // the stale terms of the first SSTable are filtered out
            assertIds("p.age = 41");
            assertIds("p.age = 42", 2);
            assertIds("p.age > 35", 2);
            assertIds("p.name = 'Ann'");
            assertIds("p.name = 'Anna'", 1);
            // the other fields of an updated row are still there
            assertIds("p.age = 30", 1);
            assertIds("p.name = 'Bob' AND p.age = 42", 2);
            // a deleted field
            assertIds("p.home.city = 'Rome'");
            // a row with only one field
            assertIds("p.home.city = 'Milan'", 3);
            assertIds("p.age != 30", 2);
        };
        check.apply();
        flushBoth();
        check.apply();
        compactBoth();
        check.apply();
    }

    @Test
    public void testTTL() throws Throwable
    {
        createTables("CREATE TABLE %s (id int PRIMARY KEY, addr " + udt(address) + ')');
        index("addr.city");
        index("addr.zip");

        write("INSERT INTO %s (id, addr) VALUES (1, {city: 'Rome', zip: 1}) USING TTL 1");
        write("INSERT INTO %s (id, addr) VALUES (2, {city: 'Rome', zip: 2})");
        if (!frozen)
            write("UPDATE %s USING TTL 1 SET addr.zip = 5 WHERE id = 2");
        flushBoth();
        write("INSERT INTO %s (id, addr) VALUES (3, {city: 'Rome', zip: 3})");
        if (!frozen)
            write("UPDATE %s USING TTL 1 SET addr.city = 'Paris' WHERE id = 3");

        // wait for the TTLs to expire
        Thread.sleep(2000);

        CheckedFunction check = () -> {
            assertIds("addr.city = 'Rome'", frozen ? new Object[]{ 2, 3 } : new Object[]{ 2 });
            assertIds("addr.zip = 1");
            assertIds("addr.zip = 5");
            // the TTL of a single field of a non-frozen UDT
            assertIds("addr.zip = 2", frozen ? new Object[]{ 2 } : new Object[0]);
            assertIds("addr.city = 'Paris'");
            assertIds("addr.zip = 3", 3);
        };
        check.apply();
        flushBoth();
        check.apply();
        compactBoth();
        check.apply();
    }

    @Test
    public void testNullAndUnsetFields() throws Throwable
    {
        createTables("CREATE TABLE %s (id int PRIMARY KEY, addr " + udt(address) + ')');
        index("addr.city");
        index("addr.zip");

        write("INSERT INTO %s (id, addr) VALUES (1, {city: null, zip: 1})");
        write("INSERT INTO %s (id, addr) VALUES (2, {city: 'Rome', zip: null})");
        write("INSERT INTO %s (id, addr) VALUES (3, null)");
        write("INSERT INTO %s (id, addr) VALUES (4, {zip: null})");
        write("INSERT INTO %s (id, addr) VALUES (5, {city: 'Milan'})");

        inAllStates(() -> {
            assertIds("addr.city = 'Rome'", 2);
            assertIds("addr.city = 'Milan'", 5);
            assertIds("addr.zip = 1", 1);
            assertIds("addr.zip > 0", 1);
            assertIds("addr.city != 'Rome' ALLOW FILTERING", 5);
            assertIds("addr.zip != 2", 1);
            assertIds("addr.city >= 'A'", 2, 5);
        });

        // null and unset values can't be queried
        assertInvalidMessage("Unsupported null value for field addr.city", "SELECT id FROM %s WHERE addr.city = ?", (Object) null);
        assertInvalidMessage("Unsupported unset value for field addr.city", "SELECT id FROM %s WHERE addr.city = ?", ByteBufferUtil.UNSET_BYTE_BUFFER);
    }

    @Test
    public void testContainsOnCollectionField() throws Throwable
    {
        createTables("CREATE TABLE %s (id int PRIMARY KEY, addr " + udt(address) + ')');
        index("addr.tags");
        index("addr.city");
        insertAddresses();

        inAllStates(() -> {
            // DSE addrs.tags:y, on a single UDT
            assertIds("addr.tags CONTAINS 'y'", 1, 2);
            assertIds("addr.tags CONTAINS 'x'", 1);
            assertIds("addr.tags CONTAINS 'z'");
            assertIds("addr.tags CONTAINS 'x' AND addr.tags CONTAINS 'y'", 1);
            assertIds("addr.tags CONTAINS 'y' AND addr.city = 'Paris'", 2);
            assertIds("addr.tags CONTAINS 'y' OR addr.city = 'Milan'", 1, 2, 4);
            assertIds("addr.tags NOT CONTAINS 'x' AND addr.city != 'Turin' ALLOW FILTERING", 2, 3, 4);
            // EQ on the whole collection field, filtered (the index is on its values)
            assertIds("addr.tags = {'y'} ALLOW FILTERING", 2);
        });
    }

    @Test
    public void testWholeColumnAndFieldIndexes() throws Throwable
    {
        Assume.assumeTrue("the whole-value index is on frozen UDTs", frozen);

        createTables("CREATE TABLE %s (id int PRIMARY KEY, addr " + udt(address) + ')');
        index("addr");
        index("addr.city");
        insertAddresses();

        inAllStates(() -> {
            // each index serves its own form only
            assertIds("addr = {street: 'b', city: 'Paris', zip: 100, tags: {'y'}}", 2);
            assertIds("addr.city = 'Paris'", 2);
            // a field restriction and a whole-value restriction on the same column are independent
            assertIds("addr = {street: 'b', city: 'Paris', zip: 100, tags: {'y'}} AND addr.city = 'Paris'", 2);
            assertIds("addr = {street: 'b', city: 'Paris', zip: 100, tags: {'y'}} AND addr.city = 'Rome'");
            assertIds("addr = {street: 'b', city: 'Paris', zip: 100, tags: {'y'}} OR addr.city = 'Rome'", 1, 2, 3);
            // the whole-value index doesn't serve another field
            assertIds("addr.zip = 100 ALLOW FILTERING", 2);
        });

        assertInvalidMessage(String.format(StatementRestrictions.FIELD_REQUIRES_ALLOW_FILTERING_MESSAGE, "addr.zip"),
                             "SELECT id FROM %s WHERE addr.zip = 100");
    }

    @Test
    public void testFieldIndexOnlyServesItsField() throws Throwable
    {
        createTables("CREATE TABLE %s (id int PRIMARY KEY, addr " + udt(address) + ", t tuple<int, text>)");
        index("addr.loc");
        index("addr.city");
        write("INSERT INTO %s (id, addr, t) VALUES (1, {city: 'Rome', zip: 1, loc: {lat: 1.0, lon: 2.0}}, (1, 'a'))");
        write("INSERT INTO %s (id, addr) VALUES (2, {city: 'Paris', zip: 2, loc: {lat: 3.0, lon: 4.0}})");

        inAllStates(() -> {
            // the index on addr.loc serves addr.loc, not its own fields
            assertIds("addr.loc = {lat: 1.0, lon: 2.0}", 1);
            assertIds("addr.loc.lat = 3.0 ALLOW FILTERING", 2);
            assertIds("addr.zip = 1 ALLOW FILTERING", 1);
            assertIds("addr.city = 'Rome' AND addr.zip = 1 ALLOW FILTERING", 1);
            assertIds("t.field1 = 1 ALLOW FILTERING", 1);
        });

        // the message of the missing ALLOW FILTERING names the fields that no index serves
        assertInvalidMessage(String.format(StatementRestrictions.FIELD_REQUIRES_ALLOW_FILTERING_MESSAGE, "addr.loc.lat"),
                             "SELECT id FROM %s WHERE addr.loc.lat = 3.0");
        assertInvalidMessage(String.format(StatementRestrictions.FIELD_REQUIRES_ALLOW_FILTERING_MESSAGE, "addr.zip"),
                             "SELECT id FROM %s WHERE addr.city = 'Rome' AND addr.zip = 1");
        assertInvalidMessage(String.format(StatementRestrictions.FIELD_REQUIRES_ALLOW_FILTERING_MESSAGE, "t.field1"),
                             "SELECT id FROM %s WHERE t.field1 = 1");
        // an index on the field that doesn't support the operator is named
        assertInvalidMessage(String.format(StatementRestrictions.HAS_UNSUPPORTED_INDEX_RESTRICTION_MESSAGE_SINGLE, "addr.loc"),
                             "SELECT id FROM %s WHERE addr.loc > {lat: 1.0, lon: 2.0}");
    }

    @Test
    public void testKeyAndStaticColumns() throws Throwable
    {
        Assume.assumeTrue("the UDTs of primary key columns are frozen", frozen);

        createTables("CREATE TABLE %s (k1 int, k2 frozen<" + geo + ">, c frozen<" + address + ">, s frozen<" + geo + "> static, id int, " +
                     "PRIMARY KEY ((k1, k2), c)) WITH CLUSTERING ORDER BY (c DESC)");
        index("k2.lat");
        index("c.city");
        index("c.zip");
        index("s.lon");

        write("INSERT INTO %s (k1, k2, c, s, id) VALUES (1, {lat: 1.0, lon: 2.0}, {city: 'Rome', zip: 1}, {lon: 7.0}, 1)");
        write("INSERT INTO %s (k1, k2, c, id) VALUES (1, {lat: 1.0, lon: 2.0}, {city: 'Milan', zip: 2}, 2)");
        write("INSERT INTO %s (k1, k2, c, s, id) VALUES (2, {lat: 3.0}, {city: 'Rome', zip: 3}, {lon: 8.0}, 3)");

        inAllStates(() -> {
            assertIds("c.city = 'Rome'", 1, 3);
            assertIds("c.zip > 1", 2, 3);
            assertIds("c.city = 'Rome' AND c.zip < 3", 1);
            assertIds("k2.lat = 1.0", 1, 2);
            assertIds("k2.lat > 2.0", 3);
            assertIds("s.lon = 8.0", 3);
            assertIds("s.lon = 7.0 AND c.city = 'Milan'", 2);
            assertIds("k1 = 1 AND k2 = {lat: 1.0, lon: 2.0} AND c.city = 'Milan'", 2);
            assertIds("k2.lon = 2.0 ALLOW FILTERING", 1, 2);
            assertIds("c.street = 'x' ALLOW FILTERING");
            // the fields of clustering columns can be under OR, as the regular columns
            assertIds("c.city = 'Milan' OR c.zip = 3", 2, 3);
            assertIds("k2.lat = 3.0 AND (c.city = 'Milan' OR c.zip = 3)", 3);
        });

        // as the partition key columns, their fields can't be restricted under OR: the partition-level and the
        // row-level expressions are evaluated separately
        for (String table : new String[]{ currentTable(), plain })
        {
            assertInvalidMessage(String.format(StatementRestrictions.PARTITION_KEY_RESTRICTION_MUST_BE_TOP_LEVEL, "k2.lat"),
                                 "SELECT id FROM " + KEYSPACE + '.' + table + " WHERE k2.lat = 1.0 OR id = 3 ALLOW FILTERING");
            assertInvalidMessage(String.format(StatementRestrictions.PARTITION_KEY_RESTRICTION_MUST_BE_TOP_LEVEL, "k2.lat"),
                                 "SELECT id FROM " + KEYSPACE + '.' + table + " WHERE id = 3 OR (k2.lat = 1.0 AND id = 1) ALLOW FILTERING");
        }

        // SELECT DISTINCT: the fields of the partition key and static columns may be restricted, not those of the
        // clustering columns, which select rows
        for (String table : new String[]{ currentTable(), plain })
        {
            assertInvalidMessage("SELECT DISTINCT with WHERE clause only supports restriction by partition key and/or static columns.",
                                 "SELECT DISTINCT k1, k2 FROM " + KEYSPACE + '.' + table + " WHERE c.city = 'Rome' ALLOW FILTERING");
            assertInvalidMessage("SELECT DISTINCT with WHERE clause only supports restriction by partition key and/or static columns.",
                                 "SELECT DISTINCT k1, k2 FROM " + KEYSPACE + '.' + table + " WHERE k2.lat = 1.0 AND c.zip = 1 ALLOW FILTERING");
        }
        UntypedResultSet distinct = execute("SELECT DISTINCT k1, k2 FROM " + KEYSPACE + '.' + plain + " WHERE k2.lat = 1.0 ALLOW FILTERING");
        assertThat(distinct.size()).isEqualTo(1);
        assertThat(distinct.one().getInt("k1")).isEqualTo(1);
        distinct = execute("SELECT DISTINCT k1, k2, s FROM " + KEYSPACE + '.' + plain + " WHERE s.lon = 8.0 ALLOW FILTERING");
        assertThat(distinct.size()).isEqualTo(1);
        assertThat(distinct.one().getInt("k1")).isEqualTo(2);
    }

    @Test
    public void testStaticUdtColumn() throws Throwable
    {
        createTables("CREATE TABLE %s (k int, c int, s " + udt(address) + " static, id int, PRIMARY KEY (k, c))");
        index("s.city");
        index("s.zip");

        write("INSERT INTO %s (k, c, s, id) VALUES (1, 1, {city: 'Rome', zip: 1}, 1)");
        write("INSERT INTO %s (k, c, id) VALUES (1, 2, 2)");
        write("INSERT INTO %s (k, c, s, id) VALUES (2, 1, {city: 'Paris', zip: 2}, 3)");
        flushBoth();
        // the static row of the second partition changes in the memtable
        if (frozen)
            write("UPDATE %s SET s = {city: 'Rome', zip: 3} WHERE k = 2");
        else
            write("UPDATE %s SET s.city = 'Rome' WHERE k = 2");

        CheckedFunction check = () -> {
            // the static fields select all the rows of their partitions
            assertIds("s.city = 'Rome'", 1, 2, 3);
            assertIds("s.city = 'Paris'");
            assertIds("s.zip = 1", 1, 2);
            assertIds(frozen ? "s.zip = 3" : "s.zip = 2", 3);
            assertIds("s.city = 'Rome' AND s.zip > 1", 3);
            assertIds("s.city = 'Rome' AND id = 2 ALLOW FILTERING", 2);
            assertIds("k = 2 AND s.city = 'Rome'", 3);
        };
        check.apply();
        flushBoth();
        check.apply();
        compactBoth();
        check.apply();
    }

    @Test
    public void testPaging() throws Throwable
    {
        createTables("CREATE TABLE %s (k1 int, k2 frozen<" + geo + ">, c frozen<" + address + ">, addr " + udt(address) + ", id int, " +
                     "PRIMARY KEY ((k1, k2), c))");
        index("addr.city");
        index("c.zip");
        index("k2.lat");

        int id = 0;
        for (int k = 0; k < 6; k++)
        {
            for (int c = 0; c < 4; c++)
            {
                write("INSERT INTO %s (k1, k2, c, addr, id) VALUES (?, {lat: " + (double) (k % 3) + "}, {zip: " + c + "}, {city: '" + (c % 2 == 0 ? "Rome" : "Paris") + "'}, ?)", k, id++);
            }
            if (k == 2)
                flushBoth();
        }

        CheckedFunction check = () -> {
            for (String where : new String[]{ "addr.city = 'Rome'", "c.zip > 1", "k2.lat >= 1.0 AND addr.city = 'Rome'", "c.zip = 0 OR addr.city = 'Paris'" })
            {
                List<Integer> expected = ids(execute("SELECT id FROM %s WHERE " + where));
                assertThat(expected).isNotEmpty();
                assertThat(ids(execute("SELECT id FROM " + KEYSPACE + '.' + plain + " WHERE " + where + (where.contains("ALLOW") ? "" : " ALLOW FILTERING"))))
                    .containsExactlyInAnyOrderElementsOf(expected);
                for (int pageSize : new int[]{ 1, 2, 5 })
                {
                    List<Integer> paged = executeNetWithPaging("SELECT id FROM %s WHERE " + where, pageSize).all()
                                                                                                         .stream()
                                                                                                         .map(row -> row.getInt("id"))
                                                                                                         .collect(Collectors.toList());
                    assertThat(paged).as(where + " with page size " + pageSize).containsExactlyInAnyOrderElementsOf(expected);
                    List<Integer> limited = executeNetWithPaging("SELECT id FROM %s WHERE " + where + " LIMIT 3", pageSize).all()
                                                                                                                       .stream()
                                                                                                                       .map(row -> row.getInt("id"))
                                                                                                                       .collect(Collectors.toList());
                    assertThat(limited).as(where + " with LIMIT and page size " + pageSize).hasSize(Math.min(3, expected.size()));
                    assertThat(expected).containsAll(limited);
                }
            }
        };
        check.apply();
        compactBoth();
        check.apply();
    }

    private static List<Integer> ids(UntypedResultSet result)
    {
        List<Integer> ids = new ArrayList<>();
        for (UntypedResultSet.Row row : result)
            ids.add(row.getInt("id"));
        return ids;
    }

    /**
     * The guardrail on the intersections of filtered mutable columns, at a consistency level that needs the
     * reconciliation of several replicas: the fields of a frozen UDT are a single cell, those of a non-frozen UDT are
     * different cells.
     */
    @Test
    public void testIntersectionGuardrail()
    {
        // the reconciliation needs several replicas
        String keyspace = createKeyspace("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}");
        String place = createType(keyspace, "CREATE TYPE %s (city text, zip int)");
        String table = createTable(keyspace, "CREATE TABLE %s (id int PRIMARY KEY, addr " + udt(place) + ", v int)");

        assertThat(intersectionWarnings(keyspace, table, "addr.city = 'x' AND addr.zip = 1")).isEqualTo(!frozen);
        assertThat(intersectionWarnings(keyspace, table, "addr.zip > 1 AND addr.zip < 3")).isFalse();
        // as for two columns
        assertThat(intersectionWarnings(keyspace, table, "addr.city = 'x' AND v = 1")).isTrue();
    }

    private static boolean intersectionWarnings(String keyspace, String table, String where)
    {
        String query = "SELECT id FROM " + keyspace + '.' + table + " WHERE " + where + " ALLOW FILTERING";
        ClientState state = ClientState.forExternalCalls(new InetSocketAddress(FBUtilities.getJustLocalAddress(), 9042));
        SelectStatement statement = (SelectStatement) QueryProcessor.parseStatement(query, state);
        ClientWarn.instance.captureWarnings();
        try
        {
            statement.getRowFilter(QueryOptions.forInternalCalls(ConsistencyLevel.QUORUM, List.of()), state);
            List<String> warnings = ClientWarn.instance.getWarnings();
            return warnings != null && warnings.stream().anyMatch(w -> w.contains("intersection on mutable columns"));
        }
        finally
        {
            ClientWarn.instance.resetWarnings();
        }
    }

    @Test
    public void testQuotedNames() throws Throwable
    {
        String type = createType("CREATE TYPE %s (\"City\" text, zip int)");
        createTables("CREATE TABLE %s (id int PRIMARY KEY, \"Addr\" " + udt(type) + ')');
        index("\"Addr\".\"City\"");
        write("INSERT INTO %s (id, \"Addr\") VALUES (1, {\"City\": 'Rome', zip: 1})");

        ResultMessage.Prepared prepared = prepare("SELECT id FROM %s WHERE \"Addr\".\"City\" = ? AND \"Addr\".zip = ? ALLOW FILTERING");
        assertThat(prepared.metadata.names.get(0).name.toString()).isEqualTo("\"Addr\".\"City\"");
        assertThat(prepared.metadata.names.get(1).name.toString()).isEqualTo("\"Addr\".zip");

        inAllStates(() -> {
            assertIds("\"Addr\".\"City\" = 'Rome'", 1);
            assertIdsBound("\"Addr\".\"City\" = ? AND \"Addr\".zip = ? ALLOW FILTERING", new Object[]{ "Rome", 1 }, 1);
        });
        assertInvalidMessage("Undefined field city", "SELECT id FROM %s WHERE \"Addr\".city = 'Rome' ALLOW FILTERING");
        assertInvalidMessage("Invalid unset value for column \"Addr\".\"City\"",
                             "SELECT id FROM %s WHERE \"Addr\".\"City\" IN (?, ?)", "Rome", ByteBufferUtil.UNSET_BYTE_BUFFER);
    }

    /**
     * The warning for the CONTAINS restrictions served by several indexes names the field, once per field.
     */
    @Test
    public void testMultipleIndexesWarningPerField()
    {
        String type = createType("CREATE TYPE %s (tags frozen<set<text>>, labels frozen<set<text>>)");
        createTables("CREATE TABLE %s (id int PRIMARY KEY, x " + udt(type) + ')');
        for (String field : new String[]{ "tags", "labels" })
        {
            index("x." + field);
            createIndex("CREATE INDEX " + currentTable() + '_' + field + "_analyzed_idx ON %s(x." + field + ") USING 'sai' WITH OPTIONS = {'case_sensitive': 'false'}");
        }
        write("INSERT INTO %s (id, x) VALUES (1, {tags: {'a'}, labels: {'b'}})");

        // each field gets its own warning, naming the field
        for (String field : new String[]{ "tags", "labels" })
        {
            com.datastax.driver.core.ResultSet result = executeNet("SELECT id FROM %s WHERE x." + field + " CONTAINS '" + (field.equals("tags") ? 'a' : 'b') + "'");
            assertThat(result.all()).hasSize(1);
            String expected = String.format(SingleColumnRestriction.ContainsRestriction.MULTIPLE_INDEXES_WARNING,
                                            "x." + field, currentTable() + '_' + "x_" + field + "_idx");
            assertThat(result.getExecutionInfo().getWarnings()).contains(expected);
        }
    }

    @Test
    public void testPreparedStatements() throws Throwable
    {
        createTables("CREATE TABLE %s (id int PRIMARY KEY, addr " + udt(address) + ", t tuple<text, int>)");
        index("addr.city");
        index("addr.zip");
        index("addr.loc.lat");
        index("t.field2");
        insertAddresses();
        write("UPDATE %s SET t = ('a', 7) WHERE id = 1");

        // the bind markers get the type and the name of the field
        ResultMessage.Prepared prepared = prepare("SELECT id FROM %s WHERE addr.city = ? AND addr.zip > ? AND addr.loc.lat < ? AND t.field2 IN ?");
        List<ColumnSpecification> names = prepared.metadata.names;
        assertThat(names).hasSize(4);
        assertThat(names.get(0).name.toString()).isEqualTo("addr.city");
        assertThat(names.get(0).type).isEqualTo(UTF8Type.instance);
        assertThat(names.get(1).name.toString()).isEqualTo("addr.zip");
        assertThat(names.get(1).type).isEqualTo(Int32Type.instance);
        assertThat(names.get(2).name.toString()).isEqualTo("addr.loc.lat");
        assertThat(names.get(2).type).isEqualTo(DoubleType.instance);
        assertThat(names.get(3).type).isEqualTo(ListType.getInstance(Int32Type.instance, false));

        inAllStates(() -> {
            assertIdsBound("addr.city = ?", new Object[]{ "Rome" }, 1, 3);
            assertIdsBound("addr.zip > ?", new Object[]{ 150 }, 1, 3);
            assertIdsBound("addr.city = ? AND addr.zip > ? AND addr.loc.lat < ?", new Object[]{ "Rome", 150, 2.0 }, 1);
            assertIdsBound("addr.city IN ?", new Object[]{ list("Paris", "Milan") }, 2, 4);
            assertIdsBound("addr.city IN (?, ?)", new Object[]{ "Paris", "Turin" }, 2, 6);
            assertIdsBound("addr.zip NOT IN ?", new Object[]{ list(100, 200) }, 3, 6);
            assertIdsBound("addr.city != ? ALLOW FILTERING", new Object[]{ "Rome" }, 2, 4, 6);
            assertIdsBound("t.field2 IN ?", new Object[]{ list(7, 8) }, 1);
            assertIdsBound("t.field2 = :v", new Object[]{ 7 }, 1);
        });
    }

    @Test
    public void testInvalidQueries() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, v int, addr " + udt(address) + ", addrs list<frozen<" + address + ">>, " +
                    "m map<text, frozen<" + address + ">>, t tuple<int, text>)");
        index("addr.city");

        // not a UDT or tuple
        assertInvalidMessage("Invalid restriction on v.x: Cannot access field x of the column of type int, which is neither a user-defined type nor a tuple",
                             "SELECT id FROM %s WHERE v.x = 1 ALLOW FILTERING");
        assertInvalidMessage("Undefined column name nope", "SELECT id FROM %s WHERE nope.x = 1 ALLOW FILTERING");
        // unknown field
        assertInvalidMessage("Invalid restriction on addr.nope: Undefined field nope in the column of type",
                             "SELECT id FROM %s WHERE addr.nope = 1 ALLOW FILTERING");
        // wrong depth
        assertInvalidMessage("Cannot access field x of field city of type text, which is neither a user-defined type nor a tuple",
                             "SELECT id FROM %s WHERE addr.city.x = 'a' ALLOW FILTERING");
        assertInvalidMessage("Undefined field nope in field loc",
                             "SELECT id FROM %s WHERE addr.loc.nope = 1.0 ALLOW FILTERING");
        // wrong value type
        assertInvalidMessage("Invalid STRING constant (a) for",
                             "SELECT id FROM %s WHERE addr.zip = 'a' ALLOW FILTERING");
        // the fields inside collections (see UdtCollectionFieldQueryTest)
        assertEmpty(execute("SELECT id FROM %s WHERE addrs.city = 'Rome' ALLOW FILTERING"));
        assertEmpty(execute("SELECT id FROM %s WHERE m.zip > 1 ALLOW FILTERING"));
        // CONTAINS on a field that isn't a collection, and the map-key forms
        assertInvalidMessage("Cannot use CONTAINS on non-collection field addr.city",
                             "SELECT id FROM %s WHERE addr.city CONTAINS 'R' ALLOW FILTERING");
        assertInvalidMessage("Cannot use NOT CONTAINS on non-collection field addr.zip",
                             "SELECT id FROM %s WHERE addr.zip NOT CONTAINS 1 ALLOW FILTERING");
        assertInvalidMessage("is not supported on fields: addr.tags",
                             "SELECT id FROM %s WHERE addr.tags CONTAINS KEY 'a' ALLOW FILTERING");
        // IS NOT NULL (P5, see UdtFieldIsNotNullTest)
        execute("INSERT INTO %s (id, addr) VALUES (1, {city: 'Rome'})");
        execute("INSERT INTO %s (id, addr) VALUES (2, {zip: 1})");
        assertRows(execute("SELECT id FROM %s WHERE addr.city IS NOT NULL"), row(1));
        execute("DELETE FROM %s WHERE id IN (1, 2)");
        // the field restrictions on an equality
        assertInvalidMessage("addr.city cannot be restricted by more than one relation if it includes an Equal",
                             "SELECT id FROM %s WHERE addr.city = 'a' AND addr.city = 'b'");
        assertInvalidMessage("More than one restriction was found for the start bound on addr.zip",
                             "SELECT id FROM %s WHERE addr.zip > 1 AND addr.zip > 2 ALLOW FILTERING");
        // only in SELECT
        assertInvalidMessage(String.format(StatementRestrictions.FIELD_RESTRICTIONS_ONLY_IN_SELECT_MESSAGE, "addr.city = 'a'"),
                             "UPDATE %s SET v = 1 WHERE id = 1 AND addr.city = 'a'");
        assertInvalidMessage(String.format(StatementRestrictions.FIELD_RESTRICTIONS_ONLY_IN_SELECT_MESSAGE, "addr.city = 'a'"),
                             "DELETE FROM %s WHERE id = 1 AND addr.city = 'a'");
        // the non-frozen UDTs can't be restricted as a whole, but their fields can
        if (!frozen)
            assertInvalidMessage("Non-frozen UDT column 'addr'", "SELECT id FROM %s WHERE addr = {city: 'a'} ALLOW FILTERING");
        // no ANN, ordering or BM25 on fields: the grammar has no field in ORDER BY
        assertInvalidSyntax("SELECT id FROM %s ORDER BY addr.city LIMIT 10");
        // a map-entry relation on a field
        assertInvalidSyntax("SELECT id FROM %s WHERE addr.m['a'] = 1 ALLOW FILTERING");
        // the restriction with the field of a column whose name has a dot is a field restriction
        assertInvalidSyntax("SELECT id FROM %s WHERE addr. = 1 ALLOW FILTERING");
    }

    @Test
    public void testColumnNameWithDot() throws Throwable
    {
        createTables("CREATE TABLE %s (id int PRIMARY KEY, addr " + udt(address) + ", \"addr.city\" text)");
        index("addr.city");
        index("\"addr.city\"");
        write("INSERT INTO %s (id, addr, \"addr.city\") VALUES (1, {city: 'Rome'}, 'Milan')");
        write("INSERT INTO %s (id, addr, \"addr.city\") VALUES (2, {city: 'Milan'}, 'Rome')");

        inAllStates(() -> {
            assertIds("addr.city = 'Rome'", 1);
            assertIds("\"addr.city\" = 'Rome'", 2);
            assertIds("addr.city = 'Rome' AND \"addr.city\" = 'Milan'", 1);
        });
    }

    /**
     * The single-UDT rows of the DSE Search reference run ({@code docs/sai-udt/dse-runtime.md}), with the
     * {@code solr_query} of each CQL query.
     */
    @Test
    public void testDseReferenceRows() throws Throwable
    {
        Assume.assumeTrue("run once", frozen);

        // u_frozen
        createTables("CREATE TABLE %s (id int PRIMARY KEY, addr frozen<" + address + ">)");
        index("addr.street");
        index("addr.city");
        index("addr.zip");
        index("addr.loc.lat");
        write("INSERT INTO %s (id, addr) VALUES (1, {street: 'a', city: 'Rome', zip: 200, loc: {lat: 1.0, lon: 0.5}})");
        write("INSERT INTO %s (id, addr) VALUES (2, {street: 'b', city: 'Paris', zip: 100})");
        inAllStates(() -> {
            assertIds("addr.city = 'Rome'", 1);                         // {!tuple}addr.city:Rome
            assertIds("addr.loc.lat = 1.0", 1);                         // {!tuple}addr.loc.lat:1.0
            assertIndexedIdsBound("addr.street LIKE ?", new Object[]{ "b%" }, 2); // {!tuple}addr.street:b*
            assertIds("addr.city = 'rome'");                            // {!tuple}addr.city:rome
            assertIds("addr.city >= 'P' AND addr.city <= 'S'", 1, 2);   // {!tuple}addr.city:[P TO S]
            assertIds("addr.zip >= 0 AND addr.zip <= 150", 2);          // direc.zip:[0 TO 5]
            assertIds("id = 1 AND addr.city = 'Rome'", 1);              // id:1 AND {!tuple}addr.city:Rome
        });

        // u_nonfrozen
        createTables("CREATE TABLE %s (id int PRIMARY KEY, p " + person + ')');
        index("p.age");
        index("p.home.city");
        write("INSERT INTO %s (id, p) VALUES (1, {name: 'Ann', age: 30, home: {city: 'Rome'}})");
        write("INSERT INTO %s (id, p) VALUES (2, {name: 'Bob', age: 40})");
        write("UPDATE %s SET p.age = 41 WHERE id = 2");
        inAllStates(() -> {
            assertIds("p.age = 41", 2);                                 // {!tuple}p.age:41
            assertIds("p.home.city = 'Rome'", 1);                       // {!tuple}p.home.city:Rome
        });

        // u_nested
        createTables("CREATE TABLE %s (id int PRIMARY KEY, p frozen<" + person + ">)");
        index("p.home.city");
        index("p.name");
        write("INSERT INTO %s (id, p) VALUES (1, {name: 'Bilbo', age: 30, home: {city: 'Rome'}})");
        write("INSERT INTO %s (id, p) VALUES (2, {name: 'Sam', age: 40})");
        inAllStates(() -> {
            assertIds("p.home.city = 'Rome'", 1);                       // {!tuple}p.home.city:Rome
            assertIds("p.name IN ('Bilbo', 'Sam')", 1, 2);              // direc.name.firstname:(Bilbo OR Sam)
            assertIds("p.name != 'Bilbo' ALLOW FILTERING", 2);          // graph Neq
        });

        // t_tuple
        createTables("CREATE TABLE %s (id int PRIMARY KEY, t tuple<text, int, double>)");
        index("t.field1");
        index("t.field2");
        write("INSERT INTO %s (id, t) VALUES (1, ('foo', 1, 1.5))");
        write("INSERT INTO %s (id, t) VALUES (2, ('bar', 2, 2.5))");
        inAllStates(() -> {
            assertIds("t.field1 = 'foo'", 1);                           // {!tuple}t.field1:foo
            assertIds("t.field2 >= 2", 2);                              // {!tuple}t.field2:[2 TO *]
        });
    }
}
