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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.messages.ResultMessage;

import static org.apache.cassandra.config.CassandraRelevantProperties.SAI_VALIDATE_MAX_TERM_SIZE_AT_COORDINATOR;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code IS NOT NULL} on the fields of UDT and tuple columns (HCD-653, phase P5): {@code addr.city IS NOT NULL}
 * selects the rows where the field is set (DSE Search {@code {!tuple}addr.city:*}). A field is set when its value is
 * present: in a frozen value, a component that is present and not null (a value written before an
 * {@code ALTER TYPE ... ADD} doesn't have the new field); in a non-frozen UDT, a live cell. An empty string is set,
 * as for the literal columns, but the empty values of the other types are not (the indexes don't index them). On a
 * path through collections, some element has the field set; in a {@code CONTAINS ELEMENT} group, the element.
 * <p>
 * An index on the field serves it by the scan of all its terms. Every query on the indexed table is also run with
 * {@code ALLOW FILTERING} on a twin table without any index, which must give the same rows. The tests are run on
 * frozen and non-frozen UDTs and collections, with the data in the memtables, after a flush and after a compaction.
 */
@RunWith(Parameterized.class)
public class UdtFieldIsNotNullTest extends SAITester
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

    @BeforeClass
    public static void setUpNetwork()
    {
        // for the paging
        requireNetwork();
    }

    @Before
    public void createTypes()
    {
        geo = createType("CREATE TYPE %s (lat double, lon double)");
        address = createType("CREATE TYPE %s (street text, city text, zip int, loc frozen<" + geo + ">, tags frozen<set<text>>)");
    }

    private String udt(String type)
    {
        return frozen ? "frozen<" + type + '>' : type;
    }

    private String list(String element)
    {
        return frozen ? "frozen<list<" + element + ">>" : "list<" + element + '>';
    }

    private void createTables(String schema)
    {
        plain = createTable(schema);
        createTable(schema);
    }

    private void index(String target)
    {
        index(target, "");
    }

    private void index(String target, String options)
    {
        String name = currentTable() + '_' + target.replaceAll("[^A-Za-z0-9]", "_") + "_idx";
        createIndex("CREATE INDEX " + name + " ON %s(" + target + ") USING 'sai'" + options);
    }

    private void write(String query, Object... values)
    {
        execute(query, values);
        execute(query.replace("%s", KEYSPACE + '.' + plain), values);
    }

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
     * Checks the query on the indexed table (without ALLOW FILTERING if it isn't in the query: it's served by the
     * indexes) and with ALLOW FILTERING on the twin table without indexes.
     */
    private void assertIds(String where, Object... ids)
    {
        assertIdsBound(where, new Object[0], ids);
    }

    private void assertIdsBound(String where, Object[] values, Object... ids)
    {
        assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE " + where, values), rows(ids));
        assertFilteredIds(where, values, ids);
    }

    private void assertIndexedIds(String where, Object... ids)
    {
        assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE " + where), rows(ids));
    }

    private void assertFilteredIds(String where, Object[] values, Object... ids)
    {
        assertRowsIgnoringOrder(execute(filtered("SELECT id FROM " + KEYSPACE + '.' + plain + " WHERE " + where), values), rows(ids));
    }

    private static String filtered(String query)
    {
        return query.contains("ALLOW FILTERING") ? query : query + " ALLOW FILTERING";
    }

    private static Object[][] rows(Object... ids)
    {
        Object[][] rows = new Object[ids.length][];
        for (int i = 0; i < ids.length; i++)
            rows[i] = row(ids[i]);
        return rows;
    }

    private static List<Integer> ids(UntypedResultSet result)
    {
        List<Integer> ids = new ArrayList<>();
        for (UntypedResultSet.Row row : result)
            ids.add(row.getInt("id"));
        return ids;
    }

    /**
     * The DSE Search u_frozen rows ({@code docs/sai-udt/dse-runtime.md}): 1 = {a, Rome, 200, lat 1},
     * 2 = {b, Paris, 100, no loc}, plus an empty street and no other field (3), no UDT (4), a loc without lat (5).
     */
    @Test
    public void testFields() throws Throwable
    {
        createTables("CREATE TABLE %s (id int PRIMARY KEY, addr " + udt(address) + ", t tuple<text, int, double>)");
        index("addr.city");
        index("addr.street");
        index("addr.zip");
        index("addr.loc.lat");
        index("t.field1");
        index("t.field3");
        index("full(addr.tags)");

        write("INSERT INTO %s (id, addr, t) VALUES (1, {street: 'a', city: 'Rome', zip: 200, loc: {lat: 1.0, lon: 0.5}, tags: {}}, ('foo', 1, 1.5))");
        write("INSERT INTO %s (id, addr, t) VALUES (2, {street: 'b', city: 'Paris', zip: 100}, ('bar', 2, null))");
        write("INSERT INTO %s (id, addr, t) VALUES (3, {street: ''}, ('', null, null))");
        write("INSERT INTO %s (id) VALUES (4)");
        write("INSERT INTO %s (id, addr, t) VALUES (5, {loc: {lon: 2.0}, tags: {'x'}}, (null, null, 2.5))");

        inAllStates(() -> {
            assertIds("addr.city IS NOT NULL", 1, 2);                          // {!tuple}addr.city:*
            assertIds("addr.loc.lat IS NOT NULL", 1);                          // {!tuple}addr.loc.lat:*
            assertIds("addr.zip IS NOT NULL", 1, 2);
            // an empty string is set
            assertIds("addr.street IS NOT NULL", 1, 2, 3);
            assertIds("t.field1 IS NOT NULL", 1, 2, 3);
            assertIds("t.field3 IS NOT NULL", 1, 5);
            // a frozen collection field, empty or not, through an index on the whole field
            assertIds("addr.tags IS NOT NULL", 1, 5);
            // the unindexed fields
            assertIds("addr.loc IS NOT NULL ALLOW FILTERING", 1, 5);
            assertIds("addr.loc.lon IS NOT NULL ALLOW FILTERING", 1, 5);
            assertIds("t.field2 IS NOT NULL ALLOW FILTERING", 1, 2);
            // with other restrictions on the same field, on other fields and in disjunctions
            assertIds("addr.city IS NOT NULL AND addr.city = 'Rome'", 1);
            assertIds("addr.city IS NOT NULL AND addr.city >= 'P'", 1, 2);
            assertIds("addr.zip IS NOT NULL AND addr.zip > 150", 1);
            assertIds("addr.zip > 150 AND addr.zip IS NOT NULL", 1);
            assertIds("addr.zip != 100 AND addr.zip IS NOT NULL", 1);
            assertIds("addr.city IS NOT NULL AND addr.loc.lat IS NOT NULL", 1);
            assertIds("addr.loc.lat IS NOT NULL OR addr.street = ''", 1, 3);
            assertIds("addr.city IS NOT NULL OR t.field3 IS NOT NULL", 1, 2, 5);
            assertIds("id = 3 AND addr.street IS NOT NULL", 3);
            assertIds("id = 4 AND addr.street IS NOT NULL");
            // an unindexed IS NOT NULL post-filtered by SAI next to an indexed restriction
            assertIds("addr.city = 'Rome' AND addr.loc IS NOT NULL ALLOW FILTERING", 1);
            assertIds("addr.city = 'Paris' AND addr.loc IS NOT NULL ALLOW FILTERING");
            assertIds("addr.zip > 0 AND addr.loc.lon IS NOT NULL ALLOW FILTERING", 1);
        });

        // the paging and LIMIT
        assertPaging("addr.street IS NOT NULL");
        assertPaging("addr.city IS NOT NULL OR t.field3 IS NOT NULL");

        // the fields set to null and the deleted fields of a non-frozen UDT
        if (!frozen)
        {
            write("UPDATE %s SET addr.city = null WHERE id = 1");
            write("DELETE addr.street FROM %s WHERE id = 3");
            write("UPDATE %s SET addr.zip = 7 WHERE id = 5");
            inAllStates(() -> {
                assertIds("addr.city IS NOT NULL", 2);
                assertIds("addr.street IS NOT NULL", 1, 2);
                assertIds("addr.zip IS NOT NULL", 1, 2, 5);
            });
        }
    }

    /**
     * IS NOT NULL on the fields of all the types an index can serve with the scan of all its terms, numeric and
     * literal, including those served only by equality otherwise (uuid, inet), on the empty values.
     */
    @Test
    public void testTypes() throws Throwable
    {
        String kinds = createType("CREATE TYPE %s (b boolean, u uuid, ti timeuuid, d double, i int, bi bigint, dec decimal, vi varint, " +
                                  "ts timestamp, a ascii, ip inet, bl blob, dt date)");
        createTables("CREATE TABLE %s (id int PRIMARY KEY, k " + udt(kinds) + ')');
        // blob is not indexable: filtered only
        for (String field : new String[]{ "b", "u", "ti", "d", "i", "bi", "dec", "vi", "ts", "a", "ip", "dt" })
            index("k." + field);

        write("INSERT INTO %s (id, k) VALUES (1, {b: true, u: 6ba7b810-9dad-11d1-80b4-00c04fd430c8, ti: 50554d6e-29bb-11e5-b345-feff819cdc9f, " +
              "d: 1.5, i: 1, bi: 2, dec: 3.5, vi: 4, ts: '2026-01-01', a: 'x', ip: '127.0.0.1', bl: 0x01, dt: '2026-01-01'})");
        write("INSERT INTO %s (id, k) VALUES (2, {b: false, a: '', bl: 0x})");
        write("INSERT INTO %s (id, k) VALUES (3, {b: null})");
        // the empty values of the non-literal types are not set
        write("INSERT INTO %s (id, k) VALUES (4, {i: blobAsInt(0x), d: blobAsDouble(0x)})");
        write("INSERT INTO %s (id, k) VALUES (5, {i: blobAsInt(0x), b: true})");
        // but the empty values of the literal types (text, ascii, boolean) are, the empty boolean included
        write("INSERT INTO %s (id, k) VALUES (6, {b: blobAsBoolean(0x)})");

        inAllStates(() -> {
            assertIds("k.b IS NOT NULL", 1, 2, 5, 6);
            assertIds("k.u IS NOT NULL", 1);
            assertIds("k.ti IS NOT NULL", 1);
            assertIds("k.d IS NOT NULL", 1);
            assertIds("k.i IS NOT NULL", 1);
            assertIds("k.bi IS NOT NULL", 1);
            assertIds("k.dec IS NOT NULL", 1);
            assertIds("k.vi IS NOT NULL", 1);
            assertIds("k.ts IS NOT NULL", 1);
            assertIds("k.a IS NOT NULL", 1, 2);
            assertIds("k.ip IS NOT NULL", 1);
            // the empty blob is not set: a blob is not a literal type
            assertIds("k.bl IS NOT NULL ALLOW FILTERING", 1);
            assertIds("k.dt IS NOT NULL", 1);
            assertIds("k.b IS NOT NULL AND k.b = false", 2);
            // combined with another expression on the same field, IS NOT NULL still excludes the empty values: the
            // anti-join of != returns rows 4 and 5, the scan of b = true row 5
            assertIds("k.i != 5 AND k.i IS NOT NULL", 1);
            assertIds("k.i < 5 AND k.i IS NOT NULL AND k.b = true", 1);
            assertIds("k.i IS NOT NULL AND k.b = true", 1);
            assertIds("k.d > 0 AND k.d IS NOT NULL", 1);
        });
    }

    /**
     * IS NOT NULL combined with another restriction on the same field keeps rejecting the empty values, which satisfy
     * the other restriction: in the SAI post-filter of a query driven by another index (the field is unindexed, so
     * that its expressions are only post-filtered), as in the filtering.
     */
    @Test
    public void testCombinedWithOtherRestrictions() throws Throwable
    {
        String kinds = createType("CREATE TYPE %s (b boolean, i int, d double, t text)");
        createTables("CREATE TABLE %s (id int PRIMARY KEY, k " + udt(kinds) + ')');
        index("k.b");
        write("INSERT INTO %s (id, k) VALUES (1, {b: true, i: 1, d: 1.0, t: 'a'})");
        write("INSERT INTO %s (id, k) VALUES (2, {b: true, i: blobAsInt(0x), d: blobAsDouble(0x), t: ''})");
        write("INSERT INTO %s (id, k) VALUES (3, {b: true})");

        inAllStates(() -> {
            assertIds("k.b = true AND k.i != 5 AND k.i IS NOT NULL ALLOW FILTERING", 1);
            assertIds("k.b = true AND k.i < 5 AND k.i IS NOT NULL ALLOW FILTERING", 1);
            assertIds("k.b = true AND k.i IS NOT NULL AND k.i != 5 ALLOW FILTERING", 1);
            assertIds("k.b = true AND k.d < 5 AND k.d IS NOT NULL ALLOW FILTERING", 1);
            assertIds("k.b = true AND k.d != 5 AND k.d IS NOT NULL ALLOW FILTERING", 1);
            // the empty strings are set
            assertIds("k.b = true AND k.t != 'x' AND k.t IS NOT NULL ALLOW FILTERING", 1, 2);
        });
    }

    /**
     * The fields added by {@code ALTER TYPE ... ADD}: the values written before have no value for them.
     */
    @Test
    public void testAddedField() throws Throwable
    {
        createTables("CREATE TABLE %s (id int PRIMARY KEY, addr " + udt(address) + ", addrs " + list("frozen<" + address + '>') + ')');
        index("addr.city");
        write("INSERT INTO %s (id, addr, addrs) VALUES (1, {city: 'Rome'}, [{city: 'Rome'}])");
        flush(KEYSPACE, currentTable(), plain);

        execute("ALTER TYPE " + KEYSPACE + '.' + address + " ADD country text");
        index("addr.country");
        index("addrs.country");
        write("INSERT INTO %s (id, addr, addrs) VALUES (2, {city: 'Milan', country: 'IT'}, [{city: 'Milan'}, {country: 'IT'}])");
        write("INSERT INTO %s (id, addr, addrs) VALUES (3, {city: 'Paris'}, [{city: 'Paris'}])");

        inAllStates(() -> {
            assertIds("addr.country IS NOT NULL", 2);
            assertIds("addrs.country IS NOT NULL", 2);
            assertIds("addr.city IS NOT NULL", 1, 2, 3);
            assertIds("addrs CONTAINS ELEMENT (country IS NOT NULL AND city = 'Milan') ALLOW FILTERING");
            assertIds("addrs CONTAINS ELEMENT (country IS NOT NULL)", 2);
        });
    }

    /**
     * IS NOT NULL on a field inside collections: some element has the field set (DSE
     * {@code {!tuple}udtcol_list.street:*}); in a CONTAINS ELEMENT group, the element; NOT CONTAINS ELEMENT, no
     * element (including the rows without elements).
     */
    @Test
    public void testCollections() throws Throwable
    {
        createTables("CREATE TABLE %s (id int PRIMARY KEY, addrs " + list("frozen<" + address + '>') + ')');
        index("addrs.city");
        index("addrs.street");
        index("addrs.loc.lat");
        index("addrs.zip");

        write("INSERT INTO %s (id, addrs) VALUES (1, [{street: 'a', city: 'Rome', zip: 200, loc: {lat: 1.0}}, {street: 'b', city: 'Milan', zip: 100}])");
        write("INSERT INTO %s (id, addrs) VALUES (2, [{city: 'Rome', zip: 100, loc: {lat: 5.0}}])");
        write("INSERT INTO %s (id, addrs) VALUES (3, [])");
        write("INSERT INTO %s (id) VALUES (4)");
        write("INSERT INTO %s (id, addrs) VALUES (5, [{street: '', zip: 300}, {zip: null}])");

        inAllStates(() -> {
            assertIds("addrs.city IS NOT NULL", 1, 2);
            assertIds("addrs.street IS NOT NULL", 1, 5);                       // {!tuple}udtcol_list.street:*
            assertIds("addrs.loc.lat IS NOT NULL", 1, 2);
            assertIds("addrs.loc IS NOT NULL ALLOW FILTERING", 1, 2);
            // independent elements
            assertIds("addrs.street IS NOT NULL AND addrs.loc.lat IS NOT NULL", 1);
            assertIds("addrs.city IS NOT NULL AND addrs.city = 'Milan'", 1);
            // the same element
            assertIds("addrs CONTAINS ELEMENT (loc.lat IS NOT NULL AND city = 'Rome')", 1, 2);
            assertIds("addrs CONTAINS ELEMENT (loc.lat IS NOT NULL AND zip = 100)", 2);
            assertIds("addrs CONTAINS ELEMENT (street IS NOT NULL AND loc.lat IS NOT NULL)", 1);
            assertIds("addrs CONTAINS ELEMENT (street IS NOT NULL AND city IS NOT NULL AND zip = 100)", 1);
            assertIds("addrs CONTAINS ELEMENT (street IS NOT NULL)", 1, 5);
            assertIds("addrs CONTAINS ELEMENT (loc IS NOT NULL OR zip = 300) ALLOW FILTERING", 1, 2, 5);
            assertIds("addrs CONTAINS ELEMENT (loc.lat IS NOT NULL OR zip = 300)", 1, 2, 5);
            // no element
            assertIds("addrs NOT CONTAINS ELEMENT (city IS NOT NULL) ALLOW FILTERING", 3, 4, 5);
            assertIds("addrs NOT CONTAINS ELEMENT (street IS NOT NULL AND city IS NOT NULL) ALLOW FILTERING", 2, 3, 4, 5);
            assertIds("addrs.zip = 300 AND addrs NOT CONTAINS ELEMENT (city IS NOT NULL) ALLOW FILTERING", 5);
        });

        assertPaging("addrs.street IS NOT NULL OR addrs.city IS NOT NULL");
        assertPaging("addrs CONTAINS ELEMENT (loc.lat IS NOT NULL AND city = 'Rome')");

        // an element added to, or removed from, a non-frozen list
        if (!frozen)
        {
            write("UPDATE %s SET addrs = addrs + [{city: 'Turin'}] WHERE id = 5");
            write("DELETE addrs[1] FROM %s WHERE id = 1");
            inAllStates(() -> {
                assertIds("addrs.city IS NOT NULL", 1, 2, 5);
                assertIds("addrs CONTAINS ELEMENT (street IS NOT NULL AND city IS NOT NULL)", 1);
                assertIds("addrs NOT CONTAINS ELEMENT (city IS NOT NULL) ALLOW FILTERING", 3, 4);
            });
        }
    }

    /**
     * A normalizing index (case_sensitive: false) serves IS NOT NULL: it indexes every value, the empty strings
     * included.
     */
    @Test
    public void testAnalyzedFields() throws Throwable
    {
        createTables("CREATE TABLE %s (id int PRIMARY KEY, addr " + udt(address) + ", addrs " + list("frozen<" + address + '>') + ')');
        index("addr.city", " WITH OPTIONS = {'case_sensitive': 'false'}");
        index("addrs.city", " WITH OPTIONS = {'case_sensitive': 'false', 'normalize': 'true'}");
        write("INSERT INTO %s (id, addr, addrs) VALUES (1, {city: 'Rome'}, [{city: 'ROME'}])");
        write("INSERT INTO %s (id, addr, addrs) VALUES (2, {city: ''}, [{city: ''}, {zip: 1}])");
        write("INSERT INTO %s (id, addr, addrs) VALUES (3, {zip: 1}, [{zip: 1}])");

        inAllStates(() -> {
            assertIds("addr.city IS NOT NULL", 1, 2);
            assertIds("addrs.city IS NOT NULL", 1, 2);
            assertIndexedIds("addr.city IS NOT NULL AND addr.city = 'rome'", 1);
            assertIndexedIds("addrs CONTAINS ELEMENT (city IS NOT NULL AND city = 'rome')", 1);
            assertIds("addrs CONTAINS ELEMENT (city IS NOT NULL AND zip = 1) ALLOW FILTERING");
        });
    }

    /**
     * A value whose term exceeds the maximum term size of the index (8 KiB by default) isn't indexed: the write only
     * logs a warning (unless the coordinator validates the term sizes, which it doesn't by default), so the
     * IS NOT NULL served by the index doesn't return the row, as the other operators the index serves don't, while
     * the filtering without index does. A documented limitation, shared with the indexes on columns.
     */
    @Test
    public void testOversizedValue() throws Throwable
    {
        try (WithProperties properties = new WithProperties().set(SAI_VALIDATE_MAX_TERM_SIZE_AT_COORDINATOR, false))
        {
            createTables("CREATE TABLE %s (id int PRIMARY KEY, addr " + udt(address) + ')');
            index("addr.city");
            String big = "x".repeat(Math.max(IndexContext.MAX_STRING_TERM_SIZE, IndexContext.MAX_FROZEN_TERM_SIZE) + 1);
            write("INSERT INTO %s (id, addr) VALUES (1, {city: 'Rome'})");
            write("INSERT INTO %s (id, addr) VALUES (2, {city: ?})", big);

            inAllStates(() -> {
                assertIndexedIds("addr.city IS NOT NULL", 1);
                assertFilteredIds("addr.city IS NOT NULL", new Object[0], 1, 2);
                assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE addr.city = ?", big));
                assertFilteredIds("addr.city = ?", new Object[]{ big }, 2);
            });
        }
    }

    /**
     * The key and static columns.
     */
    @Test
    public void testKeyAndStaticColumns() throws Throwable
    {
        createTables("CREATE TABLE %s (p frozen<" + geo + ">, q int, c frozen<tuple<text, int>>, s " + udt(geo) + " static, id int, PRIMARY KEY ((p, q), c))");
        index("p.lon");
        index("c.field1");
        index("s.lat");
        write("INSERT INTO %s (p, q, c, s, id) VALUES ({lat: 1.0, lon: 2.0}, 0, ('a', 1), {lat: 3.0}, 1)");
        write("INSERT INTO %s (p, q, c, id) VALUES ({lat: 1.0, lon: 2.0}, 0, (null, 2), 2)");
        write("INSERT INTO %s (p, q, c, s, id) VALUES ({lat: 5.0}, 0, ('b', 3), {lon: 4.0}, 3)");

        inAllStates(() -> {
            assertIds("p.lon IS NOT NULL", 1, 2);
            assertIds("c.field1 IS NOT NULL", 1, 3);
            assertIds("s.lat IS NOT NULL", 1, 2);
            assertIds("c.field2 IS NOT NULL ALLOW FILTERING", 1, 2, 3);
            assertIds("p.lon IS NOT NULL AND c.field1 IS NOT NULL", 1);
        });
    }

    @Test
    public void testPreparedStatements() throws Throwable
    {
        createTables("CREATE TABLE %s (id int PRIMARY KEY, addr " + udt(address) + ')');
        index("addr.city");
        index("addr.zip");
        write("INSERT INTO %s (id, addr) VALUES (1, {city: 'Rome', zip: 1})");
        write("INSERT INTO %s (id, addr) VALUES (2, {zip: 2})");

        ResultMessage.Prepared prepared = prepare("SELECT id FROM %s WHERE addr.city IS NOT NULL AND addr.zip = ?");
        List<ColumnSpecification> names = prepared.metadata.names;
        assertThat(names).hasSize(1);
        assertThat(names.get(0).name.toString()).isEqualTo("addr.zip");
        assertThat(names.get(0).type).isEqualTo(Int32Type.instance);
        inAllStates(() -> {
            assertIdsBound("addr.city IS NOT NULL AND addr.zip = ?", new Object[]{ 1 }, 1);
            assertIdsBound("addr.city IS NOT NULL AND addr.zip = ?", new Object[]{ 2 });
        });
    }

    /**
     * The row filter and the CQL form of IS NOT NULL on a field.
     */
    @Test
    public void testRowFilter() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, addr " + udt(address) + ", addrs " + list("frozen<" + address + '>') + ')');
        index("addr.city");
        RowFilter filter = rowFilter("addr.city IS NOT NULL AND addrs CONTAINS ELEMENT (loc.lat IS NOT NULL AND city = 'a') ALLOW FILTERING");
        // the order of the expressions depends on the kinds of the columns
        assertThat(filter.toCQLString(org.apache.cassandra.db.marshal.Redaction.NONE))
            .contains("addrs CONTAINS ELEMENT (loc.lat IS NOT NULL AND city = 'a')")
            .contains("addr.city IS NOT NULL");
        RowFilter.Expression isNotNull = filter.expressions().stream().filter(e -> e.column().name.toString().equals("addr")).findFirst().get();
        assertThat(isNotNull).isInstanceOf(RowFilter.FieldExpression.class);
        assertThat(isNotNull.operator()).isEqualTo(org.apache.cassandra.cql3.Operator.IS_NOT);
        assertThat(isNotNull.analyzer()).isNull();
    }

    private RowFilter rowFilter(String where)
    {
        String query = "SELECT id FROM " + KEYSPACE + '.' + currentTable() + " WHERE " + where;
        ClientState state = ClientState.forInternalCalls();
        SelectStatement statement = (SelectStatement) QueryProcessor.parseStatement(query, state);
        return statement.getRowFilter(QueryOptions.DEFAULT, state);
    }

    @Test
    public void testInvalidQueries() throws Throwable
    {
        createTables("CREATE TABLE %s (id int PRIMARY KEY, v int, addr " + udt(address) + ", addrs " + list("frozen<" + address + '>') + ')');
        index("addr.tags");
        index("addrs.city");
        write("INSERT INTO %s (id, addr) VALUES (1, {tags: {}})");
        write("INSERT INTO %s (id, addr) VALUES (2, {tags: {'x'}})");
        write("INSERT INTO %s (id, addr) VALUES (3, {city: 'Rome'})");

        // IS NULL isn't supported
        assertInvalidSyntaxMessage("IS NULL restrictions are not supported", "SELECT id FROM %s WHERE addr.city IS NULL ALLOW FILTERING");
        assertInvalidSyntaxMessage("IS NULL restrictions are not supported", "SELECT id FROM %s WHERE v IS NULL ALLOW FILTERING");
        // on whole columns, IS NOT NULL is only a materialized view restriction, as before
        assertInvalidMessage("Unsupported restriction: v IS NOT NULL", "SELECT id FROM %s WHERE v IS NOT NULL ALLOW FILTERING");
        assertInvalidMessage("Unsupported restriction: addr IS NOT NULL", "SELECT id FROM %s WHERE addr IS NOT NULL ALLOW FILTERING");
        // an index on the values of a leaf collection doesn't serve it (an empty collection is set, without terms)
        assertInvalidMessage(String.format(StatementRestrictions.HAS_UNSUPPORTED_INDEX_RESTRICTION_MESSAGE_SINGLE, "addr.tags"),
                             "SELECT id FROM %s WHERE addr.tags IS NOT NULL");
        assertIds("addr.tags IS NOT NULL ALLOW FILTERING", 1, 2);
        // unindexed fields need ALLOW FILTERING
        assertInvalidMessage(String.format(StatementRestrictions.FIELD_REQUIRES_ALLOW_FILTERING_MESSAGE, "addr.zip"),
                             "SELECT id FROM %s WHERE addr.zip IS NOT NULL");
        // a NOT CONTAINS ELEMENT is never served by the candidates
        assertInvalidMessage(String.format(StatementRestrictions.NOT_CONTAINS_ELEMENT_REQUIRES_ALLOW_FILTERING_MESSAGE, "addrs NOT CONTAINS ELEMENT (city IS NOT NULL)"),
                             "SELECT id FROM %s WHERE addrs NOT CONTAINS ELEMENT (city IS NOT NULL)");
        assertInvalidMessage("Undefined field nope", "SELECT id FROM %s WHERE addr.nope IS NOT NULL ALLOW FILTERING");
        // only in SELECT
        assertInvalidMessage(String.format(StatementRestrictions.FIELD_RESTRICTIONS_ONLY_IN_SELECT_MESSAGE, "addr.city IS NOT NULL"),
                             "DELETE FROM %s WHERE id = 1 AND addr.city IS NOT NULL");
        assertInvalidMessage(String.format(StatementRestrictions.FIELD_RESTRICTIONS_NOT_IN_VIEWS_MESSAGE, "addr.city IS NOT NULL"),
                             "CREATE MATERIALIZED VIEW " + KEYSPACE + ".mv AS SELECT * FROM %s WHERE id IS NOT NULL AND addr.city IS NOT NULL PRIMARY KEY (id)");
    }

    /**
     * Checks a query with the page sizes 1 and 2 and with a LIMIT: the same rows as without paging.
     */
    private void assertPaging(String where)
    {
        List<Integer> expected = ids(execute("SELECT id FROM %s WHERE " + where));
        assertThat(expected).as(where).isNotEmpty();
        for (int pageSize : new int[]{ 1, 2 })
        {
            List<Integer> paged = executeNetWithPaging("SELECT id FROM %s WHERE " + where, pageSize).all()
                                                                                                 .stream()
                                                                                                 .map(row -> row.getInt("id"))
                                                                                                 .collect(Collectors.toList());
            assertThat(paged).as(where + " with page size " + pageSize).containsExactlyInAnyOrderElementsOf(expected);
            List<Integer> limited = executeNetWithPaging("SELECT id FROM %s WHERE " + where + " LIMIT 1", pageSize).all()
                                                                                                               .stream()
                                                                                                               .map(row -> row.getInt("id"))
                                                                                                               .collect(Collectors.toList());
            assertThat(limited).as(where + " with LIMIT and page size " + pageSize).hasSize(1);
            assertThat(expected).containsAll(limited);
        }
    }
}
