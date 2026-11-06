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
import org.apache.cassandra.cql3.ElementMatchRelation;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.restrictions.ElementMatchRestriction;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.RequestFailureException;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.StorageAttachedIndexGroup;
import org.apache.cassandra.index.sai.analyzer.AnalyzerEqOperatorSupport;
import org.apache.cassandra.index.sai.disk.v1.V1OnDiskFormat;
import org.apache.cassandra.index.sai.metrics.TableQueryMetrics;
import org.apache.cassandra.index.sai.plan.QueryController;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.FBUtilities;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The {@code CONTAINS ELEMENT} groups (HCD-653, phase P4): {@code addrs CONTAINS ELEMENT (city = 'Rome' AND zip = 100)}
 * selects the rows where a single element of a collection of UDTs or tuples satisfies all the conditions of the group,
 * and {@code addrs NOT CONTAINS ELEMENT (...)} the rows where no element does, including the rows without elements.
 * Outside a group, the AND-ed relations on the fields of the elements are independent (P3).
 * <p>
 * The indexes serve a group through its positive conditions, each on "some element", a superset of the rows
 * satisfying it (the candidates), then the candidate rows are filtered element by element. Every query on the indexed
 * table is also run with {@code ALLOW FILTERING} on a twin table without any index, which must give the same rows.
 * The tests are run on frozen and on non-frozen collections, with the data in the memtables, after a flush and after
 * a compaction. The DSE Search reference results ({@code docs/sai-udt/dse-runtime.md}, {@code solr_query} with the
 * tuple parser) are given next to the CQL queries.
 */
@RunWith(Parameterized.class)
public class UdtElementMatchQueryTest extends SAITester
{
    @Parameterized.Parameter
    public boolean frozen;

    @Parameterized.Parameters(name = "frozen={0}")
    public static Collection<Object[]> data()
    {
        return Arrays.asList(new Object[]{ true }, new Object[]{ false });
    }

    /** The kinds of collections of UDTs and tuples. */
    private enum Kind
    {
        LIST, SET, MAP
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

    /**
     * @return the CQL type of a collection of the given kind with the given (frozen) elements, frozen or not
     * depending on the test parameter
     */
    private String collection(Kind kind, String element)
    {
        String type;
        switch (kind)
        {
            case LIST:
                type = "list<" + element + '>';
                break;
            case SET:
                type = "set<" + element + '>';
                break;
            default:
                type = "map<text, " + element + '>';
                break;
        }
        return frozen ? "frozen<" + type + '>' : type;
    }

    /**
     * @return the CQL literal of a collection of the given kind with the given elements (the keys of a map are
     * {@code k0}, {@code k1}, ...)
     */
    private static String literal(Kind kind, String... elements)
    {
        StringBuilder builder = new StringBuilder(kind == Kind.LIST ? "[" : "{");
        for (int i = 0; i < elements.length; i++)
        {
            if (i > 0)
                builder.append(", ");
            if (kind == Kind.MAP)
                builder.append("'k").append(i).append("': ");
            builder.append(elements[i]);
        }
        return builder.append(kind == Kind.LIST ? "]" : "}").toString();
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
     * Creates an index on the current table, named after the table and the target.
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
     * Checks that a query on the indexed table returns the rows with the given ids, without ALLOW FILTERING if it
     * isn't in the query (it's served by the indexes), and that the same query with ALLOW FILTERING on the table
     * without indexes returns the same rows.
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
        assertRowsIgnoringOrder(execute(filtered("SELECT id FROM " + KEYSPACE + '.' + plain + " WHERE " + where), values), rows(ids));
    }

    /**
     * @return the query with ALLOW FILTERING
     */
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
     * Checks a query with the page sizes 1 and 2 and with a LIMIT: the same rows as without paging, on the indexed
     * table and on the twin table with ALLOW FILTERING.
     */
    private void assertPaging(String where)
    {
        List<Integer> expected = ids(execute("SELECT id FROM %s WHERE " + where));
        assertThat(expected).as(where).isNotEmpty();
        assertThat(ids(execute(filtered("SELECT id FROM " + KEYSPACE + '.' + plain + " WHERE " + where)))).as(where)
                                                                                                         .containsExactlyInAnyOrderElementsOf(expected);
        boolean allowFiltering = where.endsWith("ALLOW FILTERING");
        String restrictions = allowFiltering ? where.substring(0, where.length() - "ALLOW FILTERING".length()) : where;
        for (int pageSize : new int[]{ 1, 2 })
        {
            List<Integer> paged = netIds("SELECT id FROM %s WHERE " + where, pageSize);
            assertThat(paged).as(where + " with page size " + pageSize).containsExactlyInAnyOrderElementsOf(expected);
            List<Integer> limited = netIds("SELECT id FROM %s WHERE " + restrictions + " LIMIT 1" + (allowFiltering ? " ALLOW FILTERING" : ""), pageSize);
            assertThat(limited).as(where + " with LIMIT and page size " + pageSize).hasSize(1);
            assertThat(expected).containsAll(limited);
        }
    }

    private List<Integer> netIds(String query, int pageSize)
    {
        return executeNetWithPaging(query, pageSize).all().stream().map(row -> row.getInt("id")).collect(Collectors.toList());
    }

    private static final String ROME = "{street: 'a', city: 'Rome', zip: 200, loc: {lat: 1.0, lon: 0.5}, tags: {'x', 'y'}}";
    private static final String MILAN = "{street: 'b', city: 'Milan', zip: 100, loc: {lat: 3.0}, tags: {'z'}}";
    private static final String ROME2 = "{street: 'bc', city: 'Rome', zip: 100, loc: {lat: 5.0}, tags: {'y'}}";
    private static final String NO_CITY = "{street: 'd', zip: 300}";

    /**
     * Inserts the u_list rows of the DSE Search reference run: 1 = [{Rome, 200, lat 1}, {Milan, 100, lat 3}],
     * 2 = [{Rome, 100, lat 5}], plus an empty collection (3), no collection (4) and an element without city (5).
     */
    private void insertAddresses(Kind kind)
    {
        write("INSERT INTO %s (id, addrs) VALUES (1, " + literal(kind, ROME, MILAN) + ')');
        write("INSERT INTO %s (id, addrs) VALUES (2, " + literal(kind, ROME2) + ')');
        write("INSERT INTO %s (id, addrs) VALUES (3, " + literal(kind) + ')');
        write("INSERT INTO %s (id) VALUES (4)");
        write("INSERT INTO %s (id, addrs) VALUES (5, " + literal(kind, NO_CITY) + ')');
    }

    @Test
    public void testListOfUDTs() throws Throwable
    {
        testCollectionOfUDTs(Kind.LIST);
    }

    @Test
    public void testSetOfUDTs() throws Throwable
    {
        testCollectionOfUDTs(Kind.SET);
    }

    @Test
    public void testMapOfUDTs() throws Throwable
    {
        testCollectionOfUDTs(Kind.MAP);
    }

    /**
     * The u_list queries of the DSE Search reference run, where the AND inside one tuple query is on the same element,
     * on a collection of the given kind.
     */
    private void testCollectionOfUDTs(Kind kind) throws Throwable
    {
        createTables("CREATE TABLE %s (id int PRIMARY KEY, addrs " + collection(kind, "frozen<" + address + '>') + ')');
        index("addrs.city");
        index("values(addrs).zip");
        index("addrs.loc.lat");
        index("addrs.tags");
        index("addrs.street");
        insertAddresses(kind);

        inAllStates(() -> {
            // the same element
            assertIds("addrs CONTAINS ELEMENT (city = 'Rome' AND zip = 100)", 2);           // {!tuple}addrs.city:Rome AND addrs.zip:100
            assertIds("addrs CONTAINS ELEMENT (city = 'Rome' AND zip = 200)", 1);
            assertIds("addrs CONTAINS ELEMENT (city = 'Milan' AND zip = 200)");
            assertIds("addrs CONTAINS ELEMENT (city = 'Rome')", 1, 2);                      // {!tuple}addrs.city:Rome
            assertIds("addrs CONTAINS ELEMENT (zip > 150 AND zip < 150)");
            assertIds("addrs CONTAINS ELEMENT (zip >= 150 AND zip < 250)", 1);
            assertIds("addrs CONTAINS ELEMENT (tags CONTAINS 'y' AND zip = 100)", 2);
            assertIds("addrs CONTAINS ELEMENT (tags CONTAINS 'z' AND city = 'Rome')");
            assertIds("addrs CONTAINS ELEMENT (city IN ('Milan', 'Turin') AND zip = 100)", 1);
            assertIds("addrs CONTAINS ELEMENT (\"city\" = 'Rome' AND zip = 100)", 2);
            assertIndexedIds("addrs CONTAINS ELEMENT (city = 'Rome' AND street LIKE 'b%%')", 2);
            assertIndexedIds("addrs CONTAINS ELEMENT (street LIKE 'b%%' AND zip = 200)");
            // cross-level: DSE {!tuple}addrs.zip:200 AND addrs.loc.lat:1.0 matches nothing, but row 1 has the element
            assertIds("addrs CONTAINS ELEMENT (zip = 200 AND loc.lat = 1.0)", 1);
            assertIds("addrs CONTAINS ELEMENT (zip = 100 AND loc.lat > 4)", 2);
            assertIds("addrs CONTAINS ELEMENT (city = 'Milan' AND loc.lat > 4)");

            // OR and negations inside the element
            assertIds("addrs CONTAINS ELEMENT (city = 'Rome' OR city = 'Milan')", 1, 2);    // {!tuple}addrs.city:Rome OR addrs.city:Milan
            assertIds("addrs CONTAINS ELEMENT (city = 'Milan' OR zip = 300)", 1, 5);
            assertIds("addrs CONTAINS ELEMENT (city = 'Rome' AND (zip >= 150 OR loc.lat > 4))", 1, 2);
            assertIds("addrs CONTAINS ELEMENT (city = 'Rome' AND (zip >= 150 OR loc.lat < 2) AND zip != 100)", 1);
            assertIds("addrs CONTAINS ELEMENT ((city = 'Rome' AND zip = 100) OR (city = 'Milan' AND loc.lat < 4))", 1, 2);
            assertIds("addrs CONTAINS ELEMENT (city = 'Rome' AND zip != 100)", 1);           // {!tuple}addrs.city:Rome AND NOT addrs.zip:100
            assertIds("addrs CONTAINS ELEMENT (city = 'Rome' AND zip NOT IN (100, 300))", 1);
            assertIds("addrs CONTAINS ELEMENT (zip = 100 AND city NOT IN ('Milan'))", 2);
            assertIds("addrs CONTAINS ELEMENT (zip = 100 AND tags NOT CONTAINS 'y')", 1);
            // the unset fields don't satisfy !=, as outside groups on single fields
            assertIds("addrs CONTAINS ELEMENT (zip = 300 AND city != 'Rome')");

            // the groups are independent from each other and from the relations outside them
            assertIds("addrs CONTAINS ELEMENT (city = 'Rome' AND zip = 200) AND addrs CONTAINS ELEMENT (city = 'Milan' AND zip = 100)", 1);
            assertIds("addrs CONTAINS ELEMENT (city = 'Rome' AND zip = 100) AND addrs CONTAINS ELEMENT (city = 'Rome' AND zip = 200)");
            assertIds("addrs CONTAINS ELEMENT (city = 'Rome' AND zip = 200) AND addrs.zip = 100", 1);
            assertIds("addrs CONTAINS ELEMENT (city = 'Rome' AND zip = 100) AND addrs.city = 'Rome'", 2);
            assertIds("addrs.city = 'Rome' AND addrs.zip = 100", 1, 2);                     // {"q": "{!tuple}addrs.city:Rome", "fq": "{!tuple}addrs.zip:100"}
            // in disjunctions, with other restrictions
            assertIds("addrs CONTAINS ELEMENT (city = 'Rome' AND zip = 100) OR addrs.zip = 300", 2, 5);
            assertIds("addrs CONTAINS ELEMENT (city = 'Rome' AND zip = 100) OR addrs CONTAINS ELEMENT (city = 'Milan' AND zip = 100)", 1, 2);
            assertIds("(addrs CONTAINS ELEMENT (city = 'Rome' AND zip = 200) OR addrs.zip = 300) AND addrs.tags CONTAINS 'x'", 1);
            assertIds("id = 1 AND addrs CONTAINS ELEMENT (city = 'Rome' AND zip = 200)", 1);  // id:1 AND {!tuple}addrs.city:Rome
            assertIds("id = 2 AND addrs CONTAINS ELEMENT (city = 'Rome' AND zip = 200)");

            // no element: the rows without elements are included (-{!tuple}(...))
            assertIds("addrs NOT CONTAINS ELEMENT (city = 'Rome' AND zip = 100) ALLOW FILTERING", 1, 3, 4, 5);
            assertIds("addrs NOT CONTAINS ELEMENT (city = 'Rome' AND zip = 200) ALLOW FILTERING", 2, 3, 4, 5);
            assertIds("addrs NOT CONTAINS ELEMENT (city = 'Rome' OR city = 'Milan') ALLOW FILTERING", 3, 4, 5);
            assertIds("addrs NOT CONTAINS ELEMENT (zip >= 100) ALLOW FILTERING", 3, 4);
            assertIds("addrs NOT CONTAINS ELEMENT (city = 'Rome' AND zip != 100) ALLOW FILTERING", 2, 3, 4, 5);
            // with a single equality, it's the != on the field, served by the index ("no element equals")
            assertIds("addrs NOT CONTAINS ELEMENT (city = 'Rome')", 3, 4, 5);
            assertIds("addrs NOT CONTAINS ELEMENT (zip = 100)", 3, 4, 5);
            assertIds("addrs NOT CONTAINS ELEMENT (city = 'Turin')", 1, 2, 3, 4, 5);
            // with other restrictions served by the indexes
            assertIds("addrs.zip = 300 AND addrs NOT CONTAINS ELEMENT (city = 'Rome' AND zip = 100) ALLOW FILTERING", 5);
            assertIds("addrs.city = 'Rome' AND addrs NOT CONTAINS ELEMENT (city = 'Rome' AND zip = 100) ALLOW FILTERING", 1);
            assertIds("addrs CONTAINS ELEMENT (city = 'Rome') AND addrs NOT CONTAINS ELEMENT (city = 'Milan')", 2);
            assertIds("addrs NOT CONTAINS ELEMENT (city = 'Rome' AND zip = 100) OR addrs.zip = 100 ALLOW FILTERING", 1, 2, 3, 4, 5);
        });

        // the paging and LIMIT
        for (String where : new String[]{ "addrs CONTAINS ELEMENT (city = 'Rome' AND zip = 100)",
                                          "addrs CONTAINS ELEMENT (city = 'Rome' OR zip = 300)",
                                          "addrs CONTAINS ELEMENT (city = 'Rome' AND zip != 100) OR addrs.zip = 300",
                                          "addrs NOT CONTAINS ELEMENT (city = 'Rome')",
                                          "addrs NOT CONTAINS ELEMENT (city = 'Rome' AND zip = 100) ALLOW FILTERING" })
            assertPaging(where);
    }

    /**
     * The t_list rows of the DSE Search reference run: lists of tuples.
     */
    @Test
    public void testListOfTuples() throws Throwable
    {
        createTables("CREATE TABLE %s (id int PRIMARY KEY, ts " + collection(Kind.LIST, "frozen<tuple<text, int>>") + ')');
        index("ts.field1");
        index("ts.field2");
        // DSE t_list 1 = [(foo,1),(bar,2)]
        write("INSERT INTO %s (id, ts) VALUES (1, [('foo', 1), ('bar', 2)])");
        write("INSERT INTO %s (id, ts) VALUES (2, [('foo', 3)])");
        write("INSERT INTO %s (id, ts) VALUES (3, [(null, 2), ('baz', null)])");

        inAllStates(() -> {
            assertIds("ts CONTAINS ELEMENT (field1 = 'foo' AND field2 = 2)");               // {!tuple}ts.field1:foo AND ts.field2:2
            assertIds("ts CONTAINS ELEMENT (field1 = 'foo' AND field2 = 1)", 1);            // {!tuple}ts.field1:foo AND ts.field2:1
            assertIds("ts CONTAINS ELEMENT (field1 = 'baz' AND field2 = 2)");
            assertIds("ts CONTAINS ELEMENT (field1 = 'foo' AND field2 > 1)", 2);
            assertIds("ts CONTAINS ELEMENT (field2 = 2)", 1, 3);
            assertIds("ts CONTAINS ELEMENT (field2 = 2 AND field1 != 'bar')");
            assertIds("ts NOT CONTAINS ELEMENT (field1 = 'foo' AND field2 = 1) ALLOW FILTERING", 2, 3);
            assertIds("ts NOT CONTAINS ELEMENT (field1 = 'foo')", 3);
        });
    }

    /**
     * The groups on collections inside UDTs and tuples ({@code direc.names}), inside the elements of other
     * collections ({@code dirs.names}), and the conditions on the collections inside the elements.
     */
    @Test
    public void testNestedCollections() throws Throwable
    {
        String name = createType("CREATE TYPE %s (firstname text, lastname text, hobbies frozen<set<text>>)");
        String direction = createType("CREATE TYPE %s (city text, names frozen<list<frozen<" + name + ">>>)");
        // the parameter applies to the UDT column and to the outer collection, the collections inside UDTs and
        // tuples are frozen
        createTables("CREATE TABLE %s (id int PRIMARY KEY, direc " + (frozen ? "frozen<" + direction + '>' : direction) + ", " +
                     "tt tuple<int, text, frozen<list<frozen<tuple<int, text>>>>>, " +
                     "dirs " + collection(Kind.LIST, "frozen<" + direction + '>') + ')');
        index("direc.names.firstname");
        index("direc.names.hobbies");
        index("tt.field3.field1");
        index("dirs.names.firstname");
        index("dirs.city");

        String bilbo = "{firstname: 'Bilbo', lastname: 'Baggins', hobbies: {'swim1', 'eat'}}";
        String sam = "{firstname: 'Sam', lastname: 'Gamgee', hobbies: {'cook'}}";
        String frodo = "{firstname: 'Frodo', lastname: 'Baggins'}";
        write("INSERT INTO %s (id, direc, tt, dirs) VALUES (1, {city: 'bcn', names: [" + bilbo + ", " + sam + "]}, " +
              "(1, 'a', [(9, 'x'), (6, 'y')]), [{city: 'bcn', names: [" + bilbo + "]}, {city: 'mad', names: [" + sam + "]}])");
        write("INSERT INTO %s (id, direc, tt, dirs) VALUES (2, {city: 'mad', names: [" + frodo + "]}, (2, 'b', [(6, 'z')]), [{city: 'mad', names: [" + frodo + ", " + sam + "]}])");
        write("INSERT INTO %s (id, direc, tt, dirs) VALUES (3, {city: 'bcn', names: []}, (3, 'c', []), [{city: 'bcn'}])");

        inAllStates(() -> {
            // a collection inside a UDT (DSE direc.names.firstname:Bilbo AND direc.names.lastname:Baggins)
            assertIds("direc.names CONTAINS ELEMENT (firstname = 'Bilbo' AND lastname = 'Baggins')", 1);
            assertIds("direc.names CONTAINS ELEMENT (firstname = 'Sam' AND lastname = 'Baggins')");
            assertIds("direc.names CONTAINS ELEMENT (firstname = 'Bilbo' AND hobbies CONTAINS 'eat')", 1);
            assertIds("direc.names CONTAINS ELEMENT (firstname = 'Sam' AND hobbies CONTAINS 'eat')");
            assertIds("direc.names CONTAINS ELEMENT (firstname IN ('Sam', 'Frodo') AND lastname = 'Baggins')", 2);
            assertIds("direc.names NOT CONTAINS ELEMENT (lastname = 'Baggins') ALLOW FILTERING", 3);
            assertIds("direc.names NOT CONTAINS ELEMENT (firstname = 'Bilbo')", 2, 3);
            // a collection inside a tuple
            assertIds("tt.field3 CONTAINS ELEMENT (field1 = 9 AND field2 = 'x')", 1);
            assertIds("tt.field3 CONTAINS ELEMENT (field1 = 6 AND field2 = 'x')");
            assertIds("tt.field3 CONTAINS ELEMENT (field1 = 6 AND field2 = 'z')", 2);
            // the conditions on a collection inside the elements (dirs.names inside dirs)
            assertIds("dirs CONTAINS ELEMENT (city = 'mad' AND names.firstname = 'Sam')", 1, 2);
            assertIds("dirs CONTAINS ELEMENT (city = 'bcn' AND names.firstname = 'Sam')");
            // no name is Sam: also the element without names
            assertIds("dirs CONTAINS ELEMENT (city = 'bcn' AND names.firstname != 'Sam')", 1, 3);
            assertIds("dirs CONTAINS ELEMENT (city = 'mad' AND names.lastname = 'Baggins')", 2);
            // the elements of a collection inside the elements of another one
            assertIds("dirs.names CONTAINS ELEMENT (firstname = 'Frodo' AND lastname = 'Baggins')", 2);
            assertIds("dirs.names CONTAINS ELEMENT (firstname = 'Sam' AND lastname = 'Baggins')");
            assertIds("dirs.names CONTAINS ELEMENT (firstname = 'Bilbo' AND hobbies CONTAINS 'swim1')", 1);
        });
    }

    /**
     * A group whose conditions don't restrict the rows through the indexes (no positive condition served by an
     * index, or a NOT CONTAINS ELEMENT that isn't a single equality) needs ALLOW FILTERING, as an unindexed field.
     */
    @Test
    public void testGroupsWithoutIndexedConditions() throws Throwable
    {
        createTables("CREATE TABLE %s (id int PRIMARY KEY, v int, addrs " + collection(Kind.LIST, "frozen<" + address + '>') + ')');
        index("addrs.city");
        index("v");
        insertAddresses(Kind.LIST);
        write("UPDATE %s SET v = 1 WHERE id IN (1, 2, 5)");

        // each group and the message for it, which names the group; a NOT CONTAINS ELEMENT group that isn't a single
        // equality (reduced to the != on its field) is never served by the indexes, whatever the indexes on its fields
        String fields = StatementRestrictions.FIELD_REQUIRES_ALLOW_FILTERING_MESSAGE;
        String negated = StatementRestrictions.NOT_CONTAINS_ELEMENT_REQUIRES_ALLOW_FILTERING_MESSAGE;
        String[][] unserved = { { "addrs CONTAINS ELEMENT (city != 'Rome')", fields },
                                { "addrs CONTAINS ELEMENT (street = 'a' AND zip = 200)", fields },
                                { "addrs CONTAINS ELEMENT (city = 'Rome' OR street = 'd')", fields },
                                { "addrs CONTAINS ELEMENT (city = 'Rome' OR zip != 100)", fields },
                                { "addrs NOT CONTAINS ELEMENT (city = 'Rome' AND zip = 100)", negated },
                                { "addrs NOT CONTAINS ELEMENT (zip = 100)", fields } };
        for (String[] entry : unserved)
        {
            String where = entry[0];
            String message = String.format(entry[1], where);
            assertInvalidMessage(message, "SELECT id FROM %s WHERE " + where);
            // also with a restriction the indexes serve
            assertInvalidMessage(message, "SELECT id FROM %s WHERE v = 1 AND " + where);
        }
        // a filtered-only group next to another unindexed field: the message names both
        assertInvalidMessage(String.format(StatementRestrictions.NOT_CONTAINS_ELEMENT_AND_FIELDS_REQUIRE_ALLOW_FILTERING_MESSAGE,
                                           "addrs NOT CONTAINS ELEMENT (city = 'Rome' AND zip = 100)", "addrs.zip"),
                             "SELECT id FROM %s WHERE addrs.zip = 1 AND addrs NOT CONTAINS ELEMENT (city = 'Rome' AND zip = 100)");

        inAllStates(() -> {
            assertIds("addrs CONTAINS ELEMENT (city != 'Rome') ALLOW FILTERING", 1);
            assertIds("addrs CONTAINS ELEMENT (street = 'a' AND zip = 200) ALLOW FILTERING", 1);
            assertIds("addrs CONTAINS ELEMENT (city = 'Rome' OR street = 'd') ALLOW FILTERING", 1, 2, 5);
            assertIds("v = 1 AND addrs CONTAINS ELEMENT (city = 'Rome' OR zip != 100) ALLOW FILTERING", 1, 2, 5);
            assertIds("v = 1 AND addrs NOT CONTAINS ELEMENT (zip = 100) ALLOW FILTERING", 5);
            // an unindexed condition in a group served by an index doesn't need ALLOW FILTERING
            assertIds("addrs CONTAINS ELEMENT (city = 'Rome' AND street = 'bc')", 2);
            assertIds("addrs CONTAINS ELEMENT (city = 'Rome' AND (zip = 200 OR street = 'bc'))", 1, 2);
            assertIds("v = 1 AND addrs CONTAINS ELEMENT (city = 'Milan' AND loc.lat = 3.0)", 1);
        });
    }

    /**
     * The conditions of a group are analyzed by the indexes on their fields as outside groups: the equalities, the
     * {@code :} and the LIKE are normalized by a case-insensitive index, while the ranges and the exclusions, which it
     * doesn't serve, compare the raw values (also with ALLOW FILTERING on the indexed table), as without any index,
     * and so does the {@code !=} of a NOT CONTAINS ELEMENT with a single equality, except on a field with an analyzed
     * index, where the group is filtered with the analyzed {@code =} (on zip it is the raw {@code !=}).
     */
    @Test
    public void testAnalyzedConditions() throws Throwable
    {
        createTables("CREATE TABLE %s (id int PRIMARY KEY, addrs " + collection(Kind.LIST, "frozen<" + address + '>') + ')');
        index("addrs.city", " WITH OPTIONS = {'case_sensitive': 'false'}");
        index("addrs.zip");
        write("INSERT INTO %s (id, addrs) VALUES (1, [{city: 'Rome', zip: 100}, {city: 'Milan', zip: 200}])");
        write("INSERT INTO %s (id, addrs) VALUES (2, [{city: 'ROME', zip: 200}])");
        write("INSERT INTO %s (id, addrs) VALUES (3, [{city: 'Paris', zip: 100}])");

        inAllStates(() -> {
            // normalized
            assertIndexedIds("addrs CONTAINS ELEMENT (city = 'rome' AND zip = 100)", 1);
            assertIndexedIds("addrs CONTAINS ELEMENT (city = 'rome' AND zip = 200)", 2);
            assertIndexedIds("addrs CONTAINS ELEMENT (city : 'MILAN' AND zip = 200)", 1);
            assertIndexedIds("addrs CONTAINS ELEMENT (city LIKE 'ro%%' AND zip = 100)", 1);
            assertIndexedIds("addrs CONTAINS ELEMENT (city = 'rome' AND zip = 100) ALLOW FILTERING", 1);
            assertIndexedIds("addrs NOT CONTAINS ELEMENT (city = 'rome' AND zip = 100) ALLOW FILTERING", 2, 3);
            // raw values, as on the twin table
            assertIds("addrs CONTAINS ELEMENT (city >= 'p' AND city < 's' AND zip = 200)");
            assertIds("addrs CONTAINS ELEMENT (city >= 'P' AND city < 'S' AND zip = 200)", 2);
            assertIds("addrs CONTAINS ELEMENT (zip = 100 AND city != 'ROME')", 1, 3);
            assertIds("addrs CONTAINS ELEMENT (zip = 100 AND city != 'Rome')", 3);
            assertIds("addrs CONTAINS ELEMENT (zip = 200 AND city NOT IN ('rome', 'x'))", 1, 2);
            assertIds("addrs CONTAINS ELEMENT (zip = 200 AND city NOT IN ('ROME', 'x'))", 1);
            assertIds("addrs CONTAINS ELEMENT (zip = 100 AND city != 'ROME') ALLOW FILTERING", 1, 3);
            // a NOT CONTAINS ELEMENT with a single equality isn't reduced to the raw != when the index analyzes the =:
            // it's filtered (ALLOW FILTERING), as the complement of the analyzed CONTAINS ELEMENT
            assertIndexedIds("addrs CONTAINS ELEMENT (city = 'ROME')", 1, 2);
            assertInvalidMessage(String.format(StatementRestrictions.NOT_CONTAINS_ELEMENT_REQUIRES_ALLOW_FILTERING_MESSAGE,
                                               "addrs NOT CONTAINS ELEMENT (city = 'ROME')"),
                                 "SELECT id FROM %s WHERE addrs NOT CONTAINS ELEMENT (city = 'ROME')");
            assertIndexedIds("addrs NOT CONTAINS ELEMENT (city = 'ROME') ALLOW FILTERING", 3);
            assertIndexedIds("addrs NOT CONTAINS ELEMENT (city = 'rome') ALLOW FILTERING", 3);
            // intended: without index the = of the group compares the raw values, so the twin table also returns the
            // row whose element is 'Rome' (the indexed table filters with the analyzed =, as CONTAINS ELEMENT does)
            assertFilteredIds("addrs NOT CONTAINS ELEMENT (city = 'ROME')", new Object[0], 1, 3);
            // the same with a bind marker
            assertInvalidMessage(String.format(StatementRestrictions.NOT_CONTAINS_ELEMENT_REQUIRES_ALLOW_FILTERING_MESSAGE,
                                               "addrs NOT CONTAINS ELEMENT (city = ?)"),
                                 "SELECT id FROM %s WHERE addrs NOT CONTAINS ELEMENT (city = ?)", "ROME");
            assertIndexedIdsBound("addrs NOT CONTAINS ELEMENT (city = ?) ALLOW FILTERING", new Object[]{ "ROME" }, 3);
            // the zip index has no analyzer: the reduction to the != holds, served by the index
            assertIds("addrs NOT CONTAINS ELEMENT (zip = 200)", 3);
            assertIds("addrs.zip != 200", 3);
            // the raw values without index
            assertFilteredIds("addrs CONTAINS ELEMENT (city = 'rome' AND zip = 100)", new Object[0]);
        });
    }

    /**
     * With {@code equals_behaviour_when_analyzed: 'unsupported'} the analyzed index doesn't serve the {@code =}, which
     * compares the raw values: a NOT CONTAINS ELEMENT with a single equality is reduced to the {@code !=} on its field,
     * which the analyzed index doesn't serve either, so it needs ALLOW FILTERING and gives the raw results.
     */
    @Test
    public void testAnalyzedConditionsWithUnsupportedEquals() throws Throwable
    {
        createTables("CREATE TABLE %s (id int PRIMARY KEY, addrs " + collection(Kind.LIST, "frozen<" + address + '>') + ')');
        index("addrs.city", " WITH OPTIONS = {'case_sensitive': 'false', 'equals_behaviour_when_analyzed': 'unsupported'}");
        write("INSERT INTO %s (id, addrs) VALUES (1, [{city: 'Rome', zip: 100}, {city: 'Milan', zip: 200}])");
        write("INSERT INTO %s (id, addrs) VALUES (2, [{city: 'ROME', zip: 200}])");
        write("INSERT INTO %s (id, addrs) VALUES (3, [{city: 'Paris', zip: 100}])");

        // the message names the group, which is reduced to the != on addrs.city
        assertInvalidMessage(String.format(StatementRestrictions.FIELD_REQUIRES_ALLOW_FILTERING_MESSAGE, "addrs NOT CONTAINS ELEMENT (city = 'ROME')"),
                             "SELECT id FROM %s WHERE addrs NOT CONTAINS ELEMENT (city = 'ROME')");
        assertInvalidMessage(String.format(StatementRestrictions.FIELD_REQUIRES_ALLOW_FILTERING_MESSAGE, "addrs CONTAINS ELEMENT (city = 'ROME')"),
                             "SELECT id FROM %s WHERE addrs CONTAINS ELEMENT (city = 'ROME')");

        inAllStates(() -> {
            // the raw values, as on the twin table
            assertIds("addrs NOT CONTAINS ELEMENT (city = 'ROME') ALLOW FILTERING", 1, 3);
            assertIds("addrs CONTAINS ELEMENT (city = 'ROME') ALLOW FILTERING", 2);
            // ':' is served by the analyzed index
            assertIndexedIds("addrs CONTAINS ELEMENT (city : 'rome')", 1, 2);
        });
    }

    /**
     * An analyzed and a raw index on the same field: without hints the {@code =} of a group is ambiguous, as on a
     * column; the index hints select the index, and with it whether a NOT CONTAINS ELEMENT with a single equality is
     * reduced to the {@code !=} on its field (the raw index, served) or filtered (the analyzed index).
     */
    @Test
    public void testAnalyzedAndRawIndexes() throws Throwable
    {
        createTables("CREATE TABLE %s (id int PRIMARY KEY, addrs " + collection(Kind.LIST, "frozen<" + address + '>') + ')');
        String analyzed = currentTable() + "_analyzed_idx";
        String raw = currentTable() + "_raw_idx";
        createIndex("CREATE INDEX " + analyzed + " ON %s(addrs.city) USING 'sai' WITH OPTIONS = {'case_sensitive': 'false'}");
        createIndex("CREATE INDEX " + raw + " ON %s(addrs.city) USING 'sai'");
        write("INSERT INTO %s (id, addrs) VALUES (1, [{city: 'Rome', zip: 100}, {city: 'Milan', zip: 200}])");
        write("INSERT INTO %s (id, addrs) VALUES (2, [{city: 'ROME', zip: 200}])");
        write("INSERT INTO %s (id, addrs) VALUES (3, [{city: 'Paris', zip: 100}])");

        String ambiguous = String.format(AnalyzerEqOperatorSupport.EQ_AMBIGUOUS_ERROR, "addrs.city", analyzed, raw, analyzed);
        assertInvalidMessage(ambiguous, "SELECT id FROM %s WHERE addrs NOT CONTAINS ELEMENT (city = 'ROME')");
        assertInvalidMessage(ambiguous, "SELECT id FROM %s WHERE addrs CONTAINS ELEMENT (city = 'ROME')");
        assertInvalidMessage(ambiguous, "SELECT id FROM %s WHERE addrs NOT CONTAINS ELEMENT (city = 'ROME') ALLOW FILTERING");

        String withRaw = " WITH included_indexes = {" + raw + '}';
        String withAnalyzed = " WITH included_indexes = {" + analyzed + '}';
        String withoutRaw = " WITH excluded_indexes = {" + raw + '}';
        inAllStates(() -> {
            // the raw index: reduced to the != and served, the raw values
            assertIndexedIds("addrs NOT CONTAINS ELEMENT (city = 'ROME')" + withRaw, 1, 3);
            assertIndexedIds("addrs CONTAINS ELEMENT (city = 'ROME')" + withRaw, 2);
            // the analyzed index: the = is analyzed, the group isn't reduced but filtered (so the analyzed index
            // can't be an included index of the NOT CONTAINS ELEMENT, which no index serves)
            assertIndexedIds("addrs CONTAINS ELEMENT (city = 'ROME')" + withAnalyzed, 1, 2);
            assertIndexedIds("addrs CONTAINS ELEMENT (city = 'ROME')" + withoutRaw, 1, 2);
            assertIndexedIds("addrs NOT CONTAINS ELEMENT (city = 'ROME') ALLOW FILTERING" + withoutRaw, 3);
        });
        assertInvalidMessage(String.format(StatementRestrictions.NOT_CONTAINS_ELEMENT_REQUIRES_ALLOW_FILTERING_MESSAGE,
                                           "addrs NOT CONTAINS ELEMENT (city = 'ROME')"),
                             "SELECT id FROM %s WHERE addrs NOT CONTAINS ELEMENT (city = 'ROME')" + withoutRaw);
        assertInvalidMessage("It's not possible to use all the specified included indexes with this query",
                             "SELECT id FROM %s WHERE addrs NOT CONTAINS ELEMENT (city = 'ROME') ALLOW FILTERING" + withAnalyzed);
    }

    /**
     * The bind markers of the conditions get the type and the absolute name of the field of the elements.
     */
    @Test
    public void testPreparedStatements() throws Throwable
    {
        createTables("CREATE TABLE %s (id int PRIMARY KEY, addrs " + collection(Kind.LIST, "frozen<" + address + '>') + ')');
        index("addrs.city");
        index("addrs.zip");
        insertAddresses(Kind.LIST);

        ResultMessage.Prepared prepared = prepare("SELECT id FROM %s WHERE addrs CONTAINS ELEMENT (city = ? AND zip > ? AND zip IN ? AND tags CONTAINS ?)");
        List<ColumnSpecification> names = prepared.metadata.names;
        assertThat(names).hasSize(4);
        assertThat(names.get(0).name.toString()).isEqualTo("addrs.city");
        assertThat(names.get(0).type).isEqualTo(UTF8Type.instance);
        assertThat(names.get(1).name.toString()).isEqualTo("addrs.zip");
        assertThat(names.get(1).type).isEqualTo(Int32Type.instance);
        assertThat(names.get(2).type).isEqualTo(ListType.getInstance(Int32Type.instance, false));
        assertThat(names.get(3).type).isEqualTo(UTF8Type.instance);

        inAllStates(() -> {
            assertIdsBound("addrs CONTAINS ELEMENT (city = ? AND zip = ?)", new Object[]{ "Rome", 100 }, 2);
            assertIdsBound("addrs CONTAINS ELEMENT (city = ? AND zip > ?)", new Object[]{ "Rome", 150 }, 1);
            assertIdsBound("addrs CONTAINS ELEMENT (city = :c AND zip IN :z)", new Object[]{ "Milan", list(100, 300) }, 1);
            assertIdsBound("addrs CONTAINS ELEMENT (city = ? AND zip != ?)", new Object[]{ "Rome", 100 }, 1);
            assertIdsBound("addrs NOT CONTAINS ELEMENT (city = ?)", new Object[]{ "Rome" }, 3, 4, 5);
            assertIdsBound("addrs NOT CONTAINS ELEMENT (city = ? AND zip = ?) ALLOW FILTERING", new Object[]{ "Rome", 100 }, 1, 3, 4, 5);
        });

        assertInvalidMessage("Unsupported null value for field addrs.city",
                             "SELECT id FROM %s WHERE addrs CONTAINS ELEMENT (city = ? AND zip = 1)", (Object) null);
    }

    /**
     * The elements added, removed and replaced in the memtable (the index update path) and across SSTables.
     */
    @Test
    public void testElementUpdates() throws Throwable
    {
        createTables("CREATE TABLE %s (id int PRIMARY KEY, addrs " + collection(Kind.LIST, "frozen<" + address + '>') + ')');
        index("addrs.city");
        index("addrs.zip");
        write("INSERT INTO %s (id, addrs) VALUES (1, [{city: 'Rome', zip: 200}, {city: 'Milan', zip: 100}])");

        // in the memtable, then flushed: [Rome 200, Milan 100, Rome 100]
        if (frozen)
            write("UPDATE %s SET addrs = [{city: 'Rome', zip: 200}, {city: 'Milan', zip: 100}, {city: 'Rome', zip: 100}] WHERE id = 1");
        else
            write("UPDATE %s SET addrs = addrs + [{city: 'Rome', zip: 100}] WHERE id = 1");
        CheckedFunction check = () -> {
            assertIds("addrs CONTAINS ELEMENT (city = 'Rome' AND zip = 100)", 1);
            assertIds("addrs CONTAINS ELEMENT (city = 'Milan' AND zip = 200)");
        };
        check.apply();
        flushBoth();
        check.apply();

        // across SSTables: [Milan 100, Rome 100]
        if (frozen)
            write("UPDATE %s SET addrs = [{city: 'Milan', zip: 100}, {city: 'Rome', zip: 100}] WHERE id = 1");
        else
            write("DELETE addrs[0] FROM %s WHERE id = 1");
        CheckedFunction afterRemoval = () -> {
            assertIds("addrs CONTAINS ELEMENT (city = 'Rome' AND zip = 200)");
            assertIds("addrs CONTAINS ELEMENT (city = 'Rome' AND zip = 100)", 1);
            assertIds("addrs NOT CONTAINS ELEMENT (zip = 200)", 1);
            assertIds("addrs NOT CONTAINS ELEMENT (city = 'Rome' AND zip = 200) ALLOW FILTERING", 1);
        };
        afterRemoval.apply();
        flushBoth();
        afterRemoval.apply();
        compactBoth();
        afterRemoval.apply();
    }

    /**
     * The groups on the frozen collections of the primary key and on static collections.
     */
    @Test
    public void testKeyAndStaticColumns() throws Throwable
    {
        createTables("CREATE TABLE %s (p int, k frozen<list<frozen<" + geo + ">>>, c frozen<set<frozen<tuple<text, int>>>>, " +
                     "s " + collection(Kind.LIST, "frozen<" + geo + '>') + " static, id int, PRIMARY KEY ((p, k), c))");
        index("k.lat");
        index("c.field2");
        index("s.lon");

        write("INSERT INTO %s (p, k, c, s, id) VALUES (0, [{lat: 1.0, lon: 2.0}, {lat: 2.0}], {('a', 1), ('b', 2)}, [{lat: 0.0, lon: 5.0}], 1)");
        write("INSERT INTO %s (p, k, c, id) VALUES (0, [{lat: 1.0, lon: 2.0}, {lat: 2.0}], {('c', 3)}, 2)");
        write("INSERT INTO %s (p, k, c, s, id) VALUES (0, [{lat: 3.0}], {('a', 4)}, [{lat: 1.0, lon: 6.0}, {lon: 7.0}], 3)");

        inAllStates(() -> {
            assertIds("k CONTAINS ELEMENT (lat = 1.0 AND lon = 2.0)", 1, 2);
            assertIds("k CONTAINS ELEMENT (lat = 2.0 AND lon = 2.0)");
            assertIds("c CONTAINS ELEMENT (field1 = 'a' AND field2 = 4)", 3);
            assertIds("c CONTAINS ELEMENT (field1 = 'b' AND field2 = 1)");
            assertIds("c NOT CONTAINS ELEMENT (field2 = 1)", 2, 3);
            assertIds("s CONTAINS ELEMENT (lon = 6.0 AND lat = 1.0)", 3);
            assertIds("s CONTAINS ELEMENT (lon = 7.0 AND lat = 1.0)");
            assertIds("s CONTAINS ELEMENT (lon = 5.0 AND lat = 0.0) AND c CONTAINS ELEMENT (field2 = 3)", 2);
            assertIds("k CONTAINS ELEMENT (lat = 3.0) AND s NOT CONTAINS ELEMENT (lon = 6.0)");
        });

        // the restrictions on the fields of the partition key must be in the top-level AND
        assertInvalidMessage(String.format(StatementRestrictions.PARTITION_KEY_RESTRICTION_MUST_BE_TOP_LEVEL, "k CONTAINS ELEMENT (lat = 1.0)"),
                             "SELECT id FROM %s WHERE k CONTAINS ELEMENT (lat = 1.0) OR c.field2 = 1");
    }

    /**
     * The index candidates of a group are the rows where each positive condition is satisfied by some element: the
     * rows where no single element satisfies the group are read and rejected by the same-element filter, counted by
     * a table query metric.
     */
    @Test
    public void testFalsePositivesMetric() throws Throwable
    {
        createTables("CREATE TABLE %s (id int PRIMARY KEY, addrs " + collection(Kind.LIST, "frozen<" + address + '>') + ')');
        index("addrs.city");
        index("addrs.zip");
        insertAddresses(Kind.LIST);
        write("INSERT INTO %s (id, addrs) VALUES (6, [{city: 'Rome', zip: 300}, {city: 'Turin', zip: 100}])");
        flushBoth();

        StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(getCurrentColumnFamilyStore());
        TableQueryMetrics.PerTable metrics = group.queryMetrics().perTableMetrics.get(TableQueryMetrics.QueryKind.ALL);

        // candidates 1, 2 and 6 (Rome and 100 in some elements), only 2 has them in the same element
        long before = metrics.totalElementFilterRejectedRows.getCount();
        assertIds("addrs CONTAINS ELEMENT (city = 'Rome' AND zip = 100)", 2);
        assertThat(metrics.totalElementFilterRejectedRows.getCount() - before).isEqualTo(2);

        // no false positives
        before = metrics.totalElementFilterRejectedRows.getCount();
        assertIds("addrs CONTAINS ELEMENT (city = 'Rome')", 1, 2, 6);
        assertThat(metrics.totalElementFilterRejectedRows.getCount() - before).isZero();

        // the filtering without candidates (a NOT CONTAINS ELEMENT) isn't counted
        before = metrics.totalElementFilterRejectedRows.getCount();
        assertIds("addrs.city = 'Rome' AND addrs NOT CONTAINS ELEMENT (city = 'Rome' AND zip = 100) ALLOW FILTERING", 1, 6);
        assertThat(metrics.totalElementFilterRejectedRows.getCount() - before).isZero();

        // nor a group under an OR, whose candidates are not the source of the keys (here 1, 2, 5 and 6 are read)
        before = metrics.totalElementFilterRejectedRows.getCount();
        assertIds("addrs CONTAINS ELEMENT (city = 'Rome' AND zip = 100) OR addrs.zip = 300", 2, 5, 6);
        assertThat(metrics.totalElementFilterRejectedRows.getCount() - before).isZero();

        // but a group AND-ed with an OR is: the rows the element filter evaluates, here 1 and 6 if the planner
        // intersects the candidates with the OR, also 5 if it keeps only the scans of the OR (the metric counts the
        // rows evaluated and rejected by the filter, an approximation of the false positives of the candidates)
        before = metrics.totalElementFilterRejectedRows.getCount();
        assertIds("addrs CONTAINS ELEMENT (city = 'Rome' AND zip = 100) AND (addrs.zip = 100 OR addrs.zip = 300)", 2);
        assertThat(metrics.totalElementFilterRejectedRows.getCount() - before).isBetween(2L, 3L);
    }

    /**
     * The SSTable indexes read for the candidates of a group count for the guardrail on the SSTable indexes per
     * query.
     */
    @Test
    public void testSSTableIndexesGuardrail() throws Throwable
    {
        createTables("CREATE TABLE %s (id int PRIMARY KEY, addrs " + collection(Kind.LIST, "frozen<" + address + '>') + ')');
        index("addrs.city");
        index("addrs.zip");
        insertAddresses(Kind.LIST);
        flushBoth();
        write("INSERT INTO %s (id, addrs) VALUES (6, [{city: 'Rome', zip: 100}])");
        flushBoth();

        // 2 indexes on 2 SSTables
        assertThat(numSSTableIndexes("addrs CONTAINS ELEMENT (city = 'Rome' AND zip = 100)")).isEqualTo(4);
        assertThat(numSSTableIndexes("addrs CONTAINS ELEMENT (city = 'Rome' AND zip != 100)")).isEqualTo(2);
        assertThat(numSSTableIndexes("addrs.zip = 100 AND addrs NOT CONTAINS ELEMENT (city = 'Rome' AND zip = 100)")).isEqualTo(2);

        int warn = Guardrails.instance.getSaiSSTableIndexesPerQueryWarnThreshold();
        int fail = Guardrails.instance.getSaiSSTableIndexesPerQueryFailThreshold();
        try
        {
            Guardrails.instance.setSaiSSTableIndexesPerQueryThreshold(-1, 3);
            assertThatThrownBy(() -> execute("SELECT id FROM %s WHERE addrs CONTAINS ELEMENT (city = 'Rome' AND zip = 100)"))
            .isInstanceOf(RequestFailureException.class);
            assertIndexedIds("addrs CONTAINS ELEMENT (city = 'Rome' AND zip != 100)", 1);
            Guardrails.instance.setSaiSSTableIndexesPerQueryThreshold(-1, 4);
            assertIndexedIds("addrs CONTAINS ELEMENT (city = 'Rome' AND zip = 100)", 2, 6);
        }
        finally
        {
            Guardrails.instance.setSaiSSTableIndexesPerQueryThreshold(warn, fail);
        }
    }

    /**
     * @return the number of SSTable indexes referenced by the SAI filter of the query, as counted by the guardrail
     */
    private int numSSTableIndexes(String where)
    {
        String query = "SELECT id FROM " + KEYSPACE + '.' + currentTable() + " WHERE " + where + " ALLOW FILTERING";
        ClientState state = ClientState.forInternalCalls();
        SelectStatement statement = (SelectStatement) QueryProcessor.parseStatement(query, state);
        ReadCommand command = (ReadCommand) statement.getQuery(QueryOptions.DEFAULT, FBUtilities.nowInSeconds());
        QueryController controller = new QueryController(getCurrentColumnFamilyStore(), command, V1OnDiskFormat.instance.indexFeatureSet(), new QueryContext(), null);
        try
        {
            return controller.buildFilter().numSSTableIndexes();
        }
        finally
        {
            controller.abort();
        }
    }

    /**
     * A NOT IN with an empty list (literal or bound) adds no condition: a group left without conditions is refused,
     * rather than satisfied by any element on the coordinator and refused by the replicas.
     */
    @Test
    public void testEmptyGroups() throws Throwable
    {
        createTables("CREATE TABLE %s (id int PRIMARY KEY, addrs " + collection(Kind.LIST, "frozen<" + address + '>') + ')');
        index("addrs.city");
        insertAddresses(Kind.LIST);

        for (String group : new String[]{ "addrs CONTAINS ELEMENT (zip NOT IN ())", "addrs NOT CONTAINS ELEMENT (zip NOT IN ())" })
        {
            String message = String.format(ElementMatchRestriction.EMPTY_GROUP_MESSAGE, group);
            assertInvalidMessage(message, "SELECT id FROM %s WHERE " + group + " ALLOW FILTERING");
            assertInvalidMessage(message, "SELECT id FROM %s WHERE addrs.city = 'Rome' AND " + group + " ALLOW FILTERING");
            String bound = group.replace("()", "?");
            assertInvalidMessage(String.format(ElementMatchRestriction.EMPTY_GROUP_MESSAGE, bound),
                                 "SELECT id FROM %s WHERE " + bound + " ALLOW FILTERING", list());
        }
        // with other conditions, the empty NOT IN restricts nothing
        assertIds("addrs CONTAINS ELEMENT (city = 'Rome' AND zip NOT IN ())", 1, 2);
        assertIdsBound("addrs CONTAINS ELEMENT (city = 'Rome' AND zip NOT IN ?)", new Object[]{ list() }, 1, 2);
        assertIdsBound("addrs CONTAINS ELEMENT (city = 'Rome' AND zip NOT IN ?)", new Object[]{ list(100) }, 1);
    }

    /**
     * The row filter of a group: a NOT CONTAINS ELEMENT with a single equality is the != on the field, the CQL forms
     * of the groups name the fields relative to the elements.
     */
    @Test
    public void testRowFilters() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, addrs " + collection(Kind.LIST, "frozen<" + address + '>') + ')');
        index("addrs.city");
        index("addrs.zip");

        RowFilter filter = rowFilter("addrs CONTAINS ELEMENT (city = 'Rome' AND (zip >= 150 OR loc.lat < 2) AND zip != 100)");
        assertThat(filter.expressions()).hasSize(1);
        RowFilter.Expression group = filter.expressions().get(0);
        assertThat(group).isInstanceOf(RowFilter.ElementMatchExpression.class);
        assertThat(group.toString()).isEqualTo("addrs CONTAINS ELEMENT (city = 'Rome' AND zip != 100 AND (zip >= 150 OR loc.lat < 2.0))");
        assertThat(filter.toCQLString(org.apache.cassandra.db.marshal.Redaction.REDACT)).doesNotContain("Rome");
        // the candidates, on the absolute paths: the positive conditions
        assertThat(((RowFilter.ElementMatchExpression) group).candidates().toCQLString(org.apache.cassandra.db.marshal.Redaction.NONE))
        .isEqualTo("addrs.city = 'Rome' AND (addrs.zip >= 150 OR addrs.loc.lat < 2.0)");

        filter = rowFilter("addrs NOT CONTAINS ELEMENT (city = 'Rome')");
        assertThat(filter.expressions()).hasSize(1);
        assertThat(filter.expressions().get(0)).isInstanceOf(RowFilter.FieldExpression.class);
        assertThat(filter.expressions().get(0).toString()).isEqualTo("addrs.city != 'Rome'");

        filter = rowFilter("addrs NOT CONTAINS ELEMENT (city = 'Rome' AND zip = 100) ALLOW FILTERING");
        assertThat(filter.expressions().get(0).toString()).isEqualTo("addrs NOT CONTAINS ELEMENT (city = 'Rome' AND zip = 100)");
        assertThat(((RowFilter.ElementMatchExpression) filter.expressions().get(0)).candidates()).isNull();
    }

    private RowFilter rowFilter(String where)
    {
        String query = "SELECT id FROM " + KEYSPACE + '.' + currentTable() + " WHERE " + where;
        ClientState state = ClientState.forInternalCalls();
        SelectStatement statement = (SelectStatement) QueryProcessor.parseStatement(query, state);
        return statement.getRowFilter(QueryOptions.DEFAULT, state);
    }

    /**
     * {@code element} is an unreserved keyword: it remains a valid name of column, field and function, and
     * {@code CONTAINS element(x)} is still a call to a function named {@code element}.
     */
    @Test
    public void testElementAsName() throws Throwable
    {
        String type = createType("CREATE TYPE %s (element int, other int)");
        createTables("CREATE TABLE %s (id int PRIMARY KEY, element " + collection(Kind.LIST, "frozen<" + type + '>') + ", nums " + collection(Kind.SET, "int") + ')');
        index("element.element");
        execute("CREATE FUNCTION IF NOT EXISTS " + KEYSPACE + ".element(x int) CALLED ON NULL INPUT RETURNS int LANGUAGE java AS 'return x + 1;'");
        write("INSERT INTO %s (id, element, nums) VALUES (1, [{element: 1, other: 2}], {2})");
        write("INSERT INTO %s (id, element, nums) VALUES (2, [{element: 2, other: 1}], {3})");

        assertIds("element CONTAINS ELEMENT (element = 1 AND other = 2)", 1);
        assertIds("element.element = 2", 2);
        assertFilteredIds("nums CONTAINS element(1)", new Object[0], 1);
        assertFilteredIds("nums NOT CONTAINS element(1)", new Object[0], 2);
        assertFilteredIds("nums CONTAINS element(?)", new Object[]{ 2 }, 2);
    }

    @Test
    public void testInvalidQueries() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, addrs " + collection(Kind.LIST, "frozen<" + address + '>') + ", " +
                    "addr frozen<" + address + ">, nums " + collection(Kind.LIST, "int") + ", v int, " +
                    "m map<text, int>)");
        index("addrs.city");

        // the collection must be a collection of UDTs or tuples
        assertInvalidMessage(String.format(ElementMatchRelation.NOT_A_COLLECTION_OF_UDTS_MESSAGE, "addr", "frozen<" + address + '>'),
                             "SELECT id FROM %s WHERE addr CONTAINS ELEMENT (city = 'Rome') ALLOW FILTERING");
        assertInvalidMessage("CONTAINS ELEMENT requires a collection of user-defined types or tuples, but nums is of type",
                             "SELECT id FROM %s WHERE nums CONTAINS ELEMENT (x = 1) ALLOW FILTERING");
        assertInvalidMessage("CONTAINS ELEMENT requires a collection of user-defined types or tuples, but addr.tags is of type",
                             "SELECT id FROM %s WHERE addr.tags CONTAINS ELEMENT (x = 1) ALLOW FILTERING");
        assertInvalidMessage("CONTAINS ELEMENT requires a collection of user-defined types or tuples, but addrs.city is of type",
                             "SELECT id FROM %s WHERE addrs.city CONTAINS ELEMENT (x = 1) ALLOW FILTERING");
        assertInvalidMessage("CONTAINS ELEMENT requires a collection of user-defined types or tuples, but v is of type",
                             "SELECT id FROM %s WHERE v CONTAINS ELEMENT (x = 1) ALLOW FILTERING");
        assertInvalidMessage("Invalid restriction on addr.nope: Undefined field nope",
                             "SELECT id FROM %s WHERE addr.nope CONTAINS ELEMENT (x = 1) ALLOW FILTERING");
        assertInvalidMessage("Undefined column name nope",
                             "SELECT id FROM %s WHERE nope CONTAINS ELEMENT (x = 1) ALLOW FILTERING");

        // the conditions are relations on the fields of the elements
        assertInvalidMessage("Invalid restriction on addrs.nope: Undefined field nope",
                             "SELECT id FROM %s WHERE addrs CONTAINS ELEMENT (nope = 1) ALLOW FILTERING");
        assertInvalidMessage("Invalid restriction on addrs.loc.nope: Undefined field nope",
                             "SELECT id FROM %s WHERE addrs CONTAINS ELEMENT (loc.nope = 1) ALLOW FILTERING");
        assertInvalidMessage("Invalid STRING constant (a) for",
                             "SELECT id FROM %s WHERE addrs CONTAINS ELEMENT (zip = 'a') ALLOW FILTERING");
        assertInvalidMessage("Cannot use CONTAINS on non-collection field addrs.city",
                             "SELECT id FROM %s WHERE addrs CONTAINS ELEMENT (city CONTAINS 'a') ALLOW FILTERING");
        assertInvalidMessage("is not supported on fields: addrs.tags",
                             "SELECT id FROM %s WHERE addrs CONTAINS ELEMENT (tags CONTAINS KEY 'a') ALLOW FILTERING");
        assertInvalidMessage("Nested CONTAINS ELEMENT groups are not supported",
                             "SELECT id FROM %s WHERE addrs CONTAINS ELEMENT (city = 'a' AND tags CONTAINS ELEMENT (x = 1)) ALLOW FILTERING");
        assertInvalidMessage("IS NOT NULL restrictions on fields are not yet supported: addrs.city IS NOT NULL",
                             "SELECT id FROM %s WHERE addrs CONTAINS ELEMENT (city IS NOT NULL) ALLOW FILTERING");
        assertInvalidMessage(String.format(ElementMatchRelation.UNSUPPORTED_RELATION_MESSAGE, "token(id) > 0"),
                             "SELECT id FROM %s WHERE addrs CONTAINS ELEMENT (token(id) > 0) ALLOW FILTERING");
        assertInvalidMessage(String.format(ElementMatchRelation.UNSUPPORTED_RELATION_MESSAGE, "city['a'] = 1"),
                             "SELECT id FROM %s WHERE addrs CONTAINS ELEMENT (city['a'] = 1) ALLOW FILTERING");
        assertInvalidMessage(String.format(ElementMatchRelation.UNSUPPORTED_RELATION_MESSAGE, "(city, zip) = ('a', 1)"),
                             "SELECT id FROM %s WHERE addrs CONTAINS ELEMENT ((city, zip) = ('a', 1)) ALLOW FILTERING");
        // LIKE needs an index, as outside groups
        assertInvalidMessage(String.format(StatementRestrictions.RESTRICTION_REQUIRES_INDEX_MESSAGE, "LIKE", "addrs.street LIKE 'a%'"),
                             "SELECT id FROM %s WHERE addrs CONTAINS ELEMENT (city = 'a' AND street LIKE 'a%%') ALLOW FILTERING");
        assertInvalidMessage("Unsupported null value for field addrs.city",
                             "SELECT id FROM %s WHERE addrs CONTAINS ELEMENT (city = ?)", (Object) null);

        // only in SELECT, and not in materialized views
        assertInvalidMessage(String.format(StatementRestrictions.FIELD_RESTRICTIONS_ONLY_IN_SELECT_MESSAGE, "addrs CONTAINS ELEMENT (city = 'a')"),
                             "DELETE FROM %s WHERE id = 1 AND addrs CONTAINS ELEMENT (city = 'a')");
        assertInvalidMessage(String.format(StatementRestrictions.FIELD_RESTRICTIONS_ONLY_IN_SELECT_MESSAGE, "addrs CONTAINS ELEMENT (city = 'a')"),
                             "UPDATE %s SET v = 1 WHERE id = 1 AND addrs CONTAINS ELEMENT (city = 'a')");
        assertInvalidMessage(String.format(StatementRestrictions.FIELD_RESTRICTIONS_NOT_IN_VIEWS_MESSAGE, "addrs CONTAINS ELEMENT (city = 'a')"),
                             "CREATE MATERIALIZED VIEW " + KEYSPACE + ".mv AS SELECT * FROM %s WHERE id IS NOT NULL AND addrs CONTAINS ELEMENT (city = 'a') PRIMARY KEY (id)");
        assertInvalidMessage("SELECT DISTINCT with WHERE clause only supports restriction by partition key and/or static columns",
                             "SELECT DISTINCT id FROM %s WHERE addrs CONTAINS ELEMENT (city = 'a')");

        // the syntax
        // without conditions, it's a call to a function named element
        assertInvalidMessage("Unknown function ELEMENT called (in a WHERE clause, CONTAINS ELEMENT needs at least one condition)",
                             "SELECT id FROM %s WHERE addrs CONTAINS ELEMENT () ALLOW FILTERING");
        assertInvalidSyntax("SELECT id FROM %s WHERE addrs CONTAINS ELEMENT city = 'a' ALLOW FILTERING");
        assertInvalidSyntax("SELECT id FROM %s WHERE addrs CONTAINS ELEMENT (city = 'a' AND) ALLOW FILTERING");
    }
}
