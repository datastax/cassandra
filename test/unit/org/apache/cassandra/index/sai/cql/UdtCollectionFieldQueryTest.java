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

import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.ListMultimap;
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
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.v1.V1OnDiskFormat;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.plan.Operation;
import org.apache.cassandra.index.sai.plan.QueryController;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.FBUtilities;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * CQL {@code WHERE} restrictions on the fields inside collections (HCD-653, phase P3): the fields of the elements of
 * lists and sets of UDTs and tuples, and of the values of maps of them, the collections inside UDTs and tuples, at any
 * depth, and the leaf collections inside the elements ({@code addrs.tags CONTAINS 'y'}).
 * <p>
 * A relation on such a field is satisfied by the rows where <em>some</em> element satisfies it; the AND-ed relations
 * are independent (each may be satisfied by a different element); {@code !=}, {@code NOT IN} and
 * {@code NOT CONTAINS} are satisfied by the rows where <em>no</em> element is equal to (contains) the value, including
 * the rows without elements.
 * <p>
 * Every query on an indexed field is also run with {@code ALLOW FILTERING} on a twin table without any index, which
 * must give the same rows. The tests are run on frozen and on non-frozen collections.
 */
@RunWith(Parameterized.class)
public class UdtCollectionFieldQueryTest extends SAITester
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
        // for the paging and the client warnings
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
     * Runs the checks, then flushes and runs them again, then compacts and runs them again.
     */
    private void checkFlushedAndCompacted(CheckedFunction check) throws Throwable
    {
        check.apply();
        flushBoth();
        check.apply();
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

    private static List<Integer> ids(UntypedResultSet result)
    {
        List<Integer> ids = new ArrayList<>();
        for (UntypedResultSet.Row row : result)
            ids.add(row.getInt("id"));
        return ids;
    }

    private static final String ROME = "{street: 'a', city: 'Rome', zip: 200, loc: {lat: 1.0, lon: 0.5}, tags: {'x', 'y'}}";
    private static final String MILAN = "{street: 'b', city: 'Milan', zip: 100, loc: {lat: 3.0}, tags: {'z'}}";
    private static final String ROME2 = "{street: 'bc', city: 'Rome', zip: 100, loc: {lat: 5.0}, tags: {'y'}}";
    private static final String NO_CITY = "{street: 'd', zip: 300}";

    /**
     * Inserts the u_list rows of the DSE Search reference run ({@code docs/sai-udt/dse-runtime.md}): 1 =
     * [{Rome, 200, lat 1}, {Milan, 100, lat 3}], 2 = [{Rome, 100, lat 5}], plus an empty collection (3), no collection
     * (4) and an element without city (5).
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
     * The u_list queries of the DSE Search reference run, with the {@code solr_query} of each CQL query, on a
     * collection of the given kind. In DSE the AND inside one tuple query is on the same element; the CQL AND is
     * independent (the DSE separate {@code q} and {@code fq}), the same element is {@code CONTAINS ELEMENT} (see
     * {@code UdtElementMatchQueryTest}).
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
            // some element
            assertIds("addrs.city = 'Rome'", 1, 2);                              // {!tuple}addrs.city:Rome
            assertIds("addrs.city = 'Milan'", 1);
            assertIds("addrs.city = 'rome'");
            assertIds("addrs.zip = 100", 1, 2);
            assertIds("addrs.zip = 300", 5);
            assertIds("addrs.zip >= 150", 1, 5);                                 // {!tuple}addrs.zip:[150 TO *]
            assertIds("addrs.loc.lat >= 2 AND addrs.loc.lat <= 6", 1, 2);        // {!tuple}addrs.loc.lat:[2 TO 6]
            assertIds("addrs.loc.lat < 2", 1);
            assertIds("addrs.tags CONTAINS 'y'", 1, 2);                          // {!tuple}addrs.tags:y
            assertIds("addrs.tags CONTAINS 'z'", 1);
            assertIds("addrs.tags CONTAINS 'w'");
            assertIds("addrs.city IN ('Milan', 'Turin')", 1);                    // {!tuple}addrs.city:Rome OR addrs.city:Milan
            assertIds("addrs.city IN ('Rome', 'Milan')", 1, 2);
            assertIds("addrs.zip IN (300, 400)", 5);
            assertIds("addrs.city >= 'N'", 1, 2);
            assertIds("addrs.city < 'N'", 1);
            assertIndexedIdsBound("addrs.street LIKE ?", new Object[]{ "b%" }, 1, 2);
            assertIndexedIdsBound("addrs.street LIKE ?", new Object[]{ "%c" }, 2);

            // independent: each relation may be satisfied by a different element
            assertIds("addrs.city = 'Rome' AND addrs.zip = 100", 1, 2);          // {"q": "{!tuple}addrs.city:Rome", "fq": "{!tuple}addrs.zip:100"}
            assertIds("addrs.city = 'Rome' AND addrs.city = 'Milan'", 1);
            assertIds("addrs.city = 'Milan' AND addrs.loc.lat > 4");
            assertIds("addrs.city = 'Rome' AND addrs.loc.lat > 4", 2);
            assertIds("addrs.tags CONTAINS 'x' AND addrs.tags CONTAINS 'z'", 1);
            // two bounds on the same field are independent too: here a single element can't satisfy both
            assertIds("addrs.zip > 150 AND addrs.zip < 150", 1);
            assertIds("addrs.loc.lat < 2 AND addrs.loc.lat > 2", 1);
            assertIds("addrs.zip >= 150 AND addrs.zip < 300", 1);
            assertIds("addrs.zip > 150 AND addrs.zip > 250", 5);

            // no element: the rows without elements, or whose elements have no value for the field, are included
            assertIds("addrs.zip != 100", 3, 4, 5);                             // -{!tuple}addrs.zip:100
            assertIds("addrs.zip != 200", 2, 3, 4, 5);
            assertIds("addrs.zip NOT IN (100, 300)", 3, 4);
            // the != on a text field inside collections is served by the index (as NOT CONTAINS on set<text> is)
            assertIds("addrs.city != 'Rome'", 3, 4, 5);
            assertIds("addrs.city NOT IN ('Milan', 'Turin')", 2, 3, 4, 5);
            assertIds("addrs.city NOT IN ('Milan', 'Turin', 'Rome')", 3, 4, 5);
            assertIds("addrs.zip NOT IN (100, 200, 300)", 3, 4);
            assertIds("addrs.city >= 'N' AND addrs.city != 'Milan'", 2);
            assertIds("addrs.tags NOT CONTAINS 'y'", 3, 4, 5);
            assertIds("addrs.tags NOT CONTAINS 'z'", 2, 3, 4, 5);
            // a range and a != on the same field are independent: no element is excluded from the range alone
            assertIds("addrs.zip >= 100 AND addrs.zip != 100", 5);
            assertIds("addrs.city = 'Rome' AND addrs.zip != 100");
            assertIds("addrs.zip > 150 AND addrs.zip != 300", 1);

            // combined with other restrictions
            assertIds("id = 1 AND addrs.city = 'Rome'", 1);                     // id:1 AND {!tuple}addrs.city:Rome
            assertIds("addrs.city = 'Milan' OR addrs.zip = 300", 1, 5);
            assertIds("(addrs.city = 'Rome' AND addrs.zip = 200) OR addrs.tags CONTAINS 'y'", 1, 2);
            // a multi-valued != in a disjunction
            assertIds("addrs.zip != 100 OR addrs.city = 'Milan'", 1, 3, 4, 5);
            assertIds("addrs.city != 'Rome' OR addrs.zip = 200", 1, 3, 4, 5);
        });

        // the paging and LIMIT
        for (String where : new String[]{ "addrs.city = 'Rome'", "addrs.zip != 100", "addrs.city = 'Rome' AND addrs.zip = 100", "addrs.tags NOT CONTAINS 'z'" })
            assertPaging(where);
    }

    /**
     * Checks a query with the page sizes 1 and 2 and with a LIMIT: the same rows as without paging.
     */
    private void assertPaging(String where)
    {
        List<Integer> expected = ids(execute("SELECT id FROM %s WHERE " + where));
        assertThat(expected).isNotEmpty();
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
            assertIds("ts.field1 = 'foo'", 1, 2);                                // tuplecollection_list.field1:3
            assertIds("ts.field2 = 1", 1);
            assertIds("ts.field2 > 1", 1, 2, 3);
            // independent: DSE {!tuple}ts.field1:foo AND ts.field2:2 (same element) matches nothing
            assertIds("ts.field1 = 'foo' AND ts.field2 = 2", 1);
            assertIds("ts.field1 = 'foo' AND ts.field2 = 1", 1);
            assertIds("ts.field1 = 'baz' AND ts.field2 = 2", 3);
            assertIds("ts.field2 != 2", 2);
            assertIds("ts.field1 != 'foo' ALLOW FILTERING", 3);
            assertIds("ts.field2 IN (3, 4)", 2);
        });
    }

    /**
     * The collections inside UDTs and tuples (DSE {@code direc.names.firstname}, {@code direc.names.hobbies},
     * {@code tuplewithtuplecollection.field3.field1}), and the UDTs inside collections inside UDTs inside
     * collections.
     */
    @Test
    public void testNestedCollections() throws Throwable
    {
        String name = createType("CREATE TYPE %s (firstname text, lastname text, hobbies frozen<set<text>>)");
        String direction = createType("CREATE TYPE %s (city text, names frozen<list<frozen<" + name + ">>>, listfield frozen<list<text>>)");
        // the parameter applies to the UDT column and to the outer collection, the collections inside UDTs and
        // tuples are frozen
        createTables("CREATE TABLE %s (id int PRIMARY KEY, direc " + (frozen ? "frozen<" + direction + '>' : direction) + ", " +
                     "tt tuple<int, text, frozen<list<frozen<tuple<int, text>>>>>, " +
                     "dirs " + collection(Kind.LIST, "frozen<" + direction + '>') + ')');
        index("direc.names.firstname");
        index("direc.names.hobbies");
        index("direc.listfield");
        index("tt.field3.field1");
        index("dirs.names.firstname");
        index("dirs.city");

        String bilbo = "{firstname: 'Bilbo', lastname: 'Baggins', hobbies: {'swim1', 'eat'}}";
        String sam = "{firstname: 'Sam', lastname: 'Gamgee', hobbies: {'cook'}}";
        String frodo = "{firstname: 'Frodo', lastname: 'Baggins'}";
        write("INSERT INTO %s (id, direc, tt, dirs) VALUES (1, {city: 'bcn', names: [" + bilbo + ", " + sam + "], listfield: ['listval1']}, " +
              "(1, 'a', [(9, 'x'), (6, 'y')]), [{city: 'bcn', names: [" + bilbo + "]}, {city: 'mad', names: [" + sam + "]}])");
        write("INSERT INTO %s (id, direc, tt, dirs) VALUES (2, {city: 'mad', names: [" + frodo + "]}, (2, 'b', [(6, 'z')]), [{city: 'mad', names: [" + frodo + ", " + sam + "]}])");
        write("INSERT INTO %s (id, direc, tt, dirs) VALUES (3, {city: 'bcn', names: []}, (3, 'c', []), [{city: 'bcn'}])");

        inAllStates(() -> {
            assertIds("direc.names.firstname = 'Bilbo'", 1);                     // direc.names.firstname:Bilbo
            assertIds("direc.names.firstname IN ('Sam', 'Frodo')", 1, 2);
            assertIds("direc.names.hobbies CONTAINS 'swim1'", 1);                // direc.names.hobbies:swim1
            assertIds("direc.names.hobbies NOT CONTAINS 'cook'", 2, 3);
            assertIds("direc.listfield CONTAINS 'listval1'", 1);                 // direc.listfield:listval1
            assertIds("tt.field3.field1 = 9", 1);                                // tuplewithtuplecollection.field3.field1:9
            assertIds("tt.field3.field1 = 6", 1, 2);
            assertIds("tt.field3.field1 > 6", 1);
            assertIds("tt.field3.field1 != 6", 3);
            // independent, also on an unindexed field
            assertIds("direc.names.firstname = 'Bilbo' AND direc.names.lastname = 'Gamgee' ALLOW FILTERING", 1);
            assertIds("direc.names.firstname = 'Frodo' AND direc.names.lastname = 'Gamgee' ALLOW FILTERING");
            assertIds("tt.field3.field1 = 9 AND tt.field3.field2 = 'y' ALLOW FILTERING", 1);
            // two levels of collections
            assertIds("dirs.names.firstname = 'Sam'", 1, 2);
            assertIds("dirs.names.firstname = 'Bilbo' AND dirs.city = 'mad'", 1);
            assertIds("dirs.names.firstname != 'Sam' ALLOW FILTERING", 3);
            assertIds("dirs.names.hobbies CONTAINS 'cook' ALLOW FILTERING", 1, 2);
            assertIds("dirs.names.lastname = 'Baggins' AND dirs.city != 'mad' ALLOW FILTERING");
        });
    }

    /**
     * The elements added to, removed from and replaced in the collections, in the memtable (the index update path)
     * and across SSTables (the stale terms of the older SSTables are filtered out).
     */
    @Test
    public void testElementUpdates() throws Throwable
    {
        for (Kind kind : Kind.values())
        {
            createTables("CREATE TABLE %s (id int PRIMARY KEY, addrs " + collection(kind, "frozen<" + address + '>') + ')');
            index("addrs.city");
            index("addrs.zip");

            String rome = "{city: 'Rome', zip: 200}";
            String milan = "{city: 'Milan', zip: 100}";
            String turin = "{city: 'Turin', zip: 300}";
            String naples = "{city: 'Naples', zip: 400}";
            write("INSERT INTO %s (id, addrs) VALUES (1, " + literal(kind, rome, milan) + ')');
            write("INSERT INTO %s (id, addrs) VALUES (2, " + literal(kind, rome) + ')');

            // in the memtable: id 1 = [rome, milan, turin], id 2 = [] (with the frozen collections, the whole
            // collection is overwritten)
            appendElement(kind, 1, turin, literal(kind, rome, milan, turin));
            removeFirstElement(kind, 2, rome, literal(kind));
            CheckedFunction afterFirst = () -> {
                assertIds("addrs.city = 'Rome'", 1);
                assertIds("addrs.city = 'Turin'", 1);
                assertIds("addrs.zip = 200", 1);
                assertIds("addrs.zip != 100", 2);
                assertIds("addrs.zip != 400", 1, 2);
                assertIds("addrs.zip > 250", 1);
            };
            afterFirst.apply();

            // across SSTables: id 1 = [milan, turin, naples], id 2 = [naples]
            flushBoth();
            afterFirst.apply();
            removeFirstElement(kind, 1, rome, literal(kind, milan, turin));
            appendElement(kind, 1, naples, literal(kind, milan, turin, naples));
            appendElement(kind, 2, naples, literal(kind, naples));
            checkFlushedAndCompacted(() -> {
                assertIds("addrs.city = 'Rome'");
                assertIds("addrs.zip = 200");
                assertIds("addrs.city = 'Naples'", 1, 2);
                assertIds("addrs.city = 'Naples' AND addrs.city = 'Milan'", 1);
                assertIds("addrs.zip != 200", 1, 2);
                assertIds("addrs.zip != 100", 2);
                assertIds("addrs.zip >= 400", 1, 2);
                assertIds("addrs.city != 'Turin' ALLOW FILTERING", 2);
            });

            // a list element replaced by its index, a map value replaced by its key
            if (kind != Kind.SET && !frozen)
            {
                write("UPDATE %s SET addrs[" + (kind == Kind.LIST ? "0" : "'k1'") + "] = " + rome + " WHERE id = 1");
                checkFlushedAndCompacted(() -> {
                    assertIds("addrs.city = 'Milan'");
                    assertIds("addrs.city = 'Rome'", 1);
                    assertIds("addrs.zip != 100", 1, 2);
                });
            }
        }
    }

    /**
     * Adds an element to a collection: appended to a list, added to a set, put in a map (with a key that isn't used
     * by the literals), or, for a frozen collection, the whole collection is overwritten with the given literal.
     */
    private void appendElement(Kind kind, int id, String element, String frozenLiteral)
    {
        if (frozen)
        {
            write("UPDATE %s SET addrs = " + frozenLiteral + " WHERE id = ?", id);
            return;
        }
        switch (kind)
        {
            case LIST:
                write("UPDATE %s SET addrs = addrs + [" + element + "] WHERE id = ?", id);
                break;
            case SET:
                write("UPDATE %s SET addrs = addrs + {" + element + "} WHERE id = ?", id);
                break;
            default:
                write("UPDATE %s SET addrs['n" + element.hashCode() + "'] = " + element + " WHERE id = ?", id);
                break;
        }
    }

    /**
     * Removes the first element of a collection, the given one: by its index from a list, by value from a set, by
     * its key ({@code k0}) from a map, or, for a frozen collection, the whole collection is overwritten with the given
     * literal.
     */
    private void removeFirstElement(Kind kind, int id, String element, String frozenLiteral)
    {
        if (frozen)
        {
            write("UPDATE %s SET addrs = " + frozenLiteral + " WHERE id = ?", id);
            return;
        }
        switch (kind)
        {
            case LIST:
                write("DELETE addrs[0] FROM %s WHERE id = ?", id);
                break;
            case SET:
                write("UPDATE %s SET addrs = addrs - {" + element + "} WHERE id = ?", id);
                break;
            default:
                write("DELETE addrs['k0'] FROM %s WHERE id = ?", id);
                break;
        }
    }

    /**
     * The duplicate elements of the lists, the elements whose fields are null, the empty strings, a list element
     * removed by value.
     */
    @Test
    public void testDuplicatesAndNulls() throws Throwable
    {
        createTables("CREATE TABLE %s (id int PRIMARY KEY, addrs " + collection(Kind.LIST, "frozen<" + address + '>') + ')');
        index("addrs.city");
        index("addrs.zip");
        index("addrs.street");

        write("INSERT INTO %s (id, addrs) VALUES (1, [{city: 'Rome', zip: 1}, {city: 'Rome', zip: 1}, {city: 'Milan'}])");
        write("INSERT INTO %s (id, addrs) VALUES (2, [{city: null, zip: null}, {zip: 2}])");
        write("INSERT INTO %s (id, addrs) VALUES (3, [{street: '', city: 'Turin'}])");
        write("INSERT INTO %s (id, addrs) VALUES (4, null)");

        inAllStates(() -> {
            assertIds("addrs.city = 'Rome'", 1);
            assertIds("addrs.zip = 1", 1);
            assertIds("addrs.zip > 0", 1, 2);
            assertIds("addrs.zip != 1", 2, 3, 4);
            assertIds("addrs.zip NOT IN (1, 2)", 3, 4);
            assertIds("addrs.city != 'Milan' ALLOW FILTERING", 2, 3, 4);
            assertIds("addrs.city IN ('Rome', 'Turin')", 1, 3);
            assertIds("addrs.street = ''", 3);
        });

        if (!frozen)
        {
            // removing one of the duplicates by value removes both (as for list<text>), removing one by index keeps
            // the other
            write("UPDATE %s SET addrs = addrs - [{city: 'Milan'}] WHERE id = 1");
            write("DELETE addrs[0] FROM %s WHERE id = 1");
            checkFlushedAndCompacted(() -> {
                assertIds("addrs.city = 'Rome'", 1);
                assertIds("addrs.city = 'Milan'");
                assertIds("addrs.zip = 1", 1);
            });
            write("UPDATE %s SET addrs = addrs - [{city: 'Rome', zip: 1}] WHERE id = 1");
            checkFlushedAndCompacted(() -> {
                assertIds("addrs.city = 'Rome'");
                assertIds("addrs.zip != 1", 1, 2, 3, 4);
            });
        }
    }

    /**
     * An analyzer on a field inside collections normalizes each element, also for the values excluded by
     * {@code !=} / {@code NOT IN} (the index serving the ranges on that field normalizes them, as for the single
     * fields of P2).
     */
    @Test
    public void testAnalyzedField() throws Throwable
    {
        createTables("CREATE TABLE %s (id int PRIMARY KEY, addrs " + collection(Kind.LIST, "frozen<" + address + '>') + ')');
        index("addrs.city", " WITH OPTIONS = {'case_sensitive': 'false'}");
        write("INSERT INTO %s (id, addrs) VALUES (1, [{city: 'Rome'}, {city: 'Milan'}])");
        write("INSERT INTO %s (id, addrs) VALUES (2, [{city: 'ROME'}])");
        write("INSERT INTO %s (id, addrs) VALUES (3, [{city: 'Paris'}])");

        inAllStates(() -> {
            assertIndexedIds("addrs.city = 'rome'", 1, 2);
            assertIndexedIds("addrs.city : 'rome'", 1, 2);
            assertIndexedIds("addrs.city = 'MILAN' AND addrs.city = 'rome'", 1);
            assertIndexedIdsBound("addrs.city LIKE ?", new Object[]{ "mi%" }, 1);
            assertIndexedIds("addrs.city >= 'p' AND addrs.city < 's'", 1, 2, 3);
            // normalized exclusions, with a range and alone: the index serves them ("no element equals")
            assertIndexedIds("addrs.city >= 'a' AND addrs.city != 'rome'", 3);
            assertIndexedIds("addrs.city != 'rome'", 3);
            assertIndexedIds("addrs.city NOT IN ('PARIS', 'milan')", 2);
            assertIndexedIds("addrs.city NOT IN ('PARIS', 'milan', 'nowhere')", 2);
            // also normalized with ALLOW FILTERING
            assertIndexedIds("addrs.city != 'rome' ALLOW FILTERING", 3);
            // IN on an analyzed field, as on an analyzed column
            assertInvalidMessage("IN operation does not support analyzers", "SELECT id FROM %s WHERE addrs.city IN ('rome', 'paris')");
            // the raw values without index
            assertFilteredIds("addrs.city != 'ROME'", new Object[0], 1, 3);
            assertFilteredIds("addrs.city = 'rome'", new Object[0]);
        });
    }

    /**
     * Bind markers on fields inside collections get the type and the name of the field of the elements.
     */
    @Test
    public void testPreparedStatements() throws Throwable
    {
        createTables("CREATE TABLE %s (id int PRIMARY KEY, addrs " + collection(Kind.LIST, "frozen<" + address + '>') + ')');
        index("addrs.city");
        index("addrs.zip");
        index("addrs.tags");
        insertAddresses(Kind.LIST);

        ResultMessage.Prepared prepared = prepare("SELECT id FROM %s WHERE addrs.city = ? AND addrs.zip > ? AND addrs.tags CONTAINS ? AND addrs.zip IN ?");
        List<ColumnSpecification> names = prepared.metadata.names;
        assertThat(names).hasSize(4);
        assertThat(names.get(0).name.toString()).isEqualTo("addrs.city");
        assertThat(names.get(0).type).isEqualTo(UTF8Type.instance);
        assertThat(names.get(1).name.toString()).isEqualTo("addrs.zip");
        assertThat(names.get(1).type).isEqualTo(Int32Type.instance);
        assertThat(names.get(2).type).isEqualTo(UTF8Type.instance);
        assertThat(names.get(3).type).isEqualTo(ListType.getInstance(Int32Type.instance, false));

        inAllStates(() -> {
            assertIdsBound("addrs.city = ?", new Object[]{ "Rome" }, 1, 2);
            assertIdsBound("addrs.city = ? AND addrs.zip = ?", new Object[]{ "Rome", 100 }, 1, 2);
            assertIdsBound("addrs.zip > ? AND addrs.zip < ?", new Object[]{ 150, 150 }, 1);
            assertIdsBound("addrs.tags CONTAINS ?", new Object[]{ "y" }, 1, 2);
            assertIdsBound("addrs.zip IN ?", new Object[]{ list(300, 400) }, 5);
            assertIdsBound("addrs.zip NOT IN ?", new Object[]{ list(100, 300) }, 3, 4);
            assertIdsBound("addrs.zip != :v", new Object[]{ 100 }, 3, 4, 5);
        });
    }

    /**
     * The fields inside the elements of frozen collections in the primary key and in static columns.
     */
    @Test
    public void testKeyAndStaticColumns() throws Throwable
    {
        // the key columns are always frozen, the static column is frozen or not depending on the parameter
        createTables("CREATE TABLE %s (p int, k frozen<list<frozen<" + geo + ">>>, c frozen<set<frozen<tuple<text, int>>>>, " +
                     "s " + collection(Kind.LIST, "frozen<" + geo + '>') + " static, id int, PRIMARY KEY ((p, k), c))");
        index("k.lat");
        index("c.field2");
        index("s.lon");

        write("INSERT INTO %s (p, k, c, s, id) VALUES (0, [{lat: 1.0}, {lat: 2.0}], {('a', 1), ('b', 2)}, [{lon: 5.0}], 1)");
        write("INSERT INTO %s (p, k, c, id) VALUES (0, [{lat: 1.0}, {lat: 2.0}], {('c', 3)}, 2)");
        write("INSERT INTO %s (p, k, c, s, id) VALUES (0, [{lat: 3.0}], {('a', 4)}, [{lon: 6.0}, {lon: 7.0}], 3)");

        inAllStates(() -> {
            assertIds("k.lat = 2.0", 1, 2);
            assertIds("k.lat > 2.5", 3);
            assertIds("k.lat != 1.0", 3);
            assertIds("c.field2 = 2", 1);
            assertIds("c.field2 > 2", 2, 3);
            assertIds("c.field2 != 1", 2, 3);
            assertIds("c.field1 = 'a' AND c.field2 = 4 ALLOW FILTERING", 3);
            assertIds("s.lon = 5.0", 1, 2);
            assertIds("s.lon >= 6.0 AND k.lat = 3.0", 3);
            assertIds("k.lat = 1.0 AND c.field2 = 3", 2);
            // != on the field of a static column: the static row of the partition
            assertIds("s.lon != 5.0", 3);
            assertIds("s.lon NOT IN (6.0, 8.0)", 1, 2);
        });

        // an element added to the static column
        if (frozen)
            write("UPDATE %s SET s = [{lon: 6.0}, {lon: 7.0}, {lon: 8.0}] WHERE p = 0 AND k = [{lat: 3.0}]");
        else
            write("UPDATE %s SET s = s + [{lon: 8.0}] WHERE p = 0 AND k = [{lat: 3.0}]");
        checkFlushedAndCompacted(() -> {
            assertIds("s.lon = 8.0", 3);
            assertIds("s.lon != 8.0", 1, 2);
        });
    }

    /**
     * The elements that expire (TTL) after being indexed, in the memtable and in an SSTable flushed before the
     * expiration: the != / NOT IN / NOT CONTAINS served as "all the keys minus the keys with an equal term" must not
     * subtract the keys of the rows whose term expired.
     */
    @Test
    public void testExpiringElements() throws Throwable
    {
        createTables("CREATE TABLE %s (id int PRIMARY KEY, v int, addrs " + collection(Kind.LIST, "frozen<" + address + '>') + ", " +
                     "nums " + collection(Kind.LIST, "int") + ')');
        index("addrs.zip");
        index("addrs.city");
        index("addrs.tags");
        if (!frozen)
            index("nums");

        for (boolean flushBeforeExpiry : new boolean[]{ false, true })
        {
            int id = flushBeforeExpiry ? 10 : 0;
            write("INSERT INTO %s (id, v, addrs, nums) VALUES (?, 0, [{zip: 200}], [1])", id + 1);
            write("INSERT INTO %s (id, v) VALUES (?, 0)", id + 2);
            String element = "{city: 'Rome', zip: 100, tags: {'t'}}";
            if (frozen)
                write("UPDATE %s USING TTL 1 SET addrs = [" + element + "], nums = [5] WHERE id = ?", id + 2);
            else
                write("UPDATE %s USING TTL 1 SET addrs = addrs + [" + element + "], nums = nums + [5] WHERE id = ?", id + 2);
            if (flushBeforeExpiry)
                flushBoth();

            // wait for the TTL to expire
            Thread.sleep(2000);

            int[] ids = flushBeforeExpiry ? new int[]{ 1, 2, 11, 12 } : new int[]{ 1, 2 };
            Object[] all = Arrays.stream(ids).boxed().toArray();
            CheckedFunction check = () -> {
                assertIds("addrs.zip = 100");
                assertIds("addrs.zip != 100", all);
                assertIds("addrs.zip NOT IN (100, 300)", all);
                assertIds("addrs.city != 'Rome'", all);
                assertIds("addrs.tags NOT CONTAINS 't'", all);
                assertIds("addrs.zip != 100 AND v = 0 ALLOW FILTERING", all);
                if (!frozen)
                {
                    // the NOT CONTAINS on a whole collection column shares the plan
                    assertIds("nums NOT CONTAINS 5", all);
                    assertIds("nums CONTAINS 5");
                }
            };
            check.apply();
            if (flushBeforeExpiry)
            {
                compactBoth();
                check.apply();
            }
        }
    }

    /**
     * Elements that expire, but not before the query: the keys with an equal term are still subtracted (the anti-join
     * is used), in the memtable and in an SSTable, and the results are right.
     */
    @Test
    public void testNotYetExpiredElements() throws Throwable
    {
        createTables("CREATE TABLE %s (id int PRIMARY KEY, v int, addrs " + collection(Kind.LIST, "frozen<" + address + '>') + ')');
        index("addrs.zip");
        index("addrs.city");
        String rome = "{city: 'Rome', zip: 100}";
        String milan = "{city: 'Milan', zip: 200}";
        // appends and frozen overwrites: no tombstones
        write("UPDATE %s SET v = 0, addrs = " + (frozen ? "[" + milan + ']' : "addrs + [" + milan + ']') + " WHERE id = 1");
        write("UPDATE %s USING TTL 1000 SET addrs = " + (frozen ? "[" + rome + ']' : "addrs + [" + rome + ']') + " WHERE id = 2");
        write("UPDATE %s SET v = 0 WHERE id = 3");

        IndexContext zip = getIndexContext(currentTable() + "_addrs_zip_idx");
        CheckedFunction check = () -> {
            long now = FBUtilities.nowInSeconds();
            assertThat(zip.hasShadowingDeletions(now)).isFalse();
            assertThat(zip.getLiveMemtableIndexes()).noneMatch(index -> index.mayHaveExpiredTerms(now));
            assertThat(zip.getView().getIndexes()).noneMatch(index -> IndexContext.mayHaveExpiredTerms(index.getSSTable(), now));
            assertIds("addrs.zip != 100", 1, 3);
            assertIds("addrs.zip NOT IN (100, 200)", 3);
            assertIds("addrs.city != 'Rome'", 1, 3);
        };
        check.apply();
        flushBoth();
        check.apply();
    }

    /**
     * A partition deletion in the memtable, then a re-inserted row: the old elements stay in the reconciled row of the
     * memtable and their terms in the memtable index (and in the SSTable of its flush), but not in the live data.
     */
    @Test
    public void testDeletionAndReinsert() throws Throwable
    {
        createTables("CREATE TABLE %s (id int PRIMARY KEY, v int, addrs " + collection(Kind.LIST, "frozen<" + address + '>') + ')');
        index("addrs.zip");
        index("addrs.city");
        index("addrs.tags");
        write("INSERT INTO %s (id, v, addrs) VALUES (1, 0, [{city: 'Rome', zip: 100, tags: {'x'}}])");
        write("INSERT INTO %s (id, v, addrs) VALUES (2, 0, [{city: 'Rome', zip: 100, tags: {'x'}}])");
        write("DELETE FROM %s WHERE id = 1");
        write("INSERT INTO %s (id, v) VALUES (1, 0)");

        checkFlushedAndCompacted(() -> {
            assertIds("addrs.city != 'Rome'", 1);
            assertIds("addrs.city NOT IN ('Rome')", 1);
            assertIds("addrs.city NOT IN ('Rome', 'Milan')", 1);
            assertIds("addrs.zip != 100", 1);
            assertIds("addrs.zip NOT IN (100, 200)", 1);
            assertIds("addrs.tags NOT CONTAINS 'x'", 1);
            assertIds("addrs.city = 'Rome'", 2);
        });
    }

    /**
     * The empty strings in the elements, written by an INSERT (index path) and by an UPDATE of a row in the memtable
     * (update path).
     */
    @Test
    public void testEmptyStringsInUpdates() throws Throwable
    {
        createTables("CREATE TABLE %s (id int PRIMARY KEY, addrs " + collection(Kind.LIST, "frozen<" + address + '>') + ", name text)");
        index("addrs.street");
        index("name");
        write("INSERT INTO %s (id, addrs) VALUES (1, [{street: ''}])");
        write("INSERT INTO %s (id, addrs) VALUES (2, [{street: 'x'}])");
        if (frozen)
            write("UPDATE %s SET addrs = [{street: 'x'}, {street: ''}] WHERE id = 2");
        else
            write("UPDATE %s SET addrs = addrs + [{street: ''}] WHERE id = 2");
        // the same on a text column: from a value and from null to an empty string
        write("INSERT INTO %s (id, name) VALUES (3, 'x')");
        write("UPDATE %s SET name = '' WHERE id = 3");
        write("UPDATE %s SET name = '' WHERE id = 1");
        checkFlushedAndCompacted(() -> {
            assertIds("addrs.street = ''", 1, 2);
            assertIds("addrs.street = 'x'", 2);
            assertIds("name = ''", 1, 3);
            assertIds("name = 'x'");
        });
    }

    /**
     * A NOT IN on an indexed field inside collections is a single NOT_EQ with exclusions, served by one anti-join
     * (all the keys minus the keys matching any of the values), not one anti-join per value.
     */
    @Test
    public void testNotInPlan() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, addrs " + collection(Kind.LIST, "frozen<" + address + '>') + ')');
        index("addrs.zip");
        index("addrs.city");

        for (String where : new String[]{ "addrs.zip NOT IN (1, 2, 3)", "addrs.city NOT IN ('a', 'b', 'c')" })
        {
            List<Expression> expressions = analyze(where);
            assertThat(expressions).as(where).hasSize(1);
            Expression notIn = expressions.get(0);
            assertThat(notIn.getOp()).isEqualTo(Expression.Op.NOT_EQ);
            assertThat(notIn.context.isIndexed()).isTrue();
            assertThat(notIn.negations()).hasSize(3);
        }

        // the != are combined, but not with the other expressions on the field
        List<Expression> expressions = analyze("addrs.zip > 0 AND addrs.zip != 1 AND addrs.zip != 2");
        assertThat(expressions).hasSize(2);
        assertThat(expressions.get(0).getOp()).isEqualTo(Expression.Op.RANGE);
        assertThat(expressions.get(1).getOp()).isEqualTo(Expression.Op.NOT_EQ);
        assertThat(expressions.get(1).negations()).hasSize(2);
    }

    /**
     * @return the SAI expressions of the given restrictions, as analyzed by the query planner
     */
    private List<Expression> analyze(String where) throws Exception
    {
        String query = "SELECT id FROM " + KEYSPACE + '.' + currentTable() + " WHERE " + where;
        ClientState state = ClientState.forInternalCalls();
        SelectStatement statement = (SelectStatement) QueryProcessor.parseStatement(query, state);
        RowFilter filter = statement.getRowFilter(QueryOptions.DEFAULT, state);
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        ReadCommand command = PartitionRangeReadCommand.allDataRead(cfs.metadata(), FBUtilities.nowInSeconds());
        QueryController controller = new QueryController(cfs, command, V1OnDiskFormat.instance.indexFeatureSet(), new QueryContext(), null);
        Method analyzeGroup = Operation.class.getDeclaredMethod("analyzeGroup", QueryController.class, Operation.OperationType.class, List.class);
        analyzeGroup.setAccessible(true);
        @SuppressWarnings("unchecked")
        ListMultimap<ColumnMetadata, Expression> analyzed = (ListMultimap<ColumnMetadata, Expression>) analyzeGroup.invoke(null, controller, Operation.OperationType.AND, new ArrayList<>(filter.expressions()));
        return new ArrayList<>(analyzed.values());
    }

    /**
     * The guardrail on the intersections of filtered mutable columns: each restriction on a field inside a
     * non-frozen collection may be satisfied by a different element, that is, by a different cell.
     */
    @Test
    public void testIntersectionGuardrail()
    {
        String keyspace = createKeyspace("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}");
        String place = createType(keyspace, "CREATE TYPE %s (city text, zip int)");
        String table = createTable(keyspace, "CREATE TABLE %s (id int PRIMARY KEY, addrs " + collection(Kind.LIST, "frozen<" + place + '>') + ", v int)");

        assertThat(intersectionWarnings(keyspace, table, "addrs.city = 'x'")).isFalse();
        assertThat(intersectionWarnings(keyspace, table, "addrs.city = 'x' AND addrs.city = 'y'")).isEqualTo(!frozen);
        assertThat(intersectionWarnings(keyspace, table, "addrs.city = 'x' AND addrs.zip = 1")).isEqualTo(!frozen);
        assertThat(intersectionWarnings(keyspace, table, "addrs.city = 'x' AND v = 1")).isTrue();
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
    public void testInvalidQueries() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, addrs " + collection(Kind.LIST, "frozen<" + address + '>') + ", " +
                    "mk map<frozen<" + geo + ">, int>)");
        index("addrs.city");

        // the error messages name the field
        assertInvalidMessage("Invalid restriction on addrs.nope: Undefined field nope in the column of type",
                             "SELECT id FROM %s WHERE addrs.nope = 1 ALLOW FILTERING");
        assertInvalidMessage("Invalid STRING constant (a) for",
                             "SELECT id FROM %s WHERE addrs.zip = 'a' ALLOW FILTERING");
        assertInvalidMessage("Cannot use CONTAINS on non-collection field addrs.city",
                             "SELECT id FROM %s WHERE addrs.city CONTAINS 'R' ALLOW FILTERING");
        assertInvalidMessage("is not supported on fields: addrs.tags",
                             "SELECT id FROM %s WHERE addrs.tags CONTAINS KEY 'a' ALLOW FILTERING");
        assertInvalidMessage(String.format(StatementRestrictions.FIELD_REQUIRES_ALLOW_FILTERING_MESSAGE, "addrs.zip"),
                             "SELECT id FROM %s WHERE addrs.zip = 1");
        // the != on a text field inside collections is served by its (non-tokenizing) index
        assertEmpty(execute("SELECT id FROM %s WHERE addrs.city != 'Rome'"));
        // the index on the values of a leaf collection inside the elements doesn't serve != on the whole leaf, alone
        // or with a restriction it serves (the tokenizing indexes, which serve no !=, are refused on these fields,
        // see UdtFieldIndexDDLTest)
        index("addrs.tags");
        assertInvalidMessage(String.format(StatementRestrictions.HAS_UNSUPPORTED_INDEX_RESTRICTION_MESSAGE_SINGLE, "addrs.tags"),
                             "SELECT id FROM %s WHERE addrs.tags != {'a'}");
        assertInvalidMessage(String.format(StatementRestrictions.HAS_UNSUPPORTED_INDEX_RESTRICTION_MESSAGE_SINGLE, "addrs.tags"),
                             "SELECT id FROM %s WHERE addrs.tags CONTAINS 'a' AND addrs.tags != {'b'}");
        // the paths through the keys of a map
        assertInvalidMessage("field paths through the keys of a map are not supported",
                             "SELECT id FROM %s WHERE mk.lat = 1.0 ALLOW FILTERING");
        // IS NOT NULL (P5, see UdtFieldIsNotNullTest), served by the index on the field
        execute("INSERT INTO %s (id, addrs) VALUES (1, [{city: 'Rome'}, {zip: 1}])");
        execute("INSERT INTO %s (id, addrs) VALUES (2, [{zip: 1}])");
        assertRows(execute("SELECT id FROM %s WHERE addrs.city IS NOT NULL"), row(1));
        execute("DELETE FROM %s WHERE id IN (1, 2)");
        // the same element: CONTAINS ELEMENT (P4, see UdtElementMatchQueryTest)
        assertEmpty(execute("SELECT id FROM %s WHERE addrs CONTAINS ELEMENT (city = 'Rome')"));
        // only in SELECT
        assertInvalidMessage(String.format(StatementRestrictions.FIELD_RESTRICTIONS_ONLY_IN_SELECT_MESSAGE, "addrs.city = 'a'"),
                             "DELETE FROM %s WHERE id = 1 AND addrs.city = 'a'");
        // null values
        assertInvalidMessage("Unsupported null value for field addrs.city", "SELECT id FROM %s WHERE addrs.city = ?", (Object) null);
    }
}
