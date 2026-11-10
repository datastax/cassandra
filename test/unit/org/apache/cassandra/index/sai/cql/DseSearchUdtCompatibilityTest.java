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

import java.util.Arrays;
import java.util.Collection;

import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.index.sai.SAITester;

/**
 * The DSE Search queries on the fields of UDT and tuple columns ({@code solr_query} with the tuple query parser,
 * {@code {!tuple}}) and their CQL counterparts on SAI indexes on the fields (HCD-653).
 * <p>
 * The first part mirrors the scenarios of the DSE Search functional tests ({@code Cql3TupleTest},
 * {@code FrozenCollectionsTest}, {@code NativeCQLSolrQueryTest}) on the schema of {@code Cql3TupleTest}; the second
 * part mirrors the queries of the reference run of DSE 6.9.26 ({@code docs/sai-udt/dse-runtime.md}) on its tables,
 * including the cases where SAI does more than DSE Search (conditions at different nesting levels on the same
 * element, maps of UDTs) and those DSE Search refuses (sorting and faceting on fields). Each CQL query is given with
 * the DSE {@code solr_query} it mirrors.
 * <p>
 * The queries on the indexed table are also run with {@code ALLOW FILTERING} on a twin table without any index, which
 * must give the same rows, except the queries that only an index can serve ({@code LIKE}, normalizing indexes). The
 * checks run with both tables in the memtables, with the indexed table in the memtable and the twin table flushed,
 * after a flush of both and after a compaction, on frozen and non-frozen UDT and collection columns (tuples are
 * always frozen).
 */
@RunWith(Parameterized.class)
public class DseSearchUdtCompatibilityTest extends SAITester
{
    @Parameterized.Parameter
    public boolean frozen;

    @Parameterized.Parameters(name = "frozen={0}")
    public static Collection<Object[]> data()
    {
        return Arrays.asList(new Object[]{ true }, new Object[]{ false });
    }

    private static final String TIME = "1970-01-17T15:17:11.014Z";
    private static final String LATER_TIME = "2000-01-01T00:00:00.000Z";

    /** The twin table, without indexes, of the current (indexed) table. */
    private String plain;

    private String udt(String type)
    {
        return frozen ? "frozen<" + type + '>' : type;
    }

    private String collection(String kind, String element)
    {
        String type = kind + '<' + element + '>';
        return frozen ? "frozen<" + type + '>' : type;
    }

    /**
     * Skips the second run of a test whose schema doesn't depend on the parameter.
     */
    private void runOnce()
    {
        Assume.assumeTrue("the schema doesn't depend on the parameter: run once", frozen);
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

    /**
     * Runs the checks with both tables in the memtables, then with the twin table flushed (the indexed table still in
     * its memtable), then after a flush of both, then after a compaction.
     */
    private void inAllStates(CheckedFunction check) throws Throwable
    {
        check.apply();
        flush(KEYSPACE, plain);
        beforeAndAfterFlush(check);
        compact(KEYSPACE, currentTable(), plain);
        check.apply();
    }

    /**
     * Checks that a query on the indexed table returns the rows with the given ids, without ALLOW FILTERING if it
     * isn't in the query (it's served by the indexes), and that the same query with ALLOW FILTERING on the table
     * without indexes returns the same rows.
     */
    private void assertIds(String where, Object... ids)
    {
        assertIndexedIds(where, ids);
        assertRowsIgnoringOrder(execute(filtered("SELECT id FROM " + KEYSPACE + '.' + plain + " WHERE " + where)), rows(ids));
    }

    /**
     * Checks the query on the indexed table only, for the queries that only an index can serve (LIKE, normalizing
     * indexes).
     */
    private void assertIndexedIds(String where, Object... ids)
    {
        assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE " + where), rows(ids));
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

    // The schema of Cql3TupleTest (helper init, collection section C6.1), with these differences:
    // - the key is an int (a text in DSE);
    // - the text columns body, date and title are omitted (only name is kept), and so is the column
    //   tuplewithtuplecollectionwithcollection (tuplewithtuplecollection with text: the same paths);
    // - listfield and setfield are frozen (DSE makes them non-frozen in the non-frozen mode, which CQL doesn't allow
    //   in a non-frozen UDT);
    // - the collections inside tuples are frozen (as CQL requires).

    /**
     * Creates the tables with the schema of {@code Cql3TupleTest}, the indexes on the fields its queries use, and
     * inserts its rows: 1 and 2 with all the columns, 3 with Sam in {@code direc.name} and in {@code udtcol_list}
     * (the row that {@code testBlockJoins} adds), 4 without UDT and tuple values.
     */
    private void createCql3TupleTestTables()
    {
        String fullname = createType("CREATE TYPE %s (firstname text, lastname text, hobbies frozen<list<text>>)");
        String address = createType("CREATE TYPE %s (street text, city text, zip int, time timestamp, empty_field text, " +
                                    "listfield frozen<list<text>>, setfield frozen<set<text>>, mapfield frozen<map<text, text>>, " +
                                    "name frozen<" + fullname + ">, names frozen<list<frozen<" + fullname + ">>>)");
        createTables("CREATE TABLE %s (id int PRIMARY KEY, name text, " +
                     "direc " + udt(address) + ", " +
                     "udtcol_list " + collection("list", "frozen<" + address + '>') + ", " +
                     "udtcol_set " + collection("set", "frozen<" + address + '>') + ", " +
                     "tuplecolumn tuple<int, text, float, frozen<map<text, text>>, timestamp, text>, " +
                     "tuplecollection_list " + collection("list", "frozen<tuple<int, text, float>>") + ", " +
                     "tuplecollection_set " + collection("set", "frozen<tuple<int, text, float>>") + ", " +
                     "tuplewithcollection tuple<int, text, frozen<list<text>>>, " +
                     "nestedtuple tuple<int, text, frozen<tuple<int>>>, " +
                     "tuplewithtuplecollection tuple<int, text, frozen<list<frozen<tuple<frozen<list<int>>>>>>>)");

        // CREATE SEARCH INDEX ... (all the leaves by default): one index per leaf used by the queries
        for (String target : new String[]{ "direc.city", "direc.zip", "direc.time", "direc.empty_field", "direc.listfield",
                                           "direc.setfield", "direc.mapfield", "direc.name.firstname", "direc.names.firstname",
                                           "direc.names.hobbies", "udtcol_list.street", "udtcol_list.city",
                                           "udtcol_list.name.firstname", "udtcol_set.street", "tuplecolumn.field1",
                                           "tuplecolumn.field2", "tuplecolumn.field3", "tuplecolumn.field5", "tuplecolumn.field6",
                                           "tuplecollection_list.field1", "tuplecollection_set.field1", "tuplewithcollection.field3",
                                           "nestedtuple.field3.field1", "tuplewithtuplecollection.field3.field1" })
            index(target);

        write("INSERT INTO %s (id, name, direc, udtcol_list, udtcol_set, tuplecolumn, tuplecollection_list, tuplecollection_set, " +
              "tuplewithcollection, nestedtuple, tuplewithtuplecollection) VALUES (1, 'n1', " +
              "{street: 'st1', city: 'bcn', zip: 1, time: '" + TIME + "', empty_field: '', listfield: ['listval1', 'listval2'], " +
              "setfield: {'setval1'}, mapfield: {'key1': 'dynVal1', 'key2': 'dynVal2'}, " +
              "name: {firstname: 'Bilbo', lastname: 'Baggins', hobbies: ['swim1']}, " +
              "names: [{firstname: 'Bilbo', lastname: 'Baggins', hobbies: ['swim1', 'run1']}, {firstname: 'Frodo', lastname: 'Baggins', hobbies: ['walk1']}]}, " +
              "[{street: 'l1', city: 'bcn', zip: 1, name: {firstname: 'Bilbo'}}, {street: 'l2', city: 'mad', zip: 2, name: {firstname: 'Frodo'}}], " +
              "{{street: 's1', city: 'bcn', zip: 3}}, " +
              "(1, 'text', 1.5, {'k1': 'v1'}, '" + TIME + "', ''), " +
              "[(3, 'a', 1.5), (4, 'b', 2.5)], " +
              "{(3, 'a', 1.5)}, " +
              "(1, 'text', ['text1', 'text2']), " +
              "(1, 'a', (6)), " +
              "(1, 'a', [([9, 10]), ([11])]))");
        write("INSERT INTO %s (id, name, direc, udtcol_list, udtcol_set, tuplecolumn, tuplecollection_list, tuplecollection_set, " +
              "tuplewithcollection, nestedtuple, tuplewithtuplecollection) VALUES (2, 'n2', " +
              "{street: 'st2', city: 'mad', zip: 2, time: '" + LATER_TIME + "', empty_field: 'x', listfield: ['listval3'], " +
              "setfield: {'setval2'}, mapfield: {'key2': 'dynVal3', 'key3': 'dynVal2'}, " +
              "name: {firstname: 'Sam', lastname: 'Gamgee'}, " +
              "names: [{firstname: 'Sam', lastname: 'Gamgee', hobbies: ['swim2']}]}, " +
              "[{street: 'l3', city: 'mad', zip: 5, name: {firstname: 'Frodo'}}], " +
              "{{city: 'mad', zip: 4}}, " +
              "(2, 'text', 2.5, null, '" + LATER_TIME + "', 'y'), " +
              "[(5, 'c', 3.5)], " +
              "{(3, 'b', 2.5)}, " +
              "(2, 'text', ['text3']), " +
              "(2, 'b', (7)), " +
              "(2, 'b', [([12])]))");
        write("INSERT INTO %s (id, name, direc, udtcol_list, tuplecolumn, tuplewithcollection) VALUES (3, 'n3', " +
              "{city: 'bcn', zip: 3, name: {firstname: 'Sam'}}, " +
              "[{city: 'bcn', zip: 6, name: {firstname: 'Sam'}}], " +
              "(3, 'other', null, null, null, null), " +
              "(3, 'text3', ['text4']))");
        write("INSERT INTO %s (id, name) VALUES (4, 'n4')");
    }

    /**
     * {@code Cql3TupleTest.testSubFieldSearch}: term queries on the fields of a UDT and of a tuple.
     */
    @Test
    public void testSubFieldSearch() throws Throwable
    {
        createCql3TupleTestTables();
        inAllStates(() -> {
            assertIds("direc.zip = 1", 1);                    // {!tuple}direc.zip:1
            assertIds("direc.zip = 2", 2);                    // {!tuple}direc.zip:2
            assertIds("direc.city = 'bcn'", 1, 3);            // {!tuple}direc.city:bcn
            assertIds("tuplecolumn.field1 = 1", 1);           // {!tuple}tuplecolumn.field1:1
            assertIds("tuplecolumn.field3 = 1.5", 1);         // {!tuple}tuplecolumn.field3:1.5
            assertIds("direc.zip IS NOT NULL", 1, 2, 3);      // {!tuple}direc.zip:*
            // the returned row holds the matching field (DSE: the JSON string of the UDT holds the zip)
            assertRowsIgnoringOrder(execute("SELECT id, direc.zip FROM %s WHERE direc.city = 'bcn'"), row(1, 1), row(3, 3));
            // SELECT * FROM t WHERE solr_query='{!tuple}direc.city:bcn'
            assertRowsIgnoringOrder(execute("SELECT id, name FROM %s WHERE direc.city = 'bcn'"), row(1, "n1"), row(3, "n3"));
            // testSelectJson: SELECT JSON * FROM t WHERE solr_query='{!tuple}direc.city:bcn'
            assertRowsIgnoringOrder(execute("SELECT JSON id FROM %s WHERE direc.city = 'bcn'"), row("{\"id\": 1}"), row("{\"id\": 3}"));
        });
    }

    /**
     * {@code Cql3TupleTest.testNestedUDTSearch}: a UDT inside a UDT, a tuple inside a tuple.
     */
    @Test
    public void testNestedUDTSearch() throws Throwable
    {
        createCql3TupleTestTables();
        inAllStates(() -> {
            assertIds("direc.name.firstname = 'Bilbo'", 1);   // {!tuple}direc.name.firstname:Bilbo
            assertIds("nestedtuple.field3.field1 = 6", 1);    // {!tuple}nestedtuple.field3.field1:6
            assertIds("nestedtuple.field3.field1 >= 6", 1, 2); // {!tuple}nestedtuple.field3.field1:[6 TO *]
            // the nested value comes back with the row (DSE: the JSON holds the nested object)
            assertRows(execute("SELECT direc.name.lastname FROM %s WHERE direc.name.firstname = 'Bilbo'"), row("Baggins"));
        });
    }

    /**
     * {@code Cql3TupleTest.testCollectionsInsideUDTSearch}: lists and sets inside a UDT, a list inside a tuple.
     */
    @Test
    public void testCollectionsInsideUDTSearch() throws Throwable
    {
        createCql3TupleTestTables();
        inAllStates(() -> {
            assertIds("direc.listfield CONTAINS 'listval1'", 1);           // {!tuple}direc.listfield:listval1
            assertIds("direc.setfield CONTAINS 'setval1'", 1);             // {!tuple}direc.setfield:setval1
            assertIds("tuplewithcollection.field3 CONTAINS 'text1'", 1);   // {!tuple}tuplewithcollection.field3:text1
            // {!tuple}tuplewithcollection.field3:(text1 OR text3)
            assertIds("tuplewithcollection.field3 CONTAINS 'text1' OR tuplewithcollection.field3 CONTAINS 'text3'", 1, 2);
            assertIds("direc.listfield NOT CONTAINS 'listval1'", 2, 3, 4); // -{!tuple}direc.listfield:listval1
        });
    }

    /**
     * {@code Cql3TupleTest.testDynFieldSearch}: the dynamic fields of a map inside a UDT. DSE indexes each entry of
     * {@code mapfield} as a field named after the key ({@code direc.mapfieldkey2}); SAI has no map-entry restriction on
     * fields, the values of the map can be searched with {@code CONTAINS} (any key).
     */
    @Test
    public void testDynFieldSearch() throws Throwable
    {
        createCql3TupleTestTables();
        inAllStates(() -> {
            // {!tuple}direc.mapfieldkey2:dynVal2 selects row 1 only: CONTAINS also matches row 2, where dynVal2 is
            // the value of key3
            assertIds("direc.mapfield CONTAINS 'dynVal2'", 1, 2);
            assertIds("direc.mapfield CONTAINS 'dynVal3'", 2);
        });
        // no map-entry restriction on a field, and no CONTAINS KEY on fields
        assertInvalidSyntax("SELECT id FROM %s WHERE direc.mapfield['key2'] = 'dynVal2' ALLOW FILTERING");
        assertInvalidMessage("is not supported on fields: direc.mapfield",
                             "SELECT id FROM %s WHERE direc.mapfield CONTAINS KEY 'key2' ALLOW FILTERING");
    }

    /**
     * {@code Cql3TupleTest.testUDTCollectionInsideUDTSearch}: a list of UDTs inside a UDT, a list of tuples inside a
     * tuple.
     */
    @Test
    public void testUDTCollectionInsideUDTSearch() throws Throwable
    {
        createCql3TupleTestTables();
        inAllStates(() -> {
            assertIds("direc.names.firstname IS NOT NULL", 1, 2);        // {!tuple}direc.names.firstname:*
            assertIds("direc.names.firstname = 'Bilbo'", 1);             // {!tuple}direc.names.firstname:Bilbo
            assertIds("direc.names.firstname = 'Frodo'", 1);             // {!tuple}direc.names.firstname:Frodo
            assertIds("tuplewithtuplecollection.field3.field1 CONTAINS 9", 1); // {!tuple}tuplewithtuplecollection.field3.field1:9
        });
    }

    /**
     * {@code Cql3TupleTest.testCollectionsInsideNestedUDTCollectionsSearch}: collections inside the UDTs and tuples
     * that are inside collections.
     */
    @Test
    public void testCollectionsInsideNestedUDTCollectionsSearch() throws Throwable
    {
        createCql3TupleTestTables();
        inAllStates(() -> {
            assertIds("direc.names.hobbies CONTAINS 'swim1'", 1);        // {!tuple}direc.names.hobbies:swim1
            assertIds("direc.names.hobbies CONTAINS 'swim2'", 2);        // {!tuple}direc.names.hobbies:swim2
            assertIds("direc.names.hobbies CONTAINS 'walk1'", 1);        // {!tuple}direc.names.hobbies:walk1
            // {!tuple}direc.names.firstname:Bilbo AND direc.names.hobbies:swim1 (the same element)
            assertIds("direc.names CONTAINS ELEMENT (firstname = 'Bilbo' AND hobbies CONTAINS 'swim1')", 1);
            assertIds("direc.names CONTAINS ELEMENT (firstname = 'Frodo' AND hobbies CONTAINS 'swim1')");
            // {"q": "{!tuple}direc.names.firstname:Frodo", "fq": "{!tuple}direc.names.hobbies:swim1"} (any element each)
            assertIds("direc.names.firstname = 'Frodo' AND direc.names.hobbies CONTAINS 'swim1'", 1);

            // the tuple counterpart: lists of int inside the tuples of a list inside a tuple
            assertIds("tuplewithtuplecollection.field3.field1 CONTAINS 12", 2); // {!tuple}tuplewithtuplecollection.field3.field1:12
            // {!tuple}tuplewithtuplecollection.field3.field1:9 AND tuplewithtuplecollection.field3.field1:10 (same element)
            assertIds("tuplewithtuplecollection.field3 CONTAINS ELEMENT (field1 CONTAINS 9 AND field1 CONTAINS 10)", 1);
            assertIds("tuplewithtuplecollection.field3 CONTAINS ELEMENT (field1 CONTAINS 9 AND field1 CONTAINS 11)");
            assertIds("tuplewithtuplecollection.field3.field1 CONTAINS 9 AND tuplewithtuplecollection.field3.field1 CONTAINS 11", 1);
        });
    }

    /**
     * {@code Cql3TupleTest.testCollectionsOfUDTSSearch}: lists and sets of UDTs and of tuples.
     */
    @Test
    public void testCollectionsOfUDTSSearch() throws Throwable
    {
        createCql3TupleTestTables();
        inAllStates(() -> {
            assertIds("udtcol_list.street IS NOT NULL", 1, 2);       // {!tuple}udtcol_list.street:*
            assertIds("udtcol_set.street IS NOT NULL", 1);           // {!tuple}udtcol_set.street:*
            assertIds("tuplecollection_list.field1 = 3", 1);         // {!tuple}tuplecollection_list.field1:3
            assertIds("tuplecollection_set.field1 = 3", 1, 2);       // {!tuple}tuplecollection_set.field1:3
            assertIds("udtcol_list.city = 'mad'", 1, 2);             // {!tuple}udtcol_list.city:mad
            assertIds("tuplecollection_list.field1 >= 4", 1, 2);     // {!tuple}tuplecollection_list.field1:[4 TO *]
            // -{!tuple}udtcol_list.street:* (no element with a street, including the rows without elements)
            assertIds("udtcol_list NOT CONTAINS ELEMENT (street IS NOT NULL) ALLOW FILTERING", 3, 4);
        });
    }

    /**
     * {@code Cql3TupleTest.testBlockJoins}: OR and AND across and inside the tuple queries. Across clauses (and
     * columns) they are the ordinary CQL AND / OR; several conditions on the same element of a collection are a
     * {@code CONTAINS ELEMENT} group.
     */
    @Test
    public void testBlockJoins() throws Throwable
    {
        createCql3TupleTestTables();
        inAllStates(() -> {
            assertIds("direc.name.firstname IN ('Bilbo', 'Sam')", 1, 2, 3);           // {!tuple}direc.name.firstname:(Bilbo OR Sam)
            assertIds("udtcol_list.name.firstname IN ('Bilbo', 'Sam')", 1, 3);        // {!tuple}udtcol_list.name.firstname:(Bilbo OR Sam)
            // ({!tuple}direc.name.firstname:Bilbo OR {!tuple}direc.name.firstname:Sam), and the same with
            // {!tuple v='direc.name.firstname:Bilbo OR direc.name.firstname:Sam'}
            assertIds("direc.name.firstname = 'Bilbo' OR direc.name.firstname = 'Sam'", 1, 2, 3);
            // ({!tuple}direc.name.firstname:Sam AND {!tuple}udtcol_list.name.firstname:Sam), and the same with
            // +{!tuple v='direc.name.firstname:Sam'} +{!tuple v='udtcol_list.name.firstname:Sam'}
            assertIds("direc.name.firstname = 'Sam' AND udtcol_list.name.firstname = 'Sam'", 3);
            assertIds("direc.name.firstname = 'Sam' AND udtcol_list.name.firstname = 'Bilbo'");
            // ({!tuple}tuplewithcollection.field3:text3 AND {!tuple}tuplecolumn.field2:text)
            assertIds("tuplewithcollection.field3 CONTAINS 'text3' AND tuplecolumn.field2 = 'text'", 2);
            assertIds("tuplewithcollection.field3 CONTAINS 'text3' AND tuplecolumn.field2 = 'text3'");
            // an OR across columns: ({!tuple}direc.name.firstname:Bilbo OR {!tuple}tuplecolumn.field2:other)
            assertIds("direc.name.firstname = 'Bilbo' OR tuplecolumn.field2 = 'other'", 1, 3);

            // the same element: {!tuple}udtcol_list.city:bcn AND udtcol_list.name.firstname:Bilbo
            assertIds("udtcol_list CONTAINS ELEMENT (city = 'bcn' AND name.firstname = 'Bilbo')", 1);
            assertIds("udtcol_list CONTAINS ELEMENT (city = 'mad' AND name.firstname = 'Bilbo')");
            // {"q": "{!tuple}udtcol_list.city:mad", "fq": "{!tuple}udtcol_list.name.firstname:Bilbo"} (any element each)
            assertIds("udtcol_list.city = 'mad' AND udtcol_list.name.firstname = 'Bilbo'", 1);
            // {!tuple}udtcol_list.city:mad OR udtcol_list.name.firstname:Sam (within the element)
            assertIds("udtcol_list CONTAINS ELEMENT (city = 'mad' OR name.firstname = 'Sam')", 1, 2, 3);
            // {!tuple}udtcol_list.city:bcn AND NOT udtcol_list.name.firstname:Bilbo (within the element)
            assertIds("udtcol_list CONTAINS ELEMENT (city = 'bcn' AND name.firstname != 'Bilbo')", 3);
        });
    }

    /**
     * {@code Cql3TupleTest.testEmptySubFieldSearch}: DSE doesn't index the empty strings, so
     * {@code {!tuple}_parent_:F AND -direc.empty_field:[* TO *]} (the rows without a value in the field) returns all
     * the rows. SAI indexes the empty strings of the text fields as values: {@code = ''} finds them and
     * {@code IS NOT NULL} includes them. The rows without a value can't be selected: CQL has no {@code IS NULL}
     * restriction (on fields inside collections, {@code NOT CONTAINS ELEMENT (field IS NOT NULL)} selects the rows
     * where no element has the field).
     */
    @Test
    public void testEmptySubFieldSearch() throws Throwable
    {
        createCql3TupleTestTables();
        inAllStates(() -> {
            assertIds("direc.empty_field = ''", 1);
            assertIds("direc.empty_field IS NOT NULL", 1, 2);      // {!tuple}direc.empty_field:[* TO *] (row 2 only in DSE)
            assertIds("tuplecolumn.field6 = ''", 1);
            assertIds("tuplecolumn.field6 IS NOT NULL", 1, 2);     // {!tuple}tuplecolumn.field6:[* TO *] (row 2 only in DSE)
            // the empty value comes back as an empty text, as in DSE
            assertRows(execute("SELECT direc.empty_field FROM %s WHERE direc.empty_field = ''"), row(""));
        });
        // {!tuple}_parent_:F AND -direc.empty_field:[* TO *]: no IS NULL
        assertInvalidSyntaxMessage("IS NULL restrictions are not supported",
                                   "SELECT id FROM %s WHERE direc.empty_field IS NULL ALLOW FILTERING");
        assertInvalidSyntaxMessage("IS NULL restrictions are not supported",
                                   "SELECT id FROM %s WHERE tuplecolumn.field6 IS NULL ALLOW FILTERING");
    }

    /**
     * {@code Cql3TupleTest.testDateSubFieldSearch}: timestamps in a UDT and in a tuple.
     */
    @Test
    public void testDateSubFieldSearch() throws Throwable
    {
        createCql3TupleTestTables();
        inAllStates(() -> {
            assertIds("direc.time = '" + TIME + "'", 1);           // {!tuple}direc.time:"1970-01-17T15:17:11.014Z"
            assertIds("tuplecolumn.field5 = '" + TIME + "'", 1);   // {!tuple}tuplecolumn.field5:"1970-01-17T15:17:11.014Z"
            assertIds("direc.time > '" + TIME + "'", 2);           // {!tuple}direc.time:{"1970-01-17T15:17:11.014Z" TO *]
            assertIds("tuplecolumn.field5 <= '" + LATER_TIME + "'", 1, 2); // {!tuple}tuplecolumn.field5:[* TO "2000-01-01T00:00:00.000Z"]
        });
    }

    /**
     * {@code Cql3TupleTest.testTupleExplicitlyUnsupportedFeatures}: DSE refuses sorting ("Sorting on Tuple fields is
     * not supported") and faceting ("Faceting on Tuple fields is not supported") on the fields; CQL has no field in
     * {@code ORDER BY}, and {@code GROUP BY} refuses the fields of columns.
     */
    @Test
    public void testTupleExplicitlyUnsupportedFeatures() throws Throwable
    {
        createCql3TupleTestTables();
        // {"q": "{!tuple}direc.city:bcn", "sort": "direc.city asc"}
        assertInvalidSyntax("SELECT id FROM %s WHERE direc.city = 'bcn' ORDER BY direc.city LIMIT 10");
        // {"q": "id:*", "facet": {"field": "direc.city"}}
        assertInvalidMessage("Group by is not supported on the fields of columns, got direc.city",
                             "SELECT direc.city, count(*) FROM %s GROUP BY direc.city");
        // the facet queries of the same test work: {"q": "*:*", "facet": {"query": "{!tuple}direc.zip:[0 TO 5]"}}
        assertRows(execute("SELECT count(*) FROM %s WHERE direc.zip >= 0 AND direc.zip <= 5"), row(3L));
        assertRows(execute("SELECT count(*) FROM %s WHERE direc.zip >= 5"), row(0L));
    }

    /**
     * {@code FrozenCollectionsTest.shouldIndexFrozenCollectionOfUserDefinedType} and
     * {@code shouldIndexEmptyFrozenCollectionOfUserDefinedType}: frozen lists and sets of UDTs, with elements and
     * empty.
     */
    @Test
    public void testFrozenCollectionsOfUserDefinedType() throws Throwable
    {
        runOnce();
        String name = createType("CREATE TYPE %s (first text, last text)");
        createTables("CREATE TABLE %s (id int PRIMARY KEY, names frozen<list<frozen<" + name + ">>>, " +
                     "nameset frozen<set<frozen<" + name + ">>>, before int, after int)");
        index("names.first");
        index("names.last");
        index("nameset.first");
        index("before");
        index("after");
        write("INSERT INTO %s (id, names, nameset, before, after) VALUES (1, " +
              "[{first: 'foo', last: 'bar'}, {first: 'John', last: 'Smith'}], {{first: 'foo', last: 'bar'}, {first: 'John', last: 'Smith'}}, 0, 0)");
        write("INSERT INTO %s (id, names, nameset, before, after) VALUES (2, [], {}, 1, 1)");

        inAllStates(() -> {
            assertIds("names.first = 'foo'", 1);        // {!tuple}names.first:foo
            assertIds("names.last = 'bar'", 1);         // {!tuple}names.last:bar
            assertIds("names.first = 'John'", 1);       // {!tuple}names.first:John
            assertIds("names.last = 'Smith'", 1);       // {!tuple}names.last:Smith
            assertIds("nameset.first = 'John'", 1);     // {!tuple}nameset.first:John
            assertIds("before = 0 AND after = 0", 1);   // before:0 AND after:0
            // the same element, and independent elements
            assertIds("names CONTAINS ELEMENT (first = 'John' AND last = 'Smith')", 1);
            assertIds("names CONTAINS ELEMENT (first = 'John' AND last = 'bar')");
            assertIds("names.first = 'John' AND names.last = 'bar'", 1);

            // the empty frozen collections: the row is there (id:*, id IS NOT NULL), and it has no element
            assertRows(execute("SELECT id FROM %s WHERE id = 2"), row(2));
            assertIds("names.first IS NOT NULL", 1);
            assertIds("names NOT CONTAINS ELEMENT (first IS NOT NULL) ALLOW FILTERING", 2);
            assertIds("nameset NOT CONTAINS ELEMENT (first = 'foo')", 2);
        });
    }

    /**
     * {@code NativeCQLSolrQueryTest.testUDTEQ} and {@code testTupleEQ}: the equality with a whole frozen UDT or tuple
     * value, served by an index on the whole column (as by the DSE search index); a partial value doesn't match (the
     * other fields are null), an empty field value matches. The fields have their own indexes.
     */
    @Test
    public void testUDTEQ() throws Throwable
    {
        runOnce();
        String skill = createType("CREATE TYPE %s (name text, type text)");
        createTables("CREATE TABLE %s (id int PRIMARY KEY, skill frozen<" + skill + ">, tskill tuple<text, text>)");
        index("skill");
        index("tskill");
        index("skill.name");
        index("tskill.field2");
        write("INSERT INTO %s (id, skill, tskill) VALUES (1, {name: 'masonry', type: 'bricks'}, ('masonry', 'bricks'))");
        write("INSERT INTO %s (id, skill, tskill) VALUES (2, {name: 'masonry', type: ''}, ('masonry', ''))");
        write("INSERT INTO %s (id, skill, tskill) VALUES (3, {name: 'gardening', type: 'roses'}, ('gardening', 'roses'))");

        inAllStates(() -> {
            assertIds("skill = {name: 'masonry', type: 'bricks'}", 1);   // WHERE skill={name:'masonry',type:'bricks'}
            assertIds("skill = {name: 'masonry'}");                      // partial value
            assertIds("skill = {name: 'masonry', type: ''}", 2);         // empty field value
            assertIds("tskill = ('masonry', 'bricks')", 1);              // WHERE skill=('masonry','bricks')
            assertIds("tskill = ('masonry', '')", 2);
            // the fields: {!tuple}skill.name:masonry
            assertIds("skill.name = 'masonry'", 1, 2);
            assertIds("tskill.field2 = 'roses'", 3);
            assertIds("skill.name = 'masonry' AND skill = {name: 'masonry', type: 'bricks'}", 1);
        });
    }

    /**
     * {@code NativeCQLSolrQueryTest.testUDTWithCollectionFails}, {@code testUDTWithNestedTupleFails},
     * {@code testTupleWithCollectionFails} and {@code testTupleWithNestedTupleFails}: DSE refuses the equality with a
     * whole UDT or tuple value that has a collection or a nested UDT or tuple; SAI serves it with an index on the
     * whole column (more than DSE).
     */
    @Test
    public void testWholeValueEqualityWithCollectionsAndNesting() throws Throwable
    {
        runOnce();
        String skill = createType("CREATE TYPE %s (name text, tags frozen<set<text>>)");
        String nested = createType("CREATE TYPE %s (name text, inner frozen<" + skill + ">)");
        createTables("CREATE TABLE %s (id int PRIMARY KEY, s frozen<" + skill + ">, n frozen<" + nested + ">, " +
                     "ts tuple<text, frozen<list<int>>>, nt tuple<text, frozen<tuple<int, int>>>)");
        index("s");
        index("n");
        index("ts");
        index("nt");
        write("INSERT INTO %s (id, s, n, ts, nt) VALUES (1, {name: 'masonry', tags: {'a'}}, " +
              "{name: 'x', inner: {name: 'masonry', tags: {'a'}}}, ('masonry', [1, 2]), ('masonry', (1, 2)))");
        write("INSERT INTO %s (id, s, n, ts, nt) VALUES (2, {name: 'masonry', tags: {'b'}}, " +
              "{name: 'x', inner: {name: 'masonry'}}, ('masonry', [2]), ('masonry', (2, 1)))");

        inAllStates(() -> {
            assertIds("s = {name: 'masonry', tags: {'a'}}", 1);
            assertIds("n = {name: 'x', inner: {name: 'masonry'}}", 2);
            assertIds("ts = ('masonry', [1, 2])", 1);
            assertIds("nt = ('masonry', (2, 1))", 2);
        });
    }

    /**
     * {@code Cql3TupleTest.testNonFrozenTupleSubFieldProjection} and {@code testNonFrozenUDTWithNULLValues}: the
     * selection of fields, also with mixed-case quoted names, and the fields without a value. DSE rejects aliases on
     * the selected fields; CQL accepts them.
     */
    @Test
    public void testSubFieldProjectionAndNullFields() throws Throwable
    {
        String fullname = createType("CREATE TYPE %s (lastname text, \"firstName\" text)");
        createTables("CREATE TABLE %s (id int PRIMARY KEY, name " + udt(fullname) + ')');
        index("name.\"firstName\"");
        index("name.lastname");
        write("INSERT INTO %s (id, name) VALUES (1, {lastname: 'ln_0', \"firstName\": 'Fn_0'})");
        write("INSERT INTO %s (id, name) VALUES (2, {lastname: null, \"firstName\": 'Fn_1'})");
        write("INSERT INTO %s (id, name) VALUES (3, {lastname: 'ln_2'})");
        write("INSERT INTO %s (id, name) VALUES (4, null)");

        inAllStates(() -> {
            // SELECT name.lastname, name."firstName" FROM t WHERE solr_query='{!tuple}name.firstName:Fn_0'
            assertRows(execute("SELECT name.lastname, name.\"firstName\" FROM %s WHERE name.\"firstName\" = 'Fn_0'"), row("ln_0", "Fn_0"));
            assertRows(execute("SELECT name.lastname AS l FROM %s WHERE name.\"firstName\" = 'Fn_0'"), row("ln_0"));
            // the fields without a value: {!tuple}name.lastname:*, {!tuple}name.firstName:*
            assertIds("name.lastname IS NOT NULL", 1, 3);
            assertIds("name.\"firstName\" IS NOT NULL", 1, 2);
            assertIds("name.lastname = 'ln_2'", 3);
            assertRowsIgnoringOrder(execute("SELECT id, name.lastname FROM %s WHERE name.\"firstName\" >= 'Fn'"), row(1, "ln_0"), row(2, null));
        });
        // the field names are case-sensitive when quoted
        assertInvalidMessage("Undefined field firstname", "SELECT id FROM %s WHERE name.firstName = 'Fn_0' ALLOW FILTERING");
    }

    /**
     * {@code Cql3TupleTest.testAddingTupleDoesntCauseDuplicates}, {@code testDeleteByQuery*} and
     * {@code Cql3SolrTTLTest.testNonFrozenTuplesSubFieldsTTL}: a UDT value added to a row and then changed, a row
     * deleted, and a field that expires.
     */
    @Test
    public void testUpdatesDeletesAndTtl() throws Throwable
    {
        String fullname = createType("CREATE TYPE %s (firstname text, lastname text)");
        createTables("CREATE TABLE %s (id int PRIMARY KEY, v int, name " + udt(fullname) + ')');
        index("name.firstname");
        index("name.lastname");
        // an insert without the UDT, then the UDT added, then changed: one row each time
        write("INSERT INTO %s (id, v) VALUES (1, 1)");
        assertIds("name.firstname IS NOT NULL");
        write("UPDATE %s SET name = {firstname: 'fn_0', lastname: 'ln_0'} WHERE id = 1");
        assertIds("name.firstname IS NOT NULL", 1);                 // {!tuple}name.firstname:* (numFound 1)
        flushBoth();
        write("UPDATE %s SET name = {firstname: 'fn_1', lastname: 'ln_0'} WHERE id = 1");
        // the rows to delete (delete by query id:2) and the expiring field
        write("INSERT INTO %s (id, name) VALUES (2, {firstname: 'fn_0', lastname: 'ln_2'})");
        write("INSERT INTO %s (id, name) VALUES (3, {firstname: 'fn_3', lastname: 'ln_3'})");
        flushBoth();
        write("DELETE FROM %s WHERE id = 2");
        if (frozen)
            write("UPDATE %s USING TTL 1 SET name = {firstname: 'fn_3', lastname: 'ln_ttl'} WHERE id = 3");
        else
            write("UPDATE %s USING TTL 1 SET name.lastname = 'ln_ttl' WHERE id = 3");
        Thread.sleep(2100);

        inAllStates(() -> {
            assertIds("name.firstname = 'fn_0'");                       // {!tuple}name.firstname:fn_0 (changed, and deleted)
            assertIds("name.firstname = 'fn_1'", 1);
            assertIds("name.lastname = 'ln_0'", 1);
            assertIds("name.lastname = 'ln_2'");                        // the deleted row
            assertIds("name.lastname = 'ln_ttl'");                      // {!tuple}name.lastname:ln_ttl after the TTL
            // the expired frozen value leaves no field; the expired field of a non-frozen UDT leaves the others
            if (frozen)
                assertIds("name.firstname IS NOT NULL", 1);
            else
                assertIds("name.firstname IS NOT NULL", 1, 3);
            assertRows(execute("SELECT count(*) FROM %s WHERE name.firstname IS NOT NULL"), row(frozen ? 1L : 2L));
        });
    }

    // The tables of the DSE 6.9.26 reference run (docs/sai-udt/dse-runtime.md)

    private String geo;
    private String address;

    private void createRuntimeTypes()
    {
        geo = createType("CREATE TYPE %s (lat double, lon double)");
        address = createType("CREATE TYPE %s (street text, city text, zip int, loc frozen<" + geo + ">, tags set<text>)");
    }

    private static final String ROME = "{street: 'a', city: 'Rome', zip: 200, loc: {lat: 1.0, lon: 0.5}, tags: {'x', 'y'}}";
    private static final String PARIS = "{street: 'b', city: 'Paris', zip: 100}";

    /**
     * u_frozen: u_frozen 1 = {a, Rome, 200, lat 1}, 2 = {b, Paris, 100, no loc}.
     */
    @Test
    public void testFrozenUdt() throws Throwable
    {
        runOnce();
        createRuntimeTypes();
        createTables("CREATE TABLE %s (id int PRIMARY KEY, addr frozen<" + address + ">)");
        index("addr.city");
        index("addr.street");
        index("addr.zip");
        index("addr.loc.lat");
        index("addr.tags");
        write("INSERT INTO %s (id, addr) VALUES (1, " + ROME + ')');
        write("INSERT INTO %s (id, addr) VALUES (2, " + PARIS + ')');

        inAllStates(() -> {
            assertIds("addr.city = 'Rome'", 1);                    // {!tuple}addr.city:Rome
            assertIds("addr.loc.lat = 1.0", 1);                    // {!tuple}addr.loc.lat:1.0
            assertIndexedIds("addr.street LIKE 'b%%'", 2);         // {!tuple}addr.street:b*
            assertIds("addr.city = 'rome'");                       // {!tuple}addr.city:rome (StrField: case-sensitive)
            assertIds("addr.city >= 'P' AND addr.city <= 'S'", 1, 2); // {!tuple}addr.city:[P TO S]
            assertIds("addr.city IS NOT NULL", 1, 2);              // {!tuple}addr.city:*
            assertIds("addr.loc.lat IS NOT NULL", 1);              // {!tuple}addr.loc.lat:*
            assertIds("addr.tags CONTAINS 'y'", 1);                // {!tuple}addr.tags:y
            assertIds("addr.zip > 100", 1);                        // {!tuple}addr.zip:{100 TO *]
            assertIds("addr.zip IN (100, 300)", 2);                // {!tuple}addr.zip:(100 OR 300)
            assertIds("addr.zip != 200", 2);                       // {!tuple}(addr.zip:* AND -addr.zip:200)
            assertIds("addr.zip NOT IN (200, 300)", 2);            // {!tuple}(addr.zip:* AND -addr.zip:(200 OR 300))
            assertIds("addr.city = 'Rome' AND addr.zip = 200", 1); // {!tuple}addr.city:Rome AND addr.zip:200
            // {!tuple}addr.city:Rome AND {!tuple}addr.zip:200, {!tuple}a AND _query_:"{!tuple}b": none in DSE (the
            // second parser isn't honoured); the CQL AND is an ordinary AND
            assertIds("addr.city = 'Rome' AND addr.zip = 200 AND addr.loc.lat = 1.0", 1);
            // DSE Graph suffix and contains predicates: {!tuple}addr.city:*me, {!tuple}addr.city:/.*ari.*/
            assertIndexedIds("addr.city LIKE '%%me'", 1);
            assertIndexedIds("addr.city LIKE '%%ari%%'", 2);
            assertIds("id = 1 AND addr.city = 'Rome'", 1);         // id:1 AND {!tuple}addr.city:Rome
            assertIds("id = 2 AND addr.city = 'Rome'");
        });

        // addr.city:Rome without the tuple parser matches nothing in DSE (the fields are only reachable through
        // {!tuple}): there is no CQL counterpart to write, a field restriction is always on the field.
        // {!tuple}*:* AND NOT addr.loc.lat:* (an error in DSE): no IS NULL
        assertInvalidSyntaxMessage("IS NULL restrictions are not supported", "SELECT id FROM %s WHERE addr.loc.lat IS NULL ALLOW FILTERING");
        // sort on addr.zip: "Sorting on Tuple fields is not supported. Offending field [addr.zip]"
        assertInvalidSyntax("SELECT id FROM %s ORDER BY addr.zip LIMIT 10");
        // facet on addr.city: "Faceting on Tuple fields is not supported. Offending field [addr.city]"
        assertInvalidMessage("Group by is not supported on the fields of columns, got addr.city",
                             "SELECT addr.city, count(*) FROM %s GROUP BY addr.city");
    }

    /**
     * u_frozen with the DSE {@code lowerCase} behaviour: an index with {@code case_sensitive: false} on the field. It
     * normalizes the equalities, but it doesn't serve the ranges, which compare the raw values with ALLOW FILTERING:
     * the results of the ranges, {@code !=} and {@code NOT IN} don't depend on the analyzers of the indexes, while
     * those of {@code =}, {@code :}, LIKE, CONTAINS and NOT CONTAINS do, as on columns.
     */
    @Test
    public void testCaseInsensitiveField() throws Throwable
    {
        runOnce();
        createRuntimeTypes();
        createTables("CREATE TABLE %s (id int PRIMARY KEY, addr frozen<" + address + ">)");
        index("addr.city", " WITH OPTIONS = {'case_sensitive': 'false'}");
        write("INSERT INTO %s (id, addr) VALUES (1, " + ROME + ')');
        write("INSERT INTO %s (id, addr) VALUES (2, " + PARIS + ')');

        inAllStates(() -> {
            assertIndexedIds("addr.city = 'rome'", 1);             // {!tuple}addr.city:rome with a lowerCase field type
            assertIndexedIds("addr.city = 'ROME'", 1);
            // {!tuple}addr.city:[p TO s]: not served by the index
            assertInvalidMessage("has an index but does not support the operators specified in the query",
                                 "SELECT id FROM %s WHERE addr.city >= 'p' AND addr.city <= 's'");
            assertIds("addr.city >= 'p' AND addr.city <= 's' ALLOW FILTERING");
            assertIds("addr.city >= 'P' AND addr.city <= 'S' ALLOW FILTERING", 1, 2);
            assertIds("addr.city != 'ROME' ALLOW FILTERING", 1, 2);
            assertIds("addr.city != 'Rome' ALLOW FILTERING", 2);
        });
    }

    /**
     * u_nonfrozen: 1 = {Ann, 30, home Rome}, 2 = {Bob, 40} then {@code UPDATE SET p.age = 41}; the index on
     * {@code p.age} is DSE's {@code CREATE SEARCH INDEX ... WITH COLUMNS id, p.age}.
     */
    @Test
    public void testNonFrozenUdt() throws Throwable
    {
        runOnce();
        createRuntimeTypes();
        String person = createType("CREATE TYPE %s (name text, age int, home frozen<" + address + ">)");
        createTables("CREATE TABLE %s (id int PRIMARY KEY, p " + person + ')');
        index("p.age");
        index("p.home.city");
        write("INSERT INTO %s (id, p) VALUES (1, {name: 'Ann', age: 30, home: {city: 'Rome'}})");
        write("INSERT INTO %s (id, p) VALUES (2, {name: 'Bob', age: 40})");
        flushBoth();
        // the single-field update, in the memtable over the flushed row
        write("UPDATE %s SET p.age = 41 WHERE id = 2");

        inAllStates(() -> {
            assertIds("p.age = 41", 2);                // {!tuple}p.age:41 (single-field updates are reindexed)
            assertIds("p.age = 40");
            assertIds("p.home.city = 'Rome'", 1);      // {!tuple}p.home.city:Rome (frozen UDT inside a non-frozen UDT)
            assertIds("p.age >= 30", 1, 2);            // {!tuple}p.age:[30 TO *]
        });
    }

    /**
     * u_nested: a frozen UDT inside a frozen UDT inside a frozen UDT.
     */
    @Test
    public void testNestedUdt() throws Throwable
    {
        runOnce();
        createRuntimeTypes();
        String person = createType("CREATE TYPE %s (name text, age int, home frozen<" + address + ">)");
        createTables("CREATE TABLE %s (id int PRIMARY KEY, p frozen<" + person + ">)");
        index("p.age");
        index("p.home.city");
        index("p.home.loc.lat");
        write("INSERT INTO %s (id, p) VALUES (1, {name: 'Ann', age: 30, home: " + ROME + "})");
        write("INSERT INTO %s (id, p) VALUES (2, {name: 'Bob', age: 41, home: " + PARIS + "})");

        inAllStates(() -> {
            assertIds("p.home.city = 'Rome'", 1);      // {!tuple}p.home.city:Rome
            assertIds("p.home.loc.lat >= 1.0", 1);     // {!tuple}p.home.loc.lat:[1.0 TO *]
            assertIds("p.home.loc.lat IS NOT NULL", 1); // {!tuple}p.home.loc.lat:*
            assertIds("p.age = 41", 2);                // {!tuple}p.age:41
        });
    }

    /**
     * t_tuple: 1 = (foo, 1, 1.5), 2 = (bar, 2, 2.5); the components are named {@code field1}, {@code field2}, ...
     */
    @Test
    public void testTuple() throws Throwable
    {
        runOnce();
        createTables("CREATE TABLE %s (id int PRIMARY KEY, t tuple<text, int, double>)");
        index("t.field1");
        index("t.field2");
        index("t.field3");
        write("INSERT INTO %s (id, t) VALUES (1, ('foo', 1, 1.5))");
        write("INSERT INTO %s (id, t) VALUES (2, ('bar', 2, 2.5))");

        inAllStates(() -> {
            assertIds("t.field1 = 'foo'", 1);          // {!tuple}t.field1:foo
            assertIds("t.field2 >= 2", 2);             // {!tuple}t.field2:[2 TO *]
            assertIds("t.field3 < 2.0", 1);            // {!tuple}t.field3:{* TO 2.0}
        });
        assertInvalidMessage("Undefined field field0", "SELECT id FROM %s WHERE t.field0 = 'foo' ALLOW FILTERING");
        assertInvalidMessage("Undefined field field4", "SELECT id FROM %s WHERE t.field4 = 'foo' ALLOW FILTERING");
    }

    @Test
    public void testListOfUdts() throws Throwable
    {
        testCollectionOfUdts("list");
    }

    @Test
    public void testSetOfUdts() throws Throwable
    {
        testCollectionOfUdts("set");
    }

    /**
     * u_list and u_set: 1 = [{Rome, 200, lat 1}, {Milan, 100, lat 3}], 2 = [{Rome, 100, lat 5}]; in a tuple query,
     * the AND of conditions on the elements is on the same element ({@code CONTAINS ELEMENT}), while separate
     * {@code q} and {@code fq} are on any element each (AND-ed relations).
     */
    private void testCollectionOfUdts(String kind) throws Throwable
    {
        createRuntimeTypes();
        createTables("CREATE TABLE %s (id int PRIMARY KEY, addrs " + collection(kind, "frozen<" + address + '>') + ')');
        index("addrs.city");
        index("addrs.zip");
        index("addrs.loc.lat");
        index("addrs.tags");
        String open = "list".equals(kind) ? "[" : "{";
        String close = "list".equals(kind) ? "]" : "}";
        write("INSERT INTO %s (id, addrs) VALUES (1, " + open +
              "{street: 'a', city: 'Rome', zip: 200, loc: {lat: 1.0}, tags: {'x', 'y'}}, {street: 'b', city: 'Milan', zip: 100, loc: {lat: 3.0}, tags: {'z'}}" +
              close + ')');
        write("INSERT INTO %s (id, addrs) VALUES (2, " + open + "{street: 'bc', city: 'Rome', zip: 100, loc: {lat: 5.0}, tags: {'y'}}" + close + ')');
        write("INSERT INTO %s (id) VALUES (3)");

        inAllStates(() -> {
            assertIds("addrs.city = 'Rome'", 1, 2);                                     // {!tuple}addrs.city:Rome
            assertIds("addrs CONTAINS ELEMENT (city = 'Rome' AND zip = 100)", 2);       // {!tuple}addrs.city:Rome AND addrs.zip:100
            assertIds("addrs CONTAINS ELEMENT (city = 'Rome' OR city = 'Milan')", 1, 2); // {!tuple}addrs.city:Rome OR addrs.city:Milan
            assertIds("addrs.city IN ('Rome', 'Milan')", 1, 2);
            assertIds("addrs CONTAINS ELEMENT (city = 'Rome' AND zip != 100)", 1);      // {!tuple}addrs.city:Rome AND NOT addrs.zip:100
            assertIds("addrs.zip >= 150", 1);                                           // {!tuple}addrs.zip:[150 TO *]
            assertIds("addrs CONTAINS ELEMENT (loc.lat >= 2 AND loc.lat <= 6)", 1, 2);  // {!tuple}addrs.loc.lat:[2 TO 6]
            assertIds("addrs.tags CONTAINS 'y'", 1, 2);                                 // {!tuple}addrs.tags:y
            assertIds("addrs.city = 'Rome' AND addrs.zip = 100", 1, 2);                 // {"q": "{!tuple}addrs.city:Rome", "fq": "{!tuple}addrs.zip:100"}
            assertIds("id = 1 AND addrs.city = 'Rome'", 1);                             // id:1 AND {!tuple}addrs.city:Rome
            // {!tuple}addrs.zip:200 AND addrs.loc.lat:1.0 mixes nesting levels and matches nothing in DSE, though
            // row 1 has that element: CONTAINS ELEMENT matches it
            assertIds("addrs CONTAINS ELEMENT (zip = 200 AND loc.lat = 1.0)", 1);
            // -{!tuple}addrs.city:Rome AND addrs.zip:100 (no such element, including the rows without elements)
            assertIds("addrs NOT CONTAINS ELEMENT (city = 'Rome' AND zip = 100) ALLOW FILTERING", 1, 3);
            // -{!tuple}addrs.city:Rome
            assertIds("addrs.city != 'Rome'", 3);
            assertIds("addrs.loc.lat IS NOT NULL", 1, 2);                               // {!tuple}addrs.loc.lat:*
            // {!tuple}addrs.city:Rome AND {!tuple}addrs.zip:100 (unparenthesized, second parser ignored): none in
            // DSE; the CQL AND is an ordinary AND (any element each)
            assertIds("addrs.city = 'Rome' AND addrs.zip = 200", 1);
        });
        // sort on addrs.zip: "Sorting on Tuple fields is not supported. Offending field [addrs.zip]"
        assertInvalidSyntax("SELECT id FROM %s ORDER BY addrs.zip LIMIT 10");
    }

    /**
     * The DSE Graph negations on the fields inside collections, {@code (x:* AND -x:23)} and
     * {@code (x:* AND -x:(23 OR 45))} after the tuple parser, are evaluated on each element: some element has the
     * field and it isn't 23. That's a {@code CONTAINS ELEMENT} group with {@code !=} / {@code NOT IN}; the
     * {@code !=} / {@code NOT IN} on the field outside a group means "no element is equal", which selects the
     * opposite rows: not [23, 45], but the rows without elements.
     */
    @Test
    public void testNegationsOnFieldsInsideCollections() throws Throwable
    {
        createRuntimeTypes();
        createTables("CREATE TABLE %s (id int PRIMARY KEY, addrs " + collection("list", "frozen<" + address + '>') + ')');
        index("addrs.zip");
        write("INSERT INTO %s (id, addrs) VALUES (1, [{zip: 23}, {zip: 45}])");
        write("INSERT INTO %s (id, addrs) VALUES (2, [{zip: 23}])");
        write("INSERT INTO %s (id, addrs) VALUES (3, [{city: 'Rome'}])");
        write("INSERT INTO %s (id) VALUES (4)");

        inAllStates(() -> {
            // {!tuple}(addrs.zip:* AND -addrs.zip:23): row 1 (the element 45)
            assertIds("addrs CONTAINS ELEMENT (zip != 23) ALLOW FILTERING", 1);
            assertIds("addrs.zip != 23", 3, 4);
            // {!tuple}(addrs.zip:* AND -addrs.zip:(23 OR 45)): none
            assertIds("addrs CONTAINS ELEMENT (zip NOT IN (23, 45)) ALLOW FILTERING");
            assertIds("addrs.zip NOT IN (23, 45)", 3, 4);
        });
    }

    /**
     * u_map: DSE Search refuses to index a map of UDTs ("Dynamic fields cannot be tuple/UDT on CQL3 tables"); SAI
     * indexes the fields of its values.
     */
    @Test
    public void testMapOfUdts() throws Throwable
    {
        createRuntimeTypes();
        createTables("CREATE TABLE %s (id int PRIMARY KEY, addrs " + collection("map", "text, frozen<" + address + '>') + ')');
        index("values(addrs).city");
        index("addrs.zip");
        write("INSERT INTO %s (id, addrs) VALUES (1, {'home': {city: 'Rome', zip: 200}, 'work': {city: 'Milan', zip: 100}})");
        write("INSERT INTO %s (id, addrs) VALUES (2, {'home': {city: 'Rome', zip: 100}})");

        inAllStates(() -> {
            assertIds("addrs.city = 'Rome'", 1, 2);
            assertIds("addrs CONTAINS ELEMENT (city = 'Rome' AND zip = 100)", 2);
            assertIds("addrs.city = 'Rome' AND addrs.zip = 100", 1, 2);
        });
    }

    /**
     * t_list: 1 = [(foo, 1), (bar, 2)]; the AND inside a tuple query is on the same element.
     */
    @Test
    public void testListOfTuples() throws Throwable
    {
        createTables("CREATE TABLE %s (id int PRIMARY KEY, ts " + collection("list", "frozen<tuple<text, int>>") + ')');
        index("ts.field1");
        index("ts.field2");
        write("INSERT INTO %s (id, ts) VALUES (1, [('foo', 1), ('bar', 2)])");
        write("INSERT INTO %s (id, ts) VALUES (2, [('baz', 3)])");

        inAllStates(() -> {
            assertIds("ts CONTAINS ELEMENT (field1 = 'foo' AND field2 = 2)");      // {!tuple}ts.field1:foo AND ts.field2:2
            assertIds("ts CONTAINS ELEMENT (field1 = 'foo' AND field2 = 1)", 1);   // {!tuple}ts.field1:foo AND ts.field2:1
            assertIds("ts.field1 = 'foo' AND ts.field2 = 2", 1);                   // separate q and fq
            assertIds("ts.field1 = 'foo'", 1);                                     // {!tuple}ts.field1:foo
            assertIds("ts.field2 > 1", 1, 2);                                      // {!tuple}ts.field2:{1 TO *]
        });
    }
}
