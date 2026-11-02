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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.cql3.FieldPath;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.cql3.statements.schema.CreateIndexStatement;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.virtual.VirtualKeyspace;
import org.apache.cassandra.db.virtual.VirtualKeyspaceRegistry;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.SSTableIndex;
import org.apache.cassandra.index.sai.disk.v1.SegmentBuilder;
import org.apache.cassandra.index.sai.iterators.KeyRangeIterator;
import org.apache.cassandra.index.sai.memory.MemtableIndex;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.view.View;
import org.apache.cassandra.index.sai.virtual.SSTableIndexesSystemView;
import org.apache.cassandra.index.sai.utils.NamedMemoryLimiter;
import org.apache.cassandra.index.sai.virtual.SegmentsSystemView;
import org.apache.cassandra.inject.ActionBuilder;
import org.apache.cassandra.inject.Injection;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.inject.InvokePointBuilder;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.serializers.CollectionSerializer;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.inject.Expression.expr;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * DDL and index building of the storage-attached indexes on the fields of UDT and tuple columns (HCD-653).
 * <p>
 * The schemas and the accepted shapes are those of the DSE Search reference run ({@code docs/sai-udt/dse-runtime.md}).
 * Queries on fields are not supported yet, so the content of the indexes is checked by searching the memtable and
 * SSTable indexes directly, and through the SAI system views.
 */
public class UdtFieldIndexDDLTest extends SAITester
{
    private String geo;
    private String address;
    private String person;

    @BeforeClass
    public static void registerViews()
    {
        VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace(SchemaConstants.VIRTUAL_VIEWS,
                                                                      ImmutableList.of(new SSTableIndexesSystemView(SchemaConstants.VIRTUAL_VIEWS),
                                                                                       new SegmentsSystemView(SchemaConstants.VIRTUAL_VIEWS))));
    }

    @Before
    public void createTypes()
    {
        geo = createType("CREATE TYPE %s (lat double, lon double)");
        address = createType("CREATE TYPE %s (street text, city text, zip int, loc frozen<" + geo + ">, tags set<text>)");
        person = createType("CREATE TYPE %s (name text, age int, home frozen<" + address + ">)");
    }

    private String index(String name, String target)
    {
        return createIndex("CREATE INDEX " + name + " ON %s(" + target + ") USING 'sai'");
    }

    private TableMetadata table()
    {
        return Keyspace.open(KEYSPACE).getColumnFamilyStore(currentTable()).metadata();
    }

    private String target(String index)
    {
        return table().indexes.get(index).orElseThrow().options.get(IndexTarget.TARGET_OPTION_NAME);
    }

    @Test
    public void testFrozenUDT() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, addr frozen<" + address + ">)");
        index("street_idx", "addr.street");
        index("city_idx", "addr.city");
        index("zip_idx", "addr.zip");
        index("lat_idx", "addr.loc.lat");
        index("loc_idx", "addr.loc");
        index("tags_idx", "addr.tags");
        index("full_tags_idx", "full(addr.tags)");

        assertThat(target("city_idx")).isEqualTo("addr.city");
        assertThat(target("lat_idx")).isEqualTo("addr.loc.lat");
        assertThat(target("loc_idx")).isEqualTo("addr.loc");
        // a simple target on a collection field means its values, as for columns
        assertThat(target("tags_idx")).isEqualTo("values(addr.tags)");
        assertThat(target("full_tags_idx")).isEqualTo("full(addr.tags)");

        IndexContext city = getIndexContext("city_idx");
        assertThat(city.getFieldPath()).isEqualTo(FieldPath.of(List.of(FieldPath.Step.field(1))));
        assertThat(city.getValidator()).isEqualTo(UTF8Type.instance);
        assertThat(city.isMultiValued()).isFalse();
        assertThat(city.isLiteral()).isTrue();
        assertThat(getIndexContext("zip_idx").getValidator()).isEqualTo(Int32Type.instance);
        assertThat(getIndexContext("lat_idx").getValidator()).isEqualTo(DoubleType.instance);
        assertThat(getIndexContext("tags_idx").getValidator()).isEqualTo(UTF8Type.instance);
        assertThat(getIndexContext("tags_idx").isMultiValued()).isTrue();
        assertThat(getIndexContext("full_tags_idx").getValidator().isCollection()).isTrue();
        assertThat(getIndexContext("full_tags_idx").isMultiValued()).isFalse();

        // the reference data: 1 = {a, Rome, 200, lat 1}, 2 = {b, Paris, 100, no loc}
        execute("INSERT INTO %s (id, addr) VALUES (1, {street: 'a', city: 'Rome', zip: 200, loc: {lat: 1.0, lon: 0.5}, tags: {'x', 'y'}})");
        execute("INSERT INTO %s (id, addr) VALUES (2, {street: 'b', city: 'Paris', zip: 100, tags: {'y'}})");
        execute("INSERT INTO %s (id, addr) VALUES (3, {street: '', city: 'Rome'})");
        execute("INSERT INTO %s (id) VALUES (4)");

        assertIndexContent(() -> {
            assertMatches("city_idx", "Rome", 1, 3);
            assertMatches("city_idx", "Paris", 2);
            assertMatches("city_idx", "rome");
            assertMatches("street_idx", "b", 2);
            // an empty string is indexed for text fields, as for text columns
            assertMatches("street_idx", "", 3);
            assertMatches("zip_idx", 200, 1);
            assertMatches("zip_idx", 100, 2);
            assertMatches("lat_idx", 1.0, 1);
            assertMatches("tags_idx", "y", 1, 2);
            assertMatches("tags_idx", "x", 1);
        });

        // the index on the field doesn't serve the restrictions on the whole column, nor makes them "unsupported
        // by an index": they need ALLOW FILTERING as without any index
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE addr = {street: 'b', city: 'Paris', zip: 100, tags: {'y'}}");
        assertRows(execute("SELECT id FROM %s WHERE addr = {street: 'b', city: 'Paris', zip: 100, tags: {'y'}} ALLOW FILTERING"), row(2));
    }

    @Test
    public void testFieldIndexOnlyNeedsAllowFiltering() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, addr frozen<" + address + ">, t tuple<int, text>)");
        index("city_idx", "addr.city");
        index("t_idx", "t.field1");
        execute("INSERT INTO %s (id, addr, t) VALUES (1, {city: 'Rome'}, (1, 'a'))");

        for (String restriction : new String[]{ "addr = {city: 'Rome'}", "t = (1, 'a')", "addr = {city: 'Rome'} AND t = (1, 'a')" })
        {
            String query = "SELECT id FROM %s WHERE " + restriction;
            assertInvalidThrowMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE, InvalidRequestException.class, query);
            assertRows(execute(query + " ALLOW FILTERING"), row(1));
        }
    }

    @Test
    public void testAnalyzerOnField() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, addr frozen<" + address + ">)");
        // DSE lowerCase: an analyzer on a text field of a frozen UDT
        createIndex("CREATE INDEX city_idx ON %s(addr.city) USING 'sai' WITH OPTIONS = {'case_sensitive': 'false'}");
        assertThat(getIndexContext("city_idx").isAnalyzed()).isTrue();

        execute("INSERT INTO %s (id, addr) VALUES (1, {city: 'Rome'})");
        assertIndexContent(() -> assertMatches("city_idx", "rome", 1));

        // an analyzer on a frozen collection field is refused, as on frozen collection columns
        assertInvalidMessage("because it's a frozen collection",
                             "CREATE INDEX ON %s(full(addr.tags)) USING 'sai' WITH OPTIONS = {'case_sensitive': 'false'}");
        // but not on its values
        createIndex("CREATE INDEX tags_idx ON %s(addr.tags) USING 'sai' WITH OPTIONS = {'case_sensitive': 'false'}");
    }

    @Test
    public void testNonFrozenUDT() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, p " + person + ")");
        index("name_idx", "p.name");
        index("age_idx", "p.age");
        index("home_city_idx", "p.home.city");
        index("home_lat_idx", "p.home.loc.lat");

        // the whole non-frozen UDT still can't be indexed
        assertInvalidMessage(String.format(CreateIndexStatement.CREATE_ON_NON_FROZEN_UDT, "p"), "CREATE INDEX ON %s(p) USING 'sai'");

        execute("INSERT INTO %s (id, p) VALUES (1, {name: 'Ann', age: 30, home: {city: 'Rome', loc: {lat: 1.0}}})");
        execute("INSERT INTO %s (id, p) VALUES (2, {name: 'Bob', age: 40})");

        assertMemtableMatches("age_idx", 40, 2);
        // a single-field update in the memtable replaces the term of the merged row
        execute("UPDATE %s SET p.age = 41 WHERE id = 2");
        assertMemtableMatches("age_idx", 41, 2);
        assertMemtableMatches("age_idx", 40);
        assertMemtableMatches("name_idx", "Bob", 2);
        assertMemtableMatches("home_city_idx", "Rome", 1);
        assertMemtableMatches("home_lat_idx", 1.0, 1);

        // deleting a field removes its term
        execute("DELETE p.name FROM %s WHERE id = 1");
        assertMemtableMatches("name_idx", "Ann");

        flush();
        assertSSTableMatches("age_idx", 41, 2);
        assertSSTableMatches("age_idx", 30, 1);
        assertSSTableMatches("name_idx", "Ann");
        assertSSTableMatches("home_city_idx", "Rome", 1);

        // an update of a single field in a new memtable indexes only that field
        execute("UPDATE %s SET p.age = 42 WHERE id = 2");
        assertMemtableMatches("age_idx", 42, 2);
        assertMemtableMatches("name_idx", "Bob");

        flush();
        compact();
        assertSSTableMatches("age_idx", 42, 2);
        assertSSTableMatches("age_idx", 41);
        assertSSTableMatches("name_idx", "Bob", 2);
        assertSSTableMatches("home_city_idx", "Rome", 1);
    }

    @Test
    public void testNestedFrozenUDT() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, p frozen<" + person + ">)");
        index("lat_idx", "p.home.loc.lat");
        index("city_idx", "p.home.city");
        assertThat(target("lat_idx")).isEqualTo("p.home.loc.lat");

        execute("INSERT INTO %s (id, p) VALUES (1, {name: 'Ann', home: {city: 'Rome', loc: {lat: 1.0}}})");
        execute("INSERT INTO %s (id, p) VALUES (2, {name: 'Bob'})");
        assertIndexContent(() -> {
            assertMatches("lat_idx", 1.0, 1);
            assertMatches("city_idx", "Rome", 1);
        });
    }

    @Test
    public void testTuple() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, t tuple<text, int, double>)");
        index("f1_idx", "t.field1");
        index("f2_idx", "t.field2");
        index("f3_idx", "t.\"field3\"");
        assertThat(target("f1_idx")).isEqualTo("t.field1");
        assertThat(target("f3_idx")).isEqualTo("t.field3");

        assertInvalidMessage("Undefined field field4", "CREATE INDEX ON %s(t.field4) USING 'sai'");
        assertInvalidMessage("Undefined field field0", "CREATE INDEX ON %s(t.field0) USING 'sai'");
        assertInvalidMessage("Undefined field x", "CREATE INDEX ON %s(t.x) USING 'sai'");

        execute("INSERT INTO %s (id, t) VALUES (1, ('foo', 1, 1.5))");
        execute("INSERT INTO %s (id, t) VALUES (2, ('bar', 2, 2.5))");
        assertIndexContent(() -> {
            assertMatches("f1_idx", "foo", 1);
            assertMatches("f2_idx", 2, 2);
            assertMatches("f3_idx", 1.5, 1);
        });
    }

    @Test
    public void testListOfUDTs() throws Throwable
    {
        testCollectionOfUDTs("list<frozen<" + address + ">>", "[", "]", false);
    }

    @Test
    public void testFrozenListOfUDTs() throws Throwable
    {
        testCollectionOfUDTs("frozen<list<frozen<" + address + ">>>", "[", "]", false);
    }

    @Test
    public void testSetOfUDTs() throws Throwable
    {
        testCollectionOfUDTs("set<frozen<" + address + ">>", "{", "}", false);
    }

    @Test
    public void testMapOfUDTs() throws Throwable
    {
        testCollectionOfUDTs("map<text, frozen<" + address + ">>", "{", "}", true);
    }

    private void testCollectionOfUDTs(String type, String open, String close, boolean map) throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, addrs " + type + ")");
        index("city_idx", "addrs.city");
        index("zip_idx", "values(addrs).zip");
        index("lat_idx", "addrs.loc.lat");
        index("tags_idx", "addrs.tags");
        // the canonical form leaves the elements implicit
        assertThat(target("zip_idx")).isEqualTo("addrs.zip");
        assertThat(target("tags_idx")).isEqualTo("values(addrs.tags)");
        assertThat(getIndexContext("city_idx").isMultiValued()).isTrue();
        assertThat(getIndexContext("city_idx").isNonFrozenCollection()).isFalse();
        // the explicit and implicit forms are the same index
        assertInvalidMessage("is a duplicate of existing index city_idx", "CREATE INDEX dup_idx ON %s(values(addrs).city) USING 'sai'");

        String rome = (map ? "'r': " : "") + "{city: 'Rome', zip: 200, loc: {lat: 1.0}, tags: {'x'}}";
        String milan = (map ? "'m': " : "") + "{city: 'Milan', zip: 100, loc: {lat: 3.0}, tags: {'y', 'z'}}";
        String rome2 = (map ? "'r': " : "") + "{city: 'Rome', zip: 100, loc: {lat: 5.0}}";
        execute("INSERT INTO %s (id, addrs) VALUES (1, " + open + rome + ", " + milan + close + ")");
        execute("INSERT INTO %s (id, addrs) VALUES (2, " + open + rome2 + close + ")");
        execute("INSERT INTO %s (id, addrs) VALUES (3, " + open + close + ")");

        assertIndexContent(() -> {
            assertMatches("city_idx", "Rome", 1, 2);
            assertMatches("city_idx", "Milan", 1);
            assertMatches("zip_idx", 100, 1, 2);
            assertMatches("zip_idx", 200, 1);
            assertMatches("lat_idx", 5.0, 2);
            assertMatches("tags_idx", "z", 1);
        });
        assertThat(sstableCellCount("city_idx")).isEqualTo(2);
    }

    @Test
    public void testElementUpdatesInMemtable() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, addrs list<frozen<" + address + ">>, s set<frozen<" + address + ">>)");
        index("list_city_idx", "addrs.city");
        index("set_city_idx", "s.city");

        execute("INSERT INTO %s (id, addrs, s) VALUES (1, [{city: 'Rome'}], {{city: 'Rome'}})");
        execute("UPDATE %s SET addrs = addrs + [{city: 'Milan'}], s = s + {{city: 'Milan'}} WHERE id = 1");
        assertMemtableMatches("list_city_idx", "Milan", 1);
        assertMemtableMatches("set_city_idx", "Milan", 1);

        execute("DELETE addrs[0] FROM %s WHERE id = 1");
        execute("UPDATE %s SET s = s - {{city: 'Rome'}} WHERE id = 1");
        assertMemtableMatches("list_city_idx", "Rome");
        assertMemtableMatches("set_city_idx", "Rome");
        assertMemtableMatches("list_city_idx", "Milan", 1);

        flush();
        assertSSTableMatches("list_city_idx", "Rome");
        assertSSTableMatches("set_city_idx", "Milan", 1);
    }

    @Test
    public void testListOfTuples() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, ts list<frozen<tuple<text, int>>>)");
        index("f1_idx", "ts.field1");
        index("f2_idx", "ts.field2");

        execute("INSERT INTO %s (id, ts) VALUES (1, [('foo', 1), ('bar', 2)])");
        assertIndexContent(() -> {
            assertMatches("f1_idx", "bar", 1);
            assertMatches("f2_idx", 1, 1);
        });
    }

    @Test
    public void testKeyColumns() throws Throwable
    {
        createTable("CREATE TABLE %s (k1 int, k2 frozen<" + geo + ">, c frozen<" + address + ">, s frozen<" + geo + "> static, " +
                    "PRIMARY KEY ((k1, k2), c)) WITH CLUSTERING ORDER BY (c DESC)");
        index("k2_idx", "k2.lat");
        index("c_idx", "c.city");
        index("c_zip_idx", "c.zip");
        index("s_idx", "s.lon");

        execute("INSERT INTO %s (k1, k2, c, s) VALUES (1, {lat: 1.0, lon: 2.0}, {city: 'Rome', zip: 1}, {lon: 7.0})");
        execute("INSERT INTO %s (k1, k2, c) VALUES (1, {lat: 1.0, lon: 2.0}, {city: 'Milan', zip: 2})");
        execute("INSERT INTO %s (k1, k2, c, s) VALUES (2, {lat: 3.0}, {city: 'Rome', zip: 3}, {lon: 8.0})");

        assertIndexContent(() -> {
            assertMatches("k2_idx", 1.0, 1);
            assertMatches("k2_idx", 3.0, 2);
            assertMatches("c_idx", "Rome", 1, 2);
            assertMatches("c_idx", "Milan", 1);
            assertMatches("c_zip_idx", 3, 2);
            assertMatches("s_idx", 8.0, 2);
        });

        // the only partition key column can't be indexed, not even by a field
        createTable("CREATE TABLE %s (k frozen<" + geo + "> PRIMARY KEY, v int)");
        assertInvalidMessage("Cannot create secondary index on the only partition key column", "CREATE INDEX ON %s(k.lat) USING 'sai'");
    }

    @Test
    public void testRefusedShapes() throws Throwable
    {
        String withDuration = createType("CREATE TYPE %s (d duration, i int)");
        String withOthers = createType("CREATE TYPE %s (d duration, v vector<float, 2>, g frozen<" + geo + ">, i int, wd frozen<" + withDuration + ">)");
        createTable("CREATE TABLE %s (id int PRIMARY KEY, addr frozen<" + address + ">, o frozen<" + withOthers + ">, " +
                    "od frozen<" + withDuration + ">, n int, mk map<frozen<" + geo + ">, int>, addrs list<frozen<" + address + ">>, " +
                    "t tuple<int, duration>)");

        assertInvalidMessage("Undefined field country", "CREATE INDEX ON %s(addr.country) USING 'sai'");
        assertInvalidMessage("Undefined field alt in field loc", "CREATE INDEX ON %s(addr.loc.alt) USING 'sai'");
        assertInvalidMessage("neither a user-defined type nor a tuple", "CREATE INDEX ON %s(n.x) USING 'sai'");
        assertInvalidMessage("neither a user-defined type nor a tuple", "CREATE INDEX ON %s(addr.city.x) USING 'sai'");
        assertInvalidMessage("Undefined column name nope", "CREATE INDEX ON %s(nope.x) USING 'sai'");

        // vector and duration fields
        assertInvalidMessage("vector fields can't be indexed", "CREATE INDEX ON %s(o.v) USING 'sai'");
        assertInvalidMessage("secondary indexes are not supported on durations", "CREATE INDEX ON %s(o.d) USING 'sai'");
        assertInvalidMessage("secondary indexes are not supported on durations", "CREATE INDEX ON %s(t.field2) USING 'sai'");
        assertInvalidMessage("not supported on values containing durations", "CREATE INDEX ON %s(o.wd) USING 'sai'");
        assertInvalidMessage("Secondary indexes are not supported on UDTs containing durations", "CREATE INDEX ON %s(od) USING 'sai'");
        // the other fields of a UDT with a duration can be indexed
        index("i_idx", "o.i");
        index("g_idx", "o.g.lat");
        index("od_idx", "od.i");
        index("t_idx", "t.field1");

        // map keys
        assertInvalidMessage("field paths through the keys of a map are not supported", "CREATE INDEX ON %s(mk.lat) USING 'sai'");
        assertInvalidMessage("mismatched input '.'", "CREATE INDEX ON %s(keys(addr.tags)) USING 'sai'");
        assertInvalidMessage("mismatched input '.'", "CREATE INDEX ON %s(entries(addr.tags)) USING 'sai'");

        // values() and full()
        assertInvalidMessage("Cannot use values() on addr of type", "CREATE INDEX ON %s(values(addr).city) USING 'sai'");
        assertInvalidMessage("Cannot use values() on addr.loc of type", "CREATE INDEX ON %s(values(addr.loc).lat) USING 'sai'");
        assertInvalidMessage("Cannot create values() index on addr.city", "CREATE INDEX ON %s(values(addr.city)) USING 'sai'");
        assertInvalidMessage(CreateIndexStatement.FULL_ON_FROZEN_COLLECTIONS, "CREATE INDEX ON %s(full(addr.city)) USING 'sai'");
        assertInvalidMessage(CreateIndexStatement.FULL_ON_FROZEN_COLLECTIONS, "CREATE INDEX ON %s(full(addrs.city)) USING 'sai'");

        // only SAI
        assertInvalidMessage("only supported by storage-attached indexes", "CREATE INDEX ON %s(addr.city) USING 'legacy_local_table'");
        assertInvalidMessage("only supported by storage-attached indexes", "CREATE CUSTOM INDEX ON %s(addr.city) USING 'org.apache.cassandra.index.StubIndex'");
        // SAI by its other names
        createIndex("CREATE CUSTOM INDEX city_idx ON %s(addr.city) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX zip_idx ON %s(addr.zip) USING 'org.apache.cassandra.index.sai.StorageAttachedIndex'");
        // a SAI index on several targets is refused, with or without fields
        assertInvalidMessage("cannot be created over multiple columns", "CREATE CUSTOM INDEX ON %s(addr.street, n) USING 'sai'");
    }

    @Test
    public void testDuplicates() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, addr frozen<" + address + ">)");
        index("city_idx", "addr.city");
        assertInvalidMessage("is a duplicate of existing index city_idx", "CREATE INDEX other_idx ON %s(addr.city) USING 'sai'");
        assertInvalidMessage("Cannot create duplicate storage-attached index on field: addr.city",
                             "CREATE INDEX other_idx ON %s(addr.city) USING 'sai' WITH OPTIONS = {'case_sensitive': 'true'}");
        // IF NOT EXISTS is a no-op
        execute("CREATE INDEX IF NOT EXISTS other_idx ON %s(addr.city) USING 'sai'");
        assertThat(table().indexes.get("other_idx")).isEmpty();

        // an analyzed and a non-analyzed index on the same field, an index on the whole column and on another field
        createIndex("CREATE INDEX city_ci_idx ON %s(addr.city) USING 'sai' WITH OPTIONS = {'case_sensitive': 'false'}");
        index("addr_idx", "addr");
        index("loc_idx", "addr.loc");
        index("lat_idx", "addr.loc.lat");
        assertThat(table().indexes.size()).isEqualTo(5);

        // the whole-column index still serves the restrictions on the column
        execute("INSERT INTO %s (id, addr) VALUES (1, {city: 'Rome'})");
        assertRows(execute("SELECT id FROM %s WHERE addr = {city: 'Rome'}"), row(1));
    }

    @Test
    public void testDescribeRoundTrip() throws Throwable
    {
        String quoted = createType("CREATE TYPE %s (\"Street Name\" text, \"select\" int, inner frozen<" + geo + ">, tags set<text>)");
        createTable("CREATE TABLE %s (id int PRIMARY KEY, \"Addr\" frozen<" + quoted + ">, t tuple<text, int>, addrs list<frozen<" + address + ">>)");
        String[] targets = { "\"Addr\".\"Street Name\"", "\"Addr\".\"select\"", "\"Addr\".inner.lat", "\"Addr\".tags",
                             "full(\"Addr\".tags)", "t.field2", "values(addrs).loc.lon" };
        String[] canonical = { "\"Addr\".\"Street Name\"", "\"Addr\".\"select\"", "\"Addr\".inner.lat", "values(\"Addr\".tags)",
                               "full(\"Addr\".tags)", "t.field2", "addrs.loc.lon" };
        for (int i = 0; i < targets.length; i++)
        {
            String name = "idx_" + i;
            createIndex("CREATE INDEX " + name + " ON %s(" + targets[i] + ") USING 'sai'");
            assertThat(target(name)).isEqualTo(canonical[i]);

            IndexMetadata before = table().indexes.get(name).orElseThrow();
            String cql = before.toCqlString(table(), false);
            assertThat(cql).contains("(" + canonical[i] + ")");
            dropIndex("DROP INDEX %s." + name);
            createIndex(cql);
            IndexMetadata after = table().indexes.get(name).orElseThrow();
            assertThat(after.options).isEqualTo(before.options);
            assertThat(getIndexContext(name).getFieldPath()).isNotNull();
        }

        // DESCRIBE prints the canonical targets
        StringBuilder describe = new StringBuilder();
        executeNet("DESCRIBE TABLE " + KEYSPACE + '.' + currentTable()).forEach(row -> describe.append(row.getString("create_statement")).append('\n'));
        for (String target : canonical)
            assertThat(describe).contains("(" + target + ")");
    }

    @Test
    public void testDefaultIndexNames() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, addr frozen<" + address + ">, t tuple<text, int>, \"Q\" frozen<" + geo + ">)");
        schemaChange(formatQuery("CREATE INDEX ON %s(addr.city) USING 'sai'"));
        schemaChange(formatQuery("CREATE INDEX ON %s(addr.loc.lat) USING 'sai'"));
        schemaChange(formatQuery("CREATE INDEX ON %s(t.field2) USING 'sai'"));
        schemaChange(formatQuery("CREATE INDEX ON %s(\"Q\".lon) USING 'sai'"));
        schemaChange(formatQuery("CREATE INDEX ON %s(addr) USING 'sai'"));
        schemaChange(formatQuery("CREATE INDEX ON %s(addr.city) USING 'sai' WITH OPTIONS = {'case_sensitive': 'false'}"));

        String table = currentTable();
        Set<String> names = new HashSet<>();
        table().indexes.forEach(i -> names.add(i.name));
        assertThat(names).containsExactlyInAnyOrder(table + "_addr_city_idx",
                                                    table + "_addr_loc_lat_idx",
                                                    table + "_t_field2_idx",
                                                    table + "_Q_lon_idx",
                                                    table + "_addr_idx",
                                                    table + "_addr_city_idx_1");
    }

    @Test
    public void testAlterTypeRename() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, p " + person + ", a frozen<" + address + ">)");
        index("home_city_idx", "p.home.city");
        String table = currentTable();

        // fields on the path of an index, at any level
        assertInvalidMessage("Cannot rename field city of type " + KEYSPACE + '.' + address + " because it is used by index home_city_idx",
                             "ALTER TYPE " + KEYSPACE + '.' + address + " RENAME city TO town");
        assertInvalidMessage("drop the index first", "ALTER TYPE " + KEYSPACE + '.' + person + " RENAME home TO house");
        assertInvalidMessage("drop the index first", "ALTER TYPE " + KEYSPACE + '.' + address + " RENAME street TO road AND city TO town");

        // the other fields can be renamed
        execute("ALTER TYPE " + KEYSPACE + '.' + address + " RENAME street TO road");
        execute("ALTER TYPE " + KEYSPACE + '.' + person + " RENAME name TO full_name");
        assertThat(target("home_city_idx")).isEqualTo("p.home.city");

        // an index of another table using the type, through another column
        createTable("CREATE TABLE %s (id int PRIMARY KEY, addrs list<frozen<" + address + ">>)");
        index("zip_idx", "addrs.zip");
        assertInvalidMessage("because it is used by index zip_idx", "ALTER TYPE " + KEYSPACE + '.' + address + " RENAME zip TO code");

        // after dropping the indexes, they can be renamed
        dropIndex("DROP INDEX %s.zip_idx");
        dropIndex("DROP INDEX " + KEYSPACE + ".home_city_idx");
        execute("ALTER TYPE " + KEYSPACE + '.' + address + " RENAME zip TO code AND city TO town");
        assertThat(Keyspace.open(KEYSPACE).getColumnFamilyStore(table).metadata().indexes.size()).isZero();
    }

    @Test
    public void testAlterTypeAddThenBuild() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, addr frozen<" + address + ">, p " + person + ")");
        index("zip_idx", "addr.zip");
        index("age_idx", "p.age");
        execute("INSERT INTO %s (id, addr, p) VALUES (1, {city: 'Rome', zip: 1}, {age: 30})");
        flush();

        execute("ALTER TYPE " + KEYSPACE + '.' + address + " ADD country text");
        execute("ALTER TYPE " + KEYSPACE + '.' + person + " ADD nick text");
        // the indexes created before the new fields read the values with them
        execute("INSERT INTO %s (id, addr, p) VALUES (2, {city: 'Milan', zip: 2, country: 'Italy'}, {age: 40, nick: 'b'})");
        assertMemtableMatches("zip_idx", 2, 2);
        assertMemtableMatches("age_idx", 40, 2);

        // an index on a new field is built on the values written before the field existed (no term) and after
        index("country_idx", "addr.country");
        index("nick_idx", "p.nick");
        assertAnyMatches("country_idx", "Italy", 2);
        assertAnyMatches("nick_idx", "b", 2);
        assertAnyMatches("zip_idx", 1, 1);

        flush();
        compact();
        assertSSTableMatches("country_idx", "Italy", 2);
        assertSSTableMatches("zip_idx", 1, 1);
        assertSSTableMatches("zip_idx", 2, 2);
        assertSSTableMatches("nick_idx", "b", 2);
        assertThat(sstableCellCount("country_idx")).isEqualTo(1);
        assertThat(sstableCellCount("zip_idx")).isEqualTo(2);
    }

    @Test
    public void testBuildOnExistingData() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, addr frozen<" + address + ">, addrs list<frozen<" + address + ">>)");
        execute("INSERT INTO %s (id, addr, addrs) VALUES (1, {city: 'Rome'}, [{zip: 1}, {zip: 2}])");
        flush();
        execute("INSERT INTO %s (id, addr, addrs) VALUES (2, {city: 'Milan'}, [{zip: 2}])");
        flush();
        execute("INSERT INTO %s (id, addr) VALUES (3, {city: 'Rome'})");

        // the initial build indexes the SSTables, the memtable is indexed on flush
        index("city_idx", "addr.city");
        index("zip_idx", "addrs.zip");
        assertAnyMatches("city_idx", "Rome", 1, 3);
        assertAnyMatches("zip_idx", 2, 1, 2);
        flush();
        assertSSTableMatches("city_idx", "Rome", 1, 3);
        assertThat(sstableCellCount("city_idx")).isEqualTo(3);

        compact();
        assertSSTableMatches("city_idx", "Rome", 1, 3);
        assertSSTableMatches("zip_idx", 1, 1);
        assertSSTableMatches("zip_idx", 2, 1, 2);

        rebuildIndexes("city_idx", "zip_idx");
        assertSSTableMatches("city_idx", "Milan", 2);
        assertSSTableMatches("zip_idx", 2, 1, 2);
        assertThat(sstableCellCount("zip_idx")).isEqualTo(2);
    }

    @Test
    public void testColumnNameWithDot() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, addr frozen<" + address + ">, \"addr.city\" text)");
        index("field_idx", "addr.city");
        index("column_idx", "\"addr.city\"");

        assertThat(target("field_idx")).isEqualTo("addr.city");
        assertThat(target("column_idx")).isEqualTo("\"addr.city\"");
        assertThat(getIndexContext("field_idx").hasFieldPath()).isTrue();
        assertThat(getIndexContext("field_idx").getColumnName()).isEqualTo("addr");
        assertThat(getIndexContext("column_idx").hasFieldPath()).isFalse();
        assertThat(getIndexContext("column_idx").getColumnName()).isEqualTo("addr.city");
        assertThat(getIndexContext("field_idx").getTargetName()).isEqualTo("addr.city");
        assertThat(getIndexContext("column_idx").getTargetName()).isEqualTo("\"addr.city\"");

        execute("INSERT INTO %s (id, addr, \"addr.city\") VALUES (1, {city: 'Rome'}, 'Milan')");
        assertMemtableMatches("field_idx", "Rome", 1);
        assertMemtableMatches("field_idx", "Milan");
        assertMemtableMatches("column_idx", "Milan", 1);
        assertMemtableMatches("column_idx", "Rome");

        // DESCRIBE round trip
        for (String name : new String[]{ "field_idx", "column_idx" })
        {
            IndexMetadata before = table().indexes.get(name).orElseThrow();
            String cql = before.toCqlString(table(), false);
            dropIndex("DROP INDEX %s." + name);
            createIndex(cql);
            assertThat(table().indexes.get(name).orElseThrow().options).isEqualTo(before.options);
        }
        assertThat(getIndexContext("field_idx").hasFieldPath()).isTrue();
        assertThat(getIndexContext("column_idx").hasFieldPath()).isFalse();
        flush();
        assertSSTableMatches("field_idx", "Rome", 1);
        assertSSTableMatches("column_idx", "Milan", 1);
    }

    @Test
    public void testOversizeAndEmptyLastTerms() throws Throwable
    {
        boolean validate = CassandraRelevantProperties.SAI_VALIDATE_MAX_TERM_SIZE_AT_COORDINATOR.getBoolean();
        CassandraRelevantProperties.SAI_VALIDATE_MAX_TERM_SIZE_AT_COORDINATOR.setBoolean(false);
        try
        {
            createTable("CREATE TABLE %s (id int PRIMARY KEY, addrs list<frozen<" + address + ">>)");
            index("city_idx", "addrs.city");
            index("zip_idx", "addrs.zip");

            String big = "x".repeat(IndexContext.MAX_STRING_TERM_SIZE + 1);
            // the last term of each row is too large or empty (not indexed for int fields)
            execute("INSERT INTO %s (id, addrs) VALUES (1, ?)", list(address(text("Rome"), Int32Type.instance.decompose(1)),
                                                                     address(text(big), ByteBufferUtil.EMPTY_BYTE_BUFFER)));
            execute("INSERT INTO %s (id, addrs) VALUES (2, ?)", list(address(text("Milan"), Int32Type.instance.decompose(2)),
                                                                     address(text(big), null)));
            flush();

            assertSSTableMatches("city_idx", "Rome", 1);
            assertSSTableMatches("city_idx", "Milan", 2);
            assertSSTableMatches("zip_idx", 1, 1);
            assertSSTableMatches("zip_idx", 2, 2);
            // both rows are counted, though their last terms weren't indexed
            assertThat(sstableCellCount("city_idx")).isEqualTo(2);
            assertThat(sstableCellCount("zip_idx")).isEqualTo(2);
        }
        finally
        {
            CassandraRelevantProperties.SAI_VALIDATE_MAX_TERM_SIZE_AT_COORDINATOR.setBoolean(validate);
        }
    }

    private static ByteBuffer text(String value)
    {
        return UTF8Type.instance.decompose(value);
    }

    /** a serialized address with only the street (null), city and zip fields */
    private static ByteBuffer address(ByteBuffer city, ByteBuffer zip)
    {
        return TupleType.buildValue(new ByteBuffer[]{ null, city, zip });
    }

    private static ByteBuffer list(ByteBuffer... elements)
    {
        return CollectionSerializer.pack(Arrays.asList(elements), elements.length);
    }

    @Test
    public void testMultiValuedRowsInSmallSegments() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, addrs list<frozen<" + address + ">>)");
        index("city_idx", "addrs.city");
        int rows = 5;
        for (int i = 0; i < rows; i++)
            execute("INSERT INTO %s (id, addrs) VALUES (?, [{city: 'a" + i + "'}, {city: 'b" + i + "'}, {city: 'all'}])", i);

        flush();

        // The segment memory limit is reached after every term, so the SSTable index writer flushes a segment as
        // soon as it is allowed to: it must be before the first term of a row, never between the terms of a row
        // (which would put a row in two segments). The rebuild writes the index from the SSTable rows with the
        // SSTable index writer, as compaction does.
        Injection limitExceeded = Injections.newCustom("segment_memory_limit_exceeded")
                                            .add(InvokePointBuilder.newInvokePoint()
                                                                   .onClass(NamedMemoryLimiter.class)
                                                                   .onMethod("usageExceedsLimit")
                                                                   .atEntry())
                                            .add(ActionBuilder.newActionBuilder().actions().doReturn(expr("true")))
                                            .build();
        Injection minimumFlushSizeReached = Injections.newCustom("segment_minimum_flush_size_reached")
                                                      .add(InvokePointBuilder.newInvokePoint()
                                                                             .onClass(SegmentBuilder.class)
                                                                             .onMethod("hasReachedMinimumFlushSize")
                                                                             .atEntry())
                                                      .add(ActionBuilder.newActionBuilder().actions().doReturn(expr("true")))
                                                      .build();
        Injections.inject(limitExceeded, minimumFlushSizeReached);
        try
        {
            rebuildIndexes("city_idx");
        }
        finally
        {
            limitExceeded.disable();
            minimumFlushSizeReached.disable();
        }

        Object[][] segments = getRows(execute("SELECT min_sstable_row_id, max_sstable_row_id, cell_count FROM system_views.sstable_index_segments " +
                                                  "WHERE keyspace_name = ? AND index_name = ?", KEYSPACE, "city_idx"));
        assertThat(segments.length).isEqualTo(rows);
        for (Object[] segment : segments)
        {
            assertThat(segment[0]).isEqualTo(segment[1]);
            assertThat(segment[2]).isEqualTo(1L);
        }
        assertThat(sstableCellCount("city_idx")).isEqualTo(rows);
        for (int i = 0; i < rows; i++)
        {
            assertSSTableMatches("city_idx", "a" + i, i);
            assertSSTableMatches("city_idx", "b" + i, i);
        }
        assertSSTableMatches("city_idx", "all", 0, 1, 2, 3, 4);
    }

    @Test
    public void testSyntax() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, addr frozen<" + address + ">)");
        assertThatThrownBy(() -> execute("CREATE INDEX ON %s(addr.) USING 'sai'")).isInstanceOf(SyntaxException.class);
        assertThatThrownBy(() -> execute("CREATE INDEX ON %s(full(addr).tags) USING 'sai'")).isInstanceOf(SyntaxException.class);
        assertThatThrownBy(() -> execute("CREATE INDEX ON %s(addr.loc.) USING 'sai'")).isInstanceOf(SyntaxException.class);
        execute("CREATE INDEX ON %s(addr.city) USING 'sai'");
        assertThatThrownBy(() -> execute("CREATE INDEX ON %s(addr.nope) USING 'sai'")).isInstanceOf(InvalidRequestException.class);
    }

    /**
     * Runs the check on the memtable indexes, then on the SSTable indexes after a flush, after a compaction and
     * after a rebuild of the indexes.
     */
    private void assertIndexContent(ThrowingRunnable check) throws Throwable
    {
        mode = Mode.MEMTABLE;
        check.run();
        flush();
        mode = Mode.SSTABLE;
        check.run();
        compact();
        check.run();
        rebuildTableIndexes();
        check.run();
    }

    private enum Mode { MEMTABLE, SSTABLE }

    private Mode mode;

    @FunctionalInterface
    private interface ThrowingRunnable
    {
        void run() throws Throwable;
    }

    private void assertMatches(String index, Object value, int... keys) throws Throwable
    {
        if (mode == Mode.MEMTABLE)
            assertMemtableMatches(index, value, keys);
        else
            assertSSTableMatches(index, value, keys);
    }

    /**
     * Checks the keys matched by the memtable and SSTable indexes together, for checks made right after creating an
     * index, which may or may not flush the memtable.
     */
    private void assertAnyMatches(String index, Object value, int... keys) throws Throwable
    {
        Set<String> found = memtableMatches(index, value);
        found.addAll(sstableMatches(index, value));
        assertThat(found).as("index %s, term %s", index, value).isEqualTo(expected(keys));
    }

    private void assertMemtableMatches(String index, Object value, int... keys) throws Throwable
    {
        assertThat(memtableMatches(index, value)).as("memtable index %s, term %s", index, value).isEqualTo(expected(keys));
    }

    private Set<String> memtableMatches(String index, Object value) throws Throwable
    {
        IndexContext context = getIndexContext(index);
        Expression expression = expression(context, value);
        Set<String> found = new HashSet<>();
        for (MemtableIndex memtableIndex : context.getLiveMemtables().values())
        {
            try (KeyRangeIterator iterator = memtableIndex.search(new QueryContext(), expression, Range.unbounded(context.getPartitioner())))
            {
                collect(context, iterator, found);
            }
        }
        return found;
    }

    private void assertSSTableMatches(String index, Object value, int... keys) throws Throwable
    {
        assertThat(sstableMatches(index, value)).as("SSTable index %s, term %s", index, value).isEqualTo(expected(keys));
    }

    private Set<String> sstableMatches(String index, Object value) throws Throwable
    {
        IndexContext context = getIndexContext(index);
        Expression expression = expression(context, value);
        Set<String> found = new HashSet<>();
        View view = context.getReferencedView(TimeUnit.SECONDS.toNanos(5));
        assertThat(view).isNotNull();
        try
        {
            for (SSTableIndex sstableIndex : view.getIndexes())
            {
                try (KeyRangeIterator iterator = sstableIndex.search(expression, Range.unbounded(context.getPartitioner()), new QueryContext(), false))
                {
                    collect(context, iterator, found);
                }
            }
        }
        finally
        {
            view.release();
        }
        return found;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static Expression expression(IndexContext context, Object value)
    {
        ByteBuffer term = ((AbstractType) context.getValidator()).decompose(value);
        return new Expression(context).add(Operator.EQ, term);
    }

    private static void collect(IndexContext context, KeyRangeIterator iterator, Set<String> found)
    {
        AbstractType<?> keyType = context.keyValidator();
        while (iterator.hasNext())
        {
            ByteBuffer key = iterator.next().partitionKey().getKey();
            // the first component of a composite key, an int in all the tables of this test
            found.add(keyType instanceof CompositeType
                      ? Integer.toString(ByteBufferUtil.toInt(CompositeType.extractComponent(key, 0)))
                      : keyType.getString(key));
        }
    }

    private static Set<String> expected(int... keys)
    {
        Set<String> expected = new HashSet<>();
        for (int key : keys)
            expected.add(Integer.toString(key));
        return expected;
    }

    private long sstableCellCount(String index) throws Throwable
    {
        long count = 0;
        for (Object[] row : getRows(execute("SELECT cell_count FROM system_views.sai_sstable_indexes WHERE keyspace_name = ? AND index_name = ?",
                                            KEYSPACE, index)))
            count += (Long) row[0];
        return count;
    }
}
