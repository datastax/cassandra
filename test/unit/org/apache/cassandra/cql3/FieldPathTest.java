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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.FieldPath.Step;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.serializers.CollectionSerializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.TimeUUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class FieldPathTest
{
    private static final String KS = "ks";

    private static final UserType GEO = udt("geo", false, "lat", DoubleType.instance, "lon", DoubleType.instance);
    private static final SetType<String> TAGS = SetType.getInstance(UTF8Type.instance, false);
    private static final UserType ADDRESS = udt("address", false,
                                                "street", UTF8Type.instance,
                                                "city", UTF8Type.instance,
                                                "zip", Int32Type.instance,
                                                "loc", GEO,
                                                "tags", TAGS);
    private static final UserType PERSON = udt("person", true,
                                               "name", UTF8Type.instance,
                                               "age", Int32Type.instance,
                                               "home", ADDRESS);
    private static final TupleType TUPLE = new TupleType(Arrays.asList(UTF8Type.instance, Int32Type.instance, DoubleType.instance));

    @BeforeClass
    public static void setUp()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    private static UserType udt(String name, boolean multiCell, Object... fields)
    {
        List<FieldIdentifier> names = new ArrayList<>();
        List<AbstractType<?>> types = new ArrayList<>();
        for (int i = 0; i < fields.length; i += 2)
        {
            names.add(FieldIdentifier.forInternalString((String) fields[i]));
            types.add((AbstractType<?>) fields[i + 1]);
        }
        return new UserType(KS, UTF8Type.instance.decompose(name), names, types, multiCell);
    }

    private static List<FieldIdentifier> names(String... names)
    {
        return Arrays.stream(names).map(FieldIdentifier::forUnquoted).collect(Collectors.toList());
    }

    private static FieldPath resolve(AbstractType<?> type, String... names)
    {
        return FieldPath.resolve(type, names(names));
    }

    private static FieldPath path(Step... steps)
    {
        return FieldPath.of(Arrays.asList(steps));
    }

    private static ByteBuffer text(String s)
    {
        return UTF8Type.instance.decompose(s);
    }

    private static ByteBuffer integer(int i)
    {
        return Int32Type.instance.decompose(i);
    }

    private static ByteBuffer real(double d)
    {
        return DoubleType.instance.decompose(d);
    }

    private static ByteBuffer set(ByteBuffer... elements)
    {
        return CollectionSerializer.pack(Arrays.asList(elements), elements.length);
    }

    private static ByteBuffer address(String street, String city, Integer zip, ByteBuffer loc, ByteBuffer tags)
    {
        return TupleType.buildValue(street == null ? null : text(street),
                                    city == null ? null : text(city),
                                    zip == null ? null : integer(zip),
                                    loc,
                                    tags);
    }

    private static List<ByteBuffer> collect(FieldPath path, AbstractType<?> type, ByteBuffer value, boolean unpackLeaf)
    {
        List<ByteBuffer> values = new ArrayList<>();
        path.collectValues(type, value, unpackLeaf, values::add);
        return values;
    }

    private static List<ByteBuffer> collect(FieldPath path, ColumnMetadata column, Row row, long nowInSec, boolean unpackLeaf)
    {
        List<ByteBuffer> values = new ArrayList<>();
        ComplexColumnData data = row.getComplexColumnData(column);
        path.collectValues(column.type, data, nowInSec, unpackLeaf, values::add);
        return values;
    }

    @Test
    public void testResolveFrozenUDT()
    {
        UserType frozen = ADDRESS.freeze();
        FieldPath city = resolve(frozen, "city");
        assertThat(city.steps()).containsExactly(Step.field(1));
        assertThat(city.leafType(frozen)).isEqualTo(UTF8Type.instance);
        assertThat(city.hasElements()).isFalse();
        assertThat(city.isMultiValued(frozen, true)).isFalse();

        FieldPath lat = resolve(frozen, "loc", "lat");
        assertThat(lat.steps()).containsExactly(Step.field(3), Step.field(0));
        assertThat(lat.leafType(frozen)).isEqualTo(DoubleType.instance);

        FieldPath loc = resolve(frozen, "loc");
        assertThat(loc.leafType(frozen)).isEqualTo(GEO);

        FieldPath tags = resolve(frozen, "tags");
        assertThat(tags.leafType(frozen)).isEqualTo(TAGS);
        assertThat(tags.isMultiValued(frozen, true)).isTrue();
        assertThat(tags.isMultiValued(frozen, false)).isFalse();

        // the reversed type of a clustering column is unwrapped
        AbstractType<?> reversed = ReversedType.getInstance(frozen);
        assertThat(resolve(reversed, "city")).isEqualTo(city);
        assertThat(city.leafType(reversed)).isEqualTo(UTF8Type.instance);
    }

    @Test
    public void testResolveNonFrozenAndNestedUDT()
    {
        FieldPath age = resolve(PERSON, "age");
        assertThat(age.steps()).containsExactly(Step.field(1));
        assertThat(age.leafType(PERSON)).isEqualTo(Int32Type.instance);

        FieldPath lat = resolve(PERSON, "home", "loc", "lat");
        assertThat(lat.steps()).containsExactly(Step.field(2), Step.field(3), Step.field(0));
        assertThat(lat.leafType(PERSON)).isEqualTo(DoubleType.instance);
        assertThat(lat.toCQLString(PERSON)).isEqualTo("home.loc.lat");
    }

    @Test
    public void testResolveTuple()
    {
        assertThat(resolve(TUPLE, "field1").steps()).containsExactly(Step.field(0));
        assertThat(resolve(TUPLE, "field3").leafType(TUPLE)).isEqualTo(DoubleType.instance);
        // unquoted identifiers are case-insensitive
        assertThat(FieldPath.resolve(TUPLE, Arrays.asList(FieldIdentifier.forUnquoted("Field2"))).steps()).containsExactly(Step.field(1));
        assertThat(resolve(TUPLE, "field2").toCQLString(TUPLE)).isEqualTo("field2");
        assertThat(FieldPath.tupleFieldName(0)).isEqualTo("field1");

        assertThatThrownBy(() -> resolve(TUPLE, "field0")).isInstanceOf(InvalidRequestException.class)
                                                         .hasMessageContaining("Undefined field field0")
                                                         .hasMessageContaining("field1 to field3");
        assertThatThrownBy(() -> resolve(TUPLE, "field4")).isInstanceOf(InvalidRequestException.class)
                                                         .hasMessageContaining("Undefined field field4");
        assertThatThrownBy(() -> resolve(TUPLE, "field01")).isInstanceOf(InvalidRequestException.class);
        assertThatThrownBy(() -> resolve(TUPLE, "first")).isInstanceOf(InvalidRequestException.class);
        assertThatThrownBy(() -> FieldPath.resolve(TUPLE, Arrays.asList(FieldIdentifier.forQuoted("Field1"))))
        .isInstanceOf(InvalidRequestException.class);

        // a UDT field named like a tuple component is a plain UDT field
        UserType udt = udt("t", false, "field2", UTF8Type.instance, "field1", Int32Type.instance);
        assertThat(resolve(udt, "field1").steps()).containsExactly(Step.field(1));

        // a tuple of a tuple
        TupleType nested = new TupleType(Arrays.asList(Int32Type.instance, TUPLE));
        assertThat(resolve(nested, "field2", "field1").steps()).containsExactly(Step.field(1), Step.field(0));
    }

    @Test
    public void testResolveCollections()
    {
        UserType frozen = ADDRESS.freeze();
        for (AbstractType<?> type : Arrays.asList(ListType.getInstance(frozen, true),
                                                  ListType.getInstance(frozen, false),
                                                  SetType.getInstance(frozen, true),
                                                  SetType.getInstance(frozen, false),
                                                  MapType.getInstance(UTF8Type.instance, frozen, true),
                                                  MapType.getInstance(Int32Type.instance, frozen, false)))
        {
            FieldPath city = resolve(type, "city");
            assertThat(city.steps()).containsExactly(Step.ELEMENTS, Step.field(1));
            assertThat(city.hasElements()).isTrue();
            assertThat(city.isMultiValued(type, false)).isTrue();
            assertThat(city.leafType(type)).isEqualTo(UTF8Type.instance);
            assertThat(city.toCQLString(type)).isEqualTo("city");
            assertThat(city.names(type)).isEqualTo(names("city"));
        }

        // a collection inside a UDT inside a collection
        UserType name = udt("name", false, "first", UTF8Type.instance, "last", UTF8Type.instance);
        UserType family = udt("family", false, "names", ListType.getInstance(name, false)).freeze();
        AbstractType<?> families = ListType.getInstance(family, true);
        assertThat(resolve(families, "names", "first").steps()).containsExactly(Step.ELEMENTS, Step.field(0), Step.ELEMENTS, Step.field(0));

        // list of tuples
        AbstractType<?> tuples = ListType.getInstance(new TupleType(Arrays.asList(UTF8Type.instance, Int32Type.instance)), true);
        assertThat(resolve(tuples, "field2").steps()).containsExactly(Step.ELEMENTS, Step.field(1));
    }

    @Test
    public void testResolveErrors()
    {
        UserType frozen = ADDRESS.freeze();
        assertThatThrownBy(() -> resolve(frozen, "country")).isInstanceOf(InvalidRequestException.class)
                                                           .hasMessage("Undefined field country in the column of type address");
        assertThatThrownBy(() -> resolve(frozen, "loc", "alt")).isInstanceOf(InvalidRequestException.class)
                                                              .hasMessage("Undefined field alt in field loc of type geo");
        assertThatThrownBy(() -> resolve(frozen, "city", "x")).isInstanceOf(InvalidRequestException.class)
                                                             .hasMessage("Cannot access field x of field city of type text, which is neither a user-defined type nor a tuple");
        assertThatThrownBy(() -> resolve(Int32Type.instance, "x")).isInstanceOf(InvalidRequestException.class)
                                                                 .hasMessageContaining("neither a user-defined type nor a tuple");
        assertThatThrownBy(() -> resolve(frozen, "tags", "x")).isInstanceOf(InvalidRequestException.class)
                                                             .hasMessageContaining("neither a user-defined type nor a tuple");
        assertThatThrownBy(() -> resolve(ListType.getInstance(Int32Type.instance, true), "x")).isInstanceOf(InvalidRequestException.class)
                                                                                             .hasMessageContaining("neither a user-defined type nor a tuple");
        assertThatThrownBy(() -> FieldPath.resolve(frozen, new ArrayList<>())).isInstanceOf(InvalidRequestException.class);

        // map keys can't be traversed
        assertThatThrownBy(() -> resolve(MapType.getInstance(frozen, Int32Type.instance, true), "city"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("field paths through the keys of a map are not supported");
        // when both keys and values are UDTs, the values are traversed
        assertThat(resolve(MapType.getInstance(GEO.freeze(), frozen, true), "city").steps()).containsExactly(Step.ELEMENTS, Step.field(1));
    }

    @Test
    public void testNamesAndQuoting()
    {
        UserType udt = udt("q", false, "Camel Case", UTF8Type.instance, "select", Int32Type.instance, "plain", GEO);
        FieldPath path = FieldPath.resolve(udt, Arrays.asList(FieldIdentifier.forQuoted("Camel Case")));
        assertThat(path.toCQLString(udt)).isEqualTo("\"Camel Case\"");
        assertThat(FieldPath.resolve(udt, Arrays.asList(FieldIdentifier.forQuoted("select"))).toCQLString(udt)).isEqualTo("\"select\"");
        assertThat(resolve(udt, "plain", "lat").toCQLString(udt)).isEqualTo("plain.lat");
        assertThat(path.names(udt)).containsExactly(FieldIdentifier.forQuoted("Camel Case"));
    }

    @Test
    public void testEqualsAndSteps()
    {
        UserType frozen = ADDRESS.freeze();
        assertThat(resolve(frozen, "loc", "lat")).isEqualTo(path(Step.field(3), Step.field(0)));
        assertThat(resolve(frozen, "loc", "lat").hashCode()).isEqualTo(path(Step.field(3), Step.field(0)).hashCode());
        assertThat(resolve(frozen, "loc", "lat")).isNotEqualTo(path(Step.field(3), Step.field(1)));
        assertThat(path(Step.ELEMENTS, Step.field(1))).isNotEqualTo(path(Step.field(1)));
        assertThat(path(Step.ELEMENTS, Step.field(1)).toString()).isEqualTo("[*, 1]");
        assertThatThrownBy(() -> Step.field(-1)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testForEachUserTypeField()
    {
        List<String> visited = new ArrayList<>();
        resolve(PERSON, "home", "loc", "lat").forEachUserTypeField(PERSON, (type, position) -> visited.add(type.getNameAsString() + '.' + type.fieldNameAsString(position)));
        assertThat(visited).containsExactly("person.home", "address.loc", "geo.lat");

        visited.clear();
        TupleType tuple = new TupleType(Arrays.asList(Int32Type.instance, GEO));
        resolve(tuple, "field2", "lon").forEachUserTypeField(tuple, (type, position) -> visited.add(type.getNameAsString() + '.' + type.fieldNameAsString(position)));
        assertThat(visited).containsExactly("geo.lon");
    }

    @Test
    public void testExtractFromFrozenUDT()
    {
        UserType frozen = ADDRESS.freeze();
        ByteBuffer tags = set(text("a"), text("b"));
        ByteBuffer value = address("main st", "Rome", 100, TupleType.buildValue(real(1.0), real(2.0)), tags);

        assertThat(collect(resolve(frozen, "city"), frozen, value, false)).containsExactly(text("Rome"));
        assertThat(collect(resolve(frozen, "zip"), frozen, value, false)).containsExactly(integer(100));
        assertThat(collect(resolve(frozen, "loc", "lon"), frozen, value, false)).containsExactly(real(2.0));
        assertThat(collect(resolve(frozen, "loc"), frozen, value, false)).containsExactly(TupleType.buildValue(real(1.0), real(2.0)));
        // a collection leaf: the whole value, or its elements
        assertThat(collect(resolve(frozen, "tags"), frozen, value, false)).containsExactly(tags);
        assertThat(collect(resolve(frozen, "tags"), frozen, value, true)).containsExactly(text("a"), text("b"));

        // null fields give no value
        ByteBuffer nulls = address("b st", null, null, null, null);
        assertThat(collect(resolve(frozen, "city"), frozen, nulls, false)).isEmpty();
        assertThat(collect(resolve(frozen, "loc", "lat"), frozen, nulls, false)).isEmpty();
        assertThat(collect(resolve(frozen, "tags"), frozen, nulls, true)).isEmpty();
        assertThat(collect(resolve(frozen, "street"), frozen, nulls, false)).containsExactly(text("b st"));

        // an empty string is a value
        assertThat(collect(resolve(frozen, "city"), frozen, address("x", "", null, null, null), false)).containsExactly(text(""));

        // no value at all
        assertThat(collect(resolve(frozen, "city"), frozen, null, false)).isEmpty();
    }

    @Test
    public void testExtractShortAndLongValues()
    {
        UserType frozen = ADDRESS.freeze();
        // written before fields were added to the type: the missing trailing fields are null
        ByteBuffer shortValue = TupleType.buildValue(text("s"), text("Rome"));
        assertThat(collect(resolve(frozen, "city"), frozen, shortValue, false)).containsExactly(text("Rome"));
        assertThat(collect(resolve(frozen, "zip"), frozen, shortValue, false)).isEmpty();
        assertThat(collect(resolve(frozen, "loc", "lat"), frozen, shortValue, false)).isEmpty();
        assertThat(collect(resolve(frozen, "tags"), frozen, shortValue, true)).isEmpty();
        assertThat(collect(resolve(frozen, "street"), frozen, ByteBufferUtil.EMPTY_BYTE_BUFFER, false)).isEmpty();
        // a nested value with fewer fields
        ByteBuffer shortLoc = address("s", "Rome", 1, TupleType.buildValue(real(1.0)), null);
        assertThat(collect(resolve(frozen, "loc", "lat"), frozen, shortLoc, false)).containsExactly(real(1.0));
        assertThat(collect(resolve(frozen, "loc", "lon"), frozen, shortLoc, false)).isEmpty();

        // written after fields were added to the type: the values of the known fields are read
        ByteBuffer longValue = TupleType.buildValue(text("s"), text("Rome"), integer(1), null, null, text("Italy"));
        assertThat(collect(resolve(frozen, "zip"), frozen, longValue, false)).containsExactly(integer(1));
    }

    @Test
    public void testExtractFromTuple()
    {
        ByteBuffer value = TupleType.buildValue(text("foo"), integer(1), real(1.5));
        assertThat(collect(resolve(TUPLE, "field1"), TUPLE, value, false)).containsExactly(text("foo"));
        assertThat(collect(resolve(TUPLE, "field2"), TUPLE, value, false)).containsExactly(integer(1));
        assertThat(collect(resolve(TUPLE, "field3"), TUPLE, value, false)).containsExactly(real(1.5));
    }

    @Test
    public void testExtractFromFrozenCollections()
    {
        UserType frozen = ADDRESS.freeze();
        ByteBuffer rome = address("a", "Rome", 200, TupleType.buildValue(real(1.0), real(0.0)), set(text("x")));
        ByteBuffer milan = address("b", "Milan", 100, null, set(text("y"), text("z")));
        ByteBuffer list = CollectionSerializer.pack(Arrays.asList(rome, milan, rome), 3);

        AbstractType<?> listType = ListType.getInstance(frozen, false);
        assertThat(collect(resolve(listType, "city"), listType, list, false)).containsExactly(text("Rome"), text("Milan"), text("Rome"));
        assertThat(collect(resolve(listType, "loc", "lat"), listType, list, false)).containsExactly(real(1.0), real(1.0));
        assertThat(collect(resolve(listType, "tags"), listType, list, true)).containsExactly(text("x"), text("y"), text("z"), text("x"));

        AbstractType<?> mapType = MapType.getInstance(UTF8Type.instance, frozen, false);
        ByteBuffer map = CollectionSerializer.pack(Arrays.asList(text("home"), rome, text("work"), milan), 2);
        assertThat(collect(resolve(mapType, "zip"), mapType, map, false)).containsExactly(integer(200), integer(100));

        // a frozen map leaf, indexed by its values
        UserType withMap = udt("wm", false, "m", MapType.getInstance(UTF8Type.instance, Int32Type.instance, false)).freeze();
        ByteBuffer withMapValue = TupleType.buildValue(CollectionSerializer.pack(Arrays.asList(text("k1"), integer(1), text("k2"), integer(2)), 2));
        assertThat(collect(resolve(withMap, "m"), withMap, withMapValue, true)).containsExactly(integer(1), integer(2));

        // an empty collection
        assertThat(collect(resolve(listType, "city"), listType, CollectionSerializer.pack(new ArrayList<>(), 0), false)).isEmpty();
    }

    @Test
    public void testExtractFromNonFrozenUDT()
    {
        ColumnMetadata column = ColumnMetadata.regularColumn(KS, "t", "p", PERSON);
        long now = 1000;
        ByteBuffer home = address("a", "Rome", 1, null, null);
        Row row = row(BufferCell.live(column, 1, text("Ann"), fieldPath(0)),
                      BufferCell.tombstone(column, 1, now - 1, fieldPath(1)),
                      BufferCell.live(column, 1, home, fieldPath(2)));

        assertThat(collect(resolve(PERSON, "name"), column, row, now, false)).containsExactly(text("Ann"));
        assertThat(collect(resolve(PERSON, "age"), column, row, now, false)).isEmpty();
        assertThat(collect(resolve(PERSON, "home", "city"), column, row, now, false)).containsExactly(text("Rome"));
        assertThat(collect(resolve(PERSON, "home", "zip"), column, row, now, false)).containsExactly(integer(1));
        assertThat(collect(resolve(PERSON, "home", "loc", "lat"), column, row, now, false)).isEmpty();

        // a row with only some of the fields, as written by an UPDATE of a single field
        Row update = row(BufferCell.live(column, 2, integer(41), fieldPath(1)));
        assertThat(collect(resolve(PERSON, "age"), column, update, now, false)).containsExactly(integer(41));
        assertThat(collect(resolve(PERSON, "name"), column, update, now, false)).isEmpty();
    }

    @Test
    public void testExtractFromNonFrozenCollections()
    {
        UserType frozen = ADDRESS.freeze();
        ByteBuffer rome = address("a", "Rome", 200, null, null);
        ByteBuffer milan = address("b", "Milan", 100, null, null);
        long now = 1000;

        ColumnMetadata list = ColumnMetadata.regularColumn(KS, "t", "l", ListType.getInstance(frozen, true));
        Row listRow = row(BufferCell.live(list, 1, rome, CellPath.create(TimeUUID.Generator.nextTimeUUID().toBytes())),
                          BufferCell.live(list, 1, milan, CellPath.create(TimeUUID.Generator.nextTimeUUID().toBytes())),
                          BufferCell.tombstone(list, 1, now - 1, CellPath.create(TimeUUID.Generator.nextTimeUUID().toBytes())));
        assertThat(collect(resolve(list.type, "city"), list, listRow, now, false)).containsExactly(text("Rome"), text("Milan"));

        ColumnMetadata set = ColumnMetadata.regularColumn(KS, "t", "s", SetType.getInstance(frozen, true));
        Row setRow = row(BufferCell.live(set, 1, ByteBufferUtil.EMPTY_BYTE_BUFFER, CellPath.create(rome)),
                         BufferCell.live(set, 1, ByteBufferUtil.EMPTY_BYTE_BUFFER, CellPath.create(milan)));
        assertThat(collect(resolve(set.type, "zip"), set, setRow, now, false)).containsExactlyInAnyOrder(integer(200), integer(100));

        ColumnMetadata map = ColumnMetadata.regularColumn(KS, "t", "m", MapType.getInstance(UTF8Type.instance, frozen, true));
        Row mapRow = row(BufferCell.live(map, 1, rome, CellPath.create(text("home"))),
                         BufferCell.live(map, 1, milan, CellPath.create(text("work"))));
        assertThat(collect(resolve(map.type, "city"), map, mapRow, now, false)).containsExactly(text("Rome"), text("Milan"));

        // no cells
        List<ByteBuffer> values = new ArrayList<>();
        resolve(map.type, "city").collectValues(map.type, null, now, false, values::add);
        assertThat(values).isEmpty();
    }

    @Test
    public void testSingleValue()
    {
        UserType frozen = ADDRESS.freeze();
        ByteBuffer value = address("main st", "Rome", 100, TupleType.buildValue(real(1.0), real(2.0)), set(text("a")));
        for (String[] names : new String[][]{ { "street" }, { "city" }, { "zip" }, { "loc" }, { "loc", "lon" }, { "tags" } })
        {
            FieldPath path = resolve(frozen, names);
            List<ByteBuffer> expected = collect(path, frozen, value, false);
            assertThat(path.singleValue(value)).isEqualTo(expected.get(0));
        }
        // short, null and missing values
        assertThat(resolve(frozen, "loc", "lat").singleValue(address("s", null, null, null, null))).isNull();
        assertThat(resolve(frozen, "zip").singleValue(TupleType.buildValue(text("s")))).isNull();
        assertThat(resolve(frozen, "zip").singleValue(null)).isNull();
        // a value inside a larger buffer
        ByteBuffer padded = ByteBuffer.allocate(value.remaining() + 8);
        padded.putInt(42).put(value.duplicate()).putInt(43);
        padded.position(4).limit(4 + value.remaining());
        assertThat(resolve(frozen, "loc", "lon").singleValue(padded.slice())).isEqualTo(real(2.0));
        assertThat(resolve(frozen, "loc", "lon").singleValue(padded)).isEqualTo(real(2.0));

        ColumnMetadata column = ColumnMetadata.regularColumn(KS, "t", "p", PERSON);
        Row row = row(BufferCell.live(column, 1, text("Ann"), fieldPath(0)),
                      BufferCell.live(column, 1, address("a", "Rome", 1, null, null), fieldPath(2)));
        assertThat(resolve(PERSON, "name").singleValue(row.getComplexColumnData(column), 1000)).isEqualTo(text("Ann"));
        assertThat(resolve(PERSON, "age").singleValue(row.getComplexColumnData(column), 1000)).isNull();
        assertThat(resolve(PERSON, "home", "city").singleValue(row.getComplexColumnData(column), 1000)).isEqualTo(text("Rome"));
        assertThat(resolve(PERSON, "home", "city").singleValue(null, 1000)).isNull();

        assertThatThrownBy(() -> resolve(ListType.getInstance(frozen, false), "city").singleValue(value)).isInstanceOf(AssertionError.class);
    }

    @Test
    public void testMalformedValues()
    {
        UserType frozen = ADDRESS.freeze();
        // a component size larger than the value, close to Integer.MAX_VALUE (no int overflow)
        ByteBuffer huge = ByteBuffer.allocate(8);
        huge.putInt(Integer.MAX_VALUE - 2).putInt(0).flip();
        assertThatThrownBy(() -> collect(resolve(frozen, "street"), frozen, huge, false)).isInstanceOf(MarshalException.class);
        assertThatThrownBy(() -> resolve(frozen, "street").singleValue(huge)).isInstanceOf(MarshalException.class);
        // a truncated size
        ByteBuffer truncated = ByteBuffer.allocate(2);
        assertThatThrownBy(() -> resolve(frozen, "street").singleValue(truncated)).isInstanceOf(MarshalException.class);
    }

    private static CellPath fieldPath(int position)
    {
        return CellPath.create(ByteBufferUtil.bytes((short) position));
    }

    private static Row row(BufferCell... cells)
    {
        Row.Builder builder = BTreeRow.unsortedBuilder();
        builder.newRow(Clustering.EMPTY);
        for (BufferCell cell : cells)
            builder.addCell(cell);
        return builder.build();
    }
}
