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

package org.apache.cassandra.db.filter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.FieldIdentifier;
import org.apache.cassandra.cql3.FieldPath;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.Redaction;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.db.partitions.SingletonUnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.InvalidPatternException;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.btree.BTree;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RowFilterTest
{
    @Test
    public void testRowFilterClose()
    {
        // CASSANDRA-15126
        TableMetadata metadata = TableMetadata.builder("testks", "testcf")
                                              .addPartitionKeyColumn("pk", Int32Type.instance)
                                              .addStaticColumn("s", Int32Type.instance)
                                              .addRegularColumn("r", Int32Type.instance)
                                              .build();
        ColumnMetadata s = metadata.getColumn(new ColumnIdentifier("s", true));
        ColumnMetadata r = metadata.getColumn(new ColumnIdentifier("r", true));

        ByteBuffer one = Int32Type.instance.decompose(1);
        RowFilter.Builder filter = RowFilter.builder(false);
        filter.add(s, Operator.NEQ, one);
        AtomicBoolean closed = new AtomicBoolean();
        UnfilteredPartitionIterator iter = filter.build().filter(new SingletonUnfilteredPartitionIterator(new UnfilteredRowIterator()
        {
            public DeletionTime partitionLevelDeletion() { return null; }
            public EncodingStats stats() { return null; }
            public TableMetadata metadata() { return metadata; }
            public boolean isReverseOrder() { return false; }
            public RegularAndStaticColumns columns() { return null; }
            public DecoratedKey partitionKey() { return null; }
            public boolean hasNext() { return false; }
            public Unfiltered next() { return null; }
            public Row staticRow()
            {
                return BTreeRow.create(Clustering.STATIC_CLUSTERING,
                                       LivenessInfo.EMPTY,
                                       Row.Deletion.LIVE,
                                       BTree.singleton(new BufferCell(s, 1, Cell.NO_TTL, Cell.NO_DELETION_TIME, one, null)));
            }
            public void close()
            {
                closed.set(true);
            }
        }), 1);
        assertFalse(iter.hasNext());
        assertTrue(closed.get());

        filter = RowFilter.builder(false);
        filter.add(r, Operator.NEQ, one);
        closed.set(false);
        iter = filter.build().filter(new SingletonUnfilteredPartitionIterator(new UnfilteredRowIterator()
        {
            boolean hasNext = true;
            public DeletionTime partitionLevelDeletion() { return null; }
            public EncodingStats stats() { return null; }
            public TableMetadata metadata() { return metadata; }
            public boolean isReverseOrder() { return false; }
            public RegularAndStaticColumns columns() { return null; }
            public DecoratedKey partitionKey() { return null; }
            public Row staticRow() { return Rows.EMPTY_STATIC_ROW; }
            public boolean hasNext()
            {
                boolean r = hasNext;
                hasNext = false;
                return r;
            }
            public Unfiltered next()
            {
                return BTreeRow.create(Clustering.EMPTY,
                                       LivenessInfo.EMPTY,
                                       Row.Deletion.LIVE,
                                       BTree.singleton(new BufferCell(r, 1, Cell.NO_TTL, Cell.NO_DELETION_TIME, one, null)));
            }
            public void close()
            {
                closed.set(true);
            }
        }), 1);
        assertFalse(iter.hasNext());
        assertTrue(closed.get());
    }

    @Test
    public void testMutableIntersections()
    {
        TableMetadata metadata = TableMetadata.builder("testks", "testcf")
                                              .addPartitionKeyColumn("pk", Int32Type.instance)
                                              .addRegularColumn("r", Int32Type.instance)
                                              .addRegularColumn("t", UTF8Type.instance)
                                              .build();

        // Empty filter - no expressions
        RowFilter filter = RowFilter.builder(false).build();
        assertFalse(filter.isMutableIntersection());
        
        ColumnMetadata r = metadata.getColumn(new ColumnIdentifier("r", true));
        
        // Single expression on one column
        RowFilter.Builder builder = RowFilter.builder(false);
        builder.add(r, Operator.GT, ByteBufferUtil.EMPTY_BYTE_BUFFER);
        filter = builder.build();
        assertFalse(filter.isMutableIntersection());

        // Two expressions on the same column
        builder = RowFilter.builder(false);
        builder.add(r, Operator.GT, ByteBufferUtil.EMPTY_BYTE_BUFFER);
        builder.add(r, Operator.LT, ByteBufferUtil.EMPTY_BYTE_BUFFER);
        filter = builder.build();
        assertFalse(filter.isMutableIntersection());

        // Three expressions on two different columns - this is a mutable intersection
        ColumnMetadata t = metadata.getColumn(new ColumnIdentifier("t", true));
        builder = RowFilter.builder(false);
        builder.add(r, Operator.GT, ByteBufferUtil.EMPTY_BYTE_BUFFER);
        builder.add(r, Operator.LT, ByteBufferUtil.EMPTY_BYTE_BUFFER);
        builder.add(t, Operator.EQ, ByteBufferUtil.EMPTY_BYTE_BUFFER);
        filter = builder.build();
        assertTrue(filter.isMutableIntersection());
    }

    /**
     * A LIKE pattern that can't be compiled into an automaton fails the filtering with an invalid pattern error naming
     * the restricted column, which the operator evaluating the pattern doesn't know.
     */
    @Test
    public void testInvalidLikePatternNamesColumn()
    {
        TableMetadata metadata = TableMetadata.builder("testks", "testcf")
                                              .addPartitionKeyColumn("pk", Int32Type.instance)
                                              .addRegularColumn("v", UTF8Type.instance)
                                              .build();
        ColumnMetadata v = metadata.getColumn(new ColumnIdentifier("v", true));

        // a generic pattern too complex to determinize
        StringBuilder pattern = new StringBuilder();
        for (int i = 0; i < 1000; i++)
            pattern.append("a%");
        pattern.append('a');

        RowFilter.Builder builder = RowFilter.builder(false);
        builder.add(v, Operator.LIKE_MATCHES, UTF8Type.instance.decompose(pattern.toString()));
        RowFilter filter = builder.build();
        Row row = BTreeRow.create(Clustering.EMPTY,
                                  LivenessInfo.EMPTY,
                                  Row.Deletion.LIVE,
                                  BTree.singleton(new BufferCell(v, 1, Cell.NO_TTL, Cell.NO_DELETION_TIME, UTF8Type.instance.decompose("abc"), null)));
        DecoratedKey key = Murmur3Partitioner.instance.decorateKey(Int32Type.instance.decompose(1));

        assertThatThrownBy(() -> filter.isSatisfiedBy(metadata, key, row, 1))
            .isInstanceOf(InvalidPatternException.class)
            .hasMessageStartingWith("Invalid pattern for LIKE '<term>' restriction on column v: ");
    }

    private static final UserType GEO = new UserType("testks", UTF8Type.instance.decompose("geo"),
                                                     Arrays.asList(FieldIdentifier.forUnquoted("lat"), FieldIdentifier.forUnquoted("lon")),
                                                     Arrays.asList(DoubleType.instance, DoubleType.instance), false);
    private static final UserType ADDRESS = new UserType("testks", UTF8Type.instance.decompose("address"),
                                                         Arrays.asList(FieldIdentifier.forUnquoted("city"),
                                                                       FieldIdentifier.forUnquoted("zip"),
                                                                       FieldIdentifier.forUnquoted("loc"),
                                                                       FieldIdentifier.forUnquoted("tags")),
                                                         Arrays.asList(UTF8Type.instance, Int32Type.instance, GEO.freeze(),
                                                                       SetType.getInstance(UTF8Type.instance, false)),
                                                         false);
    private static final TupleType TUPLE = new TupleType(Arrays.asList(UTF8Type.instance, Int32Type.instance));

    private static TableMetadata fieldsTable()
    {
        return TableMetadata.builder("testks", "testcf")
                            .addPartitionKeyColumn("pk", Int32Type.instance)
                            .addClusteringColumn("c", TUPLE)
                            .addRegularColumn("addr", ADDRESS.freeze())
                            .addRegularColumn("t", TUPLE)
                            .build();
    }

    private static FieldPath path(AbstractType<?> type, String... names)
    {
        List<FieldIdentifier> identifiers = new ArrayList<>();
        for (String name : names)
            identifiers.add(FieldIdentifier.forUnquoted(name));
        return FieldPath.resolve(type, identifiers);
    }

    private static ByteBuffer address(String city, Integer zip, Double lat)
    {
        ByteBuffer loc = lat == null ? null : TupleType.buildValue(new ByteBuffer[]{ DoubleType.instance.decompose(lat), null });
        return TupleType.buildValue(new ByteBuffer[]{ city == null ? null : UTF8Type.instance.decompose(city),
                                                      zip == null ? null : Int32Type.instance.decompose(zip),
                                                      loc,
                                                      SetType.getInstance(UTF8Type.instance, false).decompose(new java.util.HashSet<>(Arrays.asList("x", "y"))) });
    }

    /**
     * The expressions on the fields of UDT and tuple columns (kind FIELD) survive a serialization round trip, and
     * their serialized size is the number of bytes written.
     */
    @Test
    public void testFieldExpressionSerialization() throws IOException
    {
        TableMetadata metadata = fieldsTable();
        ColumnMetadata addr = metadata.getColumn(new ColumnIdentifier("addr", true));
        ColumnMetadata t = metadata.getColumn(new ColumnIdentifier("t", true));
        ColumnMetadata c = metadata.getColumn(new ColumnIdentifier("c", true));

        RowFilter.Builder builder = RowFilter.builder(false);
        builder.add(addr, path(addr.type, "city"), Operator.EQ, UTF8Type.instance.decompose("Rome"));
        builder.add(addr, path(addr.type, "zip"), Operator.GT, Int32Type.instance.decompose(100));
        builder.add(addr, path(addr.type, "loc", "lat"), Operator.LTE, DoubleType.instance.decompose(2.5));
        builder.add(addr, path(addr.type, "tags"), Operator.CONTAINS, UTF8Type.instance.decompose("x"));
        builder.add(addr, path(addr.type, "city"), Operator.LIKE_PREFIX, UTF8Type.instance.decompose("Ro"));
        builder.add(addr, path(addr.type, "zip"), Operator.IN, ListType.getInstance(Int32Type.instance, false).decompose(Arrays.asList(1, 2)));
        builder.add(t, path(t.type, "field2"), Operator.NEQ, Int32Type.instance.decompose(7));
        builder.add(c, path(c.type, "field1"), Operator.EQ, UTF8Type.instance.decompose("a"));
        // an expression on a whole column, and the null path of the builder
        builder.add(t, null, Operator.EQ, TupleType.buildValue(new ByteBuffer[]{ UTF8Type.instance.decompose("a"), null }));
        RowFilter filter = builder.build();

        int version = MessagingService.current_version;
        for (RowFilter.Expression expression : filter.expressions())
        {
            try (DataOutputBuffer out = new DataOutputBuffer())
            {
                RowFilter.Expression.serializer.serialize(expression, out, version);
                assertThat(out.getLength()).isEqualTo(RowFilter.Expression.serializer.serializedSize(expression, version));
                try (DataInputBuffer in = new DataInputBuffer(out.buffer(), false))
                {
                    RowFilter.Expression deserialized = RowFilter.Expression.serializer.deserialize(in, version, metadata, IndexHints.NONE);
                    assertThat(in.available()).isZero();
                    assertThat(deserialized).isEqualTo(expression);
                    assertThat(deserialized.fieldPath()).isEqualTo(expression.fieldPath());
                    assertThat(deserialized.getClass()).isEqualTo(expression.getClass());
                    assertThat(deserialized.toString()).isEqualTo(expression.toString());
                }
            }
        }

        // the kind byte, then the column, the operator, the path and the value
        RowFilter.Expression city = filter.expressions().get(0);
        assertThat(city).isInstanceOf(RowFilter.FieldExpression.class);
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            RowFilter.Expression.serializer.serialize(city, out, version);
            ByteBuffer bytes = out.buffer();
            assertThat(bytes.get(0)).isEqualTo((byte) 5);
        }

        // the CQL forms name the fields
        assertThat(filter.expressions().get(0).toString()).isEqualTo("addr.city = 'Rome'");
        assertThat(filter.expressions().get(2).toString()).isEqualTo("addr.loc.lat <= 2.5");
        assertThat(filter.expressions().get(4).toString()).isEqualTo("addr.city LIKE 'Ro%'");
        assertThat(filter.expressions().get(6).toString()).isEqualTo("t.field2 != 7");

        // the expressions on different fields or on the whole column differ
        assertThat(filter.expressions().get(0)).isNotEqualTo(new RowFilter.SimpleExpression(addr, Operator.EQ, UTF8Type.instance.decompose("Rome"), null, null));
        assertThat(new RowFilter.FieldExpression(t, path(t.type, "field1"), Operator.EQ, Int32Type.instance.decompose(7), null))
            .isNotEqualTo(new RowFilter.FieldExpression(t, path(t.type, "field2"), Operator.EQ, Int32Type.instance.decompose(7), null));

        // a whole filter
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            RowFilter.FilterElement.serializer.serialize(filter.root(), out, version);
            assertThat(out.getLength()).isEqualTo(RowFilter.FilterElement.serializer.serializedSize(filter.root(), version));
            try (DataInputBuffer in = new DataInputBuffer(out.buffer(), false))
            {
                RowFilter.FilterElement deserialized = RowFilter.FilterElement.serializer.deserialize(in, version, metadata, IndexHints.NONE);
                assertThat(deserialized.expressions()).isEqualTo(filter.root().expressions());
            }
        }
    }

    /**
     * A field path that doesn't match the type of the column on the receiving node fails the deserialization.
     */
    @Test
    public void testFieldExpressionWithInvalidPath() throws IOException
    {
        TableMetadata metadata = fieldsTable();
        ColumnMetadata t = metadata.getColumn(new ColumnIdentifier("t", true));
        TableMetadata wider = TableMetadata.builder("testks", "testcf")
                                           .addPartitionKeyColumn("pk", Int32Type.instance)
                                           .addRegularColumn("t", new TupleType(Arrays.asList(UTF8Type.instance, Int32Type.instance, Int32Type.instance)))
                                           .build();
        ColumnMetadata wideT = wider.getColumn(new ColumnIdentifier("t", true));
        RowFilter.Expression expression = new RowFilter.FieldExpression(wideT, path(wideT.type, "field3"), Operator.EQ, Int32Type.instance.decompose(1), null);
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            RowFilter.Expression.serializer.serialize(expression, out, MessagingService.current_version);
            try (DataInputBuffer in = new DataInputBuffer(out.buffer(), false))
            {
                assertThatThrownBy(() -> RowFilter.Expression.serializer.deserialize(in, MessagingService.current_version, metadata, IndexHints.NONE))
                    .hasMessageContaining("Invalid field path");
            }
        }
        assertThat(t).isNotNull();
    }

    /**
     * The filtering of the expressions on fields, with and without index: the value of the field is compared with
     * the value of the expression, an unset field matches nothing.
     */
    @Test
    public void testFieldExpressionIsSatisfiedBy()
    {
        TableMetadata metadata = fieldsTable();
        ColumnMetadata addr = metadata.getColumn(new ColumnIdentifier("addr", true));
        ColumnMetadata c = metadata.getColumn(new ColumnIdentifier("c", true));
        DecoratedKey key = Murmur3Partitioner.instance.decorateKey(Int32Type.instance.decompose(1));
        Clustering<?> clustering = Clustering.make(TupleType.buildValue(new ByteBuffer[]{ UTF8Type.instance.decompose("a"), Int32Type.instance.decompose(1) }));

        Row rome = BTreeRow.create(clustering, LivenessInfo.EMPTY, Row.Deletion.LIVE,
                                   BTree.singleton(new BufferCell(addr, 1, Cell.NO_TTL, Cell.NO_DELETION_TIME, address("Rome", 200, 1.0), null)));
        Row noZip = BTreeRow.create(clustering, LivenessInfo.EMPTY, Row.Deletion.LIVE,
                                    BTree.singleton(new BufferCell(addr, 1, Cell.NO_TTL, Cell.NO_DELETION_TIME, address("Paris", null, null), null)));
        Row noAddr = BTreeRow.emptyRow(clustering);

        RowFilter.Expression cityEq = new RowFilter.FieldExpression(addr, path(addr.type, "city"), Operator.EQ, UTF8Type.instance.decompose("Rome"), null);
        assertTrue(cityEq.isSatisfiedBy(metadata, key, rome, 1));
        assertFalse(cityEq.isSatisfiedBy(metadata, key, noZip, 1));
        assertFalse(cityEq.isSatisfiedBy(metadata, key, noAddr, 1));

        RowFilter.Expression zipNeq = new RowFilter.FieldExpression(addr, path(addr.type, "zip"), Operator.NEQ, Int32Type.instance.decompose(100), null);
        assertTrue(zipNeq.isSatisfiedBy(metadata, key, rome, 1));
        assertFalse(zipNeq.isSatisfiedBy(metadata, key, noZip, 1));

        RowFilter.Expression latRange = new RowFilter.FieldExpression(addr, path(addr.type, "loc", "lat"), Operator.GT, DoubleType.instance.decompose(0.5), null);
        assertTrue(latRange.isSatisfiedBy(metadata, key, rome, 1));
        assertFalse(latRange.isSatisfiedBy(metadata, key, noZip, 1));

        RowFilter.Expression tags = new RowFilter.FieldExpression(addr, path(addr.type, "tags"), Operator.CONTAINS, UTF8Type.instance.decompose("y"), null);
        assertTrue(tags.isSatisfiedBy(metadata, key, rome, 1));
        RowFilter.Expression notTags = new RowFilter.FieldExpression(addr, path(addr.type, "tags"), Operator.NOT_CONTAINS, UTF8Type.instance.decompose("z"), null);
        assertTrue(notTags.isSatisfiedBy(metadata, key, rome, 1));
        assertTrue(notTags.isSatisfiedBy(metadata, key, noAddr, 1));

        // a field of a clustering column, also through the clustering-only check
        RowFilter.Expression clusteringField = new RowFilter.FieldExpression(c, path(c.type, "field2"), Operator.GTE, Int32Type.instance.decompose(1), null);
        assertTrue(clusteringField.isSatisfiedBy(metadata, key, noAddr, 1));
        RowFilter.Builder builder = RowFilter.builder(false);
        builder.add(c, path(c.type, "field2"), Operator.GT, Int32Type.instance.decompose(1));
        assertFalse(builder.build().clusteringKeyRestrictionsAreSatisfiedBy(clustering));
        builder = RowFilter.builder(false);
        builder.add(c, path(c.type, "field1"), Operator.EQ, UTF8Type.instance.decompose("a"));
        assertTrue(builder.build().clusteringKeyRestrictionsAreSatisfiedBy(clustering));
    }

    /**
     * The expressions on different fields of a non-frozen UDT are an intersection on mutable targets: its fields are
     * different cells. Those on the fields of a frozen UDT are on a single cell, as are those on primary key columns.
     */
    @Test
    public void testMutableIntersectionsOnFields()
    {
        UserType multiCell = new UserType("testks", UTF8Type.instance.decompose("place"),
                                          Arrays.asList(FieldIdentifier.forUnquoted("city"), FieldIdentifier.forUnquoted("zip")),
                                          Arrays.asList(UTF8Type.instance, Int32Type.instance), true);
        TableMetadata metadata = TableMetadata.builder("testks", "testcf")
                                              .addPartitionKeyColumn("pk", Int32Type.instance)
                                              .addClusteringColumn("c", TUPLE)
                                              .addRegularColumn("addr", ADDRESS.freeze())
                                              .addRegularColumn("p", multiCell)
                                              .build();
        ColumnMetadata addr = metadata.getColumn(new ColumnIdentifier("addr", true));
        ColumnMetadata p = metadata.getColumn(new ColumnIdentifier("p", true));
        ColumnMetadata c = metadata.getColumn(new ColumnIdentifier("c", true));

        // the fields of a frozen UDT: a single cell
        RowFilter.Builder builder = RowFilter.builder(true);
        builder.add(addr, path(addr.type, "zip"), Operator.GT, Int32Type.instance.decompose(1));
        builder.add(addr, path(addr.type, "zip"), Operator.LT, Int32Type.instance.decompose(5));
        builder.add(addr, path(addr.type, "city"), Operator.EQ, UTF8Type.instance.decompose("Rome"));
        assertFalse(builder.build().isMutableIntersection());
        // with the whole column too
        builder.add(addr, Operator.NEQ, address("Paris", 1, null));
        assertFalse(builder.build().isMutableIntersection());
        // and with the fields of a clustering column, not mutable
        builder.add(c, path(c.type, "field1"), Operator.EQ, UTF8Type.instance.decompose("a"));
        assertFalse(builder.build().isMutableIntersection());

        // the fields of a non-frozen UDT: different cells
        builder = RowFilter.builder(true);
        builder.add(p, path(p.type, "zip"), Operator.GT, Int32Type.instance.decompose(1));
        builder.add(p, path(p.type, "zip"), Operator.LT, Int32Type.instance.decompose(5));
        assertFalse(builder.build().isMutableIntersection());
        builder.add(p, path(p.type, "city"), Operator.EQ, UTF8Type.instance.decompose("Rome"));
        assertTrue(builder.build().isMutableIntersection());

        // a frozen UDT field and a non-frozen UDT field: different columns
        builder = RowFilter.builder(true);
        builder.add(addr, path(addr.type, "city"), Operator.EQ, UTF8Type.instance.decompose("Rome"));
        builder.add(p, path(p.type, "city"), Operator.EQ, UTF8Type.instance.decompose("Rome"));
        assertTrue(builder.build().isMutableIntersection());
    }

    /**
     * Removing the expressions on a column doesn't remove those on its fields, which keep showing in the CQL form of
     * the filter (e.g. the slow query log and the tracing of the read commands).
     */
    @Test
    public void testWithoutKeepsFieldExpressions()
    {
        TableMetadata metadata = fieldsTable();
        ColumnMetadata c = metadata.getColumn(new ColumnIdentifier("c", true));
        ByteBuffer a = UTF8Type.instance.decompose("a");

        RowFilter.Builder builder = RowFilter.builder(false);
        builder.add(c, path(c.type, "field1"), Operator.EQ, a);
        builder.add(c, Operator.EQ, TupleType.buildValue(new ByteBuffer[]{ a, null }));
        RowFilter filter = builder.build();
        assertThat(filter.expressions()).hasSize(2);

        RowFilter withoutColumn = filter.withoutFirstLevelExpression(c);
        assertThat(withoutColumn.expressions()).hasSize(1);
        assertThat(withoutColumn.expressions().get(0).fieldPath()).isNotNull();
        assertThat(withoutColumn.toCQLString(Redaction.NONE)).isEqualTo("c.field1 = 'a'");

        // the value of the field expression is that of the field: even an equal value doesn't remove it
        RowFilter withoutValue = filter.without(c, Operator.EQ, a);
        assertThat(withoutValue.expressions()).hasSize(2);
        RowFilter withoutTuple = filter.without(c, Operator.EQ, TupleType.buildValue(new ByteBuffer[]{ a, null }));
        assertThat(withoutTuple.expressions()).hasSize(1);
        assertThat(withoutTuple.expressions().get(0).fieldPath()).isNotNull();
    }

    /**
     * The deserialization of the field paths rejects the invalid ones.
     */
    @Test
    public void testFieldPathDeserializationErrors() throws IOException
    {
        TableMetadata metadata = fieldsTable();
        ColumnMetadata addr = metadata.getColumn(new ColumnIdentifier("addr", true));
        int version = MessagingService.current_version;

        // an oversized step count
        assertFieldExpressionRejected(metadata, version, addr, out -> out.writeUnsignedVInt32(65), "Invalid field path size 65");
        assertFieldExpressionRejected(metadata, version, addr, out -> out.writeUnsignedVInt32(-1), "Invalid field path size");
        // an empty path
        assertFieldExpressionRejected(metadata, version, addr, out -> out.writeUnsignedVInt32(0), "Invalid field path size 0");
        // an unknown step tag
        assertFieldExpressionRejected(metadata, version, addr, out -> { out.writeUnsignedVInt32(1); out.writeByte(7); }, "Unknown field path step 7");
        // a path ending with the elements of a collection (the elements are only crossed to reach a field of them)
        assertFieldExpressionRejected(metadata, version, addr,
                                      out -> { out.writeUnsignedVInt32(2); out.writeByte(0); out.writeUnsignedVInt32(3); out.writeByte(1); },
                                      "it doesn't end with a field");
        // the elements of a column that isn't a collection
        assertFieldExpressionRejected(metadata, version, addr,
                                      out -> { out.writeUnsignedVInt32(2); out.writeByte(1); out.writeByte(0); out.writeUnsignedVInt32(0); },
                                      "Invalid field path");
        // a field position out of the type
        assertFieldExpressionRejected(metadata, version, addr,
                                      out -> { out.writeUnsignedVInt32(1); out.writeByte(0); out.writeUnsignedVInt32(9); },
                                      "Invalid field path");
    }

    private static TableMetadata collectionFieldsTable()
    {
        return TableMetadata.builder("testks", "testcf")
                            .addPartitionKeyColumn("pk", Int32Type.instance)
                            .addClusteringColumn("cl", ListType.getInstance(TUPLE, false))
                            .addRegularColumn("addrs", ListType.getInstance(ADDRESS.freeze(), true))
                            .addRegularColumn("faddrs", ListType.getInstance(ADDRESS.freeze(), false))
                            .build();
    }

    /**
     * The expressions on the fields inside collections (multi-valued paths) survive a serialization round trip.
     */
    @Test
    public void testMultiValuedFieldExpressionSerialization() throws IOException
    {
        TableMetadata metadata = collectionFieldsTable();
        ColumnMetadata addrs = metadata.getColumn(new ColumnIdentifier("addrs", true));
        ColumnMetadata faddrs = metadata.getColumn(new ColumnIdentifier("faddrs", true));
        ColumnMetadata cl = metadata.getColumn(new ColumnIdentifier("cl", true));

        RowFilter.Builder builder = RowFilter.builder(false);
        builder.add(addrs, path(addrs.type, "city"), Operator.EQ, UTF8Type.instance.decompose("Rome"));
        builder.add(addrs, path(addrs.type, "loc", "lat"), Operator.GTE, DoubleType.instance.decompose(2.0));
        builder.add(addrs, path(addrs.type, "zip"), Operator.NEQ, Int32Type.instance.decompose(100));
        builder.add(faddrs, path(faddrs.type, "tags"), Operator.CONTAINS, UTF8Type.instance.decompose("y"));
        builder.add(faddrs, path(faddrs.type, "city"), Operator.IN, ListType.getInstance(UTF8Type.instance, false).decompose(Arrays.asList("a", "b")));
        builder.add(cl, path(cl.type, "field2"), Operator.LT, Int32Type.instance.decompose(3));
        RowFilter filter = builder.build();

        int version = MessagingService.current_version;
        for (RowFilter.Expression expression : filter.expressions())
        {
            assertThat(expression.fieldPath().hasElements()).isTrue();
            try (DataOutputBuffer out = new DataOutputBuffer())
            {
                RowFilter.Expression.serializer.serialize(expression, out, version);
                assertThat(out.getLength()).isEqualTo(RowFilter.Expression.serializer.serializedSize(expression, version));
                try (DataInputBuffer in = new DataInputBuffer(out.buffer(), false))
                {
                    RowFilter.Expression deserialized = RowFilter.Expression.serializer.deserialize(in, version, metadata, IndexHints.NONE);
                    assertThat(in.available()).isZero();
                    assertThat(deserialized).isEqualTo(expression);
                    assertThat(deserialized.fieldPath()).isEqualTo(expression.fieldPath());
                    assertThat(deserialized.toString()).isEqualTo(expression.toString());
                }
            }
        }

        // the CQL forms name the fields, the elements are implicit
        assertThat(filter.expressions().get(0).toString()).isEqualTo("addrs.city = 'Rome'");
        assertThat(filter.expressions().get(1).toString()).isEqualTo("addrs.loc.lat >= 2.0");
        assertThat(filter.expressions().get(3).toString()).isEqualTo("faddrs.tags CONTAINS 'y'");
        assertThat(filter.expressions().get(5).toString()).isEqualTo("cl.field2 < 3");
    }

    private static Cell<?> listCell(ColumnMetadata column, ByteBuffer element)
    {
        return BufferCell.live(column, 1, element, CellPath.create(ByteBuffer.wrap(TimeUUID.Generator.nextTimeUUIDAsBytes())));
    }

    private static Row listRow(Clustering<?> clustering, ColumnMetadata column, ByteBuffer... elements)
    {
        Row.Builder builder = BTreeRow.sortedBuilder();
        builder.newRow(clustering);
        for (ByteBuffer element : elements)
            builder.addCell(listCell(column, element));
        return builder.build();
    }

    /**
     * The filtering of the expressions on the fields inside collections: some element satisfies the positive
     * operators, no element is equal to (contains) the value of the negative ones; the elements whose field is unset
     * are skipped, and a row without elements satisfies only the negative operators.
     */
    @Test
    public void testMultiValuedFieldExpressionIsSatisfiedBy()
    {
        TableMetadata metadata = collectionFieldsTable();
        ColumnMetadata addrs = metadata.getColumn(new ColumnIdentifier("addrs", true));
        ColumnMetadata faddrs = metadata.getColumn(new ColumnIdentifier("faddrs", true));
        ColumnMetadata cl = metadata.getColumn(new ColumnIdentifier("cl", true));
        DecoratedKey key = Murmur3Partitioner.instance.decorateKey(Int32Type.instance.decompose(1));
        ByteBuffer clusteringValue = ListType.getInstance(TUPLE, false)
                                             .decompose(Arrays.asList(TupleType.buildValue(new ByteBuffer[]{ UTF8Type.instance.decompose("a"), Int32Type.instance.decompose(1) }),
                                                                      TupleType.buildValue(new ByteBuffer[]{ UTF8Type.instance.decompose("b"), null })));
        Clustering<?> clustering = Clustering.make(clusteringValue);

        ByteBuffer rome = address("Rome", 200, 1.0);
        ByteBuffer milan = address("Milan", 100, 3.0);
        ByteBuffer unset = address(null, null, null);
        Row nonFrozen = listRow(clustering, addrs, rome, milan, unset);
        Row empty = BTreeRow.emptyRow(clustering);
        Row frozen = BTreeRow.create(clustering, LivenessInfo.EMPTY, Row.Deletion.LIVE,
                                     BTree.singleton(new BufferCell(faddrs, 1, Cell.NO_TTL, Cell.NO_DELETION_TIME,
                                                                    ListType.getInstance(ADDRESS.freeze(), false).decompose(Arrays.asList(rome, milan, unset)),
                                                                    null)));

        for (ColumnMetadata column : Arrays.asList(addrs, faddrs))
        {
            Row row = column == addrs ? nonFrozen : frozen;
            // some element
            assertTrue(expression(column, Operator.EQ, UTF8Type.instance.decompose("Rome"), "city").isSatisfiedBy(metadata, key, row, 1));
            assertTrue(expression(column, Operator.EQ, UTF8Type.instance.decompose("Milan"), "city").isSatisfiedBy(metadata, key, row, 1));
            assertFalse(expression(column, Operator.EQ, UTF8Type.instance.decompose("Turin"), "city").isSatisfiedBy(metadata, key, row, 1));
            assertTrue(expression(column, Operator.GTE, Int32Type.instance.decompose(150), "zip").isSatisfiedBy(metadata, key, row, 1));
            assertFalse(expression(column, Operator.GT, Int32Type.instance.decompose(200), "zip").isSatisfiedBy(metadata, key, row, 1));
            assertTrue(expression(column, Operator.LT, DoubleType.instance.decompose(2.0), "loc", "lat").isSatisfiedBy(metadata, key, row, 1));
            assertTrue(expression(column, Operator.IN, ListType.getInstance(UTF8Type.instance, false).decompose(Arrays.asList("Turin", "Milan")), "city")
                       .isSatisfiedBy(metadata, key, row, 1));
            assertTrue(expression(column, Operator.LIKE_PREFIX, UTF8Type.instance.decompose("Mi"), "city").isSatisfiedBy(metadata, key, row, 1));
            assertTrue(expression(column, Operator.CONTAINS, UTF8Type.instance.decompose("y"), "tags").isSatisfiedBy(metadata, key, row, 1));
            assertFalse(expression(column, Operator.CONTAINS, UTF8Type.instance.decompose("z"), "tags").isSatisfiedBy(metadata, key, row, 1));
            // no element
            assertFalse(expression(column, Operator.NEQ, UTF8Type.instance.decompose("Rome"), "city").isSatisfiedBy(metadata, key, row, 1));
            assertTrue(expression(column, Operator.NEQ, UTF8Type.instance.decompose("Turin"), "city").isSatisfiedBy(metadata, key, row, 1));
            assertFalse(expression(column, Operator.NOT_CONTAINS, UTF8Type.instance.decompose("y"), "tags").isSatisfiedBy(metadata, key, row, 1));
            assertTrue(expression(column, Operator.NOT_CONTAINS, UTF8Type.instance.decompose("z"), "tags").isSatisfiedBy(metadata, key, row, 1));
            // no elements: only the negative operators
            assertFalse(expression(column, Operator.EQ, UTF8Type.instance.decompose("Rome"), "city").isSatisfiedBy(metadata, key, empty, 1));
            assertTrue(expression(column, Operator.NEQ, UTF8Type.instance.decompose("Rome"), "city").isSatisfiedBy(metadata, key, empty, 1));
            assertTrue(expression(column, Operator.NOT_CONTAINS, UTF8Type.instance.decompose("y"), "tags").isSatisfiedBy(metadata, key, empty, 1));
        }

        // a field inside the elements of a clustering column, also through the clustering-only check
        RowFilter.Builder builder = RowFilter.builder(false);
        builder.add(cl, path(cl.type, "field1"), Operator.EQ, UTF8Type.instance.decompose("b"));
        assertTrue(builder.build().clusteringKeyRestrictionsAreSatisfiedBy(clustering));
        builder = RowFilter.builder(false);
        builder.add(cl, path(cl.type, "field2"), Operator.GT, Int32Type.instance.decompose(1));
        assertFalse(builder.build().clusteringKeyRestrictionsAreSatisfiedBy(clustering));
        builder = RowFilter.builder(false);
        builder.add(cl, path(cl.type, "field2"), Operator.NEQ, Int32Type.instance.decompose(2));
        assertTrue(builder.build().clusteringKeyRestrictionsAreSatisfiedBy(clustering));
        assertTrue(new RowFilter.FieldExpression(cl, path(cl.type, "field1"), Operator.EQ, UTF8Type.instance.decompose("a"), null)
                   .isSatisfiedBy(metadata, key, empty, 1));
    }

    private static RowFilter.Expression expression(ColumnMetadata column, Operator operator, ByteBuffer value, String... names)
    {
        return new RowFilter.FieldExpression(column, path(column.type, names), operator, value, null);
    }

    /**
     * Each expression on a field inside the elements of a non-frozen collection is a mutable target of its own: the
     * expressions may be satisfied by different elements, that is, by different cells. Inside a frozen collection,
     * they're all on a single cell.
     */
    @Test
    public void testMutableIntersectionsOnMultiValuedFields()
    {
        TableMetadata metadata = collectionFieldsTable();
        ColumnMetadata addrs = metadata.getColumn(new ColumnIdentifier("addrs", true));
        ColumnMetadata faddrs = metadata.getColumn(new ColumnIdentifier("faddrs", true));

        RowFilter.Builder builder = RowFilter.builder(true);
        builder.add(addrs, path(addrs.type, "city"), Operator.EQ, UTF8Type.instance.decompose("Rome"));
        assertFalse(builder.build().isMutableIntersection());
        builder.add(addrs, path(addrs.type, "city"), Operator.EQ, UTF8Type.instance.decompose("Milan"));
        assertTrue(builder.build().isMutableIntersection());

        builder = RowFilter.builder(true);
        builder.add(faddrs, path(faddrs.type, "city"), Operator.EQ, UTF8Type.instance.decompose("Rome"));
        builder.add(faddrs, path(faddrs.type, "city"), Operator.EQ, UTF8Type.instance.decompose("Milan"));
        builder.add(faddrs, path(faddrs.type, "zip"), Operator.GT, Int32Type.instance.decompose(1));
        assertFalse(builder.build().isMutableIntersection());
    }

    /**
     * Builds the conditions of a CONTAINS ELEMENT group on the given column: city = 'Rome' AND zip != 100 AND
     * (zip >= 150 OR loc.lat < 2), with the absolute paths, as the restrictions add them.
     */
    private static RowFilter.FilterElement romeConditions(RowFilter.Builder builder, ColumnMetadata column)
    {
        RowFilter.Builder conditions = builder.nestedBuilder(false);
        conditions.add(column, path(column.type, "city"), Operator.EQ, UTF8Type.instance.decompose("Rome"));
        conditions.add(column, path(column.type, "zip"), Operator.NEQ, Int32Type.instance.decompose(100));
        RowFilter.Builder or = builder.nestedBuilder(true);
        or.add(column, path(column.type, "zip"), Operator.GTE, Int32Type.instance.decompose(150));
        or.add(column, path(column.type, "loc", "lat"), Operator.LT, DoubleType.instance.decompose(2.0));
        conditions.addChild(or.buildElement());
        return conditions.buildElement();
    }

    /**
     * The CONTAINS ELEMENT groups (kind ELEMENT_MATCH) survive a serialization round trip, with conditions relative
     * to the elements, and their serialized size is the number of bytes written.
     */
    @Test
    public void testElementMatchSerialization() throws IOException
    {
        TableMetadata metadata = collectionFieldsTable();
        ColumnMetadata addrs = metadata.getColumn(new ColumnIdentifier("addrs", true));
        ColumnMetadata faddrs = metadata.getColumn(new ColumnIdentifier("faddrs", true));
        ColumnMetadata cl = metadata.getColumn(new ColumnIdentifier("cl", true));

        RowFilter.Builder builder = RowFilter.builder(false);
        builder.addElementMatch(addrs, FieldPath.elementsOf(null), false, romeConditions(builder, addrs));
        builder.addElementMatch(faddrs, FieldPath.elementsOf(null), true, romeConditions(builder, faddrs));
        RowFilter.Builder tags = builder.nestedBuilder(false);
        tags.add(faddrs, path(faddrs.type, "tags"), Operator.CONTAINS, UTF8Type.instance.decompose("y"));
        tags.add(faddrs, path(faddrs.type, "city"), Operator.IN, ListType.getInstance(UTF8Type.instance, false).decompose(Arrays.asList("a", "b")));
        builder.addElementMatch(faddrs, FieldPath.elementsOf(null), false, tags.buildElement());
        RowFilter.Builder tuples = builder.nestedBuilder(false);
        tuples.add(cl, path(cl.type, "field1"), Operator.EQ, UTF8Type.instance.decompose("a"));
        tuples.add(cl, path(cl.type, "field2"), Operator.LT, Int32Type.instance.decompose(3));
        builder.addElementMatch(cl, FieldPath.elementsOf(null), false, tuples.buildElement());
        RowFilter filter = builder.build();
        assertThat(filter.expressions()).hasSize(4);

        int version = MessagingService.current_version;
        for (RowFilter.Expression expression : filter.expressions())
        {
            assertThat(expression).isInstanceOf(RowFilter.ElementMatchExpression.class);
            try (DataOutputBuffer out = new DataOutputBuffer())
            {
                RowFilter.Expression.serializer.serialize(expression, out, version);
                assertThat(out.getLength()).isEqualTo(RowFilter.Expression.serializer.serializedSize(expression, version));
                assertThat(out.buffer().get(0)).isEqualTo((byte) 6);
                try (DataInputBuffer in = new DataInputBuffer(out.buffer(), false))
                {
                    RowFilter.Expression deserialized = RowFilter.Expression.serializer.deserialize(in, version, metadata, IndexHints.NONE);
                    assertThat(in.available()).isZero();
                    assertThat(deserialized).isEqualTo(expression);
                    assertThat(deserialized.fieldPath()).isEqualTo(FieldPath.elementsOf(null));
                    assertThat(deserialized.toString()).isEqualTo(expression.toString());
                    RowFilter.ElementMatchExpression match = (RowFilter.ElementMatchExpression) deserialized;
                    for (RowFilter.Expression condition : match.conditions().expressions())
                        assertThat(((RowFilter.FieldExpression) condition).isRelative()).isTrue();
                }
            }
        }

        // the CQL forms, with the conditions relative to the elements
        assertThat(filter.expressions().get(0).toString()).isEqualTo("addrs CONTAINS ELEMENT (city = 'Rome' AND zip != 100 AND (zip >= 150 OR loc.lat < 2.0))");
        assertThat(filter.expressions().get(1).toString()).isEqualTo("faddrs NOT CONTAINS ELEMENT (city = 'Rome' AND zip != 100 AND (zip >= 150 OR loc.lat < 2.0))");
        assertThat(filter.expressions().get(2).toString()).isEqualTo("faddrs CONTAINS ELEMENT (tags CONTAINS 'y' AND city IN ['a', ...)");
        assertThat(filter.expressions().get(3).toString()).isEqualTo("cl CONTAINS ELEMENT (field1 = 'a' AND field2 < 3)");

        // the relative paths: the field steps from the elements
        RowFilter.ElementMatchExpression rome = (RowFilter.ElementMatchExpression) filter.expressions().get(0);
        assertThat(rome.conditions().expressions().get(0).fieldPath()).isEqualTo(path(ADDRESS, "city"));
        assertThat(rome.conditions().children().get(0).isDisjunction()).isTrue();
        assertThat(rome.conditions().children().get(0).expressions().get(1).fieldPath()).isEqualTo(path(ADDRESS, "loc", "lat"));

        // the groups differ by the polarity, the column and the conditions
        assertThat(filter.expressions().get(0)).isNotEqualTo(filter.expressions().get(1));
        RowFilter.Builder other = RowFilter.builder(false);
        RowFilter.Builder otherConditions = other.nestedBuilder(false);
        otherConditions.add(cl, path(cl.type, "field1"), Operator.EQ, UTF8Type.instance.decompose("a"));
        other.addElementMatch(cl, FieldPath.elementsOf(null), false, otherConditions.buildElement());
        assertThat(other.build().expressions().get(0)).isNotEqualTo(filter.expressions().get(3));

        // a whole filter, with a group in a disjunction
        RowFilter.Builder disjunction = RowFilter.builder(false);
        RowFilter.Builder or = disjunction.nestedBuilder(true);
        or.addElementMatch(addrs, FieldPath.elementsOf(null), false, romeConditions(disjunction, addrs));
        or.add(addrs, path(addrs.type, "zip"), Operator.EQ, Int32Type.instance.decompose(300));
        disjunction.addChild(or.buildElement());
        RowFilter.FilterElement root = disjunction.build().root();
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            RowFilter.FilterElement.serializer.serialize(root, out, version);
            assertThat(out.getLength()).isEqualTo(RowFilter.FilterElement.serializer.serializedSize(root, version));
            try (DataInputBuffer in = new DataInputBuffer(out.buffer(), false))
            {
                RowFilter.FilterElement deserialized = RowFilter.FilterElement.serializer.deserialize(in, version, metadata, IndexHints.NONE);
                assertThat(deserialized.children().get(0).expressions()).isEqualTo(root.children().get(0).expressions());
                assertThat(deserialized.toCQLString(org.apache.cassandra.db.marshal.Redaction.NONE)).isEqualTo(root.toCQLString(org.apache.cassandra.db.marshal.Redaction.NONE));
            }
        }
    }

    /**
     * The candidates of a group: the positive conditions on the absolute paths, a superset of the rows satisfying
     * the group; none for an OR with a negative condition, for a group of negative conditions only and for NOT
     * CONTAINS ELEMENT.
     */
    @Test
    public void testElementMatchCandidates()
    {
        TableMetadata metadata = collectionFieldsTable();
        ColumnMetadata addrs = metadata.getColumn(new ColumnIdentifier("addrs", true));

        RowFilter.Builder builder = RowFilter.builder(false);
        builder.addElementMatch(addrs, FieldPath.elementsOf(null), false, romeConditions(builder, addrs));
        RowFilter.ElementMatchExpression match = (RowFilter.ElementMatchExpression) builder.build().expressions().get(0);
        assertThat(match.candidates().toCQLString(org.apache.cassandra.db.marshal.Redaction.NONE))
            .isEqualTo("addrs.city = 'Rome' AND (addrs.zip >= 150 OR addrs.loc.lat < 2.0)");
        assertThat(match.candidateExpressions()).hasSize(3);

        builder = RowFilter.builder(false);
        RowFilter.Builder or = builder.nestedBuilder(true);
        or.add(addrs, path(addrs.type, "city"), Operator.EQ, UTF8Type.instance.decompose("Rome"));
        or.add(addrs, path(addrs.type, "zip"), Operator.NEQ, Int32Type.instance.decompose(100));
        builder.addElementMatch(addrs, FieldPath.elementsOf(null), false, or.buildElement());
        RowFilter.Builder negative = builder.nestedBuilder(false);
        negative.add(addrs, path(addrs.type, "zip"), Operator.NEQ, Int32Type.instance.decompose(100));
        builder.addElementMatch(addrs, FieldPath.elementsOf(null), false, negative.buildElement());
        RowFilter.Builder positive = builder.nestedBuilder(false);
        positive.add(addrs, path(addrs.type, "zip"), Operator.EQ, Int32Type.instance.decompose(100));
        builder.addElementMatch(addrs, FieldPath.elementsOf(null), true, positive.buildElement());
        for (RowFilter.Expression expression : builder.build().expressions())
        {
            assertThat(((RowFilter.ElementMatchExpression) expression).candidates()).as(expression.toString()).isNull();
            assertThat(((RowFilter.ElementMatchExpression) expression).candidateExpressions()).isEmpty();
        }
    }

    /**
     * The filtering of the groups: some element satisfies all the conditions (none for NOT CONTAINS ELEMENT, also
     * satisfied by a row without elements), on non-frozen and frozen collections and on clustering columns.
     */
    @Test
    public void testElementMatchIsSatisfiedBy()
    {
        TableMetadata metadata = collectionFieldsTable();
        ColumnMetadata addrs = metadata.getColumn(new ColumnIdentifier("addrs", true));
        ColumnMetadata faddrs = metadata.getColumn(new ColumnIdentifier("faddrs", true));
        ColumnMetadata cl = metadata.getColumn(new ColumnIdentifier("cl", true));
        DecoratedKey key = Murmur3Partitioner.instance.decorateKey(Int32Type.instance.decompose(1));
        ByteBuffer clusteringValue = ListType.getInstance(TUPLE, false)
                                             .decompose(Arrays.asList(TupleType.buildValue(new ByteBuffer[]{ UTF8Type.instance.decompose("a"), Int32Type.instance.decompose(1) }),
                                                                      TupleType.buildValue(new ByteBuffer[]{ UTF8Type.instance.decompose("b"), Int32Type.instance.decompose(2) })));
        Clustering<?> clustering = Clustering.make(clusteringValue);

        ByteBuffer rome = address("Rome", 200, 1.0);
        ByteBuffer milan = address("Milan", 100, 3.0);
        ByteBuffer unset = address(null, null, null);
        Row nonFrozen = listRow(clustering, addrs, rome, milan, unset);
        Row empty = BTreeRow.emptyRow(clustering);
        Row frozen = BTreeRow.create(clustering, LivenessInfo.EMPTY, Row.Deletion.LIVE,
                                     BTree.singleton(new BufferCell(faddrs, 1, Cell.NO_TTL, Cell.NO_DELETION_TIME,
                                                                    ListType.getInstance(ADDRESS.freeze(), false).decompose(Arrays.asList(rome, milan, unset)),
                                                                    null)));

        for (ColumnMetadata column : Arrays.asList(addrs, faddrs))
        {
            Row row = column == addrs ? nonFrozen : frozen;
            assertTrue(group(column, false, "city", Operator.EQ, utf8("Rome"), "zip", Operator.EQ, int32(200)).isSatisfiedBy(metadata, key, row, 1));
            assertFalse(group(column, false, "city", Operator.EQ, utf8("Rome"), "zip", Operator.EQ, int32(100)).isSatisfiedBy(metadata, key, row, 1));
            assertTrue(group(column, false, "city", Operator.EQ, utf8("Milan"), "zip", Operator.LT, int32(150)).isSatisfiedBy(metadata, key, row, 1));
            assertTrue(group(column, false, "city", Operator.EQ, utf8("Rome"), "zip", Operator.NEQ, int32(100)).isSatisfiedBy(metadata, key, row, 1));
            assertFalse(group(column, false, "city", Operator.EQ, utf8("Rome"), "zip", Operator.NEQ, int32(200)).isSatisfiedBy(metadata, key, row, 1));
            // the unset fields don't satisfy !=
            assertFalse(group(column, false, "city", Operator.NEQ, utf8("Rome"), "zip", Operator.NEQ, int32(100)).isSatisfiedBy(metadata, key, row, 1));
            // the OR of romeConditions: the Rome element, zip 200 >= 150
            RowFilter.Builder builder = RowFilter.builder(false);
            builder.addElementMatch(column, FieldPath.elementsOf(null), false, romeConditions(builder, column));
            assertTrue(builder.build().expressions().get(0).isSatisfiedBy(metadata, key, row, 1));
            // none
            assertTrue(group(column, true, "city", Operator.EQ, utf8("Rome"), "zip", Operator.EQ, int32(100)).isSatisfiedBy(metadata, key, row, 1));
            assertFalse(group(column, true, "city", Operator.EQ, utf8("Rome"), "zip", Operator.EQ, int32(200)).isSatisfiedBy(metadata, key, row, 1));
            // no elements
            assertFalse(group(column, false, "city", Operator.EQ, utf8("Rome"), "zip", Operator.EQ, int32(200)).isSatisfiedBy(metadata, key, empty, 1));
            assertTrue(group(column, true, "city", Operator.EQ, utf8("Rome"), "zip", Operator.EQ, int32(200)).isSatisfiedBy(metadata, key, empty, 1));
        }

        // the elements of a clustering column, also through the clustering-only check
        assertTrue(group(cl, false, "field1", Operator.EQ, utf8("b"), "field2", Operator.EQ, int32(2)).isSatisfiedBy(metadata, key, empty, 1));
        assertFalse(group(cl, false, "field1", Operator.EQ, utf8("b"), "field2", Operator.EQ, int32(1)).isSatisfiedBy(metadata, key, empty, 1));
        RowFilter.Builder builder = RowFilter.builder(false);
        builder.add(group(cl, true, "field1", Operator.EQ, utf8("b"), "field2", Operator.EQ, int32(1)));
        assertTrue(builder.build().clusteringKeyRestrictionsAreSatisfiedBy(clustering));
        builder = RowFilter.builder(false);
        builder.add(group(cl, false, "field1", Operator.EQ, utf8("a"), "field2", Operator.EQ, int32(2)));
        assertFalse(builder.build().clusteringKeyRestrictionsAreSatisfiedBy(clustering));
    }

    private static ByteBuffer utf8(String value)
    {
        return UTF8Type.instance.decompose(value);
    }

    private static ByteBuffer int32(int value)
    {
        return Int32Type.instance.decompose(value);
    }

    /**
     * @return the group of the AND of two conditions on the elements of the column
     */
    private static RowFilter.Expression group(ColumnMetadata column, boolean negated, String field1, Operator operator1, ByteBuffer value1, String field2, Operator operator2, ByteBuffer value2)
    {
        RowFilter.Builder builder = RowFilter.builder(false);
        RowFilter.Builder conditions = builder.nestedBuilder(false);
        conditions.add(column, path(column.type, field1), operator1, value1);
        conditions.add(column, path(column.type, field2), operator2, value2);
        builder.addElementMatch(column, FieldPath.elementsOf(null), negated, conditions.buildElement());
        return builder.build().expressions().get(0);
    }

    /**
     * Each group on a non-frozen collection is a mutable target of its own (it may be satisfied by an element, a
     * cell, other than that satisfying another restriction); the conditions of a group are on one element.
     */
    @Test
    public void testElementMatchMutableIntersections()
    {
        TableMetadata metadata = collectionFieldsTable();
        ColumnMetadata addrs = metadata.getColumn(new ColumnIdentifier("addrs", true));
        ColumnMetadata faddrs = metadata.getColumn(new ColumnIdentifier("faddrs", true));

        RowFilter.Builder builder = RowFilter.builder(true);
        builder.addElementMatch(addrs, FieldPath.elementsOf(null), false, romeConditions(builder, addrs));
        assertFalse(builder.build().isMutableIntersection());
        builder.add(addrs, path(addrs.type, "city"), Operator.EQ, utf8("Milan"));
        assertTrue(builder.build().isMutableIntersection());

        builder = RowFilter.builder(true);
        builder.addElementMatch(faddrs, FieldPath.elementsOf(null), false, romeConditions(builder, faddrs));
        builder.add(faddrs, path(faddrs.type, "city"), Operator.EQ, utf8("Milan"));
        assertFalse(builder.build().isMutableIntersection());
    }

    /**
     * The deserialization of the groups refuses the prefixes that don't reach the elements of a collection of UDTs or
     * tuples, the conditions that aren't relative field expressions on the column of the group or whose relative
     * paths don't match the elements, an empty group and an operator other than CONTAINS / NOT CONTAINS.
     */
    @Test
    public void testElementMatchDeserializationErrors() throws IOException
    {
        TableMetadata metadata = collectionFieldsTable();
        int version = MessagingService.current_version;
        ColumnMetadata addrs = metadata.getColumn(new ColumnIdentifier("addrs", true));
        ColumnMetadata faddrs = metadata.getColumn(new ColumnIdentifier("faddrs", true));
        PathWriter elements = out -> { out.writeUnsignedVInt32(1); out.writeByte(1); };
        PathWriter city = out -> { out.writeUnsignedVInt32(1); out.writeByte(0); out.writeUnsignedVInt32(0); };

        // the prefix: a field of the elements, the elements of the elements
        assertElementMatchRejected(metadata, version, addrs, Operator.CONTAINS,
                                   out -> { out.writeUnsignedVInt32(2); out.writeByte(1); out.writeByte(0); out.writeUnsignedVInt32(0); },
                                   out -> writeCondition(out, addrs, city), "it doesn't reach the elements of a collection of UDTs or tuples");
        assertElementMatchRejected(metadata, version, addrs, Operator.CONTAINS,
                                   out -> { out.writeUnsignedVInt32(2); out.writeByte(1); out.writeByte(1); },
                                   out -> writeCondition(out, addrs, city), "Invalid CONTAINS ELEMENT path");
        // the conditions: a whole-column expression, a condition on another column, a path out of the elements,
        // a path ending with the elements, an empty group
        assertElementMatchRejected(metadata, version, addrs, Operator.CONTAINS, elements,
                                   out -> {
                                       out.writeUnsignedVInt32(1);
                                       out.writeByte(0);
                                       ByteBufferUtil.writeWithShortLength(addrs.name.bytes, out);
                                       Operator.EQ.writeTo(out);
                                       ByteBufferUtil.writeWithShortLength(utf8("Rome"), out);
                                       out.writeBoolean(false);
                                       out.writeUnsignedVInt32(0);
                                   }, "Invalid CONTAINS ELEMENT condition of kind SIMPLE");
        assertElementMatchRejected(metadata, version, addrs, Operator.CONTAINS, elements,
                                   out -> writeCondition(out, faddrs, city), "Invalid CONTAINS ELEMENT condition on column faddrs");
        assertElementMatchRejected(metadata, version, addrs, Operator.CONTAINS, elements,
                                   out -> writeCondition(out, addrs, o -> { o.writeUnsignedVInt32(1); o.writeByte(0); o.writeUnsignedVInt32(9); }),
                                   "Invalid field path [9] for the elements of column addrs");
        assertElementMatchRejected(metadata, version, addrs, Operator.CONTAINS, elements,
                                   out -> writeCondition(out, addrs, o -> { o.writeUnsignedVInt32(2); o.writeByte(0); o.writeUnsignedVInt32(2); o.writeByte(1); }),
                                   "Invalid field path");
        assertElementMatchRejected(metadata, version, addrs, Operator.CONTAINS, elements,
                                   out -> { out.writeUnsignedVInt32(0); out.writeBoolean(false); out.writeUnsignedVInt32(0); },
                                   "Empty CONTAINS ELEMENT group");
        // the operator
        assertElementMatchRejected(metadata, version, addrs, Operator.EQ, elements,
                                   out -> writeCondition(out, addrs, city), "Invalid CONTAINS ELEMENT operator");
    }

    /**
     * IS NOT NULL on a field (operator IS_NOT, no operand): a round trip, the CQL form, and the filtering on single and
     * multi-valued paths and in a group, where the empty strings are set but the empty values of the other types
     * are not.
     */
    @Test
    public void testFieldIsNotNull() throws IOException
    {
        TableMetadata metadata = collectionFieldsTable();
        ColumnMetadata addrs = metadata.getColumn(new ColumnIdentifier("addrs", true));
        ColumnMetadata faddrs = metadata.getColumn(new ColumnIdentifier("faddrs", true));
        int version = MessagingService.current_version;

        RowFilter.Builder builder = RowFilter.builder(false);
        builder.add(addrs, path(addrs.type, "city"), Operator.IS_NOT, ByteBufferUtil.EMPTY_BYTE_BUFFER);
        RowFilter.Builder conditions = builder.nestedBuilder(false);
        conditions.add(faddrs, path(faddrs.type, "loc", "lat"), Operator.IS_NOT, ByteBufferUtil.EMPTY_BYTE_BUFFER);
        conditions.add(faddrs, path(faddrs.type, "city"), Operator.EQ, utf8("Rome"));
        builder.addElementMatch(faddrs, FieldPath.elementsOf(null), false, conditions.buildElement());
        RowFilter filter = builder.build();
        assertThat(filter.expressions().get(0).toString()).isEqualTo("addrs.city IS NOT NULL");
        assertThat(filter.expressions().get(1).toString()).isEqualTo("faddrs CONTAINS ELEMENT (loc.lat IS NOT NULL AND city = 'Rome')");
        assertThat(((RowFilter.ElementMatchExpression) filter.expressions().get(1)).candidates().toCQLString(Redaction.NONE))
            .isEqualTo("faddrs.loc.lat IS NOT NULL AND faddrs.city = 'Rome'");
        for (RowFilter.Expression expression : filter.expressions())
        {
            try (DataOutputBuffer out = new DataOutputBuffer())
            {
                RowFilter.Expression.serializer.serialize(expression, out, version);
                assertThat(out.getLength()).isEqualTo(RowFilter.Expression.serializer.serializedSize(expression, version));
                try (DataInputBuffer in = new DataInputBuffer(out.buffer(), false))
                {
                    RowFilter.Expression deserialized = RowFilter.Expression.serializer.deserialize(in, version, metadata, IndexHints.NONE);
                    assertThat(deserialized).isEqualTo(expression);
                    assertThat(deserialized.toString()).isEqualTo(expression.toString());
                }
            }
        }

        DecoratedKey key = Murmur3Partitioner.instance.decorateKey(Int32Type.instance.decompose(1));
        Clustering<?> clustering = Clustering.make(ListType.getInstance(TUPLE, false).decompose(Arrays.asList()));
        Row withCity = listRow(clustering, addrs, address(null, 1, null), address("", null, null));
        Row withoutCity = listRow(clustering, addrs, address(null, 1, 2.0));
        RowFilter.Expression city = filter.expressions().get(0);
        assertTrue(city.isSatisfiedBy(metadata, key, withCity, 1));
        assertFalse(city.isSatisfiedBy(metadata, key, withoutCity, 1));
        assertFalse(city.isSatisfiedBy(metadata, key, BTreeRow.emptyRow(clustering), 1));

        // the empty values: set for the literal types only
        assertTrue(RowFilter.FieldExpression.isSet(UTF8Type.instance, ByteBufferUtil.EMPTY_BYTE_BUFFER));
        assertFalse(RowFilter.FieldExpression.isSet(Int32Type.instance, ByteBufferUtil.EMPTY_BYTE_BUFFER));
        assertTrue(RowFilter.FieldExpression.isSet(Int32Type.instance, int32(0)));
        assertFalse(RowFilter.FieldExpression.isSet(UTF8Type.instance, null));
    }

    /**
     * A group without conditions (e.g. built from a NOT IN with an empty list, which the restriction refuses) would
     * be satisfied by any element locally, but it doesn't survive a serialization round trip: the replicas refuse it.
     */
    @Test
    public void testEmptyElementMatchRoundTrip() throws IOException
    {
        TableMetadata metadata = collectionFieldsTable();
        ColumnMetadata addrs = metadata.getColumn(new ColumnIdentifier("addrs", true));
        int version = MessagingService.current_version;

        RowFilter.Builder builder = RowFilter.builder(false);
        builder.addElementMatch(addrs, FieldPath.elementsOf(null), false, builder.nestedBuilder(false).buildElement());
        RowFilter.Expression empty = builder.build().expressions().get(0);
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            RowFilter.Expression.serializer.serialize(empty, out, version);
            assertThat(out.getLength()).isEqualTo(RowFilter.Expression.serializer.serializedSize(empty, version));
            try (DataInputBuffer in = new DataInputBuffer(out.buffer(), false))
            {
                assertThatThrownBy(() -> RowFilter.Expression.serializer.deserialize(in, version, metadata, IndexHints.NONE))
                    .hasMessageContaining("Empty CONTAINS ELEMENT group on addrs");
            }
        }
    }

    /**
     * Writes a filter element with one condition, a field expression city = 'Rome' (kind FIELD) on the given column
     * with the given path.
     */
    private static void writeCondition(DataOutputBuffer out, ColumnMetadata column, PathWriter path) throws IOException
    {
        out.writeUnsignedVInt32(1);
        out.writeByte(5);
        ByteBufferUtil.writeWithShortLength(column.name.bytes, out);
        Operator.EQ.writeTo(out);
        path.write(out);
        ByteBufferUtil.writeWithShortLength(utf8("Rome"), out);
        out.writeBoolean(false);
        out.writeUnsignedVInt32(0);
    }

    private static void assertElementMatchRejected(TableMetadata metadata,
                                                   int version,
                                                   ColumnMetadata column,
                                                   Operator operator,
                                                   PathWriter prefix,
                                                   PathWriter conditions,
                                                   String error) throws IOException
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            // the kind byte, the column name, the operator, the prefix and the conditions
            out.writeByte(6);
            ByteBufferUtil.writeWithShortLength(column.name.bytes, out);
            operator.writeTo(out);
            prefix.write(out);
            conditions.write(out);
            try (DataInputBuffer in = new DataInputBuffer(out.buffer(), false))
            {
                assertThatThrownBy(() -> RowFilter.Expression.serializer.deserialize(in, version, metadata, IndexHints.NONE))
                    .hasMessageContaining(error);
            }
        }
    }

    private interface PathWriter
    {
        void write(DataOutputBuffer out) throws IOException;
    }

    private static void assertFieldExpressionRejected(TableMetadata metadata, int version, ColumnMetadata column, PathWriter path, String error) throws IOException
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            // the kind byte, the column name, the operator, the path and the value
            out.writeByte(5);
            ByteBufferUtil.writeWithShortLength(column.name.bytes, out);
            Operator.EQ.writeTo(out);
            path.write(out);
            ByteBufferUtil.writeWithShortLength(Int32Type.instance.decompose(1), out);
            try (DataInputBuffer in = new DataInputBuffer(out.buffer(), false))
            {
                assertThatThrownBy(() -> RowFilter.Expression.serializer.deserialize(in, version, metadata, IndexHints.NONE))
                    .hasMessageContaining(error);
            }
        }
    }
}
