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
package org.apache.cassandra.index.sai.json;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.filter.IndexHints;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.cql3.Operator.EQ;
import static org.apache.cassandra.cql3.Operator.GT;
import static org.apache.cassandra.cql3.Operator.GTE;
import static org.apache.cassandra.cql3.Operator.LT;
import static org.apache.cassandra.cql3.Operator.NEQ;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class JsonPathRowFilterTest extends SAITester
{
    private static final String PREFIX = "CREATE CUSTOM INDEX %s ON %%s(doc) USING 'StorageAttachedIndex' WITH OPTIONS = ";

    private TableMetadata meta;
    private DecoratedKey key;
    private long now;
    private String sIndexName;
    private String tIndexName;

    /**
     * Sets up a test table with integer and text JSON path indexes before each test.
     */
    @Before
    public void setupTable()
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, doc text, other text)");
        sIndexName = createIndex(String.format(PREFIX, "s_idx") + "{'json_path': '$.s', 'json_type': 'int'}");
        tIndexName = createIndex(String.format(PREFIX, "t_idx") + "{'json_path': '$.t', 'json_type': 'text'}");
        meta = currentTableMetadata();
        key = meta.partitioner.decorateKey(Int32Type.instance.decompose(1));
        now = FBUtilities.nowInSeconds();
    }

    /**
     * Tests that multiple conditions against an array path evaluate independently (each matching element satisfies).
     */
    @Test
    public void independentArraySemantics()
    {
        Row row = rowWithDoc("{\"s\":[10,50]}");
        assertTrue(expr("$.s", GT, 30).isSatisfiedBy(meta, key, row, now));
        assertTrue(expr("$.s", LT, 40).isSatisfiedBy(meta, key, row, now));
        assertFalse(expr("$.s", EQ, 30).isSatisfiedBy(meta, key, row, now));
        assertTrue(expr("$.s", EQ, 50).isSatisfiedBy(meta, key, row, now));
        assertFalse(expr("$.s", GT, 50).isSatisfiedBy(meta, key, row, now));
        assertTrue(expr("$.s", GTE, 50).isSatisfiedBy(meta, key, row, now));
    }

    /**
     * Tests that NEQ expressions match rows when the JSON path is absent or the document is empty/malformed.
     */
    @Test
    public void neqMatchesMissingPath()
    {
        assertTrue(expr("$.t", NEQ, "admin").isSatisfiedBy(meta, key, rowWithDoc("{}"), now));
        assertFalse(expr("$.t", NEQ, "admin").isSatisfiedBy(meta, key, rowWithDoc("{\"t\":[\"admin\",\"dev\"]}"), now));
        assertTrue(expr("$.t", NEQ, "admin").isSatisfiedBy(meta, key, rowWithDoc("{\"t\":[\"ops\",\"dev\"]}"), now));
        assertTrue(expr("$.t", NEQ, "admin").isSatisfiedBy(meta, key, rowWithDoc("{broken"), now));
        assertFalse(expr("$.t", EQ, "admin").isSatisfiedBy(meta, key, rowWithDoc("{broken"), now));
    }

    /**
     * Tests that expressions handle rows where the target document cell is completely absent/null.
     */
    @Test
    public void absentCell()
    {
        execute("INSERT INTO %s (pk, other) VALUES (1, 'x')");
        Row row = Util.getOnlyRow(Util.cmd(getCurrentColumnFamilyStore(), 1).build());
        assertTrue(expr("$.t", NEQ, "admin").isSatisfiedBy(meta, key, row, now));
        assertFalse(expr("$.t", EQ, "admin").isSatisfiedBy(meta, key, row, now));
        assertFalse(expr("$.s", GT, 0).isSatisfiedBy(meta, key, row, now));
    }

    /**
     * Tests typed value comparisons, verifying that values of mismatched types do not produce matching terms.
     */
    @Test
    public void typedComparison()
    {
        // int path: a string value or out of range number is not a term, so it does not match
        Row row = rowWithDoc("{\"s\":[\"7\", 9], \"t\":\"admin\"}");
        assertFalse(expr("$.s", EQ, 7).isSatisfiedBy(meta, key, row, now));
        assertTrue(expr("$.s", EQ, 9).isSatisfiedBy(meta, key, row, now));
        assertTrue(expr("$.t", EQ, "admin").isSatisfiedBy(meta, key, row, now));
    }

    /**
     * Tests CQL string rendering of JsonPathExpression instances.
     */
    @Test
    public void toCQLString()
    {
        assertEquals("doc['$.s'] > 30", expr("$.s", GT, 30).toCQLString(false));
        assertEquals("doc['$.t'] = 'admin'", expr("$.t", EQ, "admin").toCQLString(false));
        assertEquals("doc['$.s'] > 30", expr("$['s']", GT, 30).toCQLString(false));
    }

    /**
     * Tests that null expression values are rejected during validation.
     */
    @Test
    public void validateRejectsNull()
    {
        RowFilter.JsonPathExpression e = new RowFilter.JsonPathExpression(column("doc"), JsonPath.parse("$.s"),
                                                                          Int32Type.instance, EQ, null);
        assertThatThrownBy(e::validate).isInstanceOf(InvalidRequestException.class);
        expr("$.s", EQ, 1).validate();
    }

    /**
     * Tests serialization and deserialization round-trip of JsonPathExpression over the wire.
     */
    @Test
    public void serializationRoundTrip() throws IOException
    {
        RowFilter.Expression e = expr("$.s", GTE, 7);
        RowFilter.Expression back = roundTrip(e, meta);
        assertEquals(e, back);
        assertEquals(e.hashCode(), back.hashCode());
        assertEquals(Int32Type.instance, ((RowFilter.JsonPathExpression) back).valueType());
        assertEquals(JsonPath.parse("$.s"), ((RowFilter.JsonPathExpression) back).path());

        RowFilter.Expression t = expr("$.t", NEQ, "admin");
        assertEquals(t, roundTrip(t, meta));
    }

    /**
     * Tests that deserialization fails when the referenced JSON path index does not exist in the table schema.
     */
    @Test
    public void deserializationWithoutMatchingIndexFails() throws IOException
    {
        RowFilter.Expression e = expr("$.s", GTE, 7);
        DataOutputBuffer out = new DataOutputBuffer();
        RowFilter.Expression.serializer.serialize(e, out, MessagingService.current_version);
        dropIndex("DROP INDEX %s." + sIndexName);
        TableMetadata withoutIndex = currentTableMetadata();
        assertThatThrownBy(() -> RowFilter.Expression.serializer.deserialize(new DataInputBuffer(out.buffer(), true),
                                                                             MessagingService.current_version,
                                                                             withoutIndex,
                                                                             IndexHints.NONE))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("$.s");
    }

    /**
     * Tests equals and hashCode behavior across different path spellings, operators, and target values.
     */
    @Test
    public void equalsIncludesPath()
    {
        assertEquals(expr("$.s", EQ, 1), expr("$['s']", EQ, 1));
        assertFalse(expr("$.s", EQ, 1).equals(expr("$.s", EQ, 2)));
        assertFalse(expr("$.s", EQ, 1).equals(expr("$.s", GT, 1)));
        assertFalse(expr("$.s", EQ, 1).equals(new RowFilter.JsonPathExpression(column("doc"), JsonPath.parse("$.x"),
                                                                                Int32Type.instance, EQ,
                                                                                Int32Type.instance.decompose(1))));
    }

    /**
     * Tests that SAI index selection routes JsonPathExpression only to the matching path-aware index.
     */
    @Test
    public void indexSelectionByPath()
    {
        StorageAttachedIndex sIdx = index(sIndexName), tIdx = index(tIndexName);
        assertTrue(sIdx.supportsExpression(expr("$.s", EQ, 1)));
        assertTrue(sIdx.supportsExpression(expr("$.s", GT, 1)));
        assertTrue(sIdx.supportsExpression(expr("$.s", NEQ, 1)));
        assertFalse(tIdx.supportsExpression(expr("$.s", EQ, 1)));
        assertTrue(tIdx.supportsExpression(expr("$.t", EQ, "a")));
        assertFalse(tIdx.supportsExpression(expr("$.t", GT, "a")));   // no range on text
        assertFalse(sIdx.supportsExpression(column("doc"), EQ));       // plain column expression
        assertFalse(sIdx.supportsExpression(RowFilter.builder(false).add(column("doc"), EQ, Int32Type.instance.decompose(1))));
        assertTrue(sIdx.supportsJsonPathExpression(column("doc"), JsonPath.parse("$['s']"), EQ));
        assertFalse(sIdx.supportsJsonPathExpression(column("other"), JsonPath.parse("$.s"), EQ));
    }

    /**
     * Tests that a plain column index does not claim or serve a JsonPathExpression.
     */
    @Test
    public void plainIndexDoesNotServeJsonPathExpression()
    {
        String plain = createIndex("CREATE CUSTOM INDEX ON %s(other) USING 'StorageAttachedIndex'");
        StorageAttachedIndex idx = index(plain);
        assertFalse(idx.supportsJsonPathExpression(column("other"), JsonPath.parse("$.s"), EQ));
        assertFalse(idx.supportsExpression(new RowFilter.JsonPathExpression(column("other"), JsonPath.parse("$.s"),
                                                                            Int32Type.instance, EQ,
                                                                            Int32Type.instance.decompose(1))));
        assertTrue(idx.supportsExpression(RowFilter.builder(false).add(column("other"), EQ, Int32Type.instance.decompose(1))));
    }

    /**
     * Serializes and deserializes an expression to test wire format round-tripping.
     */
    private static RowFilter.Expression roundTrip(RowFilter.Expression e, TableMetadata metadata) throws IOException
    {
        int version = MessagingService.current_version;
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            RowFilter.Expression.serializer.serialize(e, out, version);
            assertEquals(out.getLength(), RowFilter.Expression.serializer.serializedSize(e, version));
            try (DataInputBuffer in = new DataInputBuffer(out.buffer(), true))
            {
                return RowFilter.Expression.serializer.deserialize(in, version, metadata, IndexHints.NONE);
            }
        }
    }

    /**
     * Inserts a row with the given JSON document and reads it back from the memtable.
     */
    private Row rowWithDoc(String json)
    {
        execute("INSERT INTO %s (pk, doc) VALUES (1, ?)", json);
        return Util.getOnlyRow(Util.cmd(getCurrentColumnFamilyStore(), 1).build());
    }

    /**
     * Constructs a JsonPathExpression for testing against the configured table schema.
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private RowFilter.JsonPathExpression expr(String path, Operator op, Object value)
    {
        JsonPath jsonPath = JsonPath.parse(path);
        ColumnMetadata doc = column("doc");
        AbstractType<?> type = JsonPathIndexes.findIndexedType(meta, doc, jsonPath).orElseThrow();
        return new RowFilter.JsonPathExpression(doc, jsonPath, type, op, ((AbstractType) type).decompose(value));
    }

    /**
     * Looks up column metadata by name in the current table metadata.
     */
    private ColumnMetadata column(String name)
    {
        return meta.getColumn(ColumnIdentifier.getInterned(name, false));
    }

    /**
     * Retrieves the StorageAttachedIndex instance by index name from the secondary index manager.
     */
    private StorageAttachedIndex index(String name)
    {
        return (StorageAttachedIndex) getCurrentColumnFamilyStore().indexManager.getIndexByName(name);
    }
}
