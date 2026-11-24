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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.json.JsonPath;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class JsonPathWritePathTest extends SAITester
{
    private static final String PREFIX = "CREATE CUSTOM INDEX ON %s(doc) USING 'StorageAttachedIndex' WITH OPTIONS = ";

    /**
     * Tests that IndexContext correctly extracts multi-valued typed terms from a JSON document cell in a row.
     */
    @Test
    public void extractsTermsFromRow()
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, doc text)");
        String idx = createIndex(PREFIX + "{'json_path': '$.a', 'json_type': 'int'}");
        execute("INSERT INTO %s (pk, doc) VALUES (1, '{\"a\":[1,2]}')");
        IndexContext ctx = getIndexContext(idx);
        assertTrue(ctx.isJsonPath());
        assertTrue(ctx.isMultiValued());
        assertEquals(JsonPath.parse("$.a"), ctx.getJsonPath());
        assertEquals(Int32Type.instance, ctx.getValidator());
        Row row = Util.getOnlyRow(Util.cmd(getCurrentColumnFamilyStore(), 1).build());
        assertEquals(List.of(1, 2), decode(ctx.getValuesOf(row, FBUtilities.nowInSeconds()), Int32Type.instance));
    }

    /**
     * Tests that a plain column restriction (e.g. doc = 'x') is rejected and not served by a JSON path index on that column.
     */
    @Test
    public void plainColumnRestrictionNotServedByJsonIndex()
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, doc text)");
        createIndex(PREFIX + "{'json_path': '$.a', 'json_type': 'text'}");
        execute("INSERT INTO %s (pk, doc) VALUES (1, '{\"a\":\"x\"}')");
        assertInvalidMessage("ALLOW FILTERING", "SELECT pk FROM %s WHERE doc = 'x'");
    }

    /**
     * Tests that inserting non-JSON strings into a column with a JSON path index succeeds without error during write and flush.
     */
    @Test
    public void nonJsonWriteSucceeds()
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, doc text)");
        createIndex(PREFIX + "{'json_path': '$.a', 'json_type': 'text'}");
        execute("INSERT INTO %s (pk, doc) VALUES (1, 'not json')");
        flush();
    }

    /**
     * Tests that overwrites, type mismatches, missing paths, and malformed JSON rows succeed cleanly through write and flush.
     */
    @Test
    public void jsonWritesOverwritesAndFlushSucceed()
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, doc text)");
        createIndex(PREFIX + "{'json_path': '$.a', 'json_type': 'int'}");
        execute("INSERT INTO %s (pk, doc) VALUES (1, '{\"a\":[1,2]}')");
        execute("INSERT INTO %s (pk, doc) VALUES (1, '{\"a\":3}')");
        execute("INSERT INTO %s (pk, doc) VALUES (2, '{\"a\":\"wrong type\"}')");
        execute("INSERT INTO %s (pk, doc) VALUES (3, '{\"b\":1}')");
        execute("INSERT INTO %s (pk, doc) VALUES (4, '{broken')");
        flush();
    }

    /**
     * Tests that IndexContext.supports(Operator) enforces supported operators per JSON target type (EQ/NEQ for all, slices for numeric only).
     */
    @Test
    public void supportedOperators()
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, doc text)");
        IndexContext intCtx = getIndexContext(createIndex(PREFIX + "{'json_path': '$.i', 'json_type': 'int'}"));
        IndexContext bigintCtx = getIndexContext(createIndex(PREFIX + "{'json_path': '$.l', 'json_type': 'bigint'}"));
        IndexContext doubleCtx = getIndexContext(createIndex(PREFIX + "{'json_path': '$.d', 'json_type': 'double'}"));
        IndexContext textCtx = getIndexContext(createIndex(PREFIX + "{'json_path': '$.t', 'json_type': 'text'}"));
        IndexContext asciiCtx = getIndexContext(createIndex(PREFIX + "{'json_path': '$.s', 'json_type': 'ascii'}"));
        IndexContext boolCtx = getIndexContext(createIndex(PREFIX + "{'json_path': '$.b', 'json_type': 'boolean'}"));

        for (IndexContext ctx : List.of(intCtx, bigintCtx, doubleCtx, textCtx, asciiCtx, boolCtx))
        {
            assertTrue(ctx.supports(Operator.EQ));
            assertTrue(ctx.supports(Operator.NEQ));
            for (Operator op : List.of(Operator.IN, Operator.CONTAINS, Operator.CONTAINS_KEY, Operator.NOT_CONTAINS,
                                       Operator.NOT_CONTAINS_KEY, Operator.LIKE, Operator.LIKE_PREFIX,
                                       Operator.LIKE_CONTAINS, Operator.ANN, Operator.BOUNDED_ANN, Operator.NOT_IN, Operator.ORDER_BY_ASC,
                                       Operator.ORDER_BY_DESC, Operator.ANALYZER_MATCHES, Operator.BM25))
                assertFalse(ctx.getIndexName() + ' ' + op, ctx.supports(op));
        }
        for (IndexContext ctx : List.of(intCtx, bigintCtx, doubleCtx))
            for (Operator op : List.of(Operator.LT, Operator.LTE, Operator.GT, Operator.GTE))
                assertTrue(ctx.getIndexName() + ' ' + op, ctx.supports(op));
        for (IndexContext ctx : List.of(textCtx, asciiCtx, boolCtx))
            for (Operator op : List.of(Operator.LT, Operator.LTE, Operator.GT, Operator.GTE))
                assertFalse(ctx.getIndexName() + ' ' + op, ctx.supports(op));
    }

    /**
     * Tests that standard non-JSON indexes on scalar columns are not marked as multi-valued or JSON path indexes.
     */
    @Test
    public void nonJsonIndexIsNotMultiValued()
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, doc text, m map<int,int>)");
        IndexContext plain = getIndexContext(createIndex("CREATE CUSTOM INDEX ON %s(doc) USING 'StorageAttachedIndex'"));
        IndexContext coll = getIndexContext(createIndex("CREATE CUSTOM INDEX ON %s(m) USING 'StorageAttachedIndex'"));
        assertFalse(plain.isJsonPath());
        assertFalse(plain.isMultiValued());
        assertEquals(null, plain.getJsonPath());
        assertFalse(coll.isJsonPath());
        assertTrue(coll.isMultiValued());
    }

    /**
     * Decodes an iterator of ByteBuffers into a list of composed Java values according to the specified AbstractType.
     */
    private static <T> List<T> decode(Iterator<ByteBuffer> values, AbstractType<T> type)
    {
        List<T> result = new ArrayList<>();
        while (values != null && values.hasNext())
            result.add(type.compose(values.next()));
        return result;
    }
}
