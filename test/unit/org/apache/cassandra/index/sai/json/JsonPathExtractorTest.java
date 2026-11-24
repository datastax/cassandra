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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class JsonPathExtractorTest
{
    /**
     * Helper to extract and compose terms from a JSON string for a given path and target type.
     */
    private static List<Object> terms(String json, String path, AbstractType<?> type)
    {
        return terms(json.getBytes(StandardCharsets.UTF_8), path, type);
    }

    /**
     * Helper to extract and compose terms from raw JSON bytes for a given path and target type.
     */
    private static List<Object> terms(byte[] json, String path, AbstractType<?> type)
    {
        Iterator<ByteBuffer> it = new JsonPathExtractor(JsonPath.parse(path), type).extract(ByteBuffer.wrap(json));
        List<Object> result = new ArrayList<>();
        while (it.hasNext())
            result.add(type.compose(it.next()));
        return result;
    }

    /**
     * Tests extraction of scalar values at both top-level and nested paths.
     */
    @Test
    public void scalarAndNested()
    {
        assertEquals(List.of(30), terms("{\"profile\":{\"age\":30}}", "$.profile.age", Int32Type.instance));
        assertEquals(List.of("berlin"), terms("{\"address\":{\"city\":\"berlin\"}}", "$.address.city", UTF8Type.instance));
        assertEquals(List.of(true), terms("{\"ok\":true}", "$.ok", BooleanType.instance));
    }

    /**
     * Tests that array elements at the path target are individually extracted, ignoring non-fitting elements and sub-objects.
     */
    @Test
    public void arrayElementsUnderArrayPath()
    {
        assertEquals(List.of("admin", "dev"), terms("{\"tags\":[\"admin\",\"dev\",1,[\"x\"],{\"y\":1}]}", "$.tags", UTF8Type.instance));
        assertEquals(List.of("a", "a"), terms("{\"tags\":[\"a\",\"a\"]}", "$.tags", UTF8Type.instance));
        assertEquals(List.of("dev"), terms("{\"tags\":[\"admin\",\"dev\"]}", "$.tags[1]", UTF8Type.instance));
    }

    /**
     * Tests that mid-path arrays require explicit [n] index steps and are not automatically fanned out.
     */
    @Test
    public void midPathArraysAreNotFannedOut()
    {
        assertEquals(List.of(), terms("{\"a\":[{\"b\":1}]}", "$.a.b", Int32Type.instance));
        assertEquals(List.of(), terms("{\"a\":{\"b\":1}}", "$.a[0]", Int32Type.instance));
        assertEquals(List.of(1), terms("{\"a\":[{\"b\":1}]}", "$.a[0].b", Int32Type.instance));
    }

    /**
     * Tests number value extraction, including value equivalence (e.g. 30, 30.0, 3e1) and range checks for Int32 vs Long vs Double.
     */
    @Test
    public void numbersByValue()
    {
        assertEquals(List.of(30, 30, 30), terms("{\"a\":[30,30.0,3e1,30.5]}", "$.a", Int32Type.instance));
        assertEquals(List.of(), terms("{\"a\":3000000000}", "$.a", Int32Type.instance));
        assertEquals(List.of(3000000000L), terms("{\"a\":3000000000}", "$.a", LongType.instance));
        assertEquals(List.of(30.5), terms("{\"a\":30.5}", "$.a", DoubleType.instance));
    }

    /**
     * Tests that type mismatches (strings vs numbers, objects, nulls, non-ASCII chars for AsciiType) yield no terms.
     */
    @Test
    public void mismatchesGiveNoTerms()
    {
        assertEquals(List.of(), terms("{\"a\":\"30\"}", "$.a", Int32Type.instance));
        assertEquals(List.of(), terms("{\"a\":{\"b\":1}}", "$.a", Int32Type.instance));
        assertEquals(List.of(), terms("{\"a\":null}", "$.a", UTF8Type.instance));
        assertEquals(List.of(), terms("{\"b\":1}", "$.a", Int32Type.instance));
        assertEquals(List.of(), terms("{\"a\":\"ü\"}", "$.a", AsciiType.instance));
    }

    /**
     * Tests that malformed JSON payloads return empty terms without throwing exceptions.
     */
    @Test
    public void malformedGivesNoTerms()
    {
        assertEquals(List.of(), terms("not json", "$.a", UTF8Type.instance));
        assertEquals(List.of(), terms("{\"a\":", "$.a", UTF8Type.instance));
        assertEquals(List.of(), terms(new byte[0], "$.a", UTF8Type.instance));
        assertEquals(List.of(), terms(new byte[]{ '{', '"', 'a', '"', ':', '"', (byte) 0xff, (byte) 0xfe, '"', '}' }, "$.a", UTF8Type.instance));
    }

    /**
     * Tests that extracting terms from identical JSON input is deterministic.
     */
    @Test
    public void deterministic()
    {
        String doc = "{\"a\":[3,1,2]}";
        assertEquals(terms(doc, "$.a", Int32Type.instance), terms(doc, "$.a", Int32Type.instance));
    }

    /**
     * Tests that supportedTypes accurately reports supported vs unsupported Cassandra types for JSON path indexing.
     */
    @Test
    public void supportedTypes()
    {
        assertTrue(JsonPathExtractor.isSupportedType(UTF8Type.instance));
        assertTrue(JsonPathExtractor.isSupportedType(AsciiType.instance));
        assertTrue(JsonPathExtractor.isSupportedType(Int32Type.instance));
        assertTrue(JsonPathExtractor.isSupportedType(LongType.instance));
        assertTrue(JsonPathExtractor.isSupportedType(DoubleType.instance));
        assertTrue(JsonPathExtractor.isSupportedType(BooleanType.instance));
        assertFalse(JsonPathExtractor.isSupportedType(org.apache.cassandra.db.marshal.UUIDType.instance));
    }
}
