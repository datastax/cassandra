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

import java.util.Optional;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.json.JsonPath;
import org.apache.cassandra.index.sai.json.JsonPathIndexes;

public class JsonPathIndexDDLTest extends SAITester
{
    private static final String PREFIX = "CREATE CUSTOM INDEX ON %s(doc) USING 'StorageAttachedIndex' WITH OPTIONS = ";

    /**
     * Tests creation of JSON path indexes with different path and type options alongside standard SAI column indexes.
     */
    @Test
    public void createsPathIndexes()
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, doc text)");
        createIndex(PREFIX + "{'json_path': '$.profile.age', 'json_type': 'int'}");
        createIndex(PREFIX + "{'json_path': '$.city', 'json_type': 'text'}");
        createIndex("CREATE CUSTOM INDEX ON %s(doc) USING 'StorageAttachedIndex'");
        assertEquals(Optional.of(Int32Type.instance),
                     JsonPathIndexes.findIndexedType(currentTableMetadata(), getColumn("doc"), JsonPath.parse("$['profile'].age")));
        assertEquals(Optional.empty(),
                     JsonPathIndexes.findIndexedType(currentTableMetadata(), getColumn("doc"), JsonPath.parse("$.nope")));
    }

    /**
     * Tests that duplicate indexes on the same path are rejected even if the path syntax is spelled differently.
     */
    @Test
    public void rejectsDuplicatePathEvenIfSpelledDifferently()
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, doc text)");
        createIndex(PREFIX + "{'json_path': '$.a', 'json_type': 'int'}");
        assertInvalidMessage("duplicate", PREFIX + "{'json_path': '$[''a'']', 'json_type': 'bigint'}");
    }

    /**
     * Tests that invalid index definitions (missing required options, unsupported types, non-text columns, analyzer combinations) are rejected.
     */
    @Test
    public void rejectsBadDefinitions()
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, doc text, n int, m map<text,text>)");
        assertInvalidMessage("json_type", PREFIX + "{'json_path': '$.a'}");
        assertInvalidMessage("json_path", PREFIX + "{'json_type': 'int'}");
        assertInvalid(PREFIX + "{'json_path': '$..a', 'json_type': 'int'}");
        assertInvalidMessage("json_type", PREFIX + "{'json_path': '$.a', 'json_type': 'varint'}");
        assertInvalidMessage("text or ascii", "CREATE CUSTOM INDEX ON %s(n) USING 'StorageAttachedIndex' WITH OPTIONS = {'json_path': '$.a', 'json_type': 'int'}");
        assertInvalidMessage("text or ascii", "CREATE CUSTOM INDEX ON %s(values(m)) USING 'StorageAttachedIndex' WITH OPTIONS = {'json_path': '$.a', 'json_type': 'text'}");
        assertInvalidMessage("cannot be combined", PREFIX + "{'json_path': '$.a', 'json_type': 'text', 'case_sensitive': 'false'}");
    }
}
