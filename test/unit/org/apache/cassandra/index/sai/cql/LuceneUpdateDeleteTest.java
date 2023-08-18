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

import org.junit.Test;

import org.apache.cassandra.index.sai.SAITester;

import static org.junit.Assert.assertEquals;

public class LuceneUpdateDeleteTest extends SAITester
{
    @Test
    public void removeUpdateAndDeleteTextInMemoryTest() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, val text)");

        createIndex("CREATE CUSTOM INDEX ON %s(val) " +
                    "USING 'org.apache.cassandra.index.sai.StorageAttachedIndex' " +
                    "WITH OPTIONS = { 'index_analyzer': '[{\"tokenizer\": \"standard\"}, {\"filter\": \"lowercase\"}]' }");

        waitForIndexQueryable();

        // The analyzed text column will result in overlapping and non-overlapping tokens in the in memory trie map.
        // Note that capitalization is covered as well as tokenization.
        execute("INSERT INTO %s (id, val) VALUES (0, 'a sad doG.')");
        execute("INSERT INTO %s (id, val) VALUES (1, 'A Happy DOG.')");

        // Prove initial assumptions about data structures are correct.
        assertEquals(2, execute("SELECT * FROM %s WHERE val : 'dog'").size());
        assertEquals(2, execute("SELECT * FROM %s WHERE val : 'a'").size());
        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'happy'").size());
        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'sad'").size());

        execute("UPDATE %s SET val = null WHERE id = 0");

        // Prove that we can remove a row when we update the data
        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'dog'").size());
        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'a'").size());
        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'happy'").size());
        assertEquals(0, execute("SELECT * FROM %s WHERE val : 'sad'").size());

        execute("UPDATE %s SET val = 'the dog' WHERE id = 0");

        // Prove that we can remove a row when we update the data
        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'the'").size());
        assertEquals(2, execute("SELECT * FROM %s WHERE val : 'dog'").size());
        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'a'").size());
        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'happy'").size());
        assertEquals(0, execute("SELECT * FROM %s WHERE val : 'sad'").size());

        execute("DELETE from %s WHERE id = 1");

        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'dog'").size());
        assertEquals(0, execute("SELECT * FROM %s WHERE val : 'a'").size());
        assertEquals(0, execute("SELECT * FROM %s WHERE val : 'happy'").size());
        assertEquals(0, execute("SELECT * FROM %s WHERE val : 'sad'").size());
    }
}
