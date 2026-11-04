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

/**
 * An empty string is a value of the text columns, indexed by an INSERT: it must be indexed as well when it's written
 * by an update of a row already in the memtable (the update path of the memtable index), from null or from another
 * value, for a text column and for the elements of a collection.
 */
public class EmptyStringUpdatesTest extends SAITester
{
    @Test
    public void testEmptyStringsWrittenByUpdates() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, v int, name text, tags list<text>)");
        createIndex("CREATE INDEX ON %s(name) USING 'sai'");
        createIndex("CREATE INDEX ON %s(tags) USING 'sai'");

        // the index path
        execute("INSERT INTO %s (id, name, tags) VALUES (1, '', [''])");
        // the update path: from null, and from another value
        execute("INSERT INTO %s (id, v) VALUES (2, 0)");
        execute("UPDATE %s SET name = '', tags = tags + [''] WHERE id = 2");
        execute("INSERT INTO %s (id, name, tags) VALUES (3, 'x', ['x'])");
        execute("UPDATE %s SET name = '', tags = tags + [''] WHERE id = 3");
        // and back to a value
        execute("INSERT INTO %s (id, name) VALUES (4, '')");
        execute("UPDATE %s SET name = 'y' WHERE id = 4");

        CheckedFunction check = () -> {
            assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE name = ''"), row(1), row(2), row(3));
            assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE tags CONTAINS ''"), row(1), row(2), row(3));
            assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE name = 'x'"));
            assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE name = 'y'"), row(4));
        };
        check.apply();
        flush();
        check.apply();
        compact();
        check.apply();
    }
}
