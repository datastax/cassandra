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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.utils.TypeUtil;

public class NotOnTruncatedKeyTest extends AbstractQueryTester
{
    @Test
    public void testLossyDecimalQueries() throws Throwable
    {
        createTable("CREATE TABLE %s (pk text PRIMARY KEY, x decimal)");
        createIndex("CREATE CUSTOM INDEX ON %s(x) USING 'StorageAttachedIndex'");

        // Decimals are truncated to 24 bytes in the numeric index.
        // These numbers are chosen to test for the expected results in the case of truncation.
        var a = "1111111111111111111111111111111111111111111111111111111111";
        var b = "1111111111111111111111111111111111111111111111111111111112";
        var c = "1111111111111111111111111111111111111111111111111111111113";

        var d = "1";

        execute("INSERT INTO %s (pk, x) VALUES ('a', " + a + ')');
        execute("INSERT INTO %s (pk, x) VALUES ('b', " + b + ')');
        execute("INSERT INTO %s (pk, x) VALUES ('c', " + c + ')');
        execute("INSERT INTO %s (pk, x) VALUES ('d', " + d + ')');

        beforeAndAfterFlush(() -> {
            // Test two kinds of NOT queries
            assertRows(execute("SELECT pk FROM %s WHERE x NOT IN (" + b + ')'), row("a"), row("c"), row("d"));
            assertRows(execute("SELECT pk FROM %s WHERE x != " + b), row("a"), row("c"), row("d"));
        });
    }

    @Test
    public void testLossyVarintQueries() throws Throwable
    {
        createTable("CREATE TABLE %s (pk text PRIMARY KEY, x varint)");
        createIndex("CREATE CUSTOM INDEX ON %s(x) USING 'StorageAttachedIndex'");

        // Big integers are truncated to 24 bytes in the numeric index.
        // These numbers are chosen to test for the expected results in the case of truncation.
        var a = "1111111111111111111111111111111111111111111111111111111111";
        var b = "1111111111111111111111111111111111111111111111111111111112";
        var c = "1111111111111111111111111111111111111111111111111111111113";
        var d = "1";

        execute("INSERT INTO %s (pk, x) VALUES ('a', " + a + ')');
        execute("INSERT INTO %s (pk, x) VALUES ('b', " + b + ')');
        execute("INSERT INTO %s (pk, x) VALUES ('c', " + c + ')');
        execute("INSERT INTO %s (pk, x) VALUES ('d', " + d + ')');

        beforeAndAfterFlush(() -> {
            // Test two kinds of NOT queries
            assertRows(execute("SELECT pk FROM %s WHERE x NOT IN (" + b + ')'), row("a"), row("c"), row("d"));
            assertRows(execute("SELECT pk FROM %s WHERE x != " + b), row("a"), row("c"), row("d"));
        });
    }
}
