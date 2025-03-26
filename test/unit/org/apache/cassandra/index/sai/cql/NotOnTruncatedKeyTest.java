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

import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.SAIUtil;
import org.apache.cassandra.index.sai.disk.format.Version;

@RunWith(Parameterized.class)
public class NotOnTruncatedKeyTest extends SAITester
{
    @Parameterized.Parameter
    public Version version;
    @Parameterized.Parameter(1)
    public String truncatedType;

    private Version latest;

    @Before
    public void setup() throws Throwable
    {
        latest = Version.latest();
        SAIUtil.setLatestVersion(version);
    }

    @After
    public void teardown() throws Throwable
    {
        SAIUtil.setLatestVersion(latest);
    }

    @Test
    public void testLossyQueries() throws Throwable
    {
        createTable("CREATE TABLE %s (pk text PRIMARY KEY, x " + truncatedType + ')');
        createIndex("CREATE CUSTOM INDEX ON %s(x) USING 'StorageAttachedIndex'");

        // Decimals and Big Integers are truncated to 24 bytes in the numeric index.
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
            assertRows(execute("SELECT pk FROM %s WHERE x NOT IN (" + b + ')'),
                       row("a"), row("c"), row("d"));
            assertRows(execute("SELECT pk FROM %s WHERE x != " + b),
                       row("a"), row("c"), row("d"));
        });
    }

    @Parameterized.Parameters
    public static List<Object[]> data()
    {
        var indexVersions = List.of(Version.DB, Version.EB);
        var truncatedTypes = List.of("decimal", "varint");
        return Lists.cartesianProduct(indexVersions, truncatedTypes)
                    .stream().map(List::toArray).collect(Collectors.toList());
    }
}
