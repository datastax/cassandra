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

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.tracing.TracingTestImpl;

import static org.assertj.core.api.Assertions.assertThat;

public class VectorTracingTest extends VectorTester.VersionedWithChecksums
{
    @BeforeClass
    public static void setupClass()
    {
        System.setProperty("cassandra.custom_tracing_class", "org.apache.cassandra.tracing.TracingTestImpl");
    }

    @Test
    public void tracingTest()
    {
        createTable("CREATE TABLE %s (pk int, str_val text, val vector<float, 3>, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (pk, str_val, val) VALUES (0, 'A', [1.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (1, 'B', [2.0, 3.0, 4.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (2, 'C', [3.0, 4.0, 5.0])");
        execute("INSERT INTO %s (pk, str_val, val) VALUES (3, 'D', [4.0, 5.0, 6.0])");

        flush();

        execute("INSERT INTO %s (pk, str_val, val) VALUES (4, 'E', [5.0, 2.0, 3.0])");

        Tracing.instance.newSession(ClientState.forInternalCalls(), Tracing.TraceType.QUERY);
        execute("SELECT * FROM %s ORDER BY val ann of [9.5, 5.5, 6.5] LIMIT 5");
        for (String trace : ((TracingTestImpl) Tracing.instance).getTraces())
            assertThat(trace).doesNotContain("Executing single-partition query");
        // manual inspection to verify that no extra traces were included
        logger.info(((TracingTestImpl) Tracing.instance).getTraces().toString());

        // because we parameterized the test class we need to clean up after ourselves or the second run will fail
        Tracing.instance.stopSession();
    }
}
