/*
 * Copyright IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.index.sai.cql;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.tracing.TracingTestImpl;
import org.assertj.core.api.Assertions;

public class QueryTracingTest extends SAITester
{
    private static TracingTestImpl tracing;

    @BeforeClass
    public static void setUpClass()
    {
        System.setProperty("cassandra.custom_tracing_class", "org.apache.cassandra.tracing.TracingTestImpl");
        SAITester.setUpClass();
        Tracing.instance.newSession(ClientState.forInternalCalls(), Tracing.TraceType.QUERY);
        tracing = (TracingTestImpl) Tracing.instance;
    }

    @AfterClass
    public static void tearDownClass()
    {
        tracing.stopSession();
        SAITester.tearDownClass();
    }

    @Test
    public void testTracing()
    {
        createTable("CREATE TABLE %s(k int PRIMARY KEY, n int, t text, at text, v vector<float,2>)");
        execute("INSERT INTO %s(k, n, t, at, v) VALUES (0, 0, 'hello', 'good bye', [1, 2])");
        createIndex("CREATE CUSTOM INDEX ON %s(n) USING 'org.apache.cassandra.index.sai.StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(t) USING 'org.apache.cassandra.index.sai.StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(at) USING 'org.apache.cassandra.index.sai.StorageAttachedIndex' WITH OPTIONS = { 'index_analyzer': 'standard' }");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'org.apache.cassandra.index.sai.StorageAttachedIndex'");

        // queries on numeric index
        assertTraceHasPlan("SELECT * FROM %s WHERE n = 0",
                           "Filter n = 0",
                           "NumericIndexScan",
                           "predicate: Expression{name: n, op: EQ, lower: (0, true), upper: (0, true), exclusions: []}");
        assertTraceHasPlan("SELECT * FROM %s WHERE n > 0",
                           "Filter n > 0",
                           "NumericIndexScan",
                           "predicate: Expression{name: n, op: RANGE, lower: (0, false), upper: (null, false), exclusions: []}");
        assertTraceHasPlan("SELECT * FROM %s WHERE n < 0",
                           "Filter n < 0",
                           "NumericIndexScan",
                           "predicate: Expression{name: n, op: RANGE, lower: (null, false), upper: (0, false), exclusions: []}");
        assertTraceHasPlan("SELECT * FROM %s WHERE n >= 0",
                           "Filter n >= 0",
                           "NumericIndexScan",
                           "predicate: Expression{name: n, op: RANGE, lower: (0, true), upper: (null, false), exclusions: []}");
        assertTraceHasPlan("SELECT * FROM %s WHERE n <= 0",
                           "Filter n <= 0",
                           "NumericIndexScan",
                           "predicate: Expression{name: n, op: RANGE, lower: (null, false), upper: (0, true), exclusions: []}");
        assertTraceHasPlan("SELECT * FROM %s ORDER BY n LIMIT 10",
                           "NumericIndexScan",
                           "ordering: n ASC");

        // queries on literal index
        assertTraceHasPlan("SELECT * FROM %s WHERE t = 'hello'",
                           "Filter t = 'hello'",
                           "LiteralIndexScan",
                           "predicate: Expression{name: t, op: EQ, lower: (hello, true), upper: (hello, true), exclusions: []}");
        assertTraceHasPlan("SELECT * FROM %s ORDER BY t LIMIT 10",
                           "LiteralIndexScan",
                           "ordering: t ASC");

        // queries on analyzed index
        assertTraceHasPlan("SELECT * FROM %s WHERE at : 'good'",
                           "Filter at : 'good'",
                           "LiteralIndexScan",
                           "predicate: Expression{name: at, op: MATCH, lower: (good, true), upper: (good, true), exclusions: []}");
        assertTraceHasPlan("SELECT * FROM %s ORDER BY at BM25 OF 'good' LIMIT 10",
                           "Bm25IndexScan",
                           "at BM25 OF 'good' DESC");

        // queries on vector index
        assertTraceHasPlan("SELECT * FROM %s ORDER BY v ANN OF [1.2, 3.4] LIMIT 10",
                           "AnnIndexScan",
                           "v ANN OF [1.2, 3.4] DESC");
    }

    private void assertTraceHasPlan(String query, String... expected)
    {
        TracingTestImpl tracing = (TracingTestImpl) Tracing.instance;
        tracing.getTraces().clear();

        execute(query);

        for (String trace : tracing.getTraces())
        {
            if (!trace.startsWith("Query execution plan"))
                continue;

            for (String s : expected)
            {
                Assertions.assertThat(trace).as("Trace should contain {} but found: {}", expected, trace).contains(s);
            }
            return;
        }
        Assert.fail("Query plan not found in traces");
    }
}
