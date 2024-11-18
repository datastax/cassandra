/*
 * Copyright DataStax, Inc.
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
package org.apache.cassandra.guardrails;


import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static java.lang.String.format;

/**
 * Tests the guardrail for the number of column value filters per SELECT query, {@link Guardrails#queryFilters}.
 */
public class GuardrailQueryFiltersTest extends GuardrailTester
{
    private static final int WARN_THRESHOLD = 2;
    private static final int FAIL_THRESHOLD = 4;

    private int defaultWarnThreshold;
    private int defaultFailThreshold;

    @Before
    public void before()
    {
        defaultWarnThreshold = config().query_filters_warn_threshold;
        defaultFailThreshold = config().query_filters_fail_threshold;
        config().query_filters_warn_threshold = WARN_THRESHOLD;
        config().query_filters_fail_threshold = FAIL_THRESHOLD;
    }

    @After
    public void after()
    {
        config().query_filters_warn_threshold = defaultWarnThreshold;
        config().query_filters_fail_threshold = defaultFailThreshold;
    }

    @Test
    public void testConfigValidation()
    {
        config().query_filters_fail_threshold = -1;
        testValidationOfStrictlyPositiveProperty((c, v) -> c.query_filters_warn_threshold = v.intValue(),
                                                 "query_filters_warn_threshold");

        config().query_filters_warn_threshold = -1;
        testValidationOfStrictlyPositiveProperty((c, v) -> c.query_filters_fail_threshold = v.intValue(),
                                                 "query_filters_fail_threshold");

        // warn threshold larger than fail threshold
        config().query_filters_warn_threshold = 2;
        config().query_filters_fail_threshold = 1;
        assertConfigFails(config()::validate, "The warn threshold 2 for the query_filters_threshold guardrail " +
                                              "should be lower than the failure threshold 1");
    }

    @Test
    public void testQueryFilters() throws Throwable
    {
        createTable("CREATE TABLE %s (k1 int, k2 int, c1 int, c2 int, x text, y text, z text, PRIMARY KEY((k1, k2), c1, c2))");

        String x = createIndex("CREATE CUSTOM INDEX ON %s(x) " +
                               "USING 'org.apache.cassandra.index.sai.StorageAttachedIndex' " +
                               "WITH OPTIONS = { 'index_analyzer': 'standard' }");

        String y = createIndex("CREATE CUSTOM INDEX ON %s(y) " +
                               "USING 'org.apache.cassandra.index.sai.StorageAttachedIndex' " +
                               "WITH OPTIONS = { 'index_analyzer': 'standard' }");

        createIndex("CREATE CUSTOM INDEX ON %s(z) USING 'org.apache.cassandra.index.sai.StorageAttachedIndex'");

        // single column, single expression (analyzed)
        assertValid("SELECT * FROM %s WHERE x = '1'");
        assertValid("SELECT * FROM %s WHERE x = '1 2'");
        assertWarns("SELECT * FROM %s WHERE x = '1 2 3'", 3);
        assertWarns("SELECT * FROM %s WHERE x = '1 2 3 4'", 4);
        assertFails("SELECT * FROM %s WHERE x = '1 2 3 4 5'", 5);
        assertFails("SELECT * FROM %s WHERE x = '1 2 3 4 5 6'", 6);

        // single column, single expression (not analyzed)
        assertValid("SELECT * FROM %s WHERE z = '1'");
        assertValid("SELECT * FROM %s WHERE z = '1 2'");
        assertValid("SELECT * FROM %s WHERE z = '1 2 3'");
        assertValid("SELECT * FROM %s WHERE z = '1 2 3 4'");
        assertValid("SELECT * FROM %s WHERE z = '1 2 3 4 5'");
        assertValid("SELECT * FROM %s WHERE z = '1 2 3 4 5 6'");

        // single column, multiple expressions (analyzed, AND)
        assertValid("SELECT * FROM %s WHERE x = '1' AND x = '2'");
        assertWarns("SELECT * FROM %s WHERE x = '1' AND x = '2 3'", 3);
        assertWarns("SELECT * FROM %s WHERE x = '1' AND x = '2 3 4'", 4);
        assertFails("SELECT * FROM %s WHERE x = '1' AND x = '2 3 4 5'", 5);
        assertFails("SELECT * FROM %s WHERE x = '1' AND x = '2 3 4 5 6'", 6);

        // single column, multiple expressions (analyzed, OR)
        assertValid("SELECT * FROM %s WHERE x = '1' OR x = '2'");
        assertWarns("SELECT * FROM %s WHERE x = '1' OR x = '2 3'", 3);
        assertWarns("SELECT * FROM %s WHERE x = '1' OR x = '2 3 4'", 4);
        assertFails("SELECT * FROM %s WHERE x = '1' OR x = '2 3 4 5'", 5);
        assertFails("SELECT * FROM %s WHERE x = '1' OR x = '2 3 4 5 6'", 6);

        // multiple columns (analyzed, AND)
        assertValid("SELECT * FROM %s WHERE x = '1' AND y = '2'");
        assertWarns("SELECT * FROM %s WHERE x = '1' AND y = '2 3'", 3);
        assertWarns("SELECT * FROM %s WHERE x = '1' AND y = '2 3 4'", 4);
        assertFails("SELECT * FROM %s WHERE x = '1' AND y = '2 3 4 5'", 5);
        assertFails("SELECT * FROM %s WHERE x = '1' AND y = '2 3 4 5 6'", 6);

        // multiple columns (analyzed, OR)
        assertValid("SELECT * FROM %s WHERE x = '1' OR y = '2'");
        assertWarns("SELECT * FROM %s WHERE x = '1' OR y = '2 3'", 3);
        assertWarns("SELECT * FROM %s WHERE x = '1' OR y = '2 3 4'", 4);
        assertFails("SELECT * FROM %s WHERE x = '1' OR y = '2 3 4 5'", 5);
        assertFails("SELECT * FROM %s WHERE x = '1' OR y = '2 3 4 5 6'", 6);

        // multiple columns (analyzed and not analyzed, AND)
        assertWarns("SELECT * FROM %s WHERE x = '1' AND y = '2' AND z = '3'", 3);
        assertWarns("SELECT * FROM %s WHERE x = '1' AND y = '2' AND z = '3 3'", 3);
        assertWarns("SELECT * FROM %s WHERE x = '1' AND y = '2 3' AND z = '4'", 4);
        assertWarns("SELECT * FROM %s WHERE x = '1' AND y = '2 3' AND z = '4 4'", 4);
        assertFails("SELECT * FROM %s WHERE x = '1' AND y = '2 3 4' AND z = '5'", 5);
        assertFails("SELECT * FROM %s WHERE x = '1' AND y = '2 3 4' AND z = '5 5'", 5);

        // multiple columns (analyzed and not analyzed, OR)
        assertWarns("SELECT * FROM %s WHERE x = '1' OR y = '2' OR z = '3'", 3);
        assertWarns("SELECT * FROM %s WHERE x = '1' OR y = '2' OR z = '3 3'", 3);
        assertWarns("SELECT * FROM %s WHERE x = '1' OR y = '2 3' OR z = '4'", 4);
        assertWarns("SELECT * FROM %s WHERE x = '1' OR y = '2 3' OR z = '4 4'", 4);
        assertFails("SELECT * FROM %s WHERE x = '1' OR y = '2 3 4' OR z = '5'", 5);
        assertFails("SELECT * FROM %s WHERE x = '1' OR y = '2 3 4' OR z = '5 5'", 5);

        // full partition key restrictions don't count as filters
        assertValid("SELECT * FROM %s WHERE k1 = 0 AND k2 = 0 AND x = '1'");
        assertValid("SELECT * FROM %s WHERE k1 = 0 AND k2 = 0 AND x = '1 2'");
        assertWarns("SELECT * FROM %s WHERE k1 = 0 AND k2 = 0 AND x = '1 2 3'", 3);
        assertWarns("SELECT * FROM %s WHERE k1 = 0 AND k2 = 0 AND x = '1 2 3 4'", 4);
        assertFails("SELECT * FROM %s WHERE k1 = 0 AND k2 = 0 AND x = '1 2 3 4 5'", 5);
        assertFails("SELECT * FROM %s WHERE k1 = 0 AND k2 = 0 AND x = '1 2 3 4 5 6'", 6);

        // partial partition key restrictions do count as filters
        assertValid("SELECT * FROM %s WHERE k1 = 0 AND x = '1' ALLOW FILTERING");
        assertWarns("SELECT * FROM %s WHERE k1 = 0 AND x = '1 2' ALLOW FILTERING", 3);
        assertWarns("SELECT * FROM %s WHERE k1 = 0 AND x = '1 2 3' ALLOW FILTERING", 4);
        assertFails("SELECT * FROM %s WHERE k1 = 0 AND x = '1 2 3 4' ALLOW FILTERING", 5);
        assertFails("SELECT * FROM %s WHERE k1 = 0 AND x = '1 2 3 4 5' ALLOW FILTERING", 6);
        assertValid("SELECT * FROM %s WHERE k2 = 0 AND x = '1' ALLOW FILTERING");
        assertWarns("SELECT * FROM %s WHERE k2 = 0 AND x = '1 2' ALLOW FILTERING", 3);
        assertWarns("SELECT * FROM %s WHERE k2 = 0 AND x = '1 2 3' ALLOW FILTERING", 4);
        assertFails("SELECT * FROM %s WHERE k2 = 0 AND x = '1 2 3 4' ALLOW FILTERING", 5);
        assertFails("SELECT * FROM %s WHERE k2 = 0 AND x = '1 2 3 4 5' ALLOW FILTERING", 6);

        // full primary key restrictions don't count as filters
        assertValid("SELECT * FROM %s WHERE k1 = 0 AND k2 = 0 AND c1 = 0 AND x = '1'");
        assertValid("SELECT * FROM %s WHERE k1 = 0 AND k2 = 0 AND c1 = 0 AND x = '1 2'");
        assertWarns("SELECT * FROM %s WHERE k1 = 0 AND k2 = 0 AND c1 = 0 AND x = '1 2 3'", 3);
        assertWarns("SELECT * FROM %s WHERE k1 = 0 AND k2 = 0 AND c1 = 0 AND x = '1 2 3 4'", 4);
        assertFails("SELECT * FROM %s WHERE k1 = 0 AND k2 = 0 AND c1 = 0 AND x = '1 2 3 4 5'", 5);
        assertFails("SELECT * FROM %s WHERE k1 = 0 AND k2 = 0 AND c1 = 0 AND x = '1 2 3 4 5 6'", 6);
        assertValid("SELECT * FROM %s WHERE k1 = 0 AND k2 = 0 AND c1 = 0 AND c2 = 0 AND x = '1'");
        assertValid("SELECT * FROM %s WHERE k1 = 0 AND k2 = 0 AND c1 = 0 AND c2 = 0 AND x = '1 2'");
        assertWarns("SELECT * FROM %s WHERE k1 = 0 AND k2 = 0 AND c1 = 0 AND c2 = 0 AND x = '1 2 3'", 3);
        assertWarns("SELECT * FROM %s WHERE k1 = 0 AND k2 = 0 AND c1 = 0 AND c2 = 0 AND x = '1 2 3 4'", 4);
        assertFails("SELECT * FROM %s WHERE k1 = 0 AND k2 = 0 AND c1 = 0 AND c2 = 0 AND x = '1 2 3 4 5'", 5);
        assertFails("SELECT * FROM %s WHERE k1 = 0 AND k2 = 0 AND c1 = 0 AND c2 = 0 AND x = '1 2 3 4 5 6'", 6);

        // partial primary key restrictions do count as filters
        assertValid("SELECT * FROM %s WHERE c2 = 0 AND x = '1' ALLOW FILTERING");
        assertWarns("SELECT * FROM %s WHERE c2 = 0 AND x = '1 2' ALLOW FILTERING", 3);
        assertWarns("SELECT * FROM %s WHERE c2 = 0 AND x = '1 2 3' ALLOW FILTERING", 4);
        assertFails("SELECT * FROM %s WHERE c2 = 0 AND x = '1 2 3 4 5' ALLOW FILTERING", 6);
        assertFails("SELECT * FROM %s WHERE c2 = 0 AND x = '1 2 3 4 5 6' ALLOW FILTERING", 7);
        assertValid("SELECT * FROM %s WHERE k1 = 0 AND k2 = 0 AND c2 = 0 AND x = '1' ALLOW FILTERING");
        assertWarns("SELECT * FROM %s WHERE k1 = 0 AND k2 = 0 AND c2 = 0 AND x = '1 2' ALLOW FILTERING", 3);
        assertWarns("SELECT * FROM %s WHERE k1 = 0 AND k2 = 0 AND c2 = 0 AND x = '1 2 3' ALLOW FILTERING", 4);
        assertFails("SELECT * FROM %s WHERE k1 = 0 AND k2 = 0 AND c2 = 0 AND x = '1 2 3 4 5' ALLOW FILTERING", 6);
        assertFails("SELECT * FROM %s WHERE k1 = 0 AND k2 = 0 AND c2 = 0 AND x = '1 2 3 4 5 6' ALLOW FILTERING", 7);

        // without the analyzed indexes
        dropIndex("DROP INDEX %s." + x);
        dropIndex("DROP INDEX %s." + y);
        assertValid("SELECT * FROM %s WHERE x = '1' ALLOW FILTERING");
        assertValid("SELECT * FROM %s WHERE x = '1 2' ALLOW FILTERING");
        assertValid("SELECT * FROM %s WHERE x = '1 2 3' ALLOW FILTERING");
        assertValid("SELECT * FROM %s WHERE x = '1' AND y = '2' ALLOW FILTERING");
        assertValid("SELECT * FROM %s WHERE x = '1 2' AND y = '3 4' ALLOW FILTERING");
        assertValid("SELECT * FROM %s WHERE x = '1 2 3' AND y = '4 5 6' ALLOW FILTERING");
        assertWarns("SELECT * FROM %s WHERE x = '1' AND y = '2' AND z = '3' ALLOW FILTERING", 3);
        assertWarns("SELECT * FROM %s WHERE x = '1 2' AND y = '3 4' AND z = '5 6' ALLOW FILTERING", 3);
        assertWarns("SELECT * FROM %s WHERE x = '1 2 3' AND y = '4 5 6' AND z = '7 8 9' ALLOW FILTERING", 3);
    }

    @Test
    public void testExcludedUsers() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, x text, y text)");

        createIndex("CREATE CUSTOM INDEX ON %s(x) " +
                    "USING 'org.apache.cassandra.index.sai.StorageAttachedIndex' " +
                    "WITH OPTIONS = { 'index_analyzer': 'standard' }");

        createIndex("CREATE CUSTOM INDEX ON %s(y) " +
                    "USING 'org.apache.cassandra.index.sai.StorageAttachedIndex' " +
                    "WITH OPTIONS = { 'index_analyzer': 'standard' }");

        testExcludedUsers(() -> "SELECT * FROM %s WHERE x = '1 2 3'",
                          () -> "SELECT * FROM %s WHERE x = '1 2 3' AND y = '4 5 6'");
    }

    @Test
    public void testDisabledGuardrail() throws Throwable
    {
        config().query_filters_warn_threshold = -1;
        config().query_filters_fail_threshold = -1;

        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text)");

        createIndex("CREATE CUSTOM INDEX ON %s(v) " +
                    "USING 'org.apache.cassandra.index.sai.StorageAttachedIndex' " +
                    "WITH OPTIONS = { 'index_analyzer': 'standard' }");

        assertValid("SELECT * FROM %s WHERE v = '1'");
        assertValid("SELECT * FROM %s WHERE v = '1 2 3 4 5 6'");
    }

    private void assertWarns(String query, int operations) throws Throwable
    {
        assertWarns(format("Select query has %s column value filters after analysis, this exceeds the warning threshold of %s.",
                           operations, WARN_THRESHOLD),
                    query);
    }

    private void assertFails(String query, int operations) throws Throwable
    {
        assertFails(format("Select query has %s column value filters after analysis, this exceeds the failure threshold of %s.",
                           operations, FAIL_THRESHOLD),
                    query);
    }
}
