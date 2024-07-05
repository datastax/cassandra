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
package org.apache.cassandra.db.guardrails;


import org.junit.Test;

import static java.lang.String.format;

/**
 * Tests the guardrail for the number of rows that a LIMIT/OFFSET SELECT query can skip, {@link Guardrails#offsetRows}.
 */
public class GuardrailOffsetRowsTest extends ThresholdTester
{
    private static final int WARN_THRESHOLD = 2;
    private static final int FAIL_THRESHOLD = 4;

    public GuardrailOffsetRowsTest()
    {
        super(WARN_THRESHOLD,
              FAIL_THRESHOLD,
              Guardrails.offsetRows,
              Guardrails::setOffsetRowsThreshold,
              Guardrails::getOffsetRowsWarnThreshold,
              Guardrails::getOffsetRowsFailThreshold);
    }

    @Test
    public void testOffset() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c1 int, c2 int, v int, PRIMARY KEY (k, c1, c2))");

        testGuardrail("SELECT * FROM %s LIMIT 100 OFFSET %d");
        testGuardrail("SELECT * FROM %s PER PARTITION LIMIT 3 LIMIT 100 OFFSET %d");
        testGuardrail("SELECT * FROM %s WHERE k = 0 GROUP BY k, c1 LIMIT 100 OFFSET %d");
        testGuardrail("SELECT k, c1, c2, sum(v) FROM %s WHERE k = 0 GROUP BY k, c1 LIMIT 100 OFFSET %d");
        testGuardrail("SELECT k, c1, c2, sum(v) FROM %s WHERE k = 0 LIMIT 100 OFFSET %d");
    }

    @Test
    public void testExcludedUsers() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c1 int, c2 int, v int, PRIMARY KEY (k, c1, c2))");
        int offset = FAIL_THRESHOLD + 1;
        testExcludedUsers(() -> formatQuery("SELECT * FROM %s LIMIT 100 OFFSET %d", offset),
                          () -> formatQuery("SELECT * FROM %s PER PARTITION LIMIT 3 LIMIT 100 OFFSET %d", offset),
                          () -> formatQuery("SELECT * FROM %s WHERE k = 0 GROUP BY k, c1 LIMIT 100 OFFSET %d", offset),
                          () -> formatQuery("SELECT k, c1, c2, sum(v) FROM %s WHERE k = 0 GROUP BY k, c1 LIMIT 100 OFFSET %d", offset),
                          () -> formatQuery("SELECT k, c1, c2, sum(v) FROM %s WHERE k = 0 LIMIT 100 OFFSET %d", offset));
    }

    private void testGuardrail(String query) throws Throwable
    {
        assertValid(query, 1);
        assertValid(query, WARN_THRESHOLD);
        assertWarns(query, WARN_THRESHOLD + 1);
        assertWarns(query, FAIL_THRESHOLD);
        assertFails(query, FAIL_THRESHOLD + 1);
        assertFails(query, Integer.MAX_VALUE);
    }

    private String formatQuery(String query, int offset)
    {
        return format(query, currentTable(), offset);
    }

    private void assertValid(String query, int offset) throws Throwable
    {
        super.assertValid(formatQuery(query, offset));
    }

    private void assertWarns(String query, int offset) throws Throwable
    {
        assertWarns(formatQuery(query, offset),
                    format("Select query requested to skip %s rows, this exceeds the warning threshold of %s.",
                           offset, WARN_THRESHOLD));
    }

    private void assertFails(String query, int offset) throws Throwable
    {
        assertFails(formatQuery(query, offset),
                    format("Select query requested to skip %s rows, this exceeds the failure threshold of %s.",
                           offset, FAIL_THRESHOLD));
    }
}