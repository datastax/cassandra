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

package org.apache.cassandra.cql3.statements;

import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.datastax.driver.core.ResultSet;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.schema.KeyspaceParams;

import static org.hamcrest.CoreMatchers.containsStringIgnoringCase;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class CreateFunctionStatementTest extends CQLTester
{
    private static final String[] DETERMINISTIC_WARN =
        new String[] { "Unsupported function property was ignored (DETERMINISTIC)" };
    private static final String[] MONOTONIC_WARN =
        new String[] { "Unsupported function property was ignored (MONOTONIC)" };
    private static final String[] BOTH_WARNS = ArrayUtils.addAll(DETERMINISTIC_WARN, MONOTONIC_WARN);

    @Parameterized.Parameters(name = "functionProperties = {0}, warnings = {1}")
    public static Set<Object[]> functionProperties()
    {
        return ImmutableSet.of(
            new Object[]{ "detERMINistic", DETERMINISTIC_WARN },
            new Object[]{ "DETERMINISTIC", DETERMINISTIC_WARN },
            new Object[]{ "MONOTONIC", MONOTONIC_WARN },
            new Object[]{ "MONOTONIC ON num", MONOTONIC_WARN },
            new Object[]{ "DETERMINISTIC MONOTONIC", BOTH_WARNS },
            new Object[]{ "DETERMINISTIC MONOTONIC ON num", BOTH_WARNS }
        );
    }

    @Parameterized.Parameter(0)
    public String functionProperty;
    @Parameterized.Parameter(1)
    public String[] warnings;

    @BeforeClass
    public static void setup() throws Exception
    {
        SchemaLoader.createKeyspace("ks", KeyspaceParams.simple(1));
    }

    @Test
    public void ignoreUnsupportedKeywordsInFunctionCreationCommand() throws Throwable
    {
        String functionName = createFunctionName("ks");

        // should not throw
        ResultSet rows = executeNet(String.format("CREATE OR REPLACE FUNCTION %s (" +
                                                  "  column text, num int ) " +
                                                  "  RETURNS NULL ON NULL INPUT " +
                                                  "  RETURNS text " +
                                                  "  %s " +
                                                  "  LANGUAGE javascript AS " +
                                                  "    $$" +
                                                  "      column.substring(0, num)" +
                                                  "    $$;", functionName, functionProperty));

        assertTrue(rows.wasApplied());

        List<String> executionWarns = rows.getAllExecutionInfo().get(0).getWarnings();
        assertThat(executionWarns, containsInAnyOrder(warnings));

        assertNoUnsupportedProperties(functionName);
    }

    private void assertNoUnsupportedProperties(String functionName) throws Throwable
    {
        ResultSet result = executeNet("DESCRIBE FUNCTION " + functionName);

        String createStatement = result.one().getString("create_statement");
        assertThat(createStatement, not(containsStringIgnoringCase("DETERMINISTIC")));
        assertThat(createStatement, not(containsStringIgnoringCase("MONOTONIC")));
    }
}