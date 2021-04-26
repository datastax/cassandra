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

import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.ResultSet;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.schema.KeyspaceParams;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.containsStringIgnoringCase;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertTrue;

public class CreateAggregateStatementTest extends CQLTester
{
    @BeforeClass
    public static void setup() throws Exception
    {
        SchemaLoader.createKeyspace("ks", KeyspaceParams.simple(1));
    }

    @Test
    public void ignoreUnsupportedKeywordsInAggregateCreationCommand() throws Throwable
    {
        String aggregateName = createAggregateName("ks");
        String functionName = createFunctionName("ks");

        executeNet(String.format("CREATE OR REPLACE FUNCTION %s(state double, val double) " +
                                 "  RETURNS NULL ON NULL INPUT " +
                                 "  RETURNS double " +
                                 "  LANGUAGE javascript " +
                                 "  AS '\"string\";';", functionName));

        // should not throw
        ResultSet rows = executeNet(String.format("CREATE OR REPLACE AGGREGATE %s(double) " +
                                    "SFUNC %s " +
                                    "STYPE double " +
                                    "INITCOND 0 " +
                                    "DETERMINISTIC", aggregateName, shortFunctionName(functionName)));

        assertTrue(rows.wasApplied());

        String warning = rows.getAllExecutionInfo().get(0).getWarnings().get(0);
        assertThat(warning, containsString("Unsupported aggregate property was ignored (DETERMINISTIC)"));

        assertNoUnsupportedProperties(aggregateName);
    }

    private void assertNoUnsupportedProperties(String aggregateName) throws Throwable
    {
        ResultSet result = executeNet("DESCRIBE AGGREGATE " + aggregateName);

        String createStatement = result.one().getString("create_statement");
        assertThat(createStatement, not(containsStringIgnoringCase("DETERMINISTIC")));
    }
}