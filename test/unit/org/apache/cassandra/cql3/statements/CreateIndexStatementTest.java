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
import com.datastax.driver.core.exceptions.InvalidConfigurationInQueryException;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.schema.CreateIndexStatement;
import org.apache.cassandra.schema.KeyspaceParams;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


public class CreateIndexStatementTest extends CQLTester
{

    @BeforeClass
    public static void setup() throws Exception
    {
        SchemaLoader.createKeyspace("ks", KeyspaceParams.simple(1));
        QueryProcessor.executeOnceInternal("CREATE TABLE ks.tbl (k int, c int, v int, primary key (k, c))");
    }

    private void assertNoIndex(String indexName, String indexClass) throws Throwable
    {
        try
        {
            executeNet("DESCRIBE INDEX ks." + indexName);
            fail(String.format("Expected InvalidQueryException caused by a missing index [%s]", indexClass));
        }
        catch (InvalidQueryException e)
        {
            assertTrue(String.format("Index '%s' found", indexClass),
                       e.getMessage().contains(indexName + "' not found"));
        }
    }

    @Test
    public void dseIndexCreationShouldBeIgonerWithWarning() throws Throwable
    {
        for (String indexClass : CreateIndexStatement.DSE_INDEXES)
        {
            // should not throw
            ResultSet rows = executeNet(String.format("CREATE CUSTOM INDEX index_name ON ks.tbl (v) USING '%s'", indexClass));

            assertTrue(String.format("Custom DSE index '%s' creation not applied", indexClass),
                       rows.wasApplied()); // the command is ignored

            String warning = rows.getAllExecutionInfo().get(0).getWarnings().get(0);
            assertTrue(String.format("Custom DSE index '%s' creation should cause a warning", indexClass),
                       warning.contains("DSE custom index"));

            assertNoIndex("index_name", indexClass);
        }
    }

    @Test(expected=InvalidConfigurationInQueryException.class)
    public void unknownCustomIndexCreationFails() throws Throwable
    {
        String indexClass = "org.apache.cassandra.index.sasi.SASIIndex";
        executeNet(String.format("CREATE CUSTOM INDEX index_name ON ks.tbl (v) USING '%s'", indexClass));
    }

    @Test
    public void unknownCustomIndexCreationIgnored() throws Throwable
    {
        Boolean previous = CassandraRelevantProperties.INDEX_UNKNOWN_IGNORE.setBoolean(true);
        try
        {
            String indexClass = "org.apache.cassandra.index.sasi.SASIIndex";
            // should not throw
            ResultSet rows = executeNet(String.format("CREATE CUSTOM INDEX index_name ON ks.tbl (v) USING '%s'", indexClass));

            assertTrue(rows.wasApplied());

            String warning = rows.getAllExecutionInfo().get(0).getWarnings().get(0);
            assertTrue(String.format("Custom index '%s' creation shoud cause warning", indexClass),
                       warning.contains(String.format("Index index_name was not created. Unknown custom index (%s)", indexClass)));

            assertNoIndex("index_name", indexClass);
        }
        finally
        {
            if (null != previous)
                CassandraRelevantProperties.INDEX_UNKNOWN_IGNORE.setBoolean((boolean)previous);
            else
                CassandraRelevantProperties.INDEX_UNKNOWN_IGNORE.reset();
        }
    }
}
