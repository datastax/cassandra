/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
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

package org.apache.cassandra.db.guardrails;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.InvalidRequestException;

import static org.junit.Assert.assertEquals;

public class GuardrailSASIIndexesPerTableTest extends GuardrailTester
{
    private int defaultSASIPerTableFailureThreshold;

    @Before
    public void before()
    {
        defaultSASIPerTableFailureThreshold = DatabaseDescriptor.getGuardrailsConfig().getSasiIndexesPerTableFailThreshold();
    }

    @After
    public void after()
    {
        DatabaseDescriptor.getGuardrailsConfig().setSasiIndexesPerTableThreshold(-1, defaultSASIPerTableFailureThreshold);
    }

    @Test
    public void testCreateIndex() throws Throwable
    {
        createTable("CREATE TABLE %s (k int primary key, v1 int, v2 int)");

        DatabaseDescriptor.setSASIIndexesEnabled(false);
        assertCreationDisabled("v1");
        assertNumIndexes(0);

        DatabaseDescriptor.setSASIIndexesEnabled(true);
        DatabaseDescriptor.getGuardrailsConfig().setSasiIndexesPerTableThreshold(-1, 1);
        createIndex(getCreateIndexStatement("v1"));
        assertNumIndexes(1);
        assertCreationFailed("v2");
        assertNumIndexes(1);
    }

    @Test
    public void testExcludedUsers() throws Throwable
    {
        createTable("CREATE TABLE %s (k int primary key, v1 int, v2 int)");

        DatabaseDescriptor.getGuardrailsConfig().setSasiIndexesPerTableThreshold(-1, 1);
        testExcludedUsers(() -> getCreateIndexStatement("excluded_1", "v1"),
                          () -> getCreateIndexStatement("excluded_2", "v2"),
                          () -> "DROP INDEX excluded_1",
                          () -> "DROP INDEX excluded_2");
    }

    private void assertNumIndexes(int count)
    {
        assertEquals(count, getCurrentColumnFamilyStore().indexManager.listIndexes().size());
    }

    private void assertCreationFailed(String column) throws Throwable
    {
        String expectedMessage = String.format("aborting the creation of secondary index on table %s", currentTable());
        assertFails(getCreateIndexStatement(column), expectedMessage);
    }

    private void assertCreationDisabled(String column) throws Throwable
    {
        String expectedMessage = String.format("failed to create SASI index on table %s", currentTable());
        assertThrows(() -> execute(getCreateIndexStatement(column)), InvalidRequestException.class, "SASI indexes are disabled. Enable in cassandra.yaml to use.");
    }

    private String getCreateIndexStatement(String column)
    {
        return String.format("CREATE CUSTOM INDEX ON %%s (%s) USING 'org.apache.cassandra.index.sasi.SASIIndex'", column);
    }

    private String getCreateIndexStatement(String indexName, String column)
    {
        return String.format("CREATE CUSTOM INDEX %s ON %%s (%s) USING 'org.apache.cassandra.index.sasi.SASIIndex'", indexName, column);
    }
}