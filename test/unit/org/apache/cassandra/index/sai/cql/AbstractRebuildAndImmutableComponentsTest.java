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

import org.junit.After;
import org.junit.Before;

import org.apache.cassandra.index.sai.SAITester;

import static org.apache.cassandra.config.CassandraRelevantProperties.IMMUTABLE_SAI_COMPONENTS;
import static org.junit.Assert.assertEquals;

/**
 * Common code for tests to validate the behavior of the {@link org.apache.cassandra.config.CassandraRelevantProperties#IMMUTABLE_SAI_COMPONENTS} property.
 * <p>
 * Implementation note: this was meant to contain the common `@Test` to both implementation of this abstract class,
 * with just the final validation of sstables being abstracted, but when doing that, something in CI (I suspect `ant`)
 * tries to run the test directly on the abstract class (which fails with an `InstantiationException`, obviously, but
 * ends up as a test failure). If there is a magic flag to make it work, I don't know it and don't care to looking for
 * it. So the concrete tests are in the subclasses, even if it's not quite DRY.
 */
public abstract class AbstractRebuildAndImmutableComponentsTest extends SAITester
{
     private Boolean defaultImmutableSetting;

    protected abstract boolean useImmutableComponents();

    @Before
    public void setup() throws Throwable
    {
        defaultImmutableSetting = IMMUTABLE_SAI_COMPONENTS.getBoolean();
        IMMUTABLE_SAI_COMPONENTS.setBoolean(useImmutableComponents());
        requireNetwork();
    }

    @After
    public void tearDown()
    {
        if (defaultImmutableSetting != null)
            IMMUTABLE_SAI_COMPONENTS.setBoolean(defaultImmutableSetting);
    }

    protected String createTableWithIndexAndRebuild() throws Throwable
    {
        // Setup: create index, insert data, flush, and make sure everything is correct.
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");
        String name = createIndex("CREATE CUSTOM INDEX test_index ON %s(val) USING 'StorageAttachedIndex'");


        execute("INSERT INTO %s (id, val) VALUES ('0', 'testValue')");
        execute("INSERT INTO %s (id, val) VALUES ('1', 'otherValue')");
        execute("INSERT INTO %s (id, val) VALUES ('2', 'testValue')");
        execute("INSERT INTO %s (id, val) VALUES ('3', 'otherValue')");

        flush();

        assertEquals(2, execute("SELECT id FROM %s WHERE val = 'testValue'").size());

        // Rebuild the index
        rebuildIndexes(name);

        assertEquals(2, execute("SELECT id FROM %s WHERE val = 'testValue'").size());

        return name;
    }
}
