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
import org.junit.Ignore;
import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SAITester;

import static org.apache.cassandra.config.CassandraRelevantProperties.IMMUTABLE_SAI_COMPONENTS;
import static org.junit.Assert.assertEquals;

@Ignore
public abstract class AbstractRebuildAndImmutableComponentsTester extends SAITester
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

    @Test
    public void rebuildCreateNewGenerationFiles() throws Throwable
    {
        // Setup: create index, insert data, flush, and make sure everything is correct.
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");
        String name = createIndex("CREATE CUSTOM INDEX test_index ON %s(val) USING 'StorageAttachedIndex'");

        IndexContext context = createIndexContext(name, UTF8Type.instance);

        execute("INSERT INTO %s (id, val) VALUES ('0', 'testValue')");
        execute("INSERT INTO %s (id, val) VALUES ('1', 'otherValue')");
        execute("INSERT INTO %s (id, val) VALUES ('2', 'testValue')");
        execute("INSERT INTO %s (id, val) VALUES ('3', 'otherValue')");

        flush();

        assertEquals(2, execute("SELECT id FROM %s WHERE val = 'testValue'").size());

        // Rebuild the index
        rebuildIndexes(name);

        assertEquals(2, execute("SELECT id FROM %s WHERE val = 'testValue'").size());

        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(currentTable());
        validateSSTables(cfs, context);
    }

    protected abstract void validateSSTables(ColumnFamilyStore cfs, IndexContext context) throws Exception;
}
