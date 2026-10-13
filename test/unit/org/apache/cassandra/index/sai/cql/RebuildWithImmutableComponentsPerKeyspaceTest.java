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

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.StorageAttachedIndexGroup;
import org.apache.cassandra.index.sai.disk.format.ImmutableSAIComponentsConfigProvider;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests that immutable SAI components can be enabled for some keyspaces only, through
 * {@link ImmutableSAIComponentsConfigProvider}.
 */
public class RebuildWithImmutableComponentsPerKeyspaceTest extends SAITester
{
    /**
     * Enables immutable components only for the keyspaces in {@link #KEYSPACES}.
     */
    public static class PerKeyspaceConfigProvider implements ImmutableSAIComponentsConfigProvider
    {
        static final Set<String> KEYSPACES = ConcurrentHashMap.newKeySet();

        @Override
        public boolean useImmutableComponentsForKeyspace(String keyspace)
        {
            return KEYSPACES.contains(keyspace);
        }

        @Override
        public boolean useImmutableComponentsForTenant(String tenant)
        {
            throw new UnsupportedOperationException();
        }
    }

    @BeforeClass
    public static void setUpConfigProvider()
    {
        // The provider is loaded once per JVM (unit tests are forked per test class), so this must be set before
        // ImmutableSAIComponentsConfigProvider is initialized by the first SAI index build.
        CassandraRelevantProperties.IMMUTABLE_SAI_COMPONENTS_CONFIG_PROVIDER_CLASS.setString(PerKeyspaceConfigProvider.class.getName());
    }

    @AfterClass
    public static void tearDownConfigProvider()
    {
        CassandraRelevantProperties.IMMUTABLE_SAI_COMPONENTS_CONFIG_PROVIDER_CLASS.clearValue();
    }

    @Before
    public void setup() throws Throwable
    {
        requireNetwork();
    }

    @After
    public void tearDown()
    {
        PerKeyspaceConfigProvider.KEYSPACES.clear();
    }

    @Test
    public void testRebuildWithImmutableComponentsOnlyForSelectedKeyspace() throws Throwable
    {
        String immutableKeyspace = createKeyspace("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
        PerKeyspaceConfigProvider.KEYSPACES.add(immutableKeyspace);
        assertTrue(ImmutableSAIComponentsConfigProvider.INSTANCE instanceof PerKeyspaceConfigProvider);

        String mutableTable = createTableWithIndexAndData(KEYSPACE);
        String immutableTable = createTableWithIndexAndData(immutableKeyspace);

        // Initial build of a flushed sstable always uses generation 0
        assertGeneration(KEYSPACE, mutableTable, 0);
        assertGeneration(immutableKeyspace, immutableTable, 0);

        rebuildTableIndexes(KEYSPACE, mutableTable);
        rebuildTableIndexes(immutableKeyspace, immutableTable);

        // Only the selected keyspace creates new generation files on rebuild
        assertGeneration(KEYSPACE, mutableTable, 0);
        assertGeneration(immutableKeyspace, immutableTable, 1);

        assertEquals(2, execute(String.format("SELECT id FROM %s.%s WHERE val = 'testValue'", KEYSPACE, mutableTable)).size());
        assertEquals(2, execute(String.format("SELECT id FROM %s.%s WHERE val = 'testValue'", immutableKeyspace, immutableTable)).size());
    }

    private String createTableWithIndexAndData(String keyspace) throws Throwable
    {
        String table = createTable(keyspace, "CREATE TABLE %s (id text PRIMARY KEY, val text)");
        createIndex(keyspace, "CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");

        execute(String.format("INSERT INTO %s.%s (id, val) VALUES ('0', 'testValue')", keyspace, table));
        execute(String.format("INSERT INTO %s.%s (id, val) VALUES ('1', 'otherValue')", keyspace, table));
        execute(String.format("INSERT INTO %s.%s (id, val) VALUES ('2', 'testValue')", keyspace, table));
        execute(String.format("INSERT INTO %s.%s (id, val) VALUES ('3', 'otherValue')", keyspace, table));
        flush(keyspace, table);
        return table;
    }

    private static void assertGeneration(String keyspace, String table, int expectedGeneration)
    {
        ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
        StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(cfs);
        assertNotNull(group);
        assertFalse(cfs.getLiveSSTables().isEmpty());

        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            IndexDescriptor descriptor = group.descriptorFor(sstable);
            assertEquals(expectedGeneration, descriptor.perSSTableComponents().buildId().generation());
            for (StorageAttachedIndex index : group.getIndexes())
                assertEquals(expectedGeneration, descriptor.perIndexComponents(index.getIndexContext()).buildId().generation());
        }
    }
}
