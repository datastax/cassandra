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
package org.apache.cassandra.index.sai.functional;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.index.sai.StorageAttachedIndexGroup;
import org.apache.cassandra.index.sai.disk.io.IndexComponents;
import org.apache.cassandra.index.sai.metrics.AbstractMetricsTest;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.schema.Schema;

import static org.apache.cassandra.inject.InvokePointBuilder.newInvokePoint;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Abstract class for building node lifecycle functional tests
 * The idea here is that this class includes only building blocks for tests
 * and does not include any junit specific code (!)
 */
//TODO It would be better to rename all these abstract tests so we don't have to ignore them
@Ignore
public abstract class AbstractNodeLifecycleTest extends AbstractMetricsTest
{
    static final Injections.Counter perSSTableValidationCounter =
            Injections.newCounter("PerSSTableValidationCounter")
                      .add(newInvokePoint().onClass(IndexComponents.class).onMethod("validatePerSSTableComponents"))
                      .build();

    static final Injections.Counter perColumnValidationCounter =
            Injections.newCounter("PerColumnValidationCounter")
                      .add(newInvokePoint().onClass(IndexComponents.class).onMethod("validatePerColumnComponents", "boolean"))
                      .build();

    static final String CREATE_TABLE_TEMPLATE = "CREATE TABLE %s (id text PRIMARY KEY, v1 int, v2 text) " +
                                                        "WITH compaction = {'class' : 'SizeTieredCompactionStrategy', 'enabled' : false }";

    @Before
    public void initializeTest() throws Throwable
    {
        requireNetwork();

        startJMXServer();

        createMBeanServerConnection();

        resetValidationCount();

    }

    void createSingleRowIndex() throws Throwable
    {
        createTable(CREATE_TABLE_TEMPLATE);
        verifyIndexFiles(0, 0);

        execute("INSERT INTO %s (id, v1, v2) VALUES ('0', 0, '0')");
        flush();

        createIndex(String.format(CREATE_INDEX_TEMPLATE, "v1"));
        waitForIndexQueryable();
        verifyIndexFiles(1, 0);
        assertNumRows(1, "SELECT * FROM %%s WHERE v1 >= 0");
        assertValidationCount(0, 0);
    }

    void assertAllFileExists(List<String> filePaths) throws Exception
    {
        for (String path : filePaths)
        {
            File file = new File(path);
            assertTrue("Expect file exists, but it's removed: " + path, file.exists());
        }
    }

    void assertAllFileRemoved(List<String> filePaths) throws Exception
    {
        for (String path : filePaths)
        {
            File file = new File(path);
            System.err.println("## check="+path);
            assertFalse("Expect file being removed, but it still exists: " + path, file.exists());
        }
    }

    int getOpenIndexFiles(String table) throws Exception
    {
        return (int) getMetricValue(objectNameNoIndex("OpenIndexFiles", KEYSPACE, table, "IndexGroupMetrics"));
    }

    long getDiskUsage(String table) throws Exception
    {
        return (long) getMetricValue(objectNameNoIndex("DiskUsedBytes", KEYSPACE, table, "IndexGroupMetrics"));
    }

    void verifyIndexComponentsIncludedInSSTable(String table) throws Exception
    {
        verifySSTableComponents(table, true);
    }

    void verifyIndexComponentsNotIncludedInSSTable(String table) throws Exception
    {
        verifySSTableComponents(table, false);
    }

    void verifySSTableComponents(String table, boolean indexComponentsExist) throws Exception
    {
        ColumnFamilyStore cfs = Objects.requireNonNull(Schema.instance.getKeyspaceInstance(KEYSPACE)).getColumnFamilyStore(table);
        for (SSTable sstable : cfs.getLiveSSTables())
        {
            Set<Component> components = sstable.components;
            StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(cfs);
            Set<Component> ndiComponents = group == null ? Collections.emptySet() : group.getComponents();

            Set<Component> diff = Sets.difference(ndiComponents, components);
            if (indexComponentsExist)
                assertTrue("Expect all index components are tracked by SSTable, but " + diff + " are not included.",
                           !ndiComponents.isEmpty() && diff.isEmpty());
            else
                assertFalse("Expect no index components, but got " + components, components.toString().contains(IndexComponents.TYPE_PREFIX));

            Set<Component> tocContents = SSTable.readTOC(sstable.descriptor);
            assertEquals(components, tocContents);
        }
    }

    long totalDiskSpaceUsed(String table)
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(table);
        return cfs.metric.totalDiskSpaceUsed.getCount();
    }

    long indexDiskSpaceUse(String table)
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(table);
        return Objects.requireNonNull(StorageAttachedIndexGroup.getIndexGroup(cfs)).totalDiskUsage();
    }

    void assertValidationCount(int perSSTable, int perColumn)
    {
        Assert.assertEquals(perSSTable, perSSTableValidationCounter.get());
        Assert.assertEquals(perColumn, perColumnValidationCounter.get());
    }

    void resetValidationCount()
    {
        perSSTableValidationCounter.reset();
        perColumnValidationCounter.reset();
    }
}
