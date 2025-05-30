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
package org.apache.cassandra.index.sai.metrics;

import javax.management.InstanceNotFoundException;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.disk.v1.V1OnDiskFormat;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;


public class IndexGroupMetricsTest extends AbstractMetricsTest
{
    @Before
    public void setup() throws Exception
    {
        requireNetwork();

        startJMXServer();

        createMBeanServerConnection();
    }

    @Test
    public void verifyIndexGroupMetrics() throws Throwable
    {
        // create first index
        createTable(CREATE_TABLE_TEMPLATE);
        String v1IndexName = createIndex(String.format(CREATE_INDEX_TEMPLATE, "v1"));
        IndexContext v1IndexContext = createIndexContext(v1IndexName, Int32Type.instance);

        // no open files
        assertEquals(0, getOpenIndexFiles());
        assertEquals(0, getDiskUsage());

        int sstables = 10;
        for (int i = 0; i < sstables; i++)
        {
            execute("INSERT INTO %s (id1, v1, v2) VALUES ('0', 0, '0')");
            flush();
        }

        // with 10 sstable
        int indexopenFileCountWithOnlyNumeric = getOpenIndexFiles();
        assertEquals(sstables * (Version.current().onDiskFormat().openFilesPerSSTable() +
                                 Version.current().onDiskFormat().openFilesPerIndex(v1IndexContext)),
                     indexopenFileCountWithOnlyNumeric);

        long diskUsageWithOnlyNumeric = getDiskUsage();
        assertNotEquals(0, diskUsageWithOnlyNumeric);

        // create second index
        String v2IndexName = createIndex(String.format(CREATE_INDEX_TEMPLATE, "v2"));
        IndexContext v2IndexContext = createIndexContext(v2IndexName, UTF8Type.instance);

        // same number of sstables, but more string index files.
        int stringIndexOpenFileCount = sstables * V1OnDiskFormat.instance.openFilesPerIndex(v2IndexContext);
        assertEquals(indexopenFileCountWithOnlyNumeric, getOpenIndexFiles() - stringIndexOpenFileCount);

        // Index Group disk usage doesn't change with more indexes
        long diskUsageWithBothIndexes = getDiskUsage();
        assertEquals(diskUsageWithBothIndexes, diskUsageWithOnlyNumeric);

        // compaction should reduce open files
        compact();

        long perSSTableFileDiskUsage = getDiskUsage();
        assertEquals(Version.current().onDiskFormat().openFilesPerSSTable() +
                     Version.current().onDiskFormat().openFilesPerIndex(v2IndexContext) +
                     Version.current().onDiskFormat().openFilesPerIndex(v1IndexContext),
                     getOpenIndexFiles());

        // drop string index, reduce open string index files, per-sstable file disk usage remains the same
        dropIndex("DROP INDEX %s." + v2IndexName);
        assertEquals(Version.current().onDiskFormat().openFilesPerSSTable() +
                     Version.current().onDiskFormat().openFilesPerIndex(v1IndexContext),
                     getOpenIndexFiles());
        assertEquals(perSSTableFileDiskUsage, getDiskUsage());

        // drop last index, no open index files
        dropIndex("DROP INDEX %s." + v1IndexName);
        assertThatThrownBy(this::getOpenIndexFiles).hasRootCauseInstanceOf(InstanceNotFoundException.class);
        assertThatThrownBy(this::getDiskUsage).hasRootCauseInstanceOf(InstanceNotFoundException.class);
    }

    protected int getOpenIndexFiles()
    {
        return (int) getMetricValue(objectNameNoIndex("OpenIndexFiles", KEYSPACE, currentTable(), "IndexGroupMetrics"));
    }

    protected long getDiskUsage()
    {
        return (long) getMetricValue(objectNameNoIndex("DiskUsedBytes", KEYSPACE, currentTable(), "IndexGroupMetrics"));
    }
}
