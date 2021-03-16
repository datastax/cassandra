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

import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.function.LongSupplier;

import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.SimpleStatement;
import com.sun.management.UnixOperatingSystemMXBean;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.SegmentBuilder;
import org.apache.cassandra.index.sai.disk.v1.BKDReader;
import org.apache.cassandra.index.sai.disk.v1.TermsReader;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.inject.InvokePointBuilder;

import static org.junit.Assert.assertEquals;

public class IndexSegmentationTest extends SAITester
{
    private static final String CREATE_TABLE_TEMPLATE = "CREATE TABLE %s (id1 TEXT PRIMARY KEY, v1 INT, v2 TEXT)" +
                                                        "  WITH compaction = {'class' : 'SizeTieredCompactionStrategy', 'enabled' : false }";

    private static final int PAGE_SIZE = 5000;

    @BeforeClass
    public static void setupCQLTester()
    {
        Config conf = DatabaseDescriptor.loadConfig();
        conf.num_tokens = 16;
        DatabaseDescriptor.daemonInitialization(() -> conf);
    }

    @Before
    public void setup()
    {
        requireNetwork();
    }

    @After
    public void resetCountersAndInjections()
    {
        SegmentBuilder.updateLastValidSegmentRowId(-1); // reset
        Injections.deleteAll();
    }

    @Test
    public void testNumOpenFiles() throws Throwable
    {
        // only work for unix system
        Assume.assumeTrue(ManagementFactory.getOperatingSystemMXBean() instanceof UnixOperatingSystemMXBean);

        createTable(CREATE_TABLE_TEMPLATE);
        createIndex(String.format(CREATE_INDEX_TEMPLATE, "v1"));
        createIndex(String.format(CREATE_INDEX_TEMPLATE, "v2"));
        waitForIndexQueryable();

        int sstable = 10;
        int num = 100;
        for (int s = 0; s < sstable; s++)
        {
            for (int i = 0; i < num; i++)
            {
                execute("INSERT INTO %s (id1, v1, v2) VALUES (?, 0, '0')", Integer.toString(i));
            }
            flush();
        }

        // remove all index files, get open files without index
        releaseIndexFiles();

        Assert.assertEquals(0, getOpenIndexFiles());
        assertRowCount(0, "SELECT id1 FROM %s WHERE v1>=0");
        assertRowCount(0, "SELECT id1 FROM %s WHERE v2='0'");

        long openFilesWithNoIndex = getOpenVmFiles();

        // get open files with 1 index segment per sstable
        upgradeSSTables();
        waitForAssert(() -> verifyIndexFiles(sstable, sstable));

        long indexDescriptor = getOpenIndexFiles();
        long openFilesWithOneSegment = getOpenVmFiles();
        long increased = openFilesWithOneSegment - openFilesWithNoIndex;

        // expect the increased open files are index open files
        assertEquals(increased, indexDescriptor, 0.05 * indexDescriptor);

        // rewrite 100 segment per sstable
        SegmentBuilder.updateLastValidSegmentRowId(1);

        upgradeSSTables();
        waitForAssert(() -> verifyIndexFiles(sstable, sstable));

        // verify open files after increasing segment count
        long openFilesWithThousandSegment = getOpenVmFiles();
        assertEquals(openFilesWithThousandSegment, openFilesWithOneSegment, 0.05 * openFilesWithOneSegment);
    }

    @Test
    public void testWidePartition() throws Throwable
    {
        createTable("CREATE TABLE %s (id1 text, ck int, v1 int, v2 text, primary key(id1, ck))");

        String v1IndexName = createIndex(String.format(CREATE_INDEX_TEMPLATE, "v1"));
        String v2IndexName = createIndex(String.format(CREATE_INDEX_TEMPLATE, "v2"));
        waitForIndexQueryable();

        int num = 20000;
        for (int i = 0; i < num; i++)
        {
            execute("INSERT INTO %s (id1, ck, v1, v2) VALUES ('0', ?, 0, '0')", i);
        }
        flush();

        verifyIndexFiles(1, 1);
        int segments = getSegmentCount(v1IndexName);
        assertEquals("Expect exactly 1 segment created during flush", 1, segments);
        verifySegments(v2IndexName, segments);

        assertRowCount(num, "SELECT id1 FROM %s WHERE v1>=0");
        assertRowCount(num, "SELECT id1 FROM %s WHERE v2='0'");

        int pages = num / PAGE_SIZE;

        Injections.Counter indexSearchCounter = Injections.newCounter("IndexSearchCounter")
                                                          .add(InvokePointBuilder.newInvokePoint().onClass(BKDReader.class).onMethod("intersect"))
                                                          .add(InvokePointBuilder.newInvokePoint().onClass(TermsReader.class).onMethod("exactMatch"))
                                                          .build();

        Injections.inject(indexSearchCounter);
        LongSupplier getIndexFetchCount = indexSearchCounter::get;

        for (int lastValidSegmentRowId : Arrays.asList(999, 10001, 19999, 21000))
        {
            SegmentBuilder.updateLastValidSegmentRowId(lastValidSegmentRowId);

            // compaction to rewrite segments
            upgradeSSTables();

            // Post-build there should only ever be 1 segment
            verifySegments(v1IndexName, 1);
            verifySegments(v2IndexName, 1);
            waitForAssert(() -> verifyIndexFiles(1, 1));

            // verify small limit query touches the segment
            long count = getIndexFetchCount.getAsLong();
            assertRowCount(1, "SELECT id1 FROM %s WHERE v1>=0 LIMIT 1");
            assertEquals(count + 1, getIndexFetchCount.getAsLong());

            count = getIndexFetchCount.getAsLong();
            assertRowCount(1, "SELECT id1 FROM %s WHERE v2='0' LIMIT 1");
            assertEquals(count + 1, getIndexFetchCount.getAsLong());

            // verify no-limit query touches the segment for every page
            count = getIndexFetchCount.getAsLong();
            assertRowCount(num, "SELECT id1 FROM %s WHERE v1>=0");
            assertEquals(count + 1 + pages, getIndexFetchCount.getAsLong()); // last page has no result

            count = getIndexFetchCount.getAsLong();
            assertRowCount(num, "SELECT id1 FROM %s WHERE v2='0'");
            assertEquals(count + 1 + pages, getIndexFetchCount.getAsLong()); // last page has no result
        }
    }

    @Test
    public void testSkinnyPartition() throws Throwable
    {
        createTable(CREATE_TABLE_TEMPLATE);

        String v1IndexName = createIndex(String.format(CREATE_INDEX_TEMPLATE, "v1"));
        String v2IndexName = createIndex(String.format(CREATE_INDEX_TEMPLATE, "v2"));
        waitForIndexQueryable();

        int num = 1000;
        for (int i = 0; i < num; i++)
        {
            execute("INSERT INTO %s (id1, v1, v2) VALUES (?, 0, '0')", Integer.toString(i));
        }
        flush();

        verifyIndexFiles(1, 1);
        int segments = getSegmentCount(v1IndexName);
        assertEquals("Expect exactly 1 segment created during flush", 1, segments);
        verifySegments(v2IndexName, segments);

        assertRowCount(num, "SELECT id1 FROM %s WHERE v1>=0");
        assertRowCount(num, "SELECT id1 FROM %s WHERE v2='0'");

        for (int i = 0; i < num; i++)
        {
            assertRowCount(1, "SELECT id1 FROM %s WHERE v1>=0 AND id1='" + i + "'");
            assertRowCount(1, "SELECT id1 FROM %s WHERE v2='0' AND id1='" + i + "'");
        }

        Injections.Counter indexSearchCounter = Injections.newCounter("IndexSearchCounterWide")
                                                          .add(InvokePointBuilder.newInvokePoint().onClass(BKDReader.class).onMethod("intersect"))
                                                          .add(InvokePointBuilder.newInvokePoint().onClass(TermsReader.class).onMethod("exactMatch"))
                                                          .build();

        Injections.inject(indexSearchCounter);
        LongSupplier getIndexFetchCount = indexSearchCounter::get;

        for (int lastValidSegmentRowId : Arrays.asList(0, 1, 3, 9, 25, 30, 50, 60, 99, 101, 499, 999, 1001))
        {
            SegmentBuilder.updateLastValidSegmentRowId(lastValidSegmentRowId);

            // compaction to rewrite segments
            upgradeSSTables();

            verifySegmentSplit(v1IndexName, v2IndexName, num, getIndexFetchCount);
        }
    }

    private void verifySegmentSplit(String v1IndexName, String v2IndexName, int num, LongSupplier getIndexFetchCount) throws Throwable
    {
        verifySegments(v1IndexName, 1);
        verifySegments(v2IndexName, 1);
        waitForAssert(() -> verifyIndexFiles(1, 1));

        // verify small limit query touches all segments
        long count = getIndexFetchCount.getAsLong();
        assertRowCount(1, "SELECT id1 FROM %s WHERE v1>=0 LIMIT 1");
        assertEquals(count + 1, getIndexFetchCount.getAsLong());

        count = getIndexFetchCount.getAsLong();
        assertRowCount(1, "SELECT id1 FROM %s WHERE v2='0' LIMIT 1");
        assertEquals(count + 1, getIndexFetchCount.getAsLong());

        // verify no limit query touches all segments
        count = getIndexFetchCount.getAsLong();
        assertRowCount(num, "SELECT id1 FROM %s WHERE v1>=0");
        assertEquals(count + 1, getIndexFetchCount.getAsLong());

        count = getIndexFetchCount.getAsLong();
        assertRowCount(num, "SELECT id1 FROM %s WHERE v2='0'");
        assertEquals(count + 1, getIndexFetchCount.getAsLong());

        // verify partition restricted query only touches one segment
        for (int i = 0; i < num; i++)
        {
            count = getIndexFetchCount.getAsLong();
            assertRowCount(1, "SELECT id1 FROM %s WHERE v1>=0 AND id1='" + i + "'");
            assertEquals(count + 1, getIndexFetchCount.getAsLong());

            count = getIndexFetchCount.getAsLong();
            assertRowCount(1, "SELECT id1 FROM %s WHERE v2='0' AND id1='" + i + "'");
            assertEquals(count + 1, getIndexFetchCount.getAsLong());
        }
    }

    private void assertRowCount(int count, String query) throws Throwable
    {
        assertEquals(count,
                     sessionNet(getDefaultVersion())
                             .execute(new SimpleStatement(formatQuery(query))
                                              .setFetchSize(PAGE_SIZE)).all().size());
    }
}
