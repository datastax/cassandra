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

package org.apache.cassandra.tools.nodetool;

import java.io.IOException;
import java.util.Collections;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.IScrubber;
import org.apache.cassandra.io.sstable.IScrubber.OverwriteTTLMode;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.ToolRunner;
import org.apache.cassandra.utils.OutputHandler;
import org.assertj.core.api.Assertions;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Tests the {@code nodetool scrub --overwrite-ttl NO_TTL [--bump-ttl-timestamp]} path from the command line down to
 * the scrubber: {@link Scrub}, {@link NodeProbe}, {@code StorageServiceMBean}, {@link StorageService},
 * {@link ColumnFamilyStore} and {@link CompactionManager}.
 */
public class ScrubNoTTLTest extends CQLTester
{
    private static final long TIMESTAMP = 1000;
    private static final int TTL = 3600;
    private static final int SUCCESS = CompactionManager.AllSSTableOpStatus.SUCCESSFUL.statusCode;
    private static final String BUMP_REQUIRES_NO_TTL = "'--bump-ttl-timestamp' can only be used with '--overwrite-ttl NO_TTL'";

    private static NodeProbe probe;

    @BeforeClass
    public static void setup() throws Exception
    {
        requireNetwork();
        startJMXServer();
        probe = new NodeProbe(jmxHost, jmxPort);
    }

    @AfterClass
    public static void teardown() throws IOException
    {
        probe.close();
    }

    /** Creates a table with a single row with a TTL and flushes it, so that there is an sstable to scrub. */
    private void createTableWithExpiringRow() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        execute("INSERT INTO %s (k, v) VALUES (0, 0) USING TTL " + TTL + " AND TIMESTAMP " + TIMESTAMP);
        flush();
    }

    private void assertTTLRemoved(long expectedTimestamp) throws Throwable
    {
        assertRows(execute("SELECT k, v, ttl(v), writetime(v) FROM %s"), row(0, 0, null, expectedTimestamp));
    }

    private void assertTTLKept() throws Throwable
    {
        int ttl = execute("SELECT ttl(v) FROM %s").one().getInt("ttl(v)");
        Assertions.assertThat(ttl).isGreaterThan(TTL - 600).isLessThanOrEqualTo(TTL);
        assertEquals(TIMESTAMP, execute("SELECT writetime(v) FROM %s").one().getLong("writetime(v)"));
    }

    // ---- nodetool command ------------------------------------------------------------------------------------

    /** nodetool reports command line validation errors on stdout and exits with a non-zero code. */
    private static void assertNodetoolFails(String expectedMessage, String... args)
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool(args);
        tool.asserts().failure();
        Assertions.assertThat(tool.getStdout()).contains(expectedMessage);
    }

    @Test
    public void testNodetoolNoTTLKeepsTimestampByDefault() throws Throwable
    {
        createTableWithExpiringRow();

        ToolRunner.invokeNodetool("scrub", "--no-snapshot", "--overwrite-ttl", "NO_TTL", keyspace(), currentTable())
                  .assertOnCleanExit();

        assertTTLRemoved(TIMESTAMP);
    }

    @Test
    public void testNodetoolNoTTLWithBumpTimestamp() throws Throwable
    {
        createTableWithExpiringRow();

        ToolRunner.invokeNodetool("scrub", "--no-snapshot", "-o", "NO_TTL", "--bump-ttl-timestamp", keyspace(), currentTable())
                  .assertOnCleanExit();

        assertTTLRemoved(TIMESTAMP + 1);
    }

    @Test
    public void testNodetoolBumpTimestampRequiresNoTTL() throws Throwable
    {
        createTableWithExpiringRow();

        assertNodetoolFails(BUMP_REQUIRES_NO_TTL, "scrub", "--no-snapshot", "--bump-ttl-timestamp", keyspace(), currentTable());

        assertNodetoolFails(BUMP_REQUIRES_NO_TTL, "scrub", "--no-snapshot", "-o", "NONE", "--bump-ttl-timestamp", keyspace(), currentTable());

        assertNodetoolFails(BUMP_REQUIRES_NO_TTL, "scrub", "--no-snapshot", "-r", "--bump-ttl-timestamp", keyspace(), currentTable());

        // nothing was scrubbed
        assertTTLKept();
    }

    @Test
    public void testNodetoolOverwriteTTLAndReinsertConflict() throws Throwable
    {
        createTableWithExpiringRow();

        assertNodetoolFails("Only one of '--reinsert-overflowed-ttl' or '--overwrite-ttl' is permitted.", "scrub", "--no-snapshot", "-r", "-o", "NO_TTL", keyspace(), currentTable());
    }

    @Test
    public void testNodetoolReinsertOverflowedTTLAndNoneDoNotStripTTL() throws Throwable
    {
        createTableWithExpiringRow();

        ToolRunner.invokeNodetool("scrub", "--no-snapshot", "-r", keyspace(), currentTable()).assertOnCleanExit();
        ToolRunner.invokeNodetool("scrub", "--no-snapshot", "-o", "NONE", keyspace(), currentTable()).assertOnCleanExit();

        assertTTLKept();
    }

    @Test
    public void testNodetoolHelpDocumentsBumpTimestamp()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("help", "scrub");
        tool.assertOnCleanExit();
        Assertions.assertThat(tool.getStdout()).contains("--bump-ttl-timestamp");
    }

    // ---- NodeProbe (JMX client) -> StorageService ----------------------------------------------------------------

    @Test
    public void testProbeNoTTLWithoutBump() throws Throwable
    {
        createTableWithExpiringRow();

        assertEquals(SUCCESS, probe.scrub(true, false, true, OverwriteTTLMode.NO_TTL, 1, keyspace(), currentTable()));

        assertTTLRemoved(TIMESTAMP);
    }

    @Test
    public void testProbeNoTTLWithBump() throws Throwable
    {
        createTableWithExpiringRow();

        assertEquals(SUCCESS, probe.scrub(true, false, true, OverwriteTTLMode.NO_TTL, true, 1, keyspace(), currentTable()));

        assertTTLRemoved(TIMESTAMP + 1);
    }

    @Test
    public void testProbeNoTTLBumpDisabledExplicitly() throws Throwable
    {
        createTableWithExpiringRow();

        assertEquals(SUCCESS, probe.scrub(true, false, true, OverwriteTTLMode.NO_TTL, false, 1, keyspace(), currentTable()));

        assertTTLRemoved(TIMESTAMP);
    }

    @Test
    public void testProbeOtherModesGoThroughBooleanMethod() throws Throwable
    {
        createTableWithExpiringRow();

        assertEquals(SUCCESS, probe.scrub(true, false, true, OverwriteTTLMode.NONE, 1, keyspace(), currentTable()));
        assertEquals(SUCCESS, probe.scrub(true, false, true, OverwriteTTLMode.REINSERT_OVERFLOWED_TTL, 1, keyspace(), currentTable()));
        assertEquals(SUCCESS, probe.scrub(true, false, true, OverwriteTTLMode.NONE, true, 1, keyspace(), currentTable()));
        assertEquals(SUCCESS, probe.scrub(true, false, true, true, 1, keyspace(), currentTable()));

        // none of these modes removes the TTL
        assertTTLKept();
    }

    @Test
    public void testProbeScrubWithOutput() throws Throwable
    {
        createTableWithExpiringRow();
        probe.scrub(probe.output().out, true, false, true, OverwriteTTLMode.NO_TTL, 1, keyspace(), currentTable());
        assertTTLRemoved(TIMESTAMP);

        createTableWithExpiringRow();
        probe.scrub(probe.output().out, true, false, true, OverwriteTTLMode.NO_TTL, true, 1, keyspace(), currentTable());
        assertTTLRemoved(TIMESTAMP + 1);

        createTableWithExpiringRow();
        probe.scrub(probe.output().out, true, false, true, false, 1, keyspace(), currentTable());
        assertTTLKept();

        createTableWithExpiringRow();
        probe.scrub(probe.output().out, true, false, true, true, 1, keyspace(), currentTable());
        assertTTLKept();
    }

    // ---- StorageService ------------------------------------------------------------------------------------------

    @Test
    public void testStorageServiceStringMode() throws Throwable
    {
        StorageService ss = StorageService.instance;

        createTableWithExpiringRow();
        assertEquals(SUCCESS, ss.scrub(true, false, true, "NO_TTL", false, 1, keyspace(), currentTable()));
        assertTTLRemoved(TIMESTAMP);

        createTableWithExpiringRow();
        assertEquals(SUCCESS, ss.scrub(true, false, true, "NO_TTL", true, 1, keyspace(), currentTable()));
        assertTTLRemoved(TIMESTAMP + 1);

        // a null or empty mode means NONE
        createTableWithExpiringRow();
        assertEquals(SUCCESS, ss.scrub(true, false, true, (String) null, false, 1, keyspace(), currentTable()));
        assertEquals(SUCCESS, ss.scrub(true, false, true, "", false, 1, keyspace(), currentTable()));
        assertTTLKept();
    }

    @Test
    public void testStorageServiceEnumMode() throws Throwable
    {
        StorageService ss = StorageService.instance;

        createTableWithExpiringRow();
        assertEquals(SUCCESS, ss.scrub(true, false, true, OverwriteTTLMode.NO_TTL, 1, keyspace(), currentTable()));
        assertTTLRemoved(TIMESTAMP);

        createTableWithExpiringRow();
        assertEquals(SUCCESS, ss.scrub(true, false, true, OverwriteTTLMode.NO_TTL, true, 1, keyspace(), currentTable()));
        assertTTLRemoved(TIMESTAMP + 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testStorageServiceRejectsBumpWithoutNoTTL() throws Throwable
    {
        createTableWithExpiringRow();
        StorageService.instance.scrub(true, false, true, "NONE", true, 1, keyspace(), currentTable());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testStorageServiceRejectsInvalidMode() throws Throwable
    {
        createTableWithExpiringRow();
        StorageService.instance.scrub(true, false, true, "NOT_A_MODE", false, 1, keyspace(), currentTable());
    }

    // ---- ColumnFamilyStore / CompactionManager -------------------------------------------------------------------

    @Test
    public void testColumnFamilyStoreScrubOverloads() throws Throwable
    {
        createTableWithExpiringRow();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        assertEquals(CompactionManager.AllSSTableOpStatus.SUCCESSFUL,
                     cfs.scrub(true, false, OverwriteTTLMode.NO_TTL, true, 1));
        assertTTLRemoved(TIMESTAMP);

        createTableWithExpiringRow();
        cfs = getCurrentColumnFamilyStore();
        assertEquals(CompactionManager.AllSSTableOpStatus.SUCCESSFUL,
                     cfs.scrub(true, false, OverwriteTTLMode.NO_TTL, false, true, 1));
        assertTTLRemoved(TIMESTAMP);
    }

    @Test
    public void testCompactionManagerPerformScrubWithMode() throws Throwable
    {
        createTableWithExpiringRow();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        assertEquals(CompactionManager.AllSSTableOpStatus.SUCCESSFUL,
                     CompactionManager.instance.performScrub(cfs, false, true, OverwriteTTLMode.NO_TTL, 1));

        assertTTLRemoved(TIMESTAMP);
    }

    @Test
    public void testCompactionManagerPerformScrubWithBump() throws Throwable
    {
        createTableWithExpiringRow();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        IScrubber.Options options = IScrubber.options()
                                             .checkData()
                                             .overwriteTTLMode(OverwriteTTLMode.NO_TTL)
                                             .bumpTTLTimestamp(true)
                                             .build();
        assertEquals(CompactionManager.AllSSTableOpStatus.SUCCESSFUL,
                     CompactionManager.instance.performScrub(cfs, options, 1));

        assertTTLRemoved(TIMESTAMP + 1);
    }

    // ---- regular flows must not be affected ----------------------------------------------------------------------

    /**
     * Regular compaction must neither strip TTLs nor touch timestamps: the NO_TTL logic is only reachable from the
     * scrubber when explicitly requested.
     */
    @Test
    public void testCompactionDoesNotStripTTLOrBumpTimestamp() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        execute("INSERT INTO %s (k, v) VALUES (0, 0) USING TTL " + TTL + " AND TIMESTAMP 1000");
        flush();
        execute("INSERT INTO %s (k, v) VALUES (1, 1) USING TTL " + TTL + " AND TIMESTAMP 2000");
        flush();
        execute("INSERT INTO %s (k, v) VALUES (2, 2) USING TIMESTAMP 3000");
        flush();

        compact();
        assertEquals(1, getCurrentColumnFamilyStore().getLiveSSTables().size());

        assertRowsIgnoringOrder(execute("SELECT k, writetime(v) FROM %s"),
                                row(0, 1000L), row(1, 2000L), row(2, 3000L));
        assertEquals(TTL, execute("SELECT ttl(v) FROM %s WHERE k = 0").one().getInt("ttl(v)"), 600);
        assertEquals(TTL, execute("SELECT ttl(v) FROM %s WHERE k = 1").one().getInt("ttl(v)"), 600);
        assertRows(execute("SELECT ttl(v) FROM %s WHERE k = 2"), row((Object) null));
    }

    /**
     * A scrub without NO_TTL (default options, or the other TTL modes) goes through the regular scrub flow: it keeps
     * TTLs and timestamps and never reports overwritten rows.
     */
    @Test
    public void testScrubWithoutNoTTLKeepsTTLAndTimestamp() throws Throwable
    {
        createTableWithExpiringRow();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        // default options, as used by every regular scrub
        IScrubber.Options defaults = IScrubber.options().checkData().build();
        assertEquals(OverwriteTTLMode.NONE, defaults.overwriteTTLMode);
        assertFalse(defaults.bumpTTLTimestamp);
        assertEquals(CompactionManager.AllSSTableOpStatus.SUCCESSFUL, CompactionManager.instance.performScrub(cfs, defaults, 1));
        assertTTLKept();

        // the TTL modes that are not NO_TTL
        for (OverwriteTTLMode mode : new OverwriteTTLMode[]{ OverwriteTTLMode.NONE, OverwriteTTLMode.REINSERT_OVERFLOWED_TTL })
        {
            IScrubber.Options options = IScrubber.options().checkData().overwriteTTLMode(mode).build();
            assertEquals(CompactionManager.AllSSTableOpStatus.SUCCESSFUL, CompactionManager.instance.performScrub(cfs, options, 1));
            assertTTLKept();
        }

        // plain nodetool scrub
        ToolRunner.invokeNodetool("scrub", "--no-snapshot", keyspace(), currentTable()).assertOnCleanExit();
        assertTTLKept();
    }

    /** The scrubber only reports overwritten rows when NO_TTL was requested. */
    @Test
    public void testScrubResultReportsOverwrittenRowsOnlyForNoTTL() throws Throwable
    {
        createTableWithExpiringRow();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        assertEquals(0, scrubResult(cfs, IScrubber.options().checkData().build()).noTTLOverwrittenRows);
        assertEquals(1, scrubResult(cfs, IScrubber.options().checkData().overwriteTTLMode(OverwriteTTLMode.NO_TTL).build()).noTTLOverwrittenRows);
    }

    private static IScrubber.ScrubResult scrubResult(ColumnFamilyStore cfs, IScrubber.Options options)
    {
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        try (LifecycleTransaction txn = cfs.getTracker().tryModify(Collections.singletonList(sstable), OperationType.SCRUB);
             IScrubber scrubber = sstable.descriptor.getFormat().getScrubber(cfs, txn, new OutputHandler.LogOutput(), options))
        {
            return scrubber.scrubWithResult();
        }
    }

    @Test
    public void testScrubIsIdempotentWithBump() throws Throwable
    {
        createTableWithExpiringRow();

        for (int i = 0; i < 2; i++)
            assertEquals(SUCCESS, probe.scrub(true, false, true, OverwriteTTLMode.NO_TTL, true, 1, keyspace(), currentTable()));

        // the second scrub finds no TTL left and must not bump the timestamp again
        assertTTLRemoved(TIMESTAMP + 1);
    }
}
