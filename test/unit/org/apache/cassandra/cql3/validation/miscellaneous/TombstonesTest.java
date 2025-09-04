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
package org.apache.cassandra.cql3.validation.miscellaneous;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Throwables;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.filter.TombstoneOverwhelmingException;
import org.apache.cassandra.db.memtable.TrieMemtable;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.fail;
import static org.junit.Assert.assertFalse;

/**
 * Test that TombstoneOverwhelmingException gets thrown when it should be and doesn't when it shouldn't be.
 */
@RunWith(Parameterized.class)
public class TombstonesTest extends CQLTester
{
    @Parameterized.Parameter
    public String memtableClass;

    @Parameterized.Parameter(1)
    public boolean flush;

    @Parameterized.Parameters(name = "{0} flush: {1}")
    public static Iterable<Object[]> parameters()
    {
        return Arrays.asList(new Object[] {"SkipListMemtable", false},
                             new Object[] {"TrieMemtableStage1", true}, // this uses the same partition code as SkipListMemtable
                             new Object[] {"TrieMemtableStage2", false}, // this flushes like SkipListMemtable, no need to test flushed
                             new Object[] {"TrieMemtable", false},
                             new Object[] {"TrieMemtable", true});
    }

    static final int ORIGINAL_FAILURE_THRESHOLD = DatabaseDescriptor.getGuardrailsConfig().tombstone_failure_threshold;
    static final int FAILURE_THRESHOLD = 100;

    static final int ORIGINAL_WARN_THRESHOLD = DatabaseDescriptor.getGuardrailsConfig().tombstone_warn_threshold;
    static final int WARN_THRESHOLD = 50;

    @BeforeClass
    public static void setUp() throws Throwable
    {
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.getGuardrailsConfig().setTombstoneWarnThreshold(WARN_THRESHOLD);
        DatabaseDescriptor.getGuardrailsConfig().setTombstoneFailureThreshold(FAILURE_THRESHOLD);
    }

    @AfterClass
    public static void tearDown()
    {
        DatabaseDescriptor.getGuardrailsConfig().setTombstoneFailureThreshold(ORIGINAL_FAILURE_THRESHOLD);
        DatabaseDescriptor.getGuardrailsConfig().setTombstoneWarnThreshold(ORIGINAL_WARN_THRESHOLD);
    }

    @Test
    public void testBelowThresholdSelect() throws Throwable
    {

        String tableName = createTable("CREATE TABLE %s (a text, b text, c text, PRIMARY KEY (a, b)) WITH memtable = {'class': '" + memtableClass + "'};");
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(tableName);
        long oldFailures = cfs.metric.tombstoneFailures.getCount();
        long oldWarnings = cfs.metric.tombstoneWarnings.getCount();
        boolean tombstonesCountTowardsThresholds = flush || !(cfs.getCurrentMemtable() instanceof TrieMemtable);

        // insert exactly the amount of tombstones that shouldn't trigger an exception
        for (int i = 0; i < FAILURE_THRESHOLD; i++)
            execute("DELETE FROM %s WHERE a = 'key' and b = '" + i + "'");
        if (flush)
            flush();

        try
        {
            execute("SELECT * FROM %s WHERE a = 'key';");
            assertEquals(oldFailures, cfs.metric.tombstoneFailures.getCount());
            assertEquals(oldWarnings + (tombstonesCountTowardsThresholds ? 1 : 0), cfs.metric.tombstoneWarnings.getCount());
        }
        catch (Throwable e)
        {
            fail("SELECT with tombstones below the threshold should not have failed, but has: " + e);
        }
    }

    @Test
    public void testBeyondThresholdSelect() throws Throwable
    {
        String tableName = createTable("CREATE TABLE %s (a text, b text, c text, PRIMARY KEY (a, b)) WITH memtable = {'class': '" + memtableClass + "'};");
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(tableName);
        long oldFailures = cfs.metric.tombstoneFailures.getCount();
        long oldWarnings = cfs.metric.tombstoneWarnings.getCount();
        boolean tombstonesCountTowardsThresholds = flush || !(cfs.getCurrentMemtable() instanceof TrieMemtable);

        // insert exactly the amount of tombstones that *SHOULD* trigger an exception
        for (int i = 0; i < FAILURE_THRESHOLD + 1; i++)
            execute("DELETE FROM %s WHERE a = 'key' and b = '" + i + "'");
        if (flush)
            flush();

        try
        {
            execute("SELECT * FROM %s WHERE a = 'key';");
            assertFalse("SELECT with tombstones beyond the threshold should have failed, but hasn't", tombstonesCountTowardsThresholds);
        }
        catch (Throwable e)
        {
            assertTrue(memtableClass + " should not be affected by the number of tombstones", tombstonesCountTowardsThresholds);

            String error = "Expected exception instanceof TombstoneOverwhelmingException instead got "
                           + System.lineSeparator()
                           + Throwables.getStackTraceAsString(e);
            assertTrue(error, e instanceof TombstoneOverwhelmingException);
            assertEquals(oldWarnings, cfs.metric.tombstoneWarnings.getCount());
            assertEquals(oldFailures + 1, cfs.metric.tombstoneFailures.getCount());
        }
    }

    @Test
    public void testAllShadowedSelect() throws Throwable
    {
        testAllShadowedSelect(false);
    }

    @Test
    public void testAllShadowedInSeparateSSTable() throws Throwable
    {
        testAllShadowedSelect(true);
    }

    public void testAllShadowedSelect(boolean flushBetween) throws Throwable
    {
        String tableName = createTable("CREATE TABLE %s (a text, b text, c text, PRIMARY KEY (a, b)) WITH memtable = {'class': '" + memtableClass + "'};");
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(tableName);
        long oldFailures = cfs.metric.tombstoneFailures.getCount();
        long oldWarnings = cfs.metric.tombstoneWarnings.getCount();

        // insert exactly the amount of tombstones that *SHOULD* normally trigger an exception
        for (int i = 0; i < FAILURE_THRESHOLD + 1; i++)
            execute("INSERT INTO %s (a, b, c) VALUES ('key', 'column" + i + "', null);");

        if (flushBetween)
            flush();

        // delete all with a partition level tombstone
        execute("DELETE FROM %s WHERE a = 'key'");

        if (flush)
            flush();

        try
        {
            execute("SELECT * FROM %s WHERE a = 'key';");
            assertEquals(oldFailures, cfs.metric.tombstoneFailures.getCount());
            assertEquals(oldWarnings, cfs.metric.tombstoneWarnings.getCount());
        }
        catch (Throwable e)
        {
            fail("SELECT with tombstones shadowed by a partition tombstone should not have failed, but has: " + e);
        }
    }

    @Test
    public void testLiveShadowedCellsSelect() throws Throwable
    {
        String tableName = createTable("CREATE TABLE %s (a text, b text, c text, PRIMARY KEY (a, b)) WITH memtable = {'class': '" + memtableClass + "'};");
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(tableName);
        long oldFailures = cfs.metric.tombstoneFailures.getCount();
        long oldWarnings = cfs.metric.tombstoneWarnings.getCount();

        for (int i = 0; i < FAILURE_THRESHOLD + 1; i++)
            execute("INSERT INTO %s (a, b, c) VALUES ('key', 'column" + i + "', 'column');");

        // delete all with a partition level tombstone
        execute("DELETE FROM %s WHERE a = 'key'");

        if (flush)
            flush();

        try
        {
            execute("SELECT * FROM %s WHERE a = 'key';");
            assertEquals(oldFailures, cfs.metric.tombstoneFailures.getCount());
            assertEquals(oldWarnings, cfs.metric.tombstoneWarnings.getCount());
        }
        catch (Throwable e)
        {
            fail("SELECT with regular cells shadowed by a partition tombstone should not have failed, but has: " + e);
        }
    }

    @Test
    public void testExpiredTombstones() throws Throwable
    {
        String tableName = createTable("CREATE TABLE %s (a text, b text, c text, PRIMARY KEY (a, b)) WITH gc_grace_seconds = 1 AND memtable = {'class': '" + memtableClass + "'};");
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(tableName);
        long oldFailures = cfs.metric.tombstoneFailures.getCount();
        long oldWarnings = cfs.metric.tombstoneWarnings.getCount();

        for (int i = 0; i < FAILURE_THRESHOLD + 1; i++)
            execute("INSERT INTO %s (a, b, c) VALUES ('key', 'column" + i + "', null);");
        if (flush)
            flush();

        // not yet past gc grace - must throw a TOE
        try
        {
            execute("SELECT * FROM %s WHERE a = 'key';");
            fail("SELECT with tombstones beyond the threshold should have failed, but hasn't");
        }
        catch (Throwable e)
        {
            assertTrue(e instanceof TombstoneOverwhelmingException);

            assertEquals(++oldFailures, cfs.metric.tombstoneFailures.getCount());
            assertEquals(oldWarnings, cfs.metric.tombstoneWarnings.getCount());
        }

        // sleep past gc grace
        TimeUnit.SECONDS.sleep(2);

        // past gc grace - must not throw a TOE now
        try
        {
            execute("SELECT * FROM %s WHERE a = 'key';");
            assertEquals(oldFailures, cfs.metric.tombstoneFailures.getCount());
            assertEquals(oldWarnings, cfs.metric.tombstoneWarnings.getCount());
        }
        catch (Throwable e)
        {
            fail("SELECT with expired tombstones beyond the threshold should not have failed, but has: " + e);
        }
    }

    @Test
    public void testBeyondWarnThresholdSelect() throws Throwable
    {
        String tableName = createTable("CREATE TABLE %s (a text, b text, c text, PRIMARY KEY (a,b)) WITH memtable = {'class': '" + memtableClass + "'};");
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(tableName);
        long oldFailures = cfs.metric.tombstoneFailures.getCount();
        long oldWarnings = cfs.metric.tombstoneWarnings.getCount();
        boolean tombstonesCountTowardsThresholds = flush || !(cfs.getCurrentMemtable() instanceof TrieMemtable);

        // insert the number of tombstones that *SHOULD* trigger an Warning
        for (int i = 0; i < WARN_THRESHOLD + 1; i++)
            execute("DELETE FROM %s WHERE a = 'key' and b = '" + i + "'");
        if (flush)
            flush();

        try
        {
            execute("SELECT * FROM %s WHERE a = 'key';");
            assertEquals(oldWarnings + (tombstonesCountTowardsThresholds ? 1 : 0), cfs.metric.tombstoneWarnings.getCount());
            assertEquals(oldFailures, cfs.metric.tombstoneFailures.getCount());
        }
        catch (Throwable e)
        {
            fail("SELECT with tombstones below the failure threshold and above warning threashhold should not have failed, but has: " + e);
        }
    }


}
