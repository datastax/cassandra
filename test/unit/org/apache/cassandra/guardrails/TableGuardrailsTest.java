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

package org.apache.cassandra.guardrails;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.statements.schema.TableAttributes;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.utils.Throwables;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TableGuardrailsTest extends CQLTester
{
    private static boolean defaultGuardrailsEnabled;
    private static long defaultTablesSoftLimit;
    private static long defaultTableHardLimit;
    private static Set<String> defaultAllowedTableProperties;

    @BeforeClass
    public static void setup()
    {
        requireAuthentication();
    }

    @Before
    public void before()
    {
        useSuperUser();

        queryNet("CREATE USER test WITH PASSWORD 'test'");
        queryNet("GRANT ALL PERMISSIONS ON KEYSPACE " + KEYSPACE + " TO test");
        queryNet("GRANT ALL PERMISSIONS ON ALL FUNCTIONS IN KEYSPACE " + KEYSPACE + " TO test");

        useUser("test", "test");

        defaultGuardrailsEnabled = DatabaseDescriptor.getGuardrailsConfig().enabled;
        defaultTablesSoftLimit = DatabaseDescriptor.getGuardrailsConfig().tables_warn_threshold;
        defaultTableHardLimit = DatabaseDescriptor.getGuardrailsConfig().tables_failure_threshold;
        defaultAllowedTableProperties = DatabaseDescriptor.getGuardrailsConfig().table_properties_disallowed;

        DatabaseDescriptor.getGuardrailsConfig().enabled = true;
    }

    @After
    public void after()
    {
        useSuperUser();
        queryNet("DROP USER test");

        DatabaseDescriptor.getGuardrailsConfig().enabled = defaultGuardrailsEnabled;
        DatabaseDescriptor.getGuardrailsConfig().tables_warn_threshold = defaultTablesSoftLimit;
        DatabaseDescriptor.getGuardrailsConfig().tables_failure_threshold = defaultTableHardLimit;
        DatabaseDescriptor.getGuardrailsConfig().table_properties_disallowed = defaultAllowedTableProperties;
    }

    @Test
    public void testTableLimit() throws InterruptedException
    {
        // check previous async dropping schema tasks have been finished...
        int waitInSeconds = 30;
        while (schemaCleanup.getActiveCount() > 0 && waitInSeconds-- >= 0)
        {
            Thread.sleep(1000);
        }

        int currentTables = Schema.instance.getUserKeyspaces().stream().map(Keyspace::open)
                                           .mapToInt(ks -> ks.getColumnFamilyStores().size()).sum();
        long warn = currentTables + 1;
        long fail = currentTables + 3;
        DatabaseDescriptor.getGuardrailsConfig().tables_warn_threshold = warn;
        DatabaseDescriptor.getGuardrailsConfig().tables_failure_threshold = fail;

        assertWarn(this::create, String.format("current number of tables %d exceeds warning threshold of %d", currentTables + 2, warn));
        assertWarn(this::create, String.format("current number of tables %d exceeds warning threshold of %d", currentTables + 3, warn));
        assertFails(this::create, String.format("Cannot have more than %s tables, failed to create table", fail));
    }

    @Test
    public void testTableProperties()
    {
        // only allow "gc_grace_seconds"
        DatabaseDescriptor.getGuardrailsConfig().table_properties_disallowed = TableAttributes.validKeywords.stream()
                                                                                                            .filter(p -> !p.equals("gc_grace_seconds")).map(String::toUpperCase).collect(Collectors.toSet());

        // table properties is not allowed
        assertFails(this::create, null);
        queryNet("USE  " + keyspace());
        assertFails(() -> createNet("with id = " + UUID.randomUUID()), "[id]");
        assertFails(() -> createNet("with compression = { 'enabled': 'false' }"), "[compression]");
        assertFails(() -> createNet("with compression = { 'enabled': 'false' } AND id = " + UUID.randomUUID()), "[compression, id]");
        assertFails(() -> createNet("with compaction = { 'class': 'SizeTieredCompactionStrategy' }"), "[compaction]");
        assertFails(() -> createNet("with compaction = { 'class': 'SizeTieredCompactionStrategy' }"), "[compaction]");
        assertFails(() -> createNet("with gc_grace_seconds = 1000 and compression = { 'enabled': 'false' }"), "[compression]");
        assertValid(() -> createNet("with gc_grace_seconds = 1000"));

        // alter column is allowed
        assertValid(this::create);
        assertValid(() -> query("ALTER TABLE %s ADD v1 int"));
        assertValid(() -> query("ALTER TABLE %s DROP v1"));
        assertValid(() -> query("ALTER TABLE %s RENAME pk to pk1"));

        // alter table properties except "gc_grace_seconds" is not allowed
        assertValid(() -> query("ALTER TABLE %s WITH gc_grace_seconds = 1000"));
        assertValid(() -> queryNet("ALTER TABLE %s WITH gc_grace_seconds = 1000"));
        assertFails(() -> queryNet("ALTER TABLE %s WITH compaction = { 'class': 'SizeTieredCompactionStrategy' } AND default_time_to_live = 1"),
                    "[compaction, default_time_to_live]");
        assertFails(() -> queryNet("ALTER TABLE %s WITH compaction = { 'class': 'SizeTieredCompactionStrategy' } AND crc_check_chance = 1"),
                    "[compaction, crc_check_chance]");

        // skip table properties guardrails for super user
        useSuperUser();
        queryNet("USE  " + keyspace());
        assertValid(() -> createNet("with compaction = { 'class': 'SizeTieredCompactionStrategy' }"));
        assertValid(() -> createNet("with gc_grace_seconds = 1000"));
        assertValid(() -> queryNet("ALTER TABLE %s WITH gc_grace_seconds = 1000 and default_time_to_live = 1000"));
        assertValid(() -> queryNet("ALTER TABLE %s WITH compaction = { 'class': 'SizeTieredCompactionStrategy' }"));
    }

    @Test
    public void testInvalidTableProperties()
    {
        GuardrailsConfig config = DatabaseDescriptor.getGuardrailsConfig();

        config.table_properties_disallowed = new HashSet<>(Arrays.asList("ID1", "gc_grace_seconds"));
        assertFails(config::validate, "[id1]");

        config.table_properties_disallowed = new HashSet<>(Arrays.asList("ID", "Gc_Grace_Seconds"));
        assertValid(config::validate);
    }

    private void assertWarn(Runnable runnable, String message)
    {
        boolean noWarning = message == null;
        ClientWarn.instance.captureWarnings();
        try
        {
            runnable.run();

            // Client Warnings
            List<String> warnings = ClientWarn.instance.getWarnings();
            if (warnings == null)
                warnings = Collections.emptyList();

            assertEquals(noWarning
                         ? "Expected not to warn, but warning was received: " + warnings
                         : "Expected to warn, but no warning was received", warnings.isEmpty(), noWarning);

            if (noWarning)
                return;

            assertEquals(format("Got more thant 1 warning (got %d => %s)", warnings.size(), warnings), 1, warnings.size());
            String warning = warnings.get(0);
            assertTrue(format("Warning message '%s' does not contain expected message '%s'", warning, message), warning.contains(message));
        }
        finally
        {
            ClientWarn.instance.resetWarnings();
        }
    }

    private void assertValid(Runnable runnable)
    {
        assertFails(runnable, null);
    }

    private void assertFails(Runnable runnable, String message)
    {
        boolean noFailure = message == null;
        try
        {
            runnable.run();
            if (!noFailure)
                fail("Expected to fail, but it did not");
        }
        catch (RuntimeException e)
        {
            assertFalse("Expect no failure, but got " + e.getMessage(), noFailure);
            assertTrue(format("Error message '%s' does not contain expected message '%s'", e.getMessage(), message),
                       e.getMessage().contains(message));
        }
    }

    private void create()
    {
        create("");
    }

    private void create(String withClause)
    {
        try
        {
            createTable(KEYSPACE, "CREATE TABLE %s(pk int, ck int, v int, primary key(pk, ck)) " + withClause);
        }
        catch (RuntimeException e)
        {
            throw unwrap(e);
        }
    }

    private void createNet(String withClause)
    {
        try
        {
            // allow table to be dropped after test
            String table = createTableName();
            executeNet(String.format("CREATE TABLE %s(pk int, ck int, v int, primary key(pk, ck)) " + withClause, table));
        }
        catch (Throwable e)
        {
            throw unwrap(e);
        }
    }

    private void query(String query)
    {
        try
        {
            execute(query);
        }
        catch (Throwable e)
        {
            throw unwrap(e);
        }
    }

    private void queryNet(String query)
    {
        try
        {
            executeNet(query);
        }
        catch (Throwable e)
        {
            throw unwrap(e);
        }
    }

    private static RuntimeException unwrap(Throwable e)
    {
        if (e.getCause() != null)
            e = e.getCause();

        return Throwables.unchecked(e);
    }
}
