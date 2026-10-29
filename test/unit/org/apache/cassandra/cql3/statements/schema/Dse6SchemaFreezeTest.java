/*
 * Copyright IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.cql3.statements.schema;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.exceptions.InvalidQueryException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.net.DseCompatRefusals;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.service.DseUpgradeGuards;
import org.apache.cassandra.triggers.TriggersTest;

import static org.apache.cassandra.config.CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE_ALLOW_DDL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The schema freeze of the DSE 6 upgrade: every kind of schema statement that would change the schema is refused in
 * {@code DSE_6} mode, through the client path and the internal one alike, unless
 * {@code cassandra.internode_compatibility_mode.allow_ddl} is set; one that changes nothing runs.
 */
public class Dse6SchemaFreezeTest extends CQLTester
{
    private InternodeCompatibilityMode modeBefore;

    @BeforeClass
    public static void setUpNetwork() throws Exception
    {
        requireNetwork();
    }

    @Before
    public void saveMode()
    {
        modeBefore = DatabaseDescriptor.getInternodeCompatibilityMode();
        DseCompatRefusals.reset();
    }

    @After
    public void restoreMode()
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(modeBefore);
        DseCompatRefusals.reset();
    }

    @Test
    public void everySchemaStatementIsRefusedInDse6Mode() throws Throwable
    {
        // objects to drop or alter: a view on a table of its own, so that the main table can be dropped
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        String view = createView("CREATE MATERIALIZED VIEW %s AS SELECT k, v FROM %s WHERE k IS NOT NULL AND v IS NOT NULL PRIMARY KEY (v, k)");
        String table = createTable("CREATE TABLE %s (k int PRIMARY KEY, v int, x int)");
        execute("INSERT INTO %s (k, v) VALUES (1, 1)");
        String index = createIndex("CREATE INDEX ON %s (x)");
        String type = createType("CREATE TYPE %s (a int)");
        String function = createFunction(KEYSPACE, "int, int", "CREATE FUNCTION %s (s int, a int) CALLED ON NULL INPUT RETURNS int LANGUAGE java AS 'return s;'");
        execute("CREATE TRIGGER dse6_existing_trigger ON %s USING '" + TriggersTest.TestTrigger.class.getName() + '\'');

        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        String[] statements = {
            "CREATE KEYSPACE dse6_frozen WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",
            "ALTER KEYSPACE " + KEYSPACE + " WITH durable_writes = false",
            "DROP KEYSPACE " + KEYSPACE,
            "CREATE TABLE " + KEYSPACE + ".dse6_frozen (k int PRIMARY KEY)",
            "ALTER TABLE %s ADD w int",
            "DROP TABLE %s",
            "CREATE INDEX dse6_frozen_idx ON %s (v)",
            "CREATE CUSTOM INDEX dse6_frozen_sai ON %s (v) USING 'StorageAttachedIndex'",
            "CREATE TYPE " + KEYSPACE + ".dse6_frozen_type (a int)",
            "CREATE MATERIALIZED VIEW " + KEYSPACE + ".dse6_frozen_view AS SELECT k, v FROM %s WHERE k IS NOT NULL AND v IS NOT NULL PRIMARY KEY (v, k)",
            "CREATE FUNCTION " + KEYSPACE + ".dse6_frozen_fn (a int) RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE java AS 'return a;'",
            "CREATE TRIGGER dse6_frozen_trigger ON %s USING '" + TriggersTest.TestTrigger.class.getName() + '\'',
            "DROP INDEX " + KEYSPACE + '.' + index,
            "ALTER TYPE " + KEYSPACE + '.' + type + " ADD b int",
            "DROP TYPE " + KEYSPACE + '.' + type,
            "CREATE AGGREGATE " + KEYSPACE + ".dse6_frozen_agg (int) SFUNC " + function.substring(function.indexOf('.') + 1) + " STYPE int",
            "DROP FUNCTION " + function,
            "DROP TRIGGER dse6_existing_trigger ON %s",
            "ALTER MATERIALIZED VIEW " + KEYSPACE + '.' + view + " WITH comment = 'frozen'",
            "DROP MATERIALIZED VIEW " + KEYSPACE + '.' + view,
        };
        for (String statement : statements)
        {
            assertThatThrownBy(() -> execute(statement))
            .as(statement)
            .isInstanceOf(InvalidRequestException.class)
            .hasMessageStartingWith(DseUpgradeGuards.SCHEMA_FROZEN)
            .hasMessageContaining("-Dcassandra.internode_compatibility_mode.allow_ddl=true");
        }
        assertThat(DseCompatRefusals.count(DseUpgradeGuards.SCHEMA_CHANGE)).isEqualTo(statements.length);

        // DML and reads are not affected by the freeze
        execute("INSERT INTO %s (k, v) VALUES (2, 2)");
        assertRows(execute("SELECT k, v FROM %s WHERE k = 2"), row(2, 2));
        assertThat(currentTable()).isEqualTo(table);
    }

    @Test
    public void theClientPathIsFrozenToo() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        assertThatThrownBy(() -> executeNet("ALTER TABLE " + KEYSPACE + '.' + currentTable() + " ADD w int"))
        .isInstanceOf(InvalidQueryException.class)
        .hasMessageStartingWith(DseUpgradeGuards.SCHEMA_FROZEN);
        assertThat(DseCompatRefusals.count(DseUpgradeGuards.SCHEMA_CHANGE)).isEqualTo(1);
        assertThat(Schema.instance.getTableMetadata(KEYSPACE, currentTable()).getColumn(ColumnIdentifier.getInterned("w", false))).isNull();
    }

    @Test
    public void statementsThatChangeNothingRun() throws Throwable
    {
        String table = createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        String view = createView("CREATE MATERIALIZED VIEW %s AS SELECT k, v FROM %s WHERE k IS NOT NULL AND v IS NOT NULL PRIMARY KEY (v, k)");
        String index = createIndex("CREATE INDEX ON %s (v)");
        String type = createType("CREATE TYPE %s (a int)");
        String function = createFunction(KEYSPACE, "int, int", "CREATE FUNCTION %s (s int, a int) CALLED ON NULL INPUT RETURNS int LANGUAGE java AS 'return s;'");
        String sfunc = function.substring(function.indexOf('.') + 1);
        String aggregate = createAggregate(KEYSPACE, "int", "CREATE AGGREGATE %s (int) SFUNC " + sfunc + " STYPE int");
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        // what applications issue at their start: an existing object created again, a missing one dropped
        executeNet("CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}");
        executeNet("CREATE TABLE IF NOT EXISTS " + KEYSPACE + '.' + table + " (k int PRIMARY KEY, v int, w int)");
        execute("CREATE TABLE IF NOT EXISTS %s (k int PRIMARY KEY, v int)");
        executeNet("CREATE INDEX IF NOT EXISTS " + index + " ON " + KEYSPACE + '.' + table + " (v)");
        executeNet("CREATE TYPE IF NOT EXISTS " + KEYSPACE + '.' + type + " (a int)");
        executeNet("CREATE FUNCTION IF NOT EXISTS " + function + " (s int, a int) CALLED ON NULL INPUT RETURNS int LANGUAGE java AS 'return s;'");
        executeNet("CREATE AGGREGATE IF NOT EXISTS " + aggregate + " (int) SFUNC " + sfunc + " STYPE int");
        executeNet("CREATE MATERIALIZED VIEW IF NOT EXISTS " + KEYSPACE + '.' + view + " AS SELECT k, v FROM " + KEYSPACE + '.' + table +
                   " WHERE k IS NOT NULL AND v IS NOT NULL PRIMARY KEY (v, k)");
        executeNet("ALTER TABLE IF EXISTS " + KEYSPACE + ".dse6_missing ADD w int");
        executeNet("DROP KEYSPACE IF EXISTS dse6_missing_ks");
        executeNet("DROP TABLE IF EXISTS " + KEYSPACE + ".dse6_missing");
        executeNet("DROP INDEX IF EXISTS " + KEYSPACE + ".dse6_missing_idx");
        executeNet("DROP TYPE IF EXISTS " + KEYSPACE + ".dse6_missing_type");
        executeNet("DROP FUNCTION IF EXISTS " + KEYSPACE + ".dse6_missing_fn");
        executeNet("DROP AGGREGATE IF EXISTS " + KEYSPACE + ".dse6_missing_agg");
        executeNet("DROP MATERIALIZED VIEW IF EXISTS " + KEYSPACE + ".dse6_missing_view");
        executeNet("DROP TRIGGER IF EXISTS dse6_missing_trigger ON " + KEYSPACE + '.' + table);
        assertThat(DseCompatRefusals.count(DseUpgradeGuards.SCHEMA_CHANGE)).isZero();
        // nothing changed: the existing table keeps its columns
        assertThat(Schema.instance.getTableMetadata(KEYSPACE, table).getColumn(ColumnIdentifier.getInterned("w", false))).isNull();

        // the same statements are refused when they would change the schema
        assertThatThrownBy(() -> executeNet("CREATE TABLE IF NOT EXISTS " + KEYSPACE + ".dse6_new (k int PRIMARY KEY)"))
        .isInstanceOf(InvalidQueryException.class)
        .hasMessageStartingWith(DseUpgradeGuards.SCHEMA_FROZEN);
        assertThatThrownBy(() -> execute("DROP INDEX IF EXISTS " + KEYSPACE + '.' + index))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageStartingWith(DseUpgradeGuards.SCHEMA_FROZEN);
        assertThat(DseCompatRefusals.count(DseUpgradeGuards.SCHEMA_CHANGE)).isEqualTo(2);
        assertThat(Schema.instance.getTableMetadata(KEYSPACE, "dse6_new")).isNull();
        assertThat(Schema.instance.getKeyspaceMetadata(KEYSPACE).findIndexedTable(index)).isPresent();
    }

    @Test
    public void statementsThatFailToApplyFailWithTheirOwnError() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        // as without the freeze: not a refusal of the freeze, not counted
        assertThatThrownBy(() -> execute("ALTER TABLE %s ADD v int"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessage("Column with name 'v' already exists");
        assertThatThrownBy(() -> executeNet("DROP TABLE " + KEYSPACE + ".dse6_missing"))
        .isInstanceOf(InvalidQueryException.class)
        .hasMessage("Table '" + KEYSPACE + ".dse6_missing' doesn't exist");
        // a disabled feature
        boolean viewsEnabled = DatabaseDescriptor.getMaterializedViewsEnabled();
        DatabaseDescriptor.setMaterializedViewsEnabled(false);
        try
        {
            assertThatThrownBy(() -> execute("CREATE MATERIALIZED VIEW " + KEYSPACE + ".dse6_disabled_view AS SELECT k, v FROM %s " +
                                             "WHERE k IS NOT NULL AND v IS NOT NULL PRIMARY KEY (v, k)"))
            .isInstanceOf(InvalidRequestException.class)
            .hasMessageNotContaining(DseUpgradeGuards.SCHEMA_FROZEN)
            .hasMessageContaining("disabled");
        }
        finally
        {
            DatabaseDescriptor.setMaterializedViewsEnabled(viewsEnabled);
        }
        assertThat(DseCompatRefusals.count(DseUpgradeGuards.SCHEMA_CHANGE)).isZero();
    }

    @Test
    public void thePropertyLiftsTheFreeze() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        try (WithProperties properties = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE_ALLOW_DDL, true))
        {
            execute("ALTER TABLE %s ADD w int");
            execute("INSERT INTO %s (k, v, w) VALUES (1, 1, 1)");
            assertRows(execute("SELECT w FROM %s WHERE k = 1"), row(1));
        }
        assertThat(DseCompatRefusals.count(DseUpgradeGuards.SCHEMA_CHANGE)).isZero();
        assertThatThrownBy(() -> execute("ALTER TABLE %s ADD x int")).isInstanceOf(InvalidRequestException.class)
                                                                     .hasMessageStartingWith(DseUpgradeGuards.SCHEMA_FROZEN);
    }

    @Test
    public void noneModeIsUnchanged() throws Throwable
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        execute("ALTER TABLE %s ADD w int");
        execute("DROP TABLE %s");
        assertThat(DseCompatRefusals.total()).isZero();
    }
}
