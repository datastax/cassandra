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

package org.apache.cassandra.schema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.InvalidColumnTypeException;
import org.apache.cassandra.utils.FBUtilities;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests that existing schemas and sstables with a duration where values are sorted (a primary key column, or the keys
 * or elements of a non-frozen map or set) load, while new tables with such types are still rejected.
 */
public class DurationOrderingOnLoadTest extends CQLTester
{
    /** Durations in the order they are inserted, which is the reverse of the order of their serialized bytes. */
    private static final List<String> DURATIONS = Arrays.asList("1mo", "40d", "1d", "-1d", "1h");

    /**
     * Column definitions and the column with the duration; {@code DUR} stands for the duration type and {@code UDT}
     * for a UDT with a duration.
     */
    private static final String[][] SHAPES = {
    // Accepted by Apache Cassandra
    { "pk int PRIMARY KEY, s set<frozen<tuple<DUR, int>>>", "s" },
    { "pk int PRIMARY KEY, m map<frozen<list<DUR>>, int>", "m" },
    { "pk frozen<set<frozen<tuple<DUR, int>>>> PRIMARY KEY, v int", "pk" },
    { "pk int, ck frozen<set<frozen<list<DUR>>>>, v int, PRIMARY KEY (pk, ck)", "ck" },
    { "pk int PRIMARY KEY, s set<frozen<UDT>>", "s" },
    // Reached on Apache Cassandra with ALTER TYPE
    { "pk int, ck frozen<UDT>, v int, PRIMARY KEY (pk, ck)", "ck" },
    // Accepted before CNDB-9753
    { "pk frozen<map<DUR, text>> PRIMARY KEY, v int", "pk" },
    { "pk int PRIMARY KEY, s set<frozen<map<DUR, int>>>", "s" },
    // Not accepted by any version
    { "pk int PRIMARY KEY, m map<DUR, text>", "m" },
    { "pk DUR PRIMARY KEY, v int", "pk" }
    };

    @Test
    public void testSchemaReload() throws Throwable
    {
        String udt = createType("CREATE TYPE %s (d duration, i int)");
        String udtWithBigint = createType("CREATE TYPE %s (d bigint, i int)");

        for (String[] shape : SHAPES)
        {
            String columns = shape[0].replace("DUR", "bigint").replace("UDT", udtWithBigint);
            String table = createTable(String.format("CREATE TABLE %%s (%s)", columns));
            String stored = storedType(table, shape[1]);
            String rewritten = stored.replace("bigint", "duration").replace(udtWithBigint, udt);
            setStoredType(table, shape[1], rewritten);
            try
            {
                TableMetadata metadata = SchemaKeyspace.fetchNonSystemKeyspaces().getNullable(KEYSPACE).getTableOrViewNullable(table);
                assertThat(column(metadata, shape[1]).type.asCQL3Type().toSchemaString()).describedAs(shape[0]).isEqualTo(rewritten);
            }
            finally
            {
                setStoredType(table, shape[1], stored);
            }
        }
    }

    @Test
    public void testDroppedColumn() throws Throwable
    {
        String table = createTable("CREATE TABLE %s (pk int PRIMARY KEY, v int)");
        String type = "set<frozen<tuple<duration, int>>>";
        execute("INSERT INTO system_schema.dropped_columns " +
                "(keyspace_name, table_name, column_name, dropped_time, kind, type) VALUES (?, ?, ?, ?, ?, ?)",
                KEYSPACE, table, "d", new Date(), "regular", type);
        try
        {
            TableMetadata metadata = SchemaKeyspace.fetchNonSystemKeyspaces().getNullable(KEYSPACE).getTableOrViewNullable(table);
            assertThat(metadata.droppedColumns.values())
            .anySatisfy(c -> assertThat(c.column.type.asCQL3Type().toSchemaString()).isEqualTo(type));
        }
        finally
        {
            execute("DELETE FROM system_schema.dropped_columns WHERE keyspace_name = ? AND table_name = ? AND column_name = ?",
                    KEYSPACE, table, "d");
        }
    }

    @Test
    public void testOtherViolationsStillThrowOnLoad() throws Throwable
    {
        // A non-frozen primary key column that also has a duration
        String table = createTable("CREATE TABLE %s (pk frozen<set<frozen<tuple<bigint, int>>>> PRIMARY KEY, v int)");
        String stored = storedType(table, "pk");
        setStoredType(table, "pk", "set<frozen<tuple<duration, int>>>");
        try
        {
            assertThatThrownBy(SchemaKeyspace::fetchNonSystemKeyspaces)
            .isInstanceOf(InvalidColumnTypeException.class)
            .hasMessageContaining("non-frozen collections are not supported for PRIMARY KEY columns");
        }
        finally
        {
            setStoredType(table, "pk", stored);
        }
    }

    @Test
    public void testCreateTableStillRejected() throws Throwable
    {
        String udt = KEYSPACE + '.' + createType("CREATE TYPE %s (d duration, i int)");
        for (String[] shape : SHAPES)
        {
            String ddl = String.format("CREATE TABLE %s.%s (%s)", KEYSPACE, createTableName(),
                                       shape[0].replace("DUR", "duration").replace("UDT", udt));
            assertThatThrownBy(() -> execute(ddl))
            .describedAs(ddl)
            .isInstanceOf(InvalidColumnTypeException.class)
            .hasMessageContaining("duration types are not supported");
        }
    }

    @Test
    public void testSetOfTuples() throws Throwable
    {
        List<String> inserts = new ArrayList<>();
        List<String> sorted = new ArrayList<>();
        for (String d : DURATIONS)
        {
            inserts.add("UPDATE %s SET s = s + {(" + d + ", 0)} WHERE pk = 0");
            sorted.add(0, '(' + d + ", 0)");
        }
        testSchemaMergeAndSSTables("pk int PRIMARY KEY, s set<frozen<tuple<duration, int>>>", "s", inserts,
                                   "SELECT s FROM %s WHERE pk = 0",
                                   Collections.singletonList('{' + String.join(", ", sorted) + '}'));
    }

    @Test
    public void testClusteringSetOfLists() throws Throwable
    {
        List<String> inserts = new ArrayList<>();
        List<String> sorted = new ArrayList<>();
        for (String d : DURATIONS)
        {
            inserts.add("INSERT INTO %s (pk, ck, v) VALUES (0, {[" + d + "]}, 0)");
            sorted.add(0, "{[" + d + "]}");
        }
        testSchemaMergeAndSSTables("pk int, ck frozen<set<frozen<list<duration>>>>, v int, PRIMARY KEY (pk, ck)", "ck",
                                   inserts, "SELECT ck FROM %s WHERE pk = 0", sorted);
    }

    /**
     * Creates the table through a schema merge, as if received from a peer that has it, writes the rows with a flush
     * after every second one, and checks that the values read back in the expected order: from the memtable and
     * sstables, after a major compaction, and after the sstables are opened again from disk.
     */
    private void testSchemaMergeAndSSTables(String columns, String column, List<String> inserts, String select,
                                            List<String> expected) throws Throwable
    {
        String table = createTableName();
        KeyspaceMetadata keyspace = Schema.instance.getKeyspaceMetadata(KEYSPACE);
        String ddl = String.format("CREATE TABLE %s.%s (%s)", KEYSPACE, table, columns);
        TableMetadata metadata = CreateTableStatement.parse(ddl, KEYSPACE, keyspace.types).build();
        ((DefaultSchemaUpdateHandler) Schema.instance.updateHandler)
        .applyMutations(Collections.singletonList(SchemaTestUtil.createTableMutation(keyspace, metadata, FBUtilities.timestampMicros())));
        assertThat(Schema.instance.getTableMetadata(KEYSPACE, table)).isNotNull();

        ColumnFamilyStore cfs = getColumnFamilyStore(KEYSPACE, table);
        cfs.disableAutoCompaction();
        for (int i = 0; i < inserts.size(); i++)
        {
            execute(inserts.get(i).replace("%s", KEYSPACE + '.' + table));
            if (i % 2 == 1)
                flush(KEYSPACE, table);
        }
        assertValuesInOrder(table, select, column, expected);

        flush(KEYSPACE, table);
        cfs.forceMajorCompaction();
        int sstables = cfs.getLiveSSTables().size();
        assertThat(sstables).isPositive();
        assertValuesInOrder(table, select, column, expected);

        // Open the sstables again from disk, which validates the types in their headers. The compaction's obsolete
        // inputs are deleted asynchronously; loadNewSSTables() must not find them half-deleted.
        LifecycleTransaction.waitForDeletions();
        cfs.clearUnsafe();
        assertThat(cfs.getLiveSSTables()).isEmpty();
        cfs.loadNewSSTables();
        assertThat(cfs.getLiveSSTables()).hasSize(sstables);
        assertValuesInOrder(table, select, column, expected);
    }

    private void assertValuesInOrder(String table, String select, String column, List<String> expected) throws Throwable
    {
        AbstractType<?> type = column(Schema.instance.getTableMetadata(KEYSPACE, table), column).type;
        List<String> actual = new ArrayList<>();
        for (UntypedResultSet.Row row : execute(select.replace("%s", KEYSPACE + '.' + table)))
            actual.add(type.toCQLString(row.getBytes(column)));
        assertThat(actual).isEqualTo(expected);
    }

    private static ColumnMetadata column(TableMetadata metadata, String column)
    {
        return metadata.getColumn(ColumnIdentifier.getInterned(column, false));
    }

    private String storedType(String table, String column) throws Throwable
    {
        return execute("SELECT type FROM system_schema.columns WHERE keyspace_name = ? AND table_name = ? AND column_name = ?",
                       KEYSPACE, table, column).one().getString("type");
    }

    private void setStoredType(String table, String column, String type) throws Throwable
    {
        execute("UPDATE system_schema.columns SET type = ? WHERE keyspace_name = ? AND table_name = ? AND column_name = ?",
                type, KEYSPACE, table, column);
    }
}
