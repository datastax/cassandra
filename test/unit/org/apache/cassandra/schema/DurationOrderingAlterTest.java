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

import java.util.Collections;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests that a table with an existing duration where values are sorted (see
 * {@link org.apache.cassandra.db.marshal.AbstractType#hasSortedDuration}), as Apache Cassandra or nodes
 * before CNDB-9753 created it, can still be altered, as can its keyspace and the types it uses, while a schema change
 * that would give another column such a duration is still rejected.
 */
public class DurationOrderingAlterTest extends CQLTester
{
    /** A clustering column and a regular column with a duration where values are sorted. */
    private static final String COLUMNS = "pk int, ck frozen<set<frozen<list<duration>>>>, " +
                                          "s set<frozen<tuple<duration, int>>>, v int, w int, PRIMARY KEY (pk, ck)";

    private static final String DURATION_ERROR = "duration types are not supported";

    @BeforeClass
    public static void enableFeatures()
    {
        DatabaseDescriptor.setMaterializedViewsEnabled(true);
        DatabaseDescriptor.setDynamicDataMaskingEnabled(true);
        // Materialized views need the distributed system keyspaces
        requireNetwork();
    }

    @Test
    public void testAlterKeyspace() throws Throwable
    {
        String keyspace = createKeyspace("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
        createTableFromPeer(keyspace, COLUMNS);

        execute(String.format("ALTER KEYSPACE %s WITH replication = {'class': 'NetworkTopologyStrategy', '%s': 1} " +
                              "AND durable_writes = false", keyspace, DATA_CENTER));

        KeyspaceMetadata metadata = Schema.instance.getKeyspaceMetadata(keyspace);
        assertThat(metadata.params.durableWrites).isFalse();
        assertThat(metadata.params.replication.klass.getSimpleName()).isEqualTo("NetworkTopologyStrategy");
    }

    @Test
    public void testAlterType() throws Throwable
    {
        String keyspace = createKeyspace("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
        execute(String.format("CREATE TYPE %s.unrelated (a int)", keyspace));
        execute(String.format("CREATE TYPE %s.with_duration (d duration, i int)", keyspace));
        execute(String.format("CREATE TYPE %s.in_clustering (a int)", keyspace));
        execute(String.format("CREATE TYPE %s.in_set (a int)", keyspace));
        execute(String.format("CREATE TYPE %s.in_regular (a int)", keyspace));
        createTableFromPeer(keyspace, COLUMNS);
        createTableFromPeer(keyspace, "pk int PRIMARY KEY, s set<frozen<with_duration>>");
        execute(String.format("CREATE TABLE %s.t_clustering (pk int, ck frozen<in_clustering>, PRIMARY KEY (pk, ck))", keyspace));
        execute(String.format("CREATE TABLE %s.t_set (pk int PRIMARY KEY, s set<frozen<in_set>>)", keyspace));
        execute(String.format("CREATE TABLE %s.t_regular (pk int PRIMARY KEY, r frozen<in_regular>)", keyspace));

        // A type that no table with a duration uses
        execute(String.format("ALTER TYPE %s.unrelated ADD b int", keyspace));
        execute(String.format("ALTER TYPE %s.unrelated RENAME a TO c", keyspace));

        // The type of a column that already has a duration where values are sorted
        execute(String.format("ALTER TYPE %s.with_duration ADD e int", keyspace));
        execute(String.format("ALTER TYPE %s.with_duration ADD f duration", keyspace));

        // A duration where values are sorted would be new for these columns
        assertThatThrownBy(() -> execute(String.format("ALTER TYPE %s.in_clustering ADD d duration", keyspace)))
        .hasMessageContaining(DURATION_ERROR + " within PRIMARY KEY columns");
        assertThatThrownBy(() -> execute(String.format("ALTER TYPE %s.in_set ADD d duration", keyspace)))
        .hasMessageContaining(DURATION_ERROR + " within non-frozen sets");

        // A duration in a regular frozen column is not sorted
        execute(String.format("ALTER TYPE %s.in_regular ADD d duration", keyspace));

        Types types = Schema.instance.getKeyspaceMetadata(keyspace).types;
        assertThat(types.getNullable(bytes("unrelated")).fieldNames()).hasSize(2);
        assertThat(types.getNullable(bytes("with_duration")).fieldNames()).hasSize(4);
        assertThat(types.getNullable(bytes("in_clustering")).fieldNames()).hasSize(1);
        assertThat(types.getNullable(bytes("in_set")).fieldNames()).hasSize(1);
        assertThat(types.getNullable(bytes("in_regular")).fieldNames()).hasSize(2);
    }

    @Test
    public void testAlterTable() throws Throwable
    {
        String table = createTableFromPeer(KEYSPACE, COLUMNS);
        String name = KEYSPACE + '.' + table;

        execute("ALTER TABLE " + name + " ADD x int");
        execute("ALTER TABLE " + name + " ADD f frozen<set<frozen<tuple<duration, int>>>>");
        execute("ALTER TABLE " + name + " WITH comment = 'altered'");
        execute("ALTER TABLE " + name + " RENAME pk TO id");
        execute("ALTER TABLE " + name + " DROP w");
        execute("ALTER TABLE " + name + " ALTER v MASKED WITH DEFAULT");

        // New columns with a duration where values are sorted
        assertThatThrownBy(() -> execute("ALTER TABLE " + name + " ADD s2 set<frozen<tuple<duration, int>>>"))
        .hasMessageContaining(DURATION_ERROR + " within non-frozen sets");
        assertThatThrownBy(() -> execute("ALTER TABLE " + name + " ADD m map<frozen<list<duration>>, int>"))
        .hasMessageContaining(DURATION_ERROR + " within non-frozen map keys");

        // The data is still there
        execute("INSERT INTO " + name + " (id, ck, s, v, x) VALUES (0, {[1d]}, {(1d, 0)}, 1, 2)");
        assertRows(execute("SELECT id, v, x FROM " + name), row(0, 1, 2));

        TableMetadata metadata = Schema.instance.getTableMetadata(KEYSPACE, table);
        assertThat(metadata.params.comment).isEqualTo("altered");
        assertThat(metadata.getColumn(ColumnIdentifier.getInterned("id", false))).isNotNull();
        assertThat(metadata.getColumn(ColumnIdentifier.getInterned("w", false))).isNull();
        assertThat(metadata.getColumn(ColumnIdentifier.getInterned("v", false)).isMasked()).isTrue();

        // Dropping the column with the duration works, and the dropped column does not stop later changes
        execute("ALTER TABLE " + name + " DROP s");
        execute("ALTER TABLE " + name + " ADD y int");

        // Adding the dropped column back is a new column, so it cannot have the duration again
        assertThatThrownBy(() -> execute("ALTER TABLE " + name + " ADD s set<frozen<tuple<duration, int>>>"))
        .hasMessageContaining(DURATION_ERROR + " within non-frozen sets");
    }

    @Test
    public void testAlterTypeForCleanColumnOfToleratedTable() throws Throwable
    {
        // Tolerance is per column: a table with tolerated columns also has clean ones using 'shared'
        String keyspace = createKeyspace("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
        execute(String.format("CREATE TYPE %s.shared (a int)", keyspace));
        createTableFromPeer(keyspace, "pk int, ck frozen<set<frozen<list<duration>>>>, s set<frozen<tuple<duration, int>>>, " +
                                      "c set<frozen<shared>>, r frozen<shared>, PRIMARY KEY (pk, ck)");

        assertThatThrownBy(() -> execute(String.format("ALTER TYPE %s.shared ADD d duration", keyspace)))
        .hasMessageContaining(DURATION_ERROR + " within non-frozen sets");
        assertThat(Schema.instance.getKeyspaceMetadata(keyspace).types.getNullable(bytes("shared")).fieldNames()).hasSize(1);
    }

    @Test
    public void testAlterNestedType() throws Throwable
    {
        // A duration reaching a sorted position through a UDT nested in another UDT, or in a tuple, is new too
        String keyspace = createKeyspace("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
        execute(String.format("CREATE TYPE %s.inner_ck (a int)", keyspace));
        execute(String.format("CREATE TYPE %s.inner_set (a int)", keyspace));
        execute(String.format("CREATE TYPE %s.inner_map (a int)", keyspace));
        execute(String.format("CREATE TYPE %s.outer_ck (i frozen<inner_ck>)", keyspace));
        execute(String.format("CREATE TYPE %s.outer_set (i frozen<inner_set>)", keyspace));
        createTableFromPeer(keyspace, COLUMNS);
        execute(String.format("CREATE TABLE %s.t_ck (pk int, ck frozen<outer_ck>, PRIMARY KEY (pk, ck))", keyspace));
        execute(String.format("CREATE TABLE %s.t_set (pk int PRIMARY KEY, s set<frozen<outer_set>>)", keyspace));
        execute(String.format("CREATE TABLE %s.t_map (pk int PRIMARY KEY, m map<frozen<tuple<frozen<inner_map>, int>>, int>)",
                              keyspace));

        assertThatThrownBy(() -> execute(String.format("ALTER TYPE %s.inner_ck ADD d duration", keyspace)))
        .hasMessageContaining(DURATION_ERROR + " within PRIMARY KEY columns");
        assertThatThrownBy(() -> execute(String.format("ALTER TYPE %s.inner_set ADD d duration", keyspace)))
        .hasMessageContaining(DURATION_ERROR + " within non-frozen sets");
        assertThatThrownBy(() -> execute(String.format("ALTER TYPE %s.inner_map ADD d frozen<map<int, duration>>", keyspace)))
        .hasMessageContaining(DURATION_ERROR + " within non-frozen map keys");
    }

    @Test
    public void testAlterTypeOfViewClusteringColumn() throws Throwable
    {
        // A UDT that is a clustering column of a view only, not of its base table
        String keyspace = createKeyspace("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
        execute(String.format("CREATE TYPE %s.view_udt (a int)", keyspace));
        createTableFromPeer(keyspace, COLUMNS);
        execute(String.format("CREATE TABLE %s.base (pk int, ck int, u frozen<view_udt>, PRIMARY KEY (pk, ck))", keyspace));
        execute(String.format("CREATE MATERIALIZED VIEW %s.by_u AS SELECT * FROM %s.base " +
                              "WHERE pk IS NOT NULL AND ck IS NOT NULL AND u IS NOT NULL PRIMARY KEY (pk, u, ck)", keyspace, keyspace));

        assertThatThrownBy(() -> execute(String.format("ALTER TYPE %s.view_udt ADD d duration", keyspace)))
        .hasMessageContaining(DURATION_ERROR + " within PRIMARY KEY columns");

        execute(String.format("DROP MATERIALIZED VIEW %s.by_u", keyspace));
    }

    @Test
    public void testCreateIndex() throws Throwable
    {
        String table = createTableFromPeer(KEYSPACE, COLUMNS);
        String name = KEYSPACE + '.' + table;

        execute("CREATE INDEX ON " + name + " (v)");
        execute("CREATE INDEX ON " + name + " (w) USING 'sai'");
        assertThat(Schema.instance.getTableMetadata(KEYSPACE, table).indexes.size()).isEqualTo(2);

        // An index on the column with the duration was already rejected, whatever the duration's position
        assertThatThrownBy(() -> execute("CREATE INDEX ON " + name + " (values(s))"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("Secondary indexes are not supported on collections containing durations");
    }

    @Test
    public void testCreateView() throws Throwable
    {
        String table = createTableFromPeer(KEYSPACE, "pk int, ck frozen<set<frozen<list<duration>>>>, " +
                                                     "s set<frozen<tuple<duration, int>>>, v int, " +
                                                     "f frozen<set<frozen<tuple<duration, int>>>>, PRIMARY KEY (pk, ck)");
        String name = KEYSPACE + '.' + table;

        // The view keeps the columns with a duration where values are sorted where they were
        execute("CREATE MATERIALIZED VIEW " + KEYSPACE + ".by_v AS SELECT * FROM " + name +
                " WHERE pk IS NOT NULL AND ck IS NOT NULL AND v IS NOT NULL PRIMARY KEY (v, pk, ck)");
        execute("CREATE MATERIALIZED VIEW " + KEYSPACE + ".by_ck AS SELECT pk, ck, s FROM " + name +
                " WHERE pk IS NOT NULL AND ck IS NOT NULL PRIMARY KEY (ck, pk)");
        assertThat(Schema.instance.getKeyspaceMetadata(KEYSPACE).views.get("by_v")).isPresent();
        assertThat(Schema.instance.getKeyspaceMetadata(KEYSPACE).views.get("by_ck")).isPresent();

        // A regular frozen column with a duration would be sorted in the view
        assertThatThrownBy(() -> execute("CREATE MATERIALIZED VIEW " + KEYSPACE + ".by_f AS SELECT pk, ck, f FROM " + name +
                                         " WHERE pk IS NOT NULL AND ck IS NOT NULL AND f IS NOT NULL PRIMARY KEY (f, pk, ck)"))
        .hasMessageContaining("duration type is not supported for PRIMARY KEY column 'f'");

        execute("DROP MATERIALIZED VIEW " + KEYSPACE + ".by_v");
        execute("DROP MATERIALIZED VIEW " + KEYSPACE + ".by_ck");
    }

    /**
     * Creates a table through a schema merge, as if received from a peer that has it, so that it can have types that
     * CREATE TABLE rejects.
     */
    private String createTableFromPeer(String keyspace, String columns)
    {
        String table = createTableName();
        KeyspaceMetadata metadata = Schema.instance.getKeyspaceMetadata(keyspace);
        String ddl = String.format("CREATE TABLE %s.%s (%s)", keyspace, table, columns);
        TableMetadata tableMetadata = CreateTableStatement.parse(ddl, keyspace, metadata.types).build();
        ((DefaultSchemaUpdateHandler) Schema.instance.updateHandler)
        .applyMutations(Collections.singletonList(SchemaTestUtil.createTableMutation(metadata, tableMetadata, FBUtilities.timestampMicros())));
        assertThat(Schema.instance.getTableMetadata(keyspace, table)).isNotNull();
        return table;
    }
}
