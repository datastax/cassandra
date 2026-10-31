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

package org.apache.cassandra.index;

import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Index classes in a schema that came from DataStax Enterprise: the 6.8 name of the storage-attached index
 * resolves to ours, and an index type this software does not have loads as a no-op index while the DSE 6
 * compatibility mode is on (instead of preventing the node from starting).
 */
public class Dse6IndexClassesTest extends CQLTester
{
    private static final String SOLR_INDEX = "com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex";
    /** An index class that exists nowhere. */
    private static final String FOREIGN_INDEX = "com.example.MissingIndex";

    private InternodeCompatibilityMode modeBefore;

    @Before
    public void saveMode()
    {
        modeBefore = DatabaseDescriptor.getInternodeCompatibilityMode();
    }

    @After
    public void restoreMode()
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(modeBefore);
    }

    @Test
    public void dse68ClassNameIsAnAliasOfTheStorageAttachedIndex() throws Throwable
    {
        assertThat(IndexMetadata.expandAliases("com.datastax.bdp.index.StorageAttachedIndex")).isEqualTo(StorageAttachedIndex.class.getCanonicalName());
        assertThat(IndexMetadata.expandAliases("COM.datastax.BDP.index.StorageAttachedIndex")).isEqualTo(StorageAttachedIndex.class.getCanonicalName());

        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text)");
        createIndex("CREATE CUSTOM INDEX ON %s (v) USING 'com.datastax.bdp.index.StorageAttachedIndex'");
        execute("INSERT INTO %s (k, v) VALUES (1, 'one')");
        execute("INSERT INTO %s (k, v) VALUES (2, 'two')");
        assertRows(execute("SELECT k FROM %s WHERE v = 'two'"), row(2));
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        assertThat(cfs.indexManager.listIndexes()).anyMatch(index -> index instanceof StorageAttachedIndex);
    }

    @Test
    public void unknownIndexClassLoadsAsNoopOnlyInDse6Mode() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, PRIMARY KEY (a, b))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        IndexMetadata solrLike = IndexMetadata.fromSchemaMetadata("dse_solr_index",
                                                                  IndexMetadata.Kind.CUSTOM,
                                                                  Map.of(IndexTarget.CUSTOM_INDEX_OPTION_NAME, SOLR_INDEX,
                                                                         IndexTarget.TARGET_OPTION_NAME, "c"));

        // without the mode a DSE index class is refused with a text of its own, any other missing class as before
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
        assertThatThrownBy(() -> cfs.indexManager.addIndex(solrLike, true))
        .cause()
        .hasMessage("internode_compatibility_mode NONE refused: the schema still contains the DSE-only index " +
                    KEYSPACE + '.' + currentTable() + ".dse_solr_index (com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex). " +
                    "DSE Search and the other DSE features outside DSE core are not supported by this software, and a cluster " +
                    "using them cannot be upgraded: the index must be dropped on DSE before the upgrade");
        IndexMetadata foreign = IndexMetadata.fromSchemaMetadata("foreign_index",
                                                                 IndexMetadata.Kind.CUSTOM,
                                                                 Map.of(IndexTarget.CUSTOM_INDEX_OPTION_NAME, FOREIGN_INDEX,
                                                                        IndexTarget.TARGET_OPTION_NAME, "c"));
        assertThatThrownBy(() -> cfs.indexManager.addIndex(foreign, true))
        .hasMessageContaining("Unable to find Index class")
        .hasMessageNotContaining("DSE-only");

        // in DSE_6 mode an index type this software does not have loads as a no-op index
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        withLogAppender(SecondaryIndexManager.class, managerLog -> withLogAppender(NoopIndex.class, noopLog -> {
            cfs.indexManager.addIndex(foreign, true);
            // one warning, from the manager; the no-op index does not repeat it (and never logs at ERROR in this mode)
            assertThat(managerLog.list.stream().filter(e -> e.getLevel() == Level.WARN).map(ILoggingEvent::getFormattedMessage))
            .containsExactly("Cannot find index type " + FOREIGN_INDEX + " of index foreign_index; running in " +
                             "internode_compatibility_mode DSE_6, so it is loaded as a no-op index (drop it before switching " +
                             "internode_compatibility_mode back to NONE; a node in NONE mode refuses to load it)");
            assertThat(managerLog.list).noneMatch(e -> e.getLevel() == Level.ERROR);
            assertThat(noopLog.list).isEmpty();
        }));
        Index noop = cfs.indexManager.getIndexByName("foreign_index");
        assertThat(noop).isInstanceOf(NoopIndex.class);
        assertThat(((NoopIndex) noop).getUnsupportedMessage())
        .isEqualTo("Custom index " + KEYSPACE + ".foreign_index of type '" + FOREIGN_INDEX + "' on column c " +
                   "is not supported by this software. The DSE 6 compatibility mode (internode_compatibility_mode: DSE_6) is enabled " +
                   "so using a noop index that will ignore writes and won't be queryable. Please drop this index before switching " +
                   "internode_compatibility_mode back to NONE and/or use a Storage Attached Index (SAI) instead.");
    }

    @Test
    public void indexOnAPseudoColumnLoadsAsNoopInDse6Mode() throws Throwable
    {
        // an index type this software does not have may target something that is not a column of the table, as DSE search
        // indexes target solr_query
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text)");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        IndexMetadata search = IndexMetadata.fromSchemaMetadata("t_solr_query_index",
                                                                IndexMetadata.Kind.CUSTOM,
                                                                Map.of(IndexTarget.CUSTOM_INDEX_OPTION_NAME, FOREIGN_INDEX,
                                                                       IndexTarget.TARGET_OPTION_NAME, "solr_query"));

        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        cfs.indexManager.addIndex(search, true);
        Index noop = cfs.indexManager.getIndexByName("t_solr_query_index");
        assertThat(noop).isInstanceOf(NoopIndex.class);
        assertThat(((NoopIndex) noop).getUnsupportedMessage())
        .startsWith("Custom index " + KEYSPACE + ".t_solr_query_index of type '" + FOREIGN_INDEX + "' on target solr_query " +
                    "is not supported by this software.");
        assertThat(noop.dependsOn(currentTableMetadata().getColumn(ColumnIdentifier.getInterned("v", false)))).isFalse();
        execute("INSERT INTO %s (k, v) VALUES (1, 'one')");
        assertRows(execute("SELECT v FROM %s WHERE k = 1"), row("one"));

        // an index with no target at all loads too
        IndexMetadata noTarget = IndexMetadata.fromSchemaMetadata("dse_no_target_index",
                                                                  IndexMetadata.Kind.CUSTOM,
                                                                  Map.of(IndexTarget.CUSTOM_INDEX_OPTION_NAME, FOREIGN_INDEX));
        cfs.indexManager.addIndex(noTarget, true);
        Index noTargetNoop = cfs.indexManager.getIndexByName("dse_no_target_index");
        assertThat(noTargetNoop).isInstanceOf(NoopIndex.class);
        assertThat(((NoopIndex) noTargetNoop).getUnsupportedMessage())
        .startsWith("Custom index " + KEYSPACE + ".dse_no_target_index of type '" + FOREIGN_INDEX + "' with no target " +
                    "is not supported by this software.");
        assertThat(noTargetNoop.dependsOn(currentTableMetadata().getColumn(ColumnIdentifier.getInterned("v", false)))).isFalse();

        // a target of several comma-separated parts: the index depends on the columns among them, so that none is dropped
        IndexMetadata listed = IndexMetadata.fromSchemaMetadata("dse_listed_index",
                                                                IndexMetadata.Kind.CUSTOM,
                                                                Map.of(IndexTarget.CUSTOM_INDEX_OPTION_NAME, FOREIGN_INDEX,
                                                                       IndexTarget.TARGET_OPTION_NAME, "v, solr_query"));
        cfs.indexManager.addIndex(listed, true);
        Index listedNoop = cfs.indexManager.getIndexByName("dse_listed_index");
        assertThat(listedNoop).isInstanceOf(NoopIndex.class);
        assertThat(((NoopIndex) listedNoop).getUnsupportedMessage())
        .startsWith("Custom index " + KEYSPACE + ".dse_listed_index of type '" + FOREIGN_INDEX + "' on target v, solr_query " +
                    "is not supported by this software.");
        assertThat(listedNoop.dependsOn(currentTableMetadata().getColumn(ColumnIdentifier.getInterned("v", false)))).isTrue();
        assertThat(listedNoop.dependsOn(currentTableMetadata().getColumn(ColumnIdentifier.getInterned("k", false)))).isFalse();
    }

    @Test
    public void ignorePropertyWinsOverDse6Mode() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, PRIMARY KEY (a, b))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        IndexMetadata foreign = IndexMetadata.fromSchemaMetadata("foreign_index",
                                                                 IndexMetadata.Kind.CUSTOM,
                                                                 Map.of(IndexTarget.CUSTOM_INDEX_OPTION_NAME, FOREIGN_INDEX,
                                                                        IndexTarget.TARGET_OPTION_NAME, "c"));

        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        try (WithProperties properties = new WithProperties().set(CassandraRelevantProperties.INDEX_UNKNOWN_IGNORE, true))
        {
            withLogAppender(SecondaryIndexManager.class, managerLog -> withLogAppender(NoopIndex.class, noopLog -> {
                cfs.indexManager.addIndex(foreign, true);
                // as without the DSE 6 mode: two errors that name the property
                assertThat(managerLog.list).noneMatch(e -> e.getFormattedMessage().contains("DSE_6"));
                assertThat(managerLog.list).anyMatch(e -> e.getLevel() == Level.ERROR &&
                                                          e.getFormattedMessage().contains("'" + CassandraRelevantProperties.INDEX_UNKNOWN_IGNORE.getKey() + "' is true so creating noop index foreign_index"));
                assertThat(noopLog.list).hasSize(1);
                assertThat(noopLog.list.get(0).getLevel()).isEqualTo(Level.ERROR);
            }));
            Index noop = cfs.indexManager.getIndexByName("foreign_index");
            assertThat(((NoopIndex) noop).getUnsupportedMessage()).isEqualTo(ignorePropertyMessage("foreign_index", "c"));

            // an index on the solr_query pseudo-column still loads: the lenient target parsing follows the mode alone
            IndexMetadata search = IndexMetadata.fromSchemaMetadata("t_solr_query_index",
                                                                    IndexMetadata.Kind.CUSTOM,
                                                                    Map.of(IndexTarget.CUSTOM_INDEX_OPTION_NAME, FOREIGN_INDEX,
                                                                           IndexTarget.TARGET_OPTION_NAME, "solr_query"));
            cfs.indexManager.addIndex(search, true);
            Index searchNoop = cfs.indexManager.getIndexByName("t_solr_query_index");
            assertThat(((NoopIndex) searchNoop).getUnsupportedMessage())
            .startsWith("Custom index " + KEYSPACE + ".t_solr_query_index of type '" + FOREIGN_INDEX + "' on target solr_query " +
                        "is not supported anymore. " + CassandraRelevantProperties.INDEX_UNKNOWN_IGNORE.getKey() + " is enabled");
        }
    }

    @Test
    public void ignorePropertyWithoutTheModeIsUnchanged() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, PRIMARY KEY (a, b))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        IndexMetadata foreign = IndexMetadata.fromSchemaMetadata("foreign_index",
                                                                 IndexMetadata.Kind.CUSTOM,
                                                                 Map.of(IndexTarget.CUSTOM_INDEX_OPTION_NAME, FOREIGN_INDEX,
                                                                        IndexTarget.TARGET_OPTION_NAME, "c"));
        String message = ignorePropertyMessage("foreign_index", "c");

        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
        try (WithProperties properties = new WithProperties().set(CassandraRelevantProperties.INDEX_UNKNOWN_IGNORE, true))
        {
            withLogAppender(SecondaryIndexManager.class, managerLog -> withLogAppender(NoopIndex.class, noopLog -> {
                cfs.indexManager.addIndex(foreign, true);
                // the ERROR of the manager and the one of the no-op index, with the texts they always had
                assertThat(managerLog.list).noneMatch(e -> e.getFormattedMessage().contains("DSE_6"));
                assertThat(managerLog.list).anyMatch(e -> e.getLevel() == Level.ERROR &&
                                                          e.getFormattedMessage().equals("Cannot find index type " + FOREIGN_INDEX + ", but '" +
                                                                                         CassandraRelevantProperties.INDEX_UNKNOWN_IGNORE.getKey() +
                                                                                         "' is true so creating noop index foreign_index"));
                assertThat(noopLog.list).hasSize(1);
                assertThat(noopLog.list.get(0).getLevel()).isEqualTo(Level.ERROR);
                assertThat(noopLog.list.get(0).getFormattedMessage()).isEqualTo(message);
            }));
            Index noop = cfs.indexManager.getIndexByName("foreign_index");
            assertThat(((NoopIndex) noop).getUnsupportedMessage()).isEqualTo(message);
            assertThat(noop.dependsOn(currentTableMetadata().getColumn(ColumnIdentifier.getInterned("c", false)))).isTrue();
        }
    }

    /** The message of a no-op index of the class FOREIGN_INDEX loaded because of the ignore property, on a column of the current table. */
    private static String ignorePropertyMessage(String index, String column)
    {
        return "Custom index " + KEYSPACE + '.' + index + " of type '" + FOREIGN_INDEX + "' on column " + column + " is not supported anymore. " +
               CassandraRelevantProperties.INDEX_UNKNOWN_IGNORE.getKey() + " is enabled so using an noop index that will ignore writes and won't be " +
               "queryable. Please drop this index and/or use a Storage Attached Index (SAI) instead.";
    }

    @Test
    public void tableWithUnknownIndexClassCanBeAlteredInDse6Mode() throws Throwable
    {
        // the schema freeze of DSE_6 is lifted by its property here: the point is what the no-op index allows
        try (WithProperties allowDdl = new WithProperties().set(CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE_ALLOW_DDL, true))
        {
            alterTableWithUnknownIndexClass();
        }
    }

    private void alterTableWithUnknownIndexClass() throws Throwable
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text)");
        // the schema as loaded from a DSE node: the table carries indexes of a type this software does not have, loaded as no-op indexes
        TableMetadata current = currentTableMetadata();
        IndexMetadata unknown = IndexMetadata.fromSchemaMetadata("foreign_index",
                                                              IndexMetadata.Kind.CUSTOM,
                                                              Map.of(IndexTarget.CUSTOM_INDEX_OPTION_NAME, FOREIGN_INDEX,
                                                                     IndexTarget.TARGET_OPTION_NAME, "v"));
        IndexMetadata search = IndexMetadata.fromSchemaMetadata("t_solr_query_index",
                                                                IndexMetadata.Kind.CUSTOM,
                                                                Map.of(IndexTarget.CUSTOM_INDEX_OPTION_NAME, FOREIGN_INDEX,
                                                                       IndexTarget.TARGET_OPTION_NAME, "solr_query"));
        TableMetadata withUnknown = current.unbuild().indexes(current.indexes.with(unknown).with(search)).build();
        Schema.instance.transform(schema -> {
            KeyspaceMetadata ksm = schema.getNullable(KEYSPACE);
            return schema.withAddedOrUpdated(ksm.withSwapped(ksm.tables.withSwapped(withUnknown)));
        });
        assertThat(getCurrentColumnFamilyStore().indexManager.getIndexByName("foreign_index")).isInstanceOf(NoopIndex.class);
        assertThat(getCurrentColumnFamilyStore().indexManager.getIndexByName("t_solr_query_index")).isInstanceOf(NoopIndex.class);

        // NONE mode, as before: the table cannot be altered while the unknown index is in its schema
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
        assertThatThrownBy(() -> execute("ALTER TABLE %s ADD d int"))
        .hasMessageContaining("Unable to find custom indexer class '" + FOREIGN_INDEX + "'");

        // with the ignore property the index validates as it loads (as a no-op index)
        try (WithProperties properties = new WithProperties().set(CassandraRelevantProperties.INDEX_UNKNOWN_IGNORE, true))
        {
            execute("ALTER TABLE %s ADD d int");
        }

        // DSE_6 mode: altering the table works, the index stays a no-op index
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        execute("ALTER TABLE %s ADD e int");
        execute("INSERT INTO %s (k, v, d, e) VALUES (1, 'one', 2, 3)");
        assertRows(execute("SELECT k, v, d, e FROM %s"), row(1, "one", 2, 3));
        assertThat(currentTableMetadata().indexes.get("foreign_index")).isPresent();
        assertThat(getCurrentColumnFamilyStore().indexManager.getIndexByName("foreign_index")).isInstanceOf(NoopIndex.class);
        assertThat(getCurrentColumnFamilyStore().indexManager.getIndexByName("t_solr_query_index")).isInstanceOf(NoopIndex.class);
        // the index on the pseudo-column depends on no column, so it does not block dropping one
        execute("ALTER TABLE %s DROP e");

        // but a new index must still name a class that exists
        assertThatThrownBy(() -> execute("CREATE CUSTOM INDEX ON %s (d) USING 'com.example.NoSuchIndex'"))
        .hasMessageContaining("Unable to find custom indexer class 'com.example.NoSuchIndex'");
        assertThat(currentTableMetadata().indexes.size()).isEqualTo(2);

        // and dropping them works
        execute("DROP INDEX " + KEYSPACE + ".foreign_index");
        execute("DROP INDEX " + KEYSPACE + ".t_solr_query_index");
        assertThat(currentTableMetadata().indexes.isEmpty()).isTrue();
    }
}
