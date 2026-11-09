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

package org.apache.cassandra.service;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import ch.qos.logback.classic.spi.ILoggingEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Tables;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link StorageService#setUpSystemKeyspace} on a keyspace of its own (the replicated system keyspaces exist in every
 * unit test): in the DSE 6 compatibility mode a keyspace that does not exist is created as without the mode, one that
 * exists keeps the definition it was loaded with and the additions are logged; the first call with {@code NONE} makes
 * them.
 */
public class StorageServiceDse6SystemKeyspacesTest extends CQLTester
{
    /** A name of its own per run: the deferred additions of a keyspace are logged once per JVM. */
    private static final AtomicInteger RUNS = new AtomicInteger();

    private String keyspace;

    private InternodeCompatibilityMode modeBefore;

    @Before
    public void saveMode()
    {
        modeBefore = DatabaseDescriptor.getInternodeCompatibilityMode();
        keyspace = "dse6_replicated_ks_" + RUNS.incrementAndGet();
    }

    @After
    public void restoreModeAndDropKeyspace() throws Throwable
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
        schemaChange("DROP KEYSPACE IF EXISTS " + keyspace);
        DatabaseDescriptor.setInternodeCompatibilityMode(modeBefore);
    }

    @Test
    public void createdInDse6ModeThenKeptThenCompletedWithNone() throws Throwable
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        StorageService.setUpSystemKeyspace(keyspaceOf(table("a").build()), 1);
        assertThat(tableNames()).containsExactly("a");

        // the definition of a later release: a new table and a new column
        KeyspaceMetadata later = keyspaceOf(table("a").addRegularColumn("extra", UTF8Type.instance).build(), table("b").build());
        withLogAppender(StorageService.class, appender -> {
            StorageService.setUpSystemKeyspace(later, 2);
            List<String> infos = appender.list.stream().map(ILoggingEvent::getFormattedMessage).collect(Collectors.toList());
            assertThat(infos).contains("internode_compatibility_mode DSE_6: " + keyspace + " keeps the definition it was loaded with " +
                                       "(DSE's on an upgraded node); this software's additions to it are made at the first start with " +
                                       "internode_compatibility_mode NONE: [" + keyspace + ".a.extra, " + keyspace + ".b]");
        });
        assertThat(tableNames()).containsExactly("a");
        assertThat(Schema.instance.getTableMetadata(keyspace, "a").getColumn(UTF8Type.instance.decompose("extra"))).isNull();

        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
        StorageService.setUpSystemKeyspace(later, 2);
        assertThat(tableNames()).containsExactlyInAnyOrder("a", "b");
        assertThat(Schema.instance.getTableMetadata(keyspace, "a").getColumn(UTF8Type.instance.decompose("extra"))).isNotNull();
    }

    private List<String> tableNames()
    {
        return Schema.instance.getKeyspaceMetadata(keyspace).tables.stream().map(t -> t.name).collect(Collectors.toList());
    }

    private TableMetadata.Builder table(String name)
    {
        return TableMetadata.builder(keyspace, name)
                            .addPartitionKeyColumn("pk", Int32Type.instance)
                            .addRegularColumn("v", Int32Type.instance);
    }

    private KeyspaceMetadata keyspaceOf(TableMetadata... tables)
    {
        return KeyspaceMetadata.create(keyspace, KeyspaceParams.simple(1), Tables.of(tables));
    }
}
