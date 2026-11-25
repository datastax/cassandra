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

package org.apache.cassandra.schema;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link DseSystemKeyspaces#deferredAdditions}: the tables and columns of this software's definition that a loaded
 * keyspace lacks, in the order of the desired definition; table options and the loaded keyspace's own tables are not
 * compared. And the text of {@link DseSystemKeyspaces#deferredTable}.
 */
public class DseSystemKeyspacesTest
{
    private static final String KEYSPACE = "dse6_system_ks";

    @BeforeClass
    public static void setUpClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void nothingIsDeferredForTheSameDefinition()
    {
        KeyspaceMetadata desired = keyspace(table("a").build(), table("b").addRegularColumn("extra", UTF8Type.instance).build());
        assertThat(DseSystemKeyspaces.deferredAdditions(desired, desired)).isEmpty();
    }

    @Test
    public void missingTablesAndColumnsAreListed()
    {
        KeyspaceMetadata desired = keyspace(table("a").build(),
                                            table("b").addRegularColumn("extra", UTF8Type.instance).build(),
                                            table("c").build());
        KeyspaceMetadata loaded = keyspace(table("b").build());
        assertThat(DseSystemKeyspaces.deferredAdditions(desired, loaded))
        .containsExactlyInAnyOrder(KEYSPACE + ".a", KEYSPACE + ".b.extra", KEYSPACE + ".c");
    }

    @Test
    public void tableOptionsAndTablesOfTheLoadedDefinitionOnlyAreIgnored()
    {
        KeyspaceMetadata desired = keyspace(table("a").build());
        KeyspaceMetadata loaded = keyspace(table("a").comment("written by DSE").gcGraceSeconds(1).build(),
                                           table("dse_only").build());
        assertThat(DseSystemKeyspaces.deferredAdditions(desired, loaded)).isEmpty();
    }

    @Test
    public void theRefusalTextNamesTheTable()
    {
        assertThat(DseSystemKeyspaces.deferredTable("system_distributed.partition_denylist"))
        .isEqualTo("table system_distributed.partition_denylist is missing in internode_compatibility_mode DSE_6 (DSE 6 does not " +
                   "have it): the replicated system keyspaces keep DSE 6's definitions during the upgrade, and this software " +
                   "creates its own tables in them at the first start with internode_compatibility_mode NONE");
    }

    private static TableMetadata.Builder table(String name)
    {
        return TableMetadata.builder(KEYSPACE, name)
                            .addPartitionKeyColumn("pk", Int32Type.instance)
                            .addRegularColumn("v", Int32Type.instance);
    }

    private static KeyspaceMetadata keyspace(TableMetadata... tables)
    {
        return KeyspaceMetadata.create(KEYSPACE, KeyspaceParams.simple(1), Tables.of(tables));
    }
}
