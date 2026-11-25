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

import java.util.ArrayList;
import java.util.List;

/**
 * The replicated system keyspaces ({@code system_auth}, {@code system_distributed}, {@code system_traces}) in the DSE 6
 * compatibility mode: they keep the definitions DSE 6 wrote, and this software's additions to them (the tables and
 * columns DSE 6 does not have, the table options this software sets) are deferred to the first start with
 * {@code internode_compatibility_mode: NONE}, when {@code StorageService.setUpDistributedSystemKeyspaces} makes them as
 * it always does (schema mutations at fixed timestamps, so every node makes the same ones): the missing tables and
 * columns are added, while a table option this software sets takes effect only where its fixed timestamp is newer than
 * the one DSE wrote the option with.
 * <p>
 * Why: DSE 6 pulls the schema of a peer whose schema version differs from its own, and merges what it receives
 * (observed with DSE 6.8.65: after a run in the mode, DSE's schema held {@code system_auth.network_permissions} and
 * {@code system_distributed.partition_denylist}, pulled from the upgraded node). The schema is frozen during the
 * upgrade, and a table DSE does not know stays in DSE's schema on a rollback. The schema exchange with DSE is closed as
 * well ({@code MigrationCoordinator.exchangesSchemaWith}): a DSE node's pull is answered with no schema and its pushes
 * are ignored.
 * <p>
 * What this software does in the mode where it would use a deferred table or column:
 * <ul>
 * <li>{@code system_auth.network_permissions}: {@code CassandraNetworkAuthorizer} is refused at startup
 *     ({@code AuthConfig});</li>
 * <li>not deferred but missing as well: {@code system_auth.cidr_permissions}, {@code cidr_groups} and
 *     {@code identity_to_role} are created only with {@code storage_compatibility_mode} UPGRADING or NONE, so not at
 *     the first start with {@code internode_compatibility_mode: NONE} either while the storage mode stays
 *     {@code HCD_1}; the CIDR authorizer and the mutual TLS authenticators are refused at startup
 *     ({@code AuthConfig});</li>
 * <li>{@code system_auth.resource_role_permissons_index}: {@code CassandraAuthorizer} maintains it only when the loaded
 *     {@code system_auth} has it;</li>
 * <li>{@code system_distributed.partition_denylist}: the partition denylist is off, its operations refused with a
 *     message built by {@link #deferredTable(String)} ({@code PartitionDenylist});</li>
 * <li>{@code system_traces.sessions.coordinator_port}, {@code system_traces.events.source_port}: trace rows leave them
 *     out ({@code TraceKeyspace});</li>
 * <li>{@code system_distributed.repair_history.coordinator_port} and {@code participants_v2}: written by repair only,
 *     which is refused during the upgrade ({@code DseUpgradeGuards}).</li>
 * </ul>
 */
public final class DseSystemKeyspaces
{
    /** Why a table of this software is missing in the mode, and when it is created. */
    public static final String DEFERRED = "the replicated system keyspaces keep DSE 6's definitions during the upgrade, and this software creates " +
                                          "its own tables in them at the first start with internode_compatibility_mode NONE";

    private DseSystemKeyspaces()
    {
    }

    /**
     * The text used by every feature that is off or refused in the mode for lack of one of the deferred tables.
     *
     * @param table the table, as {@code keyspace.table}
     */
    public static String deferredTable(String table)
    {
        return "table " + table + " is missing in internode_compatibility_mode DSE_6 (DSE 6 does not have it): " + DEFERRED;
    }

    /**
     * What {@code SchemaTransformations.updateSystemKeyspace} would add to the keyspace as it is loaded: the tables of the
     * desired definition it does not have ({@code ks.table}) and, for the tables it has, the columns it lacks
     * ({@code ks.table.column}). Table options are not listed: the update also sets them, and that is deferred the same
     * way.
     *
     * @param desired this software's definition of the keyspace
     * @param loaded the keyspace as loaded from disk (DSE's definition on an upgraded node)
     */
    public static List<String> deferredAdditions(KeyspaceMetadata desired, KeyspaceMetadata loaded)
    {
        List<String> additions = new ArrayList<>();
        for (TableMetadata table : desired.tables)
        {
            TableMetadata existing = loaded.tables.getNullable(table.name);
            if (existing == null)
            {
                additions.add(desired.name + '.' + table.name);
                continue;
            }
            for (ColumnMetadata column : table.columns())
                if (existing.getColumn(column.name) == null)
                    additions.add(desired.name + '.' + table.name + '.' + column.name);
        }
        return additions;
    }
}
