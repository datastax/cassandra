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

package org.apache.cassandra.auth;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.SchemaTestUtil;
import org.apache.cassandra.schema.TableMetadata;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link CassandraAuthorizer} and the reverse index {@code system_auth.resource_role_permissons_index} in the DSE 6
 * compatibility mode: the index is maintained when {@code system_auth} has it (as without the mode); when it has none
 * (DSE 6.9.26's definition lacks it), GRANT and REVOKE update the permissions only and the roles holding permissions
 * on a dropped resource are found in the permissions themselves.
 * <p>
 * The test without the index removes the table from this JVM's schema, as DSE 6.9.26's definition lacks it, and adds it
 * back.
 * <p>
 * In the mode, the permissions DSE 6 does not have ({@code UNMASK}, {@code SELECT_MASKED}) are not written on data
 * resources: {@code GRANT ALL} and the grant to the creator of a table grant the others with a warning, a
 * {@code GRANT} naming them is refused.
 */
public class CassandraAuthorizerDse6Test extends CQLTester
{
    private static WithProperties properties;

    private InternodeCompatibilityMode modeBefore;

    @BeforeClass
    public static void setupClass()
    {
        properties = new WithProperties().set(CassandraRelevantProperties.ORG_APACHE_CASSANDRA_DISABLE_MBEAN_REGISTRATION, true);
        CQLTester.setUpClass();
        requireAuthentication();
        requireNetwork();
    }

    @AfterClass
    public static void resetProperties()
    {
        properties.close();
    }

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
    public void indexMaintainedInDse6ModeWhenDseHasIt() throws Throwable
    {
        checkIndexMaintained(InternodeCompatibilityMode.DSE_6, "dse6_indexed");
    }

    @Test
    public void indexMaintainedWithNone() throws Throwable
    {
        checkIndexMaintained(InternodeCompatibilityMode.NONE, "none_indexed");
    }

    @Test
    public void permissionsOnlyInDse6ModeWhenDseHasNoIndex() throws Throwable
    {
        useSuperUser();
        // created before the mode is set
        String table = createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        String resource = "data/" + KEYSPACE + '/' + table;
        String role = "dse6_unindexed";
        executeNet(format("CREATE ROLE %s WITH login = true AND password = 'secret'", role));
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        // system_auth as DSE 6.9.26 defines it: no reverse index (restored at the end for the other tests)
        TableMetadata index = Schema.instance.getTableMetadata(SchemaConstants.AUTH_KEYSPACE_NAME, AuthKeyspace.RESOURCE_ROLE_INDEX);
        SchemaTestUtil.announceTableDrop(SchemaConstants.AUTH_KEYSPACE_NAME, AuthKeyspace.RESOURCE_ROLE_INDEX);
        try
        {
            assertThat(Schema.instance.getTableMetadata(SchemaConstants.AUTH_KEYSPACE_NAME, AuthKeyspace.RESOURCE_ROLE_INDEX)).isNull();

            executeNet(format("GRANT SELECT ON %s.%s TO %s", KEYSPACE, table, role));
            assertThat(permissions(role, resource)).containsExactly("SELECT");
            executeNet(format("REVOKE SELECT ON %s.%s FROM %s", KEYSPACE, table, role));
            assertThat(permissions(role, resource)).isEmpty();

            // GRANT MODIFY is written in DSE's form; REVOKE MODIFY removes DSE's names, alone too, with no index to update
            executeNet(format("GRANT MODIFY ON %s.%s TO %s", KEYSPACE, table, role));
            assertThat(permissions(role, resource)).containsExactlyInAnyOrder("UPDATE", "TRUNCATE");
            executeNet(format("REVOKE MODIFY ON %s.%s FROM %s", KEYSPACE, table, role));
            assertThat(permissions(role, resource)).isEmpty();
            execute(format("INSERT INTO system_auth.role_permissions (role, resource, permissions) VALUES ('%s', '%s', {'TRUNCATE'})", role, resource));
            executeNet(format("REVOKE MODIFY ON %s.%s FROM %s", KEYSPACE, table, role));
            assertThat(permissions(role, resource)).isEmpty();

            // the hook of a dropped resource finds the role through the permissions and deletes them
            executeNet(format("GRANT MODIFY ON %s.%s TO %s", KEYSPACE, table, role));
            assertThat(DatabaseDescriptor.getAuthorizer().revokeAllOn(DataResource.table(KEYSPACE, table))).containsExactly(RoleResource.role(role));
            assertThat(execute(format("SELECT * FROM system_auth.role_permissions WHERE role = '%s'", role))).isEmpty();

            // the hook of a dropped role deletes its permissions
            executeNet(format("GRANT SELECT ON %s.%s TO %s", KEYSPACE, table, role));
            executeNet(format("DROP ROLE %s", role));
            assertThat(execute(format("SELECT * FROM system_auth.role_permissions WHERE role = '%s'", role))).isEmpty();
        }
        finally
        {
            SchemaTestUtil.announceNewTable(index);
        }
        assertThat(Schema.instance.getTableMetadata(SchemaConstants.AUTH_KEYSPACE_NAME, AuthKeyspace.RESOURCE_ROLE_INDEX)).isNotNull();
    }

    @Test
    public void grantAllInDse6ModeWritesOnlyWhatDseHas() throws Throwable
    {
        useSuperUser();
        String table = createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        String resource = "data/" + KEYSPACE + '/' + table;
        String role = "dse6_grant_all";
        executeNet(format("CREATE ROLE %s WITH login = true AND password = 'secret'", role));
        String notGranted = notGranted(role, resource);
        try
        {
            // without the mode, as before
            ResultSet none = executeNet(format("GRANT ALL ON %s.%s TO %s", KEYSPACE, table, role));
            assertThat(none.getExecutionInfo().getWarnings()).isEmpty();
            assertThat(permissions(role, resource)).containsExactlyInAnyOrder("ALTER", "DROP", "SELECT", "MODIFY", "AUTHORIZE", "UNMASK", "SELECT_MASKED");
            executeNet(format("REVOKE ALL ON %s.%s FROM %s", KEYSPACE, table, role));
            assertThat(permissions(role, resource)).isEmpty();
            // and naming them is accepted
            assertThat(executeNet(format("GRANT UNMASK, SELECT_MASKED ON %s.%s TO %s", KEYSPACE, table, role)).getExecutionInfo().getWarnings()).isEmpty();
            assertThat(permissions(role, resource)).containsExactlyInAnyOrder("UNMASK", "SELECT_MASKED");
            executeNet(format("REVOKE ALL ON %s.%s FROM %s", KEYSPACE, table, role));

            DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
            // MODIFY in DSE's form, and neither UNMASK nor SELECT_MASKED, with a warning
            ResultSet all = executeNet(format("GRANT ALL ON %s.%s TO %s", KEYSPACE, table, role));
            assertThat(all.getExecutionInfo().getWarnings()).containsExactly(notGranted);
            assertThat(permissions(role, resource)).containsExactlyInAnyOrder("ALTER", "DROP", "SELECT", "UPDATE", "TRUNCATE", "AUTHORIZE");

            // naming every permission of the table is GRANT ALL
            executeNet(format("REVOKE ALL ON %s.%s FROM %s", KEYSPACE, table, role));
            assertThat(executeNet(format("GRANT ALTER, DROP, SELECT, MODIFY, AUTHORIZE, UNMASK, SELECT_MASKED ON %s.%s TO %s", KEYSPACE, table, role))
                       .getExecutionInfo().getWarnings())
            .containsExactly(notGranted);
            assertThat(permissions(role, resource)).containsExactlyInAnyOrder("ALTER", "DROP", "SELECT", "UPDATE", "TRUNCATE", "AUTHORIZE");

            // naming them is refused, and nothing is written
            assertThatThrownBy(() -> executeNet(format("GRANT UNMASK ON %s.%s TO %s", KEYSPACE, table, role)))
            .isInstanceOf(InvalidQueryException.class)
            .hasMessage("UNMASK cannot be granted with internode_compatibility_mode DSE_6: DataStax Enterprise, which reads the same " +
                        "system_auth.role_permissions, does not have it. Grant it once the upgrade is complete (internode_compatibility_mode " +
                        "NONE on every node)");
            executeNet(format("REVOKE ALL ON %s.%s FROM %s", KEYSPACE, table, role));
            assertThatThrownBy(() -> executeNet(format("GRANT SELECT, SELECT_MASKED, UNMASK ON %s.%s TO %s", KEYSPACE, table, role)))
            .isInstanceOf(InvalidQueryException.class)
            .hasMessageStartingWith("UNMASK and SELECT_MASKED cannot be granted with internode_compatibility_mode DSE_6: DataStax Enterprise, " +
                                    "which reads the same system_auth.role_permissions, does not have them. Grant them once");
            assertThat(permissions(role, resource)).isEmpty();

            // a role that holds them already, granted before the mode, is told so as without the mode; REVOKE ALL removes them
            execute(format("INSERT INTO system_auth.role_permissions (role, resource, permissions) VALUES ('%s', '%s', {'UNMASK', 'SELECT_MASKED'})", role, resource));
            assertThat(executeNet(format("GRANT ALL ON %s.%s TO %s", KEYSPACE, table, role)).getExecutionInfo().getWarnings())
            .containsExactly("Role '" + role + "' was already granted UNMASK, SELECT_MASKED on <table " + KEYSPACE + '.' + table + '>');
            assertThat(permissions(role, resource)).containsExactlyInAnyOrder("ALTER", "DROP", "SELECT", "UPDATE", "TRUNCATE", "AUTHORIZE",
                                                                              "UNMASK", "SELECT_MASKED");
            executeNet(format("REVOKE ALL ON %s.%s FROM %s", KEYSPACE, table, role));
            assertThat(permissions(role, resource)).isEmpty();

            // on a keyspace and on all keyspaces too
            assertThat(executeNet(format("GRANT ALL ON KEYSPACE %s TO %s", KEYSPACE, role)).getExecutionInfo().getWarnings())
            .containsExactly(notGranted(role, "data/" + KEYSPACE));
            assertThat(permissions(role, "data/" + KEYSPACE)).containsExactlyInAnyOrder("CREATE", "ALTER", "DROP", "SELECT", "UPDATE", "TRUNCATE", "AUTHORIZE");
            assertThat(executeNet(format("GRANT ALL ON ALL KEYSPACES TO %s", role)).getExecutionInfo().getWarnings())
            .containsExactly(notGranted(role, "data"));
            assertThat(permissions(role, "data")).containsExactlyInAnyOrder("CREATE", "ALTER", "DROP", "SELECT", "UPDATE", "TRUNCATE", "AUTHORIZE");
            executeNet(format("REVOKE ALL ON KEYSPACE %s FROM %s", KEYSPACE, role));
            executeNet(format("REVOKE ALL ON ALL KEYSPACES FROM %s", role));
            assertThat(permissions(role, "data/" + KEYSPACE)).isEmpty();
            assertThat(permissions(role, "data")).isEmpty();

            // the permissions granted to the creator of a keyspace and of a table (with the property reserved for schema
            // changes in the mode set, as the tests of the mode do)
            try (WithProperties allowDdl = new WithProperties().set(CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE_ALLOW_DDL, true))
            {
                String createdKeyspace = "data/created_ks_in_dse6";
                assertThat(executeNet("CREATE KEYSPACE created_ks_in_dse6 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
                           .getExecutionInfo().getWarnings())
                .containsExactly(notGranted("cassandra", createdKeyspace));
                assertThat(permissions("cassandra", createdKeyspace)).containsExactlyInAnyOrder("CREATE", "ALTER", "DROP", "SELECT", "UPDATE", "TRUNCATE", "AUTHORIZE");
                String createdTable = "data/created_ks_in_dse6/created_in_dse6";
                assertThat(executeNet("CREATE TABLE created_ks_in_dse6.created_in_dse6 (k int PRIMARY KEY)").getExecutionInfo().getWarnings())
                .containsExactly(notGranted("cassandra", createdTable));
                assertThat(permissions("cassandra", createdTable)).containsExactlyInAnyOrder("ALTER", "DROP", "SELECT", "UPDATE", "TRUNCATE", "AUTHORIZE");
                executeNet("DROP KEYSPACE created_ks_in_dse6");
                assertThat(permissions("cassandra", createdTable)).isEmpty();
                assertThat(permissions("cassandra", createdKeyspace)).isEmpty();
            }

            // on other resources nothing changes: none of their permissions is missing on DSE
            executeNet(format("GRANT ALL ON ROLE %s TO %s", role, role));
            assertThat(permissions(role, "roles/" + role)).containsExactlyInAnyOrder("ALTER", "DROP", "AUTHORIZE", "DESCRIBE");
        }
        finally
        {
            DatabaseDescriptor.setInternodeCompatibilityMode(modeBefore);
            executeNet(format("DROP ROLE %s", role));
        }
    }

    /** The warning of a grant in the mode that leaves out UNMASK and SELECT_MASKED. */
    private static String notGranted(String role, String resource)
    {
        return "UNMASK and SELECT_MASKED not granted to role '" + role + "' on " + resource + ": with internode_compatibility_mode DSE_6 " +
               "only the permissions DataStax Enterprise also has are written. Grant them once the upgrade is complete " +
               "(internode_compatibility_mode NONE on every node)";
    }

    /** GRANT, REVOKE, the hook of a dropped resource and DROP ROLE keep the index up to date in the given mode. */
    private void checkIndexMaintained(InternodeCompatibilityMode mode, String role) throws Throwable
    {
        useSuperUser();
        String table = createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        String resource = "data/" + KEYSPACE + '/' + table;
        executeNet(format("CREATE ROLE %s WITH login = true AND password = 'secret'", role));
        DatabaseDescriptor.setInternodeCompatibilityMode(mode);

        executeNet(format("GRANT SELECT ON %s.%s TO %s", KEYSPACE, table, role));
        assertThat(indexedRoles(resource)).containsExactly(role);
        assertThat(DatabaseDescriptor.getAuthorizer().revokeAllOn(DataResource.table(KEYSPACE, table))).containsExactly(RoleResource.role(role));
        assertThat(indexedRoles(resource)).isEmpty();
        assertThat(execute(format("SELECT * FROM system_auth.role_permissions WHERE role = '%s'", role))).isEmpty();

        // a REVOKE MODIFY that removes DSE's TRUNCATE alone, not honoured here, removes the entry once nothing is left
        execute(format("INSERT INTO system_auth.role_permissions (role, resource, permissions) VALUES ('%s', '%s', {'TRUNCATE'})", role, resource));
        execute(format("INSERT INTO system_auth.%s (resource, role) VALUES ('%s', '%s')", AuthKeyspace.RESOURCE_ROLE_INDEX, resource, role));
        executeNet(format("REVOKE MODIFY ON %s.%s FROM %s", KEYSPACE, table, role));
        assertThat(permissions(role, resource)).isEmpty();
        assertThat(indexedRoles(resource)).isEmpty();

        executeNet(format("GRANT SELECT ON %s.%s TO %s", KEYSPACE, table, role));
        executeNet(format("DROP ROLE %s", role));
        assertThat(indexedRoles(resource)).isEmpty();
    }

    /** The permissions of the role on the resource, as stored. */
    private Set<String> permissions(String role, String resource) throws Throwable
    {
        UntypedResultSet rows = execute(format("SELECT permissions FROM system_auth.role_permissions WHERE role = '%s' AND resource = '%s'", role, resource));
        if (rows.isEmpty() || !rows.one().has("permissions"))
            return Set.of();
        return rows.one().getSet("permissions", UTF8Type.instance);
    }

    /** The roles the reverse index lists for the resource. */
    private List<String> indexedRoles(String resource) throws Throwable
    {
        List<String> roles = new ArrayList<>();
        for (UntypedResultSet.Row row : execute(format("SELECT role FROM system_auth.%s WHERE resource = '%s'", AuthKeyspace.RESOURCE_ROLE_INDEX, resource)))
            roles.add(row.getString("role"));
        return roles;
    }
}
