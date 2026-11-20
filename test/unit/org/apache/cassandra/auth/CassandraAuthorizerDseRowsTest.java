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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.UnauthorizedException;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.dsecompat.DseFixtures;
import org.apache.cassandra.io.util.PathUtils;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.Pair;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@code system_auth.role_permissions} rows as DataStax Enterprise leaves them after an in-place upgrade:
 * permission names and resource types this software does not have, and names of known types it cannot parse.
 * They must be ignored, not break the authorization of the role, the listing of permissions, the warm-up of the
 * permissions cache or the removal of the role.
 * <p>
 * The unknown names are logged once per JVM, so the tests that assert their logging use names of their own, and each
 * asserts the logging of its own names only.
 * <p>
 * DSE writes {@code UPDATE} + {@code TRUNCATE} for {@code GRANT ALL} and a creator's permissions (its
 * {@code GRANT MODIFY} writes the name {@code MODIFY}): the pair is read as {@code MODIFY}, one of them alone is
 * dropped and listed; this software's GRANT MODIFY writes DSE's two names in the DSE 6 compatibility mode, and its
 * REVOKE MODIFY removes them (DSE's own REVOKE MODIFY leaves them, see {@link CassandraAuthorizer#grant}).
 */
public class CassandraAuthorizerDseRowsTest extends CQLTester
{
    private static final String PASSWORD = "secret";

    private static WithProperties properties;

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

    // the lone grants, the source of their listing and the one-off message are static: each test starts afresh
    @Before
    @After
    public void resetLoneGrants()
    {
        CassandraAuthorizer.resetLoneDseWriteGrants();
    }

    @Test
    public void dseGrantsAreIgnoredAndStandardGrantsStillWork() throws Throwable
    {
        String role = "dse_app";
        useSuperUser();
        String table = createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        executeNet(format("CREATE ROLE %s WITH login = true AND password = '%s'", role, PASSWORD));
        executeNet(format("GRANT SELECT ON %s.%s TO %s", KEYSPACE, table, role));

        // What DSE writes: its own permission names mixed into a standard data row...
        String dataResource = "data/" + KEYSPACE + '/' + table;
        // (UPDATE and TRUNCATE, which DSE writes together for GRANT ALL and a creator's permissions, have their own tests
        // below: together they read as MODIFY)
        execute(format("UPDATE system_auth.role_permissions SET permissions = permissions + {'PROXY.EXECUTE', 'READ', 'WRITE', 'BACKUP'} " +
                       "WHERE role = '%s' AND resource = '%s'", role, dataResource));
        // ...its own resource types...
        String rowsResource = "rows/top_secret/data/" + KEYSPACE + '/' + table;
        String searchResource = "search/" + KEYSPACE + '.' + table;
        insertGrant(role, rowsResource, "'SELECT', 'MODIFY'");
        insertGrant(role, searchResource, "'SEARCH.CREATE', 'SEARCH.ALTER', 'SEARCH.DROP', 'SEARCH.RELOAD', 'SEARCH.REBUILD', 'SEARCH.COMMIT'");
        insertGrant(role, "rpc/DseClientTool/getData", "'EXECUTE'");
        insertGrant(role, "authentication_schemes/internal", "'EXECUTE'");
        // ...and names of known types that this software cannot parse
        String functionNoArgs = "functions/" + KEYSPACE + "/dse_function";
        String functionUnknownType = "functions/" + KEYSPACE + "/dse_function[org.apache.cassandra.db.marshal.NoSuchDseType]";
        insertGrant(role, functionNoArgs, "'EXECUTE'");
        insertGrant(role, functionUnknownType, "'EXECUTE'");

        IAuthorizer authorizer = DatabaseDescriptor.getAuthorizer();
        assertThat(authorizer).isInstanceOf(CassandraAuthorizer.class);
        withLogAppender(CassandraAuthorizer.class, appender -> {
            // read everything twice: each unknown name is logged once, with the role and the resource of the first row found
            for (int i = 0; i < 2; i++)
            {
                // the listing shows only what exists here
                assertOnlySelectOnTable(executeNet(format("LIST ALL PERMISSIONS OF %s", role)), role, table);

                // the cache warm-up reads every row and must not throw
                Map<Pair<AuthenticatedUser, IResource>, Set<Permission>> warm = ((CassandraAuthorizer) authorizer).bulkLoader().get();
                assertThat(warm.get(Pair.create(new AuthenticatedUser(role), DataResource.table(KEYSPACE, table)))).containsExactly(Permission.SELECT);
            }

            List<ILoggingEvent> events = appender.list;
            for (String permission : new String[]{ "PROXY.EXECUTE", "READ", "WRITE", "BACKUP" })
                assertLoggedOnce(events, "Ignoring the unknown permission '" + permission + "' granted to role '" + role + "' on resource '");
            // the permissions of a row whose resource is unknown are dropped with it, without being parsed
            assertThat(events).noneMatch(e -> e.getFormattedMessage().contains("Ignoring the unknown permission 'SEARCH."));
            assertLoggedOnce(events, "Ignoring the unknown permission 'BACKUP' granted to role '" + role + "' on resource '" + dataResource +
                                     "' in system_auth.role_permissions");
            assertLoggedOnce(events, "Ignoring the grants to role '" + role + "' on resource '" + rowsResource +
                                     "' found in system_auth.role_permissions: resources of type 'rows' do not exist here");
            assertLoggedOnce(events, "Ignoring the grants to role '" + role + "' on resource '" + searchResource +
                                     "' found in system_auth.role_permissions: resources of type 'search' do not exist here");
            assertLoggedOnce(events, "resources of type 'rpc' do not exist here");
            assertLoggedOnce(events, "resources of type 'authentication_schemes' do not exist here");
            assertLoggedOnce(events, "Ignoring the grants to role '" + role + "' on resource '" + functionNoArgs +
                                     "' found in system_auth.role_permissions: the name cannot be parsed");
            assertLoggedOnce(events, "Ignoring the grants to role '" + role + "' on resource '" + functionUnknownType +
                                     "' found in system_auth.role_permissions: the name cannot be parsed");
        });

        // and the role can do what it was granted, and only that
        useUser(role, PASSWORD);
        executeNet(format("SELECT * FROM %s.%s", KEYSPACE, table));
        assertThatThrownBy(() -> executeNet(format("INSERT INTO %s.%s (k, v) VALUES (1, 1)", KEYSPACE, table))).isInstanceOf(UnauthorizedException.class);

        // granting and revoking on the row that carries DSE names still works (the row is re-read, so its DSE names go through the tolerant parser)
        useSuperUser();
        executeNet(format("GRANT MODIFY ON %s.%s TO %s", KEYSPACE, table, role));
        executeNet(format("REVOKE SELECT ON %s.%s FROM %s", KEYSPACE, table, role));
        AuthenticatedUser.permissionsCache.invalidate();
        useUser(role, PASSWORD);
        executeNet(format("INSERT INTO %s.%s (k, v) VALUES (1, 1)", KEYSPACE, table));
        assertThatThrownBy(() -> executeNet(format("SELECT * FROM %s.%s", KEYSPACE, table))).isInstanceOf(UnauthorizedException.class);
    }

    @Test
    public void listAllPermissionsAndDropRoleWithDseRows() throws Throwable
    {
        String role = "dse_analytics";
        String proxied = "dse_proxied";
        useSuperUser();
        String table = createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        executeNet(format("CREATE ROLE %s WITH login = true AND password = '%s'", role, PASSWORD));
        executeNet(format("CREATE ROLE %s", proxied));
        executeNet(format("GRANT SELECT ON %s.%s TO %s", KEYSPACE, table, role));
        // names disjoint from the other test (whose rows, if it ran first, were already logged): the proxy login on a
        // role, the resource manager resources, with their current and their legacy (pre-6) roots
        insertGrant(role, "roles/" + proxied, "'PROXY.LOGIN'");
        insertGrant(role, "any_work_pool_in_any_dc/DC1/Analytics", "'CREATE', 'DESCRIBE'");
        insertGrant(role, "any_submission_in_any_workpool_in_any_dc/DC1/Analytics/app-123", "'MODIFY'");
        insertGrant(role, "any_work_pool/Analytics", "'CREATE'");
        insertGrant(role, "any_submission/Analytics/app-123", "'MODIFY'");

        withLogAppender(CassandraAuthorizer.class, appender -> {
            // LIST ALL PERMISSIONS with no grantee reads every row of role_permissions, the DSE ones included
            int rowsOfRole = 0;
            for (Row row : executeNet("LIST ALL PERMISSIONS"))
            {
                if (!role.equals(row.getString("role")))
                    continue;
                rowsOfRole++;
                assertThat(row.getString("resource")).isEqualTo("<table " + KEYSPACE + '.' + table + '>');
                assertThat(row.getString("permission")).isEqualTo("SELECT");
            }
            assertThat(rowsOfRole).isEqualTo(1);

            List<ILoggingEvent> events = appender.list;
            assertLoggedOnce(events, "Ignoring the unknown permission 'PROXY.LOGIN' granted to role '" + role + "' on resource 'roles/" + proxied + "'");
            for (String type : new String[]{ "any_work_pool_in_any_dc", "any_submission_in_any_workpool_in_any_dc", "any_work_pool", "any_submission" })
                assertLoggedOnce(events, "' found in system_auth.role_permissions: resources of type '" + type + "' do not exist here");
        });

        // DROP ROLE removes every row of the role, the DSE ones included
        assertThat(execute("SELECT resource FROM system_auth.role_permissions WHERE role = ?", role).size()).isEqualTo(6);
        executeNet(format("DROP ROLE %s", role));
        assertThat(execute("SELECT resource FROM system_auth.role_permissions WHERE role = ?", role).size()).isZero();
        assertThat(execute("SELECT role FROM system_auth.roles WHERE role = ?", role).size()).isZero();
    }

    /**
     * DSE writes {@code UPDATE} + {@code TRUNCATE} for {@code GRANT ALL} and a creator's permissions: the pair is read
     * as {@code MODIFY}, and only that, whatever the mode (the rows outlive the flag).
     */
    @Test
    public void dseModifyPairReadsAsModifyInEveryMode() throws Throwable
    {
        for (InternodeCompatibilityMode mode : InternodeCompatibilityMode.values())
        {
            String role = "dse_writer_" + mode.name().toLowerCase();
            useSuperUser();
            String table = createRoleAndTable(role);
            insertGrant(role, dataResource(table), "'UPDATE', 'TRUNCATE'");
            CassandraAuthorizer.resetLoneDseWriteGrants();

            withMode(mode, () -> withLogAppender(CassandraAuthorizer.class, appender -> {
                ResultSet listed = executeNet(format("LIST ALL PERMISSIONS OF %s", role));
                assertOnlyPermissionOnTable(listed, role, table, "MODIFY");
                Map<Pair<AuthenticatedUser, IResource>, Set<Permission>> warm = ((CassandraAuthorizer) DatabaseDescriptor.getAuthorizer()).bulkLoader().get();
                assertThat(warm.get(Pair.create(new AuthenticatedUser(role), DataResource.table(KEYSPACE, table)))).containsExactly(Permission.MODIFY);

                useUser(role, PASSWORD);
                assertWrites(table, true);
                assertThatThrownBy(() -> executeNet(format("SELECT * FROM %s.%s", KEYSPACE, table))).isInstanceOf(UnauthorizedException.class);
                assertThatThrownBy(() -> executeNet(format("ALTER TABLE %s.%s WITH comment = 'x'", KEYSPACE, table))).isInstanceOf(UnauthorizedException.class);
                assertThatThrownBy(() -> executeNet(format("DROP TABLE %s.%s", KEYSPACE, table))).isInstanceOf(UnauthorizedException.class);

                // the warm-up read the rows of the other tests too: only this role's row matters here
                assertThat(CassandraAuthorizer.loneDseWriteGrants()).noneMatch(entry -> entry.startsWith("role '" + role + "'"));
                // LIST read this role's row first, so the one-off message names it
                assertThat(appender.list.stream().filter(e -> e.getLevel() == Level.INFO && e.getFormattedMessage().equals(
                    "Reading the permissions UPDATE and TRUNCATE granted together to role '" + role + "' on resource '" + dataResource(table) +
                    "' in system_auth.role_permissions as MODIFY: DataStax Enterprise writes the two for GRANT ALL and a creator's " +
                    "permissions, and this software for every grant of MODIFY (GRANT MODIFY, GRANT ALL, a creator's permissions) with " +
                    "internode_compatibility_mode DSE_6 (logged once)")).count())
                .isEqualTo(1);
                assertThat(appender.list).noneMatch(e -> e.getLevel() == Level.WARN && e.getFormattedMessage().contains("role '" + role + "'"));
            }));
        }
    }

    /** {@code UPDATE} alone is never widened to {@code MODIFY} (which would also grant truncate): dropped, logged once and listed. */
    @Test
    public void dseUpdateAloneIsDroppedAndListed() throws Throwable
    {
        String role = "dse_updater";
        useSuperUser();
        String table = createRoleAndTable(role);
        executeNet(format("GRANT SELECT ON %s.%s TO %s", KEYSPACE, table, role));
        addNames(role, dataResource(table), "'UPDATE'");

        withLogAppender(CassandraAuthorizer.class, appender -> {
            for (int i = 0; i < 2; i++)
                assertOnlyPermissionOnTable(executeNet(format("LIST ALL PERMISSIONS OF %s", role)), role, table, "SELECT");
            useUser(role, PASSWORD);
            executeNet(format("SELECT * FROM %s.%s", KEYSPACE, table));
            assertWrites(table, false);

            assertLoggedOnce(appender.list, "Ignoring the permission 'UPDATE' granted alone to role '" + role + "' on resource '" + dataResource(table) +
                                            "' in system_auth.role_permissions: DataStax Enterprise grants it without TRUNCATE, and this software " +
                                            "has MODIFY only, which grants both, so the role loses inserts, updates and deletes. GRANT MODIFY if the " +
                                            "role should also truncate, or accept the loss (logged once per role and resource; nodetool compatstatus lists them)");
        });

        String entry = "role '" + role + "' on " + dataResource(table) + ": UPDATE";
        assertThat(CassandraAuthorizer.loneDseWriteGrants()).containsExactly(entry);
        // what nodetool compatstatus reads: only the rows read so far
        assertThat(MessagingService.instance().getDseLoneWriteGrants()).containsExactly(entry);
        assertThat(MessagingService.instance().getDseLoneWriteGrantsSource()).isEqualTo("the rows read since startup");
        // nor does the warm-up of the permissions cache grant more than SELECT
        Map<Pair<AuthenticatedUser, IResource>, Set<Permission>> warm = ((CassandraAuthorizer) DatabaseDescriptor.getAuthorizer()).bulkLoader().get();
        assertThat(warm.get(Pair.create(new AuthenticatedUser(role), DataResource.table(KEYSPACE, table)))).containsExactly(Permission.SELECT);

        // DROP ROLE forgets the role's entries (the warm-up also listed those of the rows other tests leave)
        useSuperUser();
        executeNet(format("DROP ROLE %s", role));
        assertThat(CassandraAuthorizer.loneDseWriteGrants()).noneMatch(e -> e.startsWith("role '" + role + "'"));
    }

    /** {@code TRUNCATE} alone does not grant truncate here, which CQL grants only with {@code MODIFY}. */
    @Test
    public void dseTruncateAloneIsDroppedAndListed() throws Throwable
    {
        String role = "dse_truncater";
        useSuperUser();
        String table = createRoleAndTable(role);
        insertGrant(role, dataResource(table), "'TRUNCATE'");
        try
        {
            withLogAppender(CassandraAuthorizer.class, appender -> {
                useUser(role, PASSWORD);
                assertWrites(table, false);
                assertLoggedOnce(appender.list, "Ignoring the permission 'TRUNCATE' granted alone to role '" + role + "' on resource '" + dataResource(table) +
                                                "' in system_auth.role_permissions: DataStax Enterprise grants it without UPDATE, and this software " +
                                                "has MODIFY only, which grants both, so the role loses truncate. GRANT MODIFY if the role should also " +
                                                "insert, update and delete, or accept the loss");
            });
            useSuperUser();
            assertThat(executeNet(format("LIST ALL PERMISSIONS OF %s", role)).all()).isEmpty();
            assertThat(CassandraAuthorizer.loneDseWriteGrants()).containsExactly("role '" + role + "' on " + dataResource(table) + ": TRUNCATE");

            // DROP TABLE forgets the table's entries
            executeNet(format("DROP TABLE %s.%s", KEYSPACE, table));
            assertThat(CassandraAuthorizer.loneDseWriteGrants()).isEmpty();
        }
        finally
        {
            // the row has no entry in the reverse index, so the drop of the table leaves it: DROP ROLE deletes it
            useSuperUser();
            executeNet(format("DROP ROLE %s", role));
        }
    }

    /**
     * DSE's UPDATE and TRUNCATE are known on data resources only: elsewhere they are unknown names, never
     * read as MODIFY (on an MBean it would allow setting attributes) and never listed.
     */
    @Test
    public void dseNamesOnOtherResourcesAreUnknown() throws Throwable
    {
        String role = "dse_jmx";
        String other = "dse_other_role";
        useSuperUser();
        createRoleAndTable(role);
        executeNet(format("CREATE ROLE %s", other));
        String mbean = "org.apache.cassandra.db:type=Tables,*";
        insertGrant(role, "mbean/" + mbean, "'SELECT', 'UPDATE', 'TRUNCATE'");
        insertGrant(role, "roles/" + other, "'DESCRIBE', 'TRUNCATE'");
        insertGrant(role, "functions/" + KEYSPACE, "'EXECUTE', 'UPDATE'");

        IAuthorizer authorizer = DatabaseDescriptor.getAuthorizer();
        AuthenticatedUser user = new AuthenticatedUser(role);
        withLogAppender(CassandraAuthorizer.class, appender -> {
            assertThat(authorizer.authorize(user, JMXResource.mbean(mbean))).containsExactly(Permission.SELECT);
            assertThat(authorizer.authorize(user, RoleResource.role(other))).containsExactly(Permission.DESCRIBE);
            assertThat(authorizer.authorize(user, FunctionResource.keyspace(KEYSPACE))).containsExactly(Permission.EXECUTE);
            // no other test puts these names on a resource other than data: this one logs them first, once each
            assertLoggedOnce(appender.list, "Ignoring the unknown permission 'UPDATE' granted to role '" + role + "' on resource 'mbean/" + mbean + "'");
            assertLoggedOnce(appender.list, "Ignoring the unknown permission 'TRUNCATE' granted to role '" + role + "' on resource 'mbean/" + mbean + "'");
        });
        Set<String> listed = new HashSet<>();
        for (Row row : executeNet(format("LIST ALL PERMISSIONS OF %s", role)))
            listed.add(row.getString("permission"));
        assertThat(listed).containsExactlyInAnyOrder("SELECT", "DESCRIBE", "EXECUTE");
        assertThat(CassandraAuthorizer.loneDseWriteGrants()).noneMatch(entry -> entry.startsWith("role '" + role + "'"));

        // REVOKE MODIFY on the MBean leaves them alone, like any unknown name
        ResultSet revoked = executeNet(format("REVOKE MODIFY ON MBEAN '%s' FROM %s", mbean, role));
        assertThat(revoked.getExecutionInfo().getWarnings()).containsExactly("Role '" + role + "' was not granted MODIFY on <mbean " + mbean + '>');
        assertThat(storedNamesOf(role, "mbean/" + mbean)).containsExactlyInAnyOrder("SELECT", "UPDATE", "TRUNCATE");

        // the read of a scan lists nothing on them either (the names are logged above)
        List<String> newlyListed = new ArrayList<>();
        assertThat(CassandraAuthorizer.readDseLoneWriteGrant(role, "mbean/" + mbean, Set.of("UPDATE"), newlyListed)).isNull();
        assertThat(CassandraAuthorizer.readDseLoneWriteGrant(role, "roles/" + other, Set.of("TRUNCATE"), newlyListed)).isNull();
        assertThat(newlyListed).isEmpty();
    }

    /**
     * Without the mode: GRANT MODIFY leaves a DSE pair as DSE wrote it (held already); REVOKE MODIFY removes DSE's names,
     * paired or alone, so that neither this node nor a DSE node grants the writes any more; GRANT MODIFY on a lone name
     * writes MODIFY only.
     */
    @Test
    public void grantAndRevokeModifyOnDseRowsWithoutTheMode() throws Throwable
    {
        assertThat(DatabaseDescriptor.isDse6CompatibilityMode()).isFalse();
        String role = "dse_granted";
        useSuperUser();
        String pairTable = createRoleAndTable(role);
        String updateTable = createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        String truncateTable = createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        insertGrant(role, dataResource(pairTable), "'UPDATE', 'TRUNCATE'");
        insertGrant(role, dataResource(updateTable), "'UPDATE'");
        insertGrant(role, dataResource(truncateTable), "'SELECT', 'TRUNCATE'");
        insertIndexEntry(role, dataResource(updateTable));

        // the pair already is MODIFY: nothing is written
        ResultSet granted = executeNet(format("GRANT MODIFY ON %s.%s TO %s", KEYSPACE, pairTable, role));
        assertThat(granted.getExecutionInfo().getWarnings()).containsExactly("Role '" + role + "' was already granted MODIFY on <table " + KEYSPACE + '.' + pairTable + '>');
        assertThat(storedNames(role, pairTable)).containsExactlyInAnyOrder("UPDATE", "TRUNCATE");

        // revoking MODIFY removes both DSE names
        ResultSet revoked = executeNet(format("REVOKE MODIFY ON %s.%s FROM %s", KEYSPACE, pairTable, role));
        assertThat(revoked.getExecutionInfo().getWarnings()).isEmpty();
        assertThat(storedNames(role, pairTable)).isEmpty();

        // a lone UPDATE is removed too, with a warning: the role did not hold MODIFY here, but a DSE node honours UPDATE;
        // the row is left empty, so its entry in the reverse index goes as well
        withLogAppender(CassandraAuthorizer.class, appender -> {
            ResultSet revokedLone = executeNet(format("REVOKE MODIFY ON %s.%s FROM %s", KEYSPACE, updateTable, role));
            String message = "Revoked UPDATE (a DataStax Enterprise permission), granted alone to role '" + role + "' on " +
                             dataResource(updateTable) + ", which this software did not honour";
            assertThat(revokedLone.getExecutionInfo().getWarnings())
            .containsExactlyInAnyOrder(message, "Role '" + role + "' was not granted MODIFY on <table " + KEYSPACE + '.' + updateTable + '>');
            assertThat(appender.list).anyMatch(e -> e.getLevel() == Level.INFO && e.getFormattedMessage().equals(message));
        });
        assertThat(storedNames(role, updateTable)).isEmpty();
        assertThat(indexEntries(role, dataResource(updateTable))).isZero();

        // GRANT MODIFY on a lone TRUNCATE writes this software's name only, and keeps DSE's
        listPermissions(role); // reads every row of the role: the lone TRUNCATE is listed
        assertThat(CassandraAuthorizer.loneDseWriteGrants()).containsExactly("role '" + role + "' on " + dataResource(truncateTable) + ": TRUNCATE");
        executeNet(format("GRANT MODIFY ON %s.%s TO %s", KEYSPACE, truncateTable, role));
        assertThat(storedNames(role, truncateTable)).containsExactlyInAnyOrder("SELECT", "TRUNCATE", "MODIFY");
        assertThat(CassandraAuthorizer.loneDseWriteGrants()).isEmpty();
        assertThat(indexEntries(role, dataResource(truncateTable))).isEqualTo(1);

        AuthenticatedUser.permissionsCache.invalidate();
        useUser(role, PASSWORD);
        assertWrites(truncateTable, true);
        assertWrites(pairTable, false);
        assertWrites(updateTable, false);
        useSuperUser();
        listPermissions(role);
        assertThat(CassandraAuthorizer.loneDseWriteGrants()).isEmpty();
    }

    /**
     * In the mode, GRANT MODIFY on a data resource writes UPDATE + TRUNCATE, what DSE writes for GRANT ALL and a
     * creator's permissions, so that the DSE nodes of the cluster and a node rolled back to DSE grant it, and GRANT ALL
     * writes them too; on an MBean only MODIFY is written. REVOKE MODIFY removes a lone TRUNCATE and REVOKE ALL the pair
     * GRANT ALL wrote (that REVOKE MODIFY always removes both names is tested by
     * {@link #grantAndRevokeModifyStatementsInTheMode}).
     */
    @Test
    public void grantAndRevokeModifyInTheModeWriteDseNames() throws Throwable
    {
        String role = "dse_granted_in_mode";
        useSuperUser();
        String table = createRoleAndTable(role);
        String updateTable = createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        String truncateTable = createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        String pairTable = createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        String allTable = createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        insertGrant(role, dataResource(updateTable), "'UPDATE'");
        insertGrant(role, dataResource(truncateTable), "'TRUNCATE'");
        insertGrant(role, dataResource(pairTable), "'UPDATE', 'TRUNCATE'");

        withMode(InternodeCompatibilityMode.DSE_6, () -> {
            executeNet(format("GRANT MODIFY ON %s.%s TO %s", KEYSPACE, table, role));
            assertThat(storedNames(role, table)).containsExactlyInAnyOrder("UPDATE", "TRUNCATE");
            assertThat(indexEntries(role, dataResource(table))).isEqualTo(1);

            listPermissions(role);
            assertThat(CassandraAuthorizer.loneDseWriteGrants()).contains("role '" + role + "' on " + dataResource(updateTable) + ": UPDATE");
            executeNet(format("GRANT MODIFY ON %s.%s TO %s", KEYSPACE, updateTable, role));
            assertThat(storedNames(role, updateTable)).containsExactlyInAnyOrder("UPDATE", "TRUNCATE");
            assertThat(CassandraAuthorizer.loneDseWriteGrants()).noneMatch(entry -> entry.contains(dataResource(updateTable)));

            // the pair is MODIFY already: the client is told so, and the name MODIFY is not written
            ResultSet granted = executeNet(format("GRANT MODIFY ON %s.%s TO %s", KEYSPACE, pairTable, role));
            assertThat(granted.getExecutionInfo().getWarnings()).containsExactly("Role '" + role + "' was already granted MODIFY on <table " + KEYSPACE + '.' + pairTable + '>');
            assertThat(storedNames(role, pairTable)).containsExactlyInAnyOrder("UPDATE", "TRUNCATE");

            executeNet(format("GRANT ALL ON %s.%s TO %s", KEYSPACE, allTable, role));
            assertThat(storedNames(role, allTable)).contains("UPDATE", "TRUNCATE", "SELECT", "ALTER").doesNotContain("MODIFY");
            // UPDATE + TRUNCATE are written on data resources only: elsewhere this software's name alone is written
            String mbean = "org.apache.cassandra.db:type=Tables,*";
            executeNet(format("GRANT MODIFY ON MBEAN '%s' TO %s", mbean, role));
            assertThat(storedNamesOf(role, "mbean/" + mbean)).containsExactly("MODIFY");

            AuthenticatedUser.permissionsCache.invalidate();
            useUser(role, PASSWORD);
            assertWrites(table, true);
            assertWrites(allTable, true);
            useSuperUser();

            // a revoke that removes the two names, as this software's REVOKE MODIFY does: nothing is left that grants MODIFY here
            execute(format("UPDATE system_auth.role_permissions SET permissions = permissions - {'UPDATE', 'TRUNCATE'} WHERE role = '%s' AND resource = '%s'",
                           role, dataResource(table)));
            // a lone TRUNCATE revoked in the mode: reported, and removed
            ResultSet revokedLone = executeNet(format("REVOKE MODIFY ON %s.%s FROM %s", KEYSPACE, truncateTable, role));
            assertThat(revokedLone.getExecutionInfo().getWarnings())
            .contains("Revoked TRUNCATE (a DataStax Enterprise permission), granted alone to role '" + role + "' on " +
                      dataResource(truncateTable) + ", which this software did not honour");
            assertThat(storedNames(role, truncateTable)).isEmpty();
            executeNet(format("REVOKE ALL ON %s.%s FROM %s", KEYSPACE, allTable, role));
            assertThat(storedNames(role, allTable)).isEmpty();
            AuthenticatedUser.permissionsCache.invalidate();
            useUser(role, PASSWORD);
            assertWrites(table, false);
            assertWrites(allTable, false);
            assertWrites(truncateTable, false);
            useSuperUser();
        });
    }

    /**
     * In the mode, what GRANT and REVOKE MODIFY write on a data resource does not depend on the LOCAL_ONE read of the
     * row: REVOKE removes DSE's two names, and the name MODIFY (what DSE's GRANT MODIFY writes), even when the read saw
     * none (a DSE node may just have granted them), and GRANT writes the two even when the row read already holds MODIFY.
     */
    @Test
    public void grantAndRevokeModifyStatementsInTheMode() throws Throwable
    {
        String role = "dse_statements";
        useSuperUser();
        String table = createRoleAndTable(role);
        String resource = dataResource(table);
        insertGrant(role, resource, "'UPDATE', 'TRUNCATE'");
        List<String> queries = new ArrayList<>();
        AtomicBoolean readsNothing = new AtomicBoolean();
        CassandraAuthorizer recording = new AuthTestUtils.LocalCassandraAuthorizer()
        {
            @Override
            UntypedResultSet process(String query, ConsistencyLevel cl)
            {
                queries.add(query);
                if (readsNothing.get() && query.startsWith("SELECT permissions FROM"))
                    return super.process("SELECT permissions FROM system_auth.role_permissions WHERE role = 'no_such_role' AND resource = 'data'", cl);
                return super.process(query, cl);
            }
        };
        String read = format("SELECT permissions FROM system_auth.role_permissions WHERE role = '%s' AND resource = '%s'", role, resource);
        DataResource data = DataResource.table(KEYSPACE, table);

        withMode(InternodeCompatibilityMode.DSE_6, () -> withLogAppender(CassandraAuthorizer.class, appender -> {
            // the row read holds the pair: MODIFY is held, and the two names are written again
            assertThat(recording.grant(AuthenticatedUser.SYSTEM_USER, EnumSet.of(Permission.MODIFY), data, RoleResource.role(role))).isEmpty();
            assertThat(queries).containsExactly(read,
                                                format("UPDATE system_auth.role_permissions SET permissions = permissions + {'UPDATE','TRUNCATE'} WHERE role = '%s' AND resource = '%s'", role, resource),
                                                format("INSERT INTO system_auth.resource_role_permissons_index (resource, role) VALUES ('%s','%s')", resource, role));

            // the read sees nothing: both names, and MODIFY, are removed all the same, silently, and the reverse index is left as it is
            String revokeAll = format("UPDATE system_auth.role_permissions SET permissions = permissions - {'MODIFY','UPDATE','TRUNCATE'} " +
                                      "WHERE role = '%s' AND resource = '%s'", role, resource);
            queries.clear();
            readsNothing.set(true);
            assertThat(recording.revoke(AuthenticatedUser.SYSTEM_USER, EnumSet.of(Permission.MODIFY), data, RoleResource.role(role))).isEmpty();
            assertThat(queries).containsExactly(read, revokeAll);
            assertThat(appender.list).noneMatch(e -> e.getFormattedMessage().startsWith("Revoked "));
            assertThat(storedNamesOf(role, resource)).isEmpty();

            // the name MODIFY, as DSE's GRANT MODIFY writes it, that the read misses does not outlive the revoke either
            insertGrant(role, resource, "'MODIFY'");
            queries.clear();
            assertThat(recording.revoke(AuthenticatedUser.SYSTEM_USER, EnumSet.of(Permission.MODIFY), data, RoleResource.role(role))).isEmpty();
            assertThat(queries).containsExactly(read, revokeAll);
            assertThat(storedNamesOf(role, resource)).isEmpty();
        }));
    }

    /** Without the mode, GRANT and REVOKE send the same statements as before for rows without DSE's names. */
    @Test
    public void plainGrantAndRevokeStatementsAreUnchangedWithoutTheMode() throws Throwable
    {
        assertThat(DatabaseDescriptor.isDse6CompatibilityMode()).isFalse();
        String role = "plain_statements";
        useSuperUser();
        String table = createRoleAndTable(role);
        String resource = dataResource(table);
        List<String> queries = new ArrayList<>();
        CassandraAuthorizer recording = new AuthTestUtils.LocalCassandraAuthorizer()
        {
            @Override
            UntypedResultSet process(String query, ConsistencyLevel cl)
            {
                queries.add(query);
                return super.process(query, cl);
            }
        };
        String read = format("SELECT permissions FROM system_auth.role_permissions WHERE role = '%s' AND resource = '%s'", role, resource);

        Set<Permission> granted = recording.grant(AuthenticatedUser.SYSTEM_USER, EnumSet.of(Permission.SELECT, Permission.MODIFY), DataResource.table(KEYSPACE, table), RoleResource.role(role));
        assertThat(granted).containsExactlyInAnyOrder(Permission.SELECT, Permission.MODIFY);
        assertThat(queries).containsExactly(read,
                                            format("UPDATE system_auth.role_permissions SET permissions = permissions + {'SELECT','MODIFY'} WHERE role = '%s' AND resource = '%s'", role, resource),
                                            format("INSERT INTO system_auth.resource_role_permissons_index (resource, role) VALUES ('%s','%s')", resource, role));

        queries.clear();
        Set<Permission> revoked = recording.revoke(AuthenticatedUser.SYSTEM_USER, EnumSet.of(Permission.SELECT, Permission.MODIFY), DataResource.table(KEYSPACE, table), RoleResource.role(role));
        assertThat(revoked).containsExactlyInAnyOrder(Permission.SELECT, Permission.MODIFY);
        assertThat(queries.get(0)).isEqualTo(read);
        assertThat(queries.get(1)).isIn(format("UPDATE system_auth.role_permissions SET permissions = permissions - {'SELECT','MODIFY'} WHERE role = '%s' AND resource = '%s'", role, resource),
                                        format("UPDATE system_auth.role_permissions SET permissions = permissions - {'MODIFY','SELECT'} WHERE role = '%s' AND resource = '%s'", role, resource));
        assertThat(queries.subList(2, queries.size()))
        .containsExactly(format("DELETE FROM system_auth.resource_role_permissons_index WHERE resource = '%s' and role = '%s'", resource, role));
    }

    /** Rows without DSE's names are written and read as before without the mode, and nothing about DSE is logged. */
    @Test
    public void plainRowsAreUnchangedWithoutTheMode() throws Throwable
    {
        assertThat(DatabaseDescriptor.isDse6CompatibilityMode()).isFalse();
        String role = "plain_writer";
        useSuperUser();
        String table = createRoleAndTable(role);
        withLogAppender(CassandraAuthorizer.class, appender -> {
            executeNet(format("GRANT MODIFY ON %s.%s TO %s", KEYSPACE, table, role));
            executeNet(format("GRANT SELECT ON %s.%s TO %s", KEYSPACE, table, role));
            assertThat(storedNames(role, table)).containsExactlyInAnyOrder("MODIFY", "SELECT");
            AuthenticatedUser.permissionsCache.invalidate();
            useUser(role, PASSWORD);
            assertWrites(table, true);
            useSuperUser();
            ResultSet revoked = executeNet(format("REVOKE MODIFY ON %s.%s FROM %s", KEYSPACE, table, role));
            assertThat(revoked.getExecutionInfo().getWarnings()).isEmpty();
            assertThat(storedNames(role, table)).containsExactly("SELECT");
            assertOnlyPermissionOnTable(executeNet(format("LIST ALL PERMISSIONS OF %s", role)), role, table, "SELECT");
            assertThat(appender.list).noneMatch(e -> e.getFormattedMessage().contains("DataStax Enterprise"));
        });
        assertThat(CassandraAuthorizer.loneDseWriteGrants()).isEmpty();
    }

    /**
     * The read of a row for a scan of the table: the lone grant it lists first is reported as new, then not; a lone
     * grant a scan forgets (it failed before logging) is new again for the next read.
     */
    @Test
    public void scanReadsReportTheLoneGrantsTheyListFirst() throws Throwable
    {
        String resource = "data/ks_scan/t";
        List<String> newlyListed = new ArrayList<>();
        String entry = "role 'dse_scan_role' on " + resource + ": UPDATE";
        assertThat(CassandraAuthorizer.readDseLoneWriteGrant("dse_scan_role", resource, Set.of("SELECT", "UPDATE"), newlyListed)).isEqualTo(entry);
        assertThat(newlyListed).containsExactly(entry);
        assertThat(CassandraAuthorizer.loneDseWriteGrants()).containsExactly(entry);

        newlyListed.clear();
        assertThat(CassandraAuthorizer.readDseLoneWriteGrant("dse_scan_role", resource, Set.of("SELECT", "UPDATE"), newlyListed)).isEqualTo(entry);
        assertThat(newlyListed).isEmpty();
        // another name alone on the same row is new
        String truncate = "role 'dse_scan_role' on " + resource + ": TRUNCATE";
        assertThat(CassandraAuthorizer.readDseLoneWriteGrant("dse_scan_role", resource, Set.of("TRUNCATE"), newlyListed)).isEqualTo(truncate);
        assertThat(newlyListed).containsExactly(truncate);

        CassandraAuthorizer.forgetDseLoneWriteGrants(List.of(truncate));
        assertThat(CassandraAuthorizer.loneDseWriteGrants()).isEmpty();
        newlyListed.clear();
        assertThat(CassandraAuthorizer.readDseLoneWriteGrant("dse_scan_role", resource, Set.of("TRUNCATE"), newlyListed)).isEqualTo(truncate);
        assertThat(newlyListed).containsExactly(truncate);

        // a data resource name that cannot be parsed: nothing listed (a name of its own: each is logged once per JVM)
        assertThat(CassandraAuthorizer.readDseLoneWriteGrant("dse_scan_role", "data/ks_scan/t/extra", Set.of("UPDATE"), newlyListed)).isNull();
        assertThat(newlyListed).containsExactly(truncate);
        assertThat(CassandraAuthorizer.loneDseWriteGrants()).containsExactly(truncate);
        // a resource of a type this software does not have, and so not a data resource either (a type of its own)
        assertThat(CassandraAuthorizer.readDseLoneWriteGrant("dse_scan_role", "dse_scan_type/t", Set.of("UPDATE"), newlyListed)).isNull();
        assertThat(newlyListed).containsExactly(truncate);

        // the pair, or no permissions at all: nothing listed, the entry forgotten
        newlyListed.clear();
        assertThat(CassandraAuthorizer.readDseLoneWriteGrant("dse_scan_role", resource, Set.of("UPDATE", "TRUNCATE"), newlyListed)).isNull();
        assertThat(CassandraAuthorizer.readDseLoneWriteGrant("dse_scan_role", resource, Set.of(), newlyListed)).isNull();
        assertThat(newlyListed).isEmpty();
        assertThat(CassandraAuthorizer.loneDseWriteGrants()).isEmpty();
    }

    /**
     * A scan records how the list was made, which is then its source; a complete scan also forgets the entries of the
     * rows it did not find granting one alone, an incomplete one forgets nothing.
     */
    @Test
    public void scansRecordTheSourceAndACompleteOneForgetsWhatItDidNotFind() throws Throwable
    {
        List<String> newlyListed = new ArrayList<>();
        String kept = CassandraAuthorizer.readDseLoneWriteGrant("dse_scanned_role", "data/ks_scanned/kept", Set.of("UPDATE"), newlyListed);
        String gone = CassandraAuthorizer.readDseLoneWriteGrant("dse_scanned_role", "data/ks_scanned/gone", Set.of("TRUNCATE"), newlyListed);
        assertThat(CassandraAuthorizer.loneDseWriteGrants()).containsExactly(gone, kept);
        assertThat(CassandraAuthorizer.loneDseWriteGrantsSource()).isEqualTo("the rows read since startup");

        CassandraAuthorizer.dseLoneWriteGrantsScanned("a partial scan", List.of(kept), false);
        assertThat(CassandraAuthorizer.loneDseWriteGrants()).containsExactly(gone, kept);
        assertThat(CassandraAuthorizer.loneDseWriteGrantsSource()).isEqualTo("a partial scan");

        CassandraAuthorizer.dseLoneWriteGrantsScanned("a complete scan", List.of(kept), true);
        assertThat(CassandraAuthorizer.loneDseWriteGrants()).containsExactly(kept);
        assertThat(CassandraAuthorizer.loneDseWriteGrantsSource()).isEqualTo("a complete scan");
    }

    /**
     * The read of GRANT and REVOKE neither lists nor logs a lone DSE name, nor logs the pair; the authorizer's next read
     * of the row does.
     */
    @Test
    public void grantAndRevokeDoNotReportTheLoneNameTheyRead() throws Throwable
    {
        for (InternodeCompatibilityMode mode : InternodeCompatibilityMode.values())
        {
            String role = "dse_statement_read_" + mode.name().toLowerCase();
            useSuperUser();
            String table = createRoleAndTable(role);
            String resource = dataResource(table);
            insertGrant(role, resource, "'UPDATE'");
            String entry = "role '" + role + "' on " + resource + ": UPDATE";
            CassandraAuthorizer.resetLoneDseWriteGrants();
            try
            {
                withMode(mode, () -> withLogAppender(CassandraAuthorizer.class, appender -> {
                    // the read of GRANT and REVOKE SELECT: nothing listed, nothing logged
                    executeNet(format("GRANT SELECT ON %s.%s TO %s", KEYSPACE, table, role));
                    executeNet(format("REVOKE SELECT ON %s.%s FROM %s", KEYSPACE, table, role));
                    assertThat(appender.list).noneMatch(e -> e.getFormattedMessage().contains("role '" + role + "'"));
                    assertThat(CassandraAuthorizer.loneDseWriteGrants()).isEmpty();

                    // the authorizer's read lists and logs it
                    listPermissions(role);
                    assertThat(CassandraAuthorizer.loneDseWriteGrants()).containsExactly(entry);
                    assertLoggedOnce(appender.list, "Ignoring the permission 'UPDATE' granted alone to role '" + role + "'");

                    // a listed entry stays listed through the read of a GRANT SELECT, even when that read finds the pair
                    // (it may be a stale replica): the authorizer's next read of the row forgets it
                    executeNet(format("GRANT SELECT ON %s.%s TO %s", KEYSPACE, table, role));
                    assertThat(CassandraAuthorizer.loneDseWriteGrants()).containsExactly(entry);
                    execute(format("UPDATE system_auth.role_permissions SET permissions = permissions + {'TRUNCATE'} WHERE role = '%s' AND resource = '%s'",
                                   role, resource));
                    executeNet(format("REVOKE SELECT ON %s.%s FROM %s", KEYSPACE, table, role));
                    assertThat(CassandraAuthorizer.loneDseWriteGrants()).containsExactly(entry);
                    String pairRead = "Reading the permissions UPDATE and TRUNCATE granted together to role '" + role + "'";
                    assertThat(appender.list).noneMatch(e -> e.getFormattedMessage().startsWith(pairRead));
                    listPermissions(role);
                    assertThat(CassandraAuthorizer.loneDseWriteGrants()).isEmpty();
                    assertThat(appender.list.stream().filter(e -> e.getLevel() == Level.INFO && e.getFormattedMessage().startsWith(pairRead)).count())
                    .isEqualTo(1);
                    assertLoggedOnce(appender.list, "Ignoring the permission 'UPDATE' granted alone to role '" + role + "'");
                }));
            }
            finally
            {
                // also deletes the role's rows of role_permissions
                useSuperUser();
                executeNet(format("DROP ROLE %s", role));
            }
        }
    }

    /** A row holding MODIFY and one of DSE's names alone grants MODIFY: nothing is dropped, logged or listed. */
    @Test
    public void modifyWithOneDseNameReadsAsModify() throws Throwable
    {
        String role = "dse_modify_and_update";
        useSuperUser();
        String table = createRoleAndTable(role);
        insertGrant(role, dataResource(table), "'SELECT', 'MODIFY', 'UPDATE'");
        withLogAppender(CassandraAuthorizer.class, appender -> {
            assertThat(DatabaseDescriptor.getAuthorizer().authorize(new AuthenticatedUser(role), DataResource.table(KEYSPACE, table)))
            .containsExactlyInAnyOrder(Permission.SELECT, Permission.MODIFY);
            assertThat(appender.list).noneMatch(e -> e.getFormattedMessage().contains("role '" + role + "'"));
        });
        assertThat(CassandraAuthorizer.loneDseWriteGrants()).noneMatch(entry -> entry.startsWith("role '" + role + "'"));
    }

    /**
     * The rows of {@code system_auth.role_permissions} DSE 6.8.65 wrote (the sstables of {@link DseFixtures#schemaDataDirectory()}):
     * the permissions DSE grants the creator of a keyspace, a table and their functions, with the creator's
     * {@code UPDATE} + {@code TRUNCATE} read as MODIFY, and {@code PROXY.LOGIN} and {@code PROXY.EXECUTE} on a role,
     * which this software does not have, dropped; nothing is granted alone.
     */
    @Test
    public void rowsWrittenByDse6865() throws Throwable
    {
        useSuperUser();
        // DSE's definition of role_permissions, under another name, to read its sstables
        createTable("CREATE TABLE %s (role text, resource text, permissions set<text>, restricted set<text>, grantables set<text>, " +
                    "PRIMARY KEY (role, resource))");
        Path fixture = DseFixtures.schemaDataDirectory().resolve("system_auth").resolve("role_permissions-3afbe79f219431a7add7f5ab90d8ec9c");
        // the importer takes the keyspace and table from the two last directories of the path
        Path temp = Files.createTempDirectory("dse-role-permissions");
        PathUtils.deleteRecursiveOnExit(temp);
        Path copy = Files.createDirectories(temp.resolve(KEYSPACE).resolve(currentTable()));
        try (Stream<Path> files = Files.list(fixture))
        {
            for (Path file : (Iterable<Path>) files::iterator)
                Files.copy(file, copy.resolve(file.getFileName()));
        }
        assertThat(getCurrentColumnFamilyStore().importNewSSTables(Set.of(copy.toString()), false, false, false, false, false, false, false)).isEmpty();
        UntypedResultSet rows = execute("SELECT role, resource, permissions FROM %s");
        assertThat(rows.size()).isEqualTo(25);
        // as DSE stored them: the creator's UPDATE + TRUNCATE, never alone and never the name MODIFY; PROXY.* the only unknown names
        assertThat(storedNamesIn(rows, "dseadmin", "data/test_ks_fixture")).contains("UPDATE", "TRUNCATE").doesNotContain("MODIFY");
        assertThat(storedNamesIn(rows, "dseadmin", "data/test_ks_fixture/t")).contains("UPDATE", "TRUNCATE").doesNotContain("MODIFY");
        Set<String> unknown = new HashSet<>();
        for (UntypedResultSet.Row row : rows)
        {
            Set<String> names = row.getSet("permissions", UTF8Type.instance);
            assertThat(names).as(row.getString("resource")).doesNotContain("MODIFY");
            assertThat(CassandraAuthorizer.dseLoneWriteGrantName(row.getString("resource"), names)).as(row.getString("resource")).isNull();
            assertThat(CassandraAuthorizer.isKnownResourceName(row.getString("resource"))).as(row.getString("resource")).isTrue();
            unknown.addAll(CassandraAuthorizer.unknownPermissionNames(row.getString("resource"), names));
        }
        assertThat(unknown).containsExactlyInAnyOrder("PROXY.EXECUTE", "PROXY.LOGIN");

        String admin = "dseadmin";
        executeNet(format("CREATE ROLE %s WITH login = true AND password = '%s'", admin, PASSWORD));
        for (UntypedResultSet.Row row : rows)
            execute("INSERT INTO system_auth.role_permissions (role, resource, permissions) VALUES (?, ?, ?)",
                    row.getString("role"), row.getString("resource"), row.getSet("permissions", UTF8Type.instance));
        try
        {
            IAuthorizer authorizer = DatabaseDescriptor.getAuthorizer();
            AuthenticatedUser user = new AuthenticatedUser(admin);
            assertThat(authorizer.authorize(user, DataResource.keyspace("test_ks_fixture")))
            .containsExactlyInAnyOrder(Permission.CREATE, Permission.ALTER, Permission.DROP, Permission.SELECT, Permission.MODIFY,
                                       Permission.AUTHORIZE, Permission.DESCRIBE);
            assertThat(authorizer.authorize(user, DataResource.table("test_ks_fixture", "t")))
            .containsExactlyInAnyOrder(Permission.ALTER, Permission.DROP, Permission.SELECT, Permission.MODIFY, Permission.AUTHORIZE);
            assertThat(authorizer.authorize(user, FunctionResource.keyspace("test_ks_fixture")))
            .containsExactlyInAnyOrder(Permission.CREATE, Permission.ALTER, Permission.DROP, Permission.AUTHORIZE, Permission.EXECUTE);

            // the creator of the role dseadmin, the superuser cassandra, was granted PROXY.LOGIN and PROXY.EXECUTE on it too:
            // names the authorizer skips (read here without the authorizer, which logs each unknown name once per JVM, and
            // other tests assert the logging of those two)
            Set<String> stored = storedNamesOf("cassandra", "roles/" + admin);
            assertThat(stored).containsExactlyInAnyOrder("ALTER", "AUTHORIZE", "DESCRIBE", "DROP", "PROXY.EXECUTE", "PROXY.LOGIN");
            assertThat(CassandraAuthorizer.unknownPermissionNames("roles/" + admin, stored)).containsExactly("PROXY.EXECUTE", "PROXY.LOGIN");
            assertThat(CassandraAuthorizer.dseLoneWriteGrantName("roles/" + admin, stored)).isNull();
            assertThat(CassandraAuthorizer.loneDseWriteGrants()).noneMatch(entry -> entry.startsWith("role '" + admin + "'"));
        }
        finally
        {
            for (UntypedResultSet.Row row : rows)
                execute("DELETE FROM system_auth.role_permissions WHERE role = ? AND resource = ?", row.getString("role"), row.getString("resource"));
            executeNet(format("DROP ROLE %s", admin));
        }
    }

    /**
     * setup() reads nothing, in the mode or not: the lone grants are listed as their rows are read.
     */
    @Test
    public void setupReadsNothing() throws Throwable
    {
        List<String> queries = new ArrayList<>();
        CassandraAuthorizer recording = new AuthTestUtils.LocalCassandraAuthorizer()
        {
            @Override
            UntypedResultSet process(String query, ConsistencyLevel cl)
            {
                queries.add(query);
                return super.process(query, cl);
            }
        };
        recording.setup();
        assertThat(CassandraAuthorizer.loneDseWriteGrantsSource()).isEqualTo("the rows read since startup");
        withMode(InternodeCompatibilityMode.DSE_6, () -> {
            recording.setup();
            assertThat(CassandraAuthorizer.loneDseWriteGrantsSource()).isEqualTo("the rows read since startup");
        });
        assertThat(queries).isEmpty();
    }

    /**
     * DSE writes its rows with INSERT, so a row can be left with a null permissions set; GRANT and REVOKE used to fail
     * on it with a NullPointerException.
     */
    @Test
    public void grantAndRevokeOnRowWithoutPermissions() throws Throwable
    {
        String role = "dse_emptied";
        useSuperUser();
        String table = createRoleAndTable(role);
        insertGrant(role, dataResource(table), "'SELECT'");
        execute(format("UPDATE system_auth.role_permissions SET permissions = permissions - {'SELECT'} WHERE role = '%s' AND resource = '%s'", role, dataResource(table)));
        UntypedResultSet row = execute("SELECT permissions FROM system_auth.role_permissions WHERE role = ? AND resource = ?", role, dataResource(table));
        assertThat(row.size()).isEqualTo(1);
        assertThat(row.one().has("permissions")).isFalse();

        executeNet(format("GRANT SELECT ON %s.%s TO %s", KEYSPACE, table, role));
        assertThat(storedNames(role, table)).containsExactly("SELECT");
        executeNet(format("REVOKE SELECT ON %s.%s FROM %s", KEYSPACE, table, role));
        ResultSet revoked = executeNet(format("REVOKE SELECT ON %s.%s FROM %s", KEYSPACE, table, role));
        assertThat(revoked.getExecutionInfo().getWarnings()).containsExactly("Role '" + role + "' was not granted SELECT on <table " + KEYSPACE + '.' + table + '>');
        assertThat(storedNames(role, table)).isEmpty();
    }

    private String createRoleAndTable(String role) throws Throwable
    {
        String table = createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        executeNet(format("CREATE ROLE %s WITH login = true AND password = '%s'", role, PASSWORD));
        return table;
    }

    private static String dataResource(String table)
    {
        return "data/" + KEYSPACE + '/' + table;
    }

    private void addNames(String role, String resource, String names) throws Throwable
    {
        execute(format("UPDATE system_auth.role_permissions SET permissions = permissions + {%s} WHERE role = '%s' AND resource = '%s'", names, role, resource));
    }

    private void insertIndexEntry(String role, String resource) throws Throwable
    {
        execute(format("INSERT INTO system_auth.resource_role_permissons_index (resource, role) VALUES ('%s', '%s')", resource, role));
    }

    private int indexEntries(String role, String resource) throws Throwable
    {
        return execute("SELECT role FROM system_auth.resource_role_permissons_index WHERE resource = ? AND role = ?", resource, role).size();
    }

    private Set<String> storedNames(String role, String table) throws Throwable
    {
        return storedNamesOf(role, dataResource(table));
    }

    private Set<String> storedNamesOf(String role, String resource) throws Throwable
    {
        UntypedResultSet rows = execute("SELECT permissions FROM system_auth.role_permissions WHERE role = ? AND resource = ?", role, resource);
        if (rows.isEmpty() || !rows.one().has("permissions"))
            return Collections.emptySet();
        return rows.one().getSet("permissions", UTF8Type.instance);
    }

    private void listPermissions(String role) throws Throwable
    {
        executeNet(format("LIST ALL PERMISSIONS OF %s", role));
    }

    // INSERT, UPDATE, DELETE and TRUNCATE, the operations MODIFY covers, as the current user
    private void assertWrites(String table, boolean allowed) throws Throwable
    {
        String[] statements = { format("INSERT INTO %s.%s (k, v) VALUES (1, 1)", KEYSPACE, table),
                                format("UPDATE %s.%s SET v = 2 WHERE k = 1", KEYSPACE, table),
                                format("DELETE FROM %s.%s WHERE k = 1", KEYSPACE, table),
                                format("TRUNCATE %s.%s", KEYSPACE, table) };
        for (String statement : statements)
        {
            if (allowed)
                executeNet(statement);
            else
                assertThatThrownBy(() -> executeNet(statement)).as(statement).isInstanceOf(UnauthorizedException.class);
        }
    }

    private static void withMode(InternodeCompatibilityMode mode, ThrowingRunnable test) throws Throwable
    {
        InternodeCompatibilityMode before = DatabaseDescriptor.getInternodeCompatibilityMode();
        DatabaseDescriptor.setInternodeCompatibilityMode(mode);
        try
        {
            test.run();
        }
        finally
        {
            DatabaseDescriptor.setInternodeCompatibilityMode(before);
        }
    }

    private interface ThrowingRunnable
    {
        void run() throws Throwable;
    }

    private static Set<String> storedNamesIn(UntypedResultSet rows, String role, String resource)
    {
        for (UntypedResultSet.Row row : rows)
            if (row.getString("role").equals(role) && row.getString("resource").equals(resource))
                return row.getSet("permissions", UTF8Type.instance);
        throw new AssertionError("no row of role " + role + " on " + resource);
    }

    private void insertGrant(String role, String resource, String permissions) throws Throwable
    {
        execute(format("INSERT INTO system_auth.role_permissions (role, resource, permissions) VALUES ('%s', '%s', {%s})", role, resource, permissions));
    }

    private static void assertOnlySelectOnTable(ResultSet listed, String role, String table)
    {
        assertOnlyPermissionOnTable(listed, role, table, "SELECT");
    }

    private static void assertOnlyPermissionOnTable(ResultSet listed, String role, String table, String permission)
    {
        int rows = 0;
        for (Row row : listed)
        {
            rows++;
            assertThat(row.getString("role")).isEqualTo(role);
            assertThat(row.getString("resource")).isEqualTo("<table " + KEYSPACE + '.' + table + '>');
            assertThat(row.getString("permission")).isEqualTo(permission);
        }
        assertThat(rows).isEqualTo(1);
    }

    private static void assertLoggedOnce(List<ILoggingEvent> events, String text)
    {
        assertThat(events.stream().filter(e -> e.getLevel() == Level.WARN && e.getFormattedMessage().contains(text)).count())
        .as("warnings containing \"%s\" in %s", text, events)
        .isEqualTo(1);
    }
}
