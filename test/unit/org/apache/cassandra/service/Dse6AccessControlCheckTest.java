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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;

import org.apache.cassandra.auth.AllowAllAuthorizer;
import org.apache.cassandra.auth.AuthKeyspace;
import org.apache.cassandra.auth.CassandraAuthorizer;
import org.apache.cassandra.auth.IAuthorizer;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.dsecompat.Dse51Fixtures;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.StartupException;
import org.apache.cassandra.exceptions.TombstoneAbortException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.schema.Dse51SchemaRows;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.SchemaKeyspace;
import org.apache.cassandra.schema.SchemaKeyspaceTables;
import org.apache.cassandra.schema.SchemaTestUtil;
import org.apache.cassandra.schema.SystemDistributedKeyspace;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.schema.Types;
import org.apache.cassandra.schema.ViewMetadata;
import org.apache.cassandra.schema.Views;
import org.apache.cassandra.service.Dse6AccessControlCheck.Findings;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.config.CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE_ALLOW_DDL;
import static org.apache.cassandra.config.CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE_DSE_RESTRICTIONS_CHECK_WAIT_SECONDS;
import static org.apache.cassandra.config.CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE_DSE_RESTRICTIONS_LOCAL_CHECK_ONLY;
import static org.apache.cassandra.config.CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE_IGNORE_DSE_RESTRICTIONS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * The fail-fast check on DSE access controls: {@code RESTRICT} entries, row-level grants and tables restricted to rows
 * (the {@code DSE_RLACA} table extension) refuse the start in {@code DSE_6} mode, delegations only warn, the other table
 * extensions are not access controls, the override property and {@code NONE} let the node start. The rows are
 * written as DSE leaves them; the DSE-only columns are added to this software's {@code role_permissions} table for the
 * tests that need them. This node never joins a ring, so the cluster-wide pass cannot reach its consistency level
 * here: what it does then (refuse, unless overridden) is tested here, and what it finds on a real cluster in the in-JVM
 * {@code Dse6GuardsTest}. The tests of the lone DSE write grants that need a complete cluster-wide read give this node a
 * token, which makes it the replica of every partition.
 * <p>
 * The lone DSE write grants ({@code UPDATE} or {@code TRUNCATE} alone, which {@code CassandraAuthorizer} drops) are
 * listed by every pass, logged in one warning by the second startup pass, and those the periodic check lists first by
 * it.
 * <p>
 * The cluster-wide pass retries a read failing on the request for a while; the tests of what it does when the read
 * cannot be made run it with a single attempt, and its retries are tested with a cluster-wide read, a clock and a pause
 * of their own.
 */
public class Dse6AccessControlCheckTest extends CQLTester
{
    private static final String OVERRIDE = "-Dcassandra.internode_compatibility_mode.ignore_dse_restrictions=true";
    private static final String ROLL_BACK = "Roll this node back to DSE, remove them there (";
    /** The advice of a refusal that read rows of this node's replica. */
    private static final String STALE = "The rows listed from system_auth.role_permissions may be stale: the replica may hold partitions this " +
                                        "node no longer owns (once it is rolled back to DSE, run nodetool cleanup system_auth on it), or miss a " +
                                        "removal made on DSE while this node was down (once it is rolled back to DSE, hints or nodetool repair " +
                                        "system_auth deliver it). ";
    /** The warning of every start with the override set. */
    private static final String OVERRIDE_SET = "cassandra.internode_compatibility_mode.ignore_dse_restrictions=true is set: the DSE access " +
                                               "controls this software does not enforce are dropped knowingly instead of refusing the start, " +
                                               "and the cluster-wide check is not retried; remove it once they are removed on DSE";
    private static final String UPGRADE_AGAIN = ") and upgrade it again; or start with " + OVERRIDE + " to drop them knowingly";
    /** The advice of a refusal that found row-level grants only. */
    private static final String REMEDY = ROLL_BACK + "REVOKE the row-level grants" + UPGRADE_AGAIN;
    private static final String LONE_GRANTS_REMEDY = ". GRANT MODIFY where the role should also have the other, or accept the loss " +
                                                     "(nodetool compatstatus lists them)";
    private static final String DELEGATE_TRUNCATE = "role 'delegate' on data/ks2: TRUNCATE";
    private static final String LOCAL_QUORUM_FAILED = "Cannot achieve consistency level LOCAL_QUORUM";
    private static final String ANALYST_UPDATE = "role 'analyst' on data/ks1/t1: UPDATE";
    private static final String AUDITOR_TRUNCATE = "role 'auditor' on data/ks1: TRUNCATE";
    private static final String LOCAL_ONLY = "this node's replica only: the cluster-wide read failed (" + LOCAL_QUORUM_FAILED + ')';
    /** The warning of the second pass with the override when its read of this node's replica fails too. */
    private static final String NOT_READ_AGAIN = "The startup check of the DSE access controls could not read this node's replica again; the " +
                                                 "schema and what the first pass found in the replica are listed, the DSE AUTHORIZE FOR " +
                                                 "delegations the replica holds, dropped, are not";
    private static final String DROPPED = ", DataStax Enterprise permissions: this software has MODIFY only, which grants both, so they are " +
                                          "dropped and these roles lose the access they gave: ";
    /** The materialized view of the DSE 5.1.51 schema fixture that was restricted to rows, then unrestricted. */
    private static final String DSE51_VIEW = Dse51SchemaRows.KEYSPACE + ".rlac_by_v";
    /** The table of the DSE 5.1.51 schema fixture restricted to rows. */
    private static final String DSE51_TABLE = Dse51SchemaRows.KEYSPACE + ".rlac";
    /** The resources of the row-level grants of the DSE 5.1.51 schema fixture (its {@code select-system-auth.txt}). */
    private static final List<String> DSE51_ROW_RESOURCES = List.of("rows/EU/data/test_ks/rlac",
                                                                    "rows/EU/data/test_ks/rlac_by_v");
    /** The row-level grants of the DSE 5.1.51 schema fixture, as the check lists them. */
    private static final List<String> DSE51_ROW_GRANTS = List.of("role 'reader' on rows/EU/data/test_ks/rlac: [SELECT]",
                                                                 "role 'reader' on rows/EU/data/test_ks/rlac_by_v: " +
                                                                 "[SELECT]");
    /** What a refusal of DSE_5_1 says of a materialized view restricted to rows. */
    private static final String DSE51_VIEW_ADVICE = "No remedy is validated yet for the materialized views " +
                                                    "restricted to rows [" + DSE51_VIEW + "]: DSE 5.1 keeps a " +
                                                    "RESTRICT ROWS on a view on the node that ran it only, and an " +
                                                    "UNRESTRICT ROWS on a view fails on DSE 5.1.51 and leaves on " +
                                                    "that node a row of system_schema.tables named after the view, " +
                                                    "with which neither DSE 5.1.51 nor this software starts the " +
                                                    "node: do not run UNRESTRICT ROWS on a view";
    private static final String DSE51_REFUSED = "internode_compatibility_mode DSE_5_1 refused: DSE access controls " +
                                                "that this software does not enforce would grant access if ignored " +
                                                "(found by this node's replica): ";
    private InternodeCompatibilityMode modeBefore;
    private boolean ownsAToken;
    private final List<String> rolesToDelete = new ArrayList<>(List.of("analyst", "auditor", "delegate"));

    /** A single attempt of the cluster-wide read, for the tests where it cannot be made: restored after the class. */
    private static WithProperties singleAttempt;

    @BeforeClass
    public static void setUpAuth()
    {
        singleAttempt = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE_DSE_RESTRICTIONS_CHECK_WAIT_SECONDS, 0);
        requireAuthentication();
    }

    @AfterClass
    public static void restoreWait()
    {
        if (singleAttempt != null)
            singleAttempt.close();
    }

    @Before
    public void setUp()
    {
        modeBefore = DatabaseDescriptor.getInternodeCompatibilityMode();
        CassandraAuthorizer.resetLoneDseWriteGrants();
    }

    @After
    public void tearDown() throws Throwable
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(modeBefore);
        leaveTheRing();
        Dse6AccessControlCheck.resetLastCheck();
        CassandraAuthorizer.resetLoneDseWriteGrants();
        for (String role : rolesToDelete)
            execute("DELETE FROM system_auth.role_permissions WHERE role = ?", role);
    }

    /** Makes this node the replica of every partition, so that the cluster-wide read succeeds. */
    private void joinTheRing()
    {
        StorageService.instance.getTokenMetadata().updateNormalToken(DatabaseDescriptor.getPartitioner().getRandomToken(),
                                                                     FBUtilities.getBroadcastAddressAndPort());
        ownsAToken = true;
    }

    /** Back to a node without tokens, whose cluster-wide read cannot reach its consistency level. */
    private void leaveTheRing()
    {
        if (ownsAToken)
            StorageService.instance.getTokenMetadata().removeEndpoint(FBUtilities.getBroadcastAddressAndPort());
        ownsAToken = false;
    }

    private static void dse6()
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
    }

    private interface Check
    {
        void run() throws Exception;
    }

    /** Runs {@code check} and returns the WARN messages {@link Dse6AccessControlCheck} logged meanwhile. */
    private static List<String> warnings(Check check) throws Exception
    {
        return messages(Dse6AccessControlCheck.class, Level.WARN, check);
    }

    /** Runs {@code check} and returns the messages of {@code level} that the logger of {@code type} logged meanwhile. */
    private static List<String> messages(Class<?> type, Level level, Check check) throws Exception
    {
        return events(type, check).stream()
                                  .filter(e -> e.getLevel() == level)
                                  .map(ILoggingEvent::getFormattedMessage)
                                  .collect(Collectors.toList());
    }

    /** Runs {@code check} and returns what the logger of {@code type} logged meanwhile, at every level it logs. */
    private static List<ILoggingEvent> events(Class<?> type, Check check) throws Exception
    {
        Logger logger = (Logger) LoggerFactory.getLogger(type);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        try
        {
            check.run();
        }
        finally
        {
            logger.detachAppender(appender);
        }
        return appender.list;
    }

    private void addDseColumns() throws Throwable
    {
        TableMetadata table = Schema.instance.getTableMetadata(SchemaConstants.AUTH_KEYSPACE_NAME, AuthKeyspace.ROLE_PERMISSIONS);
        if (table.getColumn(ColumnIdentifier.getInterned("restricted", true)) == null)
            execute("ALTER TABLE system_auth.role_permissions ADD (restricted set<text>, grantables set<text>)");
    }

    @Test
    public void nothingToFindOnAPlainNode() throws Throwable
    {
        dse6();
        execute("INSERT INTO system_auth.role_permissions (role, resource, permissions) VALUES ('analyst', 'data/ks1/t1', {'SELECT'})");
        Findings findings = Dse6AccessControlCheck.scan(null);
        assertThat(findings.restrictions).isEmpty();
        assertThat(findings.rowLevelGrants).isEmpty();
        assertThat(findings.delegations).isEmpty();
        Dse6AccessControlCheck.verifyAtStartup();
    }

    @Test
    public void rowLevelGrantsRefuseTheStart() throws Throwable
    {
        dse6();
        execute("INSERT INTO system_auth.role_permissions (role, resource, permissions) VALUES ('analyst', 'rows/top_secret/data/ks1/table1', {'SELECT', 'MODIFY'})");
        execute("INSERT INTO system_auth.role_permissions (role, resource, permissions) VALUES ('analyst', 'data/ks1/table1', {'SELECT'})");
        assertThat(Dse6AccessControlCheck.scan(null).rowLevelGrants).containsExactly("role 'analyst' on rows/top_secret/data/ks1/table1: [MODIFY, SELECT]");
        assertThatThrownBy(Dse6AccessControlCheck::verifyAtStartup)
        .isInstanceOf(StartupException.class)
        .hasMessage("internode_compatibility_mode DSE_6 refused: DSE access controls that this software does not enforce would grant access " +
                    "if ignored (found by this node's replica): row-level grants [role 'analyst' on rows/top_secret/data/ks1/table1: [MODIFY, SELECT]]. " +
                    STALE + REMEDY);
    }

    @Test
    public void thePeriodicCheckReportsControlsThatAppearAfterTheStart() throws Throwable
    {
        dse6();
        // this node never joined a ring: the cluster-wide read fails, which is said, never reported as an all-clear
        assertThat(warnings(() -> assertThat(Dse6AccessControlCheck.recheck()).isEmpty()))
        .containsExactly("The periodic check of the DSE access controls could not read system_auth.role_permissions at LOCAL_QUORUM " +
                         "(Cannot achieve consistency level LOCAL_QUORUM); only this node's replica was checked");
        assertThat(Dse6AccessControlCheck.lastCheck().findings).isEmpty();
        assertThat(Dse6AccessControlCheck.lastCheck().source)
        .isEqualTo("this node's replica only: the cluster-wide read failed (Cannot achieve consistency level LOCAL_QUORUM)");

        // a DSE node grants row-level access after this node started: reported, the node keeps running
        execute("INSERT INTO system_auth.role_permissions (role, resource, permissions) VALUES ('analyst', 'rows/top_secret/data/ks1/table1', {'SELECT'})");
        assertThat(Dse6AccessControlCheck.recheck()).containsExactly("row-level grant role 'analyst' on rows/top_secret/data/ks1/table1: [SELECT]");
        assertThat(Dse6AccessControlCheck.lastCheck().findings).containsExactly("row-level grant role 'analyst' on rows/top_secret/data/ks1/table1: [SELECT]");

        execute("DELETE FROM system_auth.role_permissions WHERE role = 'analyst'");
        assertThat(Dse6AccessControlCheck.recheck()).isEmpty();

        // nothing is read, or logged, with NONE
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
        execute("INSERT INTO system_auth.role_permissions (role, resource, permissions) VALUES ('analyst', 'rows/top_secret/data/ks1/table1', {'SELECT'})");
        execute("INSERT INTO system_auth.role_permissions (role, resource, permissions) VALUES ('auditor', 'data/ks1', {'TRUNCATE'})");
        assertThat(warnings(() -> assertThat(Dse6AccessControlCheck.recheck()).isEmpty())).isEmpty();
        assertThat(CassandraAuthorizer.loneDseWriteGrants()).isEmpty();
    }

    @Test
    public void restrictionsRefuseTheStartAndDelegationsOnlyWarn() throws Throwable
    {
        addDseColumns();
        dse6();
        execute("INSERT INTO system_auth.role_permissions (role, resource, permissions, grantables) VALUES ('delegate', 'data/ks1', {'SELECT'}, {'SELECT'})");
        Findings findings = Dse6AccessControlCheck.scan(null);
        assertThat(findings.delegations).containsExactly("role 'delegate' on data/ks1: [SELECT]");
        assertThat(findings.wouldGrantAccessIfDropped()).isFalse();
        // fail closed: dropped with a warning, the node starts; the warning comes from the cluster-wide pass only (once per start)
        assertThat(warnings(Dse6AccessControlCheck::verifyAtStartup)).isEmpty();
        try (WithProperties properties = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE_DSE_RESTRICTIONS_LOCAL_CHECK_ONLY, true))
        {
            assertThat(warnings(Dse6AccessControlCheck::verifyAfterJoin))
            .contains("DSE AUTHORIZE FOR delegation in system_auth.role_permissions is not enforced by this software and is dropped " +
                      "(the delegate can no longer grant; re-grant AUTHORIZE where needed): role 'delegate' on data/ks1: [SELECT]");
        }
        // and so with the override when the cluster-wide read fails
        try (WithProperties properties = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE_IGNORE_DSE_RESTRICTIONS, true))
        {
            assertThat(warnings(Dse6AccessControlCheck::verifyAfterJoin))
            .contains("DSE AUTHORIZE FOR delegation in system_auth.role_permissions is not enforced by this software and is dropped " +
                      "(the delegate can no longer grant; re-grant AUTHORIZE where needed): role 'delegate' on data/ks1: [SELECT]");
        }

        execute("INSERT INTO system_auth.role_permissions (role, resource, permissions, restricted) VALUES ('analyst', 'data/ks1/t1', {}, {'SELECT', 'MODIFY'})");
        execute("INSERT INTO system_auth.role_permissions (role, resource, permissions, restricted) VALUES ('auditor', 'data/ks1', {'SELECT'}, {})");
        execute("INSERT INTO system_auth.role_permissions (role, resource, permissions) VALUES ('auditor', 'rows/audit/data/ks1/t2', {'SELECT'})");
        findings = Dse6AccessControlCheck.scan(null);
        assertThat(findings.restrictions).containsExactly("role 'analyst' on data/ks1/t1: [MODIFY, SELECT]");
        assertThat(findings.rowLevelGrants).containsExactly("role 'auditor' on rows/audit/data/ks1/t2: [SELECT]");
        assertThatThrownBy(Dse6AccessControlCheck::verifyAtStartup)
        .isInstanceOf(StartupException.class)
        .hasMessageContaining("RESTRICT [role 'analyst' on data/ks1/t1: [MODIFY, SELECT]]; row-level grants [role 'auditor' on rows/audit/data/ks1/t2: [SELECT]]")
        .hasMessageEndingWith(ROLL_BACK + "UNRESTRICT the roles, REVOKE the row-level grants" + UPGRADE_AGAIN);

        // the advice names what was found only
        execute("DELETE FROM system_auth.role_permissions WHERE role = 'auditor'");
        assertThatThrownBy(Dse6AccessControlCheck::verifyAtStartup)
        .isInstanceOf(StartupException.class)
        .hasMessageEndingWith(ROLL_BACK + "UNRESTRICT the roles" + UPGRADE_AGAIN);
    }

    @Test
    public void aClusterWideReadThatCannotBeMadeRefusesTheStart() throws Throwable
    {
        dse6();
        execute("INSERT INTO system_auth.role_permissions (role, resource, permissions) VALUES ('analyst', 'rows/top_secret/data/ks1/table1', {'SELECT'})");
        // this node never joined a ring: the read at the authorizer's consistency (LOCAL_QUORUM) fails, and the check fails closed
        assertThatThrownBy(Dse6AccessControlCheck::verifyAfterJoin)
        .isInstanceOf(StartupException.class)
        .hasMessage("internode_compatibility_mode DSE_6 refused: unable to read system_auth.role_permissions at LOCAL_QUORUM (Cannot achieve " +
                    "consistency level LOCAL_QUORUM), so the DSE access controls this software does not enforce cannot be checked across the " +
                    "cluster; make the replicas of system_auth reachable, or start with " +
                    "-Dcassandra.internode_compatibility_mode.dse_restrictions_local_check_only=true to rely on the check of this node's replica alone")
        .hasCauseInstanceOf(UnavailableException.class);

        // the local-only property skips the cluster-wide read, with a warning, but not the refusal of the local findings
        try (WithProperties properties = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE_DSE_RESTRICTIONS_LOCAL_CHECK_ONLY, true))
        {
            // the local findings are refused by this pass too, with the local text
            assertThatThrownBy(Dse6AccessControlCheck::verifyAfterJoin)
            .isInstanceOf(StartupException.class)
            .hasMessageContaining("(found by this node's replica only: the cluster-wide read failed (Cannot achieve consistency level LOCAL_QUORUM)): " +
                                  "row-level grants [role 'analyst' on rows/top_secret/data/ks1/table1: [SELECT]]. The rows listed from " +
                                  "system_auth.role_permissions may be stale: the replica may hold partitions this node no longer owns (once " +
                                  "it is rolled back to DSE, run nodetool cleanup system_auth on it), or miss a removal made on DSE while this " +
                                  "node was down (once it is rolled back to DSE, hints or nodetool repair system_auth deliver it). ");
            assertThatThrownBy(Dse6AccessControlCheck::verifyAtStartup).isInstanceOf(StartupException.class);

            // with a clean table the node starts, with a warning
            execute("DELETE FROM system_auth.role_permissions WHERE role = 'analyst'");
            assertThat(warnings(Dse6AccessControlCheck::verifyAfterJoin))
            .containsExactly("unable to read system_auth.role_permissions at LOCAL_QUORUM (Cannot achieve consistency level LOCAL_QUORUM), so the " +
                             "DSE access controls this software does not enforce cannot be checked across the cluster; only this node's replica " +
                             "was checked (cassandra.internode_compatibility_mode.dse_restrictions_local_check_only=true)");
        }
        // the override that drops the controls knowingly also accepts the unchecked cluster
        try (WithProperties properties = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE_IGNORE_DSE_RESTRICTIONS, true))
        {
            Dse6AccessControlCheck.verifyAfterJoin();
        }
    }

    @Test
    public void theOverridePropertyLetsTheNodeStart() throws Throwable
    {
        dse6();
        execute("INSERT INTO system_auth.role_permissions (role, resource, permissions) VALUES ('analyst', 'rows/top_secret/data/ks1/table1', {'SELECT'})");
        try (WithProperties properties = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE_IGNORE_DSE_RESTRICTIONS, true))
        {
            assertThat(warnings(Dse6AccessControlCheck::verifyAtStartup))
            .containsExactly(OVERRIDE_SET,
                             "DSE access controls that this software does not enforce are ignored " +
                             "(cassandra.internode_compatibility_mode.ignore_dse_restrictions=true) and access opens up accordingly " +
                             "(this node's replica): row-level grants [role 'analyst' on rows/top_secret/data/ks1/table1: [SELECT]]");
        }
        // compatstatus shows what was dropped knowingly from the start, not only after the first periodic check
        assertThat(Dse6AccessControlCheck.lastCheck().findings).containsExactly("row-level grant role 'analyst' on rows/top_secret/data/ks1/table1: [SELECT]");
        assertThat(Dse6AccessControlCheck.lastCheck().source).isEqualTo("this node's replica");
        assertThatThrownBy(Dse6AccessControlCheck::verifyAtStartup).isInstanceOf(StartupException.class);
    }

    /** A table as {@code RESTRICT ROWS ON <table> USING tenant} leaves it on DSE: 0x74656e616e74 is 'tenant' in UTF-8. */
    private String createRowLevelRestrictedTable() throws Throwable
    {
        return KEYSPACE + '.' + createTable("CREATE TABLE %s (tenant text, k int, v int, PRIMARY KEY (tenant, k)) " +
                                            "WITH extensions = {'DSE_RLACA': 0x74656e616e74, 'other': 0x02}");
    }

    /** The advice of a refusal that found the given tables restricted to rows only. */
    private static String unrestrictRows(String... tables)
    {
        return ROLL_BACK + Arrays.stream(tables).map(table -> "UNRESTRICT ROWS ON " + table).collect(Collectors.joining(", ")) + UPGRADE_AGAIN;
    }

    @Test
    public void rowLevelRestrictedTablesRefuseTheStart() throws Throwable
    {
        String table = createRowLevelRestrictedTable();
        // a case-sensitive tenant column is quoted as CQL needs it, a value that is not UTF-8 is shown in hexadecimal
        String quoted = KEYSPACE + '.' + createTable("CREATE TABLE %s (\"Tenant\" text PRIMARY KEY, v int) WITH extensions = {'DSE_RLACA': 0x54656e616e74}");
        String undecodable = KEYSPACE + '.' + createTable("CREATE TABLE %s (tenant text PRIMARY KEY, v int) WITH extensions = {'DSE_RLACA': 0xff}");
        List<String> tables = List.of(table + " (tenant column tenant)", quoted + " (tenant column \"Tenant\")",
                                      undecodable + " (tenant column 0xff (not a column name))");
        assertThat(Dse6AccessControlCheck.rowLevelRestrictedTables()).isEqualTo(tables);
        dse6();
        // no advice about a stale replica: the tables come from the schema, not from the rows of this node's replica
        assertThatThrownBy(Dse6AccessControlCheck::verifyAtStartup)
        .isInstanceOf(StartupException.class)
        .hasMessage("internode_compatibility_mode DSE_6 refused: DSE access controls that this software does not enforce would grant access " +
                    "if ignored (found by this node's replica): row-level restricted tables in the schema " + tables + ". " +
                    unrestrictRows(table, quoted, undecodable));
        assertThat(Dse6AccessControlCheck.lastCheck().findings)
        .isEqualTo(tables.stream().map(t -> "row-level restricted table in the schema " + t).collect(Collectors.toList()));

        // the second pass refuses them too, here from this node's replica
        try (WithProperties properties = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE_DSE_RESTRICTIONS_LOCAL_CHECK_ONLY, true))
        {
            assertThatThrownBy(Dse6AccessControlCheck::verifyAfterJoin)
            .isInstanceOf(StartupException.class)
            .hasMessageContaining("(found by " + LOCAL_ONLY + "): row-level restricted tables in the schema " + tables + ". " +
                                  unrestrictRows(table, quoted, undecodable));
        }
        // and so does a pass whose read of the permissions succeeds with nothing in them
        assertThatThrownBy(() -> Dse6AccessControlCheck.verifyAfterJoin(consistency -> new Findings(), () -> 0L, nanos -> {}, 0, PAUSE))
        .isInstanceOf(StartupException.class)
        .hasMessage("internode_compatibility_mode DSE_6 refused: DSE access controls that this software does not enforce would grant access " +
                    "if ignored (found by a LOCAL_QUORUM read): row-level restricted tables in the schema " + tables + ". " +
                    unrestrictRows(table, quoted, undecodable));
    }

    /** The name of the tenant column goes into logs and refusals: shown as CQL writes it, cut when long, in hexadecimal when it is not text. */
    @Test
    public void theTenantColumnIsShownSafely()
    {
        assertThat(Dse6AccessControlCheck.tenantColumn(ByteBufferUtil.bytes("tenant_id"))).isEqualTo("tenant_id");
        assertThat(Dse6AccessControlCheck.tenantColumn(ByteBufferUtil.bytes("select"))).isEqualTo("\"select\"");
        assertThat(Dse6AccessControlCheck.tenantColumn(ByteBufferUtil.EMPTY_BYTE_BUFFER)).isEqualTo("\"\"");
        assertThat(Dse6AccessControlCheck.tenantColumn(ByteBufferUtil.bytes("t\u00E9nant"))).isEqualTo("\"t\u00E9nant\"");
        String longName = "t".repeat(Dse6AccessControlCheck.TENANT_COLUMN_SHOWN + 1);
        assertThat(Dse6AccessControlCheck.tenantColumn(ByteBufferUtil.bytes(longName)))
        .isEqualTo("t".repeat(Dse6AccessControlCheck.TENANT_COLUMN_SHOWN) + "... (cut)");
        assertThat(Dse6AccessControlCheck.tenantColumn(ByteBufferUtil.bytes("t".repeat(Dse6AccessControlCheck.TENANT_COLUMN_SHOWN))))
        .isEqualTo("t".repeat(Dse6AccessControlCheck.TENANT_COLUMN_SHOWN));
        // characters, not UTF-16 units: a character outside the BMP is never cut in two
        String outsideTheBmp = "\uD83D\uDE00";
        assertThat(Dse6AccessControlCheck.tenantColumn(ByteBufferUtil.bytes(outsideTheBmp.repeat(Dse6AccessControlCheck.TENANT_COLUMN_SHOWN + 1))))
        .isEqualTo('"' + outsideTheBmp.repeat(Dse6AccessControlCheck.TENANT_COLUMN_SHOWN) + "\"... (cut)");
        // a line break or another control character would forge log lines: the bytes are shown instead
        assertThat(Dse6AccessControlCheck.tenantColumn(ByteBufferUtil.bytes("a\nb"))).isEqualTo("0x610a62 (not a column name)");
        // so would a line separator, and a bidirectional override would disguise the text
        assertThat(Dse6AccessControlCheck.tenantColumn(ByteBufferUtil.bytes("a\u2028b"))).isEqualTo("0x61e280a862 (not a column name)");
        assertThat(Dse6AccessControlCheck.tenantColumn(ByteBufferUtil.bytes("a\u202Eb"))).isEqualTo("0x61e280ae62 (not a column name)");
        byte[] notText = new byte[Dse6AccessControlCheck.TENANT_COLUMN_SHOWN + 1];
        Arrays.fill(notText, (byte) 0xff);
        assertThat(Dse6AccessControlCheck.tenantColumn(ByteBuffer.wrap(notText)))
        .isEqualTo("0x" + "ff".repeat(Dse6AccessControlCheck.TENANT_COLUMN_SHOWN) + "... (cut) (not a column name)");
        // the value is not consumed
        ByteBuffer value = ByteBufferUtil.bytes("tenant");
        Dse6AccessControlCheck.tenantColumn(value);
        assertThat(value.remaining()).isEqualTo(6);
    }

    @Test
    public void rowLevelRestrictedTablesAreListedWithTheOtherControls() throws Throwable
    {
        String table = createRowLevelRestrictedTable();
        dse6();
        execute("INSERT INTO system_auth.role_permissions (role, resource, permissions) VALUES ('analyst', 'rows/top_secret/data/ks1/table1', {'SELECT'})");
        // the advice about a stale replica is back with a row of this node's replica
        assertThatThrownBy(Dse6AccessControlCheck::verifyAtStartup)
        .isInstanceOf(StartupException.class)
        .hasMessage("internode_compatibility_mode DSE_6 refused: DSE access controls that this software does not enforce would grant access " +
                    "if ignored (found by this node's replica): row-level grants [role 'analyst' on rows/top_secret/data/ks1/table1: [SELECT]]; " +
                    "row-level restricted tables in the schema [" + table + " (tenant column tenant)]. " + STALE + ROLL_BACK +
                    "REVOKE the row-level grants, UNRESTRICT ROWS ON " + table + UPGRADE_AGAIN);

        // the periodic check reports them, from this node's replica when the cluster-wide read cannot be made
        List<String> errors = messages(Dse6AccessControlCheck.class, Level.ERROR, () ->
                                       assertThat(Dse6AccessControlCheck.recheck())
                                       .containsExactly("row-level grant role 'analyst' on rows/top_secret/data/ks1/table1: [SELECT]",
                                                        "row-level restricted table in the schema " + table + " (tenant column tenant)"));
        assertThat(errors).containsExactly("The periodic check (" + LOCAL_ONLY + ") found DSE access controls this node does not enforce: " +
                                           "row-level grants [role 'analyst' on rows/top_secret/data/ks1/table1: [SELECT]]; " +
                                           "row-level restricted tables in the schema [" + table + " (tenant column tenant)]; remove the " +
                                           "row-level grants on DSE, and do not run GRANT, REVOKE, RESTRICT, UNRESTRICT, " +
                                           "RESTRICT ROWS or UNRESTRICT ROWS on DSE nodes during the upgrade" + tablesRemedyOfTheErrorFor(table));
        assertThat(Dse6AccessControlCheck.lastCheck().source).isEqualTo(LOCAL_ONLY);
    }

    /** When neither read of the permissions can be made, the tables of the schema are still listed and logged. */
    @Test
    public void thePeriodicCheckListsTheTablesWhenNoReadCanBeMade() throws Throwable
    {
        String table = createRowLevelRestrictedTable();
        dse6();
        RuntimeException broken = new IllegalStateException("broken read");
        RuntimeException clusterWide = new IllegalStateException("broken cluster-wide read");
        List<ILoggingEvent> events = events(Dse6AccessControlCheck.class, () ->
                                            assertThatThrownBy(() -> Dse6AccessControlCheck.recheck(consistency -> { throw consistency == null ? broken : clusterWide; }))
                                            .isSameAs(broken));
        // the caller logs the failure: the one of the cluster-wide read goes with it, and no warning claims a read of the replica
        assertThat(broken.getSuppressed()).containsExactly(clusterWide);
        assertThat(events).noneMatch(event -> event.getLevel() == Level.WARN);
        String source = "not checked: the cluster-wide read failed (broken cluster-wide read) and so did the read of this node's replica (broken read)";
        // the tables only: nothing to remove on DSE, their remedy is the refusal's
        assertThat(events).filteredOn(event -> event.getLevel() == Level.ERROR).extracting(ILoggingEvent::getFormattedMessage)
                          .containsExactly("The periodic check (" + source + ") found DSE access controls this node does not enforce: " +
                                           "row-level restricted tables in the schema [" + table + " (tenant column tenant)]; do not run GRANT, " +
                                           "REVOKE, RESTRICT, UNRESTRICT, RESTRICT ROWS or UNRESTRICT ROWS on DSE nodes during the upgrade" +
                                           tablesRemedyOfTheErrorFor(table));
        assertThat(Dse6AccessControlCheck.lastCheck().source).isEqualTo(source);
        assertThat(Dse6AccessControlCheck.lastCheck().findings).containsExactly("row-level restricted table in the schema " + table + " (tenant column tenant)");
    }

    /** The refusal names an UNRESTRICT ROWS statement for the first tables only, and lists them all. */
    @Test
    public void theRefusalNamesTheFirstStatementsOnly() throws Throwable
    {
        List<String> tables = new ArrayList<>();
        for (int i = 0; i <= Dse6AccessControlCheck.UNRESTRICT_ROWS_NAMED; i++)
            tables.add(createRowLevelRestrictedTable());
        tables.sort(null);
        dse6();
        Throwable refusal = catchThrowable(Dse6AccessControlCheck::verifyAtStartup);
        assertThat(refusal).isInstanceOf(StartupException.class);
        for (String table : tables)
            assertThat(refusal.getMessage()).containsOnlyOnce(table + " (tenant column tenant)");
        assertThat(refusal.getMessage())
        .endsWith(ROLL_BACK + tables.subList(0, Dse6AccessControlCheck.UNRESTRICT_ROWS_NAMED).stream()
                                    .map(table -> "UNRESTRICT ROWS ON " + table).collect(Collectors.joining(", ")) +
                  ", the same on the other 1 listed" + UPGRADE_AGAIN);

        // the INFO line on the other extension keys names the first tables only, and a key that could forge a log line in hexadecimal
        try (WithProperties ddl = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE_ALLOW_DDL, true))
        {
            execute("ALTER TABLE " + tables.get(0) + " WITH extensions = {'DSE_RLACA': 0x74656e616e74, 'a\u202Eb': 0x02}");
        }
        List<String> infos = messages(Dse6AccessControlCheck.class, Level.INFO, () -> catchThrowable(Dse6AccessControlCheck::verifyAtStartup));
        assertThat(infos).hasSize(1);
        assertThat(infos.get(0)).startsWith("Tables with schema extensions other than DSE_RLACA, kept and not interpreted by this software " +
                                            "(not DSE access controls): [" + tables.get(0) + ": [0x61e280ae62 (bytes, not text)], " + tables.get(1) + ": ['other'], ")
                                .endsWith(tables.get(Dse6AccessControlCheck.OTHER_EXTENSIONS_LOGGED - 1) + ": ['other']] and 1 more");
    }

    /** The part of the periodic ERROR on a table restricted to rows, which an UNRESTRICT ROWS on DSE does not remove here. */
    private static String tablesRemedyOfTheErrorFor(String table)
    {
        return "; the tables restricted to rows are in this node's schema, which DSE DDL does not reach, and refuse its next start: roll " +
               "it back to DSE, run UNRESTRICT ROWS ON " + table + " there and upgrade it again";
    }

    @Test
    public void theOverridePropertyDropsRowLevelRestrictedTables() throws Throwable
    {
        String table = createRowLevelRestrictedTable();
        dse6();
        execute("INSERT INTO system_auth.role_permissions (role, resource, permissions) VALUES ('analyst', 'rows/top_secret/data/ks1/table1', {'SELECT'})");
        String grant = "role 'analyst' on rows/top_secret/data/ks1/table1: [SELECT]";
        String tables = "row-level grants [" + grant + "]; row-level restricted tables in the schema [" + table + " (tenant column tenant)]";
        List<String> found = List.of("row-level grant " + grant, "row-level restricted table in the schema " + table + " (tenant column tenant)");
        try (WithProperties properties = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE_IGNORE_DSE_RESTRICTIONS, true))
        {
            assertThat(warnings(Dse6AccessControlCheck::verifyAtStartup))
            .containsExactly(OVERRIDE_SET,
                             "DSE access controls that this software does not enforce are ignored " +
                             "(cassandra.internode_compatibility_mode.ignore_dse_restrictions=true) and access opens up accordingly " +
                             "(this node's replica): " + tables);
            assertThat(Dse6AccessControlCheck.lastCheck().findings).isEqualTo(found);

            // the second pass: its cluster-wide read fails here; what this node's replica and the schema hold is dropped knowingly
            // all the same, and stays listed
            String source = "not checked: the cluster-wide read failed and cassandra.internode_compatibility_mode.ignore_dse_restrictions=true; " +
                            "what this node's replica and the schema hold is listed";
            assertThat(warnings(Dse6AccessControlCheck::verifyAfterJoin))
            .containsExactly("unable to read system_auth.role_permissions at LOCAL_QUORUM (" + LOCAL_QUORUM_FAILED + "), so the DSE access " +
                             "controls this software does not enforce cannot be checked across the cluster; not checked " +
                             "(cassandra.internode_compatibility_mode.ignore_dse_restrictions=true: DSE access controls are dropped knowingly)",
                             "DSE access controls that this software does not enforce are ignored " +
                             "(cassandra.internode_compatibility_mode.ignore_dse_restrictions=true) and access opens up accordingly (" +
                             source + "): " + tables);
            assertThat(Dse6AccessControlCheck.lastCheck().source).isEqualTo(source);
            // compatstatus shows a source starting so as unknown, never as an all-clear
            assertThat(Dse6AccessControlCheck.lastCheck().source).startsWith(Dse6AccessControlCheck.NOT_CHECKED);
            assertThat(Dse6AccessControlCheck.lastCheck().findings).isEqualTo(found);

            // and the periodic check does not log them at ERROR; here it runs after a DSE node revoked the grant, so its record
            // holds the table only
            execute("DELETE FROM system_auth.role_permissions WHERE role = 'analyst'");
            assertThat(messages(Dse6AccessControlCheck.class, Level.ERROR, Dse6AccessControlCheck::recheck)).isEmpty();
            assertThat(Dse6AccessControlCheck.lastCheck().source).isEqualTo(LOCAL_ONLY);
            assertThat(Dse6AccessControlCheck.lastCheck().findings).containsExactly("row-level restricted table in the schema " + table + " (tenant column tenant)");

            // when the second read of this node's replica fails, what the schema holds is still listed and dropped knowingly, and what
            // the first pass found in the replica stays listed, although the periodic check above replaced its record
            RuntimeException broken = new IllegalStateException("broken read");
            List<String> warnings = messages(Dse6AccessControlCheck.class, Level.WARN, () ->
                                             Dse6AccessControlCheck.verifyAfterJoin(consistency -> { throw broken; }, () -> 0L, nanos -> {}, 0, PAUSE));
            String schemaOnly = "not checked: the cluster-wide read failed and cassandra.internode_compatibility_mode.ignore_dse_restrictions=true; " +
                                "what the schema holds and what the first pass found in this node's replica are listed (the read of this " +
                                "node's replica failed again: broken read)";
            assertThat(warnings).containsExactly("unable to read system_auth.role_permissions at LOCAL_QUORUM (broken read), so the DSE access " +
                                                 "controls this software does not enforce cannot be checked across the cluster; not checked " +
                                                 "(cassandra.internode_compatibility_mode.ignore_dse_restrictions=true: DSE access controls are " +
                                                 "dropped knowingly)",
                                                 NOT_READ_AGAIN,
                                                 "DSE access controls that this software does not enforce are ignored " +
                                                 "(cassandra.internode_compatibility_mode.ignore_dse_restrictions=true) and access opens up accordingly (" +
                                                 schemaOnly + "): row-level restricted tables in the schema [" + table + " (tenant column tenant)]");
            assertThat(Dse6AccessControlCheck.lastCheck().source).isEqualTo(schemaOnly);
            assertThat(Dse6AccessControlCheck.lastCheck().findings).isEqualTo(found);

            // the periodic check that can read nothing records the tables, throws, and logs no ERROR: they are dropped knowingly
            List<String> errors = messages(Dse6AccessControlCheck.class, Level.ERROR, () ->
                                           assertThatThrownBy(() -> Dse6AccessControlCheck.recheck(consistency -> { throw broken; })).isSameAs(broken));
            assertThat(errors).isEmpty();
            // the same failure for both reads: not suppressed by itself
            assertThat(broken.getSuppressed()).isEmpty();
            assertThat(Dse6AccessControlCheck.lastCheck().source)
            .isEqualTo("not checked: the cluster-wide read failed (broken read) and so did the read of this node's replica (broken read)");
            assertThat(Dse6AccessControlCheck.lastCheck().findings).containsExactly("row-level restricted table in the schema " + table + " (tenant column tenant)");
        }
    }

    /** DSE writes no table extension key but DSE_RLACA: the others are neither refused nor warned about, and listed in one INFO line. */
    @Test
    public void otherExtensionKeysAreNotAccessControls() throws Throwable
    {
        String table = KEYSPACE + '.' + createTable("CREATE TABLE %s (k int PRIMARY KEY, v int) WITH extensions = {'dse_rlac': 0x01, 'other': 0x02}");
        // the key is matched exactly, as DSE writes it
        String lowerCase = KEYSPACE + '.' + createTable("CREATE TABLE %s (k int PRIMARY KEY, v int) WITH extensions = {'dse_rlaca': 0x74656e616e74}");
        String restricted = createRowLevelRestrictedTable();
        assertThat(Dse6AccessControlCheck.rowLevelRestrictedTables()).containsExactly(restricted + " (tenant column tenant)");
        assertThat(Dse6AccessControlCheck.tablesWithOtherExtensions()).containsExactly(table + ": ['dse_rlac', 'other']", lowerCase + ": ['dse_rlaca']",
                                                                                       restricted + ": ['other']");
        // UNRESTRICT ROWS on DSE removes the key
        execute("ALTER TABLE " + restricted + " WITH extensions = {'other': 0x02}");
        assertThat(Dse6AccessControlCheck.rowLevelRestrictedTables()).isEmpty();

        // a long key is cut, an unsafe one is shown as its bytes, cut too
        String longKey = "k".repeat(Dse6AccessControlCheck.TENANT_COLUMN_SHOWN + 1);
        String unsafeKey = "\n".repeat(Dse6AccessControlCheck.TENANT_COLUMN_SHOWN + 1);
        String keys = KEYSPACE + '.' + createTable("CREATE TABLE %s (k int PRIMARY KEY, v int) WITH extensions = {'" + longKey + "': 0x01, '" +
                                                   unsafeKey + "': 0x02}");
        assertThat(Dse6AccessControlCheck.tablesWithOtherExtensions())
        .contains(keys + ": [0x" + "0a".repeat(Dse6AccessControlCheck.TENANT_COLUMN_SHOWN) + "... (cut) (bytes, not text), " +
                  "'" + "k".repeat(Dse6AccessControlCheck.TENANT_COLUMN_SHOWN) + "'... (cut)]");
        execute("DROP TABLE " + keys);
        dse6();

        List<ILoggingEvent> events = events(Dse6AccessControlCheck.class, Dse6AccessControlCheck::verifyAtStartup);
        assertThat(events).noneMatch(event -> event.getLevel().isGreaterOrEqual(Level.WARN));
        assertThat(events).filteredOn(event -> event.getLevel() == Level.INFO).extracting(ILoggingEvent::getFormattedMessage)
                          .containsExactly("Tables with schema extensions other than DSE_RLACA, kept and not interpreted by this software " +
                                           "(not DSE access controls): [" + table + ": ['dse_rlac', 'other'], " + lowerCase + ": ['dse_rlaca'], " +
                                           restricted + ": ['other']]");
        assertThat(Dse6AccessControlCheck.lastCheck().findings).isEmpty();

        // the first pass only: neither the second pass nor the periodic check lists them again
        try (WithProperties properties = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE_DSE_RESTRICTIONS_LOCAL_CHECK_ONLY, true))
        {
            assertThat(messages(Dse6AccessControlCheck.class, Level.INFO, Dse6AccessControlCheck::verifyAfterJoin))
            .noneMatch(message -> message.contains("schema extensions"));
        }
        assertThat(messages(Dse6AccessControlCheck.class, Level.INFO, Dse6AccessControlCheck::recheck))
        .noneMatch(message -> message.contains("schema extensions"));
    }

    @Test
    public void noneModeIgnoresRowLevelRestrictedTables() throws Throwable
    {
        createRowLevelRestrictedTable();
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
        Dse6AccessControlCheck.resetLastCheck();
        assertThat(events(Dse6AccessControlCheck.class, () -> {
            Dse6AccessControlCheck.verifyAtStartup();
            Dse6AccessControlCheck.verifyAfterJoin();
            assertThat(Dse6AccessControlCheck.recheck()).isEmpty();
        })).isEmpty();
        assertThat(Dse6AccessControlCheck.lastCheck()).isNull();
    }

    @Test
    public void noneModeReadsNothing() throws Throwable
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
        Dse6AccessControlCheck.resetLastCheck();
        execute("INSERT INTO system_auth.role_permissions (role, resource, permissions) VALUES ('analyst', 'rows/top_secret/data/ks1/table1', {'SELECT'})");
        Dse6AccessControlCheck.verifyAtStartup();
        // the wait property of the DSE_6 mode is not even read: a wrong value says nothing
        try (WithProperties wait = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE_DSE_RESTRICTIONS_CHECK_WAIT_SECONDS, "two minutes"))
        {
            assertThat(warnings(Dse6AccessControlCheck::verifyAfterJoin)).isEmpty();
        }
        // no check was made: nothing to report
        assertThat(Dse6AccessControlCheck.lastCheck()).isNull();
        assertThat(CassandraAuthorizer.loneDseWriteGrantsSource()).isEqualTo("the rows read since startup; no startup check with internode_compatibility_mode NONE");
    }

    /** The warning of a startup pass that found the two lone grants of {@link #insertLoneWriteGrants}. */
    private static String bothFound(String source)
    {
        return "Found 2 grant(s) of UPDATE or TRUNCATE alone in system_auth.role_permissions (" + source + ')' + DROPPED +
               '[' + ANALYST_UPDATE + ", " + AUDITOR_TRUNCATE + ']' + LONE_GRANTS_REMEDY;
    }

    private void insertLoneWriteGrants() throws Throwable
    {
        execute("INSERT INTO system_auth.role_permissions (role, resource, permissions) VALUES ('analyst', 'data/ks1/t1', {'SELECT', 'UPDATE'})");
        execute("INSERT INTO system_auth.role_permissions (role, resource, permissions) VALUES ('auditor', 'data/ks1', {'TRUNCATE'})");
        // the pair DSE's GRANT ALL writes, read as MODIFY, and a lone name covered by MODIFY: neither is listed
        execute("INSERT INTO system_auth.role_permissions (role, resource, permissions) VALUES ('delegate', 'data/ks1/t1', {'UPDATE', 'TRUNCATE'})");
        execute("INSERT INTO system_auth.role_permissions (role, resource, permissions) VALUES ('delegate', 'data/ks1/t2', {'UPDATE', 'MODIFY'})");
    }

    /**
     * The startup passes list the lone grants for compatstatus through CassandraAuthorizer's parsing, without its
     * warning for each row; the second startup pass logs them in one warning, here from this node's replica.
     */
    @Test
    public void loneWriteGrantsAreListedAndLoggedOnceAtStartup() throws Throwable
    {
        dse6();
        insertLoneWriteGrants();
        assertThat(Dse6AccessControlCheck.scan(null).loneWriteGrants).containsExactly(ANALYST_UPDATE, AUDITOR_TRUNCATE);
        CassandraAuthorizer.resetLoneDseWriteGrants();

        List<String> authorizerWarnings = messages(CassandraAuthorizer.class, Level.WARN, () -> {
            // the first pass lists them, and logs nothing
            assertThat(warnings(Dse6AccessControlCheck::verifyAtStartup)).isEmpty();
            assertThat(CassandraAuthorizer.loneDseWriteGrants()).containsExactly(ANALYST_UPDATE, AUDITOR_TRUNCATE);
            assertThat(CassandraAuthorizer.loneDseWriteGrantsSource()).isEqualTo("the startup check (this node's replica), then the rows read since");

            // the second pass logs them once (here it cannot reach LOCAL_QUORUM, and relies on this node's replica)
            try (WithProperties properties = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE_DSE_RESTRICTIONS_LOCAL_CHECK_ONLY, true))
            {
                assertThat(warnings(Dse6AccessControlCheck::verifyAfterJoin)).containsOnlyOnce(bothFound(LOCAL_ONLY));
            }
            assertThat(CassandraAuthorizer.loneDseWriteGrantsSource()).isEqualTo("the startup check (" + LOCAL_ONLY + "), then the rows read since");
        });
        // not one warning per row as well
        assertThat(authorizerWarnings).noneMatch(message -> message.startsWith("Ignoring the permission '"));

        // the override that accepts an unchecked cluster reads this node's replica again and logs what it lists
        String unchecked = "not checked: the cluster-wide read failed and cassandra.internode_compatibility_mode.ignore_dse_restrictions=true; " +
                           "what this node's replica and the schema hold is listed";
        try (WithProperties properties = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE_IGNORE_DSE_RESTRICTIONS, true))
        {
            assertThat(warnings(Dse6AccessControlCheck::verifyAfterJoin)).containsOnlyOnce(bothFound(unchecked));
        }
        assertThat(CassandraAuthorizer.loneDseWriteGrantsSource()).isEqualTo("the startup check (" + unchecked + "), then the rows read since");

        // when that read fails too (and the schema holds no table restricted to rows), the failure is logged, and what the
        // first pass listed
        RuntimeException broken = new IllegalStateException("broken read");
        try (WithProperties properties = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE_IGNORE_DSE_RESTRICTIONS, true))
        {
            List<ILoggingEvent> events = events(Dse6AccessControlCheck.class, () ->
                                                Dse6AccessControlCheck.verifyAfterJoin(consistency -> { throw broken; }, () -> 0L, nanos -> {}, 0, PAUSE));
            // nothing is dropped knowingly: no "access opens up" line
            assertThat(events).filteredOn(event -> event.getLevel() == Level.WARN).extracting(ILoggingEvent::getFormattedMessage)
                              .containsExactly("unable to read system_auth.role_permissions at LOCAL_QUORUM (broken read), so the DSE access " +
                                               "controls this software does not enforce cannot be checked across the cluster; not checked " +
                                               "(cassandra.internode_compatibility_mode.ignore_dse_restrictions=true: DSE access controls are " +
                                               "dropped knowingly)",
                                               NOT_READ_AGAIN,
                                               bothFound("this node's replica at startup, and the rows read since"));
            assertThat(events).filteredOn(event -> NOT_READ_AGAIN.equals(event.getFormattedMessage())).extracting(ILoggingEvent::getThrowableProxy)
                              .extracting(proxy -> proxy.getMessage()).containsExactly("broken read");
        }
        // the local-only fallback reads the replica through the same scan, and refuses the start when it fails
        try (WithProperties properties = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE_DSE_RESTRICTIONS_LOCAL_CHECK_ONLY, true))
        {
            assertThatThrownBy(() -> Dse6AccessControlCheck.verifyAfterJoin(consistency -> { throw broken; }, () -> 0L, nanos -> {}, 0, PAUSE))
            .isInstanceOf(StartupException.class)
            .hasMessage("internode_compatibility_mode DSE_6 refused: unable to read system_auth.role_permissions on this node " +
                        "(java.lang.IllegalStateException: broken read), so the DSE access controls this software does not enforce cannot be checked")
            .hasCause(broken);
        }
    }

    /**
     * A complete read (the whole table at the authorizer's consistency level) lists the lone grants and forgets the
     * entries of the rows it no longer finds; a read of this node's replica only forgets the rows it reads. A lone
     * grant the periodic check finds first is logged by it, once: not again by the authorizer's read of its row.
     */
    @Test
    public void thePeriodicCheckKeepsTheListCurrent() throws Throwable
    {
        dse6();
        insertLoneWriteGrants();
        joinTheRing();
        assertThat(warnings(Dse6AccessControlCheck::verifyAfterJoin)).containsExactly(bothFound("a LOCAL_QUORUM read"));
        assertThat(CassandraAuthorizer.loneDseWriteGrants()).containsExactly(ANALYST_UPDATE, AUDITOR_TRUNCATE);
        assertThat(CassandraAuthorizer.loneDseWriteGrantsSource()).isEqualTo("the startup check (a LOCAL_QUORUM read), then the rows read since");

        // a DSE node drops the role 'auditor' and revokes UPDATE from 'analyst', whose row is left with no permissions
        execute("DELETE FROM system_auth.role_permissions WHERE role = 'auditor'");
        execute("UPDATE system_auth.role_permissions SET permissions = permissions - {'SELECT', 'UPDATE'} WHERE role = 'analyst' AND resource = 'data/ks1/t1'");
        execute("INSERT INTO system_auth.role_permissions (role, resource, permissions) VALUES ('delegate', 'data/ks2', {'TRUNCATE'})");

        // this node's replica only: the row read with no permissions is forgotten, the deleted one is not; the new one is logged
        leaveTheRing();
        assertThat(warnings(() -> assertThat(Dse6AccessControlCheck.recheck()).isEmpty()))
        .containsExactly("The periodic check of the DSE access controls could not read system_auth.role_permissions at LOCAL_QUORUM (" +
                         LOCAL_QUORUM_FAILED + "); only this node's replica was checked",
                         "The periodic check (" + LOCAL_ONLY + ") found 1 new grant(s) of UPDATE or TRUNCATE alone in " +
                         "system_auth.role_permissions" + DROPPED + '[' + DELEGATE_TRUNCATE + ']' + LONE_GRANTS_REMEDY);
        assertThat(CassandraAuthorizer.loneDseWriteGrants()).containsExactly(AUDITOR_TRUNCATE, DELEGATE_TRUNCATE);
        assertThat(CassandraAuthorizer.loneDseWriteGrantsSource()).isEqualTo("the periodic check (" + LOCAL_ONLY + "), then the rows read since");

        // logged once: the authorizer's read of the row does not log it again
        CassandraAuthorizer authorizer = (CassandraAuthorizer) DatabaseDescriptor.getAuthorizer();
        assertThat(messages(CassandraAuthorizer.class, Level.WARN, () -> authorizer.bulkLoader().get()))
        .noneMatch(message -> message.startsWith("Ignoring the permission 'TRUNCATE' granted alone to role 'delegate'"));

        // conversely, a lone grant the authorizer read first, and logged, is not logged by the periodic check
        execute("INSERT INTO system_auth.role_permissions (role, resource, permissions) VALUES ('delegate', 'data/ks3', {'UPDATE'})");
        assertThat(messages(CassandraAuthorizer.class, Level.WARN, () -> authorizer.bulkLoader().get()))
        .containsOnlyOnce("Ignoring the permission 'UPDATE' granted alone to role 'delegate' on resource 'data/ks3' in system_auth.role_permissions: " +
                          "DataStax Enterprise grants it without TRUNCATE, and this software has MODIFY only, which grants both, so the role " +
                          "loses inserts, updates and deletes. GRANT MODIFY if the role should also truncate, or accept the loss (logged once " +
                          "per role and resource; nodetool compatstatus lists them)");

        // a complete read forgets the deleted row too, and logs nothing new: compatstatus lists them
        joinTheRing();
        assertThat(warnings(() -> assertThat(Dse6AccessControlCheck.recheck()).isEmpty())).isEmpty();
        assertThat(CassandraAuthorizer.loneDseWriteGrants()).containsExactly(DELEGATE_TRUNCATE, "role 'delegate' on data/ks3: UPDATE");
        assertThat(CassandraAuthorizer.loneDseWriteGrantsSource()).isEqualTo("the periodic check (a LOCAL_QUORUM read), then the rows read since");

        // a new one found by a complete read is logged too, with its source
        execute("INSERT INTO system_auth.role_permissions (role, resource, permissions) VALUES ('auditor', 'data/ks4', {'TRUNCATE'})");
        assertThat(warnings(() -> assertThat(Dse6AccessControlCheck.recheck()).isEmpty()))
        .containsExactly("The periodic check (a LOCAL_QUORUM read) found 1 new grant(s) of UPDATE or TRUNCATE alone in system_auth.role_permissions" +
                         DROPPED + "[role 'auditor' on data/ks4: TRUNCATE]" + LONE_GRANTS_REMEDY);
    }

    /**
     * The rows of {@code rows}, then {@code fail} run by {@code hasNext()} (it throws), as a paged read whose next page
     * cannot be read fails.
     */
    private static Iterable<UntypedResultSet.Row> failingAfter(UntypedResultSet rows, Runnable fail)
    {
        return () -> new Iterator<>()
        {
            private final Iterator<UntypedResultSet.Row> iterator = rows.iterator();

            public boolean hasNext()
            {
                if (iterator.hasNext())
                    return true;
                fail.run();
                return false;
            }

            public UntypedResultSet.Row next()
            {
                return iterator.next();
            }
        };
    }

    /** A scan that fails part-way forgets the lone grants it listed first: the next periodic check logs them. */
    @Test
    public void aScanThatFailsPartWayForgetsWhatItListed() throws Throwable
    {
        dse6();
        insertLoneWriteGrants();
        UntypedResultSet analyst = execute("SELECT role, resource, permissions FROM system_auth.role_permissions WHERE role = 'analyst'");
        Findings findings = new Findings();
        IllegalStateException failure = new IllegalStateException("the next page cannot be read");
        assertThatThrownBy(() -> Dse6AccessControlCheck.readRows(failingAfter(analyst, () -> { throw failure; }), findings, false, false, true)).isSameAs(failure);
        assertThat(findings.newLoneWriteGrants).containsExactly(ANALYST_UPDATE);
        assertThat(CassandraAuthorizer.loneDseWriteGrants()).isEmpty();

        // an Error too
        Findings again = new Findings();
        AssertionError error = new AssertionError("broken read");
        assertThatThrownBy(() -> Dse6AccessControlCheck.readRows(failingAfter(analyst, () -> { throw error; }), again, false, false, true)).isSameAs(error);
        assertThat(again.newLoneWriteGrants).containsExactly(ANALYST_UPDATE);
        assertThat(CassandraAuthorizer.loneDseWriteGrants()).isEmpty();

        // the next check lists them first again, and logs them
        assertThat(warnings(() -> assertThat(Dse6AccessControlCheck.recheck()).isEmpty()))
        .containsExactly("The periodic check of the DSE access controls could not read system_auth.role_permissions at LOCAL_QUORUM (" +
                         LOCAL_QUORUM_FAILED + "); only this node's replica was checked",
                         "The periodic check (" + LOCAL_ONLY + ") found 2 new grant(s) of UPDATE or TRUNCATE alone in system_auth.role_permissions" +
                         DROPPED + '[' + ANALYST_UPDATE + ", " + AUDITOR_TRUNCATE + ']' + LONE_GRANTS_REMEDY);
    }

    /**
     * The warning names the first 20 lone grants and counts the others; that there are none is said at INFO after a
     * complete read only.
     */
    @Test
    public void theLoneWriteGrantsWarningIsBounded() throws Throwable
    {
        dse6();
        try (WithProperties properties = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE_DSE_RESTRICTIONS_LOCAL_CHECK_ONLY, true))
        {
            assertThat(messages(Dse6AccessControlCheck.class, Level.INFO, Dse6AccessControlCheck::verifyAfterJoin))
            .noneMatch(message -> message.startsWith("No grant of UPDATE or TRUNCATE alone"));
        }
        joinTheRing();
        assertThat(messages(Dse6AccessControlCheck.class, Level.INFO, Dse6AccessControlCheck::verifyAfterJoin))
        .containsOnlyOnce("No grant of UPDATE or TRUNCATE alone found in system_auth.role_permissions (a LOCAL_QUORUM read)");

        List<String> lone = new ArrayList<>();
        for (int i = 0; i < Dse6AccessControlCheck.LONE_GRANTS_LOGGED + 1; i++)
        {
            String role = String.format("dse_bounded_%02d", i);
            rolesToDelete.add(role);
            execute("INSERT INTO system_auth.role_permissions (role, resource, permissions) VALUES (?, 'data/ks1', {'TRUNCATE'})", role);
            lone.add("role '" + role + "' on data/ks1: TRUNCATE");
        }
        assertThat(warnings(Dse6AccessControlCheck::verifyAfterJoin))
        .containsExactly("Found 21 grant(s) of UPDATE or TRUNCATE alone in system_auth.role_permissions (a LOCAL_QUORUM read)" +
                         DROPPED + lone.subList(0, 20) + " and 1 more" + LONE_GRANTS_REMEDY);
        assertThat(CassandraAuthorizer.loneDseWriteGrants()).isEqualTo(lone);
    }

    /** With another authorizer the lone grants are not dropped: the scans neither list nor log them. */
    @Test
    public void loneWriteGrantsAreIgnoredWithAnotherAuthorizer() throws Throwable
    {
        dse6();
        insertLoneWriteGrants();
        joinTheRing();
        IAuthorizer authorizer = DatabaseDescriptor.getAuthorizer();
        DatabaseDescriptor.setAuthorizer(new AllowAllAuthorizer());
        try
        {
            assertThat(Dse6AccessControlCheck.scan(null).loneWriteGrants).isEmpty();
            assertThat(messages(Dse6AccessControlCheck.class, Level.INFO, Dse6AccessControlCheck::verifyAfterJoin)).isEmpty();
            assertThat(warnings(Dse6AccessControlCheck::verifyAfterJoin)).isEmpty();
            assertThat(warnings(() -> assertThat(Dse6AccessControlCheck.recheck()).isEmpty())).isEmpty();
        }
        finally
        {
            DatabaseDescriptor.setAuthorizer(authorizer);
        }
        assertThat(CassandraAuthorizer.loneDseWriteGrants()).isEmpty();
        assertThat(CassandraAuthorizer.loneDseWriteGrantsSource()).isEqualTo("startup check not run yet; only the rows read so far");
    }

    // ---- the retries of the cluster-wide read ----

    private static final long PAUSE = SECONDS.toNanos(Dse6AccessControlCheck.RETRY_INTERVAL_SECONDS);
    private static final String TIMED_OUT = "Operation timed out - received only 0 responses.";

    /** A clock the pauses and the reads move forward, recording the pauses. */
    private static final class FakeTime implements LongConsumer
    {
        final AtomicLong now = new AtomicLong();
        final List<Long> pauses = new ArrayList<>();

        @Override
        public void accept(long nanos)
        {
            pauses.add(nanos);
            now.addAndGet(nanos);
        }
    }

    /**
     * A cluster-wide read that takes {@code readNanos} of {@code time}, throws the given failures in turn, then returns
     * {@code findings}, or times out for ever when there are none; counts its attempts.
     */
    private static final class ScriptedRead implements Function<ConsistencyLevel, Findings>
    {
        final Iterator<RuntimeException> failures;
        final Findings findings;
        final FakeTime time;
        final long readNanos;
        int attempts;

        ScriptedRead(List<RuntimeException> failures, Findings findings, FakeTime time, long readNanos)
        {
            this.failures = failures.iterator();
            this.findings = findings;
            this.time = time;
            this.readNanos = readNanos;
        }

        ScriptedRead(List<RuntimeException> failures, Findings findings, FakeTime time)
        {
            this(failures, findings, time, 0);
        }

        @Override
        public Findings apply(ConsistencyLevel consistency)
        {
            attempts++;
            time.now.addAndGet(readNanos);
            assertThat(consistency).isEqualTo(ConsistencyLevel.LOCAL_QUORUM);
            if (failures.hasNext())
                throw failures.next();
            if (findings == null)
                throw new ReadTimeoutException(ConsistencyLevel.LOCAL_QUORUM, 0, 2, false);
            return findings;
        }
    }

    private static String retryWarning(int attempt, String failure, long waitSeconds)
    {
        return "The startup check of the DSE access controls could not read system_auth.role_permissions at LOCAL_QUORUM (attempt " + attempt +
               ": " + failure + "); retrying in " + Dse6AccessControlCheck.RETRY_INTERVAL_SECONDS + " s, for up to " + waitSeconds + " s " +
               "(cassandra.internode_compatibility_mode.dse_restrictions_check_wait_seconds): the replicas of system_auth may not be reachable yet";
    }

    private static String unableToRead(String failure, String tried)
    {
        return "unable to read system_auth.role_permissions at LOCAL_QUORUM (" + failure + ')' + tried + ", so the DSE access controls this " +
               "software does not enforce cannot be checked across the cluster";
    }

    private static String refusal(String failure, String tried)
    {
        return "internode_compatibility_mode DSE_6 refused: " + unableToRead(failure, tried) + "; make the replicas of system_auth reachable, " +
               "or start with -Dcassandra.internode_compatibility_mode.dse_restrictions_local_check_only=true to rely on the check of this node's " +
               "replica alone";
    }

    /** Replicas not reachable yet, then a timeout, then the read succeeds: the pass waits for it and checks as usual. */
    @Test
    public void aClusterWideReadFailingOnTheRequestIsRetriedUntilTheReplicasAnswer() throws Throwable
    {
        dse6();
        FakeTime time = new FakeTime();
        ScriptedRead read = new ScriptedRead(List.of(UnavailableException.create(ConsistencyLevel.LOCAL_QUORUM, 2, 1),
                                                     new ReadTimeoutException(ConsistencyLevel.LOCAL_QUORUM, 0, 2, false)),
                                             new Findings(), time);
        List<String> infos = new ArrayList<>();
        List<String> warnings = warnings(() -> infos.addAll(messages(Dse6AccessControlCheck.class, Level.INFO,
                                                                     () -> Dse6AccessControlCheck.verifyAfterJoin(read, time.now::get, time,
                                                                                                                  SECONDS.toNanos(120), PAUSE))));
        assertThat(read.attempts).isEqualTo(3);
        assertThat(time.pauses).containsExactly(PAUSE, PAUSE);
        assertThat(warnings).containsExactly(retryWarning(1, LOCAL_QUORUM_FAILED, 120), retryWarning(2, TIMED_OUT, 120));
        assertThat(infos).contains("The startup check of the DSE access controls read system_auth.role_permissions at LOCAL_QUORUM on attempt 3");
        assertThat(Dse6AccessControlCheck.lastCheck().source).isEqualTo("a LOCAL_QUORUM read");
    }

    /** What the read finds once it succeeds after retries is refused as always. */
    @Test
    public void aClusterWideReadThatSucceedsAfterRetriesStillRefusesWhatItFinds()
    {
        dse6();
        Findings findings = new Findings();
        findings.rowLevelGrants.add("role 'analyst' on rows/top_secret/data/ks1/table1: [SELECT]");
        FakeTime time = new FakeTime();
        ScriptedRead read = new ScriptedRead(List.of(new ReadTimeoutException(ConsistencyLevel.LOCAL_QUORUM, 1, 2, false)), findings, time);
        assertThatThrownBy(() -> Dse6AccessControlCheck.verifyAfterJoin(read, time.now::get, time, SECONDS.toNanos(120), PAUSE))
        .isInstanceOf(StartupException.class)
        .hasMessage("internode_compatibility_mode DSE_6 refused: DSE access controls that this software does not enforce would grant access " +
                    "if ignored (found by a LOCAL_QUORUM read): row-level grants [role 'analyst' on rows/top_secret/data/ks1/table1: [SELECT]]. " +
                    REMEDY);
        assertThat(read.attempts).isEqualTo(2);
    }

    /**
     * A read still failing when the next attempt would start after the wait refuses the start as before, naming the
     * attempts; an attempt that would start exactly at the end of the wait is still made.
     */
    @Test
    public void aClusterWideReadStillFailingAfterTheWaitRefusesTheStart() throws Throwable
    {
        dse6();
        // attempts at 0, 5 and 10 s: the one at 10 s starts exactly at the end of the wait, one at 15 s would start after it
        FakeTime time = new FakeTime();
        ScriptedRead read = new ScriptedRead(List.of(), null, time);
        assertThatThrownBy(() -> Dse6AccessControlCheck.verifyAfterJoin(read, time.now::get, time, SECONDS.toNanos(10), PAUSE))
        .isInstanceOf(StartupException.class)
        .hasMessage(refusal(TIMED_OUT, " after 3 attempts in 10 s"))
        .hasCauseInstanceOf(ReadTimeoutException.class);
        assertThat(read.attempts).isEqualTo(3);
        assertThat(time.pauses).containsExactly(PAUSE, PAUSE);
    }

    /** A wait of 0 makes a single attempt, whatever the pause and the clock. */
    @Test
    public void aWaitOfZeroMakesASingleAttempt()
    {
        dse6();
        FakeTime time = new FakeTime();
        ScriptedRead read = new ScriptedRead(List.of(), null, time);
        assertThatThrownBy(() -> Dse6AccessControlCheck.verifyAfterJoin(read, time.now::get, time, 0, 0))
        .isInstanceOf(StartupException.class)
        .hasMessage(refusal(TIMED_OUT, ""));
        assertThat(read.attempts).isEqualTo(1);
        assertThat(time.pauses).isEmpty();
    }

    /** The time the reads themselves take counts: with 5 s timeouts, attempts start at 0, 10 and 20 s within a 22 s wait. */
    @Test
    public void theTimeOfTheReadsCountsInTheWait()
    {
        dse6();
        FakeTime time = new FakeTime();
        ScriptedRead read = new ScriptedRead(List.of(), null, time, SECONDS.toNanos(5));
        assertThatThrownBy(() -> Dse6AccessControlCheck.verifyAfterJoin(read, time.now::get, time, SECONDS.toNanos(22), PAUSE))
        .isInstanceOf(StartupException.class)
        .hasMessage(refusal(TIMED_OUT, " after 3 attempts in 25 s"));
        assertThat(read.attempts).isEqualTo(3);
        assertThat(time.pauses).containsExactly(PAUSE, PAUSE);
    }

    /** A read failing otherwise than on the request, or aborted by the guardrails, is a real failure: refused at once, never retried. */
    @Test
    public void aClusterWideReadFailingOtherwiseIsNotRetried()
    {
        dse6();
        RuntimeException aborted = new TombstoneAbortException("too many tombstones", 1, 100_001, false, ConsistencyLevel.LOCAL_QUORUM, 0, 2, Map.of());
        for (RuntimeException failure : List.of(new IllegalStateException("unreadable row"), aborted))
        {
            FakeTime time = new FakeTime();
            ScriptedRead read = new ScriptedRead(List.of(failure), new Findings(), time);
            assertThatThrownBy(() -> Dse6AccessControlCheck.verifyAfterJoin(read, time.now::get, time, SECONDS.toNanos(120), PAUSE))
            .isInstanceOf(StartupException.class)
            .hasMessage(refusal(failure.getMessage(), ""))
            .hasCause(failure);
            assertThat(read.attempts).isEqualTo(1);
            assertThat(time.pauses).isEmpty();
        }
    }

    /** The WARN of a {@code DSE_5_1} start on this node's replica after the cluster-wide read failed. */
    private static String dse51FallbackWarning(String failure, String tried)
    {
        return "The startup check of the DSE access controls could not read system_auth.role_permissions at " +
               "LOCAL_QUORUM (" + failure + ')' + tried + "; with internode_compatibility_mode DSE_5_1 only this " +
               "node's replica was checked, since DSE 5.1 has no RESTRICT and no delegation and the restrictions DSE " +
               "5.1 applies on this node are in the schema this node checked; the periodic check reads the whole " +
               "table again every 10 minutes";
    }

    /** The cluster-wide read of {@code read}, and this node's replica read as the pass reads it. */
    private static Function<ConsistencyLevel, Findings> orThisNodesReplica(ScriptedRead read)
    {
        return consistency -> consistency == null ? Dse6AccessControlCheck.scan(null) : read.apply(consistency);
    }

    /** {@code system_auth.role_permissions} as DSE 5.1 has it: without the columns of DSE 6. */
    private void removeDseColumns() throws Throwable
    {
        for (String column : List.of("restricted", "grantables"))
        {
            TableMetadata table = Schema.instance.getTableMetadata(SchemaConstants.AUTH_KEYSPACE_NAME,
                                                                   AuthKeyspace.ROLE_PERMISSIONS);
            if (table.getColumn(ColumnIdentifier.getInterned(column, true)) != null)
                execute("ALTER TABLE system_auth.role_permissions DROP " + column);
        }
    }

    /**
     * In DSE_5_1, a cluster-wide read still failing on the request at the end of the wait is followed by the check of
     * this node's replica, and the node starts with a WARN of its own; compatstatus' source says the cluster-wide read
     * failed. What the replica holds is still refused.
     */
    @Test
    public void dse51AClusterWideReadStillFailingAfterTheWaitFallsBackToThisNodesReplica() throws Throwable
    {
        removeDseColumns();
        dse51();
        FakeTime time = new FakeTime();
        ScriptedRead read = new ScriptedRead(List.of(), null, time);
        Function<ConsistencyLevel, Findings> scan = orThisNodesReplica(read);
        List<String> warnings = warnings(() -> Dse6AccessControlCheck.verifyAfterJoin(scan, time.now::get, time,
                                                                                      SECONDS.toNanos(10), PAUSE));
        assertThat(read.attempts).isEqualTo(3);
        assertThat(time.pauses).containsExactly(PAUSE, PAUSE);
        assertThat(warnings).containsExactly(retryWarning(1, TIMED_OUT, 10), retryWarning(2, TIMED_OUT, 10),
                                             dse51FallbackWarning(TIMED_OUT, " after 3 attempts in 10 s"));
        assertThat(Dse6AccessControlCheck.lastCheck().source)
        .isEqualTo("this node's replica only: the cluster-wide read failed (" + TIMED_OUT + ')');
        assertThat(Dse6AccessControlCheck.lastCheck().findings).isEmpty();

        // a single attempt with a wait of 0, the same fallback
        FakeTime once = new FakeTime();
        RuntimeException unavailable = UnavailableException.create(ConsistencyLevel.LOCAL_QUORUM, 2, 1);
        ScriptedRead single = new ScriptedRead(List.of(unavailable), new Findings(), once);
        Function<ConsistencyLevel, Findings> singleScan = orThisNodesReplica(single);
        assertThat(warnings(() -> Dse6AccessControlCheck.verifyAfterJoin(singleScan, once.now::get, once, 0, 0)))
        .containsExactly(dse51FallbackWarning(LOCAL_QUORUM_FAILED, ""));
        assertThat(single.attempts).isEqualTo(1);

        // a row-level grant held on this node's replica is refused, with the text of the local-only fallback
        execute("INSERT INTO system_auth.role_permissions (role, resource, permissions) " +
                "VALUES ('analyst', 'rows/top_secret/data/ks1/table1', {'SELECT'})");
        FakeTime again = new FakeTime();
        Function<ConsistencyLevel, Findings> refusedScan = orThisNodesReplica(new ScriptedRead(List.of(), null, again));
        assertThatThrownBy(() -> Dse6AccessControlCheck.verifyAfterJoin(refusedScan, again.now::get, again, 0, 0))
        .isInstanceOf(StartupException.class)
        .hasMessageStartingWith("internode_compatibility_mode DSE_5_1 refused: DSE access controls that this " +
                                "software does not enforce would grant access if ignored (found by this node's " +
                                "replica only: the cluster-wide read failed (" + TIMED_OUT + ")): row-level grants " +
                                "[role 'analyst' on rows/top_secret/data/ks1/table1: [SELECT]]. " + STALE);
    }

    /**
     * In DSE_5_1, a permissions table with a column of DSE 6 ({@code restricted} or {@code grantables}, each alone or
     * both) keeps the refusal of a cluster-wide read that cannot be made, which says why: a RESTRICT or a delegation
     * held on another replica only would go unseen.
     */
    @Test
    public void dse51KeepsTheRefusalWhenThePermissionsTableHasTheColumnsOfDse6() throws Throwable
    {
        String dse6Shaped = "; this node's system_auth.role_permissions has the restricted or grantables column of " +
                            "DSE 6, so this node's replica alone is not relied on";
        for (String columns : List.of("restricted set<text>", "grantables set<text>",
                                      "restricted set<text>, grantables set<text>"))
        {
            removeDseColumns();
            execute("ALTER TABLE system_auth.role_permissions ADD (" + columns + ')');
            try
            {
                dse51();
                FakeTime time = new FakeTime();
                ScriptedRead read = new ScriptedRead(List.of(), null, time);
                Function<ConsistencyLevel, Findings> scan = orThisNodesReplica(read);
                String refusal = refusal(TIMED_OUT, " after 3 attempts in 10 s").replace("DSE_6 refused",
                                                                                         "DSE_5_1 refused");
                String unable = unableToRead(TIMED_OUT, " after 3 attempts in 10 s");
                assertThatThrownBy(() -> Dse6AccessControlCheck.verifyAfterJoin(scan, time.now::get, time,
                                                                                SECONDS.toNanos(10), PAUSE))
                .as(columns)
                .isInstanceOf(StartupException.class)
                .hasMessage(refusal.replace(unable, unable + dse6Shaped))
                .hasCauseInstanceOf(ReadTimeoutException.class);
                assertThat(read.attempts).isEqualTo(3);
                // no start on this node's replica was recorded
                assertThat(Dse6AccessControlCheck.lastCheck()).as(columns).isNull();
            }
            finally
            {
                // the schema is frozen in the mode
                DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
                removeDseColumns();
            }
        }
    }

    /**
     * DSE_6 keeps its refusal of a cluster-wide read that cannot be made on a permissions table of DSE 5.1's shape too:
     * the fallback after the wait is DSE_5_1's, whatever the table.
     */
    @Test
    public void dse6RefusesTheStartOnAPermissionsTableOfDse51sShape() throws Throwable
    {
        removeDseColumns();
        dse6();
        FakeTime time = new FakeTime();
        ScriptedRead read = new ScriptedRead(List.of(), null, time);
        Function<ConsistencyLevel, Findings> scan = orThisNodesReplica(read);
        long wait = SECONDS.toNanos(10);
        String refused = refusal(TIMED_OUT, " after 3 attempts in 10 s");
        List<String> warnings = warnings(() -> {
            assertThatThrownBy(() -> Dse6AccessControlCheck.verifyAfterJoin(scan, time.now::get, time, wait, PAUSE))
            .isInstanceOf(StartupException.class)
            .hasMessage(refused);
        });
        assertThat(read.attempts).isEqualTo(3);
        // the retries only, no WARN of a start on this node's replica
        assertThat(warnings).containsExactly(retryWarning(1, TIMED_OUT, 10), retryWarning(2, TIMED_OUT, 10));
        assertThat(Dse6AccessControlCheck.lastCheck()).isNull();
    }

    /**
     * In DSE_5_1 the cluster-wide read is taken when it succeeds, after retries; one failing otherwise than on the
     * request (aborted by the guardrails, or any other error) still refuses the start, never falling back.
     */
    @Test
    public void dse51TakesTheClusterWideReadAndRefusesItsOtherFailures() throws Throwable
    {
        removeDseColumns();
        dse51();
        FakeTime time = new FakeTime();
        ScriptedRead read = new ScriptedRead(List.of(UnavailableException.create(ConsistencyLevel.LOCAL_QUORUM, 2, 1)),
                                             new Findings(), time);
        Function<ConsistencyLevel, Findings> scan = orThisNodesReplica(read);
        List<String> warnings = warnings(() -> Dse6AccessControlCheck.verifyAfterJoin(scan, time.now::get, time,
                                                                                      SECONDS.toNanos(120), PAUSE));
        assertThat(read.attempts).isEqualTo(2);
        assertThat(warnings).containsExactly(retryWarning(1, LOCAL_QUORUM_FAILED, 120));
        assertThat(Dse6AccessControlCheck.lastCheck().source).isEqualTo("a LOCAL_QUORUM read");

        RuntimeException aborted = new TombstoneAbortException("too many tombstones", 1, 100_001, false,
                                                               ConsistencyLevel.LOCAL_QUORUM, 0, 2, Map.of());
        for (RuntimeException failure : List.of(new IllegalStateException("unreadable row"), aborted))
        {
            FakeTime once = new FakeTime();
            ScriptedRead failing = new ScriptedRead(List.of(failure), new Findings(), once);
            Function<ConsistencyLevel, Findings> failingScan = orThisNodesReplica(failing);
            assertThatThrownBy(() -> Dse6AccessControlCheck.verifyAfterJoin(failingScan, once.now::get, once,
                                                                            SECONDS.toNanos(120), PAUSE))
            .isInstanceOf(StartupException.class)
            .hasMessage(refusal(failure.getMessage(), "").replace("DSE_6 refused", "DSE_5_1 refused"))
            .hasCause(failure);
            assertThat(failing.attempts).isEqualTo(1);
        }
    }

    /**
     * The real pass, with the wait property: retried (one pause of 5 s within a 7 s wait) then refused on this node,
     * which never joined a ring; not retried at all when the local-only or ignore property says what to do; an invalid
     * value refuses the start with the property named. The declared default is 120 seconds.
     */
    @Test
    public void theWaitPropertyOfTheRealPass() throws Throwable
    {
        dse6();
        assertThat(INTERNODE_COMPATIBILITY_MODE_DSE_RESTRICTIONS_CHECK_WAIT_SECONDS.getDefaultValue()).isEqualTo("120");
        try (WithProperties wait = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE_DSE_RESTRICTIONS_CHECK_WAIT_SECONDS, 7))
        {
            assertThatThrownBy(Dse6AccessControlCheck::verifyAfterJoin)
            .isInstanceOf(StartupException.class)
            .hasMessageStartingWith("internode_compatibility_mode DSE_6 refused: unable to read system_auth.role_permissions at LOCAL_QUORUM (" +
                                    LOCAL_QUORUM_FAILED + ") after 2 attempts in ");
            for (CassandraRelevantProperties property : new CassandraRelevantProperties[]{ INTERNODE_COMPATIBILITY_MODE_DSE_RESTRICTIONS_LOCAL_CHECK_ONLY,
                                                                                            INTERNODE_COMPATIBILITY_MODE_IGNORE_DSE_RESTRICTIONS })
            {
                try (WithProperties fallback = new WithProperties().set(property, true))
                {
                    List<String> warnings = warnings(Dse6AccessControlCheck::verifyAfterJoin);
                    assertThat(warnings).as(property.getKey()).noneMatch(line -> line.contains("retrying in"));
                    assertThat(warnings).as(property.getKey()).anyMatch(line -> line.startsWith(unableToRead(LOCAL_QUORUM_FAILED, "")));
                }
            }
        }
        // a negative value makes a single attempt
        try (WithProperties wait = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE_DSE_RESTRICTIONS_CHECK_WAIT_SECONDS, -5))
        {
            assertThat(warnings(() -> assertThatThrownBy(Dse6AccessControlCheck::verifyAfterJoin)
                                      .isInstanceOf(StartupException.class)
                                      .hasMessageStartingWith("internode_compatibility_mode DSE_6 refused: unable to read system_auth.role_permissions " +
                                                              "at LOCAL_QUORUM (" + LOCAL_QUORUM_FAILED + "), so the DSE access controls")))
            .noneMatch(line -> line.contains("retrying in"));
        }
        // a value that is not a decimal number of seconds, whether or not a fallback property makes the wait unused; the first
        // pass refuses it already, before the node joins the ring
        for (String invalid : new String[]{ "two minutes", "0x78" })
        {
            try (WithProperties wait = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE_DSE_RESTRICTIONS_CHECK_WAIT_SECONDS, invalid))
            {
                String refused = "internode_compatibility_mode DSE_6 refused: invalid value '" + invalid + "' for " +
                                 "cassandra.internode_compatibility_mode.dse_restrictions_check_wait_seconds: a number of seconds is expected";
                assertThatThrownBy(Dse6AccessControlCheck::verifyAfterJoin).isInstanceOf(StartupException.class).hasMessage(refused);
                assertThatThrownBy(Dse6AccessControlCheck::verifyAtStartup).isInstanceOf(StartupException.class).hasMessage(refused);
                for (CassandraRelevantProperties fallback : new CassandraRelevantProperties[]{ INTERNODE_COMPATIBILITY_MODE_DSE_RESTRICTIONS_LOCAL_CHECK_ONLY,
                                                                                                INTERNODE_COMPATIBILITY_MODE_IGNORE_DSE_RESTRICTIONS })
                {
                    try (WithProperties set = new WithProperties().set(fallback, true))
                    {
                        assertThatThrownBy(Dse6AccessControlCheck::verifyAfterJoin).as(fallback.getKey()).isInstanceOf(StartupException.class).hasMessage(refused);
                    }
                }
            }
        }
    }

    /**
     * With the override, the controls a DSE node adds after the start are dropped knowingly too: the periodic check logs
     * them at WARN, once, and never at ERROR.
     */
    @Test
    public void withTheOverrideThePeriodicCheckWarnsOfTheControlsAddedSince() throws Throwable
    {
        dse6();
        joinTheRing();
        try (WithProperties properties = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE_IGNORE_DSE_RESTRICTIONS, true))
        {
            Dse6AccessControlCheck.verifyAtStartup();
            Dse6AccessControlCheck.verifyAfterJoin();
            execute("INSERT INTO system_auth.role_permissions (role, resource, permissions) VALUES ('analyst', 'rows/top_secret/data/ks1/table1', {'SELECT'})");
            String grant = "row-level grant role 'analyst' on rows/top_secret/data/ks1/table1: [SELECT]";
            List<ILoggingEvent> first = events(Dse6AccessControlCheck.class, Dse6AccessControlCheck::recheck);
            assertThat(first).noneMatch(e -> e.getLevel() == Level.ERROR);
            assertThat(first).filteredOn(e -> e.getLevel() == Level.WARN).extracting(ILoggingEvent::getFormattedMessage)
                             .containsExactly("The periodic check (a LOCAL_QUORUM read) found DSE access controls this software does not enforce, " +
                                              "dropped knowingly (cassandra.internode_compatibility_mode.ignore_dse_restrictions=true) and access " +
                                              "opens up accordingly: [" + grant + ']');
            assertThat(Dse6AccessControlCheck.lastCheck().findings).containsExactly(grant);
            // logged once: the next periodic check finds nothing new
            assertThat(events(Dse6AccessControlCheck.class, Dse6AccessControlCheck::recheck))
            .noneMatch(e -> e.getLevel() == Level.ERROR || e.getLevel() == Level.WARN);

            // what a startup pass dropped and logged is not logged again by the periodic check
            Dse6AccessControlCheck.resetLastCheck();
            Dse6AccessControlCheck.verifyAtStartup();
            assertThat(events(Dse6AccessControlCheck.class, Dse6AccessControlCheck::recheck))
            .noneMatch(e -> e.getLevel() == Level.ERROR || e.getLevel() == Level.WARN);
        }
    }

    /** Each property of the check that is set is logged at WARN by every start, whatever the check finds. */
    @Test
    public void theOverridesSetAreLoggedAtEveryStart() throws Throwable
    {
        dse6();
        assertThat(warnings(Dse6AccessControlCheck::verifyAtStartup)).isEmpty();
        try (WithProperties properties = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE_IGNORE_DSE_RESTRICTIONS, true))
        {
            assertThat(warnings(Dse6AccessControlCheck::verifyAtStartup))
            .containsExactly(OVERRIDE_SET);
        }
        try (WithProperties properties = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE_DSE_RESTRICTIONS_LOCAL_CHECK_ONLY, true))
        {
            assertThat(warnings(Dse6AccessControlCheck::verifyAtStartup))
            .containsExactly("cassandra.internode_compatibility_mode.dse_restrictions_local_check_only=true is set: when the cluster-wide check of " +
                             "the DSE access controls fails, only this node's replica is checked, and the cluster-wide check is not retried; " +
                             "remove it at the next restart");
        }
    }

    /**
     * A row of a {@code rows/} resource with no permissions grants nothing, so it is not refused as a row-level grant:
     * REVOKE could not remove it. Its other cells are still checked.
     */
    @Test
    public void aRowLevelResourceWithoutPermissionsIsNotAGrant() throws Throwable
    {
        addDseColumns();
        dse6();
        execute("INSERT INTO system_auth.role_permissions (role, resource, grantables) VALUES ('delegate', 'rows/top_secret/data/ks1/table1', {'SELECT'})");
        Findings findings = Dse6AccessControlCheck.scan(null);
        assertThat(findings.rowLevelGrants).isEmpty();
        assertThat(findings.delegations).containsExactly("role 'delegate' on rows/top_secret/data/ks1/table1: [SELECT]");
        Dse6AccessControlCheck.verifyAtStartup();
    }

    /**
     * Without {@code system_auth.role_permissions} nothing can be checked: the second startup pass refuses unless the
     * override is set (the first one runs before a new node has received the schema, and records the check as not
     * made), and the periodic check records the check as not made, which compatstatus shows as unknown. The table
     * cannot be dropped here, so the scans are stand-ins with the flag {@code scan} sets.
     */
    @Test
    public void aMissingPermissionsTableIsNotAnAllClear() throws Throwable
    {
        dse6();
        Function<ConsistencyLevel, Findings> missing = consistency -> {
            Findings findings = new Findings();
            findings.permissionsTableMissing = true;
            return findings;
        };
        // the first pass does not refuse: a new node may not have received the schema yet
        Dse6AccessControlCheck.verifyAtStartup(missing);
        assertThat(Dse6AccessControlCheck.lastCheck().source)
        .isEqualTo("not checked: system_auth.role_permissions is not in the schema (this node's replica)");

        String refusal = "internode_compatibility_mode DSE_6 refused: system_auth.role_permissions is not in the schema, so the DSE access " +
                         "controls this software does not enforce cannot be checked; start with " + OVERRIDE + " to start without that check";
        assertThatThrownBy(() -> Dse6AccessControlCheck.verifyAfterJoin(missing, () -> 0L, nanos -> {}, 0, PAUSE))
        .isInstanceOf(StartupException.class)
        .hasMessage(refusal);
        String source = "not checked: system_auth.role_permissions is not in the schema (a LOCAL_QUORUM read)";
        assertThat(Dse6AccessControlCheck.lastCheck().source).isEqualTo(source);
        try (WithProperties properties = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE_IGNORE_DSE_RESTRICTIONS, true))
        {
            Dse6AccessControlCheck.verifyAfterJoin(missing, () -> 0L, nanos -> {}, 0, PAUSE);
        }
        assertThat(Dse6AccessControlCheck.recheck(missing)).isEmpty();
        assertThat(Dse6AccessControlCheck.lastCheck().source).isEqualTo(source).startsWith(Dse6AccessControlCheck.NOT_CHECKED);
    }

    /**
     * With the override, a control dropped knowingly that a DSE node removes and adds again is logged again by the
     * periodic check.
     */
    @Test
    public void withTheOverrideAControlAddedAgainIsLoggedAgain() throws Throwable
    {
        dse6();
        joinTheRing();
        try (WithProperties properties = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE_IGNORE_DSE_RESTRICTIONS, true))
        {
            Dse6AccessControlCheck.verifyAtStartup();
            String insert = "INSERT INTO system_auth.role_permissions (role, resource, permissions) VALUES ('analyst', 'rows/top_secret/data/ks1/table1', {'SELECT'})";
            execute(insert);
            assertThat(warnings(Dse6AccessControlCheck::recheck)).hasSize(1);
            execute("DELETE FROM system_auth.role_permissions WHERE role = 'analyst'");
            assertThat(warnings(Dse6AccessControlCheck::recheck)).isEmpty();
            execute(insert);
            assertThat(warnings(Dse6AccessControlCheck::recheck)).hasSize(1);
        }
    }

    /** A role or resource name that could break or disguise a log line is shown in hexadecimal. */
    @Test
    public void unsafeRoleAndResourceNamesAreShownInHexadecimal()
    {
        assertThat(Dse6AccessControlCheck.permissionsEntry("a\nb", "rows/x\u202e/data/ks/t", Set.of("SELECT")))
        .isEqualTo("role '0x610a62 (bytes, not text)' on 0x726f77732f78e280ae2f646174612f6b732f74 (bytes, not text): [SELECT]");
        assertThat(Dse6AccessControlCheck.permissionsEntry("analyst", "data/ks1", Set.of("SELECT", "A\rB")))
        .isEqualTo("role 'analyst' on data/ks1: [0x410d42 (bytes, not text), SELECT]");
        // a long one is cut, as the tenant column is
        String longName = "\n" + "r".repeat(100);
        assertThat(Dse6AccessControlCheck.permissionsEntry(longName, "data/ks1", Set.of("SELECT")))
        .startsWith("role '0x0a72").contains("... (cut) (bytes, not text)' on data/ks1");
    }

    // ---- DSE 5.1 ----

    private static void dse51()
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_5_1);
    }

    /** Removes the rows of the keyspace of the DSE 5.1.51 schema fixture from the schema tables of this node. */
    private static void deleteDse51SchemaRows()
    {
        for (String table : SchemaKeyspaceTables.ALL)
            QueryProcessor.executeInternal("DELETE FROM system_schema." + table + " WHERE keyspace_name = ?",
                                           Dse51SchemaRows.KEYSPACE);
    }

    /** Removes the row of {@code system_schema.tables} named after the view of the DSE 5.1.51 schema fixture. */
    private static void deleteTheTableRowOfTheView()
    {
        QueryProcessor.executeInternal("DELETE FROM system_schema.tables WHERE keyspace_name = ? AND table_name = ?",
                                       Dse51SchemaRows.KEYSPACE, "rlac_by_v");
    }

    /** The row-level grants of the DSE 5.1.51 schema fixture, written to {@code system_auth.role_permissions}. */
    private void insertDse51RowGrants() throws Throwable
    {
        for (String resource : DSE51_ROW_RESOURCES)
            execute("INSERT INTO system_auth.role_permissions (role, resource, permissions) " +
                    "VALUES ('reader', ?, {'SELECT'})", resource);
    }

    /**
     * Node 1 of the DSE 5.1.51 schema fixture, as DSE 5.1.51 left it after the failed {@code UNRESTRICT ROWS} on the
     * view: a row of {@code system_schema.tables} named after the view, with its id, beside the view. Without the mode
     * (as in {@code DSE_6}) the build of the schema fails on the shared id, with an error that does not say why; in
     * {@code DSE_5_1} the start is refused with the rows named before the schema is loaded, and so is every read of
     * the schema. Without that row the view loads from DSE 5.1's rows, which have no {@code version} column (a view
     * of version 0).
     */
    @Test
    public void dse51TheRowUnrestrictRowsLeavesBesideAViewRefusesTheSchema() throws Throwable
    {
        Dse51SchemaRows.apply(Dse51SchemaRows.KEYSPACE);
        String id = Dse51Fixtures.RLAC_VIEW_ID.toString();
        try
        {
            for (InternodeCompatibilityMode mode : List.of(InternodeCompatibilityMode.NONE,
                                                           InternodeCompatibilityMode.DSE_6))
            {
                DatabaseDescriptor.setInternodeCompatibilityMode(mode);
                assertThatThrownBy(SchemaKeyspace::fetchNonSystemKeyspaces)
                .as("%s", mode)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Multiple entries with same key: " + id + '=' + DSE51_VIEW + " and " + id + '=' +
                            DSE51_VIEW);
            }
            // in DSE_5_1 the startup check refuses the row before the schema is loaded, with the text of the
            // check of SchemaKeyspace, which stands behind it for every read of the schema
            dse51();
            String refusal = "internode_compatibility_mode DSE_5_1 refused: system_schema.tables holds rows of " +
                             "materialized views of system_schema.views: the table row " + DSE51_VIEW + " (id " + id +
                             ") beside the materialized view " + DSE51_VIEW + " (id " + id + "). DSE 5.1.51 leaves " +
                             "such a row on the node where UNRESTRICT ROWS on a view failed, after which neither DSE " +
                             "5.1.51 nor this software can start the node; this software does not repair it, and no " +
                             "repair is validated yet: the node cannot be upgraded while the row is there. Do not " +
                             "run UNRESTRICT ROWS on a view";
            assertThatThrownBy(Dse6AccessControlCheck::verifyDse51SchemaRowsAtStartup)
            .isInstanceOfSatisfying(StartupException.class,
                                    e -> assertThat(e.returnCode).isEqualTo(StartupException.ERR_WRONG_CONFIG))
            .hasMessage(refusal);
            assertThatThrownBy(SchemaKeyspace::fetchNonSystemKeyspaces)
            .isInstanceOf(ConfigurationException.class)
            .hasMessage(refusal);
            for (InternodeCompatibilityMode mode : List.of(InternodeCompatibilityMode.NONE,
                                                           InternodeCompatibilityMode.DSE_6))
            {
                DatabaseDescriptor.setInternodeCompatibilityMode(mode);
                Dse6AccessControlCheck.verifyDse51SchemaRowsAtStartup();
            }
            dse51();

            // without that row the start goes on and the schema of the keyspace is built, the view with the key of
            // its restriction
            deleteTheTableRowOfTheView();
            Dse6AccessControlCheck.verifyDse51SchemaRowsAtStartup();
            KeyspaceMetadata keyspace = SchemaKeyspace.fetchNonSystemKeyspaces().getNullable(Dse51SchemaRows.KEYSPACE);
            assertThat(keyspace.views.getNullable("rlac_by_v").metadata.params.extensions)
            .containsExactly(Map.entry(Dse6AccessControlCheck.RLAC_EXTENSION_KEY, ByteBufferUtil.bytes("region")));
            // a view of version 0, as DSE 5.1 writes every view (its row has no version), with no hidden column:
            // the columns of its definition only
            UntypedResultSet viewRow = QueryProcessor.executeInternal("SELECT version FROM system_schema.views " +
                                                                      "WHERE keyspace_name = ? AND view_name = ?",
                                                                      Dse51SchemaRows.KEYSPACE, "rlac_by_v");
            assertThat(viewRow.size()).isEqualTo(1);
            assertThat(viewRow.one().has("version")).isFalse();
            assertThat(keyspace.views.getNullable("rlac_by_v").metadata.columns())
            .extracting(column -> column.name.toString())
            .containsExactlyInAnyOrder("v", "region", "id");
        }
        finally
        {
            deleteDse51SchemaRows();
        }
    }

    /**
     * The restricted table and the view of the DSE 5.1.51 schema fixture, with its row-level grants, as node 1 has them
     * (the view's key there), then as nodes 2 and 3 have them (the view without the key): node 1 refuses the table,
     * the view and the grants, with no remedy for the view, the others the table and the grants. The DSE 5.1 node has
     * no {@code restricted} or {@code grantables} column and grants neither UPDATE nor TRUNCATE.
     */
    @Test
    public void dse51RestrictedTablesViewsAndRowGrantsRefuseTheStart() throws Throwable
    {
        rolesToDelete.add("reader");
        // the drop of a view records it in system_distributed, which this test node does not create
        String distributed = SchemaConstants.DISTRIBUTED_KEYSPACE_NAME;
        boolean distributedAdded = Schema.instance.getKeyspaceMetadata(distributed) == null;
        if (distributedAdded)
            SchemaTestUtil.addOrUpdateKeyspace(SystemDistributedKeyspace.metadata(), true);
        KeyspaceMetadata node1 = dse51FixtureKeyspace();
        try
        {
            SchemaTestUtil.addOrUpdateKeyspace(node1, true);
            insertDse51RowGrants();
            dse51();
            Findings findings = Dse6AccessControlCheck.scan(null);
            assertThat(findings.rowLevelGrants).isEqualTo(DSE51_ROW_GRANTS);
            assertThat(findings.restrictions).isEmpty();
            assertThat(findings.delegations).isEmpty();
            String tables = "row-level restricted tables in the schema [" + DSE51_TABLE + " (tenant column region)]";
            String views = "row-level restricted materialized views in the schema [" + DSE51_VIEW +
                           " (tenant column region)]";
            String grants = "row-level grants " + DSE51_ROW_GRANTS;
            String remedy = ROLL_BACK + "REVOKE the row-level grants, UNRESTRICT ROWS ON " + DSE51_TABLE +
                            ") and upgrade it again";
            String end = "; or start with " + OVERRIDE + " to drop them knowingly";
            assertThatThrownBy(Dse6AccessControlCheck::verifyAtStartup)
            .isInstanceOf(StartupException.class)
            .hasMessage(DSE51_REFUSED + grants + "; " + tables + "; " + views + ". " + STALE + remedy + ". " +
                        DSE51_VIEW_ADVICE + end);
            assertThat(Dse6AccessControlCheck.lastCheck().findings)
            .containsExactly("row-level grant " + DSE51_ROW_GRANTS.get(0), "row-level grant " + DSE51_ROW_GRANTS.get(1),
                             "row-level restricted table in the schema " + DSE51_TABLE + " (tenant column region)",
                             "row-level restricted materialized view in the schema " + DSE51_VIEW +
                             " (tenant column region)");
            // the lone grants of DSE 6 are not looked for
            assertThat(CassandraAuthorizer.loneDseWriteGrantsSource())
            .startsWith("the rows read since startup; DSE 5.1 grants neither");

            // the periodic check reports them, with the statements DSE 5.1 has
            List<String> errors = messages(Dse6AccessControlCheck.class, Level.ERROR, Dse6AccessControlCheck::recheck);
            assertThat(errors).hasSize(1);
            assertThat(errors.get(0))
            .contains("do not run GRANT, REVOKE, RESTRICT ROWS or UNRESTRICT ROWS on DSE 5.1 nodes during the upgrade")
            .endsWith("; the materialized views restricted to rows are in this node's schema and refuse its next " +
                      "start. " + DSE51_VIEW_ADVICE);

            // the view alone, as the only finding: no advice to roll back
            execute("DELETE FROM system_auth.role_permissions WHERE role = 'reader'");
            DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
            TableMetadata table = Schema.instance.getTableMetadata(Dse51SchemaRows.KEYSPACE, "rlac");
            TableMetadata unrestricted = table.unbuild()
                                              .params(table.params.unbuild().extensions(Map.of()).build())
                                              .build();
            SchemaTestUtil.addOrUpdateKeyspace(node1.withSwapped(node1.tables.withSwapped(unrestricted)), true);
            dse51();
            assertThatThrownBy(Dse6AccessControlCheck::verifyAtStartup)
            .hasMessage(DSE51_REFUSED + views + ". " + DSE51_VIEW_ADVICE + end);

            // nodes 2 and 3: the view without the key, which only the node that ran RESTRICT ROWS on it has
            DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
            SchemaTestUtil.addOrUpdateKeyspace(withoutTheViewsKey(node1), true);
            assertThat(Schema.instance.getKeyspaceMetadata(Dse51SchemaRows.KEYSPACE).views.getNullable("rlac_by_v")
                                      .metadata.params.extensions).isEmpty();
            insertDse51RowGrants();
            dse51();
            assertThatThrownBy(Dse6AccessControlCheck::verifyAtStartup)
            .hasMessage(DSE51_REFUSED + grants + "; " + tables + ". " + STALE + remedy + end);

            // in DSE_6 a view's key is not looked for (DSE 6's behaviour is unchanged)
            DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
            SchemaTestUtil.addOrUpdateKeyspace(node1, true);
            execute("DELETE FROM system_auth.role_permissions WHERE role = 'reader'");
            dse6();
            assertThatThrownBy(Dse6AccessControlCheck::verifyAtStartup)
            .hasMessage("internode_compatibility_mode DSE_6 refused: DSE access controls that this software does not " +
                        "enforce would grant access if ignored (found by this node's replica): " + tables + ". " +
                        ROLL_BACK + "UNRESTRICT ROWS ON " + DSE51_TABLE + UPGRADE_AGAIN);
        }
        finally
        {
            DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
            SchemaTestUtil.dropKeyspaceIfExist(Dse51SchemaRows.KEYSPACE, true);
            if (distributedAdded)
                SchemaTestUtil.dropKeyspaceIfExist(distributed, true);
            deleteDse51SchemaRows();
        }
    }

    /**
     * The keyspace of the DSE 5.1.51 schema fixture with its table {@code rlac} and the view {@code rlac_by_v} only, as
     * node 1 has them once the row of {@code system_schema.tables} named after the view is removed: the other tables
     * are left out (their SASI index and compact storage are not what these tests are about).
     */
    private static KeyspaceMetadata dse51FixtureKeyspace() throws Exception
    {
        Dse51SchemaRows.apply(Dse51SchemaRows.KEYSPACE);
        deleteTheTableRowOfTheView();
        KeyspaceMetadata keyspace = SchemaKeyspace.fetchNonSystemKeyspaces().getNullable(Dse51SchemaRows.KEYSPACE);
        deleteDse51SchemaRows();
        TableMetadata table = keyspace.tables.getNullable("rlac");
        assertThat(table.id.asUUID()).isEqualTo(Dse51Fixtures.RLAC_TABLE_ID);
        assertThat(table.params.extensions)
        .containsExactly(Map.entry(Dse6AccessControlCheck.RLAC_EXTENSION_KEY, ByteBufferUtil.bytes("region")));
        ViewMetadata view = keyspace.views.getNullable("rlac_by_v");
        assertThat(view.metadata.id.asUUID()).isEqualTo(Dse51Fixtures.RLAC_VIEW_ID);
        return keyspace.withSwapped(Tables.of(table))
                       .withSwapped(Views.builder().put(view).build())
                       .withSwapped(Types.none());
    }

    /** {@code keyspace} with its view as nodes 2 and 3 of the fixture have it, without the key of its restriction. */
    private static KeyspaceMetadata withoutTheViewsKey(KeyspaceMetadata keyspace)
    {
        ViewMetadata view = keyspace.views.getNullable("rlac_by_v");
        TableMetadata unrestricted = view.metadata.unbuild()
                                                  .params(view.metadata.params.unbuild().extensions(Map.of()).build())
                                                  .build();
        return keyspace.withSwapped(Views.builder().put(view.copy(unrestricted)).build());
    }
}
