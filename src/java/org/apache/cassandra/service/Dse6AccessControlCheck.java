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
import java.nio.charset.CharacterCodingException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.auth.AuthKeyspace;
import org.apache.cassandra.auth.CassandraAuthorizer;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.PageSize;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.ReadAbortException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.StartupException;
import org.apache.cassandra.net.DseCompat;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Clock;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * The fail-fast check on DataStax Enterprise access controls this software does not enforce: anything that restricts
 * access on DSE and is silently ignored here would grant access after the upgrade, so a node in
 * {@code internode_compatibility_mode: DSE_6} refuses to start while such controls exist.
 * <p>
 * What is matched in {@code system_auth.role_permissions} (the table DSE leaves behind):
 * <ul>
 * <li>a non-empty {@code restricted} set: a DSE {@code RESTRICT} (per-role deny list). The column exists in DSE's
 *     table only, so it is queried only when the loaded schema has it;</li>
 * <li>a resource starting with {@code rows/}: a DSE row-level access control grant
 *     ({@code rows/<target>/data/<ks>/<table>}).</li>
 * </ul>
 * What is matched in the schema: a user table whose {@code extensions} map has the key {@value #RLAC_EXTENSION_KEY}, the
 * one DSE writes with {@code RESTRICT ROWS ON <ks>.<table> USING <column>} (and removes with {@code UNRESTRICT ROWS}),
 * whose value is the name of the tenant column. The key is refused whether or not
 * {@code authorization_options.allow_row_level_security} is on in the DSE nodes' {@code dse.yaml}, which this software
 * cannot read: the check fails closed. The tables are those of this node's schema, which DSE DDL does not reach in
 * {@code DSE_6} mode: a {@code RESTRICT ROWS} run on a DSE node after this node started is not seen by any check, hence
 * the instruction of the periodic check not to run it during the upgrade. The other keys of that map are not DSE
 * access controls (DSE writes no other table extension key): the first startup pass lists them in one INFO line, no
 * warning.
 * <p>
 * A non-empty {@code grantables} set (a DSE {@code AUTHORIZE FOR} delegation) only removes access when dropped and
 * is reported with one warning per entry. A DSE {@code UPDATE} or {@code TRUNCATE} granted alone on a data resource,
 * which {@code CassandraAuthorizer} drops (DSE writes the two together for {@code GRANT ALL} and a creator's
 * permissions, read as {@code MODIFY}; its {@code GRANT MODIFY} writes the name {@code MODIFY}), is also dropped
 * with a warning: with {@code CassandraAuthorizer} configured, the rows are read through its parsing, the lone
 * grants are listed by {@code nodetool compatstatus}, the second startup pass
 * (cluster-wide, or this node's replica with {@code dse_restrictions_local_check_only}, or, when the cluster-wide read
 * fails and {@code ignore_dse_restrictions} is set, this node's replica read again) logs them all in one warning, and
 * the periodic check logs those it lists first in one warning. The delegations follow the same path; when that
 * second read of the replica fails too, the lone grants listed at that point are logged and what the first pass
 * recorded stays listed for compatstatus, but the delegations are not logged (they are kept nowhere), which the
 * warning on that failure says.
 * <p>
 * Two passes, both from {@code CassandraDaemon.setup}:
 * <ol>
 * <li>{@link #verifyAtStartup()} reads this node's own replica after the schema is loaded and the commit log
 *     replayed, before {@code StorageService.initServer}: the startup checks run before the schema is loaded and
 *     cannot read {@code system_auth}. It catches what this node holds before the node joins the ring;</li>
 * <li>{@link #verifyAfterJoin()} reads the table at the authorizer's read consistency once the node has joined, before the client
 *     transports start: {@code system_auth} is usually replicated to fewer nodes than the cluster has, so a node
 *     that is not a replica of a restricted role's partition holds nothing locally and would otherwise coordinate
 *     for that role without the restriction. A read that fails on the request (replicas not alive yet, a timeout, a
 *     read failure: the connections to the other replicas, DSE ones included, may still be coming up) is retried, with
 *     a pause of {@value #RETRY_INTERVAL_SECONDS} seconds after each failed attempt, for up to
 *     {@code cassandra.internode_compatibility_mode.dse_restrictions_check_wait_seconds} (120 by default); one that still
 *     fails then, or fails otherwise, refuses the start too (fail closed).</li>
 * </ol>
 * The check refuses whatever authorizer is configured (it fails closed: with one that enforces nothing, such as
 * {@code AllowAllAuthorizer}, the controls are not enforced either). With {@code NONE} nothing is read.
 */
public final class Dse6AccessControlCheck
{
    private static final Logger logger = LoggerFactory.getLogger(Dse6AccessControlCheck.class);

    public static final String RESTRICTED_COLUMN = "restricted";
    public static final String GRANTABLES_COLUMN = "grantables";
    public static final String ROWS_RESOURCE_PREFIX = "rows/";
    /** The table extension key of DSE's row-level access control; its value is the name of the tenant column. */
    public static final String RLAC_EXTENSION_KEY = "DSE_RLACA";
    /**
     * How many characters of a tenant column name or of an extension key, or bytes of a value that is not text, are
     * shown in logs and refusals.
     */
    @VisibleForTesting
    static final int TENANT_COLUMN_SHOWN = 64;
    /**
     * How many tables restricted to rows a refusal or the periodic ERROR names an {@code UNRESTRICT ROWS} statement for;
     * both list them all.
     */
    @VisibleForTesting
    static final int UNRESTRICT_ROWS_NAMED = 20;
    /** How many tables with other schema extensions the INFO line of the first startup pass names. */
    @VisibleForTesting
    static final int OTHER_EXTENSIONS_LOGGED = 20;
    /**
     * The source of a check that read this node's replica only (the first pass, or a fallback of the second pass or of
     * the periodic check): the rows of the other replicas are not seen, so what it found is a lower bound.
     */
    public static final String LOCAL_SOURCE = "this node's replica";
    /** The start of the compatstatus entry of a table restricted to rows. */
    private static final String RESTRICTED_TABLE_ENTRY = "row-level restricted table in the schema ";
    private static final int PAGE_SIZE = 1000;
    /** The lone DSE write grants named in a warning; compatstatus lists them all. */
    @VisibleForTesting
    static final int LONE_GRANTS_LOGGED = 20;
    private static final String LONE_GRANTS_REMEDY = "GRANT MODIFY where the role should also have the other, or accept the loss " +
                                                     "(nodetool compatstatus lists them)";

    /** The pause between two attempts of the cluster-wide read of the second startup pass. */
    @VisibleForTesting
    static final long RETRY_INTERVAL_SECONDS = 5;

    /** The source of a check that could read nothing: compatstatus shows it as unknown, never as an all-clear. */
    public static final String NOT_CHECKED = "not checked";

    /** The result of the last check (a startup pass or the periodic one), for compatstatus; null before the first one. */
    private static volatile LastCheck lastCheck;

    /**
     * The entries of what the first pass found in {@code system_auth.role_permissions}, kept for the second pass when
     * neither of its reads can be made with {@code ignore_dse_restrictions}: a periodic check may replace
     * {@link #lastCheck} in between.
     */
    private static volatile List<String> firstPassPermissionEntries = List.of();

    /**
     * The entries {@code ignore_dse_restrictions} dropped knowingly and logged at WARN since the start, so that the
     * periodic check logs at WARN only those a DSE node added since.
     */
    private static final Set<String> ignoredEntriesLogged = ConcurrentHashMap.newKeySet();

    /** What a check found that would grant access, and how it was made; one immutable snapshot. */
    public static final class LastCheck
    {
        /** How the check was made ({@code a LOCAL_QUORUM read}, ...), or {@link #NOT_CHECKED} followed by the reason. */
        public final String source;
        /** {@code RESTRICT ...}, {@code row-level grant ...} and {@code row-level restricted table ...} entries. */
        public final List<String> findings;

        LastCheck(String source, List<String> findings)
        {
            this.source = source;
            this.findings = List.copyOf(findings);
        }
    }

    /**
     * What the scan of {@code system_auth.role_permissions} found, as {@code role 'x' on <resource>: <set>} lines, and
     * the tables of the schema DSE restricts to rows.
     */
    public static final class Findings
    {
        /** DSE {@code RESTRICT} entries: refused. */
        public final List<String> restrictions = new ArrayList<>();
        /** Row-level access control grants: refused. */
        public final List<String> rowLevelGrants = new ArrayList<>();
        /** DSE {@code AUTHORIZE FOR} delegations: dropped with a warning. */
        public final List<String> delegations = new ArrayList<>();
        /**
         * DSE {@code UPDATE} or {@code TRUNCATE} granted alone on a data resource, as {@code role 'x' on <resource>: <name>}
         * ({@link CassandraAuthorizer#readDseLoneWriteGrant}): dropped with a warning; empty with another authorizer.
         */
        public final List<String> loneWriteGrants = new ArrayList<>();
        /** Those of {@link #loneWriteGrants} this scan listed first, which the periodic check logs. */
        public final List<String> newLoneWriteGrants = new ArrayList<>();
        /**
         * The tables of the schema with DSE's {@value #RLAC_EXTENSION_KEY} extension, sorted by their CQL name: refused.
         * Filled by the passes, through {@code withSchema}, not by {@link #scan}.
         */
        final List<TableMetadata> rowLevelRestrictedTables = new ArrayList<>();
        /** Whether {@code system_auth.role_permissions} is missing from the schema, so that nothing could be read. */
        boolean permissionsTableMissing;

        public boolean wouldGrantAccessIfDropped()
        {
            return permissionsWouldGrantAccessIfDropped() || !rowLevelRestrictedTables.isEmpty();
        }

        /** Whether the rows of {@code system_auth.role_permissions} hold anything that would grant access if dropped. */
        boolean permissionsWouldGrantAccessIfDropped()
        {
            return !restrictions.isEmpty() || !rowLevelGrants.isEmpty();
        }
    }

    private Dse6AccessControlCheck()
    {
    }

    /**
     * The first pass: refuses the start in {@code DSE_6} mode while this node's replica of the permissions holds a
     * DSE restriction or row-level grant, or the schema holds a table DSE restricts to rows, unless
     * {@code cassandra.internode_compatibility_mode.ignore_dse_restrictions} is set; a no-op with {@code NONE}.
     *
     * @throws StartupException with the roles and resources found, and what to do; also when
     *         {@code dse_restrictions_check_wait_seconds} is not a number of seconds (a missing
     *         {@code system_auth.role_permissions} is recorded as not checked: a new node may not have the schema yet,
     *         and the second pass refuses it)
     */
    public static void verifyAtStartup() throws StartupException
    {
        verifyAtStartup(Dse6AccessControlCheck::scan);
    }

    /** {@link #verifyAtStartup()} with its scan of this node's replica given, as {@link #scan} (called with null). */
    @VisibleForTesting
    static void verifyAtStartup(Function<ConsistencyLevel, Findings> localScan) throws StartupException
    {
        if (!DseCompat.enabled())
            return;
        // a wrong value refuses the start before the node joins the ring, not only at the second pass
        waitSeconds();
        warnOfTheOverridesSet();
        firstPassPermissionEntries = List.of();
        ignoredEntriesLogged.clear();
        logOtherExtensions();
        // the delegations and the lone write grants are logged by the second pass (cluster-wide, or this node's replica
        // with dse_restrictions_local_check_only, or, when the cluster-wide read fails and ignore_dse_restrictions is set,
        // this node's replica read again; if that read fails too, the lone grants listed at that point, this pass's list):
        // once per start
        Findings findings = scanLocally(localScan);
        firstPassPermissionEntries = List.copyOf(permissionEntries(findings));
        verify(findings, LOCAL_SOURCE, false, false);
    }

    /** One WARN per start for each property of this check that is set, so that a forgotten one never goes unnoticed. */
    private static void warnOfTheOverridesSet()
    {
        if (CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE_IGNORE_DSE_RESTRICTIONS.getBoolean())
            logger.warn("{}=true is set: the DSE access controls this software does not enforce are dropped knowingly instead of refusing the " +
                        "start, and the cluster-wide check is not retried; remove it once they are removed on DSE",
                        CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE_IGNORE_DSE_RESTRICTIONS.getKey());
        if (CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE_DSE_RESTRICTIONS_LOCAL_CHECK_ONLY.getBoolean())
            logger.warn("{}=true is set: when the cluster-wide check of the DSE access controls fails, only this node's replica is " +
                        "checked, and the cluster-wide check is not retried; remove it at the next restart",
                        CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE_DSE_RESTRICTIONS_LOCAL_CHECK_ONLY.getKey());
    }

    /** The read of this node's replica at startup, with {@code scan} as {@link #scan}; a failure refuses the start with the check named. */
    private static Findings scanLocally(Function<ConsistencyLevel, Findings> scan) throws StartupException
    {
        try
        {
            return scan.apply(null);
        }
        catch (RuntimeException e)
        {
            throw new StartupException(StartupException.ERR_WRONG_CONFIG,
                                       "internode_compatibility_mode DSE_6 refused: unable to read system_auth.role_permissions on this node (" + e +
                                       "), so the DSE access controls this software does not enforce cannot be checked", e);
        }
    }

    /**
     * The second pass, once the node has joined the ring: the same refusal on a read of the whole table at the
     * consistency level of the authorizer's own reads ({@code auth_read_consistency_level}, {@code LOCAL_QUORUM} by
     * default), which also covers the partitions this node does not replicate. A read that fails on the request (not
     * enough replicas alive, a timeout, a read failure: right after the start the connections to the other replicas,
     * DSE ones included, may not be up yet) is retried, with a pause of {@value #RETRY_INTERVAL_SECONDS} seconds after
     * each failed attempt, for up to {@code cassandra.internode_compatibility_mode.dse_restrictions_check_wait_seconds};
     * every failure but the last is logged at WARN. A read that still fails then, or fails otherwise (including a read
     * aborted by the guardrails, which a retry would not change), refuses the start as well: the check fails closed.
     * {@code cassandra.internode_compatibility_mode.dse_restrictions_local_check_only} accepts the first pass alone in
     * that case, with a warning, and {@code cassandra.internode_compatibility_mode.ignore_dse_restrictions} accepts no
     * cluster-wide check at all: what this node's replica and the schema hold is then listed and dropped knowingly,
     * under a {@code not checked: ...} source; with either of them set the read is not retried, since the operator
     * already chose what happens when it fails. The restrictions found by the first pass are still refused.
     *
     * @throws StartupException also when {@code dse_restrictions_check_wait_seconds} is not a number of seconds, or
     *         {@code system_auth.role_permissions} is not in the schema
     */
    public static void verifyAfterJoin() throws StartupException
    {
        if (!DseCompat.enabled())
            return;
        long waitSeconds = waitSeconds();
        boolean fallbackChosen = CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE_DSE_RESTRICTIONS_LOCAL_CHECK_ONLY.getBoolean() ||
                                 CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE_IGNORE_DSE_RESTRICTIONS.getBoolean();
        long waitNanos = fallbackChosen ? 0 : SECONDS.toNanos(waitSeconds);
        verifyAfterJoin(Dse6AccessControlCheck::scan, Clock.Global::nanoTime, nanos -> Uninterruptibles.sleepUninterruptibly(nanos, NANOSECONDS),
                        waitNanos, SECONDS.toNanos(RETRY_INTERVAL_SECONDS));
    }

    /**
     * The value of {@code dse_restrictions_check_wait_seconds}, a decimal number of seconds (parsed strictly: not an octal
     * or hexadecimal literal, which the generic converter would accept); a start refused, with the property named, when
     * it is anything else. Read whether or not a fallback property makes the wait unused, so that a wrong value is never
     * silent.
     */
    private static long waitSeconds() throws StartupException
    {
        try
        {
            // surrounding spaces are tolerated; the refusal below quotes the value exactly as it was set
            return Long.parseLong(CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE_DSE_RESTRICTIONS_CHECK_WAIT_SECONDS.getString().trim());
        }
        catch (NumberFormatException e)
        {
            throw new StartupException(StartupException.ERR_WRONG_CONFIG,
                                       "internode_compatibility_mode DSE_6 refused: invalid value '" +
                                       CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE_DSE_RESTRICTIONS_CHECK_WAIT_SECONDS.getString() + "' for " +
                                       CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE_DSE_RESTRICTIONS_CHECK_WAIT_SECONDS.getKey() +
                                       ": a number of seconds is expected", e);
        }
    }

    /**
     * {@link #verifyAfterJoin()} with its cluster-wide read, clock and pause given; the wait is taken as given, whatever
     * the fallback properties (the public method passes 0 when one of them is set).
     *
     * @param clusterScan the scan of {@code system_auth.role_permissions}, as {@link #scan}: null for a read of this
     *                    node's replica (made by the fallbacks of {@code dse_restrictions_local_check_only} and
     *                    {@code ignore_dse_restrictions} when the cluster-wide read fails), otherwise the consistency
     *                    level of the cluster-wide read
     * @param waitNanos how long a read failing on the request is retried, from the first attempt: an attempt that would
     *                  start after that time (the time of the failed attempt plus the pause) is not made; 0 or less makes
     *                  a single attempt
     * @param retryNanos the pause after each failed attempt
     */
    @VisibleForTesting
    static void verifyAfterJoin(Function<ConsistencyLevel, Findings> clusterScan, LongSupplier clock, LongConsumer pause,
                                long waitNanos, long retryNanos) throws StartupException
    {
        ConsistencyLevel consistency = DatabaseDescriptor.getAuthReadConsistencyLevel();
        Findings findings;
        RuntimeException failure;
        long start = clock.getAsLong();
        int attempts = 0;
        for (;;)
        {
            attempts++;
            try
            {
                findings = clusterScan.apply(consistency);
                failure = null;
                break;
            }
            catch (ReadAbortException e) // aborted by the guardrails (tombstones, size): a retry would not change it, fail closed
            {
                findings = null;
                failure = e;
                break;
            }
            catch (RequestExecutionException e) // unavailable replicas, a timeout, a read failure: the replicas may not be reachable yet
            {
                if (waitNanos <= 0 || clock.getAsLong() - start + retryNanos > waitNanos)
                {
                    findings = null;
                    failure = e;
                    break;
                }
                logger.warn("The startup check of the DSE access controls could not read {}.{} at {} (attempt {}: {}); retrying in {} s, " +
                            "for up to {} s ({}): the replicas of {} may not be reachable yet",
                            SchemaConstants.AUTH_KEYSPACE_NAME, AuthKeyspace.ROLE_PERMISSIONS, consistency, attempts, message(e),
                            NANOSECONDS.toSeconds(retryNanos), NANOSECONDS.toSeconds(waitNanos),
                            CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE_DSE_RESTRICTIONS_CHECK_WAIT_SECONDS.getKey(),
                            SchemaConstants.AUTH_KEYSPACE_NAME);
                pause.accept(retryNanos);
            }
            catch (RuntimeException e) // anything else the read throws is not retried: fail closed
            {
                findings = null;
                failure = e;
                break;
            }
        }
        if (failure != null)
        {
            RuntimeException e = failure;
            String tried = attempts == 1 ? "" : " after " + attempts + " attempts in " + NANOSECONDS.toSeconds(clock.getAsLong() - start) + " s";
            String problem = "unable to read system_auth.role_permissions at " + consistency + " (" + message(e) + ')' + tried + ", so the DSE access " +
                             "controls this software does not enforce cannot be checked across the cluster";
            if (CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE_DSE_RESTRICTIONS_LOCAL_CHECK_ONLY.getBoolean())
            {
                logger.warn("{}; only this node's replica was checked ({}=true)", problem,
                            CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE_DSE_RESTRICTIONS_LOCAL_CHECK_ONLY.getKey());
                // the local findings are still refused, with the same text as the first pass
                verify(scanLocally(clusterScan), LOCAL_SOURCE + " only: the cluster-wide read failed (" + message(e) + ')', true, false);
                return;
            }
            if (CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE_IGNORE_DSE_RESTRICTIONS.getBoolean())
            {
                logger.warn("{}; not checked ({}=true: DSE access controls are dropped knowingly)", problem,
                            CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE_IGNORE_DSE_RESTRICTIONS.getKey());
                // the cluster is not checked (compatstatus says so, never an all-clear); what this node's replica and the
                // schema hold is handled as the local-only fallback does (listed, dropped knowingly, the dropped grants logged)
                String notChecked = NOT_CHECKED + ": the cluster-wide read failed and " +
                                    CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE_IGNORE_DSE_RESTRICTIONS.getKey() + "=true; ";
                Findings known;
                try
                {
                    known = clusterScan.apply(null);
                }
                catch (RuntimeException local)
                {
                    logger.warn("The startup check of the DSE access controls could not read {} again; the schema and what the first pass " +
                                "found in the replica are listed, the DSE AUTHORIZE FOR delegations the replica holds, dropped, are not",
                                LOCAL_SOURCE, local);
                    Findings schemaOnly = withSchema(new Findings());
                    String source = notChecked + "what the schema holds and what the first pass found in " + LOCAL_SOURCE + " are listed (the " +
                                    "read of " + LOCAL_SOURCE + " failed again: " + message(local) + ')';
                    // the RESTRICT entries and row-level grants the first pass dropped knowingly stay listed: one snapshot
                    List<String> found = new ArrayList<>(firstPassPermissionEntries);
                    found.addAll(entries(schemaOnly));
                    lastCheck = new LastCheck(source, found);
                    if (schemaOnly.wouldGrantAccessIfDropped())
                        logIgnored(source, schemaOnly);
                    // the lone grants listed so far (by the first pass, or by a periodic check that overlapped this pass:
                    // logged twice at worst)
                    logLoneWriteGrants(CassandraAuthorizer.loneDseWriteGrants(), LOCAL_SOURCE + " at startup, and the rows read since", false);
                    return;
                }
                // never refuses: the override is set
                verify(known, notChecked + "what " + LOCAL_SOURCE + " and the schema hold is listed", true, false);
                return;
            }
            throw new StartupException(StartupException.ERR_WRONG_CONFIG,
                                       "internode_compatibility_mode DSE_6 refused: " + problem + "; make the replicas of system_auth reachable, " +
                                       "or start with -D" + CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE_DSE_RESTRICTIONS_LOCAL_CHECK_ONLY.getKey() +
                                       "=true to rely on the check of this node's replica alone", e);
        }
        if (attempts > 1)
            logger.info("The startup check of the DSE access controls read {}.{} at {} on attempt {}",
                        SchemaConstants.AUTH_KEYSPACE_NAME, AuthKeyspace.ROLE_PERMISSIONS, consistency, attempts);
        verify(findings, "a " + consistency + " read", true, true);
    }

    /**
     * The periodic check, from the status task of {@code DseUpgradeGuards} in {@code DSE_6} mode: a DSE node can still
     * RESTRICT or grant row-level access after this node started, and this node would not enforce it. What would grant
     * access is logged at ERROR (with {@code ignore_dse_restrictions}, which drops it knowingly, at WARN, and only what
     * was not logged before) and shown by {@code nodetool compatstatus}; the node keeps running (refusing to serve
     * would not remove the control). The cluster-wide read is tried first, this node's replica when it cannot be made.
     * The lone DSE write grants it lists first are logged in one warning. The first periodic check can overlap a slow
     * second startup pass (the periodic recheck task is scheduled before it): the scans are not serialized, which at
     * worst logs a lone grant twice, or leaves it out of compatstatus until its row is read again.
     * <p>
     * The tables restricted to rows are those of this node's schema, which DSE DDL does not reach in {@code DSE_6} mode:
     * a {@code RESTRICT ROWS} run on a DSE node after this node started is not detected, only the rows DSE adds to
     * {@code system_auth.role_permissions} are.
     *
     * @return what would grant access if dropped, as {@code RESTRICT ...}, {@code row-level grant ...} and
     *         {@code row-level restricted table ...} entries, or an empty list
     * @throws RuntimeException the failure of the read of this node's replica, when the cluster-wide read failed too,
     *         with the failure of the cluster-wide read added as suppressed unless both reads threw the same exception;
     *         thrown after the tables of the schema restricted to rows are recorded, under a {@code not checked: ...}
     *         source, and logged at ERROR (at WARN, when not logged before, if {@code ignore_dse_restrictions} is set)
     */
    public static List<String> recheck()
    {
        return recheck(Dse6AccessControlCheck::scan);
    }

    /**
     * {@link #recheck()} with its scan of {@code system_auth.role_permissions} given, as {@link #scan}: null for a read
     * of this node's replica, otherwise the consistency level of the cluster-wide read.
     */
    @VisibleForTesting
    public static List<String> recheck(Function<ConsistencyLevel, Findings> scan)
    {
        if (!DseCompat.enabled())
            return List.of();
        Findings findings;
        String source;
        boolean complete = false;
        ConsistencyLevel consistency = DatabaseDescriptor.getAuthReadConsistencyLevel();
        try
        {
            findings = scan.apply(consistency);
            source = "a " + consistency + " read";
            complete = true;
        }
        catch (RuntimeException e)
        {
            // not an all-clear: what this node's replica holds, and the source says the cluster was not checked
            source = LOCAL_SOURCE + " only: the cluster-wide read failed (" + message(e) + ')';
            try
            {
                findings = scan.apply(null);
            }
            catch (RuntimeException local)
            {
                // the permissions are unknown; the tables of the schema restricted to rows are listed, and logged, all the same;
                // the caller logs the failure, with the one of the cluster-wide read
                Findings schemaOnly = withSchema(new Findings());
                String notChecked = NOT_CHECKED + ": the cluster-wide read failed (" + message(e) + ") and so did the read of this " +
                                    "node's replica (" + message(local) + ')';
                record(schemaOnly, notChecked);
                logUnenforced(schemaOnly, notChecked);
                if (local != e)
                    local.addSuppressed(e);
                throw local;
            }
            logger.warn("The periodic check of the DSE access controls could not read system_auth.role_permissions at {} ({}); only this " +
                        "node's replica was checked", consistency, message(e));
        }
        withSchema(findings);
        List<String> found = record(findings, source);
        // a control dropped knowingly that a complete read no longer finds is forgotten, so that it is logged again if it comes back
        if (complete)
            ignoredEntriesLogged.retainAll(new HashSet<>(found));
        loneWriteGrantsScanned(findings, "the periodic check (" + source + ')', complete);
        // those listed already were logged, by a startup pass, an earlier periodic check or the read of their row
        List<String> newLoneGrants = findings.newLoneWriteGrants;
        if (!newLoneGrants.isEmpty())
            logger.warn("The periodic check ({}) found {} new grant(s) of UPDATE or TRUNCATE alone in {}.{}, DataStax Enterprise permissions: this " +
                        "software has MODIFY only, which grants both, so they are dropped and these roles lose the access they gave: {}{}. " +
                        LONE_GRANTS_REMEDY,
                        source, newLoneGrants.size(), SchemaConstants.AUTH_KEYSPACE_NAME, AuthKeyspace.ROLE_PERMISSIONS,
                        firstLoneGrants(newLoneGrants), moreLoneGrants(newLoneGrants));
        logUnenforced(findings, source);
        return found;
    }

    /**
     * The ERROR of the periodic check on what it found that would grant access; with the override, which drops it
     * knowingly, a WARN on the entries not dropped and logged before.
     * The tables restricted to rows are in this node's schema, which DSE DDL does not reach: an {@code UNRESTRICT ROWS} on
     * DSE does not remove them here, so the ERROR gives the remedy of the refusal for them.
     */
    private static void logUnenforced(Findings findings, String source)
    {
        if (!findings.wouldGrantAccessIfDropped())
            return;
        if (CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE_IGNORE_DSE_RESTRICTIONS.getBoolean())
        {
            // dropped knowingly: only what was not dropped and logged before (by a startup pass or an earlier periodic check)
            List<String> added = new ArrayList<>();
            for (String entry : entries(findings))
                if (ignoredEntriesLogged.add(entry))
                    added.add(entry);
            if (!added.isEmpty())
                logger.warn("The periodic check ({}) found DSE access controls this software does not enforce, dropped knowingly ({}=true) and " +
                            "access opens up accordingly: {}",
                            source, CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE_IGNORE_DSE_RESTRICTIONS.getKey(), added);
            return;
        }
        String tables = findings.rowLevelRestrictedTables.isEmpty()
                        ? ""
                        : "; the tables restricted to rows are in this node's schema, which DSE DDL does not reach, and refuse its next " +
                          "start: roll it back to DSE, run " + unrestrictRows(findings.rowLevelRestrictedTables) +
                          " there and upgrade it again";
        // DSE removes what role_permissions holds; the tables only as their remedy says
        String removeOnDse = !findings.permissionsWouldGrantAccessIfDropped() ? ""
                             : findings.rowLevelRestrictedTables.isEmpty() ? "remove them on DSE, and "
                             : "remove the " + (findings.rowLevelGrants.isEmpty() ? "restrictions"
                                                : findings.restrictions.isEmpty() ? "row-level grants"
                                                : "restrictions and row-level grants") + " on DSE, and ";
        logger.error("The periodic check ({}) found DSE access controls this node does not enforce: {}; {}do not " +
                     "run GRANT, REVOKE, RESTRICT, UNRESTRICT, RESTRICT ROWS or UNRESTRICT ROWS on DSE nodes during the upgrade{}",
                     source, describe(findings), removeOnDse, tables);
    }

    /** The message of a failure, or its string form when it has none. */
    private static String message(Throwable t)
    {
        return t.getMessage() != null ? t.getMessage() : String.valueOf(t);
    }

    /** Records what a pass found that would grant access, and how it was found, for compatstatus. */
    private static List<String> record(Findings findings, String source)
    {
        if (findings.permissionsTableMissing)
            source = NOT_CHECKED + ": " + SchemaConstants.AUTH_KEYSPACE_NAME + '.' + AuthKeyspace.ROLE_PERMISSIONS + " is not in the schema (" + source + ')';
        LastCheck check = new LastCheck(source, entries(findings));
        lastCheck = check;
        return check.findings;
    }

    /** What a scan found that would grant access, as the entries of compatstatus. */
    private static List<String> entries(Findings findings)
    {
        List<String> found = permissionEntries(findings);
        for (TableMetadata table : findings.rowLevelRestrictedTables)
            found.add(RESTRICTED_TABLE_ENTRY + describe(table));
        return found;
    }

    /**
     * The entries of what {@code system_auth.role_permissions} holds that would grant access; the capacity also covers
     * the entries of the tables, which {@link #entries} appends.
     */
    private static List<String> permissionEntries(Findings findings)
    {
        List<String> found = new ArrayList<>(findings.restrictions.size() + findings.rowLevelGrants.size() + findings.rowLevelRestrictedTables.size());
        for (String restriction : findings.restrictions)
            found.add("RESTRICT " + restriction);
        for (String grant : findings.rowLevelGrants)
            found.add("row-level grant " + grant);
        return found;
    }

    /** The result of the last check, one snapshot; null before the first check. */
    public static LastCheck lastCheck()
    {
        return lastCheck;
    }

    /** Forgets the last check and what the first pass found in {@code system_auth.role_permissions} (tests only). */
    @VisibleForTesting
    public static void resetLastCheck()
    {
        lastCheck = null;
        firstPassPermissionEntries = List.of();
        ignoredEntriesLogged.clear();
    }

    private static void logDelegations(Findings findings)
    {
        for (String delegation : findings.delegations)
            logger.warn("DSE AUTHORIZE FOR delegation in system_auth.role_permissions is not enforced by this software and is dropped " +
                        "(the delegate can no longer grant; re-grant AUTHORIZE where needed): {}", delegation);
    }

    /**
     * The lone DSE write grants a scan read, for compatstatus ({@link CassandraAuthorizer#dseLoneWriteGrantsScanned});
     * nothing with another authorizer, whose scans read none.
     */
    private static void loneWriteGrantsScanned(Findings findings, String how, boolean complete)
    {
        if (DatabaseDescriptor.getAuthorizer() instanceof CassandraAuthorizer)
            CassandraAuthorizer.dseLoneWriteGrantsScanned(how + ", then the rows read since", findings.loneWriteGrants, complete);
    }

    /**
     * Logs the lone DSE write grants a startup pass found in one warning, naming how they were found; nothing with
     * another authorizer, which does not drop them. That there are none is said at INFO after a complete read only:
     * a read of this node's replica does not see the rows of the other replicas.
     */
    private static void logLoneWriteGrants(List<String> lone, String source, boolean complete)
    {
        if (!(DatabaseDescriptor.getAuthorizer() instanceof CassandraAuthorizer))
            return;
        if (!lone.isEmpty())
            logger.warn("Found {} grant(s) of UPDATE or TRUNCATE alone in {}.{} ({}), DataStax Enterprise permissions: this software has " +
                        "MODIFY only, which grants both, so they are dropped and these roles lose the access they gave: {}{}. " + LONE_GRANTS_REMEDY,
                        lone.size(), SchemaConstants.AUTH_KEYSPACE_NAME, AuthKeyspace.ROLE_PERMISSIONS, source,
                        firstLoneGrants(lone), moreLoneGrants(lone));
        else if (complete)
            logger.info("No grant of UPDATE or TRUNCATE alone found in {}.{} ({})", SchemaConstants.AUTH_KEYSPACE_NAME, AuthKeyspace.ROLE_PERMISSIONS, source);
    }

    // the lone grants named in a warning, then how many more there are
    private static List<String> firstLoneGrants(List<String> lone)
    {
        return lone.size() <= LONE_GRANTS_LOGGED ? lone : lone.subList(0, LONE_GRANTS_LOGGED);
    }

    private static String moreLoneGrants(List<String> lone)
    {
        return lone.size() <= LONE_GRANTS_LOGGED ? "" : " and " + (lone.size() - LONE_GRANTS_LOGGED) + " more";
    }

    /**
     * Records and refuses what a startup pass found.
     *
     * @param secondPass whether this is the second pass, which logs the delegations and the lone write grants (dropped
     *                   with a warning) and refuses a missing {@code system_auth.role_permissions}: the first pass runs
     *                   before a new node has received the schema, so it only records the check as not made
     * @param complete whether the pass read the whole table at the authorizer's consistency level
     */
    private static void verify(Findings findings, String source, boolean secondPass, boolean complete) throws StartupException
    {
        withSchema(findings);
        record(findings, source);
        loneWriteGrantsScanned(findings, "the startup check (" + source + ')', complete);
        if (secondPass)
        {
            logDelegations(findings);
            logLoneWriteGrants(findings.loneWriteGrants, source, complete);
        }
        // a node DSE ran always has the table once it has the schema: without it nothing could be checked, which fails closed too
        if (secondPass && findings.permissionsTableMissing && !CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE_IGNORE_DSE_RESTRICTIONS.getBoolean())
            throw new StartupException(StartupException.ERR_WRONG_CONFIG,
                                       "internode_compatibility_mode DSE_6 refused: " + SchemaConstants.AUTH_KEYSPACE_NAME + '.' + AuthKeyspace.ROLE_PERMISSIONS +
                                       " is not in the schema, so the DSE access controls this software does not enforce cannot be checked; " +
                                       "start with -D" + CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE_IGNORE_DSE_RESTRICTIONS.getKey() +
                                       "=true to start without that check");
        if (!findings.wouldGrantAccessIfDropped())
            return;
        String what = describe(findings);
        if (CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE_IGNORE_DSE_RESTRICTIONS.getBoolean())
        {
            logIgnored(source, findings);
            return;
        }
        throw new StartupException(StartupException.ERR_WRONG_CONFIG, refusalMessage(findings, what, source));
    }

    /** The WARN of what the override drops knowingly at startup; the periodic check then logs only what is added. */
    private static void logIgnored(String source, Findings findings)
    {
        ignoredEntriesLogged.addAll(entries(findings));
        logger.warn("DSE access controls that this software does not enforce are ignored ({}=true) and access opens up accordingly ({}): {}",
                    CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE_IGNORE_DSE_RESTRICTIONS.getKey(), source, describe(findings));
    }

    /** The refusal of a startup pass, with the stale-replica advice when it read rows of this node's replica, and what to run on DSE. */
    private static String refusalMessage(Findings findings, String what, String source)
    {
        // the stale-replica advice is about the rows of system_auth.role_permissions, not about the schema
        boolean staleRowsPossible = source.startsWith(LOCAL_SOURCE) && findings.permissionsWouldGrantAccessIfDropped();
        return "internode_compatibility_mode DSE_6 refused: DSE access controls that this software does not enforce would grant " +
               "access if ignored (found by " + source + "): " + what + ". " +
               (staleRowsPossible ? "The rows listed from system_auth.role_permissions may be stale: the replica may hold partitions this node no " +
                                    "longer owns (once it is rolled back to DSE, run nodetool cleanup system_auth on it), or miss a removal made on " +
                                    "DSE while this node was down (once it is rolled back to DSE, hints or nodetool repair system_auth deliver it). " : "") +
               "Roll this node back to DSE, remove them there (" + remedies(findings) +
               ") and upgrade it again; or start with -D" +
               CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE_IGNORE_DSE_RESTRICTIONS.getKey() + "=true to drop them knowingly";
    }

    /**
     * What removes each kind of control found, on DSE: the statements themselves for the first
     * {@value #UNRESTRICT_ROWS_NAMED} tables restricted to rows, then how many more there are.
     */
    private static String remedies(Findings findings)
    {
        List<String> remedies = new ArrayList<>();
        if (!findings.restrictions.isEmpty())
            remedies.add("UNRESTRICT the roles");
        if (!findings.rowLevelGrants.isEmpty())
            remedies.add("REVOKE the row-level grants");
        if (!findings.rowLevelRestrictedTables.isEmpty())
            remedies.add(unrestrictRows(findings.rowLevelRestrictedTables));
        return String.join(", ", remedies);
    }

    /** The {@code UNRESTRICT ROWS} statements of the first {@value #UNRESTRICT_ROWS_NAMED} tables, then how many more. */
    private static String unrestrictRows(List<TableMetadata> tables)
    {
        List<String> statements = new ArrayList<>();
        for (TableMetadata table : tables.subList(0, Math.min(tables.size(), UNRESTRICT_ROWS_NAMED)))
            statements.add("UNRESTRICT ROWS ON " + table);
        if (tables.size() > UNRESTRICT_ROWS_NAMED)
            statements.add("the same on the other " + (tables.size() - UNRESTRICT_ROWS_NAMED) + " listed");
        return String.join(", ", statements);
    }

    private static String describe(Findings findings)
    {
        StringBuilder what = new StringBuilder();
        if (!findings.restrictions.isEmpty())
            what.append("RESTRICT ").append(findings.restrictions);
        if (!findings.rowLevelGrants.isEmpty())
            what.append(what.length() > 0 ? "; " : "").append("row-level grants ").append(findings.rowLevelGrants);
        if (!findings.rowLevelRestrictedTables.isEmpty())
        {
            List<String> tables = new ArrayList<>(findings.rowLevelRestrictedTables.size());
            for (TableMetadata table : findings.rowLevelRestrictedTables)
                tables.add(describe(table));
            what.append(what.length() > 0 ? "; " : "").append("row-level restricted tables in the schema ").append(tables);
        }
        return what.toString();
    }

    /**
     * Sets the tables of the schema restricted to rows in what a scan of {@code system_auth.role_permissions} found: the
     * schema is read apart from that table, so that a failure of its read is never blamed on the schema.
     */
    private static Findings withSchema(Findings findings)
    {
        List<TableMetadata> tables = restrictedTables();
        findings.rowLevelRestrictedTables.clear();
        findings.rowLevelRestrictedTables.addAll(tables);
        return findings;
    }

    /**
     * Scans {@code system_auth.role_permissions}; an absent table (a node that never ran auth) yields no findings. The
     * tables of the schema restricted to rows are added by the passes.
     *
     * @param consistency null for a read of this node's replica, otherwise the consistency level of a cluster-wide read
     */
    @VisibleForTesting
    public static Findings scan(ConsistencyLevel consistency)
    {
        Findings findings = new Findings();
        TableMetadata table = Schema.instance.getTableMetadata(SchemaConstants.AUTH_KEYSPACE_NAME, AuthKeyspace.ROLE_PERMISSIONS);
        if (table == null)
        {
            // a node DSE ran always has the table: compatstatus shows the check as not made, never as an all-clear
            logger.warn("{}.{} is not in the schema: the DSE access controls it may hold cannot be checked",
                        SchemaConstants.AUTH_KEYSPACE_NAME, AuthKeyspace.ROLE_PERMISSIONS);
            findings.permissionsTableMissing = true;
            return findings;
        }
        boolean hasRestricted = hasColumn(table, RESTRICTED_COLUMN);
        boolean hasGrantables = hasColumn(table, GRANTABLES_COLUMN);
        String query = "SELECT role, resource, permissions" + (hasRestricted ? ", " + RESTRICTED_COLUMN : "") + (hasGrantables ? ", " + GRANTABLES_COLUMN : "") +
                       " FROM " + SchemaConstants.AUTH_KEYSPACE_NAME + '.' + AuthKeyspace.ROLE_PERMISSIONS;
        // the local read is paged; the cluster-wide one is not (the permissions table is small, and a read that times out is
        // retried for up to dse_restrictions_check_wait_seconds, then fails the check closed, with the local-only property
        // as the way out)
        UntypedResultSet rows = consistency == null ? QueryProcessor.executeInternalWithPaging(query, PageSize.inRows(PAGE_SIZE))
                                                    : QueryProcessor.process(query, consistency);
        readRows(rows, findings, hasRestricted, hasGrantables, DatabaseDescriptor.getAuthorizer() instanceof CassandraAuthorizer);
        findings.restrictions.sort(null);
        findings.rowLevelGrants.sort(null);
        findings.delegations.sort(null);
        findings.loneWriteGrants.sort(null);
        findings.newLoneWriteGrants.sort(null);
        return findings;
    }

    /**
     * Reads the rows of a scan into its findings. A read that fails part-way (the next page of this node's replica)
     * logs nothing: the lone grants it listed first are forgotten, so that the next read lists and logs them.
     */
    @VisibleForTesting
    static void readRows(Iterable<UntypedResultSet.Row> rows, Findings findings, boolean hasRestricted, boolean hasGrantables, boolean loneWriteGrants)
    {
        try
        {
            for (UntypedResultSet.Row row : rows)
                readRow(row, findings, hasRestricted, hasGrantables, loneWriteGrants);
        }
        catch (RuntimeException | Error e)
        {
            try
            {
                CassandraAuthorizer.forgetDseLoneWriteGrants(findings.newLoneWriteGrants);
            }
            catch (Throwable forgetting)
            {
                e.addSuppressed(forgetting);
            }
            throw e;
        }
    }

    private static void readRow(UntypedResultSet.Row row, Findings findings, boolean hasRestricted, boolean hasGrantables, boolean loneWriteGrants)
    {
        String role = row.getString("role");
        String resource = row.getString("resource");
        if (loneWriteGrants)
        {
            // a row left with no permissions is read too, so that a lone grant a DSE node revoked is forgotten
            Set<String> permissions = row.has("permissions") ? row.getSet("permissions", UTF8Type.instance) : Set.of();
            String lone = CassandraAuthorizer.readDseLoneWriteGrant(role, resource, permissions, findings.newLoneWriteGrants);
            if (lone != null)
                findings.loneWriteGrants.add(lone);
        }
        if (hasRestricted && row.has(RESTRICTED_COLUMN) && !row.getSet(RESTRICTED_COLUMN, UTF8Type.instance).isEmpty())
            findings.restrictions.add(permissionsEntry(role, resource, row.getSet(RESTRICTED_COLUMN, UTF8Type.instance)));
        // a row of a rows/ resource with no permissions grants nothing (its other cells are checked above and below)
        if (resource.startsWith(ROWS_RESOURCE_PREFIX) && row.has("permissions") && !row.getSet("permissions", UTF8Type.instance).isEmpty())
            findings.rowLevelGrants.add(permissionsEntry(role, resource, row.getSet("permissions", UTF8Type.instance)));
        if (hasGrantables && row.has(GRANTABLES_COLUMN) && !row.getSet(GRANTABLES_COLUMN, UTF8Type.instance).isEmpty())
            findings.delegations.add(permissionsEntry(role, resource, row.getSet(GRANTABLES_COLUMN, UTF8Type.instance)));
    }

    /**
     * The user tables DSE restricts to rows: those whose {@code extensions} map has the key {@value #RLAC_EXTENSION_KEY},
     * as {@code ks.table (tenant column c)}, sorted, with the names quoted where CQL needs it.
     */
    @VisibleForTesting
    static List<String> rowLevelRestrictedTables()
    {
        List<String> tables = new ArrayList<>();
        for (TableMetadata table : restrictedTables())
            tables.add(describe(table));
        return tables;
    }

    /** The user tables with the key {@value #RLAC_EXTENSION_KEY}, sorted by their CQL name. */
    private static List<TableMetadata> restrictedTables()
    {
        List<TableMetadata> tables = new ArrayList<>();
        for (TableMetadata table : userTables())
            if (table.params.extensions.containsKey(RLAC_EXTENSION_KEY))
                tables.add(table);
        tables.sort(Comparator.comparing(TableMetadata::toString));
        return tables;
    }

    /** A table restricted to rows, as {@code ks.table (tenant column c)}. */
    private static String describe(TableMetadata table)
    {
        return table + " (tenant column " + tenantColumn(table.params.extensions.get(RLAC_EXTENSION_KEY)) + ')';
    }

    /**
     * The tenant column named by a value of {@value #RLAC_EXTENSION_KEY}. DSE writes the name in the platform charset
     * (UTF-8 in practice, and a column name is ASCII in most schemas); a value that is not UTF-8 text, or holds a
     * character that could break or disguise a log line (control, format such as the bidirectional overrides, line or
     * paragraph separator), is shown in hexadecimal instead, and either form is cut at {@value #TENANT_COLUMN_SHOWN}
     * characters or bytes, since the value goes into logs and the refusal.
     */
    public static String tenantColumn(ByteBuffer value)
    {
        String name;
        try
        {
            name = ByteBufferUtil.string(value);
        }
        catch (CharacterCodingException e)
        {
            name = null;
        }
        if (name != null && name.codePoints().noneMatch(Dse6AccessControlCheck::unsafeInALogLine))
            return name.codePointCount(0, name.length()) <= TENANT_COLUMN_SHOWN
                   ? ColumnIdentifier.maybeQuote(name)
                   : ColumnIdentifier.maybeQuote(name.substring(0, name.offsetByCodePoints(0, TENANT_COLUMN_SHOWN))) + "... (cut)";
        return hexadecimal(value) + " (not a column name)";
    }

    /** The first {@value #TENANT_COLUMN_SHOWN} bytes of a value in hexadecimal, and whether it was cut. */
    private static String hexadecimal(ByteBuffer value)
    {
        ByteBuffer shown = value.duplicate();
        if (shown.remaining() > TENANT_COLUMN_SHOWN)
            shown.limit(shown.position() + TENANT_COLUMN_SHOWN);
        return "0x" + ByteBufferUtil.bytesToHex(shown) + (value.remaining() > TENANT_COLUMN_SHOWN ? "... (cut)" : "");
    }

    private static boolean unsafeInALogLine(int codePoint)
    {
        int type = Character.getType(codePoint);
        return Character.isISOControl(codePoint) || type == Character.FORMAT || type == Character.LINE_SEPARATOR ||
               type == Character.PARAGRAPH_SEPARATOR;
    }

    /**
     * The user tables whose {@code extensions} map has keys other than {@value #RLAC_EXTENSION_KEY}, as
     * {@code ks.table: [keys]}, sorted by the CQL name of the table; a key that could break or disguise a log line is
     * shown in hexadecimal, and a long one is cut, as the tenant column is.
     */
    @VisibleForTesting
    static List<String> tablesWithOtherExtensions()
    {
        List<TableMetadata> sorted = userTables();
        sorted.sort(Comparator.comparing(TableMetadata::toString));
        List<String> tables = new ArrayList<>();
        for (TableMetadata table : sorted)
        {
            Set<String> keys = new TreeSet<>(table.params.extensions.keySet());
            keys.remove(RLAC_EXTENSION_KEY);
            if (keys.isEmpty())
                continue;
            List<String> shown = new ArrayList<>(keys.size());
            for (String key : keys)
                shown.add(shownInALog(key));
            tables.add(table + ": " + shown);
        }
        return tables;
    }

    /**
     * An extension key as a log line may show it: quoted as a CQL string, so that a comma or a bracket in it does not
     * blur the list, cut when long, or its UTF-8 bytes in hexadecimal when it could break or disguise the line.
     */
    private static String shownInALog(String key)
    {
        if (key.codePoints().anyMatch(Dse6AccessControlCheck::unsafeInALogLine))
            return hexadecimal(ByteBufferUtil.bytes(key)) + " (bytes, not text)";
        boolean cut = key.codePointCount(0, key.length()) > TENANT_COLUMN_SHOWN;
        String shown = cut ? key.substring(0, key.offsetByCodePoints(0, TENANT_COLUMN_SHOWN)) : key;
        return '\'' + shown.replace("'", "''") + '\'' + (cut ? "... (cut)" : "");
    }

    private static List<TableMetadata> userTables()
    {
        List<TableMetadata> tables = new ArrayList<>();
        for (String keyspaceName : Schema.instance.getUserKeyspaces())
        {
            KeyspaceMetadata keyspace = Schema.instance.getKeyspaceMetadata(keyspaceName);
            if (keyspace != null)
                keyspace.tables.forEach(tables::add);
        }
        return tables;
    }

    /**
     * The other table extensions, in one INFO line: DSE writes no table extension key but {@value #RLAC_EXTENSION_KEY},
     * so they restrict nothing on DSE and give no reason to warn; the line keeps them visible in the default log all the
     * same, should another DSE feature prove to write some.
     */
    private static void logOtherExtensions()
    {
        List<String> tables = tablesWithOtherExtensions();
        if (!tables.isEmpty())
            logger.info("Tables with schema extensions other than {}, kept and not interpreted by this software (not DSE access controls): {}{}",
                        RLAC_EXTENSION_KEY, tables.size() <= OTHER_EXTENSIONS_LOGGED ? tables : tables.subList(0, OTHER_EXTENSIONS_LOGGED),
                        tables.size() <= OTHER_EXTENSIONS_LOGGED ? "" : " and " + (tables.size() - OTHER_EXTENSIONS_LOGGED) + " more");
    }

    private static boolean hasColumn(TableMetadata table, String name)
    {
        return table.getColumn(new ColumnIdentifier(name, true)) != null;
    }

    /** A row of {@code system_auth.role_permissions} as the refusals and warnings list it: {@code role 'r' on <resource>: [names]}. */
    public static String permissionsEntry(String role, String resource, Set<String> permissions)
    {
        Set<String> names = new TreeSet<>();
        for (String permission : permissions)
            names.add(safeInALog(permission));
        return "role '" + safeInALog(role) + "' on " + safeInALog(resource) + ": " + names;
    }

    /**
     * A role, resource or permission name of {@code role_permissions} as a log line or a refusal may show it: as it is,
     * or its first {@value #TENANT_COLUMN_SHOWN} UTF-8 bytes in hexadecimal when it holds a character that could break or
     * disguise the line.
     */
    private static String safeInALog(String name)
    {
        if (name.codePoints().noneMatch(Dse6AccessControlCheck::unsafeInALogLine))
            return name;
        return hexadecimal(ByteBufferUtil.bytes(name)) + " (bytes, not text)";
    }
}
