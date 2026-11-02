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

package org.apache.cassandra.distributed.test;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.QueryTrace;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DataStorageSpec;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.service.StorageService;
import org.assertj.core.api.Assertions;

import static java.util.regex.Pattern.quote;
import static org.apache.cassandra.config.DataStorageSpec.DataStorageUnit.KIBIBYTES;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;

/// Tests for redaction of sensistive data in logs.
public class RedactionTest extends TestBaseImpl
{
    private static Cluster cluster;
    private static ICoordinator coordinator;
    private static IInvokableInstance node;

    private static com.datastax.driver.core.Cluster driverCluster;
    private static Session driverSession;

    private static String table;

    @BeforeClass
    public static void setupCluster() throws Exception
    {
        // setup cluster
        cluster = init(Cluster.build(1).withConfig(config -> config.with(GOSSIP, NETWORK, NATIVE_PROTOCOL)).start());
        coordinator = cluster.coordinator(1);
        node = cluster.get(1);

        // setup Java driver
        InetAddress address = node.config().broadcastAddress().getAddress();
        driverCluster = com.datastax.driver.core.Cluster.builder().addContactPoints(address).build();
        driverSession = driverCluster.connect(KEYSPACE);

        // set max log level in the tested classes
        node.runOnInstance(() -> {
            try
            {
                StorageService.instance.setLoggingLevel(QueryProcessor.class.getName(), "ALL");
                StorageService.instance.setLoggingLevel(SelectStatement.class.getName(), "ALL");
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        });
    }

    @AfterClass
    public static void closeCluster()
    {
        if (driverSession != null)
            driverSession.close();

        if (driverCluster != null)
            driverCluster.close();

        if (cluster != null)
            cluster.close();
    }

    /// Verify that the debug-level log message about stored/not-stored prepared statement uses redaction.
    @Test
    public void testPersistPreparedStatements()
    {
        boolean persist = CassandraRelevantProperties.PERSIST_PREPARED_STATEMENTS.getBoolean();
        try
        {
            testPersistPreparedStatements(true);
            testPersistPreparedStatements(false);
        }
        finally
        {
            node.runOnInstance(() -> CassandraRelevantProperties.PERSIST_PREPARED_STATEMENTS.setBoolean(persist));
        }
    }

    private void testPersistPreparedStatements(boolean persist)
    {
        node.runOnInstance(() -> CassandraRelevantProperties.PERSIST_PREPARED_STATEMENTS.setBoolean(persist));
        table = "prepared_statement_persisted_" + persist;

        assertPersistPreparedStatementLogsUseRedaction(
                format("CREATE TABLE %s.%s (k text, s text static, c text, v text, PRIMARY KEY (k, c))"),
                "CREATE TABLE ...",
                persist);

        assertPersistPreparedStatementLogsUseRedaction(
                format("INSERT INTO %s.%s (k, s, c, v) VALUES ('secret_k', 'secret_s', 'secret_c', 'secret_v')"),
                "INSERT INTO ...",
                persist);

        assertPersistPreparedStatementLogsUseRedaction(
                format("SELECT * FROM %s.%s WHERE k='secret_k' AND s='secret_s' AND c='secret_c' AND v='secret_v' ALLOW FILTERING"),
                "SELECT ...",
                persist);

        assertPersistPreparedStatementLogsUseRedaction(
                format("UPDATE %s.%s SET s='secret_s', v='secret_v' WHERE k='secret_k' AND c='secret_c'"),
                "UPDATE ...",
                persist);

        assertPersistPreparedStatementLogsUseRedaction(
                "BEGIN BATCH " +
                format("INSERT INTO %s.%s (k, s, c, v) VALUES ('secret_k', 'secret_s', 'secret_c', 'secret_v');") +
                format("UPDATE %s.%s SET s='secret_s', v='secret_v' WHERE k='secret_k' AND c='secret_c';") +
                "APPLY BATCH;",
                "BEGIN BATCH INSERT INTO ...",
                persist);
    }

    private void assertPersistPreparedStatementLogsUseRedaction(String cql, String redactedCQL, boolean persisted)
    {
        long mark = node.logs().mark();

        PreparedStatement statement = driverSession.prepare(cql);
        driverSession.execute(statement.bind());

        String expected = persisted
                          ? withKeyspace("stored prepared statement for logged keyspace '%s': ")
                          : withKeyspace("not persisting prepared statement for logged keyspace '%s': ");
        expected += quote(redactedCQL);
        assertLogsContain(mark, expected);
    }

    /// Verify that the warn-level log message about failed prepared statement recreation uses redaction.
    @SuppressWarnings("Convert2MethodRef") // method refs don't work with runOnInstance
    @Test
    public void testPreparedStatementRecreationError()
    {
        // write directly to the system table a statement that can't be prepared because its table doesn't exist
        String cql = String.format("SELECT * FROM %s.non_existing WHERE k='secret_k' AND c='secret_c'", KEYSPACE);
        ByteBuffer id = ByteBuffer.wrap(new byte[]{ 1, 2, 3, 4 });
        node.executeInternal("INSERT INTO system.prepared_statements (prepared_id, logged_keyspace, query_string) VALUES (?, ?, ?)",
                             id, KEYSPACE, cql);

        long mark = node.logs().mark();
        node.runOnInstance(() -> QueryProcessor.instance.preloadPreparedStatements());

        String expected = "Prepared statement recreation error, removing statement: .* " + quote("SELECT ... ") + KEYSPACE;
        assertLogsContain(mark, expected);
    }

    /// Verify that the trace-level log message about processed queries uses redaction.
    @Test
    public void testProcessedStatement()
    {
        table = "process_statement_trace";
        cluster.schemaChange(format("CREATE TABLE %s.%s (k text PRIMARY KEY, v text)"));

        assertProcessStatementLogsUseRedaction("INSERT INTO %s.%s (k, v) VALUES ('secret_k', 'secret_v')", "INSERT INTO ...");
        assertProcessStatementLogsUseRedaction("UPDATE %s.%s SET v = 'secret_v' WHERE k = 'secret_k'", "UPDATE ...");
        assertProcessStatementLogsUseRedaction("SELECT * FROM %s.%s WHERE k = 'secret_k'", "SELECT ...");
        assertProcessStatementLogsUseRedaction("DELETE FROM %s.%s WHERE k = 'secret_k'", "DELETE FROM ...");
    }

    private void assertProcessStatementLogsUseRedaction(String cql, String redactedCQL)
    {
        cql = format(cql);

        long mark = node.logs().mark();
        driverSession.execute(cql);
        assertLogsContain(mark, "Process " + quote(redactedCQL) + " @CL");
    }

    /// Verify that client `Tracing` don't use redaction, since it's meant to be consumed by the client.
    /// Also verify that it doesn't emit logs with sensitive information.
    @Test
    public void testTracing()
    {
        table = "parsing_trace";
        cluster.schemaChange(format("CREATE TABLE %s.%s (k text PRIMARY KEY, v text)"));

        assertTracingDoNotUseRedaction("INSERT INTO %s.%s (k, v) VALUES ('secret_k', 'secret_v')");
        assertTracingDoNotUseRedaction("UPDATE %s.%s SET v = 'secret_v' WHERE k = 'secret_k'");
        assertTracingDoNotUseRedaction("SELECT * FROM %s.%s WHERE k = 'secret_k'");
        assertTracingDoNotUseRedaction("DELETE FROM %s.%s WHERE k = 'secret_k'");
    }

    private void assertTracingDoNotUseRedaction(String cql)
    {
        cql = format(cql);

        long mark = node.logs().mark();
        QueryTrace trace = driverSession.execute(new SimpleStatement(cql).enableTracing())
                                        .getExecutionInfo()
                                        .getQueryTrace();

        // Tracing should contain unredacted queries, since it's meant for client consumption ...
        List<String> descriptions = trace.getEvents().stream().map(QueryTrace.Event::getDescription).collect(Collectors.toList());
        Assertions.assertThat(descriptions).contains("Parsing " + cql);
        Assertions.assertThat(descriptions).anyMatch(description -> description.contains("secret_"));

        // ... but no sensitive data should make it to the logs
        assertNoSecretsInLogs(mark);
    }

    /// Verify that the log messages for the guardrails about read size use redaction.
    @Test
    public void testReadSizeThresholds()
    {
        node.runOnInstance(() -> {
            DatabaseDescriptor.setReadThresholdsEnabled(true);
            DatabaseDescriptor.setCoordinatorReadSizeWarnThreshold(new DataStorageSpec.LongBytesBound(1, KIBIBYTES));
            DatabaseDescriptor.setCoordinatorReadSizeFailThreshold(new DataStorageSpec.LongBytesBound(4, KIBIBYTES));
        });

        table = "thresholds";
        cluster.schemaChange(format("CREATE TABLE %s.%s (k text, c int, v text, PRIMARY KEY (k, c))"));
        String value = "secret_" + "v".repeat(512);
        for (int c = 0; c < 10; c++)
            coordinator.execute(format("INSERT INTO %s.%s (k, c, v) VALUES ('secret_k', ?, ?)"), ConsistencyLevel.ONE, c, value);

        // the reads below return 3 rows (~1.5KiB), which is over the warn threshold but under the fail threshold
        assertReadSizeWarning("SELECT * FROM %s.%s WHERE k = 'secret_k' AND c < 3",
                              "SELECT * FROM %s.%s WHERE k = ? AND c < ?");
        assertReadSizeWarning("SELECT * FROM %s.%s WHERE k IN ('secret_k', 'secret_k2') AND c < 3",
                              "SELECT * FROM %s.%s WHERE k IN (?, ?) AND c < ?");
        assertReadSizeWarning("SELECT * FROM %s.%s WHERE v = '" + value + "' AND c < 3 ALLOW FILTERING",
                              "SELECT * FROM %s.%s WHERE v = ?[>100B] AND c < ?");

        // the reads below return 10 rows (~5KiB), which is over the fail threshold
        assertReadSizeFailure("SELECT * FROM %s.%s WHERE k = 'secret_k'",
                              "SELECT * FROM %s.%s WHERE k = ?");
        assertReadSizeFailure("SELECT * FROM %s.%s WHERE k IN ('secret_k', 'secret_k2')",
                              "SELECT * FROM %s.%s WHERE k IN (?, ?)");
        assertReadSizeFailure("SELECT * FROM %s.%s WHERE v = '" + value + "' ALLOW FILTERING",
                              "SELECT * FROM %s.%s WHERE v = ?[>100B]");
    }

    private void assertReadSizeWarning(String cql, String redactedCql)
    {
        long mark = node.logs().mark();
        coordinator.execute(format(cql), ConsistencyLevel.ONE);
        assertLogsContain(mark, "has exceeded the size warning threshold .* with query " + quote(format(redactedCql)));
    }

    private void assertReadSizeFailure(String cql, String redactedCql)
    {
        long mark = node.logs().mark();
        Assertions.assertThatThrownBy(() -> coordinator.execute(format(cql), ConsistencyLevel.ONE)).isNotNull();
        assertLogsContain(mark, "has exceeded the size failure threshold .* with query " + quote(format(redactedCql)));
    }

    private void assertLogsContain(long mark, String expected)
    {
        Assertions.assertThat(node.logs().grep(mark, expected).getResult()).hasSize(1);
        assertNoSecretsInLogs(mark);
    }

    private void assertNoSecretsInLogs(long mark)
    {
        Assertions.assertThat(node.logs().grep(mark, "secret_").getResult()).isEmpty();
    }

    private static String format(String query)
    {
        return String.format(query, KEYSPACE, table);
    }
}
