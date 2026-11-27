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

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.QueryTrace;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DataStorageSpec;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.service.StorageService;
import org.assertj.core.api.Assertions;
import org.slf4j.Logger;

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
        cluster = init(Cluster.build(1)
                              .withConfig(config -> config.with(GOSSIP, NETWORK, NATIVE_PROTOCOL)
                                                          .set("user_defined_functions_enabled", true))
                              .start());
        coordinator = cluster.coordinator(1);
        node = cluster.get(1);

        // setup Java driver
        InetAddress address = node.config().broadcastAddress().getAddress();
        driverCluster = com.datastax.driver.core.Cluster.builder().addContactPoints(address).build();
        driverSession = driverCluster.connect(KEYSPACE);

        // set max log level everywhere, so we can verify that no sensitive data is logged at any level
        node.runOnInstance(() -> {
            try
            {
                StorageService.instance.setLoggingLevel(Logger.ROOT_LOGGER_NAME, "ALL");
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
        assertNoSecretsInLogs(mark, expected);
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
        assertNoSecretsInLogs(mark, expected);
    }

    /// Verify that the warn-level log message about failed prepared statement recreation doesn't leak the user data
    /// that can be included in the message of the exception that made the recreation fail.
    @SuppressWarnings("Convert2MethodRef") // method refs don't work with runOnInstance
    @Test
    public void testPreparedStatementRecreationErrorMessage()
    {
        table = "recreation_error_message";
        cluster.schemaChange(format("CREATE TABLE %s.%s (k int PRIMARY KEY, v int)"));

        // write directly to the system table a statement that can't be prepared because it has a text constant for an
        // int column, with the error message 'Invalid STRING constant (secret_k) for "k" of type int' for the client
        // and 'Invalid STRING constant (?) for "k" of type int' for the logs
        String cql = format("SELECT * FROM %s.%s WHERE k='secret_k'");
        ByteBuffer id = ByteBuffer.wrap(new byte[]{ 1, 2, 3 });
        node.executeInternal("INSERT INTO system.prepared_statements (prepared_id, logged_keyspace, query_string) VALUES (?, ?, ?)",
                             id, KEYSPACE, cql);

        long mark = node.logs().mark();
        node.runOnInstance(() -> QueryProcessor.instance.preloadPreparedStatements());

        assertNoSecretsInLogs(mark,
                              quote(withKeyspace("Prepared statement recreation error, " +
                                                 "removing statement: 010203 SELECT ... %s, " +
                                                 "error details: InvalidRequestException: " +
                                                 "Invalid STRING constant (?) for \"k\" of type int")));
    }

    /// Verify that the error-level log message about unparseable statements doesn't leak the user data that can be
    /// included in the message of the exception thrown by the parser.
    @Test
    public void testUnparseableStatement()
    {
        table = "unparseable_statement";

        // The dimension of the vector is not a valid int, so the parser fails with a NumberFormatException with the
        // message 'For input string: "99999999999"'. We use that number as a stand-in for sensitive data, since the
        // exception message includes the text of the offending token.
        String sensitive = "99999999999";
        String cql = format("CREATE TABLE %s.%s (k text PRIMARY KEY, v vector<float, " + sensitive + ">)");

        long mark = node.logs().mark();
        Assertions.assertThatThrownBy(() -> driverSession.execute(cql))
                  .hasMessageContaining(sensitive); // the client is allowed to see it

        Assertions.assertThat(node.logs().grep(mark, "could not be parsed").getResult()).isNotEmpty();
        Assertions.assertThat(node.logs().grep(mark, sensitive).getResult()).isEmpty();
        assertNoSecretsInLogs(mark, quote("The statement: [CREATE TABLE ...] could not be parsed, error: NumberFormatException"));
    }

    /// Verify that the binding of variables of prepared statements don't leak their values.
    @Test
    public void testPreparedStatementBoundVariables()
    {
        table = "bound_variables";
        cluster.schemaChange(format("CREATE TABLE %s.%s (k text PRIMARY KEY, v text)"));

        PreparedStatement insert = driverSession.prepare(format("INSERT INTO %s.%s (k, v) VALUES (?, ?)"));
        PreparedStatement select = driverSession.prepare(format("SELECT * FROM %s.%s WHERE k = ?"));

        long mark = node.logs().mark();
        driverSession.execute(insert.bind("secret_k", "secret_v"));
        assertNoSecretsInLogs(mark);

        mark = node.logs().mark();
        driverSession.execute(select.bind("secret_k"));
        assertNoSecretsInLogs(mark);
    }

    /// Verify that the logs about failed invocations of user-defined functions and aggregates don't leak the values
    /// that made them fail, which are included in the messages of the exceptions thrown by the function.
    @Test
    public void testUserDefinedFunctionFailure()
    {
        table = "udf_failure";
        cluster.schemaChange(format("CREATE TABLE %s.%s (k int PRIMARY KEY, v text)"));
        coordinator.execute(format("INSERT INTO %s.%s (k, v) VALUES (1, 'secret_v')"), ConsistencyLevel.ONE);

        cluster.schemaChange(format("CREATE FUNCTION %s.parse_int(input text) " +
                                    "CALLED ON NULL INPUT RETURNS int LANGUAGE java AS " +
                                    "'return Integer.parseInt(input);'"));
        cluster.schemaChange(format("CREATE FUNCTION %s.sum_parsed(state int, input text) " +
                                    "CALLED ON NULL INPUT RETURNS int LANGUAGE java AS " +
                                    "'return state + Integer.parseInt(input);'"));
        cluster.schemaChange(format("CREATE AGGREGATE %s.sum_all(text) SFUNC sum_parsed STYPE int INITCOND 0"));

        // the function fails with "NumberFormatException: For input string: "secret_v""
        long mark = node.logs().mark();
        Assertions.assertThatThrownBy(() -> driverSession.execute(format("SELECT " + KEYSPACE + ".parse_int(v) FROM %s.%s WHERE k = 1")))
                  .hasMessageContaining("For input string: \"secret_v\""); // the client is allowed to see it
        assertNoSecretsInLogs(mark, "Invocation of user-defined function", "failed with NumberFormatException");

        // same for the state function of an aggregate
        mark = node.logs().mark();
        Assertions.assertThatThrownBy(() -> driverSession.execute(format("SELECT " + KEYSPACE + ".sum_all(v) FROM %s.%s WHERE k = 1")))
                  .hasMessageContaining("For input string: \"secret_v\"");
        assertNoSecretsInLogs(mark, "Invocation of user-defined function", "failed with NumberFormatException");
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
        assertNoSecretsInLogs(mark, "Process " + quote(redactedCQL) + " @CL");
    }

    /// Verify that the trace-level log messages about received requests use redaction.
    @Test
    public void testReceivedMessages()
    {
        table = "received_messages";

        long mark = node.logs().mark();
        driverSession.execute(format("CREATE TABLE %s.%s (k text PRIMARY KEY, v text)"));
        assertNoSecretsInLogs(mark, quote("Received: QUERY CREATE TABLE ..."));

        // insert
        mark = node.logs().mark();
        driverSession.execute(format("INSERT INTO %s.%s (k, v) VALUES ('secret_k', 'secret_v')"));
        assertNoSecretsInLogs(mark, quote("Received: QUERY INSERT INTO ... [pageSize = 5000 rows]"));

        // select
        mark = node.logs().mark();
        driverSession.execute(format("SELECT * FROM %s.%s WHERE k = 'secret_k' AND v = 'secret_v' ALLOW FILTERING"));
        assertNoSecretsInLogs(mark, quote("Received: QUERY SELECT ... [pageSize = 5000 rows]"));

        // prepare
        mark = node.logs().mark();
        PreparedStatement prepared = driverSession.prepare(format("INSERT INTO %s.%s (k, v) VALUES ('secret_k', 'secret_v')"));
        assertNoSecretsInLogs(mark, quote("Received: PREPARE INSERT INTO ..."));

        // execute
        mark = node.logs().mark();
        driverSession.execute(prepared.bind());
        assertNoSecretsInLogs(mark, "Received: EXECUTE");

        // batch
        mark = node.logs().mark();
        BatchStatement batch = new BatchStatement();
        batch.add(new SimpleStatement(format("INSERT INTO %s.%s (k, v) VALUES ('secret_k1', 'secret_v1')")));
        batch.add(new SimpleStatement(format("UPDATE %s.%s SET v = 'secret_v2' WHERE k = 'secret_k2'")));
        driverSession.execute(batch);
        assertNoSecretsInLogs(mark, quote("Received: BATCH of [INSERT INTO ... with 0 values, UPDATE ... with 0 values]"));
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

        // ... but no sensitive data should make it to the logs, where tracing messages are redacted
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
        assertNoSecretsInLogs(mark, "has exceeded the size warning threshold .* with query " + quote(format(redactedCql)));
    }

    private void assertReadSizeFailure(String cql, String redactedCql)
    {
        long mark = node.logs().mark();
        Assertions.assertThatThrownBy(() -> coordinator.execute(format(cql), ConsistencyLevel.ONE)).isNotNull();
        assertNoSecretsInLogs(mark, "has exceeded the size failure threshold .* with query " + quote(format(redactedCql)));
    }

    private void assertNoSecretsInLogs(long mark, String... expected)
    {
        Assertions.assertThat(node.logs().grep(mark, "secret_").getResult()).isEmpty();

        for (String s : expected)
            Assertions.assertThat(node.logs().grep(mark, s).getResult()).isNotEmpty();
    }

    private static String format(String query)
    {
        return String.format(query, KEYSPACE, table);
    }
}
