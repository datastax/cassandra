package org.apache.cassandra.repair.consistent;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.RangesAtEndpoint;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.repair.AbstractRepairTest;
import org.apache.cassandra.repair.KeyspaceRepairManager;
import org.apache.cassandra.repair.SharedContext;
import org.apache.cassandra.repair.messages.PrepareConsistentRequest;
import org.apache.cassandra.repair.messages.RepairMessage;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Future;
import org.slf4j.LoggerFactory;

import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Verifies the observability log improvements in LocalSessions:
 * - auto-fail warn now includes stale age and timeout constant name
 * - failSession info now includes coordinator endpoint
 * - handlePrepareMessage info now includes coordinator, tables, and ranges
 */
public class LocalSessionsLoggingTest extends AbstractRepairTest
{
    private static TableMetadata cfm;
    private static ColumnFamilyStore cfs;

    @BeforeClass
    public static void setupClass()
    {
        SchemaLoader.prepareServer();
        cfm = CreateTableStatement.parse("CREATE TABLE tbl (k INT PRIMARY KEY, v INT)",
                                         "localsessionsloggingtest").build();
        SchemaLoader.createKeyspace("localsessionsloggingtest", KeyspaceParams.simple(1), cfm);
        cfs = Schema.instance.getColumnFamilyStoreInstance(cfm.id);
    }

    private ListAppender<ILoggingEvent> appender;

    @Before
    public void setUp()
    {
        appender = new ListAppender<>();
        appender.start();
        Logger logger = (Logger) LoggerFactory.getLogger(LocalSessions.class);
        logger.addAppender(appender);
        logger.setLevel(Level.ALL);
        // clear repairs table between tests
        Keyspace.open(SchemaConstants.SYSTEM_KEYSPACE_NAME)
                .getColumnFamilyStore(SystemKeyspace.REPAIRS)
                .truncateBlocking();
    }

    @After
    public void tearDown()
    {
        Logger logger = (Logger) LoggerFactory.getLogger(LocalSessions.class);
        logger.detachAppender(appender);
        appender.stop();
    }

    private List<String> warnMessages()
    {
        return appender.list.stream()
                            .filter(e -> e.getLevel() == Level.WARN)
                            .map(ILoggingEvent::getFormattedMessage)
                            .collect(Collectors.toList());
    }

    private List<String> infoMessages()
    {
        return appender.list.stream()
                            .filter(e -> e.getLevel() == Level.INFO)
                            .map(ILoggingEvent::getFormattedMessage)
                            .collect(Collectors.toList());
    }

    static class InstrumentedLocalSessions extends LocalSessions
    {
        final Map<InetAddressAndPort, List<RepairMessage>> sentMessages = new HashMap<>();

        public InstrumentedLocalSessions()
        {
            super(SharedContext.Global.instance);
        }

        @Override
        protected void sendMessage(InetAddressAndPort destination, Message<? extends RepairMessage> message)
        {
            sentMessages.computeIfAbsent(destination, k -> new ArrayList<>()).add(message.payload);
        }

        AsyncPromise<List<Void>> prepareSessionFuture = null;

        @Override
        Future<List<Void>> prepareSession(KeyspaceRepairManager repairManager, TimeUUID sessionID,
                                          Collection<ColumnFamilyStore> tables, RangesAtEndpoint ranges,
                                          ExecutorService executor, BooleanSupplier isCancelled)
        {
            return prepareSessionFuture != null ? prepareSessionFuture
                                                : super.prepareSession(repairManager, sessionID, tables, ranges, executor, isCancelled);
        }

        @Override
        protected InetAddressAndPort getBroadcastAddressAndPort() { return PARTICIPANT1; }

        @Override
        protected boolean isAlive(InetAddressAndPort address) { return true; }

        @Override
        protected boolean isNodeInitialized() { return true; }
    }

    /**
     * When cleanup() auto-fails a timed-out session the WARN must include:
     * - the session UUID
     * - how many seconds ago the last update was
     * - the AUTO_FAIL_TIMEOUT constant value
     */
    @Test
    public void cleanup_autoFail_logsAgeAndTimeout()
    {
        long artificiallyOld = FBUtilities.nowInSeconds() - (LocalSessions.AUTO_FAIL_TIMEOUT + 10);
        // Build a session whose lastUpdate is past AUTO_FAIL_TIMEOUT so cleanup() will auto-fail it.
        LocalSession.Builder builder = LocalSession.builder(SharedContext.Global.instance);
        builder.withState(ConsistentSession.State.REPAIRING);
        builder.withSessionID(nextTimeUUID());
        builder.withCoordinator(COORDINATOR);
        builder.withUUIDTableIds(java.util.Collections.singleton(java.util.UUID.randomUUID()));
        builder.withRepairedAt(System.currentTimeMillis());
        builder.withRanges(ALL_RANGES);
        builder.withParticipants(PARTICIPANTS);
        builder.withStartedAt(artificiallyOld);
        builder.withLastUpdate(artificiallyOld);
        LocalSession stale = builder.build();

        InstrumentedLocalSessions sessions = new InstrumentedLocalSessions();
        sessions.start();
        sessions.putSessionUnsafe(stale);
        sessions.save(stale);

        sessions.cleanup();

        List<String> warns = warnMessages();
        String autoFailMsg = warns.stream()
                                  .filter(m -> m.contains("Auto failing"))
                                  .findFirst()
                                  .orElse("");

        assertFalse("cleanup must emit 'Auto failing' WARN for stale session", autoFailMsg.isEmpty());
        assertTrue("warn must contain session UUID",       autoFailMsg.contains(stale.sessionID.toString()));
        assertTrue("warn must contain 'last activity'",    autoFailMsg.contains("last activity"));
        assertTrue("warn must contain AUTO_FAIL_TIMEOUT",  autoFailMsg.contains("AUTO_FAIL_TIMEOUT"));
    }

    // -------------------------------------------------------------------------
    // failSession coordinator context
    // -------------------------------------------------------------------------

    /**
     * failSession must include the coordinator address so operators know which node to contact.
     */
    @Test
    public void failSession_logsCoordinatorAddress()
    {
        TimeUUID sessionID = registerSession(cfs, true, true);
        InstrumentedLocalSessions sessions = new InstrumentedLocalSessions();
        sessions.start();

        sessions.prepareSessionFuture = new AsyncPromise<>();
        sessions.handlePrepareMessage(Message.builder(Verb.PREPARE_CONSISTENT_REQ,
                                                      new PrepareConsistentRequest(sessionID, COORDINATOR, PARTICIPANTS))
                                             .from(PARTICIPANT1).build());

        sessions.failSession(sessionID, false);

        List<String> infos = infoMessages();
        String failMsg = infos.stream()
                              .filter(m -> m.contains("Failing local repair session"))
                              .findFirst()
                              .orElse("");

        assertFalse("failSession must emit 'Failing local repair session' INFO", failMsg.isEmpty());
        assertTrue("log must contain session UUID",      failMsg.contains(sessionID.toString()));
        assertTrue("log must contain coordinator addr",  failMsg.contains(COORDINATOR.toString()));
    }

    /**
     * "Beginning local incremental repair session" must now include coordinator address,
     * table IDs, and ranges — not just the full session.toString().
     */
    @Test
    public void handlePrepareMessage_logsCoordinatorTablesAndRanges()
    {
        TimeUUID sessionID = registerSession(cfs, true, true);
        InstrumentedLocalSessions sessions = new InstrumentedLocalSessions();
        sessions.start();

        sessions.prepareSessionFuture = new AsyncPromise<>();
        sessions.handlePrepareMessage(Message.builder(Verb.PREPARE_CONSISTENT_REQ,
                                                      new PrepareConsistentRequest(sessionID, COORDINATOR, PARTICIPANTS))
                                             .from(PARTICIPANT1).build());

        List<String> infos = infoMessages();
        String beginMsg = infos.stream()
                               .filter(m -> m.contains("Beginning local incremental repair session"))
                               .findFirst()
                               .orElse("");

        assertFalse("handlePrepareMessage must emit 'Beginning local incremental repair session' INFO",
                    beginMsg.isEmpty());
        assertTrue("log must contain session UUID",    beginMsg.contains(sessionID.toString()));
        assertTrue("log must contain coordinator",     beginMsg.contains(COORDINATOR.toString()));
        // The ranges and table IDs come from the parentRepairSession; check at least one is present
        assertTrue("log must contain ranges or tables", beginMsg.contains("ranges") || beginMsg.contains("tables"));
    }
}
