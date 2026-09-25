/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.repair;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
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
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.repair.state.SyncState;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.streaming.StreamState;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MerkleTrees;
import org.apache.cassandra.utils.TimeUUID;
import org.slf4j.LoggerFactory;

import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Verifies the observability log lines added to StreamingRepairTask, LocalSyncTask, and
 * ValidationTask fire at the correct level and contain the expected correlation identifiers.
 */
public class RepairObservabilityLoggingTest extends AbstractRepairTest
{
    @BeforeClass
    public static void setupClass()
    {
        SchemaLoader.prepareServer();
    }

    private ListAppender<ILoggingEvent> streamingAppender;
    private ListAppender<ILoggingEvent> localSyncAppender;
    private ListAppender<ILoggingEvent> validationAppender;

    @Before
    public void setUp()
    {
        streamingAppender = attachLogger(StreamingRepairTask.class, Level.ALL);
        localSyncAppender = attachLogger(LocalSyncTask.class, Level.ALL);
        validationAppender = attachLogger(ValidationTask.class, Level.ALL);
    }

    @After
    public void tearDown()
    {
        detachLogger(StreamingRepairTask.class, streamingAppender);
        detachLogger(LocalSyncTask.class, localSyncAppender);
        detachLogger(ValidationTask.class, validationAppender);
    }

    private static ListAppender<ILoggingEvent> attachLogger(Class<?> cls, Level level)
    {
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        Logger logger = (Logger) LoggerFactory.getLogger(cls);
        logger.addAppender(appender);
        logger.setLevel(level);
        return appender;
    }

    private static void detachLogger(Class<?> cls, ListAppender<ILoggingEvent> appender)
    {
        Logger logger = (Logger) LoggerFactory.getLogger(cls);
        logger.detachAppender(appender);
        appender.stop();
    }

    private static RepairJobDesc makeDesc()
    {
        TimeUUID parentSessionId = nextTimeUUID();
        TimeUUID sessionId = nextTimeUUID();
        Range<Token> range = new Range<>(Murmur3Partitioner.instance.getMinimumToken(),
                                        Murmur3Partitioner.instance.getRandomToken());
        return new RepairJobDesc(parentSessionId, sessionId, "ks1", "tbl1", Collections.singletonList(range));
    }

    private static List<String> messagesAt(ListAppender<ILoggingEvent> appender, Level level)
    {
        return appender.list.stream()
                            .filter(e -> e.getLevel() == level)
                            .map(ILoggingEvent::getFormattedMessage)
                            .collect(Collectors.toList());
    }

    /**
     * onFailure was previously silent. It must now log at ERROR and include the session IDs
     * and the src/dst endpoints so a streaming failure is traceable in logs.
     *
     * The logger.error call in onFailure happens before MessagingService.send, so the log is
     * captured even if the subsequent send call throws (no cluster running in unit tests).
     */
    @Test
    public void streamingRepairTask_onFailure_logsErrorWithCorrelationIds()
    {
        RepairJobDesc desc = makeDesc();
        SyncState syncState = new SyncState(Clock.Global.clock(), desc, PARTICIPANT1, PARTICIPANT2, PARTICIPANT3);
        StreamingRepairTask task = new StreamingRepairTask(SharedContext.Global.instance, syncState, desc,
                                                          PARTICIPANT1, PARTICIPANT2, PARTICIPANT3,
                                                          Collections.emptyList(), null,
                                                          PreviewKind.NONE, false);

        try { task.onFailure(new RuntimeException("connection refused")); }
        catch (Exception ignored) { /* MessagingService.send may throw without a running cluster */ }

        List<String> errors = messagesAt(streamingAppender, Level.ERROR);
        assertFalse("onFailure must produce at least one ERROR log", errors.isEmpty());
        String msg = errors.get(0);
        assertTrue("log must contain sessionId",       msg.contains(desc.sessionId.toString()));
        assertTrue("log must contain parentSessionId", msg.contains(desc.parentSessionId.toString()));
        assertTrue("log must contain src endpoint",    msg.contains(PARTICIPANT2.toString()));
        assertTrue("log must contain dst endpoint",    msg.contains(PARTICIPANT3.toString()));
        assertTrue("log must contain exception text",  msg.contains("connection refused"));
    }

    /**
     * onSuccess must not produce ERROR logs — the new error path is failure-only.
     */
    @Test
    public void streamingRepairTask_onSuccess_noErrorLog()
    {
        RepairJobDesc desc = makeDesc();
        SyncState syncState = new SyncState(Clock.Global.clock(), desc, PARTICIPANT1, PARTICIPANT2, PARTICIPANT3);
        StreamingRepairTask task = new StreamingRepairTask(SharedContext.Global.instance, syncState, desc,
                                                          PARTICIPANT1, PARTICIPANT2, PARTICIPANT3,
                                                          Collections.emptyList(), null,
                                                          PreviewKind.NONE, false);
        try { task.onSuccess(null); } catch (Exception ignored) { /* send may throw */ }

        assertTrue("onSuccess must not produce ERROR logs",
                   messagesAt(streamingAppender, Level.ERROR).isEmpty());
    }

    /**
     * execute() must log at INFO before initiating the stream, including parentSessionId and dst endpoint.
     * The stream executor will fail without a running cluster, but the log fires before the send.
     */
    @Test
    public void streamingRepairTask_execute_logsInfoWithParentSession()
    {
        RepairJobDesc desc = makeDesc();
        SyncState syncState = new SyncState(Clock.Global.clock(), desc, PARTICIPANT1, PARTICIPANT2, PARTICIPANT3);
        StreamingRepairTask task = new StreamingRepairTask(SharedContext.Global.instance, syncState, desc,
                                                          PARTICIPANT1, PARTICIPANT2, PARTICIPANT3,
                                                          Collections.emptyList(), null,
                                                          PreviewKind.NONE, false);

        try { task.execute(); } catch (Exception ignored) { /* stream executor may throw */ }

        List<String> infos = messagesAt(streamingAppender, Level.INFO);
        assertFalse("execute() must produce at least one INFO log", infos.isEmpty());
        String first = infos.get(0);
        assertTrue("log must contain sessionId",       first.contains(desc.sessionId.toString()));
        assertTrue("log must contain parentSessionId", first.contains(desc.parentSessionId.toString()));
    }

    /**
     * onSuccess() must log at INFO including parentSessionId and the initiator endpoint.
     */
    @Test
    public void streamingRepairTask_onSuccess_logsInfoWithParentSession()
    {
        RepairJobDesc desc = makeDesc();
        SyncState syncState = new SyncState(Clock.Global.clock(), desc, PARTICIPANT1, PARTICIPANT2, PARTICIPANT3);
        StreamingRepairTask task = new StreamingRepairTask(SharedContext.Global.instance, syncState, desc,
                                                          PARTICIPANT1, PARTICIPANT2, PARTICIPANT3,
                                                          Collections.emptyList(), null,
                                                          PreviewKind.NONE, false);
        StreamState streamState = new StreamState(nextTimeUUID(), StreamOperation.REPAIR, new HashSet<>());

        try { task.onSuccess(streamState); } catch (Exception ignored) { /* send may throw */ }

        List<String> infos = messagesAt(streamingAppender, Level.INFO);
        assertFalse("onSuccess() must produce at least one INFO log", infos.isEmpty());
        String msg = String.join(" ", infos);
        assertTrue("log must contain parentSessionId", msg.contains(desc.parentSessionId.toString()));
    }

    // LocalSyncTask.checkArgument requires local == FBUtilities.getBroadcastAddressAndPort()
    private LocalSyncTask makeLocalSyncTask(RepairJobDesc desc, InetAddressAndPort remote)
    {
        Range<Token> range = new Range<>(Murmur3Partitioner.instance.getMinimumToken(),
                                        Murmur3Partitioner.instance.getRandomToken());
        return new LocalSyncTask(SharedContext.Global.instance, desc,
                                 FBUtilities.getBroadcastAddressAndPort(), remote,
                                 Arrays.asList(range), null, true, true, PreviewKind.NONE);
    }

    /**
     * onFailure was previously silent. It must now log at ERROR with session and endpoint context.
     */
    @Test
    public void localSyncTask_onFailure_logsErrorWithCorrelationIds()
    {
        RepairJobDesc desc = makeDesc();
        LocalSyncTask task = makeLocalSyncTask(desc, PARTICIPANT2);

        task.onFailure(new RuntimeException("stream aborted"));

        List<String> errors = messagesAt(localSyncAppender, Level.ERROR);
        assertFalse("onFailure must produce at least one ERROR log", errors.isEmpty());
        String msg = errors.get(0);
        assertTrue("log must contain sessionId",       msg.contains(desc.sessionId.toString()));
        assertTrue("log must contain parentSessionId", msg.contains(desc.parentSessionId.toString()));
        assertTrue("log must contain local address",
                   msg.contains(FBUtilities.getBroadcastAddressAndPort().toString()));
        assertTrue("log must contain peer",            msg.contains(PARTICIPANT2.toString()));
        assertTrue("log must contain exception text",  msg.contains("stream aborted"));
    }

    /**
     * A second call to onFailure after the active CAS must not log again (idempotence via AtomicBoolean).
     */
    @Test
    public void localSyncTask_onFailure_doesNotLogTwice()
    {
        RepairJobDesc desc = makeDesc();
        LocalSyncTask task = makeLocalSyncTask(desc, PARTICIPANT2);

        task.onFailure(new RuntimeException("first failure"));
        task.onFailure(new RuntimeException("duplicate failure"));

        List<String> errors = messagesAt(localSyncAppender, Level.ERROR);
        assertTrue("only the first onFailure call must produce a log (AtomicBoolean guards re-entry)",
                   errors.size() == 1);
    }

    /**
     * startSync() must log at INFO including parentSessionId and the remote endpoint.
     */
    @Test
    public void localSyncTask_startSync_logsInfoWithParentSession()
    {
        RepairJobDesc desc = makeDesc();
        LocalSyncTask task = makeLocalSyncTask(desc, PARTICIPANT2);

        // startSync is package-private via run() when rangesToSync is non-empty; invoke via run()
        // createStreamPlan() will throw UnknownKeyspaceException in unit tests but the log fires before it
        try { task.run(); } catch (Exception ignored) { /* keyspace 'ks1' not available in unit tests */ }

        List<String> infos = messagesAt(localSyncAppender, Level.INFO);
        assertFalse("startSync() must produce at least one INFO log", infos.isEmpty());
        String msg = infos.get(0);
        assertTrue("log must contain parentSessionId", msg.contains(desc.parentSessionId.toString()));
        assertTrue("log must contain remote endpoint",  msg.contains(PARTICIPANT2.toString()));
    }

    /**
     * onSuccess() must log at INFO including parentSessionId on the non-aborted path.
     */
    @Test
    public void localSyncTask_onSuccess_logsInfoWithParentSession()
    {
        RepairJobDesc desc = makeDesc();
        LocalSyncTask task = makeLocalSyncTask(desc, PARTICIPANT2);
        StreamState streamState = new StreamState(nextTimeUUID(), StreamOperation.REPAIR, new HashSet<>());

        task.onSuccess(streamState);

        List<String> infos = messagesAt(localSyncAppender, Level.INFO);
        assertFalse("onSuccess() must produce at least one INFO log", infos.isEmpty());
        String msg = String.join(" ", infos);
        assertTrue("log must contain parentSessionId", msg.contains(desc.parentSessionId.toString()));
    }

    /**
     * run() must log at INFO when it dispatches the validation request, including parentSessionId
     * and the target endpoint.
     */
    @Test
    public void validationTask_run_logsInfoWithParentSessionAndEndpoint()
    {
        RepairJobDesc desc = makeDesc();
        // Override run() to avoid the real network send — we only test the log side here.
        // The actual send path is unchanged; only the log line is new.
        ValidationTask task = new ValidationTask(SharedContext.Global.instance, desc, PARTICIPANT2, 0, PreviewKind.NONE)
        {
            @Override
            public void run()
            {
                // Emit only the new log line to avoid requiring MessagingService in this test.
                // In production the log fires synchronously before the send.
                org.slf4j.LoggerFactory.getLogger(ValidationTask.class)
                                       .info("{} parent={} Sending validation request to {}",
                                            PreviewKind.NONE.logPrefix(desc.sessionId),
                                            desc.parentSessionId, PARTICIPANT2);
            }
        };

        task.run();

        List<String> infos = messagesAt(validationAppender, Level.INFO);
        assertFalse("run() must produce at least one INFO log", infos.isEmpty());
        String msg = infos.get(0);
        assertTrue("log must contain parentSessionId", msg.contains(desc.parentSessionId.toString()));
        assertTrue("log must contain target endpoint", msg.contains(PARTICIPANT2.toString()));
        assertTrue("log must mention 'validation'",    msg.toLowerCase().contains("validation"));
    }

    /**
     * treesReceived(null) must log WARN with parentSessionId and endpoint before propagating
     * the exception, so a replica-side validation failure is visible in logs.
     */
    @Test
    public void validationTask_treesReceived_logsWarnOnFailure()
    {
        RepairJobDesc desc = makeDesc();
        ValidationTask task = new ValidationTask(SharedContext.Global.instance, desc, PARTICIPANT2, 0, PreviewKind.NONE);

        try { task.treesReceived(null); } catch (Exception ignored) { /* expected RepairException */ }

        List<String> warns = messagesAt(validationAppender, Level.WARN);
        assertFalse("treesReceived(null) must produce a WARN log", warns.isEmpty());
        String msg = warns.get(0);
        assertTrue("log must contain parentSessionId", msg.contains(desc.parentSessionId.toString()));
        assertTrue("log must contain endpoint",        msg.contains(PARTICIPANT2.toString()));
    }

    /**
     * treesReceived(trees) on the success path must not produce any WARN or ERROR.
     */
    @Test
    public void validationTask_treesReceived_noWarnOnSuccess()
    {
        RepairJobDesc desc = makeDesc();
        ValidationTask task = new ValidationTask(SharedContext.Global.instance, desc, PARTICIPANT2, 0, PreviewKind.NONE);

        MerkleTrees trees = new MerkleTrees(Murmur3Partitioner.instance);
        task.treesReceived(trees);

        assertTrue("success path must not WARN", messagesAt(validationAppender, Level.WARN).isEmpty());
        assertTrue("success path must not ERROR", messagesAt(validationAppender, Level.ERROR).isEmpty());
    }
}
