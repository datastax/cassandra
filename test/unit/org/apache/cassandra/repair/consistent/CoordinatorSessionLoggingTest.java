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
package org.apache.cassandra.repair.consistent;

import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.repair.AbstractRepairTest;
import org.apache.cassandra.repair.SharedContext;
import org.apache.cassandra.repair.messages.FinalizePromise;
import org.apache.cassandra.repair.messages.PrepareConsistentResponse;
import org.apache.cassandra.repair.messages.RepairMessage;
import org.slf4j.LoggerFactory;

import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Verifies the observability log improvements in CoordinatorSession:
 * - handlePrepareResponse failure includes the participant's current state
 * - handleFinalizePromise rejection includes the participant's current state
 * - fail() is now at WARN (not INFO) and includes participant count and addresses
 */
public class CoordinatorSessionLoggingTest extends AbstractRepairTest
{
    private ListAppender<ILoggingEvent> appender;

    @Before
    public void setUp()
    {
        appender = new ListAppender<>();
        appender.start();
        Logger logger = (Logger) LoggerFactory.getLogger(CoordinatorSession.class);
        logger.addAppender(appender);
        logger.setLevel(Level.ALL);
    }

    @After
    public void tearDown()
    {
        Logger logger = (Logger) LoggerFactory.getLogger(CoordinatorSession.class);
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

    private static class InstrumentedCoordinatorSession extends CoordinatorSession
    {
        InstrumentedCoordinatorSession(Builder builder) { super(builder); }
    }

    private static InstrumentedCoordinatorSession createSession()
    {
        MockMessaging msg = new MockMessaging();
        CoordinatorSession.Builder builder = CoordinatorSession.builder(SharedContext.Global.instance);
        builder.withContext(SharedContext.Global.instance.withMessaging(msg));
        builder.withState(ConsistentSession.State.PREPARING);
        builder.withSessionID(nextTimeUUID());
        builder.withCoordinator(COORDINATOR);
        builder.withUUIDTableIds(Sets.newHashSet(java.util.UUID.randomUUID()));
        builder.withRepairedAt(System.currentTimeMillis());
        builder.withRanges(ALL_RANGES);
        builder.withParticipants(PARTICIPANTS);
        return new InstrumentedCoordinatorSession(builder);
    }

    /**
     * When a participant reports prepare failure the WARN must include the participant's current
     * state so operators can immediately see what state it was in when it refused.
     */
    @Test
    public void handlePrepareResponse_failure_logsParticipantState()
    {
        InstrumentedCoordinatorSession session = createSession();

        session.handlePrepareResponse(Message.out(Verb.PREPARE_CONSISTENT_RSP,
                                                   new PrepareConsistentResponse(session.sessionID, PARTICIPANT1, false)));

        List<String> warns = warnMessages();
        String msg = warns.stream()
                          .filter(m -> m.contains("failed the prepare phase"))
                          .findFirst()
                          .orElse("");

        assertFalse("A prepare failure must produce a WARN", msg.isEmpty());
        assertTrue("warn must contain the participant address", msg.contains(PARTICIPANT1.toString()));
        assertTrue("warn must contain the session ID",          msg.contains(session.sessionID.toString()));
        assertTrue("warn must contain 'current state'",         msg.contains("current state"));
    }

    /**
     * Successful prepare responses must not produce WARN-level output.
     */
    @Test
    public void handlePrepareResponse_success_noWarn()
    {
        InstrumentedCoordinatorSession session = createSession();

        session.handlePrepareResponse(Message.out(Verb.PREPARE_CONSISTENT_RSP,
                                                   new PrepareConsistentResponse(session.sessionID, PARTICIPANT1, true)));
        session.handlePrepareResponse(Message.out(Verb.PREPARE_CONSISTENT_RSP,
                                                   new PrepareConsistentResponse(session.sessionID, PARTICIPANT2, true)));

        assertTrue("Successful prepare responses must not produce WARN",
                   warnMessages().stream().noneMatch(m -> m.contains("failed the prepare phase")));
    }

    /**
     * When a participant rejects the finalization proposal the WARN must include the participant's
     * current state, allowing operators to identify why finalization was refused.
     */
    @Test
    public void handleFinalizePromise_rejection_logsParticipantState()
    {
        InstrumentedCoordinatorSession session = createSession();
        // Advance to REPAIRING state so handleFinalizePromise is reachable
        session.setParticipantState(PARTICIPANT1, ConsistentSession.State.PREPARED);
        session.setParticipantState(PARTICIPANT2, ConsistentSession.State.PREPARED);
        session.setParticipantState(PARTICIPANT3, ConsistentSession.State.PREPARED);
        session.setRepairing();
        appender.list.clear(); // ignore state-setup logs

        session.handleFinalizePromise(Message.out(Verb.FINALIZE_PROMISE_MSG,
                                                   new FinalizePromise(session.sessionID, PARTICIPANT1, false)));

        List<String> warns = warnMessages();
        String msg = warns.stream()
                          .filter(m -> m.contains("Finalization proposal"))
                          .findFirst()
                          .orElse("");

        assertFalse("A finalization rejection must produce a WARN", msg.isEmpty());
        assertTrue("warn must contain the participant address", msg.contains(PARTICIPANT1.toString()));
        assertTrue("warn must contain the session ID",          msg.contains(session.sessionID.toString()));
        assertTrue("warn must contain 'participant state'",     msg.contains("participant state"));
    }

    /**
     * fail() was previously logged at INFO. It must now log at WARN so session failures are
     * visible without scanning INFO-level noise. The message must include participant count
     * and the set of participant addresses.
     */
    @Test
    public void fail_logsAtWarnWithParticipants()
    {
        InstrumentedCoordinatorSession session = createSession();

        session.fail();

        List<String> warns = warnMessages();
        String msg = warns.stream()
                          .filter(m -> m.contains("Incremental repair session") && m.contains("failed"))
                          .findFirst()
                          .orElse("");

        assertFalse("fail() must produce a WARN log", msg.isEmpty());
        assertTrue("warn must contain session UUID",        msg.contains(session.sessionID.toString()));
        assertTrue("warn must contain participant count",   msg.contains(String.valueOf(PARTICIPANTS.size())));
        assertTrue("warn must contain a participant address", msg.contains(PARTICIPANT1.toString()));
    }

    /**
     * fail() must NOT produce an INFO-level "session failed" line — that was the old behaviour.
     */
    @Test
    public void fail_doesNotLogAtInfo()
    {
        InstrumentedCoordinatorSession session = createSession();

        session.fail();

        boolean hasInfoFailed = infoMessages().stream()
                                              .anyMatch(m -> m.contains("failed") && m.contains(session.sessionID.toString()));
        assertFalse("fail() must not produce an INFO 'failed' log (promoted to WARN)", hasInfoFailed);
    }
}
