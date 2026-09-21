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
import org.apache.cassandra.repair.AbstractRepairTest;
import org.apache.cassandra.repair.SharedContext;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.TimeUUID;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Verifies that CoordinatorSessions.registerSession() logs an INFO line containing the
 * session ID, participant count, and table IDs — this event was previously completely silent.
 */
public class CoordinatorSessionsLoggingTest extends AbstractRepairTest
{
    private static TableMetadata cfm;
    private static ColumnFamilyStore cfs;

    @BeforeClass
    public static void setupClass()
    {
        SchemaLoader.prepareServer();
        cfm = CreateTableStatement.parse("CREATE TABLE tbl (k INT PRIMARY KEY, v INT)",
                                         "coordinatorsessionsloggingtest").build();
        SchemaLoader.createKeyspace("coordinatorsessionsloggingtest", KeyspaceParams.simple(1), cfm);
        cfs = Schema.instance.getColumnFamilyStoreInstance(cfm.id);
    }

    private ListAppender<ILoggingEvent> appender;

    @Before
    public void setUp()
    {
        appender = new ListAppender<>();
        appender.start();
        Logger logger = (Logger) LoggerFactory.getLogger(CoordinatorSessions.class);
        logger.addAppender(appender);
        logger.setLevel(Level.ALL);
    }

    @After
    public void tearDown()
    {
        Logger logger = (Logger) LoggerFactory.getLogger(CoordinatorSessions.class);
        logger.detachAppender(appender);
        appender.stop();
    }

    private List<String> infoMessages()
    {
        return appender.list.stream()
                            .filter(e -> e.getLevel() == Level.INFO)
                            .map(ILoggingEvent::getFormattedMessage)
                            .collect(Collectors.toList());
    }

    /**
     * registerSession() was previously completely silent. It must now emit an INFO line
     * containing the session UUID, participant count, participant addresses, and table IDs.
     */
    @Test
    public void registerSession_logsInfoWithSessionAndParticipants() throws Exception
    {
        TimeUUID sessionID = registerSession(cfs, true, true);
        CoordinatorSessions sessions = new CoordinatorSessions(SharedContext.Global.instance);
        sessions.registerSession(sessionID, PARTICIPANTS, false);

        List<String> infos = infoMessages();
        assertFalse("registerSession must emit at least one INFO log", infos.isEmpty());

        String msg = infos.stream()
                          .filter(m -> m.contains("Registered coordinator session"))
                          .findFirst()
                          .orElse("");

        assertFalse("No 'Registered coordinator session' INFO message found", msg.isEmpty());
        assertTrue("log must contain the session UUID",      msg.contains(sessionID.toString()));
        assertTrue("log must contain participant count '3'", msg.contains("3"));
        assertTrue("log must contain a participant address", msg.contains(PARTICIPANT1.toString()));
    }

    /**
     * The log line must include table IDs so the session can be correlated to specific tables.
     */
    @Test
    public void registerSession_logIncludesTableIds() throws Exception
    {
        TimeUUID sessionID = registerSession(cfs, true, true);
        CoordinatorSessions sessions = new CoordinatorSessions(SharedContext.Global.instance);
        sessions.registerSession(sessionID, PARTICIPANTS, false);

        String msg = infoMessages().stream()
                                   .filter(m -> m.contains("Registered coordinator session"))
                                   .findFirst()
                                   .orElse("");

        assertFalse("No registration INFO message found", msg.isEmpty());
        // cfm.id is included in the tableIds set; its string representation appears in the log
        assertTrue("log must contain table ID reference", msg.contains(cfm.id.toString()));
    }
}
