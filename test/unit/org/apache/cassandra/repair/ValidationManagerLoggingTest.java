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

import java.util.List;
import java.util.concurrent.Future;
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
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.repair.state.ValidationState;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.TimeUUID;
import org.slf4j.LoggerFactory;

import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Verifies that ValidationManager.doValidation() logs at INFO (not DEBUG) after the
 * isDebugEnabled guard was removed, and that the log line contains partition count,
 * estimated bytes, duration, and table descriptor.
 */
public class ValidationManagerLoggingTest extends AbstractRepairTest
{
    private static TableMetadata cfm;
    private static ColumnFamilyStore cfs;

    @BeforeClass
    public static void setupClass()
    {
        SchemaLoader.prepareServer();
        cfm = CreateTableStatement.parse("CREATE TABLE tbl (k INT PRIMARY KEY, v INT)",
                                         "validationmanagerloggingtest").build();
        SchemaLoader.createKeyspace("validationmanagerloggingtest", KeyspaceParams.simple(1), cfm);
        cfs = Schema.instance.getColumnFamilyStoreInstance(cfm.id);
    }

    private ListAppender<ILoggingEvent> appender;

    @Before
    public void setUp()
    {
        appender = new ListAppender<>();
        appender.start();
        Logger logger = (Logger) LoggerFactory.getLogger(ValidationManager.class);
        logger.addAppender(appender);
        logger.setLevel(Level.ALL);
        // Swallow outbound VALIDATION_RSP messages that validator.complete() sends
        MessagingService.instance().outboundSink.add((msg, to) -> true);
    }

    @After
    public void tearDown()
    {
        Logger logger = (Logger) LoggerFactory.getLogger(ValidationManager.class);
        logger.detachAppender(appender);
        appender.stop();
        MessagingService.instance().outboundSink.clear();
    }

    private List<ILoggingEvent> infoEvents()
    {
        return appender.list.stream()
                            .filter(e -> e.getLevel() == Level.INFO)
                            .collect(Collectors.toList());
    }

    private List<ILoggingEvent> debugEvents()
    {
        return appender.list.stream()
                            .filter(e -> e.getLevel() == Level.DEBUG)
                            .collect(Collectors.toList());
    }

    /**
     * After removing the isDebugEnabled guard, doValidation must emit an INFO line containing
     * partition count, estimated bytes, duration in msec, and the RepairJobDesc (which includes
     * keyspace and table name).  Previously this was only visible when DEBUG logging was enabled.
     */
    @Test
    public void doValidation_logsInfoWithTimingAndDescriptor() throws Exception
    {
        // parentSessionId must be a registered parent repair session UUID for getValidationIterator
        TimeUUID parentSessionId = registerSession(cfs, false, true);
        RepairJobDesc desc = new RepairJobDesc(parentSessionId, nextTimeUUID(),
                                              cfs.keyspace.getName(), cfs.name,
                                              ALL_RANGES);

        Validator validator = new Validator(new ValidationState(Clock.Global.clock(), desc,
                                                                FBUtilities.getBroadcastAddressAndPort()),
                                            FBUtilities.nowInSeconds(), PreviewKind.NONE);
        Future<?> future = ValidationManager.instance.submitValidation(cfs, validator);
        future.get(); // wait for validation to complete

        String msg = infoEvents().stream()
                                 .map(ILoggingEvent::getFormattedMessage)
                                 .filter(m -> m.contains("Validation of"))
                                 .findFirst()
                                 .orElse("");

        assertFalse("doValidation must emit a 'Validation of' INFO log", msg.isEmpty());
        assertTrue("log must mention 'msec'",          msg.contains("msec"));
        assertTrue("log must include the table name",  msg.contains(cfs.name));
    }

    /**
     * The 'Validation of X partitions...' line must not appear at DEBUG level.
     * The isDebugEnabled guard was removed; the line is now unconditionally INFO.
     */
    @Test
    public void doValidation_validationDurationNotAtDebugLevel() throws Exception
    {
        TimeUUID parentSessionId = registerSession(cfs, false, true);
        RepairJobDesc desc = new RepairJobDesc(parentSessionId, nextTimeUUID(),
                                              cfs.keyspace.getName(), cfs.name,
                                              ALL_RANGES);
        Validator validator = new Validator(new ValidationState(Clock.Global.clock(), desc,
                                                                FBUtilities.getBroadcastAddressAndPort()),
                                            FBUtilities.nowInSeconds(), PreviewKind.NONE);
        Future<?> future = ValidationManager.instance.submitValidation(cfs, validator);
        future.get();

        boolean hasDebugValidationLine = debugEvents().stream()
                                                      .map(ILoggingEvent::getFormattedMessage)
                                                      .anyMatch(m -> m.contains("Validation of") && m.contains("msec"));
        assertFalse("'Validation of X partitions...msec' must not appear at DEBUG", hasDebugValidationLine);
    }
}
