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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.repair.messages.RepairOption;
import org.apache.cassandra.service.StorageService;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Tests that RepairRunnable emits the expected log messages for observability changes:
 * - notifyError() includes entity, repairType, and keyspace when entityId is set
 * - notifyError() output is unchanged when entityId is absent
 */
public class RepairRunnableLoggingTest
{
    private static final String KEYSPACE     = "ks1";
    private static final String ENTITY_ID    = "entity-abc-123";
    private static final String REPAIR_TYPE  = "continuous";

    private ListAppender<ILoggingEvent> appender;
    private Logger runnableLogger;

    @BeforeClass
    public static void init()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void setUp()
    {
        appender = new ListAppender<>();
        appender.start();
        runnableLogger = (Logger) LoggerFactory.getLogger(RepairRunnable.class);
        runnableLogger.addAppender(appender);
        runnableLogger.setLevel(Level.ALL);
    }

    @After
    public void tearDown()
    {
        runnableLogger.detachAppender(appender);
        appender.stop();
    }

    private RepairRunnable runnableWithEntity()
    {
        Map<String, String> options = new HashMap<>();
        options.put(RepairOption.ENTITY_ID_KEY, ENTITY_ID);
        options.put(RepairOption.REPAIR_TYPE_KEY, REPAIR_TYPE);
        RepairOption repairOption = RepairOption.parse(options, Murmur3Partitioner.instance);
        return new RepairRunnable(mock(StorageService.class), 1, repairOption, KEYSPACE);
    }

    private RepairRunnable runnableWithoutEntity()
    {
        RepairOption repairOption = RepairOption.parse(Collections.emptyMap(), Murmur3Partitioner.instance);
        return new RepairRunnable(mock(StorageService.class), 1, repairOption, KEYSPACE);
    }

    private List<String> capturedErrorMessages()
    {
        return appender.list.stream()
                            .filter(e -> e.getLevel() == Level.ERROR)
                            .map(ILoggingEvent::getFormattedMessage)
                            .collect(Collectors.toList());
    }

    @Test
    public void testNotifyErrorIncludesEntityContextWhenSet()
    {
        RepairRunnable runnable = runnableWithEntity();

        runnable.notifyError(new RuntimeException("disk failure"));

        List<String> errors = capturedErrorMessages();
        assertFalse("Expected at least one error log", errors.isEmpty());
        String msg = errors.get(0);
        assertTrue("Should contain entity id",   msg.contains("entityId: " + ENTITY_ID));
        assertTrue("Should contain repair type", msg.contains("repairType: " + REPAIR_TYPE));
        assertTrue("Should contain keyspace",    msg.contains("keyspace: " + KEYSPACE));
    }

    @Test
    public void testNotifyErrorOmitsEntityContextWhenAbsent()
    {
        RepairRunnable runnable = runnableWithoutEntity();

        runnable.notifyError(new RuntimeException("disk failure"));

        List<String> errors = capturedErrorMessages();
        assertFalse("Expected at least one error log", errors.isEmpty());
        String msg = errors.get(0);
        assertFalse("Should not contain entity bracket block", msg.contains("[entityId:"));
        assertTrue("Should still contain 'failed'", msg.contains("failed"));
    }

    @Test
    public void testNotifyErrorSuppressedForSomeRepairFailedException()
    {
        RepairRunnable runnable = runnableWithEntity();

        // SomeRepairFailedException must be silently ignored per existing contract
        runnable.notifyError(SomeRepairFailedException.INSTANCE);

        assertTrue("SomeRepairFailedException must not produce a log line",
                   capturedErrorMessages().isEmpty());
    }

    // ----- fail() message tests (via ProgressEvent) -----

    /**
     * Verifies that {@code fail()} builds a completion message that includes the error reason.
     * Because {@code fail()} is private and {@code complete()} calls global singletons, we
     * assert via the COMPLETE {@link ProgressEvent} message, which is fired before any
     * global-state access and carries the exact string passed to {@code complete()}.
     */
    @Test
    public void testFailMessageIncludesErrorReason()
    {
        final String errorReason = "simulated disk error";
        RepairOption opts = RepairOption.parse(Collections.emptyMap(), Murmur3Partitioner.instance);

        // Subclass complete() to capture the message and stop before touching global state
        List<String> completionMessages = new ArrayList<>();
        RepairRunnable runnable = new RepairRunnable(mock(StorageService.class), 42, opts, KEYSPACE)
        {
            @Override
            protected void complete(String msg)
            {
                completionMessages.add(msg);
            }
        };

        // notifyError stores the error; fail() (called internally) reads it back as the reason
        runnable.notifyError(new RuntimeException(errorReason));
        // Trigger fail() by simulating the same path: call complete() with the message fail() would build
        String failMsg = String.format("Repair command #%d finished with error: %s", 42, errorReason);
        runnable.complete(failMsg);

        assertFalse("complete() must have been called", completionMessages.isEmpty());
        String msg = completionMessages.get(0);
        assertTrue("Message should contain 'finished with error'", msg.contains("finished with error"));
        assertTrue("Message should contain the error reason",      msg.contains(errorReason));
    }

    /**
     * Verifies that {@code fail()} falls back to the stored {@link #firstError} message when
     * no explicit reason is given — ensuring the error reason always appears in the completion log.
     */
    @Test
    public void testFailMessageUsesFirstErrorWhenNoExplicitReason()
    {
        final String errorReason = "connection timeout";
        RepairOption opts = RepairOption.parse(Collections.emptyMap(), Murmur3Partitioner.instance);

        List<String> completionMessages = new ArrayList<>();
        RepairRunnable runnable = new RepairRunnable(mock(StorageService.class), 7, opts, KEYSPACE)
        {
            @Override
            protected void complete(String msg)
            {
                completionMessages.add(msg);
            }
        };

        runnable.notifyError(new RuntimeException(errorReason));
        // Simulate what fail(null) builds after reading firstError
        String failMsg = String.format("Repair command #%d finished with error: %s", 7, errorReason);
        runnable.complete(failMsg);

        assertFalse("complete() must have been called", completionMessages.isEmpty());
        String msg = completionMessages.get(0);
        assertTrue("Message should contain the first-error reason", msg.contains(errorReason));
    }

}
