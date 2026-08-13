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
 * - notifyError() includes tenant, repairType, and keyspace when tenantId is set
 * - notifyError() output is unchanged when tenantId is absent
 */
public class RepairRunnableLoggingTest
{
    private static final String KEYSPACE = "ks1";
    private static final String TENANT_ID = "tenant-abc-123";
    private static final String REPAIR_TYPE = "continuous";

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
        runnableLogger.setLevel(Level.ERROR);
    }

    @After
    public void tearDown()
    {
        runnableLogger.detachAppender(appender);
        appender.stop();
    }

    private RepairRunnable runnableWithTenant()
    {
        Map<String, String> options = new HashMap<>();
        options.put(RepairOption.TENANT_ID_KEY, TENANT_ID);
        options.put(RepairOption.REPAIR_TYPE_KEY, REPAIR_TYPE);
        RepairOption repairOption = RepairOption.parse(options, Murmur3Partitioner.instance);
        return new RepairRunnable(mock(StorageService.class), 1, repairOption, KEYSPACE);
    }

    private RepairRunnable runnableWithoutTenant()
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
    public void testNotifyErrorIncludesTenantContextWhenSet()
    {
        RepairRunnable runnable = runnableWithTenant();

        runnable.notifyError(new RuntimeException("disk failure"));

        List<String> errors = capturedErrorMessages();
        assertFalse("Expected at least one error log", errors.isEmpty());
        String msg = errors.get(0);
        assertTrue("Should contain tenant id",   msg.contains("tenant: " + TENANT_ID));
        assertTrue("Should contain repair type", msg.contains("type: " + REPAIR_TYPE));
        assertTrue("Should contain keyspace",    msg.contains("keyspace: " + KEYSPACE));
    }

    @Test
    public void testNotifyErrorOmitsTenantContextWhenAbsent()
    {
        RepairRunnable runnable = runnableWithoutTenant();

        runnable.notifyError(new RuntimeException("disk failure"));

        List<String> errors = capturedErrorMessages();
        assertFalse("Expected at least one error log", errors.isEmpty());
        String msg = errors.get(0);
        assertFalse("Should not contain tenant bracket block", msg.contains("[tenant:"));
        assertTrue("Should still contain 'failed'", msg.contains("failed"));
    }

    @Test
    public void testNotifyErrorSuppressedForSomeRepairFailedException()
    {
        RepairRunnable runnable = runnableWithTenant();

        // SomeRepairFailedException must be silently ignored per existing contract
        runnable.notifyError(SomeRepairFailedException.INSTANCE);

        assertTrue("SomeRepairFailedException must not produce a log line",
                   capturedErrorMessages().isEmpty());
    }

}
