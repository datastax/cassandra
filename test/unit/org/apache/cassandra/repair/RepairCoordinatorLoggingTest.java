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
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.RepairException;
import org.apache.cassandra.locator.RangesAtEndpoint;
import org.apache.cassandra.repair.messages.RepairOption;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Verifies the observability log improvements in RepairCoordinator:
 * - notifyError() WARN includes entityTag when entityId is set (warn-level repair abort)
 * - notifyError() ERROR includes entityTag when entityId is set (error-level repair failure)
 * - complete() INFO includes entityTag when entityId is set
 * - notifyStarting() INFO includes entityTag when entityId is set
 * - entityTag is absent (empty string) when entityId is not set
 */
public class RepairCoordinatorLoggingTest
{
    private static final String KEYSPACE    = "system";   // always present, no setup needed
    private static final String ENTITY_ID   = "ent-abc-123";
    private static final String REPAIR_TYPE = "scheduled";

    @BeforeClass
    public static void initDD()
    {
        SchemaLoader.prepareServer();
    }

    private ListAppender<ILoggingEvent> appender;
    private Logger coordinatorLogger;

    @Before
    public void setUpAppender()
    {
        appender = new ListAppender<>();
        appender.start();
        coordinatorLogger = (Logger) LoggerFactory.getLogger(RepairCoordinator.class);
        coordinatorLogger.addAppender(appender);
        coordinatorLogger.setLevel(Level.ALL);
    }

    @After
    public void tearDownAppender()
    {
        coordinatorLogger.detachAppender(appender);
        appender.stop();
    }

    private static RepairOption optionsWithEntity()
    {
        Map<String, String> opts = new HashMap<>();
        opts.put(RepairOption.ENTITY_ID_KEY,   ENTITY_ID);
        opts.put(RepairOption.REPAIR_TYPE_KEY, REPAIR_TYPE);
        return RepairOption.parse(opts, Murmur3Partitioner.instance);
    }

    private static RepairOption optionsWithoutEntity()
    {
        return RepairOption.parse(Collections.emptyMap(), Murmur3Partitioner.instance);
    }

    /**
     * Build a RepairCoordinator using the package-private constructor so we can supply stub
     * lambdas that avoid touching the real StorageService / token metadata.
     */
    private static RepairCoordinator build(RepairOption options)
    {
        return new RepairCoordinator(
                SharedContext.Global.instance,
                (ks, tables) -> Collections.emptyList(),          // no CFS needed for log tests
                ks -> RangesAtEndpoint.empty(SharedContext.Global.instance.broadcastAddressAndPort()),
                1, options, KEYSPACE);
    }

    private List<String> infoMessages()
    {
        return appender.list.stream()
                            .filter(e -> e.getLevel() == Level.INFO)
                            .map(ILoggingEvent::getFormattedMessage)
                            .collect(Collectors.toList());
    }

    private List<String> errorMessages()
    {
        return appender.list.stream()
                            .filter(e -> e.getLevel() == Level.ERROR)
                            .map(ILoggingEvent::getFormattedMessage)
                            .collect(Collectors.toList());
    }

    @Test
    public void notifyError_warnPath_includesEntityTag()
    {
        RepairCoordinator coordinator = build(optionsWithEntity());
        coordinator.notifyError(RepairException.warn("simulated abort"));

        String msg = errorMessages().stream()
                                    .filter(m -> m.contains("failed"))
                                    .findFirst()
                                    .orElse("");

        assertFalse("notifyError must emit an ERROR 'failed' log", msg.isEmpty());
        assertTrue("log must contain the repair id",       msg.contains(coordinator.state.id.toString()));
        assertTrue("log must contain [entityId: " + ENTITY_ID, msg.contains("[entityId: " + ENTITY_ID));
        assertTrue("log must contain repairType: " + REPAIR_TYPE, msg.contains("repairType: " + REPAIR_TYPE));
    }

    /**
     * When entityId is absent, notifyError() must NOT add an entity context block.
     */
    @Test
    public void notifyError_warnPath_noEntityTagWhenAbsent()
    {
        RepairCoordinator coordinator = build(optionsWithoutEntity());
        coordinator.notifyError(RepairException.warn("simulated abort"));

        String msg = errorMessages().stream()
                                    .filter(m -> m.contains("failed"))
                                    .findFirst()
                                    .orElse("");

        assertFalse("notifyError must emit an ERROR 'failed' log", msg.isEmpty());
        assertFalse("log must not contain [entityId:] when entityId is absent",
                    msg.contains("[entityId:"));
    }

    @Test
    public void notifyError_errorPath_includesEntityTag()
    {
        RepairCoordinator coordinator = build(optionsWithEntity());
        coordinator.notifyError(new RuntimeException("hard failure"));

        String msg = errorMessages().stream()
                                    .filter(m -> m.contains("failed"))
                                    .findFirst()
                                    .orElse("");

        assertFalse("notifyError(error) must emit an ERROR 'failed' log", msg.isEmpty());
        assertTrue("log must contain the repair id",      msg.contains(coordinator.state.id.toString()));
        assertTrue("log must contain [entityId: " + ENTITY_ID, msg.contains("[entityId: " + ENTITY_ID));
    }

    /**
     * When entityId is absent, notifyError() error path must NOT add an entityTag block.
     */
    @Test
    public void notifyError_errorPath_noEntityTagWhenAbsent()
    {
        RepairCoordinator coordinator = build(optionsWithoutEntity());
        coordinator.notifyError(new RuntimeException("hard failure"));

        String msg = errorMessages().stream()
                                    .filter(m -> m.contains("failed"))
                                    .findFirst()
                                    .orElse("");

        assertFalse("notifyError(error) must emit an ERROR 'failed' log", msg.isEmpty());
        assertFalse("log must not contain [entityId:] when entityId is absent",
                    msg.contains("[entityId:"));
    }

    private static void invokeNotifyStarting(RepairCoordinator coordinator)
    {
        try
        {
            java.lang.reflect.Method m = RepairCoordinator.class.getDeclaredMethod("notifyStarting");
            m.setAccessible(true);
            m.invoke(coordinator);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * notifyStarting() INFO log must include the entityTag when entityId is set.
     */
    @Test
    public void notifyStarting_includesEntityTag()
    {
        RepairCoordinator coordinator = build(optionsWithEntity());
        invokeNotifyStarting(coordinator);

        String msg = infoMessages().stream()
                                   .filter(m -> m.contains("Starting repair command"))
                                   .findFirst()
                                   .orElse("");

        assertFalse("notifyStarting() must emit 'Starting repair command' INFO", msg.isEmpty());
        assertTrue("log must contain [entityId: " + ENTITY_ID, msg.contains("[entityId: " + ENTITY_ID));
        assertTrue("log must contain repairType: " + REPAIR_TYPE, msg.contains("repairType: " + REPAIR_TYPE));
    }

    /**
     * When entityId is absent, notifyStarting() must NOT include an entityTag block.
     */
    @Test
    public void notifyStarting_noEntityTagWhenAbsent()
    {
        RepairCoordinator coordinator = build(optionsWithoutEntity());
        invokeNotifyStarting(coordinator);

        String msg = infoMessages().stream()
                                   .filter(m -> m.contains("Starting repair command"))
                                   .findFirst()
                                   .orElse("");

        assertFalse("notifyStarting() must emit 'Starting repair command' INFO", msg.isEmpty());
        assertFalse("log must not contain [entityId:] when entityId is absent",
                    msg.contains("[entityId:"));
    }
}
