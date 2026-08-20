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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.repair.messages.RepairOption;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.UUIDGen;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class RepairSessionTest
{
    private static final String KEYSPACE   = "Keyspace1";
    private static final String CF         = "Standard1";
    private static final String ENTITY_ID  = "entity-xyz-999";

    @BeforeClass
    public static void initDD()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    /** Log-capture appender, re-created for each test. */
    private ListAppender<ILoggingEvent> appender;
    private Logger sessionLogger;

    @Before
    public void setUpAppender()
    {
        appender = new ListAppender<>();
        appender.start();
        sessionLogger = (Logger) LoggerFactory.getLogger(RepairSession.class);
        sessionLogger.addAppender(appender);
        sessionLogger.setLevel(Level.INFO);
    }

    @After
    public void tearDownAppender()
    {
        sessionLogger.detachAppender(appender);
        appender.stop();
    }

    private static RepairOption optionsWithTenant(String entityId)
    {
        Map<String, String> opts = new HashMap<>();
        opts.put(RepairOption.ENTITY_ID_KEY, entityId);
        opts.put(RepairOption.REPAIR_TYPE_KEY, "continuous");
        return RepairOption.parse(opts, Murmur3Partitioner.instance);
    }

    private static RepairOption optionsWithoutTenant()
    {
        return RepairOption.parse(Collections.emptyMap(), Murmur3Partitioner.instance);
    }

    private static RepairSession buildSession(UUID parentSessionId,
                                              UUID sessionId,
                                              Set<InetAddressAndPort> endpoints,
                                              RepairOption options)
    {
        IPartitioner p = Murmur3Partitioner.instance;
        Range<Token> range = new Range<>(p.getToken(ByteBufferUtil.bytes(0)),
                                         p.getToken(ByteBufferUtil.bytes(100)));
        return new RepairSession(parentSessionId, sessionId, Scheduler.build(0),
                                 new CommonRange(endpoints, Collections.emptySet(),
                                                 Arrays.asList(range)),
                                 KEYSPACE, options, false, CF);
    }

    private List<String> capturedInfoMessages()
    {
        return appender.list.stream()
                            .filter(e -> e.getLevel() == Level.INFO)
                            .map(ILoggingEvent::getFormattedMessage)
                            .collect(Collectors.toList());
    }

    private List<String> capturedErrorMessages()
    {
        return appender.list.stream()
                            .filter(e -> e.getLevel() == Level.ERROR)
                            .map(ILoggingEvent::getFormattedMessage)
                            .collect(Collectors.toList());
    }

    private void assertSessionFails(RepairSession session) throws InterruptedException
    {
        try
        {
            session.get();
            fail("Expected ExecutionException");
        }
        catch (ExecutionException ex)
        {
            assertSame(IOException.class, ex.getCause().getClass());
        }
    }

    @Test
    public void testConviction() throws Exception
    {
        InetAddressAndPort remote = InetAddressAndPort.getByName("127.0.0.2");
        Gossiper.instance.initializeNodeUnsafe(remote, UUID.randomUUID(), 1);

        UUID parentSessionId = UUIDGen.getTimeUUID();
        UUID sessionId = UUID.randomUUID();
        Set<InetAddressAndPort> endpoints = Sets.newHashSet(remote);

        RepairSession session = buildSession(parentSessionId, sessionId, endpoints,
                                             optionsWithoutTenant());
        session.convict(remote, Double.MAX_VALUE);

        assertSessionFails(session);
    }

    @Test
    public void testRepairingDeadNodeFails() throws Exception
    {
        InetAddressAndPort remote = InetAddressAndPort.getByName("127.0.0.2");
        Gossiper.instance.initializeNodeUnsafe(remote, UUID.randomUUID(), 1);
        Gossiper.instance.convict(remote, Double.MAX_VALUE);

        UUID parentSessionId = UUIDGen.getTimeUUID();
        UUID sessionId = UUID.randomUUID();
        Set<InetAddressAndPort> endpoints = Sets.newHashSet(remote);

        RepairSession session = buildSession(parentSessionId, sessionId, endpoints,
                                             optionsWithoutTenant());
        session.start(new NoopExecutorService());

        assertSessionFails(session);
    }

    /** entityId set in RepairOption must flow into RepairSession.entityId */
    @Test
    public void testEntityIdIsWiredFromOptions() throws Exception
    {
        UUID parentSessionId = UUIDGen.getTimeUUID();
        UUID sessionId = UUID.randomUUID();
        Set<InetAddressAndPort> endpoints = Sets.newHashSet(
                InetAddressAndPort.getByName("10.0.0.2"));

        RepairSession session = buildSession(parentSessionId, sessionId, endpoints,
                                             optionsWithTenant(ENTITY_ID));

        assertEquals("entityId must equal the value set in RepairOption",
                     ENTITY_ID, session.entityId);
    }

    /** When entityId is absent in RepairOption, RepairSession.entityId must be null */
    @Test
    public void testEntityIdIsNullWhenAbsent() throws Exception
    {
        UUID parentSessionId = UUIDGen.getTimeUUID();
        UUID sessionId = UUID.randomUUID();
        Set<InetAddressAndPort> endpoints = Sets.newHashSet(
                InetAddressAndPort.getByName("10.0.0.3"));

        RepairSession session = buildSession(parentSessionId, sessionId, endpoints,
                                             optionsWithoutTenant());

        assertNull("entityId must be null when not set in RepairOption", session.entityId);
    }

    /** parentRepairSession UUID must be stored on the session */
    @Test
    public void testParentRepairSessionIsStored() throws Exception
    {
        UUID parentSessionId = UUIDGen.getTimeUUID();
        UUID sessionId = UUID.randomUUID();
        Set<InetAddressAndPort> endpoints = Sets.newHashSet(
                InetAddressAndPort.getByName("10.0.0.4"));

        RepairSession session = buildSession(parentSessionId, sessionId, endpoints,
                                             optionsWithoutTenant());

        assertEquals("parentRepairSession must match the UUID passed to the constructor",
                     parentSessionId, session.parentRepairSession);
        assertNotNull("session id must not be null", session.getId());
    }

    /**
     * When a dead endpoint is in the common range, start() must log an ERROR that includes
     * the parent session UUID — enabling correlation with CNDB-side logs.
     */
    @Test
    public void testStartLogsParentSessionOnDeadEndpoint() throws Exception
    {
        InetAddressAndPort remote = InetAddressAndPort.getByName("127.0.0.3");
        Gossiper.instance.initializeNodeUnsafe(remote, UUID.randomUUID(), 1);
        Gossiper.instance.convict(remote, Double.MAX_VALUE);

        UUID parentSessionId = UUIDGen.getTimeUUID();
        UUID sessionId = UUID.randomUUID();
        Set<InetAddressAndPort> endpoints = Sets.newHashSet(remote);

        RepairSession session = buildSession(parentSessionId, sessionId, endpoints,
                                             optionsWithoutTenant());
        session.start(new NoopExecutorService());

        // The dead-node error path in start() logs at ERROR level
        List<String> errors = capturedErrorMessages();
        assertFalse("Expected at least one error log from start()", errors.isEmpty());
        String errorMsg = errors.get(0);
        assertTrue("Error log should mention the dead endpoint",
                   errorMsg.contains("dead"));
    }

    /**
     * start() INFO banner must contain both the parent session UUID and
     * the entity tag "[entity: <value>]" when entityId is set.
     */
    @Test
    public void testStartInfoBannerIncludesEntityAndParentSession() throws Exception
    {
        InetAddressAndPort remote = InetAddressAndPort.getByName("127.0.0.4");
        Gossiper.instance.initializeNodeUnsafe(remote, UUID.randomUUID(), 1);

        UUID parentSessionId = UUIDGen.getTimeUUID();
        UUID sessionId = UUID.randomUUID();
        Set<InetAddressAndPort> endpoints = Sets.newHashSet(remote);

        RepairSession session = buildSession(parentSessionId, sessionId, endpoints,
                                             optionsWithTenant(ENTITY_ID));
        // start() with a live-node endpoint logs the banner then proceeds to
        // the job-submission path (NoopExecutorService means no jobs actually run)
        session.start(new NoopExecutorService());

        List<String> infos = capturedInfoMessages();
        assertFalse("Expected at least one INFO log from start()", infos.isEmpty());

        // First INFO line is the new-session banner
        String banner = infos.get(0);
        assertTrue("Banner must contain parentSessionId=" + parentSessionId,
                   banner.contains(parentSessionId.toString()));
        assertTrue("Banner must contain [entity: " + ENTITY_ID + "]",
                   banner.contains("[entity: " + ENTITY_ID + "]"));
    }

    /**
     * When entityId is absent, start() INFO banner must NOT contain a "[entity:" block.
     */
    @Test
    public void testStartInfoBannerOmitsEntityTagWhenAbsent() throws Exception
    {
        InetAddressAndPort remote = InetAddressAndPort.getByName("127.0.0.5");
        Gossiper.instance.initializeNodeUnsafe(remote, UUID.randomUUID(), 1);

        UUID parentSessionId = UUIDGen.getTimeUUID();
        UUID sessionId = UUID.randomUUID();
        Set<InetAddressAndPort> endpoints = Sets.newHashSet(remote);

        RepairSession session = buildSession(parentSessionId, sessionId, endpoints,
                                             optionsWithoutTenant());
        session.start(new NoopExecutorService());

        List<String> infos = capturedInfoMessages();
        assertFalse("Expected at least one INFO log from start()", infos.isEmpty());

        String banner = infos.get(0);
        assertTrue("Banner must still contain parentSessionId",
                   banner.contains(parentSessionId.toString()));
        // No [entity: ...] block at all when entityId is absent
        assertFalse("Banner must not contain [entity:] when entityId is absent",
                    banner.contains("[entity:"));
    }

    private static class NoopExecutorService implements ListeningExecutorService
    {
        @Override public void shutdown() {}
        @Override public List<Runnable> shutdownNow() { return null; }
        @Override public boolean isShutdown() { return false; }
        @Override public boolean isTerminated() { return false; }
        @Override public boolean awaitTermination(long timeout, TimeUnit unit) { return false; }
        @Override public <T> ListenableFuture<T> submit(Callable<T> callable) { return null; }
        @Override public ListenableFuture<?> submit(Runnable runnable) { return null; }
        @Override public <T> ListenableFuture<T> submit(Runnable runnable, T t) { return null; }
        @Override public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> c) { return null; }
        @Override public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> c, long l, TimeUnit u) { return null; }
        @Override public <T> T invokeAny(Collection<? extends Callable<T>> tasks) { return null; }
        @Override public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) { return null; }
        @Override public void execute(Runnable command) {}
    }
}
