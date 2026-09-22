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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.repair.messages.SyncResponse;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.utils.WithResources;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.repair.messages.RepairOption;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.TimeUUID;
import org.slf4j.LoggerFactory;

import static org.apache.cassandra.config.CassandraRelevantProperties.NODES_DISABLE_PERSISTING_TO_SYSTEM_KEYSPACE;
import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;
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
    private static final String ENTITY_ID   = "entity-xyz-999";
    private static final String REPAIR_TYPE = "continuous";

    @BeforeClass
    public static void initDD()
    {
        NODES_DISABLE_PERSISTING_TO_SYSTEM_KEYSPACE.setBoolean(true);
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE,
                                    org.apache.cassandra.schema.KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE, CF));
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
        opts.put(RepairOption.REPAIR_TYPE_KEY, REPAIR_TYPE);
        return RepairOption.parse(opts, Murmur3Partitioner.instance);
    }

    private static RepairOption optionsWithoutTenant()
    {
        return RepairOption.parse(Collections.emptyMap(), Murmur3Partitioner.instance);
    }

    /**
     * Build a RepairSession for tests. All session flags come from {@code options}.
     */
    private static RepairSession buildSession(TimeUUID parentSessionId,
                                              Set<InetAddressAndPort> endpoints,
                                              RepairOption options)
    {
        IPartitioner p = Murmur3Partitioner.instance;
        Range<Token> range = new Range<>(p.getToken(ByteBufferUtil.bytes(0)),
                                         p.getToken(ByteBufferUtil.bytes(100)));
        return new RepairSession(SharedContext.Global.instance,
                                 Scheduler.build(0),
                                 parentSessionId,
                                 new CommonRange(endpoints, Collections.emptySet(), Arrays.asList(range)),
                                 KEYSPACE,
                                 options,
                                 options.isIncremental(),
                                 CF);
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

        TimeUUID parentSessionId = nextTimeUUID();
        Set<InetAddressAndPort> endpoints = Sets.newHashSet(remote);
        RepairSession session = buildSession(parentSessionId, endpoints, optionsWithoutTenant());
        session.convict(remote, Double.MAX_VALUE);

        assertSessionFails(session);
    }

    @Test
    public void testRepairingDeadNodeFails() throws Exception
    {
        InetAddressAndPort remote = InetAddressAndPort.getByName("127.0.0.2");
        Gossiper.instance.initializeNodeUnsafe(remote, UUID.randomUUID(), 1);
        Gossiper.instance.convict(remote, Double.MAX_VALUE);

        TimeUUID parentSessionId = nextTimeUUID();
        Set<InetAddressAndPort> endpoints = Sets.newHashSet(remote);
        RepairSession session = buildSession(parentSessionId, endpoints, optionsWithoutTenant());
        session.start(new NoopExecutorService());

        assertSessionFails(session);
    }

    /** entityId set in RepairOption must flow into RepairSession.entityId */
    @Test
    public void testEntityIdIsWiredFromOptions() throws Exception
    {
        TimeUUID parentSessionId = nextTimeUUID();
        Set<InetAddressAndPort> endpoints = Sets.newHashSet(
                InetAddressAndPort.getByName("10.0.0.2"));

        RepairSession session = buildSession(parentSessionId, endpoints, optionsWithTenant(ENTITY_ID));

        assertEquals("entityId must equal the value set in RepairOption",
                     ENTITY_ID, session.entityId);
    }

    /** When entityId is absent in RepairOption, RepairSession.entityId must be null */
    @Test
    public void testEntityIdIsNullWhenAbsent() throws Exception
    {
        TimeUUID parentSessionId = nextTimeUUID();
        Set<InetAddressAndPort> endpoints = Sets.newHashSet(
                InetAddressAndPort.getByName("10.0.0.3"));

        RepairSession session = buildSession(parentSessionId, endpoints, optionsWithoutTenant());

        assertNull("entityId must be null when not set in RepairOption", session.entityId);
    }

    /** parentRepairSession UUID must be stored on the session */
    @Test
    public void testParentRepairSessionIsStored() throws Exception
    {
        TimeUUID parentSessionId = nextTimeUUID();
        Set<InetAddressAndPort> endpoints = Sets.newHashSet(
                InetAddressAndPort.getByName("10.0.0.4"));

        RepairSession session = buildSession(parentSessionId, endpoints, optionsWithoutTenant());

        assertEquals("parentRepairSession must match the UUID passed to the constructor",
                     parentSessionId, session.state.parentRepairSession);
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

        TimeUUID parentSessionId = nextTimeUUID();
        Set<InetAddressAndPort> endpoints = Sets.newHashSet(remote);

        RepairSession session = buildSession(parentSessionId, endpoints, optionsWithoutTenant());
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
     * the entity tag "[entityId: &lt;value&gt;]" when entityId is set.
     */
    @Test
    public void testStartInfoBannerIncludesEntityAndParentSession() throws Exception
    {
        InetAddressAndPort remote = InetAddressAndPort.getByName("127.0.0.4");
        Gossiper.instance.initializeNodeUnsafe(remote, UUID.randomUUID(), 1);

        TimeUUID parentSessionId = nextTimeUUID();
        Set<InetAddressAndPort> endpoints = Sets.newHashSet(remote);

        RepairSession session = buildSession(parentSessionId, endpoints, optionsWithTenant(ENTITY_ID));
        // start() with a live-node endpoint logs the banner then proceeds to
        // the job-submission path (NoopExecutorService means no jobs actually run)
        session.start(new NoopExecutorService());

        List<String> infos = capturedInfoMessages();
        assertFalse("Expected at least one INFO log from start()", infos.isEmpty());

        // First INFO line is the new-session banner
        String banner = infos.get(0);
        assertTrue("Banner must contain parentSessionId=" + parentSessionId,
                   banner.contains(parentSessionId.toString()));
        assertTrue("Banner must contain [entityId: " + ENTITY_ID + ", repairType: " + REPAIR_TYPE + "]",
                   banner.contains("[entityId: " + ENTITY_ID + ", repairType: " + REPAIR_TYPE + "]"));
    }

    /**
     * When entityId is absent, start() INFO banner must NOT contain a "[entityId:" block.
     */
    @Test
    public void testStartInfoBannerOmitsEntityTagWhenAbsent() throws Exception
    {
        InetAddressAndPort remote = InetAddressAndPort.getByName("127.0.0.5");
        Gossiper.instance.initializeNodeUnsafe(remote, UUID.randomUUID(), 1);

        TimeUUID parentSessionId = nextTimeUUID();
        Set<InetAddressAndPort> endpoints = Sets.newHashSet(remote);

        RepairSession session = buildSession(parentSessionId, endpoints, optionsWithoutTenant());
        session.start(new NoopExecutorService());

        List<String> infos = capturedInfoMessages();
        assertFalse("Expected at least one INFO log from start()", infos.isEmpty());

        String banner = infos.get(0);
        assertTrue("Banner must still contain parentSessionId",
                   banner.contains(parentSessionId.toString()));
        // No [entityId: ...] block at all when entityId is absent
        assertFalse("Banner must not contain [entityId:] when entityId is absent",
                    banner.contains("[entityId:"));
    }

    // -------------------------------------------------------------------------
    // start() banner log — new session
    // -------------------------------------------------------------------------

    /**
     * start() INFO banner must contain parentSession, the column family, and the range.
     */
    @Test
    public void testStartBannerContainsParentSessionAndCf() throws Exception
    {
        InetAddressAndPort remote = InetAddressAndPort.getByName("127.0.0.6");
        Gossiper.instance.initializeNodeUnsafe(remote, UUID.randomUUID(), 1);

        TimeUUID parentSessionId = nextTimeUUID();
        Set<InetAddressAndPort> endpoints = Sets.newHashSet(remote);

        RepairSession session = buildSession(parentSessionId, endpoints, optionsWithoutTenant());
        session.start(new NoopExecutorService());

        List<String> infos = capturedInfoMessages();
        assertFalse("start() must emit an INFO banner", infos.isEmpty());
        String banner = infos.stream()
                             .filter(m -> m.contains("new session"))
                             .findFirst()
                             .orElse("");
        assertFalse("Banner must contain 'new session'", banner.isEmpty());
        assertTrue("Banner must contain parentSession=" + parentSessionId,
                   banner.contains("parentSession=" + parentSessionId));
        assertTrue("Banner must contain column family",  banner.contains(CF));
    }

    /**
     * start() INFO banner must include entityTag when entityId is set.
     */
    @Test
    public void testStartBannerIncludesEntityTagWhenSet() throws Exception
    {
        InetAddressAndPort remote = InetAddressAndPort.getByName("127.0.0.7");
        Gossiper.instance.initializeNodeUnsafe(remote, UUID.randomUUID(), 1);

        TimeUUID parentSessionId = nextTimeUUID();
        RepairSession session = buildSession(parentSessionId,
                                             Sets.newHashSet(remote),
                                             optionsWithTenant(ENTITY_ID));
        session.start(new NoopExecutorService());

        String banner = capturedInfoMessages().stream()
                                              .filter(m -> m.contains("new session"))
                                              .findFirst()
                                              .orElse("");
        assertFalse("Banner must exist", banner.isEmpty());
        assertTrue("Banner must contain [entityId: " + ENTITY_ID,
                   banner.contains("[entityId: " + ENTITY_ID));
        assertTrue("Banner must contain repairType: " + REPAIR_TYPE,
                   banner.contains("repairType: " + REPAIR_TYPE));
    }

    // -------------------------------------------------------------------------
    // onSuccess / onFailure callbacks — session-level completion logs
    // -------------------------------------------------------------------------

    /** Subclass that widens protected access so tests can resolve jobs directly. */
    private static class ResolvableRepairJob extends RepairJob
    {
        private final RepairJobDesc jobDesc;

        ResolvableRepairJob(RepairSession session, String cf)
        {
            super(session, cf);
            // Reconstruct the same desc that RepairJob builds internally
            jobDesc = new RepairJobDesc(session.state.parentRepairSession, session.getId(),
                                        session.state.keyspace, cf, session.state.commonRange.ranges);
        }

        public void succeed()
        {
            trySuccess(new RepairResult(jobDesc, Collections.emptyList()));
        }

        public void fail(Throwable t)
        {
            tryFailure(t);
        }
    }

    /**
     * Session subclass that overrides createJob() to produce ResolvableRepairJob instances,
     * enabling tests to call succeed()/fail() after start() without needing reflection.
     */
    private static class ResolvableRepairSession extends RepairSession
    {
        final List<ResolvableRepairJob> createdJobs = new ArrayList<>();

        ResolvableRepairSession(TimeUUID parentSessionId, CommonRange commonRange,
                                String keyspace, RepairOption options, String... cfnames)
        {
            super(SharedContext.Global.instance, Scheduler.build(0),
                  parentSessionId, commonRange, keyspace, options, options.isIncremental(), cfnames);
        }

        @Override
        protected RepairJob createJob(String columnFamily)
        {
            ResolvableRepairJob job = new ResolvableRepairJob(this, columnFamily);
            createdJobs.add(job);
            return job;
        }
    }

    /**
     * Helper: register a parent repair session and build a {@link ResolvableRepairSession}.
     */
    private ResolvableRepairSession buildLiveSession(TimeUUID parentSessionId,
                                                     InetAddressAndPort endpoint,
                                                     RepairOption options)
    {
        ActiveRepairService.instance().registerParentRepairSession(
                parentSessionId, FBUtilities.getBroadcastAddressAndPort(),
                Collections.singletonList(Keyspace.open(KEYSPACE).getColumnFamilyStore(CF)),
                Sets.newHashSet(new Range<>(
                        Murmur3Partitioner.instance.getToken(ByteBufferUtil.bytes(0)),
                        Murmur3Partitioner.instance.getToken(ByteBufferUtil.bytes(100)))),
                false, ActiveRepairService.UNREPAIRED_SSTABLE, false, PreviewKind.NONE);

        IPartitioner p = Murmur3Partitioner.instance;
        Range<Token> range = new Range<>(p.getToken(ByteBufferUtil.bytes(0)),
                                         p.getToken(ByteBufferUtil.bytes(100)));
        return new ResolvableRepairSession(parentSessionId,
                                           new CommonRange(Sets.newHashSet(endpoint), Collections.emptySet(), Arrays.asList(range)),
                                           KEYSPACE, options, CF);
    }

    /**
     * When all RepairJobs succeed, the session must log INFO containing
     * "session completed successfully", parentSession=, column family, and entityTag when set.
     */
    @Test
    public void testOnSuccessLogsInfoWithParentSessionAndEntityTag() throws Exception
    {
        InetAddressAndPort remote = InetAddressAndPort.getByName("10.0.3.1");
        Gossiper.instance.initializeNodeUnsafe(remote, UUID.randomUUID(), 1);

        TimeUUID parentSessionId = nextTimeUUID();
        ResolvableRepairSession session = buildLiveSession(parentSessionId, remote,
                                                           optionsWithTenant(ENTITY_ID));

        // start() calls createJob() -> populates session.createdJobs; NoopExecutorService
        // prevents the jobs from running so we can resolve them below manually.
        session.start(new NoopExecutorService());

        // Resolve all created jobs as successful
        for (ResolvableRepairJob job : session.createdJobs)
            job.succeed();

        // Wait for session to complete (callback runs on taskExecutor)
        try { session.get(5, TimeUnit.SECONDS); } catch (Exception ignored) {}

        String log = capturedInfoMessages().stream()
                                           .filter(m -> m.contains("session completed successfully"))
                                           .findFirst()
                                           .orElse("");
        assertFalse("onSuccess must emit 'session completed successfully' INFO", log.isEmpty());
        assertTrue("log must contain parentSession=" + parentSessionId,
                   log.contains("parentSession=" + parentSessionId));
        assertTrue("log must contain column family",  log.contains(CF));
        assertTrue("log must contain [entityId: " + ENTITY_ID,
                   log.contains("[entityId: " + ENTITY_ID));
    }

    /**
     * When a RepairJob fails, the session must log ERROR containing
     * "session failed on range", parentSession=, column family, and entityTag when set.
     */
    @Test
    public void testOnFailureLogsErrorWithParentSessionAndEntityTag() throws Exception
    {
        InetAddressAndPort remote = InetAddressAndPort.getByName("10.0.4.1");
        Gossiper.instance.initializeNodeUnsafe(remote, UUID.randomUUID(), 1);

        TimeUUID parentSessionId = nextTimeUUID();
        ResolvableRepairSession session = buildLiveSession(parentSessionId, remote,
                                                           optionsWithTenant(ENTITY_ID));

        session.start(new NoopExecutorService());

        // Fail all created jobs
        IOException cause = new IOException("injected repair failure");
        for (ResolvableRepairJob job : session.createdJobs)
            job.fail(cause);

        // Wait for session to settle
        try { session.get(5, TimeUnit.SECONDS); } catch (Exception ignored) {}

        List<String> errors = appender.list.stream()
                                           .filter(e -> e.getLevel() == Level.ERROR)
                                           .map(ILoggingEvent::getFormattedMessage)
                                           .collect(Collectors.toList());
        String log = errors.stream()
                           .filter(m -> m.contains("session failed on range"))
                           .findFirst()
                           .orElse("");
        assertFalse("onFailure must emit 'session failed on range' ERROR", log.isEmpty());
        assertTrue("log must contain parentSession=" + parentSessionId,
                   log.contains("parentSession=" + parentSessionId));
        assertTrue("log must contain column family", log.contains(CF));
        assertTrue("log must contain [entityId: " + ENTITY_ID,
                   log.contains("[entityId: " + ENTITY_ID));
    }

    /**
     * Helper: build a RepairJobDesc whose ranges/session IDs match {@code session}.
     */
    private static RepairJobDesc descFor(RepairSession session)
    {
        IPartitioner p = Murmur3Partitioner.instance;
        Range<Token> range = new Range<>(p.getToken(ByteBufferUtil.bytes(0)),
                                         p.getToken(ByteBufferUtil.bytes(100)));
        return new RepairJobDesc(session.state.parentRepairSession, session.getId(),
                                 KEYSPACE, CF, Collections.singletonList(range));
    }

    /**
     * Helper: register a stub {@link SymmetricRemoteSyncTask} so that
     * {@code session.syncComplete()} does not exit early on the null-task guard.
     */
    private static SymmetricRemoteSyncTask registerStubSyncTask(RepairSession session,
                                                                 RepairJobDesc desc,
                                                                 InetAddressAndPort coordinator,
                                                                 InetAddressAndPort peer)
    {
        SymmetricRemoteSyncTask task = new SymmetricRemoteSyncTask(SharedContext.Global.instance,
                                                                    desc, coordinator, peer,
                                                                    new ArrayList<>(), PreviewKind.NONE);
        session.trackSyncCompletion(Pair.create(desc, task.nodePair()), task);
        return task;
    }

    /**
     * When {@code success=false}, syncComplete() must log at INFO and the message must contain
     * {@code parentSession=}, the coordinator address, the peer address, and the column family.
     */
    @Test
    public void testSyncCompleteFailureLogsInfoWithParentSession() throws Exception
    {
        InetAddressAndPort coordinator = InetAddressAndPort.getByName("10.0.1.1");
        InetAddressAndPort peer       = InetAddressAndPort.getByName("10.0.1.2");
        InetAddressAndPort from       = InetAddressAndPort.getByName("10.0.1.3");

        TimeUUID parentSessionId = nextTimeUUID();
        RepairSession session = buildSession(parentSessionId,
                                             Sets.newHashSet(coordinator, peer),
                                             optionsWithTenant(ENTITY_ID));

        RepairJobDesc desc = descFor(session);
        registerStubSyncTask(session, desc, coordinator, peer);

        Message<SyncResponse> msg = Message.builder(
                Verb.SYNC_RSP,
                new SyncResponse(desc, new SyncNodePair(coordinator, peer), false, Collections.emptyList()))
                                           .from(from).build();

        session.syncComplete(desc, msg);

        List<String> infos = capturedInfoMessages();
        assertFalse("syncComplete(failure) must emit an INFO log", infos.isEmpty());
        String log = infos.stream()
                          .filter(m -> m.contains("sync FAILED"))
                          .findFirst()
                          .orElse("");
        assertFalse("INFO log must contain 'sync FAILED'", log.isEmpty());
        assertTrue("log must contain parentSession=" + parentSessionId,
                   log.contains("parentSession=" + parentSessionId));
        assertTrue("log must contain coordinator address",  log.contains(coordinator.toString()));
        assertTrue("log must contain peer address",         log.contains(peer.toString()));
        assertTrue("log must contain column family",        log.contains(CF));
        assertTrue("log must contain [entityId: " + ENTITY_ID + "]",
                   log.contains("[entityId: " + ENTITY_ID));
    }

    /**
     * When {@code success=true}, syncComplete() must NOT emit an INFO log —
     * the success path is gated behind {@code logger.isDebugEnabled()}.
     */
    @Test
    public void testSyncCompleteSuccessNoInfoLog() throws Exception
    {
        InetAddressAndPort coordinator = InetAddressAndPort.getByName("10.0.2.1");
        InetAddressAndPort peer       = InetAddressAndPort.getByName("10.0.2.2");
        InetAddressAndPort from       = InetAddressAndPort.getByName("10.0.2.3");

        TimeUUID parentSessionId = nextTimeUUID();
        RepairSession session = buildSession(parentSessionId,
                                             Sets.newHashSet(coordinator, peer),
                                             optionsWithoutTenant());

        RepairJobDesc desc = descFor(session);
        registerStubSyncTask(session, desc, coordinator, peer);

        Message<SyncResponse> msg = Message.builder(
                Verb.SYNC_RSP,
                new SyncResponse(desc, new SyncNodePair(coordinator, peer), true, Collections.emptyList()))
                                           .from(from).build();

        session.syncComplete(desc, msg);

        assertTrue("syncComplete(success) must not produce an INFO 'sync FAILED' log",
                   capturedInfoMessages().stream().noneMatch(m -> m.contains("sync FAILED")));
    }

    /**
     * A do-nothing executor used in tests to exercise job-submission paths without actually
     * running any tasks or managing any threads.  All lifecycle and scheduling methods are
     * intentionally inert.
     */
    private static class NoopExecutorService implements ExecutorPlus
    {
        @Override public void shutdown() {
            // Intentionally does not run the command. Tests using this executor only exercise
            // log lines emitted before job submission in RepairSession.start(); submitted jobs
            // are never expected to complete.
        }
        @Override public List<Runnable> shutdownNow() { return null; }
        @Override public boolean isShutdown() { return false; }
        @Override public boolean isTerminated() { return false; }
        @Override public boolean awaitTermination(long timeout, TimeUnit unit) { return false; }
        @Override public <T> org.apache.cassandra.utils.concurrent.Future<T> submit(Callable<T> task) { return null; }
        @Override public <T> org.apache.cassandra.utils.concurrent.Future<T> submit(Runnable task, T result) { return null; }
        @Override public org.apache.cassandra.utils.concurrent.Future<?> submit(Runnable task) { return null; }
        @Override public void execute(WithResources withResources, Runnable task){
            // Intentionally does not run the command. Tests using this executor only exercise
            // log lines emitted before job submission in RepairSession.start(); submitted jobs
            // are never expected to complete.
        }
        @Override public <T> org.apache.cassandra.utils.concurrent.Future<T> submit(WithResources withResources, Callable<T> task) { return null; }
        @Override public org.apache.cassandra.utils.concurrent.Future<?> submit(WithResources withResources, Runnable task) { return null; }
        @Override public <T> org.apache.cassandra.utils.concurrent.Future<T> submit(WithResources withResources, Runnable task, T result) { return null; }
        @Override public boolean inExecutor() { return false; }
        @Override public void execute(Runnable command) {
            // Intentionally does not run the command. Tests using this executor only exercise
            // log lines emitted before job submission in RepairSession.start(); submitted jobs
            // are never expected to complete.
        }
        @Override public int getCorePoolSize() { return 0; }
        @Override public void setCorePoolSize(int newCorePoolSize) {
            // Intentionally does not run the command. Tests using this executor only exercise
            // log lines emitted before job submission in RepairSession.start(); submitted jobs
            // are never expected to complete.
        }
        @Override public int getMaximumPoolSize() { return 0; }
        @Override public void setMaximumPoolSize(int newMaximumPoolSize) {
            // Intentionally does not run the command. Tests using this executor only exercise
            // log lines emitted before job submission in RepairSession.start(); submitted jobs
            // are never expected to complete.
        }
        @Override public int getActiveTaskCount() { return 0; }
        @Override public long getCompletedTaskCount() { return 0; }
        @Override public int getPendingTaskCount() { return 0; }
    }
}
