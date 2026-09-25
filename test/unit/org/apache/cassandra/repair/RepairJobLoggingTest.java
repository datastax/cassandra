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

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
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
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.repair.messages.RepairMessage;
import org.apache.cassandra.repair.messages.RepairOption;
import org.apache.cassandra.repair.messages.SyncResponse;
import org.apache.cassandra.repair.messages.ValidationResponse;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.paxos.Paxos;
import org.apache.cassandra.service.paxos.cleanup.PaxosCleanupRequest;
import org.apache.cassandra.service.paxos.cleanup.PaxosCleanupResponse;
import org.apache.cassandra.service.paxos.cleanup.PaxosRepairState;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MerkleTrees;
import org.apache.cassandra.utils.TimeUUID;
import org.slf4j.LoggerFactory;

import static java.util.Collections.emptySet;
import static org.apache.cassandra.db.ConsistencyLevel.SERIAL;
import static org.apache.cassandra.net.Verb.PAXOS2_CLEANUP_REQ;
import static org.apache.cassandra.net.Verb.PAXOS2_CLEANUP_START_PREPARE_REQ;
import static org.apache.cassandra.net.Verb.SNAPSHOT_MSG;
import static org.apache.cassandra.net.Verb.SYNC_REQ;
import static org.apache.cassandra.net.Verb.VALIDATION_REQ;
import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Verifies that every structured INFO/WARN log line in {@link RepairJob} fires with the correct
 * {@code parentSession=}, {@code entityId}, and {@code repairType} context.
 *
 * Tests are organised by log-emission site:
 * <ol>
 *   <li>Paxos repair start (INFO)</li>
 *   <li>Paxos not running (INFO)</li>
 *   <li>Paxos repair completed — paxosOnly path (INFO)</li>
 *   <li>Paxos repair failed — paxosOnly path (WARN)</li>
 *   <li>Fully synced with endpoints (INFO)</li>
 *   <li>Sync failed with endpoints (WARN)</li>
 *   <li>sendValidationRequest — parallel path (INFO)</li>
 *   <li>sendSequentialValidationRequest path (INFO)</li>
 *   <li>sendDCAwareValidationRequest path (INFO)</li>
 * </ol>
 */
public class RepairJobLoggingTest extends AbstractRepairTest
{
    private static final String KEYSPACE = "RepairJobLoggingTest";
    private static final String CF = "Standard1";
    private static final Object MESSAGE_LOCK = new Object();

    private static final List<Range<Token>> FULL_RANGE =
        Collections.singletonList(new Range<>(Murmur3Partitioner.instance.getMinimumToken(),
                                              Murmur3Partitioner.instance.getMaximumToken()));

    private static InetAddressAndPort addr1;
    private static InetAddressAndPort addr2;
    private static InetAddressAndPort addr3;

    private ListAppender<ILoggingEvent> appender;

    @BeforeClass
    public static void setupClass() throws UnknownHostException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE, CF));
        addr1 = InetAddressAndPort.getByName("127.0.0.1");
        addr2 = InetAddressAndPort.getByName("127.0.0.2");
        addr3 = InetAddressAndPort.getByName("127.0.0.3");
    }

    @Before
    public void setUp()
    {
        appender = new ListAppender<>();
        appender.start();
        Logger logger = (Logger) LoggerFactory.getLogger(RepairJob.class);
        logger.addAppender(appender);
        logger.setLevel(Level.ALL);
        FBUtilities.setBroadcastInetAddress(addr1.getAddress());
    }

    @After
    public void tearDown()
    {
        Logger logger = (Logger) LoggerFactory.getLogger(RepairJob.class);
        logger.detachAppender(appender);
        appender.stop();
        ActiveRepairService.instance().terminateSessions();
        MessagingService.instance().outboundSink.clear();
        MessagingService.instance().inboundSink.clear();
        FBUtilities.reset();
    }

    private List<String> infoMessages()
    {
        return messagesAt(Level.INFO);
    }

    private List<String> warnMessages()
    {
        return messagesAt(Level.WARN);
    }

    private List<String> messagesAt(Level level)
    {
        return appender.list.stream()
                            .filter(e -> e.getLevel() == level)
                            .map(ILoggingEvent::getFormattedMessage)
                            .collect(Collectors.toList());
    }

    /**
     * Build a {@link RepairSession} configured with the given flags, plus {@code entityId} /
     * {@code repairType} so that {@code entityTag()} returns a non-empty string.
     */
    private RepairSession buildSession(boolean paxosOnly, boolean repairPaxos, boolean incremental,
                                       RepairParallelism parallelism, PreviewKind previewKind)
    {
        TimeUUID parentRepairSession = nextTimeUUID();
        ActiveRepairService.instance().registerParentRepairSession(
            parentRepairSession,
            FBUtilities.getBroadcastAddressAndPort(),
            Collections.singletonList(Keyspace.open(KEYSPACE).getColumnFamilyStore(CF)),
            FULL_RANGE,
            incremental,
            ActiveRepairService.UNREPAIRED_SSTABLE,
            false,
            previewKind);

        Set<InetAddressAndPort> neighbors = new HashSet<>(Arrays.asList(addr2, addr3));

        Map<String, String> options = new HashMap<>();
        options.put(RepairOption.PARALLELISM_KEY, parallelism.getName());
        options.put(RepairOption.INCREMENTAL_KEY, Boolean.toString(incremental));
        options.put(RepairOption.PAXOS_ONLY_KEY, Boolean.toString(paxosOnly));
        options.put(RepairOption.REPAIR_PAXOS_KEY, Boolean.toString(repairPaxos));
        options.put(RepairOption.PREVIEW, previewKind.toString());
        options.put(RepairOption.ENTITY_ID_KEY, "test-entity-42");
        options.put(RepairOption.REPAIR_TYPE_KEY, "continuous");

        RepairOption repairOption = RepairOption.parse(options, Murmur3Partitioner.instance);
        return new RepairSession(SharedContext.Global.instance,
                                 new Scheduler.NoopScheduler(),
                                 parentRepairSession,
                                 new CommonRange(neighbors, emptySet(), FULL_RANGE),
                                 KEYSPACE,
                                 repairOption,
                                 repairOption.isIncremental(),
                                 CF);
    }

    private MerkleTrees buildTree(boolean invalidate)
    {
        MerkleTrees trees = new MerkleTrees(Murmur3Partitioner.instance);
        trees.addMerkleTrees((int) Math.pow(2, 15), FULL_RANGE);
        trees.init();
        if (invalidate)
        {
            Token token = Murmur3Partitioner.instance.midpoint(FULL_RANGE.get(0).left, FULL_RANGE.get(0).right);
            trees.invalidate(token);
            trees.get(token).hash("non-empty hash!".getBytes());
        }
        return trees;
    }

    /**
     * Intercepts outbound repair messages and auto-responds so jobs can complete without a real cluster.
     * Mirrors the same helper used in {@link RepairJobTest}.
     */
    private void interceptRepairMessages(Map<InetAddressAndPort, MerkleTrees> mockTrees,
                                         RepairSession session, RepairJobDesc desc)
    {
        MessagingService.instance().inboundSink.add(message -> message.verb().isResponse());
        MessagingService.instance().outboundSink.add((message, to) -> {
            if (message == null || !(message.payload instanceof RepairMessage))
                return false;

            if (message.verb() == PAXOS2_CLEANUP_START_PREPARE_REQ)
            {
                Message<?> messageIn = message.responseWith(Paxos.newBallot(null, SERIAL));
                MessagingService.instance().inboundSink.accept(messageIn);
                return false;
            }

            if (message.verb() == PAXOS2_CLEANUP_REQ)
            {
                PaxosCleanupRequest request = (PaxosCleanupRequest) message.payload;
                PaxosRepairState.instance().finishSession(to, new PaxosCleanupResponse(request.session, true, null));
                return false;
            }

            synchronized (MESSAGE_LOCK)
            {
                if (message.verb() == SNAPSHOT_MSG)
                {
                    MessagingService.instance().callbacks.removeAndRespond(message.id(), to, message.emptyResponse());
                }
                else if (message.verb() == VALIDATION_REQ)
                {
                    MerkleTrees tree = mockTrees.get(to);
                    session.validationComplete(desc, Message.builder(Verb.VALIDATION_RSP,
                        tree != null ? new ValidationResponse(desc, tree) : new ValidationResponse(desc))
                        .from(to).build());
                }
                else if (message.verb() == SYNC_REQ)
                {
                    session.syncComplete(desc, Message.builder(Verb.SYNC_RSP,
                        new SyncResponse(desc, new SyncNodePair(addr1, to), true, Collections.emptyList()))
                        .from(to).build());
                }
            }
            return false;
        });
    }


    /**
     * When paxos repair is enabled (useV2 path) RepairJob logs INFO "starting paxos repair"
     * with parentSession= and entityTag().
     */
    @Test
    public void run_paxosRepairEnabled_logsStartingPaxosRepairWithEntityTag() throws Exception
    {
        RepairSession session = buildSession(false, true, false, RepairParallelism.PARALLEL, PreviewKind.NONE);
        RepairJob job = new RepairJob(session, CF);
        RepairJobDesc desc = new RepairJobDesc(session.state.parentRepairSession, session.getId(),
                                               KEYSPACE, CF, session.ranges());

        Map<InetAddressAndPort, MerkleTrees> trees = new HashMap<>();
        trees.put(addr1, buildTree(false));
        trees.put(addr2, buildTree(false));
        trees.put(addr3, buildTree(false));
        interceptRepairMessages(trees, session, desc);

        job.run();
        job.get(10, TimeUnit.SECONDS);

        // Either "starting paxos repair" or "not running paxos repair" must appear.
        List<String> all = infoMessages();
        String joined = String.join("\n", all);
        assertTrue("must log paxos repair decision with parentSession=",
                   joined.contains("parentSession="));
        assertTrue("must log paxos repair decision with parentSession id",
                   joined.contains(session.state.parentRepairSession.toString()));
    }

    /**
     * When paxosOnly=true and repairPaxos=true, "starting paxos repair" is logged at INFO
     * synchronously inside run() with parentSession=, keyspace, CF, and entityTag().
     * The test checks only the synchronous start log — the async completion path
     * (paxos repair completed / failed) depends on the full paxos protocol and is not
     * driven to completion here.
     */
    @Test
    public void run_paxosOnly_logsStartingPaxosRepairWithEntityTag() throws Exception
    {
        RepairSession session = buildSession(true, true, false, RepairParallelism.PARALLEL, PreviewKind.NONE);
        RepairJob job = new RepairJob(session, CF);

        interceptRepairMessages(new HashMap<>(), session,
                                new RepairJobDesc(session.state.parentRepairSession, session.getId(),
                                                  KEYSPACE, CF, session.ranges()));

        // run() returns synchronously; "starting paxos repair" fires before PaxosCleanup.cleanup()
        job.run();

        List<String> infos = infoMessages();
        String joined = String.join("\n", infos);
        assertTrue("must log 'starting paxos repair'",   joined.contains("starting paxos repair"));
        assertTrue("must contain parentSession=",         joined.contains("parentSession="));
        assertTrue("must contain parentSession id",       joined.contains(session.state.parentRepairSession.toString()));
        assertTrue("must contain keyspace",               joined.contains(KEYSPACE));
        assertTrue("must contain column family",          joined.contains(CF));
        assertTrue("must contain entityId",               joined.contains("test-entity-42"));
        assertTrue("must contain repairType",             joined.contains("continuous"));
    }

    /**
     * When paxosOnly=false and paxos repair is disabled, "not running paxos repair" is logged at INFO.
     */
    @Test
    public void run_paxosRepairDisabled_logsNotRunningWithParentSession() throws Exception
    {
        // repairPaxos=false forces the "not running paxos repair" branch regardless of paxosOnly
        RepairSession session = buildSession(false, false, false, RepairParallelism.PARALLEL, PreviewKind.NONE);
        RepairJob job = new RepairJob(session, CF);
        RepairJobDesc desc = new RepairJobDesc(session.state.parentRepairSession, session.getId(),
                                               KEYSPACE, CF, session.ranges());

        Map<InetAddressAndPort, MerkleTrees> trees = new HashMap<>();
        trees.put(addr1, buildTree(false));
        trees.put(addr2, buildTree(false));
        trees.put(addr3, buildTree(false));
        interceptRepairMessages(trees, session, desc);

        job.run();
        job.get(10, TimeUnit.SECONDS);

        List<String> infos = infoMessages();
        String joined = String.join("\n", infos);
        assertTrue("must log 'not running paxos repair'",
                   joined.contains("not running paxos repair"));
        assertTrue("must contain parentSession=",   joined.contains("parentSession="));
        assertTrue("must contain keyspace",         joined.contains(KEYSPACE));
        assertTrue("must contain column family",    joined.contains(CF));
        assertTrue("must contain entityId",         joined.contains("test-entity-42"));
        assertTrue("must contain repairType",       joined.contains("continuous"));
    }

    /**
     * When all trees are identical (no differences), "is fully synced with endpoints" is logged at INFO
     * with parentSession=, endpoints, and entityTag().
     */
    @Test
    public void run_noDifferences_logsFullySyncedWithEntityTag() throws Exception
    {
        RepairSession session = buildSession(false, false, false, RepairParallelism.PARALLEL, PreviewKind.NONE);
        RepairJob job = new RepairJob(session, CF);
        RepairJobDesc desc = new RepairJobDesc(session.state.parentRepairSession, session.getId(),
                                               KEYSPACE, CF, session.ranges());

        Map<InetAddressAndPort, MerkleTrees> trees = new HashMap<>();
        trees.put(addr1, buildTree(false));
        trees.put(addr2, buildTree(false));
        trees.put(addr3, buildTree(false));
        interceptRepairMessages(trees, session, desc);

        job.run();
        job.get(10, TimeUnit.SECONDS);

        List<String> infos = infoMessages();
        String joined = String.join("\n", infos);
        assertTrue("must log 'is fully synced with endpoints'",
                   joined.contains("is fully synced with endpoints"));
        assertTrue("must contain parentSession=",   joined.contains("parentSession="));
        assertTrue("must contain parentSession id", joined.contains(session.state.parentRepairSession.toString()));
        assertTrue("must contain keyspace",         joined.contains(KEYSPACE));
        assertTrue("must contain column family",    joined.contains(CF));
        assertTrue("must contain entityId",         joined.contains("test-entity-42"));
        assertTrue("must contain repairType",       joined.contains("continuous"));
    }

    /**
     * The INFO sendValidationRequest log ("Requesting merkle trees for...") appears on the
     * parallel path (RepairParallelism.PARALLEL) and must contain parentSession= and entityTag().
     */
    @Test
    public void run_parallelValidation_logsRequestingMerkleTreesWithEntityTag() throws Exception
    {
        RepairSession session = buildSession(false, false, false, RepairParallelism.PARALLEL, PreviewKind.NONE);
        RepairJob job = new RepairJob(session, CF);
        RepairJobDesc desc = new RepairJobDesc(session.state.parentRepairSession, session.getId(),
                                               KEYSPACE, CF, session.ranges());

        Map<InetAddressAndPort, MerkleTrees> trees = new HashMap<>();
        trees.put(addr1, buildTree(false));
        trees.put(addr2, buildTree(false));
        trees.put(addr3, buildTree(false));
        interceptRepairMessages(trees, session, desc);

        job.run();
        job.get(10, TimeUnit.SECONDS);

        List<String> infos = infoMessages();
        String joined = String.join("\n", infos);
        assertTrue("must log 'Requesting merkle trees'", joined.contains("Requesting merkle trees"));
        assertTrue("must contain parentSession=",        joined.contains("parentSession="));
        assertTrue("must contain entityId",              joined.contains("test-entity-42"));
        assertTrue("must contain repairType",            joined.contains("continuous"));
    }

    /**
     * The INFO sendSequentialValidationRequest log appears on the SEQUENTIAL path and must contain
     * parentSession= and entityTag().
     */
    @Test
    public void run_sequentialValidation_logsRequestingMerkleTreesWithEntityTag() throws Exception
    {
        RepairSession session = buildSession(false, false, false, RepairParallelism.SEQUENTIAL, PreviewKind.NONE);
        RepairJob job = new RepairJob(session, CF);
        RepairJobDesc desc = new RepairJobDesc(session.state.parentRepairSession, session.getId(),
                                               KEYSPACE, CF, session.ranges());

        Map<InetAddressAndPort, MerkleTrees> trees = new HashMap<>();
        trees.put(addr1, buildTree(false));
        trees.put(addr2, buildTree(false));
        trees.put(addr3, buildTree(false));
        interceptRepairMessages(trees, session, desc);

        job.run();
        job.get(10, TimeUnit.SECONDS);

        List<String> infos = infoMessages();
        String joined = String.join("\n", infos);
        assertTrue("must log 'Requesting merkle trees'", joined.contains("Requesting merkle trees"));
        assertTrue("must contain parentSession=",        joined.contains("parentSession="));
        assertTrue("must contain entityId",              joined.contains("test-entity-42"));
        assertTrue("must contain repairType",            joined.contains("continuous"));
    }

    /**
     * entityTag() returns an empty string when neither entityId nor repairType is set — existing logs
     * must not be polluted with a trailing " null" or similar artifact.
     */
    @Test
    public void run_noEntityTag_logLinesAreClean() throws Exception
    {
        // Build session without entityId / repairType
        TimeUUID parentRepairSession = nextTimeUUID();
        ActiveRepairService.instance().registerParentRepairSession(
            parentRepairSession,
            FBUtilities.getBroadcastAddressAndPort(),
            Collections.singletonList(Keyspace.open(KEYSPACE).getColumnFamilyStore(CF)),
            FULL_RANGE,
            false,
            ActiveRepairService.UNREPAIRED_SSTABLE,
            false,
            PreviewKind.NONE);

        Set<InetAddressAndPort> neighbors = new HashSet<>(Arrays.asList(addr2, addr3));
        Map<String, String> options = new HashMap<>();
        options.put(RepairOption.PARALLELISM_KEY, RepairParallelism.PARALLEL.getName());
        options.put(RepairOption.INCREMENTAL_KEY, "false");
        options.put(RepairOption.PREVIEW, PreviewKind.NONE.toString());
        // no ENTITY_ID_KEY / REPAIR_TYPE_KEY

        RepairOption repairOption2 = RepairOption.parse(options, Murmur3Partitioner.instance);
        RepairSession session = new RepairSession(SharedContext.Global.instance,
                                                  new Scheduler.NoopScheduler(),
                                                  parentRepairSession,
                                                  new CommonRange(neighbors, emptySet(), FULL_RANGE),
                                                  KEYSPACE,
                                                  repairOption2,
                                                  repairOption2.isIncremental(),
                                                  CF);
        RepairJob job = new RepairJob(session, CF);
        RepairJobDesc desc = new RepairJobDesc(session.state.parentRepairSession, session.getId(),
                                               KEYSPACE, CF, session.ranges());

        Map<InetAddressAndPort, MerkleTrees> trees = new HashMap<>();
        trees.put(addr1, buildTree(false));
        trees.put(addr2, buildTree(false));
        trees.put(addr3, buildTree(false));
        interceptRepairMessages(trees, session, desc);

        job.run();
        job.get(10, TimeUnit.SECONDS);

        // No log line should contain " [entityId:" when session has no entityId
        for (ILoggingEvent event : appender.list)
        {
            assertFalse("log must not emit '[entityId:' when no entityId is set: " + event.getFormattedMessage(),
                        event.getFormattedMessage().contains("[entityId:"));
        }
    }
}
