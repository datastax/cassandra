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

package org.apache.cassandra.distributed.test;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;

import com.google.common.util.concurrent.Uninterruptibles;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.repair.SystemDistributedKeyspace;
import org.apache.cassandra.service.ActiveRepairService;

import static java.lang.String.format;
import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.junit.Assert.assertTrue;

/**
 * Regression test for HCD-587 VALIDATION_REQ timeout on local node due to busy AntiEntropyStage: fails (repair hangs)
 * if incremental repair session startup runs on AntiEntropyStage.
 */
public class IncrementalRepairSessionStartTimeoutTest extends TestBaseImpl
{
    private static final String SLOW_KS_PREFIX = "slow_";
    private static final int SLOW_WRITE_MS = 300;
    private static final int TABLES = 10;
    private static final long REPAIR_MSG_TIMEOUT_MS = 2000;
    // with the fix a repair completes in ~10s; without it the repair hangs
    private static final int REPAIR_AWAIT_SECONDS = 60;

    private static final Pattern SESSION_START = Pattern.compile("Starting RepairSession for");
    // in-JVM thread names are prefixed with the instance name, e.g. [node1_AntiEntropyStage:1]
    private static final Pattern SESSION_START_ON_ANTI_ENTROPY = Pattern.compile("\\[(node\\d+_)?AntiEntropyStage:\\d+\\].*Starting RepairSession for");
    private static final Pattern VALIDATION_REQ_TIMEOUT = Pattern.compile("VALIDATION_REQ failed on .*: TIMEOUT");
    private static final Pattern SELF_TIMEOUT_IGNORED = Pattern.compile("not supporting repair message timeouts \\(version = null\\)");
    private static final Pattern VALIDATION_REQ_DROPPED = Pattern.compile("VALIDATION_REQ messages were dropped");

    private static Cluster CLUSTER;

    @BeforeClass
    public static void setupCluster() throws IOException
    {
        // 4 nodes with RF 3 gives node1 three local ranges with distinct replica sets, i.e. three repair sessions
        CLUSTER = init(Cluster.build(4)
                              .withConfig(c -> c.with(NETWORK, GOSSIP)
                                                .set("repair_request_timeout_in_ms", REPAIR_MSG_TIMEOUT_MS)
                                                .set("repair_prepare_message_timeout_in_ms", REPAIR_MSG_TIMEOUT_MS))
                              .withInstanceInitializer(SlowRepairHistoryWrites::install)
                              .start());
    }

    @AfterClass
    public static void teardownCluster()
    {
        FileUtils.closeQuietly(CLUSTER);
    }

    /**
     * Incremental repair session startup, including the slow repair_history writes, must not run on AntiEntropyStage.
     * Otherwise the coordinator's own VALIDATION_REQs for the first sessions queue behind the remaining sessions'
     * startup, expire and are dropped. The ack timeout to self is ignored (no peers row, version = null), so the
     * repair hangs.
     */
    @Test
    public void testIncrementalRepairSessionStartDoesNotBlockAntiEntropyStage() throws Exception
    {
        String ks = createKeyspace(SLOW_KS_PREFIX + "ir", TABLES);
        IInvokableInstance coordinator = CLUSTER.get(1);
        long mark = coordinator.logs().mark();

        CompletableFuture<NodeToolResult> repair = CompletableFuture.supplyAsync(() -> coordinator.nodetoolResult("repair", ks));
        awaitRepair(coordinator, mark, repair).asserts().success();

        List<String> starts = coordinator.logs().grep(mark, SESSION_START).getResult();
        assertTrue("Expected more than one repair session, got " + starts, starts.size() > 1);
        assertNoMatch(coordinator, mark, SESSION_START_ON_ANTI_ENTROPY);
        assertNoMatch(coordinator, mark, VALIDATION_REQ_TIMEOUT);
        assertNoMatch(coordinator, mark, VALIDATION_REQ_DROPPED);
    }

    /**
     * While node1 starts the sessions of its incremental repair, an unrelated incremental repair coordinated by node2
     * needs node1's AntiEntropyStage (PREPARE_MSG, then VALIDATION_REQ). It must not be blocked by node1's startup.
     */
    @Test
    public void testOtherRepairNotBlockedWhileCoordinatorStartsSessions() throws Exception
    {
        String slowKs = createKeyspace(SLOW_KS_PREFIX + "busy", TABLES);
        String otherKs = createKeyspace("other_ks", 1);
        IInvokableInstance busy = CLUSTER.get(1);
        long mark = busy.logs().mark();

        CompletableFuture<NodeToolResult> slowRepair = CompletableFuture.supplyAsync(() -> busy.nodetoolResult("repair", slowKs));
        busy.logs().watchFor(mark, Duration.ofMinutes(1), SESSION_START);

        NodeToolResult other = CLUSTER.get(2).nodetoolResult("repair", otherKs);
        NodeToolResult slow = awaitRepair(busy, mark, slowRepair);
        other.asserts().success();
        slow.asserts().success();
    }

    /**
     * Waits for a repair that should finish in seconds. If it hangs, terminates the coordinator's sessions so the
     * cluster can be reused, and fails with what the coordinator logged about its own validation requests.
     */
    private static NodeToolResult awaitRepair(IInvokableInstance coordinator, long mark, CompletableFuture<NodeToolResult> repair) throws Exception
    {
        try
        {
            return repair.get(REPAIR_AWAIT_SECONDS, TimeUnit.SECONDS);
        }
        catch (TimeoutException e)
        {
            coordinator.runOnInstance(() -> ActiveRepairService.instance.terminateSessions());
            repair.get(1, TimeUnit.MINUTES);
            throw new AssertionError(format("Incremental repair did not complete within %ds. Sessions started on AntiEntropyStage: %d, " +
                                            "VALIDATION_REQ timeouts: %d (ignored for self: %d), VALIDATION_REQ drops: %d",
                                            REPAIR_AWAIT_SECONDS,
                                            count(coordinator, mark, SESSION_START_ON_ANTI_ENTROPY),
                                            count(coordinator, mark, VALIDATION_REQ_TIMEOUT),
                                            count(coordinator, mark, SELF_TIMEOUT_IGNORED),
                                            count(coordinator, mark, VALIDATION_REQ_DROPPED)));
        }
    }

    private static int count(IInvokableInstance instance, long mark, Pattern pattern)
    {
        return instance.logs().grep(mark, pattern).getResult().size();
    }

    private static void assertNoMatch(IInvokableInstance instance, long mark, Pattern pattern)
    {
        List<String> matches = instance.logs().grep(mark, pattern).getResult();
        assertTrue("Unexpected log lines matching " + pattern + ": " + matches, matches.isEmpty());
    }

    private static String createKeyspace(String ks, int tables)
    {
        CLUSTER.schemaChange(format("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}", ks));
        for (int t = 0; t < tables; t++)
        {
            CLUSTER.schemaChange(format("CREATE TABLE %s.tbl%d (k int PRIMARY KEY, v int)", ks, t));
            for (int k = 0; k < 10; k++)
                CLUSTER.coordinator(1).execute(format("INSERT INTO %s.tbl%d (k, v) VALUES (?, ?)", ks, t), ConsistencyLevel.ALL, k, k);
        }
        CLUSTER.forEach(i -> i.flush(ks));
        return ks;
    }

    public static class SlowRepairHistoryWrites
    {
        public static void install(ClassLoader cl, int node)
        {
            new ByteBuddy().rebase(SystemDistributedKeyspace.class)
                           .method(named("processSilent"))
                           .intercept(MethodDelegation.to(SlowRepairHistoryWrites.class))
                           .make()
                           .load(cl, ClassLoadingStrategy.Default.INJECTION);
        }

        // slow down updating distributed system keyspace
        public static void processSilent(String fmtQry, String[] values, @SuperCall Callable<?> superCalllable) throws Exception
        {
            // only the per-table, per-range STARTED rows written by RepairSession.start for the slow keyspaces
            if (fmtQry.contains("'" + SLOW_KS_PREFIX) && fmtQry.contains("'STARTED'"))
                Uninterruptibles.sleepUninterruptibly(SLOW_WRITE_MS, TimeUnit.MILLISECONDS);
            superCalllable.call();
        }
    }
}
