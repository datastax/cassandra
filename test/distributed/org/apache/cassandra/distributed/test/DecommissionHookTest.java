/*
 * Copyright IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.distributed.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.SystemKeyspace.BootstrapState;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.service.DecommissionHook;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.service.StorageService.Mode.DECOMMISSIONED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * {@link DecommissionHook}: hooks run on a decommissioning node once it has left the ring, while
 * it can still query the cluster.
 *
 * 3 nodes at RF=2, decommissioning node3, so the two survivors can still serve the hook's QUORUM
 * read. The decommission is forceful because system_distributed is RF=3 on a 3-node cluster, which
 * trips the RF-vs-live-nodes check whatever the test keyspace does -- the same reason
 * {@link DecommissionTest} forces.
 *
 * Note the hooks below are static nested classes, not lambdas or anonymous classes: an anonymous
 * class declared in a test method captures the test instance, which makes the enclosing
 * runOnInstance closure unserializable.
 */
public class DecommissionHookTest extends TestBaseImpl
{
    /**
     * What the hooks observed. Lives in the node's classloader: the hooks write it during the
     * decommission, and the assertions read it back via a later runOnInstance on the same node.
     */
    public static class Observed
    {
        static final List<String> order = Collections.synchronizedList(new ArrayList<>());
        /** Names of the hooks that were entered with the interrupt flag already set. Must stay empty. */
        static final List<String> enteredInterrupted = Collections.synchronizedList(new ArrayList<>());
        static volatile boolean stillRingMember = true;
        static volatile boolean nativeTransportRunning = false;
        static volatile int rowsReadByHook = -1;
        static volatile String queryError = null;
    }

    /** Records that it ran, under its own name, and how it found the interrupt flag on entry. */
    public static class RecordingHook implements DecommissionHook
    {
        private final String name;

        RecordingHook(String name)
        {
            this.name = name;
        }

        public String name()
        {
            return name;
        }

        public void onDecommission()
        {
            // A hook is entitled to a clear flag: it may block, and a leaked interrupt would fail
            // its very first blocking call. Checked without consuming, so a leak stays visible.
            if (Thread.currentThread().isInterrupted())
                Observed.enteredInterrupted.add(name);
            Observed.order.add(name);
        }
    }

    /** Records what the node looks like from inside a hook, and tries to query the cluster. */
    public static class ProbingHook implements DecommissionHook
    {
        public String name()
        {
            return "probe";
        }

        public void onDecommission()
        {
            Observed.order.add("probe");
            // unbootstrap() has run, so leaveRing() has removed us from the ring: no coordinator
            // routes mutations here any more. This also transitively proves the batch log finished
            // its final replay -- batchlogReplay.get() is sequenced before leaveRing() inside
            // unbootstrap().
            Observed.stillRingMember = StorageService.instance.getTokenMetadata()
                                                              .isMember(FBUtilities.getBroadcastAddressAndPort());
            // ...but nothing is shut down yet, so we can still coordinate queries.
            Observed.nativeTransportRunning = StorageService.instance.isNativeTransportRunning();
            try
            {
                UntypedResultSet rs = QueryProcessor.execute("SELECT * FROM " + KEYSPACE + ".tbl",
                                                             ConsistencyLevel.QUORUM);
                Observed.rowsReadByHook = rs.size();
            }
            catch (Throwable t)
            {
                Observed.queryError = t.toString();
            }
        }
    }

    /**
     * Returns normally but leaves the thread interrupted -- the standard
     * {@code catch (InterruptedException e) { Thread.currentThread().interrupt(); return; }} idiom
     * minus the rethrow. runDecommissionHooks() has to consume that flag, or the shutdown steps
     * after the hooks fail on it and strand the node.
     */
    public static class InterruptRestoringHook implements DecommissionHook
    {
        public String name()
        {
            return "interrupt-restoring";
        }

        public void onDecommission()
        {
            Observed.order.add("interrupt-restoring");
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Restores the interrupt flag and then throws something else -- the other way a hook can leave
     * with the flag set, e.g. {@code catch (InterruptedException e) { Thread.currentThread()
     * .interrupt(); throw new RuntimeException(e); }}. The flag has to be consumed on this path too,
     * or it leaks into the next hook.
     */
    public static class InterruptRestoringExplodingHook implements DecommissionHook
    {
        public String name()
        {
            return "interrupt-restoring-exploding";
        }

        public void onDecommission()
        {
            Observed.order.add("interrupt-restoring-exploding");
            Thread.currentThread().interrupt();
            throw new RuntimeException("simulated hook failure with the interrupt flag restored");
        }
    }

    /** Fails, to show the hooks behind it still run and the decommission does not claim success. */
    public static class ExplodingHook implements DecommissionHook
    {
        public String name()
        {
            return "exploding";
        }

        public void onDecommission()
        {
            Observed.order.add("exploding");
            throw new RuntimeException("simulated hook failure");
        }
    }

    @Test
    public void testHooksRunInOrderAfterLeavingTheRingAndCanQuery() throws Throwable
    {
        // NATIVE_PROTOCOL so the "transport is still up" assertion below means something: without
        // it the transport would never have been running and the assertion would be vacuous.
        try (Cluster cluster = init(Cluster.build(3)
                                           .withConfig(config -> config.with(GOSSIP).with(NETWORK).with(NATIVE_PROTOCOL))
                                           .start(), 2))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (k int PRIMARY KEY, v int)");
            for (int i = 0; i < 10; i++)
                cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (k, v) VALUES (?, ?)",
                                               org.apache.cassandra.distributed.api.ConsistencyLevel.ALL, i, i);

            cluster.get(3).runOnInstance(() -> {
                StorageService.instance.registerDecommissionHook(new ProbingHook());
                StorageService.instance.registerDecommissionHook(new RecordingHook("second"));

                try
                {
                    StorageService.instance.decommission(true);
                }
                catch (Throwable t)
                {
                    fail("decommission should have succeeded, but failed on: " + t);
                }
            });

            cluster.get(3).runOnInstance(() -> {
                assertEquals("both hooks must run, in registration order",
                             Arrays.asList("probe", "second"), Observed.order);
                assertFalse("hooks must run after the node has left the ring, so no mutations arrive",
                            Observed.stillRingMember);
                assertTrue("the native transport must still be up so a hook can be queried through",
                           Observed.nativeTransportRunning);
                assertNull("the hook's coordinated read failed: " + Observed.queryError,
                           Observed.queryError);
                assertEquals("a hook must be able to read the cluster at QUORUM",
                             10, Observed.rowsReadByHook);
            });
        }
    }

    /**
     * A hook that hands back an interrupted thread must not derail the rest of the decommission:
     * the shutdown after the hooks waits on futures that fail instantly if the flag is still set.
     */
    @Test
    public void testHookLeavingTheThreadInterruptedStillCompletesTheDecommission() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(3)
                                           .withConfig(config -> config.with(GOSSIP).with(NETWORK))
                                           .start(), 2))
        {
            cluster.get(3).runOnInstance(() -> {
                Observed.order.clear();
                Observed.enteredInterrupted.clear();
                StorageService.instance.registerDecommissionHook(new InterruptRestoringHook());
                StorageService.instance.registerDecommissionHook(new RecordingHook("after-the-interrupt"));

                try
                {
                    StorageService.instance.decommission(true);
                }
                catch (Throwable t)
                {
                    fail("a hook returning with the interrupt flag set must not fail the decommission, got: " + t);
                }

                assertEquals("a restored interrupt flag must not skip the hooks behind it",
                             Arrays.asList("interrupt-restoring", "after-the-interrupt"), Observed.order);
                assertEquals("a restored interrupt flag must not leak into the next hook",
                             Collections.emptyList(), Observed.enteredInterrupted);
                assertEquals(DECOMMISSIONED.name(), StorageService.instance.getOperationMode());
                assertFalse("the interrupt flag must not outlive the hook run",
                            Thread.currentThread().isInterrupted());
            });
        }
    }

    /**
     * The same, for a hook that restores the flag and then throws. The interrupt must be consumed on
     * the failure path too: the next hook is entitled to a clear flag, and would otherwise fail on
     * its first blocking call for a reason that has nothing to do with it.
     */
    @Test
    public void testHookThrowingWithTheInterruptFlagSetDoesNotLeakItToTheNextHook() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(3)
                                           .withConfig(config -> config.with(GOSSIP).with(NETWORK))
                                           .start(), 2))
        {
            cluster.get(3).runOnInstance(() -> {
                Observed.order.clear();
                Observed.enteredInterrupted.clear();
                StorageService.instance.registerDecommissionHook(new InterruptRestoringExplodingHook());
                StorageService.instance.registerDecommissionHook(new RecordingHook("after-the-interrupting-explosion"));

                Throwable thrown = null;
                try
                {
                    StorageService.instance.decommission(true);
                }
                catch (Throwable t)
                {
                    thrown = t;
                }
                assertNotNull("a failing hook must be reported to the caller", thrown);
                assertTrue("the failure must name the offending hook, was: " + thrown,
                           String.valueOf(thrown.getMessage()).contains("interrupt-restoring-exploding"));

                assertEquals("a hook that throws must not skip the hooks behind it",
                             Arrays.asList("interrupt-restoring-exploding", "after-the-interrupting-explosion"),
                             Observed.order);
                assertEquals("an interrupt restored before throwing must not leak into the next hook",
                             Collections.emptyList(), Observed.enteredInterrupted);
                assertEquals(DECOMMISSIONED.name(), StorageService.instance.getOperationMode());
                assertFalse("the interrupt flag must not outlive the hook run",
                            Thread.currentThread().isInterrupted());
            });
        }
    }

    /** Registering a null hook must fail the caller, not the decommission. */
    @Test
    public void testNullHookIsRejectedAtRegistration() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(1)
                                           .withConfig(config -> config.with(GOSSIP).with(NETWORK))
                                           .start(), 1))
        {
            cluster.get(1).runOnInstance(() -> {
                try
                {
                    StorageService.instance.registerDecommissionHook(null);
                    fail("a null hook must be rejected at registration");
                }
                catch (NullPointerException expected)
                {
                    // A null reaching runDecommissionHooks() would have no safe way to fail.
                }
            });
        }
    }

    /**
     * A hook that throws must not silently skip the hooks behind it, must be reported, and must
     * still leave the node fully decommissioned.
     *
     * The node state is the subtle part. Hooks run after unbootstrap(), so the data has already
     * streamed away and leaveRing() has run. Stopping at DECOMMISSION_FAILED there would strand the
     * node for good: a retry is rejected by decommission()'s ring-membership check (leaveRing()
     * removed us from TokenMetadata), and leaveRing() persisted NEEDS_BOOTSTRAP, so a restart would
     * bootstrap the node back into the ring instead. So the decommission finishes and the failure
     * is reported afterwards.
     */
    @Test
    public void testFailingHookIsReportedButStillCompletesTheDecommission() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(3)
                                           .withConfig(config -> config.with(GOSSIP).with(NETWORK))
                                           .start(), 2))
        {
            cluster.get(3).runOnInstance(() -> {
                Observed.order.clear();
                Observed.enteredInterrupted.clear();
                StorageService.instance.registerDecommissionHook(new ExplodingHook());
                StorageService.instance.registerDecommissionHook(new RecordingHook("after-the-explosion"));

                // Capture rather than assert inside the try: a fail() there would be caught by the
                // catch below and re-reported under the wrong message.
                Throwable thrown = null;
                try
                {
                    StorageService.instance.decommission(true);
                }
                catch (Throwable t)
                {
                    thrown = t;
                }
                assertNotNull("a failing hook must be reported to the caller", thrown);
                assertTrue("the failure must name the offending hook, was: " + thrown,
                           String.valueOf(thrown.getMessage()).contains("exploding"));

                assertEquals("a failing hook must not skip the hooks behind it",
                             Arrays.asList("exploding", "after-the-explosion"), Observed.order);

                // The decommission itself completed: the node is not stranded mid-leave.
                assertEquals(DECOMMISSIONED.name(), StorageService.instance.getOperationMode());
                assertEquals(BootstrapState.DECOMMISSIONED.name(), StorageService.instance.getBootstrapState());
                assertFalse(StorageService.instance.isDecommissioning());

                // ...so the operator is not stuck. Reaching DECOMMISSIONED is what makes the repeat
                // call hit decommission()'s existing early return instead of the ring-membership
                // check that would reject it forever -- which is the whole reason a hook failure
                // must not stop the decommission short. Hooks do not run a second time.
                Observed.order.clear();
                try
                {
                    StorageService.instance.decommission(true);
                }
                catch (Throwable t)
                {
                    fail("decommissioning an already decommissioned node should be a no-op, got: " + t);
                }
                assertEquals("a repeat decommission must not re-run the hooks",
                             Collections.emptyList(), Observed.order);
            });
        }
    }
}
