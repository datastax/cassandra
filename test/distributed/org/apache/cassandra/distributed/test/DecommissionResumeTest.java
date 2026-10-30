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

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.db.SystemKeyspace.TRANSFERRED_RANGES_V2;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.test.ring.BootstrapTest.populate;
import static org.apache.cassandra.service.StorageService.Mode.DECOMMISSION_FAILED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Counterpart of {@link DecommissionTest#testAbortingDecommissionRestreams}: a decommission that failed after
 * streaming and is retried without a restart is still leaving, so the writes made in between kept reaching the
 * pending replicas, and the retry may skip the ranges it already streamed instead of resetting
 * system.transferred_ranges_v2.
 */
public class DecommissionResumeTest extends TestBaseImpl
{
    @Test
    public void testResumingFailedDecommissionWithoutRestartKeepsTransferredRanges() throws Exception
    {
        try (Cluster cluster = builder().withNodes(4)
                                        .withConfig(config -> config.with(NETWORK, GOSSIP)
                                                                    .set("hinted_handoff_enabled", false))
                                        .withInstanceInitializer((cl, threadGroup, num, generation) -> {
                                            if (num == 2)
                                                BB.install(cl);
                                        })
                                        .start())
        {
            populate(cluster, 0, 100, 1, 2, ConsistencyLevel.QUORUM);

            IInvokableInstance leavingNode = cluster.get(2);

            leavingNode.nodetoolResult("decommission").asserts().failure();
            leavingNode.runOnInstance(() -> {
                assertEquals(DECOMMISSION_FAILED, StorageService.Mode.valueOf(StorageService.instance.getOperationMode()));
                assertTrue(StorageService.instance.getTokenMetadata().isLeaving(FBUtilities.getBroadcastAddressAndPort()));
            });
            Object[][] ranges = leavingNode.executeInternal("SELECT keyspace_name FROM system." + TRANSFERRED_RANGES_V2);
            assertTrue("transferred ranges present for keyspace", Arrays.stream(ranges).anyMatch(x -> x[0].equals(KEYSPACE)));

            // the node is still leaving, so these reach the pending replicas: the retry can skip the streamed ranges
            populate(cluster, 100, 200, 1, 2, ConsistencyLevel.ONE);

            long mark = leavingNode.logs().mark();
            leavingNode.nodetoolResult("decommission").asserts().success();

            assertTrue(leavingNode.logs().grep(mark, "resetting transferred ranges").getResult().isEmpty());
            assertFalse(leavingNode.logs().grep(mark, "Skipping transferred range .* of keyspace " + KEYSPACE).getResult().isEmpty());

            for (int i = 0; i < 200; i++)
                assertEquals(1, cluster.get(1).coordinator().execute("SELECT pk FROM " + KEYSPACE + ".tbl WHERE pk = ?", ConsistencyLevel.ALL, i).length);
        }
    }

    public static class BB
    {
        private static final AtomicBoolean failed = new AtomicBoolean();

        static void install(ClassLoader cl)
        {
            new ByteBuddy().rebase(StorageService.class)
                           .method(named("streamHints"))
                           .intercept(MethodDelegation.to(BB.class))
                           .make()
                           .load(cl, ClassLoadingStrategy.Default.INJECTION);
        }

        @SuppressWarnings("unused")
        public static Future<?> streamHints(@SuperCall Callable<Future<?>> zuper) throws Exception
        {
            // fail the first attempt once all the data has been streamed, let the retry through
            if (failed.compareAndSet(false, true))
                return ImmediateFuture.failure(new IOException("failing hints so that decommission fails at the last possible moment"));
            return zuper.call();
        }
    }
}
