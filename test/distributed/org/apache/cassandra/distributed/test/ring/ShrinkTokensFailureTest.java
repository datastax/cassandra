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

package org.apache.cassandra.distributed.test.ring;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.BeforeClass;
import org.junit.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.TokenCountOverride;
import org.apache.cassandra.streaming.StreamState;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.config.CassandraRelevantProperties.RING_DELAY;
import static org.apache.cassandra.config.CassandraRelevantProperties.UNSAFE_SYSTEM;
import static org.apache.cassandra.distributed.test.ring.ShrinkTokensTest.NODES;
import static org.apache.cassandra.distributed.test.ring.ShrinkTokensTest.TOKENS;
import static org.apache.cassandra.distributed.test.ring.ShrinkTokensTest.assertDataPlacement;
import static org.apache.cassandra.distributed.test.ring.ShrinkTokensTest.createSchema;
import static org.apache.cassandra.distributed.test.ring.ShrinkTokensTest.shrink;
import static org.apache.cassandra.distributed.test.ring.ShrinkTokensTest.tokens;
import static org.apache.cassandra.distributed.test.ring.ShrinkTokensTest.tokensSeenBy;
import static org.apache.cassandra.distributed.test.ring.ShrinkTokensTest.write;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * Failures and aborts of {@code StorageService.shrinkTokens} (nodetool settokens): rollback, restart during the
 * operation, other range movements refused meanwhile.
 */
public class ShrinkTokensFailureTest extends TestBaseImpl
{
    @BeforeClass
    public static void setUpRingDelay()
    {
        RING_DELAY.setLong(5000);
    }

    /** Faults injected in node 1. */
    public static class Faults
    {
        public static volatile boolean failStream;
        public static volatile boolean blockStream;
        public static volatile boolean streamReached;
        public static volatile boolean failRecord;
        public static volatile java.util.concurrent.CompletableFuture<StreamState> blockedStream;

        static void install(ClassLoader classLoader, Integer node)
        {
            if (node != 1)
                return;
            new ByteBuddy().rebase(org.apache.cassandra.service.RangeRelocator.class)
                           .method(named("stream"))
                           .intercept(MethodDelegation.to(Faults.class))
                           .make()
                           .load(classLoader, ClassLoadingStrategy.Default.INJECTION);
            new ByteBuddy().rebase(TokenCountOverride.class)
                           .method(named("record"))
                           .intercept(MethodDelegation.to(Faults.class))
                           .make()
                           .load(classLoader, ClassLoadingStrategy.Default.INJECTION);
        }

        public static Future<StreamState> stream(@SuperCall Callable<Future<StreamState>> zuper) throws Exception
        {
            streamReached = true;
            if (failStream)
                throw new RuntimeException("injected streaming failure");
            if (blockStream)
            {
                // completes only when the test says so
                blockedStream = new java.util.concurrent.CompletableFuture<>();
                return blockedStream;
            }
            return zuper.call();
        }

        public static void record(Collection<Token> previous, Collection<Token> kept, int numTokens, @SuperCall Callable<Void> zuper) throws Exception
        {
            if (failRecord)
                throw new RuntimeException("injected failure while recording the token count");
            zuper.call();
        }
    }

    private static void assertNotShrinking(Cluster cluster, IInvokableInstance node, int tokens)
    {
        for (IInvokableInstance observer : cluster)
        {
            if (observer.isShutdown())
                continue;
            await().atMost(60, TimeUnit.SECONDS).untilAsserted(() -> {
                assertThat(observer.callOnInstance(() -> StorageService.instance.getTokenMetadata().getSizeOfShrinkingEndpoints())).isZero();
                assertThat(tokensSeenBy(observer, node)).hasSize(tokens);
            });
        }
        assertThat(node.callOnInstance(() -> TokenCountOverride.exists())).isFalse();
    }

    private static void awaitAllAlive(Cluster cluster)
    {
        for (IInvokableInstance observer : cluster)
            await().atMost(60, TimeUnit.SECONDS).untilAsserted(() -> assertThat(observer.callOnInstance(() -> org.apache.cassandra.gms.Gossiper.instance.getLiveMembers().size())).isEqualTo(NODES));
    }

    @Test
    public void testFailedShrinkRollsBack() throws Throwable
    {
        try (Cluster cluster = ShrinkTokensTest.builder(3).withInstanceInitializer(Faults::install).start())
        {
            createSchema(cluster);
            write(cluster, 0, 200);
            IInvokableInstance node = cluster.get(1);
            List<String> current = tokens(node);
            List<String> keep = new ArrayList<>(current.subList(0, 4));

            // gossip must be running
            node.nodetoolResult("disablegossip").asserts().success();
            assertThatThrownBy(() -> shrink(node, keep)).hasMessageContaining("Gossip is disabled");
            node.nodetoolResult("enablegossip").asserts().success();
            awaitAllAlive(cluster);

            // streaming fails
            node.runOnInstance(() -> Faults.failStream = true);
            assertThatThrownBy(() -> shrink(node, keep)).hasMessageContaining("failed, the node keeps its " + TOKENS + " tokens")
                                                         .hasMessageContaining("injected streaming failure");
            assertThat(tokens(node)).containsExactlyInAnyOrderElementsOf(current);
            assertThat(node.callOnInstance(() -> StorageService.instance.getOperationMode())).isEqualTo("NORMAL");
            assertNotShrinking(cluster, node, TOKENS);
            node.runOnInstance(() -> Faults.failStream = false);

            // recording the new token count fails (e.g. the metadata directory is full), at the commit point
            node.runOnInstance(() -> Faults.failRecord = true);
            assertThatThrownBy(() -> shrink(node, keep)).hasMessageContaining("injected failure while recording the token count");
            assertThat(tokens(node)).containsExactlyInAnyOrderElementsOf(current);
            assertNotShrinking(cluster, node, TOKENS);
            node.runOnInstance(() -> Faults.failRecord = false);

            // once the failures are gone the shrink can be retried
            shrink(node, keep);
            assertThat(tokens(node)).containsExactlyInAnyOrderElementsOf(keep);
            Set<Integer> keys = new HashSet<>();
            for (int i = 0; i < 200; i++)
                keys.add(i);
            assertDataPlacement(cluster, keys, false);
        }
    }

    /**
     * Restarting the node is the way to abort a shrink: it must not hang, and the node comes back with its tokens.
     * Other range movements are refused while the node is shrinking.
     */
    @Test
    public void testRestartDuringShrink() throws Throwable
    {
        try (Cluster cluster = ShrinkTokensTest.builder(5).withInstanceInitializer(Faults::install).start())
        {
            createSchema(cluster);
            write(cluster, 0, 200);
            IInvokableInstance node = cluster.get(1);
            List<String> current = tokens(node);
            List<String> keep = new ArrayList<>(current.subList(0, 8));

            node.runOnInstance(() -> Faults.blockStream = true);
            AtomicReference<Throwable> shrinkError = new AtomicReference<>();
            Thread shrinker = new Thread(() -> {
                try
                {
                    shrink(node, keep);
                }
                catch (Throwable t)
                {
                    shrinkError.set(t);
                }
            });
            shrinker.start();
            await().atMost(120, TimeUnit.SECONDS).until(() -> node.callOnInstance(() -> Faults.streamReached));
            assertThat(node.callOnInstance(() -> StorageService.instance.getOperationMode())).isEqualTo("SHRINKING");
            for (IInvokableInstance observer : cluster)
                assertThat(observer.callOnInstance(() -> StorageService.instance.getTokenMetadata().getSizeOfShrinkingEndpoints())).isEqualTo(1);

            // no other range movement while shrinking
            cluster.get(2).nodetoolResult("decommission", "--force").asserts().failure().errorContains("while nodes are shrinking");
            node.nodetoolResult("move", "123").asserts().failure();

            // drain (the first step of a graceful stop) doesn't wait for the shrink, and the shrink failing afterwards
            // doesn't bring the node back to NORMAL
            node.nodetoolResult("drain").asserts().success();
            assertThat(node.callOnInstance(() -> StorageService.instance.getOperationMode())).isEqualTo("DRAINED");
            node.runOnInstance(() -> Faults.blockedStream.completeExceptionally(new RuntimeException("streaming interrupted by the drain")));
            shrinker.join(TimeUnit.MINUTES.toMillis(2));
            assertThat(shrinker.isAlive()).isFalse();
            assertThat(shrinkError.get()).hasMessageContaining("failed, the node keeps its " + TOKENS + " tokens");
            assertThat(node.callOnInstance(() -> StorageService.instance.getOperationMode())).isEqualTo("DRAINED");

            // the in-JVM shutdown flushes the schema unless the system keyspaces are unsafe, which a drained node refuses
            try (WithProperties properties = new WithProperties().set(UNSAFE_SYSTEM, true))
            {
                node.shutdown().get(2, TimeUnit.MINUTES);
            }

            node.startup();
            assertThat(tokens(node)).containsExactlyInAnyOrderElementsOf(current);
            assertNotShrinking(cluster, node, TOKENS);

            node.runOnInstance(() -> Faults.blockStream = false);
            shrink(node, keep);
            assertThat(tokens(node)).containsExactlyInAnyOrderElementsOf(keep);
        }
    }
}
