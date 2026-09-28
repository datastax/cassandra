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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.dht.tokenallocator.TokenReductionPlanner;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.Constants;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.TokenCountOverride;
import org.apache.cassandra.streaming.StreamState;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * In-place reduction of the number of tokens of live nodes with {@code StorageService.shrinkTokens} (nodetool
 * settokens), see docs/operations/num-tokens-reduction-design.md.
 */
public class ShrinkTokensTest extends TestBaseImpl
{
    private static final Logger logger = LoggerFactory.getLogger(ShrinkTokensTest.class);

    private static final int NODES = 4;
    private static final int TOKENS = 32;
    private static final int ROWS = 2000;

    private static TokenSupplier randomTokens(long seed)
    {
        Random random = new Random(seed);
        Set<Long> used = new HashSet<>();
        List<List<String>> tokens = new ArrayList<>();
        for (int n = 0; n < NODES; n++)
        {
            List<String> nodeTokens = new ArrayList<>();
            while (nodeTokens.size() < TOKENS)
            {
                long token = random.nextLong();
                if (token != Long.MIN_VALUE && used.add(token))
                    nodeTokens.add(Long.toString(token));
            }
            tokens.add(nodeTokens);
        }
        return node -> tokens.get(node - 1);
    }

    private static Cluster.Builder builder(long seed)
    {
        return Cluster.build(NODES)
                      .withConfig(c -> c.with(GOSSIP, NETWORK))
                      .withTokenCount(TOKENS)
                      .withTokenSupplier(randomTokens(seed));
    }

    private static void createSchema(Cluster cluster)
    {
        cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter0': 3}"));
        cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int PRIMARY KEY, v int)"));
    }

    private static void write(Cluster cluster, int from, int to)
    {
        for (int i = from; i < to; i++)
            cluster.coordinator(2).execute(withKeyspace("INSERT INTO %s.tbl (pk, v) VALUES (?, ?)"), ConsistencyLevel.QUORUM, i, i);
    }

    private static List<String> tokens(IInvokableInstance instance)
    {
        return instance.callOnInstance(() -> new ArrayList<>(StorageService.instance.getTokens()));
    }

    /** Tokens of {@code endpoint} as seen by {@code observer}. */
    private static Set<String> tokensSeenBy(IInvokableInstance observer, IInvokableInstance endpoint)
    {
        String address = endpoint.config().broadcastAddress().getAddress().getHostAddress() + ':' + endpoint.config().broadcastAddress().getPort();
        return observer.callOnInstance(() -> {
            Set<String> tokens = new HashSet<>();
            try
            {
                for (Token token : StorageService.instance.getTokenMetadata().getTokens(InetAddressAndPort.getByName(address)))
                    tokens.add(token.toString());
            }
            catch (java.net.UnknownHostException e)
            {
                throw new AssertionError(e);
            }
            return tokens;
        });
    }

    private static String address(IInvokableInstance instance)
    {
        return instance.config().broadcastAddress().getAddress().getHostAddress() + ':' + instance.config().broadcastAddress().getPort();
    }

    private static void shrink(IInvokableInstance instance, List<String> keep)
    {
        instance.runOnInstance(() -> {
            try
            {
                StorageService.instance.shrinkTokens(keep);
            }
            catch (Exception e)
            {
                throw new RuntimeException(e.getMessage(), e);
            }
        });
    }

    private static Set<Integer> localKeys(IInvokableInstance instance)
    {
        Set<Integer> keys = new HashSet<>();
        for (Object[] row : instance.executeInternal(withKeyspace("SELECT pk FROM %s.tbl")))
            keys.add((Integer) row[0]);
        return keys;
    }

    /**
     * Every key is on all its replicas according to the current ring; with {@code exact}, the nodes have no other key
     * (i.e. after cleanup).
     */
    private static void assertDataPlacement(Cluster cluster, Set<Integer> keys, boolean exact)
    {
        String ks = KEYSPACE;
        List<Integer> keyList = new ArrayList<>(keys);
        Map<Integer, List<String>> replicas = cluster.get(1).callOnInstance(() -> {
            Map<Integer, List<String>> result = new HashMap<>();
            for (int key : keyList)
                result.put(key, StorageService.instance.getNaturalEndpointsWithPort(ks, "tbl", Integer.toString(key)));
            return result;
        });
        Map<String, Set<Integer>> expected = new HashMap<>();
        replicas.forEach((key, endpoints) -> endpoints.forEach(endpoint -> expected.computeIfAbsent(endpoint, e -> new HashSet<>()).add(key)));
        for (IInvokableInstance instance : cluster)
        {
            Set<Integer> local = localKeys(instance);
            Set<Integer> shouldHave = expected.getOrDefault(address(instance), Collections.emptySet());
            assertThat(local).as("keys of %s", address(instance)).containsAll(shouldHave);
            if (exact)
                assertThat(local).as("keys of %s after cleanup", address(instance)).isEqualTo(shouldHave);
        }
    }

    @Test
    public void testShrinkUnderWrites() throws Throwable
    {
        try (Cluster cluster = builder(1).start())
        {
            createSchema(cluster);
            write(cluster, 0, ROWS);
            IInvokableInstance node = cluster.get(1);
            List<String> current = tokens(node);
            assertThat(current).hasSize(TOKENS);

            // refusals: not a subset, not a strict subset, empty
            assertThatThrownBy(() -> shrink(node, Collections.singletonList(tokens(cluster.get(2)).get(0))))
            .hasMessageContaining("must be tokens of this node");
            assertThatThrownBy(() -> shrink(node, current)).hasMessageContaining("already has exactly these");
            assertThatThrownBy(() -> shrink(node, Collections.emptyList())).hasMessageContaining("at least one token");

            List<String> shuffled = new ArrayList<>(current);
            Collections.shuffle(shuffled, new Random(2));
            List<String> keep = new ArrayList<>(shuffled.subList(0, 8));

            // writes keep flowing during the shrink, at QUORUM, through another coordinator
            AtomicBoolean stop = new AtomicBoolean();
            AtomicInteger written = new AtomicInteger(ROWS);
            Thread writer = new Thread(() -> {
                while (!stop.get())
                {
                    int key = written.get();
                    cluster.coordinator(2).execute(withKeyspace("INSERT INTO %s.tbl (pk, v) VALUES (?, ?)"), ConsistencyLevel.QUORUM, key, key);
                    written.incrementAndGet();
                }
            });
            writer.start();
            try
            {
                shrink(node, keep);
            }
            finally
            {
                stop.set(true);
                writer.join();
            }
            logger.info("{} rows written during the shrink", written.get() - ROWS);
            assertThat(written.get()).isGreaterThan(ROWS);

            assertThat(tokens(node)).containsExactlyInAnyOrderElementsOf(keep);
            for (IInvokableInstance observer : cluster)
                await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> assertThat(tokensSeenBy(observer, node)).containsExactlyInAnyOrderElementsOf(new HashSet<>(keep)));
            assertThat(node.callOnInstance(() -> StorageService.instance.getTokenMetadata().getSizeOfShrinkingEndpoints())).isZero();

            Set<Integer> keys = new HashSet<>();
            for (int i = 0; i < written.get(); i++)
                keys.add(i);
            // every write, before and during the shrink, is on all its replicas of the new ring
            assertDataPlacement(cluster, keys, false);
            // and after cleanup no node has data it doesn't replicate
            for (IInvokableInstance instance : cluster)
                instance.nodetoolResult("cleanup", KEYSPACE).asserts().success();
            assertDataPlacement(cluster, keys, true);

            // the node restarts with its 8 tokens although num_tokens is still 32
            assertThat(node.callOnInstance(() -> TokenCountOverride.exists())).isTrue();
            node.shutdown().get();
            node.startup();
            assertThat(tokens(node)).containsExactlyInAnyOrderElementsOf(keep);
            // but not with yet another num_tokens
            node.shutdown().get();
            node.config().set("num_tokens", 20);
            node.config().set(Constants.KEY_DTEST_API_STARTUP_FAILURE_AS_SHUTDOWN, false);
            assertThatThrownBy(node::startup).hasMessageContaining("Cannot change the number of tokens from 8 to 20");
            node.shutdown().get();
            node.config().set(Constants.KEY_DTEST_API_STARTUP_FAILURE_AS_SHUTDOWN, true);
            // once num_tokens is updated, the override is removed
            node.config().set("num_tokens", 8);
            node.startup();
            assertThat(tokens(node)).containsExactlyInAnyOrderElementsOf(keep);
            assertThat(node.callOnInstance(() -> TokenCountOverride.exists())).isFalse();
        }
    }

    public static class FailStreaming
    {
        public static volatile boolean fail = true;

        static void install(ClassLoader classLoader, Integer node)
        {
            if (node != 1)
                return;
            new ByteBuddy().rebase(org.apache.cassandra.service.RangeRelocator.class)
                           .method(named("stream"))
                           .intercept(MethodDelegation.to(FailStreaming.class))
                           .make()
                           .load(classLoader, ClassLoadingStrategy.Default.INJECTION);
        }

        public static Future<StreamState> stream(@SuperCall Callable<Future<StreamState>> zuper) throws Exception
        {
            if (fail)
                throw new RuntimeException("injected streaming failure");
            return zuper.call();
        }
    }

    @Test
    public void testFailedShrinkRollsBack() throws Throwable
    {
        try (Cluster cluster = builder(3).withInstanceInitializer(FailStreaming::install).start())
        {
            createSchema(cluster);
            write(cluster, 0, 200);
            IInvokableInstance node = cluster.get(1);
            List<String> current = tokens(node);
            List<String> keep = new ArrayList<>(current.subList(0, 4));

            assertThatThrownBy(() -> shrink(node, keep)).hasMessageContaining("failed, the node keeps its " + TOKENS + " tokens")
                                                         .hasMessageContaining("injected streaming failure");
            assertThat(tokens(node)).containsExactlyInAnyOrderElementsOf(current);
            assertThat(node.callOnInstance(() -> StorageService.instance.getOperationMode())).isEqualTo("NORMAL");
            for (IInvokableInstance observer : cluster)
            {
                await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
                    assertThat(observer.callOnInstance(() -> StorageService.instance.getTokenMetadata().getSizeOfShrinkingEndpoints())).isZero();
                    assertThat(tokensSeenBy(observer, node)).hasSize(TOKENS);
                });
            }
            assertThat(node.callOnInstance(() -> TokenCountOverride.exists())).isFalse();

            // once the failure is gone the shrink can be retried
            node.runOnInstance(() -> FailStreaming.fail = false);
            shrink(node, keep);
            assertThat(tokens(node)).containsExactlyInAnyOrderElementsOf(keep);
            Set<Integer> keys = new HashSet<>();
            for (int i = 0; i < 200; i++)
                keys.add(i);
            assertDataPlacement(cluster, keys, false);
        }
    }

    /**
     * Plans a reduction from 32 to 8 tokens with the planner, runs every step and compares the resulting ownership
     * with the plan.
     */
    @Test
    public void testPlannedReduction() throws Throwable
    {
        try (Cluster cluster = builder(4).start())
        {
            createSchema(cluster);
            write(cluster, 0, ROWS);

            List<TokenReductionPlanner.Node> nodes = new ArrayList<>();
            Map<String, IInvokableInstance> byAddress = new HashMap<>();
            for (IInvokableInstance instance : cluster)
            {
                List<Token> tokens = new ArrayList<>();
                for (String token : tokens(instance))
                    tokens.add(Murmur3Partitioner.instance.getTokenFactory().fromString(token));
                nodes.add(new TokenReductionPlanner.Node(address(instance), "datacenter0", "rack0", tokens));
                byAddress.put(address(instance), instance);
            }
            TokenReductionPlanner.Plan plan = TokenReductionPlanner.plan(nodes, Collections.singletonMap("datacenter0", 3), TokenReductionPlanner.rounds(TOKENS, 8, 2));
            assertThat(plan.rounds).hasSize(2);

            for (TokenReductionPlanner.Round round : plan.rounds)
            {
                for (TokenReductionPlanner.Step step : round.steps)
                {
                    List<String> keep = new ArrayList<>();
                    for (Token token : step.keep)
                        keep.add(token.toString());
                    shrink(byAddress.get(step.endpoint), keep);
                    for (IInvokableInstance observer : cluster)
                        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> assertThat(tokensSeenBy(observer, byAddress.get(step.endpoint))).hasSize(round.targetTokens));
                }
            }

            String ks = KEYSPACE;
            Map<String, Float> ownership = cluster.get(1).callOnInstance(() -> StorageService.instance.effectiveOwnershipWithPort(ks));
            logger.info("Ownership after the planned reduction: {}, planned: {}", ownership, plan.finalOwnership("datacenter0"));
            plan.finalOwnership("datacenter0").forEach((endpoint, planned) -> assertThat((double) ownership.get(endpoint)).as(endpoint).isCloseTo(planned, org.assertj.core.data.Offset.offset(1e-4)));

            Set<Integer> keys = new HashSet<>();
            for (int i = 0; i < ROWS; i++)
                keys.add(i);
            for (IInvokableInstance instance : cluster)
                instance.nodetoolResult("cleanup", KEYSPACE).asserts().success();
            assertDataPlacement(cluster, keys, true);
            Object[][] count = cluster.coordinator(1).execute(withKeyspace("SELECT count(*) FROM %s.tbl"), ConsistencyLevel.ALL);
            assertThat(count[0][0]).isEqualTo((long) ROWS);
        }
    }
}
