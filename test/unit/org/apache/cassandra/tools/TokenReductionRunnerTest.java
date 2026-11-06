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

package org.apache.cassandra.tools;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TokenReductionRunnerTest
{
    private Path dir;

    @Before
    public void createDirectory() throws Exception
    {
        dir = Files.createTempDirectory("tokenreductionrunner");
    }

    @After
    public void deleteDirectory()
    {
        FileUtils.deleteRecursive(new File(dir));
    }

    /** A cluster where shrinks apply at once on the node and after {@link #lag} polls on the other nodes. */
    private static final class FakeCluster implements TokenReductionRunner.ClusterOperations
    {
        final Map<String, Set<String>> tokens = new HashMap<>();
        final Map<String, Map<String, Set<String>>> seenBy = new HashMap<>();
        final Set<String> down = new HashSet<>();
        final Set<String> moving = new HashSet<>();
        final Map<String, Set<String>> hints = new HashMap<>();
        final List<String> shrunk = new ArrayList<>();
        final List<String> cleaned = new ArrayList<>();
        final Map<String, Integer> polls = new HashMap<>();
        String failShrinkOf;           // the shrink fails and the node keeps its tokens
        String loseConnectionOf;       // the shrink completes on the node but the call fails
        String failCleanupOf;
        String mode = "NORMAL";
        int lag;

        public Set<String> endpoints()
        {
            return new HashSet<>(tokens.keySet());
        }

        public Set<String> liveEndpoints()
        {
            Set<String> live = endpoints();
            live.removeAll(down);
            return live;
        }

        public Set<String> endpointsInRangeMovement()
        {
            return new HashSet<>(moving);
        }

        public Set<String> tokens(String observer, String endpoint)
        {
            if (observer.equals(endpoint))
                return new HashSet<>(tokens.get(endpoint));
            Set<String> seen = seenBy.computeIfAbsent(observer, o -> new HashMap<>()).get(endpoint);
            if (seen == null)
                return new HashSet<>(tokens.get(endpoint));
            int count = polls.merge(observer + '>' + endpoint, 1, Integer::sum);
            if (count > lag)
            {
                seenBy.get(observer).remove(endpoint);
                return new HashSet<>(tokens.get(endpoint));
            }
            return new HashSet<>(seen);
        }

        public String operationMode(String endpoint)
        {
            return mode;
        }

        public String hostId(String endpoint)
        {
            return "id-" + endpoint;
        }

        public Set<String> hostIdsWithPendingHints(String node)
        {
            return hints.getOrDefault(node, Collections.emptySet());
        }

        public void shrink(String endpoint, List<String> keep) throws IOException
        {
            if (endpoint.equals(failShrinkOf))
                throw new IOException("injected failure");
            for (String observer : tokens.keySet())
                if (!observer.equals(endpoint))
                    seenBy.computeIfAbsent(observer, o -> new HashMap<>()).put(endpoint, new HashSet<>(tokens.get(endpoint)));
            tokens.put(endpoint, new HashSet<>(keep));
            shrunk.add(endpoint);
            if (endpoint.equals(loseConnectionOf))
                throw new IOException("connection lost");
        }

        public void flushAndCleanup(String endpoint, PrintStream out) throws IOException
        {
            if (endpoint.equals(failCleanupOf))
                throw new IOException("injected cleanup failure");
            cleaned.add(endpoint);
        }
    }

    private static final class Setup
    {
        final FakeCluster cluster = new FakeCluster();
        TokenReductionRunner.Plan plan;
    }

    /** 5 nodes x 32 tokens in dc1 and 3 nodes x 32 tokens in dc2, planned to 8 tokens by halving. */
    private Setup plan(boolean ports) throws Exception
    {
        Random random = new Random(21);
        Setup setup = new Setup();
        List<String> csv = new ArrayList<>();
        Set<Token> used = new HashSet<>();
        for (int node = 1; node <= 8; node++)
        {
            String endpoint = "10.0.0." + node + ":7000";
            String dc = node <= 5 ? "dc1" : "dc2";
            Set<String> nodeTokens = new HashSet<>();
            while (nodeTokens.size() < 32)
            {
                Token token = Murmur3Partitioner.instance.getRandomToken(random);
                if (used.add(token))
                {
                    nodeTokens.add(token.toString());
                    csv.add(String.format("%s,%s,rack%d,%s", ports ? endpoint : "10.0.0." + node, dc, node % 3, token));
                }
            }
            setup.cluster.tokens.put(endpoint, nodeTokens);
        }
        Path ring = dir.resolve("ring.csv");
        Files.write(ring, csv, StandardCharsets.UTF_8);
        Path output = dir.resolve("plan");
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        int exit = TokenReductionPlannerTool.run(new String[]{ "--ring", ring.toString(), "--replication", "dc1:3,dc2:2", "--target", "8", "--output", output.toString() },
                                                 new PrintStream(out, true, StandardCharsets.UTF_8.name()), new PrintStream(out, true, StandardCharsets.UTF_8.name()));
        assertThat(exit).as(out.toString(StandardCharsets.UTF_8.name())).isZero();
        setup.plan = TokenReductionRunner.readPlan(output);
        return setup;
    }

    private static TokenReductionRunner.Options options()
    {
        TokenReductionRunner.Options options = new TokenReductionRunner.Options();
        options.pollMillis = 1;
        options.waitTimeoutMillis = 5000;
        return options;
    }

    private static int run(Setup setup, TokenReductionRunner.Options options) throws IOException
    {
        return TokenReductionRunner.run(setup.plan, setup.cluster, options, new PrintStream(new ByteArrayOutputStream()));
    }

    @Test
    public void testReadPlan() throws Exception
    {
        Setup setup = plan(true);
        // rounds 16 and 8, every node in each round, dc1 before dc2, steps in order
        assertThat(setup.plan.steps).hasSize(16);
        int previousRound = 0;
        String previousDc = "";
        int previousNumber = 0;
        for (TokenReductionRunner.Step step : setup.plan.steps)
        {
            if (step.round != previousRound)
            {
                assertThat(step.round).isEqualTo(previousRound + 1);
                previousRound = step.round;
                previousDc = "";
            }
            if (!step.datacenter.equals(previousDc))
            {
                assertThat(step.datacenter.compareTo(previousDc)).isPositive();
                previousDc = step.datacenter;
                previousNumber = 0;
            }
            assertThat(step.number).isEqualTo(previousNumber + 1);
            previousNumber = step.number;
            assertThat(step.keep).hasSize(step.round == 1 ? 16 : 8);
            assertThat(step.endpoint).matches("10\\.0\\.0\\.\\d:7000");
        }
        // the initial ring is in the plan
        assertThat(setup.plan.initialTokens).hasSize(8);
        setup.cluster.tokens.forEach((endpoint, tokens) -> assertThat(new HashSet<>(setup.plan.initialTokens.get(endpoint))).isEqualTo(tokens));
        assertThatThrownBy(() -> TokenReductionRunner.readPlan(dir)).hasMessageContaining("not a plan written by tokenreductionplanner");
    }

    @Test
    public void testRunAndResume() throws Exception
    {
        Setup setup = plan(true);
        setup.cluster.lag = 3; // the other nodes see the new tokens after a few polls
        assertThat(run(setup, options())).isEqualTo(16);
        for (Set<String> tokens : setup.cluster.tokens.values())
            assertThat(tokens).hasSize(8);
        assertThat(setup.cluster.cleaned).isEqualTo(setup.cluster.shrunk);
        List<String> order = new ArrayList<>();
        for (TokenReductionRunner.Step step : setup.plan.steps)
            order.add(step.endpoint);
        assertThat(setup.cluster.shrunk).isEqualTo(order);

        // a second run has no shrink to do: the round 1 steps are skipped (the nodes are at round 2), the round 2
        // steps are done and only flush and clean up again
        setup.cluster.cleaned.clear();
        assertThat(run(setup, options())).isZero();
        assertThat(setup.cluster.shrunk).hasSize(16);
        assertThat(setup.cluster.cleaned).hasSize(8);
    }

    @Test
    public void testStopsAtFirstFailureAndResumes() throws Exception
    {
        Setup setup = plan(true);
        String failing = setup.plan.steps.get(3).endpoint;
        setup.cluster.failShrinkOf = failing;
        assertThatThrownBy(() -> run(setup, options())).hasMessageContaining("nodetool settokens failed: injected failure")
                                                       .hasMessageContaining("the node kept its tokens")
                                                       .hasMessageContaining(failing);
        assertThat(setup.cluster.shrunk).hasSize(3);
        setup.cluster.failShrinkOf = null;
        assertThat(run(setup, options())).isEqualTo(13);
    }

    @Test
    public void testCleanupRunsAgainOnResume() throws Exception
    {
        Setup setup = plan(true);
        String failing = setup.plan.steps.get(0).endpoint;
        setup.cluster.failCleanupOf = failing;
        assertThatThrownBy(() -> run(setup, options())).hasMessageContaining("injected cleanup failure");
        assertThat(setup.cluster.shrunk).containsExactly(failing);
        assertThat(setup.cluster.cleaned).isEmpty();
        setup.cluster.failCleanupOf = null;
        TokenReductionRunner.Options options = options();
        options.round = 1;
        assertThat(run(setup, options)).isEqualTo(7);
        // the node of the first step got its cleanup, first
        assertThat(setup.cluster.cleaned.get(0)).isEqualTo(failing);
        assertThat(setup.cluster.cleaned).hasSize(8);
    }

    @Test
    public void testLostConnectionDuringShrink() throws Exception
    {
        Setup setup = plan(true);
        // the call fails but the node completed the shrink: the runner goes on
        setup.cluster.loseConnectionOf = setup.plan.steps.get(0).endpoint;
        TokenReductionRunner.Options options = options();
        options.round = 1;
        options.datacenter = "dc1";
        assertThat(run(setup, options)).isEqualTo(5);
    }

    @Test
    public void testRoundsCannotBeSkipped() throws Exception
    {
        Setup setup = plan(true);
        TokenReductionRunner.Options options = options();
        options.round = 2;
        assertThatThrownBy(() -> run(setup, options)).hasMessageContaining("a round of the plan was skipped");
        assertThat(setup.cluster.shrunk).isEmpty();
        // also in a dry run
        options.dryRun = true;
        assertThatThrownBy(() -> run(setup, options)).hasMessageContaining("a round of the plan was skipped");
    }

    @Test
    public void testPreconditions() throws Exception
    {
        Setup setup = plan(true);
        String first = setup.plan.steps.get(0).endpoint;

        setup.cluster.down.add("10.0.0.8:7000");
        assertThatThrownBy(() -> run(setup, options())).hasMessageContaining("nodes are down: [10.0.0.8:7000]");
        setup.cluster.down.clear();

        setup.cluster.moving.add("10.0.0.7:7000");
        assertThatThrownBy(() -> run(setup, options())).hasMessageContaining("joining, leaving, moving or shrinking: [10.0.0.7:7000]");
        setup.cluster.moving.clear();

        setup.cluster.hints.put("10.0.0.6:7000", Collections.singleton("id-" + first));
        assertThatThrownBy(() -> run(setup, options())).hasMessageContaining("pending hints").hasMessageContaining("10.0.0.6:7000");
        setup.cluster.hints.clear();

        // the ring changed since the plan was made
        setup.cluster.tokens.put(first, new HashSet<>(Arrays.asList("123", "456")));
        assertThatThrownBy(() -> run(setup, options())).hasMessageContaining("the ring changed since the plan was made");
        assertThat(setup.cluster.shrunk).isEmpty();
    }

    @Test
    public void testWaitTimeout() throws Exception
    {
        Setup setup = plan(true);
        setup.cluster.lag = Integer.MAX_VALUE;
        TokenReductionRunner.Options options = options();
        options.waitTimeoutMillis = 50;
        assertThatThrownBy(() -> run(setup, options)).hasMessageContaining("don't see the new tokens yet").hasMessageContaining("run again to flush and clean up");
    }

    @Test
    public void testDryRunAndFilters() throws Exception
    {
        Setup setup = plan(true);
        // a dry run checks every round of the plan, as if the previous steps were done
        TokenReductionRunner.Options options = options();
        options.dryRun = true;
        assertThat(run(setup, options)).isZero();
        assertThat(setup.cluster.shrunk).isEmpty();
        options.round = 1;
        assertThat(run(setup, options)).isZero();
        assertThat(setup.cluster.shrunk).isEmpty();

        options = options();
        options.round = 1;
        options.datacenter = "dc2";
        assertThat(run(setup, options)).isEqualTo(3);
        assertThat(setup.cluster.shrunk).containsExactlyInAnyOrder("10.0.0.6:7000", "10.0.0.7:7000", "10.0.0.8:7000");
        assertThat(setup.cluster.cleaned).hasSize(3);

        options.cleanup = false;
        options.datacenter = "dc1";
        assertThat(run(setup, options)).isEqualTo(5);
        assertThat(setup.cluster.cleaned).hasSize(3);
    }

    @Test
    public void testPlanWithoutPorts() throws Exception
    {
        Setup setup = plan(false);
        assertThat(setup.plan.steps.get(0).endpoint).doesNotContain(":");
        assertThat(run(setup, options())).isEqualTo(16);
        assertThat(setup.cluster.shrunk).allMatch(e -> e.endsWith(":7000"));
    }

    @Test
    public void testAddresses() throws Exception
    {
        assertThat(TokenReductionRunner.JmxCluster.address("10.0.0.1:7000")).isEqualTo("10.0.0.1");
        assertThat(TokenReductionRunner.JmxCluster.address("10.0.0.1")).isEqualTo("10.0.0.1");
        assertThat(TokenReductionRunner.JmxCluster.address("[::1]:7000")).isEqualTo("::1");
        assertThat(TokenReductionRunner.JmxCluster.address("::1")).isEqualTo("::1");

        Path file = dir.resolve("jmx.txt");
        Files.write(file, Arrays.asList("# endpoint jmx", "10.0.0.1:7000 10.1.0.1:7199", "10.0.0.2 jmx2.example:17199"), StandardCharsets.UTF_8);
        Map<String, String> addresses = TokenReductionRunner.readJmxAddresses(file);
        TokenReductionRunner.JmxCluster cluster = new TokenReductionRunner.JmxCluster("10.0.0.9", 7199, addresses, null, null);
        assertThat(cluster.jmxAddress("10.0.0.1:7000")).isEqualTo("10.1.0.1:7199");
        assertThat(cluster.jmxAddress("10.0.0.2:7000")).isEqualTo("jmx2.example:17199");
        assertThat(cluster.jmxAddress("10.0.0.3:7000")).isEqualTo("10.0.0.3:7199");
        assertThat(cluster.jmxAddress("[::3]:7000")).isEqualTo("[::3]:7199");
        assertThat(cluster.jmxAddress(null)).isEqualTo("10.0.0.9:7199");
        Files.write(file, Collections.singletonList("10.0.0.1 no-port"), StandardCharsets.UTF_8);
        assertThatThrownBy(() -> TokenReductionRunner.readJmxAddresses(file)).hasMessageContaining("expected '<endpoint> <jmx host>:<jmx port>'");
    }

    @Test
    public void testPasswordFile() throws Exception
    {
        Path file = dir.resolve("jmxremote.password");
        Files.write(file, Arrays.asList("monitor secret1", "admin secret2"), StandardCharsets.UTF_8);
        assertThat(TokenReductionRunner.readPassword("admin", file.toString())).isEqualTo("secret2");
        assertThatThrownBy(() -> TokenReductionRunner.readPassword("nobody", file.toString())).hasMessageContaining("No password for nobody");
    }

    @Test
    public void testExitCodes() throws Exception
    {
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        PrintStream stream = new PrintStream(err, true);
        assertThat(TokenReductionRunner.run(new String[0], stream, stream)).isEqualTo(TokenReductionRunner.USAGE_ERROR);
        assertThat(err.toString()).contains("Missing required option");
        assertThat(TokenReductionRunner.run(new String[]{ "--plan", dir.toString(), "--password", "x" }, stream, stream)).isEqualTo(TokenReductionRunner.USAGE_ERROR);
        assertThat(err.toString()).contains("A password needs a --username");
        // not a plan: the run stops, without the usage
        err.reset();
        assertThat(TokenReductionRunner.run(new String[]{ "--plan", dir.toString(), "--port", "1" }, stream, stream)).isEqualTo(TokenReductionRunner.STOPPED);
        assertThat(err.toString()).contains("not a plan written by tokenreductionplanner").doesNotContain("Options are");
    }
}
