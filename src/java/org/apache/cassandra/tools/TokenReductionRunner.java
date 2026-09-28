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

import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.ParseException;

import org.apache.cassandra.io.util.File;

import static org.apache.cassandra.utils.Clock.Global.nanoTime;

/**
 * Runs a plan written by {@code tokenreductionplanner}: every step, one node at a time, in the order of the plan.
 * For each step it
 * <ol>
 *     <li>skips the step if the node already has the tokens to keep, or a subset of them from a later round (so a
 *     stopped run can simply be started again), and stops if the tokens to keep are not a subset of the node's tokens
 *     (the plan doesn't match the ring);</li>
 *     <li>checks that every node is up, none is joining, leaving or moving (shrinking nodes are shown as moving),
 *     and that no node has hints for the node;</li>
 *     <li>runs {@code nodetool settokens} on the node, which blocks until the shrink is done;</li>
 *     <li>waits until every node sees the new tokens;</li>
 *     <li>runs {@code nodetool flush} and {@code nodetool cleanup} on the node.</li>
 * </ol>
 * It stops at the first failure. See docs/operations/reduce-num-tokens-runbook.md.
 */
public class TokenReductionRunner
{
    /** What the runner needs from the cluster; endpoints are the addresses of the plan, e.g. 10.0.0.1:7000. */
    public interface ClusterOperations
    {
        /** Every endpoint of the ring, as seen by the node the runner is connected to. */
        Set<String> endpoints() throws IOException;

        Set<String> liveEndpoints() throws IOException;

        /** Endpoints joining, leaving or moving (including shrinking). */
        Set<String> endpointsInRangeMovement() throws IOException;

        /** Tokens of {@code endpoint} as seen by {@code observer}. */
        Set<String> tokens(String observer, String endpoint) throws IOException;

        String hostId(String endpoint) throws IOException;

        /** Host ids for which {@code node} has pending hints. */
        Set<String> hostIdsWithPendingHints(String node) throws IOException;

        void shrink(String endpoint, List<String> keep) throws IOException;

        void flushAndCleanup(String endpoint) throws IOException;
    }

    /** A step of the plan: an endpoint keeping the tokens of a file. */
    public static final class Step
    {
        public final int round;
        public final String datacenter;
        public final int number;
        public final String endpoint;
        public final List<String> keep;

        Step(int round, String datacenter, int number, String endpoint, List<String> keep)
        {
            this.round = round;
            this.datacenter = datacenter;
            this.number = number;
            this.endpoint = endpoint;
            this.keep = keep;
        }

        @Override
        public String toString()
        {
            return String.format("round %d, %s step %d: %s keeps %d tokens", round, datacenter, number, endpoint, keep.size());
        }
    }

    public static final class Options
    {
        public boolean dryRun;
        public boolean cleanup = true;
        public long waitTimeoutMillis = TimeUnit.MINUTES.toMillis(10);
        public long pollMillis = TimeUnit.SECONDS.toMillis(5);
        public Integer round;
        public String datacenter;
    }

    private static final Pattern ROUND_DIR = Pattern.compile("round-(\\d+)-(\\d+)");
    private static final Pattern STEP_FILE = Pattern.compile("(\\d+)-(.+)\\.tokens");

    /**
     * Reads the steps of a plan directory, in execution order: rounds, then datacenters, then steps.
     */
    public static List<Step> readPlan(Path plan) throws IOException
    {
        List<Step> steps = new ArrayList<>();
        List<Path> rounds = list(plan).stream()
                                      .filter(p -> Files.isDirectory(p) && ROUND_DIR.matcher(p.getFileName().toString()).matches())
                                      .sorted((a, b) -> Integer.compare(roundNumber(a), roundNumber(b)))
                                      .collect(Collectors.toList());
        if (rounds.isEmpty())
            throw new IllegalArgumentException("No round-<n>-<tokens> directory in " + plan + ": not a plan written by tokenreductionplanner");
        for (Path round : rounds)
        {
            for (Path dc : list(round).stream().filter(Files::isDirectory).sorted().collect(Collectors.toList()))
            {
                for (Path file : list(dc).stream().sorted().collect(Collectors.toList()))
                {
                    Matcher matcher = STEP_FILE.matcher(file.getFileName().toString());
                    if (!matcher.matches())
                        throw new IllegalArgumentException("Unexpected file in the plan: " + file);
                    List<String> keep = new ArrayList<>();
                    for (String line : Files.readAllLines(file, StandardCharsets.UTF_8))
                        if (!line.trim().isEmpty())
                            keep.add(line.trim());
                    // the planner replaces ':' with '_' in the file names; endpoints have no '_'
                    String endpoint = matcher.group(2).replace('_', ':');
                    steps.add(new Step(roundNumber(round), dc.getFileName().toString(), Integer.parseInt(matcher.group(1)), endpoint, keep));
                }
            }
        }
        return steps;
    }

    private static int roundNumber(Path round)
    {
        Matcher matcher = ROUND_DIR.matcher(round.getFileName().toString());
        return matcher.matches() ? Integer.parseInt(matcher.group(1)) : -1;
    }

    private static List<Path> list(Path dir) throws IOException
    {
        try (Stream<Path> stream = Files.list(dir))
        {
            return stream.collect(Collectors.toList());
        }
    }

    /**
     * @return the number of steps run
     * @throws IllegalStateException at the first step that can't be run or fails
     */
    public static int run(List<Step> steps, ClusterOperations cluster, Options options, PrintStream out) throws IOException
    {
        int run = 0;
        for (Step step : steps)
        {
            if (options.round != null && step.round != options.round)
                continue;
            if (options.datacenter != null && !step.datacenter.equals(options.datacenter))
                continue;

            Set<String> endpoints = cluster.endpoints();
            String endpoint = resolve(step, endpoints);
            Set<String> current = cluster.tokens(endpoint, endpoint);
            Set<String> keep = new HashSet<>(step.keep);
            if (keep.containsAll(current))
            {
                // the node has these tokens, or a subset of them from a later step of the plan
                out.println(step + ": already done, skipped");
                continue;
            }
            if (!current.containsAll(keep))
                throw new IllegalStateException(step + ": the tokens to keep are not a subset of the " + current.size() + " tokens of the node; the plan doesn't match the ring, plan again");

            checkCluster(step, endpoint, endpoints, cluster);
            if (options.dryRun)
            {
                out.println(step + ": would shrink from " + current.size() + " tokens (dry run)");
                continue;
            }

            out.println(step + ": shrinking from " + current.size() + " tokens");
            long start = nanoTime();
            try
            {
                cluster.shrink(endpoint, step.keep);
            }
            catch (IOException | RuntimeException e)
            {
                throw new IllegalStateException(step + ": nodetool settokens failed: " + e.getMessage(), e);
            }
            awaitTokens(step, endpoint, endpoints, keep, cluster, options);
            if (options.cleanup)
            {
                out.println(step + ": flush and cleanup");
                cluster.flushAndCleanup(endpoint);
            }
            run++;
            out.printf("%s: done in %d s%n", step, TimeUnit.NANOSECONDS.toSeconds(nanoTime() - start));
        }
        out.println(run + " steps run" + (options.dryRun ? " (dry run)" : ""));
        return run;
    }

    /**
     * The ring endpoint of a step: the plan may have been written from {@code nodetool ring} without {@code -pp}, i.e.
     * without ports.
     */
    @VisibleForTesting
    static String resolve(Step step, Set<String> endpoints)
    {
        if (endpoints.contains(step.endpoint))
            return step.endpoint;
        List<String> sameAddress = new ArrayList<>();
        for (String endpoint : endpoints)
            if (JmxCluster.address(endpoint).equals(JmxCluster.address(step.endpoint)))
                sameAddress.add(endpoint);
        if (sameAddress.size() == 1)
            return sameAddress.get(0);
        throw new IllegalStateException(step + (sameAddress.isEmpty() ? ": the node is not in the ring" : ": more than one node has this address, use a plan with ports (nodetool ring -pp): " + sameAddress));
    }

    private static void checkCluster(Step step, String endpoint, Set<String> endpoints, ClusterOperations cluster) throws IOException
    {
        Set<String> down = new HashSet<>(endpoints);
        down.removeAll(cluster.liveEndpoints());
        if (!down.isEmpty())
            throw new IllegalStateException(step + ": nodes are down: " + down);
        Set<String> moving = cluster.endpointsInRangeMovement();
        if (!moving.isEmpty())
            throw new IllegalStateException(step + ": nodes are joining, leaving, moving or shrinking: " + moving);
        String hostId = cluster.hostId(endpoint);
        List<String> withHints = new ArrayList<>();
        for (String node : endpoints)
            if (cluster.hostIdsWithPendingHints(node).contains(hostId))
                withHints.add(node);
        if (!withHints.isEmpty())
            throw new IllegalStateException(step + ": nodes have pending hints for it (they would be applied only on it after it streams its ranges): " + withHints +
                                            "; wait for the hints to be delivered (nodetool listpendinghints)");
    }

    private static void awaitTokens(Step step, String endpoint, Set<String> endpoints, Set<String> keep, ClusterOperations cluster, Options options) throws IOException
    {
        long deadline = nanoTime() + TimeUnit.MILLISECONDS.toNanos(options.waitTimeoutMillis);
        while (true)
        {
            List<String> behind = new ArrayList<>();
            for (String observer : endpoints)
                if (!cluster.tokens(observer, endpoint).equals(keep))
                    behind.add(observer);
            if (behind.isEmpty())
                return;
            if (nanoTime() > deadline)
                throw new IllegalStateException(step + ": these nodes don't see the new tokens yet: " + behind);
            Uninterruptibles.sleepUninterruptibly(options.pollMillis, TimeUnit.MILLISECONDS);
        }
    }

    /** {@link ClusterOperations} through JMX (nodetool). */
    static final class JmxCluster implements ClusterOperations, AutoCloseable
    {
        private final String host;
        private final int jmxPort;
        private final String username;
        private final String password;
        private final Map<String, NodeProbe> probes = new HashMap<>();

        JmxCluster(String host, int jmxPort, String username, String password)
        {
            this.host = host;
            this.jmxPort = jmxPort;
            this.username = username;
            this.password = password;
        }

        private NodeProbe probe(String endpoint) throws IOException
        {
            String address = endpoint == null ? host : address(endpoint);
            NodeProbe probe = probes.get(address);
            if (probe == null)
            {
                probe = username == null ? new NodeProbe(address, jmxPort) : new NodeProbe(address, jmxPort, username, password);
                probes.put(address, probe);
            }
            return probe;
        }

        /** The address of an endpoint without its port, e.g. 10.0.0.1 for 10.0.0.1:7000 or ::1 for [::1]:7000. */
        static String address(String endpoint)
        {
            if (endpoint.startsWith("["))
                return endpoint.substring(1, endpoint.indexOf(']'));
            int colon = endpoint.indexOf(':');
            return colon > 0 && endpoint.indexOf(':', colon + 1) < 0 ? endpoint.substring(0, colon) : endpoint;
        }

        public Set<String> endpoints() throws IOException
        {
            return new HashSet<>(probe(null).getTokenToEndpointMap(true).values());
        }

        public Set<String> liveEndpoints() throws IOException
        {
            return new HashSet<>(probe(null).getLiveNodes(true));
        }

        public Set<String> endpointsInRangeMovement() throws IOException
        {
            NodeProbe probe = probe(null);
            Set<String> moving = new HashSet<>(probe.getJoiningNodes(true));
            moving.addAll(probe.getLeavingNodes(true));
            moving.addAll(probe.getMovingNodes(true));
            return moving;
        }

        public Set<String> tokens(String observer, String endpoint) throws IOException
        {
            try
            {
                return new HashSet<>(probe(observer).getTokens(endpoint));
            }
            catch (java.net.UnknownHostException e)
            {
                throw new IOException(e);
            }
        }

        public String hostId(String endpoint) throws IOException
        {
            for (Map.Entry<String, String> entry : probe(null).getHostIdToEndpointWithPort().entrySet())
                if (entry.getValue().equals(endpoint))
                    return entry.getKey();
            throw new IOException("No host id for " + endpoint);
        }

        public Set<String> hostIdsWithPendingHints(String node) throws IOException
        {
            Set<String> hostIds = new HashSet<>();
            for (Map<String, String> hints : probe(node).listPendingHints())
                hostIds.add(hints.get("host_id"));
            return hostIds;
        }

        public void shrink(String endpoint, List<String> keep) throws IOException
        {
            probe(endpoint).shrinkTokens(keep);
        }

        public void flushAndCleanup(String endpoint) throws IOException
        {
            NodeProbe probe = probe(endpoint);
            try
            {
                for (String keyspace : probe.getNonLocalStrategyKeyspaces())
                {
                    probe.forceKeyspaceFlush(keyspace);
                    probe.forceKeyspaceCleanup(System.out, 0, keyspace);
                }
            }
            catch (InterruptedException | java.util.concurrent.ExecutionException e)
            {
                throw new IOException(e);
            }
        }

        public void close()
        {
            for (NodeProbe probe : probes.values())
            {
                try
                {
                    probe.close();
                }
                catch (IOException e)
                {
                    // closing
                }
            }
        }
    }

    public static void main(String[] args)
    {
        System.exit(run(args, System.out, System.err));
    }

    @VisibleForTesting
    static int run(String[] args, PrintStream out, PrintStream err)
    {
        org.apache.commons.cli.Options cli = new org.apache.commons.cli.Options();
        Option plan = new Option(null, "plan", true, "Directory written by tokenreductionplanner.");
        plan.setRequired(true);
        cli.addOption(plan);
        cli.addOption("h", "host", true, "Node to connect to for the ring (default 127.0.0.1); every node is also reached through JMX on its own address.");
        cli.addOption("p", "port", true, "JMX port of the nodes (default 7199).");
        cli.addOption("u", "username", true, "JMX username.");
        cli.addOption("pw", "password", true, "JMX password.");
        cli.addOption(null, "round", true, "Only run this round.");
        cli.addOption(null, "datacenter", true, "Only run the steps of this datacenter.");
        cli.addOption(null, "dry-run", false, "Only check and print the steps.");
        cli.addOption(null, "no-cleanup", false, "Don't run flush and cleanup after each step.");
        cli.addOption(null, "wait-timeout", true, "Seconds to wait for every node to see the new tokens of a node (default 600).");
        try
        {
            CommandLine cmd = new GnuParser().parse(cli, args, false);
            Options options = new Options();
            options.dryRun = cmd.hasOption("dry-run");
            options.cleanup = !cmd.hasOption("no-cleanup");
            if (cmd.hasOption("round"))
                options.round = Integer.parseInt(cmd.getOptionValue("round"));
            options.datacenter = cmd.getOptionValue("datacenter");
            if (cmd.hasOption("wait-timeout"))
                options.waitTimeoutMillis = TimeUnit.SECONDS.toMillis(Long.parseLong(cmd.getOptionValue("wait-timeout")));
            List<Step> steps = readPlan(new File(cmd.getOptionValue("plan")).toPath());
            try (JmxCluster cluster = new JmxCluster(cmd.getOptionValue("host", "127.0.0.1"), Integer.parseInt(cmd.getOptionValue("port", "7199")),
                                                     cmd.getOptionValue("username"), cmd.getOptionValue("password")))
            {
                run(steps, cluster, options, out);
            }
            return 0;
        }
        catch (ParseException | IllegalArgumentException e)
        {
            err.println(e.getMessage());
            err.println();
            java.io.PrintWriter writer = new java.io.PrintWriter(err);
            new HelpFormatter().printHelp(writer, HelpFormatter.DEFAULT_WIDTH, "tokenreduction-run --plan DIR [--host HOST] [--port JMX_PORT]",
                                          "--\nRuns the steps of a plan written by tokenreductionplanner, one node at a time.\nOptions are:",
                                          cli, HelpFormatter.DEFAULT_LEFT_PAD, HelpFormatter.DEFAULT_DESC_PAD, "");
            writer.flush();
            return 1;
        }
        catch (IllegalStateException e)
        {
            err.println("Stopped: " + e.getMessage());
            err.println("Fix the cause and run the same command again: the steps already done are skipped.");
            return 2;
        }
        catch (IOException e)
        {
            err.println("Stopped: " + e.getMessage());
            return 2;
        }
    }
}
