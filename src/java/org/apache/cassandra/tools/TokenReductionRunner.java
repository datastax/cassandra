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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.Future;
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

import org.apache.cassandra.concurrent.SequentialExecutorPlus;
import org.apache.cassandra.io.util.File;

import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.config.CassandraRelevantProperties.USER_HOME;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

/**
 * Runs a plan written by {@code tokenreductionplanner}: every step, one node at a time, in the order of the plan.
 * For each step it
 * <ol>
 *     <li>checks where the node is in the plan: a node that already has the tokens of the step only gets the flush and
 *     cleanup of the step again (they may not have run, e.g. if the runner was stopped); a node that has the tokens of
 *     a later step is skipped; a node must otherwise have exactly the tokens its previous step (or the initial ring)
 *     left it with, so that no round is skipped and a plan that doesn't match the ring is refused;</li>
 *     <li>checks that every node is up, none is joining, leaving or moving (shrinking nodes are shown as moving),
 *     and that no node has hints for the node;</li>
 *     <li>runs {@code nodetool settokens} on the node, following the operation through a separate connection, so that
 *     a JMX connection lost during the hours a shrink can take neither hangs the runner nor hides the outcome;</li>
 *     <li>waits until every node sees the new tokens;</li>
 *     <li>runs {@code nodetool flush} and {@code nodetool cleanup} on the node.</li>
 * </ol>
 * It stops at the first failure; running it again resumes. See docs/operations/reduce-num-tokens-runbook.md.
 */
public class TokenReductionRunner
{
    /** What the runner needs from the cluster; endpoints are the addresses of the ring, e.g. 10.0.0.1:7000. */
    public interface ClusterOperations
    {
        /** Every endpoint of the ring, as seen by the node the runner is connected to. */
        Set<String> endpoints() throws IOException;

        Set<String> liveEndpoints() throws IOException;

        /** Endpoints joining, leaving or moving (including shrinking). */
        Set<String> endpointsInRangeMovement() throws IOException;

        /** Tokens of {@code endpoint} as seen by {@code observer}. */
        Set<String> tokens(String observer, String endpoint) throws IOException;

        /** The operation mode of the node (e.g. NORMAL, SHRINKING), through a connection not used by {@link #shrink}. */
        String operationMode(String endpoint) throws IOException;

        String hostId(String endpoint) throws IOException;

        /** Host ids for which {@code node} has pending hints. */
        Set<String> hostIdsWithPendingHints(String node) throws IOException;

        /** Blocks until the shrink is done; may run on another thread than the other calls. */
        void shrink(String endpoint, List<String> keep) throws IOException;

        /** Flushes and cleans up every keyspace of the node, and fails if a cleanup failed. */
        void flushAndCleanup(String endpoint, PrintStream out) throws IOException;
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

    /** The steps of a plan and the tokens every node had when the plan was made. */
    public static final class Plan
    {
        public final List<Step> steps;
        public final Map<String, List<String>> initialTokens;

        public Plan(List<Step> steps, Map<String, List<String>> initialTokens)
        {
            this.steps = steps;
            this.initialTokens = initialTokens;
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
    private static final Pattern INITIAL_FILE = Pattern.compile("(.+)\\.tokens");

    /** Directory of the plan with the tokens of every node when the plan was made. */
    public static final String INITIAL_DIR = "initial";

    /**
     * The name of the file of an endpoint in a plan: the planner replaces ':' with '_' (endpoints have no '_').
     */
    static String fileName(String endpoint)
    {
        return endpoint.replace(':', '_');
    }

    private static String endpointOf(String fileName)
    {
        return fileName.replace('_', ':');
    }

    /**
     * Reads a plan directory: the steps in execution order (rounds, then datacenters, then steps), and the initial
     * tokens of every node.
     */
    public static Plan readPlan(Path plan) throws IOException
    {
        List<Step> steps = new ArrayList<>();
        List<Path> rounds = list(plan).stream()
                                      .filter(p -> Files.isDirectory(p) && ROUND_DIR.matcher(p.getFileName().toString()).matches())
                                      .sorted((a, b) -> Integer.compare(roundNumber(a), roundNumber(b)))
                                      .collect(Collectors.toList());
        Path initial = plan.resolve(INITIAL_DIR);
        if (rounds.isEmpty() || !Files.isDirectory(initial))
            throw new IllegalArgumentException("No round-<n>-<tokens> or initial directory in " + plan + ": not a plan written by tokenreductionplanner");
        for (Path round : rounds)
        {
            for (Path dc : list(round).stream().filter(Files::isDirectory).sorted().collect(Collectors.toList()))
            {
                for (Path file : list(dc).stream().sorted().collect(Collectors.toList()))
                {
                    Matcher matcher = STEP_FILE.matcher(file.getFileName().toString());
                    if (!matcher.matches())
                        throw new IllegalArgumentException("Unexpected file in the plan: " + file);
                    steps.add(new Step(roundNumber(round), dc.getFileName().toString(), Integer.parseInt(matcher.group(1)),
                                       endpointOf(matcher.group(2)), readTokens(file)));
                }
            }
        }
        Map<String, List<String>> initialTokens = new HashMap<>();
        for (Path dc : list(initial).stream().filter(Files::isDirectory).collect(Collectors.toList()))
        {
            for (Path file : list(dc))
            {
                Matcher matcher = INITIAL_FILE.matcher(file.getFileName().toString());
                if (!matcher.matches())
                    throw new IllegalArgumentException("Unexpected file in the plan: " + file);
                initialTokens.put(endpointOf(matcher.group(1)), readTokens(file));
            }
        }
        for (Step step : steps)
            if (!initialTokens.containsKey(step.endpoint))
                throw new IllegalArgumentException("No initial tokens in the plan for " + step.endpoint);
        return new Plan(steps, initialTokens);
    }

    private static List<String> readTokens(Path file) throws IOException
    {
        List<String> tokens = new ArrayList<>();
        for (String line : Files.readAllLines(file, StandardCharsets.UTF_8))
            if (!line.trim().isEmpty())
                tokens.add(line.trim());
        return tokens;
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
     * @return the number of shrinks run
     * @throws IllegalStateException at the first step that can't be run or fails
     */
    public static int run(Plan plan, ClusterOperations cluster, Options options, PrintStream out) throws IOException
    {
        int run = 0;
        // in a dry run, the tokens the nodes would have after the steps already checked
        Map<String, Set<String>> dryRunTokens = new HashMap<>();
        for (int i = 0; i < plan.steps.size(); i++)
        {
            Step step = plan.steps.get(i);
            if (options.round != null && step.round != options.round)
                continue;
            if (options.datacenter != null && !step.datacenter.equals(options.datacenter))
                continue;

            Set<String> endpoints = cluster.endpoints();
            String endpoint = resolve(step, endpoints);
            Set<String> current = dryRunTokens.containsKey(endpoint) ? dryRunTokens.get(endpoint) : cluster.tokens(endpoint, endpoint);
            Set<String> keep = new HashSet<>(step.keep);

            if (current.equals(keep))
            {
                // done, but its flush and cleanup may not have run (e.g. the runner stopped): they are idempotent
                if (options.cleanup && !options.dryRun)
                {
                    out.println(step + ": already done, running flush and cleanup again");
                    cluster.flushAndCleanup(endpoint, out);
                }
                else
                {
                    out.println(step + ": already done, skipped");
                }
                continue;
            }
            if (isLaterStepOf(plan, i, current))
            {
                out.println(step + ": the node is already at a later step of the plan, skipped");
                continue;
            }
            Set<String> expected = new HashSet<>(previousTokens(plan, i));
            if (!current.equals(expected))
                throw new IllegalStateException(step + ": the node has " + current.size() + " tokens, not the " + expected.size() +
                                                " tokens its previous step leaves it with: the ring changed since the plan was made, " +
                                                "or a round of the plan was skipped");

            checkCluster(step, endpoint, endpoints, cluster);
            if (options.dryRun)
            {
                out.println(step + ": would shrink from " + current.size() + " tokens (dry run)");
                dryRunTokens.put(endpoint, keep);
                continue;
            }

            out.println(step + ": shrinking from " + current.size() + " tokens");
            long start = nanoTime();
            shrink(step, endpoint, current, keep, cluster, options, out);
            awaitTokens(step, endpoint, endpoints, keep, cluster, options);
            if (options.cleanup)
            {
                out.println(step + ": flush and cleanup");
                cluster.flushAndCleanup(endpoint, out);
            }
            run++;
            out.printf("%s: done in %d s%n", step, TimeUnit.NANOSECONDS.toSeconds(nanoTime() - start));
        }
        out.println(run + " shrinks run" + (options.dryRun ? " (dry run)" : ""));
        return run;
    }

    /** The tokens the node of step {@code i} has before the step: those of its previous step, or its initial tokens. */
    private static List<String> previousTokens(Plan plan, int i)
    {
        Step step = plan.steps.get(i);
        for (int j = i - 1; j >= 0; j--)
            if (plan.steps.get(j).endpoint.equals(step.endpoint))
                return plan.steps.get(j).keep;
        return plan.initialTokens.get(step.endpoint);
    }

    private static boolean isLaterStepOf(Plan plan, int i, Set<String> current)
    {
        Step step = plan.steps.get(i);
        for (int j = i + 1; j < plan.steps.size(); j++)
            if (plan.steps.get(j).endpoint.equals(step.endpoint) && new HashSet<>(plan.steps.get(j).keep).equals(current))
                return true;
        return false;
    }

    /**
     * Runs the shrink on another thread, and follows its outcome on the node itself: the call can last for hours and
     * its connection can be lost while the shrink goes on.
     */
    private static void shrink(Step step, String endpoint, Set<String> before, Set<String> keep, ClusterOperations cluster, Options options, PrintStream out)
    {
        SequentialExecutorPlus executor = executorFactory().sequential("settokens-" + endpoint);
        try
        {
            Future<?> call = executor.submit(() -> {
                cluster.shrink(endpoint, step.keep);
                return null;
            });
            boolean callFailed = false;
            Throwable callError = null;
            while (true)
            {
                if (!callFailed && call.isDone())
                {
                    try
                    {
                        call.get();
                        return; // completed
                    }
                    catch (Exception e)
                    {
                        callFailed = true;
                        callError = e.getCause() != null ? e.getCause() : e;
                        out.println(step + ": nodetool settokens returned an error (" + callError.getMessage() + "), checking the node");
                    }
                }
                if (callFailed)
                {
                    String mode;
                    Set<String> tokens;
                    try
                    {
                        mode = cluster.operationMode(endpoint);
                        tokens = cluster.tokens(endpoint, endpoint);
                    }
                    catch (IOException | RuntimeException e)
                    {
                        throw new IllegalStateException(step + ": nodetool settokens failed (" + callError.getMessage() + ") and the node can't be reached (" +
                                                        e.getMessage() + "); check the node, then run again", callError);
                    }
                    if (tokens.equals(keep) && !"SHRINKING".equals(mode))
                    {
                        out.println(step + ": the node has its new tokens, the error was only the connection");
                        return;
                    }
                    if (!"SHRINKING".equals(mode))
                        throw new IllegalStateException(step + ": nodetool settokens failed: " + callError.getMessage() +
                                                        (tokens.equals(before) ? " (the node kept its tokens)" : ""), callError);
                    // still shrinking: the connection of the call was lost, keep following the node
                }
                Uninterruptibles.sleepUninterruptibly(options.pollMillis, TimeUnit.MILLISECONDS);
            }
        }
        finally
        {
            executor.shutdownNow();
        }
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
                throw new IllegalStateException(step + ": these nodes don't see the new tokens yet: " + behind + "; the step is done, run again to flush and clean up the node");
            Uninterruptibles.sleepUninterruptibly(options.pollMillis, TimeUnit.MILLISECONDS);
        }
    }

    /** {@link ClusterOperations} through JMX (nodetool). */
    static final class JmxCluster implements ClusterOperations, AutoCloseable
    {
        private final String host;
        private final int jmxPort;
        private final Map<String, String> jmxAddresses;
        private final String username;
        private final String password;
        private final Map<String, NodeProbe> probes = new HashMap<>();

        /**
         * @param jmxAddresses JMX host:port of endpoints, for the endpoints whose JMX isn't their address and
         *                     {@code jmxPort}
         */
        JmxCluster(String host, int jmxPort, Map<String, String> jmxAddresses, String username, String password)
        {
            this.host = host;
            this.jmxPort = jmxPort;
            this.jmxAddresses = jmxAddresses;
            this.username = username;
            this.password = password;
        }

        /** The JMX host:port of an endpoint, or of the node given with --host for null. */
        String jmxAddress(String endpoint)
        {
            if (endpoint == null)
                return bracket(host) + ':' + jmxPort;
            String mapped = jmxAddresses.get(endpoint);
            if (mapped == null)
                mapped = jmxAddresses.get(address(endpoint));
            return mapped != null ? mapped : bracket(address(endpoint)) + ':' + jmxPort;
        }

        private static String bracket(String address)
        {
            return address.contains(":") && !address.startsWith("[") ? '[' + address + ']' : address;
        }

        /**
         * @param purpose probes are per JMX address and purpose, so that the long settokens call has its own connection
         */
        private synchronized NodeProbe probe(String endpoint, String purpose) throws IOException
        {
            String jmx = jmxAddress(endpoint);
            String key = purpose + '@' + jmx;
            NodeProbe probe = probes.get(key);
            if (probe == null)
            {
                int colon = jmx.lastIndexOf(':');
                String jmxHost = jmx.substring(0, colon);
                if (jmxHost.startsWith("["))
                    jmxHost = jmxHost.substring(1, jmxHost.length() - 1);
                int port = Integer.parseInt(jmx.substring(colon + 1));
                probe = username == null ? new NodeProbe(jmxHost, port) : new NodeProbe(jmxHost, port, username, password);
                probes.put(key, probe);
            }
            return probe;
        }

        /** Runs an operation, reconnecting once if the cached connection was lost (e.g. the node restarted). */
        private <T> T withProbe(String endpoint, String purpose, ProbeCall<T> call) throws IOException
        {
            try
            {
                return call.call(probe(endpoint, purpose));
            }
            catch (IOException | RuntimeException e)
            {
                synchronized (this)
                {
                    closeQuietly(probes.remove(purpose + '@' + jmxAddress(endpoint)));
                }
                return call.call(probe(endpoint, purpose));
            }
        }

        private interface ProbeCall<T>
        {
            T call(NodeProbe probe) throws IOException;
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
            return withProbe(null, "ring", probe -> new HashSet<>(probe.getTokenToEndpointMap(true).values()));
        }

        public Set<String> liveEndpoints() throws IOException
        {
            return withProbe(null, "ring", probe -> new HashSet<>(probe.getLiveNodes(true)));
        }

        public Set<String> endpointsInRangeMovement() throws IOException
        {
            return withProbe(null, "ring", probe -> {
                Set<String> moving = new HashSet<>(probe.getJoiningNodes(true));
                moving.addAll(probe.getLeavingNodes(true));
                moving.addAll(probe.getMovingNodes(true));
                return moving;
            });
        }

        public Set<String> tokens(String observer, String endpoint) throws IOException
        {
            return withProbe(observer, "ring", probe -> new HashSet<>(probe.getTokens(endpoint)));
        }

        public String operationMode(String endpoint) throws IOException
        {
            return withProbe(endpoint, "ring", NodeProbe::getOperationMode);
        }

        public String hostId(String endpoint) throws IOException
        {
            return withProbe(null, "ring", probe -> {
                for (Map.Entry<String, String> entry : probe.getHostIdToEndpointWithPort().entrySet())
                    if (entry.getValue().equals(endpoint))
                        return entry.getKey();
                throw new IOException("No host id for " + endpoint);
            });
        }

        public Set<String> hostIdsWithPendingHints(String node) throws IOException
        {
            return withProbe(node, "ring", probe -> {
                Set<String> hostIds = new HashSet<>();
                for (Map<String, String> hints : probe.listPendingHints())
                    hostIds.add(hints.get("host_id"));
                return hostIds;
            });
        }

        public void shrink(String endpoint, List<String> keep) throws IOException
        {
            // no retry: the runner follows the node if this connection is lost
            probe(endpoint, "settokens").shrinkTokens(keep);
        }

        public void flushAndCleanup(String endpoint, PrintStream out) throws IOException
        {
            NodeProbe probe = probe(endpoint, "cleanup");
            try
            {
                for (String keyspace : probe.getNonLocalStrategyKeyspaces())
                {
                    probe.forceKeyspaceFlush(keyspace);
                    probe.forceKeyspaceCleanup(out, 0, keyspace);
                    if (probe.isFailed())
                        throw new IOException("The cleanup of keyspace " + keyspace + " failed on " + endpoint);
                }
            }
            catch (InterruptedException | java.util.concurrent.ExecutionException e)
            {
                throw new IOException(e);
            }
            finally
            {
                // a probe that failed stays failed: use a new one next time
                synchronized (this)
                {
                    closeQuietly(probes.remove("cleanup@" + jmxAddress(endpoint)));
                }
            }
        }

        private static void closeQuietly(NodeProbe probe)
        {
            if (probe == null)
                return;
            try
            {
                probe.close();
            }
            catch (IOException | RuntimeException e)
            {
                // closing
            }
        }

        public synchronized void close()
        {
            for (NodeProbe probe : probes.values())
                closeQuietly(probe);
            probes.clear();
        }
    }

    /**
     * Reads a JMX address file: lines {@code <endpoint or address> <jmx host>:<jmx port>}.
     */
    @VisibleForTesting
    static Map<String, String> readJmxAddresses(Path file) throws IOException
    {
        Map<String, String> addresses = new HashMap<>();
        for (String raw : Files.readAllLines(file, StandardCharsets.UTF_8))
        {
            String line = raw.trim();
            if (line.isEmpty() || line.startsWith("#"))
                continue;
            String[] fields = line.split("\\s+");
            if (fields.length != 2 || fields[1].lastIndexOf(':') < 0)
                throw new IllegalArgumentException("expected '<endpoint> <jmx host>:<jmx port>' but got: " + raw);
            addresses.put(fields[0], fields[1]);
        }
        return addresses;
    }

    /** The password of {@code username} in a JMX password file, as nodetool reads it. */
    @VisibleForTesting
    static String readPassword(String username, String passwordFile)
    {
        try (Scanner scanner = new Scanner(new File(passwordFile).toJavaIOFile()).useDelimiter("\\s+"))
        {
            while (scanner.hasNextLine())
            {
                if (scanner.hasNext())
                {
                    String role = scanner.next();
                    if (role.equals(username) && scanner.hasNext())
                        return scanner.next();
                }
                scanner.nextLine();
            }
        }
        catch (FileNotFoundException e)
        {
            throw new IllegalArgumentException("Cannot read the password file " + passwordFile);
        }
        throw new IllegalArgumentException("No password for " + username + " in " + passwordFile);
    }

    public static void main(String[] args)
    {
        System.exit(run(args, System.out, System.err));
    }

    /** Exit code of a usage error. */
    public static final int USAGE_ERROR = 1;
    /** Exit code of a stop: a step failed or can't run, or the plan can't be used. */
    public static final int STOPPED = 2;

    /**
     * @return the exit code: 0, {@link #USAGE_ERROR} or {@link #STOPPED}
     */
    public static int run(String[] args, PrintStream out, PrintStream err)
    {
        org.apache.commons.cli.Options cli = new org.apache.commons.cli.Options();
        Option plan = new Option(null, "plan", true, "Directory written by tokenreductionplanner.");
        plan.setRequired(true);
        cli.addOption(plan);
        cli.addOption("h", "host", true, "Node to connect to for the ring (default 127.0.0.1); every node is also reached through JMX on its own address.");
        cli.addOption("p", "port", true, "JMX port of the nodes (default 7199).");
        cli.addOption(null, "jmx-addresses", true, "File with lines '<endpoint> <jmx host>:<jmx port>' for nodes whose JMX is not on their address and --port.");
        cli.addOption("u", "username", true, "JMX username.");
        cli.addOption("pw", "password", true, "JMX password (visible in the process list: prefer --password-file).");
        cli.addOption("pwf", "password-file", true, "JMX password file, as for nodetool (default ~/.cassandra/jmxremote.password when a username is given).");
        cli.addOption(null, "round", true, "Only run this round.");
        cli.addOption(null, "datacenter", true, "Only run the steps of this datacenter.");
        cli.addOption(null, "dry-run", false, "Only check and print the steps.");
        cli.addOption(null, "no-cleanup", false, "Don't run flush and cleanup after each step.");
        cli.addOption(null, "wait-timeout", true, "Seconds to wait for every node to see the new tokens of a node (default 600).");
        CommandLine cmd;
        Options options = new Options();
        String username;
        String password;
        Map<String, String> jmxAddresses = new HashMap<>();
        try
        {
            cmd = new GnuParser().parse(cli, args, false);
            options.dryRun = cmd.hasOption("dry-run");
            options.cleanup = !cmd.hasOption("no-cleanup");
            if (cmd.hasOption("round"))
                options.round = Integer.parseInt(cmd.getOptionValue("round"));
            options.datacenter = cmd.getOptionValue("datacenter");
            if (cmd.hasOption("wait-timeout"))
                options.waitTimeoutMillis = TimeUnit.SECONDS.toMillis(Long.parseLong(cmd.getOptionValue("wait-timeout")));
            username = cmd.getOptionValue("username");
            password = cmd.getOptionValue("password");
            if (username == null && (password != null || cmd.hasOption("password-file")))
                throw new IllegalArgumentException("A password needs a --username");
            if (username != null && password == null)
                password = readPassword(username, cmd.getOptionValue("password-file", USER_HOME.getString() + "/.cassandra/jmxremote.password"));
            if (cmd.hasOption("jmx-addresses"))
                jmxAddresses = readJmxAddresses(new File(cmd.getOptionValue("jmx-addresses")).toPath());
        }
        catch (ParseException | IllegalArgumentException | IOException e)
        {
            err.println(e.getMessage());
            err.println();
            PrintWriter writer = new PrintWriter(err);
            new HelpFormatter().printHelp(writer, HelpFormatter.DEFAULT_WIDTH, "tokenreduction-run --plan DIR [--host HOST] [--port JMX_PORT] [--ssl]",
                                          "--\nRuns the steps of a plan written by tokenreductionplanner, one node at a time.\nOptions are:",
                                          cli, HelpFormatter.DEFAULT_LEFT_PAD, HelpFormatter.DEFAULT_DESC_PAD, "");
            writer.flush();
            return USAGE_ERROR;
        }

        try (JmxCluster cluster = new JmxCluster(cmd.getOptionValue("host", "127.0.0.1"), Integer.parseInt(cmd.getOptionValue("port", "7199")),
                                                 jmxAddresses, username, password))
        {
            run(readPlan(new File(cmd.getOptionValue("plan")).toPath()), cluster, options, out);
            return 0;
        }
        catch (IllegalStateException e)
        {
            err.println("Stopped: " + e.getMessage());
            err.println("Fix the cause and run the same command again: the steps already done are skipped.");
            return STOPPED;
        }
        catch (Exception e)
        {
            err.println("Stopped: " + e);
            return STOPPED;
        }
    }
}
