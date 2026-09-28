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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.dht.tokenallocator.TokenReductionPlanner;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Command line front end of {@link TokenReductionPlanner}: reads the ring of a cluster, plans the reduction of the
 * number of tokens in rounds and writes a report and, for every step, the tokens the node keeps.
 */
public class TokenReductionPlannerTool
{
    private static final String RING = "ring";
    private static final String REPLICATION = "replication";
    private static final String TARGET = "target";
    private static final String FACTOR = "factor";
    private static final String ROUNDS = "rounds";
    private static final String OUTPUT = "output";
    private static final String PARTITIONER = "partitioner";

    private static final Set<String> NODETOOL_STATUSES = ImmutableSet.of("Up", "Down", "?");
    private static final Set<String> NODETOOL_STATES = ImmutableSet.of("Normal", "Joining", "Leaving", "Moving");

    /** Nodes read from a ring description, with their load when the description has it. */
    @VisibleForTesting
    static final class Ring
    {
        final List<TokenReductionPlanner.Node> nodes;
        final Map<String, Long> loads;

        Ring(List<TokenReductionPlanner.Node> nodes, Map<String, Long> loads)
        {
            this.nodes = nodes;
            this.loads = loads;
        }
    }

    public static void main(String[] args)
    {
        Options options = getOptions();
        try
        {
            CommandLine cmd = new GnuParser().parse(options, args, false);
            IPartitioner partitioner = FBUtilities.newPartitioner(cmd.getOptionValue(PARTITIONER, Murmur3Partitioner.class.getSimpleName()));
            Ring ring = parseRing(Files.readAllLines(new File(cmd.getOptionValue(RING)).toPath(), StandardCharsets.UTF_8), partitioner);
            Map<String, Integer> replication = parseReplication(cmd.getOptionValue(REPLICATION));
            int target = Integer.parseInt(cmd.getOptionValue(TARGET));
            List<Integer> rounds;
            if (cmd.hasOption(ROUNDS))
            {
                rounds = new ArrayList<>();
                for (String round : cmd.getOptionValue(ROUNDS).split(","))
                    rounds.add(Integer.parseInt(round.trim()));
                if (rounds.get(rounds.size() - 1) != target)
                    throw new IllegalArgumentException("the last round must be the target (" + target + "): " + rounds);
            }
            else
            {
                int current = 0;
                for (TokenReductionPlanner.Node node : ring.nodes)
                    current = Math.max(current, node.tokens.size());
                rounds = TokenReductionPlanner.rounds(current, target, Double.parseDouble(cmd.getOptionValue(FACTOR, "2")));
            }

            TokenReductionPlanner.Plan plan = TokenReductionPlanner.plan(ring.nodes, replication, rounds);
            Path output = new File(cmd.getOptionValue(OUTPUT)).toPath();
            write(plan, ring, replication, output);
            report(plan, ring, replication, System.out);
            System.out.println();
            System.out.println("Tokens to keep at every step written under " + output.toAbsolutePath());
        }
        catch (ParseException | IllegalArgumentException | IllegalStateException e)
        {
            System.err.println(e.getMessage());
            System.err.println();
            printUsage(options);
            System.exit(1);
        }
        catch (IOException e)
        {
            System.err.println("I/O error: " + e.getMessage());
            System.exit(1);
        }
    }

    /**
     * Parses either the output of {@code nodetool ring} (all datacenters, with or without {@code -pp}) or CSV lines
     * {@code endpoint,datacenter,rack,token}. Empty lines and lines starting with {@code #} are ignored.
     */
    @VisibleForTesting
    static Ring parseRing(List<String> lines, IPartitioner partitioner)
    {
        Map<String, String> datacenters = new LinkedHashMap<>();
        Map<String, String> racks = new LinkedHashMap<>();
        Map<String, List<Token>> tokens = new LinkedHashMap<>();
        Map<String, Long> loads = new TreeMap<>();
        String datacenter = null;
        boolean nodetoolFormat = false;
        for (String raw : lines)
        {
            String line = raw.trim();
            if (line.isEmpty() || line.startsWith("#"))
                continue;
            if (line.startsWith("Datacenter:"))
            {
                datacenter = line.substring("Datacenter:".length()).trim();
                nodetoolFormat = true;
                continue;
            }

            String endpoint, rack, token;
            if (nodetoolFormat)
            {
                // Address Rack Status State Load Owns Token, where Load is "?" or a number and a unit; headers,
                // separators and notes don't have a status and a state
                String[] fields = line.split("\\s+");
                if (fields.length < 7 || !NODETOOL_STATUSES.contains(fields[2]) || !NODETOOL_STATES.contains(fields[3]))
                    continue;
                endpoint = fields[0];
                rack = fields[1];
                token = fields[fields.length - 1];
                if (fields.length == 8)
                    loads.put(endpoint, (long) FileUtils.parseFileSize(fields[4] + ' ' + fields[5]));
            }
            else
            {
                String[] fields = line.split(",");
                if (fields.length != 4)
                    throw new IllegalArgumentException("expected endpoint,datacenter,rack,token but got: " + raw);
                endpoint = fields[0].trim();
                datacenter = fields[1].trim();
                rack = fields[2].trim();
                token = fields[3].trim();
            }
            if (datacenter == null)
                throw new IllegalArgumentException("no datacenter for " + endpoint);

            String previousDc = datacenters.putIfAbsent(endpoint, datacenter);
            String previousRack = racks.putIfAbsent(endpoint, rack);
            if ((previousDc != null && !previousDc.equals(datacenter)) || (previousRack != null && !previousRack.equals(rack)))
                throw new IllegalArgumentException("endpoint " + endpoint + " appears in more than one datacenter or rack");
            tokens.computeIfAbsent(endpoint, e -> new ArrayList<>()).add(partitioner.getTokenFactory().fromString(token));
        }
        if (tokens.isEmpty())
            throw new IllegalArgumentException("no tokens found in the ring description");

        List<TokenReductionPlanner.Node> nodes = new ArrayList<>();
        for (Map.Entry<String, List<Token>> e : tokens.entrySet())
            nodes.add(new TokenReductionPlanner.Node(e.getKey(), datacenters.get(e.getKey()), racks.get(e.getKey()), e.getValue()));
        return new Ring(nodes, loads.size() == nodes.size() ? loads : Collections.emptyMap());
    }

    @VisibleForTesting
    static Map<String, Integer> parseReplication(String value)
    {
        Map<String, Integer> replication = new TreeMap<>();
        for (String entry : value.split(","))
        {
            String[] parts = entry.split(":");
            if (parts.length != 2)
                throw new IllegalArgumentException("expected dc:rf[,dc:rf...] but got: " + value);
            replication.put(parts[0].trim(), Integer.parseInt(parts[1].trim()));
        }
        return replication;
    }

    /**
     * Writes one file per step, {@code round-<n>-<tokens>/<step>-<endpoint>.tokens}, with the tokens the node keeps,
     * one per line, and the report in {@code plan.txt}.
     */
    @VisibleForTesting
    static void write(TokenReductionPlanner.Plan plan, Ring ring, Map<String, Integer> replication, Path output) throws IOException
    {
        Files.createDirectories(output);
        for (int r = 0; r < plan.rounds.size(); r++)
        {
            TokenReductionPlanner.Round round = plan.rounds.get(r);
            Path dir = output.resolve(String.format("round-%d-%d", r + 1, round.targetTokens));
            Files.createDirectories(dir);
            for (int s = 0; s < round.steps.size(); s++)
            {
                TokenReductionPlanner.Step step = round.steps.get(s);
                List<String> keep = new ArrayList<>();
                for (Token token : step.keep)
                    keep.add(token.toString());
                Path file = dir.resolve(String.format("%03d-%s.tokens", s + 1, step.endpoint.replace(':', '_')));
                Files.write(file, keep, StandardCharsets.UTF_8);
            }
        }
        try (PrintStream out = new PrintStream(Files.newOutputStream(output.resolve("plan.txt")), true, StandardCharsets.UTF_8.name()))
        {
            report(plan, ring, replication, out);
        }
    }

    @VisibleForTesting
    static void report(TokenReductionPlanner.Plan plan, Ring ring, Map<String, Integer> replication, PrintStream out)
    {
        List<Integer> targets = new ArrayList<>();
        for (TokenReductionPlanner.Round round : plan.rounds)
            targets.add(round.targetTokens);
        out.printf("Token reduction plan: rounds %s, replication %s%n", targets, replication);
        out.println("Ownership is the replicated ownership (nodetool status \"Owns (effective)\"), relative to the fair share of the datacenter.");
        out.println("Streamed is the data sent by the shrinking node, in copies of the datacenter data set (a new datacenter streams 1.00).");

        for (String dc : plan.initialOwnership.keySet())
        {
            Map<String, Double> initial = plan.initialOwnership.get(dc);
            double fair = total(initial) / initial.size();
            double bytesPerOwnership = bytesPerOwnership(ring.loads, initial);
            double dataSet = total(initial);

            out.printf("%nDatacenter %s: %d nodes, RF %d%n", dc, initial.size(), replication.get(dc));
            out.printf("  initial ownership: %s%n", stats(initial, fair));
            for (int r = 0; r < plan.rounds.size(); r++)
            {
                TokenReductionPlanner.Round round = plan.rounds.get(r);
                out.printf("  round %d, %d tokens per node:%n", r + 1, round.targetTokens);
                double roundPeak = 0;
                double roundStreamed = 0;
                for (int s = 0; s < round.steps.size(); s++)
                {
                    TokenReductionPlanner.Step step = round.steps.get(s);
                    if (!step.datacenter.equals(dc))
                        continue;
                    String max = Collections.max(step.ownershipAfter.entrySet(), Map.Entry.comparingByValue()).getKey();
                    out.printf("    step %3d  %-40s drops %4d tokens, streams %.3f%s; max ownership after %.3f (%s)%n",
                               s + 1, step.endpoint, step.dropped, step.streamedOwnership / dataSet,
                               bytes(step.streamedOwnership, bytesPerOwnership), step.maxOwnershipAfter() / fair, max);
                    roundPeak = Math.max(roundPeak, step.maxOwnershipAfter());
                    roundStreamed += step.streamedOwnership;
                }
                out.printf("    round peak ownership %.3f%s, streamed %.3f%s%n", roundPeak / fair, bytes(roundPeak, bytesPerOwnership),
                           roundStreamed / dataSet, bytes(roundStreamed, bytesPerOwnership));
            }
            Map<String, Double> end = plan.finalOwnership(dc);
            out.printf("  final ownership: %s%n", stats(end, fair));
            out.printf("  peak ownership of any node: %.3f%s%n", plan.peakOwnership(dc) / fair, bytes(plan.peakOwnership(dc), bytesPerOwnership));
            out.printf("  total streamed: %.3f copies of the data set%s (a new datacenter copies 1.00%s)%n",
                       plan.streamedOwnership(dc) / dataSet, bytes(plan.streamedOwnership(dc), bytesPerOwnership), bytes(dataSet, bytesPerOwnership));
        }
    }

    private static double total(Map<String, Double> ownership)
    {
        double total = 0;
        for (double owned : ownership.values())
            total += owned;
        return total;
    }

    private static double bytesPerOwnership(Map<String, Long> loads, Map<String, Double> ownership)
    {
        if (loads.isEmpty())
            return 0;
        double bytes = 0;
        for (String endpoint : ownership.keySet())
            bytes += loads.getOrDefault(endpoint, 0L);
        return bytes / total(ownership);
    }

    private static String bytes(double ownership, double bytesPerOwnership)
    {
        return bytesPerOwnership == 0 ? "" : " (" + FileUtils.stringifyFileSize(ownership * bytesPerOwnership) + ')';
    }

    private static String stats(Map<String, Double> ownership, double fair)
    {
        double min = Collections.min(ownership.values()) / fair;
        double max = Collections.max(ownership.values()) / fair;
        double variance = 0;
        for (double owned : ownership.values())
            variance += Math.pow(owned / fair - 1, 2);
        return String.format("min %.3f, max %.3f, stddev %.3f", min, max, Math.sqrt(variance / ownership.size()));
    }

    private static Options getOptions()
    {
        Options options = new Options();
        options.addOption(requiredOption(null, RING, true, "File with the output of 'nodetool ring', or CSV lines endpoint,datacenter,rack,token."));
        options.addOption(requiredOption(null, REPLICATION, true, "Replication factor to balance for, per datacenter: dc1:3[,dc2:3...]. Use the RF of the keyspaces that hold most of the data."));
        options.addOption(requiredOption(null, TARGET, true, "Number of tokens per node at the end."));
        options.addOption(null, FACTOR, true, "Divide the number of tokens by this factor at every round (default 2).");
        options.addOption(null, ROUNDS, true, "Explicit token counts of the rounds, e.g. 128,64,32,16; overrides --factor.");
        options.addOption(requiredOption(null, OUTPUT, true, "Directory where the plan is written."));
        options.addOption("p", PARTITIONER, true, "Partitioner, Murmur3Partitioner (default) or RandomPartitioner.");
        return options;
    }

    private static Option requiredOption(String shortOpt, String longOpt, boolean hasArg, String description)
    {
        Option option = new Option(shortOpt, longOpt, hasArg, description);
        option.setRequired(true);
        return option;
    }

    private static void printUsage(Options options)
    {
        String usage = "tokenreductionplanner --ring RING --replication DC:RF[,DC:RF] --target TOKENS --output DIR";
        String header = "--\n" +
                        "Plans the in-place reduction of the number of tokens of every node, in rounds.\n" +
                        "Options are:";
        new HelpFormatter().printHelp(usage, header, options, "");
    }
}
