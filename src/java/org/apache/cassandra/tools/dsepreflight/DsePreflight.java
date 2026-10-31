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

package org.apache.cassandra.tools.dsepreflight;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import org.apache.cassandra.config.CassandraRelevantEnv;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JsonUtils;
import org.apache.cassandra.utils.logging.LoggingSupportFactory;

/**
 * {@code dsepreflight}: the preflight inspection of a DataStax Enterprise 6.8/6.9 node before its in-place upgrade to
 * HCD 2.0. Run on every DSE node, DSE running or stopped, from the unpacked HCD 2.0 tarball; it reads the node's files
 * and never writes to them. It reports whether and how the node can be upgraded, and exits with {@value #EXIT_BLOCKED}
 * when it finds a feature that forbids the upgrade. The aggregate mode checks the reports of every node of a cluster
 * together.
 * <p>
 * Exit codes: {@value #EXIT_READY} ready (warnings allowed), {@value #EXIT_BLOCKED} blocking findings (or, in the
 * aggregate mode, a cluster that is not ready), {@value #EXIT_ERROR} wrong usage or an error that stopped the inspection.
 */
public final class DsePreflight
{
    public static final int EXIT_READY = 0;
    public static final int EXIT_BLOCKED = 1;
    public static final int EXIT_ERROR = 2;

    static final long DEFAULT_NODETOOL_TIMEOUT_SECONDS = 120;
    static final String USAGE = "dsepreflight [options]                                 inspect this DSE node\n" +
                                "       dsepreflight --aggregate <directory> [--json <file>]   check the reports of a cluster";
    /** The options the aggregate mode takes; it refuses every other, so that none is silently ignored. */
    static final Set<String> AGGREGATE_OPTIONS = Set.of("aggregate", "json", "help");

    private DsePreflight()
    {
    }

    public static void main(String[] args)
    {
        quietLibraryWarnings();
        int exitCode = run(args, System.out, System.err);
        System.exit(exitCode);
    }

    /**
     * Quiets the loggers whose messages are noise in the output of the tool, where anything that matters to the
     * inspection is a finding: the statistics of a DSE sstable read for the schema can hold a partition size or cell
     * count histogram with values beyond the largest bucket of this software, which it logs with a warning as it clears
     * that bucket, and a transaction log the scan cannot read (finding TXN_LOG_UNREADABLE) is also logged with the whole
     * listing of its directory.
     */
    private static void quietLibraryWarnings()
    {
        quiet(StatsMetadata.class.getName(), "ERROR");
        quiet("org.apache.cassandra.db.lifecycle", "OFF");
    }

    private static void quiet(String logger, String level)
    {
        try
        {
            LoggingSupportFactory.getLoggingSupport().setLoggingLevel(logger, level);
        }
        catch (Exception e)
        {
            // the messages stay: harmless
        }
    }

    /** Runs the tool; returns the exit code. */
    public static int run(String[] args, PrintStream out, PrintStream err)
    {
        Options options = options();
        CommandLine cmd;
        try
        {
            cmd = new DefaultParser().parse(options, args);
        }
        catch (ParseException e)
        {
            err.println(e.getMessage());
            printUsage(options, err);
            return EXIT_ERROR;
        }
        if (cmd.hasOption("help"))
        {
            printUsage(options, out);
            return EXIT_READY;
        }
        if (!cmd.getArgList().isEmpty())
        {
            err.println("unexpected arguments: " + cmd.getArgList());
            printUsage(options, err);
            return EXIT_ERROR;
        }
        try
        {
            // as a client: the configuration of the HCD 2.0 installation the tool runs from is neither read nor needed, and
            // none of its directories is created
            DatabaseDescriptor.clientInitialization(false);
        }
        catch (Throwable e)
        {
            err.println("cannot initialise the tool: " + e);
            return EXIT_ERROR;
        }
        String misuse = misuse(options, cmd);
        if (misuse != null)
        {
            err.println(misuse);
            printUsage(options, err);
            return EXIT_ERROR;
        }
        try
        {
            if (cmd.hasOption("aggregate"))
                return aggregate(cmd, out, err);
            return inspect(cmd, out, err);
        }
        catch (Throwable e)
        {
            // an error too, so that the exit code is never that of blocking findings
            err.println((cmd.hasOption("aggregate") ? "the aggregate failed: " : "the inspection failed: ") + e);
            return EXIT_ERROR;
        }
    }

    private static NodeLayout layout(CommandLine cmd)
    {
        NodeLayout.Options layoutOptions = new NodeLayout.Options();
        layoutOptions.dseHome = path(cmd, "dse-home");
        layoutOptions.confDir = path(cmd, "conf-dir");
        layoutOptions.cassandraYaml = path(cmd, "cassandra-yaml");
        layoutOptions.dseYaml = path(cmd, "dse-yaml");
        layoutOptions.rackdc = path(cmd, "rackdc");
        if (cmd.hasOption("data-dir"))
            for (String dataDirectory : cmd.getOptionValues("data-dir"))
                layoutOptions.dataDirectories.add(Path.of(dataDirectory));
        layoutOptions.metadataDirectory = path(cmd, "metadata-dir");
        layoutOptions.commitLogDirectory = path(cmd, "commitlog-dir");
        layoutOptions.hintsDirectory = path(cmd, "hints-dir");
        layoutOptions.dseHomeFromEnvironment = CassandraRelevantEnv.DSE_HOME.getString();
        return new NodeLayout(layoutOptions);
    }

    private static int inspect(CommandLine cmd, PrintStream out, PrintStream err) throws IOException
    {
        NodeLayout layout = layout(cmd);

        Map<String, NodeReport.LiveCommand> live = null;
        if (cmd.hasOption("nodetool"))
        {
            long timeout = DEFAULT_NODETOOL_TIMEOUT_SECONDS;
            if (cmd.hasOption("nodetool-timeout"))
            {
                try
                {
                    timeout = Long.parseLong(cmd.getOptionValue("nodetool-timeout"));
                }
                catch (NumberFormatException e)
                {
                    timeout = 0;
                }
                if (timeout < 1)
                {
                    err.println("--nodetool-timeout must be a number of seconds, 1 or more: " + cmd.getOptionValue("nodetool-timeout"));
                    return EXIT_ERROR;
                }
            }
            String[] extra = cmd.getOptionValues("nodetool-arg");
            live = LiveState.collect(cmd.getOptionValue("nodetool"), extra == null ? List.of() : List.of(extra), timeout);
        }

        NodeReport report = new NodeInspector(layout, live).inspect(hostname());
        // printed first, so that a report that cannot be written as JSON still leaves the findings with the operator
        printNodeReport(report, out);
        if (cmd.hasOption("json"))
            writeAtomically(Path.of(cmd.getOptionValue("json")), JsonUtils.JSON_OBJECT_PRETTY_WRITER.writeValueAsBytes(report));
        return report.blocked() ? EXIT_BLOCKED : EXIT_READY;
    }

    /** Why the options given do not go together; null when they do. */
    private static String misuse(Options options, CommandLine cmd)
    {
        if (cmd.hasOption("aggregate"))
            for (Option option : options.getOptions())
                if (!AGGREGATE_OPTIONS.contains(option.getLongOpt()) && cmd.hasOption(option.getLongOpt()))
                    return "--" + option.getLongOpt() + " does not apply to --aggregate";
        return null;
    }

    private static int aggregate(CommandLine cmd, PrintStream out, PrintStream err) throws IOException
    {
        Path directory = Path.of(cmd.getOptionValue("aggregate"));
        if (!Files.isDirectory(directory))
        {
            err.println("--aggregate needs a directory of JSON reports: " + directory);
            return EXIT_ERROR;
        }
        Path json = cmd.hasOption("json") ? Path.of(cmd.getOptionValue("json")) : null;
        if (json != null && Files.exists(json, LinkOption.NOFOLLOW_LINKS) && !isAggregateReport(json))
        {
            // a node report given by mistake would be lost
            err.println("--json names an existing file that is not an aggregate report of " + NodeReport.TOOL + ", which it would replace: " + json);
            return EXIT_ERROR;
        }
        ClusterAggregate.AggregateReport aggregate = ClusterAggregate.aggregate(directory);
        aggregate.softwareVersion = FBUtilities.getReleaseVersionString();
        aggregate.generatedAt = FBUtilities.now().toString();
        // printed first, so that a report that cannot be written as JSON still leaves the findings with the operator
        printAggregate(aggregate, out);
        if (json != null)
            writeAtomically(json, JsonUtils.JSON_OBJECT_PRETTY_WRITER.writeValueAsBytes(aggregate));
        return NodeReport.READY.equals(aggregate.result) ? EXIT_READY : EXIT_BLOCKED;
    }

    /** Whether {@code file} is a regular file (not a link) holding an aggregate report of this tool. */
    private static boolean isAggregateReport(Path file)
    {
        try
        {
            if (!Files.isRegularFile(file, LinkOption.NOFOLLOW_LINKS) || Files.size(file) > ClusterAggregate.MAX_REPORT_BYTES)
                return false;
            JsonNode tree = JsonUtils.JSON_OBJECT_MAPPER.readTree(Files.readAllBytes(file));
            return tree != null && NodeReport.TOOL.equals(tree.path("tool").textValue()) && NodeReport.AGGREGATE.equals(tree.path("kind").textValue());
        }
        catch (IOException | RuntimeException e)
        {
            return false;
        }
    }

    static void printNodeReport(NodeReport report, PrintStream out)
    {
        NodeReport.Node node = report.node;
        out.println("DSE 6 preflight inspection for the upgrade to HCD 2.0 (" + NodeReport.TOOL + ' ' + report.softwareVersion + ')');
        out.println("Node: " + (node.broadcastAddress != null ? node.broadcastAddress : report.hostname) + " (host " + report.hostname + ", host id " + node.hostId +
                    ", data center " + node.dataCenter + ", rack " + node.rack + ", cluster " + node.clusterName + ')');
        out.println("DSE release: " + (report.upgradePath.dseRelease == null ? "unknown" : report.upgradePath.dseRelease));
        out.println("Peers in nodes/peers: " + report.peers.size() + (report.peers.stream().anyMatch(p -> p.upgraded) ? " (some already upgraded)" : ""));
        out.println();
        out.println("Result: " + report.result + " (" + report.count(Finding.Severity.BLOCKING) + " blocking, " + report.count(Finding.Severity.WARNING) +
                    " warnings, " + report.count(Finding.Severity.INFO) + " notes)");
        printFindings(report.findings, out);
        out.println();
        out.println("Upgrade path:");
        out.println("  SAI on-disk version to pin: " + (report.upgradePath.saiVersion == null ? "none (release not supported)" : report.upgradePath.saiVersion));
        out.println("  Settings to carry into HCD 2.0:");
        report.upgradePath.settings.forEach((key, value) -> out.println("    " + key + ": " + value));
        out.println("  Upgrade steps for this node:");
        report.upgradePath.upgradeSteps.forEach(step -> out.println("    - " + step));
        if (!report.live.isEmpty())
        {
            out.println();
            out.println("Live state (nodetool):");
            report.live.forEach((command, result) -> out.println("  " + command + ": " + (result.error != null ? result.error : "exit code " + result.exitCode)));
        }
    }

    static void printAggregate(ClusterAggregate.AggregateReport aggregate, PrintStream out)
    {
        out.println("DSE 6 preflight inspection, cluster aggregate (" + NodeReport.TOOL + ' ' + aggregate.softwareVersion + ')');
        out.println("Reports read: " + aggregate.reportsRead + dataCenterCounts(aggregate));
        // the node lines grouped by data center, in the order of the report within each
        for (String dataCenter : aggregate.dataCenters.keySet())
        {
            out.println("  Data center " + dataCenter + ':');
            for (Map<String, Object> node : aggregate.nodes)
                if (dataCenter.equals(node.get("data_center")))
                    out.println("    " + node.get("node") + " (" + node.get("file") + "), rack " + node.get("rack") + ": " + node.get("result") + ", " +
                                node.get("blocking") + " blocking, " + node.get("warnings") + " warnings");
        }
        out.println();
        out.println("Result: " + aggregate.result);
        printFindings(aggregate.findings, out);
        if (!aggregate.warningsByCode.isEmpty())
        {
            out.println();
            out.println("Warnings of the node reports, by code:");
            aggregate.warningsByCode.forEach((code, nodes) -> out.println("  " + code + ": " + NodeInspector.list(nodes)));
        }
    }

    /**
     * {@code , in 2 data centers}, with {@code , 1 with no data center recorded} when some reports have none, or
     * {@code , none with a data center recorded}; nothing without reports. For the line of the reports read.
     */
    private static String dataCenterCounts(ClusterAggregate.AggregateReport aggregate)
    {
        StringBuilder counts = new StringBuilder();
        long known = aggregate.dataCenters.keySet().stream().filter(dc -> !ClusterAggregate.UNKNOWN.equals(dc)).count();
        if (known > 0)
            counts.append(", in ").append(known).append(known == 1 ? " data center" : " data centers");
        Map<String, List<String>> unknown = aggregate.dataCenters.get(ClusterAggregate.UNKNOWN);
        if (unknown != null && known == 0)
            counts.append(", none with a data center recorded");
        else if (unknown != null)
            counts.append(", ").append(unknown.values().stream().mapToInt(List::size).sum()).append(" with no data center recorded");
        return counts.toString();
    }

    private static void printFindings(List<Finding> findings, PrintStream out)
    {
        for (Finding.Severity severity : Finding.Severity.values())
        {
            boolean header = false;
            for (Finding finding : findings)
            {
                if (finding.severity != severity)
                    continue;
                if (!header)
                {
                    out.println();
                    out.println(severity == Finding.Severity.BLOCKING ? "Blocking findings:" : severity == Finding.Severity.WARNING ? "Warnings:" : "Notes:");
                    header = true;
                }
                out.println("  [" + finding.code + "] " + finding.message);
                if (finding.remedy != null)
                    out.println("      What to do: " + finding.remedy);
            }
        }
    }

    /**
     * Writes a report to a new temporary file next to {@code target}, then renames it, so that a report is never left half
     * written. The temporary file is created with a name of its own, readable by its owner only, so that no existing file,
     * or link, of that directory is written through.
     */
    private static void writeAtomically(Path target, byte[] content) throws IOException
    {
        Path absolute = target.toAbsolutePath();
        Path temporary = Files.createTempFile(absolute.getParent(), absolute.getFileName() + ".", ".tmp");
        try
        {
            Files.write(temporary, content);
            Files.move(temporary, absolute, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        }
        catch (IOException | RuntimeException e)
        {
            try
            {
                Files.deleteIfExists(temporary);
            }
            catch (IOException | RuntimeException cleanup)
            {
                e.addSuppressed(cleanup);
            }
            throw e;
        }
    }

    private static Path path(CommandLine cmd, String option)
    {
        return cmd.hasOption(option) ? Path.of(cmd.getOptionValue(option)) : null;
    }

    /** The host name, from the kernel where Linux tells it (no name lookup), else from the local address. */
    @VisibleForTesting
    static String hostname()
    {
        try
        {
            Path kernel = Path.of("/proc/sys/kernel/hostname");
            if (Files.isReadable(kernel))
                return new String(Files.readAllBytes(kernel), StandardCharsets.UTF_8).trim();
            return InetAddress.getLocalHost().getHostName();
        }
        catch (IOException e)
        {
            return "unknown";
        }
    }

    static Options options()
    {
        Options options = new Options();
        options.addOption(Option.builder().longOpt("dse-home").hasArg().argName("dir")
                                .desc("the DSE installation directory (ds_version.txt); default DSE_HOME, else /opt/dse, else /usr/share/dse").build());
        options.addOption(Option.builder().longOpt("conf-dir").hasArg().argName("dir")
                                .desc("DSE's Cassandra configuration directory; default /etc/dse/cassandra, else <dse home>/resources/cassandra/conf").build());
        options.addOption(Option.builder().longOpt("cassandra-yaml").hasArg().argName("file").desc("DSE's cassandra.yaml; default <conf dir>/cassandra.yaml").build());
        options.addOption(Option.builder().longOpt("dse-yaml").hasArg().argName("file")
                                .desc("DSE's dse.yaml; default /etc/dse/dse.yaml, else <dse home>/resources/dse/conf/dse.yaml").build());
        options.addOption(Option.builder().longOpt("rackdc").hasArg().argName("file")
                                .desc("DSE's cassandra-rackdc.properties; default next to cassandra.yaml").build());
        options.addOption(Option.builder().longOpt("data-dir").hasArg().argName("dir")
                                .desc("a data directory, instead of data_file_directories of cassandra.yaml (repeat for each)").build());
        options.addOption(Option.builder().longOpt("metadata-dir").hasArg().argName("dir").desc("instead of metadata_directory of cassandra.yaml").build());
        options.addOption(Option.builder().longOpt("commitlog-dir").hasArg().argName("dir").desc("instead of commitlog_directory of cassandra.yaml").build());
        options.addOption(Option.builder().longOpt("hints-dir").hasArg().argName("dir").desc("instead of hints_directory of cassandra.yaml").build());
        options.addOption(Option.builder().longOpt("nodetool").hasArg().argName("path")
                                .desc("DSE's nodetool, to collect the live state of the running node (read-only commands)").build());
        options.addOption(Option.builder().longOpt("nodetool-arg").hasArg().argName("arg")
                                .desc("an argument passed to every nodetool command before it, written --nodetool-arg=<arg> when it starts with a dash, " +
                                      "such as --nodetool-arg=-h --nodetool-arg=10.0.0.1 (repeat for each); give a password with -pwf <file>, never " +
                                      "with -pw, which the process list shows").build());
        options.addOption(Option.builder().longOpt("nodetool-timeout").hasArg().argName("seconds")
                                .desc("the time limit of each nodetool command; default " + DEFAULT_NODETOOL_TIMEOUT_SECONDS).build());
        options.addOption(Option.builder().longOpt("json").hasArg().argName("file").desc("also write the report as JSON to this file").build());
        options.addOption(Option.builder().longOpt("aggregate").hasArg().argName("directory")
                                .desc("check the JSON reports (*.json) of the nodes of a cluster in this directory, instead of inspecting this node").build());
        options.addOption(Option.builder("h").longOpt("help").desc("print this help").build());
        return options;
    }

    private static void printUsage(Options options, PrintStream stream)
    {
        PrintWriter writer = new PrintWriter(new OutputStreamWriter(stream, StandardCharsets.UTF_8));
        new HelpFormatter().printHelp(writer, 120, USAGE,
                                      "\nInspects a DSE 6.8/6.9 node, read-only, before its upgrade to HCD 2.0. " +
                                      "With --aggregate, checks the JSON reports of the nodes of a cluster together.\n\n",
                                      options, 2, 2,
                                      "\nExit codes: 0 ready (warnings allowed), 1 blocking findings or cluster not ready, 2 usage or inspection error.");
        writer.flush();
    }
}
