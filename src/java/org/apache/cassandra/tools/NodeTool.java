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
package org.apache.cassandra.tools;

import static com.google.common.base.Throwables.getStackTraceAsString;
import static com.google.common.collect.Iterables.toArray;
import static com.google.common.collect.Lists.newArrayList;
import static java.lang.Integer.parseInt;
import static java.lang.String.format;
import static org.apache.cassandra.config.CassandraRelevantProperties.CUSTOM_NODETOOL_EXTENSION_CLASSES;
import static org.apache.cassandra.io.util.File.WriteMode.APPEND;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_STRING_ARRAY;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;

import java.io.Console;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileWriter;
import java.io.FileNotFoundException;
import java.io.IOError;
import java.io.IOException;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.Set;
import java.util.SortedMap;

import javax.annotation.Nullable;
import javax.management.InstanceNotFoundException;

import com.google.common.base.Joiner;
import com.google.common.base.Throwables;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.EndpointSnitchInfoMBean;
import org.apache.cassandra.tools.nodetool.*;
import org.apache.cassandra.tools.nodetool.GuardrailsConfigCommand.GetGuardrailsConfig;
import org.apache.cassandra.tools.nodetool.GuardrailsConfigCommand.SetGuardrailsConfig;
import org.apache.cassandra.utils.FBUtilities;

import com.google.common.collect.Maps;

import io.airlift.airline.Cli;
import io.airlift.airline.Command;
import io.airlift.airline.Help;
import io.airlift.airline.Option;
import io.airlift.airline.OptionType;
import io.airlift.airline.ParseArgumentsMissingException;
import io.airlift.airline.ParseArgumentsUnexpectedException;
import io.airlift.airline.ParseCommandMissingException;
import io.airlift.airline.ParseCommandUnrecognizedException;
import io.airlift.airline.ParseOptionConversionException;
import io.airlift.airline.ParseOptionMissingException;
import io.airlift.airline.ParseOptionMissingValueException;
import io.airlift.airline.model.CommandGroupMetadata;
import io.airlift.airline.model.CommandMetadata;
import io.airlift.airline.model.GlobalMetadata;

public class NodeTool
{
    static
    {
        FBUtilities.preventIllegalAccessWarnings();
    }

    private static final String HISTORYFILE = "nodetool.history";

    private final INodeProbeFactory nodeProbeFactory;
    private final Output output;

    public static void main(String... args)
    {
        System.exit(new NodeTool(new NodeProbeFactory(), Output.CONSOLE).execute(args));
    }

    public NodeTool(INodeProbeFactory nodeProbeFactory, Output output)
    {
        this.nodeProbeFactory = nodeProbeFactory;
        this.output = output;
    }

    public int execute(String... args)
    {
        Cli<NodeToolCmdRunnable> parser = buildParser(loadExtensions());

        int status = 0;
        try
        {
            NodeToolCmdRunnable parse = parser.parse(args);
            printHistory(args);
            parse.run(nodeProbeFactory, output);
        } catch (IllegalArgumentException |
                IllegalStateException |
                ParseArgumentsMissingException |
                ParseArgumentsUnexpectedException |
                ParseOptionConversionException |
                ParseOptionMissingException |
                ParseOptionMissingValueException |
                ParseCommandMissingException |
                ParseCommandUnrecognizedException e)
        {
            badUse(e);
            status = 1;
        } catch (Throwable throwable)
        {
            err(Throwables.getRootCause(throwable));
            status = 2;
        }

        return status;
    }

    /**
     * @return a builder holding the built-in commands and groups
     */
    private static Cli.CliBuilder<NodeToolCmdRunnable> builtinCommands()
    {
        List<Class<? extends NodeToolCmdRunnable>> commands = newArrayList(
                Assassinate.class,
                CassHelp.class,
                CIDRFilteringStats.class,
                CheckTokenMetadata.class,
                Cleanup.class,
                ClearSnapshot.class,
                ClientStats.class,
                Compact.class,
                CompactionHistory.class,
                CompactionStats.class,
                CreateSystemKey.class,
                DataPaths.class,
                Decommission.class,
                DescribeCluster.class,
                DescribeRing.class,
                DisableAuditLog.class,
                DisableAutoCompaction.class,
                DisableBackup.class,
                DisableBinary.class,
                DisableFullQueryLog.class,
                DisableGossip.class,
                DisableHandoff.class,
                DisableHintsForDC.class,
                DisableOldProtocolVersions.class,
                Drain.class,
                DropCIDRGroup.class,
                EnableAuditLog.class,
                EnableAutoCompaction.class,
                EnableBackup.class,
                EnableBinary.class,
                EnableFullQueryLog.class,
                EnableGossip.class,
                EnableHandoff.class,
                EnableHintsForDC.class,
                EnableOldProtocolVersions.class,
                FailureDetectorInfo.class,
                Flush.class,
                GarbageCollect.class,
                GcStats.class,
                GetAuditLog.class,
                GetAuthCacheConfig.class,
                GetBatchlogReplayTrottle.class,
                GetCIDRGroupsOfIP.class,
                GetColumnIndexSize.class,
                GetCompactionThreshold.class,
                GetCompactionThroughput.class,
                GetConcurrency.class,
                GetConcurrentCompactors.class,
                GetConcurrentViewBuilders.class,
                GetDefaultKeyspaceRF.class,
                GetEndpoints.class,
                GetFullQueryLog.class,
                GetGuardrailsConfig.class,
                GetInterDCStreamThroughput.class,
                GetLoggingLevels.class,
                GetMaxHintWindow.class,
                GetSSTables.class,
                GetSeeds.class,
                GetSnapshotThrottle.class,
                GetStreamThroughput.class,
                GetTimeout.class,
                GetTraceProbability.class,
                GossipInfo.class,
                Import.class,
                Info.class,
                InvalidateCIDRPermissionsCache.class,
                InvalidateCounterCache.class,
                InvalidateCredentialsCache.class,
                InvalidateJmxPermissionsCache.class,
                ReloadCIDRGroupsCache.class,
                InvalidateKeyCache.class,
                InvalidateNetworkPermissionsCache.class,
                InvalidatePermissionsCache.class,
                InvalidateRolesCache.class,
                InvalidateRowCache.class,
                Join.class,
                ListCIDRGroups.class,
                ListPendingHints.class,
                ListSnapshots.class,
                Move.class,
                NetStats.class,
                PauseHandoff.class,
                ProfileLoad.class,
                ProxyHistograms.class,
                RangeKeySample.class,
                Rebuild.class,
                RebuildIndex.class,
                RecompressSSTables.class,
                Refresh.class,
                RefreshSizeEstimates.class,
                ReloadLocalSchema.class,
                ReloadSeeds.class,
                ReloadSslCertificates.class,
                ReloadTriggers.class,
                RelocateSSTables.class,
                RemoveNode.class,
                Repair.class,
                ReplayBatchlog.class,
                ResetFullQueryLog.class,
                ResetLocalSchema.class,
                ResumeHandoff.class,
                Ring.class,
                Scrub.class,
                SetAuthCacheConfig.class,
                SetBatchlogReplayThrottle.class,
                SetCacheCapacity.class,
                SetCacheKeysToSave.class,
                SetColumnIndexSize.class,
                SetCompactionThreshold.class,
                SetCompactionThroughput.class,
                SetConcurrency.class,
                SetConcurrentCompactors.class,
                SetConcurrentViewBuilders.class,
                SetDefaultKeyspaceRF.class,
                SetGuardrailsConfig.class,
                SetHintedHandoffThrottleInKB.class,
                SetInterDCStreamThroughput.class,
                SetLoggingLevel.class,
                SetMaxHintWindow.class,
                SetSnapshotThrottle.class,
                SetStreamThroughput.class,
                SetTimeout.class,
                SetTokens.class,
                SetTraceProbability.class,
                Sjk.class,
                Snapshot.class,
                Status.class,
                StatusAutoCompaction.class,
                StatusBackup.class,
                StatusBinary.class,
                StatusGossip.class,
                StatusHandoff.class,
                Stop.class,
                StopDaemon.class,
                TableHistograms.class,
                TableStats.class,
                TopPartitions.class,
                TpStats.class,
                TruncateHints.class,
                UpdateCIDRGroup.class,
                UpgradeSSTable.class,
                Verify.class,
                Version.class,
                ViewBuildStatus.class,
                ForceCompact.class
        );

        Cli.CliBuilder<NodeToolCmdRunnable> builder = Cli.builder("nodetool");

        builder.withDescription("Manage your Cassandra cluster")
                 .withDefaultCommand(CassHelp.class)
                 .withCommands(commands);

        // bootstrap commands
        builder.withGroup("bootstrap")
                .withDescription("Monitor/manage node's bootstrap process")
                .withDefaultCommand(CassHelp.class)
                .withCommand(BootstrapResume.class);

        builder.withGroup("repair_admin")
               .withDescription("list and fail incremental repair sessions")
               .withDefaultCommand(RepairAdmin.ListCmd.class)
               .withCommand(RepairAdmin.ListCmd.class)
               .withCommand(RepairAdmin.CancelCmd.class)
               .withCommand(RepairAdmin.CleanupDataCmd.class)
               .withCommand(RepairAdmin.SummarizePendingCmd.class)
               .withCommand(RepairAdmin.SummarizeRepairedCmd.class);

        return builder;
    }

    /**
     * Instantiates the {@link NodeToolExtension}s listed in {@link CassandraRelevantProperties#CUSTOM_NODETOOL_EXTENSION_CLASSES},
     * in the order they are listed. An extension that cannot be instantiated, or is listed twice, is skipped with a
     * warning on the error output: a broken extension must never take the built-in commands down.
     */
    private List<NodeToolExtension> loadExtensions()
    {
        List<NodeToolExtension> extensions = new ArrayList<>();
        Set<String> loaded = new HashSet<>();
        for (String classname : FBUtilities.splitCommaSeparated(CUSTOM_NODETOOL_EXTENSION_CLASSES.getString()))
        {
            if (!loaded.add(classname))
            {
                warnIgnoringExtension(classname, "listed twice in " + CUSTOM_NODETOOL_EXTENSION_CLASSES.getKey(), null);
                continue;
            }
            try
            {
                extensions.add(FBUtilities.construct(classname, NodeToolExtension.class,
                                                     "nodetool extension (set with " + CUSTOM_NODETOOL_EXTENSION_CLASSES.getKey() + ')'));
            }
            catch (RuntimeException | LinkageError e)
            {
                warnIgnoringExtension(classname, e);
            }
        }
        return extensions;
    }

    /**
     * Builds the command parser: the built-in commands and groups, plus the commands and groups of the given extensions.
     * <p>
     * The extensions are added one at a time, each on top of the ones accepted so far, and the parser is rebuilt
     * every time: an extension that cannot be added (a name clashing with a built-in command or group, with another
     * extension or within the extension, a class not annotated with {@link Command @Command}, an invalid name, or
     * command definitions airline rejects, for instance a global option conflicting with the built-in ones or a
     * field whose type is missing from the classpath) is skipped alone, with a warning on the error output.
     */
    private Cli<NodeToolCmdRunnable> buildParser(List<NodeToolExtension> extensions)
    {
        Cli<NodeToolCmdRunnable> parser = builtinCommands().build();
        if (extensions.isEmpty())
            return parser;

        // commands and groups share the top-level namespace: the first argument of nodetool selects one or the other
        Set<String> builtinNames = new HashSet<>();
        GlobalMetadata metadata = parser.getMetadata();
        for (CommandMetadata command : metadata.getDefaultGroupCommands())
            builtinNames.add(command.getName());
        for (CommandGroupMetadata group : metadata.getCommandGroups())
            builtinNames.add(group.getName());

        List<NodeToolExtension> accepted = new ArrayList<>();
        for (NodeToolExtension extension : extensions)
        {
            try
            {
                Cli.CliBuilder<NodeToolCmdRunnable> builder = builtinCommands();
                Set<String> topLevelNames = new HashSet<>(builtinNames);
                for (NodeToolExtension acceptedExtension : accepted)
                    addExtension(builder, topLevelNames, acceptedExtension);
                addExtension(builder, topLevelNames, extension);
                parser = builder.build();
                accepted.add(extension);
            }
            catch (RuntimeException | LinkageError e)
            {
                warnIgnoringExtension(extension.getClass().getName(), e);
            }
        }
        return parser;
    }

    private void warnIgnoringExtension(String extension, Throwable cause)
    {
        warnIgnoringExtension(extension, cause.getMessage() != null ? cause.getMessage() : cause.toString(), cause);
    }

    private void warnIgnoringExtension(String extension, String message, @Nullable Throwable cause)
    {
        StringBuilder warning = new StringBuilder("warning: ignoring nodetool extension ").append(extension).append(": ").append(message);
        Throwable rootCause = cause == null ? null : Throwables.getRootCause(cause);
        if (rootCause != null && rootCause != cause)
            warning.append(" (caused by ").append(rootCause).append(')');
        output.err.println(warning);
    }

    /**
     * Adds the commands and groups of an extension to the builder, after validating all of them against the names
     * already taken.
     *
     * @param topLevelNames the names of the top-level commands and groups registered so far; the ones of the extension
     *                      are added to it
     * @throws ConfigurationException if the extension cannot be added, with the reason
     */
    private static void addExtension(Cli.CliBuilder<NodeToolCmdRunnable> builder, Set<String> topLevelNames, NodeToolExtension extension)
    {
        List<Class<? extends NodeToolCmdRunnable>> commands = extension.commands();
        List<NodeToolExtension.CommandGroup> groups = extension.commandGroups();
        if (commands == null || groups == null)
            throw new ConfigurationException("it returned a null list of commands");

        Set<String> names = new HashSet<>();
        for (Class<? extends NodeToolCmdRunnable> command : commands)
        {
            String name = commandName(command);
            if (topLevelNames.contains(name) || !names.add(name))
                throw new ConfigurationException(format("command '%s' (%s) has the name of an existing nodetool command or group", name, command.getName()));
        }
        for (NodeToolExtension.CommandGroup group : groups)
        {
            if (group == null)
                throw new ConfigurationException("it lists a null command group");
            checkName(group.name, "group");
            if (topLevelNames.contains(group.name) || !names.add(group.name))
                throw new ConfigurationException(format("group '%s' has the name of an existing nodetool command or group", group.name));

            Set<String> groupNames = new HashSet<>();
            for (Class<? extends NodeToolCmdRunnable> command : group.commands)
            {
                String name = commandName(command);
                if (!groupNames.add(name))
                    throw new ConfigurationException(format("group '%s' lists the command '%s' twice", group.name, name));
            }
            commandName(defaultCommand(group));
        }

        topLevelNames.addAll(names);
        builder.withCommands(commands);
        for (NodeToolExtension.CommandGroup group : groups)
        {
            Cli.GroupBuilder<NodeToolCmdRunnable> groupBuilder = builder.withGroup(group.name)
                                                                        .withDefaultCommand(defaultCommand(group))
                                                                        .withCommands(group.commands);
            if (group.description != null)
                groupBuilder.withDescription(group.description);
        }
    }

    private static Class<? extends NodeToolCmdRunnable> defaultCommand(NodeToolExtension.CommandGroup group)
    {
        return group.defaultCommand == null ? CassHelp.class : group.defaultCommand;
    }

    /**
     * @return the name a command class is invoked with, that is the name of its {@link Command @Command} annotation,
     * looked up through the superclasses as airline does
     * @throws ConfigurationException if the class is null, not annotated, or its name is invalid
     */
    private static String commandName(@Nullable Class<?> command)
    {
        if (command == null)
            throw new ConfigurationException("it lists a null command class");
        for (Class<?> cls = command; cls != null && cls != Object.class; cls = cls.getSuperclass())
        {
            Command annotation = cls.getAnnotation(Command.class);
            if (annotation != null)
            {
                checkName(annotation.name(), "command (" + command.getName() + ')');
                return annotation.name();
            }
        }
        throw new ConfigurationException(format("command class %s is not annotated with @%s", command.getName(), Command.class.getName()));
    }

    /**
     * Rejects the names that could not be given on the command line: empty, with whitespace, or looking like an option.
     */
    private static void checkName(@Nullable String name, String what)
    {
        if (name == null || name.isEmpty() || name.startsWith("-") || name.chars().anyMatch(Character::isWhitespace))
            throw new ConfigurationException(format("invalid %s name '%s': a name is not empty, has no whitespace and does not start with '-'", what, name));
    }

    private static void printHistory(String... args)
    {
        //don't bother to print if no args passed (meaning, nodetool is just printing out the sub-commands list)
        if (args.length == 0)
            return;

        String cmdLine = Joiner.on(" ").skipNulls().join(args);
        cmdLine = cmdLine.replaceFirst("(?<=(-pw|--password))\\s+\\S+", " <hidden>");

        try (FileWriter writer = new File(FBUtilities.getToolsOutputDirectory(), HISTORYFILE).newWriter(APPEND))
        {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
            writer.append(sdf.format(new Date())).append(": ").append(cmdLine).append(System.lineSeparator());
        }
        catch (IOException | IOError ioe)
        {
            //quietly ignore any errors about not being able to write out history
        }
    }

    protected void badUse(Exception e)
    {
        output.out.println("nodetool: " + e.getMessage());
        output.out.println("See 'nodetool help' or 'nodetool help <command>'.");
    }

    protected void err(Throwable e)
    {
        // CASSANDRA-11537: friendly error message when server is not ready
        if (e instanceof InstanceNotFoundException)
            throw new IllegalArgumentException("Server is not initialized yet, cannot run nodetool.");

        output.err.println("error: " + e.getMessage());
        output.err.println("-- StackTrace --");
        output.err.println(getStackTraceAsString(e));
    }

    public static class CassHelp extends Help implements NodeToolCmdRunnable
    {
        public void run(INodeProbeFactory nodeProbeFactory, Output output)
        {
            run();
        }
    }

    public interface NodeToolCmdRunnable
    {
        void run(INodeProbeFactory nodeProbeFactory, Output output);
    }

    public static abstract class NodeToolCmd implements NodeToolCmdRunnable
    {

        @Option(type = OptionType.GLOBAL, name = {"-h", "--host"}, description = "Node hostname or ip address")
        private String host = "127.0.0.1";

        @Option(type = OptionType.GLOBAL, name = {"-p", "--port"}, description = "Remote jmx agent port number")
        private String port = "7199";

        @Option(type = OptionType.GLOBAL, name = {"-u", "--username"}, description = "Remote jmx agent username")
        private String username = EMPTY;

        @Option(type = OptionType.GLOBAL, name = {"-pw", "--password"}, description = "Remote jmx agent password")
        private String password = EMPTY;

        @Option(type = OptionType.GLOBAL, name = {"-pwf", "--password-file"}, description = "Path to the JMX password file")
        private String passwordFilePath = EMPTY;

        @Option(type = OptionType.GLOBAL, name = { "-pp", "--print-port"}, description = "Operate in 4.0 mode with hosts disambiguated by port number", arity = 0)
        protected boolean printPort = false;

        private INodeProbeFactory nodeProbeFactory;
        protected Output output;

        @Override
        public void run(INodeProbeFactory nodeProbeFactory, Output output)
        {
            this.nodeProbeFactory = nodeProbeFactory;
            this.output = output;
            runInternal();
        }

        public void runInternal()
        {
            if (isNotEmpty(username)) {
                if (isNotEmpty(passwordFilePath))
                    password = readUserPasswordFromFile(username, passwordFilePath);

                if (isEmpty(password))
                    password = promptAndReadPassword();
            }

            try (NodeProbe probe = connect())
            {
                execute(probe);
                if (probe.isFailed())
                    throw new RuntimeException("nodetool failed, check server logs");
            }
            catch (IOException e)
            {
                throw new RuntimeException("Error while closing JMX connection", e);
            }

        }

        private String readUserPasswordFromFile(String username, String passwordFilePath) {
            String password = EMPTY;

            File passwordFile = new File(passwordFilePath);
            try (Scanner scanner = new Scanner(passwordFile.toJavaIOFile()).useDelimiter("\\s+"))
            {
                while (scanner.hasNextLine())
                {
                    if (scanner.hasNext())
                    {
                        String jmxRole = scanner.next();
                        if (jmxRole.equals(username) && scanner.hasNext())
                        {
                            password = scanner.next();
                            break;
                        }
                    }
                    scanner.nextLine();
                }
            }
            catch (FileNotFoundException e)
            {
                throw new RuntimeException(e);
            }

            return password;
        }

        private String promptAndReadPassword()
        {
            String password = EMPTY;

            Console console = System.console();
            if (console != null)
                password = String.valueOf(console.readPassword("Password:"));

            return password;
        }

        protected abstract void execute(NodeProbe probe);

        private NodeProbe connect()
        {
            NodeProbe nodeClient = null;

            try
            {
                if (username.isEmpty())
                    nodeClient = nodeProbeFactory.create(host, parseInt(port));
                else
                    nodeClient = nodeProbeFactory.create(host, parseInt(port), username, password);

                nodeClient.setOutput(output);
            } catch (IOException | SecurityException e)
            {
                Throwable rootCause = Throwables.getRootCause(e);
                output.err.println(format("nodetool: Failed to connect to '%s:%s' - %s: '%s'.", host, port, rootCause.getClass().getSimpleName(), rootCause.getMessage()));
                System.exit(1);
            }

            return nodeClient;
        }

        protected enum KeyspaceSet
        {
            ALL, NON_SYSTEM, NON_LOCAL_STRATEGY
        }

        protected List<String> parseOptionalKeyspace(List<String> cmdArgs, NodeProbe nodeProbe)
        {
            return parseOptionalKeyspace(cmdArgs, nodeProbe, KeyspaceSet.ALL);
        }

        protected List<String> parseOptionalKeyspace(List<String> cmdArgs, NodeProbe nodeProbe, KeyspaceSet defaultKeyspaceSet)
        {
            List<String> keyspaces = new ArrayList<>();


            if (cmdArgs == null || cmdArgs.isEmpty())
            {
                if (defaultKeyspaceSet == KeyspaceSet.NON_LOCAL_STRATEGY)
                    keyspaces.addAll(keyspaces = nodeProbe.getNonLocalStrategyKeyspaces());
                else if (defaultKeyspaceSet == KeyspaceSet.NON_SYSTEM)
                    keyspaces.addAll(keyspaces = nodeProbe.getNonSystemKeyspaces());
                else
                    keyspaces.addAll(nodeProbe.getKeyspaces());
            }
            else
            {
                keyspaces.add(cmdArgs.get(0));
            }

            for (String keyspace : keyspaces)
            {
                if (!nodeProbe.getKeyspaces().contains(keyspace))
                    throw new IllegalArgumentException("Keyspace [" + keyspace + "] does not exist.");
            }

            return Collections.unmodifiableList(keyspaces);
        }

        protected String[] parseOptionalTables(List<String> cmdArgs)
        {
            return cmdArgs.size() <= 1 ? EMPTY_STRING_ARRAY : toArray(cmdArgs.subList(1, cmdArgs.size()), String.class);
        }

        protected String[] parsePartitionKeys(List<String> cmdArgs)
        {
            return cmdArgs.size() <= 2 ? EMPTY_STRING_ARRAY : toArray(cmdArgs.subList(2, cmdArgs.size()), String.class);
        }
    }

    public static SortedMap<String, SetHostStatWithPort> getOwnershipByDcWithPort(NodeProbe probe, boolean resolveIp,
                                                                  Map<String, String> tokenToEndpoint,
                                                                  Map<String, Float> ownerships)
    {
        SortedMap<String, SetHostStatWithPort> ownershipByDc = Maps.newTreeMap();
        EndpointSnitchInfoMBean epSnitchInfo = probe.getEndpointSnitchInfoProxy();
        try
        {
            for (Entry<String, String> tokenAndEndPoint : tokenToEndpoint.entrySet())
            {
                String dc = epSnitchInfo.getDatacenter(tokenAndEndPoint.getValue());
                if (!ownershipByDc.containsKey(dc))
                    ownershipByDc.put(dc, new SetHostStatWithPort(resolveIp));
                ownershipByDc.get(dc).add(tokenAndEndPoint.getKey(), tokenAndEndPoint.getValue(), ownerships);
            }
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }
        return ownershipByDc;
    }
}
