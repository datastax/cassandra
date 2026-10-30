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
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.google.common.collect.ImmutableList;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.airlift.airline.Command;
import io.airlift.airline.Option;
import io.airlift.airline.OptionType;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.service.SampleNodeToolServerExtension;
import org.apache.cassandra.tools.NodeTool.NodeToolCmdRunnable;
import org.apache.cassandra.tools.NodeToolExtension.CommandGroup;
import org.apache.cassandra.tools.SampleNodeToolExtension.Echo;
import org.apache.cassandra.tools.SampleNodeToolExtension.Ping;
import org.mockito.Mockito;

import static org.apache.cassandra.config.CassandraRelevantProperties.CUSTOM_NODETOOL_EXTENSION_CLASSES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

/**
 * Runs nodetool in-process, against a mocked {@link NodeProbe}, with {@link NodeToolExtension}s configured through
 * {@link org.apache.cassandra.config.CassandraRelevantProperties#CUSTOM_NODETOOL_EXTENSION_CLASSES}.
 * The JMX round trip of an extension command is covered by {@code NodeToolExtensionJmxTest} and by the in-JVM dtest
 * {@code NodeToolExtensionDistributedTest}.
 */
public class NodeToolExtensionTest
{
    private static final String SAMPLE = SampleNodeToolExtension.class.getName();
    /** The sample group as listed by the nodetool help, on its own line (a built-in command name contains "sample"). */
    private static final String GROUP_LISTED = "(?m)^\\s+" + SampleNodeToolExtension.GROUP + "\\s";
    private static final String IGNORING = "warning: ignoring nodetool extension ";

    private final NodeProbe probe = Mockito.mock(NodeProbe.class);
    private final INodeProbeFactory probeFactory = new INodeProbeFactory()
    {
        @Override
        public NodeProbe create(String host, int port)
        {
            return probe;
        }

        @Override
        public NodeProbe create(String host, int port, String username, String password)
        {
            return probe;
        }
    };
    private final MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
    private ObjectName sampleMBean;
    private WithProperties properties;

    @Before
    public void setup() throws Exception
    {
        properties = new WithProperties();
        sampleMBean = new ObjectName(SampleNodeToolServerExtension.MBEAN_NAME);
        mbeanServer.registerMBean(new SampleNodeToolServerExtension.Sample(), sampleMBean);
        when(probe.getMbeanServerConn()).thenReturn(mbeanServer);
        when(probe.getReleaseVersion()).thenReturn("1.2.3");
    }

    @After
    public void cleanup() throws Exception
    {
        properties.close();
        mbeanServer.unregisterMBean(sampleMBean);
    }

    private void extensions(Class<?>... extensions)
    {
        properties.set(CUSTOM_NODETOOL_EXTENSION_CLASSES, Arrays.stream(extensions).map(Class::getName).toArray(String[]::new));
    }

    private static final class Run
    {
        final int exitCode;
        final String out;
        final String err;

        Run(int exitCode, String out, String err)
        {
            this.exitCode = exitCode;
            this.out = out;
            this.err = err;
        }
    }

    /**
     * Runs nodetool in-process, capturing what it writes to its {@link Output} and to {@code System.out/err}: the
     * built-in help prints to the latter.
     */
    private Run nodetool(String... args)
    {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        PrintStream originalOut = System.out;
        PrintStream originalErr = System.err;
        int exitCode;
        try (PrintStream outStream = new PrintStream(out, true, StandardCharsets.UTF_8.name());
             PrintStream errStream = new PrintStream(err, true, StandardCharsets.UTF_8.name()))
        {
            System.setOut(outStream);
            System.setErr(errStream);
            Output output = new Output(outStream, errStream);
            when(probe.output()).thenReturn(output);
            exitCode = new NodeTool(probeFactory, output).execute(args);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        finally
        {
            System.setOut(originalOut);
            System.setErr(originalErr);
        }
        return new Run(exitCode, new String(out.toByteArray(), StandardCharsets.UTF_8), new String(err.toByteArray(), StandardCharsets.UTF_8));
    }

    private static void assertSuccess(Run run)
    {
        assertThat(run.exitCode).describedAs("exit code, stdout: %s, stderr: %s", run.out, run.err).isEqualTo(0);
        assertThat(run.err).isEmpty();
    }

    private static void assertBadUse(Run run)
    {
        assertThat(run.exitCode).describedAs("exit code, stdout: %s, stderr: %s", run.out, run.err).isEqualTo(1);
        assertThat(run.out).contains("See 'nodetool help'");
    }

    /**
     * A built-in command runs and succeeds, with a single warning about the ignored extension on the error output.
     */
    private Run assertExtensionIgnored(String... warningParts)
    {
        Run run = nodetool("version");
        assertThat(run.exitCode).describedAs("exit code, stdout: %s, stderr: %s", run.out, run.err).isEqualTo(0);
        assertThat(run.out).contains("ReleaseVersion: 1.2.3");
        assertThat(run.err).startsWith(IGNORING).contains(warningParts).doesNotContain("StackTrace");
        assertThat(run.err.trim()).doesNotContain(System.lineSeparator());
        return run;
    }

    @Test
    public void withoutExtensionsTheCommandsAreUnknown()
    {
        assertBadUse(nodetool(SampleNodeToolExtension.ECHO_COMMAND, "hello"));
        assertBadUse(nodetool(SampleNodeToolExtension.GROUP, SampleNodeToolExtension.PING_COMMAND, "hello"));
        assertThat(nodetool("help").out).doesNotContain(SampleNodeToolExtension.ECHO_COMMAND)
                                        .doesNotContainPattern(GROUP_LISTED);
    }

    @Test
    public void blankPropertyMeansNoExtension()
    {
        properties.set(CUSTOM_NODETOOL_EXTENSION_CLASSES, " , ");
        assertBadUse(nodetool(SampleNodeToolExtension.ECHO_COMMAND, "hello"));
        assertSuccess(nodetool("help"));
    }

    @Test
    public void topLevelCommandRuns()
    {
        extensions(SampleNodeToolExtension.class);
        Run run = nodetool(SampleNodeToolExtension.ECHO_COMMAND, "hello", "world");
        assertSuccess(run);
        assertThat(run.out).isEqualTo("hello world" + System.lineSeparator());
    }

    @Test
    public void globalOptionsAreAcceptedBeforeExtensionCommands()
    {
        extensions(SampleNodeToolExtension.class);
        Run run = nodetool("-h", "127.0.0.1", "-p", "7199", SampleNodeToolExtension.ECHO_COMMAND, "hello");
        assertSuccess(run);
        assertThat(run.out).isEqualTo("hello" + System.lineSeparator());
    }

    @Test
    public void groupCommandTalksToTheMBeanThroughTheProbe()
    {
        extensions(SampleNodeToolExtension.class);
        Run run = nodetool(SampleNodeToolExtension.GROUP, SampleNodeToolExtension.PING_COMMAND, "hello");
        assertSuccess(run);
        assertThat(run.out).isEqualTo("pong hello" + System.lineSeparator());
    }

    @Test
    public void groupCommandFailuresAreReportedLikeBuiltinOnes()
    {
        extensions(SampleNodeToolExtension.class);
        // missing required argument
        assertBadUse(nodetool(SampleNodeToolExtension.GROUP, SampleNodeToolExtension.PING_COMMAND));
    }

    @Test
    public void helpListsExtensionCommandsAndGroups()
    {
        extensions(SampleNodeToolExtension.class);
        Run help = nodetool("help");
        assertSuccess(help);
        assertThat(help.out).contains(SampleNodeToolExtension.ECHO_COMMAND)
                            .contains("Prints its arguments")
                            .containsPattern(GROUP_LISTED)
                            .contains(SampleNodeToolExtension.GROUP_DESCRIPTION);

        Run commandHelp = nodetool("help", SampleNodeToolExtension.ECHO_COMMAND);
        assertSuccess(commandHelp);
        assertThat(commandHelp.out).contains("nodetool " + SampleNodeToolExtension.ECHO_COMMAND + " - Prints its arguments")
                                   .contains("The words to print");

        Run groupHelp = nodetool("help", SampleNodeToolExtension.GROUP);
        assertSuccess(groupHelp);
        assertThat(groupHelp.out).contains(SampleNodeToolExtension.GROUP_DESCRIPTION)
                                 .contains(SampleNodeToolExtension.PING_COMMAND);
    }

    @Test
    public void bareGroupPrintsTheNodetoolHelpWhenNoDefaultCommandIsGiven()
    {
        extensions(SampleNodeToolExtension.class);
        Run run = nodetool(SampleNodeToolExtension.GROUP);
        assertSuccess(run);
        // same as the built-in "nodetool bootstrap"
        assertThat(run.out).startsWith("usage: nodetool").containsPattern(GROUP_LISTED);
    }

    @Test
    public void groupDefaultCommandRuns()
    {
        extensions(GroupWithDefaultExtension.class);
        Run run = nodetool("sampledefault");
        assertSuccess(run);
        assertThat(run.out).isEqualTo("pong default" + System.lineSeparator());
    }

    @Test
    public void builtinCommandsStillRunWithExtensions()
    {
        extensions(SampleNodeToolExtension.class);
        Run run = nodetool("version");
        assertSuccess(run);
        assertThat(run.out).contains("ReleaseVersion: 1.2.3");
    }

    @Test
    public void severalExtensionsAreLoadedInOrder()
    {
        extensions(SampleNodeToolExtension.class, SecondExtension.class);
        assertThat(nodetool(SampleNodeToolExtension.ECHO_COMMAND, "one").out).isEqualTo("one" + System.lineSeparator());
        assertThat(nodetool("secondecho", "two").out).isEqualTo("two" + System.lineSeparator());
        String help = nodetool("help").out;
        assertThat(help).contains(SampleNodeToolExtension.ECHO_COMMAND).contains("secondecho");
        assertThat(help.indexOf(SampleNodeToolExtension.ECHO_COMMAND)).isLessThan(help.indexOf("secondecho"));
    }

    // a broken extension is skipped with a warning, and never takes the built-in commands down

    @Test
    public void unknownClassIsIgnored()
    {
        properties.set(CUSTOM_NODETOOL_EXTENSION_CLASSES, "org.apache.cassandra.tools.DoesNotExist");
        assertExtensionIgnored("org.apache.cassandra.tools.DoesNotExist",
                               "Unable to find nodetool extension (set with " + CUSTOM_NODETOOL_EXTENSION_CLASSES.getKey() + ") class");
    }

    @Test
    public void classFailingToInitializeIsIgnoredWithTheCause()
    {
        extensions(FailingInitializerExtension.class);
        assertExtensionIgnored(FailingInitializerExtension.class.getName(), "Error initializing", "caused by", "IllegalStateException: static init boom");
    }

    @Test
    public void brokenExtensionDoesNotTakeTheOthersDown()
    {
        properties.set(CUSTOM_NODETOOL_EXTENSION_CLASSES, "org.apache.cassandra.tools.DoesNotExist," + SAMPLE);
        Run run = nodetool(SampleNodeToolExtension.ECHO_COMMAND, "still", "there");
        assertThat(run.exitCode).isEqualTo(0);
        assertThat(run.out).isEqualTo("still there" + System.lineSeparator());
        assertThat(run.err).startsWith(IGNORING + "org.apache.cassandra.tools.DoesNotExist");
    }

    @Test
    public void commandOfAnIgnoredExtensionIsUnknown()
    {
        extensions(ThrowingExtension.class);
        Run run = nodetool("sampleecho", "hello");
        assertBadUse(run);
        assertThat(run.err).startsWith(IGNORING + ThrowingExtension.class.getName());
    }

    @Test
    public void classThatIsNotAnExtensionIsIgnored()
    {
        extensions(String.class);
        assertExtensionIgnored(String.class.getName(), "is not a " + NodeToolExtension.class.getName());
    }

    @Test
    public void classWithoutPublicConstructorIsIgnored()
    {
        extensions(PrivateConstructorExtension.class);
        assertExtensionIgnored(PrivateConstructorExtension.class.getName(), "constructor");
    }

    @Test
    public void classWithThrowingConstructorIsIgnoredWithTheCause()
    {
        extensions(ThrowingConstructorExtension.class);
        assertExtensionIgnored(ThrowingConstructorExtension.class.getName(), "Error instantiating", "caused by", "IllegalStateException: no constructor for you");
    }

    @Test
    public void classListedTwiceIsIgnoredTheSecondTime()
    {
        properties.set(CUSTOM_NODETOOL_EXTENSION_CLASSES, SAMPLE + ',' + SAMPLE);
        Run run = nodetool(SampleNodeToolExtension.ECHO_COMMAND, "once");
        assertThat(run.exitCode).isEqualTo(0);
        assertThat(run.out).isEqualTo("once" + System.lineSeparator());
        assertThat(run.err).startsWith(IGNORING + SAMPLE + ": listed twice");
    }

    @Test
    public void commandClashingWithBuiltinCommandIsIgnored()
    {
        extensions(ClashingCommandExtension.class);
        assertExtensionIgnored(ClashingCommandExtension.class.getName(),
                               "command 'version'",
                               ClashingCommandExtension.Version.class.getName(),
                               "has the name of an existing nodetool command or group");
    }

    @Test
    public void commandClashingWithBuiltinGroupIsIgnored()
    {
        extensions(CommandNamedLikeBuiltinGroupExtension.class);
        assertExtensionIgnored("command 'repair_admin'", "has the name of an existing nodetool command or group");
        // the built-in group is untouched (the warning is printed on every run)
        Run help = nodetool("help", "repair_admin");
        assertThat(help.exitCode).isEqualTo(0);
        assertThat(help.out).contains("nodetool repair_admin").contains("cancel");
    }

    @Test
    public void groupClashingWithBuiltinGroupIsIgnored()
    {
        extensions(ClashingGroupExtension.class);
        assertExtensionIgnored("group 'repair_admin'", "has the name of an existing nodetool command or group");
        assertThat(nodetool("help", "repair_admin").out).doesNotContain(SampleNodeToolExtension.PING_COMMAND);
    }

    @Test
    public void groupClashingWithBuiltinCommandIsIgnored()
    {
        extensions(GroupNamedLikeBuiltinCommandExtension.class);
        assertExtensionIgnored("group 'status'", "has the name of an existing nodetool command or group");
    }

    @Test
    public void clashBetweenExtensionsIgnoresTheSecondOne()
    {
        extensions(SampleNodeToolExtension.class, EchoAgainExtension.class);
        Run run = nodetool(SampleNodeToolExtension.ECHO_COMMAND, "first");
        assertThat(run.exitCode).isEqualTo(0);
        assertThat(run.out).isEqualTo("first" + System.lineSeparator());
        assertThat(run.err).startsWith(IGNORING + EchoAgainExtension.class.getName())
                           .contains("command '" + SampleNodeToolExtension.ECHO_COMMAND + "'");
    }

    @Test
    public void clashWithinAnExtensionIsIgnored()
    {
        extensions(SelfClashingExtension.class);
        assertExtensionIgnored(SelfClashingExtension.class.getName(), "group '" + SampleNodeToolExtension.ECHO_COMMAND + "'");
        assertBadUse(nodetool(SampleNodeToolExtension.ECHO_COMMAND, "hello"));
    }

    @Test
    public void duplicateCommandInGroupIsIgnored()
    {
        extensions(DuplicateInGroupExtension.class);
        assertExtensionIgnored("group 'dup'", "lists the command '" + SampleNodeToolExtension.PING_COMMAND + "' twice");
    }

    @Test
    public void invalidNamesAreIgnored()
    {
        extensions(BlankGroupExtension.class);
        assertExtensionIgnored(BlankGroupExtension.class.getName(), "invalid group name ' '");

        extensions(OptionLikeGroupExtension.class);
        assertExtensionIgnored(OptionLikeGroupExtension.class.getName(), "invalid group name '-h'");

        extensions(WhitespaceCommandNameExtension.class);
        assertExtensionIgnored(WhitespaceCommandNameExtension.class.getName(), "invalid command (" + WhitespaceCommandNameExtension.BadName.class.getName() + ") name 'bad name'");
    }

    @Test
    public void commandWithoutAnnotationIsIgnored()
    {
        extensions(NotAnnotatedExtension.class);
        assertExtensionIgnored(NotAnnotatedExtension.NotAnnotated.class.getName(), "is not annotated with @" + Command.class.getName());
    }

    @Test
    public void groupDefaultCommandWithoutAnnotationIsIgnored()
    {
        extensions(NotAnnotatedDefaultExtension.class);
        assertExtensionIgnored(NotAnnotatedExtension.NotAnnotated.class.getName(), "is not annotated with @" + Command.class.getName());
    }

    @Test
    public void extensionFailingToListItsCommandsIsIgnored()
    {
        extensions(ThrowingExtension.class);
        assertExtensionIgnored(ThrowingExtension.class.getName(), "boom");
    }

    @Test
    public void extensionWithMissingDependencyIsIgnored()
    {
        extensions(MissingDependencyExtension.class);
        assertExtensionIgnored(MissingDependencyExtension.class.getName(), "com/example/Missing");
    }

    @Test
    public void extensionReturningNullIsIgnored()
    {
        extensions(NullExtension.class);
        assertExtensionIgnored(NullExtension.class.getName(), "null list of commands");

        extensions(NullCommandExtension.class);
        assertExtensionIgnored(NullCommandExtension.class.getName(), "null command class");
    }

    @Test
    public void conflictingGlobalOptionIsIgnored()
    {
        extensions(GlobalOptionExtension.class, SampleNodeToolExtension.class, SecondExtension.class);
        // airline rejects the definitions of that extension alone: the ones before and after it are kept
        assertExtensionIgnored(GlobalOptionExtension.class.getName(), "-p");
        assertThat(nodetool(SampleNodeToolExtension.ECHO_COMMAND, "hello").out).isEqualTo("hello" + System.lineSeparator());
        assertThat(nodetool("secondecho", "hello").out).isEqualTo("hello" + System.lineSeparator());
        assertBadUse(nodetool("withglobalport"));
    }

    // fixtures

    public static class SecondExtension implements NodeToolExtension
    {
        @Command(name = "secondecho", description = "Prints its arguments")
        public static class SecondEcho extends Echo
        {
        }

        @Override
        public List<Class<? extends NodeToolCmdRunnable>> commands()
        {
            return ImmutableList.of(SecondEcho.class);
        }
    }

    /** Contributes the sample echo command again, under its name. */
    public static class EchoAgainExtension implements NodeToolExtension
    {
        @Override
        public List<Class<? extends NodeToolCmdRunnable>> commands()
        {
            return ImmutableList.of(Echo.class);
        }
    }

    /** A group named like one of its own top-level commands. */
    public static class SelfClashingExtension implements NodeToolExtension
    {
        @Override
        public List<Class<? extends NodeToolCmdRunnable>> commands()
        {
            return ImmutableList.of(Echo.class);
        }

        @Override
        public List<CommandGroup> commandGroups()
        {
            return ImmutableList.of(new CommandGroup(SampleNodeToolExtension.ECHO_COMMAND, null, null, ImmutableList.of(Ping.class)));
        }
    }

    public static class GroupWithDefaultExtension implements NodeToolExtension
    {
        @Command(name = "pingdefault")
        public static class PingDefault extends NodeTool.NodeToolCmd
        {
            @Override
            protected void execute(NodeProbe probe)
            {
                output.out.println("pong default");
            }
        }

        @Override
        public List<CommandGroup> commandGroups()
        {
            return ImmutableList.of(new CommandGroup("sampledefault", null, PingDefault.class, ImmutableList.of(PingDefault.class, Ping.class)));
        }
    }

    public static class PrivateConstructorExtension implements NodeToolExtension
    {
        private PrivateConstructorExtension()
        {
        }
    }

    public static class FailingInitializerExtension implements NodeToolExtension
    {
        static
        {
            if (true)
                throw new IllegalStateException("static init boom");
        }
    }

    public static class ThrowingConstructorExtension implements NodeToolExtension
    {
        public ThrowingConstructorExtension()
        {
            throw new IllegalStateException("no constructor for you");
        }
    }

    public static class ClashingCommandExtension implements NodeToolExtension
    {
        @Command(name = "version")
        public static class Version extends Echo
        {
        }

        @Override
        public List<Class<? extends NodeToolCmdRunnable>> commands()
        {
            return ImmutableList.of(Version.class);
        }
    }

    public static class CommandNamedLikeBuiltinGroupExtension implements NodeToolExtension
    {
        @Command(name = "repair_admin")
        public static class RepairAdmin extends Echo
        {
        }

        @Override
        public List<Class<? extends NodeToolCmdRunnable>> commands()
        {
            return ImmutableList.of(RepairAdmin.class);
        }
    }

    public static class ClashingGroupExtension implements NodeToolExtension
    {
        @Override
        public List<CommandGroup> commandGroups()
        {
            return ImmutableList.of(new CommandGroup("repair_admin", null, null, ImmutableList.of(Ping.class)));
        }
    }

    public static class GroupNamedLikeBuiltinCommandExtension implements NodeToolExtension
    {
        @Override
        public List<CommandGroup> commandGroups()
        {
            return ImmutableList.of(new CommandGroup("status", null, null, ImmutableList.of(Ping.class)));
        }
    }

    public static class DuplicateInGroupExtension implements NodeToolExtension
    {
        @Override
        public List<CommandGroup> commandGroups()
        {
            return ImmutableList.of(new CommandGroup("dup", null, null, ImmutableList.of(Ping.class, Ping.class)));
        }
    }

    public static class BlankGroupExtension implements NodeToolExtension
    {
        @Override
        public List<CommandGroup> commandGroups()
        {
            return ImmutableList.of(new CommandGroup(" ", null, null, ImmutableList.of(Ping.class)));
        }
    }

    public static class OptionLikeGroupExtension implements NodeToolExtension
    {
        @Override
        public List<CommandGroup> commandGroups()
        {
            return ImmutableList.of(new CommandGroup("-h", null, null, ImmutableList.of(Ping.class)));
        }
    }

    public static class WhitespaceCommandNameExtension implements NodeToolExtension
    {
        @Command(name = "bad name")
        public static class BadName extends Echo
        {
        }

        @Override
        public List<Class<? extends NodeToolCmdRunnable>> commands()
        {
            return ImmutableList.of(BadName.class);
        }
    }

    public static class NotAnnotatedExtension implements NodeToolExtension
    {
        public static class NotAnnotated implements NodeToolCmdRunnable
        {
            @Override
            public void run(INodeProbeFactory nodeProbeFactory, Output output)
            {
            }
        }

        @Override
        public List<Class<? extends NodeToolCmdRunnable>> commands()
        {
            return ImmutableList.of(NotAnnotated.class);
        }
    }

    public static class NotAnnotatedDefaultExtension implements NodeToolExtension
    {
        @Override
        public List<CommandGroup> commandGroups()
        {
            return ImmutableList.of(new CommandGroup("notannotated", null, NotAnnotatedExtension.NotAnnotated.class, ImmutableList.of(Ping.class)));
        }
    }

    public static class ThrowingExtension implements NodeToolExtension
    {
        @Override
        public List<Class<? extends NodeToolCmdRunnable>> commands()
        {
            throw new IllegalStateException("boom");
        }
    }

    /** What a command class referencing a class of a missing jar looks like when it is resolved. */
    public static class MissingDependencyExtension implements NodeToolExtension
    {
        @Override
        public List<Class<? extends NodeToolCmdRunnable>> commands()
        {
            throw new NoClassDefFoundError("com/example/Missing");
        }
    }

    public static class NullExtension implements NodeToolExtension
    {
        @Override
        public List<Class<? extends NodeToolCmdRunnable>> commands()
        {
            return null;
        }
    }

    public static class NullCommandExtension implements NodeToolExtension
    {
        @Override
        public List<Class<? extends NodeToolCmdRunnable>> commands()
        {
            return Arrays.asList(Echo.class, null);
        }
    }

    /** A command declaring a global option that conflicts with the ones of {@link NodeTool.NodeToolCmd}. */
    public static class GlobalOptionExtension implements NodeToolExtension
    {
        @Command(name = "withglobalport")
        public static class WithGlobalPort implements NodeToolCmdRunnable
        {
            @Option(type = OptionType.GLOBAL, name = { "-p", "--port" }, description = "Conflicts with the built-in port option")
            private int port = 0;

            @Override
            public void run(INodeProbeFactory nodeProbeFactory, Output output)
            {
            }
        }

        @Override
        public List<Class<? extends NodeToolCmdRunnable>> commands()
        {
            return ImmutableList.of(WithGlobalPort.class);
        }
    }
}
