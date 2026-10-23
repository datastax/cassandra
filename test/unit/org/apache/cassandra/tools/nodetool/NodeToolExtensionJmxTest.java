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
package org.apache.cassandra.tools.nodetool;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.service.NodeToolServerExtensions;
import org.apache.cassandra.service.SampleNodeToolServerExtension;
import org.apache.cassandra.tools.SampleNodeToolExtension;
import org.apache.cassandra.tools.ToolRunner;
import org.apache.cassandra.tools.ToolRunner.ToolResult;
import org.apache.cassandra.utils.MBeanWrapper;

import static org.apache.cassandra.config.CassandraRelevantProperties.CUSTOM_NODETOOL_EXTENSION_CLASSES;
import static org.apache.cassandra.config.CassandraRelevantProperties.CUSTOM_NODETOOL_SERVER_EXTENSION_CLASSES;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end check of the nodetool extension hooks: the server-side sample extension is set up in this JVM, the
 * way the daemon does at startup, and {@code bin/nodetool} is forked with the client-side sample extension
 * configured through a {@code -D} argument, so that its command reaches the MBean over JMX.
 */
public class NodeToolExtensionJmxTest extends CQLTester
{
    private static final String EXTENSION_ARG = "-D" + CUSTOM_NODETOOL_EXTENSION_CLASSES.getKey() + '=' + SampleNodeToolExtension.class.getName();

    private static WithProperties properties;

    @BeforeClass
    public static void setup() throws Exception
    {
        startJMXServer();
        properties = new WithProperties().set(CUSTOM_NODETOOL_SERVER_EXTENSION_CLASSES, SampleNodeToolServerExtension.class.getName());
        NodeToolServerExtensions.setupAll(NodeToolServerExtensions.load());
    }

    @AfterClass
    public static void unregisterSampleMBean()
    {
        if (properties != null)
            properties.close();
        MBeanWrapper.instance.unregisterMBean(SampleNodeToolServerExtension.MBEAN_NAME, MBeanWrapper.OnException.IGNORE);
    }

    /**
     * The sample extension classes live in the test classes, which bin/nodetool does not put on its classpath.
     */
    private static Map<String, String> env() throws URISyntaxException
    {
        Path testClasses = Paths.get(SampleNodeToolExtension.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        return Collections.singletonMap("EXTRA_CLASSPATH", testClasses.toAbsolutePath().toString());
    }

    private static ToolResult nodetool(String... args) throws URISyntaxException
    {
        List<String> allArgs = new ArrayList<>();
        allArgs.add(EXTENSION_ARG);
        Collections.addAll(allArgs, args);
        return ToolRunner.invoke(env(), buildNodetoolArgs(allArgs));
    }

    @Test
    public void extensionCommandReachesTheMBeanOverJmx() throws Exception
    {
        ToolResult tool = nodetool(SampleNodeToolExtension.GROUP, SampleNodeToolExtension.PING_COMMAND, "hello");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).isEqualTo("pong hello" + System.lineSeparator());
    }

    @Test
    public void extensionCommandThatDoesNotNeedTheNodeRuns() throws Exception
    {
        ToolResult tool = nodetool(SampleNodeToolExtension.ECHO_COMMAND, "hello", "world");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).isEqualTo("hello world" + System.lineSeparator());
    }

    @Test
    public void helpListsTheExtensionCommands() throws Exception
    {
        ToolResult tool = nodetool("help");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).contains(SampleNodeToolExtension.ECHO_COMMAND)
                                    .containsPattern("(?m)^\\s+" + SampleNodeToolExtension.GROUP + "\\s")
                                    .contains(SampleNodeToolExtension.GROUP_DESCRIPTION);
    }

    @Test
    public void withoutThePropertyTheCommandIsUnknown() throws Exception
    {
        ToolResult tool = ToolRunner.invoke(env(), buildNodetoolArgs(List.of(SampleNodeToolExtension.ECHO_COMMAND, "hello")));
        assertThat(tool.getExitCode()).isEqualTo(1);
        assertThat(tool.getStdout()).contains("See 'nodetool help'");
    }

    @Test
    public void misconfiguredExtensionIsIgnoredWithAWarning() throws Exception
    {
        String arg = "-D" + CUSTOM_NODETOOL_EXTENSION_CLASSES.getKey() + "=org.apache.cassandra.tools.DoesNotExist," + SampleNodeToolExtension.class.getName();
        ToolResult tool = ToolRunner.invoke(env(), buildNodetoolArgs(List.of(arg, SampleNodeToolExtension.ECHO_COMMAND, "hello")));
        assertThat(tool.getExitCode()).isEqualTo(0);
        assertThat(tool.getStdout()).isEqualTo("hello" + System.lineSeparator());
        assertThat(tool.getStderr()).contains("warning: ignoring nodetool extension org.apache.cassandra.tools.DoesNotExist")
                                    .contains("Unable to find nodetool extension (set with " + CUSTOM_NODETOOL_EXTENSION_CLASSES.getKey() + ") class");
    }
}
