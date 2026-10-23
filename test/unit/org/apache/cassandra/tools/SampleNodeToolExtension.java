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

import java.util.ArrayList;
import java.util.List;

import javax.management.JMX;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import com.google.common.collect.ImmutableList;

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import org.apache.cassandra.service.SampleNodeToolServerExtension;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;
import org.apache.cassandra.tools.NodeTool.NodeToolCmdRunnable;

/**
 * Sample client-side extension used by the nodetool extension tests: a top-level command that does not need the
 * node, and a command group whose command calls the MBean registered by
 * {@link SampleNodeToolServerExtension}.
 */
public class SampleNodeToolExtension implements NodeToolExtension
{
    public static final String ECHO_COMMAND = "sampleecho";
    public static final String GROUP = "sample";
    public static final String GROUP_DESCRIPTION = "Sample commands contributed by a nodetool extension";
    public static final String PING_COMMAND = "ping";

    @Override
    public List<Class<? extends NodeToolCmdRunnable>> commands()
    {
        return ImmutableList.of(Echo.class);
    }

    @Override
    public List<CommandGroup> commandGroups()
    {
        return ImmutableList.of(new CommandGroup(GROUP, GROUP_DESCRIPTION, null, ImmutableList.of(Ping.class)));
    }

    /** Prints its arguments without connecting to the node. */
    @Command(name = ECHO_COMMAND, description = "Prints its arguments")
    public static class Echo implements NodeToolCmdRunnable
    {
        @Arguments(description = "The words to print")
        private List<String> words = new ArrayList<>();

        @Override
        public void run(INodeProbeFactory nodeProbeFactory, Output output)
        {
            output.out.println(String.join(" ", words));
        }
    }

    /** Calls the sample MBean over the JMX connection of the probe. */
    @Command(name = PING_COMMAND, description = "Pings the MBean registered by the sample server-side extension")
    public static class Ping extends NodeToolCmd
    {
        @Arguments(title = "message", description = "The message to send", required = true)
        private String message;

        @Override
        protected void execute(NodeProbe probe)
        {
            try
            {
                ObjectName name = new ObjectName(SampleNodeToolServerExtension.MBEAN_NAME);
                SampleNodeToolServerExtension.SampleMBean mbean =
                    JMX.newMBeanProxy(probe.getMbeanServerConn(), name, SampleNodeToolServerExtension.SampleMBean.class);
                output.out.println(mbean.ping(message));
            }
            catch (MalformedObjectNameException e)
            {
                throw new RuntimeException(e);
            }
        }
    }
}
