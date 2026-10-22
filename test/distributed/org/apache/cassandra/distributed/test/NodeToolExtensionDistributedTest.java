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
package org.apache.cassandra.distributed.test;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.service.NodeToolServerExtension;
import org.apache.cassandra.service.SampleNodeToolServerExtension;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tools.SampleNodeToolExtension;
import org.apache.cassandra.utils.MBeanWrapper;

import static org.apache.cassandra.config.CassandraRelevantProperties.CUSTOM_NODETOOL_EXTENSION_CLASSES;
import static org.apache.cassandra.config.CassandraRelevantProperties.CUSTOM_NODETOOL_SERVER_EXTENSION_CLASSES;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.JMX;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The nodetool extension hooks in the in-JVM dtest harness: the server-side extension is set up when the instance
 * starts, and the client-side extension commands run through {@code nodetoolResult}, reaching the MBeans of the
 * instance.
 */
public class NodeToolExtensionDistributedTest extends TestBaseImpl
{
    @Test
    public void extensionCommandsRunAgainstTheInstance() throws Throwable
    {
        try (WithProperties properties = new WithProperties().set(CUSTOM_NODETOOL_SERVER_EXTENSION_CLASSES,
                                                                  SampleNodeToolServerExtension.class.getName(),
                                                                  RecordingServerExtension.class.getName())
                                                             .set(CUSTOM_NODETOOL_EXTENSION_CLASSES, SampleNodeToolExtension.class.getName());
             Cluster cluster = init(Cluster.build(1).withConfig(config -> config.with(GOSSIP, NETWORK, JMX)).start()))
        {
            IInvokableInstance node = cluster.get(1);

            assertThat(node.callOnInstance(() -> SampleNodeToolServerExtension.setups.get())).isEqualTo(1);
            assertThat(node.callOnInstance(() -> MBeanWrapper.instance.isRegistered(SampleNodeToolServerExtension.MBEAN_NAME))).isTrue();
            // the extensions are set up before the node joins the ring, with the JMX server already up
            assertThat(node.callOnInstance(() -> RecordingServerExtension.storageServiceInitializedAtSetup)).isFalse();
            assertThat(node.callOnInstance(() -> RecordingServerExtension.sampleMBeanRegisteredAtSetup)).isTrue();

            node.nodetoolResult(SampleNodeToolExtension.GROUP, SampleNodeToolExtension.PING_COMMAND, "hello")
                .asserts().success().stdoutContains("pong hello");
            node.nodetoolResult(SampleNodeToolExtension.ECHO_COMMAND, "hello", "world")
                .asserts().success().stdoutContains("hello world");
            // the built-in help prints to System.out, which nodetoolResult does not capture: only its exit code is checked
            node.nodetoolResult("help").asserts().success();
            node.nodetoolResult("version").asserts().success();
        }
    }

    @Test
    public void withoutThePropertiesNothingIsAdded() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(1).withConfig(config -> config.with(JMX)).start()))
        {
            IInvokableInstance node = cluster.get(1);
            assertThat(node.callOnInstance(() -> SampleNodeToolServerExtension.setups.get())).isEqualTo(0);
            assertThat(node.callOnInstance(() -> MBeanWrapper.instance.isRegistered(SampleNodeToolServerExtension.MBEAN_NAME))).isFalse();
            node.nodetoolResult(SampleNodeToolExtension.ECHO_COMMAND, "hello").asserts().failure();
        }
    }

    /** Records, when it is set up, what the state of the node is. Listed after the sample server extension. */
    public static class RecordingServerExtension implements NodeToolServerExtension
    {
        public static volatile Boolean storageServiceInitializedAtSetup;
        public static volatile Boolean sampleMBeanRegisteredAtSetup;

        @Override
        public void setup()
        {
            storageServiceInitializedAtSetup = StorageService.instance.isInitialized();
            sampleMBeanRegisteredAtSetup = MBeanWrapper.instance.isRegistered(SampleNodeToolServerExtension.MBEAN_NAME);
        }
    }
}
