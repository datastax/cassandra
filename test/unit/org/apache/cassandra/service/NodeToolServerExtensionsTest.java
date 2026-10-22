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
package org.apache.cassandra.service;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.MBeanWrapper;

import static org.apache.cassandra.config.CassandraRelevantProperties.CUSTOM_NODETOOL_SERVER_EXTENSION_CLASSES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class NodeToolServerExtensionsTest
{
    private WithProperties properties;

    @Before
    public void setup()
    {
        properties = new WithProperties();
        SampleNodeToolServerExtension.setups.set(0);
        CountingExtension.setups.set(0);
    }

    @After
    public void cleanup()
    {
        properties.close();
        if (MBeanWrapper.instance.isRegistered(SampleNodeToolServerExtension.MBEAN_NAME))
            MBeanWrapper.instance.unregisterMBean(SampleNodeToolServerExtension.MBEAN_NAME);
    }

    private void extensions(String... classnames)
    {
        properties.set(CUSTOM_NODETOOL_SERVER_EXTENSION_CLASSES, classnames);
    }

    private static void loadAndSetup()
    {
        NodeToolServerExtensions.setupAll(NodeToolServerExtensions.load());
    }

    @Test
    public void noPropertyMeansNoExtension()
    {
        assertThat(NodeToolServerExtensions.load()).isEmpty();
        loadAndSetup();
        assertThat(MBeanWrapper.instance.isRegistered(SampleNodeToolServerExtension.MBEAN_NAME)).isFalse();
    }

    @Test
    public void blankPropertyMeansNoExtension()
    {
        extensions(" , ");
        assertThat(NodeToolServerExtensions.load()).isEmpty();
    }

    @Test
    public void setupRunsTheExtension()
    {
        extensions(SampleNodeToolServerExtension.class.getName());
        loadAndSetup();
        assertThat(SampleNodeToolServerExtension.setups.get()).isEqualTo(1);
        assertThat(MBeanWrapper.instance.isRegistered(SampleNodeToolServerExtension.MBEAN_NAME)).isTrue();
    }

    @Test
    public void extensionsAreLoadedAndSetUpInOrder()
    {
        extensions(CountingExtension.class.getName(), SampleNodeToolServerExtension.class.getName());
        List<NodeToolServerExtension> extensions = NodeToolServerExtensions.load();
        assertThat(extensions).hasSize(2);
        assertThat(extensions.get(0)).isInstanceOf(CountingExtension.class);
        assertThat(extensions.get(1)).isInstanceOf(SampleNodeToolServerExtension.class);

        NodeToolServerExtensions.setupAll(extensions);
        assertThat(CountingExtension.setups.get()).isEqualTo(1);
        assertThat(SampleNodeToolServerExtension.setups.get()).isEqualTo(1);
    }

    @Test
    public void unknownClassIsRejected()
    {
        extensions("org.apache.cassandra.service.DoesNotExist");
        assertThatThrownBy(NodeToolServerExtensionsTest::loadAndSetup).isInstanceOf(ConfigurationException.class)
                                                           .hasMessageContaining("Unable to find nodetool server extension")
                                                           .hasMessageContaining(CUSTOM_NODETOOL_SERVER_EXTENSION_CLASSES.getKey())
                                                           .hasMessageContaining("org.apache.cassandra.service.DoesNotExist");
    }

    @Test
    public void classThatIsNotAnExtensionIsRejected()
    {
        extensions(String.class.getName());
        assertThatThrownBy(NodeToolServerExtensionsTest::loadAndSetup).isInstanceOf(ConfigurationException.class)
                                                           .hasMessageContaining(String.class.getName())
                                                           .hasMessageContaining("is not a " + NodeToolServerExtension.class.getName());
    }

    @Test
    public void classListedTwiceIsRejected()
    {
        extensions(SampleNodeToolServerExtension.class.getName(), SampleNodeToolServerExtension.class.getName());
        assertThatThrownBy(NodeToolServerExtensions::load).isInstanceOf(ConfigurationException.class)
                                                          .hasMessageContaining(SampleNodeToolServerExtension.class.getName())
                                                          .hasMessageContaining("is listed twice");
    }

    @Test
    public void loadDoesNotSetUp()
    {
        extensions(SampleNodeToolServerExtension.class.getName());
        assertThat(NodeToolServerExtensions.load()).hasSize(1);
        assertThat(SampleNodeToolServerExtension.setups.get()).isEqualTo(0);
        assertThat(MBeanWrapper.instance.isRegistered(SampleNodeToolServerExtension.MBEAN_NAME)).isFalse();
    }

    @Test
    public void extensionFailingToSetUpAbortsTheSetup()
    {
        extensions(ThrowingExtension.class.getName(), SampleNodeToolServerExtension.class.getName());
        assertThatThrownBy(NodeToolServerExtensionsTest::loadAndSetup).isInstanceOf(IllegalStateException.class).hasMessage("boom");
        assertThat(SampleNodeToolServerExtension.setups.get()).isEqualTo(0);
    }

    public static class CountingExtension implements NodeToolServerExtension
    {
        static final AtomicInteger setups = new AtomicInteger();

        @Override
        public void setup()
        {
            setups.incrementAndGet();
        }
    }

    public static class ThrowingExtension implements NodeToolServerExtension
    {
        @Override
        public void setup()
        {
            throw new IllegalStateException("boom");
        }
    }
}
